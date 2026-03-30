//! # The credible layer sidecar
#![doc = include_str!("../README.md")]

use assertion_da_client::DaClient;
use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use credible_utils::shutdown::wait_for_sigterm;
use flume::unbounded;
use mdbx::{
    Reader,
    StateReader,
    common::CircularBufferConfig,
};
use metrics::counter;
use sidecar::{
    args::{
        Config,
        StateSourceConfig,
    },
    cache::{
        Sources,
        sources::{
            Source,
            eth_rpc_source::EthRpcSource,
            mdbx::MdbxSource,
        },
    },
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    critical,
    da_reachability::run_da_reachability_monitor,
    engine::{
        CoreEngine,
        CoreEngineConfig,
        queue::{
            TransactionQueueReceiver,
            TransactionQueueSender,
        },
    },
    event_sequencing::EventSequencing,
    graphql_event_source::GraphqlEventSource,
    health::{
        AssertionDaReadiness,
        HealthServer,
        HealthState,
        TransactionObserverReadiness,
    },
    indexer,
    indexer::IndexerCfg,
    metrics::{
        StateWorkerRuntimeSnapshot,
        state_worker_runtime_state,
    },
    transaction_observer::{
        IncidentReportReceiver,
        IncidentReportSender,
        TransactionObserver,
        TransactionObserverConfig,
    },
    transactions_state::{
        TransactionResultEvent,
        TransactionsState,
    },
    transport::{
        Transport,
        grpc::GrpcTransport,
    },
    utils::ErrorRecoverability,
};
use state_worker::{
    metrics::{
        clear_flush_permitted_head,
        clear_runtime_transient_heads,
        clear_traced_head,
        initialize_runtime_heads,
    },
    runtime::{
        RuntimeObserver,
        StateWorkerRuntimeConfig,
        run_supervisor,
    },
};
use std::{
    env,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread,
    thread::JoinHandle,
    time::Duration,
};
use tracing::{
    error,
    info,
    warn,
};

struct SidecarInfo {
    version: &'static str,
    git_commit: &'static str,
    rustc_version: &'static str,
    cpu_cores: usize,
    memory_available: String,
    os_info: String,
}

const STATE_WORKER_RESTART_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_HOST_GENESIS_PATH: &str =
    "docker/maru-besu-sidecar/config/l2-genesis-initialization/genesis-besu.json";
const DEFAULT_CONTAINER_GENESIS_PATH: &str = "/etc/credible-sidecar/genesis-besu.json";

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    let _guard = rust_tracing::trace();
    let config = Config::load()?;

    // Saves the dhat file on drop()
    #[cfg(feature = "dhat-heap")]
    let _dhat_profiler = if let Some(ref output_path) = config.dhat_output_path {
        tracing::info!(
            "Starting dhat memory profiler, output will be written to: {}",
            output_path.display()
        );
        Some(dhat::Profiler::builder().file_name(output_path).build())
    } else {
        None
    };

    SidecarInfo::collect().log(&config);

    let executor_config = init_executor_config(&config);
    let assertion_store = init_assertion_store(&config)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());
    let health_bind_addr: SocketAddr = config.transport.health_bind_addr.parse()?;

    loop {
        let should_shutdown = Box::pin(run_sidecar_once(
            &config,
            &executor_config,
            &assertion_store,
            &assertion_executor,
            health_bind_addr,
        ))
        .await?;

        if should_shutdown {
            break;
        }

        counter!("sidecar_restarts_total").increment(1);
        tracing::warn!(
            state_worker_restart_backoff_seconds = STATE_WORKER_RESTART_BACKOFF.as_secs_f64(),
            "Sidecar restarting..."
        );
        tokio::time::sleep(STATE_WORKER_RESTART_BACKOFF).await;
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}

impl SidecarInfo {
    fn collect() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            git_commit: option_env!("SIDECAR_GIT_SHA").unwrap_or("unknown"),
            rustc_version: option_env!("SIDECAR_RUSTC_VERSION").unwrap_or("unknown"),
            cpu_cores: std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(0),
            memory_available: available_memory_info(),
            os_info: run_command("uname", &["-a"])
                .unwrap_or_else(|| format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)),
        }
    }

    fn log(&self, config: &Config) {
        tracing::info!(
            version = self.version,
            git_commit = self.git_commit,
            rustc_version = self.rustc_version,
            "Sidecar build info"
        );
        tracing::info!(
            cpu_cores = self.cpu_cores,
            memory_available = %self.memory_available,
            os_info = %self.os_info,
            "Sidecar host info"
        );
        tracing::info!("Sidecar config: {config:?}");
    }
}

fn run_command(command: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(command)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

fn available_memory_info() -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if let Some(value) = line.strip_prefix("MemAvailable:") {
                    return value.trim().to_string();
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(vm_stat) = run_command("vm_stat", &[])
            && let Some(parsed) = parse_vm_stat_available(&vm_stat)
        {
            return parsed;
        }
    }

    "unknown".to_string()
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_available(vm_stat: &str) -> Option<String> {
    let page_size = parse_vm_stat_page_size(vm_stat)?;
    let free = parse_vm_stat_pages(vm_stat, "Pages free:")?;
    let inactive = parse_vm_stat_pages(vm_stat, "Pages inactive:").unwrap_or(0);
    let speculative = parse_vm_stat_pages(vm_stat, "Pages speculative:").unwrap_or(0);

    let available_pages = free.saturating_add(inactive).saturating_add(speculative);
    let available_bytes = available_pages.saturating_mul(page_size);
    let available_mib = available_bytes / (1024 * 1024);
    Some(format!("{available_mib} MiB"))
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_page_size(vm_stat: &str) -> Option<u64> {
    let first_line = vm_stat.lines().next()?;
    let marker = "page size of ";
    let start = first_line.find(marker)? + marker.len();
    let rest = &first_line[start..];
    let end = rest.find(" bytes")?;
    rest[..end].trim().parse::<u64>().ok()
}

#[cfg(target_os = "macos")]
fn parse_vm_stat_pages(vm_stat: &str, prefix: &str) -> Option<u64> {
    let line = vm_stat
        .lines()
        .find(|line| line.trim_start().starts_with(prefix))?;
    let value = line
        .split(':')
        .nth(1)?
        .trim()
        .trim_end_matches('.')
        .replace('.', "");
    value.parse::<u64>().ok()
}

/// Create gRPC transport with optional result streaming support.
///
/// With `result_event_rx`, enables `SubscribeResults` streaming.
fn create_transport_from_args(
    config: &Config,
    tx_sender: TransactionQueueSender,
    state_results: Arc<TransactionsState>,
    result_event_rx: Option<flume::Receiver<TransactionResultEvent>>,
) -> anyhow::Result<GrpcTransport> {
    let config = config.transport.clone();
    let transport = match result_event_rx {
        Some(rx) => GrpcTransport::with_result_receiver(&config, tx_sender, state_results, rx)?,
        None => GrpcTransport::new(config, tx_sender, state_results)?,
    };
    Ok(transport)
}

/// Holds handles to spawned threads for graceful shutdown
struct ThreadHandles {
    state_worker: Option<JoinHandle<anyhow::Result<()>>>,
    state_worker_shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    engine: Option<JoinHandle<Result<(), sidecar::engine::EngineError>>>,
    event_sequencing:
        Option<JoinHandle<Result<(), sidecar::event_sequencing::EventSequencingError>>>,
    transaction_observer:
        Option<JoinHandle<Result<(), sidecar::transaction_observer::TransactionObserverError>>>,
}

impl ThreadHandles {
    fn new() -> Self {
        Self {
            state_worker: None,
            state_worker_shutdown_tx: None,
            engine: None,
            event_sequencing: None,
            transaction_observer: None,
        }
    }

    fn join_all(&mut self) {
        if let Some(shutdown_tx) = self.state_worker_shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.state_worker.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("State worker thread exited cleanly"),
                Ok(Err(err)) => {
                    tracing::error!(error = ?err, "State worker thread exited with error")
                }
                Err(_) => tracing::error!("State worker thread panicked"),
            }
        }
        if let Some(handle) = self.engine.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Engine thread exited cleanly"),
                Ok(Err(e)) => tracing::error!(error = ?e, "Engine thread exited with error"),
                Err(_) => tracing::error!("Engine thread panicked"),
            }
        }
        if let Some(handle) = self.event_sequencing.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Event sequencing thread exited cleanly"),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Event sequencing thread exited with error");
                }
                Err(_) => tracing::error!("Event sequencing thread panicked"),
            }
        }
        if let Some(handle) = self.transaction_observer.take() {
            match handle.join() {
                Ok(Ok(())) => tracing::info!("Transaction observer thread exited cleanly"),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Transaction observer thread exited with error");
                }
                Err(_) => tracing::error!("Transaction observer thread panicked"),
            }
        }
    }
}

fn transaction_observer_config_from(config: &Config) -> Option<TransactionObserverConfig> {
    match (
        &config.credible.transaction_observer_db_path,
        &config.credible.transaction_observer_endpoint,
        &config.credible.transaction_observer_auth_token,
        config.credible.transaction_observer_endpoint_rps_max,
        config.credible.transaction_observer_poll_interval_ms,
    ) {
        (
            Some(db_path),
            Some(endpoint),
            Some(auth_token),
            Some(endpoint_rps_max),
            Some(poll_interval_ms),
        ) => {
            Some(TransactionObserverConfig {
                db_path: db_path.clone(),
                endpoint: endpoint.clone(),
                auth_token: auth_token.expose().to_string(),
                endpoint_rps_max,
                poll_interval: Duration::from_millis(poll_interval_ms),
                aeges_url: config.credible.aeges_url.clone(),
            })
        }
        _ => None,
    }
}

async fn build_sources_from_config(config: &Config) -> anyhow::Result<Vec<Arc<dyn Source>>> {
    let mut sources: Vec<Arc<dyn Source>> = Vec::new();

    if config.state.sources.is_empty() {
        if config.state.legacy.has_any() {
            warn!("Using legacy state source configuration; migrate to state.sources.");
        }
        if let (Some(state_worker_path), Some(state_worker_depth)) = (
            config.state.legacy.state_worker_mdbx_path.as_ref(),
            config.state.legacy.state_worker_depth,
        ) {
            match StateReader::new(
                state_worker_path,
                CircularBufferConfig::new(u8::try_from(state_worker_depth)?)?,
            ) {
                Ok(state_worker_client) => {
                    sources.push(Arc::new(MdbxSource::new(state_worker_client)));
                }
                Err(e) => {
                    error!(error = ?e, "Failed to connect MDBX");
                }
            }
        }

        if let (Some(eth_rpc_source_ws_url), Some(eth_rpc_source_http_url)) = (
            &config.state.legacy.eth_rpc_source_ws_url,
            &config.state.legacy.eth_rpc_source_http_url,
        ) && let Ok(eth_rpc_source) = EthRpcSource::try_build(
            eth_rpc_source_ws_url.as_str(),
            eth_rpc_source_http_url.as_str(),
        )
        .await
        {
            sources.push(eth_rpc_source);
        }
    } else {
        if config.state.legacy.has_any() {
            warn!("Ignoring legacy state source configuration; state.sources is set.");
        }
        for source_config in &config.state.sources {
            match source_config {
                StateSourceConfig::Mdbx { mdbx_path, depth } => {
                    match StateReader::new(
                        mdbx_path,
                        CircularBufferConfig::new(u8::try_from(*depth)?)?,
                    ) {
                        Ok(state_worker_client) => {
                            sources.push(Arc::new(MdbxSource::new(state_worker_client)));
                        }
                        Err(e) => {
                            error!(error = ?e, "Failed to connect MDBX");
                        }
                    }
                }
                StateSourceConfig::EthRpc { ws_url, http_url } => {
                    if let Ok(eth_rpc_source) =
                        EthRpcSource::try_build(ws_url.as_str(), http_url.as_str()).await
                    {
                        sources.push(eth_rpc_source);
                    }
                }
            }
        }
    }

    Ok(sources)
}

fn result_ttl(config: &Config) -> Duration {
    if config.credible.accepted_txs_ttl_ms.is_zero() {
        Duration::from_secs(2)
    } else {
        config.credible.accepted_txs_ttl_ms
    }
}

async fn observer_exit_future(
    observer_exited_rx: Option<
        tokio::sync::oneshot::Receiver<
            Result<(), sidecar::transaction_observer::TransactionObserverError>,
        >,
    >,
) -> Result<(), sidecar::transaction_observer::TransactionObserverError> {
    if let Some(observer_exited) = observer_exited_rx {
        observer_exited
            .await
            .map_err(|_| sidecar::transaction_observer::TransactionObserverError::ChannelClosed)?
    } else {
        std::future::pending().await
    }
}

fn record_error_recoverability(recoverable: bool) {
    if recoverable {
        counter!("sidecar_recoverable_errors_total").increment(1);
    } else {
        counter!("sidecar_irrecoverable_errors_total").increment(1);
    }
}

fn handle_transport_exit(
    health_state: &HealthState,
    result: Result<(), sidecar::transport::grpc::GrpcTransportError>,
) {
    if let Err(e) = result {
        health_state.record_component_failure("transport");
        let recoverable = ErrorRecoverability::from(&e).is_recoverable();
        record_error_recoverability(recoverable);
        if recoverable {
            tracing::error!(error = ?e, "Transport exited");
        } else {
            critical!(error = ?e, "Transport exited");
        }
    }
}

fn handle_engine_exit(
    health_state: &HealthState,
    result: Result<
        Result<(), sidecar::engine::EngineError>,
        tokio::sync::oneshot::error::RecvError,
    >,
) {
    match result {
        Ok(Ok(())) => {
            health_state.record_component_failure("engine");
            tracing::warn!("Engine exited unexpectedly");
        }
        Ok(Err(e)) => {
            health_state.record_component_failure("engine");
            let recoverable = ErrorRecoverability::from(&e).is_recoverable();
            record_error_recoverability(recoverable);
            if recoverable {
                tracing::error!(error = ?e, "Engine exited with recoverable error");
            } else {
                critical!(error = ?e, "Engine exited with unrecoverable error");
            }
        }
        Err(_) => {
            health_state.record_component_failure("engine");
            tracing::error!("Engine notification channel dropped");
        }
    }
}

fn handle_event_sequencing_exit(
    health_state: &HealthState,
    result: Result<
        Result<(), sidecar::event_sequencing::EventSequencingError>,
        tokio::sync::oneshot::error::RecvError,
    >,
) {
    match result {
        Ok(Ok(())) => {
            health_state.record_component_failure("event_sequencing");
            tracing::warn!("Event sequencing exited unexpectedly");
        }
        Ok(Err(e)) => {
            health_state.record_component_failure("event_sequencing");
            let recoverable = ErrorRecoverability::from(&e).is_recoverable();
            record_error_recoverability(recoverable);
            if recoverable {
                tracing::error!(error = ?e, "Event sequencing exited with recoverable error");
            } else {
                critical!(error = ?e, "Event sequencing exited with unrecoverable error");
            }
        }
        Err(_) => {
            health_state.record_component_failure("event_sequencing");
            tracing::error!("Event sequencing notification channel dropped");
        }
    }
}

fn handle_observer_exit(
    health_state: &HealthState,
    result: Result<(), sidecar::transaction_observer::TransactionObserverError>,
) {
    health_state.record_component_failure("transaction_observer");
    health_state.set_transaction_observer_readiness(TransactionObserverReadiness::Failed);
    match result {
        Ok(()) => tracing::warn!("Transaction observer exited unexpectedly"),
        Err(e) => {
            let recoverable = ErrorRecoverability::from(&e).is_recoverable();
            record_error_recoverability(recoverable);
            if recoverable {
                tracing::error!(error = ?e, "Transaction observer exited with recoverable error");
            } else {
                critical!(error = ?e, "Transaction observer exited with unrecoverable error");
            }
        }
    }
}

fn handle_indexer_exit(health_state: &HealthState, result: Result<(), indexer::IndexerError>) {
    if let Err(e) = result {
        health_state.record_component_failure("assertion_indexer");
        let recoverable = ErrorRecoverability::from(&e).is_recoverable();
        record_error_recoverability(recoverable);
        if recoverable {
            tracing::error!(error = ?e, "Assertion indexer exited");
        } else {
            critical!(error = ?e, "Assertion indexer exited");
        }
    }
}

async fn run_async_components(
    transport: &mut GrpcTransport,
    health_server: &mut HealthServer,
    health_state: Arc<HealthState>,
    indexer_cfg: IndexerCfg<GraphqlEventSource>,
    engine_exited: tokio::sync::oneshot::Receiver<Result<(), sidecar::engine::EngineError>>,
    seq_exited: tokio::sync::oneshot::Receiver<
        Result<(), sidecar::event_sequencing::EventSequencingError>,
    >,
    observer_exited_rx: Option<
        tokio::sync::oneshot::Receiver<
            Result<(), sidecar::transaction_observer::TransactionObserverError>,
        >,
    >,
) -> bool {
    let mut should_shutdown = false;
    let observer_exited = observer_exit_future(observer_exited_rx);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
            should_shutdown = true;
        }
        _ = wait_for_sigterm() => {
            tracing::info!("Received SIGTERM, shutting down...");
            should_shutdown = true;
        }
        result = transport.run() => {
            handle_transport_exit(&health_state, result);
        }
        result = engine_exited => {
            handle_engine_exit(&health_state, result);
        }
        result = seq_exited => {
            handle_event_sequencing_exit(&health_state, result);
        }
        result = observer_exited => {
            handle_observer_exit(&health_state, result);
        }
        result = health_server.run() => {
            if let Err(e) = result {
                critical!(error = ?e, "Health server exited");
            }
        }
        result = indexer::run_indexer(indexer_cfg) => {
            handle_indexer_exit(&health_state, result);
        }
    }

    should_shutdown
}

fn spawn_da_reachability_monitor(
    config: &Config,
    da_client: DaClient,
    health_state: Arc<HealthState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_da_reachability_monitor(
        da_client,
        config.credible.assertion_da_rpc_url.clone(),
        health_state,
    ))
}

async fn stop_da_reachability_monitor(
    da_reachability_handle: tokio::task::JoinHandle<()>,
    health_state: &HealthState,
) {
    da_reachability_handle.abort();
    if let Err(join_err) = da_reachability_handle.await
        && !join_err.is_cancelled()
    {
        health_state.record_component_failure("assertion_da_monitor");
        tracing::error!(
            error = ?join_err,
            "Assertion DA reachability monitor exited unexpectedly"
        );
    }
}

struct SidecarRuntimeState {
    state: Arc<Sources>,
    cache: OverlayDb<Sources>,
    state_worker: Option<StateWorkerRuntimeConfig>,
}

struct SidecarChannels {
    transport_tx_sender: TransactionQueueSender,
    event_sequencing_rx: TransactionQueueReceiver,
    event_sequencing_tx: TransactionQueueSender,
    engine_rx: TransactionQueueReceiver,
    incident_report_tx: Option<IncidentReportSender>,
    incident_report_rx: Option<IncidentReportReceiver>,
    engine_state_results: Arc<TransactionsState>,
    result_event_rx: Option<flume::Receiver<TransactionResultEvent>>,
}

struct AsyncSupportServices {
    health_server: HealthServer,
    indexer_cfg: IndexerCfg<GraphqlEventSource>,
    da_reachability_handle: tokio::task::JoinHandle<()>,
}

type EngineExitReceiver = tokio::sync::oneshot::Receiver<Result<(), sidecar::engine::EngineError>>;
type SequencingExitReceiver =
    tokio::sync::oneshot::Receiver<Result<(), sidecar::event_sequencing::EventSequencingError>>;
type ObserverExitReceiver = tokio::sync::oneshot::Receiver<
    Result<(), sidecar::transaction_observer::TransactionObserverError>,
>;
type ObserverThread = (
    JoinHandle<Result<(), sidecar::transaction_observer::TransactionObserverError>>,
    ObserverExitReceiver,
);

struct SidecarThreads {
    thread_handles: ThreadHandles,
    engine_exited: EngineExitReceiver,
    seq_exited: SequencingExitReceiver,
    observer_exited_rx: Option<ObserverExitReceiver>,
}

async fn run_sidecar_once(
    config: &Config,
    executor_config: &assertion_executor::ExecutorConfig,
    assertion_store: &assertion_executor::store::AssertionStore,
    assertion_executor: &AssertionExecutor,
    health_bind_addr: SocketAddr,
) -> anyhow::Result<bool> {
    let health_state = Arc::new(HealthState::default());

    // Shared shutdown flag for this iteration
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    let runtime_state = initialize_sidecar_runtime(config).await?;
    let transaction_observer_config = initialize_health_runtime(config, health_state.as_ref());
    let mut channels = initialize_sidecar_channels(config, transaction_observer_config.is_some());

    let mut transport = create_transport_from_args(
        config,
        channels.transport_tx_sender.clone(),
        channels.engine_state_results.clone(),
        channels.result_event_rx.take(),
    )?;
    let SidecarThreads {
        thread_handles,
        engine_exited,
        seq_exited,
        observer_exited_rx,
    } = spawn_sidecar_threads(
        config,
        Arc::clone(&shutdown_flag),
        runtime_state,
        channels,
        assertion_executor,
        health_state.clone(),
        transaction_observer_config,
    )
    .await?;

    let AsyncSupportServices {
        mut health_server,
        indexer_cfg,
        da_reachability_handle,
    } = initialize_async_support_services(
        config,
        executor_config,
        assertion_store,
        health_bind_addr,
        health_state.clone(),
    )
    .await?;

    let should_shutdown = Box::pin(run_async_components(
        &mut transport,
        &mut health_server,
        health_state.clone(),
        indexer_cfg,
        engine_exited,
        seq_exited,
        observer_exited_rx,
    ))
    .await;

    shutdown_sidecar_runtime(
        shutdown_flag,
        transport,
        health_server,
        thread_handles,
        da_reachability_handle,
        health_state.as_ref(),
    )
    .await;

    Ok(should_shutdown)
}

async fn initialize_sidecar_runtime(config: &Config) -> anyhow::Result<SidecarRuntimeState> {
    initialize_state_worker_runtime(config);
    let sources = build_sources_from_config(config).await?;
    let state = Arc::new(Sources::new(sources, config.state.minimum_state_diff));
    let cache = OverlayDb::new(Some(state.clone()));
    let state_worker = integrated_state_worker_config(config)?;
    Ok(SidecarRuntimeState {
        state,
        cache,
        state_worker,
    })
}

fn initialize_health_runtime(
    config: &Config,
    health_state: &HealthState,
) -> Option<TransactionObserverConfig> {
    let transaction_observer_config = transaction_observer_config_from(config);
    let observer_readiness = if transaction_observer_config.is_some() {
        TransactionObserverReadiness::Healthy
    } else {
        TransactionObserverReadiness::Disabled
    };
    health_state.set_transaction_observer_readiness(observer_readiness);
    health_state.set_assertion_da_readiness(AssertionDaReadiness::Unknown);
    transaction_observer_config
}

fn initialize_sidecar_channels(
    config: &Config,
    transaction_observer_enabled: bool,
) -> SidecarChannels {
    // Channel: Transport -> EventSequencing
    let (transport_tx_sender, event_sequencing_rx) = unbounded();
    // Channel: EventSequencing -> CoreEngine
    let (event_sequencing_tx, engine_rx) = unbounded();
    // Channel: CoreEngine -> TransactionObserver
    let (incident_report_tx, incident_report_rx) = if transaction_observer_enabled {
        let (tx, rx) = unbounded();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (result_tx, result_rx) = unbounded();
    let accepted_txs_ttl = result_ttl(config);
    let engine_state_results =
        TransactionsState::with_result_sender_and_ttls(result_tx, accepted_txs_ttl);

    SidecarChannels {
        transport_tx_sender,
        event_sequencing_rx,
        event_sequencing_tx,
        engine_rx,
        incident_report_tx,
        incident_report_rx,
        engine_state_results,
        result_event_rx: Some(result_rx),
    }
}

async fn spawn_sidecar_threads(
    config: &Config,
    shutdown_flag: Arc<AtomicBool>,
    SidecarRuntimeState {
        state,
        cache,
        state_worker,
    }: SidecarRuntimeState,
    SidecarChannels {
        event_sequencing_rx,
        event_sequencing_tx,
        engine_rx,
        incident_report_tx,
        incident_report_rx,
        engine_state_results,
        ..
    }: SidecarChannels,
    assertion_executor: &AssertionExecutor,
    health_state: Arc<HealthState>,
    transaction_observer_config: Option<TransactionObserverConfig>,
) -> anyhow::Result<SidecarThreads> {
    let mut thread_handles = ThreadHandles::new();
    if let Some((handle, shutdown_tx)) = spawn_state_worker_supervisor(state_worker)? {
        thread_handles.state_worker = Some(handle);
        thread_handles.state_worker_shutdown_tx = Some(shutdown_tx);
    }
    let event_sequencing = EventSequencing::new(event_sequencing_rx, event_sequencing_tx);
    let (seq_handle, seq_exited) = event_sequencing.spawn(Arc::clone(&shutdown_flag))?;
    thread_handles.event_sequencing = Some(seq_handle);

    let engine_config = CoreEngineConfig {
        transaction_results_max_capacity: config.credible.transaction_results_max_capacity,
        state_sources_sync_timeout: Duration::from_millis(config.state.sources_sync_timeout_ms),
        source_monitoring_period: Duration::from_millis(config.state.sources_monitoring_period_ms),
        health_state,
        overlay_cache_invalidation_every_block: config
            .credible
            .overlay_cache_invalidation_every_block
            .unwrap_or(false),
        incident_sender: incident_report_tx,
        #[cfg(feature = "cache_validation")]
        provider_ws_url: Some(config.credible.cache_checker_ws_url.clone()),
    };
    let engine = CoreEngine::new(
        cache,
        state,
        engine_rx,
        assertion_executor.clone(),
        engine_state_results,
        engine_config,
    )
    .await;
    let (engine_handle, engine_exited) = engine.spawn(Arc::clone(&shutdown_flag))?;
    thread_handles.engine = Some(engine_handle);

    let observer_exited_rx = spawn_transaction_observer(
        shutdown_flag,
        transaction_observer_config,
        incident_report_rx,
    )?
    .inspect(|_| tracing::info!("Transaction observer enabled"))
    .map(|(observer_handle, observer_exited)| {
        thread_handles.transaction_observer = Some(observer_handle);
        observer_exited
    });

    Ok(SidecarThreads {
        thread_handles,
        engine_exited,
        seq_exited,
        observer_exited_rx,
    })
}

fn spawn_transaction_observer(
    shutdown_flag: Arc<AtomicBool>,
    transaction_observer_config: Option<TransactionObserverConfig>,
    incident_report_rx: Option<IncidentReportReceiver>,
) -> anyhow::Result<Option<ObserverThread>> {
    if let (Some(transaction_observer_config), Some(incident_report_rx)) =
        (transaction_observer_config, incident_report_rx)
    {
        let transaction_observer =
            TransactionObserver::new(transaction_observer_config, incident_report_rx)?;
        let observer_thread = transaction_observer.spawn(shutdown_flag)?;
        return Ok(Some(observer_thread));
    }

    tracing::info!("Transaction observer disabled: missing config");
    Ok(None)
}

async fn initialize_async_support_services(
    config: &Config,
    executor_config: &assertion_executor::ExecutorConfig,
    assertion_store: &assertion_executor::store::AssertionStore,
    health_bind_addr: SocketAddr,
    health_state: Arc<HealthState>,
) -> anyhow::Result<AsyncSupportServices> {
    let health_server = HealthServer::new(health_bind_addr, health_state.clone());
    let da_client = DaClient::new(&config.credible.assertion_da_rpc_url)?;
    let indexer_cfg = init_indexer_config(
        config,
        assertion_store.clone(),
        executor_config,
        da_client.clone(),
    )
    .await?;
    let da_reachability_handle = spawn_da_reachability_monitor(config, da_client, health_state);

    Ok(AsyncSupportServices {
        health_server,
        indexer_cfg,
        da_reachability_handle,
    })
}

async fn shutdown_sidecar_runtime(
    shutdown_flag: Arc<AtomicBool>,
    mut transport: GrpcTransport,
    mut health_server: HealthServer,
    mut thread_handles: ThreadHandles,
    da_reachability_handle: tokio::task::JoinHandle<()>,
    health_state: &HealthState,
) {
    stop_da_reachability_monitor(da_reachability_handle, health_state).await;

    tracing::info!("Signaling threads to shutdown...");
    shutdown_flag.store(true, Ordering::Relaxed);

    transport.stop();
    health_server.stop();
    drop(transport);
    drop(health_server);

    thread_handles.join_all();
}

fn initialize_state_worker_runtime(config: &Config) {
    let durable_head = state_worker_available_head(config);
    state_worker_runtime_state().replace(state_worker_runtime_snapshot(
        0,
        Duration::ZERO,
        durable_head,
    ));
    initialize_runtime_heads(durable_head);
    clear_runtime_transient_heads();
}

fn integrated_state_worker_config(
    config: &Config,
) -> anyhow::Result<Option<StateWorkerRuntimeConfig>> {
    let Some((mdbx_path, depth)) = state_worker_mdbx_config(config) else {
        return Ok(None);
    };
    let Some(ws_url) = state_worker_ws_url(config) else {
        return Ok(None);
    };
    let Some(genesis_path) = integrated_state_worker_genesis_path() else {
        warn!("State worker genesis file was not found; integrated worker will stay disabled");
        return Ok(None);
    };

    Ok(Some(StateWorkerRuntimeConfig {
        ws_url,
        mdbx_path,
        start_block: None,
        state_depth: depth,
        genesis_path,
    }))
}

fn state_worker_runtime_snapshot(
    restart_count: u64,
    restart_backoff: Duration,
    durable_head: Option<u64>,
) -> StateWorkerRuntimeSnapshot {
    StateWorkerRuntimeSnapshot::restart_snapshot(restart_count, restart_backoff, durable_head)
}

fn state_worker_available_head(config: &Config) -> Option<u64> {
    let (path, depth) = state_worker_mdbx_config(config)?;
    let backend = StateReader::new(path.as_str(), CircularBufferConfig::new(depth).ok()?).ok()?;
    backend
        .get_available_block_range()
        .ok()
        .flatten()
        .map(|(_, head)| head)
}

fn state_worker_mdbx_config(config: &Config) -> Option<(String, u8)> {
    if config.state.sources.is_empty() {
        let path = config.state.legacy.state_worker_mdbx_path.clone()?;
        let depth = u8::try_from(config.state.legacy.state_worker_depth?).ok()?;
        return Some((path, depth));
    }

    config.state.sources.iter().find_map(|source| {
        match source {
            StateSourceConfig::Mdbx { mdbx_path, depth } => {
                Some((mdbx_path.clone(), u8::try_from(*depth).ok()?))
            }
            StateSourceConfig::EthRpc { .. } => None,
        }
    })
}

fn state_worker_ws_url(config: &Config) -> Option<String> {
    if config.state.sources.is_empty() {
        return config.state.legacy.eth_rpc_source_ws_url.clone();
    }

    config.state.sources.iter().find_map(|source| {
        match source {
            StateSourceConfig::EthRpc { ws_url, .. } => Some(ws_url.clone()),
            StateSourceConfig::Mdbx { .. } => None,
        }
    })
}

fn integrated_state_worker_genesis_path() -> Option<String> {
    [
        env::var("SIDECAR_STATE_WORKER_FILE_TO_GENESIS").ok(),
        env::var("STATE_WORKER_FILE_TO_GENESIS").ok(),
        existing_path(DEFAULT_CONTAINER_GENESIS_PATH),
        repo_relative_existing_path(DEFAULT_HOST_GENESIS_PATH),
    ]
    .into_iter()
    .flatten()
    .find(|path| Path::new(path).exists())
}

fn existing_path(path: &str) -> Option<String> {
    Path::new(path).exists().then(|| path.to_string())
}

fn repo_relative_existing_path(path: &str) -> Option<String> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let candidate = repo_root.join(path);
    candidate.exists().then(|| candidate.display().to_string())
}

struct SidecarStateWorkerObserver;

impl RuntimeObserver for SidecarStateWorkerObserver {
    fn on_restart_state(&self, restart_count: u64, restart_backoff: Duration) {
        let runtime_state = state_worker_runtime_state();
        runtime_state.set_restart_state(restart_count, restart_backoff);
        runtime_state.clear_transient_heads();
        clear_runtime_transient_heads();
    }

    fn on_traced_head(&self, block_number: u64) {
        let runtime_state = state_worker_runtime_state();
        if runtime_state
            .snapshot()
            .durable_head
            .is_some_and(|durable_head| block_number <= durable_head)
        {
            clear_traced_head();
            return;
        }

        runtime_state.set_traced_head(Some(block_number));
    }

    fn on_flush_permitted_head(&self, block_number: u64) {
        let runtime_state = state_worker_runtime_state();
        if runtime_state
            .snapshot()
            .durable_head
            .is_some_and(|durable_head| block_number <= durable_head)
        {
            clear_flush_permitted_head();
            return;
        }

        runtime_state.set_flush_permitted_head(Some(block_number));
    }

    fn on_durable_head(&self, block_number: u64) {
        state_worker_runtime_state().set_durable_head(Some(block_number));
    }
}

fn spawn_state_worker_supervisor(
    config: Option<StateWorkerRuntimeConfig>,
) -> anyhow::Result<
    Option<(
        JoinHandle<anyhow::Result<()>>,
        tokio::sync::broadcast::Sender<()>,
    )>,
> {
    let Some(config) = config else {
        info!("Integrated state worker disabled: missing runtime configuration");
        return Ok(None);
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
    let handle = thread::Builder::new()
        .name("sidecar-state-worker".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_supervisor(
                &config,
                shutdown_rx,
                Arc::new(SidecarStateWorkerObserver),
            ))
        })?;

    Ok(Some((handle, shutdown_tx)))
}

#[cfg(test)]
mod tests {
    use super::{
        SidecarStateWorkerObserver,
        integrated_state_worker_genesis_path,
        state_worker_runtime_snapshot,
    };
    use sidecar::metrics::state_worker_runtime_state;
    use state_worker::runtime::RuntimeObserver;
    use std::{
        sync::Mutex,
        time::Duration,
    };

    static STATE_WORKER_RUNTIME_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn state_worker_runtime_snapshot_keeps_transient_heads_unknown_until_runtime_events() {
        let snapshot = state_worker_runtime_snapshot(3, Duration::from_secs(2), Some(42));

        assert_eq!(snapshot.restart_count, 3);
        assert_eq!(snapshot.restart_backoff, Duration::from_secs(2));
        assert_eq!(snapshot.traced_head, None);
        assert_eq!(snapshot.flush_permitted_head, None);
        assert_eq!(snapshot.durable_head, Some(42));
    }

    #[test]
    fn state_worker_observer_ignores_bootstrap_transient_heads_at_durable_head() {
        let _guard = STATE_WORKER_RUNTIME_TEST_LOCK.lock().unwrap();
        let runtime_state = state_worker_runtime_state();
        runtime_state.reset();
        runtime_state.replace(state_worker_runtime_snapshot(0, Duration::ZERO, Some(42)));

        let observer = SidecarStateWorkerObserver;
        observer.on_traced_head(42);
        observer.on_flush_permitted_head(42);

        let snapshot = runtime_state.snapshot();
        assert_eq!(snapshot.traced_head, None);
        assert_eq!(snapshot.flush_permitted_head, None);
        assert_eq!(snapshot.durable_head, Some(42));

        observer.on_traced_head(43);
        observer.on_flush_permitted_head(43);

        let snapshot = runtime_state.snapshot();
        assert_eq!(snapshot.traced_head, Some(43));
        assert_eq!(snapshot.flush_permitted_head, Some(43));
        assert_eq!(snapshot.durable_head, Some(42));

        runtime_state.reset();
    }

    #[test]
    fn state_worker_observer_clears_transient_heads_on_restart() {
        let _guard = STATE_WORKER_RUNTIME_TEST_LOCK.lock().unwrap();
        let runtime_state = state_worker_runtime_state();
        runtime_state.reset();
        runtime_state.set_durable_head(Some(42));
        runtime_state.set_traced_head(Some(43));
        runtime_state.set_flush_permitted_head(Some(43));

        let observer = SidecarStateWorkerObserver;
        observer.on_restart_state(2, Duration::from_secs(1));

        let snapshot = runtime_state.snapshot();
        assert_eq!(snapshot.restart_count, 2);
        assert_eq!(snapshot.restart_backoff, Duration::from_secs(1));
        assert_eq!(snapshot.traced_head, None);
        assert_eq!(snapshot.flush_permitted_head, None);
        assert_eq!(snapshot.durable_head, Some(42));

        runtime_state.reset();
    }

    #[test]
    fn integrated_state_worker_genesis_path_falls_back_to_repo_fixture() {
        let path = integrated_state_worker_genesis_path().expect("missing genesis path");
        assert!(
            path.ends_with(
                "docker/maru-besu-sidecar/config/l2-genesis-initialization/genesis-besu.json"
            ) || path == "/etc/credible-sidecar/genesis-besu.json"
        );
    }
}
