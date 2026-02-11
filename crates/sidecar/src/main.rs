//! # The credible layer sidecar
#![doc = include_str!("../README.md")]
#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::indexing_slicing)]

use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use credible_utils::shutdown::wait_for_sigterm;
use flume::unbounded;
use mdbx::{
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
        queue::TransactionQueueSender,
    },
    event_sequencing::EventSequencing,
    health::HealthServer,
    indexer,
    transaction_observer::{
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
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::Duration,
};
use tracing::{
    error,
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
        tracing::warn!("Sidecar restarting...");
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
    engine: Option<JoinHandle<Result<(), sidecar::engine::EngineError>>>,
    event_sequencing:
        Option<JoinHandle<Result<(), sidecar::event_sequencing::EventSequencingError>>>,
    transaction_observer:
        Option<JoinHandle<Result<(), sidecar::transaction_observer::TransactionObserverError>>>,
}

impl ThreadHandles {
    fn new() -> Self {
        Self {
            engine: None,
            event_sequencing: None,
            transaction_observer: None,
        }
    }

    fn join_all(&mut self) {
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

fn result_ttls(config: &Config) -> (Duration, Duration) {
    let pending_requests_ttl = if config
        .credible
        .transaction_results_pending_requests_ttl_ms
        .is_zero()
    {
        Duration::from_secs(2)
    } else {
        config.credible.transaction_results_pending_requests_ttl_ms
    };
    let accepted_txs_ttl = if config.credible.accepted_txs_ttl_ms.is_zero() {
        Duration::from_secs(2)
    } else {
        config.credible.accepted_txs_ttl_ms
    };
    (pending_requests_ttl, accepted_txs_ttl)
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

fn handle_transport_exit(result: Result<(), sidecar::transport::grpc::GrpcTransportError>) {
    if let Err(e) = result {
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
    result: Result<
        Result<(), sidecar::engine::EngineError>,
        tokio::sync::oneshot::error::RecvError,
    >,
) {
    match result {
        Ok(Ok(())) => tracing::warn!("Engine exited unexpectedly"),
        Ok(Err(e)) => {
            let recoverable = ErrorRecoverability::from(&e).is_recoverable();
            record_error_recoverability(recoverable);
            if recoverable {
                tracing::error!(error = ?e, "Engine exited with recoverable error");
            } else {
                critical!(error = ?e, "Engine exited with unrecoverable error");
            }
        }
        Err(_) => tracing::error!("Engine notification channel dropped"),
    }
}

fn handle_event_sequencing_exit(
    result: Result<
        Result<(), sidecar::event_sequencing::EventSequencingError>,
        tokio::sync::oneshot::error::RecvError,
    >,
) {
    match result {
        Ok(Ok(())) => tracing::warn!("Event sequencing exited unexpectedly"),
        Ok(Err(e)) => {
            let recoverable = ErrorRecoverability::from(&e).is_recoverable();
            record_error_recoverability(recoverable);
            if recoverable {
                tracing::error!(error = ?e, "Event sequencing exited with recoverable error");
            } else {
                critical!(error = ?e, "Event sequencing exited with unrecoverable error");
            }
        }
        Err(_) => tracing::error!("Event sequencing notification channel dropped"),
    }
}

fn handle_observer_exit(
    result: Result<(), sidecar::transaction_observer::TransactionObserverError>,
) {
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

fn handle_indexer_exit(result: Result<(), assertion_executor::store::IndexerError>) {
    if let Err(e) = result {
        let recoverable = ErrorRecoverability::from(&e).is_recoverable();
        record_error_recoverability(recoverable);
        if recoverable {
            tracing::error!(error = ?e, "Indexer exited");
        } else {
            critical!(error = ?e, "Indexer exited");
        }
    }
}

async fn run_async_components(
    transport: &mut GrpcTransport,
    health_server: &mut HealthServer,
    indexer_cfg: assertion_executor::store::IndexerCfg,
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
            handle_transport_exit(result);
        }
        result = engine_exited => {
            handle_engine_exit(result);
        }
        result = seq_exited => {
            handle_event_sequencing_exit(result);
        }
        result = observer_exited => {
            handle_observer_exit(result);
        }
        result = health_server.run() => {
            if let Err(e) = result {
                critical!(error = ?e, "Health server exited");
            }
        }
        result = indexer::run_indexer(indexer_cfg) => {
            handle_indexer_exit(result);
        }
    }

    should_shutdown
}

async fn run_sidecar_once(
    config: &Config,
    executor_config: &assertion_executor::ExecutorConfig,
    assertion_store: &assertion_executor::store::AssertionStore,
    assertion_executor: &AssertionExecutor,
    health_bind_addr: SocketAddr,
) -> anyhow::Result<bool> {
    let mut thread_handles = ThreadHandles::new();

    // Shared shutdown flag for this iteration
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    let sources = build_sources_from_config(config).await?;
    let state = Arc::new(Sources::new(sources, config.state.minimum_state_diff));
    let cache: OverlayDb<Sources> = OverlayDb::new(Some(state.clone()));

    let transaction_observer_config = transaction_observer_config_from(config);

    // Channel: Transport -> EventSequencing
    let (transport_tx_sender, event_sequencing_rx) = unbounded();
    // Channel: EventSequencing -> CoreEngine
    let (event_sequencing_tx, engine_rx) = unbounded();
    // Channel: CoreEngine -> TransactionObserver
    let (incident_report_tx, incident_report_rx) = if transaction_observer_config.is_some() {
        let (tx, rx) = unbounded();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (result_tx, result_rx) = unbounded();
    let (pending_requests_ttl, accepted_txs_ttl) = result_ttls(config);
    let engine_state_results = TransactionsState::with_result_sender_and_ttls(
        result_tx,
        pending_requests_ttl,
        accepted_txs_ttl,
    );
    let result_event_rx = Some(result_rx);

    let mut transport = create_transport_from_args(
        config,
        transport_tx_sender,
        engine_state_results.clone(),
        result_event_rx,
    )?;

    // Spawn EventSequencing on a dedicated OS thread
    let event_sequencing = EventSequencing::new(event_sequencing_rx, event_sequencing_tx);
    let (seq_handle, seq_exited) = event_sequencing.spawn(Arc::clone(&shutdown_flag))?;
    thread_handles.event_sequencing = Some(seq_handle);

    // Spawn CoreEngine on a dedicated OS thread
    let engine_config = CoreEngineConfig {
        transaction_results_max_capacity: config.credible.transaction_results_max_capacity,
        state_sources_sync_timeout: Duration::from_millis(config.state.sources_sync_timeout_ms),
        source_monitoring_period: Duration::from_millis(config.state.sources_monitoring_period_ms),
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
        engine_state_results.clone(),
        engine_config,
    )
    .await;
    let (engine_handle, engine_exited) = engine.spawn(Arc::clone(&shutdown_flag))?;
    thread_handles.engine = Some(engine_handle);

    let observer_exited_rx = if let (Some(transaction_observer_config), Some(incident_report_rx)) =
        (transaction_observer_config, incident_report_rx)
    {
        let transaction_observer =
            TransactionObserver::new(transaction_observer_config, incident_report_rx)?;
        let (observer_handle, observer_exited) =
            transaction_observer.spawn(Arc::clone(&shutdown_flag))?;
        thread_handles.transaction_observer = Some(observer_handle);
        Some(observer_exited)
    } else {
        tracing::info!("Transaction observer disabled: missing config");
        None
    };

    let mut health_server = HealthServer::new(health_bind_addr);

    let indexer_cfg = init_indexer_config(config, assertion_store.clone(), executor_config).await?;
    let da_reachability_handle = tokio::spawn(run_da_reachability_monitor(
        config.credible.assertion_da_rpc_url.clone(),
    ));

    let should_shutdown = Box::pin(run_async_components(
        &mut transport,
        &mut health_server,
        indexer_cfg,
        engine_exited,
        seq_exited,
        observer_exited_rx,
    ))
    .await;

    da_reachability_handle.abort();
    match da_reachability_handle.await {
        Err(join_err) if !join_err.is_cancelled() => {
            tracing::error!(
                error = ?join_err,
                "Assertion DA reachability monitor exited unexpectedly"
            );
        }
        _ => {}
    }

    // Signal threads to stop
    tracing::info!("Signaling threads to shutdown...");
    shutdown_flag.store(true, Ordering::Relaxed);

    // Cleanup async components
    transport.stop();
    health_server.stop();
    drop(transport);
    drop(health_server);

    // Wait for threads
    thread_handles.join_all();

    Ok(should_shutdown)
}
