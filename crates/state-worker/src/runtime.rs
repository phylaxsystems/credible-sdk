use crate::{
    genesis::{
        self,
        GenesisState,
    },
    geth_version::{
        MIN_GETH_VERSION,
        parse_geth_version,
    },
    metrics,
    state,
    system_calls::SystemCalls,
    worker::StateWorker,
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
    WsConnect,
};
use anyhow::{
    Context,
    Result,
};
use futures_util::FutureExt;
use mdbx::{
    Reader,
    StateWriter,
    Writer,
    common::CircularBufferConfig,
};
use std::{
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{
    info,
    warn,
};

const RESTART_BACKOFF: Duration = Duration::from_secs(1);
const TRACE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWorkerRuntimeConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    pub genesis_path: String,
}

pub trait RuntimeObserver: Send + Sync + 'static {
    fn on_restart_state(&self, _restart_count: u64, _restart_backoff: Duration) {}

    fn on_traced_head(&self, _block_number: u64) {}

    fn on_flush_permitted_head(&self, _block_number: u64) {}

    fn on_durable_head(&self, _block_number: u64) {}
}

#[derive(Debug, Default)]
pub struct NoopRuntimeObserver;

impl RuntimeObserver for NoopRuntimeObserver {}

pub async fn run_supervisor(
    config: &StateWorkerRuntimeConfig,
    mut shutdown_rx: broadcast::Receiver<()>,
    observer: Arc<dyn RuntimeObserver>,
) -> Result<()> {
    let mut restart_count = 0_u64;
    observer.on_restart_state(restart_count, Duration::ZERO);

    loop {
        if shutdown_rx.try_recv().is_ok() {
            info!("Shutdown signal received before state worker start");
            return Ok(());
        }

        let result = AssertUnwindSafe(run_once(
            config,
            shutdown_rx.resubscribe(),
            Arc::clone(&observer),
        ))
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(())) => {
                info!("State worker shutdown gracefully");
                return Ok(());
            }
            Ok(Err(err)) => {
                warn!(error = %err, "state worker failed; restarting");
            }
            Err(panic_payload) => {
                if let Some(message) = panic_payload.downcast_ref::<&str>() {
                    warn!(panic = %message, "state worker panicked; restarting");
                } else if let Some(message) = panic_payload.downcast_ref::<String>() {
                    warn!(panic = %message, "state worker panicked; restarting");
                } else {
                    warn!("state worker panicked; restarting");
                }
            }
        }

        restart_count = restart_count.saturating_add(1);
        observer.on_restart_state(restart_count, RESTART_BACKOFF);
        info!(
            restart_count,
            restart_delay_secs = RESTART_BACKOFF.as_secs(),
            "restarting state worker"
        );
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received during state worker restart backoff");
                return Ok(());
            }
            () = tokio::time::sleep(RESTART_BACKOFF) => {}
        }
    }
}

pub async fn run_once(
    config: &StateWorkerRuntimeConfig,
    shutdown_rx: broadcast::Receiver<()>,
    observer: Arc<dyn RuntimeObserver>,
) -> Result<()> {
    let provider = connect_provider(&config.ws_url).await?;

    validate_geth_version(&provider).await?;
    let writer_reader = match StateWriter::new(
        config.mdbx_path.as_str(),
        CircularBufferConfig::new(config.state_depth)?,
    ) {
        Ok(writer_reader) => {
            metrics::set_db_healthy(true);
            writer_reader
        }
        Err(err) => {
            metrics::set_db_healthy(false);
            return Err(err).context("failed to initialize database client");
        }
    };

    publish_existing_durable_head(&writer_reader, observer.as_ref())?;

    let genesis_state = load_genesis_state(&config.genesis_path)?;
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), TRACE_TIMEOUT);
    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
        observer,
    );

    worker
        .run(config.start_block, shutdown_rx)
        .await
        .context("state worker terminated unexpectedly")
}

pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

pub async fn validate_geth_version(provider: &RootProvider) -> Result<()> {
    let client_version = provider
        .get_client_version()
        .await
        .context("failed to get client version via web3_clientVersion")?;

    info!(client_version = %client_version, "connected to execution client");

    if let Some(version) = parse_geth_version(&client_version) {
        if version >= MIN_GETH_VERSION {
            info!(
                geth_version = %version,
                min_version = %MIN_GETH_VERSION,
                "Geth version validated"
            );
            return Ok(());
        }

        anyhow::bail!(
            "Geth version {} is too old; minimum supported version is {}",
            version,
            MIN_GETH_VERSION
        );
    }

    warn!(
        client_version = %client_version,
        "execution client did not identify as Geth; skipping Geth version check"
    );
    Ok(())
}

pub async fn shutdown_signal() -> Result<()> {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("failed to install SIGTERM handler")?;
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .context("failed to install SIGINT handler")?;

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c()
            .await
            .context("failed to listen for ctrl-c")?;
        info!("Received Ctrl+C");
    }

    Ok(())
}

fn load_genesis_state(path: &str) -> Result<GenesisState> {
    info!("Loading genesis from file: {}", path);
    let contents = std::fs::read_to_string(path)
        .inspect_err(|err| warn!(error = ?err, file_path = path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {path}"))?;
    genesis::parse_from_str(&contents)
        .inspect_err(|err| warn!(error = ?err, file_path = path, "Failed to parse genesis file"))
        .with_context(|| format!("failed to parse genesis file: {path}"))
}

fn publish_existing_durable_head<WR>(
    writer_reader: &WR,
    observer: &dyn RuntimeObserver,
) -> Result<()>
where
    WR: Reader + Writer,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    let durable_head = writer_reader
        .latest_block_number()
        .map_err(anyhow::Error::new)
        .context("failed to read current block from the database")?;

    if let Some(block_number) = durable_head {
        metrics::set_traced_head(block_number);
        metrics::set_flush_permitted_head(block_number);
        metrics::set_durable_head(block_number);
        observer.on_traced_head(block_number);
        observer.on_flush_permitted_head(block_number);
        observer.on_durable_head(block_number);
    }

    Ok(())
}
