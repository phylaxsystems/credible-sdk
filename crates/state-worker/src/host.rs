//! Reusable state-worker bootstrap helpers for embedders and the compatibility binary.

use crate::{
    control::ControlMessage,
    genesis::{
        self,
        GenesisState,
    },
    geth_version::{
        GethVersionError,
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
use mdbx::{
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    broadcast,
    mpsc,
};
use tracing::{
    info,
    warn,
};

/// Shared configuration for running the state worker in-process.
#[derive(Debug, Clone)]
pub struct EmbeddedStateWorkerConfig {
    pub ws_url: String,
    pub mdbx_path: PathBuf,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    pub genesis_path: PathBuf,
    pub trace_timeout: Duration,
}

impl EmbeddedStateWorkerConfig {
    #[must_use]
    pub fn new(
        ws_url: impl Into<String>,
        mdbx_path: impl Into<PathBuf>,
        start_block: Option<u64>,
        state_depth: u8,
        genesis_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            mdbx_path: mdbx_path.into(),
            start_block,
            state_depth,
            genesis_path: genesis_path.into(),
            trace_timeout: Duration::from_secs(30),
        }
    }
}

/// Run the worker using an externally-owned shutdown channel.
///
/// # Errors
///
/// Returns an error if provider setup, genesis loading, MDBX initialization,
/// or the worker loop fails.
pub async fn run_embedded_worker(
    config: &EmbeddedStateWorkerConfig,
    control_rx: mpsc::UnboundedReceiver<ControlMessage>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let provider = connect_provider(&config.ws_url).await?;
    validate_geth_version(&provider).await?;

    let writer_reader = StateWriter::new(
        &config.mdbx_path,
        CircularBufferConfig::new(config.state_depth)?,
    )
    .inspect(|_| {
        metrics::set_db_healthy(true);
    })
    .inspect_err(|_| {
        metrics::set_db_healthy(false);
    })
    .context("failed to initialize database client")?;

    let genesis_state = load_genesis_state(&config.genesis_path)?;
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), config.trace_timeout);
    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
        control_rx,
    );

    worker
        .run(config.start_block, shutdown_rx)
        .await
        .context("state worker terminated unexpectedly")
}

/// Run the worker on an isolated single-threaded Tokio runtime and stop on process signals.
///
/// # Errors
///
/// Returns an error if runtime creation, signal handling, or the worker run
/// fails.
pub fn run_embedded_worker_until_shutdown(config: EmbeddedStateWorkerConfig) -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build state worker runtime")?
        .block_on(async move {
            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            let (control_tx, control_rx) = mpsc::unbounded_channel();
            let shutdown_tx_clone = shutdown_tx.clone();
            let _ = control_tx.send(ControlMessage::CommitHead(u64::MAX));

            tokio::spawn(async move {
                if let Err(err) = shutdown_signal().await {
                    warn!(error = %err, "error setting up shutdown signal handler");
                    return;
                }

                info!("shutdown signal received, initiating graceful shutdown");
                let _ = shutdown_tx_clone.send(());
            });

            run_embedded_worker(&config, control_rx, shutdown_rx).await
        })
}

fn load_genesis_state(path: &PathBuf) -> Result<GenesisState> {
    info!(path = %path.display(), "loading genesis from file");

    let contents = std::fs::read_to_string(path)
        .inspect_err(
            |err| warn!(error = ?err, path = %path.display(), "failed to read genesis file"),
        )
        .with_context(|| format!("failed to read genesis file: {}", path.display()))?;

    genesis::parse_from_str(&contents)
        .inspect_err(
            |err| warn!(error = ?err, path = %path.display(), "failed to parse genesis file"),
        )
        .with_context(|| format!("failed to parse genesis file: {}", path.display()))
}

/// Wait for SIGTERM or SIGINT (Ctrl+C).
///
/// # Errors
///
/// Returns an error if the process signal handlers cannot be installed.
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
                info!("received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c()
            .await
            .context("failed to listen for ctrl-c")?;
        info!("received Ctrl+C");
    }

    Ok(())
}

/// Establish a WebSocket connection to the execution node.
///
/// # Errors
///
/// Returns an error if the websocket connection cannot be established.
pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;

    Ok(Arc::new(provider.root().clone()))
}

/// Validate that the connected execution client meets trace-version requirements.
///
/// # Errors
///
/// Returns an error if the client version cannot be queried or if Geth is
/// older than the minimum supported version.
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

        return Err(GethVersionError {
            current: version,
            minimum: MIN_GETH_VERSION,
        }
        .into());
    }

    warn!(
        client_version = %client_version,
        "connected client is not Geth or version could not be parsed; \
         skipping prestateTracer version validation. Ensure your client \
         correctly implements EIP-6780 SELFDESTRUCT semantics in traces."
    );
    Ok(())
}
