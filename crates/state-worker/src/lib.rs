#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod cli;
mod genesis;
mod geth_version;
#[cfg(test)]
mod integration_tests;
mod metrics;
mod state;
mod system_calls;
mod worker;

use crate::{
    genesis::GenesisState,
    geth_version::{
        GethVersionError,
        MIN_GETH_VERSION,
        parse_geth_version,
    },
    system_calls::SystemCalls,
    worker::{
        StandaloneStateWorkerRuntime,
        StateWorker,
    },
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
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::{
    runtime::Builder,
    sync::{
        broadcast,
        watch,
    },
};
use tracing::{
    info,
    warn,
};

pub use cli::Args;

const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_RESTART_DELAY: Duration = Duration::from_secs(32);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub type CommitHeadSignalSender = watch::Sender<Option<u64>>;
pub type CommitHeadSignalReceiver = watch::Receiver<Option<u64>>;

#[derive(Debug, Clone)]
pub struct StateWorkerConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    pub file_to_genesis: Option<String>,
}

impl From<Args> for StateWorkerConfig {
    fn from(value: Args) -> Self {
        Self {
            ws_url: value.ws_url,
            mdbx_path: value.mdbx_path,
            start_block: value.start_block,
            state_depth: value.state_depth,
            file_to_genesis: Some(value.file_to_genesis),
        }
    }
}

#[must_use]
pub fn commit_head_signal_channel() -> (CommitHeadSignalSender, CommitHeadSignalReceiver) {
    watch::channel(None)
}

pub async fn run_state_worker_once(
    config: &StateWorkerConfig,
    shutdown_rx: broadcast::Receiver<()>,
    commit_head_signal_rx: Option<CommitHeadSignalReceiver>,
    available_observed_head: Arc<AtomicU64>,
) -> Result<()> {
    run_state_worker_once_internal(
        config,
        shutdown_rx,
        commit_head_signal_rx,
        available_observed_head,
    )
    .await
}

pub async fn run_standalone_state_worker_once(
    config: &StateWorkerConfig,
    shutdown_rx: broadcast::Receiver<()>,
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

    let genesis_state = load_genesis_state(config.file_to_genesis.as_deref())?;
    let system_calls = genesis_state
        .as_ref()
        .map(|genesis| SystemCalls::new(genesis.config().cancun_time, genesis.config().prague_time))
        .unwrap_or_default();

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));
    let worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        genesis_state,
        system_calls,
    );
    let mut runtime = StandaloneStateWorkerRuntime::new(worker).with_auto_advance_commit_target();
    let result = runtime.run(config.start_block, shutdown_rx).await;

    match result {
        Ok(()) => {
            info!("State worker shutdown gracefully");
            Ok(())
        }
        Err(err) => Err(err).context("state worker terminated unexpectedly"),
    }
}

async fn run_state_worker_once_internal(
    config: &StateWorkerConfig,
    shutdown_rx: broadcast::Receiver<()>,
    commit_head_signal_rx: Option<CommitHeadSignalReceiver>,
    available_observed_head: Arc<AtomicU64>,
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

    let genesis_state = load_genesis_state(config.file_to_genesis.as_deref())?;
    let system_calls = genesis_state
        .as_ref()
        .map(|genesis| SystemCalls::new(genesis.config().cancun_time, genesis.config().prague_time))
        .unwrap_or_default();

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));
    let worker = StateWorker::new_with_commit_head_signal(
        provider,
        trace_provider,
        writer_reader,
        genesis_state,
        system_calls,
        commit_head_signal_rx,
        available_observed_head,
    );
    let mut worker = worker;
    let result = worker.run(config.start_block, shutdown_rx).await;

    match result {
        Ok(()) => {
            info!("State worker shutdown gracefully");
            Ok(())
        }
        Err(err) => Err(err).context("state worker terminated unexpectedly"),
    }
}

pub fn run_state_worker_until_shutdown(
    config: StateWorkerConfig,
    shutdown: Arc<AtomicBool>,
    commit_head_signal_rx: Option<CommitHeadSignalReceiver>,
    available_observed_head: Arc<AtomicU64>,
) -> Result<()> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to initialize embedded state worker runtime")?;

    let mut restart_count: u64 = 0;
    let mut restart_delay = INITIAL_RESTART_DELAY;

    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let shutdown_flag = Arc::clone(&shutdown);
        let shutdown_bridge = runtime.spawn(async move {
            while !shutdown_flag.load(Ordering::Acquire) {
                tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
            }
            let _ = shutdown_tx.send(());
        });

        let result = runtime.block_on(
            AssertUnwindSafe(run_state_worker_once(
                &config,
                shutdown_rx,
                commit_head_signal_rx.clone(),
                available_observed_head.clone(),
            ))
            .catch_unwind(),
        );

        shutdown_bridge.abort();

        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }

        match result {
            Ok(Ok(())) => {
                warn!("embedded state worker exited unexpectedly; restarting");
            }
            Ok(Err(err)) => {
                warn!(error = %err, "embedded state worker failed; restarting");
            }
            Err(panic_payload) => {
                if let Some(message) = panic_payload.downcast_ref::<&str>() {
                    warn!(panic = %message, "embedded state worker panicked; restarting");
                } else if let Some(message) = panic_payload.downcast_ref::<String>() {
                    warn!(panic = %message, "embedded state worker panicked; restarting");
                } else {
                    warn!("embedded state worker panicked; restarting");
                }
            }
        }

        restart_count = restart_count.saturating_add(1);
        info!(
            restart_count,
            restart_delay_secs = restart_delay.as_secs(),
            "restarting embedded state worker"
        );

        let sleep_started_at = Instant::now();
        while sleep_started_at.elapsed() < restart_delay {
            if shutdown.load(Ordering::Acquire) {
                return Ok(());
            }
            std::thread::sleep(SHUTDOWN_POLL_INTERVAL);
        }

        restart_delay = (restart_delay * 2).min(MAX_RESTART_DELAY);
    }
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

pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

async fn validate_geth_version(provider: &RootProvider) -> Result<()> {
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

    Ok(())
}

fn load_genesis_state(file_to_genesis: Option<&str>) -> Result<Option<GenesisState>> {
    let Some(file_path) = file_to_genesis else {
        info!("No genesis file configured; skipping genesis hydration setup");
        return Ok(None);
    };

    info!("Loading genesis from file: {}", file_path);
    let contents = std::fs::read_to_string(file_path)
        .inspect_err(|err| warn!(error = ?err, file_path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {file_path}"))?;
    let genesis_state = genesis::parse_from_str(&contents)
        .inspect_err(|err| warn!(error = ?err, file_path, "Failed to parse genesis from file"))
        .with_context(|| format!("failed to parse genesis from file: {file_path}"))?;

    Ok(Some(genesis_state))
}
