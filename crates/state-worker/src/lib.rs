#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod cli;
pub mod coordination;
mod genesis;
mod geth_version;
#[cfg(test)]
mod integration_tests;
mod metrics;
mod state;
mod system_calls;
mod worker;

pub use coordination::FlushControl;
pub use genesis::GenesisState;
pub use worker::StateWorker;

use crate::{
    geth_version::{
        MIN_GETH_VERSION,
        parse_geth_version,
    },
    system_calls::SystemCalls,
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
    anyhow,
    ensure,
};
use futures_util::FutureExt;
use mdbx::{
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    panic::AssertUnwindSafe,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
    time::Duration,
};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{
    info,
    warn,
};
use url::Url;

pub const DEFAULT_TRACE_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_RESTART_DELAY: Duration = Duration::from_secs(300);

/// Configuration for a single state worker run.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Execution client websocket URL (used for `newHeads` + `prestateTracer`).
    pub ws_url: Url,
    /// Filesystem path to the MDBX database directory.
    pub mdbx_path: PathBuf,
    /// Optional manual override for the first block to trace.
    pub start_block: Option<u64>,
    /// Circular buffer depth — how many historical blocks to retain in MDBX.
    pub mdbx_depth: u8,
    /// Maximum traced blocks allowed ahead of the commit head before
    /// back-pressure is applied.
    pub buffer_capacity: usize,
    /// Path to the genesis JSON file used to hydrate block 0.
    pub genesis_file: PathBuf,
    /// Maximum time the worker will wait for a `prestateTracer` RPC response.
    pub trace_timeout: Duration,
}

impl TryFrom<cli::Args> for WorkerConfig {
    type Error = anyhow::Error;

    fn try_from(args: cli::Args) -> Result<Self> {
        Ok(Self {
            ws_url: Url::parse(&args.ws_url).context("invalid STATE_WORKER_WS_URL / --ws-url")?,
            mdbx_path: PathBuf::from(args.mdbx_path),
            start_block: args.start_block,
            mdbx_depth: args.state_depth,
            buffer_capacity: usize::from(args.state_depth),
            genesis_file: PathBuf::from(args.file_to_genesis),
            trace_timeout: DEFAULT_TRACE_TIMEOUT,
        })
    }
}

/// Run the supervised worker loop until shutdown is requested.
///
/// # Errors
///
/// Returns an error only if cancellation or worker construction fails in a way
/// that escapes the supervisor loop.
pub async fn run_supervisor_loop(
    config: WorkerConfig,
    flush_control: Option<Arc<FlushControl>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut restart_count: u64 = 0;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let result = AssertUnwindSafe(async {
            let mut worker = build_state_worker(&config, flush_control.clone()).await?;
            worker.run(config.start_block, shutdown.clone()).await
        })
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(())) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                warn!("state worker exited; restarting");
            }
            Ok(Err(err)) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
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
        let restart_delay = restart_delay_for_attempt(restart_count);
        info!(
            restart_count,
            restart_delay_secs = restart_delay.as_secs(),
            "restarting state worker"
        );

        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = time::sleep(restart_delay) => {}
        }
    }
}

fn restart_delay_for_attempt(restart_count: u64) -> Duration {
    let shift = restart_count.saturating_sub(1).min(63);
    let multiplier = 1_u32.checked_shl(shift as u32).unwrap_or(u32::MAX);
    (INITIAL_RESTART_DELAY * multiplier).min(MAX_RESTART_DELAY)
}

/// Build a worker instance from runtime config and optional flush coordination.
///
/// # Errors
///
/// Returns an error when configuration is invalid, provider connection fails,
/// Geth validation fails, MDBX cannot be opened, or genesis loading fails.
pub async fn build_state_worker(
    config: &WorkerConfig,
    flush_control: Option<Arc<FlushControl>>,
) -> Result<StateWorker<StateWriter>> {
    ensure!(
        config.buffer_capacity > 0,
        "state worker buffer_capacity must be greater than zero"
    );

    let provider = connect_provider(config.ws_url.as_str()).await?;

    validate_geth_version(&provider).await?;
    let writer_reader = match StateWriter::new(
        &config.mdbx_path,
        CircularBufferConfig::new(config.mdbx_depth)?,
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

    let genesis_state = load_genesis_state(&config.genesis_file)?;

    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), config.trace_timeout);

    Ok(StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
        flush_control,
        config.buffer_capacity,
    ))
}

/// Load and parse the genesis file used to hydrate block 0 state.
///
/// # Errors
///
/// Returns an error when the file cannot be read or its JSON cannot be parsed
/// into a valid genesis state.
pub fn load_genesis_state(file_path: &Path) -> Result<GenesisState> {
    info!("Loading genesis from file: {}", file_path.display());
    let contents = std::fs::read_to_string(file_path)
        .inspect_err(|e| warn!(error = ?e, ?file_path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {}", file_path.display()))?;
    genesis::parse_from_str(&contents)
        .inspect_err(|e| warn!(error = ?e, ?file_path, "Failed to parse genesis from file"))
        .with_context(|| format!("failed to parse genesis from file: {}", file_path.display()))
}

/// Connect to the execution client over websocket.
///
/// # Errors
///
/// Returns an error when the websocket provider connection cannot be
/// established.
pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

/// Validate that the connected execution client is a supported Geth version.
///
/// # Errors
///
/// Returns an error when client version discovery fails or when the connected
/// Geth version is older than the supported minimum.
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

        return Err(anyhow!(crate::geth_version::GethVersionError {
            current: version,
            minimum: MIN_GETH_VERSION,
        }));
    }

    warn!(
        client_version = %client_version,
        "non-Geth execution client detected; skipping version validation"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        INITIAL_RESTART_DELAY,
        MAX_RESTART_DELAY,
        WorkerConfig,
        restart_delay_for_attempt,
    };
    use crate::cli::Args;
    use std::time::Duration;

    #[test]
    fn restart_delay_starts_at_one_second() {
        assert_eq!(restart_delay_for_attempt(1), INITIAL_RESTART_DELAY);
    }

    #[test]
    fn restart_delay_grows_exponentially() {
        assert_eq!(restart_delay_for_attempt(2), Duration::from_secs(2));
        assert_eq!(restart_delay_for_attempt(3), Duration::from_secs(4));
        assert_eq!(restart_delay_for_attempt(4), Duration::from_secs(8));
    }

    #[test]
    fn restart_delay_is_capped() {
        assert_eq!(restart_delay_for_attempt(100), MAX_RESTART_DELAY);
    }

    #[test]
    fn worker_config_rejects_invalid_ws_url() {
        let result = WorkerConfig::try_from(Args {
            ws_url: "not a url".to_string(),
            mdbx_path: "/tmp/state".to_string(),
            start_block: None,
            state_depth: 3,
            file_to_genesis: "/tmp/genesis.json".to_string(),
        });

        assert!(result.is_err());
    }
}
