use crate::{
    error::{
        Result,
        StateWorkerError,
        boxed_error,
    },
    genesis,
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
use mdbx::{
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    sync::{
        Arc,
        atomic::AtomicU64,
    },
    time::Duration,
};
use tokio::{
    sync::{
        broadcast,
        watch,
    },
    task,
};
use tracing::info;

#[derive(Clone, Debug)]
pub struct WorkerRuntimeConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    pub genesis_file_path: String,
    pub max_buffered_blocks: usize,
}

/// Run one state-worker instance until shutdown or failure.
///
/// # Errors
///
/// Returns an error if provider setup, genesis loading, MDBX initialization,
/// tracing, or MDBX persistence fails.
pub async fn run_worker_once(
    config: &WorkerRuntimeConfig,
    flush_rx: Option<watch::Receiver<u64>>,
    committed_head: Arc<AtomicU64>,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let provider = connect_provider(&config.ws_url).await?;
    validate_geth_version(&provider).await?;

    let writer_reader = StateWriter::new(
        config.mdbx_path.as_str(),
        CircularBufferConfig::new(config.state_depth).map_err(|source| {
            StateWorkerError::DatabaseInit {
                path: config.mdbx_path.clone(),
                source,
            }
        })?,
    )
    .map_err(|source| {
        StateWorkerError::DatabaseInit {
            path: config.mdbx_path.clone(),
            source,
        }
    })?;
    metrics::set_db_healthy(true);

    let contents = tokio::fs::read_to_string(&config.genesis_file_path)
        .await
        .map_err(|source| {
            StateWorkerError::ReadGenesisFile {
                path: config.genesis_file_path.clone(),
                source,
            }
        })?;
    let genesis_state = genesis::parse_from_str(&contents).map_err(|source| {
        StateWorkerError::ParseGenesisFile {
            path: config.genesis_file_path.clone(),
            source: boxed_error(source),
        }
    })?;

    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));

    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
    );

    if let Some(flush_rx) = flush_rx {
        worker = worker.with_commit_head_control(
            flush_rx,
            config.max_buffered_blocks,
            Arc::clone(&committed_head),
        );
    }

    worker.run(config.start_block, shutdown_rx).await
}

pub async fn spawn_shutdown_signal_task(shutdown_tx: broadcast::Sender<()>) {
    let signal = shutdown_signal().await;
    if signal.is_ok() {
        let _ = shutdown_tx.send(());
    }
}

async fn shutdown_signal() -> Result<()> {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).map_err(|source| {
                StateWorkerError::ClientVersion {
                    source: boxed_error(source),
                }
            })?;
        let mut sigint =
            signal::unix::signal(signal::unix::SignalKind::interrupt()).map_err(|source| {
                StateWorkerError::ClientVersion {
                    source: boxed_error(source),
                }
            })?;

        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.map_err(|source| {
            StateWorkerError::ClientVersion {
                source: boxed_error(source),
            }
        })?;
    }

    Ok(())
}

/// Connect to the execution client websocket and return a root provider.
///
/// # Errors
///
/// Returns an error if the websocket connection cannot be established.
pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|source| {
            StateWorkerError::ConnectProvider {
                ws_url: ws_url.to_string(),
                source: boxed_error(source),
            }
        })?;
    Ok(Arc::new(provider.root().clone()))
}

async fn validate_geth_version(provider: &RootProvider) -> Result<()> {
    let client_version = provider.get_client_version().await.map_err(|source| {
        StateWorkerError::ClientVersion {
            source: boxed_error(source),
        }
    })?;

    info!(client_version = %client_version, "connected to execution client");

    if let Some(version) = parse_geth_version(&client_version)
        && version < MIN_GETH_VERSION
    {
        return Err(StateWorkerError::GethVersion(
            crate::geth_version::GethVersionError {
                current: version,
                minimum: MIN_GETH_VERSION,
            },
        ));
    }

    Ok(())
}

#[must_use]
pub fn spawn_signal_listener(shutdown_tx: broadcast::Sender<()>) -> task::JoinHandle<()> {
    tokio::spawn(spawn_shutdown_signal_task(shutdown_tx))
}
