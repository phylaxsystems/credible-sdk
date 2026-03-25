use crate::{
    genesis::{
        GenesisState,
        GenesisStateError,
    },
    geth_version::{
        GethVersionError,
        MIN_GETH_VERSION,
        parse_geth_version,
    },
    metrics,
    state,
    system_calls::SystemCalls,
    worker::{
        BufferedCommitConfig,
        StateWorker,
        StateWorkerError,
        UNINITIALIZED_COMMITTED_HEIGHT,
    },
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
    WsConnect,
};
use flume::Receiver;
use mdbx::{
    Reader,
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::info;

pub const DEFAULT_TRACE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct StateWorkerConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub file_to_genesis: String,
    pub buffered_blocks_capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateWorkerCommand {
    FlushUpTo(u64),
}

#[derive(Debug, Clone)]
pub enum StateWorkerMode {
    Immediate,
    Buffered {
        command_rx: Receiver<StateWorkerCommand>,
        committed_height: Arc<AtomicU64>,
    },
}

#[derive(Debug, Error)]
pub enum StateWorkerServiceError {
    #[error("failed to connect to websocket provider: {0}")]
    ConnectProvider(String),
    #[error("failed to get client version via web3_clientVersion: {0}")]
    GetClientVersion(String),
    #[error(transparent)]
    GethVersion(#[from] GethVersionError),
    #[error("failed to initialize MDBX at {path}: {source}")]
    InitializeDatabase {
        path: String,
        #[source]
        source: mdbx::common::error::StateError,
    },
    #[error("invalid circular buffer configuration: {0}")]
    InvalidDatabaseConfig(#[source] mdbx::common::error::StateError),
    #[error("failed to read genesis file {path}: {source}")]
    ReadGenesisFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse genesis file {path}: {source}")]
    ParseGenesisFile {
        path: String,
        #[source]
        source: GenesisStateError,
    },
    #[error("buffered_blocks_capacity must be greater than 0")]
    InvalidBufferedBlocksCapacity,
    #[error(transparent)]
    Worker(#[from] StateWorkerError),
}

/// Run one state-worker lifecycle.
///
/// # Errors
///
/// Returns an error if provider setup, genesis loading, MDBX initialization,
/// or worker execution fails.
pub async fn run_state_worker_once(
    config: &StateWorkerConfig,
    mode: &StateWorkerMode,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), StateWorkerServiceError> {
    if config.buffered_blocks_capacity == 0 {
        return Err(StateWorkerServiceError::InvalidBufferedBlocksCapacity);
    }

    let provider = connect_provider(&config.ws_url).await?;
    validate_geth_version(&provider).await?;

    let writer_reader = StateWriter::new(
        config.mdbx_path.as_str(),
        CircularBufferConfig::new(1).map_err(StateWorkerServiceError::InvalidDatabaseConfig)?,
    )
    .map_err(|source| {
        StateWorkerServiceError::InitializeDatabase {
            path: config.mdbx_path.clone(),
            source,
        }
    })?;
    metrics::set_db_healthy(true);

    let genesis_state = load_genesis_state(Path::new(&config.file_to_genesis))?;
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );
    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), DEFAULT_TRACE_TIMEOUT);

    let mut worker = match mode {
        StateWorkerMode::Immediate => {
            StateWorker::new(
                provider,
                trace_provider,
                writer_reader,
                Some(genesis_state),
                system_calls,
            )
        }
        StateWorkerMode::Buffered {
            command_rx,
            committed_height,
        } => {
            let latest_committed = writer_reader.latest_block_number().map_err(|err| {
                metrics::set_db_healthy(false);
                StateWorkerError::ReadCommittedHeight { source: err }
            })?;
            committed_height.store(
                latest_committed.unwrap_or(UNINITIALIZED_COMMITTED_HEIGHT),
                Ordering::Release,
            );
            StateWorker::new_buffered(
                provider,
                trace_provider,
                writer_reader,
                Some(genesis_state),
                system_calls,
                BufferedCommitConfig {
                    command_rx: command_rx.clone(),
                    committed_height: committed_height.clone(),
                    buffered_blocks_capacity: config.buffered_blocks_capacity,
                },
            )
        }
    };

    worker.run(config.start_block, shutdown_rx).await?;
    Ok(())
}

/// Load and parse the genesis state file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or the genesis contents are invalid.
pub fn load_genesis_state(path: &Path) -> Result<GenesisState, StateWorkerServiceError> {
    let path_string = path.display().to_string();
    let contents = fs::read_to_string(path).map_err(|source| {
        StateWorkerServiceError::ReadGenesisFile {
            path: path_string.clone(),
            source,
        }
    })?;

    crate::genesis::parse_from_str(&contents).map_err(|source| {
        StateWorkerServiceError::ParseGenesisFile {
            path: path_string,
            source,
        }
    })
}

/// Connect to the execution client websocket provider.
///
/// # Errors
///
/// Returns an error if the websocket connection fails.
pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>, StateWorkerServiceError> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|err| StateWorkerServiceError::ConnectProvider(err.to_string()))?;
    Ok(Arc::new(provider.root().clone()))
}

/// Validate the connected client version when it is Geth.
///
/// # Errors
///
/// Returns an error if client version lookup fails or the Geth version is too old.
pub async fn validate_geth_version(provider: &RootProvider) -> Result<(), StateWorkerServiceError> {
    let client_version = provider
        .get_client_version()
        .await
        .map_err(|err| StateWorkerServiceError::GetClientVersion(err.to_string()))?;

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

        return Err(StateWorkerServiceError::GethVersion(GethVersionError {
            current: version,
            minimum: MIN_GETH_VERSION,
        }));
    }

    Ok(())
}
