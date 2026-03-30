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
    any::{
        Any,
        TypeId,
    },
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        Mutex,
        OnceLock,
        PoisonError,
    },
    time::Duration,
};
use tokio::sync::{
    broadcast,
    watch,
};
use tracing::{
    info,
    warn,
};

const RESTART_BACKOFF: Duration = Duration::from_secs(1);
const TRACE_TIMEOUT: Duration = Duration::from_secs(30);
static COMMIT_PERMIT_GATE: OnceLock<Mutex<Option<Arc<CommitPermitGate>>>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWorkerRuntimeConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    pub genesis_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct CommittedWindow {
    oldest_block: u64,
    durable_head: u64,
}

impl CommittedWindow {
    #[must_use]
    pub fn from_range(oldest_block: u64, durable_head: u64) -> Option<Self> {
        (oldest_block <= durable_head).then_some(Self {
            oldest_block,
            durable_head,
        })
    }

    pub fn from_block(block_number: u64) -> Self {
        Self {
            oldest_block: block_number,
            durable_head: block_number,
        }
    }

    #[must_use]
    pub fn oldest_block(self) -> u64 {
        self.oldest_block
    }

    #[must_use]
    pub fn durable_head(self) -> u64 {
        self.durable_head
    }

    #[must_use]
    pub fn range(self) -> (u64, u64) {
        (self.oldest_block, self.durable_head)
    }

    pub fn advance(self, durable_head: u64, buffer_size: u8) -> Self {
        if durable_head <= self.durable_head {
            return self;
        }

        let retained_depth = u64::from(buffer_size.saturating_sub(1));
        let oldest_block = self
            .oldest_block
            .max(durable_head.saturating_sub(retained_depth));

        Self {
            oldest_block,
            durable_head,
        }
    }
}

/// Read the current durable MDBX availability window from a reader.
///
/// # Errors
///
/// Returns an error when the reader cannot load the current block range.
pub fn committed_window_from_reader<DB>(reader: &DB) -> Result<Option<CommittedWindow>>
where
    DB: Reader,
    <DB as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    let range = reader
        .get_available_block_range()
        .map_err(anyhow::Error::new)
        .context("failed to read committed window from the database")?;

    Ok(range.and_then(|(oldest_block, durable_head)| {
        CommittedWindow::from_range(oldest_block, durable_head)
    }))
}

pub trait RuntimeObserver: Any + Send + Sync + 'static {
    fn on_restart_state(&self, _restart_count: u64, _restart_backoff: Duration) {}

    fn on_traced_head(&self, _block_number: u64) {}

    fn on_flush_permitted_head(&self, _block_number: u64) {}

    fn on_durable_head(&self, _block_number: u64) {}
}

#[derive(Debug, Default)]
pub struct NoopRuntimeObserver;

impl RuntimeObserver for NoopRuntimeObserver {}

#[derive(Debug)]
pub struct CommitPermitGate {
    permitted_head_tx: watch::Sender<Option<u64>>,
}

impl Default for CommitPermitGate {
    fn default() -> Self {
        let (permitted_head_tx, _) = watch::channel(None);
        Self { permitted_head_tx }
    }
}

impl CommitPermitGate {
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Option<u64>> {
        self.permitted_head_tx.subscribe()
    }

    #[must_use]
    pub fn current_permitted_head(&self) -> Option<u64> {
        *self.permitted_head_tx.borrow()
    }

    pub fn grant_flush_permission(&self, block_number: u64) {
        if self
            .current_permitted_head()
            .is_some_and(|permitted_head| permitted_head >= block_number)
        {
            return;
        }

        let _ = self.permitted_head_tx.send(Some(block_number));
    }
}

/// Return the shared flush-permission gate used by integrated observers.
#[must_use]
pub fn ensure_commit_permit_gate() -> Arc<CommitPermitGate> {
    let mut slot = commit_permit_gate_slot()
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    if let Some(gate) = slot.as_ref() {
        return Arc::clone(gate);
    }

    let gate = Arc::new(CommitPermitGate::default());
    *slot = Some(Arc::clone(&gate));
    gate
}

pub fn grant_flush_permission(block_number: u64) {
    if let Some(gate) = current_commit_permit_gate() {
        gate.grant_flush_permission(block_number);
    }
}

pub(crate) fn commit_permit_gate_for_observer(
    observer: &dyn RuntimeObserver,
) -> Option<Arc<CommitPermitGate>> {
    (observer.type_id() != TypeId::of::<NoopRuntimeObserver>()).then(ensure_commit_permit_gate)
}

fn current_commit_permit_gate() -> Option<Arc<CommitPermitGate>> {
    commit_permit_gate_slot()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .map(Arc::clone)
}

fn commit_permit_gate_slot() -> &'static Mutex<Option<Arc<CommitPermitGate>>> {
    COMMIT_PERMIT_GATE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub fn reset_commit_permit_gate() {
    *commit_permit_gate_slot()
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
}

/// Run the restartable state-worker supervisor loop until shutdown.
///
/// # Errors
///
/// Returns an error if the worker cannot be restarted cleanly after a shutdown
/// signal or if startup repeatedly fails without reaching the worker loop.
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

/// Start the state-worker runtime once and drive it until it exits.
///
/// # Errors
///
/// Returns an error if provider setup, database initialization, genesis loading,
/// or the worker loop itself fails.
pub async fn run_once(
    config: &StateWorkerRuntimeConfig,
    shutdown_rx: broadcast::Receiver<()>,
    observer: Arc<dyn RuntimeObserver>,
) -> Result<()> {
    let _ = commit_permit_gate_for_observer(observer.as_ref());
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

    publish_existing_durable_head(&writer_reader)?;

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
    worker.set_pending_update_capacity(usize::from(config.state_depth).max(1));

    worker
        .run(config.start_block, shutdown_rx)
        .await
        .context("state worker terminated unexpectedly")
}

/// Connect to the execution node websocket provider used by the worker runtime.
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

/// Validate the connected execution client version when it reports Geth.
///
/// # Errors
///
/// Returns an error if the client version RPC fails or if the reported Geth
/// version is older than the minimum supported version.
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
            "Geth version {version} is too old; minimum supported version is {MIN_GETH_VERSION}"
        );
    }

    warn!(
        client_version = %client_version,
        "execution client did not identify as Geth; skipping Geth version check"
    );
    Ok(())
}

/// Wait for the process shutdown signal.
///
/// # Errors
///
/// Returns an error if the OS signal handlers cannot be installed.
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

fn publish_existing_durable_head<WR>(writer_reader: &WR) -> Result<()>
where
    WR: Reader + Writer,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    let durable_head = writer_reader
        .latest_block_number()
        .map_err(anyhow::Error::new)
        .context("failed to read current block from the database")?;

    if let Some(block_number) = durable_head {
        grant_flush_permission(block_number);
        metrics::set_durable_head(block_number);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use mdbx::{
        AccountState,
        StateWriter,
        Writer,
        common::CircularBufferConfig,
    };
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };
    use uuid::Uuid;

    #[derive(Debug)]
    struct TestObserver;

    impl RuntimeObserver for TestObserver {}

    #[derive(Debug, Default)]
    struct CountingObserver {
        durable_head_notifications: AtomicUsize,
    }

    impl CountingObserver {
        fn durable_head_notifications(&self) -> usize {
            self.durable_head_notifications.load(Ordering::Acquire)
        }
    }

    impl RuntimeObserver for CountingObserver {
        fn on_durable_head(&self, _block_number: u64) {
            self.durable_head_notifications
                .fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn noop_observer_does_not_install_commit_gate() {
        reset_commit_permit_gate();

        assert!(commit_permit_gate_for_observer(&NoopRuntimeObserver).is_none());
        assert!(current_commit_permit_gate().is_none());
    }

    #[test]
    fn flush_permissions_advance_monotonically() {
        reset_commit_permit_gate();

        let gate =
            commit_permit_gate_for_observer(&TestObserver).expect("test observer should install");
        let permission_rx = gate.subscribe();
        assert_eq!(*permission_rx.borrow(), None);

        grant_flush_permission(7);
        assert_eq!(*permission_rx.borrow(), Some(7));

        grant_flush_permission(4);
        assert_eq!(*permission_rx.borrow(), Some(7));

        grant_flush_permission(9);
        assert_eq!(*permission_rx.borrow(), Some(9));
    }

    #[test]
    fn committed_window_preserves_seeded_oldest_until_buffer_wraps() {
        let window = CommittedWindow::from_range(100, 102).expect("valid committed window");

        assert_eq!(window.advance(103, 5).range(), (100, 103));
        assert_eq!(window.advance(106, 5).range(), (102, 106));
    }

    #[test]
    fn committed_window_ignores_non_advancing_heads() {
        let window = CommittedWindow::from_range(40, 45).expect("valid committed window");

        assert_eq!(window.advance(45, 8), window);
        assert_eq!(window.advance(44, 8), window);
    }

    #[test]
    fn publish_existing_durable_head_does_not_notify_observer() {
        reset_commit_permit_gate();
        let temp_dir =
            std::env::temp_dir().join(format!("state-worker-runtime-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("create temp runtime dir");
        let writer_reader = StateWriter::new(
            &temp_dir,
            CircularBufferConfig::new(4).expect("buffer config"),
        )
        .expect("state writer");
        writer_reader
            .bootstrap_from_snapshot(Vec::<AccountState>::new(), 42, B256::ZERO, B256::ZERO)
            .expect("bootstrap snapshot");
        let observer = CountingObserver::default();

        publish_existing_durable_head(&writer_reader).expect("bootstrap should succeed");

        assert_eq!(observer.durable_head_notifications(), 0);
        std::fs::remove_dir_all(&temp_dir).expect("remove temp runtime dir");
    }
}
