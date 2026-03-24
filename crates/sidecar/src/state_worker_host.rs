//! Sidecar-owned lifecycle wrapper for the embedded state worker.

use crate::{
    args::{
        Config,
        StateSourceConfig,
    },
    metrics,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use mdbx::{
    Reader,
    StateReader,
    common::CircularBufferConfig,
};
use parking_lot::Mutex;
use state_worker::{
    control::ControlMessage,
    host::{
        EmbeddedStateWorkerConfig,
        run_embedded_worker,
    },
};
use std::{
    any::Any,
    future::Future,
    path::PathBuf,
    pin::Pin,
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
use tokio::{
    runtime::Builder,
    sync::{
        broadcast,
        mpsc,
    },
    time::sleep,
};
use tracing::{
    error,
    info,
    warn,
};

const INITIAL_RESTART_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(30);

type WorkerRunFuture = Pin<Box<dyn Future<Output = Result<(), WorkerRunError>> + Send + 'static>>;

#[derive(Debug, Default)]
struct WorkerControlState {
    latest_permitted_watermark: Option<u64>,
    control_tx: Option<mpsc::UnboundedSender<ControlMessage>>,
}

#[derive(Clone, Debug)]
pub struct StateWorkerControlHandle {
    state: Arc<Mutex<WorkerControlState>>,
}

impl StateWorkerControlHandle {
    pub fn update_commit_head(&self, block_number: u64) {
        let mut state = self.state.lock();
        state.latest_permitted_watermark = Some(block_number);
        if let Some(control_tx) = &state.control_tx
            && let Err(error) = control_tx.send(ControlMessage::CommitHead(block_number))
        {
            warn!(
                block_number,
                error = ?error,
                "failed to deliver commit-head watermark to embedded state worker"
            );
            state.control_tx = None;
        }
    }
}

trait WorkerRunner: Send + Sync + 'static {
    fn run(
        &self,
        config: EmbeddedStateWorkerConfig,
        control_rx: mpsc::UnboundedReceiver<ControlMessage>,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> WorkerRunFuture;
}

impl<F> WorkerRunner for F
where
    F: Fn(
            EmbeddedStateWorkerConfig,
            mpsc::UnboundedReceiver<ControlMessage>,
            broadcast::Receiver<()>,
        ) -> WorkerRunFuture
        + Send
        + Sync
        + 'static,
{
    fn run(
        &self,
        config: EmbeddedStateWorkerConfig,
        control_rx: mpsc::UnboundedReceiver<ControlMessage>,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> WorkerRunFuture {
        self(config, control_rx, shutdown_rx)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerFailureKind {
    StartupError,
    RuntimeError,
    Panic,
}

impl WorkerFailureKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::StartupError => "startup_error",
            Self::RuntimeError => "runtime_error",
            Self::Panic => "panic",
        }
    }
}

#[derive(Debug)]
struct WorkerRunError {
    kind: WorkerFailureKind,
    error: anyhow::Error,
}

impl WorkerRunError {
    fn startup(error: anyhow::Error) -> Self {
        Self {
            kind: WorkerFailureKind::StartupError,
            error,
        }
    }

    fn runtime(error: anyhow::Error) -> Self {
        Self {
            kind: WorkerFailureKind::RuntimeError,
            error,
        }
    }
}

trait SupervisorHooks: Send + Sync + 'static {
    fn on_failure(
        &self,
        _failure_kind: WorkerFailureKind,
        _restart_attempt: u64,
        _consecutive_failures: u64,
        _resume_from_block: Option<u64>,
        _panic_payload: Option<&str>,
    ) {
    }

    fn on_restart_scheduled(
        &self,
        _restart_attempt: u64,
        _consecutive_failures: u64,
        _backoff: Duration,
        _resume_from_block: Option<u64>,
    ) {
    }
}

struct NoopSupervisorHooks;

impl SupervisorHooks for NoopSupervisorHooks {}

pub struct StateWorkerHostHandle {
    shutdown_requested: Arc<AtomicBool>,
    shutdown_tx: broadcast::Sender<()>,
    control: StateWorkerControlHandle,
    join_handle: JoinHandle<Result<()>>,
}

impl StateWorkerHostHandle {
    /// Signal the embedded worker to stop.
    pub fn signal_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Relaxed);
        let _ = self.shutdown_tx.send(());
    }

    /// Join the worker host thread.
    pub fn join(self) -> std::thread::Result<Result<()>> {
        self.join_handle.join()
    }

    #[must_use]
    pub fn control_handle(&self) -> StateWorkerControlHandle {
        self.control.clone()
    }
}

/// Spawn the embedded state worker on a sidecar-owned OS thread.
///
/// # Errors
///
/// Returns an error when the sidecar configuration cannot be converted into an
/// embedded worker config or when the worker thread cannot be spawned.
pub fn start_state_worker_host(config: &Config) -> Result<StateWorkerHostHandle> {
    start_state_worker_host_with_runner(config, default_worker_runner())
}

fn start_state_worker_host_with_runner(
    config: &Config,
    runner: Arc<dyn WorkerRunner>,
) -> Result<StateWorkerHostHandle> {
    let hooks: Arc<dyn SupervisorHooks> = Arc::new(NoopSupervisorHooks);
    start_state_worker_host_with_runner_and_hooks(config, runner, &hooks)
}

fn start_state_worker_host_with_runner_and_hooks(
    config: &Config,
    runner: Arc<dyn WorkerRunner>,
    hooks: &Arc<dyn SupervisorHooks>,
) -> Result<StateWorkerHostHandle> {
    let worker_config = embedded_state_worker_config(config)?;
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let control_state = Arc::new(Mutex::new(WorkerControlState::default()));
    let worker_shutdown_tx = shutdown_tx.clone();
    let worker_shutdown_requested = Arc::clone(&shutdown_requested);
    let worker_control_state = Arc::clone(&control_state);
    let worker_hooks = Arc::clone(hooks);

    let join_handle = std::thread::Builder::new()
        .name("sidecar-state-worker".into())
        .spawn(move || {
            run_worker_thread(
                &worker_config,
                &worker_shutdown_tx,
                &worker_shutdown_requested,
                &worker_control_state,
                &runner,
                &worker_hooks,
            )
        })
        .context("failed to spawn state worker host thread")?;

    Ok(StateWorkerHostHandle {
        shutdown_requested,
        shutdown_tx,
        control: StateWorkerControlHandle {
            state: control_state,
        },
        join_handle,
    })
}

pub fn embedded_state_worker_config(config: &Config) -> Result<EmbeddedStateWorkerConfig> {
    let ws_url = worker_ws_url(config)
        .ok_or_else(|| anyhow!("missing worker websocket source configuration"))?;
    let (mdbx_path, state_depth) = worker_mdbx_config(config)
        .ok_or_else(|| anyhow!("missing worker MDBX source configuration"))?;
    let genesis_path = config
        .state
        .worker_genesis_path
        .clone()
        .ok_or_else(|| anyhow!("missing embedded worker genesis path configuration"))?;

    let state_depth = u8::try_from(state_depth)
        .context("state worker depth exceeds the embedded worker limit")?;

    Ok(EmbeddedStateWorkerConfig::new(
        ws_url,
        mdbx_path,
        None,
        state_depth,
        genesis_path,
    ))
}

#[allow(clippy::too_many_lines)]
fn run_worker_thread(
    worker_config: &EmbeddedStateWorkerConfig,
    shutdown_tx: &broadcast::Sender<()>,
    shutdown_requested: &Arc<AtomicBool>,
    control_state: &Arc<Mutex<WorkerControlState>>,
    runner: &Arc<dyn WorkerRunner>,
    hooks: &Arc<dyn SupervisorHooks>,
) -> Result<()> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build sidecar state worker runtime")?;

    info!(
        mdbx_path = %worker_config.mdbx_path.display(),
        genesis_path = %worker_config.genesis_path.display(),
        "starting embedded state worker host supervisor"
    );

    let mut restart_attempt = 0_u64;
    let mut consecutive_failures = 0_u64;

    loop {
        if shutdown_requested.load(Ordering::Relaxed) {
            info!("state worker supervisor received shutdown before next attempt");
            return Ok(());
        }

        let resume_from_block = latest_resume_block(worker_config);
        info!(
            restart_attempt,
            consecutive_failures,
            resume_from_block = ?resume_from_block,
            "starting embedded state worker attempt"
        );

        let worker_config_for_run = worker_config.clone();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let latest_permitted_watermark = {
            let mut control_state = control_state.lock();
            control_state.control_tx = Some(control_tx.clone());
            control_state.latest_permitted_watermark
        };
        if let Some(latest_permitted_watermark) = latest_permitted_watermark {
            info!(
                latest_permitted_watermark,
                "replaying cached commit-head watermark for embedded state worker restart"
            );
            control_tx
                .send(ControlMessage::CommitHead(latest_permitted_watermark))
                .map_err(|error| {
                    anyhow!("failed to replay commit-head watermark before worker start: {error}")
                })?;
        }
        let shutdown_rx = shutdown_tx.subscribe();
        let runner = Arc::clone(runner);
        let run_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async move {
                runner
                    .run(worker_config_for_run, control_rx, shutdown_rx)
                    .await
            })
        }));
        control_state.lock().control_tx = None;

        match run_result {
            Ok(Ok(())) => {
                metrics::set_state_worker_consecutive_failures(0);
                info!("state worker supervisor exiting after graceful shutdown");
                return Ok(());
            }
            Ok(Err(run_error)) => {
                restart_attempt = restart_attempt.saturating_add(1);
                consecutive_failures = consecutive_failures.saturating_add(1);
                record_worker_failure(
                    restart_attempt,
                    consecutive_failures,
                    run_error.kind,
                    resume_from_block,
                    Some(&run_error.error),
                    None,
                );
                hooks.on_failure(
                    run_error.kind,
                    restart_attempt,
                    consecutive_failures,
                    resume_from_block,
                    None,
                );
            }
            Err(panic_payload) => {
                restart_attempt = restart_attempt.saturating_add(1);
                consecutive_failures = consecutive_failures.saturating_add(1);
                let panic_payload = panic_payload_to_string(panic_payload.as_ref());
                record_worker_failure(
                    restart_attempt,
                    consecutive_failures,
                    WorkerFailureKind::Panic,
                    resume_from_block,
                    None,
                    Some(panic_payload.as_str()),
                );
                hooks.on_failure(
                    WorkerFailureKind::Panic,
                    restart_attempt,
                    consecutive_failures,
                    resume_from_block,
                    Some(panic_payload.as_str()),
                );
            }
        }

        let backoff = restart_backoff(consecutive_failures);
        metrics::record_state_worker_restart(backoff, restart_attempt, resume_from_block);
        hooks.on_restart_scheduled(
            restart_attempt,
            consecutive_failures,
            backoff,
            resume_from_block,
        );
        warn!(
            restart_attempt,
            consecutive_failures,
            backoff_seconds = backoff.as_secs(),
            resume_from_block = ?resume_from_block,
            "state worker supervisor scheduling restart"
        );

        let should_shutdown = runtime.block_on(async {
            let mut shutdown_rx = shutdown_tx.subscribe();
            tokio::select! {
                _ = shutdown_rx.recv() => true,
                () = sleep(backoff) => false,
            }
        });

        if should_shutdown {
            info!("state worker supervisor received shutdown during restart backoff");
            metrics::set_state_worker_consecutive_failures(0);
            return Ok(());
        }
    }
}

fn default_worker_runner() -> Arc<dyn WorkerRunner> {
    Arc::new(
        |config: EmbeddedStateWorkerConfig,
         control_rx: mpsc::UnboundedReceiver<ControlMessage>,
         shutdown_rx: broadcast::Receiver<()>| {
            let future: WorkerRunFuture = Box::pin(async move {
                run_embedded_worker(&config, control_rx, shutdown_rx)
                    .await
                    .map_err(WorkerRunError::runtime)
            });
            future
        },
    )
}

fn latest_resume_block(worker_config: &EmbeddedStateWorkerConfig) -> Option<u64> {
    let config = CircularBufferConfig::new(worker_config.state_depth).ok()?;
    match StateReader::new(&worker_config.mdbx_path, config) {
        Ok(reader) => {
            match reader.latest_block_number() {
                Ok(latest_block_number) => latest_block_number,
                Err(error) => {
                    warn!(
                        error = ?error,
                        mdbx_path = %worker_config.mdbx_path.display(),
                        "failed to read latest MDBX block for worker resume point"
                    );
                    None
                }
            }
        }
        Err(error) => {
            warn!(
                error = ?error,
                mdbx_path = %worker_config.mdbx_path.display(),
                "failed to open MDBX reader for worker resume point"
            );
            None
        }
    }
}

fn restart_backoff(consecutive_failures: u64) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    let seconds = INITIAL_RESTART_BACKOFF
        .as_secs()
        .saturating_mul(1_u64 << exponent);
    Duration::from_secs(seconds).min(MAX_RESTART_BACKOFF)
}

fn record_worker_failure(
    restart_attempt: u64,
    consecutive_failures: u64,
    failure_kind: WorkerFailureKind,
    resume_from_block: Option<u64>,
    error: Option<&anyhow::Error>,
    panic_payload: Option<&str>,
) {
    metrics::record_state_worker_failure(failure_kind.as_str(), consecutive_failures);
    let panic_payload = panic_payload.unwrap_or("none");
    match error {
        Some(error) => {
            error!(
                restart_attempt,
                consecutive_failures,
                failure_kind = failure_kind.as_str(),
                resume_from_block = ?resume_from_block,
                panic_payload,
                error = ?error,
                "state worker supervisor observed a worker failure"
            );
        }
        None => {
            error!(
                restart_attempt,
                consecutive_failures,
                failure_kind = failure_kind.as_str(),
                resume_from_block = ?resume_from_block,
                panic_payload,
                "state worker supervisor observed a worker failure"
            );
        }
    }
}

fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn worker_ws_url(config: &Config) -> Option<String> {
    config
        .state
        .sources
        .iter()
        .find_map(|source| {
            match source {
                StateSourceConfig::EthRpc { ws_url, .. } => Some(ws_url.clone()),
                StateSourceConfig::Mdbx { .. } => None,
            }
        })
        .or_else(|| config.state.legacy.eth_rpc_source_ws_url.clone())
}

fn worker_mdbx_config(config: &Config) -> Option<(PathBuf, usize)> {
    config
        .state
        .sources
        .iter()
        .find_map(|source| {
            match source {
                StateSourceConfig::Mdbx { mdbx_path, depth } => {
                    Some((PathBuf::from(mdbx_path), *depth))
                }
                StateSourceConfig::EthRpc { .. } => None,
            }
        })
        .or_else(|| {
            config
                .state
                .legacy
                .state_worker_mdbx_path
                .clone()
                .zip(config.state.legacy.state_worker_depth)
                .map(|(path, depth)| (PathBuf::from(path), depth))
        })
}

#[cfg(test)]
mod tests {
    use super::{
        SupervisorHooks,
        WorkerFailureKind,
        WorkerRunError,
        embedded_state_worker_config,
        restart_backoff,
        start_state_worker_host_with_runner,
        start_state_worker_host_with_runner_and_hooks,
    };
    use crate::args::Config;
    use anyhow::anyhow;
    use std::{
        collections::VecDeque,
        io::Write,
        sync::{
            Arc,
            Mutex,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        time::Duration,
    };
    use tempfile::NamedTempFile;
    use tokio::sync::{
        broadcast,
        mpsc,
    };
    use tracing_test::traced_test;

    enum TestRunOutcome {
        StartupError(&'static str),
        RuntimeError(&'static str),
        Panic(&'static str),
        WaitForShutdown,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum SupervisorEvent {
        Failure {
            failure_kind: WorkerFailureKind,
            restart_attempt: u64,
            consecutive_failures: u64,
            panic_payload: Option<String>,
        },
        RestartScheduled {
            restart_attempt: u64,
            consecutive_failures: u64,
            backoff: Duration,
        },
    }

    #[derive(Default)]
    struct TestSupervisorHooks {
        events: Mutex<Vec<SupervisorEvent>>,
    }

    impl SupervisorHooks for TestSupervisorHooks {
        fn on_failure(
            &self,
            failure_kind: WorkerFailureKind,
            restart_attempt: u64,
            consecutive_failures: u64,
            _resume_from_block: Option<u64>,
            panic_payload: Option<&str>,
        ) {
            self.events
                .lock()
                .expect("hooks lock")
                .push(SupervisorEvent::Failure {
                    failure_kind,
                    restart_attempt,
                    consecutive_failures,
                    panic_payload: panic_payload.map(str::to_string),
                });
        }

        fn on_restart_scheduled(
            &self,
            restart_attempt: u64,
            consecutive_failures: u64,
            backoff: Duration,
            _resume_from_block: Option<u64>,
        ) {
            self.events
                .lock()
                .expect("hooks lock")
                .push(SupervisorEvent::RestartScheduled {
                    restart_attempt,
                    consecutive_failures,
                    backoff,
                });
        }
    }

    fn write_config(state_json: &str) -> Config {
        let mut temp_file = NamedTempFile::new().expect("temp config file");
        write!(
            temp_file,
            r#"{{
  "chain": {{
    "spec_id": "CANCUN",
    "chain_id": 1
  }},
  "credible": {{
    "assertion_gas_limit": 30000000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {state_json}
}}"#
        )
        .expect("write config");
        temp_file.flush().expect("flush config");
        Config::from_file(temp_file.path()).expect("resolve sidecar config")
    }

    #[test]
    fn test_state_worker_host_uses_state_sources_config() {
        let config = write_config(
            r#"{
    "sources": [
      {"type": "eth-rpc", "ws_url": "ws://rpc.example:8546", "http_url": "http://rpc.example:8545"},
      {"type": "mdbx", "mdbx_path": "/data/state_worker.mdbx", "depth": 7}
    ],
    "worker_genesis_path": "/data/genesis.json",
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let worker_config =
            embedded_state_worker_config(&config).expect("embedded worker config from sources");

        assert_eq!(worker_config.ws_url, "ws://rpc.example:8546");
        assert_eq!(
            worker_config.mdbx_path.as_os_str(),
            "/data/state_worker.mdbx"
        );
        assert_eq!(worker_config.state_depth, 7);
        assert_eq!(worker_config.genesis_path.as_os_str(), "/data/genesis.json");
        assert_eq!(worker_config.start_block, None);
    }

    #[test]
    fn test_state_worker_host_uses_legacy_state_fields() {
        let config = write_config(
            r#"{
    "worker_genesis_path": "/data/legacy-genesis.json",
    "eth_rpc_source_ws_url": "ws://legacy.example:8546",
    "eth_rpc_source_http_url": "http://legacy.example:8545",
    "state_worker_mdbx_path": "/data/legacy-state-worker.mdbx",
    "state_worker_depth": 5,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let worker_config =
            embedded_state_worker_config(&config).expect("embedded worker config from legacy");

        assert_eq!(worker_config.ws_url, "ws://legacy.example:8546");
        assert_eq!(
            worker_config.mdbx_path.as_os_str(),
            "/data/legacy-state-worker.mdbx"
        );
        assert_eq!(worker_config.state_depth, 5);
        assert_eq!(
            worker_config.genesis_path.as_os_str(),
            "/data/legacy-genesis.json"
        );
    }

    #[test]
    fn test_state_worker_host_restart_backoff_caps_at_thirty_seconds() {
        assert_eq!(restart_backoff(1), Duration::from_secs(1));
        assert_eq!(restart_backoff(2), Duration::from_secs(2));
        assert_eq!(restart_backoff(3), Duration::from_secs(4));
        assert_eq!(restart_backoff(6), Duration::from_secs(30));
        assert_eq!(restart_backoff(10), Duration::from_secs(30));
    }

    #[traced_test]
    #[test]
    fn state_worker_host_restart_retries_startup_and_runtime_errors_until_shutdown() {
        let config = write_config(
            r#"{
    "sources": [
      {"type": "eth-rpc", "ws_url": "ws://rpc.example:8546", "http_url": "http://rpc.example:8545"},
      {"type": "mdbx", "mdbx_path": "/tmp/state-worker-host-restart.mdbx", "depth": 7}
    ],
    "worker_genesis_path": "/tmp/genesis.json",
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let attempts = Arc::new(AtomicUsize::new(0));
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            TestRunOutcome::StartupError("startup failed"),
            TestRunOutcome::RuntimeError("runtime failed"),
            TestRunOutcome::WaitForShutdown,
        ])));
        let runner = test_runner(Arc::clone(&attempts), Arc::clone(&outcomes));
        let hooks = Arc::new(TestSupervisorHooks::default());
        let hooks_trait: Arc<dyn SupervisorHooks> = hooks.clone();
        let handle = start_state_worker_host_with_runner_and_hooks(&config, runner, &hooks_trait)
            .expect("start host with runner");

        wait_for_attempts(&attempts, 3);
        handle.signal_shutdown();

        let result = handle.join().expect("worker thread should join");
        assert!(
            result.is_ok(),
            "worker thread should exit cleanly: {result:?}"
        );
        let events = hooks.events.lock().expect("hooks lock").clone();
        assert!(events.contains(&SupervisorEvent::Failure {
            failure_kind: WorkerFailureKind::StartupError,
            restart_attempt: 1,
            consecutive_failures: 1,
            panic_payload: None,
        }));
        assert!(events.contains(&SupervisorEvent::Failure {
            failure_kind: WorkerFailureKind::RuntimeError,
            restart_attempt: 2,
            consecutive_failures: 2,
            panic_payload: None,
        }));
        assert!(events.contains(&SupervisorEvent::RestartScheduled {
            restart_attempt: 1,
            consecutive_failures: 1,
            backoff: Duration::from_secs(1),
        }));
    }

    #[traced_test]
    #[test]
    fn state_worker_host_panic_is_contained_and_restarted_until_shutdown() {
        let config = write_config(
            r#"{
    "sources": [
      {"type": "eth-rpc", "ws_url": "ws://rpc.example:8546", "http_url": "http://rpc.example:8545"},
      {"type": "mdbx", "mdbx_path": "/tmp/state-worker-host-panic.mdbx", "depth": 7}
    ],
    "worker_genesis_path": "/tmp/genesis.json",
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }"#,
        );

        let attempts = Arc::new(AtomicUsize::new(0));
        let outcomes = Arc::new(Mutex::new(VecDeque::from([
            TestRunOutcome::Panic("boom"),
            TestRunOutcome::WaitForShutdown,
        ])));
        let runner = test_runner(Arc::clone(&attempts), Arc::clone(&outcomes));
        let hooks = Arc::new(TestSupervisorHooks::default());
        let hooks_trait: Arc<dyn SupervisorHooks> = hooks.clone();
        let handle = start_state_worker_host_with_runner_and_hooks(&config, runner, &hooks_trait)
            .expect("start host with runner");

        wait_for_attempts(&attempts, 2);
        handle.signal_shutdown();

        let result = handle.join().expect("worker thread should join");
        assert!(
            result.is_ok(),
            "worker thread should exit cleanly: {result:?}"
        );
        let events = hooks.events.lock().expect("hooks lock").clone();
        assert!(events.contains(&SupervisorEvent::Failure {
            failure_kind: WorkerFailureKind::Panic,
            restart_attempt: 1,
            consecutive_failures: 1,
            panic_payload: Some("boom".to_string()),
        }));
    }

    fn test_runner(
        attempts: Arc<AtomicUsize>,
        outcomes: Arc<Mutex<VecDeque<TestRunOutcome>>>,
    ) -> Arc<dyn super::WorkerRunner> {
        Arc::new(
            move |_config,
                  _control_rx: mpsc::UnboundedReceiver<ControlMessage>,
                  mut shutdown_rx: broadcast::Receiver<()>| {
                let attempts = Arc::clone(&attempts);
                let outcomes = Arc::clone(&outcomes);
                let future: super::WorkerRunFuture = Box::pin(async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let outcome = outcomes
                        .lock()
                        .expect("runner outcomes lock")
                        .pop_front()
                        .unwrap_or(TestRunOutcome::WaitForShutdown);
                    match outcome {
                        TestRunOutcome::StartupError(message) => {
                            Err(WorkerRunError {
                                kind: WorkerFailureKind::StartupError,
                                error: anyhow!(message),
                            })
                        }
                        TestRunOutcome::RuntimeError(message) => {
                            Err(WorkerRunError {
                                kind: WorkerFailureKind::RuntimeError,
                                error: anyhow!(message),
                            })
                        }
                        TestRunOutcome::Panic(message) => panic!("{message}"),
                        TestRunOutcome::WaitForShutdown => {
                            let _ = shutdown_rx.recv().await;
                            Ok(())
                        }
                    }
                });
                future
            },
        )
    }

    fn wait_for_attempts(attempts: &AtomicUsize, expected_attempts: usize) {
        for _ in 0..250 {
            if attempts.load(Ordering::Relaxed) >= expected_attempts {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "expected at least {expected_attempts} attempts, got {}",
            attempts.load(Ordering::Relaxed)
        );
    }
}
