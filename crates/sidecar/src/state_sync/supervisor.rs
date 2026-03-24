use crate::state_sync::mdbx_runtime;
use anyhow::{
    Context,
    Result,
};
use state_worker::{
    EmbeddedStateWorkerHandle,
    EmbeddedStateWorkerRuntime,
    StateWorker,
    WorkerStatusSnapshot,
    connect_provider,
    state,
    system_calls::SystemCalls,
    validate_geth_version,
};
use std::{
    future::Future,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    thread::{
        Builder,
        JoinHandle,
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::{
    runtime::Builder as RuntimeBuilder,
    sync::broadcast,
};
use tracing::warn;

const SUPERVISOR_THREAD_NAME: &str = "sidecar-state-worker";
const TEST_SUPERVISOR_THREAD_NAME: &str = "sidecar-test-state-worker";
const RESTART_BACKOFF: Duration = Duration::from_secs(1);
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const TRACE_TIMEOUT: Duration = Duration::from_secs(30);
const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(3);
const NONE_BLOCK_SENTINEL: u64 = u64::MAX;

pub trait CommitTargetSink: std::fmt::Debug + Send + Sync {
    fn publish(&self, block_number: u64);
}

pub type CommitTargetHandle = Arc<dyn CommitTargetSink>;

pub trait WorkerStatusReader: std::fmt::Debug + Send + Sync {
    fn snapshot(&self) -> WorkerStatusSnapshot;
}

pub type WorkerStatusHandle = Arc<dyn WorkerStatusReader>;

trait CommitTargetPublisher {
    fn publish(&self, block_number: u64);
}

impl CommitTargetPublisher for state_worker::CommitTargetHandle {
    fn publish(&self, block_number: u64) {
        state_worker::CommitTargetHandle::publish(self, block_number);
    }
}

#[derive(Debug)]
struct StableCommitTarget {
    latest_target: AtomicU64,
}

impl Default for StableCommitTarget {
    fn default() -> Self {
        Self {
            latest_target: AtomicU64::new(NONE_BLOCK_SENTINEL),
        }
    }
}

impl StableCommitTarget {
    fn publish(&self, block_number: u64) {
        let _ = self
            .latest_target
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current == NONE_BLOCK_SENTINEL || block_number > current {
                    Some(block_number)
                } else {
                    None
                }
            });
    }

    fn current_target(&self) -> Option<u64> {
        match self.latest_target.load(Ordering::Acquire) {
            NONE_BLOCK_SENTINEL => None,
            block_number => Some(block_number),
        }
    }
}

fn replay_latest_commit_target(
    stable_target: &StableCommitTarget,
    publisher: &impl CommitTargetPublisher,
) {
    if let Some(block_number) = stable_target.current_target() {
        publisher.publish(block_number);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisorStatusSnapshot {
    pub healthy: bool,
    pub restarting: bool,
}

#[derive(Debug, Default)]
struct SupervisorStatus {
    healthy: AtomicBool,
    restarting: AtomicBool,
}

impl SupervisorStatus {
    fn snapshot(&self) -> SupervisorStatusSnapshot {
        SupervisorStatusSnapshot {
            healthy: self.healthy.load(Ordering::Acquire),
            restarting: self.restarting.load(Ordering::Acquire),
        }
    }

    fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    fn set_restarting(&self, restarting: bool) {
        self.restarting.store(restarting, Ordering::Release);
    }
}

pub(crate) trait SupervisedWorker: Send {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> Result<()>;
}

type WorkerFactory = Box<dyn FnMut() -> Result<Box<dyn SupervisedWorker>> + Send + 'static>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedWorkerConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
}

pub struct StateWorkerSupervisor {
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
    stable_commit_target: Arc<StableCommitTarget>,
    status: Arc<SupervisorStatus>,
    restart_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Clone, Debug)]
struct SupervisorWorkerHandleFacade {
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
    stable_commit_target: Arc<StableCommitTarget>,
    supervisor_status: Arc<SupervisorStatus>,
}

impl SupervisorWorkerHandleFacade {
    fn current_handle(&self) -> Option<EmbeddedStateWorkerHandle> {
        self.control.lock().ok().and_then(|guard| guard.clone())
    }
}

impl CommitTargetSink for SupervisorWorkerHandleFacade {
    fn publish(&self, block_number: u64) {
        self.stable_commit_target.publish(block_number);
        if let Some(handle) = self.current_handle() {
            handle.control.publish(block_number);
        }
    }
}

impl WorkerStatusReader for SupervisorWorkerHandleFacade {
    fn snapshot(&self) -> WorkerStatusSnapshot {
        let supervisor_status = self.supervisor_status.snapshot();

        if let Some(handle) = self.current_handle() {
            let mut snapshot = handle.status.snapshot();
            snapshot.healthy &= supervisor_status.healthy;
            snapshot.restarting |= supervisor_status.restarting;
            return snapshot;
        }

        WorkerStatusSnapshot {
            latest_head_seen: None,
            highest_staged_block: None,
            mdbx_synced_through: None,
            healthy: false,
            restarting: supervisor_status.restarting,
        }
    }
}

impl StateWorkerSupervisor {
    pub fn spawn(config: EmbeddedWorkerConfig) -> Result<Self> {
        let control = Arc::new(Mutex::new(None));
        let factory_control = Arc::clone(&control);
        let stable_commit_target = Arc::new(StableCommitTarget::default());
        let worker_commit_target = Arc::clone(&stable_commit_target);
        Self::spawn_with_factory(
            Box::new(move || {
                Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(EmbeddedWorkerTask {
                    config: config.clone(),
                    control: Arc::clone(&factory_control),
                    stable_commit_target: Arc::clone(&worker_commit_target),
                }))
            }),
            Some(control),
            Some(stable_commit_target),
            SUPERVISOR_THREAD_NAME,
            RESTART_BACKOFF,
        )
    }

    fn spawn_with_factory(
        factory: WorkerFactory,
        control: Option<Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>>,
        stable_commit_target: Option<Arc<StableCommitTarget>>,
        thread_name: &'static str,
        restart_backoff: Duration,
    ) -> Result<Self> {
        let effective_control = control.unwrap_or_else(|| Arc::new(Mutex::new(None)));
        let stable_commit_target =
            stable_commit_target.unwrap_or_else(|| Arc::new(StableCommitTarget::default()));
        let status = Arc::new(SupervisorStatus::default());
        let restart_count = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_control = Arc::clone(&effective_control);

        let thread = Builder::new()
            .name(thread_name.to_string())
            .spawn({
                let control = Arc::clone(&effective_control);
                let status = Arc::clone(&status);
                let restart_count = Arc::clone(&restart_count);
                let shutdown = Arc::clone(&shutdown);
                let mut factory = factory;
                move || {
                    run_supervisor_loop(
                        &mut factory,
                        control,
                        status,
                        restart_count,
                        shutdown,
                        restart_backoff,
                    );
                }
            })
            .context("failed to spawn embedded state worker supervisor")?;

        Ok(Self {
            control: thread_control,
            stable_commit_target,
            status,
            restart_count,
            shutdown,
            thread: Some(thread),
        })
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        clear_control(&self.control);
    }

    pub fn join(&mut self) {
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            warn!("embedded state worker supervisor thread panicked");
        }
    }

    #[must_use]
    pub fn control(&self) -> Option<EmbeddedStateWorkerHandle> {
        self.control.lock().ok().and_then(|guard| guard.clone())
    }

    #[must_use]
    pub fn commit_target_sink(&self) -> CommitTargetHandle {
        Arc::new(SupervisorWorkerHandleFacade {
            control: Arc::clone(&self.control),
            stable_commit_target: Arc::clone(&self.stable_commit_target),
            supervisor_status: Arc::clone(&self.status),
        })
    }

    #[must_use]
    pub fn worker_status_handle(&self) -> WorkerStatusHandle {
        Arc::new(SupervisorWorkerHandleFacade {
            control: Arc::clone(&self.control),
            stable_commit_target: Arc::clone(&self.stable_commit_target),
            supervisor_status: Arc::clone(&self.status),
        })
    }

    #[must_use]
    pub fn status(&self) -> SupervisorStatusSnapshot {
        self.status.snapshot()
    }

    #[must_use]
    pub fn restart_count(&self) -> u64 {
        self.restart_count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn spawn_test_with_factory(factory: WorkerFactory) -> Result<Self> {
        Self::spawn_with_factory(
            factory,
            None,
            None,
            TEST_SUPERVISOR_THREAD_NAME,
            Duration::from_millis(10),
        )
    }
}

impl Drop for StateWorkerSupervisor {
    fn drop(&mut self) {
        self.shutdown();
        self.join();
    }
}

struct EmbeddedWorkerTask {
    config: EmbeddedWorkerConfig,
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
    stable_commit_target: Arc<StableCommitTarget>,
}

impl SupervisedWorker for EmbeddedWorkerTask {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> Result<()> {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create embedded worker tokio runtime")?;

        runtime.block_on(async move {
            let startup = run_until_shutdown(Arc::clone(&shutdown), async {
                let provider = connect_provider(&self.config.ws_url)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to connect to embedded worker websocket {}",
                            self.config.ws_url
                        )
                    })?;
                validate_geth_version(&provider).await?;

                let mdbx_runtime = mdbx_runtime::init(&self.config.mdbx_path)?;
                let writer_reader = mdbx_runtime.writer();
                let trace_provider = state::create_trace_provider(provider.clone(), TRACE_TIMEOUT);
                let worker = StateWorker::new(
                    provider,
                    trace_provider,
                    writer_reader,
                    None,
                    SystemCalls::default(),
                );

                Ok::<_, anyhow::Error>(EmbeddedStateWorkerRuntime::new(worker))
            })
            .await?;
            let Some(mut runtime) = startup else {
                clear_control(&self.control);
                return Ok(());
            };

            let handle = runtime.handle();
            replay_latest_commit_target(&self.stable_commit_target, &handle.control);
            if let Ok(mut guard) = self.control.lock() {
                *guard = Some(handle);
            }

            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            let monitor = tokio::spawn({
                let shutdown = Arc::clone(&shutdown);
                let shutdown_tx = shutdown_tx.clone();
                async move {
                    while !shutdown.load(Ordering::Acquire) {
                        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
                    }
                    let _ = shutdown_tx.send(());
                }
            });

            let result = runtime.run(self.config.start_block, shutdown_rx).await;
            monitor.abort();
            result
        })
    }
}

pub(crate) async fn run_until_shutdown<F, T>(
    shutdown: Arc<AtomicBool>,
    future: F,
) -> Result<Option<T>>
where
    F: Future<Output = Result<T>>,
{
    tokio::select! {
        result = future => result.map(Some),
        _ = wait_for_shutdown(shutdown) => Ok(None),
    }
}

async fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

fn run_supervisor_loop<F>(
    worker_factory: &mut F,
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
    status: Arc<SupervisorStatus>,
    restart_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    restart_backoff: Duration,
) where
    F: FnMut() -> Result<Box<dyn SupervisedWorker>>,
{
    while !shutdown.load(Ordering::Acquire) {
        let worker = match worker_factory() {
            Ok(worker) => worker,
            Err(error) => {
                clear_control(&control);
                status.set_healthy(false);
                status.set_restarting(true);
                restart_count.fetch_add(1, Ordering::AcqRel);
                warn!(error = %error, "failed to construct embedded worker");
                sleep_with_shutdown(&shutdown, restart_backoff);
                continue;
            }
        };

        status.set_healthy(true);
        status.set_restarting(false);

        let result = catch_unwind(AssertUnwindSafe(|| worker.run(Arc::clone(&shutdown))));
        clear_control(&control);
        if shutdown.load(Ordering::Acquire) {
            break;
        }

        match result {
            Ok(Ok(())) => warn!("embedded worker exited; supervisor restarting"),
            Ok(Err(error)) => {
                warn!(error = %error, "embedded worker failed; supervisor restarting")
            }
            Err(payload) => {
                if let Some(message) = payload.downcast_ref::<&str>() {
                    warn!(panic = %message, "embedded worker panicked; supervisor restarting");
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    warn!(panic = %message, "embedded worker panicked; supervisor restarting");
                } else {
                    warn!("embedded worker panicked; supervisor restarting");
                }
            }
        }

        status.set_healthy(false);
        status.set_restarting(true);
        restart_count.fetch_add(1, Ordering::AcqRel);
        sleep_with_shutdown(&shutdown, restart_backoff);
    }

    status.set_restarting(false);
    clear_control(&control);
}

fn clear_control(control: &Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>) {
    if let Ok(mut guard) = control.lock() {
        *guard = None;
    }
}

fn sleep_with_shutdown(shutdown: &Arc<AtomicBool>, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(SHUTDOWN_POLL_INTERVAL);
    }
}

struct PanickingTestWorker;

impl SupervisedWorker for PanickingTestWorker {
    fn run(self: Box<Self>, _shutdown: Arc<AtomicBool>) -> Result<()> {
        panic!("expected supervisor test panic");
    }
}

struct HealthyTestWorker;

impl SupervisedWorker for HealthyTestWorker {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> Result<()> {
        while !shutdown.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_supervisor_with_panicking_worker() -> StateWorkerSupervisor {
    let panicked = Arc::new(AtomicBool::new(false));
    StateWorkerSupervisor::spawn_test_with_factory(Box::new({
        let panicked = Arc::clone(&panicked);
        move || {
            if panicked.swap(true, Ordering::AcqRel) {
                Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(HealthyTestWorker))
            } else {
                Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(PanickingTestWorker))
            }
        }
    }))
    .expect("failed to spawn test supervisor")
}

#[cfg(test)]
pub(crate) fn wait_for_restart_count(supervisor: &StateWorkerSupervisor, expected: u64) {
    let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
    while Instant::now() < deadline {
        if supervisor.restart_count() >= expected && supervisor.status().healthy {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    panic!(
        "timed out waiting for restart count {expected}; saw {}",
        supervisor.restart_count()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct RecordingPublisher {
        values: Mutex<Vec<u64>>,
    }

    impl RecordingPublisher {
        fn values(&self) -> Vec<u64> {
            self.values.lock().unwrap().clone()
        }
    }

    impl CommitTargetPublisher for RecordingPublisher {
        fn publish(&self, block_number: u64) {
            self.values.lock().unwrap().push(block_number);
        }
    }

    #[test]
    fn test_stable_commit_target_replays_latest_target_to_restarted_worker() {
        let stable_target = StableCommitTarget::default();
        stable_target.publish(41);
        stable_target.publish(40);

        let publisher = RecordingPublisher::default();
        replay_latest_commit_target(&stable_target, &publisher);

        assert_eq!(publisher.values(), vec![41]);
    }
}
