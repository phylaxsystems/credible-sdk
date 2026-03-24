use crate::state_sync::mdbx_runtime;
use anyhow::{
    Context,
    Result,
};
use state_worker::{
    EmbeddedStateWorkerHandle,
    EmbeddedStateWorkerRuntime,
    StateWorker,
    connect_provider,
    state,
    system_calls::SystemCalls,
    validate_geth_version,
};
use std::{
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

trait SupervisedWorker: Send {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedWorkerConfig {
    pub ws_url: String,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
}

pub struct StateWorkerSupervisor {
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
    status: Arc<SupervisorStatus>,
    restart_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl StateWorkerSupervisor {
    pub fn spawn(config: EmbeddedWorkerConfig) -> Result<Self> {
        let control = Arc::new(Mutex::new(None));
        let status = Arc::new(SupervisorStatus::default());
        let restart_count = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread = Builder::new()
            .name(SUPERVISOR_THREAD_NAME.to_string())
            .spawn({
                let control = Arc::clone(&control);
                let status = Arc::clone(&status);
                let restart_count = Arc::clone(&restart_count);
                let shutdown = Arc::clone(&shutdown);
                move || {
                    run_supervisor_loop(
                        move || {
                            Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(
                                EmbeddedWorkerTask {
                                    config: config.clone(),
                                    control: Arc::clone(&control),
                                },
                            ))
                        },
                        status,
                        restart_count,
                        shutdown,
                        RESTART_BACKOFF,
                    );
                }
            })
            .context("failed to spawn embedded state worker supervisor")?;

        Ok(Self {
            control,
            status,
            restart_count,
            shutdown,
            thread: Some(thread),
        })
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
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
    pub fn status(&self) -> SupervisorStatusSnapshot {
        self.status.snapshot()
    }

    #[must_use]
    pub fn restart_count(&self) -> u64 {
        self.restart_count.load(Ordering::Acquire)
    }
}

struct EmbeddedWorkerTask {
    config: EmbeddedWorkerConfig,
    control: Arc<Mutex<Option<EmbeddedStateWorkerHandle>>>,
}

impl SupervisedWorker for EmbeddedWorkerTask {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> Result<()> {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create embedded worker tokio runtime")?;

        runtime.block_on(async move {
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
            let mut runtime = EmbeddedStateWorkerRuntime::new(worker);
            let handle = runtime.handle();
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

fn run_supervisor_loop<F>(
    mut worker_factory: F,
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

pub struct TestSupervisor {
    status: Arc<SupervisorStatus>,
    restart_count: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl TestSupervisor {
    #[must_use]
    pub fn spawn_with_panicking_worker() -> Self {
        let status = Arc::new(SupervisorStatus::default());
        let restart_count = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let panicked = Arc::new(AtomicBool::new(false));

        let thread = Builder::new()
            .name(TEST_SUPERVISOR_THREAD_NAME.to_string())
            .spawn({
                let status = Arc::clone(&status);
                let restart_count = Arc::clone(&restart_count);
                let shutdown = Arc::clone(&shutdown);
                let panicked = Arc::clone(&panicked);
                move || {
                    run_supervisor_loop(
                        move || {
                            if panicked.swap(true, Ordering::AcqRel) {
                                Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(
                                    HealthyTestWorker,
                                ))
                            } else {
                                Ok::<Box<dyn SupervisedWorker>, anyhow::Error>(Box::new(
                                    PanickingTestWorker,
                                ))
                            }
                        },
                        status,
                        restart_count,
                        shutdown,
                        Duration::from_millis(10),
                    );
                }
            })
            .expect("failed to spawn test supervisor thread");

        Self {
            status,
            restart_count,
            shutdown,
            thread: Some(thread),
        }
    }

    #[must_use]
    pub fn status(&self) -> SupervisorStatusSnapshot {
        self.status.snapshot()
    }

    pub fn wait_for_restart_count(&self, expected: u64) {
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while Instant::now() < deadline {
            if self.restart_count.load(Ordering::Acquire) >= expected && self.status().healthy {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        panic!(
            "timed out waiting for restart count {expected}; saw {}",
            self.restart_count.load(Ordering::Acquire)
        );
    }
}

impl Drop for TestSupervisor {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
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
