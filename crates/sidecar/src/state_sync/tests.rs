use crate::state_sync::{
    mdbx_runtime,
    supervisor::{
        StateWorkerSupervisor,
        test_supervisor_with_panicking_worker,
        wait_for_restart_count,
    },
};
use anyhow::anyhow;
use std::sync::{
    Arc,
    Barrier,
    OnceLock,
    atomic::{
        AtomicBool,
        Ordering,
    },
};

#[test]
fn test_shared_mdbx_runtime_initializes_once() {
    let tempdir = tempfile::tempdir().unwrap();
    let cell = OnceLock::new();
    let path = tempdir.path().join("state.mdbx");
    let first = mdbx_runtime::init_test_cell(&cell, &path).unwrap();
    let second = mdbx_runtime::init_test_cell(&cell, &path).unwrap();

    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn test_supervisor_restarts_worker_after_panic() {
    let supervisor = test_supervisor_with_panicking_worker();
    wait_for_restart_count(&supervisor, 1);

    assert!(supervisor.status().healthy);
}

#[test]
fn test_supervisor_drop_shuts_down_thread() {
    let shutdown_seen = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Barrier::new(2));
    let supervisor = StateWorkerSupervisor::spawn_test_with_factory(Box::new({
        let shutdown_seen = Arc::clone(&shutdown_seen);
        let started = Arc::clone(&started);
        move || {
            Ok(Box::new(TestShutdownAwareWorker {
                shutdown_seen: Arc::clone(&shutdown_seen),
                started: Arc::clone(&started),
            }))
        }
    }))
    .unwrap();

    started.wait();
    drop(supervisor);

    assert!(shutdown_seen.load(Ordering::Acquire));
}

#[test]
fn test_supervisor_restarts_after_factory_error() {
    let failed_once = Arc::new(AtomicBool::new(false));
    let supervisor = StateWorkerSupervisor::spawn_test_with_factory(Box::new({
        let failed_once = Arc::clone(&failed_once);
        move || {
            if failed_once.swap(true, Ordering::AcqRel) {
                Ok(Box::new(TestHealthyWorker))
            } else {
                Err(anyhow!("expected factory failure"))
            }
        }
    }))
    .unwrap();

    wait_for_restart_count(&supervisor, 1);

    assert!(supervisor.status().healthy);
}

struct TestShutdownAwareWorker {
    shutdown_seen: Arc<AtomicBool>,
    started: Arc<Barrier>,
}

impl crate::state_sync::supervisor::SupervisedWorker for TestShutdownAwareWorker {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> anyhow::Result<()> {
        self.started.wait();
        while !shutdown.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        self.shutdown_seen.store(true, Ordering::Release);
        Ok(())
    }
}

#[test]
fn test_shutdown_cancels_startup_future() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let shutdown_signal = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(25));
        shutdown_signal.store(true, Ordering::Release);
    });

    let result = runtime
        .block_on(crate::state_sync::supervisor::run_until_shutdown(
            Arc::clone(&shutdown),
            async {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                Ok::<_, anyhow::Error>(())
            },
        ))
        .unwrap();
    handle.join().unwrap();

    assert!(result.is_none());
}

struct TestHealthyWorker;

impl crate::state_sync::supervisor::SupervisedWorker for TestHealthyWorker {
    fn run(self: Box<Self>, shutdown: Arc<AtomicBool>) -> anyhow::Result<()> {
        while !shutdown.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }
}
