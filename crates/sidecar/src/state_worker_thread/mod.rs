//! State worker OS thread scaffold.
//!
//! `StateWorkerThread` wraps the state worker in a dedicated OS thread with:
//! - An isolated single-threaded tokio runtime (prevents starvation of sidecar main runtime)
//! - Panic isolation via `std::panic::catch_unwind`
//! - Graceful shutdown via shared `Arc<AtomicBool>` polled at 100ms intervals
//! - Restart backoff: fixed 1-second delay with saturating restart counter
//!
//! The thread name is `sidecar-state-worker`.
//! Stack size is set to 8 MiB (default 2 MiB is insufficient for async RPC deserialization depth).

mod error;

pub use error::StateWorkerError;

/// Signal sent by CoreEngine to `StateWorkerThread` after each committed block.
///
/// Carries only the block number — the state worker uses this to flush all
/// buffered `BlockStateUpdate` entries where `block_number <= signal.block_number`.
/// The signal type is separate from `engine::queue::CommitHead` which has
/// `pub(crate)` fields and carries engine-specific iteration data.
#[derive(Debug, Clone, Copy)]
pub struct CommitHeadSignal {
    pub block_number: u64,
}

use crate::utils::ErrorRecoverability;
use std::{
    panic::AssertUnwindSafe,
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
use tokio::sync::oneshot;
use tracing::{
    info,
    warn,
};

/// Timeout for shutdown flag polling inside the blocking loop.
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Stack size for the state worker OS thread. 8 MiB prevents stack overflows
/// from deeply nested async futures (WebSocket + prestateTracer deserialization).
const STACK_SIZE: usize = 8 * 1024 * 1024;

pub type StateWorkerThreadResult = Result<
    (
        JoinHandle<Result<(), StateWorkerError>>,
        oneshot::Receiver<Result<(), StateWorkerError>>,
    ),
    StateWorkerError,
>;

/// Wraps the state worker in a lifecycle-correct OS thread.
///
/// Phase 1 delivers the scaffold only — `run_blocking_inner` currently returns
/// `Ok(())` immediately. Subsequent phases wire in the actual state worker logic.
pub struct StateWorkerThread {
    shutdown: Arc<AtomicBool>,
}

impl StateWorkerThread {
    pub fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self { shutdown }
    }

    /// Spawn the state worker OS thread.
    ///
    /// Returns `(JoinHandle, oneshot::Receiver)` matching the `EventSequencing` convention
    /// so `run_sidecar_once()` can add the exit notification to its `select!`.
    ///
    /// The OS thread runs an inner restart loop: on any exit (clean, error, or panic)
    /// it waits 1 second and spawns a new inner iteration of `run_blocking_inner`.
    /// The outer loop exits when the shutdown flag is set and the inner iteration returns.
    pub fn spawn(self) -> StateWorkerThreadResult {
        let (tx, rx) = oneshot::channel();
        let shutdown = self.shutdown;

        let handle = std::thread::Builder::new()
            .name("sidecar-state-worker".into())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let result = run_thread_loop(&shutdown);
                let _ = tx.send(result.clone());
                result
            })
            .map_err(|e| StateWorkerError::ThreadSpawn(e.to_string()))?;

        Ok((handle, rx))
    }
}

/// Top-level OS thread body: restart loop with panic catch and 1-second backoff.
fn run_thread_loop(shutdown: &AtomicBool) -> Result<(), StateWorkerError> {
    let mut restart_count: u64 = 0;
    loop {
        // Check shutdown before each iteration
        if shutdown.load(Ordering::Relaxed) {
            info!(target: "state_worker_thread", "Shutdown signal received, exiting thread loop");
            return Ok(());
        }

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_blocking_inner(shutdown)
        }));

        match result {
            Ok(Ok(())) => {
                // Clean exit: if shutdown flag set, stop; otherwise restart
                if shutdown.load(Ordering::Relaxed) {
                    info!(target: "state_worker_thread", "State worker exited cleanly during shutdown");
                    return Ok(());
                }
                warn!(target: "state_worker_thread", restart_count, "State worker exited cleanly; restarting");
            }
            Ok(Err(ref err)) => {
                let recoverable = ErrorRecoverability::from(err).is_recoverable();
                if recoverable {
                    warn!(
                        target: "state_worker_thread",
                        error = %err,
                        restart_count,
                        "State worker failed with recoverable error; restarting"
                    );
                } else {
                    // Unrecoverable: propagate up so run_sidecar_once detects it
                    return Err(err.clone());
                }
            }
            Err(panic_payload) => {
                let message = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                warn!(
                    target: "state_worker_thread",
                    panic = %message,
                    restart_count,
                    "State worker panicked; restarting"
                );
                // Panics are Recoverable — continue the restart loop
            }
        }

        restart_count = restart_count.saturating_add(1);
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Inner blocking run of the state worker.
///
/// Phase 1: empty loop that polls the shutdown flag and returns Ok(()) on shutdown.
/// Phase 2+ will replace this with StateWorker::run() inside a tokio runtime.
fn run_blocking_inner(shutdown: &AtomicBool) -> Result<(), StateWorkerError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| StateWorkerError::RuntimeBuild(e.to_string()))?;

    rt.block_on(async {
        loop {
            if shutdown.load(Ordering::Acquire) {
                info!(target: "state_worker_thread", "Shutdown flag observed in inner loop");
                return Ok(());
            }
            // Poll shutdown flag at RECV_TIMEOUT intervals
            tokio::time::sleep(RECV_TIMEOUT).await;
        }
    })
}
