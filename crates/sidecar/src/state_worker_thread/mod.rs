//! State worker OS thread scaffold.
//!
//! `StateWorkerThread` wraps the state worker in a dedicated OS thread with:
//! - An isolated single-threaded tokio runtime (prevents starvation of sidecar main runtime)
//! - Panic isolation via `std::panic::catch_unwind`
//! - Graceful shutdown via shared `Arc<AtomicBool>` polled between blocks
//! - Restart backoff: fixed 1-second delay with saturating restart counter
//! - Buffer: `VecDeque<BlockStateUpdate>` bounded at `BUFFER_CAPACITY = 128` entries
//! - Flush gating: MDBX writes only after CoreEngine sends a `CommitHeadSignal`
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

use crate::{
    args::EmbeddedStateWorkerConfig,
    utils::ErrorRecoverability,
};
use alloy_provider::Provider;
use metrics::{
    counter,
    gauge,
};
use mdbx::{
    BlockStateUpdate,
    Writer,
};
use std::{
    collections::VecDeque,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
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

/// Maximum number of `BlockStateUpdate` entries held in the in-memory buffer.
///
/// When this limit is reached, tracing pauses until a `CommitHeadSignal` drains space.
/// Backpressure lives here — NOT in the channel — to avoid deadlock between
/// a full buffer (blocking tracing) and a full bounded channel (blocking the engine).
const BUFFER_CAPACITY: usize = 128;

/// Timeout used when blocking on a `CommitHeadSignal` in the backpressure branch.
///
/// Short enough to re-check the shutdown flag regularly without busy-spinning.
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
/// Provides a buffer+flush architecture driven by `CommitHeadSignal` from the core engine:
/// blocks are traced and accumulated in a `VecDeque`, then flushed to MDBX only after the
/// engine signals that up to block `N` has been committed to the canonical chain.
pub struct StateWorkerThread {
    shutdown: Arc<AtomicBool>,
    commit_head_rx: flume::Receiver<CommitHeadSignal>,
    config: EmbeddedStateWorkerConfig,
    committed_head: Arc<AtomicU64>,
}

impl StateWorkerThread {
    /// Construct a new `StateWorkerThread`.
    ///
    /// # Arguments
    ///
    /// * `shutdown` — shared flag; when set the thread exits after the current block completes
    /// * `commit_head_rx` — receives `CommitHeadSignal` from `CoreEngine` after each commit
    /// * `config` — embedded state worker configuration (WS URL, MDBX path, genesis, depth)
    /// * `committed_head` — written with `Release` ordering after each MDBX flush; read by
    ///   `MdbxSource` with `Acquire` ordering in Phase 3
    pub fn new(
        shutdown: Arc<AtomicBool>,
        commit_head_rx: flume::Receiver<CommitHeadSignal>,
        config: EmbeddedStateWorkerConfig,
        committed_head: Arc<AtomicU64>,
    ) -> Self {
        Self {
            shutdown,
            commit_head_rx,
            config,
            committed_head,
        }
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
        let commit_head_rx = self.commit_head_rx;
        let config = self.config;
        let committed_head = self.committed_head;

        let handle = std::thread::Builder::new()
            .name("sidecar-state-worker".into())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let result = run_thread_loop(&shutdown, &commit_head_rx, &config, &committed_head);
                let _ = tx.send(result.clone());
                result
            })
            .map_err(|e| StateWorkerError::ThreadSpawn(e.to_string()))?;

        Ok((handle, rx))
    }
}

/// Top-level OS thread body: restart loop with panic catch and 1-second backoff.
fn run_thread_loop(
    shutdown: &AtomicBool,
    commit_head_rx: &flume::Receiver<CommitHeadSignal>,
    config: &EmbeddedStateWorkerConfig,
    committed_head: &Arc<AtomicU64>,
) -> Result<(), StateWorkerError> {
    let mut restart_count: u64 = 0;
    loop {
        // Check shutdown before each iteration
        if shutdown.load(Ordering::Relaxed) {
            info!(target: "state_worker_thread", "Shutdown signal received, exiting thread loop");
            return Ok(());
        }

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run_blocking_inner(shutdown, commit_head_rx, config, committed_head)
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
        counter!("state_worker_restarts_total").increment(1);
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// Drain all buffered entries where `entry.block_number <= commit_head`.
///
/// Writes each drained entry to MDBX via `writer.commit_block()` and then
/// stores the block number to `committed_head` with `Release` ordering so that
/// Phase 3's `MdbxSource` sees a consistent height after the MDBX write is durable.
///
/// Buffer entries are pushed in block-number order so the front is always the
/// lowest block number — no sorting required.
fn flush_ready_blocks(
    buffer: &mut VecDeque<BlockStateUpdate>,
    commit_head: u64,
    writer: &mdbx::StateWriter,
    committed_head: &Arc<AtomicU64>,
) -> Result<(), StateWorkerError> {
    while let Some(front) = buffer.front() {
        if front.block_number > commit_head {
            break;
        }
        // pop_front is O(1); we just verified front exists so the Option is always Some.
        let Some(update) = buffer.pop_front() else {
            break; // unreachable: avoids indexing_slicing lint
        };
        writer
            .commit_block(&update)
            .map_err(|e| StateWorkerError::MdbxWrite(e.to_string()))?;
        // Release ordering: MdbxSource (Phase 3) loads with Acquire — establishes
        // happens-before so MDBX data is visible before the height update is observed.
        committed_head.store(update.block_number, Ordering::Release);
        info!(
            target: "state_worker_thread",
            block_number = update.block_number,
            "Block flushed to MDBX"
        );
    }
    gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
    Ok(())
}

/// Construct a `StateWorker` and its associated `StateWriter` from the embedded config.
///
/// Must be called inside `rt.block_on()` because the provider connection is async.
///
/// The returned `StateWriter` is separate from the one stored inside the worker so that
/// `flush_ready_blocks` can call `commit_block` without going through the worker struct.
/// This is safe because `StateWriter` is `Clone` (backed by `Arc<StateDb>`).
async fn build_worker(
    config: &EmbeddedStateWorkerConfig,
) -> Result<
    (
        state_worker::worker::StateWorker<mdbx::StateWriter>,
        mdbx::StateWriter,
    ),
    StateWorkerError,
> {
    let ws_url = config
        .ws_url
        .as_deref()
        .ok_or_else(|| StateWorkerError::Config("state_worker.ws_url not configured".into()))?;
    let mdbx_path = config
        .mdbx_path
        .as_deref()
        .ok_or_else(|| StateWorkerError::Config("state_worker.mdbx_path not configured".into()))?;
    let genesis_path = config
        .genesis_path
        .as_deref()
        .ok_or_else(|| {
            StateWorkerError::Config("state_worker.genesis_path not configured".into())
        })?;
    // Connect provider (same pattern as state-worker/src/main.rs::connect_provider)
    let ws = alloy_provider::WsConnect::new(ws_url);
    let provider = alloy_provider::ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|e| StateWorkerError::Config(format!("Failed to connect WS provider: {e}")))?;
    let provider = std::sync::Arc::new(provider.root().clone());

    // Circular buffer depth is always 1 (SIMP-02).
    // CommitHead gating ensures MDBX writes never exceed commit_head.block_number,
    // making the "went too far" scenario architecturally impossible.
    // Depth > 1 provided no safety benefit and added namespace-rotation overhead.
    let circular_config = mdbx::common::CircularBufferConfig::new(1)
        .map_err(|e| StateWorkerError::Config(format!("Invalid CircularBufferConfig: {e}")))?;
    let writer = mdbx::StateWriter::new(mdbx_path, circular_config)
        .map_err(|e| StateWorkerError::Config(format!("Failed to open MDBX writer: {e}")))?;

    // Load genesis (required for block 0 state hydration)
    let genesis_contents = std::fs::read_to_string(genesis_path)
        .map_err(|e| StateWorkerError::Config(format!("Failed to read genesis file: {e}")))?;
    let genesis_state = state_worker::genesis::parse_from_str(&genesis_contents)
        .map_err(|e| StateWorkerError::Config(format!("Failed to parse genesis: {e}")))?;

    // Extract system call fork config before consuming genesis
    let system_calls = state_worker::system_calls::SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    // Create trace provider (30s timeout matches state-worker/main.rs)
    let trace_provider = state_worker::state::create_trace_provider(
        provider.clone(),
        std::time::Duration::from_secs(30),
    );

    // Clone writer so the flush loop can call commit_block independently.
    // StateWriter is Clone (backed by Arc<StateDb>) — clone shares the same DB handle.
    let worker = state_worker::worker::StateWorker::new(
        provider,
        trace_provider,
        writer.clone(),
        Some(genesis_state),
        system_calls,
    );

    Ok((worker, writer))
}

/// Full buffer+flush loop for the embedded state worker.
///
/// Constructs a `StateWorker` from config, then runs the outer sync loop:
/// 1. Drain any pending `CommitHeadSignal`s and flush ready buffer entries
/// 2. Check shutdown flag; on shutdown, attempt best-effort flush and return
/// 3. If buffer is full, pause tracing and wait for a `CommitHeadSignal`
/// 4. Trace the next block via `rt.block_on(worker.process_block(next_block))`
/// 5. Push the returned `BlockStateUpdate` to the buffer; increment `next_block`
///
/// Restarts from the last MDBX-committed block on each invocation (`FLOW-06`).
fn run_blocking_inner(
    shutdown: &AtomicBool,
    commit_head_rx: &flume::Receiver<CommitHeadSignal>,
    config: &EmbeddedStateWorkerConfig,
    committed_head: &Arc<AtomicU64>,
) -> Result<(), StateWorkerError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| StateWorkerError::RuntimeBuild(e.to_string()))?;

    // Build StateWorker and writer inside the runtime (provider connect is async)
    let (mut worker, writer) = rt.block_on(build_worker(config))?;

    // Compute resume point from last committed MDBX block (FLOW-06)
    let mut next_block = worker
        .compute_start_block(None)
        .map_err(|e| StateWorkerError::ComputeStartBlock(e.to_string()))?;

    let mut buffer: VecDeque<BlockStateUpdate> = VecDeque::with_capacity(BUFFER_CAPACITY);

    loop {
        // Drain any pending CommitHead signals (non-blocking: engine may have sent multiple)
        while let Ok(signal) = commit_head_rx.try_recv() {
            flush_ready_blocks(&mut buffer, signal.block_number, &writer, committed_head)?;
        }

        // Check shutdown before tracing next block
        if shutdown.load(Ordering::Acquire) {
            info!(target: "state_worker_thread", "Shutdown: attempting best-effort flush");
            // Best-effort: use the highest CommitHead signal seen via try_iter
            let last_signal = commit_head_rx.try_iter().last();
            if let Some(sig) = last_signal {
                // Ignore flush error on shutdown — restart will resume from MDBX
                let _ = flush_ready_blocks(&mut buffer, sig.block_number, &writer, committed_head);
            }
            return Ok(());
        }

        // Backpressure: pause tracing when buffer is at capacity (FLOW-05, OBSV-03)
        if buffer.len() >= BUFFER_CAPACITY {
            counter!("state_worker_buffer_full_pauses_total").increment(1);
            gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
            match commit_head_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(signal) => {
                    flush_ready_blocks(&mut buffer, signal.block_number, &writer, committed_head)?;
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // No signal yet — re-check shutdown and retry
                    continue;
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Engine dropped the sender (exited) — state worker exits cleanly
                    info!(target: "state_worker_thread", "CommitHead sender disconnected; exiting");
                    return Ok(());
                }
            }
            continue; // Re-check buffer length after flush
        }

        // Trace next block (blocking: one block at a time via isolated runtime)
        match rt.block_on(worker.process_block(next_block)) {
            Ok(update) => {
                buffer.push_back(update);
                next_block += 1;
                gauge!("state_worker_buffer_depth").set(buffer.len() as f64);
            }
            Err(e) => {
                return Err(StateWorkerError::Trace(e.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_head_signal_carries_block_number() {
        let signal = CommitHeadSignal { block_number: 42 };
        assert_eq!(signal.block_number, 42);
    }

    #[test]
    fn commit_head_signal_is_copy() {
        let signal = CommitHeadSignal { block_number: 100 };
        let copied = signal;
        // Both are usable — proves Copy
        assert_eq!(signal.block_number, copied.block_number);
    }

    #[test]
    fn buffer_capacity_is_128() {
        assert_eq!(BUFFER_CAPACITY, 128);
    }

    #[test]
    fn stack_size_is_8mib() {
        assert_eq!(STACK_SIZE, 8 * 1024 * 1024);
    }

    #[test]
    fn recv_timeout_is_100ms() {
        assert_eq!(RECV_TIMEOUT, Duration::from_millis(100));
    }

    #[test]
    fn state_worker_thread_new_stores_fields() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let (tx, rx) = flume::unbounded();
        let config = EmbeddedStateWorkerConfig {
            ws_url: Some("ws://localhost:8546".into()),
            mdbx_path: Some("/tmp/test".into()),
            genesis_path: Some("/tmp/genesis.json".into()),
            state_depth: Some(1),
        };
        let committed_head = Arc::new(AtomicU64::new(0));

        let thread = StateWorkerThread::new(
            Arc::clone(&shutdown),
            rx,
            config,
            Arc::clone(&committed_head),
        );

        // Verify the shutdown flag is shared
        assert!(!thread.shutdown.load(Ordering::Relaxed));
        shutdown.store(true, Ordering::Relaxed);
        assert!(thread.shutdown.load(Ordering::Relaxed));

        // Verify committed_head is shared
        assert_eq!(thread.committed_head.load(Ordering::Relaxed), 0);
        committed_head.store(99, Ordering::Relaxed);
        assert_eq!(thread.committed_head.load(Ordering::Relaxed), 99);

        // Verify channel is connected
        tx.send(CommitHeadSignal { block_number: 7 }).unwrap();
        let received = thread.commit_head_rx.try_recv().unwrap();
        assert_eq!(received.block_number, 7);
    }

    #[test]
    fn run_thread_loop_exits_on_shutdown() {
        let shutdown = AtomicBool::new(true); // pre-set shutdown
        let (_tx, rx) = flume::unbounded();
        let config = EmbeddedStateWorkerConfig {
            ws_url: None,
            mdbx_path: None,
            genesis_path: None,
            state_depth: None,
        };
        let committed_head = Arc::new(AtomicU64::new(0));

        let result = run_thread_loop(&shutdown, &rx, &config, &committed_head);
        assert!(result.is_ok(), "Expected Ok on shutdown, got: {result:?}");
    }
}
