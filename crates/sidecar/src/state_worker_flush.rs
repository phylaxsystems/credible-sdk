//! Flush loop that drains buffered [`BlockStateUpdate`]s into MDBX, gated by
//! commit-head signals from [`CoreEngine`](crate::engine::CoreEngine).
//!
//! The loop receives updates on a `flume::Receiver<BlockStateUpdate>` produced
//! by the state worker thread, buffers them in memory, and flushes them to
//! [`StateWriter`] only up to the block number signaled on the commit-head
//! channel. After each flush batch it updates a shared
//! [`Arc<AtomicU64>`](std::sync::atomic::AtomicU64) so that
//! `MdbxSource::with_committed_height` can read the latest committed height
//! without polling.

use mdbx::{
    BlockStateUpdate,
    Writer,
};
use std::{
    collections::BTreeMap,
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
use tracing::{
    debug,
    error,
    info,
    warn,
};

/// Configuration for the flush loop.
#[derive(Debug, Clone)]
pub struct FlushLoopConfig {
    /// How long to wait for a message before re-checking the shutdown flag.
    pub recv_timeout: Duration,
}

impl Default for FlushLoopConfig {
    fn default() -> Self {
        Self {
            recv_timeout: Duration::from_millis(100),
        }
    }
}

/// Flush-loop error.
#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    /// The MDBX writer returned an error during `commit_block`.
    #[error("writer commit failed for block {block_number}: {reason}")]
    WriterCommit {
        block_number: u64,
        reason: String,
    },
}

/// In-memory buffer that orders [`BlockStateUpdate`]s by block number and
/// drains them up to a given ceiling.
#[derive(Debug, Default)]
struct UpdateBuffer {
    /// Updates keyed by block number. Using a `BTreeMap` ensures iteration
    /// yields blocks in ascending order.
    inner: BTreeMap<u64, BlockStateUpdate>,
}

impl UpdateBuffer {
    /// Insert an update into the buffer, replacing any existing entry for the
    /// same block number (last-write-wins).
    fn insert(&mut self, update: BlockStateUpdate) {
        self.inner.insert(update.block_number, update);
    }

    /// Remove and return all updates with `block_number <= ceiling`, sorted
    /// in ascending block order.
    fn drain_up_to(&mut self, ceiling: u64) -> Vec<BlockStateUpdate> {
        // `split_off` returns everything >= key; we keep that and take the rest.
        let above = self.inner.split_off(&(ceiling + 1));
        let to_flush: Vec<BlockStateUpdate> = self.inner.values().cloned().collect();
        self.inner = above;
        to_flush
    }

    /// Number of buffered updates.
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Spawn the flush loop on a dedicated OS thread named `sidecar-flush-loop`.
///
/// # Arguments
///
/// * `config`           – flush loop tuning parameters.
/// * `update_rx`        – receiver for [`BlockStateUpdate`]s from the state worker.
/// * `commit_head_rx`   – receiver for committed block numbers from `CoreEngine`.
/// * `writer`           – MDBX writer used to persist updates.
/// * `committed_height` – shared atomic updated after each successful flush
///   batch so readers can observe the latest committed block.
/// * `shutdown`         – shared flag; when set the loop exits.
///
/// # Errors
///
/// Returns `std::io::Error` if the OS thread cannot be spawned.
pub fn spawn_flush_loop<W>(
    config: FlushLoopConfig,
    update_rx: flume::Receiver<BlockStateUpdate>,
    commit_head_rx: flume::Receiver<u64>,
    writer: W,
    committed_height: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>>
where
    W: Writer + Send + 'static,
    W::Error: std::fmt::Display,
{
    std::thread::Builder::new()
        .name("sidecar-flush-loop".into())
        .spawn(move || {
            run_flush_loop(
                &config,
                &update_rx,
                &commit_head_rx,
                &writer,
                &committed_height,
                &shutdown,
            );
        })
}

/// Inner loop executed on the dedicated flush thread.
///
/// The loop alternates between two non-blocking phases:
///
/// 1. **Receive updates** – drain all pending `BlockStateUpdate`s from the
///    update channel into the in-memory buffer.
/// 2. **Receive commit-head** – read the latest committed block number.  If a
///    new ceiling is available, flush all buffered updates up to that block.
///
/// The loop uses `recv_timeout` so it can periodically check the shutdown flag.
fn run_flush_loop<W>(
    config: &FlushLoopConfig,
    update_rx: &flume::Receiver<BlockStateUpdate>,
    commit_head_rx: &flume::Receiver<u64>,
    writer: &W,
    committed_height: &Arc<AtomicU64>,
    shutdown: &Arc<AtomicBool>,
) where
    W: Writer,
    W::Error: std::fmt::Display,
{
    let mut buffer = UpdateBuffer::default();
    let mut current_ceiling: Option<u64> = None;

    info!("flush loop started");

    loop {
        if shutdown.load(Ordering::Acquire) {
            info!("flush loop shutdown flag set; exiting");
            return;
        }

        // Phase 1: drain all available updates (non-blocking after first timeout).
        match update_rx.recv_timeout(config.recv_timeout) {
            Ok(update) => {
                debug!(block = update.block_number, "buffered block state update");
                buffer.insert(update);
                // Drain any additional updates that arrived while we were busy.
                while let Ok(update) = update_rx.try_recv() {
                    debug!(block = update.block_number, "buffered block state update (batch)");
                    buffer.insert(update);
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                // No updates available — continue to check commit-head.
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                info!("update channel disconnected; draining remaining commit signals");
                // Channel closed — drain any remaining commit-head signals and exit.
                while let Ok(head) = commit_head_rx.try_recv() {
                    current_ceiling = Some(
                        current_ceiling.map_or(head, |c: u64| c.max(head)),
                    );
                }
                if let Some(ceiling) = current_ceiling {
                    flush_up_to(&mut buffer, ceiling, writer, committed_height);
                }
                info!("flush loop exiting after update channel disconnect");
                return;
            }
        }

        // Phase 2: consume all pending commit-head signals, keeping the max.
        while let Ok(head) = commit_head_rx.try_recv() {
            current_ceiling = Some(
                current_ceiling.map_or(head, |c: u64| c.max(head)),
            );
        }

        // Phase 3: flush buffered updates up to the current ceiling.
        if let Some(ceiling) = current_ceiling {
            flush_up_to(&mut buffer, ceiling, writer, committed_height);
        }
    }
}

/// Flush all buffered updates with `block_number <= ceiling` to the writer,
/// updating `committed_height` after each successful commit.
fn flush_up_to<W>(
    buffer: &mut UpdateBuffer,
    ceiling: u64,
    writer: &W,
    committed_height: &Arc<AtomicU64>,
) where
    W: Writer,
    W::Error: std::fmt::Display,
{
    let to_flush = buffer.drain_up_to(ceiling);
    if to_flush.is_empty() {
        return;
    }

    debug!(
        count = to_flush.len(),
        ceiling,
        remaining = buffer.len(),
        "flushing buffered updates"
    );

    for update in &to_flush {
        match writer.commit_block(update) {
            Ok(stats) => {
                debug!(
                    block = update.block_number,
                    accounts = stats.accounts_written,
                    "flushed block to MDBX"
                );
                committed_height.store(update.block_number, Ordering::Release);
            }
            Err(err) => {
                error!(
                    block = update.block_number,
                    error = %err,
                    "failed to flush block to MDBX; re-buffering remaining updates"
                );
                // Re-buffer this update and all subsequent ones so they can be
                // retried on the next iteration.
                buffer.insert(update.clone());
                for remaining in to_flush
                    .iter()
                    .filter(|u| u.block_number > update.block_number)
                {
                    buffer.insert(remaining.clone());
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use mdbx::CommitStats;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // Mock writer
    // -----------------------------------------------------------------------

    #[derive(Debug, thiserror::Error)]
    #[error("mock writer error")]
    struct MockWriterError;

    /// A test writer that records committed block numbers.
    struct MockWriter {
        committed: Mutex<Vec<u64>>,
        /// If set, `commit_block` will fail for this block number.
        fail_on: Option<u64>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                committed: Mutex::new(Vec::new()),
                fail_on: None,
            }
        }

        fn with_fail_on(block: u64) -> Self {
            Self {
                committed: Mutex::new(Vec::new()),
                fail_on: Some(block),
            }
        }

        fn committed_blocks(&self) -> Vec<u64> {
            self.committed.lock().unwrap().clone()
        }
    }

    impl Writer for MockWriter {
        type Error = MockWriterError;

        fn commit_block(&self, update: &BlockStateUpdate) -> Result<CommitStats, Self::Error> {
            if self.fail_on == Some(update.block_number) {
                return Err(MockWriterError);
            }
            self.committed.lock().unwrap().push(update.block_number);
            Ok(CommitStats::default())
        }

        fn bootstrap_from_snapshot(
            &self,
            _accounts: Vec<mdbx::AccountState>,
            _block_number: u64,
            _block_hash: B256,
            _state_root: B256,
        ) -> Result<CommitStats, Self::Error> {
            Ok(CommitStats::default())
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO)
    }

    // -----------------------------------------------------------------------
    // UpdateBuffer unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn buffer_insert_and_drain_ascending_order() {
        let mut buf = UpdateBuffer::default();
        buf.insert(make_update(3));
        buf.insert(make_update(1));
        buf.insert(make_update(2));

        let flushed = buf.drain_up_to(2);
        let block_nums: Vec<u64> = flushed.iter().map(|u| u.block_number).collect();
        assert_eq!(block_nums, vec![1, 2]);
        assert_eq!(buf.len(), 1); // block 3 remains
    }

    #[test]
    fn buffer_drain_up_to_returns_empty_when_no_match() {
        let mut buf = UpdateBuffer::default();
        buf.insert(make_update(5));
        buf.insert(make_update(10));

        let flushed = buf.drain_up_to(4);
        assert!(flushed.is_empty());
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn buffer_drain_up_to_drains_all_when_ceiling_exceeds_max() {
        let mut buf = UpdateBuffer::default();
        buf.insert(make_update(1));
        buf.insert(make_update(2));
        buf.insert(make_update(3));

        let flushed = buf.drain_up_to(100);
        assert_eq!(flushed.len(), 3);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn buffer_insert_replaces_existing_block() {
        let mut buf = UpdateBuffer::default();
        buf.insert(make_update(5));
        buf.insert(make_update(5)); // duplicate

        assert_eq!(buf.len(), 1);
        let flushed = buf.drain_up_to(5);
        assert_eq!(flushed.len(), 1);
    }

    // -----------------------------------------------------------------------
    // flush_up_to unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn flush_up_to_commits_blocks_in_order() {
        let writer = MockWriter::new();
        let height = Arc::new(AtomicU64::new(0));
        let mut buffer = UpdateBuffer::default();
        buffer.insert(make_update(1));
        buffer.insert(make_update(2));
        buffer.insert(make_update(3));

        flush_up_to(&mut buffer, 2, &writer, &height);

        assert_eq!(writer.committed_blocks(), vec![1, 2]);
        assert_eq!(height.load(Ordering::Acquire), 2);
        assert_eq!(buffer.len(), 1); // block 3 still buffered
    }

    #[test]
    fn flush_up_to_noop_when_buffer_empty() {
        let writer = MockWriter::new();
        let height = Arc::new(AtomicU64::new(0));
        let mut buffer = UpdateBuffer::default();

        flush_up_to(&mut buffer, 10, &writer, &height);

        assert!(writer.committed_blocks().is_empty());
        assert_eq!(height.load(Ordering::Acquire), 0);
    }

    #[test]
    fn flush_up_to_rebuffers_on_error() {
        let writer = MockWriter::with_fail_on(2);
        let height = Arc::new(AtomicU64::new(0));
        let mut buffer = UpdateBuffer::default();
        buffer.insert(make_update(1));
        buffer.insert(make_update(2));
        buffer.insert(make_update(3));

        flush_up_to(&mut buffer, 3, &writer, &height);

        // Block 1 committed, block 2 failed, blocks 2+3 re-buffered.
        assert_eq!(writer.committed_blocks(), vec![1]);
        assert_eq!(height.load(Ordering::Acquire), 1);
        assert_eq!(buffer.len(), 2); // blocks 2, 3 re-buffered
    }

    // -----------------------------------------------------------------------
    // Integration: run_flush_loop with channels
    // -----------------------------------------------------------------------

    #[test]
    fn flush_loop_drains_on_commit_signal() {
        let (update_tx, update_rx) = flume::bounded(16);
        let (commit_tx, commit_rx) = flume::bounded(16);
        let writer = Arc::new(MockWriter::new());
        let height = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Send updates first.
        update_tx.send(make_update(1)).unwrap();
        update_tx.send(make_update(2)).unwrap();
        update_tx.send(make_update(3)).unwrap();

        // Signal that block 2 is committed.
        commit_tx.send(2).unwrap();

        // Drop senders to trigger disconnect exit path.
        drop(update_tx);
        drop(commit_tx);

        let w = Arc::clone(&writer);
        let h = Arc::clone(&height);
        let s = Arc::clone(&shutdown);

        // Use a wrapper that delegates to the Arc<MockWriter>.
        struct ArcWriter(Arc<MockWriter>);
        impl Writer for ArcWriter {
            type Error = MockWriterError;
            fn commit_block(
                &self,
                update: &BlockStateUpdate,
            ) -> Result<CommitStats, Self::Error> {
                self.0.commit_block(update)
            }
            fn bootstrap_from_snapshot(
                &self,
                accounts: Vec<mdbx::AccountState>,
                block_number: u64,
                block_hash: B256,
                state_root: B256,
            ) -> Result<CommitStats, Self::Error> {
                self.0
                    .bootstrap_from_snapshot(accounts, block_number, block_hash, state_root)
            }
        }

        let config = FlushLoopConfig {
            recv_timeout: Duration::from_millis(10),
        };

        let handle = std::thread::spawn(move || {
            run_flush_loop(&config, &update_rx, &commit_rx, &ArcWriter(w), &h, &s);
        });

        handle.join().expect("flush loop panicked");

        // Blocks 1 and 2 should have been flushed (ceiling=2).
        // Block 3 is above the ceiling but with the disconnect path it gets
        // flushed too if commit_head was updated before disconnect. In our case
        // the commit head is 2, so only 1 and 2.
        let committed = writer.committed_blocks();
        assert!(committed.contains(&1));
        assert!(committed.contains(&2));
        assert_eq!(height.load(Ordering::Acquire), 2);
    }

    #[test]
    fn flush_loop_exits_on_shutdown() {
        let (_update_tx, update_rx) = flume::bounded::<BlockStateUpdate>(16);
        let (_commit_tx, commit_rx) = flume::bounded::<u64>(16);
        let height = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(true)); // already set

        struct NoopWriter;
        impl Writer for NoopWriter {
            type Error = MockWriterError;
            fn commit_block(
                &self,
                _update: &BlockStateUpdate,
            ) -> Result<CommitStats, Self::Error> {
                Ok(CommitStats::default())
            }
            fn bootstrap_from_snapshot(
                &self,
                _accounts: Vec<mdbx::AccountState>,
                _block_number: u64,
                _block_hash: B256,
                _state_root: B256,
            ) -> Result<CommitStats, Self::Error> {
                Ok(CommitStats::default())
            }
        }

        let config = FlushLoopConfig {
            recv_timeout: Duration::from_millis(10),
        };

        let h = Arc::clone(&height);
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            run_flush_loop(&config, &update_rx, &commit_rx, &NoopWriter, &h, &s);
        });

        handle.join().expect("flush loop panicked");
        // Loop should have exited immediately.
        assert_eq!(height.load(Ordering::Acquire), 0);
    }
}
