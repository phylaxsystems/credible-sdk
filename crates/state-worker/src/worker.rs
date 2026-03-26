//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and buffered in memory. Blocks are only
//! flushed to MDBX when a "flush up to block N" command arrives on the flush
//! channel, ensuring the sidecar only reads committed state.
use crate::{
    genesis::GenesisState,
    metrics,
    state::{
        BlockStateUpdateBuilder,
        TraceProvider,
    },
    system_calls::{
        SystemCallConfig,
        SystemCalls,
    },
};
use alloy::rpc::types::Header;
use alloy_provider::{
    Provider,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures_util::StreamExt;
use mdbx::{
    BlockStateUpdate,
    Reader,
    Writer,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use tokio::{
    sync::broadcast,
    time,
};
use tracing::{
    debug,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_MISSING_BLOCK_RETRIES: u32 = 3;

/// Default number of `BlockStateUpdate` entries the in-memory buffer can hold
/// before backpressure kicks in and tracing pauses.
pub const DEFAULT_BUFFER_CAPACITY: usize = 64;

/// Coordinates block ingestion, tracing, and persistence.
///
/// In embedded mode the worker buffers traced blocks in a bounded
/// [`VecDeque`] and only flushes them to MDBX when the engine signals
/// a committed head via the `flush_rx` channel.
pub struct StateWorker<WR>
where
    WR: Writer + Reader,
{
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer_reader: WR,
    genesis_state: Option<GenesisState>,
    system_calls: SystemCalls,
    /// In-memory buffer of traced but not-yet-flushed block updates.
    buffer: VecDeque<BlockStateUpdate>,
    /// Maximum number of entries the buffer may hold before backpressure.
    buffer_capacity: usize,
    /// Receives "flush up to block N" commands from the engine.
    flush_rx: flume::Receiver<u64>,
    /// Atomically publishes the latest block number flushed to MDBX.
    mdbx_height: Arc<AtomicU64>,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    /// Create a new `StateWorker`.
    ///
    /// * `flush_rx` – channel that carries "flush up to block N" commands.
    /// * `mdbx_height` – shared atomic updated with [`Ordering::Release`]
    ///   after each successful MDBX write.
    /// * `buffer_capacity` – optional override for the in-memory buffer size
    ///   (defaults to [`DEFAULT_BUFFER_CAPACITY`]).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
        flush_rx: flume::Receiver<u64>,
        mdbx_height: Arc<AtomicU64>,
        buffer_capacity: Option<usize>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            buffer: VecDeque::new(),
            buffer_capacity: buffer_capacity.unwrap_or(DEFAULT_BUFFER_CAPACITY),
            flush_rx,
            mdbx_height,
        }
    }

    fn update_sync_metrics(next_block: u64, head_block: u64) {
        let lag_blocks = if next_block > head_block {
            0
        } else {
            head_block.saturating_sub(next_block).saturating_add(1)
        };
        let is_syncing = lag_blocks > 0;

        metrics::set_head_block(head_block);
        metrics::set_sync_lag_blocks(lag_blocks);
        metrics::set_syncing(is_syncing);
        metrics::set_following_head(!is_syncing);
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker encounters an unrecoverable failure.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;

        loop {
            // Check for shutdown before starting catch-up
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received");
                return Ok(());
            }

            // Drain any pending flush commands between iterations
            self.drain_flush_commands()?;

            // Catch up block-by-block, checking shutdown between each
            if self.catch_up(&mut next_block, &mut shutdown_rx).await? {
                info!("Shutdown signal received during catch-up");
                return Ok(());
            }

            match self.stream_blocks(&mut next_block, &mut shutdown_rx).await {
                Ok(()) => {
                    info!("Shutdown signal received during streaming");
                    return Ok(());
                }
                Err(err) => {
                    let err_str = err.to_string();
                    let is_missing_block = err_str.contains("Missing block");

                    if is_missing_block {
                        missing_block_retries += 1;

                        if missing_block_retries >= MAX_MISSING_BLOCK_RETRIES {
                            critical!(
                                error = %err,
                                retries = missing_block_retries,
                                "failed to recover from missing block after multiple retries"
                            );
                        }
                    } else {
                        // Reset retry counter for non-missing-block errors
                        missing_block_retries = 0;
                    }

                    warn!(error = %err, "block subscription ended, retrying");
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received during retry sleep");
                            return Ok(());
                        }
                        () = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)) => {}
                    }
                }
            }
        }
    }

    /// Determine the next block to ingest. We respect manual overrides so
    /// operators can force a resync of historical ranges when needed.
    fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = match self.writer_reader.latest_block_number() {
            Ok(current) => {
                metrics::set_db_healthy(true);
                current
            }
            Err(err) => {
                metrics::set_db_healthy(false);
                return Err(anyhow!(err)).context("failed to read current block from the database");
            }
        };

        Ok(current.map_or(0, |b| b + 1))
    }

    /// Sequentially replay blocks until we reach the node's current head.
    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            let head = self.provider.get_block_number().await?;
            Self::update_sync_metrics(*next_block, head);

            if *next_block > head {
                return Ok(false);
            }

            while *next_block <= head {
                // Backpressure: wait for buffer space before tracing next block
                self.wait_for_buffer_space().await?;

                // Process the block fully before checking shutdown
                self.process_block(*next_block).await?;
                *next_block += 1;
                Self::update_sync_metrics(*next_block, head);

                // Drain pending flush commands between blocks
                self.drain_flush_commands()?;

                // Check for shutdown after the block is complete
                if shutdown_rx.try_recv().is_ok() {
                    return Ok(true);
                }
            }
        }
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received during block streaming");
                    return Ok(());
                }
                maybe_header = stream.next() => {
                    match maybe_header {
                        Some(header) => {
                            let Header { hash: _, inner, .. } = header;
                            let block_number = inner.number;

                            // We may receive a header we already processed if the stream
                            // briefly disconnects; skip without rewinding because we do not
                            // currently handle reorgs.
                            if block_number + 1 < *next_block {
                                debug!(block_number, next_block, "skipping stale header");
                                continue;
                            }

                            Self::update_sync_metrics(*next_block, block_number);

                            // If we are missing a block, log a warning and return error.
                            // The run() loop tracks consecutive failures and escalates to
                            // critical if we cannot recover after MAX_MISSING_BLOCK_RETRIES.
                            if block_number > *next_block {
                                warn!("Missing block {block_number} (next block: {next_block})");
                                return Err(anyhow!(
                                    "Missing block {block_number} (next block: {next_block})"
                                ));
                            }

                            while *next_block <= block_number {
                                // Backpressure: wait for buffer space
                                self.wait_for_buffer_space().await?;

                                self.process_block(*next_block).await?;
                                *next_block += 1;
                                Self::update_sync_metrics(*next_block, block_number);

                                // Drain pending flush commands between blocks
                                self.drain_flush_commands()?;
                            }
                        }
                        None => {
                            return Err(anyhow!("block subscription completed"));
                        }
                    }
                }
            }
        }
    }

    /// Pull, trace, and buffer a single block.
    ///
    /// The block update is appended to the in-memory buffer rather than
    /// written directly to MDBX. Use [`Self::flush_to_mdbx`] to persist
    /// buffered updates.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.process_genesis_block().await;
        }

        let mut update = match self.trace_provider.fetch_block_state(block_number).await {
            Ok(update) => update,
            Err(err) => {
                critical!(error = ?err, block_number, "failed to trace block");
                metrics::record_block_failure();
                return Err(anyhow!("failed to trace block {block_number}"));
            }
        };

        // Apply system call state changes (EIP-2935 & EIP-4788)
        self.apply_system_calls(&mut update, block_number).await?;

        self.buffer.push_back(update);
        info!(
            block_number,
            buffer_len = self.buffer.len(),
            "block buffered"
        );

        Ok(())
    }

    /// Flush all buffered block updates with `block_number <= up_to_block`
    /// to MDBX and update the shared atomic height.
    ///
    /// # Errors
    ///
    /// Returns an error if any MDBX commit fails.
    pub fn flush_to_mdbx(&mut self, up_to_block: u64) -> Result<()> {
        let mut latest_flushed: Option<u64> = None;

        while let Some(front) = self.buffer.front() {
            if front.block_number > up_to_block {
                break;
            }

            // Safe to remove: we just verified the front exists and its
            // block_number is within range.
            let Some(update) = self.buffer.pop_front() else {
                break;
            };

            let block_number = update.block_number;
            match self.writer_reader.commit_block(&update) {
                Ok(stats) => {
                    metrics::set_db_healthy(true);
                    metrics::record_commit(block_number, &stats);
                    latest_flushed = Some(block_number);
                    info!(block_number, "block flushed to MDBX");
                }
                Err(err) => {
                    critical!(error = ?err, block_number, "failed to persist block");
                    metrics::set_db_healthy(false);
                    metrics::record_block_failure();
                    return Err(anyhow!("failed to persist block {block_number}"));
                }
            }
        }

        if let Some(block_number) = latest_flushed {
            self.mdbx_height.store(block_number, Ordering::Release);
            debug!(block_number, "mdbx_height updated");
        }

        Ok(())
    }

    /// Drain all pending flush commands from the channel without blocking.
    ///
    /// When the sender side has been dropped (standalone mode) this flushes
    /// every buffered block so the worker behaves like the pre-refactor
    /// write-immediately mode.
    fn drain_flush_commands(&mut self) -> Result<()> {
        loop {
            match self.flush_rx.try_recv() {
                Ok(up_to_block) => self.flush_to_mdbx(up_to_block)?,
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    // Sender dropped — auto-flush everything (standalone mode).
                    self.flush_all_buffered()?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Flush every entry currently in the buffer to MDBX.
    fn flush_all_buffered(&mut self) -> Result<()> {
        if let Some(back) = self.buffer.back() {
            let up_to = back.block_number;
            self.flush_to_mdbx(up_to)?;
        }
        Ok(())
    }

    /// Block until the buffer has room for at least one more entry.
    ///
    /// When the buffer is full, this waits asynchronously on the flush channel
    /// for commands that free space, providing natural backpressure on tracing.
    ///
    /// If the flush channel sender has been dropped (standalone mode), all
    /// buffered blocks are flushed immediately to free space.
    async fn wait_for_buffer_space(&mut self) -> Result<()> {
        while self.buffer.len() >= self.buffer_capacity {
            info!(
                buffer_len = self.buffer.len(),
                buffer_capacity = self.buffer_capacity,
                "buffer full, waiting for flush command (backpressure)"
            );
            if let Ok(up_to_block) = self.flush_rx.recv_async().await {
                self.flush_to_mdbx(up_to_block)?;
            } else {
                // Sender dropped — auto-flush everything (standalone mode).
                self.flush_all_buffered()?;
                break;
            }
        }
        Ok(())
    }

    /// Apply EIP-2935 and EIP-4788 system call state changes
    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        // Fetch block header for system call data
        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await?
            .context(format!("block {block_number} not found"))?;

        // Get parent block hash (current block's parent_hash field)
        let parent_block_hash = if block_number > 0 {
            Some(block.header.parent_hash)
        } else {
            None
        };

        // Get parent beacon block root from block header (EIP-4788)
        let parent_beacon_block_root = block.header.parent_beacon_block_root;

        let config = SystemCallConfig {
            block_number,
            timestamp: block.header.timestamp,
            parent_block_hash,
            parent_beacon_block_root,
        };

        // Pass the reader to fetch existing account state
        match self
            .system_calls
            .compute_system_call_states(&config, Some(&self.writer_reader))
        {
            Ok(states) => {
                for state in states {
                    update.merge_account_state(state);
                }
                debug!(block_number, "applied system call state changes");
                Ok(())
            }
            Err(err) => {
                // Log but don't fail
                warn!(
                    block_number,
                    error = %err,
                    "failed to compute system call states, traces may already include them"
                );
                Ok(())
            }
        }
    }

    /// Process block 0 using the configured genesis state
    async fn process_genesis_block(&mut self) -> Result<()> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(());
        };

        let already_exists = match self.writer_reader.latest_block_number() {
            Ok(current) => {
                metrics::set_db_healthy(true);
                current.is_some()
            }
            Err(err) => {
                metrics::set_db_healthy(false);
                return Err(anyhow!(err)).context(
                    "failed to read current block from database during genesis hydration",
                );
            }
        };

        if already_exists {
            info!("block 0 already exists in database; skipping genesis hydration");
            return Ok(());
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(());
        }

        // Fetch genesis block header to get hash and state root
        let block = self
            .provider
            .get_block_by_number(0.into())
            .await?
            .context("genesis block not found on chain")?;

        info!("hydrating genesis state from genesis file");

        let update = BlockStateUpdateBuilder::from_accounts(
            0,
            block.header.hash,
            block.header.state_root,
            accounts,
        );

        match self.writer_reader.commit_block(&update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(0, &stats);
            }
            Err(err) => {
                critical!(error = ?err, block_number = 0, "failed to persist genesis block");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                return Err(anyhow!("failed to persist genesis block"));
            }
        }

        info!(block_number = 0, "genesis block persisted to database");
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use mdbx::{
        AccountInfo,
        AccountState,
        AddressHash,
        CommitStats,
    };
    use std::sync::Mutex;

    // ---------------------------------------------------------------------------
    // Mock Writer + Reader that records commits for assertions.
    // ---------------------------------------------------------------------------

    #[derive(Debug, Default)]
    struct MockWriterReader {
        /// Block numbers that were committed (in order).
        committed_blocks: Mutex<Vec<u64>>,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("mock error")]
    struct MockError;

    impl Writer for MockWriterReader {
        type Error = MockError;

        fn commit_block(
            &self,
            update: &BlockStateUpdate,
        ) -> std::result::Result<CommitStats, MockError> {
            if let Ok(mut blocks) = self.committed_blocks.lock() {
                blocks.push(update.block_number);
            }
            Ok(CommitStats::default())
        }

        fn bootstrap_from_snapshot(
            &self,
            _accounts: Vec<AccountState>,
            _block_number: u64,
            _block_hash: B256,
            _state_root: B256,
        ) -> std::result::Result<CommitStats, MockError> {
            Ok(CommitStats::default())
        }
    }

    impl Reader for MockWriterReader {
        type Error = MockError;

        fn latest_block_number(&self) -> std::result::Result<Option<u64>, MockError> {
            Ok(None)
        }

        fn is_block_available(&self, _block_number: u64) -> std::result::Result<bool, MockError> {
            Ok(false)
        }

        fn get_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<Option<AccountInfo>, MockError> {
            Ok(None)
        }

        fn get_storage(
            &self,
            _address_hash: AddressHash,
            _slot_hash: B256,
            _block_number: u64,
        ) -> std::result::Result<Option<alloy::primitives::U256>, MockError> {
            Ok(None)
        }

        fn get_all_storage(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<std::collections::HashMap<B256, alloy::primitives::U256>, MockError>
        {
            Ok(std::collections::HashMap::new())
        }

        fn get_code(
            &self,
            _code_hash: B256,
            _block_number: u64,
        ) -> std::result::Result<Option<alloy::primitives::Bytes>, MockError> {
            Ok(None)
        }

        fn get_full_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<Option<AccountState>, MockError> {
            Ok(None)
        }

        fn get_block_hash(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<B256>, MockError> {
            Ok(None)
        }

        fn get_state_root(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<B256>, MockError> {
            Ok(None)
        }

        fn get_block_metadata(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<mdbx::BlockMetadata>, MockError> {
            Ok(None)
        }

        fn get_available_block_range(&self) -> std::result::Result<Option<(u64, u64)>, MockError> {
            Ok(None)
        }

        fn scan_account_hashes(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Vec<AddressHash>, MockError> {
            Ok(Vec::new())
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /// Create a dummy provider that will never be used for real I/O.
    fn dummy_provider() -> Arc<RootProvider> {
        let provider = alloy_provider::ProviderBuilder::new()
            .connect_http("http://localhost:1".parse().expect("valid url"));
        Arc::new(provider.root().clone())
    }

    fn make_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO)
    }

    fn committed_blocks(wr: &MockWriterReader) -> Vec<u64> {
        wr.committed_blocks
            .lock()
            .map_or_else(|_| Vec::new(), |b| b.clone())
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    #[test]
    fn buffering_without_flush_does_not_write_to_mdbx() {
        let wr = MockWriterReader::default();
        let (_flush_tx, flush_rx) = flume::bounded::<u64>(16);
        let mdbx_height = Arc::new(AtomicU64::new(0));

        // We cannot call `new()` without a real provider/trace_provider, but we
        // can construct the struct directly inside this test module since we have
        // access to private fields.
        let mut worker = StateWorker {
            provider: dummy_provider(),
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            buffer: VecDeque::new(),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_rx,
            mdbx_height: mdbx_height.clone(),
        };

        // Push blocks directly into the buffer (simulating what process_block does
        // after tracing).
        worker.buffer.push_back(make_update(1));
        worker.buffer.push_back(make_update(2));
        worker.buffer.push_back(make_update(3));

        // Nothing should have been written yet.
        assert_eq!(committed_blocks(&worker.writer_reader), Vec::<u64>::new());
        assert_eq!(mdbx_height.load(Ordering::Acquire), 0);
        assert_eq!(worker.buffer.len(), 3);
    }

    #[test]
    fn flush_signal_writes_buffered_blocks_to_mdbx() {
        let wr = MockWriterReader::default();
        let (flush_tx, flush_rx) = flume::bounded::<u64>(16);
        let mdbx_height = Arc::new(AtomicU64::new(0));

        let mut worker = StateWorker {
            provider: dummy_provider(),
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            buffer: VecDeque::new(),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_rx,
            mdbx_height: mdbx_height.clone(),
        };

        // Buffer several blocks.
        worker.buffer.push_back(make_update(1));
        worker.buffer.push_back(make_update(2));
        worker.buffer.push_back(make_update(3));
        worker.buffer.push_back(make_update(4));

        // Nothing written yet.
        assert!(committed_blocks(&worker.writer_reader).is_empty());

        // Flush up to block 2.
        flush_tx.send(2).expect("send should succeed");
        worker.drain_flush_commands().expect("drain should succeed");

        assert_eq!(committed_blocks(&worker.writer_reader), vec![1, 2]);
        assert_eq!(mdbx_height.load(Ordering::Acquire), 2);
        // Blocks 3 and 4 remain in the buffer.
        assert_eq!(worker.buffer.len(), 2);

        // Flush up to block 4.
        flush_tx.send(4).expect("send should succeed");
        worker.drain_flush_commands().expect("drain should succeed");

        assert_eq!(committed_blocks(&worker.writer_reader), vec![1, 2, 3, 4]);
        assert_eq!(mdbx_height.load(Ordering::Acquire), 4);
        assert!(worker.buffer.is_empty());
    }

    #[test]
    fn flush_with_no_matching_blocks_is_noop() {
        let wr = MockWriterReader::default();
        let (_flush_tx, flush_rx) = flume::bounded::<u64>(16);
        let mdbx_height = Arc::new(AtomicU64::new(0));

        let mut worker = StateWorker {
            provider: dummy_provider(),
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            buffer: VecDeque::new(),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_rx,
            mdbx_height: mdbx_height.clone(),
        };

        // Buffer block 5 only.
        worker.buffer.push_back(make_update(5));

        // Flush up to block 3 – nothing should be drained because 5 > 3.
        worker.flush_to_mdbx(3).expect("flush should succeed");

        assert!(committed_blocks(&worker.writer_reader).is_empty());
        assert_eq!(mdbx_height.load(Ordering::Acquire), 0);
        assert_eq!(worker.buffer.len(), 1);
    }

    #[test]
    fn backpressure_blocks_when_buffer_full() {
        let wr = MockWriterReader::default();
        let (flush_tx, flush_rx) = flume::bounded::<u64>(16);
        let mdbx_height = Arc::new(AtomicU64::new(0));

        let mut worker = StateWorker {
            provider: dummy_provider(),
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            buffer: VecDeque::new(),
            buffer_capacity: 2, // tiny capacity for testing
            flush_rx,
            mdbx_height: mdbx_height.clone(),
        };

        // Fill the buffer to capacity.
        worker.buffer.push_back(make_update(1));
        worker.buffer.push_back(make_update(2));
        assert_eq!(worker.buffer.len(), worker.buffer_capacity);

        // Send a flush command that will free space.
        flush_tx.send(1).expect("send should succeed");

        // wait_for_buffer_space should drain the flush and make room.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            worker
                .wait_for_buffer_space()
                .await
                .expect("should free space");
        });

        // Block 1 was flushed, block 2 remains.
        assert_eq!(worker.buffer.len(), 1);
        assert_eq!(committed_blocks(&worker.writer_reader), vec![1]);
        assert_eq!(mdbx_height.load(Ordering::Acquire), 1);
    }

    #[test]
    fn buffer_capacity_defaults_to_64() {
        let (_flush_tx, flush_rx) = flume::bounded::<u64>(1);
        let mdbx_height = Arc::new(AtomicU64::new(0));

        let worker = StateWorker {
            provider: dummy_provider(),
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: MockWriterReader::default(),
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            buffer: VecDeque::new(),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            flush_rx,
            mdbx_height,
        };

        assert_eq!(worker.buffer_capacity, 64);
    }

    // A no-op trace provider used only for constructing the struct in tests.
    struct NoopTraceProvider;

    #[async_trait::async_trait]
    impl TraceProvider for NoopTraceProvider {
        async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
            Ok(make_update(block_number))
        }
    }
}
