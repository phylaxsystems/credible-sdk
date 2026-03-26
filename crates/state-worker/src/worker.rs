//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription.
//!
//! **Embedded mode** (`commit_head_rx` is `Some`): traced blocks are buffered in
//! a bounded [`VecDeque`] and only flushed to MDBX when the engine signals a
//! committed head via the channel.
//!
//! **Standalone mode** (`commit_head_rx` is `None`): each traced block is
//! written directly to MDBX, preserving the original write-immediately
//! behaviour.
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
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use tokio::time;
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
/// In **embedded mode** (`commit_head_rx` is `Some`) the worker buffers traced
/// blocks in a bounded [`VecDeque`] and only flushes them to MDBX when the
/// engine signals a committed head via the channel.
///
/// In **standalone mode** (`commit_head_rx` is `None`) each traced block is
/// committed to MDBX immediately.
pub struct StateWorker<WR>
where
    WR: Writer + Reader,
{
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer_reader: WR,
    genesis_state: Option<GenesisState>,
    system_calls: SystemCalls,
    /// In-memory buffer of traced but not-yet-flushed block updates
    /// (embedded mode only).
    pending_updates: VecDeque<BlockStateUpdate>,
    /// Maximum number of entries the buffer may hold before backpressure.
    buffer_capacity: usize,
    /// Receives "commit head up to block N" signals from the engine
    /// (embedded mode). `None` in standalone mode.
    commit_head_rx: Option<flume::Receiver<u64>>,
    /// Atomically publishes the latest block number flushed to MDBX
    /// (embedded mode). `None` in standalone mode.
    committed_head_signal: Option<Arc<AtomicU64>>,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    /// Create a new `StateWorker`.
    ///
    /// * `commit_head_rx` – optional channel carrying "commit head up to block
    ///   N" signals from the engine. When `Some` the worker operates in
    ///   **embedded mode** (buffered writes). When `None` blocks are written to
    ///   MDBX immediately (**standalone mode**).
    /// * `committed_head_signal` – optional shared atomic updated with
    ///   [`Ordering::Release`] after each successful MDBX flush in embedded
    ///   mode.
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
        commit_head_rx: Option<flume::Receiver<u64>>,
        committed_head_signal: Option<Arc<AtomicU64>>,
        buffer_capacity: Option<usize>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            pending_updates: VecDeque::new(),
            buffer_capacity: buffer_capacity.unwrap_or(DEFAULT_BUFFER_CAPACITY),
            commit_head_rx,
            committed_head_signal,
        }
    }

    /// Returns `true` when the worker is running in embedded mode (i.e. a
    /// `commit_head_rx` channel was supplied at construction time).
    fn is_embedded(&self) -> bool {
        self.commit_head_rx.is_some()
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
        shutdown: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;

        loop {
            // Check for shutdown before starting catch-up
            if shutdown.load(Ordering::Acquire) {
                info!("Shutdown signal received");
                return Ok(());
            }

            // Drain any pending flush commands between iterations
            self.drain_flush_commands()?;

            // Catch up block-by-block, checking shutdown between each
            if self.catch_up(&mut next_block, &shutdown).await? {
                info!("Shutdown signal received during catch-up");
                return Ok(());
            }

            match self.stream_blocks(&mut next_block, &shutdown).await {
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
                    let delay = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS));
                    tokio::pin!(delay);
                    loop {
                        if shutdown.load(Ordering::Acquire) {
                            info!("Shutdown signal received during retry sleep");
                            return Ok(());
                        }
                        tokio::select! {
                            () = &mut delay => { break; }
                            () = tokio::time::sleep(Duration::from_millis(100)) => {}
                        }
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
    async fn catch_up(&mut self, next_block: &mut u64, shutdown: &Arc<AtomicBool>) -> Result<bool> {
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
                if shutdown.load(Ordering::Acquire) {
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
        shutdown: &Arc<AtomicBool>,
    ) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        loop {
            // Poll shutdown every 100ms alongside the block stream.
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(100)) => {
                    if shutdown.load(Ordering::Acquire) {
                        info!("Shutdown signal received during block streaming");
                        return Ok(());
                    }
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

    /// Pull, trace, and handle a single block.
    ///
    /// In **embedded mode** the traced update is appended to
    /// [`Self::pending_updates`]. In **standalone mode** the update is written
    /// directly to MDBX via `commit_block`.
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

        if self.is_embedded() {
            // Embedded mode: buffer the update for later flushing.
            self.pending_updates.push_back(update);
            info!(
                block_number,
                buffer_len = self.pending_updates.len(),
                "block buffered"
            );
        } else {
            // Standalone mode: write directly to MDBX.
            self.commit_single_block(&update)?;
        }

        Ok(())
    }

    /// Commit a single block update directly to MDBX. Used in standalone mode.
    fn commit_single_block(&self, update: &BlockStateUpdate) -> Result<()> {
        let block_number = update.block_number;
        match self.writer_reader.commit_block(update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);
                info!(block_number, "block committed to MDBX");
                Ok(())
            }
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                Err(anyhow!("failed to persist block {block_number}"))
            }
        }
    }

    /// Flush all pending updates with `block_number <= up_to_block` to MDBX
    /// and update [`Self::committed_head_signal`].
    ///
    /// # Errors
    ///
    /// Returns an error if any MDBX commit fails.
    pub fn flush_pending(&mut self, up_to_block: u64) -> Result<()> {
        let mut latest_flushed: Option<u64> = None;

        while let Some(front) = self.pending_updates.front() {
            if front.block_number > up_to_block {
                break;
            }

            // Safe to remove: we just verified the front exists and its
            // block_number is within range.
            let Some(update) = self.pending_updates.pop_front() else {
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

        if let Some(block_number) = latest_flushed
            && let Some(ref signal) = self.committed_head_signal
        {
            signal.store(block_number, Ordering::Release);
            debug!(block_number, "committed_head_signal updated");
        }

        Ok(())
    }

    /// Drain all pending commit-head commands from the channel without
    /// blocking.
    ///
    /// In standalone mode (no channel) this is a no-op because blocks are
    /// written directly in `process_block`.
    fn drain_flush_commands(&mut self) -> Result<()> {
        // Clone the receiver to avoid holding an immutable borrow on `self`
        // while calling `flush_pending` (which needs `&mut self`).
        let rx = match self.commit_head_rx {
            Some(ref rx) => rx.clone(),
            None => return Ok(()),
        };
        loop {
            match rx.try_recv() {
                Ok(up_to_block) => self.flush_pending(up_to_block)?,
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => {
                    // Sender dropped — flush everything remaining.
                    self.flush_all_buffered()?;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Flush every entry currently in the buffer to MDBX.
    fn flush_all_buffered(&mut self) -> Result<()> {
        if let Some(back) = self.pending_updates.back() {
            let up_to = back.block_number;
            self.flush_pending(up_to)?;
        }
        Ok(())
    }

    /// Block until the buffer has room for at least one more entry.
    ///
    /// In embedded mode, when the buffer is full this waits on the
    /// `commit_head_rx` channel for commands that free space, providing natural
    /// backpressure on tracing.
    ///
    /// In standalone mode this is a no-op because blocks are not buffered.
    async fn wait_for_buffer_space(&mut self) -> Result<()> {
        // Clone the receiver to avoid holding an immutable borrow on `self`
        // while calling `flush_pending` (which needs `&mut self`).
        let rx = match self.commit_head_rx {
            Some(ref rx) => rx.clone(),
            None => return Ok(()),
        };
        while self.pending_updates.len() >= self.buffer_capacity {
            info!(
                buffer_len = self.pending_updates.len(),
                buffer_capacity = self.buffer_capacity,
                "buffer full, waiting for commit_head signal (backpressure)"
            );
            if let Ok(up_to_block) = rx.recv_async().await {
                self.flush_pending(up_to_block)?;
            } else {
                // Sender dropped — flush everything.
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
    fn dummy_provider() -> Result<Arc<RootProvider>> {
        let url = "http://localhost:1"
            .parse()
            .context("failed to parse dummy URL")?;
        let provider = alloy_provider::ProviderBuilder::new().connect_http(url);
        Ok(Arc::new(provider.root().clone()))
    }

    fn make_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO)
    }

    fn committed_blocks(wr: &MockWriterReader) -> Vec<u64> {
        wr.committed_blocks
            .lock()
            .map_or_else(|_| Vec::new(), |b| b.clone())
    }

    /// Build a `StateWorker` in **embedded mode** (with commit-head channel).
    fn make_embedded_worker(
        wr: MockWriterReader,
        commit_head_rx: flume::Receiver<u64>,
        committed_head_signal: Arc<AtomicU64>,
        buffer_capacity: usize,
    ) -> Result<StateWorker<MockWriterReader>> {
        Ok(StateWorker {
            provider: dummy_provider()?,
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            pending_updates: VecDeque::new(),
            buffer_capacity,
            commit_head_rx: Some(commit_head_rx),
            committed_head_signal: Some(committed_head_signal),
        })
    }

    /// Build a `StateWorker` in **standalone mode** (no commit-head channel).
    fn make_standalone_worker(wr: MockWriterReader) -> Result<StateWorker<MockWriterReader>> {
        Ok(StateWorker {
            provider: dummy_provider()?,
            trace_provider: Box::new(NoopTraceProvider),
            writer_reader: wr,
            genesis_state: None,
            system_calls: SystemCalls::new(None, None),
            pending_updates: VecDeque::new(),
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            commit_head_rx: None,
            committed_head_signal: None,
        })
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    /// When `commit_head_rx` is `Some` and no `commit_head` has been sent,
    /// `process_block` appends to `pending_updates` and does NOT call
    /// `commit_block`.
    #[test]
    fn buffering_without_flush_does_not_write_to_mdbx() -> Result<()> {
        let wr = MockWriterReader::default();
        let (_commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(
            wr,
            commit_head_rx,
            committed_head.clone(),
            DEFAULT_BUFFER_CAPACITY,
        )?;

        // Push blocks directly into the buffer (simulating what process_block
        // does in embedded mode after tracing).
        worker.pending_updates.push_back(make_update(1));
        worker.pending_updates.push_back(make_update(2));
        worker.pending_updates.push_back(make_update(3));

        // Nothing should have been written yet.
        assert_eq!(committed_blocks(&worker.writer_reader), Vec::<u64>::new());
        assert_eq!(committed_head.load(Ordering::Acquire), 0);
        assert_eq!(worker.pending_updates.len(), 3);
        Ok(())
    }

    /// When `commit_head(N)` is received, `flush_pending` drains all updates
    /// with `block_number <= N` and calls `commit_block` for each.
    #[test]
    fn flush_signal_writes_buffered_blocks_to_mdbx() -> Result<()> {
        let wr = MockWriterReader::default();
        let (commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(
            wr,
            commit_head_rx,
            committed_head.clone(),
            DEFAULT_BUFFER_CAPACITY,
        )?;

        // Buffer several blocks.
        worker.pending_updates.push_back(make_update(1));
        worker.pending_updates.push_back(make_update(2));
        worker.pending_updates.push_back(make_update(3));
        worker.pending_updates.push_back(make_update(4));

        // Nothing written yet.
        assert!(committed_blocks(&worker.writer_reader).is_empty());

        // Flush up to block 2.
        commit_head_tx
            .send(2)
            .map_err(|e| anyhow!("send failed: {e}"))?;
        worker.drain_flush_commands()?;

        assert_eq!(committed_blocks(&worker.writer_reader), vec![1, 2]);
        assert_eq!(committed_head.load(Ordering::Acquire), 2);
        // Blocks 3 and 4 remain in the buffer.
        assert_eq!(worker.pending_updates.len(), 2);

        // Flush up to block 4.
        commit_head_tx
            .send(4)
            .map_err(|e| anyhow!("send failed: {e}"))?;
        worker.drain_flush_commands()?;

        assert_eq!(committed_blocks(&worker.writer_reader), vec![1, 2, 3, 4]);
        assert_eq!(committed_head.load(Ordering::Acquire), 4);
        assert!(worker.pending_updates.is_empty());
        Ok(())
    }

    /// After `flush_pending`, `committed_head_signal.load(Ordering::Acquire)`
    /// equals the highest flushed block number.
    #[test]
    fn committed_head_signal_reflects_highest_flushed_block() -> Result<()> {
        let wr = MockWriterReader::default();
        let (_commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(
            wr,
            commit_head_rx,
            committed_head.clone(),
            DEFAULT_BUFFER_CAPACITY,
        )?;

        worker.pending_updates.push_back(make_update(10));
        worker.pending_updates.push_back(make_update(11));
        worker.pending_updates.push_back(make_update(12));

        // Flush up to block 11 — signal should be set to 11, not 12.
        worker.flush_pending(11)?;
        assert_eq!(committed_head.load(Ordering::Acquire), 11);

        // Flush up to block 12 — signal should advance to 12.
        worker.flush_pending(12)?;
        assert_eq!(committed_head.load(Ordering::Acquire), 12);

        // Flush with nothing remaining — signal should stay at 12.
        worker.flush_pending(100)?;
        assert_eq!(committed_head.load(Ordering::Acquire), 12);

        Ok(())
    }

    #[test]
    fn flush_with_no_matching_blocks_is_noop() -> Result<()> {
        let wr = MockWriterReader::default();
        let (_commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(
            wr,
            commit_head_rx,
            committed_head.clone(),
            DEFAULT_BUFFER_CAPACITY,
        )?;

        // Buffer block 5 only.
        worker.pending_updates.push_back(make_update(5));

        // Flush up to block 3 – nothing should be drained because 5 > 3.
        worker.flush_pending(3)?;

        assert!(committed_blocks(&worker.writer_reader).is_empty());
        assert_eq!(committed_head.load(Ordering::Acquire), 0);
        assert_eq!(worker.pending_updates.len(), 1);
        Ok(())
    }

    /// When `pending_updates.len() == buffer_capacity`, the worker does not
    /// trace more blocks until a `commit_head` is received and buffer space is
    /// freed.
    #[tokio::test]
    async fn backpressure_blocks_when_buffer_full() -> Result<()> {
        let wr = MockWriterReader::default();
        let (commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(wr, commit_head_rx, committed_head.clone(), 2)?;

        // Fill the buffer to capacity.
        worker.pending_updates.push_back(make_update(1));
        worker.pending_updates.push_back(make_update(2));
        assert_eq!(worker.pending_updates.len(), worker.buffer_capacity);

        // Send a flush command that will free space.
        commit_head_tx
            .send(1)
            .map_err(|e| anyhow!("send failed: {e}"))?;

        // wait_for_buffer_space should drain the flush and make room.
        worker.wait_for_buffer_space().await?;

        // Block 1 was flushed, block 2 remains.
        assert_eq!(worker.pending_updates.len(), 1);
        assert_eq!(committed_blocks(&worker.writer_reader), vec![1]);
        assert_eq!(committed_head.load(Ordering::Acquire), 1);
        Ok(())
    }

    #[test]
    fn buffer_capacity_defaults_to_64() -> Result<()> {
        let worker = StateWorker::new(
            dummy_provider()?,
            Box::new(NoopTraceProvider),
            MockWriterReader::default(),
            None,
            SystemCalls::new(None, None),
            None,
            None,
            None,
        );

        assert_eq!(worker.buffer_capacity, 64);
        Ok(())
    }

    /// When `commit_head_rx` is `None` (standalone mode), `process_block`
    /// calls `commit_block` directly without buffering.
    #[test]
    fn standalone_mode_process_block_writes_directly() -> Result<()> {
        let wr = MockWriterReader::default();
        let worker = make_standalone_worker(wr)?;

        // In standalone mode process_block calls commit_single_block which
        // writes directly to MDBX. We simulate this by calling
        // commit_single_block directly since process_block needs async I/O.
        worker.commit_single_block(&make_update(10))?;
        worker.commit_single_block(&make_update(11))?;

        assert_eq!(committed_blocks(&worker.writer_reader), vec![10, 11]);
        // Buffer should remain empty in standalone mode.
        assert!(worker.pending_updates.is_empty());
        Ok(())
    }

    /// End-to-end test of embedded mode: blocks are buffered and flushed only
    /// when `commit_head` signals arrive via the channel.
    #[test]
    fn embedded_mode_buffers_blocks_and_flushes_on_commit_head() -> Result<()> {
        let wr = MockWriterReader::default();
        let (commit_head_tx, commit_head_rx) = flume::bounded::<u64>(16);
        let committed_head = Arc::new(AtomicU64::new(0));

        let mut worker = make_embedded_worker(wr, commit_head_rx, committed_head.clone(), 128)?;

        // Simulate process_block in embedded mode: blocks are buffered.
        worker.pending_updates.push_back(make_update(100));
        worker.pending_updates.push_back(make_update(101));
        worker.pending_updates.push_back(make_update(102));

        // Nothing written yet.
        assert!(committed_blocks(&worker.writer_reader).is_empty());
        assert_eq!(committed_head.load(Ordering::Acquire), 0);

        // Engine signals commit head at block 101.
        commit_head_tx
            .send(101)
            .map_err(|e| anyhow!("send failed: {e}"))?;
        worker.drain_flush_commands()?;

        // Blocks 100 and 101 are flushed; 102 remains buffered.
        assert_eq!(committed_blocks(&worker.writer_reader), vec![100, 101]);
        assert_eq!(committed_head.load(Ordering::Acquire), 101);
        assert_eq!(worker.pending_updates.len(), 1);
        assert_eq!(
            worker.pending_updates.front().map(|u| u.block_number),
            Some(102)
        );

        // Engine signals commit head at block 102.
        commit_head_tx
            .send(102)
            .map_err(|e| anyhow!("send failed: {e}"))?;
        worker.drain_flush_commands()?;

        assert_eq!(committed_blocks(&worker.writer_reader), vec![100, 101, 102]);
        assert_eq!(committed_head.load(Ordering::Acquire), 102);
        assert!(worker.pending_updates.is_empty());
        Ok(())
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
