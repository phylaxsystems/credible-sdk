//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into the database.
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
use futures_util::{
    FutureExt,
    StreamExt,
};
use mdbx::{
    BlockStateUpdate,
    CommitStats,
    Reader,
    Writer,
};
use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        broadcast,
        mpsc,
    },
    time,
};
use tracing::{
    debug,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_MISSING_BLOCK_RETRIES: u32 = 3;

/// Default maximum number of blocks to buffer in memory before applying backpressure.
/// This prevents unbounded memory growth during periods where flush_buffered_blocks
/// is not called frequently enough.
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 100;

/// Default flush interval when no external coordination is available
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 5;

pub const BUFFER_STATE_FILE_NAME: &str = "state-worker-buffer.json";

/// Error returned when the memory buffer is at capacity and cannot accept more blocks.
#[derive(Debug)]
pub struct BufferCapacityError {
    pub capacity: usize,
}

impl std::fmt::Display for BufferCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Buffer is at capacity ({} blocks), cannot buffer more updates",
            self.capacity
        )
    }
}

impl std::error::Error for BufferCapacityError {}

/// Command sent to StateWorker to control buffer flushing
#[derive(Debug, Clone)]
pub enum FlushCommand {
    /// Flush all blocks up to and including the specified block number
    FlushUpTo(u64),
    /// Flush all buffered blocks immediately
    FlushAll,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedBufferState {
    buffered_blocks: Vec<BlockStateUpdate>,
}

fn buffer_block_range(buffer: &HashMap<u64, BlockStateUpdate>) -> Option<(u64, u64)> {
    if buffer.is_empty() {
        return None;
    }

    let min = *buffer.keys().min()?;
    let max = *buffer.keys().max()?;
    Some((min, max))
}

fn flush_buffered_blocks_impl<WR>(
    writer_reader: &WR,
    buffer: &mut HashMap<u64, BlockStateUpdate>,
    up_to_block: u64,
) -> Result<(usize, CommitStats)>
where
    WR: Writer,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
{
    let blocks_to_flush: Vec<u64> = buffer
        .keys()
        .copied()
        .filter(|&block_num| block_num <= up_to_block)
        .collect();

    if blocks_to_flush.is_empty() {
        debug!(up_to_block, "no blocks to flush");
        return Ok((0, CommitStats::default()));
    }

    let mut sorted_blocks = blocks_to_flush;
    sorted_blocks.sort_unstable();

    let mut total_stats = CommitStats::default();
    let mut flushed_count = 0;

    for block_number in sorted_blocks {
        let update = buffer
            .remove(&block_number)
            .context(format!("Block {block_number} should be in buffer"))?;

        match writer_reader.commit_block(&update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);

                total_stats.accounts_written += stats.accounts_written;
                total_stats.accounts_deleted += stats.accounts_deleted;
                total_stats.storage_slots_written += stats.storage_slots_written;
                total_stats.storage_slots_deleted += stats.storage_slots_deleted;
                total_stats.full_storage_deletes += stats.full_storage_deletes;
                total_stats.bytecodes_written += stats.bytecodes_written;
                total_stats.diffs_applied += stats.diffs_applied;
                total_stats.diff_bytes += stats.diff_bytes;
                total_stats.largest_account_storage = total_stats
                    .largest_account_storage
                    .max(stats.largest_account_storage);
                total_stats.preprocess_duration += stats.preprocess_duration;
                total_stats.diff_application_duration += stats.diff_application_duration;
                total_stats.batch_write_duration += stats.batch_write_duration;
                total_stats.commit_duration += stats.commit_duration;
                total_stats.total_duration += stats.total_duration;

                flushed_count += 1;

                info!(block_number, "block flushed to database");
            }
            Err(err) => {
                critical!(error = ?err, block_number, "failed to flush block to database");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();

                buffer.insert(block_number, update);

                return Err(anyhow!("failed to flush block {block_number}"));
            }
        }
    }

    info!(
        flushed_count,
        up_to_block,
        remaining_buffer_size = buffer.len(),
        "flushed blocks from buffer to database"
    );

    Ok((flushed_count, total_stats))
}

fn load_buffered_blocks(path: &Path) -> Result<HashMap<u64, BlockStateUpdate>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open buffered block state at {}", path.display()))?;
    let persisted: PersistedBufferState = serde_json::from_reader(file).with_context(|| {
        format!(
            "failed to deserialize buffered block state at {}",
            path.display()
        )
    })?;

    let mut buffer = HashMap::with_capacity(persisted.buffered_blocks.len());
    for update in persisted.buffered_blocks {
        let block_number = update.block_number;
        if buffer.insert(block_number, update).is_some() {
            return Err(anyhow!(
                "duplicate buffered block {block_number} found in persisted buffer state"
            ));
        }
    }

    Ok(buffer)
}

fn persist_buffered_blocks(path: &Path, buffer: &HashMap<u64, BlockStateUpdate>) -> Result<()> {
    if buffer.is_empty() {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to remove buffered block state at {}",
                        path.display()
                    )
                });
            }
        }
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create buffered block state directory at {}",
                parent.display()
            )
        })?;
    }

    let mut buffered_blocks: Vec<BlockStateUpdate> = buffer.values().cloned().collect();
    buffered_blocks.sort_unstable_by_key(|update| update.block_number);

    let temp_path = path.with_extension("tmp");
    let file = std::fs::File::create(&temp_path).with_context(|| {
        format!(
            "failed to create temporary buffered block state at {}",
            temp_path.display()
        )
    })?;
    serde_json::to_writer(&file, &PersistedBufferState { buffered_blocks }).with_context(|| {
        format!(
            "failed to serialize buffered block state to {}",
            temp_path.display()
        )
    })?;
    file.sync_all().with_context(|| {
        format!(
            "failed to sync temporary buffered block state at {}",
            temp_path.display()
        )
    })?;
    std::fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to replace buffered block state at {}",
            path.display()
        )
    })?;

    Ok(())
}

pub fn default_buffer_state_path(mdbx_path: &str) -> PathBuf {
    Path::new(mdbx_path).join(BUFFER_STATE_FILE_NAME)
}

fn reconcile_buffered_blocks_impl<WR>(
    writer_reader: &WR,
    buffer: &mut HashMap<u64, BlockStateUpdate>,
) -> Result<()>
where
    WR: Reader,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    let latest_persisted = writer_reader
        .latest_block_number()
        .map_err(anyhow::Error::from)
        .context("failed to read latest block during recovery")?;

    if let Some(latest_persisted) = latest_persisted {
        let retained_before = buffer.len();
        buffer.retain(|block_number, _| *block_number > latest_persisted);
        let dropped_blocks = retained_before.saturating_sub(buffer.len());

        if dropped_blocks > 0 {
            info!(
                latest_persisted,
                dropped_blocks, "discarded already-persisted buffered blocks during recovery"
            );
        }
    }

    let Some((min_block, max_block)) = buffer_block_range(buffer) else {
        return Ok(());
    };

    let expected_next_block = latest_persisted.map_or(0, |block| block.saturating_add(1));
    let expected_buffer_len =
        usize::try_from(max_block.saturating_sub(min_block).saturating_add(1))
            .context("buffer range length should fit in usize during recovery")?;

    if min_block != expected_next_block || buffer.len() != expected_buffer_len {
        let cleared_count = buffer.len();
        buffer.clear();
        warn!(
            cleared_count,
            min_block,
            max_block,
            expected_contiguous_blocks = expected_buffer_len,
            expected_next_block,
            "discarded non-contiguous buffered blocks during recovery"
        );
        return Ok(());
    }

    info!(
        min_block,
        max_block,
        buffered_blocks = buffer.len(),
        "recovered buffered blocks for coordinated flushing"
    );
    Ok(())
}

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker<WR>
where
    WR: Writer + Reader,
{
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer_reader: WR,
    genesis_state: Option<GenesisState>,
    system_calls: SystemCalls,
    /// In-memory buffer of block updates waiting to be flushed to MDBX.
    /// Key is block_number, value is the complete BlockStateUpdate.
    buffer: HashMap<u64, BlockStateUpdate>,
    /// Maximum number of blocks to buffer before applying backpressure.
    max_buffer_size: usize,
    /// Receiver for external flush commands
    flush_rx: Option<mpsc::UnboundedReceiver<FlushCommand>>,
    /// Enable automatic periodic flushing when no external coordination is available
    auto_flush_enabled: bool,
    /// Optional persisted copy of the in-memory buffer for restart recovery.
    buffer_state_path: Option<PathBuf>,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    #[allow(dead_code)]
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
    ) -> Self {
        Self::with_buffer_config(
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            DEFAULT_MAX_BUFFER_SIZE,
        )
    }

    #[allow(dead_code)]
    pub fn with_buffer_config(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
        max_buffer_size: usize,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            buffer: HashMap::new(),
            max_buffer_size,
            flush_rx: None,
            auto_flush_enabled: true,
            buffer_state_path: None,
        }
    }

    /// Create StateWorker with external flush coordination
    #[allow(dead_code)]
    pub fn with_flush_coordination(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
        max_buffer_size: usize,
        flush_rx: mpsc::UnboundedReceiver<FlushCommand>,
        buffer_state_path: PathBuf,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            buffer: HashMap::new(),
            max_buffer_size,
            flush_rx: Some(flush_rx),
            auto_flush_enabled: false,
            buffer_state_path: Some(buffer_state_path),
        }
    }

    /// Create StateWorker optimized for testing with faster auto-flush
    #[cfg(test)]
    pub fn new_for_testing(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            buffer: HashMap::new(),
            max_buffer_size: DEFAULT_MAX_BUFFER_SIZE,
            flush_rx: None,
            auto_flush_enabled: true,
            buffer_state_path: None,
        }
    }

    /// Create a flush command sender for external coordination
    pub fn create_flush_coordinator() -> (
        mpsc::UnboundedSender<FlushCommand>,
        mpsc::UnboundedReceiver<FlushCommand>,
    ) {
        mpsc::unbounded_channel()
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

    /// Recover buffered state when reusing the worker after a failure.
    ///
    /// Buffered blocks are loaded from disk, reconciled against the persisted DB head,
    /// and kept in memory until an external flush command persists them to MDBX.
    fn recover_buffer_state(&mut self) -> Result<()> {
        if let Some(path) = &self.buffer_state_path {
            self.buffer = load_buffered_blocks(path)?;
        }

        reconcile_buffered_blocks_impl(&self.writer_reader, &mut self.buffer)?;
        self.persist_buffer_state()
    }

    fn flush_all_buffered_blocks(&mut self) -> Result<(usize, CommitStats)> {
        if let Some((_, max_block)) = self.buffer_block_range() {
            return self.flush_buffered_blocks(max_block);
        }

        Ok((0, CommitStats::default()))
    }

    fn is_buffer_capacity_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<BufferCapacityError>().is_some()
    }

    fn persist_buffer_state(&self) -> Result<()> {
        if let Some(path) = &self.buffer_state_path {
            persist_buffered_blocks(path, &self.buffer)?;
        }

        Ok(())
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        // Recover buffer state on startup
        self.recover_buffer_state()?;

        let mut next_block = self.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;
        let mut buffer_backpressured = !self.has_buffer_capacity();

        // Set up flush interval timer for auto-flush mode
        let flush_duration = if cfg!(test) {
            Duration::from_millis(200) // Fast flushing for tests
        } else {
            Duration::from_secs(DEFAULT_FLUSH_INTERVAL_SECS)
        };
        let mut flush_interval = time::interval(flush_duration);
        flush_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // Take ownership of flush_rx to avoid borrowing conflicts
        let mut flush_rx = self.flush_rx.take();

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    return Ok(());
                }

                // Handle flush commands from external coordination
                flush_cmd = async {
                    match &mut flush_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await, // Never resolves if no flush_rx
                    }
                } => {
                    match flush_cmd {
                        Some(FlushCommand::FlushUpTo(up_to_block)) => {
                            if let Err(err) = self.flush_buffered_blocks(up_to_block) {
                                warn!(error = %err, up_to_block, "failed to flush blocks on external command");
                            }
                            buffer_backpressured = !self.has_buffer_capacity();
                        }
                        Some(FlushCommand::FlushAll) => {
                            if let Err(err) = self.flush_all_buffered_blocks() {
                                warn!(error = %err, "failed to flush all blocks on external command");
                            }
                            buffer_backpressured = !self.has_buffer_capacity();
                        }
                        None => {
                            flush_rx = None;
                            warn!("flush coordination channel closed; waiting for restart to restore coordination");
                        }
                    }
                }

                // Handle periodic auto-flush when enabled
                _ = flush_interval.tick(), if self.auto_flush_enabled => {
                    if let Some((_, max_block)) = self.buffer_block_range() {
                        debug!(max_block, buffer_size = self.buffer.len(), "performing periodic auto-flush");
                        if let Err(err) = self.flush_buffered_blocks(max_block) {
                            warn!(error = %err, "failed to perform periodic auto-flush");
                        }
                        buffer_backpressured = !self.has_buffer_capacity();
                    }
                }

                // Main block processing logic
                result = AssertUnwindSafe(self.process_blocks(&mut next_block, &mut missing_block_retries)).catch_unwind(), if !buffer_backpressured => {
                    match result {
                        Ok(Ok(())) => {
                            buffer_backpressured = !self.has_buffer_capacity();
                        }
                        Ok(Err(err)) => {
                            if Self::is_buffer_capacity_error(&err) {
                                buffer_backpressured = true;
                                warn!(
                                    buffer_size = self.buffer.len(),
                                    max_buffer_size = self.max_buffer_size,
                                    "buffer reached capacity; pausing ingestion until buffered blocks are flushed"
                                );
                                continue;
                            }

                            buffer_backpressured = !self.has_buffer_capacity();

                            warn!(error = %err, "block processing failed, retrying");
                            tokio::select! {
                                _ = shutdown_rx.recv() => {
                                    info!("Shutdown signal received during retry sleep");
                                    return Ok(());
                                }
                                () = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)) => {}
                            }
                        }
                        Err(panic_payload) => {
                            std::panic::resume_unwind(panic_payload);
                        }
                    }
                }
            }
        }
    }

    /// Process blocks through catch-up and streaming phases
    async fn process_blocks(
        &mut self,
        next_block: &mut u64,
        missing_block_retries: &mut u32,
    ) -> Result<()> {
        // Catch up block-by-block first
        self.catch_up(next_block).await?;

        // Then stream new blocks
        match self.stream_blocks(next_block).await {
            Ok(()) => {
                *missing_block_retries = 0;
                Ok(())
            }
            Err(err) => {
                let err_str = err.to_string();
                let is_missing_block = err_str.contains("Missing block");

                if is_missing_block {
                    *missing_block_retries += 1;

                    if *missing_block_retries >= MAX_MISSING_BLOCK_RETRIES {
                        critical!(
                            error = %err,
                            retries = *missing_block_retries,
                            "failed to recover from missing block after multiple retries"
                        );
                    }
                } else {
                    // Reset retry counter for non-missing-block errors
                    *missing_block_retries = 0;
                }

                Err(err)
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

        let next_persisted_block = current.map_or(0, |block_number| block_number.saturating_add(1));
        let next_buffered_block = self
            .buffer_block_range()
            .map_or(next_persisted_block, |(_, max_block)| {
                max_block.saturating_add(1)
            });

        Ok(next_persisted_block.max(next_buffered_block))
    }

    /// Sequentially replay blocks until we reach the node's current head.
    async fn catch_up(&mut self, next_block: &mut u64) -> Result<()> {
        let head = self.provider.get_block_number().await?;
        Self::update_sync_metrics(*next_block, head);

        if *next_block > head {
            return Ok(());
        }

        while *next_block <= head {
            self.process_block(*next_block).await?;
            *next_block += 1;
            Self::update_sync_metrics(*next_block, head);

            // Allow other tasks to run and check for shutdown
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(&mut self, next_block: &mut u64) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        // Process one batch of headers then return to allow other tasks to run
        let mut processed_count = 0;
        const MAX_HEADERS_PER_BATCH: usize = 10;

        while processed_count < MAX_HEADERS_PER_BATCH {
            match stream.next().await {
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
                        self.process_block(*next_block).await?;
                        *next_block += 1;
                        Self::update_sync_metrics(*next_block, block_number);
                        processed_count += 1;
                    }
                }
                None => {
                    return Err(anyhow!("block subscription completed"));
                }
            }
        }

        // Continue processing in the next iteration
        Ok(())
    }

    /// Pull, trace, and buffer a single block update in memory.
    /// The block will not be persisted to MDBX until flush_buffered_blocks is called.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.process_genesis_block().await;
        }

        // Check buffer capacity before processing to avoid wasted work
        if self.buffer.len() >= self.max_buffer_size {
            let err = BufferCapacityError {
                capacity: self.max_buffer_size,
            };
            warn!(
                block_number,
                buffer_size = self.buffer.len(),
                max_buffer_size = self.max_buffer_size,
                error = %err,
                "buffer at capacity, cannot process block"
            );
            return Err(anyhow::Error::from(err));
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

        // Buffer the update instead of immediately committing
        self.buffer.insert(block_number, update);
        if let Err(err) = self.persist_buffer_state() {
            self.buffer.remove(&block_number);
            return Err(err).context(format!(
                "failed to persist buffered state after processing block {block_number}"
            ));
        }

        info!(
            block_number,
            buffer_size = self.buffer.len(),
            "block buffered in memory"
        );
        Ok(())
    }

    /// Flush buffered block updates to MDBX up to and including the specified block number.
    ///
    /// This method commits all buffered blocks with block_number <= up_to_block to the
    /// underlying MDBX database in sequential order. Successfully flushed blocks are
    /// removed from the buffer.
    ///
    /// Returns the number of blocks that were successfully flushed and the total
    /// commit statistics aggregated across all flushed blocks.
    ///
    /// # Arguments
    ///
    /// * `up_to_block` - The highest block number to flush (inclusive)
    ///
    /// # Errors
    ///
    /// Returns an error if any block commit fails. Blocks are committed in sequential
    /// order, so if block N fails, blocks N+1 and higher remain in the buffer even
    /// if they were scheduled to be flushed.
    pub fn flush_buffered_blocks(&mut self, up_to_block: u64) -> Result<(usize, CommitStats)> {
        let flush_result =
            flush_buffered_blocks_impl(&self.writer_reader, &mut self.buffer, up_to_block);

        match self.persist_buffer_state() {
            Ok(()) => flush_result,
            Err(err) => Err(err).context("failed to persist buffered state after flush"),
        }
    }

    /// Get the range of block numbers currently in the buffer.
    /// Returns None if buffer is empty, otherwise returns (min, max).
    fn buffer_block_range(&self) -> Option<(u64, u64)> {
        buffer_block_range(&self.buffer)
    }

    /// Check if the buffer has capacity for more blocks.
    fn has_buffer_capacity(&self) -> bool {
        self.buffer.len() < self.max_buffer_size
    }

    /// Apply EIP-2935 and EIP-4788 system call state changes
    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        if self.system_calls.cancun_time.is_none() && self.system_calls.prague_time.is_none() {
            return Ok(());
        }

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

    /// Process block 0 using the configured genesis state.
    /// Genesis blocks are committed immediately to ensure the database is properly initialized.
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

        // Genesis blocks are always committed immediately to ensure proper database initialization
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
    use alloy_provider::ProviderBuilder;
    use alloy_transport::mock::Asserter;
    use anyhow::{
        Context,
        anyhow,
    };
    use mdbx::{
        BlockStateUpdate,
        Reader,
        StateWriter,
        Writer,
        common::CircularBufferConfig,
    };
    use std::{
        collections::HashMap,
        path::PathBuf,
        sync::Mutex,
    };

    #[test]
    fn test_buffer_capacity_error_display() {
        let error = BufferCapacityError { capacity: 100 };
        let error_msg = error.to_string();
        assert!(error_msg.contains("Buffer is at capacity (100 blocks)"));
    }

    #[test]
    fn test_default_buffer_size() {
        assert_eq!(DEFAULT_MAX_BUFFER_SIZE, 100);
    }

    #[test]
    fn test_flush_command_variants() {
        let cmd1 = FlushCommand::FlushUpTo(42);
        let cmd2 = FlushCommand::FlushAll;

        // Test Debug formatting
        assert!(format!("{:?}", cmd1).contains("FlushUpTo(42)"));
        assert!(format!("{:?}", cmd2).contains("FlushAll"));

        // Test Clone
        let cmd1_clone = cmd1.clone();
        match (cmd1, cmd1_clone) {
            (FlushCommand::FlushUpTo(a), FlushCommand::FlushUpTo(b)) => assert_eq!(a, b),
            _ => panic!("Clone failed"),
        }
    }

    fn create_test_block_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate {
            block_number,
            block_hash: B256::repeat_byte((block_number % 256) as u8),
            state_root: B256::repeat_byte(0x42),
            accounts: vec![],
        }
    }

    #[test]
    fn test_buffer_operations_isolated() {
        // Test buffer range calculation logic independently
        let mut buffer = HashMap::new();
        assert!(buffer.is_empty());

        buffer.insert(1, create_test_block_update(1));
        buffer.insert(3, create_test_block_update(3));
        buffer.insert(2, create_test_block_update(2));

        // Test range calculation
        assert_eq!(buffer_block_range(&buffer), Some((1, 3)));
        assert_eq!(buffer.len(), 3);

        // Test capacity check
        assert!(buffer.len() < 10); // has capacity
        assert!(buffer.len() >= 3); // at limit
    }

    #[test]
    fn test_flush_block_selection() {
        let mut buffer = HashMap::new();
        buffer.insert(1, create_test_block_update(1));
        buffer.insert(2, create_test_block_update(2));
        buffer.insert(3, create_test_block_update(3));
        buffer.insert(5, create_test_block_update(5));

        // Test block selection for flush up to block 3
        let blocks_to_flush: Vec<u64> = buffer
            .keys()
            .copied()
            .filter(|&block_num| block_num <= 3)
            .collect();

        assert_eq!(blocks_to_flush.len(), 3);
        assert!(blocks_to_flush.contains(&1));
        assert!(blocks_to_flush.contains(&2));
        assert!(blocks_to_flush.contains(&3));
        assert!(!blocks_to_flush.contains(&5));

        // Test sorting
        let mut sorted_blocks = blocks_to_flush;
        sorted_blocks.sort_unstable();
        assert_eq!(sorted_blocks, vec![1, 2, 3]);
    }

    #[test]
    fn test_create_flush_coordinator_channels() {
        let (tx, mut rx) = StateWorker::<StateWriter>::create_flush_coordinator();

        // Test that we can send commands
        tx.send(FlushCommand::FlushUpTo(42))
            .expect("send should work");
        tx.send(FlushCommand::FlushAll).expect("send should work");

        // Test that we can receive commands
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let cmd1 = rx.recv().await.expect("should receive first command");
            let cmd2 = rx.recv().await.expect("should receive second command");

            match cmd1 {
                FlushCommand::FlushUpTo(n) => assert_eq!(n, 42),
                _ => panic!("Expected FlushUpTo(42)"),
            }

            match cmd2 {
                FlushCommand::FlushAll => {}
                _ => panic!("Expected FlushAll"),
            }
        });

        // Test channel closure
        drop(tx);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = rx.recv().await;
            assert!(result.is_none()); // Channel should be closed
        });
    }

    #[test]
    fn test_buffer_stats_aggregation() {
        // Test the commit stats aggregation logic independently
        let mut total = mdbx::CommitStats::default();
        let block_stats = mdbx::CommitStats {
            accounts_written: 1,
            accounts_deleted: 0,
            storage_slots_written: 2,
            storage_slots_deleted: 0,
            full_storage_deletes: 0,
            bytecodes_written: 0,
            diffs_applied: 1,
            diff_bytes: 100,
            largest_account_storage: 5,
            preprocess_duration: std::time::Duration::from_millis(10),
            diff_application_duration: std::time::Duration::from_millis(20),
            batch_write_duration: std::time::Duration::from_millis(30),
            commit_duration: std::time::Duration::from_millis(40),
            total_duration: std::time::Duration::from_millis(100),
        };

        // Simulate aggregation
        total.accounts_written += block_stats.accounts_written;
        total.storage_slots_written += block_stats.storage_slots_written;
        total.diffs_applied += block_stats.diffs_applied;
        total.diff_bytes += block_stats.diff_bytes;
        total.largest_account_storage = total
            .largest_account_storage
            .max(block_stats.largest_account_storage);
        total.total_duration += block_stats.total_duration;

        assert_eq!(total.accounts_written, 1);
        assert_eq!(total.storage_slots_written, 2);
        assert_eq!(total.diffs_applied, 1);
        assert_eq!(total.diff_bytes, 100);
        assert_eq!(total.largest_account_storage, 5);
        assert_eq!(total.total_duration, std::time::Duration::from_millis(100));
    }

    struct TestMdbxDir {
        path: PathBuf,
    }

    impl TestMdbxDir {
        fn new() -> anyhow::Result<Self> {
            let path = std::env::current_dir()?
                .join("target")
                .join("mdbx-test-dbs")
                .join(uuid::Uuid::new_v4().to_string());
            std::fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        fn path_str(&self) -> anyhow::Result<&str> {
            self.path
                .to_str()
                .context("test MDBX path should be valid UTF-8")
        }
    }

    impl Drop for TestMdbxDir {
        fn drop(&mut self) {
            if self.path.exists() {
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
    }

    fn create_test_writer() -> anyhow::Result<(TestMdbxDir, StateWriter)> {
        let test_dir = TestMdbxDir::new()?;
        let writer = StateWriter::new(test_dir.path_str()?, CircularBufferConfig::new(3)?)?;
        Ok((test_dir, writer))
    }

    fn create_buffer_state_path(test_dir: &TestMdbxDir) -> PathBuf {
        test_dir.path.join(BUFFER_STATE_FILE_NAME)
    }

    #[derive(Default)]
    struct TestTraceProvider {
        updates: Mutex<HashMap<u64, BlockStateUpdate>>,
    }

    impl TestTraceProvider {
        fn from_updates(updates: impl IntoIterator<Item = BlockStateUpdate>) -> Self {
            let updates = updates
                .into_iter()
                .map(|update| (update.block_number, update))
                .collect();
            Self {
                updates: Mutex::new(updates),
            }
        }
    }

    #[async_trait::async_trait]
    impl TraceProvider for TestTraceProvider {
        async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
            let mut updates = self
                .updates
                .lock()
                .map_err(|err| anyhow!("failed to lock test trace provider: {err}"))?;

            updates
                .remove(&block_number)
                .ok_or_else(|| anyhow!("missing test update for block {block_number}"))
        }
    }

    fn buffered_block_numbers(path: &std::path::Path) -> anyhow::Result<Vec<u64>> {
        let mut block_numbers: Vec<u64> = load_buffered_blocks(path)?.into_keys().collect();
        block_numbers.sort_unstable();
        Ok(block_numbers)
    }

    fn test_provider(asserter: Asserter) -> Arc<RootProvider> {
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        Arc::new(provider.root().clone())
    }

    fn push_head_response(asserter: &Asserter, head: u64) {
        asserter.push_success(&serde_json::json!(format!("0x{head:x}")));
    }

    #[test]
    fn test_flush_buffered_blocks_impl_commits_requested_range() -> anyhow::Result<()> {
        let (_test_dir, writer) = create_test_writer()?;
        let mut buffer = HashMap::from([
            (1, create_test_block_update(1)),
            (2, create_test_block_update(2)),
            (4, create_test_block_update(4)),
        ]);

        let (flushed_count, _stats) = flush_buffered_blocks_impl(&writer, &mut buffer, 2)?;

        assert_eq!(flushed_count, 2);
        assert_eq!(writer.latest_block_number()?, Some(2));
        assert_eq!(buffer_block_range(&buffer), Some((4, 4)));
        Ok(())
    }

    #[test]
    fn test_persist_buffered_blocks_round_trips_to_disk() -> anyhow::Result<()> {
        let (test_dir, _writer) = create_test_writer()?;
        let path = create_buffer_state_path(&test_dir);
        let buffer = HashMap::from([
            (2, create_test_block_update(2)),
            (3, create_test_block_update(3)),
        ]);

        persist_buffered_blocks(&path, &buffer)?;
        let loaded = load_buffered_blocks(&path)?;

        assert_eq!(buffer_block_range(&loaded), Some((2, 3)));
        assert_eq!(loaded.len(), 2);
        Ok(())
    }

    #[test]
    fn test_reconcile_buffered_blocks_impl_keeps_contiguous_unflushed_range() -> anyhow::Result<()>
    {
        let (_test_dir, writer) = create_test_writer()?;
        writer.commit_block(&create_test_block_update(1))?;

        let mut buffer = HashMap::from([
            (2, create_test_block_update(2)),
            (3, create_test_block_update(3)),
        ]);

        reconcile_buffered_blocks_impl(&writer, &mut buffer)?;

        assert_eq!(buffer_block_range(&buffer), Some((2, 3)));
        assert_eq!(writer.latest_block_number()?, Some(1));
        Ok(())
    }

    #[test]
    fn test_reconcile_buffered_blocks_impl_discards_non_contiguous_range() -> anyhow::Result<()> {
        let (_test_dir, writer) = create_test_writer()?;
        writer.commit_block(&create_test_block_update(1))?;

        let mut buffer = HashMap::from([(3, create_test_block_update(3))]);

        reconcile_buffered_blocks_impl(&writer, &mut buffer)?;

        assert!(buffer.is_empty());
        assert_eq!(writer.latest_block_number()?, Some(1));
        Ok(())
    }

    #[test]
    fn test_reconcile_buffered_blocks_impl_discards_already_persisted_blocks() -> anyhow::Result<()>
    {
        let (_test_dir, writer) = create_test_writer()?;
        writer.commit_block(&create_test_block_update(1))?;
        writer.commit_block(&create_test_block_update(2))?;

        let mut buffer = HashMap::from([
            (2, create_test_block_update(2)),
            (3, create_test_block_update(3)),
        ]);

        reconcile_buffered_blocks_impl(&writer, &mut buffer)?;

        assert_eq!(buffer_block_range(&buffer), Some((3, 3)));
        Ok(())
    }

    #[test]
    fn test_restart_recovery_reloads_unflushed_blocks_until_explicit_flush() -> anyhow::Result<()> {
        let (test_dir, writer) = create_test_writer()?;
        writer.commit_block(&create_test_block_update(1))?;

        let path = create_buffer_state_path(&test_dir);
        let buffer = HashMap::from([
            (2, create_test_block_update(2)),
            (3, create_test_block_update(3)),
        ]);
        persist_buffered_blocks(&path, &buffer)?;

        let mut recovered_buffer = load_buffered_blocks(&path)?;
        reconcile_buffered_blocks_impl(&writer, &mut recovered_buffer)?;

        assert_eq!(writer.latest_block_number()?, Some(1));
        assert_eq!(buffer_block_range(&recovered_buffer), Some((2, 3)));

        let (flushed_count, _stats) =
            flush_buffered_blocks_impl(&writer, &mut recovered_buffer, 3)?;
        assert_eq!(flushed_count, 2);
        assert!(recovered_buffer.is_empty());

        persist_buffered_blocks(&path, &recovered_buffer)?;
        assert!(!path.exists());
        assert_eq!(writer.latest_block_number()?, Some(3));
        Ok(())
    }

    #[tokio::test]
    async fn test_process_block_buffers_until_flush() -> anyhow::Result<()> {
        let (test_dir, writer) = create_test_writer()?;
        let path = create_buffer_state_path(&test_dir);
        let asserter = Asserter::new();
        let provider = test_provider(asserter);
        let trace_provider = Box::new(TestTraceProvider::from_updates([create_test_block_update(
            1,
        )]));
        let (_flush_tx, flush_rx) = StateWorker::<StateWriter>::create_flush_coordinator();
        let mut worker = StateWorker::with_flush_coordination(
            provider,
            trace_provider,
            writer,
            None,
            SystemCalls::default(),
            2,
            flush_rx,
            path.clone(),
        );

        worker.process_block(1).await?;

        assert_eq!(worker.writer_reader.latest_block_number()?, None);
        assert_eq!(buffer_block_range(&worker.buffer), Some((1, 1)));
        assert_eq!(buffered_block_numbers(&path)?, vec![1]);

        let (flushed_count, _stats) = worker.flush_buffered_blocks(1)?;

        assert_eq!(flushed_count, 1);
        assert_eq!(worker.writer_reader.latest_block_number()?, Some(1));
        assert!(worker.buffer.is_empty());
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_backpressure_requires_flush_before_catch_up_can_resume() -> anyhow::Result<()> {
        let (test_dir, writer) = create_test_writer()?;
        let path = create_buffer_state_path(&test_dir);
        let asserter = Asserter::new();
        push_head_response(&asserter, 2);
        push_head_response(&asserter, 2);
        let provider = test_provider(asserter);
        let trace_provider = Box::new(TestTraceProvider::from_updates([
            create_test_block_update(1),
            create_test_block_update(2),
        ]));
        let (_flush_tx, flush_rx) = StateWorker::<StateWriter>::create_flush_coordinator();
        let mut worker = StateWorker::with_flush_coordination(
            provider,
            trace_provider,
            writer,
            None,
            SystemCalls::default(),
            1,
            flush_rx,
            path.clone(),
        );
        let mut next_block = 1;

        let err = worker
            .catch_up(&mut next_block)
            .await
            .expect_err("buffer should backpressure");
        assert!(StateWorker::<StateWriter>::is_buffer_capacity_error(&err));
        assert_eq!(next_block, 2);
        assert_eq!(worker.writer_reader.latest_block_number()?, None);
        assert_eq!(buffered_block_numbers(&path)?, vec![1]);

        worker.flush_buffered_blocks(1)?;
        worker.catch_up(&mut next_block).await?;

        assert_eq!(next_block, 3);
        assert_eq!(buffered_block_numbers(&path)?, vec![2]);

        worker.flush_buffered_blocks(2)?;

        assert_eq!(worker.writer_reader.latest_block_number()?, Some(2));
        assert!(worker.buffer.is_empty());
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_restart_reloads_checkpointed_blocks_before_flush() -> anyhow::Result<()> {
        let test_dir = TestMdbxDir::new()?;
        let path = create_buffer_state_path(&test_dir);
        let initial_provider = test_provider(Asserter::new());
        let initial_trace_provider =
            Box::new(TestTraceProvider::from_updates([create_test_block_update(
                1,
            )]));
        let initial_writer = StateWriter::new(test_dir.path_str()?, CircularBufferConfig::new(3)?)?;
        initial_writer.commit_block(&create_test_block_update(0))?;
        let (_initial_flush_tx, initial_flush_rx) =
            StateWorker::<StateWriter>::create_flush_coordinator();
        let mut initial_worker = StateWorker::with_flush_coordination(
            initial_provider,
            initial_trace_provider,
            initial_writer,
            None,
            SystemCalls::default(),
            2,
            initial_flush_rx,
            path.clone(),
        );

        initial_worker.process_block(1).await?;
        assert_eq!(buffered_block_numbers(&path)?, vec![1]);
        drop(initial_worker);

        let restarted_writer =
            StateWriter::new(test_dir.path_str()?, CircularBufferConfig::new(3)?)?;
        let restarted_provider = test_provider(Asserter::new());
        let restarted_trace_provider = Box::new(TestTraceProvider::default());
        let (_restarted_flush_tx, restarted_flush_rx) =
            StateWorker::<StateWriter>::create_flush_coordinator();
        let mut restarted_worker = StateWorker::with_flush_coordination(
            restarted_provider,
            restarted_trace_provider,
            restarted_writer,
            None,
            SystemCalls::default(),
            2,
            restarted_flush_rx,
            path.clone(),
        );

        restarted_worker.recover_buffer_state()?;

        assert_eq!(buffer_block_range(&restarted_worker.buffer), Some((1, 1)));
        assert_eq!(restarted_worker.compute_start_block(None)?, 2);
        assert_eq!(
            restarted_worker.writer_reader.latest_block_number()?,
            Some(0)
        );

        restarted_worker.flush_buffered_blocks(1)?;

        assert_eq!(
            restarted_worker.writer_reader.latest_block_number()?,
            Some(1)
        );
        assert!(restarted_worker.buffer.is_empty());
        assert!(!path.exists());
        Ok(())
    }
}
