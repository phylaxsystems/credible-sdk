//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and the resulting `BlockStateUpdate` is
//! committed to the database.
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
    sync::Arc,
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
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(
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
    /// Each block is traced via `process_block` and then committed to the
    /// database immediately — preserving the standalone binary semantics.
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
                // Trace the block — commit the returned update immediately.
                let update = self.process_block(*next_block).await?;
                self.commit_update(*next_block, &update)?;
                *next_block += 1;
                Self::update_sync_metrics(*next_block, head);

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
                                // Trace the block — commit the returned update immediately.
                                let update = self.process_block(*next_block).await?;
                                self.commit_update(*next_block, &update)?;
                                *next_block += 1;
                                Self::update_sync_metrics(*next_block, block_number);
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

    /// Commit a `BlockStateUpdate` produced by `process_block` to MDBX.
    ///
    /// This is the only place `commit_block` is called in the standalone worker;
    /// the sidecar buffer calls it separately after receiving a flush signal.
    ///
    /// Returns immediately (no-op) for sentinel updates returned by
    /// `process_genesis_block` when genesis is already committed or empty.
    fn commit_update(&self, block_number: u64, update: &BlockStateUpdate) -> Result<()> {
        // Sentinel: genesis was skipped (block 0 already committed or no accounts to write).
        // `process_genesis_block` returns B256::ZERO hashes when skipping — never commit that.
        if block_number == 0
            && update.block_hash == alloy::primitives::B256::ZERO
            && update.accounts.is_empty()
        {
            return Ok(());
        }

        match self.writer_reader.commit_block(update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);
                info!(block_number, "block persisted to database");
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

    /// Trace a single block and return the resulting `BlockStateUpdate`.
    ///
    /// Does NOT write to the database. The caller (`run()` for the standalone
    /// binary, or the sidecar buffer flush for the embedded mode) is responsible
    /// for calling `commit_block`.
    ///
    /// # Errors
    ///
    /// Returns an error if tracing or system-call application fails.
    pub async fn process_block(&mut self, block_number: u64) -> Result<BlockStateUpdate> {
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

        Ok(update)
    }

    /// Apply EIP-2935 and EIP-4788 system call state changes
    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        // Short-circuit: if no forks are configured, no system calls can be active.
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
    ///
    /// Returns the `BlockStateUpdate` for the genesis block (block 0). The caller
    /// is responsible for committing it.
    async fn process_genesis_block(&mut self) -> Result<BlockStateUpdate> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            // Return an empty update for block 0 when no genesis is configured.
            return Ok(BlockStateUpdate::new(
                0,
                alloy::primitives::B256::ZERO,
                alloy::primitives::B256::ZERO,
            ));
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
            // Return an empty update; the caller's commit_update call will be a no-op
            // since the block is already committed.
            return Ok(BlockStateUpdate::new(
                0,
                alloy::primitives::B256::ZERO,
                alloy::primitives::B256::ZERO,
            ));
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(BlockStateUpdate::new(
                0,
                alloy::primitives::B256::ZERO,
                alloy::primitives::B256::ZERO,
            ));
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

        Ok(update)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use anyhow::Result;
    use async_trait::async_trait;
    use mdbx::{
        AccountInfo,
        AccountState,
        AddressHash,
        BlockMetadata,
        CommitStats,
        Writer,
    };
    use std::{
        collections::HashMap,
        fmt,
        sync::atomic::{
            AtomicBool,
            Ordering,
        },
    };

    /// Mock `TraceProvider` that returns a preset `BlockStateUpdate`.
    struct MockTraceProvider {
        block_hash: B256,
        state_root: B256,
    }

    impl MockTraceProvider {
        fn new() -> Self {
            Self {
                block_hash: B256::from([1u8; 32]),
                state_root: B256::from([2u8; 32]),
            }
        }
    }

    #[async_trait]
    impl TraceProvider for MockTraceProvider {
        async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
            Ok(BlockStateUpdate::new(
                block_number,
                self.block_hash,
                self.state_root,
            ))
        }
    }

    /// Minimal error type for mock writer/reader.
    #[derive(Debug)]
    struct NoopError;

    impl fmt::Display for NoopError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("noop error")
        }
    }

    impl std::error::Error for NoopError {}

    /// No-op writer/reader that satisfies the trait bounds without touching MDBX.
    #[derive(Clone)]
    struct NoopWriterReader;

    impl Writer for NoopWriterReader {
        type Error = NoopError;

        fn commit_block(&self, _update: &BlockStateUpdate) -> Result<CommitStats, Self::Error> {
            Ok(CommitStats::default())
        }

        fn bootstrap_from_snapshot(
            &self,
            _accounts: Vec<AccountState>,
            _block_number: u64,
            _block_hash: B256,
            _state_root: B256,
        ) -> Result<CommitStats, Self::Error> {
            Ok(CommitStats::default())
        }
    }

    impl Reader for NoopWriterReader {
        type Error = NoopError;

        fn latest_block_number(&self) -> Result<Option<u64>, Self::Error> {
            Ok(Some(0))
        }

        fn is_block_available(&self, _block_number: u64) -> Result<bool, Self::Error> {
            Ok(true)
        }

        fn get_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(None)
        }

        fn get_storage(
            &self,
            _address_hash: AddressHash,
            _slot_hash: B256,
            _block_number: u64,
        ) -> Result<Option<alloy::primitives::U256>, Self::Error> {
            Ok(None)
        }

        fn get_all_storage(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> Result<HashMap<B256, alloy::primitives::U256>, Self::Error> {
            Ok(HashMap::new())
        }

        fn get_code(
            &self,
            _code_hash: B256,
            _block_number: u64,
        ) -> Result<Option<alloy::primitives::Bytes>, Self::Error> {
            Ok(None)
        }

        fn get_full_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> Result<Option<AccountState>, Self::Error> {
            Ok(None)
        }

        fn get_block_hash(&self, _block_number: u64) -> Result<Option<B256>, Self::Error> {
            Ok(None)
        }

        fn get_state_root(&self, _block_number: u64) -> Result<Option<B256>, Self::Error> {
            Ok(None)
        }

        fn get_block_metadata(
            &self,
            _block_number: u64,
        ) -> Result<Option<BlockMetadata>, Self::Error> {
            Ok(None)
        }

        fn get_available_block_range(&self) -> Result<Option<(u64, u64)>, Self::Error> {
            Ok(None)
        }

        fn scan_account_hashes(
            &self,
            _block_number: u64,
        ) -> Result<Vec<AddressHash>, Self::Error> {
            Ok(vec![])
        }
    }

    /// Build a `StateWorker` with mock provider and no-op writer/reader.
    ///
    /// The `Arc<RootProvider>` is not called during `process_block` for
    /// non-genesis, non-system-call blocks. We use a dummy URL — the worker
    /// is never `run()`.
    fn make_worker() -> StateWorker<NoopWriterReader> {
        use alloy_provider::RootProvider;
        // Build a no-op HTTP provider — it is never called in unit tests
        // because apply_system_calls() only warns on failure, it does not abort.
        let provider = Arc::new(RootProvider::new_http(
            "http://localhost:0".parse().expect("valid URL"),
        ));
        let system_calls = SystemCalls::new(None, None);

        StateWorker::new(
            provider,
            Box::new(MockTraceProvider::new()),
            NoopWriterReader,
            None, // no genesis
            system_calls,
        )
    }

    /// `process_block` must return a `BlockStateUpdate` with the correct block number.
    ///
    /// Verifies that the refactored signature (`Result<BlockStateUpdate>`)
    /// works correctly and the returned update carries the expected block_number.
    #[tokio::test]
    async fn process_block_returns_block_state_update_with_correct_block_number() -> Result<()> {
        let mut worker = make_worker();
        let block_number = 42u64;

        let update = worker.process_block(block_number).await?;

        assert_eq!(update.block_number, block_number, "block_number must match");
        Ok(())
    }

    /// `process_block` must NOT call `commit_block` — the update is returned raw.
    ///
    /// Uses an `AtomicBool` flag to detect whether `commit_block` was invoked
    /// inside `process_block`. If the refactoring is correct, the flag remains false.
    #[tokio::test]
    async fn process_block_does_not_commit_directly() -> Result<()> {
        /// A writer that records whether `commit_block` was called.
        struct CommitTrackingWriter {
            committed: Arc<AtomicBool>,
        }

        impl Writer for CommitTrackingWriter {
            type Error = NoopError;

            fn commit_block(
                &self,
                _update: &BlockStateUpdate,
            ) -> Result<CommitStats, Self::Error> {
                self.committed.store(true, Ordering::SeqCst);
                Ok(CommitStats::default())
            }

            fn bootstrap_from_snapshot(
                &self,
                _accounts: Vec<AccountState>,
                _block_number: u64,
                _block_hash: B256,
                _state_root: B256,
            ) -> Result<CommitStats, Self::Error> {
                Ok(CommitStats::default())
            }
        }

        impl Reader for CommitTrackingWriter {
            type Error = NoopError;

            fn latest_block_number(&self) -> Result<Option<u64>, Self::Error> {
                Ok(None)
            }

            fn is_block_available(&self, _: u64) -> Result<bool, Self::Error> {
                Ok(false)
            }

            fn get_account(
                &self,
                _: AddressHash,
                _: u64,
            ) -> Result<Option<AccountInfo>, Self::Error> {
                Ok(None)
            }

            fn get_storage(
                &self,
                _: AddressHash,
                _: B256,
                _: u64,
            ) -> Result<Option<alloy::primitives::U256>, Self::Error> {
                Ok(None)
            }

            fn get_all_storage(
                &self,
                _: AddressHash,
                _: u64,
            ) -> Result<HashMap<B256, alloy::primitives::U256>, Self::Error> {
                Ok(HashMap::new())
            }

            fn get_code(
                &self,
                _: B256,
                _: u64,
            ) -> Result<Option<alloy::primitives::Bytes>, Self::Error> {
                Ok(None)
            }

            fn get_full_account(
                &self,
                _: AddressHash,
                _: u64,
            ) -> Result<Option<AccountState>, Self::Error> {
                Ok(None)
            }

            fn get_block_hash(&self, _: u64) -> Result<Option<B256>, Self::Error> {
                Ok(None)
            }

            fn get_state_root(&self, _: u64) -> Result<Option<B256>, Self::Error> {
                Ok(None)
            }

            fn get_block_metadata(&self, _: u64) -> Result<Option<BlockMetadata>, Self::Error> {
                Ok(None)
            }

            fn get_available_block_range(&self) -> Result<Option<(u64, u64)>, Self::Error> {
                Ok(None)
            }

            fn scan_account_hashes(&self, _: u64) -> Result<Vec<AddressHash>, Self::Error> {
                Ok(vec![])
            }
        }

        use alloy_provider::RootProvider;
        let provider = Arc::new(RootProvider::new_http(
            "http://localhost:0".parse().expect("valid URL"),
        ));
        let system_calls = SystemCalls::new(None, None);
        let committed_flag = Arc::new(AtomicBool::new(false));
        let tracking_writer = CommitTrackingWriter {
            committed: Arc::clone(&committed_flag),
        };

        let mut worker = StateWorker::new(
            provider,
            Box::new(MockTraceProvider::new()),
            tracking_writer,
            None,
            system_calls,
        );

        let update = worker.process_block(5).await?;
        assert_eq!(update.block_number, 5);
        // commit_block must NOT have been called inside process_block
        assert!(
            !committed_flag.load(Ordering::SeqCst),
            "process_block must not call commit_block"
        );

        Ok(())
    }
}
