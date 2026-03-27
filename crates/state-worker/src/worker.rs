//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer. When integrated into the sidecar, traced
//! updates are buffered in memory and only flushed to MDBX after the engine has
//! committed the corresponding block.
use crate::{
    coordination::FlushControl,
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
    sync::Arc,
    time::Duration,
};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_MISSING_BLOCK_RETRIES: u32 = 3;

#[derive(Debug)]
struct BufferedCommitState {
    flush_control: Option<Arc<FlushControl>>,
    buffer_capacity: usize,
    buffered_updates: VecDeque<BlockStateUpdate>,
}

impl BufferedCommitState {
    fn new(flush_control: Option<Arc<FlushControl>>, buffer_capacity: usize) -> Self {
        Self {
            flush_control,
            buffer_capacity,
            buffered_updates: VecDeque::new(),
        }
    }

    fn should_listen_for_flush(&self) -> bool {
        self.flush_control.is_some() && !self.buffered_updates.is_empty()
    }

    async fn wait_for_update(&self) {
        if let Some(control) = self.flush_control.as_ref() {
            control.wait_for_update().await;
        }
    }

    fn sync_committed_head<WR>(&self, writer_reader: &WR) -> Result<()>
    where
        WR: Reader,
        <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
    {
        if let Some(control) = self.flush_control.as_ref()
            && let Some(block_number) = writer_reader.latest_block_number().map_err(|err| {
                metrics::set_db_healthy(false);
                anyhow!(err)
            })?
        {
            metrics::set_db_healthy(true);
            control.record_committed_block(block_number);
        }
        Ok(())
    }

    fn enqueue_or_commit<WR>(&mut self, writer_reader: &WR, update: BlockStateUpdate) -> Result<()>
    where
        WR: Writer + Reader,
        <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
        <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
    {
        if self.flush_control.is_none() {
            self.persist_update(writer_reader, &update)?;
            return Ok(());
        }

        self.buffered_updates.push_back(update);
        self.flush_ready_updates(writer_reader)
    }

    fn flush_ready_updates<WR>(&mut self, writer_reader: &WR) -> Result<()>
    where
        WR: Writer + Reader,
        <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
        <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
    {
        let Some(control) = self.flush_control.as_ref() else {
            return Ok(());
        };
        let Some(permitted_block) = control.permitted_flush_block() else {
            return Ok(());
        };

        while self
            .buffered_updates
            .front()
            .is_some_and(|update| update.block_number <= permitted_block)
        {
            let Some(update) = self.buffered_updates.pop_front() else {
                break;
            };
            self.persist_update(writer_reader, &update)?;
        }

        Ok(())
    }

    async fn wait_for_capacity<WR>(
        &mut self,
        writer_reader: &WR,
        shutdown: &CancellationToken,
    ) -> Result<bool>
    where
        WR: Writer + Reader,
        <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
        <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
    {
        while self.buffered_updates.len() >= self.buffer_capacity {
            let Some(control) = self.flush_control.clone() else {
                return Err(anyhow!(
                    "state worker buffer reached capacity without flush control"
                ));
            };

            let notified = async move { control.wait_for_update().await };
            tokio::pin!(notified);

            self.flush_ready_updates(writer_reader)?;
            if self.buffered_updates.len() < self.buffer_capacity {
                return Ok(false);
            }

            tokio::select! {
                () = shutdown.cancelled() => return Ok(true),
                () = &mut notified => {}
            }
        }

        Ok(false)
    }

    fn persist_update<WR>(&self, writer_reader: &WR, update: &BlockStateUpdate) -> Result<()>
    where
        WR: Writer + Reader,
        <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
        <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
    {
        match writer_reader.commit_block(update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(update.block_number, &stats);
            }
            Err(err) => {
                critical!(
                    error = ?err,
                    block_number = update.block_number,
                    "failed to persist block"
                );
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                return Err(anyhow!("failed to persist block {}", update.block_number));
            }
        }

        if let Some(control) = self.flush_control.as_ref() {
            control.record_committed_block(update.block_number);
        }

        info!(
            block_number = update.block_number,
            "block persisted to database"
        );
        Ok(())
    }
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
    buffered_commits: BufferedCommitState,
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
        flush_control: Option<Arc<FlushControl>>,
        buffer_capacity: usize,
    ) -> Self {
        let buffered_commits = BufferedCommitState::new(flush_control, buffer_capacity);
        let _ = buffered_commits.sync_committed_head(&writer_reader);

        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            buffered_commits,
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

    /// Run the worker until shutdown, retrying subscriptions as needed.
    ///
    /// # Errors
    ///
    /// Returns an error when RPC access, tracing, or MDBX persistence fails and
    /// the worker cannot continue the current run loop iteration.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;

        loop {
            if shutdown.is_cancelled() {
                info!("Shutdown signal received");
                return Ok(());
            }

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
                        missing_block_retries = 0;
                    }

                    warn!(error = %err, "block subscription ended, retrying");
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            info!("Shutdown signal received during retry sleep");
                            return Ok(());
                        }
                        () = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)) => {}
                    }
                }
            }
        }
    }

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

    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown: &CancellationToken,
    ) -> Result<bool> {
        loop {
            let head = self.provider.get_block_number().await?;
            Self::update_sync_metrics(*next_block, head);

            if *next_block > head {
                self.buffered_commits
                    .flush_ready_updates(&self.writer_reader)?;
                return Ok(false);
            }

            while *next_block <= head {
                if self
                    .buffered_commits
                    .wait_for_capacity(&self.writer_reader, shutdown)
                    .await?
                {
                    return Ok(true);
                }

                self.process_block(*next_block).await?;
                *next_block += 1;
                Self::update_sync_metrics(*next_block, head);

                if shutdown.is_cancelled() {
                    return Ok(true);
                }
            }
        }
    }

    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    info!("Shutdown signal received during block streaming");
                    return Ok(());
                }
                // `persist_update` also records the committed head, so this can wake on our own
                // writes. That is intentional: spurious wakes are harmless, but missed flush
                // permits would leave ready blocks buffered until another event arrives.
                () = self.buffered_commits.wait_for_update(), if self.buffered_commits.should_listen_for_flush() => {
                    self.buffered_commits.flush_ready_updates(&self.writer_reader)?;
                }
                maybe_header = stream.next() => {
                    match maybe_header {
                        Some(header) => {
                            let Header { hash: _, inner, .. } = header;
                            let block_number = inner.number;

                            if block_number + 1 < *next_block {
                                debug!(block_number, next_block, "skipping stale header");
                                continue;
                            }

                            Self::update_sync_metrics(*next_block, block_number);

                            if block_number > *next_block {
                                warn!("Missing block {block_number} (next block: {next_block})");
                                return Err(anyhow!(
                                    "Missing block {block_number} (next block: {next_block})"
                                ));
                            }

                            while *next_block <= block_number {
                                if self
                                    .buffered_commits
                                    .wait_for_capacity(&self.writer_reader, shutdown)
                                    .await?
                                {
                                    return Ok(());
                                }

                                self.process_block(*next_block).await?;
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

        self.apply_system_calls(&mut update, block_number).await?;
        self.buffered_commits
            .enqueue_or_commit(&self.writer_reader, update)?;
        Ok(())
    }

    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await?
            .context(format!("block {block_number} not found"))?;

        let parent_block_hash = if block_number > 0 {
            Some(block.header.parent_hash)
        } else {
            None
        };
        let parent_beacon_block_root = block.header.parent_beacon_block_root;

        let config = SystemCallConfig {
            block_number,
            timestamp: block.header.timestamp,
            parent_block_hash,
            parent_beacon_block_root,
        };

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
                warn!(
                    block_number,
                    error = %err,
                    "failed to compute system call states, traces may already include them"
                );
                Ok(())
            }
        }
    }

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
            self.buffered_commits
                .sync_committed_head(&self.writer_reader)?;
            return Ok(());
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(());
        }

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

        self.buffered_commits
            .persist_update(&self.writer_reader, &update)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedCommitState;
    use crate::coordination::FlushControl;
    use alloy::primitives::{
        B256,
        Bytes,
        U256,
    };
    use mdbx::{
        AccountInfo,
        AccountState,
        AddressHash,
        BlockMetadata,
        BlockStateUpdate,
        CommitStats,
        Reader,
        Writer,
    };
    use std::{
        collections::HashMap,
        sync::{
            Mutex,
            MutexGuard,
            PoisonError,
        },
        time::Duration,
    };
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Default)]
    struct FakeWriterReader {
        committed_blocks: Mutex<Vec<u64>>,
    }

    impl FakeWriterReader {
        fn committed_blocks(&self) -> MutexGuard<'_, Vec<u64>> {
            self.committed_blocks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        }
    }

    #[derive(Debug)]
    struct FakeError;

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("fake error")
        }
    }

    impl std::error::Error for FakeError {}

    impl Reader for FakeWriterReader {
        type Error = FakeError;

        fn latest_block_number(&self) -> std::result::Result<Option<u64>, Self::Error> {
            Ok(self.committed_blocks().last().copied())
        }

        fn is_block_available(&self, _block_number: u64) -> std::result::Result<bool, Self::Error> {
            Ok(false)
        }

        fn get_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<Option<AccountInfo>, Self::Error> {
            Ok(None)
        }

        fn get_storage(
            &self,
            _address_hash: AddressHash,
            _slot_hash: B256,
            _block_number: u64,
        ) -> std::result::Result<Option<U256>, Self::Error> {
            Ok(None)
        }

        fn get_all_storage(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<HashMap<B256, U256>, Self::Error> {
            Ok(HashMap::new())
        }

        fn get_code(
            &self,
            _code_hash: B256,
            _block_number: u64,
        ) -> std::result::Result<Option<Bytes>, Self::Error> {
            Ok(None)
        }

        fn get_full_account(
            &self,
            _address_hash: AddressHash,
            _block_number: u64,
        ) -> std::result::Result<Option<AccountState>, Self::Error> {
            Ok(None)
        }

        fn get_block_hash(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<B256>, Self::Error> {
            Ok(None)
        }

        fn get_state_root(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<B256>, Self::Error> {
            Ok(None)
        }

        fn get_block_metadata(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Option<BlockMetadata>, Self::Error> {
            Ok(None)
        }

        fn get_available_block_range(
            &self,
        ) -> std::result::Result<Option<(u64, u64)>, Self::Error> {
            Ok(None)
        }

        fn scan_account_hashes(
            &self,
            _block_number: u64,
        ) -> std::result::Result<Vec<AddressHash>, Self::Error> {
            Ok(Vec::new())
        }
    }

    impl Writer for FakeWriterReader {
        type Error = FakeError;

        fn commit_block(
            &self,
            update: &BlockStateUpdate,
        ) -> std::result::Result<CommitStats, Self::Error> {
            self.committed_blocks().push(update.block_number);
            Ok(CommitStats::default())
        }

        fn bootstrap_from_snapshot(
            &self,
            _accounts: Vec<AccountState>,
            _block_number: u64,
            _block_hash: B256,
            _state_root: B256,
        ) -> std::result::Result<CommitStats, Self::Error> {
            Ok(CommitStats::default())
        }
    }

    fn update(block_number: u64) -> BlockStateUpdate {
        let block_hash_byte = u8::try_from(block_number).unwrap_or(u8::MAX);
        BlockStateUpdate {
            block_number,
            block_hash: B256::repeat_byte(block_hash_byte),
            state_root: B256::repeat_byte(block_hash_byte.saturating_add(1)),
            accounts: Vec::new(),
        }
    }

    #[test]
    fn immediate_mode_commits_without_flush_control() -> anyhow::Result<()> {
        let writer_reader = FakeWriterReader::default();
        let mut buffered = BufferedCommitState::new(None, 1);

        buffered.enqueue_or_commit(&writer_reader, update(1))?;

        assert_eq!(*writer_reader.committed_blocks(), vec![1]);
        Ok(())
    }

    #[test]
    fn buffered_mode_commits_only_after_permission() -> anyhow::Result<()> {
        let writer_reader = FakeWriterReader::default();
        let control = FlushControl::new();
        let mut buffered = BufferedCommitState::new(Some(control.clone()), 4);

        buffered.enqueue_or_commit(&writer_reader, update(1))?;
        buffered.enqueue_or_commit(&writer_reader, update(2))?;

        assert!(writer_reader.committed_blocks().is_empty());

        control.allow_flush_to(1);
        buffered.flush_ready_updates(&writer_reader)?;
        assert_eq!(control.committed_block(), Some(1));
        assert_eq!(*writer_reader.committed_blocks(), vec![1]);

        control.allow_flush_to(2);
        buffered.flush_ready_updates(&writer_reader)?;
        assert_eq!(control.committed_block(), Some(2));
        assert_eq!(*writer_reader.committed_blocks(), vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn waits_for_capacity_until_permission_advances() -> anyhow::Result<()> {
        let writer_reader = FakeWriterReader::default();
        let control = FlushControl::new();
        let mut buffered = BufferedCommitState::new(Some(control.clone()), 1);
        let shutdown = CancellationToken::new();

        buffered.enqueue_or_commit(&writer_reader, update(1))?;
        assert!(buffered.should_listen_for_flush());

        let control_clone = control.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            control_clone.allow_flush_to(1);
        });

        let shutdown_received = buffered
            .wait_for_capacity(&writer_reader, &shutdown)
            .await?;
        assert!(!shutdown_received);
        assert_eq!(control.committed_block(), Some(1));
        assert_eq!(*writer_reader.committed_blocks(), vec![1]);
        Ok(())
    }
}
