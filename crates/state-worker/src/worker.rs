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
use futures_util::StreamExt;
use mdbx::{
    BlockStateUpdate,
    Reader,
    Writer,
};
use std::{
    collections::{
        BTreeMap,
        VecDeque,
    },
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
#[cfg(test)]
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 100;

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
    commit_head_signal_rx: Option<crate::CommitHeadSignalReceiver>,
    commit_target: Option<u64>,
    staged_updates: VecDeque<BlockStateUpdate>,
    available_observed_head: Arc<AtomicU64>,
}

/// Standalone runtime that stages updates locally and self-advances the commit
/// target so the binary keeps flushing without an external coordinator.
pub struct StandaloneStateWorkerRuntime<WR>
where
    WR: Writer + Reader,
{
    worker: StateWorker<WR>,
    staged_updates: BTreeMap<u64, BlockStateUpdate>,
    commit_target: Option<u64>,
    auto_advance_commit_target: bool,
    commit_head_signal_rx: Option<crate::CommitHeadSignalReceiver>,
}

impl<WR> StandaloneStateWorkerRuntime<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(worker: StateWorker<WR>) -> Self {
        Self {
            worker,
            staged_updates: BTreeMap::new(),
            commit_target: None,
            auto_advance_commit_target: false,
            commit_head_signal_rx: None,
        }
    }

    pub fn with_auto_advance_commit_target(mut self) -> Self {
        self.auto_advance_commit_target = true;
        self
    }

    #[allow(dead_code)]
    pub fn with_commit_head_signal_receiver(
        mut self,
        commit_head_signal_rx: crate::CommitHeadSignalReceiver,
    ) -> Self {
        self.commit_head_signal_rx = Some(commit_head_signal_rx);
        self
    }

    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut next_block = self.worker.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;

        loop {
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received");
                return Ok(());
            }

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

    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            let head = self.worker.provider_head().await?;
            self.observe_head(head)?;

            if *next_block > head {
                self.flush_ready_blocks()?;
                return Ok(false);
            }

            while *next_block <= head {
                self.prepare_and_stage_block(*next_block).await?;
                self.flush_ready_blocks()?;
                *next_block += 1;
                self.observe_head(head)?;

                if shutdown_rx.try_recv().is_ok() {
                    return Ok(true);
                }
            }
        }
    }

    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        let subscription = self.worker.provider().subscribe_blocks().await?;
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

                            if block_number + 1 < *next_block {
                                debug!(block_number, next_block, "skipping stale header");
                                continue;
                            }

                            self.observe_head(block_number)?;

                            if block_number > *next_block {
                                warn!("Missing block {block_number} (next block: {next_block})");
                                return Err(anyhow!(
                                    "Missing block {block_number} (next block: {next_block})"
                                ));
                            }

                            while *next_block <= block_number {
                                self.prepare_and_stage_block(*next_block).await?;
                                self.flush_ready_blocks()?;
                                *next_block += 1;
                                self.observe_head(block_number)?;
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

    async fn prepare_and_stage_block(&mut self, block_number: u64) -> Result<()> {
        let Some(update) = self.worker.prepare_block_update(block_number).await? else {
            return Ok(());
        };

        self.staged_updates.insert(update.block_number, update);

        if self.auto_advance_commit_target {
            self.advance_commit_target(block_number);
        }

        Ok(())
    }

    fn advance_commit_target(&mut self, block_number: u64) {
        let next_target = self
            .commit_target
            .map_or(block_number, |current| current.max(block_number));
        self.commit_target = Some(next_target);
    }

    fn observe_commit_target(&mut self) {
        let next_commit_target =
            self.commit_head_signal_rx
                .as_mut()
                .and_then(|commit_head_signal_rx| {
                    commit_head_signal_rx.borrow_and_update().as_ref().copied()
                });

        if let Some(commit_target) = next_commit_target {
            self.advance_commit_target(commit_target);
        }
    }

    fn flush_ready_blocks(&mut self) -> Result<()> {
        self.observe_commit_target();

        let Some(commit_target) = self.commit_target else {
            self.refresh_sync_metrics(None)?;
            return Ok(());
        };

        while let Some((&block_number, _)) = self.staged_updates.first_key_value() {
            if block_number > commit_target {
                break;
            }

            let update = self
                .staged_updates
                .remove(&block_number)
                .ok_or_else(|| anyhow!("staged block {block_number} disappeared before flush"))?;
            self.worker.commit_prepared_update(&update)?;
        }

        self.refresh_sync_metrics(None)?;
        Ok(())
    }

    fn observe_head(&mut self, head_block: u64) -> Result<()> {
        if self.auto_advance_commit_target {
            self.advance_commit_target(head_block);
        }

        self.refresh_sync_metrics(Some(head_block))
    }

    fn refresh_sync_metrics(&self, head_override: Option<u64>) -> Result<()> {
        let persisted_head = self.worker.current_synced_block()?;
        let head_block = head_override.or(persisted_head).unwrap_or(0);
        let next_block = persisted_head.map_or(0, |block| block.saturating_add(1));
        StateWorker::<WR>::update_sync_metrics(next_block, head_block);
        Ok(())
    }
}

#[cfg(test)]
mod buffered_tests {
    use super::{
        DEFAULT_MAX_BUFFER_SIZE,
        StandaloneStateWorkerRuntime,
        StateWorker,
    };
    use crate::{
        commit_head_signal_channel,
        integration_tests::mdbx_fixture::MdbxTestDir,
        state::TraceProvider,
        system_calls::SystemCalls,
    };
    use alloy::primitives::{
        B256,
        U256,
    };
    use alloy_provider::RootProvider;
    use anyhow::Result;
    use async_trait::async_trait;
    use mdbx::{
        AccountState,
        AddressHash,
        BlockStateUpdate,
        Reader,
        StateWriter,
        common::CircularBufferConfig,
    };
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{
                AtomicU64,
                Ordering,
            },
        },
    };

    struct NoopTraceProvider;

    #[async_trait]
    impl TraceProvider for NoopTraceProvider {
        async fn fetch_block_state(&self, _: u64) -> Result<BlockStateUpdate> {
            unreachable!("trace provider should not be called in standalone flush regression test");
        }
    }

    fn mocked_provider() -> Arc<RootProvider> {
        Arc::new(RootProvider::new_http(
            "http://127.0.0.1:8545".parse().expect("valid dummy url"),
        ))
    }

    fn test_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate {
            block_number,
            block_hash: B256::repeat_byte(u8::try_from(block_number).unwrap_or(u8::MAX)),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![AccountState {
                address_hash: AddressHash::from_hash(B256::repeat_byte(0x11)),
                balance: U256::from(block_number),
                nonce: block_number,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            }],
        }
    }

    #[tokio::test]
    async fn standalone_runtime_flushes_staged_blocks_without_external_coordinator() -> Result<()> {
        let mdbx_dir = MdbxTestDir::new().map_err(anyhow::Error::msg)?;
        let writer_reader = StateWriter::new(
            mdbx_dir.path_str().map_err(anyhow::Error::msg)?,
            CircularBufferConfig::new(3)?,
        )?;
        let worker = StateWorker::new_with_commit_head_signal(
            mocked_provider(),
            Box::new(NoopTraceProvider),
            writer_reader,
            None,
            SystemCalls::default(),
            None,
            Arc::new(AtomicU64::new(0)),
        );
        let mut runtime =
            StandaloneStateWorkerRuntime::new(worker).with_auto_advance_commit_target();

        runtime.staged_updates.insert(1, test_update(1));
        runtime.staged_updates.insert(2, test_update(2));

        runtime.observe_head(2)?;
        runtime.flush_ready_blocks()?;

        assert_eq!(runtime.worker.current_synced_block()?, Some(2));
        assert!(runtime.staged_updates.is_empty());
        assert!(runtime.worker.writer_reader.is_block_available(1)?);
        assert!(runtime.worker.writer_reader.is_block_available(2)?);
        Ok(())
    }

    #[tokio::test]
    async fn standalone_runtime_flushes_more_than_default_buffer_size_without_external_coordinator()
    -> Result<()> {
        let mdbx_dir = MdbxTestDir::new().map_err(anyhow::Error::msg)?;
        let writer_reader = StateWriter::new(
            mdbx_dir.path_str().map_err(anyhow::Error::msg)?,
            CircularBufferConfig::new(3)?,
        )?;
        let target_block = u64::try_from(DEFAULT_MAX_BUFFER_SIZE).unwrap_or(u64::MAX) + 1;
        let worker = StateWorker::new_with_commit_head_signal(
            mocked_provider(),
            Box::new(NoopTraceProvider),
            writer_reader,
            None,
            SystemCalls::default(),
            None,
            Arc::new(AtomicU64::new(0)),
        );
        let mut runtime =
            StandaloneStateWorkerRuntime::new(worker).with_auto_advance_commit_target();

        for block_number in 1..=target_block {
            runtime
                .staged_updates
                .insert(block_number, test_update(block_number));
        }

        runtime.observe_head(target_block)?;
        runtime.flush_ready_blocks()?;

        assert_eq!(runtime.worker.current_synced_block()?, Some(target_block));
        assert!(runtime.staged_updates.is_empty());
        let oldest_retained_block = target_block - 2;
        assert!(!runtime
            .worker
            .writer_reader
            .is_block_available(oldest_retained_block - 1)?);
        assert!(runtime
            .worker
            .writer_reader
            .is_block_available(oldest_retained_block)?);
        assert!(runtime
            .worker
            .writer_reader
            .is_block_available(oldest_retained_block + 1)?);
        assert!(
            runtime
                .worker
                .writer_reader
                .is_block_available(target_block)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn runtime_flushes_staged_blocks_when_commit_head_arrives() -> Result<()> {
        let mdbx_dir = MdbxTestDir::new().map_err(anyhow::Error::msg)?;
        let shared_height = Arc::new(AtomicU64::new(0));
        let writer_reader = StateWriter::new(
            mdbx_dir.path_str().map_err(anyhow::Error::msg)?,
            CircularBufferConfig::new(3)?,
        )?;
        let (commit_head_signal_tx, commit_head_signal_rx) = commit_head_signal_channel();
        let mut worker = StateWorker::new_with_commit_head_signal(
            mocked_provider(),
            Box::new(NoopTraceProvider),
            writer_reader,
            None,
            SystemCalls::default(),
            Some(commit_head_signal_rx),
            shared_height.clone(),
        );

        worker.stage_update(test_update(1));
        worker.stage_update(test_update(2));
        worker.flush_ready_blocks()?;
        assert_eq!(worker.current_synced_block()?, None);
        assert_eq!(shared_height.load(Ordering::Acquire), 0);

        commit_head_signal_tx.send(Some(2))?;
        worker.observe_commit_target();
        worker.flush_ready_blocks()?;

        assert_eq!(worker.current_synced_block()?, Some(2));
        assert!(worker.staged_updates.is_empty());
        assert_eq!(shared_height.load(Ordering::Acquire), 2);
        Ok(())
    }
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
        Self::new_with_commit_head_signal(
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            None,
            Arc::new(AtomicU64::new(0)),
        )
    }

    pub fn new_with_commit_head_signal(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<GenesisState>,
        system_calls: SystemCalls,
        commit_head_signal_rx: Option<crate::CommitHeadSignalReceiver>,
        available_observed_head: Arc<AtomicU64>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            commit_head_signal_rx,
            commit_target: None,
            staged_updates: VecDeque::new(),
            available_observed_head,
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

    fn current_synced_block(&self) -> Result<Option<u64>> {
        match self.writer_reader.latest_block_number() {
            Ok(current) => {
                metrics::set_db_healthy(true);
                Ok(current)
            }
            Err(err) => {
                metrics::set_db_healthy(false);
                Err(anyhow!(err)).context("failed to read current block from the database")
            }
        }
    }

    async fn provider_head(&self) -> Result<u64> {
        self.provider.get_block_number().await.map_err(Into::into)
    }

    fn provider(&self) -> &Arc<RootProvider> {
        &self.provider
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
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

            self.observe_commit_target();
            self.flush_ready_blocks()?;

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

        let current = self.current_synced_block()?;
        Ok(current.map_or(0, |b| b + 1))
    }

    /// Sequentially replay blocks until we reach the node's current head.
    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            self.observe_commit_target();
            self.flush_ready_blocks()?;
            let head = self.provider.get_block_number().await?;
            Self::update_sync_metrics(*next_block, head);

            if *next_block > head {
                return Ok(false);
            }

            while *next_block <= head {
                // Process the block fully before checking shutdown
                self.process_block(*next_block).await?;
                *next_block += 1;
                self.observe_commit_target();
                self.flush_ready_blocks()?;
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
                changed = async {
                    match &mut self.commit_head_signal_rx {
                        Some(commit_head_signal_rx) => commit_head_signal_rx.changed().await.ok(),
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_some() {
                        self.observe_commit_target();
                        self.flush_ready_blocks()?;
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
                                self.process_block(*next_block).await?;
                                *next_block += 1;
                                self.observe_commit_target();
                                self.flush_ready_blocks()?;
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

    async fn prepare_block_update(
        &mut self,
        block_number: u64,
    ) -> Result<Option<BlockStateUpdate>> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.build_genesis_update().await;
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

        Ok(Some(update))
    }

    fn commit_prepared_update(&mut self, update: &BlockStateUpdate) -> Result<()> {
        let block_number = update.block_number;

        match self.writer_reader.commit_block(update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);
                self.available_observed_head
                    .store(block_number, Ordering::Release);
            }
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to database");
        Ok(())
    }

    /// Pull, trace, and persist a single block.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        let Some(update) = self.prepare_block_update(block_number).await? else {
            return Ok(());
        };

        self.stage_update(update);
        if self.commit_head_signal_rx.is_none() {
            self.advance_commit_target(block_number);
        }
        self.flush_ready_blocks()?;
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

    /// Build block 0 from the configured genesis state.
    async fn build_genesis_update(&mut self) -> Result<Option<BlockStateUpdate>> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(None);
        };

        let already_exists = self
            .current_synced_block()
            .context("failed to read current block from database during genesis hydration")?
            .is_some();

        if already_exists {
            info!("block 0 already exists in database; skipping genesis hydration");
            return Ok(None);
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(None);
        }

        // Fetch genesis block header to get hash and state root
        let block = self
            .provider
            .get_block_by_number(0.into())
            .await?
            .context("genesis block not found on chain")?;

        info!("hydrating genesis state from genesis file");

        Ok(Some(BlockStateUpdateBuilder::from_accounts(
            0,
            block.header.hash,
            block.header.state_root,
            accounts,
        )))
    }

    fn stage_update(&mut self, update: BlockStateUpdate) {
        self.staged_updates.push_back(update);
    }

    fn observe_commit_target(&mut self) {
        let commit_target = {
            let Some(commit_head_signal_rx) = self.commit_head_signal_rx.as_mut() else {
                return;
            };
            *commit_head_signal_rx.borrow_and_update()
        };

        if let Some(commit_target) = commit_target {
            self.advance_commit_target(commit_target);
        }
    }

    fn advance_commit_target(&mut self, block_number: u64) {
        self.commit_target = Some(
            self.commit_target
                .map_or(block_number, |current| current.max(block_number)),
        );
    }

    fn flush_ready_blocks(&mut self) -> Result<()> {
        let Some(commit_target) = self.commit_target else {
            return Ok(());
        };

        while self
            .staged_updates
            .front()
            .is_some_and(|update| update.block_number <= commit_target)
        {
            let update = self
                .staged_updates
                .pop_front()
                .expect("staged_updates.front() guaranteed update presence");
            self.commit_prepared_update(&update)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::StateWorker;
    use crate::{
        integration_tests::mdbx_fixture::MdbxTestDir,
        state::TraceProvider,
        system_calls::SystemCalls,
    };
    use alloy::primitives::{
        B256,
        U256,
    };
    use alloy_provider::{
        Provider,
        ProviderBuilder,
        RootProvider,
        WsConnect,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
    use mdbx::{
        AccountState,
        AddressHash,
        BlockStateUpdate,
        Reader,
        StateWriter,
        common::CircularBufferConfig,
    };
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{
                AtomicU64,
                Ordering,
            },
        },
    };

    struct NoopTraceProvider;

    #[async_trait]
    impl TraceProvider for NoopTraceProvider {
        async fn fetch_block_state(&self, _: u64) -> Result<BlockStateUpdate> {
            unreachable!("trace provider should not be called in buffered commit test");
        }
    }

    async fn test_provider() -> Result<(DualProtocolMockServer, Arc<RootProvider>)> {
        let server = DualProtocolMockServer::new()
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(server.ws_url()))
            .await?;

        Ok((server, Arc::new(provider.root().clone())))
    }

    fn test_update(block_number: u64) -> BlockStateUpdate {
        BlockStateUpdate {
            block_number,
            block_hash: B256::repeat_byte(u8::try_from(block_number).unwrap_or(u8::MAX)),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![AccountState {
                address_hash: AddressHash::from_hash(B256::repeat_byte(0x11)),
                balance: U256::from(block_number),
                nonce: block_number,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            }],
        }
    }

    #[tokio::test]
    async fn buffered_blocks_wait_for_commit_heads_before_flushing() -> Result<()> {
        let (_server, provider) = test_provider().await?;
        let mdbx_dir = MdbxTestDir::new().map_err(anyhow::Error::msg)?;
        let shared_height = Arc::new(AtomicU64::new(0));
        let writer_reader = StateWriter::new(
            mdbx_dir.path_str().map_err(anyhow::Error::msg)?,
            CircularBufferConfig::new(3)?,
        )?;
        let reader = writer_reader.reader().clone();
        let (commit_head_signal_tx, commit_head_signal_rx) = crate::commit_head_signal_channel();
        let mut worker = StateWorker::new_with_commit_head_signal(
            provider,
            Box::new(NoopTraceProvider),
            writer_reader,
            None,
            SystemCalls::default(),
            Some(commit_head_signal_rx),
            shared_height.clone(),
        );

        worker.stage_update(test_update(1));
        worker.stage_update(test_update(2));
        worker.flush_ready_blocks()?;
        assert_eq!(reader.latest_block_number()?, None);
        assert_eq!(shared_height.load(Ordering::Acquire), 0);

        commit_head_signal_tx.send(Some(1))?;
        worker.observe_commit_target();
        worker.flush_ready_blocks()?;
        assert_eq!(reader.latest_block_number()?, Some(1));
        assert_eq!(shared_height.load(Ordering::Acquire), 1);

        commit_head_signal_tx.send(Some(2))?;
        worker.observe_commit_target();
        worker.flush_ready_blocks()?;
        assert_eq!(reader.latest_block_number()?, Some(2));
        assert_eq!(shared_height.load(Ordering::Acquire), 2);
        Ok(())
    }
}
