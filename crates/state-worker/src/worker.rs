//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and buffered in memory. Writes to MDBX
//! are then gated by the latest commit head that the sidecar has finalized.
use crate::{
    error::{
        Result,
        StateWorkerError,
        boxed_error,
    },
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
    sync::{
        broadcast,
        watch,
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

enum FlushMode {
    Immediate,
    CommitHead {
        rx: watch::Receiver<u64>,
        latest_allowed_block: u64,
    },
}

enum ProcessOutcome {
    Continue,
    Shutdown,
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
    buffered_updates: VecDeque<BlockStateUpdate>,
    max_buffered_blocks: usize,
    committed_head: Arc<AtomicU64>,
    flush_mode: FlushMode,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    #[must_use]
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
            buffered_updates: VecDeque::new(),
            max_buffered_blocks: 1,
            committed_head: Arc::new(AtomicU64::new(0)),
            flush_mode: FlushMode::Immediate,
        }
    }

    #[must_use]
    pub fn with_commit_head_control(
        mut self,
        flush_rx: watch::Receiver<u64>,
        max_buffered_blocks: usize,
        committed_head: Arc<AtomicU64>,
    ) -> Self {
        let latest_allowed_block = *flush_rx.borrow();
        self.max_buffered_blocks = max_buffered_blocks.max(1);
        self.committed_head = committed_head;
        self.flush_mode = FlushMode::CommitHead {
            latest_allowed_block,
            rx: flush_rx,
        };
        self
    }

    #[must_use]
    pub fn committed_head(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.committed_head)
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
    /// Returns an error if tracing, provider access, or MDBX writes fail.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override)?;
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
                    if matches!(err, StateWorkerError::MissingBlock { .. }) {
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

    fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            self.committed_head
                .store(block.saturating_sub(1), Ordering::Release);
            return Ok(block);
        }

        let current = self.writer_reader.latest_block_number().map_err(|source| {
            StateWorkerError::DatabaseReadCurrentBlock {
                source: boxed_error(source),
            }
        })?;

        metrics::set_db_healthy(true);
        let latest = current.unwrap_or(0);
        self.committed_head.store(latest, Ordering::Release);
        Ok(current.map_or(0, |b| b + 1))
    }

    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            let head = self.provider.get_block_number().await.map_err(|source| {
                StateWorkerError::GetBlockNumber {
                    source: boxed_error(source),
                }
            })?;
            Self::update_sync_metrics(*next_block, head);

            if *next_block > head {
                return Ok(false);
            }

            while *next_block <= head {
                if matches!(
                    self.process_block(*next_block, shutdown_rx).await?,
                    ProcessOutcome::Shutdown
                ) {
                    return Ok(true);
                }
                *next_block += 1;
                Self::update_sync_metrics(*next_block, head);

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
        let subscription = self.provider.subscribe_blocks().await.map_err(|source| {
            StateWorkerError::SubscribeBlocks {
                source: boxed_error(source),
            }
        })?;
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

                            Self::update_sync_metrics(*next_block, block_number);

                            if block_number > *next_block {
                                warn!("Missing block {block_number} (next block: {next_block})");
                                return Err(StateWorkerError::MissingBlock {
                                    block_number,
                                    next_block: *next_block,
                                });
                            }

                            while *next_block <= block_number {
                                if matches!(
                                    self.process_block(*next_block, shutdown_rx).await?,
                                    ProcessOutcome::Shutdown
                                ) {
                                    return Ok(());
                                }
                                *next_block += 1;
                                Self::update_sync_metrics(*next_block, block_number);
                            }
                        }
                        None => {
                            return Err(StateWorkerError::BlockSubscriptionCompleted);
                        }
                    }
                }
            }
        }
    }

    async fn process_block(
        &mut self,
        block_number: u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<ProcessOutcome> {
        info!(block_number, "processing block");

        let update = if block_number == 0 && self.genesis_state.is_some() {
            match self.build_genesis_update().await? {
                Some(update) => update,
                None => return Ok(ProcessOutcome::Continue),
            }
        } else {
            let mut update = self.trace_provider.fetch_block_state(block_number).await?;
            self.apply_system_calls(&mut update, block_number).await?;
            update
        };

        self.push_buffered_update(update, shutdown_rx).await
    }

    async fn push_buffered_update(
        &mut self,
        update: BlockStateUpdate,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<ProcessOutcome> {
        while self.buffered_updates.len() >= self.max_buffered_blocks {
            self.flush_buffered_updates()?;

            if self.buffered_updates.len() < self.max_buffered_blocks {
                break;
            }

            if self.wait_for_flush_progress(shutdown_rx).await? {
                return Ok(ProcessOutcome::Shutdown);
            }
        }

        self.buffered_updates.push_back(update);
        self.flush_buffered_updates()?;
        Ok(ProcessOutcome::Continue)
    }

    fn flush_buffered_updates(&mut self) -> Result<()> {
        let flush_limit = self.current_flush_limit();

        while self
            .buffered_updates
            .front()
            .is_some_and(|update| update.block_number <= flush_limit)
        {
            let Some(update) = self.buffered_updates.pop_front() else {
                break;
            };
            self.commit_update(&update)?;
        }

        Ok(())
    }

    fn current_flush_limit(&mut self) -> u64 {
        match &mut self.flush_mode {
            FlushMode::Immediate => u64::MAX,
            FlushMode::CommitHead {
                latest_allowed_block,
                rx,
            } => {
                *latest_allowed_block = *rx.borrow_and_update();
                *latest_allowed_block
            }
        }
    }

    async fn wait_for_flush_progress(
        &mut self,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        match &mut self.flush_mode {
            FlushMode::Immediate => Ok(false),
            FlushMode::CommitHead {
                latest_allowed_block,
                rx,
            } => {
                tokio::select! {
                    _ = shutdown_rx.recv() => Ok(true),
                    changed = rx.changed() => {
                        if changed.is_ok() {
                            *latest_allowed_block = *rx.borrow_and_update();
                            self.flush_buffered_updates()?;
                        }
                        Ok(false)
                    }
                }
            }
        }
    }

    fn commit_update(&mut self, update: &BlockStateUpdate) -> Result<()> {
        match self.writer_reader.commit_block(update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(update.block_number, &stats);
                self.committed_head
                    .store(update.block_number, Ordering::Release);
                info!(
                    block_number = update.block_number,
                    "block persisted to database"
                );
                Ok(())
            }
            Err(source) => {
                critical!(error = ?source, block_number = update.block_number, "failed to persist block");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                Err(StateWorkerError::PersistBlock {
                    block_number: update.block_number,
                    source: boxed_error(source),
                })
            }
        }
    }

    async fn apply_system_calls(
        &self,
        update: &mut BlockStateUpdate,
        block_number: u64,
    ) -> Result<()> {
        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await
            .map_err(|source| {
                StateWorkerError::GetBlockByNumber {
                    block_number,
                    source: boxed_error(source),
                }
            })?
            .ok_or(StateWorkerError::BlockNotFound { block_number })?;

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

    async fn build_genesis_update(&mut self) -> Result<Option<BlockStateUpdate>> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(None);
        };

        let already_exists = self
            .writer_reader
            .latest_block_number()
            .map_err(|source| {
                StateWorkerError::DatabaseReadGenesisStatus {
                    source: boxed_error(source),
                }
            })?
            .is_some();

        metrics::set_db_healthy(true);

        if already_exists {
            info!("block 0 already exists in database; skipping genesis hydration");
            return Ok(None);
        }

        let accounts = genesis.into_accounts();

        if accounts.is_empty() {
            warn!("genesis file contained no accounts; skipping hydration");
            return Ok(None);
        }

        let block = self
            .provider
            .get_block_by_number(0.into())
            .await
            .map_err(|source| {
                StateWorkerError::GetBlockByNumber {
                    block_number: 0,
                    source: boxed_error(source),
                }
            })?
            .ok_or(StateWorkerError::BlockNotFound { block_number: 0 })?;

        info!("hydrating genesis state from genesis file");

        Ok(Some(BlockStateUpdateBuilder::from_accounts(
            0,
            block.header.hash,
            block.header.state_root,
            accounts,
        )))
    }
}
