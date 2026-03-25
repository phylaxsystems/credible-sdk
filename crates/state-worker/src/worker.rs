//! Core orchestration loop that keeps the database in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced, buffered, and committed according to the selected commit policy.

use crate::{
    metrics,
    service::StateWorkerCommand,
    state::{
        TraceProvider,
        error::TraceProviderError,
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
use flume::Receiver;
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
use thiserror::Error;
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
pub const UNINITIALIZED_COMMITTED_HEIGHT: u64 = u64::MAX;

#[derive(Debug)]
enum CommitMode {
    Immediate,
    Buffered {
        command_rx: Receiver<StateWorkerCommand>,
        committed_height: Arc<AtomicU64>,
        buffered_updates: VecDeque<BlockStateUpdate>,
        buffered_blocks_capacity: usize,
    },
}

pub(crate) struct BufferedCommitConfig {
    pub(crate) command_rx: Receiver<StateWorkerCommand>,
    pub(crate) committed_height: Arc<AtomicU64>,
    pub(crate) buffered_blocks_capacity: usize,
}

#[derive(Debug, Error)]
pub enum StateWorkerError {
    #[error("failed to read current block from database: {source}")]
    ReadCommittedHeight {
        #[source]
        source: mdbx::common::error::StateError,
    },
    #[error("failed to subscribe to blocks: {0}")]
    SubscribeBlocks(String),
    #[error("failed to get block number: {0}")]
    GetBlockNumber(String),
    #[error("failed to get block {block_number}: {message}")]
    GetBlock { block_number: u64, message: String },
    #[error("block {0} not found")]
    MissingBlock(u64),
    #[error("missing block {block_number} (next block: {next_block})")]
    MissingBlockGap { block_number: u64, next_block: u64 },
    #[error("block subscription completed")]
    SubscriptionCompleted,
    #[error("failed to trace block {block_number}: {source}")]
    TraceBlock {
        block_number: u64,
        #[source]
        source: TraceProviderError,
    },
    #[error("failed to persist block {block_number}: {source}")]
    PersistBlock {
        block_number: u64,
        #[source]
        source: mdbx::common::error::StateError,
    },
    #[error("failed to persist genesis block: {source}")]
    PersistGenesis {
        #[source]
        source: mdbx::common::error::StateError,
    },
    #[error("command channel closed")]
    CommandChannelClosed,
}

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker<WR>
where
    WR: Writer + Reader,
{
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    writer_reader: WR,
    genesis_state: Option<crate::genesis::GenesisState>,
    system_calls: SystemCalls,
    commit_mode: CommitMode,
}

impl<WR> StateWorker<WR>
where
    WR: Writer + Reader + Send + Sync,
    WR: Writer<Error = mdbx::common::error::StateError>
        + Reader<Error = mdbx::common::error::StateError>,
{
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<crate::genesis::GenesisState>,
        system_calls: SystemCalls,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            commit_mode: CommitMode::Immediate,
        }
    }

    pub(crate) fn new_buffered(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        writer_reader: WR,
        genesis_state: Option<crate::genesis::GenesisState>,
        system_calls: SystemCalls,
        buffered_commit: BufferedCommitConfig,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
            commit_mode: CommitMode::Buffered {
                command_rx: buffered_commit.command_rx,
                committed_height: buffered_commit.committed_height,
                buffered_updates: VecDeque::with_capacity(buffered_commit.buffered_blocks_capacity),
                buffered_blocks_capacity: buffered_commit.buffered_blocks_capacity,
            },
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

    /// Run the worker until shutdown or failure.
    ///
    /// # Errors
    ///
    /// Returns an error if tracing, block streaming, or MDBX persistence fails.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), StateWorkerError> {
        let mut next_block = self.compute_start_block(start_override)?;
        let mut missing_block_retries: u32 = 0;

        loop {
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received");
                return Ok(());
            }

            self.flush_pending_commands_now()?;

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
                    let is_missing_block = matches!(err, StateWorkerError::MissingBlockGap { .. });

                    if is_missing_block {
                        missing_block_retries = missing_block_retries.saturating_add(1);

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

    fn compute_start_block(
        &mut self,
        override_start: Option<u64>,
    ) -> Result<u64, StateWorkerError> {
        if let Some(block) = override_start {
            self.set_committed_height(Some(block.saturating_sub(1)));
            return Ok(block);
        }

        let current = self
            .writer_reader
            .latest_block_number()
            .map_err(|source| StateWorkerError::ReadCommittedHeight { source })?;
        metrics::set_db_healthy(true);
        self.set_committed_height(current);
        Ok(current.map_or(0, |b| b + 1))
    }

    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool, StateWorkerError> {
        loop {
            let head = self
                .provider
                .get_block_number()
                .await
                .map_err(|err| StateWorkerError::GetBlockNumber(err.to_string()))?;
            Self::update_sync_metrics(*next_block, head);

            if *next_block > head {
                return Ok(false);
            }

            while *next_block <= head {
                if self.wait_for_buffer_capacity(shutdown_rx).await? {
                    return Ok(true);
                }

                self.process_block(*next_block).await?;
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
    ) -> Result<(), StateWorkerError> {
        let subscription = self
            .provider
            .subscribe_blocks()
            .await
            .map_err(|err| StateWorkerError::SubscribeBlocks(err.to_string()))?;
        let mut stream = subscription.into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        loop {
            if let Some(command_rx) = self.command_rx().cloned() {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received during block streaming");
                        return Ok(());
                    }
                    command = command_rx.recv_async() => {
                        let command = command.map_err(|_| StateWorkerError::CommandChannelClosed)?;
                        self.handle_command(command)?;
                    }
                    maybe_header = stream.next() => {
                        self.handle_stream_header(maybe_header, next_block, shutdown_rx).await?;
                    }
                }
            } else {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received during block streaming");
                        return Ok(());
                    }
                    maybe_header = stream.next() => {
                        self.handle_stream_header(maybe_header, next_block, shutdown_rx).await?;
                    }
                }
            }
        }
    }

    async fn handle_stream_header(
        &mut self,
        maybe_header: Option<Header>,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<(), StateWorkerError> {
        match maybe_header {
            Some(header) => {
                let Header { hash: _, inner, .. } = header;
                let block_number = inner.number;

                if block_number + 1 < *next_block {
                    debug!(block_number, next_block, "skipping stale header");
                    return Ok(());
                }

                Self::update_sync_metrics(*next_block, block_number);

                if block_number > *next_block {
                    warn!("Missing block {block_number} (next block: {next_block})");
                    return Err(StateWorkerError::MissingBlockGap {
                        block_number,
                        next_block: *next_block,
                    });
                }

                while *next_block <= block_number {
                    if self.wait_for_buffer_capacity(shutdown_rx).await? {
                        return Ok(());
                    }
                    self.process_block(*next_block).await?;
                    *next_block += 1;
                    Self::update_sync_metrics(*next_block, block_number);
                }

                Ok(())
            }
            None => Err(StateWorkerError::SubscriptionCompleted),
        }
    }

    async fn process_block(&mut self, block_number: u64) -> Result<(), StateWorkerError> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.process_genesis_block().await;
        }

        let mut update = self
            .trace_provider
            .fetch_block_state(block_number)
            .await
            .map_err(|source| {
                critical!(error = ?source, block_number, "failed to trace block");
                metrics::record_block_failure();
                StateWorkerError::TraceBlock {
                    block_number,
                    source,
                }
            })?;

        self.apply_system_calls(&mut update, block_number).await;
        self.enqueue_or_commit(update)?;

        info!(block_number, "block traced");
        Ok(())
    }

    async fn apply_system_calls(&self, update: &mut BlockStateUpdate, block_number: u64) {
        let block = match self.provider.get_block_by_number(block_number.into()).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                warn!(
                    block_number,
                    "failed to compute system call states: block not found"
                );
                return;
            }
            Err(err) => {
                warn!(
                    block_number,
                    error = %err,
                    "failed to compute system call states: unable to fetch block"
                );
                return;
            }
        };

        let parent_block_hash = if block_number > 0 {
            Some(block.header.parent_hash)
        } else {
            None
        };

        let config = SystemCallConfig {
            block_number,
            timestamp: block.header.timestamp,
            parent_block_hash,
            parent_beacon_block_root: block.header.parent_beacon_block_root,
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
            }
            Err(err) => {
                warn!(
                    block_number,
                    error = %err,
                    "failed to compute system call states, traces may already include them"
                );
            }
        }
    }

    async fn process_genesis_block(&mut self) -> Result<(), StateWorkerError> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(());
        };

        let already_exists = self
            .writer_reader
            .latest_block_number()
            .map_err(|source| StateWorkerError::ReadCommittedHeight { source })?
            .is_some();
        metrics::set_db_healthy(true);

        if already_exists {
            info!("block 0 already exists in database; skipping genesis hydration");
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
            .await
            .map_err(|err| {
                StateWorkerError::GetBlock {
                    block_number: 0,
                    message: err.to_string(),
                }
            })?
            .ok_or(StateWorkerError::MissingBlock(0))?;

        let update = crate::state::BlockStateUpdateBuilder::from_accounts(
            0,
            block.header.hash,
            block.header.state_root,
            accounts,
        );

        self.enqueue_or_commit(update)?;
        info!(block_number = 0, "genesis block prepared");
        Ok(())
    }

    fn enqueue_or_commit(&mut self, update: BlockStateUpdate) -> Result<(), StateWorkerError> {
        match &mut self.commit_mode {
            CommitMode::Immediate => {
                self.commit_update(&update)?;
                info!(
                    block_number = update.block_number,
                    "block persisted to database"
                );
            }
            CommitMode::Buffered {
                buffered_updates, ..
            } => {
                buffered_updates.push_back(update);
                self.flush_pending_commands_now()?;
            }
        }

        Ok(())
    }

    fn commit_update(&self, update: &BlockStateUpdate) -> Result<(), StateWorkerError> {
        let block_number = update.block_number;
        self.writer_reader
            .commit_block(update)
            .map(|stats| {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);
                self.set_committed_height(Some(block_number));
            })
            .map_err(|source| {
                critical!(error = ?source, block_number, "failed to persist block");
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                if block_number == 0 {
                    StateWorkerError::PersistGenesis { source }
                } else {
                    StateWorkerError::PersistBlock {
                        block_number,
                        source,
                    }
                }
            })
    }

    fn flush_pending_commands_now(&mut self) -> Result<(), StateWorkerError> {
        while let Some(command) = self.try_recv_command()? {
            self.handle_command(command)?;
        }
        Ok(())
    }

    fn try_recv_command(&self) -> Result<Option<StateWorkerCommand>, StateWorkerError> {
        let Some(command_rx) = self.command_rx() else {
            return Ok(None);
        };

        match command_rx.try_recv() {
            Ok(command) => Ok(Some(command)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Err(StateWorkerError::CommandChannelClosed),
        }
    }

    fn handle_command(&mut self, command: StateWorkerCommand) -> Result<(), StateWorkerError> {
        match command {
            StateWorkerCommand::FlushUpTo(block_number) => self.flush_buffered_up_to(block_number),
        }
    }

    fn flush_buffered_up_to(&mut self, block_number: u64) -> Result<(), StateWorkerError> {
        let mut flushed_any = false;

        loop {
            let Some(next_block) = self.peek_buffered_block_number() else {
                break;
            };
            if next_block > block_number {
                break;
            }

            let update = match &mut self.commit_mode {
                CommitMode::Buffered {
                    buffered_updates, ..
                } => buffered_updates.pop_front(),
                CommitMode::Immediate => None,
            };

            let Some(update) = update else {
                break;
            };
            self.commit_update(&update)?;
            flushed_any = true;
        }

        if flushed_any {
            debug!(block_number, "flushed buffered updates to MDBX");
        }

        Ok(())
    }

    async fn wait_for_buffer_capacity(
        &mut self,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool, StateWorkerError> {
        while self.buffer_is_full() {
            let Some(command_rx) = self.command_rx().cloned() else {
                return Ok(false);
            };

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received while waiting for buffer capacity");
                    return Ok(true);
                }
                command = command_rx.recv_async() => {
                    let command = command.map_err(|_| StateWorkerError::CommandChannelClosed)?;
                    self.handle_command(command)?;
                }
            }
        }

        Ok(false)
    }

    fn buffer_is_full(&self) -> bool {
        match &self.commit_mode {
            CommitMode::Buffered {
                buffered_updates,
                buffered_blocks_capacity,
                ..
            } => buffered_updates.len() >= *buffered_blocks_capacity,
            CommitMode::Immediate => false,
        }
    }

    fn peek_buffered_block_number(&self) -> Option<u64> {
        match &self.commit_mode {
            CommitMode::Buffered {
                buffered_updates, ..
            } => buffered_updates.front().map(|update| update.block_number),
            CommitMode::Immediate => None,
        }
    }

    fn command_rx(&self) -> Option<&Receiver<StateWorkerCommand>> {
        match &self.commit_mode {
            CommitMode::Buffered { command_rx, .. } => Some(command_rx),
            CommitMode::Immediate => None,
        }
    }

    fn set_committed_height(&self, block_number: Option<u64>) {
        let Some(committed_height) = (match &self.commit_mode {
            CommitMode::Buffered {
                committed_height, ..
            } => Some(committed_height),
            CommitMode::Immediate => None,
        }) else {
            return;
        };

        committed_height.store(
            block_number.unwrap_or(UNINITIALIZED_COMMITTED_HEIGHT),
            Ordering::Release,
        );
    }
}
