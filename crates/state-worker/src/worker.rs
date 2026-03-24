//! Core tracing and persistence primitives for the state-worker runtime.
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
use alloy_provider::{
    Provider,
    RootProvider,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use mdbx::{
    BlockStateUpdate,
    Reader,
    Writer,
};
use std::sync::Arc;
use tracing::{
    debug,
    info,
    warn,
};

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
    pub fn update_sync_metrics(next_block: u64, head_block: u64) {
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

    pub fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = self.current_synced_block()?;
        Ok(current.map_or(0, |block| block + 1))
    }

    pub fn current_synced_block(&self) -> Result<Option<u64>> {
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

    pub async fn provider_head(&self) -> Result<u64> {
        self.provider.get_block_number().await.map_err(Into::into)
    }

    pub fn provider(&self) -> &Arc<RootProvider> {
        &self.provider
    }

    pub async fn prepare_block_update(
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

        self.apply_system_calls(&mut update, block_number).await?;

        Ok(Some(update))
    }

    pub async fn commit_prepared_update(&mut self, update: BlockStateUpdate) -> Result<()> {
        let block_number = update.block_number;

        match self.writer_reader.commit_block(&update) {
            Ok(stats) => {
                metrics::set_db_healthy(true);
                metrics::record_commit(block_number, &stats);
            }
            Err(err) => {
                critical!(
                    error = ?err,
                    block_number,
                    "failed to persist block"
                );
                metrics::set_db_healthy(false);
                metrics::record_block_failure();
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to database");
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

    async fn build_genesis_update(&mut self) -> Result<Option<BlockStateUpdate>> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(None);
        };

        let already_exists = self.current_synced_block()?.is_some();
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
}
