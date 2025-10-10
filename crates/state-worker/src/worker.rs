//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.

use crate::critical;
use alloy::{
    eips::BlockId,
    rpc::types::{
        BlockNumberOrTag,
        Header,
    },
};
use alloy_provider::{
    Provider,
    RootProvider,
    ext::TraceApi,
};
use alloy_rpc_types_trace::parity::TraceType;
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures::StreamExt;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::time;
use tracing::{
    debug,
    info,
    warn,
};

use crate::{
    genesis::GenesisState,
    redis::RedisStateWriter,
    state::BlockStateUpdate,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker {
    provider: Arc<RootProvider>,
    redis: RedisStateWriter,
    genesis_state: Option<GenesisState>,
}

impl StateWorker {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(
        provider: Arc<RootProvider>,
        redis: RedisStateWriter,
        genesis_state: Option<GenesisState>,
    ) -> Self {
        Self {
            provider,
            redis,
            genesis_state,
        }
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
    pub async fn run(&mut self, start_override: Option<u64>) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override).await?;
        loop {
            self.catch_up(&mut next_block).await?;
            if let Err(err) = self.stream_blocks(&mut next_block).await {
                warn!(error = %err, "block subscription ended, retrying");
                time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)).await;
            }
        }
    }

    /// Determine the next block to ingest. We respect manual overrides so
    /// operators can force a resync of historical ranges when needed.
    async fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = self
            .redis
            .latest_block_number()
            .await
            .context("failed to read current block from redis")?;

        Ok(current.map_or(0, |b| b + 1))
    }

    /// Sequentially replay blocks until we reach the node's current head.
    async fn catch_up(&mut self, next_block: &mut u64) -> Result<()> {
        loop {
            let head = self.provider.get_block_number().await?;
            if *next_block > head {
                break;
            }

            while *next_block <= head {
                self.process_block(*next_block).await?;
                *next_block += 1;
            }
        }
        Ok(())
    }

    /// Follow the `newHeads` stream and process new blocks in order, tolerating
    /// duplicate/stale headers after reconnects.
    async fn stream_blocks(&mut self, next_block: &mut u64) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

        while let Some(header) = stream.next().await {
            let Header { hash: _, inner, .. } = header;
            let block_number = inner.number;

            // We may receive a header we already processed if the stream
            // briefly disconnects; skip without rewinding because we do not
            // currently handle reorgs.
            if block_number + 1 < *next_block {
                debug!(block_number, next_block, "skipping stale header");
                continue;
            }

            // If we are missing a block, trigger a critical error
            if block_number > *next_block {
                critical!("Missing block {block_number} (next block: {next_block})");
                return Err(anyhow!(
                    "Missing block {block_number} (next block: {next_block})"
                ));
            }

            while *next_block <= block_number {
                self.process_block(*next_block).await?;
                *next_block += 1;
            }
        }

        Err(anyhow!("block subscription completed"))
    }

    /// Pull, trace, and persist a single block.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        let block_id = BlockNumberOrTag::Number(block_number);
        let block = self
            .provider
            .get_block_by_number(block_id)
            .await?
            .with_context(|| format!("block {block_number} not found"))?;
        let block_hash = block.header.hash;

        // Use the standardized pre-state tracer options so we receive per-tx
        // diffs matching the sidecar's expectations.
        let traces = match self
            .provider
            .trace_replay_block_transactions(BlockId::Number(block_number.into()))
            .trace_type(TraceType::StateDiff)
            .await
        {
            Ok(traces) => traces,
            Err(err) => {
                critical!(error = ?err, block_number, "failed to trace block");
                return Err(anyhow!("failed to trace block {block_number}"));
            }
        };

        let mut update = BlockStateUpdate::from_traces(block_number, block_hash, traces);

        if block_number == 0 && update.accounts.is_empty() {
            match self.genesis_state.take() {
                Some(genesis) => {
                    let accounts = genesis.into_accounts();
                    if accounts.is_empty() {
                        warn!("genesis file contained no accounts; skipping hydration");
                    } else {
                        debug!("hydrating genesis state from genesis file");
                        update =
                            BlockStateUpdate::from_accounts(block_number, block_hash, accounts);
                    }
                }
                None => {
                    warn!("no genesis state configured; skipping genesis hydration");
                }
            }
        }

        match self.redis.commit_block(update).await {
            Ok(()) => (),
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to redis");
        Ok(())
    }

}
