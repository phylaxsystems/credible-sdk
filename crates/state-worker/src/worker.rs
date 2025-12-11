//! Core orchestration loop that keeps Redis in sync with the execution client.
//!
//! The worker bootstraps from the last persisted block, catches up to head via
//! RPC, and then tails new blocks from the `newHeads` subscription. Each block
//! is traced with the pre-state tracer and written into Redis.

use crate::{
    critical,
    genesis::GenesisState,
    state::{
        BlockStateUpdateBuilder,
        TraceProvider,
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
use futures::StreamExt;
use state_store::StateWriter;
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

/// Coordinates block ingestion, tracing, and persistence.
pub struct StateWorker {
    provider: Arc<RootProvider>,
    trace_provider: Box<dyn TraceProvider>,
    redis: StateWriter,
    genesis_state: Option<GenesisState>,
}

impl StateWorker {
    /// Build a worker that shares the provider/Redis client across async tasks.
    pub fn new(
        provider: Arc<RootProvider>,
        trace_provider: Box<dyn TraceProvider>,
        redis: StateWriter,
        genesis_state: Option<GenesisState>,
    ) -> Self {
        Self {
            provider,
            trace_provider,
            redis,
            genesis_state,
        }
    }

    /// Drive the catch-up + streaming loop. We keep retrying the subscription
    /// because websocket connections can drop in practice.
    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override)?;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    return Ok(());
                }
                result = self.catch_up(&mut next_block) => {
                    result?;
                }
            }

            match self.stream_blocks(&mut next_block, &mut shutdown_rx).await {
                Ok(()) => {
                    info!("Shutdown signal received during streaming");
                    return Ok(());
                }
                Err(err) => {
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

        let current = self
            .redis
            .latest_block_number()
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
    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

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
                        None => {
                            return Err(anyhow!("block subscription completed"));
                        }
                    }
                }
            }
        }
    }

    /// Pull, trace, and persist a single block.
    async fn process_block(&mut self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        if block_number == 0 && self.genesis_state.is_some() {
            return self.process_genesis_block().await;
        }

        let update = match self.trace_provider.fetch_block_state(block_number).await {
            Ok(update) => update,
            Err(err) => {
                critical!(error = ?err, block_number, "failed to trace block");
                return Err(anyhow!("failed to trace block {block_number}"));
            }
        };

        match self.redis.commit_block(update) {
            Ok(()) => (),
            Err(err) => {
                critical!(error = ?err, block_number, "failed to persist block");
                return Err(anyhow!("failed to persist block {block_number}"));
            }
        }

        info!(block_number, "block persisted to redis");
        Ok(())
    }

    /// Process block 0 using the configured genesis state
    async fn process_genesis_block(&mut self) -> Result<()> {
        let Some(genesis) = self.genesis_state.take() else {
            warn!("no genesis state configured; skipping genesis hydration");
            return Ok(());
        };

        let already_exists = self
            .redis
            .latest_block_number()
            .context("failed to check if genesis already exists")?
            .is_some();

        if already_exists {
            info!("block 0 already exists in redis; skipping genesis hydration");
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

        match self.redis.commit_block(update) {
            Ok(()) => (),
            Err(err) => {
                critical!(error = ?err, block_number = 0, "failed to persist genesis block");
                return Err(anyhow!("failed to persist genesis block"));
            }
        }

        info!(block_number = 0, "genesis block persisted to redis");
        Ok(())
    }
}
