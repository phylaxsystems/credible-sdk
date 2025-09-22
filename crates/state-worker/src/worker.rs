use std::{
    sync::Arc,
    time::Duration,
};

use alloy::rpc::types::{
    BlockNumberOrTag,
    Header,
};
use alloy_provider::{
    Provider,
    RootProvider,
    ext::DebugApi,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use futures::StreamExt;
use tokio::time;
use tracing::{
    debug,
    info,
    warn,
};

use crate::{
    redis::RedisStateWriter,
    state::{
        BlockStateUpdate,
        build_trace_options,
    },
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;

pub struct StateWorker {
    provider: Arc<RootProvider>,
    redis: RedisStateWriter,
    trace_timeout: Duration,
}

impl StateWorker {
    pub fn new(
        provider: Arc<RootProvider>,
        redis: RedisStateWriter,
        trace_timeout: Duration,
    ) -> Self {
        Self {
            provider,
            redis,
            trace_timeout,
        }
    }

    pub async fn run(&self, start_override: Option<u64>) -> Result<()> {
        let mut next_block = self.compute_start_block(start_override).await?;
        loop {
            self.catch_up(&mut next_block).await?;
            if let Err(err) = self.stream_blocks(&mut next_block).await {
                warn!(error = %err, "block subscription ended, retrying");
                time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)).await;
            }
        }
    }

    async fn compute_start_block(&self, override_start: Option<u64>) -> Result<u64> {
        if let Some(block) = override_start {
            return Ok(block);
        }

        let current = self
            .redis
            .latest_block_number()
            .await
            .context("failed to read current block from redis")?;

        Ok(current.map(|b| b + 1).unwrap_or(0))
    }

    async fn catch_up(&self, next_block: &mut u64) -> Result<()> {
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

    async fn stream_blocks(&self, next_block: &mut u64) -> Result<()> {
        let subscription = self.provider.subscribe_blocks().await?;
        let mut stream = subscription.into_stream();

        while let Some(header) = stream.next().await {
            let Header { hash: _, inner, .. } = header;
            let block_number = inner.number;

            if block_number + 1 < *next_block {
                debug!(block_number, next_block, "skipping stale header");
                continue;
            }

            while *next_block <= block_number {
                self.process_block(*next_block).await?;
                *next_block += 1;
            }
        }

        Err(anyhow!("block subscription completed"))
    }

    async fn process_block(&self, block_number: u64) -> Result<()> {
        info!(block_number, "processing block");

        let block_id = BlockNumberOrTag::Number(block_number);
        let block = self
            .provider
            .get_block_by_number(block_id)
            .await?
            .with_context(|| format!("block {block_number} not found"))?;
        let block_hash = block.header.hash;

        let trace_options = build_trace_options(self.trace_timeout);
        let traces = self
            .provider
            .debug_trace_block_by_number(block_id, trace_options)
            .await
            .with_context(|| format!("failed to trace block {block_number}"))?;

        let update = BlockStateUpdate::from_traces(block_number, block_hash, traces);
        self.redis
            .commit_block(update)
            .await
            .with_context(|| format!("failed to persist block {block_number} to redis"))?;

        info!(block_number, "block persisted to redis");
        Ok(())
    }
}
