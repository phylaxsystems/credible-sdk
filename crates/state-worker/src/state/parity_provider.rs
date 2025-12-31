//! Parity trace provider implementation

use super::{
    BlockStateUpdateBuilder,
    TraceProvider,
};
use alloy::{
    eips::BlockId,
    rpc::types::BlockNumberOrTag,
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
};
use async_trait::async_trait;
use state_store::BlockStateUpdate;
use std::sync::Arc;

pub struct ParityTraceProvider {
    provider: Arc<RootProvider>,
}

impl ParityTraceProvider {
    pub fn new(provider: Arc<RootProvider>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl TraceProvider for ParityTraceProvider {
    async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
        let block_id = BlockNumberOrTag::Number(block_number);

        let block = self
            .provider
            .get_block_by_number(block_id)
            .await?
            .with_context(|| format!("block {block_number} not found"))?;

        let block_hash = block.header.hash;
        let state_root = block.header.state_root;

        let traces = self
            .provider
            .trace_replay_block_transactions(BlockId::Number(block_number.into()))
            .trace_type(TraceType::StateDiff)
            .await
            .with_context(|| format!("failed to trace block {block_number}"))?;

        Ok(BlockStateUpdateBuilder::from_parity_traces(
            block_number,
            block_hash,
            state_root,
            traces,
        ))
    }
}
