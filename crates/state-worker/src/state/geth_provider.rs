//! Geth trace provider implementation

use super::{
    BlockStateUpdateBuilder,
    TraceProvider,
};
use alloy::rpc::types::BlockNumberOrTag;
use alloy_provider::{
    Provider,
    RootProvider,
    ext::DebugApi,
};
use alloy_rpc_types_trace::geth::{
    GethDebugTracerType,
    GethDebugTracingOptions,
    GethDefaultTracingOptions,
    PreStateConfig,
};
use anyhow::{
    Context,
    Result,
};
use async_trait::async_trait;
use state_store::BlockStateUpdate;
use std::{
    sync::Arc,
    time::Duration,
};

pub struct GethTraceProvider {
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
}

impl GethTraceProvider {
    pub fn new(provider: Arc<RootProvider>, trace_timeout: Duration) -> Self {
        Self {
            provider,
            trace_timeout,
        }
    }

    fn build_trace_options(&self) -> GethDebugTracingOptions {
        let prestate_config = PreStateConfig {
            diff_mode: Some(true),
            disable_code: None,
            disable_storage: None,
        };

        GethDebugTracingOptions {
            config: GethDefaultTracingOptions {
                enable_memory: Some(false),
                disable_stack: Some(true),
                disable_storage: Some(false),
                enable_return_data: Some(false),
                debug: Some(false),
                ..Default::default()
            },
            tracer: Some(GethDebugTracerType::BuiltInTracer(
                alloy_rpc_types_trace::geth::GethDebugBuiltInTracerType::PreStateTracer,
            )),
            tracer_config: prestate_config.into(),
            timeout: None,
        }
        .with_timeout(self.trace_timeout)
    }
}

#[async_trait]
impl TraceProvider for GethTraceProvider {
    async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
        let block_id = BlockNumberOrTag::Number(block_number);

        let block = self
            .provider
            .get_block_by_number(block_id)
            .await?
            .with_context(|| format!("block {block_number} not found"))?;

        let block_hash = block.header.hash;
        let state_root = block.header.state_root;

        let trace_options = self.build_trace_options();
        let traces = self
            .provider
            .debug_trace_block_by_number(block_id, trace_options)
            .await
            .with_context(|| format!("failed to trace block {block_number}"))?;

        Ok(BlockStateUpdateBuilder::from_geth_traces(
            block_number,
            block_hash,
            state_root,
            traces,
        ))
    }
}
