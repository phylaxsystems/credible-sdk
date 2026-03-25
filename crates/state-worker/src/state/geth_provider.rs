//! Geth trace provider implementation

use super::{
    BlockStateUpdateBuilder,
    TraceProvider,
    error::TraceProviderError,
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
use async_trait::async_trait;
use mdbx::BlockStateUpdate;
use std::{
    sync::Arc,
    time::Duration,
};

pub struct GethTraceProvider {
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
}

impl GethTraceProvider {
    #[must_use]
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
    async fn fetch_block_state(
        &self,
        block_number: u64,
    ) -> Result<BlockStateUpdate, TraceProviderError> {
        let block_id = BlockNumberOrTag::Number(block_number);

        let block = self
            .provider
            .get_block_by_number(block_id)
            .await
            .map_err(|err| {
                TraceProviderError::FetchBlock {
                    block_number,
                    message: err.to_string(),
                }
            })?
            .ok_or(TraceProviderError::MissingBlock { block_number })?;

        let block_hash = block.header.hash;
        let state_root = block.header.state_root;

        let trace_options = self.build_trace_options();
        let traces = self
            .provider
            .debug_trace_block_by_number(block_id, trace_options)
            .await
            .map_err(|err| {
                TraceProviderError::TraceBlock {
                    block_number,
                    message: err.to_string(),
                }
            })?;

        BlockStateUpdateBuilder::from_geth_traces(block_number, block_hash, state_root, traces)
    }
}
