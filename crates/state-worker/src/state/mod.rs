//! Helpers that collapse per-transaction debug traces into per-account commits.

pub mod geth;
pub mod geth_provider;
pub mod parity;
pub mod parity_provider;
#[cfg(test)]
mod tests;

use alloy::primitives::{
    B256,
    U256,
    keccak256,
};
use alloy_provider::RootProvider;
use alloy_rpc_types_trace::parity::TraceResultsWithTransactionHash;
use anyhow::Result;
use async_trait::async_trait;
use revm::primitives::KECCAK_EMPTY;
use state_store::common::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

/// Trait for fetching block state from different trace providers
#[async_trait]
pub trait TraceProvider: Send + Sync {
    async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate>;
}

#[derive(Default)]
pub(crate) struct AccountSnapshot {
    pub(crate) balance: Option<U256>,
    pub(crate) nonce: Option<u64>,
    pub(crate) code: Option<Vec<u8>>,
    pub(crate) storage_updates: HashMap<U256, U256>,
    pub(crate) touched: bool,
    pub(crate) deleted: bool,
}

impl AccountSnapshot {
    pub(crate) fn finalize(mut self, address: AddressHash) -> Option<AccountState> {
        if !self.touched && !self.deleted && self.storage_updates.is_empty() {
            return None;
        }

        let mut code_bytes = self.code.take().unwrap_or_default();
        let (balance, nonce) = if self.deleted {
            code_bytes.clear();
            (U256::ZERO, 0_u64)
        } else {
            (
                self.balance.unwrap_or_default(),
                self.nonce.unwrap_or_default(),
            )
        };

        let code_hash = if code_bytes.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code_bytes)
        };

        let code = if self.deleted || code_bytes.is_empty() {
            None
        } else {
            Some(code_bytes)
        };

        Some(AccountState {
            address_hash: address,
            balance,
            nonce,
            code_hash,
            code,
            storage: self.storage_updates,
            deleted: self.deleted,
        })
    }
}

/// Builder for `BlockStateUpdate`
pub struct BlockStateUpdateBuilder;

impl BlockStateUpdateBuilder {
    pub fn from_parity_traces(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        traces: Vec<TraceResultsWithTransactionHash>,
    ) -> BlockStateUpdate {
        let accounts = parity::process_parity_traces(traces);
        BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        }
    }

    pub fn from_geth_traces(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        traces: Vec<alloy_rpc_types_trace::geth::TraceResult>,
    ) -> BlockStateUpdate {
        let accounts = geth::process_geth_traces(traces);
        BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        }
    }

    pub fn from_accounts(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        accounts: Vec<AccountState>,
    ) -> BlockStateUpdate {
        BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        }
    }
}

/// Create a trace provider based on the provider type
pub fn create_trace_provider(
    provider_type: crate::cli::ProviderType,
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
) -> Box<dyn TraceProvider> {
    match provider_type {
        crate::cli::ProviderType::Geth => {
            Box::new(geth_provider::GethTraceProvider::new(
                provider,
                trace_timeout,
            ))
        }
        crate::cli::ProviderType::Parity => {
            Box::new(parity_provider::ParityTraceProvider::new(provider))
        }
    }
}
