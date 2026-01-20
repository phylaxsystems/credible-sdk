//! Helpers that collapse per-transaction debug traces into per-account commits.
//!
//! ## Account Deletion Semantics
//!
//! Per EIP-1052 (EXTCODEHASH), there are three distinct account states:
//! - **Non-existent**: Never created or fully deleted → `code_hash = 0x0`
//! - **Exists, no code**: EOA or empty contract → `code_hash = keccak256("")`
//! - **Exists, has code**: Contract with bytecode → `code_hash = keccak256(code)`
//!
//! ### Pre-Cancun Behavior
//! SELFDESTRUCT always fully deletes the account (balance, nonce, code, storage).
//! The account becomes non-existent → `code_hash = 0x0`
//!
//! ### Post-Cancun Behavior (EIP-6780)
//! - **Same-tx SELFDESTRUCT**: Full deletion (same as pre-Cancun) → `code_hash = 0x0`
//! - **Different-tx SELFDESTRUCT**: Account persists, only balance zeroed.
//!   The trace won't show this as a deletion, just a balance change.
//!
//! The trace provider (Geth) already implements EIP-6780 correctly,
//! so we just need to interpret the traces accurately.

pub mod geth;
pub mod geth_provider;
#[cfg(test)]
mod tests;

use alloy::primitives::{
    B256,
    Bytes,
    U256,
    keccak256,
};
use alloy_provider::RootProvider;
use anyhow::{
    Result,
    anyhow,
};
use async_trait::async_trait;
use revm::primitives::KECCAK_EMPTY;
use state_store::{
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
    pub(crate) code: Option<Bytes>,
    pub(crate) storage_updates: HashMap<B256, U256>,
    pub(crate) touched: bool,
    pub(crate) deleted: bool,
}

impl AccountSnapshot {
    pub(crate) fn finalize(mut self, address: AddressHash) -> Option<AccountState> {
        if !self.touched && self.storage_updates.is_empty() && !self.deleted {
            return None;
        }

        if self.deleted {
            return Some(AccountState {
                address_hash: address,
                balance: U256::ZERO,
                nonce: 0,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: true,
            });
        }

        let code_bytes = self.code.take().unwrap_or_default();

        // Existing account without code: keccak256("") per EIP-1052
        // Existing account with code: keccak256(code)
        let code_hash = if code_bytes.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code_bytes)
        };

        let code = if code_bytes.is_empty() {
            None
        } else {
            Some(code_bytes)
        };

        Some(AccountState {
            address_hash: address,
            balance: self.balance.unwrap_or_default(),
            nonce: self.nonce.unwrap_or_default(),
            code_hash,
            code,
            storage: self.storage_updates,
            deleted: false,
        })
    }
}

/// Builder for `BlockStateUpdate`
pub struct BlockStateUpdateBuilder;

impl BlockStateUpdateBuilder {
    pub fn from_geth_traces(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        traces: Vec<alloy_rpc_types_trace::geth::TraceResult>,
    ) -> Result<BlockStateUpdate> {
        let accounts = geth::process_geth_traces(traces).map_err(|(index, tx_hash, err)| {
            anyhow!(
                "block {block_number}: tx trace failed at index {index} (tx: {tx_hash:?}): {err}"
            )
        })?;

        Ok(BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        })
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

/// Create a trace provider.
pub fn create_trace_provider(
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
) -> Box<dyn TraceProvider> {
    Box::new(geth_provider::GethTraceProvider::new(
        provider,
        trace_timeout,
    ))
}

#[cfg(test)]
mod account_deletion_tests {
    use super::*;

    fn test_address() -> AddressHash {
        AddressHash::new([0u8; 20])
    }

    #[test]
    fn existing_account_without_code_has_keccak_empty() {
        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: Some(1),
            code: None,
            storage_updates: HashMap::new(),
            touched: true,
            deleted: false,
        };

        let state = snapshot.finalize(test_address()).unwrap();

        // Per EIP-1052: existing account without code has code_hash = keccak256("")
        assert_eq!(state.code_hash, KECCAK_EMPTY);
        assert!(!state.deleted);
    }

    #[test]
    fn existing_contract_has_code_hash() {
        let code = vec![0x60, 0x80, 0x60, 0x40];
        let expected_hash = keccak256(&code);

        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: Some(1),
            code: Some(Bytes::from_iter(code)),
            storage_updates: HashMap::new(),
            touched: true,
            deleted: false,
        };

        let state = snapshot.finalize(test_address()).unwrap();

        assert_eq!(state.code_hash, expected_hash);
        assert!(!state.deleted);
    }
}
