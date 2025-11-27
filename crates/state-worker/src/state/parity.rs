//! Parity-specific trace parsing logic.
//!
//! Processes `StateDiff` traces from Parity/OpenEthereum nodes and converts
//! them into our normalized `AccountState` format.

use alloy::primitives::{
    U256,
    keccak256,
};
use alloy_rpc_types_trace::parity::{
    Delta,
    StateDiff,
    TraceResultsWithTransactionHash,
};
use revm::primitives::KECCAK_EMPTY;
use state_store::common::{
    AccountState,
    AddressHash,
};
use std::collections::HashMap;

#[derive(Default)]
struct AccountSnapshot {
    /// Most recent balance observed in the trace stream; filled lazily.
    balance: Option<U256>,
    /// Most recent nonce observed in the trace stream; filled lazily.
    nonce: Option<u64>,
    /// Optional bytecode blob; preserved so we can emit code updates exactly once.
    code: Option<Vec<u8>>,
    /// Sparse storage slots touched by the block (slot -> value).
    storage_updates: HashMap<U256, U256>,
    /// Whether we saw a post-state for this account.
    touched: bool,
    /// Whether the account was deleted by the block.
    deleted: bool,
}

impl AccountSnapshot {
    /// Convert the accumulated snapshot into a commit payload if anything
    /// meaningful changed. Returning `None` allows callsites to drop untouched
    /// snapshots without extra bookkeeping.
    fn finalize(mut self, address: AddressHash) -> Option<AccountState> {
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

        let storage = self.storage_updates;

        Some(AccountState {
            address_hash: address,
            balance,
            nonce,
            code_hash,
            code,
            storage,
            deleted: self.deleted,
        })
    }
}

/// Convert Parity trace results into `AccountState` records
pub fn process_parity_traces(traces: Vec<TraceResultsWithTransactionHash>) -> Vec<AccountState> {
    let mut accounts: HashMap<AddressHash, AccountSnapshot> = HashMap::new();

    for trace in traces {
        if let Some(state_diff) = trace.full_trace.state_diff {
            process_diff(&mut accounts, &state_diff);
        }
    }

    accounts
        .into_iter()
        .filter_map(|(address, snapshot)| snapshot.finalize(address))
        .collect()
}

/// Merge a single transaction diff into the pending account snapshots.
fn process_diff(accounts: &mut HashMap<AddressHash, AccountSnapshot>, state_diff: &StateDiff) {
    for (address, account_diff) in &state_diff.0 {
        let snapshot = accounts.entry((*address).into()).or_default();

        // Handle balance changes
        match &account_diff.balance {
            Delta::Unchanged => {
                // No change to balance
            }
            Delta::Added(balance) => {
                // Account was created with this balance
                snapshot.balance = Some(*balance);
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Balance removed means account was deleted
                snapshot.balance = Some(U256::ZERO);
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Balance changed from one value to another
                snapshot.balance = Some(change.to);
                snapshot.touched = true;
            }
        }

        // Handle nonce changes
        match &account_diff.nonce {
            Delta::Unchanged => {
                // No change to nonce
            }
            Delta::Added(nonce) => {
                // Account was created with this nonce
                snapshot.nonce = Some(nonce.to::<u64>());
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Nonce removed means account was deleted
                snapshot.nonce = Some(0);
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Nonce changed from one value to another
                snapshot.nonce = Some(change.to.to::<u64>());
                snapshot.touched = true;
            }
        }

        // Handle code changes
        match &account_diff.code {
            Delta::Unchanged => {
                // No change to code
            }
            Delta::Added(code) => {
                // Contract was deployed
                snapshot.code = Some(code.to_vec());
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Code removed means contract was deleted
                snapshot.code = Some(Vec::new());
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Code changed (unusual but theoretically possible)
                snapshot.code = Some(change.to.to_vec());
                snapshot.touched = true;
            }
        }

        // Handle storage changes
        for (slot, storage_delta) in &account_diff.storage {
            let slot_hash = keccak256(slot.0);
            let slot_key = U256::from_be_bytes(slot_hash.into());
            match storage_delta {
                Delta::Unchanged => {
                    // No change to this storage slot
                }
                Delta::Added(value) => {
                    // New storage slot was set
                    snapshot
                        .storage_updates
                        .insert(slot_key, U256::from_be_bytes(value.0));
                    snapshot.touched = true;
                }
                Delta::Removed(_) => {
                    // Storage slot was deleted (set to zero)
                    snapshot.storage_updates.insert(slot_key, U256::ZERO);
                    snapshot.touched = true;
                }
                Delta::Changed(change) => {
                    // Storage slot value changed
                    snapshot
                        .storage_updates
                        .insert(slot_key, U256::from_be_bytes(change.to.0));
                    snapshot.touched = true;
                }
            }
        }

        // Check if the account was fully destroyed
        // This happens when balance, nonce, and code are all Removed
        let is_destroyed = matches!(account_diff.balance, Delta::Removed(_))
            && matches!(account_diff.nonce, Delta::Removed(_))
            && matches!(account_diff.code, Delta::Removed(_));

        if is_destroyed {
            snapshot.deleted = true;
            // Zero out all storage that was touched
            for value in snapshot.storage_updates.values_mut() {
                *value = U256::ZERO;
            }
        }
    }
}
