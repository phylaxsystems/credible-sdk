//! Parity-specific trace parsing logic.
//!
//! Processes `StateDiff` traces from Parity/OpenEthereum nodes and converts
//! them into our normalized `AccountState` format.
//!
//! ## Post-Cancun SELFDESTRUCT (EIP-6780)
//!
//! Post-Cancun, SELFDESTRUCT on an existing account only transfers balance. Code, storage, and nonce
//! remain intact. The tracer reports only the balance change, no deletion.

use super::AccountSnapshot;
use alloy::primitives::{
    U256,
    keccak256,
};
use alloy_rpc_types_trace::parity::{
    Delta,
    StateDiff,
    TraceResultsWithTransactionHash,
};
use state_store::{
    AccountState,
    AddressHash,
};
use std::collections::HashMap;

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
            Delta::Unchanged => {}
            Delta::Added(balance) => {
                snapshot.balance = Some(*balance);
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                snapshot.balance = None;
                snapshot.touched = true;
            }
            Delta::Changed(change) => {
                snapshot.balance = Some(change.to);
                snapshot.touched = true;
            }
        }

        // Handle nonce changes
        match &account_diff.nonce {
            Delta::Unchanged => {}
            Delta::Added(nonce) => {
                snapshot.nonce = Some(nonce.to::<u64>());
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                snapshot.nonce = None;
                snapshot.touched = true;
            }
            Delta::Changed(change) => {
                snapshot.nonce = Some(change.to.to::<u64>());
                snapshot.touched = true;
            }
        }

        // Handle code changes
        match &account_diff.code {
            Delta::Unchanged => {}
            Delta::Added(code) => {
                snapshot.code = Some(code.clone());
                snapshot.code_changed = true;
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                snapshot.code = None;
                snapshot.code_changed = true;
                snapshot.touched = true;
            }
            Delta::Changed(change) => {
                snapshot.code = Some(change.to.clone());
                snapshot.code_changed = true;
                snapshot.touched = true;
            }
        }

        // Handle storage changes
        for (slot, storage_delta) in &account_diff.storage {
            let slot_hash = keccak256(slot.0);
            match storage_delta {
                Delta::Unchanged => {}
                Delta::Added(value) => {
                    snapshot
                        .storage_updates
                        .insert(slot_hash, U256::from_be_bytes(value.0));
                    snapshot.touched = true;
                }
                Delta::Removed(_) => {
                    snapshot.storage_updates.insert(slot_hash, U256::ZERO);
                    snapshot.touched = true;
                }
                Delta::Changed(change) => {
                    snapshot
                        .storage_updates
                        .insert(slot_hash, U256::from_be_bytes(change.to.0));
                    snapshot.touched = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        B256,
        Bytes,
        U64,
    };
    use alloy_rpc_types_trace::parity::{
        AccountDiff,
        ChangedType,
        TraceResults,
        TraceResultsWithTransactionHash,
    };
    use std::collections::BTreeMap;

    fn make_trace_result(state_diff: StateDiff) -> TraceResultsWithTransactionHash {
        TraceResultsWithTransactionHash {
            full_trace: TraceResults {
                output: Bytes::new(),
                trace: vec![],
                vm_trace: None,
                state_diff: Some(state_diff),
            },
            transaction_hash: B256::ZERO,
        }
    }

    #[test]
    fn test_selfdestruct_only_balance_changes() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(1000),
                    to: U256::ZERO,
                }),
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert_eq!(results.len(), 1);
        assert!(!results[0].deleted);
        assert_eq!(results[0].balance, U256::ZERO);
    }

    #[test]
    fn test_storage_added() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let value = B256::from([0xAAu8; 32]);

        let mut storage = std::collections::BTreeMap::new();
        storage.insert(slot, Delta::Added(value));

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Added(U256::from(100)),
                nonce: Delta::Added(U64::from(1)),
                code: Delta::Unchanged,
                storage,
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            results[0].storage.get(&slot_hash),
            Some(&U256::from_be_bytes(value.0))
        );
    }

    #[test]
    fn test_storage_changed() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut storage = std::collections::BTreeMap::new();
        storage.insert(
            slot,
            Delta::Changed(ChangedType {
                from: B256::from([0xAAu8; 32]),
                to: B256::from([0xBBu8; 32]),
            }),
        );

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Unchanged,
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage,
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            results[0].storage.get(&slot_hash),
            Some(&U256::from_be_bytes(B256::from([0xBBu8; 32]).0))
        );
    }

    #[test]
    fn test_storage_removed_becomes_zero() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut storage = std::collections::BTreeMap::new();
        storage.insert(slot, Delta::Removed(B256::from([0xAAu8; 32])));

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(100),
                    to: U256::from(50),
                }),
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage,
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(results[0].storage.get(&slot_hash), Some(&U256::ZERO));
    }

    #[test]
    fn test_new_account_created() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Added(U256::from(1000)),
                nonce: Delta::Added(U64::from(0)),
                code: Delta::Added(Bytes::from(vec![0x60, 0x80, 0x60, 0x40])),
                storage: BTreeMap::default(),
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert_eq!(results.len(), 1);
        assert!(!results[0].deleted);
        assert_eq!(results[0].balance, U256::from(1000));
        assert_eq!(results[0].nonce, 0);
        assert!(results[0].code.is_some());
    }

    #[test]
    fn test_multiple_transactions_same_account() {
        let address = Address::from([0x42u8; 20]);

        let mut state_diff1 = StateDiff::default();
        state_diff1.0.insert(
            address,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(1000),
                    to: U256::from(900),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(5),
                    to: U64::from(6),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let mut state_diff2 = StateDiff::default();
        state_diff2.0.insert(
            address,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(900),
                    to: U256::from(800),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(6),
                    to: U64::from(7),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let results = process_parity_traces(vec![
            make_trace_result(state_diff1),
            make_trace_result(state_diff2),
        ]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].balance, U256::from(800));
        assert_eq!(results[0].nonce, 7);
    }

    #[test]
    fn test_unchanged_account_not_included() {
        let mut state_diff = StateDiff::default();
        let address = Address::from([0x42u8; 20]);

        state_diff.0.insert(
            address,
            AccountDiff {
                balance: Delta::Unchanged,
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let results = process_parity_traces(vec![make_trace_result(state_diff)]);

        assert!(results.is_empty());
    }
}
