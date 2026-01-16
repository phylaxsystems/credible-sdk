//! Geth-specific trace parsing logic.
//!
//! Processes `prestateTracer` output from Geth nodes to identify which accounts
//! and storage slots were touched during block execution.
//!
//! ## Post-Cancun SELFDESTRUCT (EIP-6780)
//!
//! Post-Cancun, SELFDESTRUCT on an existing account only transfers balance. Code, storage, and nonce
//! remain intact. The tracer reports only the balance change, no deletion.
//!
//! ## State Fetching Strategy
//!
//! Geth's prestateTracer in diff mode only includes fields that CHANGED in the transaction.
//! This means if a contract's storage changes but balance/nonce/code don't, those fields
//! are omitted from both pre and post state.
//!
//! To ensure 100% state correctness, we:
//! 1. Use traces to identify WHICH accounts and storage slots were touched
//! 2. Read existing state from StateReader at block N-1
//! 3. Apply POST diff values on top of existing state
//! 4. Fall back to RPC if `StateReader` unavailable

use alloy::primitives::{
    Address,
    B256,
};
use alloy_rpc_types_trace::geth::{
    AccountState as GethAccountState,
    PreStateFrame,
    TraceResult,
};
use std::collections::{
    HashMap,
    HashSet,
};

/// Extract all addresses touched in the traces (from both pre and post)
pub fn extract_touched_addresses(traces: &[TraceResult]) -> HashSet<Address> {
    let mut addresses = HashSet::new();

    for trace in traces {
        if let TraceResult::Success {
            result: alloy_rpc_types_trace::geth::GethTrace::PreStateTracer(frame),
            ..
        } = trace
        {
            match frame {
                PreStateFrame::Default(prestate) => {
                    addresses.extend(prestate.0.keys().copied());
                }
                PreStateFrame::Diff(diff) => {
                    addresses.extend(diff.pre.keys().copied());
                    addresses.extend(diff.post.keys().copied());
                }
            }
        }
    }

    addresses
}

/// Extract storage slots that were touched for each address.
/// Returns raw (unhashed) slot keys - hashing is done when building final state.
pub fn extract_touched_storage(traces: &[TraceResult]) -> HashMap<Address, HashSet<B256>> {
    let mut storage_by_address: HashMap<Address, HashSet<B256>> = HashMap::new();

    for trace in traces {
        if let TraceResult::Success {
            result: alloy_rpc_types_trace::geth::GethTrace::PreStateTracer(frame),
            ..
        } = trace
        {
            match frame {
                PreStateFrame::Default(prestate) => {
                    for (addr, account) in &prestate.0 {
                        let slots = storage_by_address.entry(*addr).or_default();
                        slots.extend(account.storage.keys().copied());
                    }
                }
                PreStateFrame::Diff(diff) => {
                    for (addr, account) in &diff.pre {
                        let slots = storage_by_address.entry(*addr).or_default();
                        slots.extend(account.storage.keys().copied());
                    }
                    for (addr, account) in &diff.post {
                        let slots = storage_by_address.entry(*addr).or_default();
                        slots.extend(account.storage.keys().copied());
                    }
                }
            }
        }
    }

    storage_by_address
}

/// Extract the final POST state values for each address from diff mode traces.
///
/// When multiple transactions touch the same address, this accumulates changes
/// so the final state reflects all transactions in the block.
///
/// Returns a map of address to their POST state. Fields that are `None` in
/// `GethAccountState` indicate the field did not change during the block.
pub fn extract_post_state(traces: &[TraceResult]) -> HashMap<Address, GethAccountState> {
    let mut post_state: HashMap<Address, GethAccountState> = HashMap::new();

    for trace in traces {
        if let TraceResult::Success {
            result: alloy_rpc_types_trace::geth::GethTrace::PreStateTracer(frame),
            ..
        } = trace
        {
            if let PreStateFrame::Diff(diff) = frame {
                for (addr, account) in &diff.post {
                    let entry = post_state.entry(*addr).or_insert_with(|| {
                        GethAccountState {
                            balance: None,
                            nonce: None,
                            code: None,
                            storage: std::collections::BTreeMap::new(),
                        }
                    });

                    // Later transactions override earlier ones
                    if account.balance.is_some() {
                        entry.balance = account.balance;
                    }
                    if account.nonce.is_some() {
                        entry.nonce = account.nonce;
                    }
                    if account.code.is_some() {
                        entry.code.clone_from(&account.code);
                    }
                    // Merge storage - later values override
                    for (slot, value) in &account.storage {
                        entry.storage.insert(*slot, *value);
                    }
                }
            }
        }
    }

    post_state
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        B256,
        Bytes,
        U256,
    };
    use alloy_rpc_types_trace::geth::{
        AccountState as GethAccountState,
        DiffMode,
        GethTrace,
        PreStateFrame,
        PreStateMode,
    };
    use std::collections::BTreeMap;

    fn make_trace_result(frame: PreStateFrame) -> TraceResult {
        TraceResult::Success {
            result: GethTrace::PreStateTracer(frame),
            tx_hash: None,
        }
    }

    fn make_diff_trace(
        pre: BTreeMap<Address, GethAccountState>,
        post: BTreeMap<Address, GethAccountState>,
    ) -> TraceResult {
        make_trace_result(PreStateFrame::Diff(DiffMode { pre, post }))
    }

    fn make_default_trace(prestate: BTreeMap<Address, GethAccountState>) -> TraceResult {
        make_trace_result(PreStateFrame::Default(PreStateMode(prestate)))
    }

    // ==================== extract_touched_addresses tests ====================

    #[test]
    fn test_extract_addresses_from_diff_mode_pre_only() {
        let address = Address::from([0x42u8; 20]);

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(pre, BTreeMap::new())];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains(&address));
    }

    #[test]
    fn test_extract_addresses_from_diff_mode_post_only() {
        let address = Address::from([0x42u8; 20]);

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains(&address));
    }

    #[test]
    fn test_extract_addresses_from_diff_mode_both_pre_and_post() {
        let address1 = Address::from([0x01u8; 20]);
        let address2 = Address::from([0x02u8; 20]);

        let mut pre = BTreeMap::new();
        pre.insert(
            address1,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let mut post = BTreeMap::new();
        post.insert(
            address2,
            GethAccountState {
                balance: Some(U256::from(200)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(pre, post)];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&address1));
        assert!(addresses.contains(&address2));
    }

    #[test]
    fn test_extract_addresses_deduplicates_same_address_in_pre_and_post() {
        let address = Address::from([0x42u8; 20]);

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(200)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(pre, post)];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains(&address));
    }

    #[test]
    fn test_extract_addresses_from_default_mode() {
        let address1 = Address::from([0x01u8; 20]);
        let address2 = Address::from([0x02u8; 20]);

        let mut prestate = BTreeMap::new();
        prestate.insert(
            address1,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );
        prestate.insert(
            address2,
            GethAccountState {
                balance: Some(U256::from(200)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_default_trace(prestate)];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&address1));
        assert!(addresses.contains(&address2));
    }

    #[test]
    fn test_extract_addresses_from_multiple_traces() {
        let address1 = Address::from([0x01u8; 20]);
        let address2 = Address::from([0x02u8; 20]);
        let address3 = Address::from([0x03u8; 20]);

        let mut post1 = BTreeMap::new();
        post1.insert(
            address1,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let mut post2 = BTreeMap::new();
        post2.insert(
            address2,
            GethAccountState {
                balance: Some(U256::from(200)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );
        post2.insert(
            address3,
            GethAccountState {
                balance: Some(U256::from(300)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![
            make_diff_trace(BTreeMap::new(), post1),
            make_diff_trace(BTreeMap::new(), post2),
        ];
        let addresses = extract_touched_addresses(&traces);

        assert_eq!(addresses.len(), 3);
        assert!(addresses.contains(&address1));
        assert!(addresses.contains(&address2));
        assert!(addresses.contains(&address3));
    }

    #[test]
    fn test_extract_addresses_empty_traces() {
        let traces: Vec<TraceResult> = vec![];
        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_extract_addresses_empty_diff() {
        let traces = vec![make_diff_trace(BTreeMap::new(), BTreeMap::new())];
        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_extract_addresses_ignores_failed_traces() {
        let traces = vec![TraceResult::Error {
            error: "execution reverted".to_string(),
            tx_hash: None,
        }];
        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.is_empty());
    }

    // ==================== extract_touched_storage tests ====================

    #[test]
    fn test_extract_storage_from_diff_mode_pre_only() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut pre_storage = BTreeMap::new();
        pre_storage.insert(slot, B256::from([0xAAu8; 32]));

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: pre_storage,
            },
        );

        let traces = vec![make_diff_trace(pre, BTreeMap::new())];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 1);
        assert!(storage.get(&address).unwrap().contains(&slot));
    }

    #[test]
    fn test_extract_storage_from_diff_mode_post_only() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut post_storage = BTreeMap::new();
        post_storage.insert(slot, B256::from([0xBBu8; 32]));

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 1);
        assert!(storage.get(&address).unwrap().contains(&slot));
    }

    #[test]
    fn test_extract_storage_combines_pre_and_post_slots() {
        let address = Address::from([0x42u8; 20]);
        let slot1 = B256::from([0x01u8; 32]);
        let slot2 = B256::from([0x02u8; 32]);

        let mut pre_storage = BTreeMap::new();
        pre_storage.insert(slot1, B256::from([0xAAu8; 32]));

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: pre_storage,
            },
        );

        let mut post_storage = BTreeMap::new();
        post_storage.insert(slot2, B256::from([0xBBu8; 32]));

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let traces = vec![make_diff_trace(pre, post)];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 1);
        let slots = storage.get(&address).unwrap();
        assert_eq!(slots.len(), 2);
        assert!(slots.contains(&slot1));
        assert!(slots.contains(&slot2));
    }

    #[test]
    fn test_extract_storage_deduplicates_same_slot_in_pre_and_post() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut pre_storage = BTreeMap::new();
        pre_storage.insert(slot, B256::from([0xAAu8; 32]));

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: pre_storage,
            },
        );

        let mut post_storage = BTreeMap::new();
        post_storage.insert(slot, B256::from([0xBBu8; 32]));

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let traces = vec![make_diff_trace(pre, post)];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 1);
        assert_eq!(storage.get(&address).unwrap().len(), 1);
        assert!(storage.get(&address).unwrap().contains(&slot));
    }

    #[test]
    fn test_extract_storage_multiple_addresses() {
        let address1 = Address::from([0x01u8; 20]);
        let address2 = Address::from([0x02u8; 20]);
        let slot1 = B256::from([0x01u8; 32]);
        let slot2 = B256::from([0x02u8; 32]);

        let mut storage1 = BTreeMap::new();
        storage1.insert(slot1, B256::from([0xAAu8; 32]));

        let mut storage2 = BTreeMap::new();
        storage2.insert(slot2, B256::from([0xBBu8; 32]));

        let mut post = BTreeMap::new();
        post.insert(
            address1,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage1,
            },
        );
        post.insert(
            address2,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage2,
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 2);
        assert!(storage.get(&address1).unwrap().contains(&slot1));
        assert!(storage.get(&address2).unwrap().contains(&slot2));
    }

    #[test]
    fn test_extract_storage_from_multiple_traces() {
        let address = Address::from([0x42u8; 20]);
        let slot1 = B256::from([0x01u8; 32]);
        let slot2 = B256::from([0x02u8; 32]);

        let mut storage1 = BTreeMap::new();
        storage1.insert(slot1, B256::from([0xAAu8; 32]));

        let mut post1 = BTreeMap::new();
        post1.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage1,
            },
        );

        let mut storage2 = BTreeMap::new();
        storage2.insert(slot2, B256::from([0xBBu8; 32]));

        let mut post2 = BTreeMap::new();
        post2.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage2,
            },
        );

        let traces = vec![
            make_diff_trace(BTreeMap::new(), post1),
            make_diff_trace(BTreeMap::new(), post2),
        ];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.len(), 1);
        let slots = storage.get(&address).unwrap();
        assert_eq!(slots.len(), 2);
        assert!(slots.contains(&slot1));
        assert!(slots.contains(&slot2));
    }

    #[test]
    fn test_extract_storage_from_default_mode() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);

        let mut storage = BTreeMap::new();
        storage.insert(slot, B256::from([0xAAu8; 32]));

        let mut prestate = BTreeMap::new();
        prestate.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage,
            },
        );

        let traces = vec![make_default_trace(prestate)];
        let result = extract_touched_storage(&traces);

        assert_eq!(result.len(), 1);
        assert!(result.get(&address).unwrap().contains(&slot));
    }

    #[test]
    fn test_extract_storage_ignores_failed_traces() {
        let traces = vec![TraceResult::Error {
            error: "execution reverted".to_string(),
            tx_hash: None,
        }];
        let storage = extract_touched_storage(&traces);
        assert!(storage.is_empty());
    }

    // ==================== extract_post_state tests ====================

    #[test]
    fn test_extract_post_state_single_transaction() {
        let address = Address::from([0x42u8; 20]);

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(5),
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];
        let post_state = extract_post_state(&traces);

        assert_eq!(post_state.len(), 1);
        let account = post_state.get(&address).unwrap();
        assert_eq!(account.balance, Some(U256::from(100)));
        assert_eq!(account.nonce, Some(5));
        assert!(account.code.is_none());
    }

    #[test]
    fn test_extract_post_state_multiple_transactions_same_address() {
        let address = Address::from([0x42u8; 20]);

        let mut post1 = BTreeMap::new();
        post1.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(1),
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let mut post2 = BTreeMap::new();
        post2.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(200)),
                nonce: Some(2),
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![
            make_diff_trace(BTreeMap::new(), post1),
            make_diff_trace(BTreeMap::new(), post2),
        ];
        let post_state = extract_post_state(&traces);

        assert_eq!(post_state.len(), 1);
        let account = post_state.get(&address).unwrap();
        // Later transaction should override
        assert_eq!(account.balance, Some(U256::from(200)));
        assert_eq!(account.nonce, Some(2));
    }

    #[test]
    fn test_extract_post_state_partial_updates() {
        let address = Address::from([0x42u8; 20]);

        // First tx changes balance only
        let mut post1 = BTreeMap::new();
        post1.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        // Second tx changes nonce only
        let mut post2 = BTreeMap::new();
        post2.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: Some(5),
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![
            make_diff_trace(BTreeMap::new(), post1),
            make_diff_trace(BTreeMap::new(), post2),
        ];
        let post_state = extract_post_state(&traces);

        let account = post_state.get(&address).unwrap();
        // Should have both values accumulated
        assert_eq!(account.balance, Some(U256::from(100)));
        assert_eq!(account.nonce, Some(5));
    }

    #[test]
    fn test_extract_post_state_storage_merge() {
        let address = Address::from([0x42u8; 20]);
        let slot1 = B256::from([0x01u8; 32]);
        let slot2 = B256::from([0x02u8; 32]);

        let mut storage1 = BTreeMap::new();
        storage1.insert(slot1, B256::from([0xAAu8; 32]));

        let mut post1 = BTreeMap::new();
        post1.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage1,
            },
        );

        let mut storage2 = BTreeMap::new();
        storage2.insert(slot2, B256::from([0xBBu8; 32]));

        let mut post2 = BTreeMap::new();
        post2.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: storage2,
            },
        );

        let traces = vec![
            make_diff_trace(BTreeMap::new(), post1),
            make_diff_trace(BTreeMap::new(), post2),
        ];
        let post_state = extract_post_state(&traces);

        let account = post_state.get(&address).unwrap();
        assert_eq!(account.storage.len(), 2);
        assert_eq!(account.storage.get(&slot1), Some(&B256::from([0xAAu8; 32])));
        assert_eq!(account.storage.get(&slot2), Some(&B256::from([0xBBu8; 32])));
    }

    #[test]
    fn test_extract_post_state_ignores_default_mode() {
        let address = Address::from([0x42u8; 20]);

        let mut prestate = BTreeMap::new();
        prestate.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(1),
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_default_trace(prestate)];
        let post_state = extract_post_state(&traces);

        // Default mode has no POST, so should be empty
        assert!(post_state.is_empty());
    }

    #[test]
    fn test_extract_post_state_ignores_failed_traces() {
        let traces = vec![TraceResult::Error {
            error: "execution reverted".to_string(),
            tx_hash: None,
        }];
        let post_state = extract_post_state(&traces);
        assert!(post_state.is_empty());
    }

    #[test]
    fn test_bug_demonstration_storage_only_change_extracts_address() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let old_value = B256::from([0xAAu8; 32]);
        let new_value = B256::from([0xBBu8; 32]);

        let mut pre_storage = BTreeMap::new();
        pre_storage.insert(slot, old_value);

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: pre_storage,
            },
        );

        let mut post_storage = BTreeMap::new();
        post_storage.insert(slot, new_value);

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let traces = vec![make_diff_trace(pre, post)];

        let addresses = extract_touched_addresses(&traces);
        assert!(
            addresses.contains(&address),
            "Should identify address even with storage-only changes"
        );

        let storage = extract_touched_storage(&traces);
        assert!(
            storage.get(&address).unwrap().contains(&slot),
            "Should identify touched storage slot"
        );

        let post_state = extract_post_state(&traces);
        let account = post_state.get(&address).unwrap();
        assert_eq!(account.storage.get(&slot), Some(&new_value));
        // Balance/nonce/code should be None (not changed)
        assert!(account.balance.is_none());
        assert!(account.nonce.is_none());
        assert!(account.code.is_none());
    }

    #[test]
    fn test_selfdestruct_scenario() {
        let address = Address::from([0x42u8; 20]);

        let mut pre = BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(pre, post)];

        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.contains(&address));

        let post_state = extract_post_state(&traces);
        assert_eq!(post_state.get(&address).unwrap().balance, Some(U256::ZERO));
    }

    #[test]
    fn test_new_contract_creation() {
        let address = Address::from([0x42u8; 20]);
        let code = Bytes::from(vec![0x60, 0x80, 0x60, 0x40]);

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(0)),
                nonce: Some(1),
                code: Some(code.clone()),
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];

        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.contains(&address));

        let post_state = extract_post_state(&traces);
        let account = post_state.get(&address).unwrap();
        assert_eq!(account.code, Some(code));
    }

    #[test]
    fn test_many_storage_slots() {
        let address = Address::from([0x42u8; 20]);

        let mut post_storage = BTreeMap::new();
        for i in 0..100 {
            let mut slot_bytes = [0u8; 32];
            slot_bytes[31] = i;
            post_storage.insert(B256::from(slot_bytes), B256::from([0xAAu8; 32]));
        }

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];
        let storage = extract_touched_storage(&traces);

        assert_eq!(storage.get(&address).unwrap().len(), 100);
    }

    #[test]
    fn test_zero_address_handled() {
        let zero_address = Address::ZERO;

        let mut post = BTreeMap::new();
        post.insert(
            zero_address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];

        let addresses = extract_touched_addresses(&traces);
        assert!(addresses.contains(&zero_address));
    }

    #[test]
    fn test_zero_storage_slot_handled() {
        let address = Address::from([0x42u8; 20]);
        let zero_slot = B256::ZERO;

        let mut storage = BTreeMap::new();
        storage.insert(zero_slot, B256::from([0xAAu8; 32]));

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage,
            },
        );

        let traces = vec![make_diff_trace(BTreeMap::new(), post)];

        let storage_map = extract_touched_storage(&traces);
        assert!(storage_map.get(&address).unwrap().contains(&zero_slot));
    }

    #[test]
    fn test_mixed_success_and_failed_transactions() {
        let address = Address::from([0x42u8; 20]);

        let mut post = BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let traces = vec![
            TraceResult::Error {
                error: "reverted".to_string(),
                tx_hash: None,
            },
            make_diff_trace(BTreeMap::new(), post),
            TraceResult::Error {
                error: "out of gas".to_string(),
                tx_hash: None,
            },
        ];

        let addresses = extract_touched_addresses(&traces);
        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains(&address));
    }
}
