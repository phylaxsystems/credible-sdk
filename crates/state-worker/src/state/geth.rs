//! Geth-specific trace parsing logic.
//!
//! Processes `prestateTracer` output from Geth nodes and converts
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
use alloy_rpc_types_trace::geth::{
    PreStateFrame,
    TraceResult,
};
use state_store::{
    AccountState,
    AddressHash,
};
use std::collections::HashMap;

pub fn process_geth_traces(traces: Vec<TraceResult>) -> Vec<AccountState> {
    let mut accounts: HashMap<AddressHash, AccountSnapshot> = HashMap::new();

    for trace in traces {
        if let TraceResult::Success {
            result: alloy_rpc_types_trace::geth::GethTrace::PreStateTracer(frame),
            ..
        } = trace
        {
            process_frame(&mut accounts, frame);
        }
    }

    accounts
        .into_iter()
        .filter_map(|(address, snapshot)| snapshot.finalize(address))
        .collect()
}

fn process_frame(accounts: &mut HashMap<AddressHash, AccountSnapshot>, frame: PreStateFrame) {
    match frame {
        PreStateFrame::Default(prestate_mode) => {
            for (address, account_state) in prestate_mode.0 {
                let snapshot = accounts.entry(address.into()).or_default();

                if let Some(balance) = account_state.balance {
                    snapshot.balance = Some(balance);
                    snapshot.touched = true;
                }

                if let Some(nonce) = account_state.nonce {
                    snapshot.nonce = Some(nonce);
                    snapshot.touched = true;
                }

                if let Some(code) = account_state.code {
                    snapshot.code = Some(code);
                    snapshot.touched = true;
                }

                // Hash storage slots exactly like Parity does
                for (slot, value) in &account_state.storage {
                    let slot_hash = keccak256(slot.0);
                    let value_u256 = U256::from_be_bytes((*value).into());
                    snapshot.storage_updates.insert(slot_hash, value_u256);
                    snapshot.touched = true;
                }
            }
        }
        PreStateFrame::Diff(diff) => {
            process_diff_frame(accounts, &diff);
        }
    }
}

fn process_diff_frame(
    accounts: &mut HashMap<AddressHash, AccountSnapshot>,
    diff: &alloy_rpc_types_trace::geth::DiffMode,
) {
    // Process pre-state first as baseline for ACCOUNT FIELDS (balance, nonce, code).
    // These are properties of the account that persist if unchanged.
    //
    // NOTE: We do NOT process pre.storage here because:
    // - pre.storage contains OLD values (before the transaction)
    // - post.storage contains NEW values of slots that CHANGED
    // - We only want to track changed slots with their new values
    for (address, account) in &diff.pre {
        let snapshot = accounts.entry((*address).into()).or_default();

        // Set baseline values from pre-state (only if not already set by earlier tx)
        if snapshot.balance.is_none() {
            snapshot.balance = account.balance;
        }
        if snapshot.nonce.is_none() {
            snapshot.nonce = account.nonce;
        }
        if snapshot.code.is_none()
            && let Some(code) = &account.code
        {
            snapshot.code = Some(code.clone());
        }

        // Geth diff mode removes zero-valued storage entries, so deletions show up
        // as slots present in pre but missing in post. Infer those as deletions.
        if let Some(post_account) = diff.post.get(address) {
            for slot in account.storage.keys() {
                if !post_account.storage.contains_key(slot) {
                    let slot_hash = keccak256(slot.0);
                    snapshot.storage_updates.insert(slot_hash, U256::ZERO);
                    snapshot.touched = true;
                }
            }
        }
    }

    // Account deletions: present in pre, missing in post (Geth diff mode encoding).
    for address in diff.pre.keys() {
        if !diff.post.contains_key(address) {
            let snapshot = accounts.entry((*address).into()).or_default();
            snapshot.deleted = true;
            snapshot.touched = true;
            snapshot.balance = None;
            snapshot.nonce = None;
            snapshot.code = None;
            snapshot.storage_updates.clear();
        }
    }

    // Process post-state to apply final values (overrides pre-state)
    for (address, account) in &diff.post {
        let snapshot = accounts.entry((*address).into()).or_default();
        snapshot.touched = true;
        snapshot.deleted = false;

        if let Some(balance) = account.balance {
            snapshot.balance = Some(balance);
        }

        if let Some(nonce) = account.nonce {
            snapshot.nonce = Some(nonce);
        }

        if let Some(code) = &account.code {
            snapshot.code = Some(code.clone());
        }

        // Storage from post contains NEW values of changed slots
        for (slot, value) in &account.storage {
            let slot_hash = keccak256(slot.0);
            let value_u256 = U256::from_be_bytes((*value).into());
            snapshot.storage_updates.insert(slot_hash, value_u256);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::similar_names)]
    use super::*;
    use alloy::primitives::{
        Address,
        B256,
        Bytes,
    };
    use alloy_rpc_types_trace::geth::{
        AccountState as GethAccountState,
        DiffMode,
        GethTrace,
        PreStateFrame,
    };
    use std::collections::BTreeMap;

    fn make_trace_result(frame: PreStateFrame) -> TraceResult {
        TraceResult::Success {
            result: GethTrace::PreStateTracer(frame),
            tx_hash: None,
        }
    }

    #[test]
    fn test_selfdestruct_only_balance_changes() {
        // Post-Cancun: SELFDESTRUCT only transfers balance, account remains in post
        let address = Address::from([0x42u8; 20]);

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(Bytes::from(vec![0x60, 0x80])),
                storage: BTreeMap::default(),
            },
        );

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: None,
                code: None,
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode { pre, post };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        assert!(!results[0].deleted);
        assert_eq!(results[0].balance, U256::ZERO);
    }

    #[test]
    fn test_storage_updated() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let new_value = B256::from([0xBBu8; 32]);

        let mut post = std::collections::BTreeMap::new();
        let mut storage = std::collections::BTreeMap::new();
        storage.insert(slot, new_value);

        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(1),
                code: None,
                storage,
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            results[0].storage.get(&slot_hash),
            Some(&U256::from_be_bytes(new_value.0))
        );
    }

    #[test]
    fn test_new_account_created() {
        let address = Address::from([0x42u8; 20]);

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(0),
                code: Some(Bytes::from(vec![0x60, 0x80, 0x60, 0x40])),
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        assert!(!results[0].deleted);
        assert_eq!(results[0].balance, U256::from(1000));
        assert_eq!(results[0].nonce, 0);
        assert!(results[0].code.is_some());
    }

    #[test]
    fn test_balance_transfer() {
        let sender = Address::from([0x01u8; 20]);
        let receiver = Address::from([0x02u8; 20]);

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            sender,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: Some(6),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        post.insert(
            receiver,
            GethAccountState {
                balance: Some(U256::from(600)),
                nonce: None,
                code: None,
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 2);

        let sender_state = results
            .iter()
            .find(|a| a.balance == U256::from(900))
            .unwrap();
        assert_eq!(sender_state.nonce, 6);
        assert!(!sender_state.deleted);

        let receiver_state = results
            .iter()
            .find(|a| a.balance == U256::from(600))
            .unwrap();
        assert!(!receiver_state.deleted);
    }

    #[test]
    fn test_multiple_transactions() {
        let address = Address::from([0x42u8; 20]);

        let mut post1 = std::collections::BTreeMap::new();
        post1.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: Some(6),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        let diff1 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: post1,
        };

        let mut post2 = std::collections::BTreeMap::new();
        post2.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(800)),
                nonce: Some(7),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        let diff2 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: post2,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff1)),
            make_trace_result(PreStateFrame::Diff(diff2)),
        ]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].balance, U256::from(800));
        assert_eq!(results[0].nonce, 7);
    }

    #[test]
    fn test_empty_diff_returns_nothing() {
        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert!(results.is_empty());
    }

    #[test]
    fn test_code_preserved_when_only_storage_changes() {
        // This test verifies the fix: when only storage changes, code should be
        // preserved from pre-state since it won't appear in post-state
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let old_value = B256::from([0xAAu8; 32]);
        let new_value = B256::from([0xBBu8; 32]);
        let code = Bytes::from(vec![0x60, 0x80, 0x60, 0x40]);

        let mut pre_storage = std::collections::BTreeMap::new();
        pre_storage.insert(slot, old_value);

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(code.clone()),
                storage: pre_storage,
            },
        );

        let mut post_storage = std::collections::BTreeMap::new();
        post_storage.insert(slot, new_value);

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None, // Not changed
                nonce: None,   // Not changed
                code: None,    // Not changed - but we need to preserve it!
                storage: post_storage,
            },
        );

        let diff = DiffMode { pre, post };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].balance, U256::from(1000));
        assert_eq!(results[0].nonce, 5);
        assert_eq!(results[0].code, Some(code));

        let slot_hash = keccak256(slot.0);
        assert_eq!(
            results[0].storage.get(&slot_hash),
            Some(&U256::from_be_bytes(new_value.0))
        );
    }

    #[test]
    fn test_storage_deletion_inferred_when_post_omits_zero() {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let old_value = B256::from([0xAAu8; 32]);

        let mut pre_storage = std::collections::BTreeMap::new();
        pre_storage.insert(slot, old_value);

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(Bytes::from(vec![0x60, 0x80])),
                storage: pre_storage,
            },
        );

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let diff = DiffMode { pre, post };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(results[0].storage.get(&slot_hash), Some(&U256::ZERO));
    }

    #[test]
    fn test_account_deletion_inferred_when_post_missing() {
        let address = Address::from([0x24u8; 20]);
        let slot = B256::from([0x02u8; 32]);
        let old_value = B256::from([0x11u8; 32]);

        let mut pre_storage = std::collections::BTreeMap::new();
        pre_storage.insert(slot, old_value);

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1)),
                nonce: Some(1),
                code: None,
                storage: pre_storage,
            },
        );

        let diff = DiffMode {
            pre,
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);

        assert_eq!(results.len(), 1);
        assert!(results[0].deleted);
    }

    #[test]
    fn test_storage_mixed_add_change_delete() {
        let address = Address::from([0x33u8; 20]);
        let slot_a = B256::from([0x0Au8; 32]);
        let slot_b = B256::from([0x0Bu8; 32]);
        let slot_c = B256::from([0x0Cu8; 32]);

        let mut pre_storage = std::collections::BTreeMap::new();
        pre_storage.insert(slot_a, B256::from(U256::from(10)));
        pre_storage.insert(slot_b, B256::from(U256::from(20)));

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: Some(0),
                code: None,
                storage: pre_storage,
            },
        );

        let mut post_storage = std::collections::BTreeMap::new();
        post_storage.insert(slot_b, B256::from(U256::from(200)));
        post_storage.insert(slot_c, B256::from(U256::from(300)));

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let diff = DiffMode { pre, post };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))]);
        assert_eq!(results.len(), 1);

        let slot_a_hash = keccak256(slot_a.0);
        let slot_b_hash = keccak256(slot_b.0);
        let slot_c_hash = keccak256(slot_c.0);

        assert_eq!(results[0].storage.get(&slot_a_hash), Some(&U256::ZERO));
        assert_eq!(results[0].storage.get(&slot_b_hash), Some(&U256::from(200)));
        assert_eq!(results[0].storage.get(&slot_c_hash), Some(&U256::from(300)));
    }

    #[test]
    fn test_storage_deletion_then_recreate_in_later_tx() {
        let address = Address::from([0x55u8; 20]);
        let slot = B256::from([0x05u8; 32]);

        let mut pre_storage = std::collections::BTreeMap::new();
        pre_storage.insert(slot, B256::from(U256::from(7)));

        let mut pre = std::collections::BTreeMap::new();
        pre.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: Some(0),
                code: None,
                storage: pre_storage,
            },
        );

        let diff1 = DiffMode {
            pre,
            post: std::collections::BTreeMap::new(),
        };

        let mut post_storage = std::collections::BTreeMap::new();
        post_storage.insert(slot, B256::from(U256::from(9)));

        let mut post = std::collections::BTreeMap::new();
        post.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            },
        );

        let diff2 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff1)),
            make_trace_result(PreStateFrame::Diff(diff2)),
        ]);

        assert_eq!(results.len(), 1);
        let slot_hash = keccak256(slot.0);
        assert_eq!(results[0].storage.get(&slot_hash), Some(&U256::from(9)));
    }

    #[test]
    fn test_account_deletion_overrides_prior_storage_updates_in_block() {
        let address = Address::from([0x21u8; 20]);
        let slot = B256::from([0x09u8; 32]);
        let value = B256::from([0xAAu8; 32]);

        let mut post1 = std::collections::BTreeMap::new();
        let mut storage1 = std::collections::BTreeMap::new();
        storage1.insert(slot, value);
        post1.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: None,
                storage: storage1,
            },
        );

        let diff1 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: post1,
        };

        let mut pre2 = std::collections::BTreeMap::new();
        pre2.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: None,
                storage: std::collections::BTreeMap::new(),
            },
        );

        let diff2 = DiffMode {
            pre: pre2,
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff1)),
            make_trace_result(PreStateFrame::Diff(diff2)),
        ]);

        assert_eq!(results.len(), 1);
        assert!(results[0].deleted);
        assert!(results[0].storage.is_empty());
    }

    #[test]
    fn test_account_deleted_then_recreated_in_later_tx() {
        let address = Address::from([0x22u8; 20]);
        let slot = B256::from([0x0Au8; 32]);
        let value = B256::from([0xBBu8; 32]);
        let code = Bytes::from(vec![0x60, 0x80]);

        let mut pre1 = std::collections::BTreeMap::new();
        pre1.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: Some(code.clone()),
                storage: std::collections::BTreeMap::new(),
            },
        );

        let diff1 = DiffMode {
            pre: pre1,
            post: std::collections::BTreeMap::new(),
        };

        let mut post2 = std::collections::BTreeMap::new();
        let mut storage2 = std::collections::BTreeMap::new();
        storage2.insert(slot, value);
        post2.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(50)),
                nonce: Some(2),
                code: Some(code.clone()),
                storage: storage2,
            },
        );

        let diff2 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: post2,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff1)),
            make_trace_result(PreStateFrame::Diff(diff2)),
        ]);

        assert_eq!(results.len(), 1);
        assert!(!results[0].deleted);
        assert_eq!(results[0].balance, U256::from(50));
        assert_eq!(results[0].nonce, 2);
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            results[0].storage.get(&slot_hash),
            Some(&U256::from_be_bytes(value.0))
        );
        assert_eq!(results[0].code, Some(code));
    }
}
