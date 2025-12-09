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
use state_store::common::{
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
                    snapshot.code = Some(code.to_vec());
                    snapshot.touched = true;
                }

                // Hash storage slots exactly like Parity does
                for (slot, value) in &account_state.storage {
                    let slot_hash = keccak256(slot.0);
                    let slot_key = U256::from_be_bytes(slot_hash.into());
                    let value_u256 = U256::from_be_bytes((*value).into());
                    snapshot.storage_updates.insert(slot_key, value_u256);
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
    // Process post-state only,  the final state after transaction
    for (address, account) in &diff.post {
        let snapshot = accounts.entry((*address).into()).or_default();

        if let Some(balance) = account.balance {
            snapshot.balance = Some(balance);
            snapshot.touched = true;
        }

        if let Some(nonce) = account.nonce {
            snapshot.nonce = Some(nonce);
            snapshot.touched = true;
        }

        if let Some(code) = &account.code {
            snapshot.code = Some(code.to_vec());
            snapshot.touched = true;
        }

        for (slot, value) in &account.storage {
            let slot_hash = keccak256(slot.0);
            let slot_key = U256::from_be_bytes(slot_hash.into());
            let value_u256 = U256::from_be_bytes((*value).into());
            snapshot.storage_updates.insert(slot_key, value_u256);
            snapshot.touched = true;
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
        let slot_key = U256::from_be_bytes(slot_hash.into());
        assert_eq!(
            results[0].storage.get(&slot_key),
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
}
