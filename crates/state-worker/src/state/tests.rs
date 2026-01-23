//! Tests for Geth trace processing output.

use crate::state::geth;
use alloy::primitives::{
    B256,
    Bytes,
    U256,
    address,
    keccak256,
};
use alloy_rpc_types_trace::geth::{
    AccountState as GethAccountState,
    DiffMode,
    GethTrace,
    PreStateFrame,
    TraceResult,
};
use mdbx::AddressHash;
use std::collections::BTreeMap;

fn make_trace_result(frame: PreStateFrame) -> TraceResult {
    TraceResult::Success {
        result: GethTrace::PreStateTracer(frame),
        tx_hash: Some(B256::ZERO),
    }
}

#[test]
fn test_simple_balance_change() {
    let address = address!("0x1111111111111111111111111111111111111111");
    let balance = U256::from(1000);
    let nonce = 5u64;

    let mut post = BTreeMap::new();
    post.insert(
        address,
        GethAccountState {
            balance: Some(balance),
            nonce: Some(nonce),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post,
    }))];

    let mut accounts = geth::process_geth_traces(traces).unwrap();
    assert_eq!(accounts.len(), 1);
    accounts.sort_by_key(|a| a.address_hash);

    let account = &accounts[0];
    assert_eq!(account.balance, balance);
    assert_eq!(account.nonce, nonce);
    assert!(!account.deleted);
}

#[test]
fn test_contract_deployment() {
    let address = address!("0x2222222222222222222222222222222222222222");
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let balance = U256::ZERO;
    let nonce = 1u64;

    let mut post = BTreeMap::new();
    post.insert(
        address,
        GethAccountState {
            balance: Some(balance),
            nonce: Some(nonce),
            code: Some(Bytes::from(code.clone())),
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post,
    }))];

    let mut accounts = geth::process_geth_traces(traces).unwrap();
    assert_eq!(accounts.len(), 1);
    accounts.sort_by_key(|a| a.address_hash);

    let account = &accounts[0];
    assert_eq!(account.balance, balance);
    assert_eq!(account.nonce, nonce);
    assert_eq!(account.code, Some(Bytes::from(code.clone())));
    assert_eq!(account.code_hash, keccak256(&code));
    assert!(!account.deleted);
}

#[test]
fn test_storage_updates() {
    let address = address!("0x3333333333333333333333333333333333333333");
    let slot1 = B256::from(U256::from(1));
    let slot2 = B256::from(U256::from(2));
    let slot3 = B256::from(U256::from(3));

    let slot1_hash = keccak256(slot1.0);
    let slot2_hash = keccak256(slot2.0);
    let slot3_hash = keccak256(slot3.0);

    let mut pre_storage = BTreeMap::new();
    pre_storage.insert(slot3, B256::from(U256::from(400)));

    let mut pre = BTreeMap::new();
    pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: Some(0),
            code: None,
            storage: pre_storage,
        },
    );

    let mut post_storage = BTreeMap::new();
    post_storage.insert(slot1, B256::from(U256::from(100)));
    post_storage.insert(slot2, B256::from(U256::from(300)));

    let mut post = BTreeMap::new();
    post.insert(
        address,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: Some(0),
            code: None,
            storage: post_storage,
        },
    );

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre,
        post,
    }))];
    let mut accounts = geth::process_geth_traces(traces).unwrap();
    assert_eq!(accounts.len(), 1);
    accounts.sort_by_key(|a| a.address_hash);

    let account = &accounts[0];
    assert_eq!(account.storage.get(&slot1_hash), Some(&U256::from(100)));
    assert_eq!(account.storage.get(&slot2_hash), Some(&U256::from(300)));
    assert_eq!(account.storage.get(&slot3_hash), Some(&U256::ZERO));
    assert!(!account.deleted);
}

#[test]
fn test_selfdestruct_balance_transfer() {
    let address = address!("0x4444444444444444444444444444444444444444");

    let mut pre = BTreeMap::new();
    pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: Some(5),
            code: Some(Bytes::from(vec![0x60, 0x80])),
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

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre,
        post,
    }))];
    let accounts = geth::process_geth_traces(traces).unwrap();

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].balance, U256::ZERO);
    assert!(!accounts[0].deleted);
}

#[test]
fn test_selfdestruct_storage_persists() {
    let address = address!("0x5555555555555555555555555555555555555555");

    let mut pre = BTreeMap::new();
    pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(5000)),
            nonce: Some(10),
            code: Some(Bytes::from(vec![0x60])),
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

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre,
        post,
    }))];
    let accounts = geth::process_geth_traces(traces).unwrap();

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].balance, U256::ZERO);
    assert!(accounts[0].storage.is_empty());
    assert!(!accounts[0].deleted);
}

#[test]
fn test_account_deletion() {
    let address = address!("0x7777777777777777777777777777777777777777");

    let mut pre = BTreeMap::new();
    pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: Some(9),
            code: Some(Bytes::from(vec![0x60, 0x80])),
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre,
        post: BTreeMap::new(),
    }))];

    let accounts = geth::process_geth_traces(traces).unwrap();
    assert_eq!(accounts.len(), 1);
    assert!(accounts[0].deleted);
}

#[test]
fn test_account_deletion_overrides_storage_updates() {
    let address = address!("0x8888888888888888888888888888888888888888");
    let slot = B256::from(U256::from(7));
    let value = B256::from(U256::from(42));

    let mut post1 = BTreeMap::new();
    let mut storage1 = BTreeMap::new();
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

    let trace1 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post: post1,
    }));

    let mut pre2 = BTreeMap::new();
    pre2.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(10)),
            nonce: Some(1),
            code: Some(Bytes::from(vec![0x60, 0x80])),
            storage: BTreeMap::new(),
        },
    );

    let trace2 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: pre2,
        post: BTreeMap::new(),
    }));

    let accounts = geth::process_geth_traces(vec![trace1, trace2]).unwrap();
    assert_eq!(accounts.len(), 1);
    assert!(accounts[0].deleted);
    assert!(accounts[0].storage.is_empty());
}

#[test]
fn test_account_deleted_then_recreated() {
    let address = address!("0x9999999999999999999999999999999999999999");
    let slot = B256::from(U256::from(9));
    let value = B256::from(U256::from(77));
    let code = Bytes::from(vec![0x60, 0x80, 0x60]);

    let mut pre1 = BTreeMap::new();
    pre1.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(10)),
            nonce: Some(1),
            code: Some(code.clone()),
            storage: BTreeMap::new(),
        },
    );

    let trace1 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: pre1,
        post: BTreeMap::new(),
    }));

    let mut post2 = BTreeMap::new();
    let mut storage2 = BTreeMap::new();
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

    let trace2 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post: post2,
    }));

    let accounts = geth::process_geth_traces(vec![trace1, trace2]).unwrap();
    assert_eq!(accounts.len(), 1);

    let account = &accounts[0];
    assert!(!account.deleted);
    assert_eq!(account.balance, U256::from(50));
    assert_eq!(account.nonce, 2);
    let slot_hash = keccak256(slot.0);
    assert_eq!(account.storage.get(&slot_hash), Some(&U256::from(77)));
    assert_eq!(account.code, Some(code));
}

#[test]
fn test_multiple_transactions() {
    let address = address!("0x6666666666666666666666666666666666666666");

    let mut post1 = BTreeMap::new();
    post1.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(900)),
            nonce: Some(1),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let trace1 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post: post1,
    }));

    let mut post2 = BTreeMap::new();
    post2.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(800)),
            nonce: Some(2),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let trace2 = make_trace_result(PreStateFrame::Diff(DiffMode {
        pre: BTreeMap::new(),
        post: post2,
    }));

    let accounts = geth::process_geth_traces(vec![trace1, trace2]).unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].balance, U256::from(800));
    assert_eq!(accounts[0].nonce, 2);
}

#[test]
fn test_complex_scenario() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let addr3 = address!("0x3333333333333333333333333333333333333333");

    let slot = B256::from(U256::from(42));
    let code = vec![0x60, 0x80];

    let mut pre = BTreeMap::new();
    pre.insert(
        addr3,
        GethAccountState {
            balance: Some(U256::from(500)),
            nonce: Some(5),
            code: Some(Bytes::from(vec![0x00])),
            storage: BTreeMap::new(),
        },
    );

    let mut storage2 = BTreeMap::new();
    storage2.insert(slot, B256::from(U256::from(999)));

    let mut post = BTreeMap::new();
    post.insert(
        addr1,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: Some(0),
            code: None,
            storage: BTreeMap::new(),
        },
    );
    post.insert(
        addr2,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: Some(1),
            code: Some(Bytes::from(code.clone())),
            storage: storage2,
        },
    );
    post.insert(
        addr3,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_trace_result(PreStateFrame::Diff(DiffMode {
        pre,
        post,
    }))];
    let mut results = geth::process_geth_traces(traces).unwrap();

    assert_eq!(results.len(), 3);
    results.sort_by_key(|a| a.address_hash);

    let mut by_addr = results
        .into_iter()
        .map(|account| (account.address_hash, account))
        .collect::<std::collections::HashMap<_, _>>();

    let account1 = by_addr.remove(&AddressHash::new(addr1)).expect("addr1");
    assert_eq!(account1.balance, U256::from(1000));
    assert_eq!(account1.nonce, 0);

    let account2 = by_addr.remove(&AddressHash::new(addr2)).expect("addr2");
    assert_eq!(account2.code_hash, keccak256(&code));
    let slot_hash = keccak256(slot.0);
    assert_eq!(account2.storage.get(&slot_hash), Some(&U256::from(999)));

    let account3 = by_addr.remove(&AddressHash::new(addr3)).expect("addr3");
    assert_eq!(account3.balance, U256::ZERO);
    assert_eq!(account3.nonce, 5);
    assert!(!account3.deleted);
}
