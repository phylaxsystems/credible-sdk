//! Integration tests for state trace processing
//!
//! These tests verify that both Geth and Parity trace providers produce
//! identical output when given equivalent state changes.
//!
//! ## Post-Cancun Behavior (EIP-6780)
//!
//! Post-Cancun, SELFDESTRUCT only transfers balance. There is no full account
//! deletion. The tracer reports only balance changes for SELFDESTRUCT operations.
use crate::state::{
    geth,
    parity,
};
use alloy::primitives::{
    B256,
    Bytes,
    U64,
    U256,
    address,
    keccak256,
};
use alloy_rpc_types_trace::{
    geth::{
        AccountState as GethAccountState,
        DiffMode,
        GethTrace,
        PreStateFrame,
        TraceResult,
    },
    parity::{
        AccountDiff,
        ChangedType,
        Delta,
        StateDiff,
        TraceResults,
        TraceResultsWithTransactionHash,
    },
};
use std::collections::BTreeMap;

// Helper to create Parity trace
fn create_parity_trace(tx_hash: B256, state_diff: StateDiff) -> TraceResultsWithTransactionHash {
    TraceResultsWithTransactionHash {
        transaction_hash: tx_hash,
        full_trace: TraceResults {
            output: Bytes::new(),
            state_diff: Some(state_diff),
            trace: vec![],
            vm_trace: None,
        },
    }
}

#[test]
fn test_simple_balance_change_produces_identical_output() {
    let address = address!("0x1111111111111111111111111111111111111111");
    let balance = U256::from(1000);
    let nonce = 5u64;

    // === Parity format ===
    let mut parity_diff = StateDiff::default();
    parity_diff.0.insert(
        address,
        AccountDiff {
            balance: Delta::Added(balance),
            nonce: Delta::Added(U64::from(nonce)),
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    // === Geth format ===
    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        address,
        GethAccountState {
            balance: Some(balance),
            nonce: Some(nonce),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: BTreeMap::new(),
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    // Sort for comparison
    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    assert_eq!(parity_account.address_hash, geth_account.address_hash);
    assert_eq!(parity_account.balance, geth_account.balance);
    assert_eq!(parity_account.nonce, geth_account.nonce);
    assert_eq!(parity_account.code_hash, geth_account.code_hash);
    assert_eq!(parity_account.code, geth_account.code);
    assert_eq!(parity_account.deleted, geth_account.deleted);
    assert_eq!(parity_account.storage.len(), geth_account.storage.len());
}

#[test]
fn test_contract_deployment_produces_identical_output() {
    let address = address!("0x2222222222222222222222222222222222222222");
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let balance = U256::from(0);
    let nonce = 1u64;

    // === Parity format ===
    let mut parity_diff = StateDiff::default();
    parity_diff.0.insert(
        address,
        AccountDiff {
            balance: Delta::Added(balance),
            nonce: Delta::Added(U64::from(nonce)),
            code: Delta::Added(Bytes::from(code.clone())),
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    // === Geth format ===
    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        address,
        GethAccountState {
            balance: Some(balance),
            nonce: Some(nonce),
            code: Some(Bytes::from(code.clone())),
            storage: BTreeMap::new(),
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: BTreeMap::new(),
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    assert_eq!(parity_account.address_hash, geth_account.address_hash);
    assert_eq!(parity_account.balance, geth_account.balance);
    assert_eq!(parity_account.nonce, geth_account.nonce);
    assert_eq!(parity_account.code_hash, geth_account.code_hash);
    assert_eq!(parity_account.code, geth_account.code);
    assert_eq!(parity_account.deleted, geth_account.deleted);

    // Verify code hash
    let expected_code_hash = keccak256(&code);
    assert_eq!(parity_account.code_hash, expected_code_hash);
    assert_eq!(geth_account.code_hash, expected_code_hash);
}

#[test]
fn test_storage_updates_produce_identical_output() {
    let address = address!("0x3333333333333333333333333333333333333333");
    let slot1 = B256::from(U256::from(1));
    let slot2 = B256::from(U256::from(2));
    let slot3 = B256::from(U256::from(3));

    // Compute hashed slot keys (what the implementation actually stores)
    let slot1_hash = keccak256(slot1.0);
    let slot2_hash = keccak256(slot2.0);
    let slot3_hash = keccak256(slot3.0);

    // === Parity format ===
    let mut parity_storage = BTreeMap::new();
    parity_storage.insert(slot1, Delta::Added(B256::from(U256::from(100))));
    parity_storage.insert(
        slot2,
        Delta::Changed(ChangedType {
            from: B256::from(U256::from(200)),
            to: B256::from(U256::from(300)),
        }),
    );
    parity_storage.insert(slot3, Delta::Removed(B256::from(U256::from(400))));

    let mut parity_diff = StateDiff::default();
    parity_diff.0.insert(
        address,
        AccountDiff {
            balance: Delta::Unchanged,
            nonce: Delta::Unchanged,
            code: Delta::Unchanged,
            storage: parity_storage,
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    // === Geth format (using post-state) ===
    let mut geth_storage = BTreeMap::new();
    geth_storage.insert(slot1, B256::from(U256::from(100)));
    geth_storage.insert(slot2, B256::from(U256::from(300)));
    geth_storage.insert(slot3, B256::ZERO); // Removed = zero

    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        address,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: Some(0),
            code: None,
            storage: geth_storage,
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: BTreeMap::new(),
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    // Verify storage is identical
    assert_eq!(parity_account.storage.len(), geth_account.storage.len());

    // Both should have the same hashed slots with the same values
    // Use hashed keys since the implementation hashes slots before storing
    assert_eq!(
        parity_account.storage.get(&slot1_hash),
        Some(&U256::from(100))
    );
    assert_eq!(
        geth_account.storage.get(&slot1_hash),
        Some(&U256::from(100))
    );

    assert_eq!(
        parity_account.storage.get(&slot2_hash),
        Some(&U256::from(300))
    );
    assert_eq!(
        geth_account.storage.get(&slot2_hash),
        Some(&U256::from(300))
    );

    assert_eq!(parity_account.storage.get(&slot3_hash), Some(&U256::ZERO));
    assert_eq!(geth_account.storage.get(&slot3_hash), Some(&U256::ZERO));
}

#[test]
fn test_selfdestruct_balance_transfer_produces_identical_output() {
    let address = address!("0x4444444444444444444444444444444444444444");

    let mut parity_diff = StateDiff::default();
    parity_diff.0.insert(
        address,
        AccountDiff {
            balance: Delta::Changed(ChangedType {
                from: U256::from(1000),
                to: U256::ZERO,
            }),
            nonce: Delta::Unchanged,
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    let mut geth_pre = BTreeMap::new();
    geth_pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: Some(5),
            code: Some(Bytes::from(vec![0x60, 0x80])),
            storage: BTreeMap::new(),
        },
    );

    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        address,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: geth_pre,
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    assert_eq!(parity_account.address_hash, geth_account.address_hash);
    assert!(!parity_account.deleted);
    assert!(!geth_account.deleted);
    assert_eq!(parity_account.balance, U256::ZERO);
    assert_eq!(geth_account.balance, U256::ZERO);
}

#[test]
fn test_selfdestruct_storage_persists_produces_identical_output() {
    let address = address!("0x5555555555555555555555555555555555555555");
    let slot1 = B256::from(U256::from(10));

    let mut parity_diff = StateDiff::default();
    parity_diff.0.insert(
        address,
        AccountDiff {
            balance: Delta::Changed(ChangedType {
                from: U256::from(5000),
                to: U256::ZERO,
            }),
            nonce: Delta::Unchanged,
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    let mut geth_pre = BTreeMap::new();
    let mut pre_storage = BTreeMap::new();
    pre_storage.insert(slot1, B256::from(U256::from(111)));
    geth_pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(5000)),
            nonce: Some(10),
            code: Some(Bytes::from(vec![0x60])),
            storage: pre_storage,
        },
    );

    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        address,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: geth_pre,
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    assert!(!parity_account.deleted);
    assert!(!geth_account.deleted);

    assert_eq!(parity_account.balance, U256::ZERO);
    assert_eq!(geth_account.balance, U256::ZERO);

    assert_eq!(parity_account.storage.len(), 0);
    assert_eq!(geth_account.storage.len(), 0);
}

#[test]
fn test_multiple_transactions_produce_identical_output() {
    let address = address!("0x6666666666666666666666666666666666666666");

    // === Parity format (two transactions) ===
    let mut parity_diff1 = StateDiff::default();
    parity_diff1.0.insert(
        address,
        AccountDiff {
            balance: Delta::Changed(ChangedType {
                from: U256::from(1000),
                to: U256::from(900),
            }),
            nonce: Delta::Changed(ChangedType {
                from: U64::from(0),
                to: U64::from(1),
            }),
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    let mut parity_diff2 = StateDiff::default();
    parity_diff2.0.insert(
        address,
        AccountDiff {
            balance: Delta::Changed(ChangedType {
                from: U256::from(900),
                to: U256::from(800),
            }),
            nonce: Delta::Changed(ChangedType {
                from: U64::from(1),
                to: U64::from(2),
            }),
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![
        create_parity_trace(B256::from(U256::from(1)), parity_diff1),
        create_parity_trace(B256::from(U256::from(2)), parity_diff2),
    ];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    // === Geth format (two transactions) ===
    let mut geth_post1 = BTreeMap::new();
    geth_post1.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(900)),
            nonce: Some(1),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let geth_trace1 = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: BTreeMap::new(),
            post: geth_post1,
        })),
        tx_hash: Some(B256::from(U256::from(1))),
    };

    let mut geth_post2 = BTreeMap::new();
    geth_post2.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(800)),
            nonce: Some(2),
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let geth_trace2 = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: BTreeMap::new(),
            post: geth_post2,
        })),
        tx_hash: Some(B256::from(U256::from(2))),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace1, geth_trace2]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 1);
    assert_eq!(geth_accounts.len(), 1);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    let parity_account = &parity_accounts[0];
    let geth_account = &geth_accounts[0];

    // Should have final state after both transactions
    assert_eq!(parity_account.balance, U256::from(800));
    assert_eq!(geth_account.balance, U256::from(800));
    assert_eq!(parity_account.nonce, 2);
    assert_eq!(geth_account.nonce, 2);
}

#[allow(clippy::too_many_lines)]
#[test]
fn test_complex_scenario_produces_identical_output() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let addr3 = address!("0x3333333333333333333333333333333333333333");

    let slot = B256::from(U256::from(42));
    let code = vec![0x60, 0x80];

    // === Parity format ===
    let mut parity_diff = StateDiff::default();

    // Account 1: New account with balance
    parity_diff.0.insert(
        addr1,
        AccountDiff {
            balance: Delta::Added(U256::from(1000)),
            nonce: Delta::Added(U64::from(0)),
            code: Delta::Unchanged,
            storage: BTreeMap::new(),
        },
    );

    // Account 2: Contract with storage
    let mut storage = BTreeMap::new();
    storage.insert(slot, Delta::Added(B256::from(U256::from(999))));
    parity_diff.0.insert(
        addr2,
        AccountDiff {
            balance: Delta::Added(U256::ZERO),
            nonce: Delta::Added(U64::from(1)),
            code: Delta::Added(Bytes::from(code.clone())),
            storage,
        },
    );

    // Account 3: Balance changed, nonce and code also specified for consistency
    parity_diff.0.insert(
        addr3,
        AccountDiff {
            balance: Delta::Changed(ChangedType {
                from: U256::from(500),
                to: U256::ZERO,
            }),
            nonce: Delta::Added(U64::from(5)), // Changed: specify the nonce value
            code: Delta::Added(Bytes::from(vec![0x00])), // Changed: specify the code
            storage: BTreeMap::new(),
        },
    );

    let parity_traces = vec![create_parity_trace(B256::ZERO, parity_diff)];
    let mut parity_accounts = parity::process_parity_traces(parity_traces);

    // === Geth format ===
    let mut geth_storage = BTreeMap::new();
    geth_storage.insert(slot, B256::from(U256::from(999)));

    let mut geth_pre = BTreeMap::new();
    geth_pre.insert(
        addr3,
        GethAccountState {
            balance: Some(U256::from(500)),
            nonce: Some(5),
            code: Some(Bytes::from(vec![0x00])),
            storage: BTreeMap::new(),
        },
    );

    let mut geth_post = BTreeMap::new();
    geth_post.insert(
        addr1,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: Some(0),
            code: None,
            storage: BTreeMap::new(),
        },
    );
    geth_post.insert(
        addr2,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: Some(1),
            code: Some(Bytes::from(code)),
            storage: geth_storage,
        },
    );
    geth_post.insert(
        addr3,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: None, // Unchanged - will use pre-state value (5)
            code: None,  // Unchanged - will use pre-state value (0x00)
            storage: BTreeMap::new(),
        },
    );

    let geth_trace = TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode {
            pre: geth_pre,
            post: geth_post,
        })),
        tx_hash: Some(B256::ZERO),
    };

    let mut geth_accounts = geth::process_geth_traces(vec![geth_trace]);

    // === Verify identical output ===
    assert_eq!(parity_accounts.len(), 3);
    assert_eq!(geth_accounts.len(), 3);

    parity_accounts.sort_by_key(|a| a.address_hash);
    geth_accounts.sort_by_key(|a| a.address_hash);

    for (parity_account, geth_account) in parity_accounts.iter().zip(geth_accounts.iter()) {
        assert_eq!(parity_account.address_hash, geth_account.address_hash);
        assert_eq!(parity_account.balance, geth_account.balance);
        assert_eq!(parity_account.nonce, geth_account.nonce);
        assert_eq!(parity_account.code_hash, geth_account.code_hash);
        assert_eq!(parity_account.code, geth_account.code);
        assert_eq!(parity_account.deleted, geth_account.deleted);
        assert_eq!(parity_account.storage.len(), geth_account.storage.len());

        assert!(!parity_account.deleted);
        assert!(!geth_account.deleted);

        // Verify storage matches
        for (key, value) in &parity_account.storage {
            assert_eq!(geth_account.storage.get(key), Some(value));
        }
    }
}
