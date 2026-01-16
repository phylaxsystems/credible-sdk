//! Integration tests for state trace processing
//!
//! ## Important Note on Geth vs Parity
//!
//! The Geth provider now uses a different approach than Parity:
//! - **Geth**: Reads existing state from StateReader at block N-1, applies POST diff changes
//! - **Parity**: Still reconstructs state from traces using AccountSnapshot (has known bugs!)
//!
//! The old comparison tests between Geth and Parity are no longer valid because the providers
//! work fundamentally differently. The Parity provider should be updated to use the same
//! StateReader-based approach as Geth.
//!
//! ## Post-Cancun Behavior (EIP-6780)
//!
//! Post-Cancun, SELFDESTRUCT only transfers balance. There is no full account
//! deletion. The tracer reports only balance changes for SELFDESTRUCT operations.

use crate::state::{
    geth::{
        extract_post_state,
        extract_touched_addresses,
        extract_touched_storage,
    },
    parity,
};
use alloy::primitives::{
    Address,
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
        PreStateMode,
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

// ==================== Helper Functions ====================

fn make_diff_trace(
    pre: BTreeMap<Address, GethAccountState>,
    post: BTreeMap<Address, GethAccountState>,
) -> TraceResult {
    TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Diff(DiffMode { pre, post })),
        tx_hash: None,
    }
}

fn make_default_trace(prestate: BTreeMap<Address, GethAccountState>) -> TraceResult {
    TraceResult::Success {
        result: GethTrace::PreStateTracer(PreStateFrame::Default(PreStateMode(prestate))),
        tx_hash: None,
    }
}

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

// ==================== Geth: extract_touched_addresses Tests ====================

#[test]
fn test_geth_extract_addresses_from_diff_mode_pre_only() {
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
fn test_geth_extract_addresses_from_diff_mode_post_only() {
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
fn test_geth_extract_addresses_deduplicates() {
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
}

#[test]
fn test_geth_extract_addresses_from_multiple_traces() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let addr3 = address!("0x3333333333333333333333333333333333333333");

    let mut post1 = BTreeMap::new();
    post1.insert(
        addr1,
        GethAccountState {
            balance: Some(U256::from(100)),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let mut post2 = BTreeMap::new();
    post2.insert(
        addr2,
        GethAccountState {
            balance: Some(U256::from(200)),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );
    post2.insert(
        addr3,
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
    assert!(addresses.contains(&addr1));
    assert!(addresses.contains(&addr2));
    assert!(addresses.contains(&addr3));
}

#[test]
fn test_geth_extract_addresses_ignores_failed_traces() {
    let traces = vec![TraceResult::Error {
        error: "execution reverted".to_string(),
        tx_hash: None,
    }];
    let addresses = extract_touched_addresses(&traces);
    assert!(addresses.is_empty());
}

#[test]
fn test_geth_extract_addresses_from_default_mode() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");

    let mut prestate = BTreeMap::new();
    prestate.insert(
        addr1,
        GethAccountState {
            balance: Some(U256::from(100)),
            nonce: Some(1),
            code: None,
            storage: BTreeMap::new(),
        },
    );
    prestate.insert(
        addr2,
        GethAccountState {
            balance: Some(U256::from(200)),
            nonce: Some(2),
            code: Some(Bytes::from(vec![0x60, 0x80])),
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_default_trace(prestate)];
    let addresses = extract_touched_addresses(&traces);

    assert_eq!(addresses.len(), 2);
    assert!(addresses.contains(&addr1));
    assert!(addresses.contains(&addr2));
}

// ==================== Geth: extract_touched_storage Tests ====================

#[test]
fn test_geth_extract_storage_from_pre_and_post() {
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

    let slots = storage.get(&address).unwrap();
    assert_eq!(slots.len(), 2);
    assert!(slots.contains(&slot1));
    assert!(slots.contains(&slot2));
}

#[test]
fn test_geth_extract_storage_deduplicates_same_slot() {
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

    assert_eq!(storage.get(&address).unwrap().len(), 1);
}

#[test]
fn test_geth_extract_storage_multiple_addresses() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let slot1 = B256::from(U256::from(1));
    let slot2 = B256::from(U256::from(2));

    let mut storage1 = BTreeMap::new();
    storage1.insert(slot1, B256::from([0xAAu8; 32]));

    let mut storage2 = BTreeMap::new();
    storage2.insert(slot2, B256::from([0xBBu8; 32]));

    let mut post = BTreeMap::new();
    post.insert(
        addr1,
        GethAccountState {
            balance: None,
            nonce: None,
            code: None,
            storage: storage1,
        },
    );
    post.insert(
        addr2,
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
    assert!(storage.get(&addr1).unwrap().contains(&slot1));
    assert!(storage.get(&addr2).unwrap().contains(&slot2));
}

// ==================== Geth: extract_post_state Tests ====================

#[test]
fn test_geth_extract_post_state_single_transaction() {
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
}

#[test]
fn test_geth_extract_post_state_multiple_transactions_override() {
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

    let account = post_state.get(&address).unwrap();
    // Later transaction should override earlier
    assert_eq!(account.balance, Some(U256::from(200)));
    assert_eq!(account.nonce, Some(2));
}

#[test]
fn test_geth_extract_post_state_partial_updates_accumulate() {
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
fn test_geth_extract_post_state_storage_merge() {
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

// ==================== Bug Reproduction Tests ====================

/// This test demonstrates the original bug scenario and how POST state extraction works.
/// When only storage changes, geth omits balance/nonce/code from the trace.
/// The new approach extracts POST state values and applies them on top of existing state
/// from StateReader.
#[test]
fn test_bug_demonstration_storage_only_change() {
    let contract_address = address!("0x4444444444444444444444444444444444444444");
    let slot = B256::from(U256::from(1));
    let old_value = B256::from(U256::from(100));
    let new_value = B256::from(U256::from(200));

    // What geth ACTUALLY returns when only storage changes:
    // NO balance, NO nonce, NO code - only storage diff
    let mut pre_storage = BTreeMap::new();
    pre_storage.insert(slot, old_value);

    let mut pre = BTreeMap::new();
    pre.insert(
        contract_address,
        GethAccountState {
            balance: None, // OMITTED!
            nonce: None,   // OMITTED!
            code: None,    // OMITTED!
            storage: pre_storage,
        },
    );

    let mut post_storage = BTreeMap::new();
    post_storage.insert(slot, new_value);

    let mut post = BTreeMap::new();
    post.insert(
        contract_address,
        GethAccountState {
            balance: None, // OMITTED!
            nonce: None,   // OMITTED!
            code: None,    // OMITTED!
            storage: post_storage,
        },
    );

    let traces = vec![make_diff_trace(pre, post)];

    // NEW APPROACH: Extract touched addresses (works correctly)
    let addresses = extract_touched_addresses(&traces);
    assert!(addresses.contains(&contract_address));

    // NEW APPROACH: Extract touched storage (works correctly)
    let storage = extract_touched_storage(&traces);
    assert!(storage.get(&contract_address).unwrap().contains(&slot));

    // NEW APPROACH: Extract POST state values
    let post_state = extract_post_state(&traces);
    let account_post = post_state.get(&contract_address).unwrap();

    // POST state correctly shows:
    // - balance/nonce/code are None (not changed, will be read from StateReader)
    // - storage has the new value
    assert!(account_post.balance.is_none());
    assert!(account_post.nonce.is_none());
    assert!(account_post.code.is_none());
    assert_eq!(account_post.storage.get(&slot), Some(&new_value));

    // The provider will then:
    // 1. Read existing balance/nonce/code from StateReader at block N-1
    // 2. Apply POST storage changes
    // This ensures 100% correct state
}

/// Test that SELFDESTRUCT (post-Cancun) correctly identifies the address
#[test]
fn test_selfdestruct_post_cancun_extracts_address() {
    let address = address!("0x5555555555555555555555555555555555555555");

    // Post-Cancun: only balance changes, code/nonce/storage remain
    let mut pre = BTreeMap::new();
    pre.insert(
        address,
        GethAccountState {
            balance: Some(U256::from(1000)),
            nonce: None, // unchanged
            code: None,  // unchanged
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

// ==================== Parity Tests ====================
// Note: Parity provider still uses AccountSnapshot approach which has the same bug.
// These tests verify the existing behavior, but the provider should be updated.

#[test]
fn test_parity_simple_balance_change() {
    let address = address!("0x1111111111111111111111111111111111111111");
    let balance = U256::from(1000);
    let nonce = 5u64;

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
    let accounts = parity::process_parity_traces(parity_traces);

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].balance, balance);
    assert_eq!(accounts[0].nonce, nonce);
}

#[test]
fn test_parity_contract_deployment() {
    let address = address!("0x2222222222222222222222222222222222222222");
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let balance = U256::from(0);
    let nonce = 1u64;

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
    let accounts = parity::process_parity_traces(parity_traces);

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].code_hash, keccak256(&code));
    assert_eq!(accounts[0].code, Some(Bytes::from(code)));
}

#[test]
fn test_parity_storage_updates() {
    let address = address!("0x3333333333333333333333333333333333333333");
    let slot1 = B256::from(U256::from(1));
    let slot2 = B256::from(U256::from(2));
    let slot3 = B256::from(U256::from(3));

    let slot1_hash = keccak256(slot1.0);
    let slot2_hash = keccak256(slot2.0);
    let slot3_hash = keccak256(slot3.0);

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
    let accounts = parity::process_parity_traces(parity_traces);

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].storage.get(&slot1_hash), Some(&U256::from(100)));
    assert_eq!(accounts[0].storage.get(&slot2_hash), Some(&U256::from(300)));
    assert_eq!(accounts[0].storage.get(&slot3_hash), Some(&U256::ZERO));
}

#[test]
fn test_parity_selfdestruct_balance_transfer() {
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
    let accounts = parity::process_parity_traces(parity_traces);

    assert_eq!(accounts.len(), 1);
    assert!(!accounts[0].deleted);
    assert_eq!(accounts[0].balance, U256::ZERO);
}

#[test]
fn test_parity_multiple_transactions() {
    let address = address!("0x6666666666666666666666666666666666666666");

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
    let accounts = parity::process_parity_traces(parity_traces);

    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].balance, U256::from(800));
    assert_eq!(accounts[0].nonce, 2);
}

// ==================== Complex Scenario Tests ====================

#[test]
fn test_geth_complex_multi_account_scenario() {
    let addr1 = address!("0x1111111111111111111111111111111111111111");
    let addr2 = address!("0x2222222222222222222222222222222222222222");
    let addr3 = address!("0x3333333333333333333333333333333333333333");

    let slot = B256::from(U256::from(42));

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
            code: Some(Bytes::from(vec![0x60, 0x80])),
            storage: geth_storage,
        },
    );
    geth_post.insert(
        addr3,
        GethAccountState {
            balance: Some(U256::ZERO),
            nonce: None,
            code: None,
            storage: BTreeMap::new(),
        },
    );

    let traces = vec![make_diff_trace(geth_pre, geth_post)];

    let addresses = extract_touched_addresses(&traces);
    assert_eq!(addresses.len(), 3);
    assert!(addresses.contains(&addr1));
    assert!(addresses.contains(&addr2));
    assert!(addresses.contains(&addr3));

    let storage = extract_touched_storage(&traces);
    assert!(storage.get(&addr2).unwrap().contains(&slot));

    let post_state = extract_post_state(&traces);
    assert_eq!(
        post_state.get(&addr1).unwrap().balance,
        Some(U256::from(1000))
    );
    assert_eq!(post_state.get(&addr2).unwrap().nonce, Some(1));
    assert_eq!(post_state.get(&addr3).unwrap().balance, Some(U256::ZERO));
    // addr3 nonce not in POST - should be read from StateReader
    assert!(post_state.get(&addr3).unwrap().nonce.is_none());
}

#[test]
fn test_geth_empty_block() {
    let traces: Vec<TraceResult> = vec![];

    let addresses = extract_touched_addresses(&traces);
    let storage = extract_touched_storage(&traces);
    let post_state = extract_post_state(&traces);

    assert!(addresses.is_empty());
    assert!(storage.is_empty());
    assert!(post_state.is_empty());
}

#[test]
fn test_geth_mixed_success_and_failed_transactions() {
    let address = address!("0x4242424242424242424242424242424242424242");

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

    let post_state = extract_post_state(&traces);
    assert_eq!(
        post_state.get(&address).unwrap().balance,
        Some(U256::from(100))
    );
}

#[test]
fn test_geth_many_storage_slots() {
    let address = address!("0x4242424242424242424242424242424242424242");

    let mut post_storage = BTreeMap::new();
    for i in 0u8..100 {
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

    let post_state = extract_post_state(&traces);
    assert_eq!(post_state.get(&address).unwrap().storage.len(), 100);
}
