#![allow(clippy::too_many_lines)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]

use crate::{
    genesis,
    integration_tests::setup::TestInstance,
};
use alloy::primitives::{
    U256,
    keccak256,
};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_hydrates_genesis_state() {
    let genesis_account = "0x00000000000000000000000000000000000000aa";
    let genesis_code = "0x6000";

    let genesis_json = format!(
        r#"{{
            "alloc": {{
                "{account}": {{
                    "balance": "0x5",
                    "nonce": "0x1",
                    "code": "{code}",
                    "storage": {{
                        "0x0": "0x1",
                        "0x1": "0x2"
                    }}
                }}
            }}
        }}"#,
        account = genesis_account.trim_start_matches("0x"),
        code = genesis_code
    );
    let genesis_state =
        genesis::parse_from_str(&genesis_json).expect("failed to parse test genesis json");

    // Create a new instance with genesis based on the backend
    let instance = TestInstance::new_mdbx_with_genesis(genesis_state)
        .await
        .expect("Failed to create MDBX instance with genesis");

    sleep(Duration::from_millis(500)).await;

    // Use the reader helper for backend-agnostic verification
    let reader = instance.create_reader();

    let address_hash = keccak256(hex::decode(genesis_account.trim_start_matches("0x")).unwrap());

    // Wait for genesis to be processed
    let mut account = None;
    for _ in 0..30 {
        account = reader
            .get_account_boxed(address_hash.into(), 0)
            .ok()
            .flatten();
        if account.is_some() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let account = account.expect("Genesis account should exist");
    assert_eq!(account.balance, U256::from(5), "Balance should be 5");
    assert_eq!(account.nonce, 1, "Nonce should be 1");

    // Verify storage slots
    let slot_zero_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let storage_zero = reader
        .get_storage_boxed(address_hash.into(), slot_zero_hash, 0)
        .expect("Failed to get storage slot 0")
        .expect("Storage slot 0 should exist");
    assert_eq!(storage_zero, U256::from(1), "Storage slot 0 should be 1");

    let storage_one = reader
        .get_storage_boxed(address_hash.into(), slot_one_hash, 0)
        .expect("Failed to get storage slot 1")
        .expect("Storage slot 1 should exist");
    assert_eq!(storage_one, U256::from(2), "Storage slot 1 should be 2");

    // Verify latest block is 0 (genesis)
    let latest = reader
        .latest_block_number_boxed()
        .expect("Failed to get latest block");
    assert_eq!(latest, Some(0), "Latest block should be 0 (genesis)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_processes_multiple_state_changes() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    sleep(Duration::from_millis(200)).await;

    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let geth_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "pre": {},
                    "post": {
                        test_address: {
                            "balance": "0x1",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002"
                            }
                        }
                    }
                }
            }
        ]
    });

    let geth_trace_block_2 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "result": {
                    "pre": {
                        test_address: {
                            "balance": "0x1",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002"
                            }
                        }
                    },
                    "post": {
                        test_address: {
                            "balance": "0x2",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {}
                        }
                    }
                }
            }
        ]
    });

    let geth_trace_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "result": {
                    "pre": {
                        test_address: {
                            "balance": "0x2",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002"
                            }
                        }
                    },
                    "post": {
                        test_address: {
                            "balance": "0x2",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000003"
                            }
                        }
                    }
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_3);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    // Use the reader helper for backend-agnostic verification
    let reader = instance.create_reader();

    let mut latest_block: Option<u64> = None;
    for _ in 0..30 {
        latest_block = reader
            .latest_block_number_boxed()
            .expect("Failed to get latest block");
        if latest_block == Some(3) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        latest_block,
        Some(3),
        "Expected current block to be 3, got {latest_block:?}",
    );

    let account = reader
        .get_account_boxed(address_hash.into(), 3)
        .expect("Failed to get account")
        .expect("Account should exist");
    assert_eq!(account.balance, U256::from(2));
    assert_eq!(account.nonce, 1);

    let storage_value = reader
        .get_storage_boxed(address_hash.into(), slot_one_hash, 3)
        .expect("Failed to get storage")
        .expect("Storage slot should exist");
    assert_eq!(storage_value, U256::from(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_handles_rapid_state_changes() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    for _ in 0..5 {
        let geth_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": []
        });

        instance
            .http_server_mock
            .add_response("debug_traceBlockByNumber", geth_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }

    sleep(Duration::from_millis(200)).await;

    let reader = instance.create_reader();

    let mut current_block: Option<u64> = None;
    for _ in 0..30 {
        current_block = reader
            .latest_block_number_boxed()
            .expect("Failed to get latest block");
        if current_block == Some(5) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let current_block_num = current_block.expect("Failed to get current block after timeout");

    assert_eq!(
        current_block_num, 5,
        "Expected 5 blocks to be processed, got {current_block_num}",
    );
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_non_consecutive_blocks_critical_alert() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    // Setup valid trace response for block 1 with no traces
    let valid_geth_trace_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": []
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", valid_geth_trace_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    // Mock an error for block 2 to simulate it not existing
    instance
        .http_server_mock
        .mock_rpc_error("debug_traceBlockByNumber", -32000, "block not found");
    instance.http_server_mock.send_new_head_with_block_number(2);
    sleep(Duration::from_millis(50)).await;

    // Setup valid trace response for block 3 with no traces
    let valid_geth_trace_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": []
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", valid_geth_trace_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;

    assert!(logs_contain("Missing block"));
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_missing_state_critical_alert() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    let geth_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": []
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    let geth_trace_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": []
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;

    assert!(logs_contain("critical"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_circular_buffer_rotation_with_state_diffs() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    for _ in 0..6 {
        let geth_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": []
        });

        instance
            .http_server_mock
            .add_response("debug_traceBlockByNumber", geth_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }

    sleep(Duration::from_millis(300)).await;

    let reader = instance.create_reader();

    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = reader
            .latest_block_number_boxed()
            .expect("Failed to get latest block");
        if latest == Some(6) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(6), "Latest block should be 6 after rotation");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_zero_storage_values_are_deleted() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Set storage slots 0 and 1 to non-zero values
    let geth_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "pre": {},
                    "post": {
                        test_address: {
                            "balance": "0x100",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000000": "0x0000000000000000000000000000000000000000000000000000000000000001",
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002"
                            }
                        }
                    }
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    let reader = instance.create_reader();
    let slot_zero_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    // Wait for block 1 to be processed and verify both slots exist
    let mut slot_zero: Option<U256> = None;
    let mut slot_one: Option<U256> = None;
    for _ in 0..30 {
        slot_zero = reader
            .get_storage_boxed(address_hash.into(), slot_zero_hash, 1)
            .ok()
            .flatten();
        slot_one = reader
            .get_storage_boxed(address_hash.into(), slot_one_hash, 1)
            .ok()
            .flatten();

        if slot_zero.is_some() && slot_one.is_some() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(slot_zero.is_some(), "Slot 0 should exist after block 1");
    assert!(slot_one.is_some(), "Slot 1 should exist after block 1");

    // Block 2: Set slot 1 to zero (should be deleted)
    let geth_trace_block_2 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "result": {
                    "pre": {
                        test_address: {
                            "balance": "0x100",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002"
                            }
                        }
                    },
                    "post": {
                        test_address: {
                            "storage": {}
                        }
                    }
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    // Wait for block 2 to be processed
    for _ in 0..30 {
        if let Some(2) = reader.latest_block_number_boxed().ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Verify slot 0 still exists and slot 1 was deleted
    let slot_zero_after = reader
        .get_storage_boxed(address_hash.into(), slot_zero_hash, 2)
        .ok()
        .flatten();

    let slot_one_after = reader
        .get_storage_boxed(address_hash.into(), slot_one_hash, 2)
        .ok()
        .flatten();

    assert!(
        slot_zero_after.is_some(),
        "Slot 0 should still exist after block 2"
    );
    assert!(
        slot_one_after.is_none(),
        "Slot 1 should be deleted after being set to zero in block 2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_account_does_not_load_storage() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Create account with multiple storage slots
    let geth_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "pre": {},
                    "post": {
                        test_address: {
                            "balance": "0x100",
                            "nonce": 1,
                            "code": "0x6080",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000000": "0x0000000000000000000000000000000000000000000000000000000000000001",
                                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x0000000000000000000000000000000000000000000000000000000000000002",
                                "0x0000000000000000000000000000000000000000000000000000000000000002": "0x0000000000000000000000000000000000000000000000000000000000000003"
                            }
                        }
                    }
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    // Wait for block to be processed
    let reader = instance.create_reader();

    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Test get_account returns AccountInfo (without storage field at all)
    let account_info = reader
        .get_account_boxed(address_hash.into(), 1)
        .expect("Failed to get account")
        .expect("Account should exist");

    assert_eq!(account_info.balance, U256::from(0x100));
    assert_eq!(account_info.nonce, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_storage_returns_individual_slot() {
    let instance = TestInstance::new_mdbx()
        .await
        .expect("Failed to create MDBX test instance");
    let test_address = "0xcccccccccccccccccccccccccccccccccccccccc";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Create account with storage
    let geth_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "pre": {},
                    "post": {
                        test_address: {
                            "balance": "0x300",
                            "nonce": 1,
                            "code": "0x",
                            "storage": {
                                "0x0000000000000000000000000000000000000000000000000000000000000005": "0x00000000000000000000000000000000000000000000000000000000000000ff"
                            }
                        }
                    }
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    // Wait for block to be processed
    let reader = instance.create_reader();

    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Use get_storage for individual slot lookup
    let slot_5_hash = keccak256(U256::from(5).to_be_bytes::<32>());

    let value = reader
        .get_storage_boxed(address_hash.into(), slot_5_hash, 1)
        .expect("Failed to get storage")
        .expect("Storage slot should exist");

    assert_eq!(value, U256::from(0xff));

    // Verify non-existent slot returns None
    let non_existent_slot = keccak256(U256::from(999).to_be_bytes::<32>());

    let missing = reader
        .get_storage_boxed(address_hash.into(), non_existent_slot, 1)
        .expect("Failed to get storage");

    assert!(missing.is_none(), "Non-existent slot should return None");
}

#[tokio::test]
async fn test_mdbx_bootstrap_recovery_without_diffs() {
    use alloy::primitives::{
        B256,
        U256,
    };
    use state_store::{
        AccountState,
        AddressHash,
        BlockStateUpdate,
        Reader,
        Writer,
        mdbx::{
            StateWriter,
            common::CircularBufferConfig,
        },
    };
    use std::collections::HashMap;

    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new()
        .expect("Failed to create MDBX test dir");
    let mdbx_path = mdbx_dir.path_str();
    let config = CircularBufferConfig::new(3).unwrap();

    let writer = StateWriter::new(mdbx_path, config.clone()).unwrap();

    let test_address_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));
    let mut storage = HashMap::new();
    storage.insert(B256::repeat_byte(0x01), U256::from(100));
    storage.insert(B256::repeat_byte(0x02), U256::from(200));

    let accounts = vec![AccountState {
        address_hash: test_address_hash,
        balance: U256::from(1000),
        nonce: 5,
        code_hash: B256::repeat_byte(0xCC),
        code: Some(alloy::primitives::Bytes::from(vec![0x60, 0x80])),
        storage,
        deleted: false,
    }];

    // =========================================================================
    // Use bootstrap_from_snapshot instead of commit_block
    // This simulates what geth-dump does: populate ALL namespaces with
    // identical state, NO state diffs stored
    // =========================================================================
    let stats = writer
        .bootstrap_from_snapshot(
            accounts.clone(),
            100,
            B256::repeat_byte(0x11),
            B256::repeat_byte(0x22),
        )
        .expect("bootstrap should succeed");

    // Verify bootstrap wrote to all 3 namespaces
    assert_eq!(
        stats.accounts_written, 3,
        "should write to all 3 namespaces"
    );

    let reader = writer.reader();

    assert!(
        reader.is_block_available(100).unwrap(),
        "Block 100 should be available"
    );
    assert_eq!(reader.latest_block_number().unwrap(), Some(100));

    // Verify account exists and has correct data
    let account = reader
        .get_account(test_address_hash, 100)
        .expect("Should get account")
        .expect("Account should exist");

    assert_eq!(account.balance, U256::from(1000));
    assert_eq!(account.nonce, 5);

    // Verify storage
    let slot_value = reader
        .get_storage(test_address_hash, B256::repeat_byte(0x01), 100)
        .expect("Should get storage")
        .expect("Storage should exist");

    assert_eq!(slot_value, U256::from(100));

    // =========================================================================
    // Now test that we can process subsequent blocks WITHOUT pre-existing diffs
    // Block 101 → namespace 2 (101 % 3 = 2), namespace already has block 100
    // No diffs needed because start_block = 101 and block_number = 101
    // =========================================================================
    let update_101 = BlockStateUpdate {
        block_number: 101,
        block_hash: B256::repeat_byte(0x33),
        state_root: B256::repeat_byte(0x44),
        accounts: vec![AccountState {
            address_hash: test_address_hash,
            balance: U256::from(1100),
            nonce: 6,
            code_hash: B256::repeat_byte(0xCC),
            code: None,
            storage: {
                let mut s = HashMap::new();
                s.insert(B256::repeat_byte(0x01), U256::from(101));
                s
            },
            deleted: false,
        }],
    };

    let stats_101 = writer
        .commit_block(&update_101)
        .expect("Block 101 should commit");

    // KEY ASSERTION: No diffs needed for first block after bootstrap
    assert_eq!(
        stats_101.diffs_applied, 0,
        "Block 101 should NOT need any diffs after bootstrap"
    );

    assert!(reader.is_block_available(101).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(101));

    let account_101 = reader
        .get_account(test_address_hash, 101)
        .expect("Should get account")
        .expect("Account should exist");

    assert_eq!(account_101.balance, U256::from(1100));
    assert_eq!(account_101.nonce, 6);

    // =========================================================================
    // Block 102 → namespace 0 (102 % 3 = 0), namespace has block 100
    // Needs diff from block 101 (which was stored when we committed 101)
    // =========================================================================
    let update_102 = BlockStateUpdate {
        block_number: 102,
        block_hash: B256::repeat_byte(0x55),
        state_root: B256::repeat_byte(0x66),
        accounts: vec![AccountState {
            address_hash: test_address_hash,
            balance: U256::from(1200),
            nonce: 7,
            code_hash: B256::repeat_byte(0xCC),
            code: None,
            storage: {
                let mut s = HashMap::new();
                s.insert(B256::repeat_byte(0x01), U256::from(102));
                s
            },
            deleted: false,
        }],
    };

    let stats_102 = writer
        .commit_block(&update_102)
        .expect("Block 102 should commit");

    // Block 102 needs 1 diff (block 101)
    assert_eq!(
        stats_102.diffs_applied, 1,
        "Block 102 should apply 1 diff (block 101)"
    );

    assert!(reader.is_block_available(102).unwrap());

    // =========================================================================
    // Block 103 → namespace 1 (103 % 3 = 1), namespace has block 100
    // Needs diffs from blocks 101 AND 102
    // =========================================================================
    let update_103 = BlockStateUpdate {
        block_number: 103,
        block_hash: B256::repeat_byte(0x77),
        state_root: B256::repeat_byte(0x88),
        accounts: vec![AccountState {
            address_hash: test_address_hash,
            balance: U256::from(1300),
            nonce: 8,
            code_hash: B256::repeat_byte(0xCC),
            code: None,
            storage: {
                let mut s = HashMap::new();
                s.insert(B256::repeat_byte(0x01), U256::from(103));
                s
            },
            deleted: false,
        }],
    };

    let stats_103 = writer
        .commit_block(&update_103)
        .expect("Block 103 should commit");

    // Block 103 needs 2 diffs (blocks 101 and 102)
    assert_eq!(
        stats_103.diffs_applied, 2,
        "Block 103 should apply 2 diffs (101, 102)"
    );

    assert!(reader.is_block_available(103).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(103));

    let account_103 = reader
        .get_account(test_address_hash, 103)
        .expect("Should get account")
        .expect("Account should exist");

    assert_eq!(account_103.balance, U256::from(1300));
    assert_eq!(account_103.nonce, 8);

    // Verify old block rotated out
    assert!(
        !reader.is_block_available(100).unwrap(),
        "Block 100 should be rotated out"
    );

    // Blocks 101, 102, 103 should all be available
    assert!(reader.is_block_available(101).unwrap());
    assert!(reader.is_block_available(102).unwrap());
    assert!(reader.is_block_available(103).unwrap());
}
