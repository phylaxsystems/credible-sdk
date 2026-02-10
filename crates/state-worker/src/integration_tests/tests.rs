use crate::{
    genesis,
    integration_tests::setup::TestInstance,
};
use alloy::primitives::{
    B256,
    Bytes,
    U256,
    keccak256,
};
use anyhow::{
    Context,
    Result,
};
use credible_utils::hex::decode_hex_trimmed_0x;
use mdbx::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
};
use serde_json::json;
use std::{
    collections::HashMap,
    time::Duration,
};
use tokio::time::sleep;
use tracing_test::traced_test;

fn address_hash_from_hex(address: &str) -> Result<alloy::primitives::B256> {
    let bytes = decode_hex_trimmed_0x(address)
        .with_context(|| format!("Failed to decode address hex {address}"))?;
    Ok(keccak256(bytes))
}

fn reader_latest_block(
    reader: &dyn crate::integration_tests::setup::ReaderHelper,
) -> Result<Option<u64>> {
    reader
        .latest_block_number_boxed()
        .map_err(anyhow::Error::msg)
}

fn reader_get_account(
    reader: &dyn crate::integration_tests::setup::ReaderHelper,
    address_hash: mdbx::AddressHash,
    block: u64,
) -> Result<Option<mdbx::AccountInfo>> {
    reader
        .get_account_boxed(address_hash, block)
        .map_err(anyhow::Error::msg)
}

fn reader_get_storage(
    reader: &dyn crate::integration_tests::setup::ReaderHelper,
    address_hash: mdbx::AddressHash,
    slot_hash: alloy::primitives::B256,
    block: u64,
) -> Result<Option<U256>> {
    reader
        .get_storage_boxed(address_hash, slot_hash, block)
        .map_err(anyhow::Error::msg)
}

fn make_storage(entries: &[(u8, u64)]) -> HashMap<B256, U256> {
    let mut storage = HashMap::new();
    for (slot, value) in entries {
        storage.insert(B256::repeat_byte(*slot), U256::from(*value));
    }
    storage
}

fn make_account_state(
    address_hash: AddressHash,
    balance: u64,
    nonce: u64,
    code: Option<Bytes>,
    storage_entries: &[(u8, u64)],
) -> AccountState {
    AccountState {
        address_hash,
        balance: U256::from(balance),
        nonce,
        code_hash: B256::repeat_byte(0xCC),
        code,
        storage: make_storage(storage_entries),
        deleted: false,
    }
}

fn make_update(
    block_number: u64,
    block_hash: u8,
    state_root: u8,
    account: AccountState,
) -> BlockStateUpdate {
    BlockStateUpdate {
        block_number,
        block_hash: B256::repeat_byte(block_hash),
        state_root: B256::repeat_byte(state_root),
        accounts: vec![account],
    }
}

fn assert_block_available(reader: &impl Reader, block: u64) -> Result<()> {
    let available = reader
        .is_block_available(block)
        .map_err(|e| anyhow::Error::msg(format!("Block {block} availability check failed: {e}")))?;
    assert!(available, "Block {block} should be available");
    Ok(())
}

fn assert_block_unavailable(reader: &impl Reader, block: u64) -> Result<()> {
    let available = reader
        .is_block_available(block)
        .map_err(|e| anyhow::Error::msg(format!("Block {block} availability check failed: {e}")))?;
    assert!(!available, "Block {block} should be unavailable");
    Ok(())
}

fn assert_latest_block(reader: &impl Reader, expected: u64) -> Result<()> {
    let latest = reader
        .latest_block_number()
        .map_err(|e| anyhow::Error::msg(format!("Failed to get latest block number: {e}")))?;
    assert_eq!(latest, Some(expected));
    Ok(())
}

fn assert_account(
    reader: &impl Reader,
    address_hash: AddressHash,
    block: u64,
    expected_balance: u64,
    expected_nonce: u64,
) -> Result<()> {
    let account = reader
        .get_account(address_hash, block)
        .map_err(|e| anyhow::Error::msg(format!("Should get account: {e}")))?
        .context("Account should exist")?;
    assert_eq!(account.balance, U256::from(expected_balance));
    assert_eq!(account.nonce, expected_nonce);
    Ok(())
}

fn assert_storage(
    reader: &impl Reader,
    address_hash: AddressHash,
    slot: u8,
    block: u64,
    expected: u64,
) -> Result<()> {
    let value = reader
        .get_storage(address_hash, B256::repeat_byte(slot), block)
        .map_err(|e| anyhow::Error::msg(format!("Should get storage: {e}")))?
        .context("Storage should exist")?;
    assert_eq!(value, U256::from(expected));
    Ok(())
}

fn trace_state_changes_block_1(test_address: &str) -> serde_json::Value {
    json!({
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
    })
}

fn trace_state_changes_block_2(test_address: &str) -> serde_json::Value {
    json!({
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
    })
}

fn trace_state_changes_block_3(test_address: &str) -> serde_json::Value {
    json!({
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
    })
}

fn trace_storage_two_slots(test_address: &str) -> serde_json::Value {
    json!({
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
    })
}

fn trace_storage_delete_slot(test_address: &str) -> serde_json::Value {
    json!({
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
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_hydrates_genesis_state() -> Result<()> {
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
        genesis::parse_from_str(&genesis_json).context("failed to parse test genesis json")?;

    // Create a new instance with genesis based on the backend
    let instance = TestInstance::new_mdbx_with_genesis(genesis_state)
        .await
        .map_err(anyhow::Error::msg)?;
    let _ = instance.mdbx_path().map_err(anyhow::Error::msg)?;
    let _ = &instance.handle_worker;

    sleep(Duration::from_millis(500)).await;

    // Use the reader helper for backend-agnostic verification
    let reader = instance.create_reader();

    let address_hash = address_hash_from_hex(genesis_account)?;

    // Wait for genesis to be processed
    let mut account = None;
    for _ in 0..30 {
        account = reader_get_account(&*reader, address_hash.into(), 0)
            .ok()
            .flatten();
        if account.is_some() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let account = account.context("Genesis account should exist")?;
    assert_eq!(account.balance, U256::from(5), "Balance should be 5");
    assert_eq!(account.nonce, 1, "Nonce should be 1");

    // Verify storage slots
    let slot_zero_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let storage_zero = reader_get_storage(&*reader, address_hash.into(), slot_zero_hash, 0)?
        .context("Storage slot 0 should exist")?;
    assert_eq!(storage_zero, U256::from(1), "Storage slot 0 should be 1");

    let storage_one = reader_get_storage(&*reader, address_hash.into(), slot_one_hash, 0)?
        .context("Storage slot 1 should exist")?;
    assert_eq!(storage_one, U256::from(2), "Storage slot 1 should be 2");

    // Verify latest block is 0 (genesis)
    let latest = reader_latest_block(&*reader).context("Failed to get latest block")?;
    assert_eq!(latest, Some(0), "Latest block should be 0 (genesis)");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_processes_multiple_state_changes() -> Result<()> {
    let instance = TestInstance::new_mdbx_with_setup(|_| {})
        .await
        .map_err(anyhow::Error::msg)?;
    sleep(Duration::from_millis(200)).await;

    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = address_hash_from_hex(test_address)?;
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let geth_trace_block_1 = trace_state_changes_block_1(test_address);
    let geth_trace_block_2 = trace_state_changes_block_2(test_address);
    let geth_trace_block_3 = trace_state_changes_block_3(test_address);

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
        latest_block = reader_latest_block(&*reader).context("Failed to get latest block")?;
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

    let account =
        reader_get_account(&*reader, address_hash.into(), 3)?.context("Account should exist")?;
    assert_eq!(account.balance, U256::from(2));
    assert_eq!(account.nonce, 1);

    let storage_value = reader_get_storage(&*reader, address_hash.into(), slot_one_hash, 3)?
        .context("Storage slot should exist")?;
    assert_eq!(storage_value, U256::from(3));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_handles_rapid_state_changes() -> Result<()> {
    let instance = TestInstance::new_mdbx_with_setup_and_genesis(|_| {}, None)
        .await
        .map_err(anyhow::Error::msg)?;
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
        current_block = reader_latest_block(&*reader).context("Failed to get latest block")?;
        if current_block == Some(5) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let current_block_num = current_block.context("Failed to get current block after timeout")?;

    assert_eq!(
        current_block_num, 5,
        "Expected 5 blocks to be processed, got {current_block_num}",
    );
    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_non_consecutive_blocks_critical_alert() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
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
    Ok(())
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_missing_state_logs_warning() -> Result<()> {
    // Test that a single missing block occurrence logs a warning (not critical).
    // Critical alerts are only triggered after MAX_MISSING_BLOCK_RETRIES consecutive
    // failures to recover from a missing block.
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
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

    // Should log warning for missing block, not critical on first occurrence
    assert!(logs_contain("Missing block"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_circular_buffer_rotation_with_state_diffs() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
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
        latest = reader_latest_block(&*reader).context("Failed to get latest block")?;
        if latest == Some(6) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(6), "Latest block should be 6 after rotation");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_zero_storage_values_are_deleted() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = address_hash_from_hex(test_address)?;

    // Block 1: Set storage slots 0 and 1 to non-zero values
    let geth_trace_block_1 = trace_storage_two_slots(test_address);

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
        slot_zero = reader_get_storage(&*reader, address_hash.into(), slot_zero_hash, 1)
            .ok()
            .flatten();
        slot_one = reader_get_storage(&*reader, address_hash.into(), slot_one_hash, 1)
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
    let geth_trace_block_2 = trace_storage_delete_slot(test_address);

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", geth_trace_block_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    // Wait for block 2 to be processed
    for _ in 0..30 {
        if let Some(2) = reader_latest_block(&*reader).ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Verify slot 0 still exists and slot 1 was deleted
    let slot_zero_after = reader_get_storage(&*reader, address_hash.into(), slot_zero_hash, 2)
        .ok()
        .flatten();

    let slot_one_after = reader_get_storage(&*reader, address_hash.into(), slot_one_hash, 2)
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
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_account_does_not_load_storage() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = address_hash_from_hex(test_address)?;

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
        if let Some(1) = reader_latest_block(&*reader).ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Test get_account returns AccountInfo (without storage field at all)
    let account_info =
        reader_get_account(&*reader, address_hash.into(), 1)?.context("Account should exist")?;

    assert_eq!(account_info.balance, U256::from(0x100));
    assert_eq!(account_info.nonce, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_storage_returns_individual_slot() -> Result<()> {
    let instance = TestInstance::new_mdbx().await.map_err(anyhow::Error::msg)?;
    let test_address = "0xcccccccccccccccccccccccccccccccccccccccc";
    let address_hash = address_hash_from_hex(test_address)?;

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
        if let Some(1) = reader_latest_block(&*reader).ok().flatten() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Use get_storage for individual slot lookup
    let slot_5_hash = keccak256(U256::from(5).to_be_bytes::<32>());

    let value = reader_get_storage(&*reader, address_hash.into(), slot_5_hash, 1)?
        .context("Storage slot should exist")?;

    assert_eq!(value, U256::from(0xff));

    // Verify non-existent slot returns None
    let non_existent_slot = keccak256(U256::from(999).to_be_bytes::<32>());

    let missing = reader_get_storage(&*reader, address_hash.into(), non_existent_slot, 1)
        .context("Failed to get storage")?;

    assert!(missing.is_none(), "Non-existent slot should return None");
    Ok(())
}

#[tokio::test]
async fn test_mdbx_bootstrap_recovery_without_diffs() -> Result<()> {
    use mdbx::{
        StateWriter,
        Writer,
        common::CircularBufferConfig,
    };

    let mdbx_dir =
        crate::integration_tests::mdbx_fixture::MdbxTestDir::new().map_err(anyhow::Error::msg)?;
    let mdbx_path = mdbx_dir.path_str().map_err(anyhow::Error::msg)?;
    let config = CircularBufferConfig::new(3).context("Failed to build circular buffer config")?;

    let writer = StateWriter::new(mdbx_path, config.clone())
        .context("Failed to create MDBX test instance")?;

    let test_address_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));
    let accounts = vec![make_account_state(
        test_address_hash,
        1000,
        5,
        Some(Bytes::from(vec![0x60, 0x80])),
        &[(0x01, 100), (0x02, 200)],
    )];

    let stats = writer
        .bootstrap_from_snapshot(
            accounts.clone(),
            100,
            B256::repeat_byte(0x11),
            B256::repeat_byte(0x22),
        )
        .context("bootstrap should succeed")?;

    assert_eq!(
        stats.accounts_written, 3,
        "should write to all 3 namespaces"
    );

    let reader = writer.reader();

    assert_block_available(reader, 100)?;
    assert_latest_block(reader, 100)?;
    assert_account(reader, test_address_hash, 100, 1000, 5)?;
    assert_storage(reader, test_address_hash, 0x01, 100, 100)?;

    let update_101 = make_update(
        101,
        0x33,
        0x44,
        make_account_state(test_address_hash, 1100, 6, None, &[(0x01, 101)]),
    );
    let stats_101 = writer
        .commit_block(&update_101)
        .context("Block 101 should commit")?;
    assert_eq!(
        stats_101.diffs_applied, 0,
        "Block 101 should NOT need any diffs after bootstrap"
    );
    assert_block_available(reader, 101)?;
    assert_latest_block(reader, 101)?;
    assert_account(reader, test_address_hash, 101, 1100, 6)?;

    let update_102 = make_update(
        102,
        0x55,
        0x66,
        make_account_state(test_address_hash, 1200, 7, None, &[(0x01, 102)]),
    );
    let stats_102 = writer
        .commit_block(&update_102)
        .context("Block 102 should commit")?;
    assert_eq!(
        stats_102.diffs_applied, 1,
        "Block 102 should apply 1 diff (block 101)"
    );
    assert_block_available(reader, 102)?;

    let update_103 = make_update(
        103,
        0x77,
        0x88,
        make_account_state(test_address_hash, 1300, 8, None, &[(0x01, 103)]),
    );
    let stats_103 = writer
        .commit_block(&update_103)
        .context("Block 103 should commit")?;
    assert_eq!(
        stats_103.diffs_applied, 2,
        "Block 103 should apply 2 diffs (101, 102)"
    );
    assert_block_available(reader, 103)?;
    assert_latest_block(reader, 103)?;
    assert_account(reader, test_address_hash, 103, 1300, 8)?;

    assert_block_unavailable(reader, 100)?;
    assert_block_available(reader, 101)?;
    assert_block_available(reader, 102)?;
    assert_block_available(reader, 103)?;
    Ok(())
}
