#![allow(clippy::too_many_lines)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]

use crate::{
    cli::ProviderType,
    connect_provider,
    genesis,
    integration_tests::{
        redis_fixture::get_shared_redis,
        setup::TestBackend,
    },
    state,
    worker::StateWorker,
};
use alloy::primitives::{
    B256,
    U256,
    keccak256,
};
use redis::Commands;
use serde_json::json;
use state_store::{
    BlockStateUpdate,
    Reader,
    Writer,
    redis::{
        ChunkedWriteConfig,
        CircularBufferConfig,
        StateWriter,
    },
};
use state_worker_test_macros::{
    database_test,
    traced_database_test,
};
use std::time::Duration;
use tokio::{
    sync::broadcast,
    time::sleep,
};

// Re-import from parent module for use in test bodies
use super::TestInstance;

/// Helper function to get the latest block number from the circular buffer.
fn get_latest_block_from_redis(
    conn: &mut redis::Connection,
    base_namespace: &str,
    buffer_size: usize,
) -> Option<u64> {
    let mut max_block: Option<u64> = None;

    for idx in 0..buffer_size {
        let namespace = format!("{base_namespace}:{idx}");
        let block_key = format!("{namespace}:block");

        if let Ok(Some(block_str)) = conn.get::<_, Option<String>>(&block_key)
            && let Ok(block_num) = block_str.parse::<u64>()
        {
            max_block = Some(max_block.map_or(block_num, |current| current.max(block_num)));
        }
    }

    max_block
}

/// Wait for a specific block to be processed in Redis (with polling and timeout).
async fn wait_for_block(
    redis_url: &str,
    base_namespace: &str,
    buffer_size: usize,
    expected_block: u64,
    timeout_secs: u64,
) -> Result<(), String> {
    let client =
        redis::Client::open(redis_url).map_err(|e| format!("Failed to connect to Redis: {e}"))?;
    let mut conn = client
        .get_connection()
        .map_err(|e| format!("Failed to get Redis connection: {e}"))?;

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    loop {
        if let Some(latest) = get_latest_block_from_redis(&mut conn, base_namespace, buffer_size)
            && latest >= expected_block
        {
            return Ok(());
        }

        if start.elapsed() > timeout {
            let latest = get_latest_block_from_redis(&mut conn, base_namespace, buffer_size);
            return Err(format!(
                "Timeout waiting for block {expected_block}. Latest block: {latest:?}"
            ));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Helper to get the namespace for a given block number.
fn get_namespace_for_block(base_namespace: &str, block_number: u64, buffer_size: usize) -> String {
    let namespace_idx = usize::try_from(block_number).unwrap() % buffer_size;
    format!("{base_namespace}:{namespace_idx}")
}

#[database_test(all)]
async fn test_state_worker_hydrates_genesis_state(instance: TestInstance) {
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
    let instance = match instance.backend {
        TestBackend::Redis => {
            TestInstance::new_redis_with_genesis(genesis_state)
                .await
                .expect("Failed to create Redis instance with genesis")
        }
        TestBackend::Mdbx => {
            TestInstance::new_mdbx_with_genesis(genesis_state)
                .await
                .expect("Failed to create MDBX instance with genesis")
        }
    };

    sleep(Duration::from_millis(500)).await;

    // Use the reader helper for backend-agnostic verification
    let reader = instance.create_reader().expect("Failed to create reader");

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

#[database_test(all)]
async fn test_state_worker_processes_multiple_state_changes(instance: TestInstance) {
    sleep(Duration::from_millis(200)).await;

    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    let parity_trace_block_2 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    let parity_trace_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_3);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    // Use the reader helper for backend-agnostic verification
    let reader = instance.create_reader().expect("Failed to create reader");

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
}

#[database_test(all)]
async fn test_state_worker_handles_rapid_state_changes(instance: TestInstance) {
    for i in 0..5 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {
                    "transactionHash": format!("0x{:064x}", i + 1),
                    "trace": [],
                    "vmTrace": null,
                    "stateDiff": {},
                    "output": "0x"
                }
            ]
        });

        instance
            .http_server_mock
            .add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }

    sleep(Duration::from_millis(200)).await;

    let reader = instance.create_reader().expect("Failed to create reader");

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

#[traced_database_test(all)]
async fn test_state_worker_non_consecutive_blocks_critical_alert(instance: TestInstance) {
    // Setup valid trace response for block 1 with empty stateDiff
    let valid_parity_trace_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", valid_parity_trace_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    // Mock an error for block 2 to simulate it not existing
    instance.http_server_mock.mock_rpc_error(
        "trace_replayBlockTransactions",
        -32000,
        "block not found",
    );
    instance.http_server_mock.send_new_head_with_block_number(2);
    sleep(Duration::from_millis(50)).await;

    // Setup valid trace response for block 3 with empty stateDiff
    let valid_parity_trace_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", valid_parity_trace_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;

    assert!(logs_contain("Missing block"));
}

#[traced_database_test(all)]
async fn test_state_worker_missing_state_critical_alert(instance: TestInstance) {
    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    let parity_trace_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;

    assert!(logs_contain("critical"));
}

#[database_test(all)]
async fn test_circular_buffer_rotation_with_state_diffs(instance: TestInstance) {
    for i in 0..6 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {
                    "transactionHash": format!("0x{:064x}", i + 1),
                    "trace": [],
                    "vmTrace": null,
                    "stateDiff": {},
                    "output": "0x"
                }
            ]
        });

        instance
            .http_server_mock
            .add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }

    sleep(Duration::from_millis(300)).await;

    let reader = instance.create_reader().expect("Failed to create reader");

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

#[database_test(all)]
async fn test_zero_storage_values_are_deleted(instance: TestInstance) {
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Set storage slots 0 and 1 to non-zero values
    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": { "+": "0x100" },
                        "nonce": { "+": "0x1" },
                        "code": { "+": "0x" },
                        "storage": {
                            "0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x0000000000000000000000000000000000000000000000000000000000000001" },
                            "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x0000000000000000000000000000000000000000000000000000000000000002" }
                        }
                    }
                },
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;

    let reader = instance.create_reader().expect("Failed to create reader");
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
    let parity_trace_block_2 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": "=",
                        "nonce": "=",
                        "code": "=",
                        "storage": {
                            "0x0000000000000000000000000000000000000000000000000000000000000001": {
                                "*": {
                                    "from": "0x0000000000000000000000000000000000000000000000000000000000000002",
                                    "to": "0x0000000000000000000000000000000000000000000000000000000000000000"
                                }
                            }
                        }
                    }
                },
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_2);
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

#[database_test(all)]
async fn test_get_account_does_not_load_storage(instance: TestInstance) {
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Create account with multiple storage slots
    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": { "+": "0x100" },
                        "nonce": { "+": "0x1" },
                        "code": { "+": "0x6080" },
                        "storage": {
                            "0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x0000000000000000000000000000000000000000000000000000000000000001" },
                            "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x0000000000000000000000000000000000000000000000000000000000000002" },
                            "0x0000000000000000000000000000000000000000000000000000000000000002": { "+": "0x0000000000000000000000000000000000000000000000000000000000000003" }
                        }
                    }
                },
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    // Wait for block to be processed
    let reader = instance.create_reader().expect("Failed to create reader");

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

#[database_test(all)]
async fn test_get_storage_returns_individual_slot(instance: TestInstance) {
    let test_address = "0xcccccccccccccccccccccccccccccccccccccccc";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Create account with storage
    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": { "+": "0x300" },
                        "nonce": { "+": "0x1" },
                        "code": { "+": "0x" },
                        "storage": {
                            "0x0000000000000000000000000000000000000000000000000000000000000005": { "+": "0x00000000000000000000000000000000000000000000000000000000000000ff" }
                        }
                    }
                },
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    // Wait for block to be processed
    let reader = instance.create_reader().expect("Failed to create reader");

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

#[database_test(redis)]
async fn test_restart_continues_from_last_block(instance: TestInstance) {
    // Use shared Redis container
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();

    // Setup mock server
    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new()
        .await
        .expect("Failed to create mock server");

    // Setup traces for blocks 1-3
    for i in 1..=3 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [{
                "transactionHash": format!("0x{:064x}", i),
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }]
        });
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }

    // First run: process blocks 1-3
    {
        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "restart_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Start worker in background
        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });

        // Send 3 blocks
        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        // Wait for blocks to be processed (with polling and 5 second timeout)
        wait_for_block(&redis_url, "restart_test", 3, 3, 5)
            .await
            .expect("Should have processed 3 blocks");

        // Shutdown first worker
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }

    // Setup traces for blocks 4-6
    for i in 4..=6 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [{
                "transactionHash": format!("0x{:064x}", i),
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }]
        });
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }

    // Second run: restart and process blocks 4-6
    {
        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "restart_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        // Verify it starts from the correct block
        let start_block = writer_reader.latest_block_number().unwrap();
        assert_eq!(start_block, Some(3), "Should resume from block 3");

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker_handle = tokio::spawn(async move {
            // Pass None to use automatic detection
            worker.run(None, shutdown_rx).await
        });

        // Send blocks 4-6
        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        // Wait for blocks to be processed (with polling and 5 second timeout)
        wait_for_block(&redis_url, "restart_test", 3, 6, 5)
            .await
            .expect("Should have processed blocks 4-6 after restart");

        // Verify block hashes for all blocks exist
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        for block_num in 1..=6 {
            let key = format!("restart_test:block_hash:{block_num}");
            let hash: Option<String> = conn.get(&key).ok();
            assert!(
                hash.is_some(),
                "Block hash for block {block_num} should exist"
            );
        }

        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
}

/// Test restart with circular buffer wrap: verify diffs are correctly applied
#[database_test(redis)]
async fn test_restart_with_buffer_wrap_applies_diffs(instance: TestInstance) {
    // Use shared Redis container
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();

    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new()
        .await
        .expect("Failed to create mock server");

    // First run: process blocks to wrap the buffer (blocks 0-4)
    {
        for i in 0..=4 {
            let parity_trace = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{
                    "transactionHash": format!("0x{:064x}", i),
                    "trace": [],
                    "vmTrace": null,
                    "stateDiff": {},
                    "output": "0x"
                }]
            });
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "wrap_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });

        for _ in 0..4 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(300)).await;

        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "wrap_test", 3);
        assert_eq!(latest, Some(4), "Should have processed blocks 0-4");

        // Verify namespace:1 contains block 4 (wrapped from block 1)
        let namespace_1 = "wrap_test:1";
        let block_key = format!("{namespace_1}:block");
        let block: String = conn.get(&block_key).unwrap();
        assert_eq!(block, "4", "Namespace 1 should contain block 4 after wrap");

        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }

    // Second run: restart and verify diffs are still available for reconstruction
    {
        // Add traces for blocks 5-7
        for i in 5..=7 {
            let parity_trace = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{
                    "transactionHash": format!("0x{:064x}", i),
                    "trace": [],
                    "vmTrace": null,
                    "stateDiff": {},
                    "output": "0x"
                }]
            });
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "wrap_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker_handle = tokio::spawn(async move { worker.run(None, shutdown_rx).await });

        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(300)).await;

        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "wrap_test", 3);
        assert_eq!(latest, Some(7), "Should continue to block 7 after restart");

        // Verify diffs exist for recent blocks
        for block_num in 5..=7 {
            let diff_key = format!("wrap_test:diff:{block_num}");
            let exists: bool = redis::cmd("EXISTS")
                .arg(&diff_key)
                .query(&mut conn)
                .unwrap();
            assert!(exists, "Diff for block {block_num} should exist");
        }

        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
}

/// Test restart after crash during block processing
#[database_test(redis)]
async fn test_restart_after_mid_block_crash(instance: TestInstance) {
    // Use shared Redis container
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();

    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new()
        .await
        .expect("Failed to create mock server");

    // Process blocks 0-2 successfully
    for i in 0..=2 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [{
                "transactionHash": format!("0x{:064x}", i),
                "trace": [],
                "vmTrace": null,
                "stateDiff": {},
                "output": "0x"
            }]
        });
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }

    {
        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "crash_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });

        for _ in 0..2 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(200)).await;

        // Simulate crash - abrupt shutdown
        drop(shutdown_tx);
        drop(worker_handle);
    }

    // Restart: should continue from block 3
    {
        for i in 3..=5 {
            let parity_trace = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [{
                    "transactionHash": format!("0x{:064x}", i),
                    "trace": [],
                    "vmTrace": null,
                    "stateDiff": {},
                    "output": "0x"
                }]
            });
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let writer_reader = StateWriter::new(
            &redis_url,
            "crash_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        // Verify it detects the correct resume point
        let start_block = writer_reader.latest_block_number().unwrap();
        assert_eq!(start_block, Some(2), "Should detect last completed block");

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let worker_handle = tokio::spawn(async move { worker.run(None, shutdown_rx).await });

        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(300)).await;

        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "crash_test", 3);
        assert_eq!(latest, Some(5), "Should recover and process to block 5");

        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
}

#[database_test(redis)]
async fn test_get_full_account_returns_all_slots(instance: TestInstance) {
    use state_store::redis::StateReader;

    let test_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());

    // Block 1: Create account with storage
    let parity_trace_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": { "+": "0x200" },
                        "nonce": { "+": "0x5" },
                        "code": { "+": "0x" },
                        "storage": {
                            "0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x000000000000000000000000000000000000000000000000000000000000000a" },
                            "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x000000000000000000000000000000000000000000000000000000000000000b" }
                        }
                    }
                },
                "output": "0x"
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    // Wait for block to be processed
    let client =
        redis::Client::open(instance.redis_url().unwrap()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = get_namespace_for_block(&instance.namespace, 1, 3);
    let block_key = format!("{namespace}:block");

    for _ in 0..30 {
        if let Ok(b) = conn.get::<_, String>(&block_key)
            && b == "1"
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    let reader = StateReader::new(
        instance.redis_url().unwrap(),
        &instance.namespace,
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Test get_full_account returns AccountState
    let account = reader
        .get_full_account(address_hash.into(), 1)
        .expect("Failed to get account with storage")
        .expect("Account should exist");

    assert_eq!(account.balance, U256::from(0x200));
    assert_eq!(account.nonce, 5);
    assert_eq!(account.storage.len(), 2);

    // Verify storage values
    let slot_0_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_1_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    assert!(
        account.storage.contains_key(&slot_0_hash),
        "Storage should contain slot 0"
    );
    assert!(
        account.storage.contains_key(&slot_1_hash),
        "Storage should contain slot 1"
    );
}

#[database_test(redis)]
async fn test_eip2935_and_eip4788_system_contracts(instance: TestInstance) {
    use alloy::eips::{
        eip2935::{
            HISTORY_STORAGE_ADDRESS,
            HISTORY_STORAGE_CODE,
        },
        eip4788::{
            BEACON_ROOTS_ADDRESS,
            BEACON_ROOTS_CODE,
        },
    };
    use state_store::redis::StateReader;

    let test_address = "0xdddddddddddddddddddddddddddddddddddddddd";

    // Process 3 blocks with a regular transaction to verify system calls merge with trace state
    for i in 1..=3 {
        let parity_trace = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [{
                "transactionHash": format!("0x{:064x}", i),
                "trace": [],
                "vmTrace": null,
                "stateDiff": {
                    test_address: {
                        "balance": { "+": format!("0x{:x}", i * 0x100) },
                        "nonce": { "+": format!("0x{:x}", i) },
                        "code": { "+": "0x" },
                        "storage": {}
                    }
                },
                "output": "0x"
            }]
        });
        instance
            .http_server_mock
            .add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }

    sleep(Duration::from_millis(300)).await;

    // Wait for block 3 to be processed
    let client =
        redis::Client::open(instance.redis_url().unwrap()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = get_latest_block_from_redis(&mut conn, &instance.namespace, 3);
        if latest == Some(3) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(3), "Should process 3 blocks");

    let reader = StateReader::new(
        instance.redis_url().unwrap(),
        &instance.namespace,
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Verify the regular account from trace exists (system calls merge with trace state)
    let test_address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let test_account = reader
        .get_account(test_address_hash.into(), 3)
        .expect("Failed to get test account")
        .expect("Test account should exist");
    assert_eq!(test_account.balance, U256::from(0x300));

    // EIP-2935 Verification
    let eip2935_address_hash = keccak256(HISTORY_STORAGE_ADDRESS);
    let eip2935_account = reader
        .get_full_account(eip2935_address_hash.into(), 3)
        .expect("Failed to get EIP-2935 account")
        .expect("EIP-2935 account should exist");

    // Verify code hash
    let expected_eip2935_code_hash = keccak256(&HISTORY_STORAGE_CODE);
    assert_eq!(
        eip2935_account.code_hash, expected_eip2935_code_hash,
        "EIP-2935 should have correct code hash"
    );
    assert_eq!(eip2935_account.nonce, 1, "EIP-2935 nonce should be 1");
    assert_eq!(
        eip2935_account.balance,
        U256::ZERO,
        "EIP-2935 balance should be 0"
    );

    // Verify storage contains parent block hashes (slot = block_number % 8191)
    assert!(
        !eip2935_account.storage.is_empty(),
        "EIP-2935 should have storage slots for parent hashes"
    );

    // EIP-4788 Verification
    let eip4788_address_hash = keccak256(BEACON_ROOTS_ADDRESS);
    let eip4788_account = reader
        .get_full_account(eip4788_address_hash.into(), 3)
        .expect("Failed to get EIP-4788 account")
        .expect("EIP-4788 account should exist");

    // Verify code hash
    let expected_eip4788_code_hash = keccak256(&BEACON_ROOTS_CODE);
    assert_eq!(
        eip4788_account.code_hash, expected_eip4788_code_hash,
        "EIP-4788 should have correct code hash"
    );
    assert_eq!(eip4788_account.nonce, 1, "EIP-4788 nonce should be 1");
    assert_eq!(
        eip4788_account.balance,
        U256::ZERO,
        "EIP-4788 balance should be 0"
    );

    // Verify the dual ring buffer pattern: timestamp at slot N, root at slot N + 8191
    assert!(
        eip4788_account.storage.len() >= 2,
        "EIP-4788 should have at least 2 storage slots (timestamp + root)"
    );
}

#[database_test(redis)]
async fn test_genesis_block_skips_system_calls(instance: TestInstance) {
    use alloy::eips::{
        eip2935::HISTORY_STORAGE_ADDRESS,
        eip4788::BEACON_ROOTS_ADDRESS,
    };
    use state_store::redis::StateReader;

    // Create genesis with a test account
    let genesis_json = r#"{
        "alloc": {
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {
                "balance": "0x100",
                "nonce": "0x0"
            }
        }
    }"#;

    let genesis_state =
        genesis::parse_from_str(genesis_json).expect("failed to parse test genesis json");

    // Create a new instance with genesis
    let instance = TestInstance::new_redis_with_genesis(genesis_state)
        .await
        .expect("Failed to create Redis instance with genesis");

    sleep(Duration::from_millis(500)).await;

    let reader = StateReader::new(
        instance.redis_url().unwrap(),
        &instance.namespace,
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Verify genesis account exists
    let genesis_address_hash =
        keccak256(hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap());
    let genesis_account = reader
        .get_account(genesis_address_hash.into(), 0)
        .expect("Failed to get genesis account");
    assert!(
        genesis_account.is_some(),
        "Genesis account should exist at block 0"
    );

    // Verify system contracts do NOT exist at block 0 (system calls skip genesis)
    let eip2935_address_hash = keccak256(HISTORY_STORAGE_ADDRESS);
    let eip2935_account = reader
        .get_account(eip2935_address_hash.into(), 0)
        .expect("Failed to get EIP-2935 account");
    assert!(
        eip2935_account.is_none(),
        "EIP-2935 should NOT exist at genesis block (block 0)"
    );

    let eip4788_address_hash = keccak256(BEACON_ROOTS_ADDRESS);
    let eip4788_account = reader
        .get_account(eip4788_address_hash.into(), 0)
        .expect("Failed to get EIP-4788 account");
    assert!(
        eip4788_account.is_none(),
        "EIP-4788 should NOT exist at genesis block (block 0)"
    );
}

#[database_test(redis)]
async fn test_recover_stale_locks_empty_when_no_locks(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_no_locks_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let writer = StateWriter::new(&redis.url, &base_namespace, config).unwrap();

    let recoveries = writer.recover_stale_locks().unwrap();
    assert!(recoveries.is_empty(), "expected no stale locks");
}

#[database_test(redis)]
async fn test_recover_stale_locks_clears_completed_write(_instance: TestInstance) {
    // Test case: crash happened AFTER metadata was updated but BEFORE lock was released
    // The state is actually consistent, so we just need to release the lock
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_completed_write_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");

    // Simulate completed write: block number is already set to target
    redis::cmd("SET")
        .arg(&block_key)
        .arg("100")
        .query::<()>(&mut conn)
        .unwrap();

    // Insert a stale lock for the same block (crash after metadata, before lock release)
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({
        "target_block": 100,
        "started_at": old_timestamp,
        "writer_id": "crashed-writer"
    })
    .to_string();

    redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Recover - should just release the lock since state is consistent
    let recoveries = writer.recover_stale_locks().unwrap();

    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 100);
    assert_eq!(recoveries[0].previous_block, Some(100)); // Same as target

    // Verify lock was cleared
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query(&mut conn)
        .unwrap();
    assert!(!exists, "lock should be cleared after recovery");
}

#[database_test(redis)]
async fn test_recover_stale_locks_repairs_state_with_diffs(_instance: TestInstance) {
    // Test case: crash happened during write, need to re-apply diffs
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_repair_with_diffs_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");

    // Insert a stale lock for block 0 (no previous block)
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({
        "target_block": 0,
        "started_at": old_timestamp,
        "writer_id": "crashed-writer"
    })
    .to_string();

    redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Store the diff that will be needed for recovery
    let diff_key = format!("{base_namespace}:diff:0");
    let diff = BlockStateUpdate::new(0, B256::from([1u8; 32]), B256::from([2u8; 32]));
    let diff_json = serde_json::to_string(&diff).unwrap();

    redis::cmd("SET")
        .arg(&diff_key)
        .arg(&diff_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Recover
    let recoveries = writer.recover_stale_locks().unwrap();

    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 0);
    assert_eq!(recoveries[0].previous_block, None);

    // Verify lock was cleared
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query(&mut conn)
        .unwrap();
    assert!(!exists, "lock should be cleared after recovery");

    // Verify block metadata was written
    let block_key = format!("{namespace}:block");
    let block_num: Option<String> = redis::cmd("GET").arg(&block_key).query(&mut conn).unwrap();
    assert_eq!(block_num, Some("0".to_string()));
}

#[database_test(redis)]
async fn test_recover_stale_locks_fails_without_diff(_instance: TestInstance) {
    // Test case: crash happened during write, but diff is missing - cannot repair
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_missing_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");

    // Insert a stale lock for block 5 (no diff stored)
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({
        "target_block": 5,
        "started_at": old_timestamp,
        "writer_id": "crashed-writer"
    })
    .to_string();

    redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .query::<()>(&mut conn)
        .unwrap();

    // DO NOT store the diff

    // Recover
    let result = writer.recover_stale_locks();

    assert!(result.is_err(), "recovery should fail without diff");
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            state_store::redis::common::error::StateError::MissingStateDiff { .. }
        ),
        "expected MissingStateDiff error, got: {err:?}",
    );

    // Verify lock is STILL in place (protecting readers from corrupt state)
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query(&mut conn)
        .unwrap();
    assert!(exists, "lock should remain when recovery fails");
}

#[database_test(redis)]
async fn test_force_recover_namespace_specific_index(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_force_recover_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    // Insert stale locks in namespaces 0 and 2
    let old_timestamp = chrono::Utc::now().timestamp() - 120;

    for idx in [0, 2] {
        let namespace = format!("{base_namespace}:{idx}");
        let lock_key = format!("{namespace}:write_lock");
        let block_key = format!("{namespace}:block");

        // Set block to simulate completed write (so recovery doesn't need diffs)
        let target_block = 100 + idx as u64;
        redis::cmd("SET")
            .arg(&block_key)
            .arg(target_block.to_string())
            .query::<()>(&mut conn)
            .unwrap();

        let lock_json = serde_json::json!({
            "target_block": target_block,
            "started_at": old_timestamp,
            "writer_id": format!("writer-{}", idx)
        })
        .to_string();

        redis::cmd("SET")
            .arg(&lock_key)
            .arg(&lock_json)
            .query::<()>(&mut conn)
            .unwrap();
    }

    // Force recover only namespace 2
    let recovery = writer.force_recover_namespace(2).unwrap();

    assert!(recovery.is_some());
    let recovery = recovery.unwrap();
    assert_eq!(recovery.namespace, format!("{base_namespace}:2"));
    assert_eq!(recovery.target_block, 102);

    // Verify namespace 0 still has its lock
    let lock_key_0 = format!("{base_namespace}:0:write_lock");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key_0)
        .query(&mut conn)
        .unwrap();
    assert!(exists, "namespace 0 lock should still exist");

    // Verify namespace 2 lock was cleared
    let lock_key_2 = format!("{base_namespace}:2:write_lock");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key_2)
        .query(&mut conn)
        .unwrap();
    assert!(!exists, "namespace 2 lock should be cleared");
}

#[database_test(redis)]
async fn test_recovery_applies_multiple_diffs(_instance: TestInstance) {
    // Test case: namespace at block 97, crashed during write of block 100
    // Recovery needs to apply diffs for blocks 98, 99, and 100
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_multi_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    // Namespace 1 (100 % 3 = 1)
    let namespace = format!("{base_namespace}:1");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");

    // Set namespace to block 97
    redis::cmd("SET")
        .arg(&block_key)
        .arg("97")
        .query::<()>(&mut conn)
        .unwrap();

    // Insert stale lock for block 100
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({
        "target_block": 100,
        "started_at": old_timestamp,
        "writer_id": "crashed-writer"
    })
    .to_string();

    redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Store diffs for blocks 98, 99, 100
    for block_num in 98..=100 {
        let diff_key = format!("{base_namespace}:diff:{block_num}");
        let diff = BlockStateUpdate::new(
            block_num,
            B256::from([block_num as u8; 32]),
            B256::from([(block_num + 100) as u8; 32]),
        );
        let diff_json = serde_json::to_string(&diff).unwrap();

        redis::cmd("SET")
            .arg(&diff_key)
            .arg(&diff_json)
            .query::<()>(&mut conn)
            .unwrap();
    }

    // Recover
    let recoveries = writer.recover_stale_locks().unwrap();

    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 100);
    assert_eq!(recoveries[0].previous_block, Some(97));

    // Verify namespace is now at block 100
    let block_num: Option<String> = redis::cmd("GET").arg(&block_key).query(&mut conn).unwrap();
    assert_eq!(block_num, Some("100".to_string()));

    // Verify lock was cleared
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query(&mut conn)
        .unwrap();
    assert!(!exists, "lock should be cleared after recovery");
}

#[database_test(redis)]
async fn test_recovery_partial_diff_chain_fails(_instance: TestInstance) {
    // Test case: namespace at block 97, crashed during write of block 100
    // But only diff for block 98 exists, 99 is missing - should fail
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_partial_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();

    let chunked_config = ChunkedWriteConfig::new(1000, 60);

    let writer = StateWriter::with_chunked_config(
        &redis.url,
        &base_namespace,
        config.clone(),
        chunked_config,
    )
    .unwrap();

    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();

    // Namespace 1 (100 % 3 = 1)
    let namespace = format!("{base_namespace}:1");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");

    // Set namespace to block 97
    redis::cmd("SET")
        .arg(&block_key)
        .arg("97")
        .query::<()>(&mut conn)
        .unwrap();

    // Insert stale lock for block 100
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({
        "target_block": 100,
        "started_at": old_timestamp,
        "writer_id": "crashed-writer"
    })
    .to_string();

    redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Only store diff for block 98 (missing 99 and 100)
    let diff_key = format!("{base_namespace}:diff:98");
    let diff = BlockStateUpdate::new(98, B256::from([98u8; 32]), B256::from([198u8; 32]));
    let diff_json = serde_json::to_string(&diff).unwrap();

    redis::cmd("SET")
        .arg(&diff_key)
        .arg(&diff_json)
        .query::<()>(&mut conn)
        .unwrap();

    // Recover - should fail because diff 99 is missing
    let result = writer.recover_stale_locks();

    assert!(result.is_err(), "recovery should fail with partial diffs");
    let err = result.unwrap_err();
    match err {
        state_store::redis::common::error::StateError::MissingStateDiff {
            needed_block,
            target_block,
        } => {
            assert_eq!(needed_block, 99);
            assert_eq!(target_block, 100);
        }
        _ => panic!("expected MissingStateDiff error, got: {err:?}"),
    }

    // Verify lock is STILL in place
    let exists: bool = redis::cmd("EXISTS")
        .arg(&lock_key)
        .query(&mut conn)
        .unwrap();
    assert!(exists, "lock should remain when recovery fails");
}
