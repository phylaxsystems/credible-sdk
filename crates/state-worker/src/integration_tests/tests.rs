#![allow(clippy::too_many_lines)]
use crate::integration_tests::setup::LocalInstance;
use redis::Commands;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_processes_multiple_state_changes() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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
    sleep(Duration::from_millis(50)).await;

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_2);

    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(50)).await;

    instance
        .http_server_mock
        .add_response("trace_replayBlockTransactions", parity_trace_block_3);

    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(50)).await;

    // Connect to Redis to verify state changes were applied
    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    // Add retry logic for Redis key retrieval
    let mut current_block = String::new();
    for _ in 0..20 {
        if let Ok(block) = conn.get::<_, String>("state_worker_test:current_block") {
            current_block = block;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert!(
        !current_block.is_empty(),
        "Failed to get current_block from Redis after timeout"
    );
    assert_eq!(
        current_block, "3",
        "Expected current block to be 3, got {current_block}"
    );

    // Verify block hashes are stored for all 3 blocks
    let expected_block_hashes = vec![
        (
            "state_worker_test:block_hash:1",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        ),
        (
            "state_worker_test:block_hash:2",
            "0x0000000000000000000000000000000000000000000000000000000000000002",
        ),
        (
            "state_worker_test:block_hash:3",
            "0x0000000000000000000000000000000000000000000000000000000000000003",
        ),
    ];

    for (key, expected_hash) in expected_block_hashes {
        let actual_hash: String = conn
            .get(key)
            .unwrap_or_else(|_| panic!("Failed to get {key} from Redis"));
        assert_eq!(
            actual_hash, expected_hash,
            "Expected block hash {expected_hash} for key {key}, got {actual_hash}",
        );
    }

    // Verify the data format is consistent
    let current_block_num: u64 = current_block
        .parse()
        .expect("Current block should be parseable as integer");
    assert_eq!(current_block_num, 3, "Current block should be 3");

    // Block hashes should be hex strings with 0x prefix and 64 hex characters
    for (key, expected_hash) in [
        (
            "state_worker_test:block_hash:1",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        ),
        (
            "state_worker_test:block_hash:2",
            "0x0000000000000000000000000000000000000000000000000000000000000002",
        ),
        (
            "state_worker_test:block_hash:3",
            "0x0000000000000000000000000000000000000000000000000000000000000003",
        ),
    ] {
        let hash: String = conn
            .get(key)
            .unwrap_or_else(|_| panic!("Failed to get {key}"));
        assert_eq!(hash, expected_hash);

        let hex_part = &hash[2..];
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "Block hash should contain only hex digits: {hash}",
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_handles_rapid_state_changes() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

    // Send multiple rapid state changes with empty blocks
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

    sleep(Duration::from_millis(50)).await;

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut current_block = String::new();
    for _ in 0..20 {
        if let Ok(block) = conn.get::<_, String>("state_worker_test:current_block") {
            current_block = block;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let current_block_num: u64 = current_block
        .parse()
        .expect("Current block should be parseable as integer");

    assert_eq!(
        current_block_num, 5,
        "Expected 5 blocks to be processed, got {current_block_num}",
    );

    // Verify block hashes exist for all processed blocks
    for block_num in 1..=current_block_num {
        let key = format!("state_worker_test:block_hash:{block_num}");
        let hash: String = conn
            .get(&key)
            .unwrap_or_else(|_| panic!("Failed to get block hash for block {block_num}"));
        assert!(hash.starts_with("0x"), "Block hash should start with 0x");
        assert_eq!(hash.len(), 66, "Block hash should be 66 characters long");
    }
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_non_consecutive_blocks_critical_alert() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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

    println!("Sending first new head (block 1)...");
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

    assert!(logs_contain("critical"));
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_missing_state_critical_alert() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

    // Empty stateDiff for block 1
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

    // Empty stateDiff for block 3
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
