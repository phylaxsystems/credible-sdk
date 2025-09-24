#![allow(clippy::too_many_lines)]
use crate::integration_tests::setup::LocalInstance;
use alloy::primitives::Address;
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

    // Test addresses for state changes
    let addr1 = Address::from([1u8; 20]);
    let addr2 = Address::from([2u8; 20]);
    let addr3 = Address::from([3u8; 20]);

    // Setup mock responses for debug trace methods that the state worker actually uses
    let trace_response_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "type": "CALL",
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": addr1.to_string(),
                    "value": "0x1bc16d674ec80000", // 2 ETH
                    "gas": "0x5208",
                    "gasUsed": "0x5208",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    let trace_response_block_2 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
                "result": {
                    "type": "CALL",
                    "from": addr1.to_string(),
                    "to": addr2.to_string(),
                    "value": "0x3635c9adc5dea00000", // 4 ETH
                    "gas": "0x7530",
                    "gasUsed": "0x7530",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    let trace_response_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "result": {
                    "type": "CALL",
                    "from": addr2.to_string(),
                    "to": addr3.to_string(),
                    "value": "0x6c6b935b8bbd400000", // 8 ETH
                    "gas": "0x9c40",
                    "gasUsed": "0x9c40",
                    "input": "0x608060405234801561001057600080fd5b", // contract creation
                    "output": "0x608060405234801561001057600080fd5b50600436106100365760003560e01c8063",
                    "calls": []
                }
            }
        ]
    });

    // Set up the trace responses for each block
    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", trace_response_block_1.clone());
    instance
        .http_server_mock
        .add_response("debug_traceBlockByHash", trace_response_block_1.clone());

    // Send the first state change (new head)
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(50)).await;

    // Update trace response for block 2
    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", trace_response_block_2.clone());
    instance
        .http_server_mock
        .add_response("debug_traceBlockByHash", trace_response_block_2.clone());

    // Send second state change
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(50)).await;

    // Update trace response for block 3
    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", trace_response_block_3.clone());
    instance
        .http_server_mock
        .add_response("debug_traceBlockByHash", trace_response_block_3.clone());

    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(50)).await;

    // Connect to Redis to verify state changes were applied
    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    // Test 1: Verify the current block number is correctly stored
    let current_block: String = conn
        .get("state_worker_test:current_block")
        .expect("Failed to get current_block from Redis");
    assert_eq!(
        current_block, "3",
        "Expected current block to be 3, got {current_block}"
    );

    // Test 2: Verify block hashes are stored for all 3 blocks
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

    // Test 3: Verify no unexpected keys exist (should only have the 4 expected keys)
    // Since mini-redis doesn't support KEYS command, we'll just verify our expected keys exist
    // and that they contain the correct data format

    // Test 4: Verify the data format is consistent
    // Current block should be a simple integer string
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

        // Verify it's valid hex
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

    // Test addresses for different accounts
    let addr1 = Address::from([1u8; 20]);
    let addr2 = Address::from([2u8; 20]);

    // Setup varying mock responses with trace data
    let balances = [
        "0x1bc16d674ec80000",      // 2 ETH
        "0x3635c9adc5dea00000",    // 4 ETH
        "0x6c6b935b8bbd400000",    // 8 ETH
        "0xd3c21bcecceda1000000",  // 16 ETH
        "0x1a784379d99db42000000", // 32 ETH
    ];

    // Send multiple rapid state changes
    for (i, balance) in balances.iter().enumerate() {
        // Create trace response for this block
        let trace_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": [
                {
                    "txHash": format!("0x{:064x}", i + 1),
                    "result": {
                        "type": "CALL",
                        "from": addr1.to_string(),
                        "to": addr2.to_string(),
                        "value": balance,
                        "gas": "0x7530",
                        "gasUsed": "0x7530",
                        "input": "0x",
                        "output": "0x",
                        "calls": []
                    }
                }
            ]
        });

        instance
            .http_server_mock
            .add_response("debug_traceBlockByNumber", trace_response.clone());
        instance
            .http_server_mock
            .add_response("debug_traceBlockByHash", trace_response);

        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(50)).await;
    }

    sleep(Duration::from_millis(50)).await;

    // Connect to Redis and verify changes were captured
    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    // Check current block number
    let current_block: String = conn
        .get("state_worker_test:current_block")
        .expect("Failed to get current_block from Redis");
    let current_block_num: u64 = current_block
        .parse()
        .expect("Current block should be parseable as integer");

    // Should have processed all 5 rapid changes
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

    // Setup valid trace response for block 1
    let addr1 = Address::from([1u8; 20]);
    let valid_trace_response_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "type": "CALL",
                    "from": "0x0000000000000000000000000000000000000000",
                    "to": addr1.to_string(),
                    "value": "0x1bc16d674ec80000", // 2 ETH
                    "gas": "0x5208",
                    "gasUsed": "0x5208",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", valid_trace_response_1);

    // Send first new head (block 1)
    println!("Sending first new head (block 1)...");
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    // Now mock an error for block 2 to simulate it not existing
    instance
        .http_server_mock
        .mock_rpc_error("debug_traceBlockByNumber", -32000, "block not found");

    instance.http_server_mock.send_new_head_with_block_number(2);
    sleep(Duration::from_millis(50)).await;

    // Setup valid trace response for block 3
    let addr2 = Address::from([2u8; 20]);
    let valid_trace_response_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "result": {
                    "type": "CALL",
                    "from": addr1.to_string(),
                    "to": addr2.to_string(),
                    "value": "0x3635c9adc5dea00000", // 4 ETH
                    "gas": "0x7530",
                    "gasUsed": "0x7530",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    // Override the error response with a valid response for block 3
    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", valid_trace_response_3);

    // Send notification for block 3 - this should create the non-consecutive scenario
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;

    // The critical alert should have been triggered when the worker detects non-consecutive processing
    assert!(logs_contain("critical"));
}

#[traced_test]
#[tokio::test]
async fn test_state_worker_missing_state_critical_alert() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

    let trace_response_block_1 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "result": {
                    "type": "CALL",
                    "from": "0x1000000000000000000000000000000000000000",
                    "to": "0x2000000000000000000000000000000000000000",
                    "value": "0x1bc16d674ec80000",
                    "gas": "0x5208",
                    "gasUsed": "0x5208",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", trace_response_block_1);

    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;

    let trace_response_block_3 = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {
                "txHash": "0x3333333333333333333333333333333333333333333333333333333333333333",
                "result": {
                    "type": "CALL",
                    "from": "0x3000000000000000000000000000000000000000",
                    "to": "0x4000000000000000000000000000000000000000",
                    "value": "0x3635c9adc5dea00000",
                    "gas": "0x7530",
                    "gasUsed": "0x7530",
                    "input": "0x",
                    "output": "0x",
                    "calls": []
                }
            }
        ]
    });

    instance
        .http_server_mock
        .add_response("debug_traceBlockByNumber", trace_response_block_3);

    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;
    assert!(logs_contain("critical"));
}
