#![allow(clippy::too_many_lines)]
use crate::{
    genesis,
    integration_tests::setup::LocalInstance,
};
use alloy::primitives::{
    B256,
    U256,
    keccak256,
};
use redis::Commands;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;

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

/// Helper to get the namespace for a given block number.
fn get_namespace_for_block(base_namespace: &str, block_number: u64, buffer_size: usize) -> String {
    let namespace_idx = usize::try_from(block_number).unwrap() % buffer_size;
    format!("{base_namespace}:{namespace_idx}")
}

#[traced_test]
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
    assert_eq!(
        genesis_state.accounts().len(),
        1,
        "expected single alloc entry in test genesis"
    );

    let instance = LocalInstance::new_with_setup_and_genesis(|_| {}, Some(genesis_state))
        .await
        .expect("Failed to start instance");

    sleep(Duration::from_millis(500)).await;

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = "state_worker_test:0";
    let account_key = format!("{namespace}:account:{}", &genesis_account[2..]);
    let storage_key = format!("{namespace}:storage:{}", &genesis_account[2..]);
    let code_hash = format!(
        "0x{}",
        hex::encode(keccak256(
            hex::decode(genesis_code.trim_start_matches("0x")).unwrap()
        ))
    );
    let code_key = format!("{namespace}:code:{}", code_hash.trim_start_matches("0x"));

    let mut balance: Option<String> = None;
    let mut nonce: Option<String> = None;
    let mut stored_code: Option<String> = None;
    let mut stored_code_hash: Option<String> = None;
    let mut storage_slot_zero: Option<String> = None;
    let mut storage_slot_one: Option<String> = None;

    for _ in 0..30 {
        balance = redis::cmd("HGET")
            .arg(&account_key)
            .arg("balance")
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        nonce = redis::cmd("HGET")
            .arg(&account_key)
            .arg("nonce")
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        stored_code_hash = redis::cmd("HGET")
            .arg(&account_key)
            .arg("code_hash")
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        stored_code = redis::cmd("GET")
            .arg(&code_key)
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        storage_slot_zero = redis::cmd("HGET")
            .arg(&storage_key)
            .arg("0x0000000000000000000000000000000000000000000000000000000000000000")
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        storage_slot_one = redis::cmd("HGET")
            .arg(&storage_key)
            .arg("0x0000000000000000000000000000000000000000000000000000000000000001")
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        if balance.is_some()
            && nonce.is_some()
            && stored_code.is_some()
            && stored_code_hash.is_some()
            && storage_slot_zero.is_some()
            && storage_slot_one.is_some()
        {
            break;
        }

        sleep(Duration::from_millis(50)).await;
    }

    if balance.is_none()
        || nonce.is_none()
        || stored_code.is_none()
        || stored_code_hash.is_none()
        || storage_slot_zero.is_none()
        || storage_slot_one.is_none()
    {
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("state_worker_test:*")
            .query(&mut conn)
            .unwrap_or_default();
        panic!("missing genesis data in redis. keys present: {keys:?}");
    }

    let balance = balance.unwrap();
    let nonce = nonce.unwrap();
    let stored_code = stored_code.unwrap();
    let storage_slot_zero = storage_slot_zero.unwrap();
    let storage_slot_one = storage_slot_one.unwrap();
    let stored_code_hash = stored_code_hash.unwrap();

    assert_eq!(balance, "5");
    assert_eq!(nonce, "1");
    assert_eq!(stored_code_hash.to_lowercase(), code_hash.to_lowercase());
    assert_eq!(stored_code.to_lowercase(), genesis_code.to_lowercase());
    assert_eq!(
        storage_slot_zero,
        format!("0x{}", hex::encode(B256::from(U256::from(1))))
    );
    assert_eq!(
        storage_slot_one,
        format!("0x{}", hex::encode(B256::from(U256::from(2))))
    );

    let block_key = format!("{namespace}:block");
    let current_block: String = conn
        .get(&block_key)
        .expect("Failed to read current block from namespace 0");
    assert_eq!(current_block, "0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_state_worker_processes_multiple_state_changes() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut current_block: Option<u64> = None;
    for _ in 0..30 {
        current_block = get_latest_block_from_redis(&mut conn, "state_worker_test", 3);
        if current_block == Some(3) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(
        current_block,
        Some(3),
        "Expected current block to be 3, got {current_block:?}",
    );

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

    for (key, expected_hash) in &expected_block_hashes {
        let mut actual_hash: Option<String> = None;
        for _ in 0..20 {
            if let Ok(hash) = conn.get::<_, String>(key) {
                actual_hash = Some(hash);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let actual_hash = actual_hash.unwrap_or_else(|| panic!("Failed to get {key} from Redis"));
        assert_eq!(
            &actual_hash, expected_hash,
            "Expected block hash {expected_hash} for key {key}, got {actual_hash}",
        );
    }

    let namespace_1 = "state_worker_test:1";
    let block_1_key = format!("{namespace_1}:block");
    let mut block_1: Option<String> = None;
    for _ in 0..30 {
        if let Ok(b) = conn.get::<_, String>(&block_1_key) {
            block_1 = Some(b);
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let block_1 = block_1.expect("Failed to get block 1 from namespace 1");
    assert_eq!(block_1, "1");

    let namespace_2 = "state_worker_test:2";
    let block_2_key = format!("{namespace_2}:block");
    let mut block_2: Option<String> = None;
    for _ in 0..20 {
        if let Ok(b) = conn.get::<_, String>(&block_2_key) {
            block_2 = Some(b);
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let block_2 = block_2.expect("Failed to get block 2 from namespace 2");
    assert_eq!(block_2, "2");

    let namespace_0 = "state_worker_test:0";
    let block_3_key = format!("{namespace_0}:block");
    let mut block_3: Option<String> = None;
    for _ in 0..20 {
        if let Ok(b) = conn.get::<_, String>(&block_3_key) {
            block_3 = Some(b);
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    let block_3 = block_3.expect("Failed to get block 3 from namespace 0");
    assert_eq!(block_3, "3");

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

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut current_block: Option<u64> = None;
    for _ in 0..30 {
        current_block = get_latest_block_from_redis(&mut conn, "state_worker_test", 3);
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

    for block_num in 1..=current_block_num {
        let key = format!("state_worker_test:block_hash:{block_num}");
        let mut hash: Option<String> = None;
        for _ in 0..20 {
            if let Ok(h) = conn.get::<_, String>(&key) {
                hash = Some(h);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        let hash = hash.unwrap_or_else(|| panic!("Failed to get block hash for block {block_num}"));
        assert!(hash.starts_with("0x"), "Block hash should start with 0x");
        assert_eq!(hash.len(), 66, "Block hash should be 66 characters long");
    }

    for block_num in 3..=5 {
        let namespace = get_namespace_for_block("state_worker_test", block_num, 3);
        let block_key = format!("{namespace}:block");

        let mut stored_block: Option<String> = None;
        for _ in 0..20 {
            if let Ok(b) = conn.get::<_, String>(&block_key) {
                stored_block = Some(b);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let stored_block = stored_block
            .unwrap_or_else(|| panic!("Failed to get block {block_num} from {namespace}"));
        assert_eq!(
            stored_block,
            block_num.to_string(),
            "Namespace should contain block {block_num}"
        );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_circular_buffer_rotation_with_state_diffs() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = get_latest_block_from_redis(&mut conn, "state_worker_test", 3);
        if latest == Some(6) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(6), "Latest block should be 6 after rotation");

    for block_num in 4..=6 {
        let namespace = get_namespace_for_block("state_worker_test", block_num, 3);
        let block_key = format!("{namespace}:block");

        let mut stored_block: Option<String> = None;
        for _ in 0..20 {
            if let Ok(b) = conn.get::<_, String>(&block_key) {
                stored_block = Some(b);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }

        let stored_block = stored_block
            .unwrap_or_else(|| panic!("Failed to get block {block_num} from {namespace}"));
        assert_eq!(
            stored_block,
            block_num.to_string(),
            "Namespace should contain block {block_num}"
        );
    }

    for block_num in 1..=6 {
        let key = format!("state_worker_test:block_hash:{block_num}");
        let mut hash: Option<String> = None;
        for _ in 0..20 {
            if let Ok(h) = conn.get::<_, String>(&key) {
                hash = Some(h);
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        let hash = hash.unwrap_or_else(|| panic!("Block hash for block {block_num} should exist"));
        assert!(hash.starts_with("0x"));
    }

    for block_num in 4..=6 {
        let diff_key = format!("state_worker_test:diff:{block_num}");
        let mut exists = false;
        for _ in 0..20 {
            if let Ok(e) = redis::cmd("EXISTS").arg(&diff_key).query::<bool>(&mut conn) {
                exists = e;
                if exists {
                    break;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(exists, "State diff for block {block_num} should exist");
    }

    sleep(Duration::from_millis(100)).await;

    for block_num in 1..=3 {
        let diff_key = format!("state_worker_test:diff:{block_num}");
        let exists: bool = redis::cmd("EXISTS")
            .arg(&diff_key)
            .query(&mut conn)
            .unwrap_or(true);
        assert!(
            !exists,
            "Old state diff for block {block_num} should be deleted"
        );
    }
}
