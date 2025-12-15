#![allow(clippy::too_many_lines)]
#![allow(clippy::manual_range_contains)]
use crate::{
    cli::ProviderType,
    connect_provider,
    genesis,
    integration_tests::setup::LocalInstance,
    state,
    worker::StateWorker,
};
use alloy::primitives::{
    B256,
    U256,
    keccak256,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use redis::Commands;
use serde_json::json;
use state_store::{
    CircularBufferConfig,
    StateReader,
    StateWriter,
    common::{
        AccountInfo,
        AccountState,
        get_account_key,
        get_storage_key,
    },
};
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use tokio::{
    sync::broadcast,
    time::sleep,
};
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

    let address_hash = keccak256(hex::decode(genesis_account.trim_start_matches("0x")).unwrap());
    let namespace = "state_worker_test:0";
    let account_key = get_account_key(namespace, &address_hash.into());
    let storage_key = get_storage_key(namespace, &address_hash.into());
    let code_hash = format!(
        "0x{}",
        hex::encode(keccak256(
            hex::decode(genesis_code.trim_start_matches("0x")).unwrap()
        ))
    );
    let code_key = format!("{namespace}:code:{}", code_hash.trim_start_matches("0x"));
    let slot_zero_key = format!(
        "0x{}",
        hex::encode(keccak256(U256::ZERO.to_be_bytes::<32>()))
    );
    let slot_one_key = format!(
        "0x{}",
        hex::encode(keccak256(U256::from(1).to_be_bytes::<32>()))
    );

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
            .arg(&slot_zero_key)
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();

        storage_slot_one = redis::cmd("HGET")
            .arg(&storage_key)
            .arg(&slot_one_key)
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

/// Test basic restart: process blocks, stop, restart, verify continuation
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_restart_continues_from_last_block() {
    // Start Redis container
    let redis_container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = redis_container
        .get_host()
        .await
        .expect("Failed to get Redis host");
    let port = redis_container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get Redis port");
    let redis_url = format!("redis://{host}:{port}");

    // Setup mock server
    let http_server_mock = DualProtocolMockServer::new()
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

        let writer = StateWriter::new(
            &redis_url,
            "restart_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
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

        let mut worker = StateWorker::new(provider, trace_provider, writer.clone(), reader, None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Start worker in background
        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });

        // Send 3 blocks
        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }

        // Wait for blocks to be processed
        sleep(Duration::from_millis(300)).await;

        // Verify blocks 1-3 are processed
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "restart_test", 3);
        assert_eq!(latest, Some(3), "Should have processed 3 blocks");

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

        let writer = StateWriter::new(
            &redis_url,
            "restart_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
            &redis_url,
            "restart_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        // Verify it starts from the correct block
        let start_block = writer.latest_block_number().unwrap();
        assert_eq!(start_block, Some(3), "Should resume from block 3");

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer.clone(), reader, None);
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

        sleep(Duration::from_millis(300)).await;

        // Verify all blocks are processed
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "restart_test", 3);
        assert_eq!(
            latest,
            Some(6),
            "Should have processed blocks 4-6 after restart"
        );

        // Verify block hashes for all blocks exist
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_restart_with_buffer_wrap_applies_diffs() {
    let redis_container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = redis_container
        .get_host()
        .await
        .expect("Failed to get Redis host");
    let port = redis_container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get Redis port");
    let redis_url = format!("redis://{host}:{port}");

    let http_server_mock = DualProtocolMockServer::new()
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

        let writer = StateWriter::new(
            &redis_url,
            "wrap_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
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

        let mut worker = StateWorker::new(provider, trace_provider, writer, reader, None);
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

        let writer = StateWriter::new(
            &redis_url,
            "wrap_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
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

        let mut worker = StateWorker::new(provider, trace_provider, writer, reader, None);
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_restart_after_mid_block_crash() {
    let redis_container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = redis_container
        .get_host()
        .await
        .expect("Failed to get Redis host");
    let port = redis_container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get Redis port");
    let redis_url = format!("redis://{host}:{port}");

    let http_server_mock = DualProtocolMockServer::new()
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

        let writer = StateWriter::new(
            &redis_url,
            "crash_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
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

        let mut worker = StateWorker::new(provider, trace_provider, writer, reader, None);
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

        let writer = StateWriter::new(
            &redis_url,
            "crash_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        let reader = StateReader::new(
            &redis_url,
            "crash_test",
            CircularBufferConfig::new(3).unwrap(),
        )
        .unwrap();

        // Verify it detects the correct resume point
        let start_block = writer.latest_block_number().unwrap();
        assert_eq!(start_block, Some(2), "Should detect last completed block");

        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer, reader, None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_zero_storage_values_are_deleted() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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

    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = get_namespace_for_block("state_worker_test", 1, 3);
    let storage_key = get_storage_key(&namespace, &address_hash.into());

    let slot_zero_key = format!(
        "0x{}",
        hex::encode(keccak256(U256::ZERO.to_be_bytes::<32>()))
    );
    let slot_one_key = format!(
        "0x{}",
        hex::encode(keccak256(U256::from(1).to_be_bytes::<32>()))
    );

    // Wait for block 1 to be processed and verify both slots exist
    let mut slot_zero: Option<String> = None;
    let mut slot_one: Option<String> = None;
    for _ in 0..30 {
        slot_zero = redis::cmd("HGET")
            .arg(&storage_key)
            .arg(&slot_zero_key)
            .query::<Option<String>>(&mut conn)
            .ok()
            .flatten();
        slot_one = redis::cmd("HGET")
            .arg(&storage_key)
            .arg(&slot_one_key)
            .query::<Option<String>>(&mut conn)
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
    let namespace_2 = get_namespace_for_block("state_worker_test", 2, 3);
    let block_2_key = format!("{namespace_2}:block");
    for _ in 0..30 {
        if let Ok(b) = conn.get::<_, String>(&block_2_key)
            && b == "2"
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Verify slot 0 still exists and slot 1 was deleted
    let storage_key_2 = get_storage_key(&namespace_2, &address_hash.into());

    let slot_zero_after: Option<String> = redis::cmd("HGET")
        .arg(&storage_key_2)
        .arg(&slot_zero_key)
        .query(&mut conn)
        .ok()
        .flatten();

    let slot_one_after: Option<String> = redis::cmd("HGET")
        .arg(&storage_key_2)
        .arg(&slot_one_key)
        .query(&mut conn)
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
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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
    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = get_namespace_for_block("state_worker_test", 1, 3);
    let block_key = format!("{namespace}:block");

    for _ in 0..30 {
        if let Ok(b) = conn.get::<_, String>(&block_key)
            && b == "1"
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Create reader
    let reader = StateReader::new(
        &instance.redis_url,
        "state_worker_test",
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Test get_account returns AccountInfo (without storage field at all)
    let account_info: AccountInfo = reader
        .get_account(address_hash.into(), 1)
        .expect("Failed to get account")
        .expect("Account should exist");

    assert_eq!(account_info.balance, U256::from(0x100));
    assert_eq!(account_info.nonce, 1);

    // Test get_full_account returns AccountState (with storage)
    let account_state: AccountState = reader
        .get_full_account(address_hash.into(), 1)
        .expect("Failed to get account with storage")
        .expect("Account should exist");

    assert_eq!(account_state.balance, U256::from(0x100));
    assert_eq!(account_state.nonce, 1);
    assert_eq!(
        account_state.storage.len(),
        3,
        "get_full_account should return all storage slots"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_full_account_returns_all_slots() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = get_namespace_for_block("state_worker_test", 1, 3);
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
        &instance.redis_url,
        "state_worker_test",
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Test get_full_account returns AccountState
    let account: AccountState = reader
        .get_full_account(address_hash.into(), 1)
        .expect("Failed to get account with storage")
        .expect("Account should exist");

    assert_eq!(account.balance, U256::from(0x200));
    assert_eq!(account.nonce, 5);
    assert_eq!(account.storage.len(), 2);

    // Verify storage values
    let slot_0_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_1_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let slot_0_key = U256::from_be_bytes(slot_0_hash.into());
    let slot_1_key = U256::from_be_bytes(slot_1_hash.into());

    assert!(
        account.storage.contains_key(&slot_0_key),
        "Storage should contain slot 0"
    );
    assert!(
        account.storage.contains_key(&slot_1_key),
        "Storage should contain slot 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_storage_returns_individual_slot() {
    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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
    let client =
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let namespace = get_namespace_for_block("state_worker_test", 1, 3);
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
        &instance.redis_url,
        "state_worker_test",
        CircularBufferConfig::new(3).unwrap(),
    )
    .expect("Failed to create reader");

    // Use get_storage for individual slot lookup
    let slot_5_hash = keccak256(U256::from(5).to_be_bytes::<32>());
    let slot_key = U256::from_be_bytes(slot_5_hash.into());

    let value = reader
        .get_storage(address_hash.into(), slot_key, 1)
        .expect("Failed to get storage")
        .expect("Storage slot should exist");

    assert_eq!(value, U256::from(0xff));

    // Verify non-existent slot returns None
    let non_existent_slot = keccak256(U256::from(999).to_be_bytes::<32>());
    let non_existent_key = U256::from_be_bytes(non_existent_slot.into());

    let missing = reader
        .get_storage(address_hash.into(), non_existent_key, 1)
        .expect("Failed to get storage");

    assert!(missing.is_none(), "Non-existent slot should return None");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_eip2935_and_eip4788_system_contracts() {
    use crate::system_calls::HISTORY_BUFFER_LENGTH;
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

    let instance = LocalInstance::new()
        .await
        .expect("Failed to start instance");

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
        redis::Client::open(instance.redis_url.as_str()).expect("Failed to create Redis client");
    let mut conn = client
        .get_connection()
        .expect("Failed to get Redis connection");

    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = get_latest_block_from_redis(&mut conn, "state_worker_test", 3);
        if latest == Some(3) {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(3), "Should process 3 blocks");

    let reader = StateReader::new(
        &instance.redis_url,
        "state_worker_test",
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

    let mut has_low_slot = false;
    let mut has_high_slot = false;
    for slot in eip4788_account.storage.keys() {
        let slot_u64 = slot.to::<u64>();
        if slot_u64 < HISTORY_BUFFER_LENGTH {
            has_low_slot = true;
        } else if slot_u64 >= HISTORY_BUFFER_LENGTH && slot_u64 < 2 * HISTORY_BUFFER_LENGTH {
            has_high_slot = true;
        }
    }
    assert!(
        has_low_slot,
        "EIP-4788 should have timestamp slot (< HISTORY_BUFFER_LENGTH)"
    );
    assert!(
        has_high_slot,
        "EIP-4788 should have root slot (>= HISTORY_BUFFER_LENGTH)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_genesis_block_skips_system_calls() {
    use alloy::eips::{
        eip2935::HISTORY_STORAGE_ADDRESS,
        eip4788::BEACON_ROOTS_ADDRESS,
    };

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

    let instance = LocalInstance::new_with_setup_and_genesis(|_| {}, Some(genesis_state))
        .await
        .expect("Failed to start instance");

    sleep(Duration::from_millis(500)).await;

    let reader = StateReader::new(
        &instance.redis_url,
        "state_worker_test",
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
