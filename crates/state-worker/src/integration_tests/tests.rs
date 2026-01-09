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

use super::TestInstance;

fn get_latest_block_from_redis(
    conn: &mut redis::Connection,
    base_namespace: &str,
    buffer_size: u8,
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

async fn wait_for_block(
    redis_url: &str,
    base_namespace: &str,
    buffer_size: u8,
    expected_block: u64,
    timeout_secs: u64,
) -> Result<(), String> {
    let client = redis::Client::open(redis_url).map_err(|e| format!("Failed to connect to Redis: {e}"))?;
    let mut conn = client.get_connection().map_err(|e| format!("Failed to get Redis connection: {e}"))?;
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
            return Err(format!("Timeout waiting for block {expected_block}. Latest block: {latest:?}"));
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

fn get_namespace_for_block(base_namespace: &str, block_number: u64, buffer_size: u8) -> String {
    let namespace_idx = usize::try_from(block_number).unwrap() % buffer_size as usize;
    format!("{base_namespace}:{namespace_idx}")
}

#[database_test(all)]
async fn test_state_worker_hydrates_genesis_state(instance: TestInstance) {
    let genesis_account = "0x00000000000000000000000000000000000000aa";
    let genesis_code = "0x6000";
    let genesis_json = format!(
        r#"{{"alloc": {{"{account}": {{"balance": "0x5", "nonce": "0x1", "code": "{code}", "storage": {{"0x0": "0x1", "0x1": "0x2"}}}}}}}}"#,
        account = genesis_account.trim_start_matches("0x"),
        code = genesis_code
    );
    let genesis_state = genesis::parse_from_str(&genesis_json).expect("failed to parse test genesis json");
    let instance = match instance.backend {
        TestBackend::Redis => TestInstance::new_redis_with_genesis(genesis_state).await.expect("Failed to create Redis instance with genesis"),
        TestBackend::Mdbx => TestInstance::new_mdbx_with_genesis(genesis_state).await.expect("Failed to create MDBX instance with genesis"),
    };
    sleep(Duration::from_millis(500)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    let address_hash = keccak256(hex::decode(genesis_account.trim_start_matches("0x")).unwrap());
    let mut account = None;
    for _ in 0..30 {
        account = reader.get_account_boxed(address_hash.into(), 0).ok().flatten();
        if account.is_some() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let account = account.expect("Genesis account should exist");
    assert_eq!(account.balance, U256::from(5), "Balance should be 5");
    assert_eq!(account.nonce, 1, "Nonce should be 1");
    let slot_zero_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());
    let storage_zero = reader.get_storage_boxed(address_hash.into(), slot_zero_hash, 0).expect("Failed to get storage slot 0").expect("Storage slot 0 should exist");
    assert_eq!(storage_zero, U256::from(1), "Storage slot 0 should be 1");
    let storage_one = reader.get_storage_boxed(address_hash.into(), slot_one_hash, 0).expect("Failed to get storage slot 1").expect("Storage slot 1 should exist");
    assert_eq!(storage_one, U256::from(2), "Storage slot 1 should be 2");
    let latest = reader.latest_block_number_boxed().expect("Failed to get latest block");
    assert_eq!(latest, Some(0), "Latest block should be 0 (genesis)");
}

#[database_test(all)]
async fn test_state_worker_processes_multiple_state_changes(instance: TestInstance) {
    sleep(Duration::from_millis(200)).await;
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(200)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    let mut latest_block: Option<u64> = None;
    for _ in 0..30 {
        latest_block = reader.latest_block_number_boxed().expect("Failed to get latest block");
        if latest_block == Some(3) { break; }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest_block, Some(3), "Expected current block to be 3, got {latest_block:?}");
}

#[database_test(all)]
async fn test_state_worker_handles_rapid_state_changes(instance: TestInstance) {
    for i in 0..5 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i + 1), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }
    sleep(Duration::from_millis(200)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    let mut current_block: Option<u64> = None;
    for _ in 0..30 {
        current_block = reader.latest_block_number_boxed().expect("Failed to get latest block");
        if current_block == Some(5) { break; }
        sleep(Duration::from_millis(100)).await;
    }
    let current_block_num = current_block.expect("Failed to get current block after timeout");
    assert_eq!(current_block_num, 5, "Expected 5 blocks to be processed, got {current_block_num}");
}

#[traced_database_test(all)]
async fn test_state_worker_non_consecutive_blocks_critical_alert(instance: TestInstance) {
    let valid_parity_trace_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", valid_parity_trace_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;
    instance.http_server_mock.mock_rpc_error("trace_replayBlockTransactions", -32000, "block not found");
    instance.http_server_mock.send_new_head_with_block_number(2);
    sleep(Duration::from_millis(50)).await;
    let valid_parity_trace_3 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333", "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", valid_parity_trace_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;
    assert!(logs_contain("Missing block"));
}

#[traced_database_test(all)]
async fn test_state_worker_missing_state_critical_alert(instance: TestInstance) {
    let parity_trace_block_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head_with_block_number(1);
    sleep(Duration::from_millis(50)).await;
    let parity_trace_block_3 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333", "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_3);
    instance.http_server_mock.send_new_head_with_block_number(3);
    sleep(Duration::from_millis(50)).await;
    assert!(logs_contain("critical"));
}

#[database_test(all)]
async fn test_circular_buffer_rotation_with_state_diffs(instance: TestInstance) {
    for i in 0..6 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i + 1), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(100)).await;
    }
    sleep(Duration::from_millis(300)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = reader.latest_block_number_boxed().expect("Failed to get latest block");
        if latest == Some(6) { break; }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(6), "Latest block should be 6 after rotation");
}

#[database_test(all)]
async fn test_zero_storage_values_are_deleted(instance: TestInstance) {
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let parity_trace_block_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0x100" }, "nonce": { "+": "0x1" }, "code": { "+": "0x" }, "storage": {"0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x0000000000000000000000000000000000000000000000000000000000000001" }, "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x0000000000000000000000000000000000000000000000000000000000000002" }}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    let slot_zero_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_one_hash = keccak256(U256::from(1).to_be_bytes::<32>());
    let mut slot_zero: Option<U256> = None;
    let mut slot_one: Option<U256> = None;
    for _ in 0..30 {
        slot_zero = reader.get_storage_boxed(address_hash.into(), slot_zero_hash, 1).ok().flatten();
        slot_one = reader.get_storage_boxed(address_hash.into(), slot_one_hash, 1).ok().flatten();
        if slot_zero.is_some() && slot_one.is_some() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(slot_zero.is_some(), "Slot 0 should exist after block 1");
    assert!(slot_one.is_some(), "Slot 1 should exist after block 1");
    let parity_trace_block_2 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": "=", "nonce": "=", "code": "=", "storage": {"0x0000000000000000000000000000000000000000000000000000000000000001": {"*": {"from": "0x0000000000000000000000000000000000000000000000000000000000000002", "to": "0x0000000000000000000000000000000000000000000000000000000000000000"}}}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(200)).await;
    for _ in 0..30 {
        if let Some(2) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let slot_zero_after = reader.get_storage_boxed(address_hash.into(), slot_zero_hash, 2).ok().flatten();
    let slot_one_after = reader.get_storage_boxed(address_hash.into(), slot_one_hash, 2).ok().flatten();
    assert!(slot_zero_after.is_some(), "Slot 0 should still exist after block 2");
    assert!(slot_one_after.is_none(), "Slot 1 should be deleted after being set to zero in block 2");
}

#[database_test(all)]
async fn test_get_account_does_not_load_storage(instance: TestInstance) {
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let parity_trace_block_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0x100" }, "nonce": { "+": "0x1" }, "code": { "+": "0x6080" }, "storage": {"0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x0000000000000000000000000000000000000000000000000000000000000001" }, "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x0000000000000000000000000000000000000000000000000000000000000002" }, "0x0000000000000000000000000000000000000000000000000000000000000002": { "+": "0x0000000000000000000000000000000000000000000000000000000000000003" }}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let account_info = reader.get_account_boxed(address_hash.into(), 1).expect("Failed to get account").expect("Account should exist");
    assert_eq!(account_info.balance, U256::from(0x100));
    assert_eq!(account_info.nonce, 1);
}

#[database_test(all)]
async fn test_get_storage_returns_individual_slot(instance: TestInstance) {
    let test_address = "0xcccccccccccccccccccccccccccccccccccccccc";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let parity_trace_block_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0x300" }, "nonce": { "+": "0x1" }, "code": { "+": "0x" }, "storage": {"0x0000000000000000000000000000000000000000000000000000000000000005": { "+": "0x00000000000000000000000000000000000000000000000000000000000000ff" }}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let slot_5_hash = keccak256(U256::from(5).to_be_bytes::<32>());
    let value = reader.get_storage_boxed(address_hash.into(), slot_5_hash, 1).expect("Failed to get storage").expect("Storage slot should exist");
    assert_eq!(value, U256::from(0xff));
    let non_existent_slot = keccak256(U256::from(999).to_be_bytes::<32>());
    let missing = reader.get_storage_boxed(address_hash.into(), non_existent_slot, 1).expect("Failed to get storage");
    assert!(missing.is_none(), "Non-existent slot should return None");
}

#[database_test(redis)]
async fn test_restart_continues_from_last_block(instance: TestInstance) {
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();
    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new().await.expect("Failed to create mock server");
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }
    {
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "restart_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });
        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }
        wait_for_block(&redis_url, "restart_test", 3, 3, 5).await.expect("Should have processed 3 blocks");
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
    for i in 4..=6 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }
    {
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "restart_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let start_block = writer_reader.latest_block_number().unwrap();
        assert_eq!(start_block, Some(3), "Should resume from block 3");
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(None, shutdown_rx).await });
        for _ in 0..3 {
            http_server_mock.send_new_head();
            sleep(Duration::from_millis(100)).await;
        }
        wait_for_block(&redis_url, "restart_test", 3, 6, 5).await.expect("Should have processed blocks 4-6 after restart");
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        for block_num in 1..=6 {
            let key = format!("restart_test:block_hash:{block_num}");
            let hash: Option<String> = conn.get(&key).ok();
            assert!(hash.is_some(), "Block hash for block {block_num} should exist");
        }
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
}

#[database_test(redis)]
async fn test_restart_with_buffer_wrap_applies_diffs(instance: TestInstance) {
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();
    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new().await.expect("Failed to create mock server");
    {
        for i in 0..=4 {
            let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "wrap_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });
        for _ in 0..4 { http_server_mock.send_new_head(); sleep(Duration::from_millis(100)).await; }
        sleep(Duration::from_millis(300)).await;
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "wrap_test", 3);
        assert_eq!(latest, Some(4), "Should have processed blocks 0-4");
        let namespace_1 = "wrap_test:1";
        let block_key = format!("{namespace_1}:block");
        let block: String = conn.get(&block_key).unwrap();
        assert_eq!(block, "4", "Namespace 1 should contain block 4 after wrap");
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
    {
        for i in 5..=7 {
            let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "wrap_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(None, shutdown_rx).await });
        for _ in 0..3 { http_server_mock.send_new_head(); sleep(Duration::from_millis(100)).await; }
        sleep(Duration::from_millis(300)).await;
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();
        let latest = get_latest_block_from_redis(&mut conn, "wrap_test", 3);
        assert_eq!(latest, Some(7), "Should continue to block 7 after restart");
        for block_num in 5..=7 {
            let diff_key = format!("wrap_test:diff:{block_num}");
            let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(&mut conn).unwrap();
            assert!(exists, "Diff for block {block_num} should exist");
        }
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), worker_handle).await;
    }
}

#[database_test(redis)]
async fn test_restart_after_mid_block_crash(instance: TestInstance) {
    let redis = get_shared_redis().await;
    let redis_url = redis.url.clone();
    let http_server_mock = int_test_utils::node_protocol_mock_server::DualProtocolMockServer::new().await.expect("Failed to create mock server");
    for i in 0..=2 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }
    {
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "crash_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(Some(0), shutdown_rx).await });
        for _ in 0..2 { http_server_mock.send_new_head(); sleep(Duration::from_millis(100)).await; }
        sleep(Duration::from_millis(200)).await;
        drop(shutdown_tx);
        drop(worker_handle);
    }
    {
        for i in 3..=5 {
            let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
            http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        }
        let provider = connect_provider(&http_server_mock.ws_url()).await.expect("Failed to connect to provider");
        let writer_reader = StateWriter::new(&redis_url, "crash_test", CircularBufferConfig::new(3).unwrap()).unwrap();
        let start_block = writer_reader.latest_block_number().unwrap();
        assert_eq!(start_block, Some(2), "Should detect last completed block");
        let trace_provider = state::create_trace_provider(ProviderType::Parity, provider.clone(), Duration::from_secs(30));
        let mut worker = StateWorker::new(provider, trace_provider, writer_reader.clone(), None);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let worker_handle = tokio::spawn(async move { worker.run(None, shutdown_rx).await });
        for _ in 0..3 { http_server_mock.send_new_head(); sleep(Duration::from_millis(100)).await; }
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
    let parity_trace_block_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0x200" }, "nonce": { "+": "0x5" }, "code": { "+": "0x" }, "storage": {"0x0000000000000000000000000000000000000000000000000000000000000000": { "+": "0x000000000000000000000000000000000000000000000000000000000000000a" }, "0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x000000000000000000000000000000000000000000000000000000000000000b" }}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_block_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;
    let client = redis::Client::open(instance.redis_url().unwrap()).expect("Failed to create Redis client");
    let mut conn = client.get_connection().expect("Failed to get Redis connection");
    let namespace = get_namespace_for_block(&instance.namespace, 1, 3);
    let block_key = format!("{namespace}:block");
    for _ in 0..30 {
        if let Ok(b) = conn.get::<_, String>(&block_key) && b == "1" { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let reader = StateReader::new(instance.redis_url().unwrap(), &instance.namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create reader");
    let account = reader.get_full_account(address_hash.into(), 1).expect("Failed to get account with storage").expect("Account should exist");
    assert_eq!(account.balance, U256::from(0x200));
    assert_eq!(account.nonce, 5);
    assert_eq!(account.storage.len(), 2);
    let slot_0_hash = keccak256(U256::ZERO.to_be_bytes::<32>());
    let slot_1_hash = keccak256(U256::from(1).to_be_bytes::<32>());
    assert!(account.storage.contains_key(&slot_0_hash), "Storage should contain slot 0");
    assert!(account.storage.contains_key(&slot_1_hash), "Storage should contain slot 1");
}

#[database_test(redis)]
async fn test_eip2935_and_eip4788_system_contracts(instance: TestInstance) {
    use alloy::eips::{eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE}, eip4788::{BEACON_ROOTS_ADDRESS, BEACON_ROOTS_CODE}};
    use state_store::redis::StateReader;
    let test_address = "0xdddddddddddddddddddddddddddddddddddddddd";
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": format!("0x{:x}", i * 0x100) }, "nonce": { "+": format!("0x{:x}", i) }, "code": { "+": "0x" }, "storage": {}}}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    sleep(Duration::from_millis(300)).await;
    let client = redis::Client::open(instance.redis_url().unwrap()).expect("Failed to create Redis client");
    let mut conn = client.get_connection().expect("Failed to get Redis connection");
    let mut latest: Option<u64> = None;
    for _ in 0..30 {
        latest = get_latest_block_from_redis(&mut conn, &instance.namespace, 3);
        if latest == Some(3) { break; }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(latest, Some(3), "Should process 3 blocks");
    let reader = StateReader::new(instance.redis_url().unwrap(), &instance.namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create reader");
    let test_address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let test_account = reader.get_account(test_address_hash.into(), 3).expect("Failed to get test account").expect("Test account should exist");
    assert_eq!(test_account.balance, U256::from(0x300));
    let eip2935_address_hash = keccak256(HISTORY_STORAGE_ADDRESS);
    let eip2935_account = reader.get_full_account(eip2935_address_hash.into(), 3).expect("Failed to get EIP-2935 account").expect("EIP-2935 account should exist");
    let expected_eip2935_code_hash = keccak256(&HISTORY_STORAGE_CODE);
    assert_eq!(eip2935_account.code_hash, expected_eip2935_code_hash, "EIP-2935 should have correct code hash");
    assert_eq!(eip2935_account.nonce, 1, "EIP-2935 nonce should be 1");
    assert_eq!(eip2935_account.balance, U256::ZERO, "EIP-2935 balance should be 0");
    assert!(!eip2935_account.storage.is_empty(), "EIP-2935 should have storage slots for parent hashes");
    let eip4788_address_hash = keccak256(BEACON_ROOTS_ADDRESS);
    let eip4788_account = reader.get_full_account(eip4788_address_hash.into(), 3).expect("Failed to get EIP-4788 account").expect("EIP-4788 account should exist");
    let expected_eip4788_code_hash = keccak256(&BEACON_ROOTS_CODE);
    assert_eq!(eip4788_account.code_hash, expected_eip4788_code_hash, "EIP-4788 should have correct code hash");
    assert_eq!(eip4788_account.nonce, 1, "EIP-4788 nonce should be 1");
    assert_eq!(eip4788_account.balance, U256::ZERO, "EIP-4788 balance should be 0");
    assert!(eip4788_account.storage.len() >= 2, "EIP-4788 should have at least 2 storage slots (timestamp + root)");
}

#[database_test(redis)]
async fn test_genesis_block_skips_system_calls(instance: TestInstance) {
    use alloy::eips::{eip2935::HISTORY_STORAGE_ADDRESS, eip4788::BEACON_ROOTS_ADDRESS};
    use state_store::redis::StateReader;
    let genesis_json = r#"{"alloc": {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {"balance": "0x100", "nonce": "0x0"}}}"#;
    let genesis_state = genesis::parse_from_str(genesis_json).expect("failed to parse test genesis json");
    let instance = TestInstance::new_redis_with_genesis(genesis_state).await.expect("Failed to create Redis instance with genesis");
    sleep(Duration::from_millis(500)).await;
    let reader = StateReader::new(instance.redis_url().unwrap(), &instance.namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create reader");
    let genesis_address_hash = keccak256(hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap());
    let genesis_account = reader.get_account(genesis_address_hash.into(), 0).expect("Failed to get genesis account");
    assert!(genesis_account.is_some(), "Genesis account should exist at block 0");
    let eip2935_address_hash = keccak256(HISTORY_STORAGE_ADDRESS);
    let eip2935_account = reader.get_account(eip2935_address_hash.into(), 0).expect("Failed to get EIP-2935 account");
    assert!(eip2935_account.is_none(), "EIP-2935 should NOT exist at genesis block (block 0)");
    let eip4788_address_hash = keccak256(BEACON_ROOTS_ADDRESS);
    let eip4788_account = reader.get_account(eip4788_address_hash.into(), 0).expect("Failed to get EIP-4788 account");
    assert!(eip4788_account.is_none(), "EIP-4788 should NOT exist at genesis block (block 0)");
}

// =============================================================================
// Stale Lock Recovery Tests
// =============================================================================

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
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_completed_write_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");
    redis::cmd("SET").arg(&block_key).arg("100").query::<()>(&mut conn).unwrap();
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({"target_block": 100, "started_at": old_timestamp, "writer_id": "crashed-writer"}).to_string();
    redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    let recoveries = writer.recover_stale_locks().unwrap();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 100);
    assert_eq!(recoveries[0].previous_block, Some(100));
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key).query(&mut conn).unwrap();
    assert!(!exists, "lock should be cleared after recovery");
    let block_num: Option<String> = redis::cmd("GET").arg(&block_key).query(&mut conn).unwrap();
    assert_eq!(block_num, Some("100".to_string()), "Block should still be 100 after recovery");
}

#[database_test(redis)]
async fn test_recover_stale_locks_repairs_state_with_diffs(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_repair_with_diffs_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({"target_block": 0, "started_at": old_timestamp, "writer_id": "crashed-writer"}).to_string();
    redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    let test_address_hash = state_store::AddressHash::from_hash(B256::repeat_byte(0xAA));
    let mut storage = std::collections::HashMap::new();
    storage.insert(B256::repeat_byte(0x01), U256::from(42));
    let diff = BlockStateUpdate {
        block_number: 0,
        block_hash: B256::from([1u8; 32]),
        state_root: B256::from([2u8; 32]),
        accounts: vec![state_store::AccountState {
            address_hash: test_address_hash,
            balance: U256::from(1000),
            nonce: 5,
            code_hash: B256::repeat_byte(0xCC),
            code: Some(alloy::primitives::Bytes::from(vec![0x60, 0x80])),
            storage,
            deleted: false,
        }],
    };
    let diff_json = serde_json::to_string(&diff).unwrap();
    redis::cmd("SET").arg(format!("{base_namespace}:diff:0")).arg(&diff_json).query::<()>(&mut conn).unwrap();
    let recoveries = writer.recover_stale_locks().unwrap();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 0);
    assert_eq!(recoveries[0].previous_block, None);
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key).query(&mut conn).unwrap();
    assert!(!exists, "lock should be cleared after recovery");
    let block_key = format!("{namespace}:block");
    let block_num: Option<String> = redis::cmd("GET").arg(&block_key).query(&mut conn).unwrap();
    assert_eq!(block_num, Some("0".to_string()));
    use state_store::redis::StateReader;
    let reader = StateReader::new(&redis.url, &base_namespace, config).expect("Failed to create reader");
    let account = reader.get_account(test_address_hash, 0).expect("Failed to get account").expect("Account should exist after recovery");
    assert_eq!(account.balance, U256::from(1000), "Balance should be recovered correctly");
    assert_eq!(account.nonce, 5, "Nonce should be recovered correctly");
    let storage_value = reader.get_storage(test_address_hash, B256::repeat_byte(0x01), 0).expect("Failed to get storage").expect("Storage should exist after recovery");
    assert_eq!(storage_value, U256::from(42), "Storage should be recovered correctly");
}

#[database_test(redis)]
async fn test_recover_stale_locks_fails_without_diff(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_missing_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let namespace = format!("{base_namespace}:0");
    let lock_key = format!("{namespace}:write_lock");
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({"target_block": 5, "started_at": old_timestamp, "writer_id": "crashed-writer"}).to_string();
    redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    let result = writer.recover_stale_locks();
    assert!(result.is_err(), "recovery should fail without diff");
    let err = result.unwrap_err();
    assert!(matches!(err, state_store::redis::common::error::StateError::MissingStateDiff { .. }), "expected MissingStateDiff error, got: {err:?}");
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key).query(&mut conn).unwrap();
    assert!(exists, "lock should remain when recovery fails");
}

#[database_test(redis)]
async fn test_force_recover_namespace_specific_index(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_force_recover_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    for idx in [0, 2] {
        let namespace = format!("{base_namespace}:{idx}");
        let lock_key = format!("{namespace}:write_lock");
        let block_key = format!("{namespace}:block");
        let target_block = 100 + idx as u64;
        redis::cmd("SET").arg(&block_key).arg(target_block.to_string()).query::<()>(&mut conn).unwrap();
        let lock_json = serde_json::json!({"target_block": target_block, "started_at": old_timestamp, "writer_id": format!("writer-{}", idx)}).to_string();
        redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    }
    let recovery = writer.force_recover_namespace(2).unwrap();
    assert!(recovery.is_some());
    let recovery = recovery.unwrap();
    assert_eq!(recovery.namespace, format!("{base_namespace}:2"));
    assert_eq!(recovery.target_block, 102);
    let lock_key_0 = format!("{base_namespace}:0:write_lock");
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key_0).query(&mut conn).unwrap();
    assert!(exists, "namespace 0 lock should still exist");
    let lock_key_2 = format!("{base_namespace}:2:write_lock");
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key_2).query(&mut conn).unwrap();
    assert!(!exists, "namespace 2 lock should be cleared");
}

#[database_test(redis)]
async fn test_recovery_applies_multiple_diffs(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_multi_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let namespace = format!("{base_namespace}:1");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");
    redis::cmd("SET").arg(&block_key).arg("97").query::<()>(&mut conn).unwrap();
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({"target_block": 100, "started_at": old_timestamp, "writer_id": "crashed-writer"}).to_string();
    redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    let test_address_hash = state_store::AddressHash::from_hash(B256::repeat_byte(0xBB));
    for block_num in 98..=100u64 {
        let diff = BlockStateUpdate {
            block_number: block_num,
            block_hash: B256::from([block_num as u8; 32]),
            state_root: B256::from([(block_num + 100) as u8; 32]),
            accounts: vec![state_store::AccountState {
                address_hash: test_address_hash,
                balance: U256::from(block_num * 100),
                nonce: block_num - 97,
                code_hash: B256::repeat_byte(0xDD),
                code: None,
                storage: std::collections::HashMap::new(),
                deleted: false,
            }],
        };
        let diff_json = serde_json::to_string(&diff).unwrap();
        redis::cmd("SET").arg(format!("{base_namespace}:diff:{block_num}")).arg(&diff_json).query::<()>(&mut conn).unwrap();
    }
    let recoveries = writer.recover_stale_locks().unwrap();
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 100);
    assert_eq!(recoveries[0].previous_block, Some(97));
    let block_num: Option<String> = redis::cmd("GET").arg(&block_key).query(&mut conn).unwrap();
    assert_eq!(block_num, Some("100".to_string()));
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key).query(&mut conn).unwrap();
    assert!(!exists, "lock should be cleared after recovery");
    use state_store::redis::StateReader;
    let reader = StateReader::new(&redis.url, &base_namespace, config).expect("Failed to create reader");
    let account = reader.get_account(test_address_hash, 100).expect("Failed to get account").expect("Account should exist after multi-diff recovery");
    assert_eq!(account.balance, U256::from(10000), "Balance should be from block 100 diff");
    assert_eq!(account.nonce, 3, "Nonce should be from block 100 diff");
}

#[database_test(redis)]
async fn test_recovery_partial_diff_chain_fails(_instance: TestInstance) {
    let redis = get_shared_redis().await;
    let base_namespace = format!("test_partial_diff_{}", uuid::Uuid::new_v4());
    let config = CircularBufferConfig::new(3).unwrap();
    let chunked_config = ChunkedWriteConfig::new(1000, 60);
    let writer = StateWriter::with_chunked_config(&redis.url, &base_namespace, config.clone(), chunked_config).unwrap();
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut conn = client.get_connection().unwrap();
    let namespace = format!("{base_namespace}:1");
    let lock_key = format!("{namespace}:write_lock");
    let block_key = format!("{namespace}:block");
    redis::cmd("SET").arg(&block_key).arg("97").query::<()>(&mut conn).unwrap();
    let old_timestamp = chrono::Utc::now().timestamp() - 120;
    let lock_json = serde_json::json!({"target_block": 100, "started_at": old_timestamp, "writer_id": "crashed-writer"}).to_string();
    redis::cmd("SET").arg(&lock_key).arg(&lock_json).query::<()>(&mut conn).unwrap();
    let diff_key = format!("{base_namespace}:diff:98");
    let diff = BlockStateUpdate::new(98, B256::from([98u8; 32]), B256::from([198u8; 32]));
    let diff_json = serde_json::to_string(&diff).unwrap();
    redis::cmd("SET").arg(&diff_key).arg(&diff_json).query::<()>(&mut conn).unwrap();
    let result = writer.recover_stale_locks();
    assert!(result.is_err(), "recovery should fail with partial diffs");
    let err = result.unwrap_err();
    match err {
        state_store::redis::common::error::StateError::MissingStateDiff { needed_block, target_block } => {
            assert_eq!(needed_block, 99);
            assert_eq!(target_block, 100);
        }
        _ => panic!("expected MissingStateDiff error, got: {err:?}"),
    }
    let exists: bool = redis::cmd("EXISTS").arg(&lock_key).query(&mut conn).unwrap();
    assert!(exists, "lock should remain when recovery fails");
}

// =============================================================================
// MDBX Bootstrap Recovery Tests
// =============================================================================

#[tokio::test]
async fn test_mdbx_bootstrap_recovery_without_diffs() {
    use state_store::{AccountState, AddressHash, BlockStateUpdate, Reader, Writer, mdbx::{StateWriter, common::CircularBufferConfig}};
    use std::collections::HashMap;

    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
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

    let stats = writer.bootstrap_from_snapshot(accounts.clone(), 100, B256::repeat_byte(0x11), B256::repeat_byte(0x22)).expect("bootstrap should succeed");
    assert_eq!(stats.accounts_written, 3, "should write to all 3 namespaces");

    let reader = writer.reader();
    assert!(reader.is_block_available(100).unwrap(), "Block 100 should be available");
    assert_eq!(reader.latest_block_number().unwrap(), Some(100));

    let account = reader.get_account(test_address_hash, 100).expect("Should get account").expect("Account should exist");
    assert_eq!(account.balance, U256::from(1000));
    assert_eq!(account.nonce, 5);

    let slot_value = reader.get_storage(test_address_hash, B256::repeat_byte(0x01), 100).expect("Should get storage").expect("Storage should exist");
    assert_eq!(slot_value, U256::from(100));

    // Simulate check_and_recover_state logic
    let buffer_size = reader.buffer_size();
    let latest_block = reader.latest_block_number().unwrap().unwrap();
    let oldest_needed = latest_block.saturating_sub(u64::from(buffer_size) - 1);

    let mut missing_diffs = Vec::new();
    for block in oldest_needed..=latest_block {
        if block == 0 { continue; }
        let has_diff = reader.has_state_diff(block).expect("Should check diff");
        if !has_diff { missing_diffs.push(block); }
    }
    assert!(!missing_diffs.is_empty(), "Expected missing diffs after bootstrap");
    assert!(missing_diffs.contains(&100), "Diff for block 100 should be missing after bootstrap");

    // Block 101 should work without needing diffs
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
            storage: { let mut s = HashMap::new(); s.insert(B256::repeat_byte(0x01), U256::from(101)); s },
            deleted: false,
        }],
    };

    let stats_101 = writer.commit_block(&update_101).expect("Block 101 should commit");
    assert_eq!(stats_101.diffs_applied, 0, "Block 101 should NOT need any diffs after bootstrap");
    assert!(reader.is_block_available(101).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(101));

    let account_101 = reader.get_account(test_address_hash, 101).expect("Should get account").expect("Account should exist");
    assert_eq!(account_101.balance, U256::from(1100));
    assert_eq!(account_101.nonce, 6);

    // Block 102 needs diff from 101
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
            storage: { let mut s = HashMap::new(); s.insert(B256::repeat_byte(0x01), U256::from(102)); s },
            deleted: false,
        }],
    };

    let stats_102 = writer.commit_block(&update_102).expect("Block 102 should commit");
    assert_eq!(stats_102.diffs_applied, 1, "Block 102 should apply 1 diff (block 101)");
    assert!(reader.is_block_available(102).unwrap());

    // Block 103 needs diffs from 101 and 102
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
            storage: { let mut s = HashMap::new(); s.insert(B256::repeat_byte(0x01), U256::from(103)); s },
            deleted: false,
        }],
    };

    let stats_103 = writer.commit_block(&update_103).expect("Block 103 should commit");
    assert_eq!(stats_103.diffs_applied, 2, "Block 103 should apply 2 diffs (101, 102)");
    assert!(reader.is_block_available(103).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(103));

    let account_103 = reader.get_account(test_address_hash, 103).expect("Should get account").expect("Account should exist");
    assert_eq!(account_103.balance, U256::from(1300));
    assert_eq!(account_103.nonce, 8);

    assert!(!reader.is_block_available(100).unwrap(), "Block 100 should be rotated out");
    assert!(reader.is_block_available(101).unwrap());
    assert!(reader.is_block_available(102).unwrap());
    assert!(reader.is_block_available(103).unwrap());

    let storage_101 = reader.get_storage(test_address_hash, B256::repeat_byte(0x01), 101).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_101, U256::from(101), "Storage at block 101");
    let storage_102 = reader.get_storage(test_address_hash, B256::repeat_byte(0x01), 102).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_102, U256::from(102), "Storage at block 102");
    let storage_103 = reader.get_storage(test_address_hash, B256::repeat_byte(0x01), 103).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_103, U256::from(103), "Storage at block 103");
}

// =============================================================================
// State Diff Reader/Writer Tests
// =============================================================================

#[database_test(all)]
async fn test_has_state_diff_returns_false_for_missing_diff(instance: TestInstance) {
    let reader = instance.create_reader().expect("Failed to create reader");
    let has_diff = reader.has_state_diff_boxed(100).expect("Should check diff");
    assert!(!has_diff, "Should return false for non-existent diff");
}

#[database_test(all)]
async fn test_has_state_diff_returns_true_after_block_commit(instance: TestInstance) {
    let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let has_diff = reader.has_state_diff_boxed(1).expect("Should check diff");
    assert!(has_diff, "Should return true for existing diff");
}

#[database_test(all)]
async fn test_get_namespace_block_returns_none_for_empty_namespace(instance: TestInstance) {
    let reader = instance.create_reader().expect("Failed to create reader");
    for ns in 0..3u8 {
        let block = reader.get_namespace_block_boxed(ns).expect("Should get namespace block");
        assert!(block.is_none(), "Namespace {ns} should be empty initially");
    }
}

#[database_test(all)]
async fn test_get_namespace_block_returns_correct_block_after_commit(instance: TestInstance) {
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(3) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let ns0 = reader.get_namespace_block_boxed(0).unwrap();
    let ns1 = reader.get_namespace_block_boxed(1).unwrap();
    let ns2 = reader.get_namespace_block_boxed(2).unwrap();
    assert_eq!(ns0, Some(3), "Namespace 0 should have block 3");
    assert_eq!(ns1, Some(1), "Namespace 1 should have block 1");
    assert_eq!(ns2, Some(2), "Namespace 2 should have block 2");
}

#[database_test(all)]
async fn test_buffer_size_returns_configured_size(instance: TestInstance) {
    let reader = instance.create_reader().expect("Failed to create reader");
    let buffer_size = reader.buffer_size_boxed();
    assert_eq!(buffer_size, 3, "Buffer size should be 3");
}

#[database_test(redis)]
async fn test_store_state_diff_creates_retrievable_diff(instance: TestInstance) {
    let writer = StateWriter::new(instance.redis_url().unwrap(), &instance.namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create writer");
    let update = BlockStateUpdate::new(50, B256::repeat_byte(0x11), B256::repeat_byte(0x22));
    writer.store_state_diff(&update).expect("Should store diff");
    let has_diff = writer.has_state_diff(50).expect("Should check diff");
    assert!(has_diff, "Stored diff should exist");
}

#[database_test(redis)]
async fn test_store_state_diff_with_accounts(instance: TestInstance) {
    let writer = StateWriter::new(instance.redis_url().unwrap(), &instance.namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create writer");
    let test_address_hash = keccak256(hex::decode("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap());
    let mut storage = std::collections::HashMap::new();
    storage.insert(B256::repeat_byte(0x01), U256::from(100));
    let update = BlockStateUpdate {
        block_number: 75,
        block_hash: B256::repeat_byte(0x33),
        state_root: B256::repeat_byte(0x44),
        accounts: vec![state_store::AccountState {
            address_hash: test_address_hash.into(),
            balance: U256::from(1000),
            nonce: 5,
            code_hash: B256::repeat_byte(0xCC),
            code: Some(alloy::primitives::Bytes::from(vec![0x60, 0x80])),
            storage,
            deleted: false,
        }],
    };
    writer.store_state_diff(&update).expect("Should store diff with accounts");
    let has_diff = writer.has_state_diff(75).expect("Should check diff");
    assert!(has_diff, "Stored diff with accounts should exist");
}

#[tokio::test]
async fn test_mdbx_store_state_diff_creates_retrievable_diff() {
    use state_store::mdbx::{StateWriter, common::CircularBufferConfig};
    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(3).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config).unwrap();
    let update = BlockStateUpdate::new(50, B256::repeat_byte(0x11), B256::repeat_byte(0x22));
    writer.store_state_diff(&update).expect("Should store diff");
    let has_diff = writer.has_state_diff(50).expect("Should check diff");
    assert!(has_diff, "Stored diff should exist");
}

#[tokio::test]
async fn test_mdbx_store_state_diff_enables_subsequent_rotation() {
    use state_store::{AccountState, Reader, Writer, mdbx::{StateWriter, common::CircularBufferConfig}};
    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(3).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config).unwrap();
    let test_address_hash = state_store::AddressHash::from_hash(B256::repeat_byte(0xAA));
    let accounts = vec![AccountState { address_hash: test_address_hash, balance: U256::from(1000), nonce: 5, code_hash: B256::repeat_byte(0xCC), code: None, storage: std::collections::HashMap::new(), deleted: false }];
    writer.bootstrap_from_snapshot(accounts, 100, B256::repeat_byte(0x11), B256::repeat_byte(0x22)).expect("bootstrap should succeed");
    let update_101 = BlockStateUpdate { block_number: 101, block_hash: B256::repeat_byte(0x33), state_root: B256::repeat_byte(0x44), accounts: vec![AccountState { address_hash: test_address_hash, balance: U256::from(1100), nonce: 6, code_hash: B256::repeat_byte(0xCC), code: None, storage: std::collections::HashMap::new(), deleted: false }] };
    writer.commit_block(&update_101).expect("Block 101 should commit");
    let update_102 = BlockStateUpdate { block_number: 102, block_hash: B256::repeat_byte(0x55), state_root: B256::repeat_byte(0x66), accounts: vec![AccountState { address_hash: test_address_hash, balance: U256::from(1200), nonce: 7, code_hash: B256::repeat_byte(0xCC), code: None, storage: std::collections::HashMap::new(), deleted: false }] };
    writer.store_state_diff(&update_102).expect("Should store diff 102");
    assert!(writer.has_state_diff(102).unwrap(), "Diff 102 should exist");
    writer.commit_block(&update_102).expect("Block 102 should commit");
    assert!(writer.reader().is_block_available(102).unwrap());
    let account = writer.reader().get_account(test_address_hash, 102).expect("Should get account").expect("Account should exist");
    assert_eq!(account.balance, U256::from(1200), "Balance should be correct after commit");
    assert_eq!(account.nonce, 7, "Nonce should be correct after commit");
}

#[database_test(redis)]
async fn test_worker_recovers_missing_diff_before_commit(instance: TestInstance) {
    let test_address = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": format!("0x{:x}", i * 100) }, "nonce": { "+": format!("0x{:x}", i) }, "code": { "+": "0x" }, "storage": {}}}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(3) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let account_3 = reader.get_account_boxed(address_hash.into(), 3).expect("Should get account").expect("Account should exist at block 3");
    assert_eq!(account_3.balance, U256::from(300), "Balance at block 3");
    if let Some(redis_url) = instance.redis_url() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let diff_key = format!("{}:diff:2", instance.namespace);
        redis::cmd("DEL").arg(&diff_key).query::<()>(&mut conn).unwrap();
        let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(&mut conn).unwrap();
        assert!(!exists, "Diff 2 should be deleted");
    }
    let parity_trace_2_recovery = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0xc8" }, "nonce": { "+": "0x2" }, "code": { "+": "0x" }, "storage": {}}}, "output": "0x"}]});
    let parity_trace_4 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x4444444444444444444444444444444444444444444444444444444444444444", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "*": { "from": "0x12c", "to": "0x190" } }, "nonce": { "*": { "from": "0x3", "to": "0x4" } }, "code": "=", "storage": {}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_2_recovery);
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_4);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(500)).await;
    for _ in 0..30 {
        if let Some(4) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let latest = reader.latest_block_number_boxed().expect("Should get latest");
    assert_eq!(latest, Some(4), "Block 4 should be processed after recovery");
    if let Some(redis_url) = instance.redis_url() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let diff_key = format!("{}:diff:2", instance.namespace);
        let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(&mut conn).unwrap();
        assert!(exists, "Diff 2 should be recovered");
    }
    let account_4 = reader.get_account_boxed(address_hash.into(), 4).expect("Should get account").expect("Account should exist at block 4");
    assert_eq!(account_4.balance, U256::from(400), "Balance should be 400 after recovery");
    assert_eq!(account_4.nonce, 4, "Nonce should be 4 after recovery");
}

#[database_test(redis)]
async fn test_startup_recovery_fills_missing_diffs(instance: TestInstance) {
    let redis_url = instance.redis_url().unwrap();
    let base_namespace = format!("startup_recovery_{}", uuid::Uuid::new_v4());
    {
        let _writer = StateWriter::new(redis_url, &base_namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create writer");
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let ns0 = format!("{base_namespace}:0");
        redis::cmd("SET").arg(format!("{ns0}:block")).arg("3").query::<()>(&mut conn).unwrap();
        redis::cmd("SET").arg(format!("{base_namespace}:latest_block")).arg("3").query::<()>(&mut conn).unwrap();
        for block_num in [1, 3] {
            let diff = BlockStateUpdate::new(block_num, B256::repeat_byte(block_num as u8), B256::repeat_byte((block_num + 100) as u8));
            let diff_json = serde_json::to_string(&diff).unwrap();
            redis::cmd("SET").arg(format!("{base_namespace}:diff:{block_num}")).arg(&diff_json).query::<()>(&mut conn).unwrap();
        }
        let exists: bool = redis::cmd("EXISTS").arg(format!("{base_namespace}:diff:2")).query(&mut conn).unwrap();
        assert!(!exists, "Diff 2 should not exist initially");
    }
    let writer = StateWriter::new(redis_url, &base_namespace, CircularBufferConfig::new(3).unwrap()).expect("Failed to create writer");
    assert!(writer.has_state_diff(1).unwrap(), "Diff 1 should exist");
    assert!(!writer.has_state_diff(2).unwrap(), "Diff 2 should be missing");
    assert!(writer.has_state_diff(3).unwrap(), "Diff 3 should exist");
}

#[database_test(redis)]
async fn test_recovery_handles_genesis_block_correctly(instance: TestInstance) {
    let reader = instance.create_reader().expect("Failed to create reader");
    let latest = reader.latest_block_number_boxed().expect("Should get latest");
    assert!(latest.is_none() || latest == Some(0), "Should start empty or at genesis");
    let has_diff_0 = reader.has_state_diff_boxed(0);
    assert!(has_diff_0.is_ok(), "Should handle genesis block check");
}

#[database_test(redis)]
async fn test_namespace_block_after_rotation(instance: TestInstance) {
    for i in 1..=6 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(6) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let ns0 = reader.get_namespace_block_boxed(0).unwrap();
    let ns1 = reader.get_namespace_block_boxed(1).unwrap();
    let ns2 = reader.get_namespace_block_boxed(2).unwrap();
    assert_eq!(ns0, Some(6), "Namespace 0 should have block 6");
    assert_eq!(ns1, Some(4), "Namespace 1 should have block 4");
    assert_eq!(ns2, Some(5), "Namespace 2 should have block 5");
}

#[database_test(redis)]
async fn test_diffs_cleaned_up_after_buffer_rotation(instance: TestInstance) {
    for i in 1..=6 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(6) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    for old_block in 1..=3 {
        let has_diff = reader.has_state_diff_boxed(old_block).unwrap();
        assert!(!has_diff, "Diff for old block {old_block} should be cleaned up");
    }
    for recent_block in 4..=6 {
        let has_diff = reader.has_state_diff_boxed(recent_block).unwrap();
        assert!(has_diff, "Diff for recent block {recent_block} should exist");
    }
}

#[tokio::test]
async fn test_mdbx_recovery_no_diffs_needed_for_sequential_blocks() {
    use state_store::{AccountState, Reader, Writer, mdbx::{StateWriter, common::CircularBufferConfig}};
    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(3).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config).unwrap();
    let test_address_hash = state_store::AddressHash::from_hash(B256::repeat_byte(0xAA));
    for i in 1..=3u64 {
        let update = BlockStateUpdate { block_number: i, block_hash: B256::repeat_byte(i as u8), state_root: B256::repeat_byte((i + 100) as u8), accounts: vec![AccountState { address_hash: test_address_hash, balance: U256::from(i * 1000), nonce: i, code_hash: B256::repeat_byte(0xCC), code: None, storage: std::collections::HashMap::new(), deleted: false }] };
        let stats = writer.commit_block(&update).expect(&format!("Block {i} should commit"));
        if i == 1 { assert_eq!(stats.diffs_applied, 0, "Block 1 needs no diffs"); }
    }
    for i in 1..=3 { assert!(writer.reader().is_block_available(i).unwrap()); }
    for i in 1..=3u64 {
        let account = writer.reader().get_account(test_address_hash, i).expect("Should get account").expect("Account should exist");
        assert_eq!(account.balance, U256::from(i * 1000), "Balance at block {}", i);
        assert_eq!(account.nonce, i, "Nonce at block {}", i);
    }
}

#[tokio::test]
async fn test_mdbx_has_state_diff_and_get_namespace_block() {
    use state_store::{AccountState, Reader, Writer, mdbx::{StateWriter, common::CircularBufferConfig}};
    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(3).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config).unwrap();
    let test_address_hash = state_store::AddressHash::from_hash(B256::repeat_byte(0xAA));
    assert!(!writer.has_state_diff(1).unwrap());
    assert!(writer.get_namespace_block(0).unwrap().is_none());
    assert!(writer.get_namespace_block(1).unwrap().is_none());
    assert!(writer.get_namespace_block(2).unwrap().is_none());
    let update = BlockStateUpdate { block_number: 1, block_hash: B256::repeat_byte(0x11), state_root: B256::repeat_byte(0x22), accounts: vec![AccountState { address_hash: test_address_hash, balance: U256::from(1000), nonce: 1, code_hash: B256::repeat_byte(0xCC), code: None, storage: std::collections::HashMap::new(), deleted: false }] };
    writer.commit_block(&update).unwrap();
    assert!(writer.has_state_diff(1).unwrap());
    assert_eq!(writer.get_namespace_block(1).unwrap(), Some(1));
    assert!(writer.get_namespace_block(0).unwrap().is_none());
    assert!(writer.get_namespace_block(2).unwrap().is_none());
}

#[tokio::test]
async fn test_mdbx_buffer_size_accessor() {
    use state_store::{Reader, mdbx::{StateWriter, common::CircularBufferConfig}};
    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(5).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config).unwrap();
    assert_eq!(writer.buffer_size(), 5);
}

#[database_test(redis)]
async fn test_recovery_report_tracking(instance: TestInstance) {
    let reader = instance.create_reader().expect("Failed to create reader");
    for i in 1..=2 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    for _ in 0..30 {
        if let Some(2) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(reader.has_state_diff_boxed(1).unwrap());
    assert!(reader.has_state_diff_boxed(2).unwrap());
}

#[database_test(redis)]
async fn test_ensure_intermediate_diffs_recovers_chain(instance: TestInstance) {
    let test_address = "0xffffffffffffffffffffffffffffffffffffffff";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    for i in 1..=3 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": format!("0x{:x}", i * 1000) }, "nonce": { "+": format!("0x{:x}", i) }, "code": { "+": "0x" }, "storage": {}}}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
        instance.http_server_mock.send_new_head();
        sleep(Duration::from_millis(150)).await;
    }
    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(3) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }
    let account = reader.get_account_boxed(address_hash.into(), 3).expect("Should get account").expect("Account should exist");
    assert_eq!(account.balance, U256::from(3000));
    for i in 4..=6 {
        let parity_trace = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": format!("0x{:064x}", i), "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "*": { "from": format!("0x{:x}", (i-1) * 1000), "to": format!("0x{:x}", i * 1000) }}, "nonce": { "*": { "from": format!("0x{:x}", i-1), "to": format!("0x{:x}", i) }}, "code": "=", "storage": {}}}, "output": "0x"}]});
        instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace);
    }
    for _ in 4..=6 { instance.http_server_mock.send_new_head(); sleep(Duration::from_millis(200)).await; }
    for _ in 0..30 {
        if let Some(6) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(100)).await;
    }
    let account_6 = reader.get_account_boxed(address_hash.into(), 6).expect("Should get account").expect("Account should exist at block 6");
    assert_eq!(account_6.balance, U256::from(6000), "Balance should be 6000 at block 6");
    assert_eq!(account_6.nonce, 6, "Nonce should be 6 at block 6");
    assert!(reader.has_state_diff_boxed(4).unwrap(), "Diff 4 should exist");
    assert!(reader.has_state_diff_boxed(5).unwrap(), "Diff 5 should exist");
    assert!(reader.has_state_diff_boxed(6).unwrap(), "Diff 6 should exist");
}

#[tokio::test]
async fn test_mdbx_bootstrap_then_check_and_recover_state_logic() {
    use state_store::{AccountState, AddressHash, BlockStateUpdate, Reader, Writer, mdbx::{StateWriter, common::CircularBufferConfig}};
    use std::collections::HashMap;

    let mdbx_dir = crate::integration_tests::mdbx_fixture::MdbxTestDir::new().expect("Failed to create MDBX test dir");
    let config = CircularBufferConfig::new(3).unwrap();
    let writer = StateWriter::new(mdbx_dir.path_str(), config.clone()).unwrap();
    let test_address_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));
    let accounts = vec![AccountState { address_hash: test_address_hash, balance: U256::from(1000), nonce: 5, code_hash: B256::repeat_byte(0xCC), code: None, storage: HashMap::new(), deleted: false }];
    writer.bootstrap_from_snapshot(accounts, 100, B256::repeat_byte(0x11), B256::repeat_byte(0x22)).expect("bootstrap should succeed");
    let reader = writer.reader();

    // Simulate check_and_recover_state logic
    let buffer_size = reader.buffer_size();
    let latest_block = reader.latest_block_number().unwrap().unwrap();
    assert_eq!(latest_block, 100);
    let oldest_needed = latest_block.saturating_sub(u64::from(buffer_size) - 1);
    let mut missing_diffs = Vec::new();
    for block in oldest_needed..=latest_block {
        if block == 0 { continue; }
        let has_diff = reader.has_state_diff(block).expect("Should check diff");
        if !has_diff { missing_diffs.push(block); }
    }
    assert_eq!(missing_diffs.len(), 3, "Should have 3 missing diffs (98, 99, 100)");
    assert!(missing_diffs.contains(&98));
    assert!(missing_diffs.contains(&99));
    assert!(missing_diffs.contains(&100));

    // Verify we can still commit block 101 without errors (no diffs needed)
    let update_101 = BlockStateUpdate { block_number: 101, block_hash: B256::repeat_byte(0x33), state_root: B256::repeat_byte(0x44), accounts: vec![AccountState { address_hash: test_address_hash, balance: U256::from(1100), nonce: 6, code_hash: B256::repeat_byte(0xCC), code: None, storage: HashMap::new(), deleted: false }] };
    let stats = writer.commit_block(&update_101).expect("Should commit block 101");
    assert_eq!(stats.diffs_applied, 0, "No diffs needed for block 101 after bootstrap");

    let account = reader.get_account(test_address_hash, 101).expect("Should get account").expect("Account should exist");
    assert_eq!(account.balance, U256::from(1100));
    assert!(reader.has_state_diff(101).unwrap(), "Diff 101 should exist after commit");
    assert!(!reader.has_state_diff(98).unwrap());
    assert!(!reader.has_state_diff(99).unwrap());
    assert!(!reader.has_state_diff(100).unwrap());
}

#[database_test(redis)]
async fn test_recovery_preserves_storage_state_correctly(instance: TestInstance) {
    let test_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let address_hash = keccak256(hex::decode(test_address.trim_start_matches("0x")).unwrap());
    let slot_hash = keccak256(U256::from(1).to_be_bytes::<32>());

    let parity_trace_1 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": { "+": "0x64" }, "nonce": { "+": "0x1" }, "code": { "+": "0x" }, "storage": {"0x0000000000000000000000000000000000000000000000000000000000000001": { "+": "0x0000000000000000000000000000000000000000000000000000000000000064" }}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_1);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    let reader = instance.create_reader().expect("Failed to create reader");
    for _ in 0..30 {
        if let Some(1) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }

    let storage_1 = reader.get_storage_boxed(address_hash.into(), slot_hash, 1).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_1, U256::from(100), "Storage at block 1");

    let parity_trace_2 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": "=", "nonce": "=", "code": "=", "storage": {"0x0000000000000000000000000000000000000000000000000000000000000001": {"*": {"from": "0x0000000000000000000000000000000000000000000000000000000000000064", "to": "0x00000000000000000000000000000000000000000000000000000000000000c8"}}}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_2);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    for _ in 0..30 {
        if let Some(2) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }

    let storage_2 = reader.get_storage_boxed(address_hash.into(), slot_hash, 2).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_2, U256::from(200), "Storage at block 2");

    let parity_trace_3 = json!({"jsonrpc": "2.0", "id": 1, "result": [{"transactionHash": "0x3333333333333333333333333333333333333333333333333333333333333333", "trace": [], "vmTrace": null, "stateDiff": {test_address: {"balance": "=", "nonce": "=", "code": "=", "storage": {"0x0000000000000000000000000000000000000000000000000000000000000001": {"*": {"from": "0x00000000000000000000000000000000000000000000000000000000000000c8", "to": "0x000000000000000000000000000000000000000000000000000000000000012c"}}}}}, "output": "0x"}]});
    instance.http_server_mock.add_response("trace_replayBlockTransactions", parity_trace_3);
    instance.http_server_mock.send_new_head();
    sleep(Duration::from_millis(300)).await;

    for _ in 0..30 {
        if let Some(3) = reader.latest_block_number_boxed().ok().flatten() { break; }
        sleep(Duration::from_millis(50)).await;
    }

    let storage_3 = reader.get_storage_boxed(address_hash.into(), slot_hash, 3).expect("Should get storage").expect("Storage should exist");
    assert_eq!(storage_3, U256::from(300), "Storage at block 3");

    assert!(reader.has_state_diff_boxed(1).unwrap(), "Diff 1 should exist");
    assert!(reader.has_state_diff_boxed(2).unwrap(), "Diff 2 should exist");
    assert!(reader.has_state_diff_boxed(3).unwrap(), "Diff 3 should exist");
}