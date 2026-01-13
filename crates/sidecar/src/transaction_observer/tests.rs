use super::{
    IncidentData,
    IncidentReport,
    TransactionObserver,
    TransactionObserverConfig,
    TransactionObserverError,
};
use crate::{
    execution_ids::TxExecutionId,
    utils::test_drivers::LocalInstanceMockDriver,
};
use assertion_executor::test_utils::{
    COUNTER_ADDRESS,
    counter_call,
};
use chrono::{
    SecondsFormat,
    Utc,
};
use httpmock::{
    Mock,
    prelude::*,
};
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    context_interface::{
        block::BlobExcessGasAndPrice,
        transaction::AccessList,
    },
    primitives::{
        Address,
        B256,
        Bytes,
        FixedBytes,
        TxKind,
        U256,
    },
};
use serde_json::{
    Map,
    Value,
    json,
};
use std::{
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tempfile::TempDir;

fn hex_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn u64_hex(value: u64) -> String {
    format!("0x{value:x}")
}

fn u128_hex(value: u128) -> String {
    format!("0x{value:x}")
}

fn u256_hex(value: U256) -> String {
    format!("0x{value:x}")
}

fn normalize_hex(value: &str) -> String {
    format!("0x{}", value.trim_start_matches("0x").to_ascii_lowercase())
}

fn u64_from_hex(value: &str) -> u64 {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).expect("u64 hex decode")
}

fn u128_from_hex(value: &str) -> u128 {
    u128::from_str_radix(value.trim_start_matches("0x"), 16).expect("u128 hex decode")
}

fn array_from_hex<const N: usize>(value: &str) -> [u8; N] {
    let trimmed = value.trim_start_matches("0x");
    let bytes = hex::decode(trimmed).expect("hex decode");
    bytes
        .as_slice()
        .try_into()
        .expect("hex length mismatch")
}

fn address_from_hex(value: &str) -> Address {
    Address::from(array_from_hex::<20>(value))
}

fn fixed_bytes_from_hex(value: &str) -> FixedBytes<32> {
    FixedBytes::from(array_from_hex::<32>(value))
}

fn b256_from_hex(value: &str) -> B256 {
    B256::from(array_from_hex::<32>(value))
}

fn format_timestamp(timestamp: u64) -> String {
    let seconds = i64::try_from(timestamp).expect("valid timestamp");
    let date_time = chrono::DateTime::<Utc>::from_timestamp(seconds, 0).expect("valid timestamp");
    date_time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn build_test_tx_pair() -> (B256, B256, TxEnv, TxEnv) {
    let tx_hash_pass = B256::from([0x11; 32]);
    let tx_hash_fail = B256::from([0x22; 32]);
    let caller = counter_call().caller;
    let chain_id = 1u64;
    let gas_limit = 100_000u64;
    let gas_price = 10u128;

    let mut tx_pass = counter_call();
    tx_pass.caller = caller;
    tx_pass.gas_limit = gas_limit;
    tx_pass.gas_price = gas_price;
    tx_pass.nonce = 0;
    tx_pass.chain_id = Some(chain_id);
    tx_pass.tx_type = 0;

    let mut tx_fail = tx_pass.clone();
    tx_fail.nonce = 1;

    (tx_hash_pass, tx_hash_fail, tx_pass, tx_fail)
}

fn tx_data_matches(tx_data: &Map<String, Value>, tx_hash: &B256, tx_env: &TxEnv) -> bool {
    let Some(expected_chain_id) = tx_env.chain_id else {
        return false;
    };
    let expected_hash = hex_bytes(tx_hash.as_slice());
    let expected_nonce = u64_hex(tx_env.nonce);
    let expected_gas_limit = u64_hex(tx_env.gas_limit);
    let expected_to_address = match &tx_env.kind {
        TxKind::Call(to) => hex_bytes(to.as_slice()),
        TxKind::Create => String::new(),
    };
    let expected_from_address = hex_bytes(tx_env.caller.as_slice());
    let expected_value = u256_hex(tx_env.value);
    let expected_gas_price = u128_hex(tx_env.gas_price);
    let expected_priority_fee = u128_hex(tx_env.gas_priority_fee.unwrap_or_default());
    let expected_blob_fee = u128_hex(tx_env.max_fee_per_blob_gas);
    let expected_data = hex_bytes(tx_env.data.as_ref());

    if tx_data.get("transaction_hash").and_then(Value::as_str) != Some(expected_hash.as_str()) {
        return false;
    }
    if tx_data.get("chain_id").and_then(Value::as_u64) != Some(expected_chain_id) {
        return false;
    }
    if tx_data.get("nonce").and_then(Value::as_str) != Some(expected_nonce.as_str()) {
        return false;
    }
    if tx_data.get("gas_limit").and_then(Value::as_str) != Some(expected_gas_limit.as_str()) {
        return false;
    }
    if tx_data.get("to_address").and_then(Value::as_str) != Some(expected_to_address.as_str()) {
        return false;
    }
    if tx_data.get("from_address").and_then(Value::as_str) != Some(expected_from_address.as_str()) {
        return false;
    }
    if tx_data.get("value").and_then(Value::as_str) != Some(expected_value.as_str()) {
        return false;
    }
    if let Some(gas_price) = tx_data.get("gas_price").and_then(Value::as_str)
        && gas_price != expected_gas_price
    {
        return false;
    }
    if let Some(max_fee_per_gas) = tx_data.get("max_fee_per_gas").and_then(Value::as_str)
        && max_fee_per_gas != expected_gas_price
    {
        return false;
    }
    if let Some(max_priority_fee) = tx_data
        .get("max_priority_fee_per_gas")
        .and_then(Value::as_str)
        && max_priority_fee != expected_priority_fee
    {
        return false;
    }
    if let Some(max_fee_per_blob_gas) = tx_data.get("max_fee_per_blob_gas").and_then(Value::as_str)
        && max_fee_per_blob_gas != expected_blob_fee
    {
        return false;
    }
    if tx_data.get("data").and_then(Value::as_str) != Some(expected_data.as_str()) {
        return false;
    }

    true
}

async fn wait_for_mock_calls(mock: &Mock<'_>, timeout: Duration) {
    let start = Instant::now();
    while mock.calls() == 0 && start.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn collect_invalid_txs(
    pass_invalid: bool,
    fail_invalid: bool,
    tx_hash_pass: B256,
    tx_pass: TxEnv,
    tx_hash_fail: B256,
    tx_fail: TxEnv,
) -> Vec<(B256, TxEnv)> {
    let mut invalid_txs = Vec::new();
    if pass_invalid {
        invalid_txs.push((tx_hash_pass, tx_pass));
    }
    if fail_invalid {
        invalid_txs.push((tx_hash_fail, tx_fail));
    }
    invalid_txs
}

fn build_incident_report() -> IncidentReport {
    let tx_env = TxEnv {
        caller: Address::from([0x01; 20]),
        gas_limit: 21_000,
        gas_price: 100u128,
        kind: TxKind::Call(Address::from([0x02; 20])),
        value: U256::from(5),
        data: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
        nonce: 7,
        chain_id: Some(1),
        tx_type: 0,
        access_list: AccessList::default(),
        gas_priority_fee: None,
        blob_hashes: Vec::new(),
        max_fee_per_blob_gas: 0,
        authorization_list: Vec::new(),
    };

    IncidentReport {
        transaction_data: (FixedBytes::from([0xaa; 32]), tx_env),
        failures: vec![IncidentData {
            adopter_address: Address::from([0x0a; 20]),
            assertion_id: FixedBytes::from([0x0b; 32]),
            assertion_fn: FixedBytes::from([0x0c; 4]),
            revert_data: Bytes::from(vec![0x08, 0x04]),
        }],
        block_env: BlockEnv {
            number: U256::from(42),
            beneficiary: Address::from([0x0d; 20]),
            timestamp: U256::from(1_700_000_111u64),
            gas_limit: 30_000_000,
            basefee: 7,
            difficulty: U256::from(123u64),
            prevrandao: None,
            blob_excess_gas_and_price: None,
        },
        incident_timestamp: 1_700_000_000,
        prev_txs: Vec::new(),
    }
}

fn build_fee_market_incident_report() -> IncidentReport {
    let max_fee_per_gas = u128_from_hex("0x2b6d453");
    let max_priority_fee_per_gas = u128_from_hex("0x1");

    let tx_env = TxEnv {
        caller: address_from_hex("0x3B7F2cA306882D240634e01a3Bf71BD04C194C23"),
        gas_limit: u64_from_hex("0x6fb0"),
        gas_price: max_fee_per_gas,
        kind: TxKind::Call(address_from_hex("0xdaA6eB4557F1AABBDEbBfb9A08BA0211C1316B2e")),
        value: U256::ZERO,
        data: Bytes::from(
            hex::decode(
                "f2fde38b00000000000000000000000080bec4d66a4fe4adb836b5c43389c349f8fa2c0b",
            )
            .expect("data decode"),
        ),
        nonce: u64_from_hex("0x2"),
        chain_id: Some(2_151_908),
        tx_type: 2,
        access_list: AccessList::default(),
        gas_priority_fee: Some(max_priority_fee_per_gas),
        blob_hashes: Vec::new(),
        max_fee_per_blob_gas: 0,
        authorization_list: Vec::new(),
    };

    let prev_tx_env = TxEnv {
        caller: address_from_hex("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
        gas_limit: u64_from_hex("0x5208"),
        gas_price: max_fee_per_gas,
        kind: TxKind::Call(address_from_hex("0x80BEc4d66a4fE4aDb836b5c43389c349f8Fa2C0B")),
        value: U256::from(u128_from_hex("0xde0b6b3a7640000")),
        data: Bytes::new(),
        nonce: u64_from_hex("0x0"),
        chain_id: Some(2_151_908),
        tx_type: 2,
        access_list: AccessList::default(),
        gas_priority_fee: Some(max_priority_fee_per_gas),
        blob_hashes: Vec::new(),
        max_fee_per_blob_gas: 0,
        authorization_list: Vec::new(),
    };

    IncidentReport {
        transaction_data: (
            fixed_bytes_from_hex(
                "0x1c7fb0f1317eb71fea5034ee7e721bc22e72b3cd9b032e8249d2d20c1a0744b5",
            ),
            tx_env,
        ),
        failures: vec![IncidentData {
            adopter_address: address_from_hex("0xdaA6eB4557F1AABBDEbBfb9A08BA0211C1316B2e"),
            assertion_id: fixed_bytes_from_hex(
                "0x2765138331bd91acf3ac6e1129dd3703028cea020db1b8984ae2b4eecc06353e",
            ),
            assertion_fn: FixedBytes::from([0x00; 4]),
            revert_data: Bytes::new(),
        }],
        block_env: BlockEnv {
            number: U256::from(31),
            beneficiary: Address::ZERO,
            timestamp: U256::from(1_767_993_930u64),
            gas_limit: 30_000_000,
            basefee: 19_922_148,
            difficulty: U256::from(0u64),
            prevrandao: Some(b256_from_hex(
                "0xcf7afacf5468e3fbcd7f97aec67a454af149067001c6e3f613e422167270d4ca",
            )),
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,
                blob_gasprice: 1,
            }),
        },
        incident_timestamp: 1_767_993_930,
        prev_txs: vec![(
            fixed_bytes_from_hex(
                "0xf2cfeba992c08d180209dc245b74e83a2936097b7714084a3bbd4c01d416c87e",
            ),
            prev_tx_env,
        )],
    }
}

fn expected_incident_body() -> serde_json::Value {
    json!({
        "failures": [
            {
                "assertion_adopter_address": hex_bytes(&[0x0a; 20]),
                "assertion_id": hex_bytes(&[0x0b; 32]),
                "assertion_fn_selector": hex_bytes(&[0x0c; 4]),
                "revert_reason": hex_bytes(&[0x08, 0x04])
            }
        ],
        "incident_timestamp": format_timestamp(1_700_000_000),
        "block_number": 42,
        "previous_block_number": 41,
        "transaction_data": {
            "transaction_hash": hex_bytes(&[0xaa; 32]),
            "chain_id": 1,
            "nonce": "0x7",
            "gas_limit": "0x5208",
            "to_address": hex_bytes(&[0x02; 20]),
            "from_address": hex_bytes(&[0x01; 20]),
            "value": "0x5",
            "type": 0.0,
            "data": hex_bytes(&[0xde, 0xad, 0xbe, 0xef]),
            "gas_price": "0x64"
        },
        "block_env": {
            "number": "42",
            "beneficiary": hex_bytes(&[0x0d; 20]),
            "timestamp": "1700000111",
            "gas_limit": "30000000",
            "basefee": "7",
            "difficulty": "123"
        }
    })
}

fn expected_fee_market_incident_body() -> serde_json::Value {
    json!({
        "failures": [
            {
                "assertion_adopter_address": normalize_hex("0xdaA6eB4557F1AABBDEbBfb9A08BA0211C1316B2e"),
                "assertion_id": normalize_hex("0x2765138331bd91acf3ac6e1129dd3703028cea020db1b8984ae2b4eecc06353e"),
                "assertion_fn_selector": "0x00000000"
            }
        ],
        "incident_timestamp": format_timestamp(1_767_993_930),
        "block_number": 31,
        "previous_block_number": 30,
        "transaction_data": {
            "type": 2.0,
            "transaction_hash": normalize_hex("0x1c7fb0f1317eb71fea5034ee7e721bc22e72b3cd9b032e8249d2d20c1a0744b5"),
            "chain_id": 2_151_908,
            "nonce": "0x2",
            "gas_limit": "0x6fb0",
            "to_address": normalize_hex("0xdaA6eB4557F1AABBDEbBfb9A08BA0211C1316B2e"),
            "from_address": normalize_hex("0x3B7F2cA306882D240634e01a3Bf71BD04C194C23"),
            "value": "0x0",
            "data": normalize_hex("0xf2fde38b00000000000000000000000080bec4d66a4fe4adb836b5c43389c349f8fa2c0b"),
            "max_fee_per_gas": "0x2b6d453",
            "max_priority_fee_per_gas": "0x1"
        },
        "previous_transactions": [
            {
                "type": 2.0,
                "transaction_hash": normalize_hex("0xf2cfeba992c08d180209dc245b74e83a2936097b7714084a3bbd4c01d416c87e"),
                "chain_id": 2_151_908,
                "nonce": "0x0",
                "gas_limit": "0x5208",
                "to_address": normalize_hex("0x80BEc4d66a4fE4aDb836b5c43389c349f8Fa2C0B"),
                "from_address": normalize_hex("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
                "value": "0xde0b6b3a7640000",
                "data": "0x",
                "max_fee_per_gas": "0x2b6d453",
                "max_priority_fee_per_gas": "0x1"
            }
        ],
        "block_env": {
            "number": "31",
            "beneficiary": "0x0000000000000000000000000000000000000000",
            "timestamp": "1767993930",
            "gas_limit": "30000000",
            "basefee": "19922148",
            "difficulty": "0",
            "prevrandao": normalize_hex("0xcf7afacf5468e3fbcd7f97aec67a454af149067001c6e3f613e422167270d4ca"),
            "blob_excess_gas_and_price": {
                "excess_blob_gas": "0",
                "blob_gasprice": "1"
            }
        }
    })
}

#[test]
fn incident_body_matches_openapi_example_format() {
    let report = build_fee_market_incident_report();
    let body = super::payload::build_incident_body(&report).expect("build incident body");
    let body_value = serde_json::to_value(body).expect("serialize body");
    assert_eq!(body_value, expected_fee_market_incident_body());
}

fn incident_response_body(message: &str, tracking_id: &str) -> serde_json::Value {
    json!({
        "message": message,
        "timestamp": format_timestamp(1_700_000_000),
        "tracking_id": tracking_id,
    })
}

#[test]
fn incident_body_includes_previous_transactions_as_transaction_data() {
    let mut report = build_incident_report();
    let mut prev_tx = report.transaction_data.1.clone();
    prev_tx.nonce = 6;
    prev_tx.chain_id = Some(1);

    let prev_hash = B256::from([0xbb; 32]);
    report.prev_txs = vec![(prev_hash, prev_tx.clone())];

    let body = super::payload::build_incident_body(&report).expect("build incident body");
    let body_value = serde_json::to_value(body).expect("serialize body");
    let previous_transactions = body_value
        .get("previous_transactions")
        .and_then(Value::as_array)
        .expect("previous_transactions");
    assert_eq!(previous_transactions.len(), 1);

    let prev_tx_value = previous_transactions[0]
        .as_object()
        .expect("previous transaction object");
    assert!(
        prev_tx_value.contains_key("type"),
        "previous transaction missing type discriminator"
    );
    assert!(
        tx_data_matches(prev_tx_value, &prev_hash, &prev_tx),
        "previous transaction did not match expected transaction data payload"
    );
}

#[test]
fn incident_body_rejects_previous_transactions_without_chain_id() {
    let mut report = build_incident_report();
    let mut prev_tx = report.transaction_data.1.clone();
    prev_tx.chain_id = None;

    let prev_hash = B256::from([0xcc; 32]);
    report.prev_txs = vec![(prev_hash, prev_tx)];

    let err = super::payload::build_incident_body(&report)
        .expect_err("expected incident body build to fail");
    assert!(
        matches!(err, TransactionObserverError::PublishFailed { .. }),
        "unexpected error when previous transaction is missing chain_id"
    );
}

#[test]
fn observer_consumes_and_clears_on_success() {
    let server = MockServer::start();
    let expected_body = expected_incident_body();

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/api/v1/enforcer/incidents")
            .json_body(expected_body.clone());
        then.status(202)
            .header("content-type", "application/json")
            .json_body(incident_response_body("queued", "tracking-success"));
    });

    let tempdir = TempDir::new().expect("tempdir");
    let (tx, rx) = flume::unbounded();
    let tx_keepalive = tx.clone();
    let config = TransactionObserverConfig {
        poll_interval: Duration::from_millis(1),
        endpoint_rps_max: 10,
        endpoint: server.url("/api/v1/enforcer/incidents"),
        auth_token: "test-token".to_string(),
        db_path: tempdir.path().to_string_lossy().to_string(),
    };
    let mut observer = TransactionObserver::new(config, rx).expect("observer");

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_sender = Arc::clone(&shutdown);
    let sender = std::thread::spawn(move || {
        tx.send(build_incident_report()).expect("send report");
        std::thread::sleep(Duration::from_millis(200));
        shutdown_sender.store(true, Ordering::Relaxed);
    });

    observer
        .run_blocking(&Arc::clone(&shutdown))
        .expect("observer run");
    drop(tx_keepalive);
    sender.join().expect("sender join");

    mock.assert();
    let remaining = observer.db.load_batch(10).expect("load batch");
    assert!(
        remaining.is_empty(),
        "incident should be removed after a successful publish"
    );
}

#[test]
fn observer_retries_after_failed_publish() {
    let expected_body = expected_incident_body();
    let tempdir = TempDir::new().expect("tempdir");

    let server_fail = MockServer::start();
    let fail_mock = server_fail.mock(|when, then| {
        when.method(POST).path("/api/v1/enforcer/incidents");
        then.status(500)
            .header("content-type", "application/json")
            .json_body(incident_response_body("error", "tracking-fail"));
    });

    {
        let (_tx, rx) = flume::unbounded();
        let config = TransactionObserverConfig {
            poll_interval: Duration::from_millis(1),
            endpoint_rps_max: 10,
            endpoint: server_fail.url("/api/v1/enforcer/incidents"),
            auth_token: "test-token".to_string(),
            db_path: tempdir.path().to_string_lossy().to_string(),
        };
        let mut observer = TransactionObserver::new(config, rx).expect("observer");
        observer
            .store_incident(&build_incident_report())
            .expect("store incident");
        observer
            .publish_invalidations()
            .expect("publish failed incident");

        fail_mock.assert();
        let remaining = observer.db.load_batch(10).expect("load batch");
        assert_eq!(
            remaining.len(),
            1,
            "incident should remain after a failed publish"
        );
    }

    let server_success = MockServer::start();
    let success_mock = server_success.mock(|when, then| {
        when.method(POST)
            .path("/api/v1/enforcer/incidents")
            .json_body(expected_body.clone());
        then.status(202)
            .header("content-type", "application/json")
            .json_body(incident_response_body("queued", "tracking-success"));
    });

    let (_tx, rx) = flume::unbounded();
    let config = TransactionObserverConfig {
        poll_interval: Duration::from_millis(1),
        endpoint_rps_max: 10,
        endpoint: server_success.url("/api/v1/enforcer/incidents"),
        auth_token: "test-token".to_string(),
        db_path: tempdir.path().to_string_lossy().to_string(),
    };
    let mut observer = TransactionObserver::new(config, rx).expect("observer");
    observer.publish_invalidations().expect("publish retry");

    success_mock.assert();
    let remaining = observer.db.load_batch(10).expect("load batch");
    assert!(
        remaining.is_empty(),
        "incident should be removed after retry succeeds"
    );
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_posts_invalidating_transaction_from_local_instance() {
    let server = MockServer::start();
    let (tx_hash_pass, tx_hash_fail, tx_pass, tx_fail) = build_test_tx_pair();
    let (body_tx, body_rx) = flume::unbounded();
    let body_tx_handle = body_tx.clone();

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/api/v1/enforcer/incidents")
            .is_true(move |req| body_tx_handle.send(req.body_string()).is_ok());
        then.status(202)
            .header("content-type", "application/json")
            .json_body(incident_response_body("queued", "tracking-success"));
    });
    let tempdir = TempDir::new().expect("tempdir");
    let (incident_tx, incident_rx) = flume::unbounded();
    let config = TransactionObserverConfig {
        poll_interval: Duration::from_millis(10),
        endpoint_rps_max: 10,
        endpoint: server.url("/api/v1/enforcer/incidents"),
        auth_token: "test-token".to_string(),
        db_path: tempdir.path().to_string_lossy().to_string(),
    };
    let observer = TransactionObserver::new(config, incident_rx).expect("observer");
    let shutdown = Arc::new(AtomicBool::new(false));
    let (observer_handle, observer_exit) = observer
        .spawn(Arc::clone(&shutdown))
        .expect("spawn observer");

    let mut instance = LocalInstanceMockDriver::new_with_incident_sender(incident_tx)
        .await
        .expect("local instance");

    instance
        .send_block_with_txs(vec![
            (tx_hash_pass, tx_pass.clone()),
            (tx_hash_fail, tx_fail.clone()),
        ])
        .await
        .expect("send block with txs");

    let tx_execution_id_pass = TxExecutionId::new(
        instance.block_number,
        instance.iteration_id,
        tx_hash_pass,
        0,
    );
    let tx_execution_id_fail = TxExecutionId::new(
        instance.block_number,
        instance.iteration_id,
        tx_hash_fail,
        1,
    );
    tokio::time::timeout(
        Duration::from_secs(8),
        instance.wait_for_transaction_processed(&tx_execution_id_pass),
    )
    .await
    .expect("timeout waiting for pass tx")
    .expect("wait for pass tx");
    tokio::time::timeout(
        Duration::from_secs(8),
        instance.wait_for_transaction_processed(&tx_execution_id_fail),
    )
    .await
    .expect("timeout waiting for fail tx")
    .expect("wait for fail tx");
    let pass_invalid = instance
        .is_transaction_invalid(&tx_execution_id_pass)
        .await
        .expect("invalid check pass");
    let fail_invalid = instance
        .is_transaction_invalid(&tx_execution_id_fail)
        .await
        .expect("invalid check fail");
    let invalid_txs = collect_invalid_txs(
        pass_invalid,
        fail_invalid,
        tx_hash_pass,
        tx_pass.clone(),
        tx_hash_fail,
        tx_fail.clone(),
    );
    assert!(
        !invalid_txs.is_empty(),
        "Expected at least one invalidating transaction"
    );
    wait_for_mock_calls(&mock, Duration::from_secs(5)).await;
    assert!(mock.calls() > 0, "timed out waiting for incident publish");
    mock.assert();
    let bodies: Vec<String> = body_rx.try_iter().collect();
    let matches_invalid = bodies.iter().any(|body| {
        let incident: Value = serde_json::from_str(body).expect("incident json");
        let tx_data = incident
            .get("transaction_data")
            .and_then(Value::as_object)
            .expect("transaction_data");
        invalid_txs
            .iter()
            .any(|(tx_hash, tx_env)| tx_data_matches(tx_data, tx_hash, tx_env))
    });
    assert!(
        matches_invalid,
        "incident payload did not match invalidating transaction"
    );

    shutdown.store(true, Ordering::Relaxed);
    let observer_result = tokio::time::timeout(Duration::from_secs(10), observer_exit)
        .await
        .expect("timeout waiting for observer exit")
        .expect("observer exit");
    observer_handle
        .join()
        .expect("observer join")
        .expect("observer error");
    observer_result.expect("observer error");
}
