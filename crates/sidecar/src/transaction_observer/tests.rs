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
use httpmock::prelude::*;
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    context_interface::transaction::AccessList,
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
    let expected_nonce = tx_env.nonce.to_string();
    let expected_gas_limit = tx_env.gas_limit.to_string();
    let expected_to_address = match &tx_env.kind {
        TxKind::Call(to) => hex_bytes(to.as_slice()),
        TxKind::Create => String::new(),
    };
    let expected_from_address = hex_bytes(tx_env.caller.as_slice());
    let expected_value = tx_env.value.to_string();
    let expected_gas_price = tx_env.gas_price.to_string();
    let expected_data = if tx_env.data.is_empty() {
        None
    } else {
        Some(hex_bytes(tx_env.data.as_ref()))
    };

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
    if tx_data.get("gas_price").and_then(Value::as_str) != Some(expected_gas_price.as_str()) {
        return false;
    }
    if let Some(expected_data) = expected_data
        && tx_data.get("data").and_then(Value::as_str) != Some(expected_data.as_str())
    {
        return false;
    }

    true
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

fn build_observer(db_path: &TempDir, endpoint: String) -> TransactionObserver {
    let (_tx, rx) = flume::unbounded();
    let config = TransactionObserverConfig {
        poll_interval: Duration::from_millis(1),
        endpoint_rps_max: 10,
        endpoint,
        auth_token: "test-token".to_string(),
        db_path: db_path.path().to_string_lossy().to_string(),
    };
    TransactionObserver::new(config, rx).expect("observer")
}

fn build_report_with_prev_txs(
    block_number: u64,
    tx_hash: [u8; 32],
    prev_hash_bytes: &[u8],
) -> (IncidentReport, Vec<FixedBytes<32>>) {
    let mut report = build_incident_report();
    report.block_env.number = U256::from(block_number);
    report.transaction_data.0 = FixedBytes::from(tx_hash);

    let mut prev_hashes = Vec::new();
    let mut prev_txs = Vec::new();
    for (nonce, byte) in prev_hash_bytes.iter().enumerate() {
        let mut prev_tx = report.transaction_data.1.clone();
        prev_tx.nonce = nonce as u64;
        prev_tx.value = U256::from((nonce + 1) as u64);
        let prev_hash = FixedBytes::from([*byte; 32]);
        prev_hashes.push(prev_hash);
        prev_txs.push((prev_hash, prev_tx));
    }
    report.prev_txs = prev_txs;

    (report, prev_hashes)
}

fn build_report_without_prev_txs(
    block_number: u64,
    tx_hash: [u8; 32],
    nonce: u64,
) -> IncidentReport {
    let mut report = build_incident_report();
    report.block_env.number = U256::from(block_number);
    report.transaction_data.0 = FixedBytes::from(tx_hash);
    report.transaction_data.1.nonce = nonce;
    report.prev_txs = Vec::new();
    report
}

fn collect_incidents_by_hash(bodies: Vec<String>) -> std::collections::HashMap<String, Value> {
    let mut incidents_by_hash = std::collections::HashMap::new();
    for body in bodies {
        let incident: Value = serde_json::from_str(&body).expect("incident json");
        let tx_hash = incident
            .get("transaction_data")
            .and_then(Value::as_object)
            .and_then(|tx| tx.get("transaction_hash"))
            .and_then(Value::as_str)
            .expect("transaction_hash")
            .to_string();
        incidents_by_hash.entry(tx_hash).or_insert(incident);
    }
    incidents_by_hash
}

fn incident_prev_hashes(incident: &Value) -> Vec<String> {
    incident
        .get("previous_transactions")
        .and_then(Value::as_array)
        .map(|prev_txs| {
            prev_txs
                .iter()
                .filter_map(|tx| tx.get("transaction_hash").and_then(Value::as_str))
                .map(std::string::ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
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
        "transaction_data": {
            "transaction_hash": hex_bytes(&[0xaa; 32]),
            "chain_id": 1,
            "nonce": "7",
            "gas_limit": "21000",
            "to_address": hex_bytes(&[0x02; 20]),
            "from_address": hex_bytes(&[0x01; 20]),
            "value": "5",
            "type": 0.0,
            "data": hex_bytes(&[0xde, 0xad, 0xbe, 0xef]),
            "gas_price": "100"
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
    let expected_prev_hash = hex_bytes(prev_hash.as_slice());
    let expected_prev_nonce = prev_tx.nonce.to_string();
    let expected_prev_gas_limit = prev_tx.gas_limit.to_string();
    let expected_prev_to_address = match &prev_tx.kind {
        TxKind::Call(to) => hex_bytes(to.as_slice()),
        TxKind::Create => String::new(),
    };
    let expected_prev_from_address = hex_bytes(prev_tx.caller.as_slice());
    let expected_prev_value = prev_tx.value.to_string();
    let expected_prev_data = if prev_tx.data.is_empty() {
        None
    } else {
        Some(hex_bytes(prev_tx.data.as_ref()))
    };
    assert_eq!(
        prev_tx_value
            .get("transaction_hash")
            .and_then(Value::as_str),
        Some(expected_prev_hash.as_str())
    );
    assert_eq!(
        prev_tx_value.get("nonce").and_then(Value::as_str),
        Some(expected_prev_nonce.as_str())
    );
    assert_eq!(
        prev_tx_value.get("gas_limit").and_then(Value::as_str),
        Some(expected_prev_gas_limit.as_str())
    );
    assert_eq!(
        prev_tx_value.get("to_address").and_then(Value::as_str),
        Some(expected_prev_to_address.as_str())
    );
    assert_eq!(
        prev_tx_value.get("from_address").and_then(Value::as_str),
        Some(expected_prev_from_address.as_str())
    );
    assert_eq!(
        prev_tx_value.get("value").and_then(Value::as_str),
        Some(expected_prev_value.as_str())
    );
    if let Some(expected_prev_data) = expected_prev_data {
        assert_eq!(
            prev_tx_value.get("data").and_then(Value::as_str),
            Some(expected_prev_data.as_str())
        );
    }
    assert!(
        tx_data_matches(prev_tx_value, &prev_hash, &prev_tx),
        "previous transaction did not match expected transaction data payload"
    );
}

#[test]
fn incident_body_builds_transaction_objects() {
    let mut report = build_incident_report();
    let mut prev_tx = report.transaction_data.1.clone();
    prev_tx.nonce = 6;
    prev_tx.chain_id = Some(1);

    let prev_hash = B256::from([0xbb; 32]);
    report.prev_txs = vec![(prev_hash, prev_tx.clone())];

    let body = super::payload::build_incident_body(&report).expect("build incident body");
    let body_value = serde_json::to_value(body).expect("serialize body");

    let tx_value = body_value
        .get("transaction_data")
        .and_then(Value::as_object)
        .expect("transaction_data");
    assert!(
        tx_value.contains_key("type"),
        "transaction_data missing type discriminator"
    );
    let tx_hash = B256::from_slice(report.transaction_data.0.as_slice());
    assert!(
        tx_data_matches(tx_value, &tx_hash, &report.transaction_data.1),
        "transaction_data did not match expected transaction data payload"
    );

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
    let expected_prev_hash = hex_bytes(prev_hash.as_slice());
    let expected_prev_nonce = prev_tx.nonce.to_string();
    let expected_prev_gas_limit = prev_tx.gas_limit.to_string();
    let expected_prev_to_address = match &prev_tx.kind {
        TxKind::Call(to) => hex_bytes(to.as_slice()),
        TxKind::Create => String::new(),
    };
    let expected_prev_from_address = hex_bytes(prev_tx.caller.as_slice());
    let expected_prev_value = prev_tx.value.to_string();
    let expected_prev_data = if prev_tx.data.is_empty() {
        None
    } else {
        Some(hex_bytes(prev_tx.data.as_ref()))
    };
    assert_eq!(
        prev_tx_value
            .get("transaction_hash")
            .and_then(Value::as_str),
        Some(expected_prev_hash.as_str())
    );
    assert_eq!(
        prev_tx_value.get("nonce").and_then(Value::as_str),
        Some(expected_prev_nonce.as_str())
    );
    assert_eq!(
        prev_tx_value.get("gas_limit").and_then(Value::as_str),
        Some(expected_prev_gas_limit.as_str())
    );
    assert_eq!(
        prev_tx_value.get("to_address").and_then(Value::as_str),
        Some(expected_prev_to_address.as_str())
    );
    assert_eq!(
        prev_tx_value.get("from_address").and_then(Value::as_str),
        Some(expected_prev_from_address.as_str())
    );
    assert_eq!(
        prev_tx_value.get("value").and_then(Value::as_str),
        Some(expected_prev_value.as_str())
    );
    if let Some(expected_prev_data) = expected_prev_data {
        assert_eq!(
            prev_tx_value.get("data").and_then(Value::as_str),
            Some(expected_prev_data.as_str())
        );
    }
    assert!(
        tx_data_matches(prev_tx_value, &prev_hash, &prev_tx),
        "previous transaction did not match expected transaction data payload"
    );
}

#[test]
fn observer_persists_and_loads_incident_with_previous_transactions() {
    let tempdir = TempDir::new().expect("tempdir");
    let (tx, rx) = flume::unbounded();
    let config = TransactionObserverConfig {
        poll_interval: Duration::from_millis(100),
        endpoint_rps_max: 0,
        endpoint: String::new(),
        auth_token: String::new(),
        db_path: tempdir.path().to_string_lossy().to_string(),
    };
    let mut observer = TransactionObserver::new(config, rx).expect("observer");
    let tx_keepalive = tx.clone();

    let mut report = build_incident_report();
    let mut prev_tx = report.transaction_data.1.clone();
    prev_tx.nonce = 6;
    prev_tx.chain_id = Some(1);

    let prev_hash = B256::from([0xbb; 32]);
    report.prev_txs = vec![(prev_hash, prev_tx.clone())];

    let expected_tx_hash = B256::from_slice(report.transaction_data.0.as_slice());
    let expected_tx = report.transaction_data.1.clone();
    let expected_prev_hash = prev_hash;
    let expected_prev_tx = prev_tx;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_sender = Arc::clone(&shutdown);
    let sender = std::thread::spawn(move || {
        tx.send(report).expect("send report");
        std::thread::sleep(Duration::from_millis(200));
        shutdown_sender.store(true, Ordering::Relaxed);
    });

    observer
        .run_blocking(&Arc::clone(&shutdown))
        .expect("observer run");
    drop(tx_keepalive);
    sender.join().expect("sender join");

    let mut loaded = observer.db.load_batch(10).expect("load batch");
    assert_eq!(loaded.len(), 1);
    let (_key, loaded_report) = loaded.pop().expect("loaded report");

    let body = super::payload::build_incident_body(&loaded_report).expect("build incident body");
    let body_value = serde_json::to_value(body).expect("serialize body");
    let tx_value = body_value
        .get("transaction_data")
        .and_then(Value::as_object)
        .expect("transaction_data");
    assert!(
        tx_value.contains_key("type"),
        "transaction_data missing type discriminator"
    );
    assert!(
        tx_data_matches(tx_value, &expected_tx_hash, &expected_tx),
        "transaction_data did not match persisted incident"
    );

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
        tx_data_matches(prev_tx_value, &expected_prev_hash, &expected_prev_tx),
        "previous transaction did not match persisted incident"
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
    let first_body = tokio::time::timeout(Duration::from_secs(8), body_rx.recv_async())
        .await
        .expect("timeout waiting for incident publish")
        .expect("incident publish channel closed");
    let mut bodies = vec![first_body];
    bodies.extend(body_rx.try_iter());
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

#[test]
fn previous_transactions_only_include_same_block() {
    let tempdir = TempDir::new().expect("tempdir");

    let server_fail = MockServer::start();
    let fail_mock = server_fail.mock(|when, then| {
        when.method(POST).path("/api/v1/enforcer/incidents");
        then.status(500)
            .header("content-type", "application/json")
            .json_body(incident_response_body("error", "tracking-fail"));
    });

    let mut observer = build_observer(&tempdir, server_fail.url("/api/v1/enforcer/incidents"));
    let (report_block0, prev_hashes) =
        build_report_with_prev_txs(0, [0x10; 32], &[0x01, 0x02, 0x03]);

    observer
        .store_incident(&report_block0)
        .expect("store block 0 incident");
    observer
        .publish_invalidations()
        .expect("publish failed incident");

    fail_mock.assert_calls(1);
    let remaining = observer.db.load_batch(10).expect("load batch");
    assert_eq!(remaining.len(), 1, "incident should remain after failure");
    drop(observer);

    let server_success = MockServer::start();
    let (body_tx, body_rx) = flume::unbounded();
    let body_tx_handle = body_tx.clone();
    let success_mock = server_success.mock(|when, then| {
        when.method(POST)
            .path("/api/v1/enforcer/incidents")
            .is_true(move |req| body_tx_handle.send(req.body_string()).is_ok());
        then.status(202)
            .header("content-type", "application/json")
            .json_body(incident_response_body("queued", "tracking-success"));
    });

    let mut observer = build_observer(&tempdir, server_success.url("/api/v1/enforcer/incidents"));
    let report_block1 = build_report_without_prev_txs(1, [0x20; 32], 9);

    observer
        .store_incident(&report_block1)
        .expect("store block 1 incident");
    observer.publish_invalidations().expect("publish retry");

    success_mock.assert_calls(2);
    let incidents_by_hash = collect_incidents_by_hash(body_rx.try_iter().collect());

    let block0_hash = hex_bytes(report_block0.transaction_data.0.as_slice());
    let block1_hash = hex_bytes(report_block1.transaction_data.0.as_slice());
    let expected_prev_hashes: Vec<String> = prev_hashes
        .iter()
        .map(|hash| hex_bytes(hash.as_slice()))
        .collect();
    assert_eq!(
        incidents_by_hash.len(),
        2,
        "expected block 0 and block 1 incidents"
    );

    let block0_incident = incidents_by_hash
        .get(&block0_hash)
        .expect("block 0 incident body");
    let block1_incident = incidents_by_hash
        .get(&block1_hash)
        .expect("block 1 incident body");

    let block0_prev_hashes = incident_prev_hashes(block0_incident);
    assert_eq!(
        block0_prev_hashes, expected_prev_hashes,
        "block 0 incident should keep previous transactions after retry"
    );

    assert!(
        incident_prev_hashes(block1_incident).is_empty(),
        "block 1 incident should have no previous transactions"
    );
}
