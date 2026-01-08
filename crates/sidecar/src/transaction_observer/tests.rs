use super::{
    IncidentData,
    IncidentReport,
    TransactionObserver,
    TransactionObserverConfig,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_posts_invalidating_transaction_from_local_instance() {
    let server = MockServer::start();
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

    let captured_bodies = Arc::new(Mutex::new(Vec::new()));
    let captured_bodies_handle = Arc::clone(&captured_bodies);

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/api/v1/enforcer/incidents")
            .is_true(move |req| {
                let mut bodies = captured_bodies_handle.lock().expect("lock body capture");
                bodies.push(req.body_string());
                true
            });
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
    let (observer_handle, _observer_exit) = observer
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
    let pass_invalid = instance
        .is_transaction_invalid(&tx_execution_id_pass)
        .await
        .expect("invalid check pass");
    let fail_invalid = instance
        .is_transaction_invalid(&tx_execution_id_fail)
        .await
        .expect("invalid check fail");
    let mut invalid_txs = Vec::new();
    if pass_invalid {
        invalid_txs.push((tx_hash_pass, &tx_pass));
    }
    if fail_invalid {
        invalid_txs.push((tx_hash_fail, &tx_fail));
    }
    assert!(
        !invalid_txs.is_empty(),
        "Expected at least one invalidating transaction"
    );

    let start = Instant::now();
    while mock.calls() == 0 && start.elapsed() < Duration::from_secs(2) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    mock.assert();

    let bodies = captured_bodies.lock().expect("lock body capture");
    let matches_invalid = bodies.iter().any(|body| {
        let incident: Value = serde_json::from_str(body).expect("incident json");
        let tx_data = incident
            .get("transaction_data")
            .and_then(Value::as_object)
            .expect("transaction_data");
        invalid_txs.iter().any(|(tx_hash, tx_env)| {
            let expected_chain_id = match tx_env.chain_id {
                Some(chain_id) => chain_id,
                None => return false,
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

            if tx_data.get("transaction_hash").and_then(Value::as_str)
                != Some(expected_hash.as_str())
            {
                return false;
            }
            if tx_data.get("chain_id").and_then(Value::as_u64) != Some(expected_chain_id) {
                return false;
            }
            if tx_data.get("nonce").and_then(Value::as_str) != Some(expected_nonce.as_str()) {
                return false;
            }
            if tx_data.get("gas_limit").and_then(Value::as_str)
                != Some(expected_gas_limit.as_str())
            {
                return false;
            }
            if tx_data.get("to_address").and_then(Value::as_str)
                != Some(expected_to_address.as_str())
            {
                return false;
            }
            if tx_data.get("from_address").and_then(Value::as_str)
                != Some(expected_from_address.as_str())
            {
                return false;
            }
            if tx_data.get("value").and_then(Value::as_str) != Some(expected_value.as_str()) {
                return false;
            }
            if tx_data.get("gas_price").and_then(Value::as_str)
                != Some(expected_gas_price.as_str())
            {
                return false;
            }
            if let Some(expected_data) = expected_data {
                if tx_data.get("data").and_then(Value::as_str) != Some(expected_data.as_str()) {
                    return false;
                }
            }

            true
        })
    });
    assert!(
        matches_invalid,
        "incident payload did not match invalidating transaction"
    );

    shutdown.store(true, Ordering::Relaxed);
    let _ = observer_handle.join();
}
