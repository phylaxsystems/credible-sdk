use super::{
    IncidentData,
    IncidentReport,
    TransactionObserver,
    TransactionObserverConfig,
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
        Bytes,
        FixedBytes,
        TxKind,
        U256,
    },
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Duration,
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
