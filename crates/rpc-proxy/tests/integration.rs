use alloy_consensus::{
    SignableTransaction,
    TxEip1559,
};
use alloy_primitives::{
    Signature,
    TxKind,
    U256,
    address,
    bytes,
    hex,
};
use alloy_rlp::Encodable;
use rpc_proxy::{
    ProxyConfig,
    fingerprint::{
        AssertionInfo,
        CacheConfig,
        Fingerprint,
        FingerprintCache,
    },
    sidecar::NoopSidecarTransport,
};
use serde_json::json;
use std::sync::Arc;
use url::Url;
use wiremock::{
    Mock,
    MockServer,
    ResponseTemplate,
    matchers::{
        method,
        path,
    },
};

/// Test that transactions are correctly forwarded to the upstream sequencer
#[tokio::test]
async fn test_forward_to_upstream() {
    // Start a mock upstream sequencer
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0xdeadbeef"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse(&mock_server.uri()).unwrap(),
        sidecar_endpoint: None,
        cache: CacheConfig::default(),
        dry_run: false,
    };

    // Test forwarding by directly calling the internal state
    // (HTTP serving is TODO, so we test the forwarding logic directly)
    let state =
        rpc_proxy::server::ProxyState::new(config, Arc::new(NoopSidecarTransport::default()));

    let tx = create_test_tx(
        address!("1111111111111111111111111111111111111111"),
        U256::from(1000),
        bytes!("a9059cbb"),
        100_000,
    );
    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    let raw_hex = format!("0x{}", hex::encode(&encoded));

    let params = vec![raw_hex];
    let result = state.handle_send_raw_transaction(params).await;

    assert!(result.is_ok(), "Forward should succeed: {:?}", result);
}

/// Test that denied fingerprints are rejected
#[tokio::test]
async fn test_denied_fingerprint_rejection() {
    let cache = FingerprintCache::new(CacheConfig::default());

    let tx = create_test_tx(
        address!("2222222222222222222222222222222222222222"),
        U256::ZERO,
        bytes!("12345678"),
        100_000,
    );

    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    let fingerprint = Fingerprint::from_signed_tx(&encoded).unwrap();

    // Mark fingerprint as denied
    let assertion = AssertionInfo {
        assertion_id: alloy_primitives::B256::ZERO,
        assertion_version: 1,
    };
    cache.observe(&fingerprint);
    cache.record_failure(&fingerprint, assertion);

    // Try to observe again - should be rejected
    match cache.observe(&fingerprint) {
        rpc_proxy::fingerprint::CacheDecision::Reject(_) => {
            // Success - fingerprint is denied
        }
        other => panic!("Expected Reject, got {:?}", other),
    }
}

/// Test that pending fingerprints time out
#[tokio::test]
async fn test_pending_timeout() {
    use std::time::Duration;

    let config = CacheConfig {
        max_denied_entries: 1000,
        denied_ttl_secs: 60,
        pending_timeout_secs: 1, // 1 second timeout for testing
    };
    let cache = FingerprintCache::new(config);

    let tx = create_test_tx(
        address!("3333333333333333333333333333333333333333"),
        U256::ZERO,
        bytes!("87654321"),
        100_000,
    );

    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    let fingerprint = Fingerprint::from_signed_tx(&encoded).unwrap();

    // Mark as pending
    cache.observe(&fingerprint);

    // Should be pending
    match cache.observe(&fingerprint) {
        rpc_proxy::fingerprint::CacheDecision::AwaitVerdict => {
            // Success
        }
        other => panic!("Expected AwaitVerdict, got {:?}", other),
    }

    // Wait for timeout
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Sweep stale entries
    cache.sweep_stale_pending(Duration::from_secs(1));

    // Should now be able to forward again
    match cache.observe(&fingerprint) {
        rpc_proxy::fingerprint::CacheDecision::Forward => {
            // Success - entry was cleaned up
        }
        other => panic!("Expected Forward after timeout, got {:?}", other),
    }
}

/// Test dry-run mode logs but doesn't reject
#[tokio::test]
async fn test_dry_run_mode() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0xabcdef"
        })))
        .mount(&mock_server)
        .await;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse(&mock_server.uri()).unwrap(),
        sidecar_endpoint: None,
        cache: CacheConfig::default(),
        dry_run: true, // Enable dry-run
    };

    let state =
        rpc_proxy::server::ProxyState::new(config, Arc::new(NoopSidecarTransport::default()));

    let cache = state.cache.clone();

    let tx = create_test_tx(
        address!("4444444444444444444444444444444444444444"),
        U256::ZERO,
        bytes!("aaaaaaaa"),
        100_000,
    );

    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    let fingerprint = Fingerprint::from_signed_tx(&encoded).unwrap();

    // Mark as denied
    let assertion = AssertionInfo {
        assertion_id: alloy_primitives::B256::ZERO,
        assertion_version: 1,
    };
    cache.observe(&fingerprint);
    cache.record_failure(&fingerprint, assertion);

    // In dry-run mode, even denied fingerprints should forward
    let raw_hex = format!("0x{}", hex::encode(&encoded));
    let params = vec![raw_hex];
    let result = state.handle_send_raw_transaction(params).await;

    assert!(
        result.is_ok(),
        "Dry-run should forward even denied transactions: {:?}",
        result
    );
}

fn create_test_tx(
    to: alloy_primitives::Address,
    value: U256,
    calldata: alloy_primitives::Bytes,
    gas_limit: u64,
) -> alloy_consensus::TxEnvelope {
    let tx = TxEip1559 {
        to: TxKind::Call(to),
        value,
        input: calldata,
        gas_limit,
        max_fee_per_gas: 20_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        chain_id: 1,
        nonce: 0,
        access_list: Default::default(),
    };
    let sig = Signature::test_signature();
    alloy_consensus::TxEnvelope::Eip1559(tx.into_signed(sig))
}
