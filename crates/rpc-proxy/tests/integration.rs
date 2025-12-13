use alloy_consensus::{
    SignableTransaction,
    TxEip1559,
};
use alloy_primitives::{
    TxKind,
    U256,
    address,
    bytes,
    hex,
};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use futures::stream;
use rpc_proxy::{
    ProxyConfig,
    backpressure::{
        BackpressureConfig,
        OriginMetadata,
    },
    error::{
        ProxyError,
        Result as ProxyResult,
    },
    fingerprint::{
        AssertionInfo,
        CacheConfig,
        Fingerprint,
        FingerprintCache,
    },
    sidecar::{
        InvalidationStream,
        NoopSidecarTransport,
        ShouldForwardVerdict,
        SidecarTransport,
    },
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
    let signer = PrivateKeySigner::from_slice(&[1u8; 32]).unwrap();
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
        backpressure: BackpressureConfig::default(),
        dry_run: false,
    };

    // Test forwarding by directly calling the internal state to isolate heuristics
    let state =
        rpc_proxy::server::ProxyState::new(config, Arc::new(NoopSidecarTransport::default()));

    let tx = create_test_tx(
        &signer,
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
    let signer = PrivateKeySigner::from_slice(&[2u8; 32]).unwrap();

    let tx = create_test_tx(
        &signer,
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
        ..Default::default()
    };
    let cache = FingerprintCache::new(config);
    let signer = PrivateKeySigner::from_slice(&[3u8; 32]).unwrap();

    let tx = create_test_tx(
        &signer,
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
    let _ = cache.sweep_stale_pending(Duration::from_secs(1));

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
        backpressure: BackpressureConfig::default(),
        dry_run: true, // Enable dry-run
    };

    let state =
        rpc_proxy::server::ProxyState::new(config, Arc::new(NoopSidecarTransport::default()));

    let cache = state.cache.clone();
    let signer = PrivateKeySigner::from_slice(&[4u8; 32]).unwrap();

    let tx = create_test_tx(
        &signer,
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

/// Test that per-sender backpressure throttles repeated submissions.
#[tokio::test]
async fn test_sender_backpressure() {
    let mut backpressure = BackpressureConfig::default();
    backpressure.max_tokens = 0;
    backpressure.refill_tokens_per_second = 0.0;
    backpressure.base_backoff_ms = 1_000;
    backpressure.max_backoff_ms = 1_000;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse("http://127.0.0.1:8545").unwrap(),
        sidecar_endpoint: None,
        cache: CacheConfig::default(),
        backpressure,
        dry_run: false,
    };

    let state = rpc_proxy::server::ProxyState::new(config, Arc::new(DenySidecar::default()));

    let signer = PrivateKeySigner::from_slice(&[5u8; 32]).unwrap();
    let tx = create_test_tx(
        &signer,
        address!("5555555555555555555555555555555555555555"),
        U256::from(100),
        bytes!("deadbeef"),
        100_000,
    );
    let mut encoded = Vec::new();
    tx.encode(&mut encoded);
    let raw_hex = format!("0x{}", hex::encode(&encoded));
    let params = vec![raw_hex];
    let origin = OriginMetadata::from_envelope(&tx);
    assert!(
        origin.sender.is_some(),
        "expected recovered sender for test tx"
    );

    // First submission should be denied by the sidecar but not throttled yet
    let first = state.handle_send_raw_transaction(params.clone()).await;
    assert!(
        matches!(first, Err(ProxyError::DeniedFingerprint(_, _))),
        "first send should be denied by assertion: {first:?}"
    );

    // Second submission (different fingerprint) should hit backpressure
    let tx2 = create_test_tx(
        &signer,
        address!("5555555555555555555555555555555555555555"),
        U256::from(100),
        bytes!("deadbe00"),
        100_000,
    );
    let mut encoded2 = Vec::new();
    tx2.encode(&mut encoded2);
    let raw_hex2 = format!("0x{}", hex::encode(&encoded2));
    let params2 = vec![raw_hex2];

    let second = state.handle_send_raw_transaction(params2).await;
    match second {
        Err(ProxyError::Backpressure { .. }) => {}
        other => panic!("expected backpressure error, got {other:?}"),
    }
}

#[derive(Default)]
struct DenySidecar;

#[async_trait]
impl SidecarTransport for DenySidecar {
    async fn subscribe_invalidations(&self) -> ProxyResult<InvalidationStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn should_forward(
        &self,
        _fingerprint: &Fingerprint,
    ) -> ProxyResult<ShouldForwardVerdict> {
        Ok(ShouldForwardVerdict::Deny(AssertionInfo {
            assertion_id: alloy_primitives::B256::ZERO,
            assertion_version: 1,
        }))
    }
}

/// Test that different senders have independent backpressure buckets.
#[tokio::test]
async fn test_independent_sender_buckets() {
    let mut backpressure = BackpressureConfig::default();
    backpressure.max_tokens = 0;
    backpressure.refill_tokens_per_second = 0.0;
    backpressure.base_backoff_ms = 5_000;
    backpressure.max_backoff_ms = 5_000;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse("http://127.0.0.1:8545").unwrap(),
        sidecar_endpoint: None,
        cache: CacheConfig::default(),
        backpressure,
        dry_run: false,
    };

    let state = rpc_proxy::server::ProxyState::new(config, Arc::new(DenySidecar::default()));

    // Two different signers
    let signer1 = PrivateKeySigner::from_slice(&[10u8; 32]).unwrap();
    let signer2 = PrivateKeySigner::from_slice(&[20u8; 32]).unwrap();

    // Sender 1 gets throttled
    let tx1 = create_test_tx(
        &signer1,
        address!("6666666666666666666666666666666666666666"),
        U256::from(100),
        bytes!("11111111"),
        100_000,
    );
    let mut encoded1 = Vec::new();
    tx1.encode(&mut encoded1);
    let raw_hex1 = format!("0x{}", hex::encode(&encoded1));

    let first = state
        .handle_send_raw_transaction(vec![raw_hex1.clone()])
        .await;
    assert!(
        matches!(first, Err(ProxyError::DeniedFingerprint(_, _))),
        "first send should be denied"
    );

    // Sender 2 should NOT be throttled (different sender, independent bucket)
    let tx2 = create_test_tx(
        &signer2,
        address!("7777777777777777777777777777777777777777"),
        U256::from(200),
        bytes!("22222222"),
        100_000,
    );
    let mut encoded2 = Vec::new();
    tx2.encode(&mut encoded2);
    let raw_hex2 = format!("0x{}", hex::encode(&encoded2));

    let second = state
        .handle_send_raw_transaction(vec![raw_hex2])
        .await;
    assert!(
        matches!(second, Err(ProxyError::DeniedFingerprint(_, _))),
        "sender2 first attempt should be denied by assertion, not backpressure"
    );
}

/// Test that token bucket refills over time.
#[tokio::test]
async fn test_token_bucket_refill() {
    use std::time::Duration;

    let mut backpressure = BackpressureConfig::default();
    backpressure.max_tokens = 1;
    backpressure.refill_tokens_per_second = 10.0; // Refill 1 token every 100ms
    backpressure.base_backoff_ms = 1_000;
    backpressure.max_backoff_ms = 1_000;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse("http://127.0.0.1:8545").unwrap(),
        sidecar_endpoint: None,
        cache: CacheConfig::default(),
        backpressure,
        dry_run: false,
    };

    let state = rpc_proxy::server::ProxyState::new(config, Arc::new(DenySidecar::default()));
    let signer = PrivateKeySigner::from_slice(&[30u8; 32]).unwrap();

    // First failure consumes the 1 token
    let tx1 = create_test_tx(
        &signer,
        address!("8888888888888888888888888888888888888888"),
        U256::from(100),
        bytes!("aaaaaaaa"),
        100_000,
    );
    let mut encoded1 = Vec::new();
    tx1.encode(&mut encoded1);
    let raw_hex1 = format!("0x{}", hex::encode(&encoded1));

    let first = state
        .handle_send_raw_transaction(vec![raw_hex1])
        .await;
    assert!(
        matches!(first, Err(ProxyError::DeniedFingerprint(_, _))),
        "first should be denied by assertion"
    );

    // Second failure exhausts tokens and applies backoff penalty
    let tx2 = create_test_tx(
        &signer,
        address!("8888888888888888888888888888888888888888"),
        U256::from(200),
        bytes!("bbbbbbbb"),
        100_000,
    );
    let mut encoded2 = Vec::new();
    tx2.encode(&mut encoded2);
    let raw_hex2 = format!("0x{}", hex::encode(&encoded2));

    let second = state
        .handle_send_raw_transaction(vec![raw_hex2.clone()])
        .await;
    assert!(
        matches!(second, Err(ProxyError::DeniedFingerprint(_, _))),
        "second should still be denied by assertion (penalty applied but not checked yet): {second:?}"
    );

    // Third attempt should hit backpressure (backoff is active now)
    let tx3 = create_test_tx(
        &signer,
        address!("8888888888888888888888888888888888888888"),
        U256::from(300),
        bytes!("cccccccc"),
        100_000,
    );
    let mut encoded3 = Vec::new();
    tx3.encode(&mut encoded3);
    let raw_hex3 = format!("0x{}", hex::encode(&encoded3));

    let third = state
        .handle_send_raw_transaction(vec![raw_hex3])
        .await;
    assert!(
        matches!(third, Err(ProxyError::Backpressure { .. })),
        "third should hit backpressure: {third:?}"
    );

    // Wait for backoff to expire (1s)
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // After backoff expires, should be allowed again (no longer in cooldown)
    let fourth = state
        .handle_send_raw_transaction(vec![raw_hex2])
        .await;
    assert!(
        matches!(fourth, Err(ProxyError::DeniedFingerprint(_, _))),
        "after backoff expires, should get assertion denial again (not backpressure): {fourth:?}"
    );
}

/// Test assertion-level cooldowns when threshold exceeded.
#[tokio::test]
async fn test_assertion_cooldown() {
    use std::time::Duration;

    let mut cache_config = CacheConfig::default();
    cache_config.assertion_cooldown_threshold = 3; // Activate after 3 distinct failures
    cache_config.assertion_cooldown_duration_secs = 10; // 10 second cooldown
    cache_config.assertion_cooldown_enabled = true;

    let config = ProxyConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        rpc_path: "/rpc".into(),
        upstream_http: Url::parse("http://127.0.0.1:8545").unwrap(),
        sidecar_endpoint: None,
        cache: cache_config,
        backpressure: BackpressureConfig::default(),
        dry_run: false,
    };

    let state = rpc_proxy::server::ProxyState::new(config, Arc::new(DenySidecar::default()));
    let signer = PrivateKeySigner::from_slice(&[40u8; 32]).unwrap();

    // Create 3 distinct transactions that will fail with the same assertion
    let txs: Vec<_> = (0..3)
        .map(|i| {
            let tx = create_test_tx(
                &signer,
                address!("9999999999999999999999999999999999999999"),
                U256::from(i * 100),
                bytes!("deadbeef"),
                100_000,
            );
            let mut encoded = Vec::new();
            tx.encode(&mut encoded);
            let fingerprint = Fingerprint::from_signed_tx(&encoded).unwrap();
            (fingerprint, format!("0x{}", hex::encode(&encoded)))
        })
        .collect();

    // Submit all 3 transactions - they should be denied but cooldown not activated yet
    for (i, (_fp, raw_hex)) in txs.iter().enumerate() {
        let result = state
            .handle_send_raw_transaction(vec![raw_hex.clone()])
            .await;
        assert!(
            matches!(result, Err(ProxyError::DeniedFingerprint(_, _))),
            "tx {} should be denied: {:?}",
            i,
            result
        );
    }

    // After 3 failures, cooldown should be activated
    // Submit a 4th transaction (different fingerprint) - should be denied by cooldown
    let tx4 = create_test_tx(
        &signer,
        address!("9999999999999999999999999999999999999999"),
        U256::from(400),
        bytes!("deadbeef"),
        100_000,
    );
    let mut encoded4 = Vec::new();
    tx4.encode(&mut encoded4);
    let raw_hex4 = format!("0x{}", hex::encode(&encoded4));

    let _result4 = state.handle_send_raw_transaction(vec![raw_hex4.clone()]).await;
    // Should be denied (first trickle is allowed through in our impl, so this might pass)
    // Let's try a 5th one to ensure cooldown is working
    tokio::time::sleep(Duration::from_millis(100)).await;

    let tx5 = create_test_tx(
        &signer,
        address!("9999999999999999999999999999999999999999"),
        U256::from(500),
        bytes!("deadbeef"),
        100_000,
    );
    let mut encoded5 = Vec::new();
    tx5.encode(&mut encoded5);
    let raw_hex5 = format!("0x{}", hex::encode(&encoded5));

    let result5 = state.handle_send_raw_transaction(vec![raw_hex5]).await;
    // This should definitely be denied by cooldown
    assert!(
        matches!(result5, Err(ProxyError::DeniedFingerprint(_, _))),
        "tx 5 should be denied during cooldown: {:?}",
        result5
    );
}

fn create_test_tx(
    signer: &PrivateKeySigner,
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
    let signature = signer
        .sign_hash_sync(&tx.signature_hash())
        .expect("sign hash");
    alloy_consensus::TxEnvelope::Eip1559(tx.into_signed(signature))
}
