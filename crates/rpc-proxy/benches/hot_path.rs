use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{address, bytes, Address, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rpc_proxy::{
    backpressure::{BackpressureConfig, OriginBackpressure, OriginMetadata},
    fingerprint::{CacheConfig, Fingerprint, FingerprintCache},
};

fn create_signed_tx(
    signer: &PrivateKeySigner,
    to: Address,
    value: U256,
    calldata: Bytes,
    nonce: u64,
) -> Vec<u8> {
    let tx = TxEip1559 {
        to: TxKind::Call(to),
        value,
        input: calldata,
        gas_limit: 100_000,
        max_fee_per_gas: 20_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        chain_id: 1,
        nonce,
        access_list: Default::default(),
    };
    let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    let envelope = TxEnvelope::Eip1559(tx.into_signed(signature));
    let mut encoded = Vec::new();
    envelope.encode(&mut encoded);
    encoded
}

fn bench_fingerprint_from_signed_tx(c: &mut Criterion) {
    let signer = PrivateKeySigner::from_slice(&[1u8; 32]).unwrap();
    let raw_tx = create_signed_tx(
        &signer,
        address!("1111111111111111111111111111111111111111"),
        U256::from(1000),
        bytes!("a9059cbb"),
        0,
    );

    let mut group = c.benchmark_group("fingerprint");
    group.throughput(Throughput::Elements(1));

    group.bench_function("from_signed_tx", |b| {
        b.iter(|| {
            let fp = Fingerprint::from_signed_tx(black_box(&raw_tx)).unwrap();
            black_box(fp);
        })
    });

    group.finish();
}

fn bench_sender_recovery(c: &mut Criterion) {
    use alloy_consensus::transaction::SignerRecoverable;
    use rpc_proxy::fingerprint::decode_envelope;

    let signer = PrivateKeySigner::from_slice(&[2u8; 32]).unwrap();
    let raw_tx = create_signed_tx(
        &signer,
        address!("2222222222222222222222222222222222222222"),
        U256::ZERO,
        bytes!("12345678"),
        0,
    );

    let envelope = decode_envelope(&raw_tx).unwrap();

    let mut group = c.benchmark_group("sender_recovery");
    group.throughput(Throughput::Elements(1));

    group.bench_function("ecdsa_recovery", |b| {
        b.iter(|| {
            let sender = envelope.recover_signer().unwrap();
            black_box(sender);
        })
    });

    // Benchmark with cache simulation
    use moka::sync::Cache;
    use std::time::Duration;

    let cache: Cache<alloy_primitives::B256, Option<Address>> = Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(300))
        .build();

    let tx_hash = *envelope.tx_hash();

    // Prime the cache
    cache.insert(tx_hash, envelope.recover_signer().ok());

    group.bench_function("cached_lookup", |b| {
        b.iter(|| {
            let sender = cache.get(&tx_hash);
            black_box(sender);
        })
    });

    group.finish();
}

fn bench_backpressure_check(c: &mut Criterion) {
    let signer = PrivateKeySigner::from_slice(&[3u8; 32]).unwrap();
    let config = BackpressureConfig::default();
    let backpressure = OriginBackpressure::new(config);

    let raw_tx = create_signed_tx(
        &signer,
        address!("3333333333333333333333333333333333333333"),
        U256::ZERO,
        bytes!("87654321"),
        0,
    );

    let envelope = rpc_proxy::fingerprint::decode_envelope(&raw_tx).unwrap();
    let origin = OriginMetadata::from_envelope(&envelope);

    let mut group = c.benchmark_group("backpressure");
    group.throughput(Throughput::Elements(1));

    group.bench_function("check_no_throttle", |b| {
        b.iter(|| {
            let result = backpressure.check(black_box(&origin));
            black_box(result);
        })
    });

    // Benchmark with throttled origin
    backpressure.record_failure(&origin);
    backpressure.record_failure(&origin);
    backpressure.record_failure(&origin);

    group.bench_function("check_throttled", |b| {
        b.iter(|| {
            let result = backpressure.check(black_box(&origin));
            black_box(result);
        })
    });

    group.finish();
}

fn bench_cache_operations(c: &mut Criterion) {
    let cache = FingerprintCache::new(CacheConfig::default());
    let signer = PrivateKeySigner::from_slice(&[4u8; 32]).unwrap();

    let mut group = c.benchmark_group("cache");
    group.throughput(Throughput::Elements(1));

    // Benchmark cache.observe() for new fingerprint
    group.bench_function("observe_new", |b| {
        let mut nonce = 0u64;
        b.iter(|| {
            let raw_tx = create_signed_tx(
                &signer,
                address!("4444444444444444444444444444444444444444"),
                U256::ZERO,
                bytes!("aaaaaaaa"),
                nonce,
            );
            nonce += 1;
            let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
            let decision = cache.observe(black_box(&fp));
            black_box(decision);
        })
    });

    // Benchmark cache.observe() for denied fingerprint
    let raw_tx_denied = create_signed_tx(
        &signer,
        address!("5555555555555555555555555555555555555555"),
        U256::ZERO,
        bytes!("bbbbbbbb"),
        999,
    );
    let fp_denied = Fingerprint::from_signed_tx(&raw_tx_denied).unwrap();
    cache.observe(&fp_denied);
    cache.record_failure(
        &fp_denied,
        rpc_proxy::fingerprint::AssertionInfo {
            assertion_id: alloy_primitives::B256::ZERO,
            assertion_version: 1,
        },
    );

    group.bench_function("observe_denied", |b| {
        b.iter(|| {
            let decision = cache.observe(black_box(&fp_denied));
            black_box(decision);
        })
    });

    group.finish();
}

fn bench_full_pipeline(c: &mut Criterion) {
    // Benchmark the complete hot path:
    // decode -> fingerprint -> sender recovery -> backpressure check -> cache observe
    let signer = PrivateKeySigner::from_slice(&[5u8; 32]).unwrap();
    let cache = FingerprintCache::new(CacheConfig::default());
    let backpressure = OriginBackpressure::new(BackpressureConfig::default());

    use moka::sync::Cache;
    use std::time::Duration;
    let sender_cache: Cache<alloy_primitives::B256, Option<Address>> = Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(300))
        .build();

    let mut group = c.benchmark_group("full_pipeline");
    group.throughput(Throughput::Elements(1));

    group.bench_function("complete_hot_path", |b| {
        let mut nonce = 0u64;
        b.iter(|| {
            let raw_tx = create_signed_tx(
                &signer,
                address!("6666666666666666666666666666666666666666"),
                U256::from(100),
                bytes!("cccccccc"),
                nonce,
            );
            nonce += 1;

            // 1. Decode envelope
            let envelope = rpc_proxy::fingerprint::decode_envelope(&raw_tx).unwrap();

            // 2. Create fingerprint
            let fingerprint = Fingerprint::from_envelope(&envelope).unwrap();

            // 3. Sender recovery (with cache)
            let tx_hash = *envelope.tx_hash();
            let sender = sender_cache.get_with(tx_hash, || {
                use alloy_consensus::transaction::SignerRecoverable;
                envelope.recover_signer().ok()
            });

            let origin = OriginMetadata {
                sender,
                ip: None,
            };

            // 4. Backpressure check
            let throttled = backpressure.check(&origin);

            // 5. Cache observe
            let decision = cache.observe(&fingerprint);

            black_box((throttled, decision));
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_fingerprint_from_signed_tx,
    bench_sender_recovery,
    bench_backpressure_check,
    bench_cache_operations,
    bench_full_pipeline,
);
criterion_main!(benches);
