use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{address, bytes, Address, Bytes, TxKind, B256, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rpc_proxy::{
    error::Result,
    fingerprint::{AssertionInfo, CacheConfig, Fingerprint, FingerprintCache},
    sidecar::{InvalidationEvent, InvalidationStream, ShouldForwardVerdict, SidecarTransport},
};
use std::{sync::Arc, time::Duration};
use futures::stream;

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

/// Mock transport that provides controlled invalidation events
struct MockTransport;

impl MockTransport {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl SidecarTransport for MockTransport {
    async fn subscribe_invalidations(&self) -> Result<InvalidationStream> {
        // Return empty stream for benchmarking purposes
        let stream = stream::iter(std::iter::empty::<std::result::Result<InvalidationEvent, rpc_proxy::error::ProxyError>>());
        Ok(Box::pin(stream))
    }

    async fn should_forward(&self, _fingerprint: &Fingerprint) -> Result<ShouldForwardVerdict> {
        Ok(ShouldForwardVerdict::Unknown)
    }
}

fn bench_cache_invalidation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let signer = PrivateKeySigner::from_slice(&[1u8; 32]).unwrap();

    let mut group = c.benchmark_group("cache_invalidation");
    group.throughput(Throughput::Elements(1));

    // Benchmark fingerprint invalidation
    group.bench_function("invalidate_single_fingerprint", |b| {
        b.to_async(&rt).iter(|| async {
            let cache = FingerprintCache::new(CacheConfig::default());

            // Create and deny a fingerprint
            let raw_tx = create_signed_tx(
                &signer,
                address!("1111111111111111111111111111111111111111"),
                U256::ZERO,
                bytes!("aaaaaaaa"),
                0,
            );
            let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
            cache.observe(&fp);

            let assertion_info = AssertionInfo {
                assertion_id: B256::ZERO,
                assertion_version: 1,
            };
            cache.record_failure(&fp, assertion_info);

            // Benchmark invalidation
            let new_version = 2;

            black_box(cache.invalidate_assertion(&B256::ZERO, new_version));
        })
    });

    // Benchmark bulk invalidation
    for count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("invalidate_bulk", count),
            &count,
            |b, &count| {
                b.to_async(&rt).iter(|| async {
                    let cache = FingerprintCache::new(CacheConfig::default());

                    // Create and deny multiple fingerprints
                    for i in 0..count {
                        let raw_tx = create_signed_tx(
                            &signer,
                            address!("1111111111111111111111111111111111111111"),
                            U256::ZERO,
                            bytes!("aaaaaaaa"),
                            i,
                        );
                        let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
                        cache.observe(&fp);

                        let assertion_info = AssertionInfo {
                            assertion_id: B256::ZERO,
                            assertion_version: 1,
                        };
                        cache.record_failure(&fp, assertion_info);
                    }

                    // Benchmark invalidation
                    let new_version = 2;

                    black_box(cache.invalidate_assertion(&B256::ZERO, new_version));
                })
            },
        );
    }

    group.finish();
}

fn bench_grpc_event_processing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let signer = PrivateKeySigner::from_slice(&[2u8; 32]).unwrap();

    let mut group = c.benchmark_group("grpc_events");
    group.throughput(Throughput::Elements(1));

    // Benchmark processing invalidation events from gRPC
    group.bench_function("process_invalidation_event", |b| {
        b.to_async(&rt).iter(|| async {
            let cache = Arc::new(FingerprintCache::new(CacheConfig::default()));

            // Create and deny a fingerprint
            let raw_tx = create_signed_tx(
                &signer,
                address!("2222222222222222222222222222222222222222"),
                U256::ZERO,
                bytes!("bbbbbbbb"),
                0,
            );
            let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
            cache.observe(&fp);

            let assertion_info = AssertionInfo {
                assertion_id: B256::from([1u8; 32]),
                assertion_version: 1,
            };
            cache.record_failure(&fp, assertion_info);

            // Create and process invalidation event
            let assertion_info = AssertionInfo {
                assertion_id: B256::from([1u8; 32]),
                assertion_version: 2,
            };

            // Process the invalidation (simulating what the listener does)
            black_box(cache.invalidate_assertion(
                &assertion_info.assertion_id,
                assertion_info.assertion_version,
            ));
        })
    });

    group.finish();
}

fn bench_concurrent_cache_access(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let signer = PrivateKeySigner::from_slice(&[3u8; 32]).unwrap();

    let mut group = c.benchmark_group("concurrent_access");
    group.throughput(Throughput::Elements(1));

    // Benchmark concurrent cache operations
    for thread_count in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_observe", thread_count),
            &thread_count,
            |b, &thread_count| {
                b.to_async(&rt).iter(|| async {
                    let cache = Arc::new(FingerprintCache::new(CacheConfig::default()));
                    let mut handles = vec![];

                    for i in 0..thread_count {
                        let cache = cache.clone();
                        let signer = signer.clone();

                        let handle = tokio::spawn(async move {
                            let raw_tx = create_signed_tx(
                                &signer,
                                address!("3333333333333333333333333333333333333333"),
                                U256::ZERO,
                                bytes!("cccccccc"),
                                i,
                            );
                            let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
                            cache.observe(&fp)
                        });

                        handles.push(handle);
                    }

                    for handle in handles {
                        black_box(handle.await.unwrap());
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_assertion_cooldown_trigger(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let signer = PrivateKeySigner::from_slice(&[4u8; 32]).unwrap();

    let mut group = c.benchmark_group("assertion_cooldown");
    group.throughput(Throughput::Elements(1));

    // Benchmark assertion cooldown activation
    group.bench_function("cooldown_activation", |b| {
        b.to_async(&rt).iter(|| async {
            let config = CacheConfig {
                assertion_cooldown_threshold: 10,
                assertion_cooldown_enabled: true,
                ..Default::default()
            };
            let cache = FingerprintCache::new(config);

            let assertion_id = B256::from([5u8; 32]);
            let assertion_version = 1;

            // Create 10 distinct fingerprints to trigger cooldown
            for i in 0..10 {
                let raw_tx = create_signed_tx(
                    &signer,
                    address!("4444444444444444444444444444444444444444"),
                    U256::ZERO,
                    bytes!("dddddddd"),
                    i,
                );
                let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
                cache.observe(&fp);
                cache.record_failure(
                    &fp,
                    AssertionInfo {
                        assertion_id,
                        assertion_version,
                    },
                );
            }

            // Benchmark the check that happens on cooldown (11th fingerprint)
            let raw_tx = create_signed_tx(
                &signer,
                address!("4444444444444444444444444444444444444444"),
                U256::ZERO,
                bytes!("dddddddd"),
                100,
            );
            let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();

            black_box(cache.observe(&fp));
        })
    });

    group.finish();
}

fn bench_pending_timeout_sweep(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let signer = PrivateKeySigner::from_slice(&[5u8; 32]).unwrap();

    let mut group = c.benchmark_group("pending_sweep");
    group.throughput(Throughput::Elements(1));

    // Benchmark pending entry cleanup
    for count in [100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("sweep_pending_entries", count),
            &count,
            |b, &count| {
                b.to_async(&rt).iter(|| async {
                    let config = CacheConfig {
                        pending_timeout_secs: 0, // Immediate timeout
                        ..Default::default()
                    };
                    let cache = FingerprintCache::new(config);

                    // Create pending entries
                    for i in 0..count {
                        let raw_tx = create_signed_tx(
                            &signer,
                            address!("5555555555555555555555555555555555555555"),
                            U256::ZERO,
                            bytes!("eeeeeeee"),
                            i,
                        );
                        let fp = Fingerprint::from_signed_tx(&raw_tx).unwrap();
                        cache.observe(&fp);
                    }

                    // Benchmark the sweep operation (0 duration means immediate timeout)
                    black_box(cache.sweep_stale_pending(Duration::from_secs(0)));
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_invalidation,
    bench_grpc_event_processing,
    bench_concurrent_cache_access,
    bench_assertion_cooldown_trigger,
    bench_pending_timeout_sweep,
);
criterion_main!(benches);
