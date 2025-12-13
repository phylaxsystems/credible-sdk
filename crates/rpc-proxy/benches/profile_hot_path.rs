// Standalone binary for profiling the hot path with samply
// Run with: samply record target/release/profile_hot_path

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{address, bytes, Address, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use rpc_proxy::{
    backpressure::{BackpressureConfig, OriginBackpressure, OriginMetadata},
    fingerprint::{CacheConfig, Fingerprint, FingerprintCache},
};
use std::sync::Arc;

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

fn main() {
    let signer = PrivateKeySigner::from_slice(&[5u8; 32]).unwrap();
    let cache = Arc::new(FingerprintCache::new(CacheConfig::default()));
    let backpressure = Arc::new(OriginBackpressure::new(BackpressureConfig::default()));

    use moka::sync::Cache;
    use std::time::Duration;
    let sender_cache: Cache<alloy_primitives::B256, Option<Address>> = Cache::builder()
        .max_capacity(100_000)
        .time_to_live(Duration::from_secs(300))
        .build();

    println!("Running hot path profiling for 10 seconds with 100k iterations...");

    let start = std::time::Instant::now();
    let iterations = 100_000;

    for nonce in 0..iterations {
        // Complete hot path
        let raw_tx = create_signed_tx(
            &signer,
            address!("6666666666666666666666666666666666666666"),
            U256::from(100),
            bytes!("cccccccc"),
            nonce,
        );

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
        let _throttled = backpressure.check(&origin);

        // 5. Cache observe
        let _decision = cache.observe(&fingerprint);
    }

    let elapsed = start.elapsed();
    println!(
        "Completed {} iterations in {:?} ({:.2} µs/iter, {:.0} req/s)",
        iterations,
        elapsed,
        elapsed.as_micros() as f64 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );
}
