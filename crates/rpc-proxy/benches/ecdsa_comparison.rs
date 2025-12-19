/// Benchmark comparing ECDSA recovery implementations:
/// - alloy (using k256 - pure Rust)
/// - secp256k1 (using libsecp256k1 - C library wrapper)
///
/// Run with: cargo bench --bench ecdsa_comparison

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{address, bytes, Address, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn create_signed_tx(signer: &PrivateKeySigner, nonce: u64) -> Vec<u8> {
    let tx = TxEip1559 {
        to: TxKind::Call(address!("1111111111111111111111111111111111111111")),
        value: U256::from(1000),
        input: bytes!("a9059cbb"),
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

fn bench_ecdsa_recovery(c: &mut Criterion) {
    let signer = PrivateKeySigner::from_slice(&[1u8; 32]).unwrap();
    let raw_tx = create_signed_tx(&signer, 0);

    let mut group = c.benchmark_group("ecdsa_recovery");
    group.throughput(Throughput::Elements(1));

    // Benchmark 1: Current alloy implementation (k256)
    group.bench_function("alloy_k256", |b| {
        b.iter(|| {
            use alloy_consensus::transaction::SignerRecoverable;
            let envelope = rpc_proxy::fingerprint::decode_envelope(black_box(&raw_tx)).unwrap();
            let sender = envelope.recover_signer().unwrap();
            black_box(sender);
        })
    });

    // Benchmark 2: Direct secp256k1 implementation
    group.bench_function("secp256k1_direct", |b| {
        b.iter(|| {
            // Decode the RLP envelope to get signature and message hash
            let envelope = rpc_proxy::fingerprint::decode_envelope(black_box(&raw_tx)).unwrap();

            // Get the signature hash (message that was signed)
            let msg_hash = match &envelope {
                TxEnvelope::Eip1559(tx) => tx.tx().signature_hash(),
                _ => panic!("unexpected tx type"),
            };

            // Get the signature components
            let signature = envelope.signature();

            // Convert to secp256k1 types
            use secp256k1::{ecdsa::RecoverableSignature, Message, Secp256k1};

            let secp = Secp256k1::verification_only();

            // For EIP-1559, v is a Parity (bool): false = 0, true = 1
            // This maps directly to secp256k1's recovery ID
            let recovery_id = if signature.v() { 1 } else { 0 };

            // Construct recoverable signature: [r || s]
            let mut sig_bytes = [0u8; 64];
            sig_bytes[0..32].copy_from_slice(&signature.r().to_be_bytes::<32>());
            sig_bytes[32..64].copy_from_slice(&signature.s().to_be_bytes::<32>());

            let rec_sig = RecoverableSignature::from_compact(
                &sig_bytes,
                secp256k1::ecdsa::RecoveryId::from_i32(recovery_id).unwrap(),
            )
            .unwrap();

            let message = Message::from_digest(msg_hash.0);
            let pubkey = secp.recover_ecdsa(&message, &rec_sig).unwrap();

            // Convert public key to Ethereum address (keccak256(pubkey)[12..32])
            use alloy_primitives::keccak256;
            let pubkey_bytes = pubkey.serialize_uncompressed();
            let hash = keccak256(&pubkey_bytes[1..]); // Skip the 0x04 prefix
            let address = Address::from_slice(&hash[12..]);

            black_box(address);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_ecdsa_recovery);
criterion_main!(benches);
