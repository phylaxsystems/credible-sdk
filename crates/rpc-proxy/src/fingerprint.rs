use std::{
    sync::Arc,
    time::Instant,
};

use alloy_consensus::{
    TxEnvelope,
    transaction::Transaction,
};
use alloy_primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use alloy_rlp::Decodable;
use dashmap::DashMap;
use thiserror::Error;

/// Normalized fingerprint for a transaction submission.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Fingerprint {
    pub hash: B256,
    pub target: Address,
    pub selector: [u8; 4],
    pub arg_hash: [u8; 16],
    pub value_bucket: u64,
    pub gas_bucket: u32,
}

impl Fingerprint {
    /// Derive a fingerprint from a signed raw transaction (RLP encoded).
    pub fn from_signed_tx(raw_tx: &[u8]) -> Result<Self, FingerprintError> {
        let mut buf = raw_tx;
        let envelope = TxEnvelope::decode(&mut buf)?;
        Self::from_envelope(&envelope)
    }

    pub fn from_envelope(envelope: &TxEnvelope) -> Result<Self, FingerprintError> {
        let target = envelope.to().ok_or(FingerprintError::ContractCreation)?;
        let input = envelope.input();
        let selector = selector_bytes(input);
        let arg_hash = argument_hash(input);
        let value = envelope.value();
        let value_bucket = bucket_value(&value);
        let gas_bucket = bucket_gas(envelope.gas_limit());
        let hash = digest(&target, &selector, &arg_hash, value_bucket, gas_bucket);

        Ok(Self {
            hash,
            target,
            selector,
            arg_hash,
            value_bucket,
            gas_bucket,
        })
    }
}

fn selector_bytes(input: &Bytes) -> [u8; 4] {
    let mut selector = [0u8; 4];
    let bytes = input.as_ref();
    if !bytes.is_empty() {
        let len = bytes.len().min(4);
        selector[..len].copy_from_slice(&bytes[..len]);
    }
    selector
}

fn argument_hash(input: &Bytes) -> [u8; 16] {
    let bytes = input.as_ref();
    if bytes.len() <= 4 {
        return [0u8; 16];
    }
    let hash = keccak256(&bytes[4..]);
    let mut truncated = [0u8; 16];
    truncated.copy_from_slice(&hash.as_slice()[..16]);
    truncated
}

fn bucket_value(value: &U256) -> u64 {
    if value.is_zero() {
        return 0;
    }
    let bytes = value.to_be_bytes::<32>();
    let leading = bytes.iter().take_while(|b| **b == 0).count() as u64;
    32 - leading
}

fn bucket_gas(gas_limit: u64) -> u32 {
    const BUCKET_SIZE: u64 = 50_000;
    (gas_limit / BUCKET_SIZE) as u32
}

fn digest(
    target: &Address,
    selector: &[u8; 4],
    arg_hash: &[u8; 16],
    value_bucket: u64,
    gas_bucket: u32,
) -> B256 {
    let mut buf = Vec::with_capacity(20 + 4 + 16 + 8 + 4);
    buf.extend_from_slice(target.as_slice());
    buf.extend_from_slice(selector);
    buf.extend_from_slice(arg_hash);
    buf.extend_from_slice(&value_bucket.to_be_bytes());
    buf.extend_from_slice(&gas_bucket.to_be_bytes());
    keccak256(buf)
}

/// Tracks fingerprints currently under evaluation to enforce the
/// "one outstanding transaction per fingerprint" rule.
#[derive(Clone, Debug, Default)]
pub struct FingerprintCache {
    entries: Arc<DashMap<B256, CacheEntry>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    state: EntryState,
    inserted_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    Pending,
    Denied,
}

pub enum CacheDecision {
    /// First time we observe this fingerprint; forward it immediately.
    Forward,
    /// Fingerprint currently pending validation; duplicate should wait.
    AwaitVerdict,
    /// Fingerprint previously denied; reject immediately.
    Reject,
}

impl FingerprintCache {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }

    pub fn observe(&self, fingerprint: &Fingerprint) -> CacheDecision {
        use dashmap::mapref::entry::Entry;

        match self.entries.entry(fingerprint.hash) {
            Entry::Vacant(entry) => {
                entry.insert(CacheEntry {
                    state: EntryState::Pending,
                    inserted_at: Instant::now(),
                });
                CacheDecision::Forward
            }
            Entry::Occupied(entry) => {
                match entry.value().state {
                    EntryState::Pending => CacheDecision::AwaitVerdict,
                    EntryState::Denied => CacheDecision::Reject,
                }
            }
        }
    }

    pub fn record_failure(&self, fingerprint: &Fingerprint) {
        self.entries.insert(
            fingerprint.hash,
            CacheEntry {
                state: EntryState::Denied,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn release(&self, fingerprint: &Fingerprint) {
        self.entries.remove(&fingerprint.hash);
    }
}

#[derive(Debug, Error)]
pub enum FingerprintError {
    #[error("transactions without a call target are not fingerprinted")]
    ContractCreation,
    #[error("failed to decode transaction: {0}")]
    Decode(#[from] alloy_rlp::Error),
}
