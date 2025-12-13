use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
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
use moka::sync::Cache;
use parking_lot::RwLock;
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
        let envelope = decode_envelope(raw_tx)?;
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

    pub fn from_parts(
        hash: B256,
        target: Address,
        selector: [u8; 4],
        arg_hash: [u8; 16],
        value_bucket: u64,
        gas_bucket: u32,
    ) -> Self {
        Self {
            hash,
            target,
            selector,
            arg_hash,
            value_bucket,
            gas_bucket,
        }
    }
}

/// Decode a raw RLP-encoded transaction into an envelope for downstream analysis.
pub fn decode_envelope(raw_tx: &[u8]) -> Result<TxEnvelope, FingerprintError> {
    let mut buf = raw_tx;
    TxEnvelope::decode(&mut buf).map_err(FingerprintError::from)
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

/// Bucket value into power-of-2 ranges for fingerprint normalization.
/// Returns the approximate log2 of the value, scaled to bucket size.
/// Bucket 0: value == 0
/// Bucket 1: 0 < value <= 1e12 (1M gwei)
/// Bucket 2: 1e12 < value <= 1e15 (1K ETH)
/// Bucket 3: 1e15 < value <= 1e18 (1M ETH)
/// And so on in powers of 1000...
fn bucket_value(value: &U256) -> u64 {
    if value.is_zero() {
        return 0;
    }

    // Find the position of the most significant bit
    let bits = value.bit_len();

    // Group into buckets by powers of 2 (roughly every 10 bits = ~1000x)
    // This gives us coarse-grained buckets: tiny, small, medium, large, huge, etc.
    let bucket = (bits / 10) + 1;
    bucket.min(255) as u64
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

/// Metadata about an assertion that caused a fingerprint to be denied.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssertionInfo {
    pub assertion_id: B256,
    pub assertion_version: u64,
}

/// Tracks fingerprints and their validation state with proper TTL and LRU eviction.
#[derive(Clone, Debug)]
pub struct FingerprintCache {
    /// Cache of denied fingerprints with TTL. These are fingerprints that
    /// the sidecar has confirmed as failing assertions.
    denied: Cache<B256, DeniedEntry>,
    /// Set of fingerprints currently pending sidecar validation.
    /// We don't use TTL here because we explicitly manage lifecycle.
    pending: Arc<RwLock<HashSet<B256>>>,
    /// Timestamps for when each fingerprint was marked as pending.
    /// Used to detect and clean up stuck entries.
    pending_timestamps: Arc<RwLock<HashMap<B256, Instant>>>,
    /// Tracks distinct fingerprints that have failed per assertion.
    /// When threshold exceeded, assertion goes into cooldown.
    assertion_failures: Arc<RwLock<HashMap<AssertionInfo, HashSet<B256>>>>,
    /// Assertions currently in cooldown with their expiration time.
    assertion_cooldowns: Arc<RwLock<HashMap<AssertionInfo, Instant>>>,
    /// Last time we allowed a transaction through during assertion cooldown.
    /// Used to enforce "1 per block" trickle rate.
    assertion_last_trickle: Arc<RwLock<HashMap<AssertionInfo, Instant>>>,
    /// Configuration for assertion cooldowns.
    config: CacheConfig,
}

#[derive(Debug, Clone)]
struct DeniedEntry {
    /// Which assertions caused this fingerprint to fail.
    /// Allows selective cache invalidation when assertion versions update.
    assertions: HashSet<AssertionInfo>,
}

#[derive(Debug)]
pub enum CacheDecision {
    /// First time we observe this fingerprint; forward it immediately.
    Forward,
    /// Fingerprint currently pending validation; duplicate should wait.
    AwaitVerdict,
    /// Fingerprint previously denied; reject immediately.
    Reject(HashSet<AssertionInfo>),
}

/// Configuration for fingerprint cache behavior.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheConfig {
    /// Maximum number of denied fingerprints to cache.
    #[serde(default = "default_max_denied_entries")]
    pub max_denied_entries: u64,
    /// Time-to-live for denied entries in seconds (default: ~64 L2 slots at 2s = 128s).
    #[serde(default = "default_denied_ttl_secs")]
    pub denied_ttl_secs: u64,
    /// Timeout for pending fingerprints in seconds (default: 30s).
    /// After this duration, pending entries are automatically cleared.
    #[serde(default = "default_pending_timeout_secs")]
    pub pending_timeout_secs: u64,
    /// Assertion-level cooldown: minimum distinct fingerprints that must fail
    /// for the same assertion before activating a global cooldown.
    #[serde(default = "default_assertion_cooldown_threshold")]
    pub assertion_cooldown_threshold: usize,
    /// Duration of assertion-level cooldown in seconds (default: 300s = 5 minutes).
    #[serde(default = "default_assertion_cooldown_duration_secs")]
    pub assertion_cooldown_duration_secs: u64,
    /// Enable assertion-level cooldowns.
    #[serde(default = "default_assertion_cooldown_enabled")]
    pub assertion_cooldown_enabled: bool,
}

fn default_max_denied_entries() -> u64 {
    10_000
}

fn default_denied_ttl_secs() -> u64 {
    128
}

fn default_pending_timeout_secs() -> u64 {
    30
}

fn default_assertion_cooldown_threshold() -> usize {
    10
}

fn default_assertion_cooldown_duration_secs() -> u64 {
    300
}

fn default_assertion_cooldown_enabled() -> bool {
    true
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_denied_entries: default_max_denied_entries(),
            denied_ttl_secs: default_denied_ttl_secs(),
            pending_timeout_secs: default_pending_timeout_secs(),
            assertion_cooldown_threshold: default_assertion_cooldown_threshold(),
            assertion_cooldown_duration_secs: default_assertion_cooldown_duration_secs(),
            assertion_cooldown_enabled: default_assertion_cooldown_enabled(),
        }
    }
}

impl FingerprintCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            denied: Cache::builder()
                .max_capacity(config.max_denied_entries)
                .time_to_live(Duration::from_secs(config.denied_ttl_secs))
                .build(),
            pending: Arc::new(RwLock::new(HashSet::new())),
            pending_timestamps: Arc::new(RwLock::new(HashMap::new())),
            assertion_failures: Arc::new(RwLock::new(HashMap::new())),
            assertion_cooldowns: Arc::new(RwLock::new(HashMap::new())),
            assertion_last_trickle: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Observe a new transaction fingerprint and determine how to handle it.
    pub fn observe(&self, fingerprint: &Fingerprint) -> CacheDecision {
        // Check if already denied
        if let Some(entry) = self.denied.get(&fingerprint.hash) {
            metrics::counter!("rpc_proxy_fingerprint_reject_total", "reason" => "denied")
                .increment(1);
            return CacheDecision::Reject(entry.assertions.clone());
        }

        // Check if pending validation
        let mut pending = self.pending.write();
        if pending.contains(&fingerprint.hash) {
            metrics::counter!("rpc_proxy_fingerprint_reject_total", "reason" => "pending")
                .increment(1);
            return CacheDecision::AwaitVerdict;
        }

        // New fingerprint - mark as pending and forward
        pending.insert(fingerprint.hash);
        drop(pending); // Release lock before acquiring timestamps lock

        // Record timestamp for pending timeout tracking
        self.pending_timestamps
            .write()
            .insert(fingerprint.hash, Instant::now());

        metrics::counter!("rpc_proxy_fingerprint_forward_total").increment(1);
        CacheDecision::Forward
    }

    /// Record that a fingerprint failed validation by the sidecar.
    /// The fingerprint will remain in the denied cache until TTL expires
    /// or it is explicitly cleared.
    pub fn record_failure(&self, fingerprint: &Fingerprint, assertion: AssertionInfo) {
        // Remove from pending set and timestamps
        self.pending.write().remove(&fingerprint.hash);
        self.pending_timestamps.write().remove(&fingerprint.hash);

        // Add or update in denied cache
        // Moka's get_with computes the value if not present, or returns existing
        let mut assertions = self
            .denied
            .get(&fingerprint.hash)
            .map(|entry| entry.assertions.clone())
            .unwrap_or_default();

        assertions.insert(assertion.clone());

        self.denied
            .insert(fingerprint.hash, DeniedEntry { assertions });

        // Track assertion-level failures for cooldown detection
        if self.config.assertion_cooldown_enabled {
            self.track_assertion_failure(&assertion, &fingerprint.hash);
        }

        metrics::counter!("rpc_proxy_cache_denied_total").increment(1);
    }

    /// Release a fingerprint from pending state (called when validation succeeds).
    pub fn release(&self, fingerprint: &Fingerprint) {
        self.pending.write().remove(&fingerprint.hash);
        self.pending_timestamps.write().remove(&fingerprint.hash);
        metrics::counter!("rpc_proxy_cache_release_total").increment(1);
    }

    /// Clear denied entries associated with a specific assertion version.
    /// This is called when the assertion store indexes a new version.
    pub fn invalidate_assertion(&self, assertion_id: &B256, assertion_version: u64) {
        let target = AssertionInfo {
            assertion_id: *assertion_id,
            assertion_version,
        };

        // Moka doesn't support conditional removal, so we need to iterate
        // and collect keys to invalidate. This is O(n) but only happens
        // on assertion version updates, which are infrequent.
        let keys_to_remove: Vec<B256> = self
            .denied
            .iter()
            .filter(|(_, entry)| entry.assertions.contains(&target))
            .map(|(key, _)| *key)
            .collect();

        for key in keys_to_remove {
            self.denied.invalidate(&key);
        }

        metrics::counter!("rpc_proxy_cache_invalidate_total").increment(1);
    }

    /// Sweep stale pending entries that have exceeded the timeout.
    /// Should be called periodically by a background task.
    /// Returns the hashes that were cleared so callers can drop auxiliary metadata.
    pub fn sweep_stale_pending(&self, timeout: Duration) -> Vec<B256> {
        let now = Instant::now();
        let mut pending = self.pending.write();
        let mut timestamps = self.pending_timestamps.write();

        // Collect fingerprints that have timed out
        let stale: Vec<B256> = timestamps
            .iter()
            .filter_map(|(fp, inserted_at)| {
                if now.duration_since(*inserted_at) > timeout {
                    Some(*fp)
                } else {
                    None
                }
            })
            .collect();

        // Remove stale entries
        for fp in &stale {
            pending.remove(fp);
            timestamps.remove(fp);
        }

        if !stale.is_empty() {
            metrics::counter!("rpc_proxy_pending_timeout_total").increment(stale.len() as u64);
        }

        stale
    }

    /// Track a failure for an assertion and activate cooldown if threshold exceeded.
    fn track_assertion_failure(&self, assertion: &AssertionInfo, fingerprint_hash: &B256) {
        let mut failures = self.assertion_failures.write();
        let fingerprints = failures.entry(assertion.clone()).or_insert_with(HashSet::new);
        fingerprints.insert(*fingerprint_hash);

        let distinct_count = fingerprints.len();
        drop(failures); // Release lock before checking cooldown

        // Check if we've exceeded the threshold and should activate cooldown
        if distinct_count >= self.config.assertion_cooldown_threshold {
            let mut cooldowns = self.assertion_cooldowns.write();
            if !cooldowns.contains_key(assertion) {
                let expires_at =
                    Instant::now() + Duration::from_secs(self.config.assertion_cooldown_duration_secs);
                cooldowns.insert(assertion.clone(), expires_at);

                metrics::counter!(
                    "rpc_proxy_assertion_cooldown_activated_total",
                    "assertion_id" => format!("{:?}", assertion.assertion_id)
                )
                .increment(1);

                tracing::warn!(
                    assertion_id = ?assertion.assertion_id,
                    assertion_version = assertion.assertion_version,
                    distinct_failures = distinct_count,
                    "assertion cooldown activated"
                );
            }
        }
    }

    /// Check if an assertion is currently in cooldown.
    /// Returns true if the assertion should be throttled (cooldown active).
    pub fn is_assertion_in_cooldown(&self, assertion: &AssertionInfo) -> bool {
        self.cleanup_expired_cooldowns();

        let cooldowns = self.assertion_cooldowns.read();
        cooldowns.contains_key(assertion)
    }

    /// Determine if we should allow a "trickle" transaction through during cooldown.
    /// Returns true if enough time has passed since last trickle (approximately 1 per block).
    pub fn should_allow_trickle(&self, assertion: &AssertionInfo) -> bool {
        const BLOCK_TIME_SECS: u64 = 2; // L2 block time

        let mut last_trickle = self.assertion_last_trickle.write();
        let now = Instant::now();

        if let Some(last) = last_trickle.get(assertion) {
            if now.duration_since(*last) >= Duration::from_secs(BLOCK_TIME_SECS) {
                last_trickle.insert(assertion.clone(), now);
                metrics::counter!(
                    "rpc_proxy_assertion_trickle_total",
                    "assertion_id" => format!("{:?}", assertion.assertion_id)
                )
                .increment(1);
                true
            } else {
                false
            }
        } else {
            // First trickle for this assertion
            last_trickle.insert(assertion.clone(), now);
            true
        }
    }

    /// Remove expired cooldowns from tracking.
    fn cleanup_expired_cooldowns(&self) {
        let now = Instant::now();
        let mut cooldowns = self.assertion_cooldowns.write();

        cooldowns.retain(|assertion, expires_at| {
            let keep = *expires_at > now;
            if !keep {
                metrics::counter!(
                    "rpc_proxy_assertion_cooldown_expired_total",
                    "assertion_id" => format!("{:?}", assertion.assertion_id)
                )
                .increment(1);
            }
            keep
        });
    }

    /// Get cache statistics for observability.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            denied_count: self.denied.entry_count(),
            pending_count: self.pending.read().len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub denied_count: u64,
    pub pending_count: usize,
}

#[derive(Debug, Error)]
pub enum FingerprintError {
    #[error("transactions without a call target are not fingerprinted")]
    ContractCreation,
    #[error("failed to decode transaction: {0}")]
    Decode(#[from] alloy_rlp::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{
        SignableTransaction,
        TxEip1559,
    };
    use alloy_primitives::{
        Signature,
        address,
        bytes,
    };

    fn create_test_tx(to: Address, value: U256, calldata: Bytes, gas_limit: u64) -> TxEnvelope {
        let tx = TxEip1559 {
            to: alloy_primitives::TxKind::Call(to),
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
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    #[test]
    fn test_value_bucketing() {
        assert_eq!(
            bucket_value(&U256::ZERO),
            0,
            "Zero value should be bucket 0"
        );

        // Small values - bucketing is logarithmic (bit_len / 10)
        // 1 has 1 bit, so bucket = 1/10 + 1 = 1
        assert_eq!(bucket_value(&U256::from(1)), 1, "1 wei should be bucket 1");

        // 1000 has ~10 bits, so bucket = 10/10 + 1 = 2
        assert_eq!(
            bucket_value(&U256::from(1000)),
            2,
            "1000 wei should be bucket 2"
        );

        // 1 ETH = 1e18 = ~2^60 = 60 bits, bucket = 60/10 + 1 = 7
        let one_eth = U256::from(1_000_000_000_000_000_000u128);
        assert_eq!(bucket_value(&one_eth), 7, "1 ETH should be in bucket 7");

        // 1000 ETH should be in a higher bucket
        let thousand_eth = one_eth * U256::from(1000);
        assert!(
            bucket_value(&thousand_eth) > bucket_value(&one_eth),
            "1000 ETH should be in higher bucket than 1 ETH"
        );

        // Values that differ by small amounts should bucket together
        assert_eq!(
            bucket_value(&U256::from(1_000_000)),
            bucket_value(&U256::from(1_000_001)),
            "Similar values should be in same bucket"
        );

        // But values that differ significantly should be in different buckets
        assert_ne!(
            bucket_value(&U256::from(1_000)),
            bucket_value(&U256::from(1_000_000)),
            "Values differing by 1000x should be in different buckets"
        );
    }

    #[test]
    fn test_gas_bucketing() {
        assert_eq!(bucket_gas(21_000), 0);
        assert_eq!(bucket_gas(50_000), 1);
        assert_eq!(bucket_gas(100_000), 2);
        assert_eq!(bucket_gas(51_000), 1, "51k should round down to bucket 1");
        assert_eq!(bucket_gas(99_999), 1, "99,999 should still be bucket 1");
    }

    #[test]
    fn test_fingerprint_determinism() {
        let to = address!("1111111111111111111111111111111111111111");
        let calldata = bytes!(
            "a9059cbb00000000000000000000000022222222222222222222222222222222222222220000000000000000000000000000000000000000000000000000000000000064"
        );

        let tx1 = create_test_tx(to, U256::from(1000), calldata.clone(), 100_000);
        let tx2 = create_test_tx(to, U256::from(1000), calldata.clone(), 100_000);

        let fp1 = Fingerprint::from_envelope(&tx1).unwrap();
        let fp2 = Fingerprint::from_envelope(&tx2).unwrap();

        assert_eq!(
            fp1.hash, fp2.hash,
            "Identical transactions should produce identical fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_different_calldata() {
        let to = address!("1111111111111111111111111111111111111111");
        let calldata1 =
            bytes!("a9059cbb0000000000000000000000002222222222222222222222222222222222222222");
        let calldata2 =
            bytes!("a9059cbb0000000000000000000000003333333333333333333333333333333333333333");

        let tx1 = create_test_tx(to, U256::ZERO, calldata1, 100_000);
        let tx2 = create_test_tx(to, U256::ZERO, calldata2, 100_000);

        let fp1 = Fingerprint::from_envelope(&tx1).unwrap();
        let fp2 = Fingerprint::from_envelope(&tx2).unwrap();

        assert_ne!(
            fp1.hash, fp2.hash,
            "Different calldata should produce different fingerprints"
        );
        assert_eq!(
            fp1.selector, fp2.selector,
            "Same function selector should match"
        );
    }

    #[test]
    fn test_fingerprint_empty_calldata() {
        let to = address!("1111111111111111111111111111111111111111");
        let tx = create_test_tx(to, U256::from(1000), Bytes::new(), 21_000);
        let fp = Fingerprint::from_envelope(&tx).unwrap();

        assert_eq!(
            fp.selector,
            [0, 0, 0, 0],
            "Empty calldata should have zero selector"
        );
        assert_eq!(fp.arg_hash, [0u8; 16], "Empty args should hash to zero");
    }

    #[test]
    fn test_cache_pending_state() {
        let cache = FingerprintCache::new(CacheConfig::default());
        let to = address!("1111111111111111111111111111111111111111");
        let tx = create_test_tx(to, U256::ZERO, bytes!("12345678"), 100_000);
        let fp = Fingerprint::from_envelope(&tx).unwrap();

        // First observe should forward
        match cache.observe(&fp) {
            CacheDecision::Forward => {}
            _ => panic!("First observe should forward"),
        }

        // Second observe should await verdict (still pending)
        match cache.observe(&fp) {
            CacheDecision::AwaitVerdict => {}
            _ => panic!("Second observe should await verdict"),
        }

        // Release should clear pending
        cache.release(&fp);

        // Third observe should forward again
        match cache.observe(&fp) {
            CacheDecision::Forward => {}
            _ => panic!("After release, should forward again"),
        }
    }

    #[test]
    fn test_cache_denied_state() {
        let cache = FingerprintCache::new(CacheConfig::default());
        let to = address!("1111111111111111111111111111111111111111");
        let tx = create_test_tx(to, U256::ZERO, bytes!("12345678"), 100_000);
        let fp = Fingerprint::from_envelope(&tx).unwrap();

        let assertion = AssertionInfo {
            assertion_id: B256::ZERO,
            assertion_version: 1,
        };

        // First observe should forward
        assert!(matches!(cache.observe(&fp), CacheDecision::Forward));

        // Record failure
        cache.record_failure(&fp, assertion.clone());

        // Now should be rejected
        match cache.observe(&fp) {
            CacheDecision::Reject(assertions) => {
                assert!(assertions.contains(&assertion));
            }
            _ => panic!("Should be rejected after recording failure"),
        }
    }

    #[test]
    fn test_cache_assertion_invalidation() {
        let cache = FingerprintCache::new(CacheConfig::default());
        let to = address!("1111111111111111111111111111111111111111");
        let tx = create_test_tx(to, U256::ZERO, bytes!("12345678"), 100_000);
        let fp = Fingerprint::from_envelope(&tx).unwrap();

        let assertion = AssertionInfo {
            assertion_id: B256::from([1u8; 32]),
            assertion_version: 1,
        };

        // Mark as pending and then denied
        cache.observe(&fp);
        cache.record_failure(&fp, assertion.clone());

        // Verify it's denied
        assert!(matches!(cache.observe(&fp), CacheDecision::Reject(_)));

        // Invalidate the assertion
        cache.invalidate_assertion(&assertion.assertion_id, assertion.assertion_version);

        // Now should forward again (cache was cleared)
        assert!(matches!(cache.observe(&fp), CacheDecision::Forward));
    }
}
