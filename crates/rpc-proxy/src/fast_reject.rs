/// Fast-reject mechanisms to avoid expensive operations (ECDSA recovery, cache writes)
/// during adversarial attacks. These checks run BEFORE sender recovery.
///
/// See PERFORMANCE_ANALYSIS.md for rationale and attack scenarios.
///
/// NOTE: We do NOT use IP-based throttling because the proxy sits behind load balancers
/// in production, so all requests appear to come from the same IP. Instead, we rely on:
/// - Tx-hash based recent-failure cache (prevents re-processing same failed tx)
/// - Sender-based backpressure (existing mechanism)
/// - Global concurrency limits (tower middleware)

use std::time::{Duration, Instant};

use alloy_primitives::B256;
use dashmap::DashMap;

/// Tracks recently-failed transaction hashes to avoid re-processing.
///
/// When a transaction fails assertion validation, its tx_hash is added here
/// with a short TTL (e.g., 60s). Subsequent submissions of the same tx_hash
/// can be rejected immediately without expensive ECDSA recovery (~115µs).
///
/// Memory overhead: ~32 bytes per entry (B256 + Instant + overhead)
/// At 100k entries: ~3.2MB
pub struct RecentFailureCache {
    entries: DashMap<B256, Instant>,
    ttl: Duration,
}

impl RecentFailureCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
        }
    }

    /// Record a transaction hash that recently failed assertion validation.
    pub fn record_failure(&self, tx_hash: B256) {
        self.entries.insert(tx_hash, Instant::now());

        // Periodically clean up expired entries (probabilistic cleanup)
        if fastrand::usize(..1000) == 0 {
            self.cleanup();
        }
    }

    /// Check if a transaction hash recently failed.
    /// Returns Some(age) if found and not expired, None otherwise.
    pub fn check(&self, tx_hash: &B256) -> Option<Duration> {
        self.entries.get(tx_hash).and_then(|entry| {
            let age = entry.elapsed();
            if age < self.ttl {
                Some(age)
            } else {
                // Expired, remove lazily
                drop(entry);
                self.entries.remove(tx_hash);
                None
            }
        })
    }

    /// Remove expired entries.
    fn cleanup(&self) {
        self.entries.retain(|_, instant| instant.elapsed() < self.ttl);
    }

    /// Get current size for metrics.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Configuration for fast-reject mechanisms.
#[derive(Clone)]
pub struct FastRejectConfig {
    /// TTL for recent failure cache entries (seconds)
    pub recent_failure_ttl_secs: u64,
    /// Maximum number of recent failures to track
    pub max_recent_failures: usize,
}

impl Default for FastRejectConfig {
    fn default() -> Self {
        Self {
            recent_failure_ttl_secs: 60,
            max_recent_failures: 100_000,
        }
    }
}

/// Combined fast-reject state to avoid expensive operations.
pub struct FastReject {
    pub recent_failures: RecentFailureCache,
}

impl FastReject {
    pub fn new(config: FastRejectConfig) -> Self {
        Self {
            recent_failures: RecentFailureCache::new(
                Duration::from_secs(config.recent_failure_ttl_secs),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recent_failure_cache() {
        let cache = RecentFailureCache::new(Duration::from_millis(100));
        let tx_hash = B256::random();

        // Not in cache initially
        assert!(cache.check(&tx_hash).is_none());

        // Record failure
        cache.record_failure(tx_hash);

        // Now in cache
        assert!(cache.check(&tx_hash).is_some());

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(150));

        // Expired
        assert!(cache.check(&tx_hash).is_none());
    }
}
