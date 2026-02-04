use alloy_primitives::B256;
use metrics::counter;
use moka::sync::Cache;
use std::{
    sync::Arc,
    time::Duration,
};

/// TTL for cache entries (10 minutes).
const TTL: Duration = Duration::from_secs(600);

/// Inner state for `ContentHashCache`, wrapped in Arc for cheap cloning.
struct ContentHashCacheInner {
    moka: Cache<B256, ()>,
    enabled: bool,
}

/// In-memory cache for transaction content hashes with TTL-based expiration.
///
/// Uses moka with a 10-minute TTL to track known-invalidating transactions.
///
/// This type is cheaply cloneable (Arc-backed) and can be shared across threads.
#[derive(Clone)]
pub struct ContentHashCache {
    inner: Arc<ContentHashCacheInner>,
}

impl std::fmt::Debug for ContentHashCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentHashCache")
            .field("enabled", &self.inner.enabled)
            .field("moka_entry_count", &self.inner.moka.entry_count())
            .finish_non_exhaustive()
    }
}

impl ContentHashCache {
    /// Create a new in-memory cache with the given capacity and a 10-minute TTL.
    pub fn new(capacity: u64) -> Self {
        let moka = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(TTL)
            .build();

        Self {
            inner: Arc::new(ContentHashCacheInner {
                moka,
                enabled: true,
            }),
        }
    }

    /// Create a disabled (no-op) cache.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(ContentHashCacheInner {
                moka: Cache::builder().max_capacity(0).build(),
                enabled: false,
            }),
        }
    }

    /// Returns `true` if `hash` is known (present in the cache), `false` otherwise.
    ///
    /// Intended usage: the transport checks if a transaction is a known-invalidating
    /// repeat, and the engine records hashes only when it observes an invalidation.
    pub fn contains(&self, hash: B256) -> bool {
        if !self.inner.enabled {
            return false;
        }

        if self.inner.moka.get(&hash).is_some() {
            counter!("sidecar_dedup_content_hash_hits_total").increment(1);
            return true;
        }

        false
    }

    /// Record a hash as invalidating, inserting into the cache.
    ///
    /// Only increments the insert counter if this is a new entry (not already in cache).
    pub fn record_invalidating(&self, hash: B256) {
        if !self.inner.enabled {
            return;
        }
        // Only count as new insert if not already present
        if self.inner.moka.get(&hash).is_none() {
            counter!("sidecar_dedup_content_hash_inserts_total").increment(1);
        }
        self.inner.moka.insert(hash, ());
    }

    /// Remove a hash from the cache (used when a previously invalid tx later validates).
    pub fn remove(&self, hash: B256) {
        if !self.inner.enabled {
            return;
        }
        self.inner.moka.invalidate(&hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_hash_returns_false() {
        let cache = ContentHashCache::new(1000);
        let hash = B256::from([0xaa; 32]);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn record_then_contains_returns_true() {
        let cache = ContentHashCache::new(1000);
        let hash = B256::from([0xbb; 32]);
        cache.record_invalidating(hash);
        assert!(cache.contains(hash));
    }

    #[test]
    fn disabled_cache_always_returns_false() {
        let cache = ContentHashCache::disabled();
        let hash = B256::from([0xcc; 32]);
        assert!(!cache.contains(hash));
        cache.record_invalidating(hash);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn remove_clears_hash() {
        let cache = ContentHashCache::new(1000);
        let hash = B256::from([0xab; 32]);
        cache.record_invalidating(hash);
        assert!(cache.contains(hash));

        cache.remove(hash);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn re_recording_same_hash_is_idempotent() {
        let cache = ContentHashCache::new(1000);
        let hash = B256::from([0xdd; 32]);

        // First record
        cache.record_invalidating(hash);
        assert!(cache.contains(hash));

        // Re-record same hash (should not change behavior)
        cache.record_invalidating(hash);
        assert!(cache.contains(hash));

        // Cache still works for other hashes
        let hash2 = B256::from([0xee; 32]);
        assert!(!cache.contains(hash2));
        cache.record_invalidating(hash2);
        assert!(cache.contains(hash2));
    }

    #[test]
    fn thread_safety() {
        let cache = ContentHashCache::new(10_000);

        let handles: Vec<_> = (0..4u8)
            .map(|thread_id| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..100u8 {
                        let mut bytes = [0u8; 32];
                        bytes[0] = thread_id;
                        bytes[1] = i;
                        let hash = B256::from(bytes);
                        cache.record_invalidating(hash);
                        let _ = cache.contains(hash);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
