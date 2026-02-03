use alloy_primitives::B256;
use bloomfilter::Bloom;
use metrics::counter;
use moka::sync::Cache;
use parking_lot::Mutex;
use std::sync::Arc;

/// Two fixed-size bloom filters that rotate to bound memory usage.
/// When the active filter reaches capacity, it becomes the backup and the old backup is cleared.
struct RotatingBloom {
    active: Bloom<B256>,
    backup: Bloom<B256>,
    count: usize,
    capacity: usize,
}

impl RotatingBloom {
    fn new(capacity: usize, fp_rate: f64) -> Self {
        Self {
            active: Bloom::new_for_fp_rate(capacity, fp_rate),
            backup: Bloom::new_for_fp_rate(capacity, fp_rate),
            count: 0,
            capacity,
        }
    }

    fn contains(&self, item: &B256) -> bool {
        self.active.check(item) || self.backup.check(item)
    }

    fn insert(&mut self, item: &B256) {
        self.active.set(item);
        self.count += 1;
        if self.count >= self.capacity {
            std::mem::swap(&mut self.active, &mut self.backup);
            self.active.clear();
            self.count = 0;
        }
    }
}

/// Inner state for `ContentHashCache`, wrapped in Arc for cheap cloning.
struct ContentHashCacheInner {
    moka: Cache<B256, u64>,
    bloom: Mutex<RotatingBloom>,
    enabled: bool,
}

/// Two-layer in-memory cache for transaction content hashes.
///
/// 1. **Moka** – bounded in-memory LRU. Fastest path.
/// 2. **Bloom filter** – probabilistic filter. May have false positives.
///
/// When moka misses but bloom contains the hash, we treat it as a hit
/// (accepting the ~1% false positive rate to avoid re-execution).
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
    /// Create a new in-memory cache with the given capacities.
    pub fn new(moka_capacity: u64, bloom_capacity: usize) -> Self {
        let moka = Cache::new(moka_capacity);
        let bloom = RotatingBloom::new(bloom_capacity, 0.01);

        Self {
            inner: Arc::new(ContentHashCacheInner {
                moka,
                bloom: Mutex::new(bloom),
                enabled: true,
            }),
        }
    }

    /// Create a disabled (no-op) cache.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(ContentHashCacheInner {
                moka: Cache::new(0),
                bloom: Mutex::new(RotatingBloom::new(1, 0.5)),
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

        // Layer 1: moka hit → known
        if self.inner.moka.get(&hash).is_some() {
            counter!("sidecar_dedup_content_hash_hits_total").increment(1);
            return true;
        }

        // Layer 2: bloom hit → treat as known (accepting ~1% false positive rate)
        let bloom_contains = {
            let bloom = self.inner.bloom.lock();
            bloom.contains(&hash)
        };

        if bloom_contains {
            counter!("sidecar_dedup_content_hash_bloom_hits_total").increment(1);
            return true;
        }

        false
    }

    /// Record a hash as invalidating, inserting into moka and bloom filter.
    pub fn record_invalidating(&self, hash: B256, block: u64) {
        if !self.inner.enabled {
            return;
        }
        counter!("sidecar_dedup_content_hash_inserts_total").increment(1);
        self.inner.moka.insert(hash, block);
        {
            let mut bloom = self.inner.bloom.lock();
            bloom.insert(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_hash_returns_false() {
        let cache = ContentHashCache::new(1000, 1000);
        let hash = B256::from([0xaa; 32]);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn record_then_contains_returns_true() {
        let cache = ContentHashCache::new(1000, 1000);
        let hash = B256::from([0xbb; 32]);
        cache.record_invalidating(hash, 10);
        assert!(cache.contains(hash));
    }

    #[test]
    fn disabled_cache_always_returns_false() {
        let cache = ContentHashCache::disabled();
        let hash = B256::from([0xcc; 32]);
        assert!(!cache.contains(hash));
        cache.record_invalidating(hash, 10);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn survives_moka_eviction_via_bloom() {
        // Moka capacity of 1 — the first entry will be evicted when second is inserted
        let cache = ContentHashCache::new(1, 1000);

        let hash_a = B256::from([0x01; 32]);
        let hash_b = B256::from([0x02; 32]);

        cache.record_invalidating(hash_a, 10);
        cache.record_invalidating(hash_b, 11);

        // Force moka eviction
        cache.inner.moka.run_pending_tasks();

        // hash_a should still be found via bloom filter
        assert!(cache.contains(hash_a));
    }

    #[test]
    fn thread_safety() {
        let cache = ContentHashCache::new(10_000, 10_000);

        let handles: Vec<_> = (0..4u8)
            .map(|thread_id| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..100u8 {
                        let mut bytes = [0u8; 32];
                        bytes[0] = thread_id;
                        bytes[1] = i;
                        let hash = B256::from(bytes);
                        cache.record_invalidating(hash, u64::from(i));
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
