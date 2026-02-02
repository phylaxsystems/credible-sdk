use crate::db::SidecarDb;
use alloy_primitives::B256;
use growable_bloom_filter::GrowableBloom;
use metrics::counter;
use moka::sync::Cache;
use parking_lot::Mutex;
use reth_db_api::{
    Database,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use reth_libmdbx::WriteFlags;
use std::sync::Arc;
use tracing::info;

/// Inner state for `ContentHashCache`, wrapped in Arc for cheap cloning.
struct ContentHashCacheInner {
    moka: Cache<B256, u64>,
    bloom: Mutex<GrowableBloom>,
    db: Option<Arc<SidecarDb>>,
    enabled: bool,
}

/// Three-layer cache for transaction content hashes.
///
/// 1. **Moka** – bounded in-memory LRU. Fastest path.
/// 2. **Bloom filter** – fast negative when moka misses. Avoids MDBX reads.
/// 3. **MDBX** – persistent store for known hashes; read on bloom positive.
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
            .field("db", &self.inner.db.is_some())
            .finish_non_exhaustive()
    }
}

impl ContentHashCache {
    /// Create a new cache, rebuilding bloom and moka from MDBX on disk.
    pub fn new(
        db: Arc<SidecarDb>,
        moka_capacity: u64,
        bloom_initial_capacity: usize,
    ) -> Result<Self, String> {
        let moka = Cache::new(moka_capacity);
        let mut bloom = GrowableBloom::new(0.01, bloom_initial_capacity);

        // Scan MDBX to rebuild in-memory state
        let tx = db.env().tx().map_err(|e| e.to_string())?;
        let mut cursor = tx
            .inner
            .cursor_with_dbi(db.content_hashes_dbi())
            .map_err(|e| e.to_string())?;

        let mut count = 0u64;
        let mut next = cursor
            .first::<Vec<u8>, Vec<u8>>()
            .map_err(|e| e.to_string())?;
        while let Some((key, value)) = next {
            if key.len() == 32 && value.len() == 8 {
                let hash = B256::from_slice(&key);
                let block =
                    u64::from_be_bytes(value.as_slice().try_into().map_err(|e| format!("{e}"))?);
                moka.insert(hash, block);
                bloom.insert(hash);
                count += 1;
            }
            next = cursor
                .next::<Vec<u8>, Vec<u8>>()
                .map_err(|e| e.to_string())?;
        }

        if count > 0 {
            info!(restored = count, "ContentHashCache rebuilt from MDBX");
        }

        Ok(Self {
            inner: Arc::new(ContentHashCacheInner {
                moka,
                bloom: Mutex::new(bloom),
                db: Some(db),
                enabled: true,
            }),
        })
    }

    /// Create a disabled (no-op) cache.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(ContentHashCacheInner {
                moka: Cache::new(0),
                bloom: Mutex::new(GrowableBloom::new(0.5, 1)),
                db: None,
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

        // Layer 2: bloom miss → definitely unknown
        let bloom_contains = {
            let bloom = self.inner.bloom.lock();
            bloom.contains(hash)
        };

        if !bloom_contains {
            return false;
        }

        // Layer 3: bloom positive → check MDBX to distinguish true positive from false positive
        let Some(db) = &self.inner.db else {
            counter!("sidecar_dedup_content_hash_db_unavailable_total").increment(1);
            return false;
        };

        let Ok(tx) = db.env().tx() else {
            counter!("sidecar_dedup_content_hash_db_read_errors_total").increment(1);
            return false;
        };

        let Ok(found) = tx.inner.get(db.content_hashes_dbi(), hash.as_slice()) else {
            counter!("sidecar_dedup_content_hash_db_read_errors_total").increment(1);
            return false;
        };
        let found: Option<Vec<u8>> = found;

        if let Some(found) = found {
            // True positive — known
            // Value is informational (block number of invalidation).
            let block = found
                .as_slice()
                .try_into()
                .map(u64::from_be_bytes)
                .unwrap_or(0);
            self.inner.moka.insert(hash, block);
            counter!("sidecar_dedup_content_hash_hits_total").increment(1);
            return true;
        }

        // Bloom false positive — treat as unknown
        counter!("sidecar_dedup_bloom_false_positives_total").increment(1);
        false
    }

    /// Record a hash as invalidating, inserting into all layers.
    pub fn record_invalidating(&self, hash: B256, block: u64) {
        if !self.inner.enabled {
            return;
        }
        counter!("sidecar_dedup_content_hash_inserts_total").increment(1);
        self.inner.moka.insert(hash, block);
        {
            let mut bloom = self.inner.bloom.lock();
            bloom.insert(hash);
        }
        // Write-through to MDBX
        if let Some(db) = &self.inner.db {
            let Ok(tx) = db.env().tx_mut() else {
                counter!("sidecar_dedup_content_hash_db_write_errors_total").increment(1);
                return;
            };

            if tx
                .inner
                .put(
                    db.content_hashes_dbi(),
                    hash.as_slice(),
                    block.to_be_bytes(),
                    WriteFlags::empty(),
                )
                .is_err()
            {
                counter!("sidecar_dedup_content_hash_db_write_errors_total").increment(1);
                return;
            }

            if tx.commit().is_err() {
                counter!("sidecar_dedup_content_hash_db_write_errors_total").increment(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SidecarDb;
    use tempfile::TempDir;

    fn open_test_db() -> (TempDir, Arc<SidecarDb>) {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(SidecarDb::open(&dir.path().to_string_lossy()).unwrap());
        (dir, db)
    }

    #[test]
    fn unknown_hash_returns_false() {
        let (_dir, db) = open_test_db();
        let cache = ContentHashCache::new(db, 1000, 1000).unwrap();
        let hash = B256::from([0xaa; 32]);
        assert!(!cache.contains(hash));
    }

    #[test]
    fn record_then_contains_returns_true() {
        let (_dir, db) = open_test_db();
        let cache = ContentHashCache::new(db, 1000, 1000).unwrap();
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
    fn survives_moka_eviction_via_mdbx() {
        let (_dir, db) = open_test_db();
        // Moka capacity of 1 — the first entry will be evicted when second is inserted
        let cache = ContentHashCache::new(Arc::clone(&db), 1, 1000).unwrap();

        let hash_a = B256::from([0x01; 32]);
        let hash_b = B256::from([0x02; 32]);

        cache.record_invalidating(hash_a, 10);
        cache.record_invalidating(hash_b, 11);

        // Force moka eviction
        cache.inner.moka.run_pending_tasks();

        // hash_a should still be found via bloom + MDBX
        assert!(cache.contains(hash_a));
    }

    #[test]
    fn rebuild_from_mdbx() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_string_lossy().to_string();

        let hash = B256::from([0xdd; 32]);

        // Phase 1: insert
        {
            let db = Arc::new(SidecarDb::open(&path).unwrap());
            let cache = ContentHashCache::new(db, 1000, 1000).unwrap();
            cache.record_invalidating(hash, 42);
            assert!(cache.contains(hash));
        }

        // Phase 2: reopen — should find it
        {
            let db = Arc::new(SidecarDb::open(&path).unwrap());
            let cache = ContentHashCache::new(db, 1000, 1000).unwrap();
            assert!(cache.contains(hash));
        }
    }

    #[test]
    fn thread_safety() {
        let (_dir, db) = open_test_db();
        let cache = ContentHashCache::new(db, 10_000, 10_000).unwrap();

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
