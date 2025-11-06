#![cfg_attr(not(test), allow(dead_code))]

//! # Redis-backed cache source

pub(crate) mod error;
mod sync_task;
pub(crate) mod utils;

pub use error::RedisCacheError;
use state_store::StateReader;

use self::{
    sync_task::publish_sync_state,
    utils::{
        decode_hex,
        encode_hex,
        encode_storage_key,
        encode_u256_hex,
        parse_b256,
        parse_u64,
        parse_u256,
        to_hex_lower,
    },
};
use crate::{
    Source,
    cache::sources::SourceName,
};
use alloy::primitives::keccak256;
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
    U256,
};
use redis::Commands;
use revm::{
    DatabaseRef,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use std::{
    collections::HashMap,
    fmt::{
        self,
        Debug,
    },
    str::FromStr,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct RedisSource {
    backend: StateReader,
    /// Target block to request from redis.
    target_block: Arc<AtomicU64>,
    /// Records newest block the background poller has seen.
    observed_head: Arc<AtomicU64>,
    /// Oldest block that exists in redis buffer. Used to prevent asking for a block redis doesnt have.
    oldest_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    sync_task: JoinHandle<()>,
}

impl RedisSource {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader) -> Self {
        let target_block = Arc::new(AtomicU64::new(0));
        let observed_head = Arc::new(AtomicU64::new(0));
        let oldest_block = Arc::new(AtomicU64::new(0));
        let sync_status = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let sync_task = sync_task::spawn_sync_task(
            backend.clone(),
            target_block.clone(),
            observed_head.clone(),
            oldest_block.clone(),
            sync_status.clone(),
            cancel_token.clone(),
            DEFAULT_SYNC_INTERVAL,
        );

        Self {
            backend,
            target_block,
            observed_head,
            oldest_block,
            sync_status,
            cancel_token,
            sync_task,
        }
    }

    fn mark_unsynced(&self) {
        publish_sync_state(
            None,
            None,
            &self.target_block,
            &self.observed_head,
            &self.oldest_block,
            &self.sync_status,
        );
    }
}

impl DatabaseRef for RedisSource {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let target_block = self.target_block.load(Ordering::Relaxed);
        let Some(account) = self
            .backend
            .get_account(address.into(), target_block)
            .map_err(Self::Error::RedisAccount)?
        else {
            return Ok(None);
        };
        let account_info = AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: account.code.map(|bytes| Bytecode::new_raw(bytes.into())),
        };
        Ok(Some(account_info))
    }

    /// Looks up the canonical hash for the requested block number.
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let block_hash = self
            .backend
            .get_block_hash(number)
            .map_err(Self::Error::RedisBlockHash)?
            .ok_or(Self::Error::BlockNotFound)?;
        Ok(block_hash)
    }

    /// Loads bytecode previously stored for a code hash.
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let target_block = self.target_block.load(Ordering::Relaxed);
        let bytecode = Bytecode::new_raw(
            self.backend
                .get_code(code_hash, target_block)
                .map_err(Self::Error::RedisCodeByHash)?
                .ok_or(Self::Error::CodeByHashNotFound)?
                .into(),
        );
        Ok(bytecode)
    }

    /// Reads a storage slot for an account, defaulting to zero when missing.
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let slot_hash = keccak256(index.to_be_bytes::<32>());
        let slot = U256::from_be_bytes(slot_hash.into());
        let target_block = self.target_block.load(Ordering::Relaxed);
        let value = self
            .backend
            .get_storage(address.into(), slot, target_block)
            .map_err(Self::Error::RedisStorage)?
            .ok_or(Self::Error::StorageNotFound)?;
        Ok(value)
    }
}

impl Source for RedisSource {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
        if !self.sync_status.load(Ordering::Acquire) {
            return false;
        }
        let redis_observed_head = self.observed_head.load(Ordering::Acquire);
        let redis_oldest_block = self.oldest_block.load(Ordering::Acquire);

        // Find the intersection of the two ranges
        let lower_bound = min_synced_block.max(redis_oldest_block);
        let upper_bound = latest_head.min(redis_observed_head);

        lower_bound <= upper_bound
    }

    /// Updates the current cache status and set the target block
    fn update_cache_status(&self, min_synced_block: u64, latest_head: u64) {
        let redis_observed_head = self.observed_head.load(Ordering::Acquire);
        let redis_oldest_block = self.oldest_block.load(Ordering::Acquire);

        // Find the intersection of the two ranges
        let lower_bound = min_synced_block.max(redis_oldest_block);
        let upper_bound = latest_head.min(redis_observed_head);

        if lower_bound <= upper_bound {
            // Pick the most recent block in the valid range
            let block_number = upper_bound;
            self.target_block.store(block_number, Ordering::Relaxed);
        }
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::Redis
    }
}

impl Drop for RedisSource {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.sync_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    };

    /// Helper struct to test the cache logic without needing a real Redis backend
    struct TestRedisCache {
        target_block: Arc<AtomicU64>,
        observed_head: Arc<AtomicU64>,
        oldest_block: Arc<AtomicU64>,
        sync_status: Arc<AtomicBool>,
    }

    impl TestRedisCache {
        fn new(oldest_block: u64, observed_head: u64, synced: bool) -> Self {
            Self {
                target_block: Arc::new(AtomicU64::new(0)),
                observed_head: Arc::new(AtomicU64::new(observed_head)),
                oldest_block: Arc::new(AtomicU64::new(oldest_block)),
                sync_status: Arc::new(AtomicBool::new(synced)),
            }
        }

        fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
            if !self.sync_status.load(Ordering::Acquire) {
                return false;
            }
            let redis_observed_head = self.observed_head.load(Ordering::Acquire);
            let redis_oldest_block = self.oldest_block.load(Ordering::Acquire);

            let lower_bound = min_synced_block.max(redis_oldest_block);
            let upper_bound = latest_head.min(redis_observed_head);

            lower_bound <= upper_bound
        }

        fn update_cache_status(&self, min_synced_block: u64, latest_head: u64) {
            let redis_observed_head = self.observed_head.load(Ordering::Acquire);
            let redis_oldest_block = self.oldest_block.load(Ordering::Acquire);

            let lower_bound = min_synced_block.max(redis_oldest_block);
            let upper_bound = latest_head.min(redis_observed_head);

            if lower_bound <= upper_bound {
                let block_number = upper_bound;
                self.target_block.store(block_number, Ordering::Relaxed);
            }
        }

        fn get_target_block(&self) -> u64 {
            self.target_block.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn test_perfect_overlap() {
        // Redis: [100, 200], Required: [100, 200]
        let cache = TestRedisCache::new(100, 200, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200);
    }

    #[test]
    fn test_redis_contains_required_range() {
        // Redis: [50, 300], Required: [100, 200]
        let cache = TestRedisCache::new(50, 300, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200); // Should pick latest_head
    }

    #[test]
    fn test_required_contains_redis_range() {
        // Redis: [150, 180], Required: [100, 200]
        let cache = TestRedisCache::new(150, 180, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 180); // Should pick redis_observed_head
    }

    #[test]
    fn test_partial_overlap_lower() {
        // Redis: [50, 150], Required: [100, 200]
        // Overlap: [100, 150]
        let cache = TestRedisCache::new(50, 150, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 150); // Most recent in overlap
    }

    #[test]
    fn test_partial_overlap_upper() {
        // Redis: [150, 250], Required: [100, 200]
        // Overlap: [150, 200]
        let cache = TestRedisCache::new(150, 250, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200); // Most recent in overlap
    }

    #[test]
    fn test_no_overlap_gap() {
        // Redis: [100, 150], Required: [200, 250]
        // No overlap
        let cache = TestRedisCache::new(100, 150, true);

        assert!(!cache.is_synced(200, 250));

        let initial_target = cache.get_target_block();
        cache.update_cache_status(200, 250);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_no_overlap_reversed() {
        // Redis: [200, 250], Required: [100, 150]
        // No overlap
        let cache = TestRedisCache::new(200, 250, true);

        assert!(!cache.is_synced(100, 150));

        let initial_target = cache.get_target_block();
        cache.update_cache_status(100, 150);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_touching_ranges_not_overlapping() {
        // Redis: [100, 150], Required: [151, 200]
        // No overlap (adjacent but not overlapping)
        let cache = TestRedisCache::new(100, 150, true);

        assert!(!cache.is_synced(151, 200));
    }

    #[test]
    fn test_touching_ranges_overlapping_by_one() {
        // Redis: [100, 150], Required: [150, 200]
        // Overlap at block 150
        let cache = TestRedisCache::new(100, 150, true);

        assert!(cache.is_synced(150, 200));
        cache.update_cache_status(150, 200);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_single_block_overlap() {
        // Redis: [100, 200], Required: [150, 150]
        // Single block requirement
        let cache = TestRedisCache::new(100, 200, true);

        assert!(cache.is_synced(150, 150));
        cache.update_cache_status(150, 150);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_sync_status_false() {
        // Redis: [100, 200], Required: [120, 180]
        // Perfect overlap but sync_status is false
        let cache = TestRedisCache::new(100, 200, false);

        assert!(!cache.is_synced(120, 180));
        // update_cache_status should still work even if sync_status is false
        cache.update_cache_status(120, 180);
        assert_eq!(cache.get_target_block(), 180);
    }

    #[test]
    fn test_zero_blocks() {
        // Edge case: block 0
        let cache = TestRedisCache::new(0, 100, true);

        assert!(cache.is_synced(0, 50));
        cache.update_cache_status(0, 50);
        assert_eq!(cache.get_target_block(), 50);
    }

    #[test]
    fn test_large_block_numbers() {
        // Test with realistic Ethereum block numbers
        let cache = TestRedisCache::new(18_000_000, 18_500_000, true);

        assert!(cache.is_synced(18_200_000, 18_400_000));
        cache.update_cache_status(18_200_000, 18_400_000);
        assert_eq!(cache.get_target_block(), 18_400_000);
    }

    #[test]
    fn test_update_multiple_times() {
        // Test that target_block updates correctly on multiple calls
        let cache = TestRedisCache::new(100, 500, true);

        cache.update_cache_status(200, 300);
        assert_eq!(cache.get_target_block(), 300);

        cache.update_cache_status(150, 250);
        assert_eq!(cache.get_target_block(), 250);

        cache.update_cache_status(400, 450);
        assert_eq!(cache.get_target_block(), 450);
    }

    #[test]
    fn test_invalid_required_range() {
        // Edge case: min_synced_block > latest_head (invalid input)
        // The logic should handle this gracefully
        let cache = TestRedisCache::new(100, 200, true);

        assert!(!cache.is_synced(250, 200)); // min > max

        let initial_target = cache.get_target_block();
        cache.update_cache_status(250, 200);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_redis_oldest_equals_observed() {
        // Edge case: Redis has only one block
        let cache = TestRedisCache::new(150, 150, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_required_range_is_single_block() {
        // Required range is a single block that exists in Redis
        let cache = TestRedisCache::new(100, 200, true);

        assert!(cache.is_synced(150, 150));
        cache.update_cache_status(150, 150);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_exactly_at_boundaries() {
        // Test when required range exactly matches boundaries
        let cache = TestRedisCache::new(100, 200, true);

        // Left boundary
        assert!(cache.is_synced(100, 100));
        cache.update_cache_status(100, 100);
        assert_eq!(cache.get_target_block(), 100);

        // Right boundary
        assert!(cache.is_synced(200, 200));
        cache.update_cache_status(200, 200);
        assert_eq!(cache.get_target_block(), 200);
    }
}
