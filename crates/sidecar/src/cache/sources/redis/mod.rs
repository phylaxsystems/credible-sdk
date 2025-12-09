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
use parking_lot::RwLock;
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
    target_block: Arc<RwLock<U256>>,
    /// Records newest block the background poller has seen.
    observed_head: Arc<RwLock<U256>>,
    /// Oldest block that exists in redis buffer. Used to prevent asking for a block redis doesnt have.
    oldest_block: Arc<RwLock<U256>>,
    sync_status: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    sync_task: JoinHandle<()>,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl RedisSource {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader) -> Self {
        let target_block = Arc::new(RwLock::new(U256::ZERO));
        let observed_head = Arc::new(RwLock::new(U256::ZERO));
        let oldest_block = Arc::new(RwLock::new(U256::ZERO));
        let sync_status = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(U256::ZERO),
            latest_head: RwLock::new(U256::ZERO),
        });
        let sync_task = sync_task::spawn_sync_task(
            cache_status.clone(),
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
            cache_status,
        }
    }

    fn mark_unsynced(&self) {
        publish_sync_state(
            self.cache_status.clone(),
            None,
            None,
            &self.target_block,
            &self.observed_head,
            &self.oldest_block,
            &self.sync_status,
        );
    }

    /// Helper to convert U256 to u64 for backend calls.
    /// Panics if the value overflows u64 (should never happen for block numbers in practice).
    #[inline]
    fn u256_to_u64(value: U256) -> u64 {
        value.try_into().expect("block number overflow u64")
    }

    /// Computes the intersection of two block ranges and returns the target block.
    ///
    /// Given the required range `[min_synced_block, latest_head]` and the Redis
    /// available range `[redis_oldest_block, redis_observed_head]`, this function
    /// returns the most recent block in the intersection (upper bound), or `None`
    /// if the ranges do not overlap.
    #[inline]
    fn calculate_target_block(
        min_synced_block: U256,
        latest_head: U256,
        redis_oldest_block: U256,
        redis_observed_head: U256,
    ) -> Option<U256> {
        let lower_bound = min_synced_block.max(redis_oldest_block);
        let upper_bound = latest_head.min(redis_observed_head);

        if lower_bound <= upper_bound {
            Some(upper_bound)
        } else {
            None
        }
    }

    /// Checks whether two block ranges have any overlap.
    #[inline]
    fn ranges_overlap(
        min_synced_block: U256,
        latest_head: U256,
        redis_oldest_block: U256,
        redis_observed_head: U256,
    ) -> bool {
        let lower_bound = min_synced_block.max(redis_oldest_block);
        let upper_bound = latest_head.min(redis_observed_head);
        lower_bound <= upper_bound
    }
}

impl DatabaseRef for RedisSource {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let Some(account) = self
            .backend
            .get_account(address.into(), target_block_u64)
            .map_err(Self::Error::RedisAccount)?
        else {
            return Ok(None);
        };
        let account_info = AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            // `code_hash` will be used to fetch it from the database, if code needs to be
            // loaded from inside `revm`.
            code: None,
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
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let bytecode = Bytecode::new_raw(
            self.backend
                .get_code(code_hash, target_block_u64)
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
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let value = self
            .backend
            .get_storage(address.into(), slot, target_block_u64)
            .map_err(Self::Error::RedisStorage)?
            .unwrap_or_default();
        Ok(value)
    }
}

impl Source for RedisSource {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, min_synced_block: U256, latest_head: U256) -> bool {
        if !self.sync_status.load(Ordering::Acquire) {
            return false;
        }
        let redis_observed_head = *self.observed_head.read();
        let redis_oldest_block = *self.oldest_block.read();

        Self::ranges_overlap(
            min_synced_block,
            latest_head,
            redis_oldest_block,
            redis_observed_head,
        )
    }

    /// Updates the current cache status and set the target block
    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        let redis_observed_head = *self.observed_head.read();
        let redis_oldest_block = *self.oldest_block.read();

        if let Some(target) = Self::calculate_target_block(
            min_synced_block,
            latest_head,
            redis_oldest_block,
            redis_observed_head,
        ) {
            *self.target_block.write() = target;
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
            Ordering,
        },
    };

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    /// Helper struct to test the cache logic without needing a real Redis backend
    struct TestRedisCache {
        target_block: Arc<RwLock<U256>>,
        observed_head: Arc<RwLock<U256>>,
        oldest_block: Arc<RwLock<U256>>,
        sync_status: Arc<AtomicBool>,
    }

    impl TestRedisCache {
        fn new(oldest_block: u64, observed_head: u64, synced: bool) -> Self {
            Self {
                target_block: Arc::new(RwLock::new(U256::ZERO)),
                observed_head: Arc::new(RwLock::new(U256::from(observed_head))),
                oldest_block: Arc::new(RwLock::new(U256::from(oldest_block))),
                sync_status: Arc::new(AtomicBool::new(synced)),
            }
        }

        fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
            if !self.sync_status.load(Ordering::Acquire) {
                return false;
            }
            let redis_observed_head = *self.observed_head.read();
            let redis_oldest_block = *self.oldest_block.read();

            RedisSource::ranges_overlap(
                U256::from(min_synced_block),
                U256::from(latest_head),
                redis_oldest_block,
                redis_observed_head,
            )
        }

        fn update_cache_status(&self, min_synced_block: u64, latest_head: u64) {
            let redis_observed_head = *self.observed_head.read();
            let redis_oldest_block = *self.oldest_block.read();

            if let Some(target) = RedisSource::calculate_target_block(
                U256::from(min_synced_block),
                U256::from(latest_head),
                redis_oldest_block,
                redis_observed_head,
            ) {
                *self.target_block.write() = target;
            }
        }

        fn get_target_block(&self) -> u64 {
            let target = *self.target_block.read();
            target.try_into().expect("target block overflow u64")
        }

        fn set_observed_head(&self, value: u64) {
            *self.observed_head.write() = U256::from(value);
        }

        fn set_oldest_block(&self, value: u64) {
            *self.oldest_block.write() = U256::from(value);
        }
    }

    #[test]
    fn calculate_perfect_overlap_identical_ranges() {
        // Redis: [100, 200], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(100), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_redis_contains_required_range() {
        // Redis: [50, 300], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(50), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_redis_contains_required_with_same_start() {
        // Redis: [100, 300], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(100), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_redis_contains_required_with_same_end() {
        // Redis: [50, 200], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(50), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_required_contains_redis_range() {
        // Redis: [150, 180], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(150), u(180));
        assert_eq!(result, Some(u(180)));
    }

    #[test]
    fn calculate_required_contains_redis_with_same_start() {
        // Redis: [100, 180], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(100), u(180));
        assert_eq!(result, Some(u(180)));
    }

    #[test]
    fn calculate_required_contains_redis_with_same_end() {
        // Redis: [150, 200], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(150), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_partial_overlap_redis_starts_earlier() {
        // Redis: [50, 150], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(50), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_partial_overlap_redis_ends_later() {
        // Redis: [150, 250], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(150), u(250));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_touching_at_single_point_redis_ends_at_required_start() {
        // Redis: [50, 100], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(50), u(100));
        assert_eq!(result, Some(u(100)));
    }

    #[test]
    fn calculate_touching_at_single_point_redis_starts_at_required_end() {
        // Redis: [200, 300], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(200), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_no_overlap_redis_before_required() {
        // Redis: [50, 99], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(50), u(99));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_redis_after_required() {
        // Redis: [201, 300], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(201), u(300));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_large_gap_redis_before() {
        // Redis: [10, 50], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(10), u(50));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_large_gap_redis_after() {
        // Redis: [500, 600], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(500), u(600));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_single_block_required_within_redis() {
        // Redis: [100, 200], Required: [150, 150]
        let result = RedisSource::calculate_target_block(u(150), u(150), u(100), u(200));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_single_block_redis_within_required() {
        // Redis: [150, 150], Required: [100, 200]
        let result = RedisSource::calculate_target_block(u(100), u(200), u(150), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_both_single_block_same() {
        // Redis: [150, 150], Required: [150, 150]
        let result = RedisSource::calculate_target_block(u(150), u(150), u(150), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_both_single_block_different() {
        // Redis: [150, 150], Required: [160, 160]
        let result = RedisSource::calculate_target_block(u(160), u(160), u(150), u(150));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_single_block_at_redis_start() {
        // Redis: [100, 200], Required: [100, 100]
        let result = RedisSource::calculate_target_block(u(100), u(100), u(100), u(200));
        assert_eq!(result, Some(u(100)));
    }

    #[test]
    fn calculate_single_block_at_redis_end() {
        // Redis: [100, 200], Required: [200, 200]
        let result = RedisSource::calculate_target_block(u(200), u(200), u(100), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_all_zeros() {
        let result = RedisSource::calculate_target_block(u(0), u(0), u(0), u(0));
        assert_eq!(result, Some(u(0)));
    }

    #[test]
    fn calculate_both_start_at_zero() {
        // Redis: [0, 100], Required: [0, 50]
        let result = RedisSource::calculate_target_block(u(0), u(50), u(0), u(100));
        assert_eq!(result, Some(u(50)));
    }

    #[test]
    fn calculate_invalid_required_range() {
        // Required: [200, 100] (invalid: min > max)
        let result = RedisSource::calculate_target_block(u(200), u(100), u(50), u(150));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_invalid_redis_range() {
        // Redis: [200, 100] (invalid: oldest > observed)
        let result = RedisSource::calculate_target_block(u(50), u(150), u(200), u(100));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_large_ethereum_block_numbers() {
        let result = RedisSource::calculate_target_block(
            u(18_000_000),
            u(18_500_000),
            u(17_900_000),
            u(18_600_000),
        );
        assert_eq!(result, Some(u(18_500_000)));
    }

    #[test]
    fn overlap_overlapping_ranges() {
        assert!(RedisSource::ranges_overlap(u(100), u(200), u(150), u(250)));
    }

    #[test]
    fn overlap_non_overlapping_ranges() {
        assert!(!RedisSource::ranges_overlap(u(100), u(200), u(300), u(400)));
    }

    #[test]
    fn overlap_touching_at_boundary() {
        assert!(RedisSource::ranges_overlap(u(100), u(200), u(200), u(300)));
    }

    #[test]
    fn overlap_adjacent_not_touching() {
        assert!(!RedisSource::ranges_overlap(u(100), u(199), u(200), u(300)));
    }

    #[test]
    fn overlap_identical_ranges() {
        assert!(RedisSource::ranges_overlap(u(100), u(200), u(100), u(200)));
    }

    #[test]
    fn overlap_one_contains_other() {
        assert!(RedisSource::ranges_overlap(u(50), u(250), u(100), u(200)));
        assert!(RedisSource::ranges_overlap(u(100), u(200), u(50), u(250)));
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

    #[test]
    fn test_target_block_updates_when_redis_syncs_late() {
        let cache = TestRedisCache::new(97, 99, true);

        // Simulate set_block_number(100) being called
        cache.update_cache_status(100, 100);

        // No overlap, target_block not updated
        assert_eq!(
            cache.get_target_block(),
            0,
            "target_block should not be set initially"
        );
        assert!(!cache.is_synced(100, 100), "Should not be synced yet");

        // RACE CONDITION: Redis syncs to block 100 AFTER update_cache_status was called
        cache.set_observed_head(100);
        cache.set_oldest_block(98);

        // Now Redis has the block, call update_cache_status again
        // (This simulates what the background sync task does)
        cache.update_cache_status(100, 100);

        // With fix: target_block should now be 100
        assert_eq!(
            cache.get_target_block(),
            100,
            "target_block should be updated to 100 after Redis syncs"
        );
        assert!(cache.is_synced(100, 100), "Should be synced now");
    }

    #[test]
    fn test_target_block_race_with_empty_cache() {
        // Simulate the exact scenario from the bug report:
        // Cache is empty/invalidated, Redis hasn't synced to new block yet
        let cache = TestRedisCache::new(97, 99, true);

        // Block 100 arrives, cache is invalidated
        cache.update_cache_status(100, 100);

        // No overlap, target_block stays at 0
        assert_eq!(cache.get_target_block(), 0);
        assert!(!cache.is_synced(100, 100));

        // Execution starts, first few transactions succeed...

        // HALFWAY THROUGH: Redis syncs to block 100
        cache.set_observed_head(100);
        cache.set_oldest_block(98);

        // Background sync task (or iter_synced_sources) calls update_cache_status again
        cache.update_cache_status(100, 100);

        // Now target_block should be correct
        assert_eq!(
            cache.get_target_block(),
            100,
            "target_block should be updated when Redis catches up"
        );
        assert!(cache.is_synced(100, 100));
    }

    #[test]
    fn test_target_block_stays_in_valid_range() {
        let cache = TestRedisCache::new(95, 98, true);

        // Set initial target
        cache.update_cache_status(96, 97);
        assert_eq!(cache.get_target_block(), 97);

        // Redis syncs forward
        cache.set_observed_head(100);

        // Update should pick the most recent valid block
        cache.update_cache_status(96, 97);
        assert_eq!(
            cache.get_target_block(),
            97,
            "Should pick min(latest_head, redis_observed_head) = 97"
        );

        // Now increase latest_head to 105
        cache.update_cache_status(96, 105);
        assert_eq!(
            cache.get_target_block(),
            100,
            "Should pick min(105, 100) = 100"
        );
    }

    #[test]
    fn test_target_block_not_updated_when_no_overlap() {
        let cache = TestRedisCache::new(100, 150, true);

        // Set valid target first
        cache.update_cache_status(120, 140);
        assert_eq!(cache.get_target_block(), 140);

        // Request block range that doesn't overlap with Redis
        cache.update_cache_status(200, 250);

        // target_block should NOT change (stays at previous valid value)
        assert_eq!(
            cache.get_target_block(),
            140,
            "target_block should not change when no overlap exists"
        );
        assert!(!cache.is_synced(200, 250));
    }

    #[test]
    fn test_target_block_multiple_sync_updates() {
        let cache = TestRedisCache::new(95, 100, true);

        // First update: Redis has [95, 100], request [98, 99]
        cache.update_cache_status(98, 99);
        assert_eq!(cache.get_target_block(), 99);

        // Redis syncs forward to 105
        cache.set_observed_head(105);

        // Second update: request [100, 102]
        cache.update_cache_status(100, 102);
        assert_eq!(
            cache.get_target_block(),
            102,
            "Should update to new valid block"
        );

        // Third update: request [103, 105]
        cache.update_cache_status(103, 105);
        assert_eq!(cache.get_target_block(), 105);
    }

    #[test]
    fn test_target_block_with_moving_oldest_block() {
        let cache = TestRedisCache::new(90, 100, true);

        // Initial state: Redis buffer is [90, 100]
        cache.update_cache_status(95, 98);
        assert_eq!(cache.get_target_block(), 98);

        // Redis buffer advances (oldest moves forward), now [95, 105]
        cache.set_oldest_block(95);
        cache.set_observed_head(105);

        // Request same range [95, 98] - should still work
        cache.update_cache_status(95, 98);
        assert_eq!(cache.get_target_block(), 98);

        // But request [90, 94] should fail now (below oldest)
        cache.update_cache_status(90, 94);
        // target_block shouldn't change from previous valid value
        assert_eq!(cache.get_target_block(), 98);
        assert!(!cache.is_synced(90, 94));
    }
}
