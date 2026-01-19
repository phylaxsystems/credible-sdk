#![cfg_attr(not(test), allow(dead_code))]

//! # State-worker backed cache source

pub(crate) mod error;
pub use error::StateWorkerCacheError;

use crate::{
    Source,
    cache::sources::SourceName,
};
use alloy::primitives::keccak256;
use alloy_eips::eip2935::{
    HISTORY_SERVE_WINDOW,
    HISTORY_STORAGE_ADDRESS,
};
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
    U256,
};
use parking_lot::RwLock;
use revm::{
    DatabaseRef,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use state_store::{
    AddressHash,
    Reader,
    mdbx::StateReader,
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
use tracing::{
    debug,
    error,
};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    /// Target block to request from state worker.
    target_block: Arc<RwLock<U256>>,
    cancel_token: CancellationToken,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl MdbxSource {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader) -> Self {
        let target_block = Arc::new(RwLock::new(U256::ZERO));
        let cancel_token = CancellationToken::new();
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(U256::ZERO),
            latest_head: RwLock::new(U256::ZERO),
        });

        Self {
            backend,
            target_block,
            cancel_token,
            cache_status,
        }
    }

    /// Helper to convert U256 to u64 for backend calls.
    /// Panics if the value overflows u64 (should never happen for block numbers in practice).
    #[inline]
    fn u256_to_u64(value: U256) -> u64 {
        value.try_into().expect("block number overflow u64")
    }

    /// Computes the intersection of two block ranges and returns the target block.
    ///
    /// Given the required range `[min_synced_block, latest_head]` and the state worker
    /// available range `[state_worker_oldest_block, state_worker_observed_head]`, this function
    /// returns the most recent block in the intersection (upper bound), or `None`
    /// if the ranges do not overlap.
    #[inline]
    fn calculate_target_block(
        min_synced_block: U256,
        latest_head: U256,
        state_worker_oldest_block: U256,
        state_worker_observed_head: U256,
    ) -> Option<U256> {
        let lower_bound = min_synced_block.max(state_worker_oldest_block);
        let upper_bound = latest_head.min(state_worker_observed_head);

        if lower_bound <= upper_bound {
            Some(upper_bound)
        } else {
            None
        }
    }

    /// Checks whether two block ranges have any overlap.
    #[cfg(test)]
    #[inline]
    fn ranges_overlap(
        min_synced_block: U256,
        latest_head: U256,
        state_worker_oldest_block: U256,
        state_worker_observed_head: U256,
    ) -> bool {
        let lower_bound = min_synced_block.max(state_worker_oldest_block);
        let upper_bound = latest_head.min(state_worker_observed_head);
        lower_bound <= upper_bound
    }
}

impl DatabaseRef for MdbxSource {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let Some(account) = self
            .backend
            .get_account(address.into(), target_block_u64)
            .map_err(Self::Error::StateWorkerAccount)?
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
    ///
    /// Called by revm for the `BLOCKHASH` opcode. Revm enforces the 256-block window internally,
    /// so this is only invoked for valid requests. Uses a fast path via `BlockMetadataTable`
    /// for recent blocks (~3), falling back to EIP-2935 storage for older blocks (up to 8191).
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        // Fast path: BlockMetadataTable (recent ~3 blocks)
        match self.backend.get_block_hash(number) {
            Ok(Some(hash)) => {
                debug!(
                    target: "state_worker",
                    source = "mdbx_metadata",
                    requested = number,
                    "block_hash_ref hit"
                );
                return Ok(hash);
            }
            Ok(None) => {
                debug!(
                    target: "state_worker",
                    source = "mdbx_metadata",
                    requested = number,
                    "block_hash_ref miss"
                );
                // Fall through to EIP-2935
            }
            Err(e) => return Err(Self::Error::StateWorkerBlockHash(e)),
        }

        let latest_head = Self::u256_to_u64(*self.cache_status.latest_head.read());

        // Guard: ensure requested block is within EIP-2935 ring buffer bounds
        if number >= latest_head || latest_head - number > HISTORY_SERVE_WINDOW as u64 {
            let diff = latest_head.saturating_sub(number);
            debug!(
                target: "state_worker",
                requested = number,
                head = latest_head,
                diff,
                window = HISTORY_SERVE_WINDOW,
                "block_hash_ref rejected, outside EIP-2935 bounds"
            );
            return Err(Self::Error::BlockNotFound);
        }

        // EIP-2935 stores at raw slot index (block_number % 8191)
        let slot_index = U256::from(number % HISTORY_SERVE_WINDOW as u64);
        let hash = self
            .backend
            .get_storage_by_raw_slot(HISTORY_STORAGE_ADDRESS.into(), slot_index, latest_head)
            .map_err(Self::Error::StateWorkerStorage)?
            .ok_or(Self::Error::BlockNotFound)?;

        debug!(
            target: "state_worker",
            source = "eip2935",
            requested = number,
            slot = %slot_index,
            "block_hash_ref hit"
        );

        Ok(B256::from(hash))
    }

    /// Loads bytecode previously stored for a code hash.
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let bytecode = Bytecode::new_raw(
            self.backend
                .get_code(code_hash, target_block_u64)
                .map_err(Self::Error::StateWorkerCodeByHash)?
                .ok_or(Self::Error::CodeByHashNotFound)?,
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
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block);
        let value = self
            .backend
            .get_storage(address.into(), slot_hash, target_block_u64)
            .map_err(Self::Error::StateWorkerStorage)?
            .unwrap_or_default();
        Ok(value)
    }
}

impl Source for MdbxSource {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, min_synced_block: U256, latest_head: U256) -> bool {
        let (state_worker_oldest_block, state_worker_observed_head) = match self
            .backend
            .get_available_block_range()
        {
            Ok(Some((state_worker_oldest_block, state_worker_observed_head))) => {
                (state_worker_oldest_block, state_worker_observed_head)
            }
            Err(e) => {
                error!(target: "state_worker", error = ?e, "failed to get available block range");
                return false;
            }
            Ok(None) => {
                error!(target: "state_worker", "missing available block range");
                return false;
            }
        };

        debug!(
            target: "state_worker",
            state_worker_oldest_block = state_worker_oldest_block,
            state_worker_observed_head = state_worker_observed_head,
            min_synced_block = %min_synced_block,
            latest_head = %latest_head,
            "is_synced"
        );

        if let Some(target) = Self::calculate_target_block(
            min_synced_block,
            latest_head,
            U256::from(state_worker_oldest_block),
            U256::from(state_worker_observed_head),
        ) {
            *self.target_block.write() = target;
            return true;
        }
        false
    }

    /// Updates the current cache status and set the target block
    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        let (state_worker_oldest_block, state_worker_observed_head) = match self
            .backend
            .get_available_block_range()
        {
            Ok(Some((state_worker_oldest_block, state_worker_observed_head))) => {
                (state_worker_oldest_block, state_worker_observed_head)
            }
            Err(e) => {
                error!(target: "state_worker", error = ?e, "failed to get available block range");
                return;
            }
            Ok(None) => {
                error!(target: "state_worker", "missing available block range");
                return;
            }
        };

        if let Some(target) = Self::calculate_target_block(
            min_synced_block,
            latest_head,
            U256::from(state_worker_oldest_block),
            U256::from(state_worker_observed_head),
        ) {
            *self.target_block.write() = target;
        }
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::StateWorker
    }
}

impl Drop for MdbxSource {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SourceError;
    use state_store::{
        AccountState,
        Writer as _,
        mdbx::{
            StateWriter,
            common::CircularBufferConfig,
        },
    };
    use tempfile::TempDir;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    /// Helper struct to test the cache logic without needing a real state worker backend
    struct TestStateWorkerCache {
        target_block: Arc<RwLock<U256>>,
        observed_head: Arc<RwLock<U256>>,
        oldest_block: Arc<RwLock<U256>>,
        sync_status: Arc<AtomicBool>,
    }

    impl TestStateWorkerCache {
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
            let state_worker_observed_head = *self.observed_head.read();
            let state_worker_oldest_block = *self.oldest_block.read();

            MdbxSource::ranges_overlap(
                U256::from(min_synced_block),
                U256::from(latest_head),
                state_worker_oldest_block,
                state_worker_observed_head,
            )
        }

        fn update_cache_status(&self, min_synced_block: u64, latest_head: u64) {
            let state_worker_observed_head = *self.observed_head.read();
            let state_worker_oldest_block = *self.oldest_block.read();

            if let Some(target) = MdbxSource::calculate_target_block(
                U256::from(min_synced_block),
                U256::from(latest_head),
                state_worker_oldest_block,
                state_worker_observed_head,
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

    /// Creates an `MdbxSource` backed by a real MDBX database for integration tests
    fn setup_mdbx_source(block_number: u64, block_hash: B256) -> (MdbxSource, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let config = CircularBufferConfig::new(5).unwrap();

        let writer = StateWriter::new(&path, config.clone()).unwrap();
        writer
            .bootstrap_from_snapshot(vec![], block_number, block_hash, B256::ZERO)
            .unwrap();
        drop(writer);

        let reader = StateReader::new(&path, config).unwrap();
        let source = MdbxSource::new(reader);
        *source.cache_status.latest_head.write() = U256::from(block_number);

        (source, tmp)
    }

    #[test]
    fn calculate_perfect_overlap_identical_ranges() {
        // state_worker: [100, 200], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(100), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_state_worker_contains_required_range() {
        // state_worker: [50, 300], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(50), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_state_worker_contains_required_with_same_start() {
        // state_worker: [100, 300], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(100), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_state_worker_contains_required_with_same_end() {
        // state_worker: [50, 200], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(50), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_required_contains_state_worker_range() {
        // state_worker: [150, 180], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(150), u(180));
        assert_eq!(result, Some(u(180)));
    }

    #[test]
    fn calculate_required_contains_state_worker_with_same_start() {
        // state_worker: [100, 180], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(100), u(180));
        assert_eq!(result, Some(u(180)));
    }

    #[test]
    fn calculate_required_contains_state_worker_with_same_end() {
        // state_worker: [150, 200], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(150), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_partial_overlap_state_worker_starts_earlier() {
        // state_worker: [50, 150], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(50), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_partial_overlap_state_worker_ends_later() {
        // state_worker: [150, 250], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(150), u(250));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_touching_at_single_point_state_worker_ends_at_required_start() {
        // state_worker: [50, 100], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(50), u(100));
        assert_eq!(result, Some(u(100)));
    }

    #[test]
    fn calculate_touching_at_single_point_state_worker_starts_at_required_end() {
        // state_worker: [200, 300], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(200), u(300));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_no_overlap_state_worker_before_required() {
        // state_worker: [50, 99], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(50), u(99));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_state_worker_after_required() {
        // state_worker: [201, 300], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(201), u(300));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_large_gap_state_worker_before() {
        // state_worker: [10, 50], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(10), u(50));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_no_overlap_large_gap_state_worker_after() {
        // state_worker: [500, 600], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(500), u(600));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_single_block_required_within_state_worker() {
        // state_worker: [100, 200], Required: [150, 150]
        let result = MdbxSource::calculate_target_block(u(150), u(150), u(100), u(200));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_single_block_state_worker_within_required() {
        // state_worker: [150, 150], Required: [100, 200]
        let result = MdbxSource::calculate_target_block(u(100), u(200), u(150), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_both_single_block_same() {
        // state_worker: [150, 150], Required: [150, 150]
        let result = MdbxSource::calculate_target_block(u(150), u(150), u(150), u(150));
        assert_eq!(result, Some(u(150)));
    }

    #[test]
    fn calculate_both_single_block_different() {
        // state_worker: [150, 150], Required: [160, 160]
        let result = MdbxSource::calculate_target_block(u(160), u(160), u(150), u(150));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_single_block_at_state_worker_start() {
        // state_worker: [100, 200], Required: [100, 100]
        let result = MdbxSource::calculate_target_block(u(100), u(100), u(100), u(200));
        assert_eq!(result, Some(u(100)));
    }

    #[test]
    fn calculate_single_block_at_state_worker_end() {
        // state_worker: [100, 200], Required: [200, 200]
        let result = MdbxSource::calculate_target_block(u(200), u(200), u(100), u(200));
        assert_eq!(result, Some(u(200)));
    }

    #[test]
    fn calculate_all_zeros() {
        let result = MdbxSource::calculate_target_block(u(0), u(0), u(0), u(0));
        assert_eq!(result, Some(u(0)));
    }

    #[test]
    fn calculate_both_start_at_zero() {
        // state_worker: [0, 100], Required: [0, 50]
        let result = MdbxSource::calculate_target_block(u(0), u(50), u(0), u(100));
        assert_eq!(result, Some(u(50)));
    }

    #[test]
    fn calculate_invalid_required_range() {
        // Required: [200, 100] (invalid: min > max)
        let result = MdbxSource::calculate_target_block(u(200), u(100), u(50), u(150));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_invalid_state_worker_range() {
        // state_worker: [200, 100] (invalid: oldest > observed)
        let result = MdbxSource::calculate_target_block(u(50), u(150), u(200), u(100));
        assert_eq!(result, None);
    }

    #[test]
    fn calculate_large_ethereum_block_numbers() {
        let result = MdbxSource::calculate_target_block(
            u(18_000_000),
            u(18_500_000),
            u(17_900_000),
            u(18_600_000),
        );
        assert_eq!(result, Some(u(18_500_000)));
    }

    #[test]
    fn overlap_overlapping_ranges() {
        assert!(MdbxSource::ranges_overlap(u(100), u(200), u(150), u(250)));
    }

    #[test]
    fn overlap_non_overlapping_ranges() {
        assert!(!MdbxSource::ranges_overlap(u(100), u(200), u(300), u(400)));
    }

    #[test]
    fn overlap_touching_at_boundary() {
        assert!(MdbxSource::ranges_overlap(u(100), u(200), u(200), u(300)));
    }

    #[test]
    fn overlap_adjacent_not_touching() {
        assert!(!MdbxSource::ranges_overlap(u(100), u(199), u(200), u(300)));
    }

    #[test]
    fn overlap_identical_ranges() {
        assert!(MdbxSource::ranges_overlap(u(100), u(200), u(100), u(200)));
    }

    #[test]
    fn overlap_one_contains_other() {
        assert!(MdbxSource::ranges_overlap(u(50), u(250), u(100), u(200)));
        assert!(MdbxSource::ranges_overlap(u(100), u(200), u(50), u(250)));
    }

    #[test]
    fn test_perfect_overlap() {
        // state_worker: [100, 200], Required: [100, 200]
        let cache = TestStateWorkerCache::new(100, 200, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200);
    }

    #[test]
    fn test_state_worker_contains_required_range() {
        // state_worker: [50, 300], Required: [100, 200]
        let cache = TestStateWorkerCache::new(50, 300, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200); // Should pick latest_head
    }

    #[test]
    fn test_required_contains_state_worker_range() {
        // state_worker: [150, 180], Required: [100, 200]
        let cache = TestStateWorkerCache::new(150, 180, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 180); // Should pick state_worker_observed_head
    }

    #[test]
    fn test_partial_overlap_lower() {
        // state_worker: [50, 150], Required: [100, 200]
        // Overlap: [100, 150]
        let cache = TestStateWorkerCache::new(50, 150, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 150); // Most recent in overlap
    }

    #[test]
    fn test_partial_overlap_upper() {
        // state_worker: [150, 250], Required: [100, 200]
        // Overlap: [150, 200]
        let cache = TestStateWorkerCache::new(150, 250, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 200); // Most recent in overlap
    }

    #[test]
    fn test_no_overlap_gap() {
        // state_worker: [100, 150], Required: [200, 250]
        // No overlap
        let cache = TestStateWorkerCache::new(100, 150, true);

        assert!(!cache.is_synced(200, 250));

        let initial_target = cache.get_target_block();
        cache.update_cache_status(200, 250);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_no_overlap_reversed() {
        // state_worker: [200, 250], Required: [100, 150]
        // No overlap
        let cache = TestStateWorkerCache::new(200, 250, true);

        assert!(!cache.is_synced(100, 150));

        let initial_target = cache.get_target_block();
        cache.update_cache_status(100, 150);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_touching_ranges_not_overlapping() {
        // state_worker: [100, 150], Required: [151, 200]
        // No overlap (adjacent but not overlapping)
        let cache = TestStateWorkerCache::new(100, 150, true);

        assert!(!cache.is_synced(151, 200));
    }

    #[test]
    fn test_touching_ranges_overlapping_by_one() {
        // state_worker: [100, 150], Required: [150, 200]
        // Overlap at block 150
        let cache = TestStateWorkerCache::new(100, 150, true);

        assert!(cache.is_synced(150, 200));
        cache.update_cache_status(150, 200);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_single_block_overlap() {
        // state_worker: [100, 200], Required: [150, 150]
        // Single block requirement
        let cache = TestStateWorkerCache::new(100, 200, true);

        assert!(cache.is_synced(150, 150));
        cache.update_cache_status(150, 150);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_sync_status_false() {
        // state_worker: [100, 200], Required: [120, 180]
        // Perfect overlap but sync_status is false
        let cache = TestStateWorkerCache::new(100, 200, false);

        assert!(!cache.is_synced(120, 180));
        // update_cache_status should still work even if sync_status is false
        cache.update_cache_status(120, 180);
        assert_eq!(cache.get_target_block(), 180);
    }

    #[test]
    fn test_zero_blocks() {
        // Edge case: block 0
        let cache = TestStateWorkerCache::new(0, 100, true);

        assert!(cache.is_synced(0, 50));
        cache.update_cache_status(0, 50);
        assert_eq!(cache.get_target_block(), 50);
    }

    #[test]
    fn test_large_block_numbers() {
        // Test with realistic Ethereum block numbers
        let cache = TestStateWorkerCache::new(18_000_000, 18_500_000, true);

        assert!(cache.is_synced(18_200_000, 18_400_000));
        cache.update_cache_status(18_200_000, 18_400_000);
        assert_eq!(cache.get_target_block(), 18_400_000);
    }

    #[test]
    fn test_update_multiple_times() {
        // Test that target_block updates correctly on multiple calls
        let cache = TestStateWorkerCache::new(100, 500, true);

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
        let cache = TestStateWorkerCache::new(100, 200, true);

        assert!(!cache.is_synced(250, 200)); // min > max

        let initial_target = cache.get_target_block();
        cache.update_cache_status(250, 200);
        assert_eq!(cache.get_target_block(), initial_target); // Should not update
    }

    #[test]
    fn test_state_worker_oldest_equals_observed() {
        // Edge case: state_worker has only one block
        let cache = TestStateWorkerCache::new(150, 150, true);

        assert!(cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_required_range_is_single_block() {
        // Required range is a single block that exists in state_worker
        let cache = TestStateWorkerCache::new(100, 200, true);

        assert!(cache.is_synced(150, 150));
        cache.update_cache_status(150, 150);
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn test_exactly_at_boundaries() {
        // Test when required range exactly matches boundaries
        let cache = TestStateWorkerCache::new(100, 200, true);

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
    fn test_target_block_updates_when_state_worker_syncs_late() {
        let cache = TestStateWorkerCache::new(97, 99, true);

        // Simulate set_block_number(100) being called
        cache.update_cache_status(100, 100);

        // No overlap, target_block not updated
        assert_eq!(
            cache.get_target_block(),
            0,
            "target_block should not be set initially"
        );
        assert!(!cache.is_synced(100, 100), "Should not be synced yet");

        // RACE CONDITION: state_worker syncs to block 100 AFTER update_cache_status was called
        cache.set_observed_head(100);
        cache.set_oldest_block(98);

        // Now state_worker has the block, call update_cache_status again
        // (This simulates what the background sync task does)
        cache.update_cache_status(100, 100);

        // With fix: target_block should now be 100
        assert_eq!(
            cache.get_target_block(),
            100,
            "target_block should be updated to 100 after state_worker syncs"
        );
        assert!(cache.is_synced(100, 100), "Should be synced now");
    }

    #[test]
    fn test_target_block_race_with_empty_cache() {
        // Simulate the exact scenario from the bug report:
        // Cache is empty/invalidated, state_worker hasn't synced to new block yet
        let cache = TestStateWorkerCache::new(97, 99, true);

        // Block 100 arrives, cache is invalidated
        cache.update_cache_status(100, 100);

        // No overlap, target_block stays at 0
        assert_eq!(cache.get_target_block(), 0);
        assert!(!cache.is_synced(100, 100));

        // Execution starts, first few transactions succeed...

        // HALFWAY THROUGH: state_worker syncs to block 100
        cache.set_observed_head(100);
        cache.set_oldest_block(98);

        // Background sync task (or iter_synced_sources) calls update_cache_status again
        cache.update_cache_status(100, 100);

        // Now target_block should be correct
        assert_eq!(
            cache.get_target_block(),
            100,
            "target_block should be updated when state_worker catches up"
        );
        assert!(cache.is_synced(100, 100));
    }

    #[test]
    fn test_target_block_stays_in_valid_range() {
        let cache = TestStateWorkerCache::new(95, 98, true);

        // Set initial target
        cache.update_cache_status(96, 97);
        assert_eq!(cache.get_target_block(), 97);

        // state_worker syncs forward
        cache.set_observed_head(100);

        // Update should pick the most recent valid block
        cache.update_cache_status(96, 97);
        assert_eq!(
            cache.get_target_block(),
            97,
            "Should pick min(latest_head, state_worker_observed_head) = 97"
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
        let cache = TestStateWorkerCache::new(100, 150, true);

        // Set valid target first
        cache.update_cache_status(120, 140);
        assert_eq!(cache.get_target_block(), 140);

        // Request block range that doesn't overlap with state_worker
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
        let cache = TestStateWorkerCache::new(95, 100, true);

        // First update: state_worker has [95, 100], request [98, 99]
        cache.update_cache_status(98, 99);
        assert_eq!(cache.get_target_block(), 99);

        // state_worker syncs forward to 105
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
        let cache = TestStateWorkerCache::new(90, 100, true);

        // Initial state: state_worker buffer is [90, 100]
        cache.update_cache_status(95, 98);
        assert_eq!(cache.get_target_block(), 98);

        // state_worker buffer advances (oldest moves forward), now [95, 105]
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

    #[test]
    fn mdbx_source_not_synced_for_nonexistent_blocks_after_bootstrap() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let config = CircularBufferConfig::new(5).unwrap();

        let writer = StateWriter::new(&path, config.clone()).unwrap();
        let addr = Address::repeat_byte(0x11);
        writer
            .bootstrap_from_snapshot(
                vec![AccountState {
                    address_hash: AddressHash(keccak256(addr)),
                    balance: u(1000),
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: HashMap::new(),
                    deleted: false,
                }],
                100,
                B256::ZERO,
                B256::ZERO,
            )
            .unwrap();
        drop(writer);

        let reader = StateReader::new(&path, config).unwrap();
        let source = MdbxSource::new(reader);

        // Requesting blocks below the bootstrap block should not consider the MDBX source synced
        assert!(!source.is_synced(u(99), u(99)));
        assert!(source.is_synced(u(100), u(100)));
    }

    #[test]
    fn block_hash_ref_returns_hash_from_metadata() {
        let expected_hash = B256::repeat_byte(0x42);
        let (source, _tmp) = setup_mdbx_source(100, expected_hash);

        let result = source.block_hash_ref(100);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_hash);
    }

    #[test]
    fn block_hash_ref_rejects_future_block() {
        let (source, _tmp) = setup_mdbx_source(100, B256::repeat_byte(0x42));

        let result = source.block_hash_ref(101);

        assert!(matches!(result, Err(SourceError::BlockNotFound)));
    }

    #[test]
    fn block_hash_ref_rejects_block_outside_history_window() {
        let (source, _tmp) = setup_mdbx_source(10000, B256::repeat_byte(0x42));

        // 10000 - 0 = 10000 > 8191, rejected by guard
        let result = source.block_hash_ref(0);

        assert!(matches!(result, Err(SourceError::BlockNotFound)));
    }

    #[test]
    fn block_hash_ref_history_window_boundary() {
        const TARGET_BLOCK: u64 = 10000;
        let (source, _tmp) = setup_mdbx_source(TARGET_BLOCK, B256::repeat_byte(0x42));

        // Just outside the history window (diff = 8192 > 8191)
        let outside = TARGET_BLOCK - HISTORY_SERVE_WINDOW as u64 - 1;
        assert_eq!(outside, 1808);
        assert_eq!(TARGET_BLOCK - outside, HISTORY_SERVE_WINDOW as u64 + 1); // 8192

        let result_outside = source.block_hash_ref(outside);
        assert!(matches!(result_outside, Err(SourceError::BlockNotFound)));

        // Exactly at the history window boundary (diff = 8191 == HISTORY_SERVE_WINDOW)
        let at_boundary = TARGET_BLOCK - HISTORY_SERVE_WINDOW as u64;
        assert_eq!(at_boundary, 1809);
        assert_eq!(TARGET_BLOCK - at_boundary, HISTORY_SERVE_WINDOW as u64); // 8191

        let result_at = source.block_hash_ref(at_boundary);
        // Passes guard, fails on storage lookup (EIP-2935 data not populated)
        assert!(result_at.is_err());
    }

    #[test]
    fn block_hash_ref_returns_hash_from_eip2935_when_metadata_missing() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let config = CircularBufferConfig::new(5).unwrap();

        let historical_block = 9900u64;
        let target_block = 10000u64;
        let expected_hash = B256::repeat_byte(0xAB);

        // EIP-2935 stores block hash at slot `block_number % HISTORY_SERVE_WINDOW`
        let slot_index = historical_block % HISTORY_SERVE_WINDOW as u64;
        let slot_key = B256::from(U256::from(slot_index));

        let writer = StateWriter::new(&path, config.clone()).unwrap();

        // Create EIP-2935 system contract with the historical block hash in storage
        let eip2935_account = AccountState {
            address_hash: HISTORY_STORAGE_ADDRESS.into(),
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::from([(slot_key, U256::from_be_bytes(expected_hash.0))]),
            deleted: false,
        };

        writer
            .bootstrap_from_snapshot(
                vec![eip2935_account],
                target_block,
                B256::repeat_byte(0x42), // target block hash, in metadata
                B256::ZERO,
            )
            .unwrap();
        drop(writer);

        let reader = StateReader::new(&path, config).unwrap();
        let source = MdbxSource::new(reader);
        *source.target_block.write() = U256::from(target_block);

        // Request historical block - should fall through to EIP-2935
        let result = source.block_hash_ref(historical_block);

        assert!(result.is_ok(), "Should retrieve hash from EIP-2935 storage");
        assert_eq!(result.unwrap(), expected_hash);
    }

    #[test]
    fn block_hash_ref_recent_block_not_in_metadata_no_eip2935() {
        let target_block = 28023285u64;
        let requested_block = 28023274u64;

        let (source, _tmp) = setup_mdbx_source(target_block, B256::repeat_byte(0x42));

        let result = source.block_hash_ref(requested_block);

        assert!(matches!(result, Err(SourceError::BlockNotFound)));
    }
}
