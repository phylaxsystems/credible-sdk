#![cfg_attr(not(test), allow(dead_code))]

//! # State-worker backed cache source

pub(crate) mod error;

pub use error::StateWorkerCacheError;
use mdbx::StateReader;

use crate::{
    Source,
    cache::sources::SourceName,
};
use alloy_eips::eip2935::{
    HISTORY_SERVE_WINDOW,
    HISTORY_STORAGE_ADDRESS,
};
use alloy_primitives::keccak256;
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
    U256,
};
use mdbx::Reader;
use parking_lot::RwLock;
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
            AtomicU64,
            Ordering,
        },
    },
};
use thiserror::Error;
use tracing::{
    debug,
    error,
    instrument,
    trace,
};

const UNSET_HEIGHT: u64 = u64::MAX;

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    /// Target block to request from state worker.
    target_block: Arc<RwLock<U256>>,
    available_height: Arc<AtomicU64>,
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
        let available_height = Arc::new(AtomicU64::new(UNSET_HEIGHT));
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(U256::ZERO),
            latest_head: RwLock::new(U256::ZERO),
        });

        Self::refresh_available_height(&backend, &available_height);

        Self {
            backend,
            target_block,
            available_height,
            cache_status,
        }
    }

    /// Helper to convert U256 to u64 for backend calls.
    #[inline]
    fn u256_to_u64(value: U256) -> Result<u64, super::SourceError> {
        value
            .try_into()
            .map_err(|_| super::SourceError::BlockNumberOverflow(value))
    }

    fn available_height(&self) -> Option<u64> {
        let height = self.available_height.load(Ordering::Acquire);

        if height == UNSET_HEIGHT {
            return None;
        }

        Some(height)
    }

    fn refresh_available_height(backend: &StateReader, available_height: &AtomicU64) {
        match backend.latest_block_number() {
            Ok(Some(height)) => {
                available_height.store(height, Ordering::Release);
            }
            Ok(None) => {
                available_height.store(UNSET_HEIGHT, Ordering::Release);
                debug!(target: "state_worker", "missing available height");
            }
            Err(e) => {
                error!(target: "state_worker", error = ?e, "failed to get available height");
            }
        }
    }
}

impl DatabaseRef for MdbxSource {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    #[instrument(
        name = "cache_source::basic_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), address = %address)
    )]
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block)?;
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
    /// First tries the block metadata table, then falls back to EIP-2935
    /// contract storage if the block is within the history window.
    #[instrument(
        name = "cache_source::block_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), block_number = number)
    )]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        // First try the block metadata table
        if let Some(block_hash) = self
            .backend
            .get_block_hash(number)
            .map_err(Self::Error::StateWorkerBlockHash)?
        {
            return Ok(block_hash);
        }

        // Fallback: try EIP-2935 contract storage lookup
        // Check if block is within the EIP-2935 history window
        let target_block = Self::u256_to_u64(*self.target_block.read())?;
        let min_block = target_block.saturating_sub(HISTORY_SERVE_WINDOW as u64);
        if number == 0 || number > target_block || number <= min_block {
            return Err(Self::Error::BlockNotFound);
        }

        // EIP-2935 stores block hashes at slot = block_number % HISTORY_SERVE_WINDOW
        let slot = U256::from(number.saturating_sub(1) % HISTORY_SERVE_WINDOW as u64);
        let value = self.storage_ref(HISTORY_STORAGE_ADDRESS, slot)?;

        if value != U256::ZERO {
            return Ok(B256::from(value.to_be_bytes::<32>()));
        }

        Err(Self::Error::BlockNotFound)
    }

    /// Loads bytecode previously stored for a code hash.
    #[instrument(
        name = "cache_source::code_by_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), code_hash = %code_hash)
    )]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block)?;
        let bytecode = Bytecode::new_raw(
            self.backend
                .get_code(code_hash, target_block_u64)
                .map_err(Self::Error::StateWorkerCodeByHash)?
                .ok_or(Self::Error::CodeByHashNotFound)?,
        );
        Ok(bytecode)
    }

    /// Reads a storage slot for an account, defaulting to zero when missing.
    #[instrument(
        name = "cache_source::storage_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), address = %address, index = %index)
    )]
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let slot_hash = keccak256(index.to_be_bytes::<32>());
        let target_block = *self.target_block.read();
        let target_block_u64 = Self::u256_to_u64(target_block)?;
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
        Self::refresh_available_height(&self.backend, &self.available_height);

        let Some(state_worker_height) = self.available_height() else {
            debug!(target: "state_worker", "missing available height");
            return false;
        };

        let state_worker_height = U256::from(state_worker_height);

        trace!(
            target: "state_worker",
            state_worker_height = %state_worker_height,
            min_synced_block = %min_synced_block,
            latest_head = %latest_head,
            "is_synced"
        );

        if min_synced_block <= latest_head && state_worker_height >= min_synced_block {
            let target = latest_head.min(state_worker_height);
            *self.target_block.write() = target;
            return true;
        }

        false
    }

    /// Updates the current cache status and set the target block
    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        Self::refresh_available_height(&self.backend, &self.available_height);

        let Some(state_worker_height) = self.available_height() else {
            debug!(target: "state_worker", "missing available height");
            return;
        };

        let state_worker_height = U256::from(state_worker_height);

        if min_synced_block <= latest_head && state_worker_height >= min_synced_block {
            *self.target_block.write() = latest_head.min(state_worker_height);
        }
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::StateWorker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdbx::{
        AccountState,
        AddressHash,
        BlockStateUpdate,
        StateReader,
        StateWriter,
        Writer as _,
        common::CircularBufferConfig,
    };
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::AtomicU64,
        },
    };
    use tempfile::TempDir;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    struct TestStateWorkerCache {
        target_block: Arc<RwLock<U256>>,
        available_height: Arc<AtomicU64>,
    }

    impl TestStateWorkerCache {
        fn new(available_height: Option<u64>) -> Self {
            Self {
                target_block: Arc::new(RwLock::new(U256::ZERO)),
                available_height: Arc::new(AtomicU64::new(
                    available_height.unwrap_or(UNSET_HEIGHT),
                )),
            }
        }

        fn available_height(&self) -> Option<u64> {
            let height = self.available_height.load(Ordering::Acquire);
            (height != UNSET_HEIGHT).then_some(height)
        }

        fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
            if min_synced_block > latest_head {
                return false;
            }

            let Some(available_height) = self.available_height() else {
                return false;
            };

            let available_height = U256::from(available_height);
            let min_synced_block = U256::from(min_synced_block);
            let latest_head = U256::from(latest_head);

            if available_height >= min_synced_block {
                *self.target_block.write() = latest_head.min(available_height);
                return true;
            }

            false
        }

        fn update_cache_status(&self, min_synced_block: u64, latest_head: u64) {
            if min_synced_block > latest_head {
                return;
            }

            let Some(available_height) = self.available_height() else {
                return;
            };

            let available_height = U256::from(available_height);
            let min_synced_block = U256::from(min_synced_block);
            let latest_head = U256::from(latest_head);

            if available_height >= min_synced_block {
                *self.target_block.write() = latest_head.min(available_height);
            }
        }

        fn get_target_block(&self) -> u64 {
            let target = *self.target_block.read();
            u64::try_from(target).unwrap()
        }

        fn set_available_height(&self, value: Option<u64>) {
            self.available_height
                .store(value.unwrap_or(UNSET_HEIGHT), Ordering::Release);
        }
    }

    #[test]
    fn syncs_when_height_covers_required_block() {
        let cache = TestStateWorkerCache::new(Some(200));

        assert!(cache.is_synced(100, 200));
        assert_eq!(cache.get_target_block(), 200);
    }

    #[test]
    fn caps_target_block_at_available_height() {
        let cache = TestStateWorkerCache::new(Some(150));

        assert!(cache.is_synced(100, 200));
        assert_eq!(cache.get_target_block(), 150);
    }

    #[test]
    fn caps_target_block_at_latest_head_when_height_is_ahead() {
        let cache = TestStateWorkerCache::new(Some(300));

        cache.update_cache_status(100, 200);

        assert_eq!(cache.get_target_block(), 200);
    }

    #[test]
    fn does_not_sync_when_height_is_below_minimum() {
        let cache = TestStateWorkerCache::new(Some(99));

        assert!(!cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);

        assert_eq!(cache.get_target_block(), 0);
    }

    #[test]
    fn does_not_sync_when_height_is_missing() {
        let cache = TestStateWorkerCache::new(None);

        assert!(!cache.is_synced(100, 200));
        cache.update_cache_status(100, 200);

        assert_eq!(cache.get_target_block(), 0);
    }

    #[test]
    fn does_not_sync_for_invalid_required_range() {
        let cache = TestStateWorkerCache::new(Some(300));

        assert!(!cache.is_synced(250, 200));
        cache.update_cache_status(250, 200);

        assert_eq!(cache.get_target_block(), 0);
    }

    #[test]
    fn treats_zero_as_a_valid_height() {
        let cache = TestStateWorkerCache::new(Some(0));

        assert!(cache.is_synced(0, 0));
        assert_eq!(cache.get_target_block(), 0);
    }

    #[test]
    fn updates_target_block_when_height_catches_up() {
        let cache = TestStateWorkerCache::new(Some(99));

        cache.update_cache_status(100, 100);
        assert_eq!(cache.get_target_block(), 0);
        assert!(!cache.is_synced(100, 100));

        cache.set_available_height(Some(100));
        cache.update_cache_status(100, 100);

        assert_eq!(cache.get_target_block(), 100);
        assert!(cache.is_synced(100, 100));
    }

    #[test]
    fn updates_target_block_across_multiple_height_advances() {
        let cache = TestStateWorkerCache::new(Some(100));

        cache.update_cache_status(98, 99);
        assert_eq!(cache.get_target_block(), 99);

        cache.set_available_height(Some(105));
        cache.update_cache_status(100, 102);
        assert_eq!(cache.get_target_block(), 102);

        cache.update_cache_status(103, 105);
        assert_eq!(cache.get_target_block(), 105);
    }

    fn create_test_account(address: Address, balance: u64) -> AccountState {
        AccountState {
            address_hash: AddressHash(keccak256(address)),
            balance: u(balance),
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }
    }

    fn commit_test_block(writer: &StateWriter, block_number: u64, address: Address, balance: u64) {
        writer
            .commit_block(&BlockStateUpdate {
                block_number,
                block_hash: B256::repeat_byte(0x11),
                state_root: B256::repeat_byte(0x22),
                accounts: vec![create_test_account(address, balance)],
            })
            .unwrap();
    }

    #[test]
    fn mdbx_source_reads_latest_block_height_without_polling() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let config = CircularBufferConfig::new(5).unwrap();

        let writer = StateWriter::new(&path, config.clone()).unwrap();
        let addr = Address::repeat_byte(0x11);
        writer
            .bootstrap_from_snapshot(
                vec![create_test_account(addr, 1000)],
                100,
                B256::ZERO,
                B256::ZERO,
            )
            .unwrap();

        let reader = StateReader::new(&path, config).unwrap();
        let source = MdbxSource::new(reader);

        assert!(!source.is_synced(u(99), u(99)));
        assert!(source.is_synced(u(100), u(100)));

        commit_test_block(&writer, 101, addr, 2000);

        assert!(source.is_synced(u(101), u(101)));
    }
}
