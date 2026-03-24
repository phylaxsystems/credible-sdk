#![cfg_attr(not(test), allow(dead_code))]

//! # State-worker backed cache source

pub(crate) mod error;

pub use error::StateWorkerCacheError;
use mdbx::StateReader;

use crate::{
    Source,
    cache::sources::SourceName,
    state_sync::supervisor::WorkerStatusHandle,
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
    sync::Arc,
};
use thiserror::Error;
use tracing::{
    debug,
    instrument,
    trace,
};

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    /// Target block to request from MDBX for reads.
    target_block: Arc<RwLock<U256>>,
    worker_status: WorkerStatusHandle,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl MdbxSource {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader, worker_status: WorkerStatusHandle) -> Self {
        Self {
            backend,
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            worker_status,
            cache_status: Arc::new(CacheStatus {
                min_synced_block: RwLock::new(U256::ZERO),
                latest_head: RwLock::new(U256::ZERO),
            }),
        }
    }

    /// Helper to convert U256 to u64 for backend calls.
    #[inline]
    fn u256_to_u64(value: U256) -> Result<u64, super::SourceError> {
        value
            .try_into()
            .map_err(|_| super::SourceError::BlockNumberOverflow(value))
    }

    fn synced_target_block(&self, latest_head: U256) -> Option<U256> {
        let snapshot = self.worker_status.snapshot();

        trace!(
            target: "state_worker",
            latest_head = %latest_head,
            latest_head_seen = ?snapshot.latest_head_seen,
            highest_staged_block = ?snapshot.highest_staged_block,
            mdbx_synced_through = ?snapshot.mdbx_synced_through,
            healthy = snapshot.healthy,
            restarting = snapshot.restarting,
            "checking mdbx source sync status"
        );

        if !snapshot.healthy || snapshot.restarting {
            debug!(target: "state_worker", "worker is unhealthy or restarting");
            return None;
        }

        let Some(synced_through) = snapshot.mdbx_synced_through else {
            debug!(target: "state_worker", "worker has not synced MDBX yet");
            return None;
        };

        if U256::from(synced_through) < latest_head {
            debug!(
                target: "state_worker",
                synced_through,
                latest_head = %latest_head,
                "worker is behind requested head"
            );
            return None;
        }

        Some(latest_head)
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
    fn is_synced(&self, _min_synced_block: U256, latest_head: U256) -> bool {
        if let Some(target) = self.synced_target_block(latest_head) {
            *self.target_block.write() = target;
            return true;
        }
        false
    }

    /// Updates the current cache status and set the target block
    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        if let Some(target) = self.synced_target_block(latest_head) {
            *self.target_block.write() = target;
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
    use state_worker::WorkerStatusSnapshot;
    use tempfile::TempDir;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    #[derive(Clone, Debug)]
    struct TestWorkerStatus {
        snapshot: Arc<WorkerStatusSnapshot>,
    }

    impl TestWorkerStatus {
        fn new(
            latest_head_seen: Option<u64>,
            highest_staged_block: Option<u64>,
            mdbx_synced_through: Option<u64>,
            healthy: bool,
        ) -> Self {
            Self {
                snapshot: Arc::new(WorkerStatusSnapshot {
                    latest_head_seen,
                    highest_staged_block,
                    mdbx_synced_through,
                    healthy,
                    restarting: false,
                }),
            }
        }

        fn restarting(
            latest_head_seen: Option<u64>,
            highest_staged_block: Option<u64>,
            mdbx_synced_through: Option<u64>,
        ) -> Self {
            Self {
                snapshot: Arc::new(WorkerStatusSnapshot {
                    latest_head_seen,
                    highest_staged_block,
                    mdbx_synced_through,
                    healthy: true,
                    restarting: true,
                }),
            }
        }

        fn into_handle(self) -> crate::state_sync::supervisor::WorkerStatusHandle {
            Arc::new(self)
        }
    }

    impl crate::state_sync::supervisor::WorkerStatusReader for TestWorkerStatus {
        fn snapshot(&self) -> WorkerStatusSnapshot {
            (*self.snapshot).clone()
        }
    }

    fn test_reader() -> StateReader {
        let tmp = TempDir::new().unwrap();
        let path = tmp.keep();
        let writer = mdbx::StateWriter::new(&path).unwrap();
        drop(writer);
        StateReader::new(&path).unwrap()
    }

    #[test]
    fn test_mdbx_source_uses_status_snapshot_without_background_poller() {
        let status = TestWorkerStatus::new(Some(120), Some(120), Some(100), true);
        let source = MdbxSource::new(test_reader(), status.into_handle());

        assert!(source.is_synced(U256::from(95), U256::from(100)));
    }

    #[test]
    fn test_mdbx_source_is_unsynced_when_worker_is_behind() {
        let status = TestWorkerStatus::new(Some(119), Some(99), Some(99), true);
        let source = MdbxSource::new(test_reader(), status.into_handle());

        assert!(!source.is_synced(U256::from(95), U256::from(100)));
    }

    #[test]
    fn test_mdbx_source_is_unsynced_when_worker_is_restarting() {
        let status = TestWorkerStatus::restarting(Some(120), Some(120), Some(100));
        let source = MdbxSource::new(test_reader(), status.into_handle());

        assert!(!source.is_synced(U256::from(95), U256::from(100)));
    }

    #[tokio::test]
    async fn mdbx_source_uses_synced_through_status_after_bootstrap() {
        use mdbx::{
            AccountState,
            AddressHash,
            Reader as _,
            StateReader,
            StateWriter,
            Writer as _,
        };
        use std::collections::HashMap;
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let writer = StateWriter::new(&path).unwrap();
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

        let reader = StateReader::new(&path).unwrap();
        let source = MdbxSource::new(
            reader,
            TestWorkerStatus::new(None, None, Some(100), true).into_handle(),
        );

        // Once worker status reports MDBX synced through block 100, that status is authoritative.
        assert!(source.is_synced(u(99), u(99)));
        assert!(source.is_synced(u(100), u(100)));
    }
}
