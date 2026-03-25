#![cfg_attr(not(test), allow(dead_code))]

//! # State-worker backed cache source

pub(crate) mod error;

pub use error::StateWorkerCacheError;
use mdbx::StateReader;
use state_worker::UNINITIALIZED_COMMITTED_HEIGHT;

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
    fmt::Debug,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
};
use tracing::{
    instrument,
    trace,
};

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    target_block: Arc<RwLock<U256>>,
    committed_height: Arc<AtomicU64>,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl MdbxSource {
    pub fn new(backend: StateReader, committed_height: Arc<AtomicU64>) -> Self {
        Self {
            backend,
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            committed_height,
            cache_status: Arc::new(CacheStatus {
                min_synced_block: RwLock::new(U256::ZERO),
                latest_head: RwLock::new(U256::ZERO),
            }),
        }
    }

    #[inline]
    fn u256_to_u64(value: U256) -> Result<u64, super::SourceError> {
        value
            .try_into()
            .map_err(|_| super::SourceError::BlockNumberOverflow(value))
    }

    fn committed_height(&self) -> Option<u64> {
        let committed_height = self.committed_height.load(Ordering::Acquire);
        (committed_height != UNINITIALIZED_COMMITTED_HEIGHT).then_some(committed_height)
    }
}

impl DatabaseRef for MdbxSource {
    type Error = super::SourceError;

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
        Ok(Some(AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: None,
        }))
    }

    #[instrument(
        name = "cache_source::block_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), block_number = number)
    )]
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        if let Some(block_hash) = self
            .backend
            .get_block_hash(number)
            .map_err(Self::Error::StateWorkerBlockHash)?
        {
            return Ok(block_hash);
        }

        let target_block = Self::u256_to_u64(*self.target_block.read())?;
        let min_block = target_block.saturating_sub(HISTORY_SERVE_WINDOW as u64);
        if number == 0 || number > target_block || number <= min_block {
            return Err(Self::Error::BlockNotFound);
        }

        let slot = keccak256(
            U256::from(number % u64::try_from(HISTORY_SERVE_WINDOW).unwrap_or(u64::MAX))
                .to_be_bytes::<32>(),
        );
        let value = self
            .backend
            .get_storage(HISTORY_STORAGE_ADDRESS.into(), slot, target_block)
            .map_err(Self::Error::StateWorkerStorage)?
            .ok_or(Self::Error::BlockNotFound)?;
        Ok(B256::from(value.to_be_bytes::<32>()))
    }

    #[instrument(
        name = "cache_source::code_by_hash_ref",
        level = "trace",
        skip(self),
        fields(source = %self.name(), code_hash = %code_hash)
    )]
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let target_block = Self::u256_to_u64(*self.target_block.read())?;
        self.backend
            .get_code(code_hash, target_block)
            .map_err(Self::Error::StateWorkerCodeByHash)?
            .map(Bytecode::new_raw)
            .ok_or(Self::Error::CodeByHashNotFound)
    }

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
        let target_block = Self::u256_to_u64(*self.target_block.read())?;
        self.backend
            .get_storage(address.into(), index.into(), target_block)
            .map_err(Self::Error::StateWorkerStorage)?
            .ok_or(Self::Error::StorageNotFound)
    }
}

impl Source for MdbxSource {
    fn is_synced(&self, _min_synced_block: U256, latest_head: U256) -> bool {
        let Some(committed_height) = self.committed_height() else {
            return false;
        };

        trace!(
            target: "state_worker",
            committed_height,
            latest_head = %latest_head,
            "mdbx source sync check"
        );

        if U256::from(committed_height) >= latest_head {
            *self.target_block.write() = latest_head;
            return true;
        }

        false
    }

    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        if let Some(committed_height) = self.committed_height()
            && U256::from(committed_height) >= latest_head
        {
            *self.target_block.write() = latest_head;
        }
    }

    fn name(&self) -> SourceName {
        SourceName::StateWorker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdbx::{
        AccountState,
        StateWriter,
        Writer as _,
        common::CircularBufferConfig,
    };
    use std::{
        collections::HashMap,
        sync::Arc,
    };
    use tempfile::TempDir;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    #[test]
    fn mdbx_source_only_reports_synced_when_committed_height_reaches_head() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let writer = StateWriter::new(&path, CircularBufferConfig::new(1).unwrap()).unwrap();
        writer
            .bootstrap_from_snapshot(
                vec![AccountState {
                    address_hash: Address::repeat_byte(0x11).into(),
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

        let reader = StateReader::new(&path, CircularBufferConfig::new(1).unwrap()).unwrap();
        let committed_height = Arc::new(AtomicU64::new(100));
        let source = MdbxSource::new(reader, committed_height.clone());

        assert!(source.is_synced(u(100), u(100)));
        committed_height.store(99, Ordering::Release);
        assert!(!source.is_synced(u(100), u(100)));
    }
}
