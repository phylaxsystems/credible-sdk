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
    debug,
    instrument,
    trace,
};

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    target_block: Arc<RwLock<U256>>,
    committed_head: Arc<AtomicU64>,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl MdbxSource {
    pub fn new(backend: StateReader, committed_head: Arc<AtomicU64>) -> Self {
        Self {
            backend,
            target_block: Arc::new(RwLock::new(U256::ZERO)),
            committed_head,
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

    #[inline]
    fn is_head_synced(committed_head: u64, latest_head: U256) -> bool {
        U256::from(committed_head) >= latest_head
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

        let account_info = AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: None,
        };
        Ok(Some(account_info))
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

        let slot = U256::from(number.saturating_sub(1) % HISTORY_SERVE_WINDOW as u64);
        let value = self.storage_ref(HISTORY_STORAGE_ADDRESS, slot)?;

        if value != U256::ZERO {
            return Ok(B256::from(value.to_be_bytes::<32>()));
        }

        Err(Self::Error::BlockNotFound)
    }

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
    fn is_synced(&self, _min_synced_block: U256, latest_head: U256) -> bool {
        let committed_head = self.committed_head.load(Ordering::Acquire);
        trace!(
            target: "state_worker",
            committed_head,
            latest_head = %latest_head,
            "checking MDBX sync state"
        );

        if !Self::is_head_synced(committed_head, latest_head) {
            debug!(target: "state_worker", committed_head, latest_head = %latest_head, "MDBX source behind commit head");
            return false;
        }

        *self.target_block.write() = latest_head;
        true
    }

    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;

        let committed_head = self.committed_head.load(Ordering::Acquire);
        if Self::is_head_synced(committed_head, latest_head) {
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

    #[test]
    fn source_is_synced_only_when_committed_head_reaches_latest_head() {
        assert!(!MdbxSource::is_head_synced(99, U256::from(100)));
        assert!(MdbxSource::is_head_synced(100, U256::from(100)));
        assert!(MdbxSource::is_head_synced(101, U256::from(100)));
    }
}
