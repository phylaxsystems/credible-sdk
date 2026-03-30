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
use state_worker::FlushControl;
use std::{
    fmt::Debug,
    sync::Arc,
};
use tracing::{
    debug,
    error,
    instrument,
    trace,
};

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    target_block: Arc<RwLock<U256>>,
    committed_head: Option<Arc<FlushControl>>,
    cache_status: Arc<CacheStatus>,
}

#[derive(Debug)]
pub(crate) struct CacheStatus {
    pub min_synced_block: RwLock<U256>,
    pub latest_head: RwLock<U256>,
}

impl MdbxSource {
    pub fn new(backend: StateReader) -> Self {
        Self::build(backend, None)
    }

    pub fn new_with_flush_control(backend: StateReader, committed_head: Arc<FlushControl>) -> Self {
        Self::build(backend, Some(committed_head))
    }

    fn build(backend: StateReader, committed_head: Option<Arc<FlushControl>>) -> Self {
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

    fn latest_committed_block(&self) -> Option<u64> {
        if let Some(control) = self.committed_head.as_ref() {
            return control.committed_block();
        }

        match self.backend.latest_block_number() {
            Ok(block_number) => block_number,
            Err(err) => {
                error!(target: "state_worker", error = ?err, "failed to get latest committed block");
                None
            }
        }
    }

    fn available_block_range(&self) -> Option<(u64, u64)> {
        match self.backend.get_available_block_range() {
            Ok(range) => range,
            Err(err) => {
                error!(
                    target: "state_worker",
                    error = ?err,
                    "failed to get available MDBX block range"
                );
                None
            }
        }
    }

    fn update_target_block_from_head(&self, latest_head: U256) -> bool {
        let Some(committed_block) = self.latest_committed_block() else {
            debug!(target: "state_worker", "missing committed block");
            return false;
        };

        let target_block = U256::from(committed_block).min(latest_head);
        *self.target_block.write() = target_block;
        true
    }

    #[cfg(test)]
    fn current_target_block(&self) -> U256 {
        *self.target_block.read()
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
        let target_block_u64 = Self::u256_to_u64(*self.target_block.read())?;
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
        let target_block_u64 = Self::u256_to_u64(*self.target_block.read())?;
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
        let target_block_u64 = Self::u256_to_u64(*self.target_block.read())?;
        let value = self
            .backend
            .get_storage(address.into(), slot_hash, target_block_u64)
            .map_err(Self::Error::StateWorkerStorage)?
            .unwrap_or_default();
        Ok(value)
    }
}

impl Source for MdbxSource {
    fn is_synced(&self, min_synced_block: U256, latest_head: U256) -> bool {
        let Some(committed_block) = self.latest_committed_block() else {
            debug!(target: "state_worker", "missing committed block");
            return false;
        };

        if self.committed_head.is_none() {
            let Some((oldest_available_block, _latest_available_block)) =
                self.available_block_range()
            else {
                debug!(target: "state_worker", "missing available MDBX block range");
                return false;
            };

            if min_synced_block < U256::from(oldest_available_block) {
                debug!(
                    target: "state_worker",
                    min_synced_block = %min_synced_block,
                    oldest_available_block,
                    "requested minimum synced block has already been evicted from MDBX",
                );
                return false;
            }
        }

        trace!(
            target: "state_worker",
            committed_block = committed_block,
            min_synced_block = %min_synced_block,
            latest_head = %latest_head,
            "is_synced"
        );

        if U256::from(committed_block) < min_synced_block {
            return false;
        }

        self.update_target_block_from_head(latest_head)
    }

    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256) {
        *self.cache_status.min_synced_block.write() = min_synced_block;
        *self.cache_status.latest_head.write() = latest_head;
        let _ = self.update_target_block_from_head(latest_head);
    }

    fn name(&self) -> SourceName {
        SourceName::StateWorker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdbx::{
        BlockStateUpdate,
        StateWriter,
        Writer,
        common::CircularBufferConfig,
    };
    use tempfile::tempdir;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    fn commit_empty_block(writer: &StateWriter, block_number: u64) {
        let block_hash_byte = u8::try_from(block_number).unwrap_or(u8::MAX);
        writer
            .commit_block(&BlockStateUpdate {
                block_number,
                block_hash: B256::repeat_byte(block_hash_byte),
                state_root: B256::repeat_byte(block_hash_byte.saturating_add(1)),
                accounts: Vec::new(),
            })
            .expect("commit block");
    }

    #[test]
    fn integrated_source_is_synced_when_committed_head_meets_minimum() {
        let dir = tempdir().expect("tmpdir");
        let path = dir.path().join("state");
        let writer =
            StateWriter::new(&path, CircularBufferConfig::new(1).expect("config")).expect("writer");
        commit_empty_block(&writer, 0);

        let reader = writer.reader().clone();
        let control = FlushControl::new();
        control.record_committed_block(0);
        let source = MdbxSource::new_with_flush_control(reader, control);

        assert!(source.is_synced(u(0), u(3)));
        assert_eq!(source.current_target_block(), u(0));
    }

    #[test]
    fn integrated_source_is_unsynced_when_committed_head_is_too_far_behind() {
        let dir = tempdir().expect("tmpdir");
        let path = dir.path().join("state");
        let writer =
            StateWriter::new(&path, CircularBufferConfig::new(1).expect("config")).expect("writer");
        commit_empty_block(&writer, 0);

        let reader = writer.reader().clone();
        let control = FlushControl::new();
        control.record_committed_block(0);
        let source = MdbxSource::new_with_flush_control(reader, control);

        assert!(!source.is_synced(u(1), u(3)));
    }

    #[test]
    fn external_source_uses_latest_committed_block_without_background_poller() {
        let dir = tempdir().expect("tmpdir");
        let path = dir.path().join("state");
        let writer =
            StateWriter::new(&path, CircularBufferConfig::new(3).expect("config")).expect("writer");
        commit_empty_block(&writer, 5);

        let reader = writer.reader().clone();
        let source = MdbxSource::new(reader);

        assert!(source.is_synced(u(5), u(9)));
        assert_eq!(source.current_target_block(), u(5));
    }

    #[test]
    fn external_source_is_unsynced_when_minimum_block_has_been_evicted() {
        let dir = tempdir().expect("tmpdir");
        let path = dir.path().join("state");
        let writer =
            StateWriter::new(&path, CircularBufferConfig::new(2).expect("config")).expect("writer");
        commit_empty_block(&writer, 4);
        commit_empty_block(&writer, 5);
        commit_empty_block(&writer, 6);

        let reader = writer.reader().clone();
        let source = MdbxSource::new(reader);

        assert!(!source.is_synced(u(4), u(9)));
    }
}
