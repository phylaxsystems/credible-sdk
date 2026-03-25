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
use revm::{
    DatabaseRef,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering,
    },
};
use tracing::{
    instrument,
    trace,
};

#[derive(Debug)]
pub struct MdbxSource {
    backend: StateReader,
    /// Shared committed head written by state worker with Release ordering after each flush.
    /// MdbxSource reads with Acquire ordering to establish happens-before: MDBX data is durable
    /// before the height is observable.
    committed_head: Arc<AtomicU64>,
}

impl MdbxSource {
    /// Creates a new MdbxSource backed by the given StateReader.
    ///
    /// `committed_head` is the Arc<AtomicU64> that the state worker thread writes after each
    /// MDBX flush (with Release ordering). MdbxSource reads it with Acquire ordering in
    /// `is_synced`, eliminating the 50ms polling loop.
    pub fn new(backend: StateReader, committed_head: Arc<AtomicU64>) -> Self {
        Self {
            backend,
            committed_head,
        }
    }

    /// Helper to convert U256 to u64 for backend calls.
    #[inline]
    fn u256_to_u64(value: U256) -> Result<u64, super::SourceError> {
        value
            .try_into()
            .map_err(|_| super::SourceError::BlockNumberOverflow(value))
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
        let target_block = U256::from(self.committed_head.load(Ordering::Acquire));
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
        let target_block = Self::u256_to_u64(U256::from(self.committed_head.load(Ordering::Acquire)))?;
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
        let target_block = U256::from(self.committed_head.load(Ordering::Acquire));
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
        let target_block = U256::from(self.committed_head.load(Ordering::Acquire));
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
    /// Reports whether the MDBX-committed height covers the minimum block needed.
    ///
    /// Reads committed_head with Acquire ordering — pairs with the Release store in
    /// flush_ready_blocks (state_worker_thread/mod.rs). This establishes a happens-before:
    /// MDBX data is durable before the height is observable here.
    fn is_synced(&self, min_synced_block: U256, _latest_head: U256) -> bool {
        // Acquire load pairs with Release store in flush_ready_blocks (state_worker_thread/mod.rs).
        // This establishes a happens-before: MDBX data is durable before the height is observable.
        let committed = self.committed_head.load(Ordering::Acquire);
        // If committed_head is 0 (initial value, no blocks flushed yet), return false immediately.
        if committed == 0 {
            return false;
        }
        // committed_head stores the block number last flushed to MDBX.
        // is_synced returns true when the MDBX-committed height covers the minimum block needed.
        let Ok(min_block) = u64::try_from(min_synced_block) else {
            return false; // U256 > u64::MAX is not a valid block number
        };

        trace!(
            target: "state_worker",
            committed = committed,
            min_synced_block = %min_synced_block,
            "is_synced"
        );

        committed >= min_block
    }

    /// No-op: sync state is now read directly from committed_head AtomicU64.
    ///
    /// The polling loop and range intersection have been removed (SIMP-01, SIMP-03).
    fn update_cache_status(&self, _min_synced_block: U256, _latest_head: U256) {
        // No-op: sync state is now read directly from committed_head AtomicU64.
        // The polling loop and range intersection have been removed (SIMP-01, SIMP-03).
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::StateWorker
    }
}
