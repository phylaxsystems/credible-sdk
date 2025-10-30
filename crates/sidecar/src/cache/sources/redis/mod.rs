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
    /// Latest head the underlying node has seen
    min_sync_head: Arc<AtomicU64>,
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
        let min_sync_head = Arc::new(AtomicU64::new(0));
        let observed_head = Arc::new(AtomicU64::new(0));
        let oldest_block = Arc::new(AtomicU64::new(0));
        let sync_status = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let sync_task = sync_task::spawn_sync_task(
            backend.clone(),
            min_sync_head.clone(),
            observed_head.clone(),
            oldest_block.clone(),
            sync_status.clone(),
            cancel_token.clone(),
            DEFAULT_SYNC_INTERVAL,
        );

        Self {
            backend,
            min_sync_head,
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
            &self.min_sync_head,
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
        let latest_head = self.min_sync_head.load(Ordering::Relaxed);
        let Some(account) = self
            .backend
            .get_account(address.into(), latest_head)
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
        let latest_head = self.min_sync_head.load(Ordering::Relaxed);
        let bytecode = Bytecode::new_raw(
            self.backend
                .get_code(code_hash, latest_head)
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
        let latest_head = self.min_sync_head.load(Ordering::Relaxed);
        let value = self
            .backend
            .get_storage(address.into(), slot, latest_head)
            .map_err(Self::Error::RedisStorage)?
            .ok_or(Self::Error::StorageNotFound)?;
        Ok(value)
    }
}

impl Source for RedisSource {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, latest_head: u64) -> bool {
        if !self.sync_status.load(Ordering::Acquire) {
            return false;
        }

        let target_head = self.min_sync_head.load(Ordering::Relaxed);
        let observed_head = self.observed_head.load(Ordering::Acquire);
        let oldest_block = self.oldest_block.load(Ordering::Acquire);

        if observed_head < latest_head {
            return false;
        }

        if target_head == 0 {
            return oldest_block == 0;
        }

        oldest_block <= target_head && target_head <= observed_head
    }

    /// Updates the min sync head that queries should target.
    fn update_min_sync_head(&self, block_number: u64) {
        self.min_sync_head.store(block_number, Ordering::Relaxed);
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
