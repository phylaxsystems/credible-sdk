#![cfg_attr(not(test), allow(dead_code))]

//! # Redis-backed cache source

pub(crate) mod error;
mod sync_task;
pub(crate) mod utils;

pub use error::RedisCacheError;
use state_store::StateReader;

use self::utils::{
    decode_hex,
    encode_hex,
    encode_storage_key,
    encode_u256_hex,
    parse_b256,
    parse_u64,
    parse_u256,
    to_hex_lower,
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
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use thiserror::Error;

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub struct RedisCache {
    backend: StateReader,
    /// Current block
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
    cancel_token: CancellationToken,
    sync_task: JoinHandle<()>,
}

impl RedisCache {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader) -> Self {
        let current_block = Arc::new(AtomicU64::new(0));
        let observed_block = Arc::new(AtomicU64::new(0));
        let sync_status = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let sync_task = sync_task::spawn_sync_task(
            backend.clone(),
            current_block.clone(),
            observed_block.clone(),
            sync_status.clone(),
            cancel_token.clone(),
            DEFAULT_SYNC_INTERVAL,
        );

        Self {
            backend,
            current_block,
            observed_block,
            sync_status,
            cancel_token,
            sync_task,
        }
    }

    fn update_observed_block(&self, observed_block: u64) {
        self.observed_block.store(observed_block, Ordering::Release);
        let target_block = self.current_block.load(Ordering::Acquire);
        let within_target = if target_block == 0 {
            observed_block == 0
        } else {
            observed_block <= target_block
        };
        self.sync_status.store(within_target, Ordering::Release);
    }

    fn mark_unsynced(&self) {
        self.observed_block.store(0, Ordering::Release);
        self.sync_status.store(false, Ordering::Release);
    }
}

impl DatabaseRef for RedisCache {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let Some(account) = self
            .backend
            .get_account(address.into(), self.current_block.load(Ordering::Relaxed))
            .map_err(Self::Error::RedisAccount)?
        else {
            return Ok(None);
        };
        Ok(Some(AccountInfo {
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code: account.code.map(|bytes| Bytecode::new_raw(bytes.into())),
        }))
    }

    /// Looks up the canonical hash for the requested block number.
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.backend
            .get_block_hash(number)
            .map_err(Self::Error::RedisBlockHash)?
            .ok_or(Self::Error::BlockNotFound)
    }

    /// Loads bytecode previously stored for a code hash.
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::new_raw(
            self.backend
                .get_code(code_hash, self.current_block.load(Ordering::Relaxed))
                .map_err(Self::Error::RedisCodeByHash)?
                .ok_or(Self::Error::CodeByHashNotFound)?
                .into(),
        ))
    }

    /// Reads a storage slot for an account, defaulting to zero when missing.
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let slot_hash = keccak256(index.to_be_bytes::<32>());
        let slot = U256::from_be_bytes(slot_hash.into());
        self.backend
            .get_storage(
                address.into(),
                slot,
                self.current_block.load(Ordering::Relaxed),
            )
            .map_err(Self::Error::RedisStorage)?
            .ok_or(Self::Error::StorageNotFound)
    }
}

impl Source for RedisCache {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, required_block_number: u64) -> bool {
        let target_block = self.current_block.load(Ordering::Relaxed);

        if !self.sync_status.load(Ordering::Acquire) {
            return false;
        }

        let observed_block = self.observed_block.load(Ordering::Acquire);
        let within_target = if target_block == 0 {
            observed_block == 0
        } else {
            observed_block <= target_block
        };

        observed_block >= required_block_number && within_target
    }

    /// Updates the block number that queries should target.
    fn update_target_block(&self, block_number: u64) {
        self.current_block.store(block_number, Ordering::Relaxed);
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::Redis
    }
}

impl Drop for RedisCache {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.sync_task.abort();
    }
}
