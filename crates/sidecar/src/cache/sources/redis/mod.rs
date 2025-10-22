#![cfg_attr(not(test), allow(dead_code))]

//! # Redis-backed cache source

pub(crate) mod error;
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
    critical,
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
            AtomicU64,
            Ordering,
        },
    },
};
use thiserror::Error;

#[derive(Debug)]
pub struct RedisCache {
    backend: StateReader,
    /// Current block
    current_block: Arc<AtomicU64>,
}

impl RedisCache {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: StateReader) -> Self {
        Self {
            backend,
            current_block: Arc::new(AtomicU64::new(0)),
        }
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
        self.backend
            .get_storage(
                address.into(),
                index,
                self.current_block.load(Ordering::Relaxed),
            )
            .map_err(Self::Error::RedisStorage)?
            .ok_or(Self::Error::StorageNotFound)
    }
}

impl Source for RedisCache {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, required_block_number: u64) -> bool {
        match self.backend.latest_block_number() {
            Ok(Some(block)) => {
                block >= required_block_number
                    && block <= self.current_block.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }

    /// No-op; we dont update the target block for redis.
    fn update_target_block(&self, block_number: u64) {
        // NOTE: Temporary patch to avoid reading redis if the redis state is higher than the sidecar state.
        self.current_block.store(block_number, Ordering::Relaxed);
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> SourceName {
        SourceName::Redis
    }
}
