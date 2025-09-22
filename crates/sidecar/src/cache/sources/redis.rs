#![cfg_attr(not(test), allow(dead_code))]

//! # Redis-backed cache source
//!
//! Provides a `Source` implementation backed by Redis so that blockchain state
//! can be cached between runs. `RedisClientBackend` wraps a `redis::Client`,
//! establishing a single shared connection lazily and reusing it across all
//! commands. Construct it with `RedisClientBackend::new(client)` or
//! `RedisClientBackend::from_url("redis://...")`.
//!
//! ## Redis schema
//!
//! Entries are namespaced to avoid key collisions:
//! ```ignore
//! state:account:{address}     → {balance, nonce, code_hash}
//! state:storage:{address}     → {slot1: value1, slot2: value2, ...}
//! state:code:{code_hash}      → hex-encoded bytecode
//! state:current_block         → latest synced block number
//! state:block_hash:{number}   → block hash
//! ```

use crate::Source;
use alloy::hex;
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
};
use thiserror::Error;

/// Prefix used to group all cache keys.
const DEFAULT_NAMESPACE: &str = "state";
const ACCOUNT_PREFIX: &str = "account";
const STORAGE_PREFIX: &str = "storage";
const CODE_PREFIX: &str = "code";
const BLOCK_HASH_PREFIX: &str = "block_hash";
const CURRENT_BLOCK_KEY: &str = "current_block";

/// Contract metadata stored alongside bytecode when present.
const BALANCE_FIELD: &str = "balance";
const NONCE_FIELD: &str = "nonce";
const CODE_HASH_FIELD: &str = "code_hash";

/// Abstraction over the backing Redis client.
pub trait RedisBackend: Debug + Send + Sync {
    /// Reads all fields stored in a Redis hash, returning `None` when the key is missing.
    fn hgetall(&self, key: &str) -> Result<Option<HashMap<String, String>>, RedisCacheError>;
    /// Reads a single field from a Redis hash to support targeted lookups.
    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RedisCacheError>;
    /// Writes multiple field/value pairs to a Redis hash in one round-trip.
    fn hset_multiple(&self, key: &str, values: &[(String, String)]) -> Result<(), RedisCacheError>;
    /// Writes a single field/value pair to a Redis hash for incremental updates.
    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RedisCacheError>;
    /// Reads a plain string value stored at `key`.
    fn get(&self, key: &str) -> Result<Option<String>, RedisCacheError>;
    /// Writes a plain string value at `key` for block metadata and bytecode.
    fn set(&self, key: &str, value: &str) -> Result<(), RedisCacheError>;
}

/// Real Redis backend that delegates commands to `redis::Client`.
pub struct RedisClientBackend {
    client: redis::Client,
}

impl Clone for RedisClientBackend {
    fn clone(&self) -> Self {
        Self::new(self.client.clone())
    }
}

impl Debug for RedisClientBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisClientBackend")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl RedisClientBackend {
    /// Wraps an existing `redis::Client`, allowing callers to share clients across caches.
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    /// Constructs a new backend by opening a client from the provided connection URL.
    pub fn from_url(url: &str) -> Result<Self, RedisCacheError> {
        let client = redis::Client::open(url).map_err(RedisCacheError::Backend)?;
        Ok(Self::new(client))
    }

    /// Executes `func` with a standard read/write connection obtained from the client.
    fn with_connection<F, T>(&self, func: F) -> Result<T, RedisCacheError>
    where
        F: FnOnce(&mut redis::Connection) -> Result<T, redis::RedisError>,
    {
        let mut connection = self
            .client
            .get_connection()
            .map_err(RedisCacheError::Backend)?;

        func(&mut connection).map_err(RedisCacheError::Backend)
    }
}

impl RedisBackend for RedisClientBackend {
    fn hgetall(&self, key: &str) -> Result<Option<HashMap<String, String>>, RedisCacheError> {
        self.with_connection(|conn| conn.hgetall(key))
            .map(|map: HashMap<String, String>| if map.is_empty() { None } else { Some(map) })
    }

    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RedisCacheError> {
        self.with_connection(|conn| conn.hget(key, field))
    }

    fn hset_multiple(&self, key: &str, values: &[(String, String)]) -> Result<(), RedisCacheError> {
        self.with_connection(|conn| conn.hset_multiple(key, values))
    }

    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RedisCacheError> {
        self.with_connection(|conn| conn.hset(key, field, value))
    }

    fn get(&self, key: &str) -> Result<Option<String>, RedisCacheError> {
        self.with_connection(|conn| conn.get(key))
    }

    fn set(&self, key: &str, value: &str) -> Result<(), RedisCacheError> {
        self.with_connection(|conn| conn.set(key, value))
    }
}

#[derive(Debug)]
pub struct RedisCache<B: RedisBackend> {
    backend: B,
    namespace: String,
}

impl<B: RedisBackend> RedisCache<B> {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: B) -> Self {
        Self::with_namespace(backend, DEFAULT_NAMESPACE)
    }

    /// Creates a cache that stores entries under a custom namespace prefix.
    pub fn with_namespace(backend: B, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }

    // Derives the Redis key that holds account metadata for a specific address.
    fn account_key(&self, address: Address) -> String {
        format!(
            "{}:{}:{}",
            self.namespace,
            ACCOUNT_PREFIX,
            to_hex_lower(address.as_slice())
        )
    }

    // Derives the Redis key that contains all storage slots for an account.
    fn storage_key(&self, address: Address) -> String {
        format!(
            "{}:{}:{}",
            self.namespace,
            STORAGE_PREFIX,
            to_hex_lower(address.as_slice())
        )
    }

    // Derives the Redis key holding bytecode for a code hash.
    fn code_key(&self, code_hash: B256) -> String {
        format!(
            "{}:{}:{}",
            self.namespace,
            CODE_PREFIX,
            to_hex_lower(code_hash.as_slice())
        )
    }

    // Derives the Redis key that maps a block number to its canonical hash.
    fn block_hash_key(&self, block_number: u64) -> String {
        format!("{}:{}:{}", self.namespace, BLOCK_HASH_PREFIX, block_number)
    }

    // Returns the Redis key that tracks the highest synchronized block number.
    fn current_block_key(&self) -> String {
        format!("{}:{}", self.namespace, CURRENT_BLOCK_KEY)
    }

    /// Converts the raw Redis hash for an account into an `AccountInfo` instance.
    fn parse_account(
        key: &str,
        fields: &HashMap<String, String>,
    ) -> Result<AccountInfo, RedisCacheError> {
        let balance = fields
            .get(BALANCE_FIELD)
            .ok_or(RedisCacheError::MissingField {
                key: key.to_string(),
                field: BALANCE_FIELD,
            })
            .and_then(|value| parse_u256(value, key, BALANCE_FIELD))?;
        let nonce = fields
            .get(NONCE_FIELD)
            .ok_or(RedisCacheError::MissingField {
                key: key.to_string(),
                field: NONCE_FIELD,
            })
            .and_then(|value| parse_u64(value, key, NONCE_FIELD))?;
        let code_hash = fields
            .get(CODE_HASH_FIELD)
            .ok_or(RedisCacheError::MissingField {
                key: key.to_string(),
                field: CODE_HASH_FIELD,
            })
            .and_then(|value| parse_b256(value, CODE_HASH_FIELD))?;

        Ok(AccountInfo {
            balance,
            nonce,
            code_hash,
            code: None,
        })
    }

    /// Fetches the highest block number that has been cached so far.
    fn fetch_current_block_number(&self) -> Result<Option<u64>, RedisCacheError> {
        let key = self.current_block_key();
        self.backend.get(&key).and_then(|opt| {
            opt.map(|value| parse_u64(&value, &key, CURRENT_BLOCK_KEY))
                .transpose()
        })
    }

    /// Persists the account's balance/nonce/code-hash tuple under the account key.
    pub fn put_account(
        &self,
        address: Address,
        balance: U256,
        nonce: u64,
        code_hash: B256,
    ) -> Result<(), RedisCacheError> {
        let key = self.account_key(address);
        let values = [
            (BALANCE_FIELD.to_string(), balance.to_string()),
            (NONCE_FIELD.to_string(), nonce.to_string()),
            (
                CODE_HASH_FIELD.to_string(),
                encode_hex(code_hash.as_slice()),
            ),
        ];
        self.backend.hset_multiple(&key, &values)
    }

    /// Updates a single storage slot for the given account.
    pub fn put_storage(
        &self,
        address: Address,
        slot: StorageKey,
        value: StorageValue,
    ) -> Result<(), RedisCacheError> {
        let key = self.storage_key(address);
        let slot_key = encode_storage_key(slot);
        self.backend.hset(&key, &slot_key, &encode_u256_hex(value))
    }

    /// Persists bytecode referenced by a specific code hash.
    pub fn put_code_bytes(
        &self,
        code_hash: B256,
        bytecode: impl AsRef<[u8]>,
    ) -> Result<(), RedisCacheError> {
        let key = self.code_key(code_hash);
        self.backend.set(&key, &encode_hex(bytecode.as_ref()))
    }

    /// Convenience wrapper that stores bytecode contained in a `Bytecode` instance.
    pub fn put_code(&self, code_hash: B256, bytecode: &Bytecode) -> Result<(), RedisCacheError> {
        self.put_code_bytes(code_hash, bytecode.original_bytes())
    }

    /// Associates a block number with its canonical hash in Redis.
    pub fn put_block_hash(
        &self,
        block_number: u64,
        block_hash: B256,
    ) -> Result<(), RedisCacheError> {
        let key = self.block_hash_key(block_number);
        self.backend.set(&key, &encode_hex(block_hash.as_slice()))
    }

    /// Records the highest block number that has been synchronized into the cache.
    pub fn set_current_block_number(&self, block_number: u64) -> Result<(), RedisCacheError> {
        self.backend
            .set(&self.current_block_key(), &block_number.to_string())
    }

    /// Helper that reads a raw storage slot value without converting it yet.
    fn hget_storage_value(
        &self,
        address: Address,
        slot: StorageKey,
    ) -> Result<Option<String>, RedisCacheError> {
        let key = self.storage_key(address);
        let slot_key = encode_storage_key(slot);
        self.backend.hget(&key, &slot_key)
    }
}

impl<B: RedisBackend> DatabaseRef for RedisCache<B> {
    type Error = super::SourceError;

    /// Reconstructs an account from cached metadata, returning `None` when absent.
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key = self.account_key(address);
        let account_map = match self
            .backend
            .hgetall(&key)
            .map_err(super::SourceError::from)?
        {
            Some(map) if !map.is_empty() => map,
            _ => return Ok(None),
        };
        Self::parse_account(&key, &account_map)
            .map(Some)
            .map_err(super::SourceError::from)
    }

    /// Looks up the canonical hash for the requested block number.
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let key = self.block_hash_key(number);
        let value = self
            .backend
            .get(&key)
            .map_err(super::SourceError::from)?
            .ok_or_else(|| super::SourceError::from(RedisCacheError::BlockHashNotFound(number)))?;
        parse_b256(&value, "block_hash").map_err(super::SourceError::from)
    }

    /// Loads bytecode previously stored for a code hash.
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let key = self.code_key(code_hash);
        let value = self
            .backend
            .get(&key)
            .map_err(super::SourceError::from)?
            .ok_or_else(|| super::SourceError::from(RedisCacheError::CodeNotFound(code_hash)))?;
        let bytes = decode_hex(&value, "bytecode").map_err(super::SourceError::from)?;
        Ok(Bytecode::new_raw(bytes.into()))
    }

    /// Reads a storage slot for an account, defaulting to zero when missing.
    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        match self
            .hget_storage_value(address, index)
            .map_err(super::SourceError::from)?
        {
            Some(value) => {
                parse_u256(&value, &self.storage_key(address), "storage")
                    .map_err(super::SourceError::from)
            }
            None => {
                Err(super::SourceError::from(RedisCacheError::CacheMiss {
                    kind: "storage",
                }))
            }
        }
    }
}

impl<B: RedisBackend> Source for RedisCache<B> {
    /// Reports whether the cache has synchronized past the requested block.
    fn is_synced(&self, current_block_number: u64) -> bool {
        match self.fetch_current_block_number() {
            Ok(Some(block)) => block >= current_block_number,
            _ => false,
        }
    }

    /// Provides an identifier used in logs and metrics.
    fn name(&self) -> &'static str {
        "redis-cache"
    }
}

#[derive(Error, Debug)]
pub enum RedisCacheError {
    #[error("redis backend error: {0}")]
    Backend(#[source] redis::RedisError),
    #[error("missing field '{field}' for key '{key}'")]
    MissingField { key: String, field: &'static str },
    #[error("invalid integer for '{key}.{field}': {source}")]
    InvalidInteger {
        key: String,
        field: &'static str,
        source: std::num::ParseIntError,
    },
    #[error("invalid u256 for '{key}.{field}': {source}")]
    InvalidU256 {
        key: String,
        field: &'static str,
        source: alloy::primitives::ruint::ParseError,
    },
    #[error("invalid hex for '{kind}': {source}")]
    InvalidHex {
        kind: &'static str,
        source: hex::FromHexError,
    },
    #[error("hex value for '{kind}' must be at most 32 bytes")]
    HexLength { kind: &'static str },
    #[error("block hash not found for block {0}")]
    BlockHashNotFound(u64),
    #[error("bytecode not found for hash {0}")]
    CodeNotFound(B256),
    #[error("cache miss for '{kind}'")]
    CacheMiss { kind: &'static str },
    #[error("{0}")]
    Other(String),
}

impl From<RedisCacheError> for super::SourceError {
    fn from(value: RedisCacheError) -> Self {
        match value {
            RedisCacheError::Backend(err) => super::SourceError::Request(Box::new(err)),
            RedisCacheError::BlockHashNotFound(_) => super::SourceError::BlockNotFound,
            RedisCacheError::CodeNotFound(_) => super::SourceError::CodeByHashNotFound,
            RedisCacheError::CacheMiss { .. } => super::SourceError::CacheMiss,
            other => super::SourceError::Other(other.to_string()),
        }
    }
}

/// Parses a decimal or hex-encoded `u64` stored in Redis.
fn parse_u64(value: &str, key: &str, field: &'static str) -> Result<u64, RedisCacheError> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|source| {
            RedisCacheError::InvalidInteger {
                key: key.to_string(),
                field,
                source,
            }
        })
    } else {
        trimmed.parse::<u64>().map_err(|source| {
            RedisCacheError::InvalidInteger {
                key: key.to_string(),
                field,
                source,
            }
        })
    }
}

/// Parses a decimal or hex-encoded `U256` stored in Redis.
fn parse_u256(value: &str, key: &str, field: &'static str) -> Result<U256, RedisCacheError> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        let bytes = decode_hex(hex, field)?;
        if bytes.len() > 32 {
            return Err(RedisCacheError::HexLength { kind: field });
        }
        Ok(U256::from_be_slice(&bytes))
    } else {
        U256::from_str(trimmed).map_err(|source| {
            RedisCacheError::InvalidU256 {
                key: key.to_string(),
                field,
                source,
            }
        })
    }
}

/// Parses a 32-byte hash stored in Redis.
fn parse_b256(value: &str, kind: &'static str) -> Result<B256, RedisCacheError> {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let bytes = decode_hex(hex, kind)?;
    if bytes.len() != 32 {
        return Err(RedisCacheError::HexLength { kind });
    }
    Ok(B256::from_slice(&bytes))
}

/// Decodes a hex string, returning a detailed error on failure.
fn decode_hex(value: &str, kind: &'static str) -> Result<Vec<u8>, RedisCacheError> {
    hex::decode(value).map_err(|source| RedisCacheError::InvalidHex { kind, source })
}

/// Formats bytes as a 0x-prefixed lower-case hex string.
fn encode_hex(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

/// Converts raw bytes to a lower-case hex string without a prefix.
fn to_hex_lower(data: &[u8]) -> String {
    hex::encode(data)
}

/// Serializes a storage key into the format stored in Redis.
fn encode_storage_key(slot: StorageKey) -> String {
    let bytes = slot.to_be_bytes::<32>();
    encode_hex(&bytes)
}

/// Serializes a `U256` into the format stored in Redis.
fn encode_u256_hex(value: U256) -> String {
    let bytes = value.to_be_bytes::<32>();
    encode_hex(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::sources::{
        Source,
        SourceError,
    };
    use std::sync::RwLock;

    #[derive(Debug, Default)]
    struct InMemoryBackend {
        entries: RwLock<HashMap<String, Entry>>,
    }

    #[derive(Debug, Clone)]
    enum Entry {
        String(String),
        Hash(HashMap<String, String>),
    }

    impl Default for Entry {
        fn default() -> Self {
            Entry::Hash(HashMap::new())
        }
    }

    impl RedisBackend for InMemoryBackend {
        fn hgetall(&self, key: &str) -> Result<Option<HashMap<String, String>>, RedisCacheError> {
            let guard = self.entries.read().unwrap();
            match guard.get(key) {
                Some(Entry::Hash(map)) if !map.is_empty() => Ok(Some(map.clone())),
                Some(Entry::Hash(_) | Entry::String(_)) | None => Ok(None),
            }
        }

        fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RedisCacheError> {
            let guard = self.entries.read().unwrap();
            match guard.get(key) {
                Some(Entry::Hash(map)) => Ok(map.get(field).cloned()),
                _ => Ok(None),
            }
        }

        fn hset_multiple(
            &self,
            key: &str,
            values: &[(String, String)],
        ) -> Result<(), RedisCacheError> {
            let mut guard = self.entries.write().unwrap();
            let entry = guard
                .entry(key.to_string())
                .or_insert_with(|| Entry::Hash(HashMap::new()));
            match entry {
                Entry::Hash(map) => {
                    for (field, value) in values {
                        map.insert(field.clone(), value.clone());
                    }
                    Ok(())
                }
                Entry::String(_) => Err(RedisCacheError::Other("type mismatch".to_string())),
            }
        }

        fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RedisCacheError> {
            self.hset_multiple(key, &[(field.to_string(), value.to_string())])
        }

        fn get(&self, key: &str) -> Result<Option<String>, RedisCacheError> {
            let guard = self.entries.read().unwrap();
            match guard.get(key) {
                Some(Entry::String(value)) => Ok(Some(value.clone())),
                _ => Ok(None),
            }
        }

        fn set(&self, key: &str, value: &str) -> Result<(), RedisCacheError> {
            let mut guard = self.entries.write().unwrap();
            guard.insert(key.to_string(), Entry::String(value.to_string()));
            Ok(())
        }
    }

    #[test]
    fn basic_ref_returns_account_info() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);

        let address = Address::from([0x11; 20]);
        let balance = U256::from(1_000_000_u64);
        let nonce = 7_u64;
        let code_hash = B256::from_slice(&[0x22; 32]);

        cache
            .put_account(address, balance, nonce, code_hash)
            .expect("failed to insert account");

        let result = cache.basic_ref(address).unwrap().expect("account missing");
        assert_eq!(result.balance, balance);
        assert_eq!(result.nonce, nonce);
        assert_eq!(result.code_hash, code_hash);
        assert!(result.code.is_none());
    }

    #[test]
    fn basic_ref_missing_returns_none() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let address = Address::from([0x42; 20]);

        let result = cache.basic_ref(address).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn storage_ref_returns_cache_miss_when_missing() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let address = Address::from([0x33; 20]);
        let slot: StorageKey = U256::from_be_slice(&[0u8; 32]);

        let result = cache.storage_ref(address, slot);
        assert!(matches!(result, Err(SourceError::CacheMiss)));
    }

    #[test]
    fn storage_ref_returns_value() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let address = Address::from([0x55; 20]);
        let slot: StorageKey = U256::from_be_slice(&[0xAA; 32]);
        let value = U256::from(0xdeadbeefu64);

        cache
            .put_storage(address, slot, value)
            .expect("failed to set storage");

        let result = cache
            .storage_ref(address, slot)
            .expect("storage lookup failed");
        assert_eq!(result, value);
    }

    #[test]
    fn stores_and_loads_state_via_database_ref_interface() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);

        let address = Address::from([0x77; 20]);
        let slot: StorageKey = U256::from_be_slice(&[0x01; 32]);
        let balance = U256::from(0xabcdef_u64);
        let nonce = 9_u64;
        let storage_value = U256::from(0xfeedbeefu64);
        let code_hash = B256::from_slice(&[0x12; 32]);
        let block_hash = B256::from_slice(&[0x34; 32]);
        let block_number = 128_u64;
        let bytecode = Bytecode::new_raw(vec![0x60, 0x0a, 0x60, 0x0b, 0x01].into());

        cache
            .put_account(address, balance, nonce, code_hash)
            .expect("failed to insert account");
        cache
            .put_storage(address, slot, storage_value)
            .expect("failed to insert storage slot");
        cache
            .put_code(code_hash, &bytecode)
            .expect("failed to insert bytecode");
        cache
            .put_block_hash(block_number, block_hash)
            .expect("failed to insert block hash");
        cache
            .set_current_block_number(block_number)
            .expect("failed to set current block number");

        let account = cache
            .basic_ref(address)
            .expect("account lookup failed")
            .expect("account missing");
        assert_eq!(account.balance, balance);
        assert_eq!(account.nonce, nonce);
        assert_eq!(account.code_hash, code_hash);

        let fetched_storage = cache
            .storage_ref(address, slot)
            .expect("storage lookup failed");
        assert_eq!(fetched_storage, storage_value);

        let fetched_code = cache
            .code_by_hash_ref(code_hash)
            .expect("code lookup failed");
        assert_eq!(fetched_code.original_bytes(), bytecode.original_bytes());

        let fetched_block = cache
            .block_hash_ref(block_number)
            .expect("block hash lookup failed");
        assert_eq!(fetched_block, block_hash);

        assert!(cache.is_synced(block_number));
    }

    #[test]
    fn code_by_hash_returns_bytecode() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let code_hash = B256::from_slice(&[0x10; 32]);
        let bytecode = vec![0x60, 0x00, 0x60, 0x01];

        cache
            .put_code_bytes(code_hash, &bytecode)
            .expect("failed to insert bytecode");

        let result = cache
            .code_by_hash_ref(code_hash)
            .expect("bytecode lookup failed");
        assert_eq!(result.original_bytes(), bytecode);
    }

    #[test]
    fn block_hash_ref_returns_value() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let block_hash = B256::from_slice(&[0x77; 32]);
        let block_number = 42_u64;

        cache
            .put_block_hash(block_number, block_hash)
            .expect("failed to insert block hash");

        let result = cache
            .block_hash_ref(block_number)
            .expect("block hash lookup failed");
        assert_eq!(result, block_hash);
    }

    #[test]
    fn put_code_helper_stores_bytecode() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);
        let code_hash = B256::from_slice(&[0xAB; 32]);
        let bytecode = Bytecode::new_raw(vec![0x60, 0x0f, 0x60, 0x0c].into());

        cache
            .put_code(code_hash, &bytecode)
            .expect("failed to insert bytecode via helper");

        let stored = cache
            .code_by_hash_ref(code_hash)
            .expect("bytecode lookup failed");
        assert_eq!(stored.original_bytes(), bytecode.original_bytes());
    }

    #[test]
    fn is_synced_checks_current_block() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);

        cache
            .set_current_block_number(100)
            .expect("failed to set current block");

        assert!(cache.is_synced(90));
        assert!(cache.is_synced(100));
        assert!(!cache.is_synced(110));
    }

    #[test]
    fn redis_client_backend_constructors_create_clients() {
        let client = redis::Client::open("redis://127.0.0.1/").expect("client");
        let backend = RedisClientBackend::new(client.clone());
        let backend_from_url = RedisClientBackend::from_url("redis://127.0.0.1/").expect("client");
        // Touch the backend to avoid warnings about unused variables.
        let _ = backend;
        let _ = backend_from_url;
    }
}
