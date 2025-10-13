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

use crate::{
    Source,
    cache::sources::SourceName,
    critical,
};
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
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    task::{
        self,
        JoinHandle,
    },
    time,
};

/// Prefix used to group all cache keys.
const DEFAULT_NAMESPACE: &str = "state";
const ACCOUNT_PREFIX: &str = "account";
const STORAGE_PREFIX: &str = "storage";
const CODE_PREFIX: &str = "code";
const BLOCK_HASH_PREFIX: &str = "block_hash";
const CURRENT_BLOCK_KEY: &str = "current_block";
/// How frequently (in milliseconds) the background task polls Redis for the current block.
const CURRENT_BLOCK_REFRESH_INTERVAL_MS: Duration = Duration::from_millis(75);

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
    /// Ensures a background refresh service keeps the current block cached locally.
    fn ensure_current_block_service(
        &self,
        current_block_key: String,
        interval: Duration,
    ) -> Result<(), RedisCacheError>;
    /// Returns the last current block observed by the refresh service, if any.
    fn cached_current_block(&self) -> Option<u64>;
    /// Updates the cached current block value maintained by the backend.
    fn update_cached_current_block(&self, value: Option<u64>);
}

/// Redis backend that delegates commands to `redis::Client`.
pub struct RedisClientBackend {
    client: redis::Client,
    /// Stores the freshest block height pulled from Redis.
    current_block_cache: Arc<AtomicU64>,
    /// Marks whether the cache has been initialised by the refresher.
    current_block_initialized: Arc<AtomicBool>,
    /// Manages the task that keeps `current_block_cache` up to date.
    refresh: Arc<RefreshCoordinator>,
}

/// Coordinates the task that keeps the cached block height fresh.
struct RefreshCoordinator {
    started: AtomicBool,
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl RefreshCoordinator {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            stop: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
        }
    }

    /// Stores the background task handle so we can later abort it.
    fn set_handle(&self, handle: JoinHandle<()>) {
        let mut guard = self.handle.lock().expect("refresh handle poisoned");
        *guard = Some(handle);
    }

    /// Returns a clone of the stop flag that the refresh task observes.
    fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }
}

impl Drop for RefreshCoordinator {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(mut guard) = self.handle.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }
}

impl Clone for RedisClientBackend {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            current_block_cache: self.current_block_cache.clone(),
            current_block_initialized: self.current_block_initialized.clone(),
            refresh: self.refresh.clone(),
        }
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
        Self {
            client,
            current_block_cache: Arc::new(AtomicU64::new(0)),
            current_block_initialized: Arc::new(AtomicBool::new(false)),
            refresh: Arc::new(RefreshCoordinator::new()),
        }
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
        let mut connection = match self.client.get_connection() {
            Ok(connection) => connection,
            Err(err) => {
                critical!(error = ?err, "redis backend connection error");
                return Err(RedisCacheError::Backend(err));
            }
        };

        match func(&mut connection) {
            Ok(result) => Ok(result),
            Err(err) => {
                critical!(error = ?err, "redis backend command error");
                Err(RedisCacheError::Backend(err))
            }
        }
    }

    /// Updates the local atomic cache with the latest block height observed by the refresher.
    fn set_cached_current_block_value(&self, value: Option<u64>) {
        Self::write_cached_current_block(
            &self.current_block_cache,
            &self.current_block_initialized,
            value,
        );
    }

    fn write_cached_current_block(
        cache: &Arc<AtomicU64>,
        initialized: &Arc<AtomicBool>,
        value: Option<u64>,
    ) {
        match value {
            Some(block) => {
                cache.store(block, Ordering::Release);
                initialized.store(true, Ordering::Release);
            }
            None => {
                initialized.store(false, Ordering::Release);
            }
        }
    }

    /// Returns the cached block height when the refresher has populated it at least once.
    fn cached_current_block_value(&self) -> Option<u64> {
        Self::read_cached_current_block(&self.current_block_cache, &self.current_block_initialized)
    }

    fn read_cached_current_block(
        cache: &Arc<AtomicU64>,
        initialized: &Arc<AtomicBool>,
    ) -> Option<u64> {
        if initialized.load(Ordering::Acquire) {
            Some(cache.load(Ordering::Acquire))
        } else {
            None
        }
    }

    /// Reads the current block straight from Redis when the local cache is cold.
    fn read_current_block(&self, current_block_key: &str) -> Result<Option<u64>, RedisCacheError> {
        Self::read_current_block_with_client(&self.client, current_block_key)
    }

    /// Starts the task that periodically refreshes our cached block height.
    fn spawn_current_block_refresh(
        &self,
        current_block_key: String,
        interval: Duration,
    ) -> Result<(), RedisCacheError> {
        if self.refresh.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let client = self.client.clone();
        let cache = self.current_block_cache.clone();
        let initialized = self.current_block_initialized.clone();
        let stop_flag = self.refresh.stop_flag();
        let handle = Handle::try_current()
            .map_err(|err| {
                RedisCacheError::Other(format!("Error trying to spawn block refresh task: {err}"))
            })?
            .spawn(Self::refresh_current_block_loop(
                client,
                cache,
                initialized,
                stop_flag.clone(),
                current_block_key,
                interval,
            ));

        self.refresh.set_handle(handle);
        Ok(())
    }

    /// Worker loop that polls Redis at the configured interval and keeps the cache warm.
    async fn refresh_current_block_loop(
        client: redis::Client,
        current_block_cache: Arc<AtomicU64>,
        current_block_initialized: Arc<AtomicBool>,
        stop_flag: Arc<AtomicBool>,
        current_block_key: String,
        interval: Duration,
    ) {
        let mut ticker = time::interval(interval);
        loop {
            if stop_flag.load(Ordering::Acquire) {
                break;
            }

            let key = current_block_key.clone();
            let client_clone = client.clone();
            let result = task::spawn_blocking(move || {
                Self::read_current_block_with_client(&client_clone, &key)
            })
            .await;

            match result {
                Ok(Ok(Some(block))) => {
                    Self::write_cached_current_block(
                        &current_block_cache,
                        &current_block_initialized,
                        Some(block),
                    );
                }
                Ok(Ok(None)) => {
                    Self::write_cached_current_block(
                        &current_block_cache,
                        &current_block_initialized,
                        None,
                    );
                }
                Ok(Err(err)) => {
                    critical!(error = ?err, "failed to refresh current block from redis");
                }
                Err(err) => {
                    critical!(error = ?err, "redis current block refresh task join error");
                }
            }

            if stop_flag.load(Ordering::Acquire) {
                break;
            }

            ticker.tick().await;
        }
    }

    fn read_current_block_with_client(
        client: &redis::Client,
        current_block_key: &str,
    ) -> Result<Option<u64>, RedisCacheError> {
        let mut connection = match client.get_connection() {
            Ok(connection) => connection,
            Err(err) => {
                critical!(error = ?err, "redis backend connection error");
                return Err(RedisCacheError::Backend(err));
            }
        };

        let response = match connection.get::<_, Option<String>>(current_block_key) {
            Ok(value) => value,
            Err(err) => {
                critical!(error = ?err, "redis backend command error");
                return Err(RedisCacheError::Backend(err));
            }
        };

        response
            .map(|value| parse_u64(&value, current_block_key, CURRENT_BLOCK_KEY))
            .transpose()
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

    fn ensure_current_block_service(
        &self,
        current_block_key: String,
        interval: Duration,
    ) -> Result<(), RedisCacheError> {
        self.spawn_current_block_refresh(current_block_key, interval)
    }

    fn cached_current_block(&self) -> Option<u64> {
        self.cached_current_block_value()
    }

    fn update_cached_current_block(&self, value: Option<u64>) {
        self.set_cached_current_block_value(value);
    }
}

#[derive(Debug)]
pub struct RedisCache<B: RedisBackend> {
    backend: B,
    namespace: String,
    /// Current block
    current_block: Arc<AtomicU64>,
}

impl<B: RedisBackend> RedisCache<B> {
    /// Creates a cache that stores entries under the default `state` namespace.
    pub fn new(backend: B) -> Self {
        Self::with_namespace(backend, DEFAULT_NAMESPACE)
    }

    /// Creates a cache that stores entries under a custom namespace prefix and wires up the
    /// background current block refresher.
    pub fn with_namespace(backend: B, namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let current_block_key = format!("{namespace}:{CURRENT_BLOCK_KEY}");
        if let Err(err) = backend
            .ensure_current_block_service(current_block_key, CURRENT_BLOCK_REFRESH_INTERVAL_MS)
        {
            critical!(error = ?err, "failed to start redis current block refresh service");
        }

        Self {
            backend,
            namespace,
            current_block: Arc::new(AtomicU64::new(0)),
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

    /// Records the highest block number that has been synchronized into the cache and primes the
    /// in-process cache with the same value.
    pub fn set_current_block_number(&self, block_number: u64) -> Result<(), RedisCacheError> {
        self.backend
            .set(&self.current_block_key(), &block_number.to_string())?;
        self.backend.update_cached_current_block(Some(block_number));
        Ok(())
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
    fn is_synced(&self, required_block_number: u64) -> bool {
        if let Some(block) = self.backend.cached_current_block() {
            return block >= required_block_number
                && block <= self.current_block.load(Ordering::Relaxed);
        }

        match self.fetch_current_block_number() {
            Ok(Some(block)) => {
                self.backend.update_cached_current_block(Some(block));
                block >= required_block_number
                    && block <= self.current_block.load(Ordering::Relaxed)
            }
            Ok(None) => {
                self.backend.update_cached_current_block(None);
                false
            }
            Err(err) => {
                critical!(error = ?err, "failed to read current block number");
                false
            }
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

/// Parses a decimal `u64` stored in Redis.
fn parse_u64(value: &str, key: &str, field: &'static str) -> Result<u64, RedisCacheError> {
    value.trim().parse::<u64>().map_err(|source| {
        RedisCacheError::InvalidInteger {
            key: key.to_string(),
            field,
            source,
        }
    })
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
    use std::{
        sync::{
            Arc,
            RwLock,
            atomic::{
                AtomicBool,
                AtomicU64,
                Ordering,
            },
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct InMemoryBackend {
        entries: RwLock<HashMap<String, Entry>>,
        /// Mirrors the production refresher by holding the last observed block height.
        current_block: Arc<AtomicU64>,
        /// Indicates if the cached block height has been initialised.
        current_block_initialized: Arc<AtomicBool>,
    }

    impl Default for InMemoryBackend {
        fn default() -> Self {
            Self {
                entries: RwLock::new(HashMap::new()),
                current_block: Arc::new(AtomicU64::new(0)),
                current_block_initialized: Arc::new(AtomicBool::new(false)),
            }
        }
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
            drop(guard);

            if key.ends_with(CURRENT_BLOCK_KEY) {
                let block = parse_u64(value, key, CURRENT_BLOCK_KEY)?;
                self.update_cached_current_block(Some(block));
            }

            Ok(())
        }

        fn ensure_current_block_service(
            &self,
            current_block_key: String,
            _interval: Duration,
        ) -> Result<(), RedisCacheError> {
            let entry = {
                let guard = self.entries.read().unwrap();
                guard.get(&current_block_key).cloned()
            };

            match entry {
                Some(Entry::String(value)) => {
                    let block = parse_u64(&value, &current_block_key, CURRENT_BLOCK_KEY)?;
                    self.update_cached_current_block(Some(block));
                }
                _ => self.update_cached_current_block(None),
            }

            Ok(())
        }

        fn cached_current_block(&self) -> Option<u64> {
            if self.current_block_initialized.load(Ordering::Acquire) {
                Some(self.current_block.load(Ordering::Acquire))
            } else {
                None
            }
        }

        fn update_cached_current_block(&self, value: Option<u64>) {
            match value {
                Some(block) => {
                    self.current_block.store(block, Ordering::Release);
                    self.current_block_initialized
                        .store(true, Ordering::Release);
                }
                None => {
                    self.current_block_initialized
                        .store(false, Ordering::Release);
                }
            }
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
        cache.update_target_block(block_number);
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

        cache.update_target_block(110);
        cache
            .set_current_block_number(100)
            .expect("failed to set current block");

        assert!(cache.is_synced(90));
        assert!(cache.is_synced(100));
        assert!(!cache.is_synced(110));
    }

    #[test]
    fn is_synced_checks_taget_block() {
        let backend = InMemoryBackend::default();
        let cache = RedisCache::new(backend);

        cache.update_target_block(89);
        cache
            .set_current_block_number(100)
            .expect("failed to set current block");

        assert!(!cache.is_synced(90));
        assert!(!cache.is_synced(100));
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
