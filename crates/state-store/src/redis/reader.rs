//! State reader implementation for querying blockchain state from Redis.

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    ReadStats,
    Reader,
    redis::{
        CircularBufferConfig,
        common::{
            RedisStateClient,
            decode_b256,
            decode_bytes,
            decode_u256,
            encode_b256,
            error::{
                StateError,
                StateResult,
            },
            get_account_key,
            get_all_accounts_pattern,
            get_block_hash_key,
            get_block_key,
            get_code_key,
            get_diff_key,
            get_namespace_for_block,
            get_state_root_key,
            get_storage_key,
            read_latest_block_number,
            read_namespace_lock,
        },
    },
};
use alloy::primitives::{
    B256,
    Bytes,
    KECCAK256_EMPTY,
    U256,
};
use redis::Pipeline;
use std::{
    collections::HashMap,
    time::Instant,
};
use tracing::{
    instrument,
    trace,
};

/// Thin wrapper for reading blockchain state from Redis.
#[derive(Clone, Debug)]
pub struct StateReader {
    client: RedisStateClient,
}

impl Reader for StateReader {
    type Error = StateError;

    /// Get the most recent block number from metadata (O(1) operation).
    /// Falls back to scanning all namespaces if metadata is unavailable.
    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client
            .with_connection(move |conn| read_latest_block_number(conn, &base_namespace))
    }

    /// Check if a specific block number is available in the circular buffer.
    ///
    /// Note: This checks availability without considering locks.
    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            is_block_available(conn, &base_namespace, buffer_size, block_number)
        })
    }

    /// Get account info without storage slots (balance, nonce, code hash, code only).
    /// This is the recommended method for most use cases to avoid large data transfers.
    /// Use `get_full_account` or `get_all_storage` separately if storage is needed.
    ///
    /// Returns an error if the namespace is locked for writing.
    #[instrument(skip(self), level = "trace")]
    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountInfo>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            get_account_at_block(
                conn,
                &base_namespace,
                buffer_size,
                &address_hash,
                block_number,
            )
        })
    }

    /// Get a specific storage slot for an account at a block.
    ///
    /// The `slot_hash` parameter should be the `keccak256` hash of the original 32-byte slot index,
    /// matching the format persisted by the writer (and Nethermind state dumps).
    ///
    /// Returns an error if the namespace is locked for writing.
    #[instrument(skip(self), level = "trace")]
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let slot = U256::from_be_bytes(slot_hash.into());

        self.client.with_connection(move |conn| {
            get_storage_at_block(
                conn,
                &base_namespace,
                buffer_size,
                &address_hash,
                slot,
                block_number,
            )
        })
    }

    /// Get all storage slots for an account at a block.
    ///
    /// Returned map keys are `keccak256(slot)` digests corresponding to the original storage slots.
    ///
    /// Returns an error if the namespace is locked for writing.
    #[instrument(skip(self), level = "debug")]
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<HashMap<B256, U256>> {
        let start = Instant::now();
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        let result = self.client.with_connection(move |conn| {
            let result = get_all_storage_at_block(
                conn,
                &base_namespace,
                buffer_size,
                &address_hash,
                block_number,
            )?;
            // Convert HashMap<U256, B256> to HashMap<B256, U256>
            Ok(result
                .into_iter()
                .map(|(slot, value)| {
                    (
                        B256::from(slot.to_be_bytes()),
                        U256::from_be_bytes(value.into()),
                    )
                })
                .collect::<HashMap<_, _>>())
        })?;

        trace!(
            slots = result.len(),
            duration_us = start.elapsed().as_micros(),
            "get_all_storage complete"
        );

        Ok(result)
    }

    /// Get contract bytecode by code hash.
    ///
    /// Returns an error if the namespace is locked for writing.
    #[instrument(skip(self), level = "trace")]
    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            let result =
                get_code_at_block(conn, &base_namespace, buffer_size, code_hash, block_number)?;
            Ok(result.map(Bytes::from))
        })
    }

    /// Get complete account state including storage.
    ///
    /// WARNING: This can transfer large amounts of data for contracts with many slots.
    #[instrument(skip(self), level = "debug")]
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        let start = Instant::now();
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        let result = self.client.with_connection(move |conn| {
            let result = get_full_account_at_block(
                conn,
                &base_namespace,
                buffer_size,
                &address_hash,
                block_number,
            )?;
            // Convert internal AccountState to crate-level AccountState
            Ok(result.map(|a| {
                AccountState {
                    address_hash: a.address_hash,
                    balance: a.balance,
                    nonce: a.nonce,
                    code_hash: a.code_hash,
                    code: a.code.map(Bytes::from),
                    storage: a
                        .storage
                        .into_iter()
                        .map(|(k, v)| (B256::from(k.to_be_bytes()), v))
                        .collect(),
                    deleted: a.deleted,
                }
            }))
        })?;

        if let Some(ref acc) = result {
            trace!(
                storage_slots = acc.storage.len(),
                has_code = acc.code.is_some(),
                duration_us = start.elapsed().as_micros(),
                "get_full_account complete"
            );
        }

        Ok(result)
    }

    /// Get block hash for a specific block number.
    ///
    /// Note: Block hashes are stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client
            .with_connection(move |conn| get_block_hash(conn, &base_namespace, block_number))
    }

    /// Get state root for a specific block number.
    ///
    /// Note: State roots are stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client
            .with_connection(move |conn| get_state_root(conn, &base_namespace, block_number))
    }

    /// Get complete block metadata (hash and state root).
    /// Optimized to fetch both in a single roundtrip.
    ///
    /// Note: Block metadata is stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client.with_connection(move |conn| {
            let block_hash_key = get_block_hash_key(&base_namespace, block_number);
            let state_root_key = get_state_root_key(&base_namespace, block_number);

            // Use pipeline for single roundtrip
            let mut pipe = Pipeline::new();
            pipe.get(&block_hash_key).get(&state_root_key);

            let results: Vec<Option<String>> = pipe.query(conn)?;

            let block_hash = results
                .first()
                .and_then(|o| o.as_ref())
                .map(|s| decode_b256(s))
                .transpose()?;

            let state_root = results
                .get(1)
                .and_then(|o| o.as_ref())
                .map(|s| decode_b256(s))
                .transpose()?;

            match (block_hash, state_root) {
                (Some(hash), Some(root)) => {
                    Ok(Some(BlockMetadata {
                        block_number,
                        block_hash: hash,
                        state_root: root,
                    }))
                }
                _ => Ok(None),
            }
        })
    }

    /// Get the range of available blocks [oldest, latest].
    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        let latest = self.latest_block_number()?;

        if let Some(latest_block) = latest {
            let buffer_size = u64::from(self.client.buffer_config.buffer_size);
            let oldest_block = latest_block.saturating_sub(buffer_size - 1);
            Ok(Some((oldest_block, latest_block)))
        } else {
            Ok(None)
        }
    }

    /// Scan all account hashes (keccak of addresses) in a namespace for a specific block.
    /// Returns a list of all keccak(address) hashes that have account data stored.
    ///
    /// Returns an error if the namespace is locked for writing.
    #[instrument(skip(self), level = "debug")]
    fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        let start = Instant::now();
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        let result = self.client.with_connection(move |conn| {
            scan_account_hashes_at_block(conn, &base_namespace, buffer_size, block_number)
        })?;

        trace!(
            accounts = result.len(),
            duration_us = start.elapsed().as_micros(),
            "scan_account_hashes complete"
        );

        Ok(result)
    }

    /// Check if a state diff exists for the given block.
    ///
    /// Used for recovery to identify missing intermediate diffs.
    fn has_state_diff(&self, block_number: u64) -> StateResult<bool> {
        let base_namespace = self.client.base_namespace.clone();

        self.client.with_connection(move |conn| {
            let diff_key = get_diff_key(&base_namespace, block_number);
            let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(conn)?;
            Ok(exists)
        })
    }

    /// Get the current block number stored in a specific namespace.
    ///
    /// Returns `None` if the namespace is empty (no blocks written yet).
    fn get_namespace_block(&self, namespace_idx: u8) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                u64::from(namespace_idx),
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            let namespace = format!("{base_namespace}:{namespace_idx}");
            let block_key = get_block_key(&namespace);
            let block_str: Option<String> = redis::cmd("GET").arg(&block_key).query(conn)?;
            parse_namespace_block(block_str)
        })
    }

    /// Get the buffer size configuration.
    fn buffer_size(&self) -> u8 {
        self.client.buffer_config.buffer_size
    }
}

impl StateReader {
    /// Create a new reader.
    pub fn new(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace.to_string(), buffer_config)?;
        Ok(Self { client })
    }

    /// Create a new reader from an existing client.
    pub(crate) fn from_client(client: RedisStateClient) -> Self {
        Self { client }
    }

    /// Get a reference to the underlying client.
    pub(crate) fn client(&self) -> &RedisStateClient {
        &self.client
    }

    /// Check if a specific block number is available and not locked for writing.
    pub fn is_block_readable(&self, block_number: u64) -> StateResult<bool> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            // First check if block is available
            if !is_block_available(conn, &base_namespace, buffer_size, block_number)? {
                return Ok(false);
            }

            // Then check if the namespace is locked
            let namespace = get_namespace_for_block(&base_namespace, block_number, buffer_size)?;
            if read_namespace_lock(conn, &namespace)?.is_some() {
                return Ok(false);
            }

            Ok(true)
        })
    }

    /// Get all storage slots for an account with statistics.
    ///
    /// Same as `get_all_storage` but returns additional timing information.
    #[instrument(skip(self), level = "debug")]
    pub fn get_all_storage_with_stats(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<(HashMap<B256, U256>, ReadStats)> {
        let start = Instant::now();
        let storage = self.get_all_storage(address_hash, block_number)?;

        let stats = ReadStats {
            storage_slots_read: storage.len(),
            duration: start.elapsed(),
        };

        Ok((storage, stats))
    }
}

// ============================================================================
// Internal helper functions
// ============================================================================

/// Internal account state for Redis (uses different storage key format)
#[derive(Debug, Clone)]
pub(crate) struct InternalAccountState {
    pub address_hash: AddressHash,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    pub code: Option<Vec<u8>>,
    pub storage: HashMap<U256, U256>,
    pub deleted: bool,
}

/// Parse namespace block number from optional string.
fn parse_namespace_block(namespace_block_str: Option<String>) -> StateResult<Option<u64>> {
    namespace_block_str
        .map(|s| {
            s.parse::<u64>()
                .map_err(|e| StateError::ParseInt(s.clone(), e))
        })
        .transpose()
}

/// Verify the namespace contains the expected block number.
fn verify_namespace_block(namespace_block: Option<u64>, expected: u64) -> StateResult<()> {
    if namespace_block != Some(expected) {
        return Err(StateError::BlockNotFound {
            block_number: expected,
        });
    }
    Ok(())
}

/// Check that namespace is not locked for writing.
/// Returns an error if the namespace has a write lock.
fn ensure_namespace_not_locked<C>(conn: &mut C, namespace: &str) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    if let Some(lock) = read_namespace_lock(conn, namespace)? {
        return Err(StateError::NamespaceLocked {
            namespace: namespace.to_string(),
            target_block: lock.target_block,
            started_at: lock.started_at,
        });
    }
    Ok(())
}

/// Get account info without storage slots.
/// This is the lightweight version that avoids loading potentially large storage.
pub(crate) fn get_account_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    address_hash: &AddressHash,
    block_number: u64,
) -> StateResult<Option<AccountInfo>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);
    let account_key = get_account_key(&namespace, address_hash);

    // Atomic read: fetch namespace block number AND account data in single pipeline
    let mut pipe = Pipeline::new();
    pipe.get(&block_key).hgetall(&account_key);

    let results: (Option<String>, HashMap<String, String>) = pipe.query(conn)?;

    let (namespace_block_str, account_fields) = results;

    // Verify block number after atomic read
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    if account_fields.is_empty() {
        return Ok(None);
    }

    let balance = account_fields
        .get("balance")
        .ok_or(StateError::MissingField("balance"))?;
    let nonce = account_fields
        .get("nonce")
        .ok_or(StateError::MissingField("nonce"))?;
    let code_hash = account_fields
        .get("code_hash")
        .ok_or(StateError::MissingField("code_hash"))?;

    let code_hash_decoded = decode_b256(code_hash)?;

    let balance_parsed =
        U256::from_str_radix(balance, 10).map_err(|e| StateError::ParseU256(balance.clone(), e))?;
    let nonce_parsed = nonce
        .parse()
        .map_err(|e| StateError::ParseInt(nonce.clone(), e))?;

    Ok(Some(AccountInfo {
        address_hash: *address_hash,
        balance: balance_parsed,
        nonce: nonce_parsed,
        code_hash: code_hash_decoded,
    }))
}

/// Get the complete account state including code and storage in a single roundtrip.
/// WARNING: This can transfer large amounts of data for contracts with many storage slots.
pub(crate) fn get_full_account_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    address_hash: &AddressHash,
    block_number: u64,
) -> StateResult<Option<InternalAccountState>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);
    let account_key = get_account_key(&namespace, address_hash);
    let storage_key = get_storage_key(&namespace, address_hash);

    // Atomic read: fetch namespace block number AND data in single pipeline
    let mut pipe = Pipeline::new();
    pipe.get(&block_key)
        .hgetall(&account_key)
        .hgetall(&storage_key);

    let results: (
        Option<String>,
        HashMap<String, String>,
        HashMap<String, String>,
    ) = pipe.query(conn)?;

    let (namespace_block_str, account_fields, storage_fields) = results;

    // Verify block number after atomic read
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    if account_fields.is_empty() {
        return Ok(None);
    }

    let balance = account_fields
        .get("balance")
        .ok_or(StateError::MissingField("balance"))?;
    let nonce = account_fields
        .get("nonce")
        .ok_or(StateError::MissingField("nonce"))?;
    let code_hash = account_fields
        .get("code_hash")
        .ok_or(StateError::MissingField("code_hash"))?;

    let code_hash_decoded = decode_b256(code_hash)?;

    // Parse storage slots
    let mut storage = HashMap::new();
    for (slot_hex, value_hex) in storage_fields {
        let slot_hash = decode_b256(&slot_hex)?;
        let slot = U256::from_be_bytes(slot_hash.into());
        let value = decode_u256(&value_hex)?;
        storage.insert(slot, value);
    }

    // Fetch code if code_hash is non-empty
    // Note: This is a separate read, but code is content-addressed so it's safe
    let code = if code_hash_decoded == KECCAK256_EMPTY {
        None
    } else {
        let code_hash_hex = hex::encode(code_hash_decoded);
        let code_key = get_code_key(&namespace, &code_hash_hex);

        let value: Option<String> = redis::cmd("GET").arg(&code_key).query(conn)?;

        value.map(|v| decode_bytes(&v)).transpose()?
    };

    let balance_parsed =
        U256::from_str_radix(balance, 10).map_err(|e| StateError::ParseU256(balance.clone(), e))?;
    let nonce_parsed = nonce
        .parse()
        .map_err(|e| StateError::ParseInt(nonce.clone(), e))?;

    Ok(Some(InternalAccountState {
        address_hash: *address_hash,
        balance: balance_parsed,
        nonce: nonce_parsed,
        code_hash: code_hash_decoded,
        code,
        storage,
        deleted: false,
    }))
}

/// Get a storage slot at a specific block.
pub(crate) fn get_storage_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    address_hash: &AddressHash,
    slot: U256,
    block_number: u64,
) -> StateResult<Option<U256>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);
    let storage_key = get_storage_key(&namespace, address_hash);
    let slot_hash = B256::from(slot.to_be_bytes::<32>());
    let slot_hex = encode_b256(slot_hash);

    // Atomic read: fetch namespace block number AND storage slot together
    let mut pipe = Pipeline::new();
    pipe.get(&block_key)
        .cmd("HGET")
        .arg(&storage_key)
        .arg(&slot_hex);

    let results: (Option<String>, Option<String>) = pipe.query(conn)?;

    let (namespace_block_str, value) = results;

    // Verify block number after atomic read
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    value.map(|v| decode_u256(&v)).transpose()
}

/// Get all storage slots for an account at a block.
pub(crate) fn get_all_storage_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    address_hash: &AddressHash,
    block_number: u64,
) -> StateResult<HashMap<U256, B256>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);
    let storage_key = get_storage_key(&namespace, address_hash);

    // Atomic read: fetch namespace block number AND all storage together
    let mut pipe = Pipeline::new();
    pipe.get(&block_key).hgetall(&storage_key);

    let results: (Option<String>, HashMap<String, String>) = pipe.query(conn)?;

    let (namespace_block_str, fields) = results;

    // Verify block number after atomic read
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    let mut result = HashMap::new();
    for (slot_hex, value_hex) in fields {
        let slot_hash = decode_b256(&slot_hex)?;
        let slot = U256::from_be_bytes(slot_hash.into());
        let value = decode_b256(&value_hex)?;
        result.insert(slot, value);
    }

    Ok(result)
}

/// Get contract code at a specific block.
pub(crate) fn get_code_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    code_hash: B256,
    block_number: u64,
) -> StateResult<Option<Vec<u8>>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);
    let code_hash_hex = hex::encode(code_hash);
    let code_key = get_code_key(&namespace, &code_hash_hex);

    // Atomic read: fetch namespace block number AND code together
    let mut pipe = Pipeline::new();
    pipe.get(&block_key).get(&code_key);

    let results: (Option<String>, Option<String>) = pipe.query(conn)?;

    let (namespace_block_str, value) = results;

    // Verify block number after atomic read
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    value.map(|v| decode_bytes(&v)).transpose()
}

/// Scan all account hashes at a specific block.
/// Keys are stored as: namespace:account:KECCAK(ADDRESS)_HEX
///
/// Note: SCAN is inherently non-atomic, so we verify the namespace block
/// both before and after the scan to detect race conditions.
fn scan_account_hashes_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    block_number: u64,
) -> StateResult<Vec<AddressHash>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check lock before reading
    ensure_namespace_not_locked(conn, &namespace)?;

    let block_key = get_block_key(&namespace);

    // Verify namespace contains the requested block before scanning
    let namespace_block_str: Option<String> = redis::cmd("GET").arg(&block_key).query(conn)?;
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    // Scan for all account keys in this namespace
    let pattern = get_all_accounts_pattern(&namespace);
    let mut cursor = 0u64;
    let mut hashes = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100)
            .query(conn)?;

        for key in keys {
            if let Some(hash_hex) = key.strip_prefix(&format!("{namespace}:account:")) {
                let hash_hex = hash_hex.strip_prefix("0x").unwrap_or(hash_hex);

                let bytes = hex::decode(hash_hex)
                    .map_err(|e| StateError::HexDecode(hash_hex.to_string(), e))?;

                if bytes.len() == 32 {
                    hashes.push(B256::from_slice(&bytes).into());
                } else {
                    return Err(StateError::InvalidB256Length(bytes.len()));
                }
            }
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    // Re-verify namespace block hasn't changed during scan
    let namespace_block_str: Option<String> = redis::cmd("GET").arg(&block_key).query(conn)?;
    let namespace_block = parse_namespace_block(namespace_block_str)?;
    verify_namespace_block(namespace_block, block_number)?;

    // Re-check lock after scan (writer might have started during scan)
    ensure_namespace_not_locked(conn, &namespace)?;

    Ok(hashes)
}

/// Get block hash (shared across namespaces).
pub(crate) fn get_block_hash<C>(
    conn: &mut C,
    base_namespace: &str,
    block_number: u64,
) -> StateResult<Option<B256>>
where
    C: redis::ConnectionLike,
{
    let key = get_block_hash_key(base_namespace, block_number);
    let value: Option<String> = redis::cmd("GET").arg(&key).query(conn)?;

    value.map(|v| decode_b256(&v)).transpose()
}

/// Get state root (shared across namespaces).
pub(crate) fn get_state_root<C>(
    conn: &mut C,
    base_namespace: &str,
    block_number: u64,
) -> StateResult<Option<B256>>
where
    C: redis::ConnectionLike,
{
    let key = get_state_root_key(base_namespace, block_number);
    let value: Option<String> = redis::cmd("GET").arg(&key).query(conn)?;

    value.map(|v| decode_b256(&v)).transpose()
}

/// Check if a block is available in the circular buffer.
pub(crate) fn is_block_available<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: u8,
    block_number: u64,
) -> StateResult<bool>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let block_key = get_block_key(&namespace);

    let namespace_block_str: Option<String> = redis::cmd("GET").arg(&block_key).query(conn)?;
    let namespace_block = parse_namespace_block(namespace_block_str)?;

    Ok(namespace_block == Some(block_number))
}
