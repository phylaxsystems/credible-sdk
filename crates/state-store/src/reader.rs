//! State reader implementation for querying blockchain state from Redis.

use crate::{
    CircularBufferConfig,
    common::{
        AccountState,
        BlockMetadata,
        RedisStateClient,
        decode_b256,
        decode_bytes,
        decode_u256,
        encode_u256,
        error::{
            StateError,
            StateResult,
        },
        get_account_key,
        get_block_hash_key,
        get_code_key,
        get_namespace_for_block,
        get_state_root_key,
        get_storage_key,
        read_latest_block_number,
        read_namespace_block_number,
    },
};
use alloy::primitives::{
    Address,
    B256,
    U256,
};
use redis::Pipeline;
use std::collections::HashMap;

/// Thin wrapper for reading blockchain state from Redis.
#[derive(Clone, Debug)]
pub struct StateReader {
    client: RedisStateClient,
}

impl StateReader {
    /// Create a new reader.
    pub fn new(
        redis_url: &str,
        base_namespace: String,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace, buffer_config)?;
        Ok(Self { client })
    }

    /// Get the most recent block number across all namespaces.
    pub fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            read_latest_block_number(conn, &base_namespace, buffer_size)
        })
    }

    /// Get complete account state including code and all storage slots.
    /// Optimized to fetch everything in a single Redis roundtrip.
    pub fn get_account(
        &self,
        address: &Address,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let address = *address;

        self.client.with_connection(move |conn| {
            get_account_at_block(conn, &base_namespace, buffer_size, &address, block_number)
        })
    }

    /// Get a specific storage slot for an account at a block.
    pub fn get_storage(
        &self,
        address: &Address,
        slot: U256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let address = *address;

        self.client.with_connection(move |conn| {
            get_storage_at_block(
                conn,
                &base_namespace,
                buffer_size,
                &address,
                slot,
                block_number,
            )
        })
    }

    /// Get all storage slots for an account at a block.
    pub fn get_all_storage(
        &self,
        address: &Address,
        block_number: u64,
    ) -> StateResult<HashMap<U256, B256>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let address = *address;

        self.client.with_connection(move |conn| {
            get_all_storage_at_block(conn, &base_namespace, buffer_size, &address, block_number)
        })
    }

    /// Get contract bytecode by code hash.
    pub fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Vec<u8>>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            get_code_at_block(conn, &base_namespace, buffer_size, code_hash, block_number)
        })
    }

    /// Get block hash for a specific block number.
    pub fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client
            .with_connection(move |conn| get_block_hash(conn, &base_namespace, block_number))
    }

    /// Get state root for a specific block number.
    pub fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        let base_namespace = self.client.base_namespace.clone();

        self.client
            .with_connection(move |conn| get_state_root(conn, &base_namespace, block_number))
    }

    /// Get complete block metadata (hash and state root).
    /// Optimized to fetch both in a single roundtrip.
    pub fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
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

    /// Check if a specific block number is available in the circular buffer.
    pub fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            is_block_available(conn, &base_namespace, buffer_size, block_number)
        })
    }

    /// Get the range of available blocks [oldest, latest].
    pub fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        let latest = self.latest_block_number()?;

        if let Some(latest_block) = latest {
            let buffer_size = self.client.buffer_config.buffer_size as u64;
            let oldest_block = latest_block.saturating_sub(buffer_size - 1);
            Ok(Some((oldest_block, latest_block)))
        } else {
            Ok(None)
        }
    }
}

// ============================================================================
// Internal helper functions
// ============================================================================

/// Get complete account state including code and storage in a single roundtrip.
fn get_account_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    address: &Address,
    block_number: u64,
) -> StateResult<Option<AccountState>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    let namespace_block = read_namespace_block_number(conn, &namespace)?;
    if namespace_block != Some(block_number) {
        return Err(StateError::BlockNotFound { block_number });
    }

    let address_hex = hex::encode(address.as_slice());
    let account_key = get_account_key(&namespace, &address_hex);
    let storage_key = get_storage_key(&namespace, &address_hex);

    // Pipeline all reads together for single roundtrip
    let mut pipe = Pipeline::new();
    pipe.hgetall(&account_key).hgetall(&storage_key);

    let results: (HashMap<String, String>, HashMap<String, String>) = pipe.query(conn)?;

    let (account_fields, storage_fields) = results;

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
        let slot = decode_u256(&slot_hex)?;
        let value = decode_u256(&value_hex)?;
        storage.insert(slot, value);
    }

    // Fetch code if code_hash is non-empty
    let code = if code_hash_decoded == B256::ZERO {
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

    Ok(Some(AccountState {
        address: *address,
        balance: balance_parsed,
        nonce: nonce_parsed,
        code_hash: code_hash_decoded,
        code,
        storage,
        deleted: false,
    }))
}

/// Get a storage slot at a specific block.
fn get_storage_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    address: &Address,
    slot: U256,
    block_number: u64,
) -> StateResult<Option<U256>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    let namespace_block = read_namespace_block_number(conn, &namespace)?;
    if namespace_block != Some(block_number) {
        return Err(StateError::BlockNotFound { block_number });
    }

    let address_hex = hex::encode(address.as_slice());
    let storage_key = get_storage_key(&namespace, &address_hex);
    let slot_hex = encode_u256(slot);

    let value: Option<String> = redis::cmd("HGET")
        .arg(&storage_key)
        .arg(&slot_hex)
        .query(conn)?;

    value.map(|v| decode_u256(&v)).transpose()
}

/// Get all storage slots for an account at a block.
fn get_all_storage_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    address: &Address,
    block_number: u64,
) -> StateResult<HashMap<U256, B256>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    let namespace_block = read_namespace_block_number(conn, &namespace)?;
    if namespace_block != Some(block_number) {
        return Err(StateError::BlockNotFound { block_number });
    }

    let address_hex = hex::encode(address.as_slice());
    let storage_key = get_storage_key(&namespace, &address_hex);

    let fields: HashMap<String, String> = redis::cmd("HGETALL").arg(&storage_key).query(conn)?;

    let mut result = HashMap::new();
    for (slot_hex, value_hex) in fields {
        let slot = decode_u256(&slot_hex)?;
        let value = decode_b256(&value_hex)?;
        result.insert(slot, value);
    }

    Ok(result)
}

/// Get contract code at a specific block.
fn get_code_at_block<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    code_hash: B256,
    block_number: u64,
) -> StateResult<Option<Vec<u8>>>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    let namespace_block = read_namespace_block_number(conn, &namespace)?;
    if namespace_block != Some(block_number) {
        return Err(StateError::BlockNotFound { block_number });
    }

    let code_hash_hex = hex::encode(code_hash);
    let code_key = get_code_key(&namespace, &code_hash_hex);

    let value: Option<String> = redis::cmd("GET").arg(&code_key).query(conn)?;

    value.map(|v| decode_bytes(&v)).transpose()
}

/// Get block hash (shared across namespaces).
fn get_block_hash<C>(
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
fn get_state_root<C>(
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
fn is_block_available<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    block_number: u64,
) -> StateResult<bool>
where
    C: redis::ConnectionLike,
{
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let namespace_block = read_namespace_block_number(conn, &namespace)?;
    Ok(namespace_block == Some(block_number))
}
