//! State writer implementation for persisting blockchain state to Redis.
//!
//! This module implements a chunked commit strategy protected by namespace
//! write locks to handle large state updates without blocking Redis for extended periods.

use crate::{
    CircularBufferConfig,
    common::{
        AccountState,
        BlockStateUpdate,
        ChunkedWriteConfig,
        NamespaceLock,
        RedisStateClient,
        encode_b256,
        encode_bytes,
        encode_u256,
        ensure_state_dump_indices,
        error::{
            StateError,
            StateResult,
        },
        get_account_key,
        get_block_hash_key,
        get_block_key,
        get_code_key,
        get_diff_key,
        get_namespace_for_block,
        get_state_root_key,
        get_storage_key,
        get_write_lock_key,
        read_latest_block_number,
        read_namespace_block_number,
        read_namespace_lock,
        update_metadata_in_pipe,
    },
};
use alloy::primitives::B256;
use uuid::Uuid;

/// Thin wrapper that writes account/state data into Redis using a circular buffer
/// approach to maintain multiple historical states.
///
/// Uses chunked commits with write locks to handle large state updates efficiently
/// while maintaining consistency for readers.
#[derive(Clone)]
pub struct StateWriter {
    client: RedisStateClient,
    chunked_config: ChunkedWriteConfig,
    writer_id: String,
}

impl StateWriter {
    /// Build a new writer with circular buffer support.
    pub fn new(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        Self::with_chunked_config(
            redis_url,
            base_namespace,
            buffer_config,
            ChunkedWriteConfig::default(),
        )
    }

    /// Build a new writer with custom chunked write configuration.
    pub fn with_chunked_config(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
        chunked_config: ChunkedWriteConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace.to_string(), buffer_config)?;
        let writer_id = Uuid::new_v4().to_string();
        Ok(Self {
            client,
            chunked_config,
            writer_id,
        })
    }

    /// Read the most recently persisted block number from Redis metadata.
    pub fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        self.client
            .with_connection(move |conn| read_latest_block_number(conn, &base_namespace))
    }

    /// Persist all account mutations for the block using chunked commits with locking.
    ///
    /// Strategy:
    /// 1. Acquire write lock on target namespace
    /// 2. Apply intermediate diffs in chunks (if needed for circular buffer rotation)
    /// 3. Apply current block's state changes in chunks
    /// 4. Finalize metadata and release lock
    pub fn commit_block(&self, update: BlockStateUpdate) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let chunked_config = self.chunked_config.clone();
        let writer_id = self.writer_id.clone();

        self.client.with_connection(move |conn| {
            commit_block_chunked(
                conn,
                &base_namespace,
                buffer_size,
                &chunked_config,
                &writer_id,
                &update,
            )
        })
    }

    /// Ensure the Redis metadata matches the configured namespace rotation size.
    pub fn ensure_dump_index_metadata(&self) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            ensure_state_dump_indices(conn, &base_namespace, buffer_size)
        })
    }

    /// Get the unique writer ID for this instance.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }
}

/// Deserialize a state diff from JSON.
fn deserialize_state_diff(json: &str, block_number: u64) -> StateResult<BlockStateUpdate> {
    serde_json::from_str(json).map_err(|e| StateError::DeserializeDiff(block_number, e))
}

/// Serialize state diff for storage.
pub(crate) fn serialize_state_diff(update: &BlockStateUpdate) -> StateResult<String> {
    serde_json::to_string(update).map_err(|e| StateError::SerializeDiff(update.block_number, e))
}

/// Acquire write lock for a namespace using SET NX (set if not exists).
///
/// Returns Ok(()) if lock was acquired, Err if:
/// - Lock is held by another active writer (`LockAcquisitionFailed`)
/// - Lock is stale from a crashed writer (`StaleLockDetected`)
fn acquire_write_lock<C>(
    conn: &mut C,
    namespace: &str,
    lock: &NamespaceLock,
    stale_timeout_secs: i64,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let lock_key = get_write_lock_key(namespace);

    // Check for existing lock
    if let Some(existing) = read_namespace_lock(conn, namespace)? {
        if existing.is_stale(stale_timeout_secs) {
            return Err(StateError::StaleLockDetected {
                namespace: namespace.to_string(),
                writer_id: existing.writer_id,
                started_at: existing.started_at,
            });
        }
        return Err(StateError::LockAcquisitionFailed {
            namespace: namespace.to_string(),
            existing_writer: existing.writer_id,
        });
    }

    // Set the lock atomically
    let lock_json = lock.to_json()?;
    let result: Option<String> = redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .arg("NX")
        .query(conn)?;

    if result.is_some() {
        Ok(())
    } else {
        Err(StateError::LockAcquisitionFailed {
            namespace: namespace.to_string(),
            existing_writer: "unknown".to_string(),
        })
    }
}

/// Release the write lock for a namespace.
/// Only releases if we still own the lock.
fn release_write_lock<C>(conn: &mut C, namespace: &str, writer_id: &str) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let lock_key = get_write_lock_key(namespace);

    // Use WATCH for optimistic locking
    redis::cmd("WATCH").arg(&lock_key).query::<()>(conn)?;

    // Read the current lock
    let existing_json: Option<String> = redis::cmd("GET").arg(&lock_key).query(conn)?;

    // Only delete it if we own it
    let should_delete = match existing_json {
        Some(json) => {
            match NamespaceLock::from_json(&json) {
                Ok(existing) => existing.writer_id == writer_id,
                Err(_) => false,
            }
        }
        None => false,
    };

    if should_delete {
        let _: Option<()> = redis::pipe().atomic().del(&lock_key).query(conn)?;
    } else {
        redis::cmd("UNWATCH").query::<()>(conn)?;
    }

    Ok(())
}

/// Write a chunk of accounts to Redis.
fn write_account_chunk<C>(
    conn: &mut C,
    namespace: &str,
    accounts: &[&AccountState],
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    if accounts.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    for account in accounts {
        write_account_to_pipe(&mut pipe, namespace, account);
    }
    pipe.query::<()>(conn)?;
    Ok(())
}

/// Write account data to a namespace within a pipeline.
fn write_account_to_pipe(pipe: &mut redis::Pipeline, namespace: &str, account: &AccountState) {
    let account_key = get_account_key(namespace, &account.address_hash);

    let balance = account.balance.to_string();
    let nonce = account.nonce.to_string();
    let code_hash = encode_b256(account.code_hash);

    pipe.hset_multiple(
        &account_key,
        &[
            ("balance", balance.as_str()),
            ("nonce", nonce.as_str()),
            ("code_hash", code_hash.as_str()),
        ],
    );

    if let Some(code) = &account.code {
        let code_hash_hex = hex::encode(account.code_hash);
        let code_key = get_code_key(namespace, &code_hash_hex);
        let code_hex = encode_bytes(code);
        pipe.set(&code_key, code_hex);
    }

    if !account.storage.is_empty() || account.deleted {
        let storage_key = get_storage_key(namespace, &account.address_hash);
        for (slot, value) in &account.storage {
            let slot_hash = B256::from(slot.to_be_bytes::<32>());
            let slot_hex = encode_b256(slot_hash);
            if value.is_zero() {
                pipe.hdel(&storage_key, slot_hex);
            } else {
                let value_hex = encode_u256(*value);
                pipe.hset(&storage_key, slot_hex, value_hex);
            }
        }
    }
}

/// Write block metadata and finalize the commit.
fn write_block_metadata<C>(
    conn: &mut C,
    namespace: &str,
    base_namespace: &str,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
    buffer_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let mut pipe = redis::pipe();
    pipe.atomic();

    let block_key = get_block_key(namespace);
    pipe.set(&block_key, block_number.to_string());

    let block_hash_key = get_block_hash_key(base_namespace, block_number);
    pipe.set(&block_hash_key, encode_b256(block_hash));

    let state_root_key = get_state_root_key(base_namespace, block_number);
    pipe.set(&state_root_key, encode_b256(state_root));

    update_metadata_in_pipe(&mut pipe, base_namespace, block_number, buffer_size);

    pipe.query::<()>(conn)?;
    Ok(())
}

/// Store the state diff for a block.
fn store_state_diff<C>(
    conn: &mut C,
    base_namespace: &str,
    update: &BlockStateUpdate,
    buffer_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let mut pipe = redis::pipe();

    let diff_key = get_diff_key(base_namespace, update.block_number);
    let diff_data = serialize_state_diff(update)?;
    pipe.set(&diff_key, diff_data);

    // Delete old state diff
    if update.block_number >= buffer_size as u64 {
        let old_block = update.block_number - buffer_size as u64;
        let old_diff_key = get_diff_key(base_namespace, old_block);
        pipe.del(&old_diff_key);
    }

    pipe.query::<()>(conn)?;
    Ok(())
}

/// Commit a block using chunked writes with locking.
pub(crate) fn commit_block_chunked<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    chunked_config: &ChunkedWriteConfig,
    writer_id: &str,
    update: &BlockStateUpdate,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let block_number = update.block_number;
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let current_block = read_namespace_block_number(conn, &namespace)?;

    // Create and acquire lock
    let lock = NamespaceLock::new(block_number, writer_id.to_string());
    acquire_write_lock(
        conn,
        &namespace,
        &lock,
        chunked_config.stale_lock_timeout_secs,
    )?;

    // Use closure to ensure the lock is released on any error
    let result = (|| -> StateResult<()> {
        let start_block = current_block.map_or(0, |old| old + 1);

        // Phase 1: Apply intermediate diffs if there's a gap
        if block_number > start_block {
            for intermediate_block in start_block..block_number {
                let diff_key = get_diff_key(base_namespace, intermediate_block);
                let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

                if let Some(json) = diff_json {
                    let diff = deserialize_state_diff(&json, intermediate_block)?;
                    let accounts: Vec<&AccountState> = diff.accounts.iter().collect();
                    for chunk in accounts.chunks(chunked_config.chunk_size) {
                        write_account_chunk(conn, &namespace, chunk)?;
                    }
                } else {
                    return Err(StateError::MissingStateDiff {
                        needed_block: intermediate_block,
                        target_block: block_number,
                    });
                }
            }
        }

        // Phase 2: Write current block's accounts in chunks
        let accounts: Vec<&AccountState> = update.accounts.iter().collect();
        for chunk in accounts.chunks(chunked_config.chunk_size) {
            write_account_chunk(conn, &namespace, chunk)?;
        }

        // Phase 3: Finalize
        write_block_metadata(
            conn,
            &namespace,
            base_namespace,
            block_number,
            update.block_hash,
            update.state_root,
            buffer_size,
        )?;

        store_state_diff(conn, base_namespace, update, buffer_size)?;

        Ok(())
    })();

    // Always release lock
    release_write_lock(conn, &namespace, writer_id)?;

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        U256,
    };

    #[test]
    fn test_serialize_deserialize_state_diff() {
        let update = BlockStateUpdate {
            block_number: 42,
            block_hash: B256::from([1u8; 32]),
            state_root: B256::from([2u8; 32]),
            accounts: vec![AccountState {
                address_hash: Address::from([3u8; 20]).into(),
                balance: U256::from(1000u64),
                nonce: 5,
                code_hash: B256::from([4u8; 32]),
                code: Some(vec![0x60, 0x80]),
                storage: std::collections::HashMap::new(),
                deleted: false,
            }],
        };

        let serialized = serialize_state_diff(&update).unwrap();
        let deserialized = deserialize_state_diff(&serialized, 42).unwrap();

        assert_eq!(deserialized.block_number, 42);
        assert_eq!(deserialized.accounts.len(), 1);
        assert_eq!(deserialized.accounts[0].nonce, 5);
    }
}
