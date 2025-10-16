//! Lightweight blocking Redis client with circular buffer support for multiple blockchain states.
//!
//! # Overview
//!
//! This module provides a Redis-based storage system for maintaining multiple historical blockchain
//! states using a circular buffer pattern.
//!
//! # Architecture
//!
//! ## Circular Buffer Pattern
//!
//! States are distributed across N namespaces (where N = `buffer_size`). Each block is assigned to
//! a namespace using modulo arithmetic: `namespace_idx = block_number % buffer_size`.
//!
//! Example with `buffer_size = 3`:
//! - Block 0 → namespace:0
//! - Block 1 → namespace:1
//! - Block 2 → namespace:2
//! - Block 3 → namespace:0 (overwrites block 0)
//! - Block 4 → namespace:1 (overwrites block 1)
//!
//! ## Cumulative State Maintenance
//!
//! The critical feature of this system is that when a namespace is overwritten, it maintains the
//! cumulative state by applying all intermediate state diffs. This ensures each namespace
//! contains the complete state up to its current block, not just the delta.
//!
//! Example: When block 3 overwrites the namespace:0 (previously containing block 0):
//! 1. Load diffs for blocks 1 and 2 from Redis
//! 2. Apply diff 1 to the namespace (updates from block 0→1)
//! 3. Apply diff 2 to the namespace (updates from block 1→2)
//! 4. Apply diff 3 to the namespace (updates from block 2→3)
//! 5. Result: the namespace:0 now contains a complete state at block 3
//!
//! ## State Diff Storage
//!
//! To enable cumulative state reconstruction, each block's state changes are stored as a separate
//! diff in Redis with key `{base_namespace}:diff:{block_number}`. These diffs are automatically
//! cleaned up after `buffer_size` blocks to prevent unbounded growth.
//!
//! # Data Model
//!
//! For each namespace, the following keys are stored:
//! - `{namespace}:block` - Current block number in this namespace
//! - `{namespace}:account:{address}` - Hash containing balance, nonce, code hash
//! - `{namespace}:storage:{address}` - Hash of storage slots for the account
//! - `{namespace}:code:{code_hash}` - Contract bytecode
//!
//! Shared across namespaces:
//! - `{base_namespace}:block_hash:{block_number}` - Block hash for each block
//! - `{base_namespace}:state_root:{block_number}` - State root for each block
//! - `{base_namespace}:diff:{block_number}` - Serialized state diff (kept for `buffer_size` blocks)

#[cfg(test)]
mod tests;

use anyhow::{
    Context,
    Result,
    anyhow,
};
use redis::RedisResult;
use std::sync::Arc;

use crate::state::{
    AccountCommit,
    BlockStateUpdate,
};
use alloy::primitives::B256;

/// Configuration for the circular buffer of states in Redis.
#[derive(Clone)]
pub struct CircularBufferConfig {
    /// Number of historical states to maintain (X in the design doc)
    pub buffer_size: usize,
}

impl CircularBufferConfig {
    pub fn new(buffer_size: usize) -> Result<Self> {
        if buffer_size == 0 {
            return Err(anyhow!("buffer_size must be greater than 0"));
        }
        Ok(Self { buffer_size })
    }
}

impl Default for CircularBufferConfig {
    fn default() -> Self {
        Self { buffer_size: 3 }
    }
}

/// Thin wrapper that writes account/state data into Redis using a circular buffer
/// approach to maintain multiple historical states.
#[derive(Clone)]
pub struct RedisStateWriter {
    client: Arc<redis::Client>,
    base_namespace: String,
    buffer_config: CircularBufferConfig,
}

impl RedisStateWriter {
    /// Build a new writer with circular buffer support.
    pub fn new(
        redis_url: &str,
        base_namespace: String,
        buffer_config: CircularBufferConfig,
    ) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client: Arc::new(client),
            base_namespace,
            buffer_config,
        })
    }

    /// Read the most recently persisted block number from Redis by checking all namespaces.
    pub async fn latest_block_number(&self) -> Result<Option<u64>> {
        let base_namespace = self.base_namespace.clone();
        let buffer_size = self.buffer_config.buffer_size;

        self.with_connection(move |conn| {
            read_latest_block_number(conn, &base_namespace, buffer_size)
        })
        .await
    }

    /// Persist all account mutations for the block using atomic transactions.
    /// This implements the circular buffer pattern where `namespace_idx` = `block_number` % `buffer_size`.
    pub async fn commit_block(&self, update: BlockStateUpdate) -> Result<()> {
        let base_namespace = self.base_namespace.clone();
        let buffer_size = self.buffer_config.buffer_size;

        self.with_connection(move |conn| {
            commit_block_atomic(conn, &base_namespace, buffer_size, &update)
        })
        .await
    }

    /// Execute a synchronous Redis operation on a dedicated blocking thread.
    async fn with_connection<T, F>(&self, func: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut redis::Connection) -> Result<T> + Send + 'static,
    {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || -> Result<T> {
            let mut conn = client.get_connection().map_err(|err| anyhow!(err))?;
            func(&mut conn)
        })
        .await
        .map_err(|err| anyhow!(err))?
    }
}

/// Get the namespace for a given block number using circular buffer logic.
fn get_namespace_for_block(
    base_namespace: &str,
    block_number: u64,
    buffer_size: usize,
) -> Result<String> {
    let namespace_idx = usize::try_from(block_number)? % buffer_size;
    Ok(format!("{base_namespace}:{namespace_idx}"))
}

/// Get the key for storing state diffs.
fn get_diff_key(base_namespace: &str, block_number: u64) -> String {
    format!("{base_namespace}:diff:{block_number}")
}

/// Read the current block number stored in a namespace.
fn read_namespace_block_number<C>(conn: &mut C, namespace: &str) -> Result<Option<u64>>
where
    C: redis::ConnectionLike,
{
    let block_key = format!("{namespace}:block");
    let value: Option<String> = redis::cmd("GET")
        .arg(&block_key)
        .query(conn)
        .map_err(|err| anyhow!(err))?;

    value
        .map(|v| v.parse::<u64>().context("invalid block number"))
        .transpose()
}

/// Deserialize a state diff from JSON.
fn deserialize_state_diff(json: &str) -> Result<BlockStateUpdate> {
    serde_json::from_str(json).context("failed to deserialize state diff")
}

/// Apply a state diff to an existing namespace by updating accounts atomically.
fn apply_state_diff_to_namespace(
    pipe: &mut redis::Pipeline,
    namespace: &str,
    diff: &BlockStateUpdate,
) {
    // Apply each account update from the diff
    for account in &diff.accounts {
        write_account_to_pipe(pipe, namespace, account);
    }
}

/// Write account data to a specific namespace within an atomic pipeline.
fn write_account_to_pipe(pipe: &mut redis::Pipeline, namespace: &str, account: &AccountCommit) {
    let account_key = format!("{namespace}:account:{}", hex::encode(account.address));
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
        let code_key = format!("{namespace}:code:{}", hex::encode(account.code_hash));
        let code_hex = encode_bytes(code);
        pipe.set(&code_key, code_hex);
    }

    if !account.storage.is_empty() || account.deleted {
        let storage_key = format!("{namespace}:storage:{}", hex::encode(account.address));
        for (slot, value) in &account.storage {
            let slot_hex = encode_b256(*slot);
            let value_hex = encode_b256(*value);
            pipe.hset(&storage_key, slot_hex, value_hex);
        }
    }
}

/// Write block metadata to a namespace within an atomic pipeline.
fn write_block_metadata_to_pipe(
    pipe: &mut redis::Pipeline,
    namespace: &str,
    base_namespace: &str,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
) {
    // Write block number to namespace
    let block_key = format!("{namespace}:block");
    pipe.set(&block_key, block_number.to_string());

    // Write block hash mapping (shared across all namespaces)
    let block_hash_key = format!("{base_namespace}:block_hash:{block_number}");
    let block_hash_hex = encode_b256(block_hash);
    pipe.set(&block_hash_key, block_hash_hex);

    let state_root_hash_key = format!("{base_namespace}:state_root:{block_number}");
    let state_root_hex = encode_b256(state_root);
    pipe.set(&state_root_hash_key, state_root_hex);
}

/// Serialize state diff for storage (simple JSON serialization).
fn serialize_state_diff(update: &BlockStateUpdate) -> Result<String> {
    serde_json::to_string(update).context("failed to serialize state diff")
}

/// Commit a block atomically using Redis MULTI/EXEC transaction.
///
/// CRITICAL: When overwriting a namespace, this function applies all intermediate
/// state diffs to maintain cumulative state history.
fn commit_block_atomic<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    update: &BlockStateUpdate,
) -> Result<()>
where
    C: redis::ConnectionLike,
{
    let block_number = update.block_number;
    let block_hash = update.block_hash;
    let state_root = update.state_root;

    // Determine target namespace using circular buffer logic
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;

    // Check what block is currently in this namespace
    let current_block = read_namespace_block_number(conn, &namespace)?;

    // Create atomic pipeline
    let mut pipe = redis::pipe();
    pipe.atomic();

    // Determine the starting block for the diff application
    let start_block = current_block.map_or(0, |old| old + 1);

    // Apply intermediate diffs if there's a gap
    if block_number > start_block {
        // We need to apply diffs from (old_block + 1) to (block_number - 1)
        // to maintain cumulative state
        for intermediate_block in start_block..block_number {
            let diff_key = get_diff_key(base_namespace, intermediate_block);
            let diff_json: Option<String> = redis::cmd("GET")
                .arg(&diff_key)
                .query(conn)
                .map_err(|err| anyhow!(err))?;

            if let Some(json) = diff_json {
                let diff = deserialize_state_diff(&json)?;
                apply_state_diff_to_namespace(&mut pipe, &namespace, &diff);
            } else {
                // If we can't find an intermediate diff, we have a problem
                // This shouldn't happen in normal operation
                return Err(anyhow!(
                    "Missing state diff for block {} needed to reconstruct state at block {}",
                    intermediate_block,
                    block_number
                ));
            }
        }
    }

    // Apply the current block's state diff
    for account in &update.accounts {
        write_account_to_pipe(&mut pipe, &namespace, account);
    }

    // Write block metadata
    write_block_metadata_to_pipe(
        &mut pipe,
        &namespace,
        base_namespace,
        block_number,
        block_hash,
        state_root,
    );

    // Store the state diff for this block
    let diff_key = get_diff_key(base_namespace, block_number);
    let diff_data = serialize_state_diff(update)?;
    pipe.set(&diff_key, diff_data);

    // Delete old state diff (block_number - buffer_size)
    if block_number >= buffer_size as u64 {
        let old_block = block_number - buffer_size as u64;
        let old_diff_key = get_diff_key(base_namespace, old_block);
        pipe.del(&old_diff_key);
    }

    // Execute the atomic transaction
    pipe.query::<()>(conn).map_err(|err| anyhow!(err))?;

    Ok(())
}

/// Read the latest block number by checking all namespaces in the circular buffer.
fn read_latest_block_number<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
) -> Result<Option<u64>>
where
    C: redis::ConnectionLike,
{
    let mut max_block: Option<u64> = None;

    for idx in 0..buffer_size {
        let namespace = format!("{base_namespace}:{idx}");
        let block_key = format!("{namespace}:block");

        let value: Option<String> = redis::cmd("GET")
            .arg(&block_key)
            .query(conn)
            .map_err(|err| anyhow!(err))?;

        if let Some(v) = value {
            let block_num = v
                .parse::<u64>()
                .with_context(|| format!("invalid block number in namespace {idx}: {v}"))?;

            max_block = Some(max_block.map_or(block_num, |current| current.max(block_num)));
        }
    }

    Ok(max_block)
}

/// Helper to render 32-byte words in `0x`-prefixed hex for Redis consumers.
fn encode_b256(value: B256) -> String {
    format!("0x{}", hex::encode(value))
}

/// Helper to render arbitrary byte slices for the code cache.
fn encode_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}
