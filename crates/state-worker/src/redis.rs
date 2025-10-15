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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{
        AccountCommit,
        BlockStateUpdate,
    };
    use alloy::primitives::{
        Address,
        B256,
        U256,
        keccak256,
    };
    use anyhow::Result;
    use redis::Commands;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::Redis;

    fn b256_from_u64(value: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        B256::from(bytes)
    }

    // Helper to setup Redis connection for each test (now async!)
    async fn setup_redis() -> (testcontainers::ContainerAsync<Redis>, redis::Connection) {
        let container = Redis::default()
            .start()
            .await
            .expect("Failed to start Redis container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get port");

        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let connection = client.get_connection().unwrap();

        (container, connection)
    }

    // Helper to create a test update with specific account data
    fn create_test_update(
        block_number: u64,
        state_root: B256,
        address: Address,
        balance: u64,
        nonce: u64,
        storage: Vec<(B256, B256)>,
        code: Option<Vec<u8>>,
    ) -> BlockStateUpdate {
        let code_hash = code.as_ref().map_or(B256::ZERO, keccak256);

        BlockStateUpdate {
            block_number,
            state_root,
            block_hash: B256::repeat_byte(u8::try_from(block_number).unwrap()),
            accounts: vec![AccountCommit {
                address,
                balance: U256::from(balance),
                nonce,
                code_hash,
                code,
                storage,
                deleted: false,
            }],
        }
    }

    // Helper to verify account state in Redis
    fn verify_account_state(
        conn: &mut redis::Connection,
        namespace: &str,
        address: Address,
        expected_balance: u64,
        expected_nonce: u64,
    ) -> Result<()> {
        let account_key = format!("{namespace}:account:{}", hex::encode(address));

        let balance: String = conn.hget(&account_key, "balance")?;
        assert_eq!(balance, expected_balance.to_string(), "Balance mismatch");

        let nonce: String = conn.hget(&account_key, "nonce")?;
        assert_eq!(nonce, expected_nonce.to_string(), "Nonce mismatch");

        Ok(())
    }

    // Helper to verify storage in Redis
    fn verify_storage(
        conn: &mut redis::Connection,
        namespace: &str,
        address: Address,
        slot: B256,
        expected_value: B256,
    ) -> Result<()> {
        let storage_key = format!("{namespace}:storage:{}", hex::encode(address));
        let slot_hex = encode_b256(slot);

        let value: String = conn.hget(&storage_key, &slot_hex)?;
        assert_eq!(value, encode_b256(expected_value), "Storage value mismatch");

        Ok(())
    }

    #[tokio::test]
    async fn test_cumulative_state_with_different_accounts() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "cumulative_accounts".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        // Block 0: Account 0x11 with balance 1000
        let addr_0x11 = Address::repeat_byte(0x11);
        let update0 =
            create_test_update(0, B256::repeat_byte(0xAA), addr_0x11, 1000, 5, vec![], None);
        writer.commit_block(update0).await?;

        // Block 1: Account 0x22 with balance 2000 (0x11 not touched)
        let addr_0x22 = Address::repeat_byte(0x22);
        let update1 = create_test_update(
            1,
            B256::repeat_byte(0xBB),
            addr_0x22,
            2000,
            10,
            vec![],
            None,
        );
        writer.commit_block(update1).await?;

        // Block 2: Account 0x33 with balance 3000 (0x11 and 0x22 not touched)
        let addr_0x33 = Address::repeat_byte(0x33);
        let update2 = create_test_update(
            2,
            B256::repeat_byte(0xCC),
            addr_0x33,
            3000,
            15,
            vec![],
            None,
        );
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Account 0x44 with balance 4000 (should apply to namespace 0)
        let addr_0x44 = Address::repeat_byte(0x44);
        let update3 = create_test_update(
            3,
            B256::repeat_byte(0xDD),
            addr_0x44,
            4000,
            20,
            vec![],
            None,
        );
        writer.commit_block(update3).await?;

        // CRITICAL: Namespace 0 should have CUMULATIVE state:
        // - Account 0x11 from block 0
        // - Account 0x22 from block 1 (applied as diff)
        // - Account 0x33 from block 2 (applied as diff)
        // - Account 0x44 from block 3 (applied as diff)

        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x11, 1000, 5)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x22, 2000, 10)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x33, 3000, 15)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x44, 4000, 20)?;

        // Verify block number is updated
        let block_key = format!("{namespace}:0:block");
        let block: String = conn.get(&block_key)?;
        assert_eq!(block, "3");

        Ok(())
    }

    #[tokio::test]
    async fn test_cumulative_state_with_account_updates() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "cumulative_updates".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0x55);

        // Block 0: Account with balance 1000, nonce 0
        let update0 =
            create_test_update(0, B256::repeat_byte(0xBB), address, 1000, 0, vec![], None);
        writer.commit_block(update0).await?;

        // Block 1: Same account, balance increases to 1500, nonce to 1
        let update1 =
            create_test_update(1, B256::repeat_byte(0xBB), address, 1500, 1, vec![], None);
        writer.commit_block(update1).await?;

        // Block 2: Same account, balance decreases to 1200, nonce to 2
        let update2 =
            create_test_update(2, B256::repeat_byte(0xBB), address, 1200, 2, vec![], None);
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Same account, balance to 2000, nonce to 3
        let update3 =
            create_test_update(3, B256::repeat_byte(0xBB), address, 2000, 3, vec![], None);
        writer.commit_block(update3).await?;

        // Namespace 0 should have the FINAL state after applying all diffs
        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 2000, 3)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cumulative_storage_updates() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "cumulative_storage".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0x66);

        // Block 0: Set storage slot 1 = 100
        let storage_0 = vec![(b256_from_u64(1), b256_from_u64(100))];
        let update0 = create_test_update(
            0,
            B256::repeat_byte(0xBB),
            address,
            1000,
            0,
            storage_0,
            None,
        );
        writer.commit_block(update0).await?;

        // Block 1: Set storage slot 2 = 200 (slot 1 not touched)
        let storage_1 = vec![(b256_from_u64(2), b256_from_u64(200))];
        let update1 = create_test_update(
            1,
            B256::repeat_byte(0xBB),
            address,
            1000,
            1,
            storage_1,
            None,
        );
        writer.commit_block(update1).await?;

        // Block 2: Update storage slot 1 = 150 (slot 2 not touched)
        let storage_2 = vec![(b256_from_u64(1), b256_from_u64(150))];
        let update2 = create_test_update(
            2,
            B256::repeat_byte(0xBB),
            address,
            1000,
            2,
            storage_2,
            None,
        );
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Set storage slot 3 = 300
        let storage_3 = vec![(b256_from_u64(3), b256_from_u64(300))];
        let update3 = create_test_update(
            3,
            B256::repeat_byte(0xBB),
            address,
            1000,
            3,
            storage_3,
            None,
        );
        writer.commit_block(update3).await?;

        // Namespace 0 should have ALL storage slots with their latest values:
        // - Slot 1 = 150 (updated in block 2)
        // - Slot 2 = 200 (set in block 1)
        // - Slot 3 = 300 (set in block 3)
        verify_storage(
            &mut conn,
            &format!("{namespace}:0"),
            address,
            b256_from_u64(1),
            b256_from_u64(150),
        )?;
        verify_storage(
            &mut conn,
            &format!("{namespace}:0"),
            address,
            b256_from_u64(2),
            b256_from_u64(200),
        )?;
        verify_storage(
            &mut conn,
            &format!("{namespace}:0"),
            address,
            b256_from_u64(3),
            b256_from_u64(300),
        )?;

        Ok(())
    }

    #[tokio::test]
    async fn test_single_block_only_one_state_available() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "single_block".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let latest = writer.latest_block_number().await?;
        assert_eq!(latest, None, "Should have no blocks initially");

        let address = Address::repeat_byte(0x11);
        let update = create_test_update(0, B256::repeat_byte(0xBB), address, 1000, 5, vec![], None);
        writer.commit_block(update).await?;

        let latest = writer.latest_block_number().await?;
        assert_eq!(latest, Some(0), "Should have block 0");

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 1000, 5)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_state_diff_storage_and_cleanup() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let base_namespace = "diff_cleanup";
        let buffer_size = 3;

        // Write blocks 0, 1, 2
        for block_num in 0..3 {
            let update = BlockStateUpdate {
                block_number: block_num,
                block_hash: B256::repeat_byte(u8::try_from(block_num).unwrap()),
                state_root: B256::repeat_byte(u8::try_from(block_num).unwrap()),
                accounts: vec![],
            };
            commit_block_atomic(&mut conn, base_namespace, buffer_size, &update)?;
        }

        // Write block 3 (should delete block 0's diff)
        let update3 = BlockStateUpdate {
            block_number: 3,
            block_hash: B256::repeat_byte(3),
            state_root: B256::repeat_byte(u8::try_from(3).unwrap()),
            accounts: vec![],
        };
        commit_block_atomic(&mut conn, base_namespace, buffer_size, &update3)?;

        // Block 0's diff should be deleted
        let diff_key_0 = get_diff_key(base_namespace, 0);
        let exists_0: bool = redis::cmd("EXISTS").arg(&diff_key_0).query(&mut conn)?;
        assert!(!exists_0, "Diff for block 0 should be deleted");

        // Blocks 1, 2, 3 diffs should exist
        for block_num in 1..=3 {
            let diff_key = get_diff_key(base_namespace, block_num);
            let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(&mut conn)?;
            assert!(exists, "Diff for block {block_num} should exist");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_large_scale_rotation() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "large_scale".to_string();
        let config = CircularBufferConfig { buffer_size: 5 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0xcc);

        // Write 20 blocks - each increments the balance by 10
        for block_num in 0..20 {
            let update = create_test_update(
                block_num,
                B256::repeat_byte(0xAA),
                address,
                block_num * 10,
                block_num,
                vec![],
                None,
            );
            writer.commit_block(update).await?;
        }

        let latest = writer.latest_block_number().await?;
        assert_eq!(latest, Some(19));

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Each namespace should have cumulative state at its block number
        // For example, namespace 0 (which now has block 15) should have the account
        // with balance 150 (from block 15)

        // Block 15 -> namespace 0 (15 % 5 = 0)
        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 150, 15)?;

        // Block 16 -> namespace 1 (16 % 5 = 1)
        verify_account_state(&mut conn, &format!("{namespace}:1"), address, 160, 16)?;

        // Block 17 -> namespace 1 (17 % 5 = 2)
        verify_account_state(&mut conn, &format!("{namespace}:2"), address, 170, 17)?;

        // Block 18 -> namespace 1 (18 % 5 = 3)
        verify_account_state(&mut conn, &format!("{namespace}:3"), address, 180, 18)?;

        // Block 19 -> namespace 1 (19 % 5 = 4)
        verify_account_state(&mut conn, &format!("{namespace}:4"), address, 190, 19)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_account_deletion_persists() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "deletion_test".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0x77);

        // Block 0: Account exists with balance
        let update0 =
            create_test_update(0, B256::repeat_byte(0xBB), address, 1000, 5, vec![], None);
        writer.commit_block(update0).await?;

        // Block 1: Account DELETED
        let update1 = BlockStateUpdate {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![AccountCommit {
                address,
                balance: U256::ZERO,
                nonce: 0,
                code_hash: B256::ZERO,
                code: None,
                storage: vec![],
                deleted: true,
            }],
        };
        writer.commit_block(update1).await?;

        // Block 2: Different account (doesn't touch deleted one)
        let other_addr = Address::repeat_byte(0x88);
        let update2 = create_test_update(
            2,
            B256::repeat_byte(0xBB),
            other_addr,
            2000,
            10,
            vec![],
            None,
        );
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Another different account (overwrites namespace 0)
        let third_addr = Address::repeat_byte(0x99);
        let update3 = create_test_update(
            3,
            B256::repeat_byte(0xBB),
            third_addr,
            3000,
            15,
            vec![],
            None,
        );
        writer.commit_block(update3).await?;

        // Namespace 0 should have:
        // - Deleted account from block 1 (with zero values)
        // - Other account from block 2
        // - Third account from block 3
        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 0, 0)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), other_addr, 2000, 10)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), third_addr, 3000, 15)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_buffer_size_one() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "buffer_one".to_string();
        let config = CircularBufferConfig { buffer_size: 1 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0xd1);

        // Write blocks 0, 1, 2, 3 - all go to namespace 0
        for block_num in 0..4 {
            let update = create_test_update(
                block_num,
                B256::repeat_byte(0xAA),
                address,
                (block_num + 1) * 100,
                block_num,
                vec![],
                None,
            );
            writer.commit_block(update).await?;
        }

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Namespace 0 should have cumulative state at block 3
        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 400, 3)?;

        // Verify block number
        let block_key = format!("{namespace}:0:block");
        let block: String = conn.get(&block_key)?;
        assert_eq!(block, "3");

        Ok(())
    }

    #[tokio::test]
    async fn test_code_updates_across_rotations() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "code_updates".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0xe1);

        // Block 0: Deploy contract with code X
        let code_x = vec![0x60, 0x80, 0x60, 0x40];
        let update0 = create_test_update(
            0,
            B256::repeat_byte(0xBB),
            address,
            0,
            1,
            vec![],
            Some(code_x.clone()),
        );
        writer.commit_block(update0).await?;

        // Block 1: Doesn't touch the contract
        let other_addr = Address::repeat_byte(0xe2);
        let update1 = create_test_update(
            1,
            B256::repeat_byte(0xBB),
            other_addr,
            1000,
            1,
            vec![],
            None,
        );
        writer.commit_block(update1).await?;

        // Block 2: Update contract code to Y
        let code_y = vec![0x61, 0x90, 0x61, 0x50];
        let code_hash_y = keccak256(&code_y);
        let update2 = create_test_update(
            2,
            B256::repeat_byte(0xBB),
            address,
            0,
            2,
            vec![],
            Some(code_y.clone()),
        );
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Overwrites namespace 0
        let third_addr = Address::repeat_byte(0xe3);
        let update3 = create_test_update(
            3,
            B256::repeat_byte(0xBB),
            third_addr,
            3000,
            3,
            vec![],
            None,
        );
        writer.commit_block(update3).await?;

        // Contract should have code Y (not X)
        let account_key = format!("{}:0:account:{}", namespace, hex::encode(address));
        let stored_code_hash: String = conn.hget(&account_key, "code_hash")?;
        assert_eq!(stored_code_hash, encode_b256(code_hash_y));

        // Verify code Y exists
        let code_key_y = format!("{}:0:code:{}", namespace, hex::encode(code_hash_y));
        let stored_code: String = conn.get(&code_key_y)?;
        assert_eq!(stored_code, encode_bytes(&code_y));

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_slot_zeroing() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "storage_zero".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0xf1);

        // Block 0: Set storage slot 1 = 100
        let storage_0 = vec![(b256_from_u64(1), b256_from_u64(100))];
        let update0 = create_test_update(
            0,
            B256::repeat_byte(0xBB),
            address,
            1000,
            0,
            storage_0,
            None,
        );
        writer.commit_block(update0).await?;

        // Block 1: Set storage slot 1 = 0 (deleted/zeroed)
        let storage_1 = vec![(b256_from_u64(1), B256::ZERO)];
        let update1 = create_test_update(
            1,
            B256::repeat_byte(0xBB),
            address,
            1000,
            1,
            storage_1,
            None,
        );
        writer.commit_block(update1).await?;

        // Block 2: Different account (doesn't touch storage)
        let other_addr = Address::repeat_byte(0xf2);
        let update2 = create_test_update(
            2,
            B256::repeat_byte(0xBB),
            other_addr,
            2000,
            2,
            vec![],
            None,
        );
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Overwrites namespace 0
        let third_addr = Address::repeat_byte(0xf3);
        let update3 = create_test_update(
            3,
            B256::repeat_byte(0xBB),
            third_addr,
            3000,
            3,
            vec![],
            None,
        );
        writer.commit_block(update3).await?;

        // Storage slot 1 should be ZERO (not 100)
        verify_storage(
            &mut conn,
            &format!("{namespace}:0"),
            address,
            b256_from_u64(1),
            B256::ZERO,
        )?;

        Ok(())
    }

    #[tokio::test]
    async fn test_missing_intermediate_diffs_should_error() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let base_namespace = "missing_diffs";
        let buffer_size = 3;

        // Manually set up namespace 0 with block 0
        let namespace = format!("{base_namespace}:0");
        let block_key = format!("{namespace}:block");
        redis::cmd("SET")
            .arg(&block_key)
            .arg("0")
            .query::<()>(&mut conn)?;

        // Store only diff for block 3 (skip 1 and 2)
        let update3 = BlockStateUpdate {
            block_number: 3,
            block_hash: B256::repeat_byte(3),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![],
        };
        let diff_key_3 = get_diff_key(base_namespace, 3);
        let diff_data = serialize_state_diff(&update3)?;
        redis::cmd("SET")
            .arg(&diff_key_3)
            .arg(diff_data)
            .query::<()>(&mut conn)?;

        // Try to commit block 3 (should fail because diffs 1 and 2 are missing)
        let result = commit_block_atomic(&mut conn, base_namespace, buffer_size, &update3);

        assert!(
            result.is_err(),
            "Should error when intermediate diffs are missing"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Missing state diff"),
            "Error should mention missing state diff"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_complete_rotations() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "multi_rotation".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let address = Address::repeat_byte(0xaa);

        // Write 9 blocks (3 complete rotations)
        // Each block increments balance by 100
        for block_num in 0..9 {
            let update = create_test_update(
                block_num,
                B256::repeat_byte(0xAA),
                address,
                (block_num + 1) * 100,
                block_num,
                vec![],
                None,
            );
            writer.commit_block(update).await?;
        }

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // After 9 blocks:
        // Namespace 0 has block 6 (0 -> 3 -> 6)
        // Namespace 1 has block 7 (1 -> 4 -> 7)
        // Namespace 2 has block 8 (2 -> 5 -> 8)

        verify_account_state(&mut conn, &format!("{namespace}:0"), address, 700, 6)?;
        verify_account_state(&mut conn, &format!("{namespace}:1"), address, 800, 7)?;
        verify_account_state(&mut conn, &format!("{namespace}:2"), address, 900, 8)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_accounts_with_mixed_operations() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "mixed_ops".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let addr_a = Address::repeat_byte(0xa1);
        let addr_b = Address::repeat_byte(0xb1);
        let addr_c = Address::repeat_byte(0xc1);

        // Block 0: Create accounts A and B
        let update0 = BlockStateUpdate {
            block_number: 0,
            block_hash: B256::ZERO,
            state_root: B256::ZERO,
            accounts: vec![
                AccountCommit {
                    address: addr_a,
                    balance: U256::from(1000u64),
                    nonce: 1,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                    deleted: false,
                },
                AccountCommit {
                    address: addr_b,
                    balance: U256::from(2000u64),
                    nonce: 2,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                    deleted: false,
                },
            ],
        };
        writer.commit_block(update0).await?;

        // Block 1: Update A, delete B, create C
        let update1 = BlockStateUpdate {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![
                AccountCommit {
                    address: addr_a,
                    balance: U256::from(1500u64),
                    nonce: 2,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                    deleted: false,
                },
                AccountCommit {
                    address: addr_b,
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                    deleted: true,
                },
                AccountCommit {
                    address: addr_c,
                    balance: U256::from(3000u64),
                    nonce: 1,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                    deleted: false,
                },
            ],
        };
        writer.commit_block(update1).await?;

        // Block 2: Update C
        let update2 = BlockStateUpdate {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            state_root: B256::repeat_byte(0xAA),
            accounts: vec![AccountCommit {
                address: addr_c,
                balance: U256::from(3500u64),
                nonce: 2,
                code_hash: B256::ZERO,
                code: None,
                storage: vec![],
                deleted: false,
            }],
        };
        writer.commit_block(update2).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        // Block 3: Overwrites namespace 0
        let addr_d = Address::repeat_byte(0xd1);
        let update3 = create_test_update(3, B256::repeat_byte(0xBB), addr_d, 4000, 4, vec![], None);
        writer.commit_block(update3).await?;

        // Namespace 0 should have cumulative state:
        // A: updated (1500, 2)
        // B: deleted (0, 0)
        // C: updated (3500, 2)
        // D: new (4000, 4)
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_a, 1500, 2)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_b, 0, 0)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_c, 3500, 2)?;
        verify_account_state(&mut conn, &format!("{namespace}:0"), addr_d, 4000, 4)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_initial_fill_cumulative_state() -> Result<()> {
        let container = Redis::default().start().await?;
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(6379).await?;

        let namespace = "initial_fill".to_string();
        let config = CircularBufferConfig { buffer_size: 3 };
        let writer =
            RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone(), config)?;

        let addr_a = Address::repeat_byte(0xa1);
        let addr_b = Address::repeat_byte(0xb1);

        // Block 0: Create account A
        let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, vec![], None);
        writer.commit_block(update0).await?;

        // Block 1: Create account B
        let update1 = create_test_update(1, B256::ZERO, addr_b, 2000, 2, vec![], None);
        writer.commit_block(update1).await?;

        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        verify_account_state(&mut conn, &format!("{namespace}:1"), addr_a, 1000, 1)?;
        verify_account_state(&mut conn, &format!("{namespace}:1"), addr_b, 2000, 2)?;

        Ok(())
    }
}
