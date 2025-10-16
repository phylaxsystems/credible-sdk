//! State writer implementation for persisting blockchain state to Redis.

use crate::{
    CircularBufferConfig,
    common::{
        AccountState,
        BlockStateUpdate,
        RedisStateClient,
        encode_b256,
        encode_bytes,
        encode_u256,
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
        read_latest_block_number,
        read_namespace_block_number,
    },
};
use alloy::primitives::B256;

/// Thin wrapper that writes account/state data into Redis using a circular buffer
/// approach to maintain multiple historical states.
#[derive(Clone)]
pub struct StateWriter {
    client: RedisStateClient,
}

impl StateWriter {
    /// Build a new writer with circular buffer support.
    pub fn new(
        redis_url: &str,
        base_namespace: String,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace, buffer_config)?;
        Ok(Self { client })
    }

    /// Read the most recently persisted block number from Redis by checking all namespaces.
    pub fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            read_latest_block_number(conn, &base_namespace, buffer_size)
        })
    }

    /// Persist all account mutations for the block using atomic transactions.
    /// This implements the circular buffer pattern where `namespace_idx` = `block_number` % `buffer_size`.
    pub fn commit_block(&self, update: BlockStateUpdate) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            commit_block_atomic(conn, &base_namespace, buffer_size, &update)
        })
    }
}

/// Deserialize a state diff from JSON.
fn deserialize_state_diff(json: &str, block_number: u64) -> StateResult<BlockStateUpdate> {
    serde_json::from_str(json).map_err(|e| StateError::DeserializeDiff(block_number, e))
}

/// Apply a state diff to an existing namespace by updating accounts atomically.
fn apply_state_diff_to_namespace(
    pipe: &mut redis::Pipeline,
    namespace: &str,
    diff: &BlockStateUpdate,
) {
    for account in &diff.accounts {
        write_account_to_pipe(pipe, namespace, account);
    }
}

/// Write account data to a specific namespace within an atomic pipeline.
fn write_account_to_pipe(pipe: &mut redis::Pipeline, namespace: &str, account: &AccountState) {
    let account_key = get_account_key(namespace, &account.address);

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
        let storage_key = get_storage_key(namespace, &account.address);
        for (slot, value) in &account.storage {
            let slot_hex = encode_u256(*slot);
            let value_hex = encode_u256(*value);
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
    let block_key = get_block_key(namespace);
    pipe.set(&block_key, block_number.to_string());

    let block_hash_key = get_block_hash_key(base_namespace, block_number);
    let block_hash_hex = encode_b256(block_hash);
    pipe.set(&block_hash_key, block_hash_hex);

    let state_root_key = get_state_root_key(base_namespace, block_number);
    let state_root_hex = encode_b256(state_root);
    pipe.set(&state_root_key, state_root_hex);
}

/// Serialize state diff for storage (simple JSON serialization).
pub(crate) fn serialize_state_diff(update: &BlockStateUpdate) -> StateResult<String> {
    serde_json::to_string(update).map_err(|e| StateError::SerializeDiff(update.block_number, e))
}

/// Commit a block atomically using Redis MULTI/EXEC transaction.
///
/// CRITICAL: When overwriting a namespace, this function applies all intermediate
/// state diffs to maintain cumulative state history.
pub(crate) fn commit_block_atomic<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    update: &BlockStateUpdate,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let block_number = update.block_number;
    let block_hash = update.block_hash;
    let state_root = update.state_root;

    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let current_block = read_namespace_block_number(conn, &namespace)?;

    let mut pipe = redis::pipe();
    pipe.atomic();

    let start_block = current_block.map_or(0, |old| old + 1);

    // Apply intermediate diffs if there's a gap
    if block_number > start_block {
        for intermediate_block in start_block..block_number {
            let diff_key = get_diff_key(base_namespace, intermediate_block);
            let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

            if let Some(json) = diff_json {
                let diff = deserialize_state_diff(&json, intermediate_block)?;
                apply_state_diff_to_namespace(&mut pipe, &namespace, &diff);
            } else {
                return Err(StateError::MissingStateDiff {
                    needed_block: intermediate_block,
                    target_block: block_number,
                });
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
    pipe.query::<()>(conn)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        U256,
        keccak256,
    };

    #[test]
    fn test_serialize_deserialize_state_diff() {
        let update = BlockStateUpdate {
            block_number: 42,
            block_hash: B256::from([1u8; 32]),
            state_root: B256::from([2u8; 32]),
            accounts: vec![AccountState {
                address: keccak256(Address::from([3u8; 20])),
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
