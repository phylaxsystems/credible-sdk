//! Lightweight blocking Redis client tailored to the worker's state schema.
//!
//! The worker runs on Tokio, but the upstream `redis` crate is synchronous. We
//! hide the spawn-blocking logic in this module so callers can commit state
//! updates without threading connection management through their code.

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

/// Thin wrapper that writes account/state data into Redis using the schema
/// documented in `README.md`.
#[derive(Clone)]
pub struct RedisStateWriter {
    client: Arc<redis::Client>,
    namespace: String,
}

impl RedisStateWriter {
    /// Build a new writer. We store the client in an `Arc` so clones can reuse
    /// the underlying connection pool when the worker is shared across tasks.
    pub fn new(redis_url: &str, namespace: String) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client: Arc::new(client),
            namespace,
        })
    }

    /// Read the most recently persisted block number from Redis, if any.
    pub async fn latest_block_number(&self) -> Result<Option<u64>> {
        let key = self.current_block_key();
        self.with_connection(move |conn| read_latest_block_number(conn, &key))
            .await
    }

    /// Persist all account mutations for the block followed by metadata.
    pub async fn commit_block(&self, update: BlockStateUpdate) -> Result<()> {
        if update.accounts.is_empty() {
            return self
                .update_block_metadata(update.block_number, update.block_hash)
                .await;
        }

        let namespace = self.namespace.clone();
        self.with_connection(move |conn| commit_block_with_connection(conn, &namespace, update))
            .await
    }

    /// Update the `current_block` pointer and per-block hash mapping without
    /// touching any accounts. Used when trace output is empty.
    pub async fn update_block_metadata(&self, block_number: u64, block_hash: B256) -> Result<()> {
        let namespace = self.namespace.clone();
        self.with_connection(move |conn| {
            write_block_metadata(conn, &namespace, block_number, block_hash)
        })
        .await
    }

    fn current_block_key(&self) -> String {
        format!("{namespace}:current_block", namespace = self.namespace)
    }

    /// Execute a synchronous Redis operation on a dedicated blocking thread so
    /// the async runtime remains responsive.
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

/// Write the account header + storage entries expected by the sidecar overlay.
fn write_account<C>(conn: &mut C, namespace: &str, account: &AccountCommit) -> Result<()>
where
    C: redis::ConnectionLike,
{
    let account_key = format!("{namespace}:account:{}", hex::encode(account.address));
    let balance = account.balance.to_string();
    let nonce = account.nonce.to_string();
    let code_hash = encode_b256(account.code_hash);
    redis::cmd("HSET")
        .arg(&account_key)
        .arg("balance")
        .arg(&balance)
        .arg("nonce")
        .arg(&nonce)
        .arg("code_hash")
        .arg(&code_hash)
        .query::<()>(conn)
        .map_err(|err| anyhow!(err))?;

    if let Some(code) = &account.code {
        let code_key = format!("{namespace}:code:{}", hex::encode(account.code_hash));
        let code_hex = encode_bytes(code);
        redis::cmd("SET")
            .arg(&code_key)
            .arg(code_hex)
            .query::<()>(conn)
            .map_err(|err| anyhow!(err))?;
    }

    if !account.storage.is_empty() || account.deleted {
        let storage_key = format!("{namespace}:storage:{}", hex::encode(account.address));
        for (slot, value) in &account.storage {
            let slot_hex = encode_b256(*slot);
            let value_hex = encode_b256(*value);
            redis::cmd("HSET")
                .arg(&storage_key)
                .arg(slot_hex)
                .arg(value_hex)
                .query::<()>(conn)
                .map_err(|err| anyhow!(err))?;
        }
    }

    Ok(())
}

/// Store block-level metadata so consumers can resume sync or map numbers to
/// hashes.
fn write_block_metadata<C>(
    conn: &mut C,
    namespace: &str,
    block_number: u64,
    block_hash: B256,
) -> Result<()>
where
    C: redis::ConnectionLike,
{
    let block_hash_key = format!("{namespace}:block_hash:{block_number}");
    let block_hash_hex = encode_b256(block_hash);
    redis::cmd("SET")
        .arg(&block_hash_key)
        .arg(block_hash_hex)
        .query::<()>(conn)
        .map_err(|err| anyhow!(err))?;
    let current_block_key = format!("{namespace}:current_block");
    redis::cmd("SET")
        .arg(&current_block_key)
        .arg(block_number.to_string())
        .query::<()>(conn)
        .map_err(|err| anyhow!(err))?;
    Ok(())
}

/// Helper to render 32-byte words in `0x`-prefixed hex for Redis consumers.
fn encode_b256(value: B256) -> String {
    format!("0x{}", hex::encode(value))
}

/// Helper to render arbitrary byte slices for the code cache.
fn encode_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn commit_block_with_connection<C>(
    conn: &mut C,
    namespace: &str,
    update: BlockStateUpdate,
) -> Result<()>
where
    C: redis::ConnectionLike,
{
    let (block_number, block_hash, accounts) = update.into_parts();

    if accounts.is_empty() {
        return write_block_metadata(conn, namespace, block_number, block_hash);
    }

    for account in &accounts {
        write_account(conn, namespace, account)?;
    }

    write_block_metadata(conn, namespace, block_number, block_hash)
}

fn read_latest_block_number<C>(conn: &mut C, key: &str) -> Result<Option<u64>>
where
    C: redis::ConnectionLike,
{
    let value: Option<String> = redis::cmd("GET")
        .arg(key)
        .query(conn)
        .map_err(|err| anyhow!(err))?;
    let parsed = value
        .map(|v| {
            v.parse::<u64>()
                .with_context(|| format!("invalid block number: {v}"))
        })
        .transpose()?;
    Ok(parsed)
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
    use redis::{
        RedisError,
        Value,
    };
    use redis_test::{
        MockCmd,
        MockRedisConnection,
    };

    fn b256_from_u64(value: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        B256::from(bytes)
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn commit_block_persists_accounts_and_metadata() -> Result<()> {
        let namespace = "state-worker:test";
        let block_number = 42_u64;
        let block_hash = B256::repeat_byte(0x2a);

        let primary_address = Address::repeat_byte(0x11);
        let primary_code = vec![0x60, 0x2a, 0x60, 0x00, 0xf3];
        let primary_code_hash = keccak256(&primary_code);
        let primary_storage = vec![
            (b256_from_u64(1), b256_from_u64(0xdead_beef)),
            (b256_from_u64(2), b256_from_u64(0xcafe_babe)),
        ];

        let deleted_address = Address::repeat_byte(0x33);
        let deleted_storage = vec![(b256_from_u64(3), B256::ZERO)];

        let update = BlockStateUpdate {
            block_number,
            block_hash,
            accounts: vec![
                AccountCommit {
                    address: primary_address,
                    balance: U256::from(1_000_000_u64),
                    nonce: 7,
                    code_hash: primary_code_hash,
                    code: Some(primary_code.clone()),
                    storage: primary_storage.clone(),
                    deleted: false,
                },
                AccountCommit {
                    address: deleted_address,
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: deleted_storage.clone(),
                    deleted: true,
                },
            ],
        };

        let account_key = format!("{namespace}:account:{}", hex::encode(primary_address));
        let code_key = format!("{namespace}:code:{}", hex::encode(primary_code_hash));
        let storage_key = format!("{namespace}:storage:{}", hex::encode(primary_address));
        let deleted_key = format!("{namespace}:account:{}", hex::encode(deleted_address));
        let deleted_storage_key = format!("{namespace}:storage:{}", hex::encode(deleted_address));
        let block_hash_key = format!("{namespace}:block_hash:{block_number}");
        let current_block_key = format!("{namespace}:current_block");

        let mut commands = Vec::new();
        commands.push(MockCmd::new(
            redis::cmd("HSET")
                .arg(&account_key)
                .arg("balance")
                .arg("1000000")
                .arg("nonce")
                .arg("7")
                .arg("code_hash")
                .arg(encode_b256(primary_code_hash)),
            Ok::<i64, RedisError>(1),
        ));
        commands.push(MockCmd::new(
            redis::cmd("SET")
                .arg(&code_key)
                .arg(encode_bytes(&primary_code)),
            Ok::<_, RedisError>("OK"),
        ));
        for (slot, value) in &primary_storage {
            commands.push(MockCmd::new(
                redis::cmd("HSET")
                    .arg(&storage_key)
                    .arg(encode_b256(*slot))
                    .arg(encode_b256(*value)),
                Ok::<i64, RedisError>(1),
            ));
        }
        commands.push(MockCmd::new(
            redis::cmd("HSET")
                .arg(&deleted_key)
                .arg("balance")
                .arg("0")
                .arg("nonce")
                .arg("0")
                .arg("code_hash")
                .arg(encode_b256(B256::ZERO)),
            Ok::<i64, RedisError>(1),
        ));
        for (slot, value) in &deleted_storage {
            commands.push(MockCmd::new(
                redis::cmd("HSET")
                    .arg(&deleted_storage_key)
                    .arg(encode_b256(*slot))
                    .arg(encode_b256(*value)),
                Ok::<i64, RedisError>(1),
            ));
        }
        commands.push(MockCmd::new(
            redis::cmd("SET")
                .arg(&block_hash_key)
                .arg(encode_b256(block_hash)),
            Ok::<_, RedisError>("OK"),
        ));
        commands.push(MockCmd::new(
            redis::cmd("SET")
                .arg(&current_block_key)
                .arg(block_number.to_string()),
            Ok::<_, RedisError>("OK"),
        ));

        let mut conn = MockRedisConnection::new(commands);
        commit_block_with_connection(&mut conn, namespace, update)?;
        Ok(())
    }

    #[test]
    fn commit_block_without_accounts_updates_metadata_only() -> Result<()> {
        let namespace = "state-worker:metadata";
        let block_number = 100_u64;
        let block_hash = B256::repeat_byte(0x44);
        let update = BlockStateUpdate {
            block_number,
            block_hash,
            accounts: Vec::new(),
        };

        let block_hash_key = format!("{namespace}:block_hash:{block_number}");
        let current_block_key = format!("{namespace}:current_block");

        let commands = vec![
            MockCmd::new(
                redis::cmd("SET")
                    .arg(&block_hash_key)
                    .arg(encode_b256(block_hash)),
                Ok::<_, RedisError>("OK"),
            ),
            MockCmd::new(
                redis::cmd("SET")
                    .arg(&current_block_key)
                    .arg(block_number.to_string()),
                Ok::<_, RedisError>("OK"),
            ),
        ];

        let mut conn = MockRedisConnection::new(commands);
        commit_block_with_connection(&mut conn, namespace, update)?;
        Ok(())
    }

    #[test]
    fn read_latest_block_number_parses_successfully() -> Result<()> {
        let key = "ns:current";
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("GET").arg(key),
            Ok::<_, RedisError>("123"),
        )]);

        let value = read_latest_block_number(&mut conn, key)?;
        assert_eq!(value, Some(123));
        Ok(())
    }

    #[test]
    fn read_latest_block_number_handles_missing_key() -> Result<()> {
        let key = "ns:current";
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("GET").arg(key),
            Ok::<_, RedisError>(Value::Nil),
        )]);

        let value = read_latest_block_number(&mut conn, key)?;
        assert_eq!(value, None);
        Ok(())
    }

    #[test]
    fn read_latest_block_number_surfaces_parse_error() {
        let key = "ns:current";
        let mut conn = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("GET").arg(key),
            Ok::<_, RedisError>("invalid"),
        )]);

        let err = read_latest_block_number(&mut conn, key).expect_err("expected parse failure");
        assert!(err.to_string().contains("invalid block number"));
    }
}
