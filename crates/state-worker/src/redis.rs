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

    #[tokio::test]
    async fn commit_block_persists_accounts_and_metadata() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

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

        // Write to Redis
        commit_block_with_connection(&mut conn, namespace, update)?;

        // Verify primary account was written correctly
        let account_key = format!("{namespace}:account:{}", hex::encode(primary_address));
        let balance: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("balance")
            .query(&mut conn)?;
        assert_eq!(balance, "1000000");

        let nonce: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("nonce")
            .query(&mut conn)?;
        assert_eq!(nonce, "7");

        let code_hash: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("code_hash")
            .query(&mut conn)?;
        assert_eq!(code_hash, encode_b256(primary_code_hash));

        // Verify code was stored
        let code_key = format!("{namespace}:code:{}", hex::encode(primary_code_hash));
        let stored_code: String = redis::cmd("GET").arg(&code_key).query(&mut conn)?;
        assert_eq!(stored_code, encode_bytes(&primary_code));

        // Verify storage slots
        let storage_key = format!("{namespace}:storage:{}", hex::encode(primary_address));
        for (slot, expected_value) in &primary_storage {
            let value: String = redis::cmd("HGET")
                .arg(&storage_key)
                .arg(encode_b256(*slot))
                .query(&mut conn)?;
            assert_eq!(value, encode_b256(*expected_value));
        }

        // Verify deleted account
        let deleted_key = format!("{namespace}:account:{}", hex::encode(deleted_address));
        let deleted_balance: String = redis::cmd("HGET")
            .arg(&deleted_key)
            .arg("balance")
            .query(&mut conn)?;
        assert_eq!(deleted_balance, "0");

        let deleted_nonce: String = redis::cmd("HGET")
            .arg(&deleted_key)
            .arg("nonce")
            .query(&mut conn)?;
        assert_eq!(deleted_nonce, "0");

        // Verify deleted storage
        let deleted_storage_key = format!("{namespace}:storage:{}", hex::encode(deleted_address));
        for (slot, expected_value) in &deleted_storage {
            let value: String = redis::cmd("HGET")
                .arg(&deleted_storage_key)
                .arg(encode_b256(*slot))
                .query(&mut conn)?;
            assert_eq!(value, encode_b256(*expected_value));
        }

        // Verify block metadata
        let block_hash_key = format!("{namespace}:block_hash:{block_number}");
        let stored_hash: String = redis::cmd("GET").arg(&block_hash_key).query(&mut conn)?;
        assert_eq!(stored_hash, encode_b256(block_hash));

        let current_block_key = format!("{namespace}:current_block");
        let stored_block: String = redis::cmd("GET").arg(&current_block_key).query(&mut conn)?;
        assert_eq!(stored_block, block_number.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn commit_block_without_accounts_updates_metadata_only() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let namespace = "state-worker:metadata";
        let block_number = 100_u64;
        let block_hash = B256::repeat_byte(0x44);

        let update = BlockStateUpdate {
            block_number,
            block_hash,
            accounts: Vec::new(),
        };

        // Write to Redis
        commit_block_with_connection(&mut conn, namespace, update)?;

        // Verify block metadata was written
        let block_hash_key = format!("{namespace}:block_hash:{block_number}");
        let stored_hash: String = redis::cmd("GET").arg(&block_hash_key).query(&mut conn)?;
        assert_eq!(stored_hash, encode_b256(block_hash));

        let current_block_key = format!("{namespace}:current_block");
        let stored_block: String = redis::cmd("GET").arg(&current_block_key).query(&mut conn)?;
        assert_eq!(stored_block, block_number.to_string());

        Ok(())
    }

    #[tokio::test]
    async fn read_latest_block_number_parses_successfully() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let key = "ns:current";

        // Set up the data
        redis::cmd("SET")
            .arg(key)
            .arg("123")
            .query::<()>(&mut conn)?;

        // Test reading it back
        let value = read_latest_block_number(&mut conn, key)?;
        assert_eq!(value, Some(123));

        Ok(())
    }

    #[tokio::test]
    async fn read_latest_block_number_handles_missing_key() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let key = "ns:current";

        // Don't set anything - key doesn't exist
        let value = read_latest_block_number(&mut conn, key)?;
        assert_eq!(value, None);

        Ok(())
    }

    #[tokio::test]
    async fn read_latest_block_number_surfaces_parse_error() {
        let (_container, mut conn) = setup_redis().await;

        let key = "ns:current";

        // Set invalid data
        redis::cmd("SET")
            .arg(key)
            .arg("invalid")
            .query::<()>(&mut conn)
            .unwrap();

        // Test that parsing fails
        let err = read_latest_block_number(&mut conn, key).expect_err("expected parse failure");
        assert!(err.to_string().contains("invalid block number"));
    }

    #[tokio::test]
    async fn async_writer_commit_block() -> Result<()> {
        let container = Redis::default()
            .start()
            .await
            .expect("Failed to start Redis container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get port");

        let namespace = "state-worker:async".to_string();
        let writer = RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone())?;

        let block_number = 99_u64;
        let block_hash = B256::repeat_byte(0x99);
        let address = Address::repeat_byte(0xaa);

        let update = BlockStateUpdate {
            block_number,
            block_hash,
            accounts: vec![AccountCommit {
                address,
                balance: U256::from(5000u64),
                nonce: 3,
                code_hash: B256::ZERO,
                code: None,
                storage: vec![],
                deleted: false,
            }],
        };

        // Commit through the async writer
        writer.commit_block(update).await?;

        // Verify with a fresh connection
        let client = redis::Client::open(format!("redis://{host}:{port}"))?;
        let mut conn = client.get_connection()?;

        let account_key = format!("{}:account:{}", namespace, hex::encode(address));
        let balance: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("balance")
            .query(&mut conn)?;
        assert_eq!(balance, "5000");

        let current_block_key = format!("{namespace}:current_block");
        let stored_block: String = redis::cmd("GET").arg(&current_block_key).query(&mut conn)?;
        assert_eq!(stored_block, "99");

        Ok(())
    }

    #[tokio::test]
    async fn async_writer_latest_block_number() -> Result<()> {
        let container = Redis::default()
            .start()
            .await
            .expect("Failed to start Redis container");

        let host = container.get_host().await.expect("Failed to get host");
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("Failed to get port");

        let namespace = "state-worker:latest".to_string();
        let writer = RedisStateWriter::new(&format!("redis://{host}:{port}"), namespace.clone())?;

        // Initially should be None
        let latest = writer.latest_block_number().await?;
        assert_eq!(latest, None);

        // Write a block
        let block_number = 42_u64;
        let block_hash = B256::repeat_byte(0x42);
        writer
            .update_block_metadata(block_number, block_hash)
            .await?;

        // Should now return the block number
        let latest = writer.latest_block_number().await?;
        assert_eq!(latest, Some(42));

        Ok(())
    }

    #[tokio::test]
    async fn write_account_with_all_fields() -> Result<()> {
        let (_container, mut conn) = setup_redis().await;

        let namespace = "test";
        let address = Address::repeat_byte(0xbb);
        let code = vec![0x60, 0x80, 0x60, 0x40];
        let code_hash = keccak256(&code);

        let account = AccountCommit {
            address,
            balance: U256::from(999u64),
            nonce: 5,
            code_hash,
            code: Some(code.clone()),
            storage: vec![
                (b256_from_u64(10), b256_from_u64(100)),
                (b256_from_u64(20), b256_from_u64(200)),
            ],
            deleted: false,
        };

        // Write the account
        write_account(&mut conn, namespace, &account)?;

        // Verify all fields
        let account_key = format!("{namespace}:account:{}", hex::encode(address));

        let balance: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("balance")
            .query(&mut conn)?;
        assert_eq!(balance, "999");

        let nonce: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("nonce")
            .query(&mut conn)?;
        assert_eq!(nonce, "5");

        let stored_code_hash: String = redis::cmd("HGET")
            .arg(&account_key)
            .arg("code_hash")
            .query(&mut conn)?;
        assert_eq!(stored_code_hash, encode_b256(code_hash));

        // Verify code
        let code_key = format!("{namespace}:code:{}", hex::encode(code_hash));
        let stored_code: String = redis::cmd("GET").arg(&code_key).query(&mut conn)?;
        assert_eq!(stored_code, encode_bytes(&code));

        // Verify storage
        let storage_key = format!("{namespace}:storage:{}", hex::encode(address));
        let slot10_value: String = redis::cmd("HGET")
            .arg(&storage_key)
            .arg(encode_b256(b256_from_u64(10)))
            .query(&mut conn)?;
        assert_eq!(slot10_value, encode_b256(b256_from_u64(100)));

        let slot20_value: String = redis::cmd("HGET")
            .arg(&storage_key)
            .arg(encode_b256(b256_from_u64(20)))
            .query(&mut conn)?;
        assert_eq!(slot20_value, encode_b256(b256_from_u64(200)));

        Ok(())
    }
}
