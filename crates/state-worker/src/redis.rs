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

#[derive(Clone)]
pub struct RedisStateWriter {
    client: Arc<redis::Client>,
    namespace: String,
}

impl RedisStateWriter {
    pub fn new(redis_url: &str, namespace: String) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client: Arc::new(client),
            namespace,
        })
    }

    pub async fn latest_block_number(&self) -> Result<Option<u64>> {
        let key = self.current_block_key();
        self.with_connection(move |conn| {
            let value: Option<String> = redis::cmd("GET")
                .arg(&key)
                .query(conn)
                .map_err(|err| anyhow!(err))?;
            let parsed = value
                .map(|v| {
                    v.parse::<u64>()
                        .with_context(|| format!("invalid block number: {v}"))
                })
                .transpose()?;
            Ok(parsed)
        })
        .await
    }

    pub async fn commit_block(&self, update: BlockStateUpdate) -> Result<()> {
        let (block_number, block_hash, accounts) = update.into_parts();

        if accounts.is_empty() {
            return self.update_block_metadata(block_number, block_hash).await;
        }

        let namespace = self.namespace.clone();
        self.with_connection(move |conn| {
            for account in &accounts {
                write_account(conn, &namespace, account)?;
            }
            write_block_metadata(conn, &namespace, block_number, block_hash)?;
            Ok(())
        })
        .await
    }

    pub async fn update_block_metadata(&self, block_number: u64, block_hash: B256) -> Result<()> {
        let namespace = self.namespace.clone();
        self.with_connection(move |conn| {
            write_block_metadata(conn, &namespace, block_number, block_hash)
        })
        .await
    }

    fn current_block_key(&self) -> String {
        format!("{}:current_block", self.namespace)
    }

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

fn write_account(
    conn: &mut redis::Connection,
    namespace: &str,
    account: &AccountCommit,
) -> Result<()> {
    let account_key = format!("{}:account:{}", namespace, hex::encode(account.address));
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
        let code_key = format!("{}:code:{}", namespace, hex::encode(account.code_hash));
        let code_hex = encode_bytes(code);
        redis::cmd("SET")
            .arg(&code_key)
            .arg(code_hex)
            .query::<()>(conn)
            .map_err(|err| anyhow!(err))?;
    }

    if !account.storage.is_empty() || account.deleted {
        let storage_key = format!("{}:storage:{}", namespace, hex::encode(account.address));
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

fn write_block_metadata(
    conn: &mut redis::Connection,
    namespace: &str,
    block_number: u64,
    block_hash: B256,
) -> Result<()> {
    let block_hash_key = format!("{}:block_hash:{}", namespace, block_number);
    let block_hash_hex = encode_b256(block_hash);
    redis::cmd("SET")
        .arg(&block_hash_key)
        .arg(block_hash_hex)
        .query::<()>(conn)
        .map_err(|err| anyhow!(err))?;
    let current_block_key = format!("{}:current_block", namespace);
    redis::cmd("SET")
        .arg(&current_block_key)
        .arg(block_number.to_string())
        .query::<()>(conn)
        .map_err(|err| anyhow!(err))?;
    Ok(())
}

fn encode_b256(value: B256) -> String {
    format!("0x{}", hex::encode(value))
}

fn encode_bytes(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}
