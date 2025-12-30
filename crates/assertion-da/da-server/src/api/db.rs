use crate::api::types::{
    DbOperation,
    DbRequest,
    DbResponse,
};

use r2d2::Pool;
use redis::Client;
use sled::Db;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Database trait implementing basic query and put/insert functionality
pub trait Database {
    fn query(&self, key: &[u8]) -> Result<Option<DbResponse>>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<DbResponse>>;
}

// Sled implementation
// TODO: maybe behind a feature flag?
impl<const FANOUT: usize> Database for Db<FANOUT> {
    fn query(&self, key: &[u8]) -> Result<Option<DbResponse>> {
        match self.get(key)? {
            Some(value) => Ok(Some(DbResponse::Value(value.to_vec()))),
            None => Ok(None),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<DbResponse>> {
        match self.insert(key, value.to_vec())? {
            // TODO: Check if we want this behavior (doesn't overwrite)
            Some(old_value) => Ok(Some(DbResponse::Value(old_value.to_vec()))),
            None => Ok(None),
        }
    }
}

// Redis implementation
// TODO: maybe behind a feature flag?
pub struct RedisDb {
    pool: Pool<Client>,
}

impl RedisDb {
    pub fn new(client: Client) -> Result<Self> {
        let pool = r2d2::Pool::builder().build(client)?;
        Ok(Self { pool })
    }
}

impl Database for RedisDb {
    fn query(&self, key: &[u8]) -> Result<Option<DbResponse>> {
        let mut conn = self.pool.get()?;
        let value: Option<Vec<u8>> = redis::cmd("GET").arg(key).query(&mut conn)?;
        Ok(value.map(DbResponse::Value))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<Option<DbResponse>> {
        let mut conn = self.pool.get()?;
        // GETSET returns the old value before setting the new one
        let old_value: Option<Vec<u8>> = redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("GET")
            .query(&mut conn)?;
        Ok(old_value.map(DbResponse::Value))
    }
}

/// Listens to a mpsc channel for database events and responds
/// accordingly.
pub async fn listen_for_db<DB: Database>(
    mut rx: mpsc::UnboundedReceiver<DbRequest>,
    db: DB,
    cancel_token: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            () = cancel_token.cancelled() => {
                tracing::info!("Database listener received cancellation signal, shutting down...");
                break;
            }
            Some(req) = rx.recv() => {
                let res = match req.request {
                    DbOperation::Get(key) => db.query(&key)?,
                    DbOperation::Insert(key, value) => {
                        db.put(&key, &value)?;
                        metrics::gauge!("database_assertions_sum").increment(1);
                        None
                    }
                };

                let _ = req.response.send(res);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sled::Db;
    use testcontainers::ImageExt;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_db_operations() {
        let db: Db<{ crate::LEAF_FANOUT }> = sled::Config::tmp().unwrap().open().unwrap();

        // Test insert
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        db.put(&key, &value).unwrap();

        // Test get
        let result = db.query(&key).unwrap();
        assert!(result.is_some());
        match result {
            Some(DbResponse::Value(val)) => assert_eq!(val, value),
            _ => panic!("Unexpected response type"),
        }

        // Test get non-existent
        let missing_key = vec![7, 8, 9];
        let result = db.query(&missing_key).unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_listen_for_db() {
        let db: Db<{ crate::LEAF_FANOUT }> = sled::Config::tmp().unwrap().open().unwrap();

        let (tx, rx) = mpsc::unbounded_channel();

        let cancel_token = CancellationToken::new();
        // Spawn the listener
        let handle = tokio::spawn(listen_for_db(rx, db.clone(), cancel_token.clone()));

        // Test get operation
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(DbRequest {
            request: DbOperation::Get(vec![1, 2, 3]),
            response: resp_tx,
        })
        .unwrap();
        let result = resp_rx.await.unwrap();
        assert!(result.is_none()); // Key doesn't exist

        // Test insert operation
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(DbRequest {
            request: DbOperation::Insert(vec![1, 2, 3], vec![4, 5, 6]),
            response: resp_tx,
        })
        .unwrap();
        let _ = resp_rx.await.unwrap();

        // Verify insertion worked
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(DbRequest {
            request: DbOperation::Get(vec![1, 2, 3]),
            response: resp_tx,
        })
        .unwrap();
        let result = resp_rx.await.unwrap();
        assert!(result.is_some());

        // Clean up
        cancel_token.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_redis_db_operations() {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::redis::Redis;

        // Start Redis container
        let container = Redis::default().with_tag("7-alpine").start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();

        // Connect to Redis
        let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let db = RedisDb::new(client).unwrap();

        // Test insert (first insert returns None since key didn't exist)
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let result = db.put(&key, &value).unwrap();
        assert!(result.is_none());

        // Test query
        let result = db.query(&key).unwrap();
        assert!(result.is_some());
        match result {
            Some(DbResponse::Value(val)) => assert_eq!(val, value),
            _ => panic!("Unexpected response type"),
        }

        // Test put returns old value
        let new_value = vec![7, 8, 9];
        let result = db.put(&key, &new_value).unwrap();
        match result {
            Some(DbResponse::Value(val)) => assert_eq!(val, value),
            _ => panic!("Expected old value to be returned"),
        }

        // Verify new value was set
        let result = db.query(&key).unwrap();
        match result {
            Some(DbResponse::Value(val)) => assert_eq!(val, new_value),
            _ => panic!("Unexpected response type"),
        }

        // Test query non-existent key
        let missing_key = vec![10, 11, 12];
        let result = db.query(&missing_key).unwrap();
        assert!(result.is_none());
    }
}
