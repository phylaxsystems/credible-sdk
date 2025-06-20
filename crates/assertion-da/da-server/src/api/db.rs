use crate::{
    api::types::{
        DbOperation,
        DbRequest,
        DbResponse,
    },
    LEAF_FANOUT,
};

use sled::Db;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Listens to a mpsc channel for database events and responds
/// accordingly.
pub async fn listen_for_db(
    mut rx: mpsc::UnboundedReceiver<DbRequest>,
    db: Db<{ LEAF_FANOUT }>,
    cancel_token: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Database listener received cancellation signal, shutting down...");
                break;
            }
            Some(req) = rx.recv() => {
                let res = match req.request {
                    DbOperation::Get(key) => db_get(&db, &key)?,
                    DbOperation::Insert(key, value) => {
                        db_insert(&db, &key, &value)?;
                        None
                    }
                };

                let _ = req.response.send(res);
            }
        }
    }
    Ok(())
}

fn db_get(db: &Db<{ LEAF_FANOUT }>, key: &Vec<u8>) -> Result<Option<DbResponse>> {
    let rax = db.get(key)?;
    if rax.is_none() {
        return Ok(None);
    }
    let rax = rax.unwrap().to_vec();
    Ok(Some(DbResponse::Value(rax)))
}

fn db_insert(db: &Db<{ LEAF_FANOUT }>, key: &Vec<u8>, value: &Vec<u8>) -> Result<()> {
    db.insert(key, value.to_owned())?;

    let db_size = db.size_on_disk()? / (1024 * 1024);
    metrics::gauge!("db_size_mb").set(db_size as f64);
    metrics::gauge!("database_assertions_sum").increment(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_db_operations() {
        let db = sled::Config::tmp().unwrap().open().unwrap();

        // Test insert
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        db_insert(&db, &key, &value).unwrap();

        // Test get
        let result = db_get(&db, &key).unwrap();
        assert!(result.is_some());
        match result {
            Some(DbResponse::Value(val)) => assert_eq!(val, value),
            _ => panic!("Unexpected response type"),
        }

        // Test get non-existent
        let missing_key = vec![7, 8, 9];
        let result = db_get(&db, &missing_key).unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_listen_for_db() {
        let db = sled::Config::tmp().unwrap().open().unwrap();

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
}
