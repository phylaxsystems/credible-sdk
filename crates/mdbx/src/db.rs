//! Database initialization and management for latest-state MDBX storage.

use crate::common::{
    error::{
        StateError,
        StateResult,
    },
    tables::TABLES,
};
use reth_db::{
    Database,
    mdbx::{
        DatabaseArguments,
        DatabaseEnv,
        DatabaseEnvKind,
    },
};
use reth_db_api::{
    models::ClientVersion,
    transaction::DbTx,
};
use std::{
    path::Path,
    sync::Arc,
};

/// Default maximum database size (2TB).
const DEFAULT_MAX_DB_SIZE: usize = 2 * 1024 * 1024 * 1024 * 1024;

/// Thin wrapper around the MDBX environment used by the state reader/writer.
#[derive(Clone, Debug)]
pub struct StateDb {
    inner: Arc<DatabaseEnv>,
}

impl StateDb {
    /// Open an existing database or create a new one.
    pub fn open(path: impl AsRef<Path>) -> StateResult<Self> {
        Self::open_with_size(path, DEFAULT_MAX_DB_SIZE)
    }

    /// Open with a custom maximum database size.
    pub fn open_with_size(path: impl AsRef<Path>, max_size: usize) -> StateResult<Self> {
        let path = path.as_ref();

        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|source| {
                StateError::CreateDir {
                    path: path.display().to_string(),
                    source,
                }
            })?;
        }

        let args = DatabaseArguments::new(ClientVersion::default())
            .with_geometry_max_size(Some(max_size))
            .with_exclusive(Some(false))
            .with_max_read_transaction_duration(Some(
                reth_libmdbx::MaxReadTransactionDuration::Unbounded,
            ));

        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: e.to_string(),
            }
        })?;

        {
            let tx = env.tx_mut().map_err(|e| {
                StateError::DatabaseOpen {
                    path: path.display().to_string(),
                    message: format!("Failed to start transaction: {e}"),
                }
            })?;

            for table_name in TABLES {
                tx.inner
                    .create_db(Some(table_name), reth_libmdbx::DatabaseFlags::default())
                    .map_err(|e| {
                        StateError::CreateTable {
                            table: table_name.to_string(),
                            message: e.to_string(),
                        }
                    })?;
            }

            tx.commit()
                .map_err(|e| StateError::CommitFailed(e.to_string()))?;
        }

        Ok(Self {
            inner: Arc::new(env),
        })
    }

    /// Open database in read-only mode.
    pub fn open_read_only(path: impl AsRef<Path>) -> StateResult<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: "Database does not exist".to_string(),
            });
        }

        let args = DatabaseArguments::new(ClientVersion::default())
            .with_geometry_max_size(Some(DEFAULT_MAX_DB_SIZE))
            .with_exclusive(Some(false))
            .with_max_read_transaction_duration(Some(
                reth_libmdbx::MaxReadTransactionDuration::Unbounded,
            ));

        let env = DatabaseEnv::open(path, DatabaseEnvKind::RO, args).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: e.to_string(),
            }
        })?;

        Ok(Self {
            inner: Arc::new(env),
        })
    }

    #[must_use]
    pub fn inner(&self) -> &DatabaseEnv {
        &self.inner
    }

    pub fn tx(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RO>> {
        self.inner.tx().map_err(StateError::Database)
    }

    pub fn tx_mut(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RW>> {
        self.inner.tx_mut().map_err(StateError::Database)
    }

    /// Latest-state MDBX always uses a single durable namespace.
    pub fn namespace_for_block(&self, _block_number: u64) -> Result<u8, StateError> {
        Ok(0)
    }

    /// Compatibility shim for older callers until sidecar/state-worker finish migrating.
    #[must_use]
    pub fn buffer_size(&self) -> u8 {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_creates_database() {
        let tmp = TempDir::new().unwrap();
        let db = StateDb::open(tmp.path().join("state")).unwrap();
        drop(db);

        let db2 = StateDb::open(tmp.path().join("state")).unwrap();
        assert_eq!(db2.buffer_size(), 1);
    }

    #[test]
    fn test_open_read_only() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let db = StateDb::open(&path).unwrap();
        drop(db);

        let db_ro = StateDb::open_read_only(&path).unwrap();
        assert_eq!(db_ro.namespace_for_block(999).unwrap(), 0);
    }

    #[test]
    fn test_open_read_only_nonexistent_fails() {
        let tmp = TempDir::new().unwrap();
        let result = StateDb::open_read_only(tmp.path().join("missing"));
        assert!(matches!(result, Err(StateError::DatabaseOpen { .. })));
    }
}
