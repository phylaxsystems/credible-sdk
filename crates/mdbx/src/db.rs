//! Database initialization and management for MDBX state storage.
//!
//! This module provides the `StateDb` wrapper around MDBX, handling:
//!
//! - Database creation and opening
//! - Table initialization
//! - Read-only vs read-write access modes
//! - Legacy layout compatibility checks
//!
//! ## Database Structure
//!
//! The database contains multiple named "databases" (we call them tables):
//!
//! ```text
//! MDBX Environment
//! ├── NamespaceBlocks     (reserved compatibility mirror of latest block)
//! ├── NamespacedAccounts  (address → account)
//! ├── NamespacedStorage   (address+slot → value)
//! ├── Bytecodes           (code_hash → bytecode)
//! ├── BlockMetadata       (block → hash+root)
//! ├── StateDiffs          (latest block diff retained for diagnostics)
//! └── Metadata            (0 → latest block)
//! ```

use crate::common::{
    error::{
        StateError,
        StateResult,
    },
    tables::{
        Bytecodes,
        Metadata,
        NamespacedAccounts,
        NamespacedStorage,
        TABLES,
    },
    types::GlobalMetadata,
};
use reth_codecs::Compact;
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
    table::Table,
    transaction::DbTx,
};
use std::{
    path::Path,
    sync::Arc,
};

/// Default maximum database size (2TB).
const DEFAULT_MAX_DB_SIZE: usize = 2 * 1024 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct StateDb {
    inner: Arc<DatabaseEnv>,
}

impl StateDb {
    /// Open an existing database or create a new one.
    ///
    /// # Errors
    ///
    /// Returns an error if the database directory cannot be created, MDBX
    /// cannot be opened, or legacy on-disk data requires a re-sync.
    pub fn open(path: impl AsRef<Path>) -> StateResult<Self> {
        Self::open_with_size(path, DEFAULT_MAX_DB_SIZE)
    }

    /// Open with a custom maximum database size.
    ///
    /// # Errors
    ///
    /// Returns an error if the database directory cannot be created, MDBX
    /// cannot be opened, or legacy on-disk data requires a re-sync.
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

        Self::validate_layout_compatibility(&env, path)?;

        Ok(Self {
            inner: Arc::new(env),
        })
    }

    /// Open an existing database in read-only mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the database does not exist, MDBX cannot be opened,
    /// or the on-disk layout requires a re-sync.
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

        Self::validate_layout_compatibility(&env, path)?;

        Ok(Self {
            inner: Arc::new(env),
        })
    }

    #[must_use]
    pub fn inner(&self) -> &DatabaseEnv {
        &self.inner
    }

    /// Start a read-only transaction.
    ///
    /// # Errors
    ///
    /// Returns any MDBX transaction startup error.
    pub fn tx(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RO>> {
        self.inner.tx().map_err(StateError::Database)
    }

    /// Start a read-write transaction.
    ///
    /// # Errors
    ///
    /// Returns any MDBX transaction startup error.
    pub fn tx_mut(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RW>> {
        self.inner.tx_mut().map_err(StateError::Database)
    }

    fn validate_layout_compatibility(env: &DatabaseEnv, path: &Path) -> StateResult<()> {
        let tx = env.tx().map_err(StateError::Database)?;

        Self::ensure_key_width(&tx, path, NamespacedAccounts::NAME, 32, 33)?;
        Self::ensure_key_width(&tx, path, NamespacedStorage::NAME, 64, 65)?;
        Self::ensure_key_width(&tx, path, Bytecodes::NAME, 32, 33)?;
        Self::ensure_metadata_encoding(&tx, path)?;

        Ok(())
    }

    fn ensure_key_width(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RO>,
        path: &Path,
        table_name: &'static str,
        expected_len: usize,
        legacy_len: usize,
    ) -> StateResult<()> {
        let db = tx.inner.open_db(Some(table_name)).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to open {table_name}: {e}"),
            }
        })?;
        let mut cursor = tx.inner.cursor_with_dbi(db.dbi()).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to open cursor for {table_name}: {e}"),
            }
        })?;

        if let Some((key, _)) = cursor.first::<Vec<u8>, Vec<u8>>().map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to read first entry from {table_name}: {e}"),
            }
        })? {
            Self::check_width(path, table_name, key.len(), expected_len, legacy_len)?;
        }

        Ok(())
    }

    fn ensure_metadata_encoding(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RO>,
        path: &Path,
    ) -> StateResult<()> {
        let table_name = Metadata::NAME;
        let db = tx.inner.open_db(Some(table_name)).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to open {table_name}: {e}"),
            }
        })?;
        let mut cursor = tx.inner.cursor_with_dbi(db.dbi()).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to open cursor for {table_name}: {e}"),
            }
        })?;

        if let Some((_key, value)) = cursor.first::<Vec<u8>, Vec<u8>>().map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: format!("failed to read first entry from {table_name}: {e}"),
            }
        })? {
            let (_metadata, remainder) = GlobalMetadata::from_compact(&value, value.len());
            if !remainder.is_empty() {
                return Err(StateError::LegacyLayoutRequiresResync {
                    path: path.display().to_string(),
                    table: table_name,
                });
            }
        }

        Ok(())
    }

    fn check_width(
        path: &Path,
        table_name: &'static str,
        observed_len: usize,
        expected_len: usize,
        legacy_len: usize,
    ) -> StateResult<()> {
        if observed_len == expected_len {
            return Ok(());
        }

        if observed_len == legacy_len {
            return Err(StateError::LegacyLayoutRequiresResync {
                path: path.display().to_string(),
                table: table_name,
            });
        }

        Err(StateError::DatabaseOpen {
            path: path.display().to_string(),
            message: format!("unexpected encoded width {observed_len} in {table_name}"),
        })
    }
}

impl std::fmt::Debug for StateDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateDb").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_db_api::transaction::DbTx;
    use tempfile::TempDir;

    #[test]
    fn test_db_create_and_open() {
        let tmp = TempDir::new().unwrap();

        let db = StateDb::open(tmp.path().join("state")).unwrap();
        drop(db);

        let reopened = StateDb::open(tmp.path().join("state")).unwrap();
        drop(reopened);
    }

    #[test]
    fn test_db_read_only() {
        let tmp = TempDir::new().unwrap();

        let db = StateDb::open(tmp.path().join("state")).unwrap();
        drop(db);

        let db_ro = StateDb::open_read_only(tmp.path().join("state")).unwrap();
        assert!(db_ro.tx().is_ok());
    }

    #[test]
    fn test_db_read_only_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let result = StateDb::open_read_only(tmp.path().join("nonexistent"));
        assert!(result.is_err());
    }

    #[test]
    fn test_transactions() {
        let tmp = TempDir::new().unwrap();
        let db = StateDb::open(tmp.path().join("state")).unwrap();

        {
            let _tx = db.tx().unwrap();
        }

        {
            let _tx = db.tx_mut().unwrap();
        }

        {
            let tx = db.tx_mut().unwrap();
            tx.commit().unwrap();
        }
    }
}
