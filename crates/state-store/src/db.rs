//! Database initialization and management for MDBX state storage.
//!
//! This module provides the `StateDb` wrapper around MDBX, handling:
//!
//! - Database creation and opening
//! - Table initialization
//! - Read-only vs read-write access modes
//! - Namespace calculation helpers
//!
//! ## Database Structure
//!
//! The database contains multiple named "databases" (we call them tables):
//!
//! ```text
//! MDBX Environment
//! ├── NamespaceBlocks     (namespace → block)
//! ├── NamespacedAccounts  (namespace+address → account)
//! ├── NamespacedStorage   (namespace+address+slot → value)
//! ├── Bytecodes           (code_hash → bytecode)
//! ├── BlockMetadata       (block → hash+root)
//! ├── StateDiffs          (block → JSON diff)
//! └── Metadata            (0 → global metadata, e.g., latest block)
//! ```

use crate::common::{
    error::{
        StateError,
        StateResult,
    },
    tables::TABLES,
    types::CircularBufferConfig,
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
///
/// MDBX requires specifying a maximum size upfront for the memory map.
/// This doesn't allocate the space - the file grows as needed.
const DEFAULT_MAX_DB_SIZE: usize = 2 * 1024 * 1024 * 1024 * 1024;

/// State database wrapper providing access to the MDBX environment.
///
/// This is a thin wrapper around `DatabaseEnv` that:
/// - Manages the MDBX environment lifecycle
/// - Creates all required tables on first open
/// - Provides transaction helpers
/// - Calculates namespace indices
///
/// ## Example
///
/// ```ignore
/// use state_store::{StateDb, CircularBufferConfig};
///
/// let config = CircularBufferConfig::new(100)?;
/// let db = StateDb::open("/path/to/db", config)?;
///
/// // Read transaction
/// let tx = db.tx()?;
/// // ... read operations ...
/// // tx is automatically rolled back on drop
///
/// // Write transaction
/// let tx = db.tx_mut()?;
/// // ... write operations ...
/// tx.commit()?;
/// ```
#[derive(Clone)]
pub struct StateDb {
    inner: Arc<DatabaseEnv>,
    pub config: CircularBufferConfig,
}

impl StateDb {
    /// Open an existing database or create a new one.
    ///
    /// Creates the database directory if it doesn't exist.
    /// Creates all required tables if they don't exist.
    ///
    /// Uses the default maximum size of 1TB.
    pub fn open(path: impl AsRef<Path>, config: CircularBufferConfig) -> StateResult<Self> {
        Self::open_with_size(path, config, DEFAULT_MAX_DB_SIZE)
    }

    /// Open with custom maximum database size.
    ///
    /// The size is the maximum the database can grow to. MDBX uses
    /// sparse files, so disk usage only grows as data is added.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory for the database files
    /// * `config` - Circular buffer configuration
    /// * `max_size` - Maximum database size in bytes
    pub fn open_with_size(
        path: impl AsRef<Path>,
        config: CircularBufferConfig,
        max_size: usize,
    ) -> StateResult<Self> {
        let path = path.as_ref();

        // Create the directory if it doesn't exist
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                StateError::CreateDir {
                    path: path.display().to_string(),
                    source: e,
                }
            })?;
        }

        // Configure MDBX
        let args = DatabaseArguments::new(ClientVersion::default())
            .with_geometry_max_size(Some(max_size))
            .with_max_read_transaction_duration(Some(
                reth_libmdbx::MaxReadTransactionDuration::Unbounded,
            ));

        // Open environment in read-write mode
        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: e.to_string(),
            }
        })?;

        // Create all tables
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
            config,
        })
    }

    /// Open database in read-only mode.
    ///
    /// The database must already exist. This mode is more efficient for
    /// pure read workloads and ensures no accidental writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the database doesn't exist.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        config: CircularBufferConfig,
    ) -> StateResult<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(StateError::DatabaseOpen {
                path: path.display().to_string(),
                message: "Database does not exist".to_string(),
            });
        }

        let args = DatabaseArguments::new(ClientVersion::default())
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
            config,
        })
    }

    /// Get reference to the inner database environment.
    ///
    /// Useful for advanced operations not exposed through the wrapper.
    pub fn inner(&self) -> &DatabaseEnv {
        &self.inner
    }

    /// Start a read-only transaction.
    ///
    /// Read transactions:
    /// - See a consistent snapshot of the database
    /// - Don't block writers
    /// - Are automatically rolled back on drop (no commit needed)
    /// - Can run concurrently with other read transactions
    pub fn tx(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RO>> {
        self.inner.tx().map_err(StateError::Database)
    }

    /// Start a read-write transaction.
    ///
    /// Write transactions:
    /// - Must call `commit()` to persist changes
    /// - Are rolled back if dropped without commit
    /// - Only one can be active at a time (MDBX enforces this)
    /// - Don't block readers
    pub fn tx_mut(&self) -> StateResult<reth_db::mdbx::tx::Tx<reth_libmdbx::RW>> {
        self.inner.tx_mut().map_err(StateError::Database)
    }

    /// Get the namespace index for a block number.
    #[inline]
    pub fn namespace_for_block(&self, block_number: u64) -> Result<u8, StateError> {
        self.config.namespace_for_block(block_number)
    }

    /// Get the buffer size.
    #[inline]
    pub fn buffer_size(&self) -> u8 {
        self.config.buffer_size
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for StateDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateDb")
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_db_create_and_open() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();

        // Create new database
        let db = StateDb::open(tmp.path().join("state"), config.clone()).unwrap();
        drop(db);

        // Reopen existing database
        let db2 = StateDb::open(tmp.path().join("state"), config).unwrap();
        drop(db2);
    }

    #[test]
    fn test_db_read_only() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();

        // Create database first
        let db = StateDb::open(tmp.path().join("state"), config.clone()).unwrap();
        drop(db);

        // Open read-only
        let db_ro = StateDb::open_read_only(tmp.path().join("state"), config).unwrap();
        assert!(db_ro.tx().is_ok());
    }

    #[test]
    fn test_db_read_only_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();

        // Should fail - database doesn't exist
        let result = StateDb::open_read_only(tmp.path().join("nonexistent"), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_namespace_calculation() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();
        let db = StateDb::open(tmp.path().join("state"), config).unwrap();

        assert_eq!(db.namespace_for_block(0).unwrap(), 0);
        assert_eq!(db.namespace_for_block(1).unwrap(), 1);
        assert_eq!(db.namespace_for_block(2).unwrap(), 2);
        assert_eq!(db.namespace_for_block(3).unwrap(), 0);
        assert_eq!(db.namespace_for_block(100).unwrap(), 1); // 100 % 3 = 1
    }

    #[test]
    fn test_buffer_size() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(5).unwrap();
        let db = StateDb::open(tmp.path().join("state"), config).unwrap();

        assert_eq!(db.buffer_size(), 5);
    }

    #[test]
    fn test_transactions() {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();
        let db = StateDb::open(tmp.path().join("state"), config).unwrap();

        // Read transaction
        {
            let _tx = db.tx().unwrap();
            // Automatically rolled back on drop
        }

        // Write transaction without commit (rolled back)
        {
            let _tx = db.tx_mut().unwrap();
            // Automatically rolled back on drop
        }

        // Write transaction with commit
        {
            let tx = db.tx_mut().unwrap();
            tx.commit().unwrap();
        }
    }
}
