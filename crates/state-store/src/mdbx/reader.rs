//! State reader implementation for querying blockchain state from MDBX.
//!
//! Provides O(1) reads for any block currently in the circular buffer.
//!
//! ## Concurrency Safety
//!
//! MDBX provides MVCC (Multi-Version Concurrency Control), which means:
//!
//! - Readers see a consistent snapshot from when their transaction started
//! - Writers don't block readers
//! - Readers never see partial writes
//!
//! This means you can safely read while writes are happening - you'll either
//! see the complete old state or the complete new state, never something in between.
//!
//! ## Example
//!
//! ```ignore
//! use state_store::{StateReader, CircularBufferConfig, AddressHash};
//! use alloy::primitives::B256;
//!
//! let config = CircularBufferConfig::new(100)?;
//! let reader = StateReader::new("/path/to/db", config)?;
//!
//! // Check available blocks
//! if let Some((oldest, latest)) = reader.get_available_block_range()? {
//!     println!("Available: {} to {}", oldest, latest);
//! }
//!
//! // Read account
//! let addr_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));
//! if let Some(account) = reader.get_account(addr_hash, 12345)? {
//!     println!("Balance: {}", account.balance);
//! }
//! ```

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    Reader,
    mdbx::{
        common::{
            error::{
                StateError,
                StateResult,
            },
            tables::{
                BlockMetadataTable,
                BlockNumber,
                Bytecodes,
                Metadata,
                MetadataKey,
                NamespaceBlocks,
                NamespaceIdx,
                NamespacedAccounts,
                NamespacedStorage,
            },
            types::{
                CircularBufferConfig,
                NamespacedAccountKey,
                NamespacedBytecodeKey,
                NamespacedStorageKey,
            },
        },
        db::StateDb,
    },
};
use alloy::primitives::{
    B256,
    Bytes,
    KECCAK256_EMPTY,
    U256,
};
use reth_db_api::{
    cursor::DbCursorRO,
    transaction::DbTx,
};
use std::{
    collections::HashMap,
    path::Path,
};

/// State reader for querying blockchain state from MDBX.
#[derive(Clone, Debug)]
pub struct StateReader {
    db: StateDb,
}

impl Reader for StateReader {
    type Error = StateError;

    /// Get the most recent block number.
    ///
    /// Returns `None` if no blocks have been written yet.
    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let tx = self.db.tx()?;
        let meta = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?;
        Ok(meta.map(|m| m.latest_block))
    }

    /// Check if a block is available in the circular buffer.
    ///
    /// A block is available if its namespace currently contains that block.
    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        let tx = self.db.tx()?;
        let namespace_idx = NamespaceIdx(self.db.namespace_for_block(block_number)?);

        let ns_block = tx
            .get::<NamespaceBlocks>(namespace_idx)
            .map_err(StateError::Database)?;
        Ok(ns_block.map(|b| b.0) == Some(block_number))
    }

    /// Get account info (`balance`, `nonce`, `code_hash`) without storage.
    ///
    /// This is the fastest way to get basic account data. If you also need
    /// storage, use `get_full_account()` instead.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountInfo>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        // Verify the block is in namespace
        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let key = NamespacedAccountKey::new(namespace_idx, address_hash);
        let account = tx
            .get::<NamespacedAccounts>(key)
            .map_err(StateError::Database)?;

        Ok(account.map(|a| {
            AccountInfo {
                address_hash,
                balance: a.balance,
                nonce: a.nonce,
                code_hash: a.code_hash,
            }
        }))
    }

    /// Get a specific storage slot value.
    ///
    /// Returns `None` if the slot doesn't exist or has value zero.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        // Verify the block is in namespace
        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let key = NamespacedStorageKey::new(namespace_idx, address_hash, slot_hash);
        let value = tx
            .get::<NamespacedStorage>(key)
            .map_err(StateError::Database)?;

        Ok(value.map(|v| v.0))
    }

    /// Get all storage slots for an account.
    ///
    /// # Warning
    ///
    /// This can be expensive for contracts with many slots (e.g., ERC20 with
    /// thousands of holders). Consider using `get_storage()` for specific slots
    /// when possible.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<HashMap<B256, U256>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        // Verify the block is in namespace
        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let mut result = HashMap::new();
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;

        // Create the prefix to start iteration
        let start_key = NamespacedStorageKey::new(namespace_idx, address_hash, B256::ZERO);

        // Iterate from the start key
        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)? {
            // Check if it belongs to the same account
            if key.namespace_idx == namespace_idx && key.address_hash == address_hash {
                result.insert(key.slot_hash, value.0);

                // Continue iterating
                while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                    if key.namespace_idx != namespace_idx || key.address_hash != address_hash {
                        break;
                    }
                    result.insert(key.slot_hash, value.0);
                }
            }
        }

        Ok(result)
    }

    /// Get contract bytecode by code hash.
    ///
    /// Bytecode is content-addressed and shared across all namespaces,
    /// so this only verifies the block exists, then looks up by hash.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        // Verify block exists
        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let result = tx
            .get::<Bytecodes>(NamespacedBytecodeKey::new(namespace_idx, code_hash))
            .map_err(StateError::Database)?;
        Ok(result.map(|b| b.0))
    }

    /// Get complete account state including storage.
    ///
    /// # Warning
    ///
    /// This can transfer large amounts of data for contracts with many storage
    /// slots. Use `get_account()` if you only need balance/nonce.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        // Verify the block is in namespace
        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let key = NamespacedAccountKey::new(namespace_idx, address_hash);
        let Some(account) = tx
            .get::<NamespacedAccounts>(key)
            .map_err(StateError::Database)?
        else {
            return Ok(None);
        };

        // Get storage
        let mut storage = HashMap::new();
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(namespace_idx, address_hash, B256::ZERO);

        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == namespace_idx
            && key.address_hash == address_hash
        {
            storage.insert(key.slot_hash, value.0);
            while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != namespace_idx || key.address_hash != address_hash {
                    break;
                }
                storage.insert(key.slot_hash, value.0);
            }
        }

        // Get code if not empty
        let code = if account.code_hash != KECCAK256_EMPTY && account.code_hash != B256::ZERO {
            tx.get::<Bytecodes>(NamespacedBytecodeKey::new(namespace_idx, account.code_hash))
                .map_err(StateError::Database)?
                .map(|b| b.0)
        } else {
            None
        };

        Ok(Some(AccountState {
            address_hash,
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code,
            storage,
            deleted: false,
        }))
    }

    /// Get block hash for a specific block number.
    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        let meta = self.get_block_metadata_internal(block_number)?;
        Ok(meta.map(|m| m.block_hash))
    }

    /// Get state root for a specific block number.
    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        let meta = self.get_block_metadata_internal(block_number)?;
        Ok(meta.map(|m| m.state_root))
    }

    /// Get block metadata (hash and state root).
    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        let meta = self.get_block_metadata_internal(block_number)?;
        Ok(meta.map(|m| {
            BlockMetadata {
                block_number,
                block_hash: m.block_hash,
                state_root: m.state_root,
            }
        }))
    }

    /// Get the range of available blocks [oldest, latest].
    ///
    /// Returns `None` if no blocks have been written.
    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        let latest = self.latest_block_number()?;

        match latest {
            Some(latest_block) => {
                let oldest = latest_block.saturating_sub(u64::from(self.db.buffer_size()) - 1);
                Ok(Some((oldest, latest_block)))
            }
            None => Ok(None),
        }
    }
}

impl StateReader {
    /// Create a new reader with read-only database access.
    pub fn new(path: impl AsRef<Path>, config: CircularBufferConfig) -> StateResult<Self> {
        let db = StateDb::open_read_only(path, config)?;
        Ok(Self { db })
    }

    /// Get the configured buffer size.
    pub fn buffer_size(&self) -> u8 {
        self.db.buffer_size()
    }

    /// Scan all account hashes in the buffer for a specific block.
    ///
    /// This returns the address hashes (keccak256 of addresses), not the
    /// original addresses. Useful for iteration/debugging.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    pub fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        let tx = self.db.tx()?;
        let namespace_idx = self.db.namespace_for_block(block_number)?;

        Self::verify_block_available(&tx, namespace_idx, block_number)?;

        let mut result = Vec::new();
        let mut cursor = tx
            .cursor_read::<NamespacedAccounts>()
            .map_err(StateError::Database)?;

        // Start from the beginning of this namespace
        let start_key = NamespacedAccountKey::new(namespace_idx, AddressHash::default());

        if let Some((key, _)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == namespace_idx
        {
            result.push(key.address_hash);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != namespace_idx {
                    break;
                }
                result.push(key.address_hash);
            }
        }

        Ok(result)
    }

    /// Get block metadata from internal table format.
    fn get_block_metadata_internal(
        &self,
        block_number: u64,
    ) -> StateResult<Option<crate::mdbx::common::types::BlockMetadata>> {
        let tx = self.db.tx()?;
        tx.get::<BlockMetadataTable>(BlockNumber(block_number))
            .map_err(StateError::Database)
    }

    /// Verify that the block is in the expected namespace.
    ///
    /// Due to MDBX's MVCC, readers always see a consistent snapshot.
    /// If the namespace contains our block, all related data is guaranteed
    /// to be consistent and complete.
    fn verify_block_available<TX: DbTx>(
        tx: &TX,
        namespace_idx: u8,
        block_number: u64,
    ) -> StateResult<()> {
        let ns_idx = NamespaceIdx(namespace_idx);

        // Check that the namespace contains the expected block
        let ns_block = tx
            .get::<NamespaceBlocks>(ns_idx)
            .map_err(StateError::Database)?;
        if ns_block.map(|b| b.0) != Some(block_number) {
            return Err(StateError::BlockNotFound {
                block_number,
                namespace_idx,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_reader() -> (StateReader, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = CircularBufferConfig::new(3).unwrap();
        let reader = StateReader::new(tmp.path().join("state"), config).unwrap();
        (reader, tmp)
    }

    #[test]
    fn test_empty_database() {
        let (reader, _tmp) = create_test_reader();
        assert_eq!(reader.latest_block_number().unwrap(), None);
        assert_eq!(reader.get_available_block_range().unwrap(), None);
    }

    #[test]
    fn test_block_not_available() {
        let (reader, _tmp) = create_test_reader();
        assert!(!reader.is_block_available(100).unwrap());
    }

    #[test]
    fn test_buffer_size() {
        let (reader, _tmp) = create_test_reader();
        assert_eq!(reader.buffer_size(), 3);
    }
}
