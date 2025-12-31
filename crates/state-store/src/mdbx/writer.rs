//! State writer implementation for persisting blockchain state to MDBX.
//!
//! Handles circular buffer rotation with state diff reconstruction.
//!
//! ## Atomicity
//!
//! All writes happen within a single MDBX transaction. This means:
//!
//! - Either all changes are committed, or none are (crash-safe)
//! - Readers never see partial writes (MVCC isolation)
//! - No explicit locking needed for read consistency
//!
//! ## Example
//!
//! ```ignore
//! use state_store::{StateWriter, CircularBufferConfig, BlockStateUpdate, AccountState};
//! use alloy::primitives::{B256, U256};
//!
//! let config = CircularBufferConfig::new(100)?;
//! let writer = StateWriter::new("/path/to/db", config)?;
//!
//! let update = BlockStateUpdate {
//!     block_number: 12345,
//!     block_hash: B256::repeat_byte(0x11),
//!     state_root: B256::repeat_byte(0x22),
//!     accounts: vec![
//!         AccountState {
//!             address_hash: B256::repeat_byte(0xAA),
//!             balance: U256::from(1000),
//!             ..Default::default()
//!         }
//!     ],
//! };
//!
//! writer.commit_block(update)?;
//! ```

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Writer,
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
                StateDiffData,
                StateDiffs,
            },
            types::{
                BlockMetadata,
                CircularBufferConfig,
                GlobalMetadata,
                NamespacedAccountKey,
                NamespacedBytecodeKey,
                NamespacedStorageKey,
                StorageValue,
            },
        },
        db::StateDb,
    },
};
use alloy::primitives::B256;
use reth_db_api::{
    cursor::DbCursorRO,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use std::path::Path;

/// State writer for persisting blockchain state to MDBX.
///
/// MDBX enforces single-writer semantics, so only one `StateWriter`
/// should be active at a time per database path.
#[derive(Debug)]
pub struct StateWriter {
    db: StateDb,
}

impl Writer for StateWriter {
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

    /// Commit a block's state update to the database.
    ///
    /// This handles:
    /// 1. Applying intermediate diffs if rotating the circular buffer
    /// 2. Writing all account and storage changes
    /// 3. Updating metadata and cleaning up old data
    ///
    /// ## Atomicity
    ///
    /// All changes happen in a single transaction. If anything fails,
    /// the entire operation is rolled back and the database remains unchanged.
    ///
    /// ## Reconstruction
    ///
    /// When a namespace rotates (e.g., block 103 replaces block 100 in namespace 1
    /// with `buffer_size`=3), the intermediate diffs (blocks 101, 102) are applied
    /// first to bring the namespace's state up to date.
    fn commit_block(&self, update: &BlockStateUpdate) -> StateResult<()> {
        let block_number = update.block_number;
        let namespace_idx = self.db.namespace_for_block(block_number)?;
        let ns_idx = NamespaceIdx(namespace_idx);

        let tx = self.db.tx_mut()?;

        // 1. Apply intermediate diffs if there's a gap (rotation)
        let current_ns_block = tx
            .get::<NamespaceBlocks>(ns_idx)
            .map_err(StateError::Database)?;
        let start_block = current_ns_block.map_or(0, |b| b.0 + 1);
        if block_number > start_block {
            for diff_block in start_block..block_number {
                Self::apply_diff_from_storage(&tx, namespace_idx, diff_block, block_number)?;
            }
        }

        // 2. Write current block's state changes
        Self::write_account_changes(&tx, namespace_idx, &update.accounts)?;

        // 3. Store the diff for future rotations
        let diff_data = update
            .to_json()
            .map_err(|e| StateError::SerializeDiff(block_number, e))?;
        tx.put::<StateDiffs>(BlockNumber(block_number), StateDiffData(diff_data))
            .map_err(StateError::Database)?;

        // 4. Update namespace block number
        tx.put::<NamespaceBlocks>(ns_idx, BlockNumber(block_number))
            .map_err(StateError::Database)?;

        // 5. Update block metadata
        tx.put::<BlockMetadataTable>(
            BlockNumber(block_number),
            BlockMetadata {
                block_hash: update.block_hash,
                state_root: update.state_root,
            },
        )
        .map_err(StateError::Database)?;

        // 6. Update global metadata
        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: block_number,
                buffer_size: self.db.buffer_size(),
            },
        )
        .map_err(StateError::Database)?;

        // 7. Cleanup old data (diffs and metadata beyond buffer)
        let buffer_size = u64::from(self.db.buffer_size());
        if block_number >= buffer_size {
            let cleanup_block = block_number - buffer_size;
            let _ = tx.delete::<StateDiffs>(BlockNumber(cleanup_block), None);
            let _ = tx.delete::<BlockMetadataTable>(BlockNumber(cleanup_block), None);
        }

        // 8. Commit transaction (atomic)
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        Ok(())
    }
}

impl StateWriter {
    /// Create a new writer.
    ///
    /// Creates the database if it doesn't exist.
    pub fn new(path: impl AsRef<Path>, config: CircularBufferConfig) -> StateResult<Self> {
        let db = StateDb::open(path, config)?;
        Ok(Self { db })
    }

    /// Apply a stored diff to a namespace.
    fn apply_diff_from_storage(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        namespace_idx: u8,
        diff_block: u64,
        target_block: u64,
    ) -> StateResult<()> {
        let diff_data = tx
            .get::<StateDiffs>(BlockNumber(diff_block))
            .map_err(StateError::Database)?
            .ok_or(StateError::MissingStateDiff {
                needed_block: diff_block,
                target_block,
            })?;

        let update = BlockStateUpdate::from_json(&diff_data.0)
            .map_err(|e| StateError::DeserializeDiff(diff_block, e))?;

        Self::write_account_changes(tx, namespace_idx, &update.accounts)
    }

    /// Write account and storage changes to a namespace.
    fn write_account_changes(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        namespace_idx: u8,
        accounts: &[AccountState],
    ) -> StateResult<()> {
        for account in accounts {
            let address_hash = account.address_hash;
            let account_key = NamespacedAccountKey::new(namespace_idx, address_hash);

            if account.deleted {
                // Delete the account and all its storage
                tx.delete::<NamespacedAccounts>(account_key, None)
                    .map_err(StateError::Database)?;
                Self::delete_account_storage(tx, namespace_idx, address_hash)?;
                continue;
            }

            // Write account data
            tx.put::<NamespacedAccounts>(
                account_key,
                AccountInfo {
                    address_hash: account.address_hash,
                    balance: account.balance,
                    nonce: account.nonce,
                    code_hash: account.code_hash,
                },
            )
            .map_err(StateError::Database)?;

            // Write bytecode if present (deduplicated by code_hash)
            if let Some(code) = &account.code
                && !code.is_empty()
            {
                tx.put::<Bytecodes>(
                    NamespacedBytecodeKey::new(namespace_idx, account.code_hash),
                    crate::mdbx::common::tables::Bytecode(code.clone()),
                )
                .map_err(StateError::Database)?;
            }

            // Write storage changes
            for (slot_hash, value) in &account.storage {
                let storage_key =
                    NamespacedStorageKey::new(namespace_idx, address_hash, *slot_hash);

                if value.is_zero() {
                    // Zero means delete (Ethereum semantics)
                    tx.delete::<NamespacedStorage>(storage_key, None)
                        .map_err(StateError::Database)?;
                } else {
                    tx.put::<NamespacedStorage>(storage_key, StorageValue(*value))
                        .map_err(StateError::Database)?;
                }
            }
        }

        Ok(())
    }

    /// Delete all storage for an account in a namespace.
    fn delete_account_storage(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        namespace_idx: u8,
        address_hash: AddressHash,
    ) -> StateResult<()> {
        let mut cursor = tx
            .cursor_write::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(namespace_idx, address_hash, B256::ZERO);

        // Collect keys to delete (can't delete while iterating)
        let mut to_delete = Vec::new();

        if let Some((key, _)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == namespace_idx
            && key.address_hash == address_hash
        {
            to_delete.push(key);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != namespace_idx || key.address_hash != address_hash {
                    break;
                }
                to_delete.push(key);
            }
        }

        // Delete collected keys
        for key in to_delete {
            tx.delete::<NamespacedStorage>(key, None)
                .map_err(StateError::Database)?;
        }

        Ok(())
    }
}
