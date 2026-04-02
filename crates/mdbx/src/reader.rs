//! State reader implementation for querying blockchain state from MDBX.
//!
//! The MDBX store keeps a single committed state snapshot. Reads are valid for
//! the latest committed block only.

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    ReadStats,
    Reader,
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
            NamespacedAccounts,
            NamespacedStorage,
        },
        types::{
            NamespacedAccountKey,
            NamespacedBytecodeKey,
            NamespacedStorageKey,
        },
    },
    db::StateDb,
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
    time::Instant,
};
use tracing::{
    instrument,
    trace,
};

#[derive(Clone, Debug)]
pub struct StateReader {
    db: StateDb,
}

impl Reader for StateReader {
    type Error = StateError;

    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let tx = self.db.tx()?;
        let meta = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?;
        Ok(meta.map(|m| m.latest_block))
    }

    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        Ok(self.latest_block_number()? == Some(block_number))
    }

    #[instrument(skip(self), level = "trace")]
    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountInfo>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;
        let key = NamespacedAccountKey::new(address_hash);
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

    #[instrument(skip(self), level = "trace")]
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;
        let key = NamespacedStorageKey::new(address_hash, slot_hash);
        let value = tx
            .get::<NamespacedStorage>(key)
            .map_err(StateError::Database)?;
        Ok(value.map(|v| v.0))
    }

    #[instrument(skip(self), level = "debug")]
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<HashMap<B256, U256>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;

        let start = Instant::now();
        let mut result = HashMap::new();
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(address_hash, B256::ZERO);

        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.address_hash == address_hash
        {
            result.insert(key.slot_hash, value.0);

            while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                if key.address_hash != address_hash {
                    break;
                }
                result.insert(key.slot_hash, value.0);
            }
        }

        trace!(
            slots = result.len(),
            duration_us = start.elapsed().as_micros(),
            "get_all_storage complete"
        );

        Ok(result)
    }

    #[instrument(skip(self), level = "trace")]
    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;
        let result = tx
            .get::<Bytecodes>(NamespacedBytecodeKey::new(code_hash))
            .map_err(StateError::Database)?;
        Ok(result.map(|b| b.0))
    }

    #[instrument(skip(self), level = "debug")]
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;

        let start = Instant::now();
        let key = NamespacedAccountKey::new(address_hash);
        let Some(account) = tx
            .get::<NamespacedAccounts>(key)
            .map_err(StateError::Database)?
        else {
            return Ok(None);
        };

        let mut storage = HashMap::new();
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(address_hash, B256::ZERO);

        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.address_hash == address_hash
        {
            storage.insert(key.slot_hash, value.0);

            while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                if key.address_hash != address_hash {
                    break;
                }
                storage.insert(key.slot_hash, value.0);
            }
        }

        let code = if account.code_hash != KECCAK256_EMPTY && account.code_hash != B256::ZERO {
            tx.get::<Bytecodes>(NamespacedBytecodeKey::new(account.code_hash))
                .map_err(StateError::Database)?
                .map(|b| b.0)
        } else {
            None
        };

        trace!(
            storage_slots = storage.len(),
            has_code = code.is_some(),
            duration_us = start.elapsed().as_micros(),
            "get_full_account complete"
        );

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

    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        let meta = self.get_block_metadata_internal(block_number)?;
        Ok(meta.map(|m| m.block_hash))
    }

    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        let meta = self.get_block_metadata_internal(block_number)?;
        Ok(meta.map(|m| m.state_root))
    }

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

    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        Ok(self.latest_block_number()?.map(|latest| (latest, latest)))
    }

    #[instrument(skip(self), level = "debug")]
    fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        let tx = self.db.tx()?;
        Self::verify_block_available(&tx, block_number)?;

        let start = Instant::now();
        let mut result = Vec::new();
        let mut cursor = tx
            .cursor_read::<NamespacedAccounts>()
            .map_err(StateError::Database)?;

        if let Some((key, _)) = cursor.first().map_err(StateError::Database)? {
            result.push(key.address_hash);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                result.push(key.address_hash);
            }
        }

        trace!(
            accounts = result.len(),
            duration_us = start.elapsed().as_micros(),
            "scan_account_hashes complete"
        );

        Ok(result)
    }
}

impl StateReader {
    /// Open a reader over an existing MDBX directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the on-disk layout
    /// requires a re-sync.
    pub fn new(path: impl AsRef<Path>) -> StateResult<Self> {
        let db = StateDb::open_read_only(path)?;
        Ok(Self { db })
    }

    pub(crate) fn from_db(db: StateDb) -> Self {
        Self { db }
    }

    pub(crate) fn db(&self) -> &StateDb {
        &self.db
    }

    /// Read all storage slots together with simple timing metadata.
    ///
    /// # Errors
    ///
    /// Returns the same storage/backend errors as `get_all_storage`.
    pub fn get_all_storage_with_stats(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<(HashMap<B256, U256>, ReadStats)> {
        let start = Instant::now();
        let storage = self.get_all_storage(address_hash, block_number)?;
        let storage_slots_read = storage.len();

        Ok((
            storage,
            ReadStats {
                storage_slots_read,
                duration: start.elapsed(),
            },
        ))
    }

    fn get_block_metadata_internal(
        &self,
        block_number: u64,
    ) -> StateResult<Option<crate::common::types::BlockMetadata>> {
        let tx = self.db.tx()?;
        tx.get::<BlockMetadataTable>(BlockNumber(block_number))
            .map_err(StateError::Database)
    }

    fn verify_block_available<TX: DbTx>(tx: &TX, block_number: u64) -> StateResult<()> {
        let latest_block = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?
            .map(|metadata| metadata.latest_block)
            .ok_or(StateError::MetadataNotAvailable)?;

        if block_number == latest_block {
            return Ok(());
        }

        Err(StateError::BlockNotAvailable {
            block_number,
            latest_block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_reader() -> (StateReader, TempDir) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state");
        let db = StateDb::open(&path).unwrap();
        let reader = StateReader::from_db(db);
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
}
