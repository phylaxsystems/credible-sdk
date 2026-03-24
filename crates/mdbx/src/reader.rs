//! State reader implementation for latest-state MDBX storage.

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
use tracing::instrument;

const ACTIVE_NAMESPACE: u8 = 0;
const LATEST_STATE_BUFFER_SIZE: u8 = 1;

/// State reader for querying the latest durable blockchain state from MDBX.
#[derive(Clone, Debug)]
pub struct StateReader {
    db: StateDb,
}

impl Reader for StateReader {
    type Error = StateError;

    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let tx = self.db.tx()?;
        Self::latest_block_from_tx(&tx)
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
        let namespace = Self::verify_block_available(&tx, block_number)?;
        tx.get::<NamespacedAccounts>(NamespacedAccountKey::new(namespace, address_hash))
            .map_err(StateError::Database)
    }

    #[instrument(skip(self), level = "trace")]
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        let tx = self.db.tx()?;
        let namespace = Self::verify_block_available(&tx, block_number)?;
        tx.get::<NamespacedStorage>(NamespacedStorageKey::new(
            namespace,
            address_hash,
            slot_hash,
        ))
        .map(|value| value.map(|value| value.0))
        .map_err(StateError::Database)
    }

    #[instrument(skip(self), level = "debug")]
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<HashMap<B256, U256>> {
        let tx = self.db.tx()?;
        let namespace = Self::verify_block_available(&tx, block_number)?;

        let mut result = HashMap::new();
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(namespace, address_hash, B256::ZERO);

        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == namespace
            && key.address_hash == address_hash
        {
            result.insert(key.slot_hash, value.0);
            while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != namespace || key.address_hash != address_hash {
                    break;
                }
                result.insert(key.slot_hash, value.0);
            }
        }

        Ok(result)
    }

    #[instrument(skip(self), level = "trace")]
    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
        let tx = self.db.tx()?;
        let namespace = Self::verify_block_available(&tx, block_number)?;
        tx.get::<Bytecodes>(NamespacedBytecodeKey::new(namespace, code_hash))
            .map(|code| code.map(|code| code.0))
            .map_err(StateError::Database)
    }

    #[instrument(skip(self), level = "debug")]
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        let tx = self.db.tx()?;
        let namespace = Self::verify_block_available(&tx, block_number)?;

        let Some(account) = tx
            .get::<NamespacedAccounts>(NamespacedAccountKey::new(namespace, address_hash))
            .map_err(StateError::Database)?
        else {
            return Ok(None);
        };

        let storage = self.get_all_storage(address_hash, block_number)?;
        let code = if account.code_hash != B256::ZERO && account.code_hash != KECCAK256_EMPTY {
            tx.get::<Bytecodes>(NamespacedBytecodeKey::new(namespace, account.code_hash))
                .map_err(StateError::Database)?
                .map(|code| code.0)
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

    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        Ok(self
            .get_block_metadata(block_number)?
            .map(|metadata| metadata.block_hash))
    }

    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        Ok(self
            .get_block_metadata(block_number)?
            .map(|metadata| metadata.state_root))
    }

    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        let tx = self.db.tx()?;
        if Self::latest_block_from_tx(&tx)? != Some(block_number) {
            return Ok(None);
        }

        let metadata = tx
            .get::<BlockMetadataTable>(BlockNumber(block_number))
            .map_err(StateError::Database)?;

        Ok(metadata.map(|metadata| {
            BlockMetadata {
                block_number,
                block_hash: metadata.block_hash,
                state_root: metadata.state_root,
            }
        }))
    }

    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        Ok(self.latest_block_number()?.map(|latest| (latest, latest)))
    }

    fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        let tx = self.db.tx()?;
        let namespace = Self::verify_block_available(&tx, block_number)?;

        let mut cursor = tx
            .cursor_read::<NamespacedAccounts>()
            .map_err(StateError::Database)?;
        let mut account_hashes = Vec::new();

        if let Some((key, _)) = cursor.first().map_err(StateError::Database)? {
            if key.namespace_idx == namespace {
                account_hashes.push(key.address_hash);
            }

            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx == namespace {
                    account_hashes.push(key.address_hash);
                }
            }
        }

        Ok(account_hashes)
    }
}

impl StateReader {
    /// Create a new reader with read-only database access.
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

    /// Compatibility shim for callers that still surface a "depth" concept.
    #[must_use]
    pub fn buffer_size(&self) -> u8 {
        self.db.buffer_size()
    }

    #[instrument(skip(self), level = "debug")]
    pub fn get_all_storage_with_stats(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<(HashMap<B256, U256>, ReadStats)> {
        let start = Instant::now();
        let storage = self.get_all_storage(address_hash, block_number)?;

        Ok((
            storage.clone(),
            ReadStats {
                storage_slots_read: storage.len(),
                duration: start.elapsed(),
            },
        ))
    }

    fn latest_block_from_tx<TX: DbTx>(tx: &TX) -> StateResult<Option<u64>> {
        Ok(tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?
            .map(|metadata| metadata.latest_block))
    }

    pub(crate) fn active_namespace_for_block<TX: DbTx>(
        tx: &TX,
        block_number: u64,
    ) -> StateResult<u8> {
        if Self::latest_block_from_tx(tx)? == Some(block_number) {
            return Ok(Self::namespace_for_latest_block(tx, block_number)?);
        }

        Err(StateError::BlockNotFound {
            block_number,
            namespace_idx: ACTIVE_NAMESPACE,
        })
    }

    fn verify_block_available<TX: DbTx>(tx: &TX, block_number: u64) -> StateResult<u8> {
        Self::active_namespace_for_block(tx, block_number)
    }

    fn namespace_for_latest_block<TX: DbTx>(tx: &TX, block_number: u64) -> StateResult<u8> {
        let buffer_size = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?
            .map(|metadata| metadata.buffer_size)
            .unwrap_or(LATEST_STATE_BUFFER_SIZE);

        if buffer_size <= LATEST_STATE_BUFFER_SIZE {
            return Ok(ACTIVE_NAMESPACE);
        }

        Ok((block_number % u64::from(buffer_size)) as u8)
    }
}
