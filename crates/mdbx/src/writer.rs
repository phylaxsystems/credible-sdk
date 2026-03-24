//! State writer implementation for latest-state MDBX storage.

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    BlockStateUpdate,
    CommitStats,
    Reader,
    StateReader,
    Writer,
    common::{
        error::{
            StateError,
            StateResult,
        },
        tables::{
            BlockMetadataTable,
            BlockNumber,
            Bytecode,
            Bytecodes,
            Metadata,
            MetadataKey,
            NamespacedAccounts,
            NamespacedStorage,
        },
        types::{
            BlockMetadata as StoredBlockMetadata,
            GlobalMetadata,
            NamespacedAccountKey,
            NamespacedBytecodeKey,
            NamespacedStorageKey,
            StorageValue,
        },
    },
    db::StateDb,
};
use alloy::primitives::{
    B256,
    U256,
};
use reth_db_api::{
    cursor::{
        DbCursorRO,
        DbCursorRW,
    },
    table::Table,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use std::{
    collections::HashSet,
    path::Path,
    time::Instant,
};
use tracing::debug;

const ACTIVE_NAMESPACE: u8 = 0;
const COMPAT_BUFFER_SIZE: u8 = 1;

/// Session for streaming bootstrap writes without loading all accounts into memory.
pub struct BootstrapWriter {
    tx: reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
    accounts_written: usize,
    storage_slots_written: usize,
    bytecodes_written: usize,
    total_start: Instant,
}

impl BootstrapWriter {
    /// Write a single account to the latest-state tables.
    pub fn write_account(&mut self, acc: &AccountState) -> StateResult<()> {
        if acc.deleted {
            return Ok(());
        }

        self.tx
            .put::<NamespacedAccounts>(
                NamespacedAccountKey::new(ACTIVE_NAMESPACE, acc.address_hash),
                AccountInfo {
                    address_hash: acc.address_hash,
                    balance: acc.balance,
                    nonce: acc.nonce,
                    code_hash: acc.code_hash,
                },
            )
            .map_err(StateError::Database)?;
        self.accounts_written += 1;

        if let Some(code) = &acc.code
            && !code.is_empty()
        {
            self.tx
                .put::<Bytecodes>(
                    NamespacedBytecodeKey::new(ACTIVE_NAMESPACE, acc.code_hash),
                    Bytecode(code.clone()),
                )
                .map_err(StateError::Database)?;
            self.bytecodes_written += 1;
        }

        for (slot_hash, value) in &acc.storage {
            if value.is_zero() {
                continue;
            }

            self.tx
                .put::<NamespacedStorage>(
                    NamespacedStorageKey::new(ACTIVE_NAMESPACE, acc.address_hash, *slot_hash),
                    StorageValue(*value),
                )
                .map_err(StateError::Database)?;
            self.storage_slots_written += 1;
        }

        Ok(())
    }

    pub fn write_batch(&mut self, accounts: &[AccountState]) -> StateResult<()> {
        for account in accounts {
            self.write_account(account)?;
        }

        Ok(())
    }

    #[must_use]
    pub fn progress(&self) -> (usize, usize, usize) {
        (
            self.accounts_written,
            self.storage_slots_written,
            self.bytecodes_written,
        )
    }

    pub fn set_metadata(&mut self, block_hash: B256, state_root: B256) {
        self.block_hash = block_hash;
        self.state_root = state_root;
    }

    pub fn set_block_number(&mut self, block_number: u64) {
        self.block_number = block_number;
    }

    pub fn finalize(self) -> StateResult<CommitStats> {
        self.tx
            .put::<BlockMetadataTable>(
                BlockNumber(self.block_number),
                StoredBlockMetadata {
                    block_hash: self.block_hash,
                    state_root: self.state_root,
                },
            )
            .map_err(StateError::Database)?;
        self.tx
            .put::<Metadata>(
                MetadataKey,
                GlobalMetadata {
                    latest_block: self.block_number,
                    buffer_size: COMPAT_BUFFER_SIZE,
                },
            )
            .map_err(StateError::Database)?;
        self.tx
            .commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        Ok(CommitStats {
            accounts_written: self.accounts_written,
            storage_slots_written: self.storage_slots_written,
            bytecodes_written: self.bytecodes_written,
            total_duration: self.total_start.elapsed(),
            ..CommitStats::default()
        })
    }
}

/// State writer for the latest durable blockchain state in MDBX.
#[derive(Clone, Debug)]
pub struct StateWriter {
    reader: StateReader,
}

impl Reader for StateWriter {
    type Error = StateError;

    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        self.reader.latest_block_number()
    }

    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        self.reader.is_block_available(block_number)
    }

    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountInfo>> {
        self.reader.get_account(address_hash, block_number)
    }

    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        self.reader
            .get_storage(address_hash, slot_hash, block_number)
    }

    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<std::collections::HashMap<B256, U256>> {
        self.reader.get_all_storage(address_hash, block_number)
    }

    fn get_code(
        &self,
        code_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<alloy::primitives::Bytes>> {
        self.reader.get_code(code_hash, block_number)
    }

    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        self.reader.get_full_account(address_hash, block_number)
    }

    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_block_hash(block_number)
    }

    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_state_root(block_number)
    }

    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        self.reader.get_block_metadata(block_number)
    }

    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        self.reader.get_available_block_range()
    }

    fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        self.reader.scan_account_hashes(block_number)
    }
}

impl Writer for StateWriter {
    type Error = StateError;

    fn commit_block(&self, update: &BlockStateUpdate) -> StateResult<CommitStats> {
        Self::validate_unique_accounts(update)?;

        let start = Instant::now();
        if let Some(latest) = self.latest_block_number()?
            && update.block_number < latest
        {
            return Err(StateError::BlockNotAvailable(update.block_number, latest));
        }

        let db = self.reader.db();
        let tx = db.tx_mut()?;

        let mut stats = CommitStats::default();
        for account in &update.accounts {
            Self::apply_account_update(&tx, account, &mut stats)?;
        }

        Self::clear_table::<BlockMetadataTable>(&tx)?;

        tx.put::<BlockMetadataTable>(
            BlockNumber(update.block_number),
            StoredBlockMetadata {
                block_hash: update.block_hash,
                state_root: update.state_root,
            },
        )
        .map_err(StateError::Database)?;
        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: update.block_number,
                buffer_size: COMPAT_BUFFER_SIZE,
            },
        )
        .map_err(StateError::Database)?;
        stats.batch_write_duration = start.elapsed();
        let commit_start = Instant::now();
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;
        stats.commit_duration = commit_start.elapsed();
        stats.total_duration = start.elapsed();

        debug!(
            block = update.block_number,
            accounts_written = stats.accounts_written,
            storage_slots_written = stats.storage_slots_written,
            "committed latest-state block"
        );

        Ok(stats)
    }

    fn bootstrap_from_snapshot(
        &self,
        accounts: Vec<AccountState>,
        block_number: u64,
        block_hash: B256,
        state_root: B256,
    ) -> StateResult<CommitStats> {
        self.bootstrap_from_iterator(accounts.into_iter(), block_number, block_hash, state_root)
    }
}

impl StateWriter {
    /// Create a new writer.
    pub fn new(path: impl AsRef<Path>) -> StateResult<Self> {
        let db = StateDb::open(path)?;
        let reader = StateReader::from_db(db);
        let writer = Self { reader };
        writer.migrate_legacy_layout_if_needed()?;
        Ok(writer)
    }

    #[must_use]
    pub fn reader(&self) -> &StateReader {
        &self.reader
    }

    /// Compatibility shim for older callers until the sidecar config is simplified.
    #[must_use]
    pub fn buffer_size(&self) -> u8 {
        self.reader.buffer_size()
    }

    pub fn begin_bootstrap(
        &self,
        block_number: u64,
        block_hash: B256,
        state_root: B256,
    ) -> StateResult<BootstrapWriter> {
        let tx = self.reader.db().tx_mut()?;
        Self::clear_table::<NamespacedAccounts>(&tx)?;
        Self::clear_table::<NamespacedStorage>(&tx)?;
        Self::clear_table::<Bytecodes>(&tx)?;
        Self::clear_table::<BlockMetadataTable>(&tx)?;
        Self::clear_table::<Metadata>(&tx)?;

        Ok(BootstrapWriter {
            tx,
            block_number,
            block_hash,
            state_root,
            accounts_written: 0,
            storage_slots_written: 0,
            bytecodes_written: 0,
            total_start: Instant::now(),
        })
    }

    pub fn bootstrap_from_iterator<I>(
        &self,
        accounts: I,
        block_number: u64,
        block_hash: B256,
        state_root: B256,
    ) -> StateResult<CommitStats>
    where
        I: Iterator<Item = AccountState>,
    {
        let mut bootstrap = self.begin_bootstrap(block_number, block_hash, state_root)?;
        for account in accounts {
            bootstrap.write_account(&account)?;
        }
        bootstrap.finalize()
    }

    pub fn fix_block_metadata(
        &self,
        block_number: u64,
        block_hash: B256,
        state_root: Option<B256>,
    ) -> StateResult<bool> {
        let db = self.reader.db();
        let (old_block_number, old_state_root) = {
            let tx = db.tx()?;
            let current_meta = tx
                .get::<Metadata>(MetadataKey)
                .map_err(StateError::Database)?
                .ok_or(StateError::MetadataNotAvailable)?;
            let old_meta = tx
                .get::<BlockMetadataTable>(BlockNumber(current_meta.latest_block))
                .map_err(StateError::Database)?;

            (
                current_meta.latest_block,
                old_meta.map(|meta| meta.state_root).unwrap_or(B256::ZERO),
            )
        };

        if old_block_number == block_number {
            return Ok(false);
        }

        let tx = db.tx_mut()?;
        Self::clear_table::<BlockMetadataTable>(&tx)?;

        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: block_number,
                buffer_size: COMPAT_BUFFER_SIZE,
            },
        )
        .map_err(StateError::Database)?;
        tx.put::<BlockMetadataTable>(
            BlockNumber(block_number),
            StoredBlockMetadata {
                block_hash,
                state_root: state_root.unwrap_or(old_state_root),
            },
        )
        .map_err(StateError::Database)?;
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        Ok(true)
    }

    fn migrate_legacy_layout_if_needed(&self) -> StateResult<()> {
        let Some(latest_block) = self.latest_block_number()? else {
            return Ok(());
        };

        let tx = self.reader.db().tx()?;
        let buffer_size = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?
            .map(|metadata| metadata.buffer_size)
            .unwrap_or(COMPAT_BUFFER_SIZE);
        if buffer_size <= COMPAT_BUFFER_SIZE {
            return Ok(());
        }

        let namespace = StateReader::active_namespace_for_block(&tx, latest_block)?;
        let block_metadata = tx
            .get::<BlockMetadataTable>(BlockNumber(latest_block))
            .map_err(StateError::Database)?
            .ok_or(StateError::MetadataNotAvailable)?;

        let mut bootstrap = self.begin_bootstrap(
            latest_block,
            block_metadata.block_hash,
            block_metadata.state_root,
        )?;
        let mut cursor = tx
            .cursor_read::<NamespacedAccounts>()
            .map_err(StateError::Database)?;

        if let Some((key, account)) = cursor.first().map_err(StateError::Database)? {
            Self::migrate_account_if_matching(&tx, &mut bootstrap, namespace, key, account)?;

            while let Some((key, account)) = cursor.next().map_err(StateError::Database)? {
                Self::migrate_account_if_matching(&tx, &mut bootstrap, namespace, key, account)?;
            }
        }

        drop(cursor);
        drop(tx);
        bootstrap.finalize()?;

        Ok(())
    }

    fn migrate_account_if_matching<TX: DbTx>(
        tx: &TX,
        bootstrap: &mut BootstrapWriter,
        namespace: u8,
        key: NamespacedAccountKey,
        account: AccountInfo,
    ) -> StateResult<()> {
        if key.namespace_idx != namespace {
            return Ok(());
        }

        let storage = Self::load_storage_from_tx(tx, namespace, key.address_hash)?;
        let code = if account.code_hash == B256::ZERO {
            None
        } else {
            tx.get::<Bytecodes>(NamespacedBytecodeKey::new(namespace, account.code_hash))
                .map_err(StateError::Database)?
                .map(|code| code.0)
        };

        bootstrap.write_account(&AccountState {
            address_hash: key.address_hash,
            balance: account.balance,
            nonce: account.nonce,
            code_hash: account.code_hash,
            code,
            storage,
            deleted: false,
        })
    }

    fn validate_unique_accounts(update: &BlockStateUpdate) -> StateResult<()> {
        let mut seen = HashSet::with_capacity(update.accounts.len());
        for account in &update.accounts {
            if !seen.insert(account.address_hash) {
                return Err(StateError::DuplicateAccount(account.address_hash));
            }
        }

        Ok(())
    }

    fn apply_account_update(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        account: &AccountState,
        stats: &mut CommitStats,
    ) -> StateResult<()> {
        let key = NamespacedAccountKey::new(ACTIVE_NAMESPACE, account.address_hash);

        if account.deleted {
            tx.delete::<NamespacedAccounts>(key, None)
                .map_err(StateError::Database)?;
            Self::delete_account_storage(tx, account.address_hash)?;
            stats.accounts_deleted += 1;
            stats.full_storage_deletes += 1;
            return Ok(());
        }

        tx.put::<NamespacedAccounts>(
            key,
            AccountInfo {
                address_hash: account.address_hash,
                balance: account.balance,
                nonce: account.nonce,
                code_hash: account.code_hash,
            },
        )
        .map_err(StateError::Database)?;
        stats.accounts_written += 1;

        if let Some(code) = &account.code
            && !code.is_empty()
        {
            tx.put::<Bytecodes>(
                NamespacedBytecodeKey::new(ACTIVE_NAMESPACE, account.code_hash),
                Bytecode(code.clone()),
            )
            .map_err(StateError::Database)?;
            stats.bytecodes_written += 1;
        }

        for (slot_hash, value) in &account.storage {
            let storage_key =
                NamespacedStorageKey::new(ACTIVE_NAMESPACE, account.address_hash, *slot_hash);
            if value.is_zero() {
                let deleted = tx
                    .delete::<NamespacedStorage>(storage_key, None)
                    .map_err(StateError::Database)?;
                if deleted {
                    stats.storage_slots_deleted += 1;
                }
            } else {
                tx.put::<NamespacedStorage>(storage_key, StorageValue(*value))
                    .map_err(StateError::Database)?;
                stats.storage_slots_written += 1;
            }
        }

        Ok(())
    }

    fn delete_account_storage(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        address_hash: AddressHash,
    ) -> StateResult<()> {
        let mut cursor = tx
            .cursor_write::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(ACTIVE_NAMESPACE, address_hash, B256::ZERO);
        let mut to_delete = Vec::new();

        if let Some((key, _)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == ACTIVE_NAMESPACE
            && key.address_hash == address_hash
        {
            to_delete.push(key);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != ACTIVE_NAMESPACE || key.address_hash != address_hash {
                    break;
                }
                to_delete.push(key);
            }
        }

        drop(cursor);

        for key in to_delete {
            tx.delete::<NamespacedStorage>(key, None)
                .map_err(StateError::Database)?;
        }

        Ok(())
    }

    fn load_storage_from_tx<TX: DbTx>(
        tx: &TX,
        namespace: u8,
        address_hash: AddressHash,
    ) -> StateResult<std::collections::HashMap<B256, U256>> {
        let mut cursor = tx
            .cursor_read::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(namespace, address_hash, B256::ZERO);
        let mut storage = std::collections::HashMap::new();

        if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.namespace_idx == namespace
            && key.address_hash == address_hash
        {
            storage.insert(key.slot_hash, value.0);
            while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                if key.namespace_idx != namespace || key.address_hash != address_hash {
                    break;
                }
                storage.insert(key.slot_hash, value.0);
            }
        }

        Ok(storage)
    }

    fn clear_table<T>(tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>) -> StateResult<()>
    where
        T: Table,
    {
        let mut cursor = tx.cursor_write::<T>().map_err(StateError::Database)?;
        while cursor.first().map_err(StateError::Database)?.is_some() {
            cursor.delete_current().map_err(StateError::Database)?;
        }
        Ok(())
    }
}
