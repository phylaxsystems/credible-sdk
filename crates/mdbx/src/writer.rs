//! State writer implementation for persisting blockchain state to MDBX.
//!
//! The MDBX layer stores a single committed state snapshot. Each block commit
//! mutates that latest snapshot in place and advances the metadata head.
use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
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
            NamespaceBlocks,
            NamespaceIdx,
            NamespacedAccounts,
            NamespacedStorage,
            StateDiffData,
            StateDiffs,
        },
        types::{
            BinaryAccountDiff,
            BinaryStateDiff,
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
    Bytes,
};
use rayon::prelude::*;
use reth_db_api::{
    cursor::DbCursorRO,
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
use tracing::{
    Span,
    debug,
    instrument,
    trace,
    warn,
};

#[derive(Clone, Debug)]
pub struct StateWriter {
    reader: StateReader,
}

pub struct BootstrapWriter {
    tx: reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
    stats: CommitStats,
    total_start: Instant,
}

impl BootstrapWriter {
    /// Write one account directly into the bootstrap transaction.
    ///
    /// # Errors
    ///
    /// Returns any MDBX write error.
    pub fn write_account(&mut self, account: &AccountState) -> StateResult<()> {
        StateWriter::apply_account(&self.tx, account, &mut self.stats)
    }

    #[must_use]
    pub fn progress(&self) -> (usize, usize, usize) {
        (
            self.stats.accounts_written,
            self.stats.storage_slots_written,
            self.stats.bytecodes_written,
        )
    }

    pub fn set_block_number(&mut self, block_number: u64) {
        self.block_number = block_number;
    }

    pub fn set_metadata(&mut self, block_hash: B256, state_root: B256) {
        self.block_hash = block_hash;
        self.state_root = state_root;
    }

    /// Persist the queued bootstrap snapshot.
    ///
    /// # Errors
    ///
    /// Returns any MDBX write or commit error.
    pub fn finalize(mut self) -> StateResult<CommitStats> {
        self.tx
            .put::<StateDiffs>(
                BlockNumber(self.block_number),
                StateDiffData(
                    BinaryStateDiff {
                        block_number: self.block_number,
                        block_hash: self.block_hash,
                        state_root: self.state_root,
                        accounts: Vec::new(),
                    }
                    .to_bytes()?,
                ),
            )
            .map_err(StateError::Database)?;
        self.tx
            .put::<Metadata>(
                MetadataKey,
                GlobalMetadata {
                    latest_block: self.block_number,
                },
            )
            .map_err(StateError::Database)?;
        self.tx
            .put::<NamespaceBlocks>(NamespaceIdx(0), BlockNumber(self.block_number))
            .map_err(StateError::Database)?;
        self.tx
            .put::<BlockMetadataTable>(
                BlockNumber(self.block_number),
                crate::common::types::BlockMetadata {
                    block_hash: self.block_hash,
                    state_root: self.state_root,
                },
            )
            .map_err(StateError::Database)?;

        let commit_start = Instant::now();
        self.tx
            .commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;
        self.stats.commit_duration = commit_start.elapsed();
        self.stats.total_duration = self.total_start.elapsed();
        Ok(self.stats)
    }
}

impl Writer for StateWriter {
    type Error = StateError;

    #[instrument(skip(self, update), level = "debug")]
    fn commit_block(&self, update: &BlockStateUpdate) -> StateResult<CommitStats> {
        let total_start = Instant::now();
        let mut stats = CommitStats::default();

        Self::validate_unique_accounts(update)?;

        let preprocess_start = Instant::now();
        let diff = Self::to_binary_diff_parallel(update);
        let diff_bytes = diff.to_bytes()?;
        stats.diff_bytes = diff_bytes.len();
        stats.largest_account_storage = diff
            .accounts
            .iter()
            .map(|account| account.storage.len())
            .max()
            .unwrap_or(0);
        stats.preprocess_duration = preprocess_start.elapsed();

        let tx = self.reader.db().tx_mut()?;
        let batch_start = Instant::now();
        Self::apply_accounts(&tx, &diff.accounts, &mut stats)?;
        stats.batch_write_duration = batch_start.elapsed();

        let previous_latest = tx
            .get::<Metadata>(MetadataKey)
            .map_err(StateError::Database)?
            .map(|meta| meta.latest_block);

        tx.put::<StateDiffs>(BlockNumber(update.block_number), StateDiffData(diff_bytes))
            .map_err(StateError::Database)?;
        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: update.block_number,
            },
        )
        .map_err(StateError::Database)?;
        tx.put::<NamespaceBlocks>(NamespaceIdx(0), BlockNumber(update.block_number))
            .map_err(StateError::Database)?;
        tx.put::<BlockMetadataTable>(
            BlockNumber(update.block_number),
            crate::common::types::BlockMetadata {
                block_hash: update.block_hash,
                state_root: update.state_root,
            },
        )
        .map_err(StateError::Database)?;

        if let Some(previous_latest) = previous_latest
            && previous_latest != update.block_number
        {
            let _ = tx
                .delete::<StateDiffs>(BlockNumber(previous_latest), None)
                .map_err(StateError::Database)?;
            let _ = tx
                .delete::<BlockMetadataTable>(BlockNumber(previous_latest), None)
                .map_err(StateError::Database)?;
        }

        let commit_start = Instant::now();
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;
        stats.commit_duration = commit_start.elapsed();
        stats.total_duration = total_start.elapsed();

        Span::current().record(
            "total_ms",
            i64::try_from(stats.total_duration.as_millis()).unwrap_or(i64::MAX),
        );
        debug!(
            total_ms = stats.total_duration.as_millis(),
            preprocess_ms = stats.preprocess_duration.as_millis(),
            batch_write_ms = stats.batch_write_duration.as_millis(),
            commit_ms = stats.commit_duration.as_millis(),
            "block committed"
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
    ) -> StateResult<Option<alloy::primitives::U256>> {
        self.reader
            .get_storage(address_hash, slot_hash, block_number)
    }

    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<std::collections::HashMap<B256, alloy::primitives::U256>> {
        self.reader.get_all_storage(address_hash, block_number)
    }

    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
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

    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<crate::BlockMetadata>> {
        self.reader.get_block_metadata(block_number)
    }

    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        self.reader.get_available_block_range()
    }

    fn scan_account_hashes(&self, block_number: u64) -> StateResult<Vec<AddressHash>> {
        self.reader.scan_account_hashes(block_number)
    }
}

impl StateWriter {
    /// Open a writer over an MDBX directory, creating it when needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created, MDBX cannot be
    /// opened, or legacy on-disk data requires a re-sync.
    pub fn new(path: impl AsRef<Path>) -> StateResult<Self> {
        let db = StateDb::open(path)?;
        let reader = StateReader::from_db(db);
        Ok(Self { reader })
    }

    #[must_use]
    pub fn reader(&self) -> &StateReader {
        &self.reader
    }

    /// Start a streaming bootstrap transaction that replaces the latest snapshot.
    ///
    /// # Errors
    ///
    /// Returns any MDBX initialization or table-clearing error.
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
        Self::clear_table::<StateDiffs>(&tx)?;
        Self::clear_table::<BlockMetadataTable>(&tx)?;
        Self::clear_table::<Metadata>(&tx)?;
        Self::clear_table::<NamespaceBlocks>(&tx)?;

        Ok(BootstrapWriter {
            tx,
            block_number,
            block_hash,
            state_root,
            stats: CommitStats::default(),
            total_start: Instant::now(),
        })
    }

    /// Bootstrap the store from an iterator of account snapshots.
    ///
    /// # Errors
    ///
    /// Returns any MDBX write or commit error.
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

    /// Rewrite the latest block metadata without changing account/state data.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata is unavailable or MDBX writes fail.
    pub fn fix_block_metadata(
        &self,
        block_number: u64,
        block_hash: B256,
        state_root: Option<B256>,
    ) -> StateResult<bool> {
        let db = self.reader.db();

        let (old_block_number, old_meta) = {
            let tx = db.tx()?;
            let current_meta = tx
                .get::<Metadata>(MetadataKey)
                .map_err(StateError::Database)?
                .ok_or(StateError::MetadataNotAvailable)?;

            let old_meta = tx
                .get::<BlockMetadataTable>(BlockNumber(current_meta.latest_block))
                .map_err(StateError::Database)?;

            (current_meta.latest_block, old_meta)
        };

        if old_block_number == block_number {
            return Ok(false);
        }

        let final_state_root = state_root
            .or_else(|| old_meta.as_ref().map(|meta| meta.state_root))
            .unwrap_or(B256::ZERO);

        let tx = db.tx_mut()?;

        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: block_number,
            },
        )
        .map_err(StateError::Database)?;
        tx.put::<NamespaceBlocks>(NamespaceIdx(0), BlockNumber(block_number))
            .map_err(StateError::Database)?;

        if old_meta.is_some() {
            let _ = tx
                .delete::<BlockMetadataTable>(BlockNumber(old_block_number), None)
                .map_err(StateError::Database)?;
        }
        let _ = tx
            .delete::<StateDiffs>(BlockNumber(old_block_number), None)
            .map_err(StateError::Database)?;

        tx.put::<BlockMetadataTable>(
            BlockNumber(block_number),
            crate::common::types::BlockMetadata {
                block_hash,
                state_root: final_state_root,
            },
        )
        .map_err(StateError::Database)?;

        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        Ok(true)
    }

    fn validate_unique_accounts(update: &BlockStateUpdate) -> StateResult<()> {
        let mut seen = HashSet::with_capacity(update.accounts.len());
        for account in &update.accounts {
            if !seen.insert(account.address_hash) {
                warn!(
                    address_hash = %account.address_hash,
                    block = update.block_number,
                    "duplicate account in block state update"
                );
                return Err(StateError::DuplicateAccount(account.address_hash));
            }
        }
        Ok(())
    }

    fn to_binary_diff_parallel(update: &BlockStateUpdate) -> BinaryStateDiff {
        let accounts = update
            .accounts
            .par_iter()
            .map(|account| {
                let mut storage: Vec<_> = account
                    .storage
                    .iter()
                    .map(|(slot, value)| (*slot, *value))
                    .collect();
                storage.sort_unstable_by_key(|(slot, _)| *slot);

                BinaryAccountDiff {
                    address_hash: account.address_hash,
                    deleted: account.deleted,
                    balance: account.balance,
                    nonce: account.nonce,
                    code_hash: account.code_hash,
                    code: account.code.clone(),
                    storage,
                }
            })
            .collect();

        BinaryStateDiff {
            block_number: update.block_number,
            block_hash: update.block_hash,
            state_root: update.state_root,
            accounts,
        }
    }

    fn apply_accounts(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        accounts: &[BinaryAccountDiff],
        stats: &mut CommitStats,
    ) -> StateResult<()> {
        for account in accounts {
            let account_key = NamespacedAccountKey::new(account.address_hash);

            if account.deleted {
                let deleted = tx
                    .delete::<NamespacedAccounts>(account_key, None)
                    .map_err(StateError::Database)?;
                if deleted {
                    stats.accounts_deleted += 1;
                }
                Self::delete_all_storage_for_account(tx, account.address_hash, stats)?;
                continue;
            }

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
            stats.accounts_written += 1;

            if let Some(code) = &account.code
                && !code.is_empty()
            {
                tx.put::<Bytecodes>(
                    NamespacedBytecodeKey::new(account.code_hash),
                    Bytecode(Bytes::clone(code)),
                )
                .map_err(StateError::Database)?;
                stats.bytecodes_written += 1;
            }

            for (slot_hash, value) in &account.storage {
                let storage_key = NamespacedStorageKey::new(account.address_hash, *slot_hash);
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
        }

        trace!(
            accounts_written = stats.accounts_written,
            accounts_deleted = stats.accounts_deleted,
            storage_written = stats.storage_slots_written,
            storage_deleted = stats.storage_slots_deleted,
            "applied account updates"
        );

        Ok(())
    }

    fn apply_account(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        account: &AccountState,
        stats: &mut CommitStats,
    ) -> StateResult<()> {
        stats.largest_account_storage = stats.largest_account_storage.max(account.storage.len());
        let account_key = NamespacedAccountKey::new(account.address_hash);

        if account.deleted {
            let deleted = tx
                .delete::<NamespacedAccounts>(account_key, None)
                .map_err(StateError::Database)?;
            if deleted {
                stats.accounts_deleted += 1;
            }
            Self::delete_all_storage_for_account(tx, account.address_hash, stats)?;
            return Ok(());
        }

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
        stats.accounts_written += 1;

        if let Some(code) = &account.code
            && !code.is_empty()
        {
            tx.put::<Bytecodes>(
                NamespacedBytecodeKey::new(account.code_hash),
                Bytecode(Bytes::clone(code)),
            )
            .map_err(StateError::Database)?;
            stats.bytecodes_written += 1;
        }

        let mut storage: Vec<_> = account
            .storage
            .iter()
            .map(|(slot, value)| (*slot, *value))
            .collect();
        storage.sort_unstable_by_key(|(slot, _)| *slot);

        for (slot_hash, value) in storage {
            let storage_key = NamespacedStorageKey::new(account.address_hash, slot_hash);
            if value.is_zero() {
                let deleted = tx
                    .delete::<NamespacedStorage>(storage_key, None)
                    .map_err(StateError::Database)?;
                if deleted {
                    stats.storage_slots_deleted += 1;
                }
            } else {
                tx.put::<NamespacedStorage>(storage_key, StorageValue(value))
                    .map_err(StateError::Database)?;
                stats.storage_slots_written += 1;
            }
        }

        Ok(())
    }

    fn delete_all_storage_for_account(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        address_hash: AddressHash,
        stats: &mut CommitStats,
    ) -> StateResult<()> {
        let mut cursor = tx
            .cursor_write::<NamespacedStorage>()
            .map_err(StateError::Database)?;
        let start_key = NamespacedStorageKey::new(address_hash, B256::ZERO);
        let mut keys_to_delete = Vec::new();

        if let Some((key, _)) = cursor.seek(start_key).map_err(StateError::Database)?
            && key.address_hash == address_hash
        {
            keys_to_delete.push(key);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if key.address_hash != address_hash {
                    break;
                }
                keys_to_delete.push(key);
            }
        }

        if !keys_to_delete.is_empty() {
            stats.full_storage_deletes += 1;
            stats.storage_slots_deleted += keys_to_delete.len();
        }

        for key in keys_to_delete {
            let _ = tx
                .delete::<NamespacedStorage>(key, None)
                .map_err(StateError::Database)?;
        }

        Ok(())
    }

    fn clear_table<T>(tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>) -> StateResult<()>
    where
        T: Table,
    {
        let db = tx.inner.open_db(Some(T::NAME)).map_err(|e| {
            StateError::DatabaseOpen {
                path: "<open transaction>".to_string(),
                message: format!("failed to open {} for clearing: {e}", T::NAME),
            }
        })?;
        tx.inner.clear_db(db.dbi()).map_err(|e| {
            StateError::DatabaseOpen {
                path: "<open transaction>".to_string(),
                message: format!("failed to clear {}: {e}", T::NAME),
            }
        })?;
        Ok(())
    }
}
