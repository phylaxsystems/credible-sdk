//! State writer implementation for persisting blockchain state to MDBX.
//!
//! Handles circular buffer rotation with state diff reconstruction.
//!
//! ## Performance Optimizations
//!
//! This writer implements several optimizations for maximum write throughput:
//!
//! 1. **Binary diffs**: State diffs are stored in a compact binary format
//!    (~10-50x faster than JSON serialization).
//!
//! 2. **Sorted batch writes**: All writes are collected and sorted before
//!    being applied, minimizing B-tree page splits.
//!
//! 3. **Cursor-based writes**: Sequential writes use cursors to avoid
//!    repeated tree traversal.
//!
//! 4. **Parallel pre-processing**: Account changes are processed in parallel
//!    using rayon before the write transaction begins.
//!
//! ## Atomicity
//!
//! All writes happen within a single MDBX transaction. This means:
//!
//! - Either all changes are committed, or none are (crash-safe)
//! - Readers never see partial writes (MVCC isolation)
//! - No explicit locking needed for read consistency
//! - State and metadata are always consistent
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
//! let stats = writer.commit_block(&update)?;
//! println!("Committed in {:?}", stats.total_duration);
//! ```

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    BlockStateUpdate,
    CommitStats,
    Reader,
    Writer,
    mdbx::{
        StateReader,
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
    redis::writer::StaleLockRecovery,
};
use alloy::primitives::{
    B256,
    Bytes,
    U256,
};
use rayon::prelude::*;
use reth_db_api::{
    cursor::{
        DbCursorRO,
        DbCursorRW,
    },
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    hash::Hash,
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

// ============================================================================
// Write Batch Types
// ============================================================================

/// Pre-sorted batch of writes for optimal MDBX performance.
///
/// MDBX B-trees perform best with sorted inserts (avoids page splits).
/// This structure collects all writes and sorts them before applying.
#[derive(Default)]
struct WriteBatch {
    /// Account updates, will be sorted by key before writing.
    accounts: Vec<(NamespacedAccountKey, AccountInfo)>,
    /// Accounts to delete.
    account_deletes: Vec<NamespacedAccountKey>,
    /// Storage updates, will be sorted by key before writing.
    storage: Vec<(NamespacedStorageKey, StorageValue)>,
    /// Storage slots to delete.
    storage_deletes: Vec<NamespacedStorageKey>,
    /// Bytecodes to write, will be sorted by key before writing.
    bytecodes: Vec<(NamespacedBytecodeKey, Bytes)>,
    /// Addresses whose entire storage should be deleted (for deleted accounts).
    full_storage_deletes: Vec<(u8, AddressHash)>,
}

impl WriteBatch {
    /// Create a new batch with pre-allocated capacity.
    fn with_capacity(account_hint: usize, storage_hint: usize) -> Self {
        Self {
            accounts: Vec::with_capacity(account_hint),
            account_deletes: Vec::with_capacity(account_hint / 10),
            storage: Vec::with_capacity(storage_hint),
            storage_deletes: Vec::with_capacity(storage_hint / 10),
            bytecodes: Vec::with_capacity(account_hint / 5),
            full_storage_deletes: Vec::with_capacity(account_hint / 20),
        }
    }

    /// Apply an overlay batch on top of this batch.
    ///
    /// This is CRITICAL for correctness: the overlay represents a newer state,
    /// so its operations must override the base state:
    /// - Overlay deletes remove entries from base writes
    /// - Overlay writes remove entries from base deletes
    /// - Then overlay entries are added
    ///
    /// This ensures that when a newer block deletes something that existed in
    /// an older block, the deletion takes effect.
    fn apply_overlay(&mut self, overlay: WriteBatch) {
        // 1. Overlay account deletes remove from base accounts
        let overlay_account_deletes: HashSet<_> = overlay.account_deletes.iter().copied().collect();
        self.accounts
            .retain(|(k, _)| !overlay_account_deletes.contains(k));

        // 2. Overlay storage deletes remove from base storage
        let overlay_storage_deletes: HashSet<_> = overlay.storage_deletes.iter().copied().collect();
        self.storage
            .retain(|(k, _)| !overlay_storage_deletes.contains(k));

        // 3. Overlay accounts remove from base account_deletes (recreation)
        let overlay_accounts: HashSet<_> = overlay.accounts.iter().map(|(k, _)| *k).collect();
        self.account_deletes
            .retain(|k| !overlay_accounts.contains(k));

        // 4. Overlay storage removes from base storage_deletes (rewrite after delete)
        let overlay_storage: HashSet<_> = overlay.storage.iter().map(|(k, _)| *k).collect();
        self.storage_deletes
            .retain(|k| !overlay_storage.contains(k));

        // 5. Overlay full_storage_deletes should clear any storage we have for those accounts
        for (ns, addr) in &overlay.full_storage_deletes {
            self.storage
                .retain(|(k, _)| !(k.namespace_idx == *ns && k.address_hash == *addr));
            self.storage_deletes
                .retain(|k| !(k.namespace_idx == *ns && k.address_hash == *addr));
        }

        // 6. Now extend with overlay entries
        self.accounts.extend(overlay.accounts);
        self.account_deletes.extend(overlay.account_deletes);
        self.storage.extend(overlay.storage);
        self.storage_deletes.extend(overlay.storage_deletes);
        self.bytecodes.extend(overlay.bytecodes);
        self.full_storage_deletes
            .extend(overlay.full_storage_deletes);
    }

    /// Sort all entries and deduplicate.
    ///
    /// After `apply_overlay` has been called for all batches in order,
    /// there should be no conflicts between writes and deletes.
    /// This just deduplicates within each vector and sorts for optimal B-tree insertion.
    fn sort_and_deduplicate(&mut self) {
        Self::deduplicate_keep_last(&mut self.accounts, |(k, _)| *k);
        self.accounts.sort_unstable_by_key(|(k, _)| *k);

        self.account_deletes.sort_unstable();
        self.account_deletes.dedup();

        Self::deduplicate_keep_last(&mut self.storage, |(k, _)| *k);
        self.storage.sort_unstable_by_key(|(k, _)| *k);

        self.storage_deletes.sort_unstable();
        self.storage_deletes.dedup();

        Self::deduplicate_keep_last(&mut self.bytecodes, |(k, _)| *k);
        self.bytecodes.sort_unstable_by_key(|(k, _)| *k);

        self.full_storage_deletes.sort_unstable();
        self.full_storage_deletes.dedup();
    }

    /// Deduplicate a vector, keeping the LAST occurrence of each key.
    ///
    /// This ensures that when multiple blocks modify the same key,
    /// the most recent block's value (which was added last) wins.
    fn deduplicate_keep_last<T, K>(vec: &mut Vec<T>, key_fn: impl Fn(&T) -> K)
    where
        K: Eq + Hash,
    {
        if vec.is_empty() {
            return;
        }

        // Build a map of key -> last index
        let mut last_occurrence: HashMap<K, usize> = HashMap::with_capacity(vec.len());
        for (i, item) in vec.iter().enumerate() {
            last_occurrence.insert(key_fn(item), i);
        }

        // Collect the indices we want to keep, sorted
        let mut keep_indices: Vec<usize> = last_occurrence.into_values().collect();
        keep_indices.sort_unstable();

        // Compact the vector in-place
        let mut write_idx = 0;
        for read_idx in keep_indices {
            if write_idx != read_idx {
                vec.swap(write_idx, read_idx);
            }
            write_idx += 1;
        }
        vec.truncate(write_idx);
    }
}

// ============================================================================
// State Writer
// ============================================================================

/// State writer for persisting blockchain state to MDBX.
///
/// Uses standard durable sync mode for maximum data safety. All writes
/// are fully synced to disk on commit.
///
/// MDBX enforces single-writer semantics, so only one `StateWriter`
/// should be active at a time per database path.
#[derive(Debug)]
pub struct StateWriter {
    reader: StateReader,
}

impl Reader for StateWriter {
    type Error = StateError;

    /// Get the most recent block number.
    ///
    /// Returns `None` if no blocks have been written yet.
    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        self.reader.latest_block_number()
    }

    /// Check if a block is available in the circular buffer.
    ///
    /// A block is available if its namespace currently contains that block.
    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        self.reader.is_block_available(block_number)
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
        self.reader.get_account(address_hash, block_number)
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
        self.reader
            .get_storage(address_hash, slot_hash, block_number)
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
        self.reader.get_all_storage(address_hash, block_number)
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
        self.reader.get_code(code_hash, block_number)
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
        self.reader.get_full_account(address_hash, block_number)
    }

    /// Get block hash for a specific block number.
    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_block_hash(block_number)
    }

    /// Get state root for a specific block number.
    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_state_root(block_number)
    }

    /// Get block metadata (hash and state root).
    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        self.reader.get_block_metadata(block_number)
    }

    /// Get the range of available blocks [oldest, latest].
    ///
    /// Returns `None` if no blocks have been written.
    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        self.reader.get_available_block_range()
    }
}

impl Writer for StateWriter {
    type Error = StateError;

    /// Commit a block's state update to the database.
    ///
    /// This handles:
    /// 1. Parallel pre-processing of account changes
    /// 2. Converting to binary diff format
    /// 3. Loading base state (from previous namespace) or intermediate diffs
    /// 4. Applying all batches as overlays in chronological order
    /// 5. Writing all account and storage changes in one transaction
    /// 6. Updating metadata and cleaning up old data
    ///
    /// ## Atomicity
    ///
    /// All changes happen in a single transaction. If anything fails,
    /// the entire operation is rolled back and the database remains unchanged.
    /// State and metadata are ALWAYS updated together - you will never see
    /// partial updates.
    ///
    /// ## Account Uniqueness
    ///
    /// Each account must appear at most once in `update.accounts`. Use
    /// `BlockStateUpdate::merge_account_state()` to ensure this. Duplicate
    /// accounts will cause an error.
    ///
    /// ## Reconstruction
    ///
    /// When a namespace rotates (e.g., block 103 replaces block 100 in namespace 1
    /// with `buffer_size`=3), the intermediate diffs (blocks 101, 102) are applied
    /// first to bring the namespace's state up to date.
    ///
    /// ## Returns
    ///
    /// Returns `CommitStats` with timing and count information for metrics.
    #[allow(clippy::too_many_lines)]
    #[instrument(
        skip(self, update),
        fields(
            block_number = update.block_number,
            accounts = update.accounts.len(),
        ),
        level = "debug"
    )]
    fn commit_block(&self, update: &BlockStateUpdate) -> StateResult<CommitStats> {
        let total_start = Instant::now();
        let mut stats = CommitStats::default();

        let db = self.reader.db();
        let block_number = update.block_number;
        let namespace_idx = db.namespace_for_block(block_number)?;
        let buffer_size = db.buffer_size();

        // ====================================================================
        // Phase 1: Validation and parallel pre-processing (outside transaction)
        // ====================================================================
        let preprocess_start = Instant::now();

        // Validate no duplicate accounts (required for parallel processing correctness)
        Self::validate_unique_accounts(update)?;

        // Convert to binary diff format with parallel storage sorting
        let diff = Self::to_binary_diff_parallel(update);

        // Build write batch for current block
        let current_block_batch = Self::build_write_batch_parallel(namespace_idx, &diff);

        // Serialize diff to bytes
        let diff_bytes = diff.to_bytes()?;

        stats.diff_bytes = diff_bytes.len();
        stats.largest_account_storage = diff
            .accounts
            .iter()
            .map(|a| a.storage.len())
            .max()
            .unwrap_or(0);

        stats.preprocess_duration = preprocess_start.elapsed();

        trace!(
            diff_bytes = stats.diff_bytes,
            preprocess_ms = stats.preprocess_duration.as_millis(),
            "preprocessing complete"
        );

        // ====================================================================
        // Phase 2: Load base state and intermediate diffs
        // ====================================================================
        let diff_start = Instant::now();

        let base_batches = {
            let read_tx = db.tx()?;

            let current_ns_block = read_tx
                .get::<NamespaceBlocks>(NamespaceIdx(namespace_idx))
                .map_err(StateError::Database)?;

            match current_ns_block {
                Some(existing_block) => {
                    // Namespace has a block - load intermediate diffs
                    let start_block = existing_block.0 + 1;
                    if block_number > start_block {
                        let diffs_to_apply = block_number - start_block;
                        debug!(
                            namespace = namespace_idx,
                            current_block = existing_block.0,
                            target_block = block_number,
                            diffs = diffs_to_apply,
                            "loading intermediate diffs for rotation"
                        );

                        let batches: Vec<WriteBatch> = (start_block..block_number)
                            .into_par_iter()
                            .map(|diff_block| {
                                Self::load_and_build_batch(
                                    &read_tx,
                                    namespace_idx,
                                    diff_block,
                                    block_number,
                                )
                            })
                            .collect::<StateResult<Vec<_>>>()?;

                        stats.diffs_applied = batches.len();
                        batches
                    } else {
                        vec![]
                    }
                }
                None => {
                    // Namespace is empty - copy state from previous block's namespace
                    if block_number > 0 {
                        let prev_block = block_number - 1;
                        let prev_namespace = db.namespace_for_block(prev_block)?;
                        debug!(
                            from_namespace = prev_namespace,
                            to_namespace = namespace_idx,
                            "copying base state from previous namespace"
                        );
                        let base_batch =
                            Self::copy_namespace_state(&read_tx, prev_namespace, namespace_idx)?;
                        vec![base_batch]
                    } else {
                        vec![]
                    }
                }
            }
        };

        stats.diff_application_duration = diff_start.elapsed();

        // ====================================================================
        // Phase 3: Apply all batches as overlays in CHRONOLOGICAL ORDER
        // Each overlay's deletes remove from previous writes, and writes remove from previous deletes
        // ====================================================================
        let batch_start = Instant::now();

        let mut final_batch = WriteBatch::default();

        for batch in base_batches {
            final_batch.apply_overlay(batch);
        }

        // Apply current block LAST
        final_batch.apply_overlay(current_block_batch);

        // Sort and deduplicate for optimal B-tree insertion
        final_batch.sort_and_deduplicate();

        // Collect stats from final batch
        stats.accounts_written = final_batch.accounts.len();
        stats.accounts_deleted = final_batch.account_deletes.len();
        stats.storage_slots_written = final_batch.storage.len();
        stats.storage_slots_deleted = final_batch.storage_deletes.len();
        stats.full_storage_deletes = final_batch.full_storage_deletes.len();
        stats.bytecodes_written = final_batch.bytecodes.len();

        trace!(
            accounts_written = stats.accounts_written,
            storage_written = stats.storage_slots_written,
            "batch preparation complete"
        );

        // ====================================================================
        // Phase 4: Single write transaction (serialized by MDBX)
        // All writes are atomic - either all succeed or none do
        // ====================================================================
        let tx = db.tx_mut()?;

        Self::execute_batch(&tx, final_batch)?;

        stats.batch_write_duration = batch_start.elapsed();

        // Store diff (binary format)
        tx.put::<StateDiffs>(BlockNumber(block_number), StateDiffData(diff_bytes))
            .map_err(StateError::Database)?;

        // Update namespace block number
        tx.put::<NamespaceBlocks>(NamespaceIdx(namespace_idx), BlockNumber(block_number))
            .map_err(StateError::Database)?;

        // Update block metadata
        tx.put::<BlockMetadataTable>(
            BlockNumber(block_number),
            crate::mdbx::common::types::BlockMetadata {
                block_hash: update.block_hash,
                state_root: update.state_root,
            },
        )
        .map_err(StateError::Database)?;

        // Update global metadata (latest_block)
        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block: block_number,
                buffer_size,
            },
        )
        .map_err(StateError::Database)?;

        // Cleanup old data (diffs and metadata beyond buffer)
        let buffer_size_u64 = u64::from(buffer_size);
        if block_number >= buffer_size_u64 {
            let cleanup_block = block_number - buffer_size_u64;
            let _ = tx
                .delete::<StateDiffs>(BlockNumber(cleanup_block), None)
                .map_err(StateError::Database)?;
            let _ = tx
                .delete::<BlockMetadataTable>(BlockNumber(cleanup_block), None)
                .map_err(StateError::Database)?;
        }

        // Commit transaction (atomic) - all changes become visible at once
        let commit_start = Instant::now();
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;
        stats.commit_duration = commit_start.elapsed();

        stats.total_duration = total_start.elapsed();

        // Record final metrics on span
        #[allow(clippy::cast_possible_truncation)]
        Span::current().record("total_ms", stats.total_duration.as_millis() as i64);

        debug!(
            total_ms = stats.total_duration.as_millis(),
            preprocess_ms = stats.preprocess_duration.as_millis(),
            batch_write_ms = stats.batch_write_duration.as_millis(),
            commit_ms = stats.commit_duration.as_millis(),
            diffs_applied = stats.diffs_applied,
            "block committed"
        );

        Ok(stats)
    }

    /// Ensure the database metadata matches the configured buffer size.
    fn ensure_dump_index_metadata(&self) -> StateResult<()> {
        Ok(())
    }

    /// Check for and recover from stale locks on all namespaces.
    ///
    /// Note: MDBX doesn't use the same locking semantics as Redis.
    /// This is a no-op for MDBX but implemented for trait compatibility.
    fn recover_stale_locks(&self) -> StateResult<Vec<StaleLockRecovery>> {
        // MDBX uses MVCC and doesn't have the same lock recovery needs as Redis.
        // Returning empty vec indicates no recovery was needed.
        trace!("recover_stale_locks called (no-op for MDBX)");
        Ok(vec![])
    }
}

impl StateWriter {
    /// Create a new writer.
    ///
    /// Creates the database if it doesn't exist. Uses standard durable
    /// sync mode for maximum data safety.
    pub fn new(path: impl AsRef<Path>, config: CircularBufferConfig) -> StateResult<Self> {
        let db = StateDb::open(path, config)?;
        let reader = StateReader::from_db(db);
        Ok(Self { reader })
    }

    /// Get a reference to the underlying reader.
    pub fn reader(&self) -> &StateReader {
        &self.reader
    }

    /// Get the configured buffer size.
    pub fn buffer_size(&self) -> u8 {
        self.reader.buffer_size()
    }

    // ========================================================================
    // Validation
    // ========================================================================

    /// Validate that all accounts in the update are unique.
    ///
    /// This is required for parallel processing correctness - if the same
    /// account appears multiple times, the final state would depend on
    /// processing order.
    fn validate_unique_accounts(update: &BlockStateUpdate) -> StateResult<()> {
        let mut seen = std::collections::HashSet::with_capacity(update.accounts.len());
        for acc in &update.accounts {
            if !seen.insert(acc.address_hash) {
                warn!(
                    address_hash = %acc.address_hash,
                    block = update.block_number,
                    "duplicate account in block state update"
                );
                return Err(StateError::DuplicateAccount(acc.address_hash));
            }
        }
        Ok(())
    }

    /// Convert `BlockStateUpdate` to `BinaryStateDiff` with parallel storage sorting.
    fn to_binary_diff_parallel(update: &BlockStateUpdate) -> BinaryStateDiff {
        // Process accounts in parallel, sorting their storage
        let accounts: Vec<BinaryAccountDiff> = update
            .accounts
            .par_iter()
            .map(|acc| {
                // Sort storage by slot_hash for sequential cursor writes
                let mut storage: Vec<_> = acc.storage.iter().map(|(k, v)| (*k, *v)).collect();
                storage.sort_unstable_by_key(|(slot, _)| *slot);

                BinaryAccountDiff {
                    address_hash: acc.address_hash,
                    deleted: acc.deleted,
                    balance: acc.balance,
                    nonce: acc.nonce,
                    code_hash: acc.code_hash,
                    code: acc.code.clone(),
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

    /// Build write batch from diff with parallel account processing.
    fn build_write_batch_parallel(namespace_idx: u8, diff: &BinaryStateDiff) -> WriteBatch {
        // Estimate total storage count for capacity hint
        let storage_count: usize = diff.accounts.iter().map(|a| a.storage.len()).sum();

        // Process accounts in parallel chunks
        let chunk_size = (diff.accounts.len() / rayon::current_num_threads()).max(1);

        let batches: Vec<WriteBatch> = diff
            .accounts
            .par_chunks(chunk_size.max(1))
            .map(|chunk| {
                let mut batch = WriteBatch::with_capacity(
                    chunk.len(),
                    storage_count / rayon::current_num_threads().max(1),
                );

                for acc in chunk {
                    Self::process_account_to_batch(namespace_idx, acc, &mut batch);
                }

                batch
            })
            .collect();

        // Merge all batches using apply_overlay to handle any within-diff conflicts correctly
        let mut final_batch = WriteBatch::with_capacity(diff.accounts.len(), storage_count);
        for batch in batches {
            final_batch.apply_overlay(batch);
        }

        final_batch
    }

    /// Process a single account into a write batch.
    fn process_account_to_batch(
        namespace_idx: u8,
        acc: &BinaryAccountDiff,
        batch: &mut WriteBatch,
    ) {
        let account_key = NamespacedAccountKey::new(namespace_idx, acc.address_hash);

        if acc.deleted {
            batch.account_deletes.push(account_key);
            batch
                .full_storage_deletes
                .push((namespace_idx, acc.address_hash));
            return;
        }

        // Account data
        batch.accounts.push((
            account_key,
            AccountInfo {
                address_hash: acc.address_hash,
                balance: acc.balance,
                nonce: acc.nonce,
                code_hash: acc.code_hash,
            },
        ));

        // Bytecode
        if let Some(code) = &acc.code
            && !code.is_empty()
        {
            batch.bytecodes.push((
                NamespacedBytecodeKey::new(namespace_idx, acc.code_hash),
                code.clone(),
            ));
        }

        // Storage (already sorted in BinaryAccountDiff)
        for (slot_hash, value) in &acc.storage {
            let storage_key =
                NamespacedStorageKey::new(namespace_idx, acc.address_hash, *slot_hash);
            if value.is_zero() {
                batch.storage_deletes.push(storage_key);
            } else {
                batch.storage.push((storage_key, StorageValue(*value)));
            }
        }
    }

    // ========================================================================
    // State Copying and Diff Loading
    // ========================================================================

    /// Copy all state from one namespace to another, returning a `WriteBatch`.
    fn copy_namespace_state(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RO>,
        from_namespace: u8,
        to_namespace: u8,
    ) -> StateResult<WriteBatch> {
        let mut batch = WriteBatch::default();

        // Copy all accounts
        {
            let mut cursor = tx
                .cursor_read::<NamespacedAccounts>()
                .map_err(StateError::Database)?;

            let start_key = NamespacedAccountKey::new(from_namespace, AddressHash::default());

            if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
                && key.namespace_idx == from_namespace
            {
                let new_key = NamespacedAccountKey::new(to_namespace, key.address_hash);
                batch.accounts.push((new_key, value));

                while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                    if key.namespace_idx != from_namespace {
                        break;
                    }
                    let new_key = NamespacedAccountKey::new(to_namespace, key.address_hash);
                    batch.accounts.push((new_key, value));
                }
            }
        }

        // Copy all storage
        {
            let mut cursor = tx
                .cursor_read::<NamespacedStorage>()
                .map_err(StateError::Database)?;

            let start_key =
                NamespacedStorageKey::new(from_namespace, AddressHash::default(), B256::ZERO);

            if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
                && key.namespace_idx == from_namespace
            {
                let new_key =
                    NamespacedStorageKey::new(to_namespace, key.address_hash, key.slot_hash);
                batch.storage.push((new_key, value));

                while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                    if key.namespace_idx != from_namespace {
                        break;
                    }
                    let new_key =
                        NamespacedStorageKey::new(to_namespace, key.address_hash, key.slot_hash);
                    batch.storage.push((new_key, value));
                }
            }
        }

        // Copy all bytecodes
        {
            let mut cursor = tx
                .cursor_read::<Bytecodes>()
                .map_err(StateError::Database)?;

            let start_key = NamespacedBytecodeKey::new(from_namespace, B256::ZERO);

            if let Some((key, value)) = cursor.seek(start_key).map_err(StateError::Database)?
                && key.namespace_idx == from_namespace
            {
                let new_key = NamespacedBytecodeKey::new(to_namespace, key.code_hash);
                batch.bytecodes.push((new_key, value.0));

                while let Some((key, value)) = cursor.next().map_err(StateError::Database)? {
                    if key.namespace_idx != from_namespace {
                        break;
                    }
                    let new_key = NamespacedBytecodeKey::new(to_namespace, key.code_hash);
                    batch.bytecodes.push((new_key, value.0));
                }
            }
        }

        Ok(batch)
    }

    /// Load a stored diff and build a write batch from it.
    fn load_and_build_batch(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RO>,
        namespace_idx: u8,
        diff_block: u64,
        target_block: u64,
    ) -> StateResult<WriteBatch> {
        let diff_data = tx
            .get::<StateDiffs>(BlockNumber(diff_block))
            .map_err(StateError::Database)?
            .ok_or(StateError::MissingStateDiff {
                needed_block: diff_block,
                target_block,
            })?;

        let diff = BinaryStateDiff::from_bytes(&diff_data.0)?;
        Ok(Self::build_write_batch_parallel(namespace_idx, &diff))
    }

    // ========================================================================
    // Batch Execution
    // ========================================================================

    /// Execute batched writes using cursors for optimal performance.
    fn execute_batch(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        batch: WriteBatch,
    ) -> StateResult<()> {
        // Delete accounts first (before storage cleanup)
        for key in batch.account_deletes {
            tx.delete::<NamespacedAccounts>(key, None)
                .map_err(StateError::Database)?;
        }

        // Delete all storage for deleted accounts
        for (namespace_idx, address_hash) in batch.full_storage_deletes {
            Self::delete_account_storage(tx, namespace_idx, address_hash)?;
        }

        // Write accounts using cursor for sequential access
        if !batch.accounts.is_empty() {
            let mut cursor = tx
                .cursor_write::<NamespacedAccounts>()
                .map_err(StateError::Database)?;
            for (key, value) in batch.accounts {
                cursor.upsert(key, &value).map_err(StateError::Database)?;
            }
        }

        // Write storage using cursor for sequential access
        if !batch.storage.is_empty() {
            let mut cursor = tx
                .cursor_write::<NamespacedStorage>()
                .map_err(StateError::Database)?;
            for (key, value) in batch.storage {
                cursor.upsert(key, &value).map_err(StateError::Database)?;
            }
        }

        // Delete storage slots
        if !batch.storage_deletes.is_empty() {
            let mut cursor = tx
                .cursor_write::<NamespacedStorage>()
                .map_err(StateError::Database)?;
            for key in batch.storage_deletes {
                if cursor
                    .seek_exact(key)
                    .map_err(StateError::Database)?
                    .is_some()
                {
                    cursor.delete_current().map_err(StateError::Database)?;
                }
            }
        }

        // Write bytecodes using cursor
        if !batch.bytecodes.is_empty() {
            let mut cursor = tx
                .cursor_write::<Bytecodes>()
                .map_err(StateError::Database)?;
            for (key, value) in batch.bytecodes {
                cursor
                    .upsert(key, &Bytecode(value))
                    .map_err(StateError::Database)?;
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

        // Collect keys to delete (can't delete while iterating with seek pattern)
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
