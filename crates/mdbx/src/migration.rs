//! Buffer size migration for the circular buffer state database.
//!
//! When the configured `buffer_size` is smaller than the stored `buffer_size`,
//! the database must be migrated: surviving namespaces are advanced to their
//! target blocks by applying stored state diffs, orphaned namespace metadata
//! is removed atomically, and a background cleanup task is returned for
//! deleting the now-unreachable account/storage/bytecode data.
//!
//! ## Safety
//!
//! The critical migration (namespace advancement + metadata update) runs in a
//! single MDBX write transaction. If the process crashes before commit, nothing
//! changes and the mismatch is detected again on the next startup.
//!
//! Background cleanup of orphaned namespace data is non-blocking and
//! crash-tolerant: orphaned data is inert (no read/write path touches it),
//! so incomplete cleanup is harmless and will be retried on next startup.

use crate::{
    AddressHash,
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
            StateDiffs,
        },
        types::{
            BinaryStateDiff,
            GlobalMetadata,
            NamespacedAccountKey,
            NamespacedBytecodeKey,
            NamespacedStorageKey,
        },
    },
    db::StateDb,
    writer::StateWriter,
};
use alloy::primitives::B256;
use reth_db_api::{
    cursor::DbCursorRO,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use tracing::{
    debug,
    info,
    warn,
};

/// Outcome of the startup migration check.
#[derive(Debug)]
pub enum MigrationResult {
    /// No migration was needed (buffer sizes already match, or DB is empty).
    NoOp,
    /// Migration completed successfully.
    Completed {
        /// Previous buffer size stored in the database.
        old_size: u8,
        /// New buffer size from configuration.
        new_size: u8,
        /// Background task for cleaning orphaned namespace data.
        /// `None` if no orphaned namespaces exist (e.g. the old namespaces
        /// were already empty).
        cleanup: Option<CleanupTask>,
    },
}

/// Background task for deleting data in orphaned namespaces.
///
/// Orphaned namespace data (accounts, storage, bytecodes) is inert after
/// migration — no read or write path can reach it — but it wastes disk
/// space. This task removes it in batched MDBX transactions.
///
/// Safe to run concurrently with normal block commits since the orphaned
/// namespaces are outside the range `0..new_buffer_size`.
#[derive(Debug)]
pub struct CleanupTask {
    db: StateDb,
    /// First orphaned namespace index (inclusive).
    start_namespace: u8,
    /// Last orphaned namespace index (exclusive).
    end_namespace: u8,
}

/// Statistics from orphaned namespace cleanup.
#[derive(Debug, Clone, Default)]
pub struct CleanupStats {
    pub accounts_deleted: usize,
    pub storage_slots_deleted: usize,
    pub bytecodes_deleted: usize,
}

impl CleanupTask {
    /// Run the cleanup, deleting all data in orphaned namespaces.
    ///
    /// Each namespace is cleaned in its own MDBX write transaction to
    /// avoid holding a single massive transaction open.
    ///
    /// # Errors
    ///
    /// Returns any database error encountered during cleanup.
    pub fn run(self) -> StateResult<CleanupStats> {
        let mut stats = CleanupStats::default();

        for ns in self.start_namespace..self.end_namespace {
            info!(namespace = ns, "cleaning orphaned namespace data");

            let ns_stats = Self::cleanup_single_namespace(&self.db, ns)?;

            stats.accounts_deleted += ns_stats.accounts_deleted;
            stats.storage_slots_deleted += ns_stats.storage_slots_deleted;
            stats.bytecodes_deleted += ns_stats.bytecodes_deleted;

            info!(
                namespace = ns,
                accounts = ns_stats.accounts_deleted,
                storage = ns_stats.storage_slots_deleted,
                bytecodes = ns_stats.bytecodes_deleted,
                "orphaned namespace cleaned"
            );
        }

        info!(
            total_accounts = stats.accounts_deleted,
            total_storage = stats.storage_slots_deleted,
            total_bytecodes = stats.bytecodes_deleted,
            "background cleanup complete"
        );

        Ok(stats)
    }
}

// ============================================================================
// Migration implementation on StateWriter
// ============================================================================

impl StateWriter {
    /// Compute the target block for a namespace after migration.
    ///
    /// Returns the most recent block `B <= latest_block` where `B % new_size == ns`.
    /// Returns `None` if no such block exists (i.e. `latest_block < ns`).
    fn compute_target_block(latest_block: u64, new_size: u8, ns: u8) -> Option<u64> {
        let ns_u64 = u64::from(ns);
        let new_size_u64 = u64::from(new_size);

        // target = latest_block - ((latest_block - ns) % new_size)
        // This gives the most recent block B where B % new_size == ns.
        let offset = latest_block.checked_sub(ns_u64)?;
        let remainder = offset % new_size_u64;
        latest_block.checked_sub(remainder)
    }
    /// Check for buffer size mismatch and perform migration if needed.
    ///
    /// Call this after construction, before any `commit_block()` calls.
    ///
    /// ## Behaviour
    ///
    /// | Stored vs configured | Action |
    /// |----------------------|--------|
    /// | Equal                | No-op (but checks for incomplete cleanup) |
    /// | Stored > configured  | Migrates: advances surviving namespaces, prunes metadata |
    /// | Stored < configured  | Returns `BufferSizeIncrease` error |
    /// | Empty DB             | No-op |
    ///
    /// ## Returns
    ///
    /// `MigrationResult::Completed { cleanup }` when migration ran.
    /// The caller should spawn `cleanup.run()` in a background thread/task
    /// to free disk space from orphaned namespace data.
    ///
    /// # Errors
    ///
    /// Returns `BufferSizeIncrease` if the configured size is larger than stored.
    /// Returns any database error from the migration transaction.
    pub fn migrate_if_needed(&self) -> StateResult<MigrationResult> {
        let db = self.reader().db();
        let new_size = db.buffer_size();

        // Read stored metadata
        let meta = {
            let tx = db.tx()?;
            tx.get::<Metadata>(MetadataKey)
                .map_err(StateError::Database)?
        };

        let Some(meta) = meta else {
            debug!("empty database, no migration needed");
            return Ok(MigrationResult::NoOp);
        };

        let stored_size = meta.buffer_size;

        if stored_size == 0 {
            // Corrupted metadata — a valid database never has buffer_size=0.
            warn!("stored buffer_size is 0, treating as empty database");
            return Ok(MigrationResult::NoOp);
        }

        if stored_size == new_size {
            return self.check_pending_cleanup(new_size);
        }

        if stored_size < new_size {
            return Err(StateError::BufferSizeIncrease {
                stored: stored_size,
                configured: new_size,
            });
        }

        // Reduction: stored_size > new_size
        info!(
            old_size = stored_size,
            new_size = new_size,
            latest_block = meta.latest_block,
            "buffer size reduction detected, starting migration"
        );

        self.perform_buffer_reduction(meta.latest_block, stored_size, new_size)
    }

    /// Perform the buffer size reduction migration in a single atomic transaction.
    fn perform_buffer_reduction(
        &self,
        latest_block: u64,
        old_size: u8,
        new_size: u8,
    ) -> StateResult<MigrationResult> {
        let db = self.reader().db();
        let tx = db.tx_mut()?;

        // Phase 1: Advance surviving namespaces to their target blocks.
        // Build a snapshot of current namespace→block assignments before mutating.
        let mut ns_blocks: Vec<(u8, Option<u64>)> = Vec::with_capacity(usize::from(old_size));
        for ns in 0..old_size {
            let block = tx
                .get::<NamespaceBlocks>(NamespaceIdx(ns))
                .map_err(StateError::Database)?
                .map(|b| b.0);
            ns_blocks.push((ns, block));
        }

        // Process "rebuild" cases (current > target) FIRST so their source
        // namespaces haven't been mutated yet. Then process "advance" cases.
        let mut rebuild_ns = Vec::new();
        let mut advance_ns = Vec::new();

        for ns in 0..new_size {
            if let Some(target) = Self::compute_target_block(latest_block, new_size, ns) {
                let current = ns_blocks.get(usize::from(ns)).and_then(|(_, b)| *b);
                match current {
                    Some(c) if c > target => rebuild_ns.push(ns),
                    _ => advance_ns.push(ns),
                }
            } else {
                advance_ns.push(ns);
            }
        }

        for ns in rebuild_ns {
            Self::advance_namespace_to_target(&tx, &ns_blocks, latest_block, new_size, ns)?;
        }
        for ns in advance_ns {
            Self::advance_namespace_to_target(&tx, &ns_blocks, latest_block, new_size, ns)?;
        }

        // Phase 2: Delete NamespaceBlocks entries for orphaned namespaces
        for ns in new_size..old_size {
            let _ = tx
                .delete::<NamespaceBlocks>(NamespaceIdx(ns), None)
                .map_err(StateError::Database)?;
            debug!(namespace = ns, "deleted orphaned namespace pointer");
        }

        // Phase 3: Prune excess StateDiffs and BlockMetadata
        Self::prune_excess_diffs(&tx, latest_block, old_size, new_size)?;

        // Phase 4: Update global metadata with new buffer size
        tx.put::<Metadata>(
            MetadataKey,
            GlobalMetadata {
                latest_block,
                buffer_size: new_size,
            },
        )
        .map_err(StateError::Database)?;

        // Commit — all-or-nothing
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        info!(
            old_size = old_size,
            new_size = new_size,
            latest_block = latest_block,
            "buffer size migration committed"
        );

        // Build cleanup task for orphaned namespace data
        let cleanup = Some(CleanupTask {
            db: db.clone(),
            start_namespace: new_size,
            end_namespace: old_size,
        });

        Ok(MigrationResult::Completed {
            old_size,
            new_size,
            cleanup,
        })
    }

    /// Advance a single namespace to its target block.
    ///
    /// Three cases:
    /// - `current == target`: no-op.
    /// - `current < target`: apply diffs `(current+1)..=target` in place.
    /// - `current > target`: the old modulo mapped a *newer* block to this
    ///   namespace. We find another namespace that holds an older-or-equal
    ///   block, copy its state here, then apply diffs up to the target.
    fn advance_namespace_to_target(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        ns_blocks: &[(u8, Option<u64>)],
        latest_block: u64,
        new_size: u8,
        ns: u8,
    ) -> StateResult<()> {
        let Some(target_block) = Self::compute_target_block(latest_block, new_size, ns) else {
            debug!(namespace = ns, "no target block, skipping");
            return Ok(());
        };

        let current_block = ns_blocks.get(usize::from(ns)).and_then(|(_, b)| *b);

        let Some(current) = current_block else {
            debug!(
                namespace = ns,
                "namespace empty, will be populated on first write"
            );
            return Ok(());
        };

        if current == target_block {
            debug!(
                namespace = ns,
                block = target_block,
                "namespace already at target"
            );
            return Ok(());
        }

        if current < target_block {
            Self::apply_diffs_to_namespace(tx, ns, current, target_block)?;
        } else {
            Self::rebuild_namespace_from_source(tx, ns_blocks, ns, current, target_block)?;
        }

        // Update namespace pointer
        tx.put::<NamespaceBlocks>(NamespaceIdx(ns), BlockNumber(target_block))
            .map_err(StateError::Database)?;

        debug!(
            namespace = ns,
            block = target_block,
            "namespace set to target"
        );
        Ok(())
    }

    /// Rebuild a namespace whose current block is newer than its target.
    ///
    /// Finds a source namespace with a block <= target, clears the target
    /// namespace, copies state from the source, then applies diffs forward.
    fn rebuild_namespace_from_source(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        ns_blocks: &[(u8, Option<u64>)],
        ns: u8,
        current: u64,
        target_block: u64,
    ) -> StateResult<()> {
        let source = ns_blocks
            .iter()
            .filter(|(src_ns, _)| *src_ns != ns)
            .filter_map(|(src_ns, block)| block.map(|b| (*src_ns, b)))
            .filter(|(_, b)| *b <= target_block)
            .max_by_key(|(_, b)| *b);

        let Some((src_ns, src_block)) = source else {
            warn!(
                namespace = ns,
                current_block = current,
                target_block = target_block,
                "no source namespace found for reconstruction, skipping"
            );
            return Ok(());
        };

        info!(
            namespace = ns,
            source_namespace = src_ns,
            source_block = src_block,
            target_block = target_block,
            "rebuilding namespace from source + diffs"
        );

        // Clear existing data so stale entries from the newer block don't
        // persist alongside the copied state.
        Self::clear_namespace_inline(tx, ns)?;

        // Copy full state from source namespace
        let base_batch = StateWriter::copy_namespace_state(tx, src_ns, ns)?;
        StateWriter::execute_batch(tx, base_batch)?;

        // Apply diffs from source_block+1 to target_block
        if src_block < target_block {
            Self::apply_diffs_to_namespace(tx, ns, src_block, target_block)?;
        }

        Ok(())
    }

    /// Apply diffs sequentially to advance a namespace from `current` to `target`.
    fn apply_diffs_to_namespace(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        ns: u8,
        current: u64,
        target: u64,
    ) -> StateResult<()> {
        let diffs_needed = target - current;
        info!(
            namespace = ns,
            current_block = current,
            target_block = target,
            diffs = diffs_needed,
            "advancing namespace by applying diffs"
        );

        for diff_block in (current + 1)..=target {
            let diff_data = tx
                .get::<StateDiffs>(BlockNumber(diff_block))
                .map_err(StateError::Database)?
                .ok_or(StateError::MissingStateDiff {
                    needed_block: diff_block,
                    target_block: target,
                })?;

            let diff = BinaryStateDiff::from_bytes(&diff_data.0)?;
            let batch = StateWriter::build_write_batch_parallel(ns, &diff);
            StateWriter::execute_batch(tx, batch)?;
        }

        Ok(())
    }

    /// Prune `StateDiffs` and `BlockMetadataTable` entries that fall outside the
    /// new (smaller) buffer window.
    fn prune_excess_diffs(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        latest_block: u64,
        old_size: u8,
        new_size: u8,
    ) -> StateResult<()> {
        // Old buffer kept diffs for: [latest - old_size + 1, latest]
        // New buffer needs diffs for: [latest - new_size + 1, latest]
        // So prune: [latest - old_size + 1, latest - new_size]
        let oldest_old = latest_block.saturating_sub(u64::from(old_size).saturating_sub(1));
        let oldest_needed = latest_block.saturating_sub(u64::from(new_size).saturating_sub(1));

        if oldest_old >= oldest_needed {
            return Ok(());
        }

        debug!(
            prune_from = oldest_old,
            prune_to = oldest_needed - 1,
            "pruning excess diffs and block metadata"
        );

        for block in oldest_old..oldest_needed {
            let _ = tx
                .delete::<StateDiffs>(BlockNumber(block), None)
                .map_err(StateError::Database)?;
            let _ = tx
                .delete::<BlockMetadataTable>(BlockNumber(block), None)
                .map_err(StateError::Database)?;
        }

        Ok(())
    }

    /// Check if a previous migration left orphaned namespace data behind
    /// (e.g. due to a crash during background cleanup).
    fn check_pending_cleanup(&self, current_size: u8) -> StateResult<MigrationResult> {
        let db = self.reader().db();
        let tx = db.tx()?;

        // Quick probe: seek into NamespacedAccounts for any entry with
        // namespace_idx >= current_size. If one exists, there is orphaned data.
        let mut cursor = tx
            .cursor_read::<NamespacedAccounts>()
            .map_err(StateError::Database)?;

        let probe_key = NamespacedAccountKey::new(current_size, AddressHash::default());
        if cursor
            .seek(probe_key)
            .map_err(StateError::Database)?
            .is_none()
        {
            return Ok(MigrationResult::NoOp);
        }

        // Determine the highest orphaned namespace by scanning to the end
        let mut max_ns = current_size;
        if let Some((key, _)) = cursor.seek(probe_key).map_err(StateError::Database)? {
            max_ns = max_ns.max(key.namespace_idx);
        }
        while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
            max_ns = max_ns.max(key.namespace_idx);
        }

        let end_namespace = max_ns.saturating_add(1);

        info!(
            current_size = current_size,
            orphaned_start = current_size,
            orphaned_end = end_namespace,
            "detected orphaned namespace data from previous migration"
        );

        Ok(MigrationResult::Completed {
            old_size: end_namespace,
            new_size: current_size,
            cleanup: Some(CleanupTask {
                db: db.clone(),
                start_namespace: current_size,
                end_namespace,
            }),
        })
    }
}

impl StateWriter {
    /// Delete all account, storage, and bytecode data for a namespace within an
    /// existing write transaction. Used when rebuilding a namespace during migration.
    fn clear_namespace_inline(
        tx: &reth_db::mdbx::tx::Tx<reth_libmdbx::RW>,
        ns: u8,
    ) -> StateResult<()> {
        let account_keys =
            Self::collect_keys_in_namespace::<NamespacedAccounts, NamespacedAccountKey>(
                tx,
                NamespacedAccountKey::new(ns, AddressHash::default()),
                |k| k.namespace_idx == ns,
            )?;
        for key in &account_keys {
            tx.delete::<NamespacedAccounts>(*key, None)
                .map_err(StateError::Database)?;
        }

        let storage_keys =
            Self::collect_keys_in_namespace::<NamespacedStorage, NamespacedStorageKey>(
                tx,
                NamespacedStorageKey::new(ns, AddressHash::default(), B256::ZERO),
                |k| k.namespace_idx == ns,
            )?;
        for key in &storage_keys {
            tx.delete::<NamespacedStorage>(*key, None)
                .map_err(StateError::Database)?;
        }

        let bytecode_keys = Self::collect_keys_in_namespace::<Bytecodes, NamespacedBytecodeKey>(
            tx,
            NamespacedBytecodeKey::new(ns, B256::ZERO),
            |k| k.namespace_idx == ns,
        )?;
        for key in &bytecode_keys {
            tx.delete::<Bytecodes>(*key, None)
                .map_err(StateError::Database)?;
        }

        debug!(
            namespace = ns,
            accounts = account_keys.len(),
            storage = storage_keys.len(),
            bytecodes = bytecode_keys.len(),
            "cleared namespace for rebuild"
        );

        Ok(())
    }

    /// Collect all keys from a table starting at `start_key` while `in_namespace`
    /// holds. Works with both `RO` and `RW` transactions.
    fn collect_keys_in_namespace<T, K>(
        tx: &impl DbTx,
        start_key: K,
        in_namespace: impl Fn(&K) -> bool,
    ) -> StateResult<Vec<K>>
    where
        T: reth_db_api::table::Table<Key = K>,
        K: Copy + std::fmt::Debug,
    {
        let mut cursor = tx.cursor_read::<T>().map_err(StateError::Database)?;
        let mut keys = Vec::new();

        if let Some((key, _)) = cursor.seek(start_key).map_err(StateError::Database)?
            && in_namespace(&key)
        {
            keys.push(key);
            while let Some((key, _)) = cursor.next().map_err(StateError::Database)? {
                if !in_namespace(&key) {
                    break;
                }
                keys.push(key);
            }
        }

        Ok(keys)
    }
}

impl CleanupTask {
    /// Delete all account, storage, and bytecode data for a single namespace.
    /// Each table is cleaned in its own write transaction.
    fn cleanup_single_namespace(db: &StateDb, ns: u8) -> StateResult<CleanupStats> {
        let accounts_deleted = Self::delete_namespace_keys::<NamespacedAccounts, _>(
            db,
            NamespacedAccountKey::new(ns, AddressHash::default()),
            |k| k.namespace_idx == ns,
        )?;

        let storage_slots_deleted = Self::delete_namespace_keys::<NamespacedStorage, _>(
            db,
            NamespacedStorageKey::new(ns, AddressHash::default(), B256::ZERO),
            |k| k.namespace_idx == ns,
        )?;

        let bytecodes_deleted = Self::delete_namespace_keys::<Bytecodes, _>(
            db,
            NamespacedBytecodeKey::new(ns, B256::ZERO),
            |k| k.namespace_idx == ns,
        )?;

        Ok(CleanupStats {
            accounts_deleted,
            storage_slots_deleted,
            bytecodes_deleted,
        })
    }

    /// Collect all keys in a namespace from a read transaction, then delete
    /// them in a write transaction. Returns the number of keys deleted.
    fn delete_namespace_keys<T, K>(
        db: &StateDb,
        start_key: K,
        in_namespace: impl Fn(&K) -> bool,
    ) -> StateResult<usize>
    where
        T: reth_db_api::table::Table<Key = K>,
        K: Copy + std::fmt::Debug,
    {
        let keys = {
            let tx = db.tx()?;
            StateWriter::collect_keys_in_namespace::<T, K>(&tx, start_key, in_namespace)?
        };

        if keys.is_empty() {
            return Ok(0);
        }

        let tx = db.tx_mut()?;
        for key in &keys {
            tx.delete::<T>(*key, None).map_err(StateError::Database)?;
        }
        tx.commit()
            .map_err(|e| StateError::CommitFailed(e.to_string()))?;

        Ok(keys.len())
    }
}

// ============================================================================
// Unit tests for compute_target_block
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_target_block_basic() {
        // buffer_size=3 → 2, latest=102
        // ns 0: 102 % 2 = 0 → target 102
        // ns 1: 101 % 2 = 1 → target 101
        assert_eq!(StateWriter::compute_target_block(102, 2, 0), Some(102));
        assert_eq!(StateWriter::compute_target_block(102, 2, 1), Some(101));
    }

    #[test]
    fn test_compute_target_block_odd_latest() {
        // latest=103, new_size=2
        // ns 0: target = 102 (102 % 2 = 0)
        // ns 1: target = 103 (103 % 2 = 1)
        assert_eq!(StateWriter::compute_target_block(103, 2, 0), Some(102));
        assert_eq!(StateWriter::compute_target_block(103, 2, 1), Some(103));
    }

    #[test]
    fn test_compute_target_block_size_one() {
        // new_size=1: everything maps to ns 0
        assert_eq!(StateWriter::compute_target_block(102, 1, 0), Some(102));
        assert_eq!(StateWriter::compute_target_block(0, 1, 0), Some(0));
    }

    #[test]
    fn test_compute_target_block_latest_less_than_ns() {
        // latest=0, new_size=2, ns=1 → no block with B%2==1 exists
        assert_eq!(StateWriter::compute_target_block(0, 2, 1), None);
        assert_eq!(StateWriter::compute_target_block(0, 2, 0), Some(0));
    }

    #[test]
    fn test_compute_target_block_large_reduction() {
        // buffer_size=5 → 2, latest=204
        // ns 0: target = 204 (204 % 2 = 0)
        // ns 1: target = 203 (203 % 2 = 1)
        assert_eq!(StateWriter::compute_target_block(204, 2, 0), Some(204));
        assert_eq!(StateWriter::compute_target_block(204, 2, 1), Some(203));
    }
}
