//! State writer implementation for persisting blockchain state to Redis.
//!
//! This module implements a chunked commit strategy protected by namespace
//! write locks to handle large state updates without blocking Redis for extended periods.

use crate::{
    CircularBufferConfig,
    common::{
        AccountState,
        BlockStateUpdate,
        ChunkedWriteConfig,
        NamespaceLock,
        RedisStateClient,
        encode_b256,
        encode_bytes,
        encode_u256,
        ensure_state_dump_indices,
        error::{
            StateError,
            StateResult,
        },
        get_account_key,
        get_block_hash_key,
        get_block_key,
        get_code_key,
        get_diff_key,
        get_namespace_for_block,
        get_state_root_key,
        get_storage_key,
        get_write_lock_key,
        read_latest_block_number,
        read_namespace_block_number,
        read_namespace_lock,
        update_metadata_in_pipe,
    },
};
use alloy::primitives::B256;
use uuid::Uuid;

/// Information about a recovered stale lock.
#[derive(Debug, Clone)]
pub struct StaleLockRecovery {
    /// The namespace that had the stale lock
    pub namespace: String,
    /// The block number that was being written when the crash occurred
    pub target_block: u64,
    /// The writer ID that held the lock
    pub writer_id: String,
    /// When the lock was acquired (unix timestamp)
    pub started_at: i64,
    /// The block number the namespace had before the failed write (if any)
    pub previous_block: Option<u64>,
}

/// Thin wrapper that writes account/state data into Redis using a circular buffer
/// approach to maintain multiple historical states.
///
/// Uses chunked commits with write locks to handle large state updates efficiently
/// while maintaining consistency for readers.
#[derive(Clone)]
pub struct StateWriter {
    client: RedisStateClient,
    chunked_config: ChunkedWriteConfig,
    writer_id: String,
}

impl StateWriter {
    /// Build a new writer with circular buffer support.
    pub fn new(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
    ) -> StateResult<Self> {
        Self::with_chunked_config(
            redis_url,
            base_namespace,
            buffer_config,
            ChunkedWriteConfig::default(),
        )
    }

    /// Repair a corrupted namespace by:
    /// 1. Copying cumulative state from a valid source namespace
    /// 2. Applying diffs to reach the target block
    ///
    /// This ensures we never build state on top of corrupted data.
    ///
    /// Example: If namespace 0 should have block 100 but is corrupted:
    /// - Copy from namespace 2 (which has valid state at block 98)
    /// - Apply diff for block 99
    /// - Apply diff for block 100
    /// - Result: namespace 0 has correct cumulative state at block 100
    pub fn repair_namespace_from_valid_state(
        &self,
        target_namespace_idx: usize,
        source_namespace_idx: usize,
        target_block: u64,
    ) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let chunk_size = self.chunked_config.chunk_size;

        if target_namespace_idx >= buffer_size || source_namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                target_namespace_idx.max(source_namespace_idx) as u64,
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            repair_namespace_with_diffs(
                conn,
                &base_namespace,
                source_namespace_idx,
                target_namespace_idx,
                target_block,
                buffer_size,
                chunk_size,
            )
        })
    }

    /// Build a new writer with custom chunked write configuration.
    pub fn with_chunked_config(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
        chunked_config: ChunkedWriteConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace.to_string(), buffer_config)?;
        let writer_id = Uuid::new_v4().to_string();
        Ok(Self {
            client,
            chunked_config,
            writer_id,
        })
    }

    /// Read the most recently persisted block number from Redis metadata.
    pub fn latest_block_number(&self) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        self.client
            .with_connection(move |conn| read_latest_block_number(conn, &base_namespace))
    }

    /// Persist all account mutations for the block using chunked commits with locking.
    ///
    /// Strategy:
    /// 1. Acquire write lock on target namespace
    /// 2. Apply intermediate diffs in chunks (if needed for circular buffer rotation)
    /// 3. Apply current block's state changes in chunks
    /// 4. Finalize metadata and release lock
    pub fn commit_block(&self, update: BlockStateUpdate) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let chunked_config = self.chunked_config.clone();
        let writer_id = self.writer_id.clone();

        self.client.with_connection(move |conn| {
            commit_block_chunked(
                conn,
                &base_namespace,
                buffer_size,
                &chunked_config,
                &writer_id,
                &update,
            )
        })
    }

    /// Ensure the Redis metadata matches the configured namespace rotation size.
    pub fn ensure_dump_index_metadata(&self) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client.with_connection(move |conn| {
            ensure_state_dump_indices(conn, &base_namespace, buffer_size)
        })
    }

    /// Get the unique writer ID for this instance.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Get the buffer size for this writer.
    pub fn buffer_size(&self) -> usize {
        self.client.buffer_config.buffer_size
    }

    /// Check for and recover from stale locks on all namespaces.
    ///
    /// Should be called during startup before processing blocks. For each stale lock:
    /// - If the state can be repaired (diffs exist): completes the write, releases lock
    /// - If repair fails (missing diffs): returns error, lock remains in place
    ///
    /// The lock is NEVER released until the state is consistent, ensuring readers
    /// cannot access corrupt data.
    ///
    /// Returns information about successfully recovered locks.
    pub fn recover_stale_locks(&self) -> StateResult<Vec<StaleLockRecovery>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let stale_timeout = self.chunked_config.stale_lock_timeout_secs;
        let chunk_size = self.chunked_config.chunk_size;

        self.client.with_connection(move |conn| {
            recover_all_stale_locks(
                conn,
                &base_namespace,
                buffer_size,
                stale_timeout,
                chunk_size,
            )
        })
    }

    /// Force recovery of a specific namespace by repairing its state and clearing the lock.
    ///
    /// This will attempt to complete the interrupted write. If the required diffs
    /// are not available, returns an error and leaves the lock in place.
    pub fn force_recover_namespace(
        &self,
        namespace_idx: usize,
    ) -> StateResult<Option<StaleLockRecovery>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let stale_timeout = self.chunked_config.stale_lock_timeout_secs;
        let chunk_size = self.chunked_config.chunk_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                namespace_idx as u64,
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            let namespace = format!("{base_namespace}:{namespace_idx}");
            recover_namespace_lock(
                conn,
                &base_namespace,
                &namespace,
                buffer_size,
                stale_timeout,
                chunk_size,
            )
        })
    }

    /// Store only the state diff without committing the block.
    /// Used during recovery when we need to fetch missing diffs.
    pub fn store_diff_only(&self, update: &BlockStateUpdate) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        let update = update.clone();

        self.client.with_connection(move |conn| {
            store_state_diff(conn, &base_namespace, &update, buffer_size)
        })
    }

    /// Check if a state diff exists for a given block number.
    pub fn has_diff(&self, block_number: u64) -> StateResult<bool> {
        let base_namespace = self.client.base_namespace.clone();

        self.client.with_connection(move |conn| {
            let diff_key = get_diff_key(&base_namespace, block_number);
            let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(conn)?;
            Ok(exists)
        })
    }

    /// Find the first valid (unlocked, consistent) namespace and return its index and block number.
    /// Returns None if no valid namespace is found.
    pub fn find_valid_namespace_state(&self) -> StateResult<Option<(usize, u64)>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        self.client
            .with_connection(move |conn| find_valid_namespace(conn, &base_namespace, buffer_size))
    }

    /// Get the current block number for a specific namespace.
    pub fn get_namespace_block(&self, namespace_idx: usize) -> StateResult<Option<u64>> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                namespace_idx as u64,
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            let namespace = format!("{base_namespace}:{namespace_idx}");
            read_namespace_block_number(conn, &namespace)
        })
    }

    /// Calculate the expected block number for a namespace given the highest known valid block.
    /// With circular buffer, namespace N should have block (`highest_block` - `offset`) where
    /// offset depends on the namespace index relative to the highest block's namespace.
    pub fn expected_block_for_namespace(
        &self,
        namespace_idx: usize,
        highest_valid_block: u64,
    ) -> u64 {
        let buffer_size = self.client.buffer_config.buffer_size as u64;
        let highest_ns_idx = highest_valid_block % buffer_size;
        let ns_idx = namespace_idx as u64;

        if ns_idx <= highest_ns_idx {
            // This namespace should have a block from the same "round"
            highest_valid_block - (highest_ns_idx - ns_idx)
        } else {
            // This namespace should have a block from the previous "round"
            if highest_valid_block >= (ns_idx - highest_ns_idx) {
                highest_valid_block - (buffer_size - (ns_idx - highest_ns_idx))
            } else {
                // Edge case: we're near the start, namespace hasn't been written yet
                ns_idx
            }
        }
    }

    /// Reset a namespace by copying state from another valid namespace.
    /// This clears the target namespace and copies all data from the source.
    ///
    /// WARNING: This copies the source namespace's block number too, which may not
    /// be correct for the target namespace. Use `write_full_state_to_namespace`
    /// instead when you need to set a specific block's state.
    pub fn reset_namespace_from(
        &self,
        target_namespace_idx: usize,
        source_namespace_idx: usize,
    ) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let chunk_size = self.chunked_config.chunk_size;

        if target_namespace_idx >= buffer_size || source_namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                target_namespace_idx.max(source_namespace_idx) as u64,
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            copy_namespace_state(
                conn,
                &base_namespace,
                source_namespace_idx,
                target_namespace_idx,
                chunk_size,
            )
        })
    }

    /// Write a complete `BlockStateUpdate` to a specific namespace.
    /// This clears the namespace first, then writes the full state.
    /// Used during repair to write the correct state for a namespace's expected block.
    pub fn write_full_state_to_namespace(
        &self,
        namespace_idx: usize,
        update: &BlockStateUpdate,
    ) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;
        let chunk_size = self.chunked_config.chunk_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                namespace_idx as u64,
                buffer_size,
            ));
        }

        let update = update.clone();

        self.client.with_connection(move |conn| {
            let namespace = format!("{base_namespace}:{namespace_idx}");

            // Clear the namespace first
            clear_namespace(conn, &namespace)?;

            // Write all accounts
            let accounts: Vec<&AccountState> = update.accounts.iter().collect();
            for chunk in accounts.chunks(chunk_size) {
                write_account_chunk(conn, &namespace, chunk)?;
            }

            // Write block metadata
            write_block_metadata(
                conn,
                &namespace,
                &base_namespace,
                update.block_number,
                update.block_hash,
                update.state_root,
                buffer_size,
            )?;

            Ok(())
        })
    }

    /// Force release a lock on a namespace (use with caution - only for manual recovery).
    pub fn force_release_lock(&self, namespace_idx: usize) -> StateResult<()> {
        let base_namespace = self.client.base_namespace.clone();
        let buffer_size = self.client.buffer_config.buffer_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                namespace_idx as u64,
                buffer_size,
            ));
        }

        self.client.with_connection(move |conn| {
            let namespace = format!("{base_namespace}:{namespace_idx}");
            let lock_key = get_write_lock_key(&namespace);
            redis::cmd("DEL").arg(&lock_key).query::<()>(conn)?;
            Ok(())
        })
    }
}

/// Find a valid (unlocked, with data) namespace.
fn find_valid_namespace<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
) -> StateResult<Option<(usize, u64)>>
where
    C: redis::ConnectionLike,
{
    let mut best: Option<(usize, u64)> = None;

    for idx in 0..buffer_size {
        let namespace = format!("{base_namespace}:{idx}");

        // Check if namespace is locked
        if read_namespace_lock(conn, &namespace)?.is_some() {
            continue;
        }

        // Get the block number for this namespace
        if let Some(block_num) = read_namespace_block_number(conn, &namespace)? {
            // Verify the namespace has actual data (not just a block number)
            let block_key = get_block_key(&namespace);
            let exists: bool = redis::cmd("EXISTS").arg(&block_key).query(conn)?;

            if exists {
                // Stronger validation: ensure the namespace contains at least one account key.
                // This avoids selecting a partially written namespace as the recovery base.
                //
                // FIX: Use a proper SCAN loop instead of single scan with COUNT=1.
                // SCAN is an iterator - we must loop until cursor returns to 0.
                let account_pattern = format!("{namespace}:account:*");
                let mut cursor = 0u64;
                let mut has_accounts = false;

                loop {
                    let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&account_pattern)
                        .arg("COUNT")
                        .arg(100) // Scan more slots per iteration for efficiency
                        .query(conn)?;

                    if !keys.is_empty() {
                        has_accounts = true;
                        break; // Found at least one account, no need to continue
                    }

                    cursor = new_cursor;
                    if cursor == 0 {
                        break; // Completed full scan, no accounts found
                    }
                }

                if has_accounts {
                    // Keep track of the highest valid block
                    match best {
                        None => best = Some((idx, block_num)),
                        Some((_, best_block)) if block_num > best_block => {
                            best = Some((idx, block_num));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(best)
}

/// Copy all state from one namespace to another.
/// This is used to rebuild corrupted namespaces from valid ones.
#[allow(clippy::too_many_lines)]
fn copy_namespace_state<C>(
    conn: &mut C,
    base_namespace: &str,
    source_idx: usize,
    target_idx: usize,
    chunk_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let source_namespace = format!("{base_namespace}:{source_idx}");
    let target_namespace = format!("{base_namespace}:{target_idx}");

    // First, clear the target namespace (delete all keys with this prefix)
    clear_namespace(conn, &target_namespace)?;

    // Scan and copy all account keys
    let account_pattern = format!("{source_namespace}:account:*");
    let mut cursor = 0u64;
    let mut keys_to_copy = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&account_pattern)
            .arg("COUNT")
            .arg(100)
            .query(conn)?;

        keys_to_copy.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    // Copy account data in chunks
    for chunk in keys_to_copy.chunks(chunk_size) {
        let mut pipe = redis::pipe();

        for source_key in chunk {
            // Get the account hash from the key
            let suffix = source_key
                .strip_prefix(&format!("{source_namespace}:account:"))
                .unwrap_or(source_key);
            let target_key = format!("{target_namespace}:account:{suffix}");

            // Copy the hash data
            let data: std::collections::HashMap<String, String> =
                redis::cmd("HGETALL").arg(source_key).query(conn)?;

            if !data.is_empty() {
                let fields: Vec<(&str, &str)> =
                    data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                pipe.hset_multiple(&target_key, &fields);
            }
        }

        pipe.query::<()>(conn)?;
    }

    // Copy storage keys
    let storage_pattern = format!("{source_namespace}:storage:*");
    cursor = 0;
    keys_to_copy.clear();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&storage_pattern)
            .arg("COUNT")
            .arg(100)
            .query(conn)?;

        keys_to_copy.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    for chunk in keys_to_copy.chunks(chunk_size) {
        let mut pipe = redis::pipe();

        for source_key in chunk {
            let suffix = source_key
                .strip_prefix(&format!("{source_namespace}:storage:"))
                .unwrap_or(source_key);
            let target_key = format!("{target_namespace}:storage:{suffix}");

            let data: std::collections::HashMap<String, String> =
                redis::cmd("HGETALL").arg(source_key).query(conn)?;

            if !data.is_empty() {
                let fields: Vec<(&str, &str)> =
                    data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                pipe.hset_multiple(&target_key, &fields);
            }
        }

        pipe.query::<()>(conn)?;
    }

    // Copy code keys
    let code_pattern = format!("{source_namespace}:code:*");
    cursor = 0;
    keys_to_copy.clear();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&code_pattern)
            .arg("COUNT")
            .arg(100)
            .query(conn)?;

        keys_to_copy.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    for chunk in keys_to_copy.chunks(chunk_size) {
        let mut pipe = redis::pipe();

        for source_key in chunk {
            let suffix = source_key
                .strip_prefix(&format!("{source_namespace}:code:"))
                .unwrap_or(source_key);
            let target_key = format!("{target_namespace}:code:{suffix}");

            let data: Option<String> = redis::cmd("GET").arg(source_key).query(conn)?;

            if let Some(code) = data {
                pipe.set(&target_key, code);
            }
        }

        pipe.query::<()>(conn)?;
    }

    // Copy the block number (this makes the namespace "valid")
    let source_block: Option<String> = redis::cmd("GET")
        .arg(get_block_key(&source_namespace))
        .query(conn)?;

    if let Some(block) = source_block {
        redis::cmd("SET")
            .arg(get_block_key(&target_namespace))
            .arg(block)
            .query::<()>(conn)?;
    }

    Ok(())
}

/// Clear all keys in a namespace.
fn clear_namespace<C>(conn: &mut C, namespace: &str) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let pattern = format!("{namespace}:*");
    let mut cursor = 0u64;

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100)
            .query(conn)?;

        if !keys.is_empty() {
            redis::cmd("DEL").arg(&keys).query::<()>(conn)?;
        }

        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    Ok(())
}

/// Repair a namespace by copying valid state and applying diffs.
///
/// This is the SAFE way to repair - we never trust corrupted state.
fn repair_namespace_with_diffs<C>(
    conn: &mut C,
    base_namespace: &str,
    source_idx: usize,
    target_idx: usize,
    target_block: u64,
    buffer_size: usize,
    chunk_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let source_namespace = format!("{base_namespace}:{source_idx}");
    let target_namespace = format!("{base_namespace}:{target_idx}");

    // Step 1: Get the source namespace's block number
    let source_block = read_namespace_block_number(conn, &source_namespace)?
        .ok_or_else(|| StateError::Other("Source namespace has no block number".into()))?;

    // Sanity check: source should be BEFORE target
    if source_block >= target_block {
        return Err(StateError::Other(format!(
            "Source block {source_block} must be before target block {target_block}",
        )));
    }

    // Step 2: Verify all required diffs exist BEFORE we start modifying state
    for block_num in (source_block + 1)..=target_block {
        let diff_key = get_diff_key(base_namespace, block_num);
        let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(conn)?;
        if !exists {
            return Err(StateError::MissingStateDiff {
                needed_block: block_num,
                target_block,
            });
        }
    }

    // Step 3: Copy cumulative state from source namespace
    // This gives us a known-good base state
    copy_namespace_state(conn, base_namespace, source_idx, target_idx, chunk_size)?;

    // Step 4: Apply diffs sequentially to reach target block
    for block_num in (source_block + 1)..=target_block {
        let diff_key = get_diff_key(base_namespace, block_num);
        let diff_json: String = redis::cmd("GET").arg(&diff_key).query(conn)?;
        let diff = deserialize_state_diff(&diff_json, block_num)?;

        // Apply account changes
        let accounts: Vec<&AccountState> = diff.accounts.iter().collect();
        for chunk in accounts.chunks(chunk_size) {
            write_account_chunk(conn, &target_namespace, chunk)?;
        }
    }

    // Step 5: Update metadata to reflect the target block
    // We need to fetch the target block's hash and state root from the diff
    let target_diff_key = get_diff_key(base_namespace, target_block);
    let target_diff_json: String = redis::cmd("GET").arg(&target_diff_key).query(conn)?;
    let target_diff = deserialize_state_diff(&target_diff_json, target_block)?;

    write_block_metadata(
        conn,
        &target_namespace,
        base_namespace,
        target_block,
        target_diff.block_hash,
        target_diff.state_root,
        buffer_size,
    )?;

    Ok(())
}

/// Recover all stale locks across all namespaces.
///
/// For each stale lock found, attempts to repair the state before releasing the lock.
/// If any recovery fails, the error is returned and remaining namespaces are not processed.
fn recover_all_stale_locks<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    stale_timeout_secs: i64,
    chunk_size: usize,
) -> StateResult<Vec<StaleLockRecovery>>
where
    C: redis::ConnectionLike,
{
    let mut recoveries = Vec::new();

    for idx in 0..buffer_size {
        let namespace = format!("{base_namespace}:{idx}");

        if let Some(recovery) = recover_namespace_lock(
            conn,
            base_namespace,
            &namespace,
            buffer_size,
            stale_timeout_secs,
            chunk_size,
        )? {
            recoveries.push(recovery);
        }
    }

    Ok(recoveries)
}

/// Check and recover a single namespace's stale lock.
///
/// Recovery process:
/// 1. Check if the lock exists and is stale
/// 2. If the namespace block already equals to the target block, then the state is consistent
/// 3. Otherwise, re-apply all diffs from (`current_block` + 1) to `target_block`
/// 4. Only release lock after the state is fully repaired
///
/// If any required diff is missing, returns an error and DOES NOT release the lock,
/// ensuring readers cannot access an inconsistent state.
fn recover_namespace_lock<C>(
    conn: &mut C,
    base_namespace: &str,
    namespace: &str,
    buffer_size: usize,
    stale_timeout_secs: i64,
    chunk_size: usize,
) -> StateResult<Option<StaleLockRecovery>>
where
    C: redis::ConnectionLike,
{
    // Check if there's a stale lock
    let lock = match read_namespace_lock(conn, namespace)? {
        Some(lock) if lock.is_stale(stale_timeout_secs) => lock,
        _ => return Ok(None),
    };

    let current_block = read_namespace_block_number(conn, namespace)?;
    let target_block = lock.target_block;

    // Case 1: Write already completed (crash happened after metadata update but before lock release)
    // State is consistent, just release the lock
    if current_block == Some(target_block) {
        let lock_key = get_write_lock_key(namespace);
        redis::cmd("DEL").arg(&lock_key).query::<()>(conn)?;

        return Ok(Some(StaleLockRecovery {
            namespace: namespace.to_string(),
            target_block,
            writer_id: lock.writer_id,
            started_at: lock.started_at,
            previous_block: current_block,
        }));
    }

    // Case 2: Write did not complete, so need to repair by re-applying diffs
    let start_block = current_block.map_or(0, |b| b + 1);

    for block_num in start_block..=target_block {
        let diff_key = get_diff_key(base_namespace, block_num);
        let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

        match diff_json {
            Some(json) => {
                let diff = deserialize_state_diff(&json, block_num)?;
                let accounts: Vec<&AccountState> = diff.accounts.iter().collect();

                // Apply account changes in chunks
                for chunk in accounts.chunks(chunk_size) {
                    write_account_chunk(conn, namespace, chunk)?;
                }

                // If this is the target block, finalize with metadata
                if block_num == target_block {
                    write_block_metadata(
                        conn,
                        namespace,
                        base_namespace,
                        target_block,
                        diff.block_hash,
                        diff.state_root,
                        buffer_size,
                    )?;
                }
            }
            None => {
                // Cannot complete recovery
                // DO NOT release the lock
                return Err(StateError::MissingStateDiff {
                    needed_block: block_num,
                    target_block,
                });
            }
        }
    }

    // State is now consistent
    let lock_key = get_write_lock_key(namespace);
    redis::cmd("DEL").arg(&lock_key).query::<()>(conn)?;

    Ok(Some(StaleLockRecovery {
        namespace: namespace.to_string(),
        target_block,
        writer_id: lock.writer_id,
        started_at: lock.started_at,
        previous_block: current_block,
    }))
}

/// Deserialize a state diff from JSON.
fn deserialize_state_diff(json: &str, block_number: u64) -> StateResult<BlockStateUpdate> {
    serde_json::from_str(json).map_err(|e| StateError::DeserializeDiff(block_number, e))
}

/// Serialize state diff for storage.
pub(crate) fn serialize_state_diff(update: &BlockStateUpdate) -> StateResult<String> {
    serde_json::to_string(update).map_err(|e| StateError::SerializeDiff(update.block_number, e))
}

/// Acquire write lock for a namespace using SET NX (set if not exists).
///
/// Returns Ok(()) if lock was acquired, Err if:
/// - Lock is held by another active writer (`LockAcquisitionFailed`)
/// - Lock is stale from a crashed writer (`StaleLockDetected`)
fn acquire_write_lock<C>(
    conn: &mut C,
    namespace: &str,
    lock: &NamespaceLock,
    stale_timeout_secs: i64,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let lock_key = get_write_lock_key(namespace);

    // Check for existing lock
    if let Some(existing) = read_namespace_lock(conn, namespace)? {
        if existing.is_stale(stale_timeout_secs) {
            return Err(StateError::StaleLockDetected {
                namespace: namespace.to_string(),
                writer_id: existing.writer_id,
                started_at: existing.started_at,
            });
        }
        return Err(StateError::LockAcquisitionFailed {
            namespace: namespace.to_string(),
            existing_writer: existing.writer_id,
        });
    }

    // Set the lock atomically
    let lock_json = lock.to_json()?;
    let result: Option<String> = redis::cmd("SET")
        .arg(&lock_key)
        .arg(&lock_json)
        .arg("NX")
        .query(conn)?;

    if result.is_some() {
        Ok(())
    } else {
        Err(StateError::LockAcquisitionFailed {
            namespace: namespace.to_string(),
            existing_writer: "unknown".to_string(),
        })
    }
}

/// Release the write lock for a namespace.
/// Only releases if we still own the lock.
fn release_write_lock<C>(conn: &mut C, namespace: &str, writer_id: &str) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let lock_key = get_write_lock_key(namespace);

    // Use WATCH for optimistic locking
    redis::cmd("WATCH").arg(&lock_key).query::<()>(conn)?;

    // Read the current lock
    let existing_json: Option<String> = redis::cmd("GET").arg(&lock_key).query(conn)?;

    // Only delete it if we own it
    let should_delete = match existing_json {
        Some(json) => {
            match NamespaceLock::from_json(&json) {
                Ok(existing) => existing.writer_id == writer_id,
                Err(_) => false,
            }
        }
        None => false,
    };

    if should_delete {
        let _: Option<()> = redis::pipe().atomic().del(&lock_key).query(conn)?;
    } else {
        redis::cmd("UNWATCH").query::<()>(conn)?;
    }

    Ok(())
}

/// Write a chunk of accounts to Redis.
fn write_account_chunk<C>(
    conn: &mut C,
    namespace: &str,
    accounts: &[&AccountState],
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    if accounts.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    for account in accounts {
        write_account_to_pipe(&mut pipe, namespace, account);
    }
    pipe.query::<()>(conn)?;
    Ok(())
}

/// Write account data to a namespace within a pipeline.
fn write_account_to_pipe(pipe: &mut redis::Pipeline, namespace: &str, account: &AccountState) {
    let account_key = get_account_key(namespace, &account.address_hash);

    let balance = account.balance.to_string();
    let nonce = account.nonce.to_string();
    let code_hash = encode_b256(account.code_hash);

    pipe.hset_multiple(
        &account_key,
        &[
            ("balance", balance.as_str()),
            ("nonce", nonce.as_str()),
            ("code_hash", code_hash.as_str()),
        ],
    );

    if let Some(code) = &account.code {
        let code_hash_hex = hex::encode(account.code_hash);
        let code_key = get_code_key(namespace, &code_hash_hex);
        let code_hex = encode_bytes(code);
        pipe.set(&code_key, code_hex);
    }

    if !account.storage.is_empty() || account.deleted {
        let storage_key = get_storage_key(namespace, &account.address_hash);
        for (slot, value) in &account.storage {
            let slot_hash = B256::from(slot.to_be_bytes::<32>());
            let slot_hex = encode_b256(slot_hash);
            if value.is_zero() {
                pipe.hdel(&storage_key, slot_hex);
            } else {
                let value_hex = encode_u256(*value);
                pipe.hset(&storage_key, slot_hex, value_hex);
            }
        }
    }
}

/// Write block metadata and finalize the commit.
fn write_block_metadata<C>(
    conn: &mut C,
    namespace: &str,
    base_namespace: &str,
    block_number: u64,
    block_hash: B256,
    state_root: B256,
    buffer_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let mut pipe = redis::pipe();
    pipe.atomic();

    let block_key = get_block_key(namespace);
    pipe.set(&block_key, block_number.to_string());

    let block_hash_key = get_block_hash_key(base_namespace, block_number);
    pipe.set(&block_hash_key, encode_b256(block_hash));

    let state_root_key = get_state_root_key(base_namespace, block_number);
    pipe.set(&state_root_key, encode_b256(state_root));

    update_metadata_in_pipe(&mut pipe, base_namespace, block_number, buffer_size);

    pipe.query::<()>(conn)?;
    Ok(())
}

/// Store the state diff for a block.
fn store_state_diff<C>(
    conn: &mut C,
    base_namespace: &str,
    update: &BlockStateUpdate,
    buffer_size: usize,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let mut pipe = redis::pipe();

    let diff_key = get_diff_key(base_namespace, update.block_number);
    let diff_data = serialize_state_diff(update)?;
    pipe.set(&diff_key, diff_data);

    // Delete old state diff
    if update.block_number >= buffer_size as u64 {
        let old_block = update.block_number - buffer_size as u64;
        let old_diff_key = get_diff_key(base_namespace, old_block);
        pipe.del(&old_diff_key);
    }

    pipe.query::<()>(conn)?;
    Ok(())
}

/// Commit a block using chunked writes with locking.
pub(crate) fn commit_block_chunked<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    chunked_config: &ChunkedWriteConfig,
    writer_id: &str,
    update: &BlockStateUpdate,
) -> StateResult<()>
where
    C: redis::ConnectionLike,
{
    let block_number = update.block_number;
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let current_block = read_namespace_block_number(conn, &namespace)?;

    // Create and acquire lock
    let lock = NamespaceLock::new(block_number, writer_id.to_string());
    acquire_write_lock(
        conn,
        &namespace,
        &lock,
        chunked_config.stale_lock_timeout_secs,
    )?;

    // Use closure to ensure the lock is released on any error
    let result = (|| -> StateResult<()> {
        let start_block = current_block.map_or(0, |old| old + 1);

        // Phase 1: Apply intermediate diffs if there's a gap
        if block_number > start_block {
            for intermediate_block in start_block..block_number {
                let diff_key = get_diff_key(base_namespace, intermediate_block);
                let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

                if let Some(json) = diff_json {
                    let diff = deserialize_state_diff(&json, intermediate_block)?;
                    let accounts: Vec<&AccountState> = diff.accounts.iter().collect();
                    for chunk in accounts.chunks(chunked_config.chunk_size) {
                        write_account_chunk(conn, &namespace, chunk)?;
                    }
                } else {
                    return Err(StateError::MissingStateDiff {
                        needed_block: intermediate_block,
                        target_block: block_number,
                    });
                }
            }
        }

        // Phase 2: Write current block's accounts in chunks
        let accounts: Vec<&AccountState> = update.accounts.iter().collect();
        for chunk in accounts.chunks(chunked_config.chunk_size) {
            write_account_chunk(conn, &namespace, chunk)?;
        }

        // Phase 3: Finalize
        write_block_metadata(
            conn,
            &namespace,
            base_namespace,
            block_number,
            update.block_hash,
            update.state_root,
            buffer_size,
        )?;

        store_state_diff(conn, base_namespace, update, buffer_size)?;

        Ok(())
    })();

    // Always release lock
    release_write_lock(conn, &namespace, writer_id)?;

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        U256,
    };

    #[test]
    fn test_serialize_deserialize_state_diff() {
        let update = BlockStateUpdate {
            block_number: 42,
            block_hash: B256::from([1u8; 32]),
            state_root: B256::from([2u8; 32]),
            accounts: vec![AccountState {
                address_hash: Address::from([3u8; 20]).into(),
                balance: U256::from(1000u64),
                nonce: 5,
                code_hash: B256::from([4u8; 32]),
                code: Some(vec![0x60, 0x80]),
                storage: std::collections::HashMap::new(),
                deleted: false,
            }],
        };

        let serialized = serialize_state_diff(&update).unwrap();
        let deserialized = deserialize_state_diff(&serialized, 42).unwrap();

        assert_eq!(deserialized.block_number, 42);
        assert_eq!(deserialized.accounts.len(), 1);
        assert_eq!(deserialized.accounts[0].nonce, 5);
    }

    #[test]
    fn test_stale_lock_recovery_struct() {
        let recovery = StaleLockRecovery {
            namespace: "chain:0".to_string(),
            target_block: 100,
            writer_id: "test-writer".to_string(),
            started_at: 1_234_567_890,
            previous_block: Some(97),
        };

        assert_eq!(recovery.namespace, "chain:0");
        assert_eq!(recovery.target_block, 100);
        assert_eq!(recovery.previous_block, Some(97));
    }
}
