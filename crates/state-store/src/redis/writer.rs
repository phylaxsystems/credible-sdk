//! State writer implementation for persisting blockchain state to Redis.
//!
//! This module implements a chunked commit strategy protected by namespace
//! write locks to handle large state updates without blocking Redis for extended periods.

use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    BlockStateUpdate,
    CommitStats,
    Reader,
    Writer,
    redis::{
        CircularBufferConfig,
        StateReader,
        common::{
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
            read_namespace_block_number,
            read_namespace_lock,
            update_metadata_in_pipe,
        },
    },
};
use alloy::primitives::{
    B256,
    Bytes,
    U256,
};
use std::{
    collections::HashMap,
    time::Instant,
};
use tracing::{
    Span,
    debug,
    instrument,
    trace,
    warn,
};
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
    reader: StateReader,
    chunked_config: ChunkedWriteConfig,
    writer_id: String,
}

impl Reader for StateWriter {
    type Error = StateError;

    /// Get the most recent block number from metadata (O(1) operation).
    /// Falls back to scanning all namespaces if metadata is unavailable.
    fn latest_block_number(&self) -> StateResult<Option<u64>> {
        self.reader.latest_block_number()
    }

    /// Check if a specific block number is available in the circular buffer.
    ///
    /// Note: This checks availability without considering locks.
    fn is_block_available(&self, block_number: u64) -> StateResult<bool> {
        self.reader.is_block_available(block_number)
    }

    /// Get account info without storage slots (balance, nonce, code hash, code only).
    /// This is the recommended method for most use cases to avoid large data transfers.
    /// Use `get_full_account` or `get_all_storage` separately if storage is needed.
    ///
    /// Returns an error if the namespace is locked for writing.
    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountInfo>> {
        self.reader.get_account(address_hash, block_number)
    }

    /// Get a specific storage slot for an account at a block.
    ///
    /// The `slot_hash` parameter should be the `keccak256` hash of the original 32-byte slot index,
    /// matching the format persisted by the writer (and Nethermind state dumps).
    ///
    /// Returns an error if the namespace is locked for writing.
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> StateResult<Option<U256>> {
        self.reader
            .get_storage(address_hash, slot_hash, block_number)
    }

    /// Get all storage slots for an account at a block.
    ///
    /// Returned map keys are `keccak256(slot)` digests corresponding to the original storage slots.
    ///
    /// Returns an error if the namespace is locked for writing.
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<HashMap<B256, U256>> {
        self.reader.get_all_storage(address_hash, block_number)
    }

    /// Get contract bytecode by code hash.
    ///
    /// Returns an error if the namespace is locked for writing.
    fn get_code(&self, code_hash: B256, block_number: u64) -> StateResult<Option<Bytes>> {
        self.reader.get_code(code_hash, block_number)
    }

    /// Get complete account state including storage.
    ///
    /// WARNING: This can transfer large amounts of data for contracts with many slots.
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> StateResult<Option<AccountState>> {
        self.reader.get_full_account(address_hash, block_number)
    }

    /// Get block hash for a specific block number.
    ///
    /// Note: Block hashes are stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_block_hash(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_block_hash(block_number)
    }

    /// Get state root for a specific block number.
    ///
    /// Note: State roots are stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_state_root(&self, block_number: u64) -> StateResult<Option<B256>> {
        self.reader.get_state_root(block_number)
    }

    /// Get complete block metadata (hash and state root).
    /// Optimized to fetch both in a single roundtrip.
    ///
    /// Note: Block metadata is stored globally, not per-namespace, so this does not
    /// check namespace locks.
    fn get_block_metadata(&self, block_number: u64) -> StateResult<Option<BlockMetadata>> {
        self.reader.get_block_metadata(block_number)
    }

    /// Get the range of available blocks [oldest, latest].
    fn get_available_block_range(&self) -> StateResult<Option<(u64, u64)>> {
        self.reader.get_available_block_range()
    }
}

impl Writer for StateWriter {
    type Error = StateError;

    /// Persist all account mutations for the block using chunked commits with locking.
    ///
    /// Strategy:
    /// 1. Acquire write lock on target namespace
    /// 2. Apply intermediate diffs in chunks (if needed for circular buffer rotation)
    /// 3. Apply current block's state changes in chunks
    /// 4. Finalize metadata and release lock
    ///
    /// Returns `CommitStats` with timing and count information for metrics.
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

        let client = self.reader.client();
        let base_namespace = client.base_namespace.clone();
        let buffer_size = client.buffer_config.buffer_size;
        let chunked_config = self.chunked_config.clone();
        let writer_id = self.writer_id.clone();

        // Convert crate-level AccountState to Redis AccountState
        let redis_update = RedisBlockStateUpdate {
            block_number: update.block_number,
            block_hash: update.block_hash,
            state_root: update.state_root,
            accounts: update
                .accounts
                .iter()
                .map(|a| {
                    AccountState {
                        address_hash: a.address_hash,
                        balance: a.balance,
                        nonce: a.nonce,
                        code_hash: a.code_hash,
                        code: a.code.clone(),
                        storage: a.storage.clone(),
                        deleted: a.deleted,
                    }
                })
                .collect(),
        };

        let stats = client.with_connection(move |conn| {
            commit_block_chunked(
                conn,
                &base_namespace,
                buffer_size,
                &chunked_config,
                &writer_id,
                &redis_update,
            )
        })?;

        let total_duration = total_start.elapsed();

        // Record final metrics on span
        Span::current().record(
            "total_ms",
            i64::try_from(total_duration.as_millis()).map_err(StateError::IntConversion)?,
        );

        debug!(
            total_ms = total_duration.as_millis(),
            preprocess_ms = stats.preprocess_duration.as_millis(),
            diff_apply_ms = stats.diff_application_duration.as_millis(),
            batch_write_ms = stats.batch_write_duration.as_millis(),
            commit_ms = stats.commit_duration.as_millis(),
            diffs_applied = stats.diffs_applied,
            accounts_written = stats.accounts_written,
            storage_written = stats.storage_slots_written,
            "block committed to redis"
        );

        Ok(stats)
    }

    /// Ensure the Redis metadata matches the configured namespace rotation size.
    fn ensure_dump_index_metadata(&self) -> StateResult<()> {
        let client = self.reader.client();
        let base_namespace = client.base_namespace.clone();
        let buffer_size = client.buffer_config.buffer_size;

        client.with_connection(move |conn| {
            ensure_state_dump_indices(conn, &base_namespace, buffer_size)
        })
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
    #[instrument(skip(self), level = "debug")]
    fn recover_stale_locks(&self) -> StateResult<Vec<StaleLockRecovery>> {
        let client = self.reader.client();
        let base_namespace = client.base_namespace.clone();
        let buffer_size = client.buffer_config.buffer_size;
        let stale_timeout = self.chunked_config.stale_lock_timeout_secs;
        let chunk_size = self.chunked_config.chunk_size;

        client.with_connection(move |conn| {
            recover_all_stale_locks(
                conn,
                &base_namespace,
                buffer_size,
                stale_timeout,
                chunk_size,
            )
        })
    }
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

    /// Build a new writer with custom chunked write configuration.
    pub fn with_chunked_config(
        redis_url: &str,
        base_namespace: &str,
        buffer_config: CircularBufferConfig,
        chunked_config: ChunkedWriteConfig,
    ) -> StateResult<Self> {
        let client = RedisStateClient::new(redis_url, base_namespace.to_string(), buffer_config)?;
        let reader = StateReader::from_client(client);
        let writer_id = Uuid::new_v4().to_string();
        Ok(Self {
            reader,
            chunked_config,
            writer_id,
        })
    }

    /// Get the unique writer ID for this instance.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Get a reference to the underlying reader.
    pub fn reader(&self) -> &StateReader {
        &self.reader
    }

    /// Force recovery of a specific namespace by repairing its state and clearing the lock.
    ///
    /// This will attempt to complete the interrupted write. If the required diffs
    /// are not available, returns an error and leaves the lock in place.
    #[instrument(skip(self), level = "debug")]
    pub fn force_recover_namespace(
        &self,
        namespace_idx: usize,
    ) -> StateResult<Option<StaleLockRecovery>> {
        let client = self.reader.client();
        let base_namespace = client.base_namespace.clone();
        let buffer_size = client.buffer_config.buffer_size;
        let stale_timeout = self.chunked_config.stale_lock_timeout_secs;
        let chunk_size = self.chunked_config.chunk_size;

        if namespace_idx >= buffer_size {
            return Err(StateError::InvalidNamespace(
                namespace_idx as u64,
                buffer_size,
            ));
        }

        client.with_connection(move |conn| {
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
}

/// Internal block state update for Redis (uses different storage key format)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RedisBlockStateUpdate {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
    pub accounts: Vec<AccountState>,
}

impl RedisBlockStateUpdate {
    fn to_json(&self) -> StateResult<String> {
        serde_json::to_string(self).map_err(|e| StateError::SerializeDiff(self.block_number, e))
    }

    fn from_json(json: &str, block_number: u64) -> StateResult<Self> {
        serde_json::from_str(json).map_err(|e| StateError::DeserializeDiff(block_number, e))
    }
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
            debug!(
                namespace = %recovery.namespace,
                target_block = recovery.target_block,
                writer_id = %recovery.writer_id,
                "recovered stale lock"
            );
            recoveries.push(recovery);
        }
    }

    if !recoveries.is_empty() {
        debug!(count = recoveries.len(), "stale lock recovery complete");
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

    trace!(
        namespace = namespace,
        current_block = ?current_block,
        target_block = target_block,
        writer_id = %lock.writer_id,
        "found stale lock, attempting recovery"
    );

    // Case 1: Write already completed (crash happened after metadata update but before lock release)
    // State is consistent, just release the lock
    if current_block == Some(target_block) {
        let lock_key = get_write_lock_key(namespace);
        redis::cmd("DEL").arg(&lock_key).query::<()>(conn)?;

        trace!(namespace = namespace, "lock released (write was complete)");

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

    debug!(
        namespace = namespace,
        start_block = start_block,
        target_block = target_block,
        diffs_to_apply = target_block - start_block + 1,
        "repairing namespace state"
    );

    for block_num in start_block..=target_block {
        let diff_key = get_diff_key(base_namespace, block_num);
        let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

        if let Some(json) = diff_json {
            let diff = RedisBlockStateUpdate::from_json(&json, block_num)?;
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
        } else {
            // Cannot complete recovery
            // DO NOT release the lock
            warn!(
                namespace = namespace,
                needed_block = block_num,
                target_block = target_block,
                "cannot recover: missing state diff"
            );
            return Err(StateError::MissingStateDiff {
                needed_block: block_num,
                target_block,
            });
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
        trace!(namespace = namespace, "write lock acquired");
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
        trace!(namespace = namespace, "write lock released");
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
            let slot_hex = encode_b256(*slot);
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

/// Commit a block using chunked writes with locking.
///
/// Returns `CommitStats` with timing and count information.
#[allow(clippy::too_many_lines)]
pub(crate) fn commit_block_chunked<C>(
    conn: &mut C,
    base_namespace: &str,
    buffer_size: usize,
    chunked_config: &ChunkedWriteConfig,
    writer_id: &str,
    update: &RedisBlockStateUpdate,
) -> StateResult<CommitStats>
where
    C: redis::ConnectionLike,
{
    let total_start = Instant::now();
    let mut stats = CommitStats::default();

    let block_number = update.block_number;
    let namespace = get_namespace_for_block(base_namespace, block_number, buffer_size)?;
    let current_block = read_namespace_block_number(conn, &namespace)?;

    // Collect stats from update
    stats.accounts_written = update.accounts.iter().filter(|a| !a.deleted).count();
    stats.accounts_deleted = update.accounts.iter().filter(|a| a.deleted).count();
    stats.storage_slots_written = update
        .accounts
        .iter()
        .flat_map(|a| a.storage.iter())
        .filter(|(_, v)| !v.is_zero())
        .count();
    stats.storage_slots_deleted = update
        .accounts
        .iter()
        .flat_map(|a| a.storage.iter())
        .filter(|(_, v)| v.is_zero())
        .count();
    stats.bytecodes_written = update.accounts.iter().filter(|a| a.code.is_some()).count();
    stats.largest_account_storage = update
        .accounts
        .iter()
        .map(|a| a.storage.len())
        .max()
        .unwrap_or(0);

    // Preprocess timing (serialization happens here)
    let preprocess_start = Instant::now();
    let diff_json = update.to_json()?;
    stats.diff_bytes = diff_json.len();
    stats.preprocess_duration = preprocess_start.elapsed();

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
        let diff_start = Instant::now();
        if block_number > start_block {
            let diffs_to_apply = block_number - start_block;
            debug!(
                namespace = %namespace,
                current_block = ?current_block,
                target_block = block_number,
                diffs = diffs_to_apply,
                "applying intermediate diffs for rotation"
            );

            for intermediate_block in start_block..block_number {
                let diff_key = get_diff_key(base_namespace, intermediate_block);
                let diff_json: Option<String> = redis::cmd("GET").arg(&diff_key).query(conn)?;

                if let Some(json) = diff_json {
                    let diff = RedisBlockStateUpdate::from_json(&json, intermediate_block)?;
                    let accounts: Vec<&AccountState> = diff.accounts.iter().collect();
                    for chunk in accounts.chunks(chunked_config.chunk_size) {
                        write_account_chunk(conn, &namespace, chunk)?;
                    }
                    stats.diffs_applied += 1;
                } else {
                    return Err(StateError::MissingStateDiff {
                        needed_block: intermediate_block,
                        target_block: block_number,
                    });
                }
            }
        }
        stats.diff_application_duration = diff_start.elapsed();

        // Phase 2: Write current block's accounts in chunks
        let batch_start = Instant::now();
        let accounts: Vec<&AccountState> = update.accounts.iter().collect();
        for chunk in accounts.chunks(chunked_config.chunk_size) {
            write_account_chunk(conn, &namespace, chunk)?;
        }
        stats.batch_write_duration = batch_start.elapsed();

        // Phase 3: Finalize
        let commit_start = Instant::now();
        write_block_metadata(
            conn,
            &namespace,
            base_namespace,
            block_number,
            update.block_hash,
            update.state_root,
            buffer_size,
        )?;

        // Store the diff (we already have it serialized)
        let diff_key = get_diff_key(base_namespace, update.block_number);
        redis::cmd("SET")
            .arg(&diff_key)
            .arg(&diff_json)
            .query::<()>(conn)?;

        // Delete old state diff
        if update.block_number >= buffer_size as u64 {
            let old_block = update.block_number - buffer_size as u64;
            let old_diff_key = get_diff_key(base_namespace, old_block);
            redis::cmd("DEL").arg(&old_diff_key).query::<()>(conn)?;
        }
        stats.commit_duration = commit_start.elapsed();

        Ok(())
    })();

    // Always release lock
    release_write_lock(conn, &namespace, writer_id)?;

    result?;

    stats.total_duration = total_start.elapsed();

    trace!(
        accounts_written = stats.accounts_written,
        storage_written = stats.storage_slots_written,
        diff_bytes = stats.diff_bytes,
        total_ms = stats.total_duration.as_millis(),
        "commit complete"
    );

    Ok(stats)
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
        let update = RedisBlockStateUpdate {
            block_number: 42,
            block_hash: B256::from([1u8; 32]),
            state_root: B256::from([2u8; 32]),
            accounts: vec![AccountState {
                address_hash: Address::from([3u8; 20]).into(),
                balance: U256::from(1000u64),
                nonce: 5,
                code_hash: B256::from([4u8; 32]),
                code: Some(vec![0x60, 0x80].into()),
                storage: std::collections::HashMap::new(),
                deleted: false,
            }],
        };

        let serialized = update.to_json().unwrap();
        let deserialized = RedisBlockStateUpdate::from_json(&serialized, 42).unwrap();

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
