#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]

use crate::redis::writer::StaleLockRecovery;
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use bytes::BufMut;
use reth_codecs::Compact;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    fmt::Display,
    time::Duration,
};

pub mod mdbx;
pub mod redis;

/// Account info without storage (for reader API).
///
/// This is the lightweight response type for `get_account()` calls.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccountInfo {
    pub address_hash: AddressHash,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
}

impl Compact for AccountInfo {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.address_hash.as_ref());
        buf.put_slice(&self.balance.to_be_bytes::<32>());
        buf.put_u64(self.nonce);
        buf.put_slice(self.code_hash.as_ref());
        32 + 32 + 8 + 32 // 104 bytes total
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let address_hash = B256::from_slice(&buf[0..32]);
        let balance = U256::from_be_slice(&buf[32..64]);
        let nonce = u64::from_be_bytes(buf[64..72].try_into().unwrap());
        let code_hash = B256::from_slice(&buf[72..104]);
        (
            Self {
                address_hash: AddressHash(address_hash),
                balance,
                nonce,
                code_hash,
            },
            &buf[104..],
        )
    }
}

/// Type for defining the keccak256(address)
#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct AddressHash(pub B256);

impl Display for AddressHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl AddressHash {
    pub fn new<T: AsRef<[u8]>>(bytes: T) -> Self {
        Self(keccak256(bytes))
    }

    /// Create from an already-computed hash (no re-hashing).
    pub fn from_hash(hash: B256) -> Self {
        Self(hash)
    }

    /// Get the underlying B256 hash.
    pub fn as_b256(&self) -> &B256 {
        &self.0
    }

    /// Convert to owned B256.
    pub fn into_b256(self) -> B256 {
        self.0
    }
}

impl Default for AddressHash {
    fn default() -> Self {
        Self(B256::ZERO)
    }
}

impl From<B256> for AddressHash {
    fn from(hash: B256) -> Self {
        Self(hash)
    }
}

impl From<Address> for AddressHash {
    fn from(address: Address) -> Self {
        Self(keccak256(address))
    }
}

impl AsRef<[u8]> for AddressHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl std::fmt::LowerHex for AddressHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl Compact for AddressHash {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.0.as_ref());
        32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let hash = B256::from_slice(&buf[..32]);
        (Self(hash), &buf[32..])
    }
}

/// Complete account state including storage (for diffs and full reads).
///
/// Used for:
/// - Block state updates (commits)
/// - State diffs (stored as JSON for reconstruction)
/// - Full account reads (when storage is needed)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountState {
    pub address_hash: AddressHash,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<Bytes>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub storage: HashMap<B256, U256>,
    #[serde(default)]
    pub deleted: bool,
}

impl Default for AccountState {
    fn default() -> Self {
        Self {
            address_hash: AddressHash::default(),
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }
    }
}

/// Complete state update for a block (used for commits and diffs).
///
/// This is the primary input to `Writer::commit_block()` and is also
/// serialized to JSON for the `StateDiffs` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockStateUpdate {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
    pub accounts: Vec<AccountState>,
}

impl BlockStateUpdate {
    pub fn new(block_number: u64, block_hash: B256, state_root: B256) -> Self {
        Self {
            block_number,
            block_hash,
            state_root,
            accounts: Vec::new(),
        }
    }

    /// Merge an account state into the update.
    ///
    /// If the account already exists in the update, merges the storage
    /// and updates the account fields.
    pub fn merge_account_state(&mut self, state: AccountState) {
        if let Some(existing) = self
            .accounts
            .iter_mut()
            .find(|a| a.address_hash == state.address_hash)
        {
            for (slot, value) in state.storage {
                existing.storage.insert(slot, value);
            }
            existing.balance = state.balance;
            existing.nonce = state.nonce;
            existing.code_hash = state.code_hash;
            if state.code.is_some() {
                existing.code = state.code;
            }
            existing.deleted = state.deleted;
        } else {
            self.accounts.push(state);
        }
    }

    /// Serialize to JSON for storage in `StateDiffs` table.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON.
    pub fn from_json(data: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// Block metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
}

/// Statistics from a block commit operation.
///
/// Returned by `Writer::commit_block()` to allow callers to track
/// performance metrics without coupling the storage layer to a
/// specific metrics library.
#[derive(Debug, Clone, Default)]
pub struct CommitStats {
    /// Number of accounts written (created or updated).
    pub accounts_written: usize,
    /// Number of accounts deleted.
    pub accounts_deleted: usize,
    /// Number of storage slots written (non-zero values).
    pub storage_slots_written: usize,
    /// Number of storage slots deleted (set to zero).
    pub storage_slots_deleted: usize,
    /// Number of accounts whose entire storage was wiped.
    pub full_storage_deletes: usize,
    /// Number of bytecodes written.
    pub bytecodes_written: usize,
    /// Number of intermediate diffs applied during rotation.
    pub diffs_applied: usize,
    /// Size of the serialized diff in bytes.
    pub diff_bytes: usize,
    /// Largest storage count for any single account in this commit.
    pub largest_account_storage: usize,

    /// Time spent in parallel preprocessing (validation, diff creation, batch building).
    pub preprocess_duration: Duration,
    /// Time spent applying intermediate diffs during rotation.
    pub diff_application_duration: Duration,
    /// Time spent executing batch writes.
    pub batch_write_duration: Duration,
    /// Time spent in `tx.commit()` call.
    pub commit_duration: Duration,
    /// Total wall-clock time for the entire commit operation.
    pub total_duration: Duration,
}

/// Statistics from read operations.
///
/// Returned by bulk read methods to help track performance.
#[derive(Debug, Clone, Default)]
pub struct ReadStats {
    /// Number of storage slots read.
    pub storage_slots_read: usize,
    /// Time spent in the read operation.
    pub duration: Duration,
}

/// Trait for reading blockchain state from a storage backend.
pub trait Reader {
    type Error: std::error::Error;

    /// Get the most recent block number.
    ///
    /// Returns `None` if no blocks have been written yet.
    fn latest_block_number(&self) -> Result<Option<u64>, Self::Error>;

    /// Check if a block is available in the circular buffer.
    ///
    /// A block is available if its namespace currently contains that block.
    fn is_block_available(&self, block_number: u64) -> Result<bool, Self::Error>;

    /// Get account info without storage slots (balance, nonce, code hash only).
    /// This is the recommended method for most use cases to avoid large data transfers.
    /// Use `get_full_account` or `get_all_storage` separately if storage is needed.
    ///
    /// Returns an error if the namespace is locked for writing.
    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<Option<AccountInfo>, Self::Error>;

    /// Get a specific storage slot for an account at a block.
    ///
    /// Returns `None` if the slot doesn't exist or has value zero.
    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> Result<Option<U256>, Self::Error>;

    /// Get all storage slots for an account at a block.
    ///
    /// WARNING: This can be expensive for contracts with large storage.
    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<HashMap<B256, U256>, Self::Error>;

    /// Get contract bytecode by code hash.
    fn get_code(&self, code_hash: B256, block_number: u64) -> Result<Option<Bytes>, Self::Error>;

    /// Get complete account state including storage.
    ///
    /// WARNING: This can transfer large amounts of data for contracts with many slots.
    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<Option<AccountState>, Self::Error>;

    /// Get block hash for a specific block number.
    fn get_block_hash(&self, block_number: u64) -> Result<Option<B256>, Self::Error>;

    /// Get state root for a specific block number.
    fn get_state_root(&self, block_number: u64) -> Result<Option<B256>, Self::Error>;

    /// Get complete block metadata (hash and state root).
    fn get_block_metadata(&self, block_number: u64) -> Result<Option<BlockMetadata>, Self::Error>;

    /// Get the range of available blocks [oldest, latest].
    ///
    /// Returns `None` if no blocks have been written.
    fn get_available_block_range(&self) -> Result<Option<(u64, u64)>, Self::Error>;

    /// Scan all account hashes in the buffer for a specific block.
    ///
    /// This returns the address hashes (keccak256 of addresses), not the
    /// original addresses. Useful for iteration/debugging.
    ///
    /// # Errors
    ///
    /// Returns `BlockNotFound` if the block is not in the circular buffer.
    fn scan_account_hashes(&self, block_number: u64) -> Result<Vec<AddressHash>, Self::Error>;

    /// Check if a state diff exists for the given block.
    ///
    /// Used for recovery to identify missing intermediate diffs.
    fn has_state_diff(&self, block_number: u64) -> Result<bool, Self::Error>;

    /// Get the current block number stored in a specific namespace.
    ///
    /// Returns `None` if the namespace is empty (no blocks written yet).
    fn get_namespace_block(&self, namespace_idx: u8) -> Result<Option<u64>, Self::Error>;

    /// Get the buffer size configuration.
    fn buffer_size(&self) -> u8;
}

/// Trait for writing blockchain state to a storage backend.
pub trait Writer {
    type Error: std::error::Error;

    /// Commit a block's state update to the database.
    ///
    /// This handles:
    /// 1. Applying intermediate diffs if rotating the circular buffer
    /// 2. Writing all account and storage changes
    /// 3. Updating metadata and cleaning up old data
    ///
    /// All changes happen in a single transaction. If anything fails,
    /// the entire operation is rolled back.
    ///
    /// Returns statistics about the commit operation for metrics tracking.
    fn commit_block(&self, update: &BlockStateUpdate) -> Result<CommitStats, Self::Error>;

    /// Ensure the Redis metadata matches the configured namespace rotation size.
    fn ensure_dump_index_metadata(&self) -> Result<(), Self::Error>;

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
    fn recover_stale_locks(&self) -> Result<Vec<StaleLockRecovery>, Self::Error>;

    /// Bootstrap the circular buffer from a single state snapshot.
    ///
    /// Copies the same state to ALL namespaces, with all namespaces pointing
    /// to the same block number. This allows the circular buffer to work
    /// correctly once new blocks start arriving.
    ///
    /// After `buffer_size` new blocks are processed, all state will be accurate
    /// (the initially duplicated state will have been overwritten).
    ///
    /// ## Note
    ///
    /// No state diffs are stored during bootstrap - they're not needed since
    /// all namespaces are initialized with identical state at the same block.
    fn bootstrap_from_snapshot(
        &self,
        accounts: Vec<AccountState>,
        block_number: u64,
        block_hash: B256,
        state_root: B256,
    ) -> Result<CommitStats, Self::Error>;

    /// Store a state diff without committing the full block state.
    ///
    /// Used for recovery when intermediate diffs are missing. This stores
    /// just the diff data so that future namespace rotations can succeed.
    ///
    /// The diff is stored in the same format as `commit_block` would store it.
    fn store_state_diff(&self, update: &BlockStateUpdate) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_state_update_json() {
        let update = BlockStateUpdate {
            block_number: 100,
            block_hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x22),
            accounts: vec![AccountState {
                address_hash: AddressHash(B256::repeat_byte(0xAA)),
                balance: U256::from(1000),
                nonce: 5,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            }],
        };

        let json = update.to_json().unwrap();
        let decoded = BlockStateUpdate::from_json(&json).unwrap();

        assert_eq!(decoded.block_number, 100);
        assert_eq!(decoded.accounts.len(), 1);
        assert_eq!(decoded.accounts[0].balance, U256::from(1000));
    }

    #[test]
    fn test_commit_stats_default() {
        let stats = CommitStats::default();
        assert_eq!(stats.accounts_written, 0);
        assert_eq!(stats.total_duration, Duration::ZERO);
    }
}
