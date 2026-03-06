//! MDBX persistence for shadow-driver outbound sidecar payloads.
//!
//! This module stores the sidecar inputs in compact, custom binary records:
//! - One `StoredNewIteration` per block
//! - Ordered `StoredTransaction` entries per block
//! - Global metadata tracking the latest fully persisted block
//!
//! All writes for a block are committed in a single MDBX RW transaction,
//! guaranteeing atomicity (no half-written block data).
//!
//! ## What Exactly Is Stored
//!
//! - `ShadowBlocks` (`block_number -> StoredNewIteration`)
//!   - One record per block with the block execution context used for sidecar iteration setup.
//! - `ShadowTransactions` (`(block_number, tx_index) -> StoredTransaction`)
//!   - One record per transaction, preserving in-block ordering by `tx_index`.
//! - `ShadowMetadata` (`MetadataKey(0) -> MetadataValue`)
//!   - Singleton metadata entry tracking `latest_block`.
//!
//! ## Struct Summary
//!
//! - `StoredNewIteration`
//!   - Block-level execution context snapshot.
//!   - Stores: block/timestamp/gas/basefee/difficulty, `prevrandao`, EIP-2935 parent block hash,
//!     EIP-4788 parent beacon block root, and blob gas pricing fields.
//! - `StoredTransaction`
//!   - Transaction execution input snapshot.
//!   - Stores: tx identity/order (`tx_hash`, `index`, `prev_tx_hash`), caller/value/gas fields,
//!     calldata, target (`transact_to`), chain id, access list, blob hashes, blob max fee,
//!     and EIP-7702 authorizations.
//! - `StoredAccessListItem`
//!   - Per-address access list segment with storage slot hashes.
//! - `StoredAuthorization`
//!   - EIP-7702 authorization tuple (`chain_id`, `address`, `nonce`, signature parts).
//! - `StoredCommitHead`
//!   - Block-finalization snapshot (`last_tx_hash`, tx count, block hash, parent beacon root, timestamp).
//! - `MetadataValue`
//!   - Latest persisted block pointer (`latest_block`) used for resume and gap checks.
//!
//! ## Key Ordering / Indexing
//!
//! - `BlockNumber` is encoded as **big-endian u64** so lexicographic key order matches numeric order.
//! - `TxKey` is encoded as `[block_number_be(8) | tx_index_be(8)]`, which clusters all txs
//!   for a block contiguously and in index order.
//!
//! ## Value Encoding
//!
//! Values use custom compact binary codecs (not protobuf). Arrays and fixed-width numbers are
//! stored in fixed-size form; variable-width fields use `u32` little-endian length prefixes.

use bytes::BufMut;
use reth_db::{
    Database,
    mdbx::{
        DatabaseArguments,
        DatabaseEnv,
        DatabaseEnvKind,
    },
};
use reth_db_api::{
    DatabaseError,
    cursor::DbCursorRO,
    models::ClientVersion,
    table::{
        Compress,
        Decode,
        Decompress,
        Encode,
        Table,
    },
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    path::Path,
    sync::Arc,
};
use thiserror::Error;

const DEFAULT_MAX_DB_SIZE: usize = 32 * 1024 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ShadowStoreError {
    #[error("failed to create directory {path}: {source}")]
    CreateDir {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to open MDBX at {path}: {message}")]
    DatabaseOpen { path: String, message: String },
    #[error("database error: {0}")]
    Database(#[from] reth_db_api::DatabaseError),
    #[error("metadata regression: trying to persist block {new_block}, latest is {latest_block}")]
    MetadataRegression { latest_block: u64, new_block: u64 },
    #[error("block gap detected: expected block {expected}, got {actual}")]
    BlockGap { expected: u64, actual: u64 },
}

type Result<T> = std::result::Result<T, ShadowStoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlockNumber(pub u64);

impl Encode for BlockNumber {
    type Encoded = [u8; 8];
    fn encode(self) -> Self::Encoded {
        self.0.to_be_bytes()
    }
}

impl Decode for BlockNumber {
    fn decode(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[..8]);
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

impl Compress for BlockNumber {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        self.0.to_be_bytes().to_vec()
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_u64(self.0);
    }
}

impl Decompress for BlockNumber {
    fn decompress(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[..8]);
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxKey {
    pub block_number: u64,
    pub tx_index: u64,
}

impl TxKey {
    #[must_use]
    pub const fn new(block_number: u64, tx_index: u64) -> Self {
        Self {
            block_number,
            tx_index,
        }
    }
}

impl Encode for TxKey {
    type Encoded = [u8; 16];
    fn encode(self) -> Self::Encoded {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&self.block_number.to_be_bytes());
        out[8..].copy_from_slice(&self.tx_index.to_be_bytes());
        out
    }
}

impl Decode for TxKey {
    fn decode(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        let mut block = [0u8; 8];
        let mut index = [0u8; 8];
        block.copy_from_slice(&value[..8]);
        index.copy_from_slice(&value[8..16]);
        Ok(Self {
            block_number: u64::from_be_bytes(block),
            tx_index: u64::from_be_bytes(index),
        })
    }
}

impl Compress for TxKey {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        self.encode().to_vec()
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_u64(self.block_number);
        buf.put_u64(self.tx_index);
    }
}

impl Decompress for TxKey {
    fn decompress(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        Self::decode(value)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct MetadataKey;

impl Encode for MetadataKey {
    type Encoded = [u8; 1];
    fn encode(self) -> Self::Encoded {
        [0]
    }
}

impl Decode for MetadataKey {
    fn decode(_value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        Ok(Self)
    }
}

impl Compress for MetadataKey {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        vec![0]
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_u8(0);
    }
}

impl Decompress for MetadataKey {
    fn decompress(_value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        Ok(Self)
    }
}

/// Stored version of sidecar `NewIteration` payload.
///
/// This is the persisted block execution context for one block, including
/// EIP-2935 (`parent_block_hash`) and EIP-4788 (`parent_beacon_block_root`) inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredNewIteration {
    pub block_number: u64,
    pub beneficiary: [u8; 20],
    pub timestamp: u64,
    pub gas_limit: u64,
    pub basefee: u64,
    pub difficulty: [u8; 32],
    pub prevrandao: Option<[u8; 32]>,
    pub excess_blob_gas: u64,
    pub blob_gasprice: u128,
    pub iteration_id: u64,
    pub parent_block_hash: Option<[u8; 32]>,
    pub parent_beacon_block_root: Option<[u8; 32]>,
}

/// Stored version of one access-list item (EIP-2930 / EIP-1559 family).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAccessListItem {
    pub address: [u8; 20],
    pub storage_keys: Vec<[u8; 32]>,
}

/// Stored version of one authorization entry (EIP-7702).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAuthorization {
    pub chain_id: [u8; 32],
    pub address: [u8; 20],
    pub nonce: u64,
    pub y_parity: u8,
    pub r: [u8; 32],
    pub s: [u8; 32],
}

/// Stored version of sidecar `Transaction` payload.
///
/// This captures tx execution input fields (env + ordering link via `prev_tx_hash`)
/// independent from sidecar execution result/ack state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTransaction {
    pub tx_hash: [u8; 32],
    pub index: u64,
    pub prev_tx_hash: Option<[u8; 32]>,
    pub tx_type: u8,
    pub caller: [u8; 20],
    pub gas_limit: u64,
    pub gas_price: u128,
    pub transact_to: Option<[u8; 20]>,
    pub value: [u8; 32],
    pub data: Vec<u8>,
    pub nonce: u64,
    pub chain_id: Option<u64>,
    pub access_list: Vec<StoredAccessListItem>,
    pub gas_priority_fee: Option<u128>,
    pub blob_hashes: Vec<[u8; 32]>,
    pub max_fee_per_blob_gas: u128,
    pub authorization_list: Vec<StoredAuthorization>,
}

/// Stored version of sidecar `CommitHead` payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCommitHead {
    pub last_tx_hash: Option<[u8; 32]>,
    pub n_transactions: u64,
    pub block_number: u64,
    pub selected_iteration_id: u64,
    pub block_hash: [u8; 32],
    pub parent_beacon_block_root: Option<[u8; 32]>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockPayload {
    pub new_iteration: StoredNewIteration,
    pub commit_head: StoredCommitHead,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionPayload(pub StoredTransaction);

impl Compress for BlockPayload {
    type Compressed = Vec<u8>;

    fn compress(self) -> Self::Compressed {
        encode_block_payload(&self)
    }

    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        let encoded = encode_block_payload(self);
        buf.put_slice(&encoded);
    }
}

impl Decompress for BlockPayload {
    fn decompress(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        decode_block_payload(value)
    }
}

impl Compress for TransactionPayload {
    type Compressed = Vec<u8>;

    fn compress(self) -> Self::Compressed {
        encode_transaction(&self.0)
    }

    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        let encoded = encode_transaction(&self.0);
        buf.put_slice(&encoded);
    }
}

impl Decompress for TransactionPayload {
    fn decompress(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        decode_transaction(value).map(Self)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MetadataValue {
    pub latest_block: u64,
}

impl Encode for MetadataValue {
    type Encoded = [u8; 8];
    fn encode(self) -> Self::Encoded {
        self.latest_block.to_be_bytes()
    }
}

impl Decode for MetadataValue {
    fn decode(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[..8]);
        Ok(Self {
            latest_block: u64::from_be_bytes(bytes),
        })
    }
}

impl Compress for MetadataValue {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        self.latest_block.to_be_bytes().to_vec()
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_u64(self.latest_block);
    }
}

impl Decompress for MetadataValue {
    fn decompress(value: &[u8]) -> std::result::Result<Self, reth_db_api::DatabaseError> {
        Self::decode(value)
    }
}

#[derive(Debug)]
pub struct Blocks;

impl Table for Blocks {
    const NAME: &'static str = "ShadowBlocks";
    const DUPSORT: bool = false;
    type Key = BlockNumber;
    type Value = BlockPayload;
}

#[derive(Debug)]
pub struct Transactions;

impl Table for Transactions {
    const NAME: &'static str = "ShadowTransactions";
    const DUPSORT: bool = false;
    type Key = TxKey;
    type Value = TransactionPayload;
}

#[derive(Debug)]
pub struct Metadata;

impl Table for Metadata {
    const NAME: &'static str = "ShadowMetadata";
    const DUPSORT: bool = false;
    type Key = MetadataKey;
    type Value = MetadataValue;
}

const TABLES: [&str; 3] = [Blocks::NAME, Transactions::NAME, Metadata::NAME];

#[derive(Clone)]
pub struct ShadowMdbxStore {
    env: Arc<DatabaseEnv>,
}

impl ShadowMdbxStore {
    /// Open or create a shadow-driver MDBX store at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created, MDBX cannot be
    /// opened, or required tables cannot be created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|source| {
                ShadowStoreError::CreateDir {
                    path: path.display().to_string(),
                    source,
                }
            })?;
        }

        let args = DatabaseArguments::new(ClientVersion::default())
            .with_geometry_max_size(Some(DEFAULT_MAX_DB_SIZE))
            .with_exclusive(Some(false))
            .with_max_read_transaction_duration(Some(
                reth_libmdbx::MaxReadTransactionDuration::Unbounded,
            ));

        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            ShadowStoreError::DatabaseOpen {
                path: path.display().to_string(),
                message: e.to_string(),
            }
        })?;

        {
            let tx = env.tx_mut().map_err(|e| {
                ShadowStoreError::DatabaseOpen {
                    path: path.display().to_string(),
                    message: e.to_string(),
                }
            })?;

            for table_name in TABLES {
                tx.inner
                    .create_db(Some(table_name), reth_libmdbx::DatabaseFlags::default())
                    .map_err(|e| {
                        ShadowStoreError::DatabaseOpen {
                            path: path.display().to_string(),
                            message: format!("failed creating table {table_name}: {e}"),
                        }
                    })?;
            }

            tx.commit().map_err(ShadowStoreError::Database)?;
        }

        Ok(Self { env: Arc::new(env) })
    }

    /// Return the latest fully persisted block number.
    ///
    /// # Errors
    ///
    /// Returns an error if the MDBX read transaction or metadata lookup fails.
    pub fn latest_block(&self) -> Result<Option<u64>> {
        let tx = self.env.tx()?;
        Ok(tx
            .get::<Metadata>(MetadataKey)?
            .map(|meta| meta.latest_block))
    }

    /// Read the stored block payload for a block number.
    ///
    /// # Errors
    ///
    /// Returns an error if MDBX read transaction or table access fails.
    pub fn get_block_payload(&self, block_number: u64) -> Result<Option<BlockPayload>> {
        let tx = self.env.tx()?;
        tx.get::<Blocks>(BlockNumber(block_number))
            .map_err(ShadowStoreError::from)
    }

    /// Read all stored transactions for a block in ascending tx index order.
    ///
    /// # Errors
    ///
    /// Returns an error if MDBX read transaction, cursor setup, or cursor
    /// traversal fails.
    pub fn get_block_transactions(&self, block_number: u64) -> Result<Vec<StoredTransaction>> {
        let tx = self.env.tx()?;
        let mut cursor = tx.cursor_read::<Transactions>()?;
        let mut transactions = Vec::new();
        let start_key = TxKey::new(block_number, 0);

        let mut next = cursor.seek(start_key)?;
        while let Some((key, payload)) = next {
            if key.block_number != block_number {
                break;
            }
            transactions.push(payload.0);
            next = cursor.next()?;
        }

        Ok(transactions)
    }

    /// Atomically append one fully committed block payload batch.
    ///
    /// A single MDBX RW transaction persists:
    /// - block new-iteration + commit-head payload
    /// - all transaction payloads for that block
    /// - latest metadata pointer
    ///
    /// # Errors
    ///
    /// Returns an error if metadata continuity checks fail, or if any MDBX
    /// read/write/commit operation fails.
    pub fn append_block(
        &self,
        block_number: u64,
        new_iteration_payload: StoredNewIteration,
        commit_head_payload: StoredCommitHead,
        transaction_payloads: &[(u64, StoredTransaction)],
    ) -> Result<()> {
        let tx = self.env.tx_mut()?;
        let latest_meta = tx.get::<Metadata>(MetadataKey)?;

        if let Some(meta) = latest_meta {
            if block_number < meta.latest_block {
                return Err(ShadowStoreError::MetadataRegression {
                    latest_block: meta.latest_block,
                    new_block: block_number,
                });
            }
            if block_number > meta.latest_block + 1 {
                return Err(ShadowStoreError::BlockGap {
                    expected: meta.latest_block + 1,
                    actual: block_number,
                });
            }
            if block_number == meta.latest_block {
                // Idempotent replay on reconnect, already persisted.
                return Ok(());
            }
        }

        tx.put::<Blocks>(
            BlockNumber(block_number),
            BlockPayload {
                new_iteration: new_iteration_payload,
                commit_head: commit_head_payload,
            },
        )?;

        for (index, payload) in transaction_payloads {
            tx.put::<Transactions>(
                TxKey::new(block_number, *index),
                TransactionPayload(payload.clone()),
            )?;
        }

        tx.put::<Metadata>(
            MetadataKey,
            MetadataValue {
                latest_block: block_number,
            },
        )?;

        tx.commit().map_err(ShadowStoreError::Database)?;
        Ok(())
    }
}

fn db_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError::Other(message.into())
}

/// Writes a `u32` length-prefixed byte slice.
///
/// Layout: `[len_le(4) | bytes(len)]`
fn write_len_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("length must fit into u32");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_len_prefixed(
    data: &[u8],
    cursor: &mut usize,
    field: &str,
) -> std::result::Result<Vec<u8>, DatabaseError> {
    let len = read_u32_count(data, cursor, field)? as usize;
    if *cursor + len > data.len() {
        return Err(db_error(format!("buffer too short for {field} bytes")));
    }
    let out = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(out)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> std::result::Result<u8, DatabaseError> {
    if *cursor + 1 > data.len() {
        return Err(db_error("buffer too short for u8"));
    }
    let value = data[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> std::result::Result<u64, DatabaseError> {
    if *cursor + 8 > data.len() {
        return Err(db_error("buffer too short for u64"));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u128(data: &[u8], cursor: &mut usize) -> std::result::Result<u128, DatabaseError> {
    if *cursor + 16 > data.len() {
        return Err(db_error("buffer too short for u128"));
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&data[*cursor..*cursor + 16]);
    *cursor += 16;
    Ok(u128::from_le_bytes(bytes))
}

fn read_fixed<const N: usize>(
    data: &[u8],
    cursor: &mut usize,
    field: &str,
) -> std::result::Result<[u8; N], DatabaseError> {
    if *cursor + N > data.len() {
        return Err(db_error(format!("buffer too short for {field}")));
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&data[*cursor..*cursor + N]);
    *cursor += N;
    Ok(out)
}

fn read_vec(
    data: &[u8],
    cursor: &mut usize,
    field: &str,
) -> std::result::Result<Vec<u8>, DatabaseError> {
    if *cursor + 4 > data.len() {
        return Err(db_error(format!("buffer too short for {field} length")));
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&data[*cursor..*cursor + 4]);
    *cursor += 4;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if *cursor + len > data.len() {
        return Err(db_error(format!("buffer too short for {field} bytes")));
    }
    let out = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(out)
}

/// Encode full block payload (`NewIteration` + `CommitHead`) into compact bytes.
///
/// Layout:
/// `[new_iteration_len_le(4) | new_iteration_bytes |
///   commit_head_len_le(4) | commit_head_bytes]`
fn encode_block_payload(payload: &BlockPayload) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);
    let new_iteration = encode_new_iteration(&payload.new_iteration);
    let commit_head = encode_commit_head(&payload.commit_head);
    write_len_prefixed(&mut buf, &new_iteration);
    write_len_prefixed(&mut buf, &commit_head);
    buf
}

/// Decode full block payload from the layout documented in `encode_block_payload`.
fn decode_block_payload(data: &[u8]) -> std::result::Result<BlockPayload, DatabaseError> {
    let mut cursor = 0usize;
    let new_iteration_bytes = read_len_prefixed(data, &mut cursor, "new_iteration")?;
    let commit_head_bytes = read_len_prefixed(data, &mut cursor, "commit_head")?;
    if cursor != data.len() {
        return Err(db_error("trailing bytes in BlockPayload"));
    }

    Ok(BlockPayload {
        new_iteration: decode_new_iteration(&new_iteration_bytes)?,
        commit_head: decode_commit_head(&commit_head_bytes)?,
    })
}

/// Encode `StoredNewIteration` into compact bytes.
///
/// Binary layout (LE for integers):
/// `[block_number(8) | beneficiary(20) | timestamp(8) | gas_limit(8) | basefee(8) | difficulty(32) |
///   prevrandao_tag(1) [prevrandao(32)]? |
///   excess_blob_gas(8) | blob_gasprice(16) | iteration_id(8) |
///   parent_block_hash_tag(1) [parent_block_hash(32)]? |
///   parent_beacon_root_tag(1) [parent_beacon_root(32)]? ]`
fn encode_new_iteration(item: &StoredNewIteration) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(&item.block_number.to_le_bytes());
    buf.extend_from_slice(&item.beneficiary);
    buf.extend_from_slice(&item.timestamp.to_le_bytes());
    buf.extend_from_slice(&item.gas_limit.to_le_bytes());
    buf.extend_from_slice(&item.basefee.to_le_bytes());
    buf.extend_from_slice(&item.difficulty);

    match item.prevrandao {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf.extend_from_slice(&item.excess_blob_gas.to_le_bytes());
    buf.extend_from_slice(&item.blob_gasprice.to_le_bytes());
    buf.extend_from_slice(&item.iteration_id.to_le_bytes());

    match item.parent_block_hash {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    match item.parent_beacon_block_root {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf
}

/// Decode `StoredNewIteration` from the layout documented in `encode_new_iteration`.
fn decode_new_iteration(data: &[u8]) -> std::result::Result<StoredNewIteration, DatabaseError> {
    let mut cursor = 0usize;

    let block_number = read_u64(data, &mut cursor)?;
    let beneficiary = read_fixed::<20>(data, &mut cursor, "beneficiary")?;
    let timestamp = read_u64(data, &mut cursor)?;
    let gas_limit = read_u64(data, &mut cursor)?;
    let basefee = read_u64(data, &mut cursor)?;
    let difficulty = read_fixed::<32>(data, &mut cursor, "difficulty")?;

    let prevrandao = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_fixed::<32>(data, &mut cursor, "prevrandao")?),
        _ => return Err(db_error("invalid prevrandao option tag")),
    };

    let excess_blob_gas = read_u64(data, &mut cursor)?;
    let blob_gasprice = read_u128(data, &mut cursor)?;
    let iteration_id = read_u64(data, &mut cursor)?;

    let parent_block_hash = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_fixed::<32>(data, &mut cursor, "parent_block_hash")?),
        _ => return Err(db_error("invalid parent_block_hash option tag")),
    };

    let parent_beacon_block_root = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => {
            Some(read_fixed::<32>(
                data,
                &mut cursor,
                "parent_beacon_block_root",
            )?)
        }
        _ => return Err(db_error("invalid parent_beacon_block_root option tag")),
    };

    if cursor != data.len() {
        return Err(db_error("trailing bytes in StoredNewIteration"));
    }

    Ok(StoredNewIteration {
        block_number,
        beneficiary,
        timestamp,
        gas_limit,
        basefee,
        difficulty,
        prevrandao,
        excess_blob_gas,
        blob_gasprice,
        iteration_id,
        parent_block_hash,
        parent_beacon_block_root,
    })
}

/// Encode `StoredCommitHead` into compact bytes.
///
/// Layout:
/// `[last_tx_hash_tag(1) [last_tx_hash(32)]? |
///   n_transactions(8) | block_number(8) | selected_iteration_id(8) |
///   block_hash(32) |
///   parent_beacon_tag(1) [parent_beacon_block_root(32)]? |
///   timestamp(8)]`
fn encode_commit_head(item: &StoredCommitHead) -> Vec<u8> {
    let mut buf = Vec::with_capacity(160);

    match item.last_tx_hash {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf.extend_from_slice(&item.n_transactions.to_le_bytes());
    buf.extend_from_slice(&item.block_number.to_le_bytes());
    buf.extend_from_slice(&item.selected_iteration_id.to_le_bytes());
    buf.extend_from_slice(&item.block_hash);

    match item.parent_beacon_block_root {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf.extend_from_slice(&item.timestamp.to_le_bytes());
    buf
}

/// Decode `StoredCommitHead` from the layout documented in `encode_commit_head`.
fn decode_commit_head(data: &[u8]) -> std::result::Result<StoredCommitHead, DatabaseError> {
    let mut cursor = 0usize;

    let last_tx_hash = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_fixed::<32>(data, &mut cursor, "last_tx_hash")?),
        _ => return Err(db_error("invalid last_tx_hash option tag")),
    };

    let n_transactions = read_u64(data, &mut cursor)?;
    let block_number = read_u64(data, &mut cursor)?;
    let selected_iteration_id = read_u64(data, &mut cursor)?;
    let block_hash = read_fixed::<32>(data, &mut cursor, "block_hash")?;

    let parent_beacon_block_root = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => {
            Some(read_fixed::<32>(
                data,
                &mut cursor,
                "parent_beacon_block_root",
            )?)
        }
        _ => return Err(db_error("invalid parent_beacon_block_root option tag")),
    };

    let timestamp = read_u64(data, &mut cursor)?;

    if cursor != data.len() {
        return Err(db_error("trailing bytes in StoredCommitHead"));
    }

    Ok(StoredCommitHead {
        last_tx_hash,
        n_transactions,
        block_number,
        selected_iteration_id,
        block_hash,
        parent_beacon_block_root,
        timestamp,
    })
}

/// Encode `StoredTransaction` into compact bytes.
///
/// Major layout sections:
/// - fixed tx identity / gas fields
/// - optional tags (`prev_tx_hash`, `transact_to`, `chain_id`, `gas_priority_fee`)
/// - length-prefixed tx data
/// - counted lists (`access_list`, `blob_hashes`, `authorization_list`) with `u32` LE counts
fn encode_transaction(item: &StoredTransaction) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512 + item.data.len());
    buf.extend_from_slice(&item.tx_hash);
    buf.extend_from_slice(&item.index.to_le_bytes());

    match item.prev_tx_hash {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf.push(item.tx_type);
    buf.extend_from_slice(&item.caller);
    buf.extend_from_slice(&item.gas_limit.to_le_bytes());
    buf.extend_from_slice(&item.gas_price.to_le_bytes());

    match item.transact_to {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value);
        }
        None => buf.push(0),
    }

    buf.extend_from_slice(&item.value);
    write_len_prefixed(&mut buf, &item.data);
    buf.extend_from_slice(&item.nonce.to_le_bytes());

    match item.chain_id {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value.to_le_bytes());
        }
        None => buf.push(0),
    }

    let access_len =
        u32::try_from(item.access_list.len()).expect("access list length must fit u32");
    buf.extend_from_slice(&access_len.to_le_bytes());
    for access_item in &item.access_list {
        buf.extend_from_slice(&access_item.address);
        let slots_len =
            u32::try_from(access_item.storage_keys.len()).expect("storage key length must fit u32");
        buf.extend_from_slice(&slots_len.to_le_bytes());
        for slot in &access_item.storage_keys {
            buf.extend_from_slice(slot);
        }
    }

    match item.gas_priority_fee {
        Some(value) => {
            buf.push(1);
            buf.extend_from_slice(&value.to_le_bytes());
        }
        None => buf.push(0),
    }

    let blob_len = u32::try_from(item.blob_hashes.len()).expect("blob hash length must fit u32");
    buf.extend_from_slice(&blob_len.to_le_bytes());
    for blob_hash in &item.blob_hashes {
        buf.extend_from_slice(blob_hash);
    }

    buf.extend_from_slice(&item.max_fee_per_blob_gas.to_le_bytes());

    let auth_len =
        u32::try_from(item.authorization_list.len()).expect("authorization length must fit u32");
    buf.extend_from_slice(&auth_len.to_le_bytes());
    for auth in &item.authorization_list {
        buf.extend_from_slice(&auth.chain_id);
        buf.extend_from_slice(&auth.address);
        buf.extend_from_slice(&auth.nonce.to_le_bytes());
        buf.push(auth.y_parity);
        buf.extend_from_slice(&auth.r);
        buf.extend_from_slice(&auth.s);
    }

    buf
}

/// Decode `StoredTransaction` from the layout documented in `encode_transaction`.
fn decode_transaction(data: &[u8]) -> std::result::Result<StoredTransaction, DatabaseError> {
    let mut cursor = 0usize;

    let tx_hash = read_fixed::<32>(data, &mut cursor, "tx_hash")?;
    let index = read_u64(data, &mut cursor)?;

    let prev_tx_hash = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_fixed::<32>(data, &mut cursor, "prev_tx_hash")?),
        _ => return Err(db_error("invalid prev_tx_hash option tag")),
    };

    let tx_type = read_u8(data, &mut cursor)?;
    let caller = read_fixed::<20>(data, &mut cursor, "caller")?;
    let gas_limit = read_u64(data, &mut cursor)?;
    let gas_price = read_u128(data, &mut cursor)?;

    let transact_to = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_fixed::<20>(data, &mut cursor, "transact_to")?),
        _ => return Err(db_error("invalid transact_to option tag")),
    };

    let value = read_fixed::<32>(data, &mut cursor, "value")?;
    let data_field = read_vec(data, &mut cursor, "tx data")?;
    let nonce = read_u64(data, &mut cursor)?;

    let chain_id = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_u64(data, &mut cursor)?),
        _ => return Err(db_error("invalid chain_id option tag")),
    };

    let access_list = decode_access_list(data, &mut cursor)?;

    let gas_priority_fee = match read_u8(data, &mut cursor)? {
        0 => None,
        1 => Some(read_u128(data, &mut cursor)?),
        _ => return Err(db_error("invalid gas_priority_fee option tag")),
    };

    let blob_hashes = decode_fixed_list::<32>(data, &mut cursor, "blob_hashes", "blob_hash")?;

    let max_fee_per_blob_gas = read_u128(data, &mut cursor)?;

    let authorization_list = decode_authorization_list(data, &mut cursor)?;

    if cursor != data.len() {
        return Err(db_error("trailing bytes in StoredTransaction"));
    }

    Ok(StoredTransaction {
        tx_hash,
        index,
        prev_tx_hash,
        tx_type,
        caller,
        gas_limit,
        gas_price,
        transact_to,
        value,
        data: data_field,
        nonce,
        chain_id,
        access_list,
        gas_priority_fee,
        blob_hashes,
        max_fee_per_blob_gas,
        authorization_list,
    })
}

/// Reads a `u32` LE element count used for repeated sections.
fn read_u32_count(
    data: &[u8],
    cursor: &mut usize,
    field: &str,
) -> std::result::Result<u32, DatabaseError> {
    if *cursor + 4 > data.len() {
        return Err(db_error(format!("buffer too short for {field} length")));
    }
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&data[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(bytes))
}

/// Decode a repeated fixed-size item list:
/// `[count_le(4) | item_0(N) | ... | item_{count-1}(N)]`
fn decode_fixed_list<const N: usize>(
    data: &[u8],
    cursor: &mut usize,
    count_field: &str,
    item_field: &str,
) -> std::result::Result<Vec<[u8; N]>, DatabaseError> {
    let count = read_u32_count(data, cursor, count_field)?;
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        items.push(read_fixed::<N>(data, cursor, item_field)?);
    }
    Ok(items)
}

/// Decode access list section:
/// `[access_count_le(4) | repeated { address(20) | storage_key_count_le(4) | storage_keys... }]`
fn decode_access_list(
    data: &[u8],
    cursor: &mut usize,
) -> std::result::Result<Vec<StoredAccessListItem>, DatabaseError> {
    let count = read_u32_count(data, cursor, "access_list")?;
    let mut access_list = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let address = read_fixed::<20>(data, cursor, "access_list.address")?;
        let storage_keys = decode_fixed_list::<32>(
            data,
            cursor,
            "access_list.storage_keys",
            "access_list.storage_key",
        )?;
        access_list.push(StoredAccessListItem {
            address,
            storage_keys,
        });
    }
    Ok(access_list)
}

/// Decode authorization list section:
/// `[auth_count_le(4) | repeated { chain_id(32) | address(20) | nonce(8) | y_parity(1) | r(32) | s(32) }]`
fn decode_authorization_list(
    data: &[u8],
    cursor: &mut usize,
) -> std::result::Result<Vec<StoredAuthorization>, DatabaseError> {
    let count = read_u32_count(data, cursor, "authorization_list")?;
    let mut authorization_list = Vec::with_capacity(count as usize);
    for _ in 0..count {
        authorization_list.push(StoredAuthorization {
            chain_id: read_fixed::<32>(data, cursor, "authorization.chain_id")?,
            address: read_fixed::<20>(data, cursor, "authorization.address")?,
            nonce: read_u64(data, cursor)?,
            y_parity: read_u8(data, cursor)?,
            r: read_fixed::<32>(data, cursor, "authorization.r")?,
            s: read_fixed::<32>(data, cursor, "authorization.s")?,
        });
    }
    Ok(authorization_list)
}
