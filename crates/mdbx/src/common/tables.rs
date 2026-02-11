//! # MDBX Table Definitions
//!
//! This module defines all database tables used by the circular buffer state storage.
//! Each table is implemented using reth's database abstractions over MDBX.
//!
//! ## MDBX Primer
//!
//! MDBX is a key-value store where:
//! - An **environment** is a directory containing the database files
//! - A **database** (what we call "table") is a named B-tree within the environment
//! - All operations happen within **transactions** (read-only or read-write)
//! - Keys and values are arbitrary byte sequences
//!
//! ## Table Overview
//!
//! | Table | Key | Value | Purpose |
//! |-------|-----|-------|---------|
//! | `NamespaceBlocks` | `u8` | `u64` | Current block in each namespace |
//! | `NamespacedAccounts` | `(ns, addr_hash)` | `AccountData` | Account state |
//! | `NamespacedStorage` | `(ns, addr_hash, slot)` | `U256` | Storage slots |
//! | `Bytecodes` | `code_hash` | `Bytes` | Contract code (shared) |
//! | `BlockMetadata` | `block_number` | `(hash, root)` | Block info |
//! | `StateDiffs` | `block_number` | `JSON` | Diffs for reconstruction |
//! | `Metadata` | `0` | `GlobalMetadata` | Latest block, buffer size |
//!
//! ## Key Encoding Strategy
//!
//! ### Simple Keys
//!
//! ```text
//! NamespaceIdx (u8):     [1 byte]
//! BlockNumber (u64):     [8 bytes, big-endian for ordering]
//! CodeHash (B256):       [32 bytes]
//! ```
//!
//! ### Composite Keys
//!
//! ```text
//! NamespacedAccountKey:  [namespace(1) | address_hash(32)]  = 33 bytes
//! NamespacedStorageKey:  [namespace(1) | address_hash(32) | slot_hash(32)] = 65 bytes
//! ```
//!
//! Big-endian encoding ensures lexicographic ordering matches numeric ordering.

use crate::{
    AccountInfo,
    common::types::{
        BlockMetadata,
        GlobalMetadata,
        NamespacedAccountKey,
        NamespacedBytecodeKey,
        NamespacedStorageKey,
        StorageValue,
    },
};
use bytes::BufMut;
use reth_codecs::Compact;
use reth_db_api::table::{
    Compress,
    Decode,
    Decompress,
    Encode,
    Table,
};
use serde::{
    Deserialize,
    Serialize,
};

/// Wrapper for namespace index (0..`buffer_size`).
///
/// Encoded as a single byte, supporting up to 255 namespaces.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespaceIdx(pub u8);

impl Encode for NamespaceIdx {
    type Encoded = [u8; 1];
    fn encode(self) -> Self::Encoded {
        [self.0]
    }
}

impl Decode for NamespaceIdx {
    fn decode(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        Ok(Self(*value.first().ok_or(
            reth_db_api::DatabaseError::Other(
                "Invalid namespace index encoding: buffer too short".to_string(),
            ),
        )?))
    }
}

impl Compress for NamespaceIdx {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        vec![self.0]
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_u8(self.0);
    }
}

impl Decompress for NamespaceIdx {
    fn decompress(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        Ok(Self(*value.first().ok_or(
            reth_db_api::DatabaseError::Other(
                "Invalid namespace index encoding: buffer too short".to_string(),
            ),
        )?))
    }
}

/// Wrapper for block number.
///
/// Encoded as 8 bytes big-endian to ensure lexicographic ordering
/// matches numeric ordering (block 100 sorts after block 99).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct BlockNumber(pub u64);

impl Encode for BlockNumber {
    type Encoded = [u8; 8];
    fn encode(self) -> Self::Encoded {
        self.0.to_be_bytes()
    }
}

impl Decode for BlockNumber {
    fn decode(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&value[..8]);
        Ok(Self(u64::from_be_bytes(buf)))
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
    fn decompress(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&value[..8]);
        Ok(Self(u64::from_be_bytes(buf)))
    }
}

/// Wrapper for bytecode.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Bytecode(pub alloy::primitives::Bytes);

impl Compress for Bytecode {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        self.0.to_vec()
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_slice(&self.0);
    }
}

impl Decompress for Bytecode {
    fn decompress(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        Ok(Self(alloy::primitives::Bytes::copy_from_slice(value)))
    }
}

/// Wrapper for state diff data (JSON serialized).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StateDiffData(pub Vec<u8>);

impl Compress for StateDiffData {
    type Compressed = Vec<u8>;
    fn compress(self) -> Self::Compressed {
        self.0
    }
    fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        buf.put_slice(&self.0);
    }
}

impl Decompress for StateDiffData {
    fn decompress(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        Ok(Self(value.to_vec()))
    }
}

/// Metadata key (always 0 for single-entry table).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct MetadataKey;

impl Encode for MetadataKey {
    type Encoded = [u8; 1];
    fn encode(self) -> Self::Encoded {
        [0]
    }
}

impl Decode for MetadataKey {
    fn decode(_value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
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
    fn decompress(_value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        Ok(Self)
    }
}

/// Macro to implement Encode/Decode/Compress/Decompress for types that
/// already implement `reth_codecs::Compact`.
macro_rules! impl_compact_table_codec {
    ($ty:ty, $size_hint:expr) => {
        impl Encode for $ty {
            type Encoded = Vec<u8>;

            fn encode(self) -> Self::Encoded {
                let mut buf = Vec::with_capacity($size_hint);
                self.to_compact(&mut buf);
                buf
            }
        }

        impl Decode for $ty {
            fn decode(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
                let (val, _) = Self::from_compact(value, value.len());
                Ok(val)
            }
        }

        impl Compress for $ty {
            type Compressed = Vec<u8>;

            fn compress(self) -> Self::Compressed {
                let mut buf = Vec::with_capacity($size_hint);
                self.to_compact(&mut buf);
                buf
            }

            fn compress_to_buf<B: BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
                self.clone().to_compact(buf);
            }
        }

        impl Decompress for $ty {
            fn decompress(value: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
                let (val, _) = Self::from_compact(value, value.len());
                Ok(val)
            }
        }
    };
}

// Implement for our custom Compact types
impl_compact_table_codec!(NamespacedAccountKey, 33);
impl_compact_table_codec!(NamespacedStorageKey, 65);
impl_compact_table_codec!(NamespacedBytecodeKey, 33);
impl_compact_table_codec!(AccountInfo, 64);
impl_compact_table_codec!(StorageValue, 32);
impl_compact_table_codec!(BlockMetadata, 64);
impl_compact_table_codec!(GlobalMetadata, 16);

/// Maps namespace index (0..`buffer_size`) to current block number in that namespace.
///
/// This is the primary lookup for determining which block is in which namespace.
///
/// ```text
/// Example with buffer_size=3 at block 105:
/// ┌─────┬───────┐
/// │ Key │ Value │
/// ├─────┼───────┤
/// │  0  │  105  │
/// │  1  │  103  │
/// │  2  │  104  │
/// └─────┴───────┘
/// ```
#[derive(Debug)]
pub struct NamespaceBlocks;

impl Table for NamespaceBlocks {
    const NAME: &'static str = "NamespaceBlocks";
    const DUPSORT: bool = false;
    type Key = NamespaceIdx;
    type Value = BlockNumber;
}

/// Account data per namespace.
///
/// Key: `NamespacedAccountKey` (`namespace_idx` + `address_hash`)
/// Value: `AccountData` (`balance`, `nonce`, `code_hash`)
///
/// Storage is in a separate table (`NamespacedStorage`) because:
/// - Different access patterns
/// - Storage can be huge
/// - Most reads only need account data
#[derive(Debug)]
pub struct NamespacedAccounts;

impl Table for NamespacedAccounts {
    const NAME: &'static str = "NamespacedAccounts";
    const DUPSORT: bool = false;
    type Key = NamespacedAccountKey;
    type Value = AccountInfo;
}

/// Storage slots per account per namespace.
///
/// Key: `NamespacedStorageKey` (`namespace_idx` + `address_hash` + `slot_hash`)
/// Value: `StorageValue` (U256)
///
/// Zero values are deleted (Ethereum semantics: zero == non-existent).
#[derive(Debug)]
pub struct NamespacedStorage;

impl Table for NamespacedStorage {
    const NAME: &'static str = "NamespacedStorage";
    const DUPSORT: bool = false;
    type Key = NamespacedStorageKey;
    type Value = StorageValue;
}

/// Contract bytecode per namespace.
///
/// Key: `NamespacedBytecodeKey` (`namespace_idx` + `code_hash`)
/// Value: `Bytecode` (Bytes)
///
/// Note: This does mean bytecode is duplicated across namespaces, but this
/// ensures correctness when querying historical state within the buffer.
#[derive(Debug)]
pub struct Bytecodes;

impl Table for Bytecodes {
    const NAME: &'static str = "Bytecodes";
    const DUPSORT: bool = false;
    type Key = NamespacedBytecodeKey;
    type Value = Bytecode;
}

/// Block metadata (hash + state root) by block number.
///
/// Key: `BlockNumber` (u64)
/// Value: `BlockMetadata` (`block_hash` + `state_root`)
///
/// Kept for `buffer_size` blocks; older entries are cleaned up.
#[derive(Debug)]
pub struct BlockMetadataTable;

impl Table for BlockMetadataTable {
    const NAME: &'static str = "BlockMetadata";
    const DUPSORT: bool = false;
    type Key = BlockNumber;
    type Value = BlockMetadata;
}

/// State diffs for reconstructing state during circular buffer rotation.
///
/// Key: `BlockNumber` (u64)
/// Value: `StateDiffData` (JSON serialized `BlockStateUpdate`)
///
/// When namespace N rotates from block B to block B+`buffer_size`, we need
/// to apply all intermediate diffs (B+1, B+2, ..., B+`buffer_size`-1) to
/// bring the state up to date.
#[derive(Debug)]
pub struct StateDiffs;

impl Table for StateDiffs {
    const NAME: &'static str = "StateDiffs";
    const DUPSORT: bool = false;
    type Key = BlockNumber;
    type Value = StateDiffData;
}

/// Global metadata (latest block, buffer size).
///
/// Key: `MetadataKey` (always 0 - single entry table)
/// Value: `GlobalMetadata`
#[derive(Debug)]
pub struct Metadata;

impl Table for Metadata {
    const NAME: &'static str = "Metadata";
    const DUPSORT: bool = false;
    type Key = MetadataKey;
    type Value = GlobalMetadata;
}

/// All tables used by the state store.
pub const TABLES: [&str; 7] = [
    NamespaceBlocks::NAME,
    NamespacedAccounts::NAME,
    NamespacedStorage::NAME,
    Bytecodes::NAME,
    BlockMetadataTable::NAME,
    StateDiffs::NAME,
    Metadata::NAME,
];
