//! Types for MDBX state storage with compact encoding.
//!
//! This module defines all the core types used throughout the state storage system,
//! including:
//!
//! - **Address hashing**: `AddressHash` for keccak256-hashed addresses
//! - **Composite keys**: `NamespacedAccountKey`, `NamespacedStorageKey` for namespaced lookups
//! - **Value types**: `AccountData`, `StorageValue`, `BlockMetadata`
//! - **State updates**: `AccountState`, `BlockStateUpdate` for commits and diffs
//! - **Configuration**: `CircularBufferConfig`
//!
//! ## Encoding Strategy
//!
//! All types implement `reth_codecs::Compact` for efficient binary serialization:
//!
//! ```text
//! NamespacedStorageKey (65 bytes):
//! ┌──────────────┬─────────────────────────────────────────┬─────────────────────────────────────────┐
//! │ namespace(1) │           address_hash(32)              │           slot_hash(32)                 │
//! └──────────────┴─────────────────────────────────────────┴─────────────────────────────────────────┘
//! ```

use crate::AddressHash;
use alloy::primitives::{
    B256,
    U256,
};
use reth_codecs::Compact;
use serde::{
    Deserialize,
    Serialize,
};

use super::error::StateError;

/// Key for namespaced account lookups: (`namespace_idx`, `address_hash`).
///
/// The namespace index comes first to enable efficient prefix scans of all
/// accounts within a namespace.
///
/// # Binary Layout
///
/// ```text
/// ┌──────────────┬─────────────────────────────────────────┐
/// │ namespace(1) │           address_hash(32)              │
/// └──────────────┴─────────────────────────────────────────┘
/// Total: 33 bytes
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedAccountKey {
    pub namespace_idx: u8,
    pub address_hash: AddressHash,
}

impl NamespacedAccountKey {
    pub const fn new(namespace_idx: u8, address_hash: AddressHash) -> Self {
        Self {
            namespace_idx,
            address_hash,
        }
    }
}

impl Compact for NamespacedAccountKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_u8(self.namespace_idx);
        buf.put_slice(self.address_hash.0.as_ref());
        33 // 1 + 32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let namespace_idx = buf[0];
        let address_hash = AddressHash(B256::from_slice(&buf[1..33]));
        (
            Self {
                namespace_idx,
                address_hash,
            },
            &buf[33..],
        )
    }
}

/// Key for namespaced storage lookups: (`namespace_idx`, `address_hash`, `slot_hash`).
///
/// # Binary Layout
///
/// ```text
/// ┌──────────────┬─────────────────────────────────────────┬─────────────────────────────────────────┐
/// │ namespace(1) │           address_hash(32)              │           slot_hash(32)                 │
/// └──────────────┴─────────────────────────────────────────┴─────────────────────────────────────────┘
/// Total: 65 bytes
/// ```
///
/// This layout enables efficient cursor scans:
/// - All storage in namespace N: seek to `[N, 0x00..., 0x00...]`
/// - All storage for account A in namespace N: seek to `[N, A, 0x00...]`
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedStorageKey {
    pub namespace_idx: u8,
    pub address_hash: AddressHash,
    pub slot_hash: B256,
}

impl NamespacedStorageKey {
    pub const fn new(namespace_idx: u8, address_hash: AddressHash, slot_hash: B256) -> Self {
        Self {
            namespace_idx,
            address_hash,
            slot_hash,
        }
    }

    /// Create the prefix key for iterating all slots of an account.
    pub const fn account_prefix(
        namespace_idx: u8,
        address_hash: AddressHash,
    ) -> NamespacedAccountKey {
        NamespacedAccountKey {
            namespace_idx,
            address_hash,
        }
    }
}

impl Compact for NamespacedStorageKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_u8(self.namespace_idx);
        buf.put_slice(self.address_hash.0.as_ref());
        buf.put_slice(self.slot_hash.as_ref());
        65 // 1 + 32 + 32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let namespace_idx = buf[0];
        let address_hash = AddressHash(B256::from_slice(&buf[1..33]));
        let slot_hash = B256::from_slice(&buf[33..65]);
        (
            Self {
                namespace_idx,
                address_hash,
                slot_hash,
            },
            &buf[65..],
        )
    }
}

/// Key for namespaced bytecode lookups: (`namespace_idx`, `code_hash`).
///
/// # Binary Layout
///
/// ```text
/// ┌──────────────┬─────────────────────────────────────────┐
/// │ namespace(1) │             code_hash(32)               │
/// └──────────────┴─────────────────────────────────────────┘
/// Total: 33 bytes
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedBytecodeKey {
    pub namespace_idx: u8,
    pub code_hash: B256,
}

impl NamespacedBytecodeKey {
    pub const fn new(namespace_idx: u8, code_hash: B256) -> Self {
        Self {
            namespace_idx,
            code_hash,
        }
    }
}

impl Compact for NamespacedBytecodeKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_u8(self.namespace_idx);
        buf.put_slice(self.code_hash.as_ref());
        33 // 1 + 32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let namespace_idx = buf[0];
        let code_hash = B256::from_slice(&buf[1..33]);
        (
            Self {
                namespace_idx,
                code_hash,
            },
            &buf[33..],
        )
    }
}

/// Storage value (U256 stored as 32 bytes for consistent sizing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StorageValue(pub U256);

impl Compact for StorageValue {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_slice(&self.0.to_be_bytes::<32>());
        32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let value = U256::from_be_slice(&buf[..32]);
        (Self(value), &buf[32..])
    }
}

/// Block metadata stored together for efficient retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Default, Compact, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub block_hash: B256,
    pub state_root: B256,
}

/// Global metadata stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, Default, Compact, Serialize, Deserialize)]
pub struct GlobalMetadata {
    pub latest_block: u64,
    pub buffer_size: u8,
}

/// Configuration for the circular buffer.
#[derive(Clone, Debug)]
pub struct CircularBufferConfig {
    /// Number of historical states to maintain (1-255).
    pub buffer_size: u8,
}

impl CircularBufferConfig {
    /// Create a new configuration.
    ///
    /// # Errors
    ///
    /// Returns `InvalidBufferSize` if `buffer_size == 0`.
    pub fn new(buffer_size: u8) -> Result<Self, StateError> {
        if buffer_size == 0 {
            return Err(StateError::InvalidBufferSize);
        }
        Ok(Self { buffer_size })
    }

    /// Get the namespace index for a block number.
    ///
    /// This is the core of the circular buffer: blocks are distributed
    /// across namespaces using modulo arithmetic.
    #[inline]
    pub fn namespace_for_block(&self, block_number: u64) -> Result<u8, StateError> {
        u8::try_from(block_number % u64::from(self.buffer_size)).map_err(StateError::IntConversion)
    }
}

impl Default for CircularBufferConfig {
    fn default() -> Self {
        Self { buffer_size: 3 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Address;
    use alloy::primitives::keccak256;

    #[test]
    fn test_namespace_calculation() {
        let config = CircularBufferConfig::new(3).unwrap();
        assert_eq!(config.namespace_for_block(0).unwrap(), 0);
        assert_eq!(config.namespace_for_block(1).unwrap(), 1);
        assert_eq!(config.namespace_for_block(2).unwrap(), 2);
        assert_eq!(config.namespace_for_block(3).unwrap(), 0);
        assert_eq!(config.namespace_for_block(4).unwrap(), 1);
        assert_eq!(config.namespace_for_block(1000).unwrap(), 1);
    }

    #[test]
    fn test_address_hash() {
        let addr = Address::repeat_byte(0x42);
        let hash = AddressHash::from(addr);
        assert_eq!(hash.as_b256(), &keccak256(addr));
    }

    #[test]
    fn test_namespaced_key_compact() {
        let key = NamespacedAccountKey::new(5, AddressHash::from_hash(B256::repeat_byte(0xAB)));

        let mut buf = Vec::with_capacity(64);
        let len = key.to_compact(&mut buf);
        assert_eq!(len, 33);
        assert_eq!(buf.len(), 33); // Data was appended

        let (decoded, _) = NamespacedAccountKey::from_compact(&buf, len);
        assert_eq!(decoded, key);
    }
}
