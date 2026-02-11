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
//! - **Binary diffs**: `BinaryStateDiff`, `BinaryAccountDiff` for fast serialization
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
    Bytes,
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
    #[must_use]
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
    #[must_use]
    pub const fn new(namespace_idx: u8, address_hash: AddressHash, slot_hash: B256) -> Self {
        Self {
            namespace_idx,
            address_hash,
            slot_hash,
        }
    }

    /// Create the prefix key for iterating all slots of an account.
    #[must_use]
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
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns `IntConversion` if the namespace index does not fit into a `u8`.
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

/// Binary-encoded account diff for fast serialization.
///
/// Storage entries are pre-sorted by `slot_hash` to enable optimal
/// sequential cursor writes to MDBX.
#[derive(Debug, Clone)]
pub struct BinaryAccountDiff {
    pub address_hash: AddressHash,
    pub deleted: bool,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    pub code: Option<Bytes>,
    /// Storage entries, pre-sorted by `slot_hash` for optimal writes.
    pub storage: Vec<(B256, U256)>,
}

/// Binary-encoded state diff.
///
/// It uses a simple binary layout with no parsing overhead.
///
/// ## Binary Layout
///
/// ```text
/// ┌────────────────┬────────────────┬────────────────┬───────────────┬─────────────┐
/// │ block_number   │ block_hash     │ state_root     │ account_count │ accounts... │
/// │ (8 bytes LE)   │ (32 bytes)     │ (32 bytes)     │ (4 bytes LE)  │ (variable)  │
/// └────────────────┴────────────────┴────────────────┴───────────────┴─────────────┘
/// ```
///
/// Each account:
/// ```text
/// ┌──────────────┬─────────┬─────────┬───────┬───────────┬──────────┬──────┬───────────────┬─────────────┐
/// │ address_hash │ deleted │ balance │ nonce │ code_hash │ code_len │ code │ storage_count │ storage...  │
/// │ (32 bytes)   │ (1)     │ (32 LE) │ (8 LE)│ (32)      │ (4 LE)   │ var  │ (4 LE)        │ (64 each)   │
/// └──────────────┴─────────┴─────────┴───────┴───────────┴──────────┴──────┴───────────────┴─────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct BinaryStateDiff {
    pub block_number: u64,
    pub block_hash: B256,
    pub state_root: B256,
    pub accounts: Vec<BinaryAccountDiff>,
}

impl BinaryStateDiff {
    /// Serialize to bytes.
    ///
    /// Pre-calculates the buffer size to avoid reallocations.
    ///
    /// # Errors
    ///
    /// Returns `IntConversion` if any length does not fit into a `u32`.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StateError> {
        // Pre-calculate size to avoid reallocations
        let mut size = 8 + 32 + 32 + 4; // header: block_number + block_hash + state_root + account_count
        for acc in &self.accounts {
            size += 32 + 1 + 32 + 8 + 32; // address_hash, deleted, balance, nonce, code_hash
            size += 4; // code length
            if let Some(code) = &acc.code {
                size += code.len();
            }
            size += 4 + acc.storage.len() * 64; // storage count + entries (32 + 32 each)
        }

        let mut buf = Vec::with_capacity(size);

        // Header
        buf.extend_from_slice(&self.block_number.to_le_bytes());
        buf.extend_from_slice(self.block_hash.as_slice());
        buf.extend_from_slice(self.state_root.as_slice());
        buf.extend_from_slice(
            &(u32::try_from(self.accounts.len()).map_err(StateError::IntConversion)?).to_le_bytes(),
        );

        // Accounts
        for acc in &self.accounts {
            buf.extend_from_slice(acc.address_hash.as_b256().as_slice());
            buf.push(u8::from(acc.deleted));
            buf.extend_from_slice(&acc.balance.to_le_bytes::<32>());
            buf.extend_from_slice(&acc.nonce.to_le_bytes());
            buf.extend_from_slice(acc.code_hash.as_slice());

            // Code
            if let Some(code) = &acc.code {
                buf.extend_from_slice(
                    &(u32::try_from(code.len()).map_err(StateError::IntConversion)?).to_le_bytes(),
                );
                buf.extend_from_slice(code);
            } else {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }

            // Storage (already sorted)
            buf.extend_from_slice(
                &(u32::try_from(acc.storage.len()).map_err(StateError::IntConversion)?)
                    .to_le_bytes(),
            );
            for (slot, value) in &acc.storage {
                buf.extend_from_slice(slot.as_slice());
                buf.extend_from_slice(&value.to_le_bytes::<32>());
            }
        }

        Ok(buf)
    }

    /// Deserialize from bytes.
    ///
    /// # Errors
    ///
    /// Returns `Codec` if the data is malformed or truncated.
    pub fn from_bytes(data: &[u8]) -> Result<Self, StateError> {
        let mut pos = 0;

        // Helper to read a slice with bounds checking
        let read_slice = |pos: &mut usize, len: usize| -> Result<&[u8], StateError> {
            let end = pos.checked_add(len).ok_or_else(|| {
                StateError::Codec("Integer overflow in position calculation".to_string())
            })?;
            if end > data.len() {
                return Err(StateError::Codec(format!(
                    "Unexpected end of data: need {} bytes at position {}, but only {} bytes available",
                    len,
                    *pos,
                    data.len().saturating_sub(*pos)
                )));
            }
            let slice = &data[*pos..end];
            *pos = end;
            Ok(slice)
        };

        // Header: block_number (8) + block_hash (32) + state_root (32) + account_count (4) = 76 bytes
        let block_number = u64::from_le_bytes(
            read_slice(&mut pos, 8)?
                .try_into()
                .map_err(|_| StateError::Codec("Invalid block_number".to_string()))?,
        );

        let block_hash = B256::from_slice(read_slice(&mut pos, 32)?);

        let state_root = B256::from_slice(read_slice(&mut pos, 32)?);

        let account_count = u32::from_le_bytes(
            read_slice(&mut pos, 4)?
                .try_into()
                .map_err(|_| StateError::Codec("Invalid account_count".to_string()))?,
        ) as usize;

        // Accounts
        let mut accounts = Vec::with_capacity(account_count.min(10_000)); // Cap allocation
        for _ in 0..account_count {
            // Account header: address_hash (32) + deleted (1) + balance (32) + nonce (8) + code_hash (32) = 105 bytes
            let address_hash = AddressHash::from_hash(B256::from_slice(read_slice(&mut pos, 32)?));

            let deleted = read_slice(&mut pos, 1)?[0] != 0;

            let balance = U256::from_le_slice(read_slice(&mut pos, 32)?);

            let nonce = u64::from_le_bytes(
                read_slice(&mut pos, 8)?
                    .try_into()
                    .map_err(|_| StateError::Codec("Invalid nonce".to_string()))?,
            );

            let code_hash = B256::from_slice(read_slice(&mut pos, 32)?);

            // Code length
            let code_len = u32::from_le_bytes(
                read_slice(&mut pos, 4)?
                    .try_into()
                    .map_err(|_| StateError::Codec("Invalid code_len".to_string()))?,
            ) as usize;

            // Code bytes
            let code = if code_len > 0 {
                Some(Bytes::copy_from_slice(read_slice(&mut pos, code_len)?))
            } else {
                None
            };

            // Storage count
            let storage_count = u32::from_le_bytes(
                read_slice(&mut pos, 4)?
                    .try_into()
                    .map_err(|_| StateError::Codec("Invalid storage_count".to_string()))?,
            ) as usize;

            // Storage entries
            let mut storage = Vec::with_capacity(storage_count.min(100_000)); // Cap allocation
            for _ in 0..storage_count {
                let slot = B256::from_slice(read_slice(&mut pos, 32)?);
                let value = U256::from_le_slice(read_slice(&mut pos, 32)?);
                storage.push((slot, value));
            }

            accounts.push(BinaryAccountDiff {
                address_hash,
                deleted,
                balance,
                nonce,
                code_hash,
                code,
                storage,
            });
        }

        Ok(Self {
            block_number,
            block_hash,
            state_root,
            accounts,
        })
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
        assert_eq!(buf.len(), 33);

        let (decoded, _) = NamespacedAccountKey::from_compact(&buf, len);
        assert_eq!(decoded, key);
    }

    #[test]
    fn test_binary_state_diff_roundtrip() {
        let diff = BinaryStateDiff {
            block_number: 12345,
            block_hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x22),
            accounts: vec![
                BinaryAccountDiff {
                    address_hash: AddressHash::from_hash(B256::repeat_byte(0xAA)),
                    deleted: false,
                    balance: U256::from(1000),
                    nonce: 5,
                    code_hash: B256::repeat_byte(0xCC),
                    code: Some(Bytes::from_static(&[0x60, 0x80, 0x60, 0x40])),
                    storage: vec![
                        (B256::repeat_byte(0x01), U256::from(100)),
                        (B256::repeat_byte(0x02), U256::from(200)),
                    ],
                },
                BinaryAccountDiff {
                    address_hash: AddressHash::from_hash(B256::repeat_byte(0xBB)),
                    deleted: true,
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: vec![],
                },
            ],
        };

        let bytes = diff.to_bytes();
        let decoded = BinaryStateDiff::from_bytes(&bytes.unwrap()).unwrap();

        assert_eq!(decoded.block_number, diff.block_number);
        assert_eq!(decoded.block_hash, diff.block_hash);
        assert_eq!(decoded.state_root, diff.state_root);
        assert_eq!(decoded.accounts.len(), 2);
        assert_eq!(decoded.accounts[0].balance, U256::from(1000));
        assert_eq!(decoded.accounts[0].storage.len(), 2);
        assert!(decoded.accounts[1].deleted);
    }
}
