//! Types for MDBX state storage with compact encoding.
//!
//! This module defines all the core types used throughout the state storage system,
//! including:
//!
//! - **Address hashing**: `AddressHash` for keccak256-hashed addresses
//! - **Composite keys**: `NamespacedAccountKey`, `NamespacedStorageKey` for state lookups
//! - **Value types**: `AccountData`, `StorageValue`, `BlockMetadata`
//! - **State updates**: `AccountState`, `BlockStateUpdate` for commits and diffs
//! - **Binary diffs**: `BinaryStateDiff`, `BinaryAccountDiff` for fast serialization
//!
//! ## Encoding Strategy
//!
//! All types implement `reth_codecs::Compact` for efficient binary serialization:
//!
//! ```text
//! NamespacedStorageKey (64 bytes):
//! ┌─────────────────────────────────────────┬─────────────────────────────────────────┐
//! │           address_hash(32)              │           slot_hash(32)                 │
//! └─────────────────────────────────────────┴─────────────────────────────────────────┘
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

/// Key for account lookups.
///
/// # Binary Layout
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │           address_hash(32)              │
/// └─────────────────────────────────────────┘
/// Total: 32 bytes
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedAccountKey {
    pub address_hash: AddressHash,
}

impl NamespacedAccountKey {
    #[must_use]
    pub const fn new(address_hash: AddressHash) -> Self {
        Self { address_hash }
    }
}

impl Compact for NamespacedAccountKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.address_hash.0.as_ref());
        32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let address_hash = AddressHash(B256::from_slice(&buf[..32]));
        (Self { address_hash }, &buf[32..])
    }
}

/// Key for storage lookups: (`address_hash`, `slot_hash`).
///
/// # Binary Layout
///
/// ```text
/// ┌─────────────────────────────────────────┬─────────────────────────────────────────┐
/// │           address_hash(32)              │           slot_hash(32)                 │
/// └─────────────────────────────────────────┴─────────────────────────────────────────┘
/// Total: 64 bytes
/// ```
///
/// This layout enables efficient cursor scans:
/// - All storage for account A: seek to `[A, 0x00...]`
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedStorageKey {
    pub address_hash: AddressHash,
    pub slot_hash: B256,
}

impl NamespacedStorageKey {
    #[must_use]
    pub const fn new(address_hash: AddressHash, slot_hash: B256) -> Self {
        Self {
            address_hash,
            slot_hash,
        }
    }

    /// Create the prefix key for iterating all slots of an account.
    #[must_use]
    pub const fn account_prefix(address_hash: AddressHash) -> NamespacedAccountKey {
        NamespacedAccountKey { address_hash }
    }
}

impl Compact for NamespacedStorageKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.address_hash.0.as_ref());
        buf.put_slice(self.slot_hash.as_ref());
        64
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let address_hash = AddressHash(B256::from_slice(&buf[..32]));
        let slot_hash = B256::from_slice(&buf[32..64]);
        (
            Self {
                address_hash,
                slot_hash,
            },
            &buf[64..],
        )
    }
}

/// Key for bytecode lookups: `code_hash`.
///
/// # Binary Layout
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │             code_hash(32)               │
/// └─────────────────────────────────────────┘
/// Total: 32 bytes
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct NamespacedBytecodeKey {
    pub code_hash: B256,
}

impl NamespacedBytecodeKey {
    #[must_use]
    pub const fn new(code_hash: B256) -> Self {
        Self { code_hash }
    }
}

impl Compact for NamespacedBytecodeKey {
    fn to_compact<B>(&self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        buf.put_slice(self.code_hash.as_ref());
        32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let code_hash = B256::from_slice(&buf[..32]);
        (Self { code_hash }, &buf[32..])
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
    fn test_address_hash() {
        let addr = Address::repeat_byte(0x42);
        let hash = AddressHash::from(addr);
        assert_eq!(hash.as_b256(), &keccak256(addr));
    }

    #[test]
    fn test_namespaced_key_compact() {
        let key = NamespacedAccountKey::new(AddressHash::from_hash(B256::repeat_byte(0xAB)));

        let mut buf = Vec::with_capacity(64);
        let len = key.to_compact(&mut buf);
        assert_eq!(len, 32);
        assert_eq!(buf.len(), 32);

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
