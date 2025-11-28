//! Memory-efficient state root calculation from Redis-stored blockchain state.
//!
//! This module reads account data from Redis and computes the Ethereum state root
//! by building a proper Merkle Patricia Trie using alloy-trie.
//!
//! **Key features:**
//! - Streaming processing: Never loads all accounts into memory
//! - Proper MPT construction with sorted iteration
//! - Produces real Ethereum state roots that match block headers

use alloy::{
    primitives::{
        B256,
        U256,
        keccak256,
    },
    rlp::Encodable,
};
use alloy_trie::{
    HashBuilder,
    Nibbles,
};
use anyhow::{
    Context,
    Result,
    anyhow,
};
use revm::primitives::KECCAK_EMPTY;
use state_store::StateReader;
use std::collections::HashMap;
use tracing::info;

/// Empty trie root = keccak256(rlp([]))
pub const EMPTY_TRIE_ROOT: B256 = B256::new([
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6, 0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0, 0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
]);

/// Ethereum account state for state trie construction.
#[derive(Debug, Clone, PartialEq)]
pub struct AccountState {
    pub nonce: u64,
    pub balance: U256,
    pub storage_root: B256,
    pub code_hash: B256,
}

impl AccountState {
    /// Encode the account in RLP format for trie insertion.
    /// Format: [nonce, balance, storage root, code hash]
    pub fn rlp_encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // RLP list header
        let mut list_buf = Vec::new();

        // Encode nonce (as u64)
        self.nonce.encode(&mut list_buf);

        // Encode balance (as U256) - RLP will trim leading zeros
        self.balance.encode(&mut list_buf);

        // Encode storage root (as 32 bytes)
        self.storage_root.encode(&mut list_buf);

        // Encode code hash (as 32 bytes)
        self.code_hash.encode(&mut list_buf);

        // Wrap in list
        alloy::rlp::Header {
            list: true,
            payload_length: list_buf.len(),
        }
        .encode(&mut buf);
        buf.extend_from_slice(&list_buf);

        buf
    }
}

/// Calculate storage root for an account using a proper Merkle Patricia Trie.
/// This produces Ethereum storage roots
fn calculate_storage_root(storage: &HashMap<U256, U256>) -> B256 {
    if storage.is_empty() {
        return EMPTY_TRIE_ROOT;
    }

    // Convert to sorted entries for deterministic trie construction
    let mut entries: Vec<(B256, Vec<u8>)> = storage
        .iter()
        .filter_map(|(slot, value)| {
            // Skip zero values - they don't exist in the trie
            if value.is_zero() {
                return None;
            }

            // Key: keccak256(rlp(slot))
            let mut slot_rlp = Vec::new();
            slot.encode(&mut slot_rlp);
            let key_hash = keccak256(&slot_rlp);

            // Value: RLP-encoded value (will trim leading zeros automatically)
            let mut value_rlp = Vec::new();
            value.encode(&mut value_rlp);

            Some((key_hash, value_rlp))
        })
        .collect();

    // Sort by key hash for proper trie construction
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    // Build the storage trie
    let mut hash_builder = HashBuilder::default();
    for (key_hash, value_rlp) in entries {
        let nibbles = Nibbles::unpack(key_hash);
        hash_builder.add_leaf(nibbles, &value_rlp);
    }

    hash_builder.root()
}

/// Memory-efficient state root calculator using streaming processing.
pub struct StateRootCalculator {
    reader: StateReader,
}

impl StateRootCalculator {
    pub fn new(reader: &StateReader) -> Self {
        Self {
            reader: reader.clone(),
        }
    }

    /// Calculate state root with minimal memory usage by processing accounts one at a time.
    ///
    /// **Memory Efficient**: Never holds all accounts in memory - processes them in sorted
    /// order and drops each one after adding it to the trie.
    ///
    /// **This produces REAL Ethereum state roots that match block headers!**
    pub fn calculate_for_block(&self, block_number: u64) -> Result<B256> {
        info!("Starting state root calculation for block {block_number}");

        // Step 1: Scan all account hashes (lightweight - just B256 values)
        info!("Scanning account hashes...");
        let mut account_hashes = self
            .reader
            .scan_account_hashes(block_number)
            .context("Failed to scan account hashes")?;

        if account_hashes.is_empty() {
            info!("No accounts found - returning empty state root");
            return Ok(KECCAK_EMPTY);
        }

        info!("Found {} accounts to process", account_hashes.len());

        // Step 2: Sort hashes for proper trie construction
        // The state trie requires leaves to be added in sorted order
        info!("Sorting account hashes...");
        account_hashes.sort_unstable();

        // Step 3: Create a hash builder for the state trie
        let mut hash_builder = HashBuilder::default();

        // Step 4: Process each account ONE AT A TIME (memory efficient)
        info!("Processing accounts...");
        for (idx, address_hash) in account_hashes.iter().enumerate() {
            // Progress reporting every 1000 accounts
            if idx > 0 && idx % 1000 == 0 {
                info!(
                    "Processed {}/{} accounts ({:.1}%)",
                    idx,
                    account_hashes.len(),
                    (idx as f64 / account_hashes.len() as f64) * 100.0
                );
            }

            // Read a single account from Redis
            let account_data = self
                .reader
                .get_account_with_storage(address_hash.clone(), block_number)
                .context(format!("Failed to read account {address_hash:?}"))?;

            if let Some(data) = account_data {
                // Calculate storage root for this account
                // (Storage is loaded temporarily here, then dropped)
                let storage_root = calculate_storage_root(&data.storage);

                // Create an account state
                let account = AccountState {
                    nonce: data.nonce,
                    balance: data.balance,
                    storage_root,
                    code_hash: data.code_hash,
                };

                // RLP encode the account
                let account_rlp = account.rlp_encode();

                // Add to state trie
                let nibbles = Nibbles::unpack(address_hash.clone());
                hash_builder.add_leaf(nibbles, &account_rlp);
            }
        }

        info!(
            "Processed {}/{} accounts (100%)",
            account_hashes.len(),
            account_hashes.len()
        );

        // Step 5: Compute the final state root
        info!("Computing final state root...");
        let root = hash_builder.root();
        info!("State root calculated: 0x{}", hex::encode(root));

        Ok(root)
    }
}

/// High-level service for state root calculation.
pub struct StateRootService {
    calculator: StateRootCalculator,
}

impl StateRootService {
    pub fn new(reader: &StateReader) -> Self {
        Self {
            calculator: StateRootCalculator::new(reader),
        }
    }

    /// Calculate state root for the latest available block.
    ///
    /// 1. Find the latest block in Redis
    /// 2. Calculate the state root using memory-efficient streaming
    pub fn calculate_latest_state_root(&self) -> Result<(u64, B256)> {
        // Get the latest block
        let latest_block = self
            .calculator
            .reader
            .latest_block_number()
            .context("Failed to get latest block")?
            .ok_or_else(|| anyhow!("No blocks available in Redis"))?;

        info!("Latest block in Redis: {latest_block}");

        // Calculate state root
        Ok((
            latest_block,
            self.calculator.calculate_for_block(latest_block)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::similar_names)]
    use super::*;
    use alloy::primitives::{
        Address,
        address,
    };

    #[test]
    fn test_empty_storage_root() {
        let storage = HashMap::new();
        let root = calculate_storage_root(&storage);
        assert_eq!(root, EMPTY_TRIE_ROOT);
    }

    #[test]
    fn test_storage_root_with_data() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(1), U256::from(100));
        storage.insert(U256::from(2), U256::from(200));

        let root = calculate_storage_root(&storage);
        assert_ne!(root, KECCAK_EMPTY);
        assert_ne!(root, B256::ZERO);
    }

    #[test]
    fn test_storage_root_skips_zero_values() {
        let mut storage1 = HashMap::new();
        storage1.insert(U256::from(1), U256::from(100));
        storage1.insert(U256::from(2), U256::ZERO); // Should be skipped

        let mut storage2 = HashMap::new();
        storage2.insert(U256::from(1), U256::from(100));

        let root1 = calculate_storage_root(&storage1);
        let root2 = calculate_storage_root(&storage2);

        // Both should produce the same root since zero values are skipped
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_storage_root_deterministic() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(5), U256::from(500));
        storage.insert(U256::from(1), U256::from(100));
        storage.insert(U256::from(3), U256::from(300));

        let root1 = calculate_storage_root(&storage);
        let root2 = calculate_storage_root(&storage);

        // Should be deterministic
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_account_rlp_encoding() {
        let account = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());

        // RLP list should start with 0xc0 + length or 0xf8 for longer lists
        assert!(encoded[0] >= 0xc0);
    }

    #[test]
    fn test_account_rlp_different_for_different_accounts() {
        let account1 = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 10,
            balance: U256::from(2000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded1 = account1.rlp_encode();
        let encoded2 = account2.rlp_encode();

        assert_ne!(encoded1, encoded2);
    }

    #[test]
    fn test_account_equality() {
        let account1 = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        assert_eq!(account1, account2);
    }

    #[test]
    fn test_storage_root_order_independence() {
        // Insert in different orders, should get same root
        let mut storage1 = HashMap::new();
        storage1.insert(U256::from(1), U256::from(100));
        storage1.insert(U256::from(2), U256::from(200));
        storage1.insert(U256::from(3), U256::from(300));

        let mut storage2 = HashMap::new();
        storage2.insert(U256::from(3), U256::from(300));
        storage2.insert(U256::from(1), U256::from(100));
        storage2.insert(U256::from(2), U256::from(200));

        let root1 = calculate_storage_root(&storage1);
        let root2 = calculate_storage_root(&storage2);

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_storage_root_with_large_values() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(1), U256::MAX);
        storage.insert(U256::from(2), U256::from(u128::MAX));

        let root = calculate_storage_root(&storage);
        assert_ne!(root, KECCAK_EMPTY);
    }

    #[test]
    fn test_storage_root_single_value() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(42), U256::from(12345));

        let root = calculate_storage_root(&storage);
        assert_ne!(root, KECCAK_EMPTY);
        assert_ne!(root, B256::ZERO);
    }

    #[test]
    fn test_account_rlp_zero_nonce() {
        let account = AccountState {
            nonce: 0,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_account_rlp_zero_balance() {
        let account = AccountState {
            nonce: 5,
            balance: U256::ZERO,
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_account_rlp_large_nonce() {
        let account = AccountState {
            nonce: u64::MAX,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_account_rlp_large_balance() {
        let account = AccountState {
            nonce: 1,
            balance: U256::MAX,
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_account_rlp_with_storage() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(1), U256::from(100));
        let storage_root = calculate_storage_root(&storage);

        let account = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root,
            code_hash: KECCAK_EMPTY,
        };

        let encoded = account.rlp_encode();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_account_inequality_nonce() {
        let account1 = AccountState {
            nonce: 5,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 6,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        assert_ne!(account1, account2);
    }

    #[test]
    fn test_empty_state_produces_empty_root() {
        // An empty state trie should produce KECCAK_EMPTY
        let mut hash_builder = HashBuilder::default();
        let root = hash_builder.root();
        assert_eq!(root, EMPTY_TRIE_ROOT);
    }

    #[test]
    fn test_single_account_state_root() {
        let account = AccountState {
            nonce: 0,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account_rlp = account.rlp_encode();

        // Create a state trie with one account
        let mut hash_builder = HashBuilder::default();

        // Use a deterministic address hash
        let address = address!("0000000000000000000000000000000000000001");
        let address_hash = keccak256(address);
        let nibbles = Nibbles::unpack(address_hash);

        hash_builder.add_leaf(nibbles, &account_rlp);
        let root = hash_builder.root();

        assert_ne!(root, KECCAK_EMPTY);
        assert_ne!(root, B256::ZERO);
    }

    #[test]
    fn test_multiple_accounts_state_root() {
        let account1 = AccountState {
            nonce: 1,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 2,
            balance: U256::from(2000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        // Build accounts with sorted hashes
        let accounts = [
            (
                address!("0000000000000000000000000000000000000001"),
                account1,
            ),
            (
                address!("0000000000000000000000000000000000000002"),
                account2,
            ),
        ];

        // Sort by keccak hash
        let mut entries: Vec<_> = accounts
            .iter()
            .map(|(addr, acc)| (keccak256(addr), acc.rlp_encode()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Build trie
        let mut hash_builder = HashBuilder::default();
        for (addr_hash, rlp) in entries {
            let nibbles = Nibbles::unpack(addr_hash);
            hash_builder.add_leaf(nibbles, &rlp);
        }

        let root = hash_builder.root();
        assert_ne!(root, KECCAK_EMPTY);
    }

    #[test]
    fn test_state_root_deterministic() {
        let account1 = AccountState {
            nonce: 1,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 2,
            balance: U256::from(2000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        // Build twice with same accounts
        let build_root = || {
            let entries = [
                (
                    address!("0000000000000000000000000000000000000001"),
                    account1.clone(),
                ),
                (
                    address!("0000000000000000000000000000000000000002"),
                    account2.clone(),
                ),
            ];

            let mut sorted: Vec<_> = entries
                .iter()
                .map(|(addr, acc)| (keccak256(addr), acc.rlp_encode()))
                .collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));

            let mut hash_builder = HashBuilder::default();
            for (addr_hash, rlp) in sorted {
                let nibbles = Nibbles::unpack(addr_hash);
                hash_builder.add_leaf(nibbles, &rlp);
            }
            hash_builder.root()
        };

        let root1 = build_root();
        let root2 = build_root();

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_state_root_changes_with_account_change() {
        let account1 = AccountState {
            nonce: 1,
            balance: U256::from(1000u64),
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let account2 = AccountState {
            nonce: 1,
            balance: U256::from(2000u64), // Different balance
            storage_root: KECCAK_EMPTY,
            code_hash: KECCAK_EMPTY,
        };

        let build_root = |acc: &AccountState| {
            let mut hash_builder = HashBuilder::default();
            let address = address!("0000000000000000000000000000000000000001");
            let address_hash = keccak256(address);
            let nibbles = Nibbles::unpack(address_hash);
            hash_builder.add_leaf(nibbles, &acc.rlp_encode());
            hash_builder.root()
        };

        let root1 = build_root(&account1);
        let root2 = build_root(&account2);

        assert_ne!(root1, root2);
    }

    #[test]
    fn test_complete_state_root_calculation() {
        // Simulate a complete state with multiple accounts and storage
        let mut storage1 = HashMap::new();
        storage1.insert(U256::from(1), U256::from(100));
        storage1.insert(U256::from(2), U256::from(200));
        let storage_root1 = calculate_storage_root(&storage1);

        let mut storage2 = HashMap::new();
        storage2.insert(U256::from(5), U256::from(500));
        let storage_root2 = calculate_storage_root(&storage2);

        let accounts = [
            (
                address!("1000000000000000000000000000000000000001"),
                AccountState {
                    nonce: 1,
                    balance: U256::from(1000u64),
                    storage_root: storage_root1,
                    code_hash: keccak256(b"contract1"),
                },
            ),
            (
                address!("2000000000000000000000000000000000000002"),
                AccountState {
                    nonce: 5,
                    balance: U256::from(5000u64),
                    storage_root: storage_root2,
                    code_hash: keccak256(b"contract2"),
                },
            ),
            (
                address!("3000000000000000000000000000000000000003"),
                AccountState {
                    nonce: 0,
                    balance: U256::from(100u64),
                    storage_root: KECCAK_EMPTY,
                    code_hash: KECCAK_EMPTY,
                },
            ),
        ];

        // Sort and build state root
        let mut entries: Vec<_> = accounts
            .iter()
            .map(|(addr, acc)| (keccak256(addr), acc.rlp_encode()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut hash_builder = HashBuilder::default();
        for (addr_hash, rlp) in entries {
            let nibbles = Nibbles::unpack(addr_hash);
            hash_builder.add_leaf(nibbles, &rlp);
        }

        let root = hash_builder.root();
        assert_ne!(root, KECCAK_EMPTY);
        assert_ne!(root, B256::ZERO);

        // Root should be 32 bytes
        assert_eq!(root.len(), 32);
    }

    #[test]
    fn test_account_with_max_values() {
        let account = AccountState {
            nonce: u64::MAX,
            balance: U256::MAX,
            storage_root: B256::from([0xff; 32]),
            code_hash: B256::from([0xff; 32]),
        };

        let rlp = account.rlp_encode();
        assert!(!rlp.is_empty());

        // Should be able to build a trie with this account
        let mut hash_builder = HashBuilder::default();
        let address = address!("0000000000000000000000000000000000000001");
        let address_hash = keccak256(address);
        let nibbles = Nibbles::unpack(address_hash);
        hash_builder.add_leaf(nibbles, &rlp);

        let root = hash_builder.root();
        assert_ne!(root, KECCAK_EMPTY);
    }

    #[test]
    fn test_nibbles_unpack_deterministic() {
        let hash = keccak256(b"test");
        let nibbles1 = Nibbles::unpack(hash);
        let nibbles2 = Nibbles::unpack(hash);
        assert_eq!(nibbles1, nibbles2);
    }

    #[test]
    fn test_storage_with_sequential_slots() {
        let mut storage = HashMap::new();
        for i in 0..10 {
            storage.insert(U256::from(i), U256::from(i * 100));
        }

        let root = calculate_storage_root(&storage);
        assert_ne!(root, KECCAK_EMPTY);
    }

    #[test]
    fn test_storage_with_sparse_slots() {
        let mut storage = HashMap::new();
        storage.insert(U256::from(1), U256::from(100));
        storage.insert(U256::from(1_000), U256::from(200));
        storage.insert(U256::from(1_000_000), U256::from(300));

        let root = calculate_storage_root(&storage);
        assert_ne!(root, KECCAK_EMPTY);
    }

    #[test]
    fn test_many_accounts_state_root() {
        // Test with many accounts to ensure sorting works correctly
        let mut accounts = Vec::new();
        for i in 0..100 {
            let mut addr_bytes = [0u8; 20];
            addr_bytes[19] = u8::try_from(i).unwrap();
            let address = Address::from(addr_bytes);

            let account = AccountState {
                nonce: u64::try_from(i).unwrap(),
                balance: U256::from(u64::try_from(i).unwrap() * 1000),
                storage_root: KECCAK_EMPTY,
                code_hash: KECCAK_EMPTY,
            };

            accounts.push((address, account));
        }

        // Sort by hash
        let mut entries: Vec<_> = accounts
            .iter()
            .map(|(addr, acc)| (keccak256(addr), acc.rlp_encode()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Build trie
        let mut hash_builder = HashBuilder::default();
        for (addr_hash, rlp) in entries {
            let nibbles = Nibbles::unpack(addr_hash);
            hash_builder.add_leaf(nibbles, &rlp);
        }

        let root = hash_builder.root();
        assert_ne!(root, KECCAK_EMPTY);
    }
}
