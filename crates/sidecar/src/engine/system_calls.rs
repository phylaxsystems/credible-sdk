//! # System Calls Module
//!
//! This module implements EIP-2935 and EIP-4788 system contract updates.
//! These are critical for maintaining state parity between the sidecar and mainnet.
//!
//! ## EIP-2935: Historical Block Hashes in State (Prague)
//! Stores the last 8191 block hashes in a ring buffer at a system contract.
//! - Contract: `0x0000F90827F1C53a10cb7A02335B175320002935`
//! - Updated at the start of each block with the parent block hash
//!
//! ## EIP-4788: Beacon Block Root in the EVM (Cancun)
//! Stores beacon chain block roots for trust-minimized consensus layer access.
//! - Contract: `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`
//! - Updated at the start of each block with the parent beacon block root

use alloy::{
    eips::{
        eip2935::{
            HISTORY_SERVE_WINDOW,
            HISTORY_STORAGE_ADDRESS,
            HISTORY_STORAGE_CODE,
        },
        eip4788::{
            BEACON_ROOTS_ADDRESS,
            BEACON_ROOTS_CODE,
        },
    },
    primitives::{
        Address,
        B256,
        Bytes,
        U256,
        address,
        keccak256,
    },
};
use assertion_executor::db::BlockHashStore;
use revm::{
    DatabaseCommit,
    DatabaseRef,
    bytecode::Bytecode,
    context::BlockEnv,
    primitives::hardfork::SpecId,
    state::{
        Account,
        AccountInfo,
        AccountStatus,
        EvmState,
        EvmStorage,
        EvmStorageSlot,
    },
};
use std::sync::Arc;
use thiserror::Error;
use tracing::{
    debug,
    info,
    trace,
    warn,
};

/// The length of the ring buffer for storing beacon roots.
pub const HISTORY_BUFFER_LENGTH: u64 = 8191;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum SystemCallError {
    #[error("Missing parent beacon block root for EIP-4788")]
    MissingParentBeaconBlockRoot,
    #[error("Genesis block cannot have non-zero parent beacon root")]
    GenesisNonZeroBeaconRoot,
}

/// Configuration for applying system calls at the start of a block.
#[derive(Debug, Clone)]
pub struct SystemCallsConfig {
    /// The current hardfork spec ID
    pub spec_id: SpecId,
    /// Current block number
    pub block_number: U256,
    /// Current block timestamp
    pub timestamp: U256,
    /// Current block hash (required for EIP-2935)
    pub block_hash: B256,
    /// Parent beacon block root (required for EIP-4788)
    /// This comes from the consensus layer
    pub parent_beacon_block_root: Option<B256>,
}

impl SystemCallsConfig {
    /// Creates a new configuration from block environment and additional data.
    pub fn new(
        spec_id: SpecId,
        block_number: U256,
        timestamp: U256,
        block_hash: B256,
        beacon_block_root: Option<B256>,
    ) -> Self {
        Self {
            spec_id,
            block_number,
            timestamp,
            block_hash,
            parent_beacon_block_root: beacon_block_root,
        }
    }
}

/// Extension trait for `SpecId` to check hardfork activation.
pub trait SpecIdExt {
    /// Returns true if the Cancun hardfork is active (EIP-4788 enabled).
    fn is_cancun_active(&self) -> bool;

    /// Returns true if the Prague hardfork is active (EIP-2935 enabled).
    fn is_prague_active(&self) -> bool;
}

impl SpecIdExt for SpecId {
    #[inline]
    fn is_cancun_active(&self) -> bool {
        *self >= SpecId::CANCUN
    }

    #[inline]
    fn is_prague_active(&self) -> bool {
        *self >= SpecId::PRAGUE
    }
}

/// Handles EIP-2935 and EIP-4788 system contract updates.
#[derive(Debug, Clone, Default)]
pub struct SystemCalls;

impl SystemCalls {
    /// Creates a new `SystemCalls` instance.
    pub fn new() -> Self {
        Self
    }

    /// Applies all applicable system calls for the given configuration.
    ///
    /// This should be called at the start of each block iteration,
    /// before processing any user transactions.
    pub fn apply_system_calls<DB: DatabaseRef + DatabaseCommit + BlockHashStore>(
        &self,
        config: &SystemCallsConfig,
        db: &mut DB,
    ) -> Result<(), SystemCallError> {
        // Cache block hash for BLOCKHASH opcode lookups, all forks.
        if config.block_number > U256::ZERO {
            let block_hash = config.block_hash;
            let block_number: u64 = config.block_number.saturating_to::<u64>(); // Should not realistically overflow
            db.store_block_hash(block_number, block_hash);
            trace!(
                target = "system_calls",
                block_number = %block_number,
                %block_hash,
                "Block hash cached for BLOCKHASH opcode"
            );
        }

        // Apply EIP-4788 first (Cancun+)
        if config.spec_id.is_cancun_active() {
            self.apply_eip4788(config, db)?;
            debug!(
                target = "system_calls",
                block_number = %config.block_number,
                "Applied EIP-4788 beacon root update"
            );
        }

        // Apply EIP-2935 (Prague+)
        if config.spec_id.is_prague_active() {
            self.apply_eip2935(config, db)?;
        }

        Ok(())
    }

    /// Applies the EIP-2935 block hash update.
    ///
    /// Stores the parent block hash in the history storage contract.
    pub fn apply_eip2935<DB: DatabaseRef + DatabaseCommit>(
        &self,
        config: &SystemCallsConfig,
        db: &mut DB,
    ) -> Result<(), SystemCallError> {
        // Skip genesis block
        if config.block_number == 0 {
            trace!(
                target = "system_calls",
                "Skipping EIP-2935 for genesis block"
            );
            return Ok(());
        }

        let block_hash = config.block_hash;

        let block_number = config.block_number;

        // Calculate storage slot using ring buffer modulo
        // EIP-2935: slot = (block.number - 1) % HISTORY_SERVE_WINDOW
        let slot = (block_number - U256::from(1u64)) % U256::from(HISTORY_SERVE_WINDOW);

        debug!(
            target = "system_calls",
            block_number = %config.block_number,
            %block_number,
            %slot,
            %block_hash,
            "Applying EIP-2935 block hash update"
        );

        // Fetch existing account info, or create default with system contract code
        let existing_info = db
            .basic_ref(HISTORY_STORAGE_ADDRESS)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                let code = Bytecode::new_raw(HISTORY_STORAGE_CODE.clone());
                let code_hash = keccak256(HISTORY_STORAGE_CODE.clone());
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash,
                    code: Some(code),
                }
            });

        // Build storage updates
        let mut storage = EvmStorage::default();

        // Store parent block hash at slot
        storage.insert(
            slot,
            EvmStorageSlot::new_changed(U256::ZERO, U256::from_be_bytes(block_hash.0), 0),
        );

        // Create account with existing info, just adding storage slots
        let account = Account {
            info: existing_info,
            transaction_id: 0,
            storage,
            status: AccountStatus::Touched,
        };

        // Commit to database
        let mut state = EvmState::default();
        state.insert(HISTORY_STORAGE_ADDRESS, account);
        db.commit(state);

        trace!(
            target = "system_calls",
            address = %HISTORY_STORAGE_ADDRESS,
            "EIP-2935 state committed"
        );

        Ok(())
    }

    /// Applies the EIP-4788 beacon root update.
    ///
    /// Stores the parent beacon block root in the beacon roots contract.
    /// Storage layout (dual ring buffer):
    /// - Slot `timestamp % HISTORY_BUFFER_LENGTH`: timestamp (for verification)
    /// - Slot `timestamp % HISTORY_BUFFER_LENGTH + HISTORY_BUFFER_LENGTH`: beacon root
    pub fn apply_eip4788<DB: DatabaseRef + DatabaseCommit>(
        &self,
        config: &SystemCallsConfig,
        db: &mut DB,
    ) -> Result<(), SystemCallError> {
        // Skip genesis block
        if config.block_number == 0 {
            // Verify genesis has zero beacon root if provided
            if let Some(root) = config.parent_beacon_block_root
                && !root.is_zero()
            {
                return Err(SystemCallError::GenesisNonZeroBeaconRoot);
            }
            // Genesis block is valid, no need to commit anything
            return Ok(());
        }

        let parent_beacon_root = config
            .parent_beacon_block_root
            .ok_or(SystemCallError::MissingParentBeaconBlockRoot)?;

        let timestamp = config.timestamp;

        // Calculate storage slots using ring buffer
        let timestamp_index = timestamp % U256::from(HISTORY_BUFFER_LENGTH);
        let timestamp_slot = U256::from(timestamp_index);
        let root_slot = U256::from(timestamp_index + U256::from(HISTORY_BUFFER_LENGTH));

        debug!(
            target = "system_calls",
            block_number = %config.block_number,
            %timestamp,
            %timestamp_index,
            %parent_beacon_root,
            "Applying EIP-4788 beacon root update"
        );

        // Fetch existing account info, or create default with system contract code
        let existing_info = db
            .basic_ref(BEACON_ROOTS_ADDRESS)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                let code = Bytecode::new_raw(BEACON_ROOTS_CODE.clone());
                let code_hash = keccak256(BEACON_ROOTS_CODE.clone());
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash,
                    code: Some(code),
                }
            });

        // Build storage updates
        let mut storage = EvmStorage::default();

        // Store timestamp at timestamp_slot (for verification during reads)
        storage.insert(
            timestamp_slot,
            EvmStorageSlot::new_changed(U256::ZERO, U256::from(timestamp), 0),
        );

        // Store beacon root at root_slot
        storage.insert(
            root_slot,
            EvmStorageSlot::new_changed(U256::ZERO, U256::from_be_bytes(parent_beacon_root.0), 0),
        );

        // Create account with existing info, just adding storage slots
        let account = Account {
            info: existing_info,
            transaction_id: 0,
            storage,
            status: AccountStatus::Touched,
        };

        // Commit to database
        let mut state = EvmState::default();
        state.insert(BEACON_ROOTS_ADDRESS, account);
        db.commit(state);

        trace!(
            target = "system_calls",
            address = %BEACON_ROOTS_ADDRESS,
            "EIP-4788 state committed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::database::DBErrorMarker;
    use std::{
        cell::RefCell,
        collections::HashMap,
    };

    /// Simple mock database for testing
    #[derive(Debug, Default)]
    struct MockDb {
        accounts: HashMap<Address, Account>,
        block_hash_cache: RefCell<HashMap<u64, B256>>,
    }

    #[derive(Error, Debug)]
    pub enum MockError {
        #[error("Block hash not found for block {0}")]
        BlockHashNotFound(u64),
    }

    impl DBErrorMarker for MockError {}

    impl DatabaseRef for MockDb {
        type Error = MockError;

        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(self.accounts.get(&address).map(|acc| acc.info.clone()))
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }

        fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
            Ok(self
                .accounts
                .get(&address)
                .and_then(|acc| acc.storage.get(&index))
                .map_or(U256::ZERO, |slot| slot.present_value))
        }

        fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
            self.block_hash_cache
                .borrow()
                .get(&number)
                .copied()
                .ok_or(MockError::BlockHashNotFound(number))
        }
    }

    impl DatabaseCommit for MockDb {
        fn commit(&mut self, changes: EvmState) {
            for (address, account) in changes {
                self.accounts.insert(address, account);
            }
        }
    }

    impl BlockHashStore for MockDb {
        fn store_block_hash(&self, number: u64, hash: B256) {
            self.block_hash_cache.borrow_mut().insert(number, hash);
        }
    }

    #[test]
    fn test_eip2935_stores_hash() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let hash = B256::repeat_byte(0xab);
        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(99),
            timestamp: U256::from(1234567890),
            block_hash: hash,
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        // Verify contract was created
        let account = db.accounts.get(&HISTORY_STORAGE_ADDRESS);
        assert!(account.is_some(), "History storage contract should exist");

        let account = account.unwrap();

        // Verify storage slot
        // EIP-2935: slot = (block_number - 1) % HISTORY_SERVE_WINDOW = 98 % 8191 = 98
        let slot = U256::from(98u64);

        assert!(account.storage.contains_key(&slot), "Slot should exist");

        // Verify stored value
        let stored_hash = account.storage.get(&slot).unwrap();
        assert_eq!(stored_hash.present_value, U256::from_be_bytes(hash.0));
    }

    #[test]
    fn test_eip4788_stores_beacon_root() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let beacon_root = B256::repeat_byte(0xcd);
        let timestamp = 1700000000u64;

        let config = SystemCallsConfig {
            spec_id: SpecId::CANCUN,
            block_number: U256::from(100),
            timestamp: U256::from(timestamp),
            block_hash: B256::ZERO,
            parent_beacon_block_root: Some(beacon_root),
        };

        let result = system_calls.apply_eip4788(&config, &mut db);
        assert!(result.is_ok());

        // Verify contract was created
        let account = db.accounts.get(&BEACON_ROOTS_ADDRESS);
        assert!(account.is_some(), "Beacon roots contract should exist");

        let account = account.unwrap();

        // Verify storage slots
        let timestamp_index = timestamp % HISTORY_BUFFER_LENGTH;
        let timestamp_slot = U256::from(timestamp_index);
        let root_slot = U256::from(timestamp_index + HISTORY_BUFFER_LENGTH);

        assert!(account.storage.contains_key(&timestamp_slot));
        assert!(account.storage.contains_key(&root_slot));

        // Verify stored values
        let stored_timestamp = account.storage.get(&timestamp_slot).unwrap();
        assert_eq!(stored_timestamp.present_value, U256::from(timestamp));

        let stored_root = account.storage.get(&root_slot).unwrap();
        assert_eq!(
            stored_root.present_value,
            U256::from_be_bytes(beacon_root.0)
        );
    }

    #[test]
    fn test_genesis_block_skipped() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(0),
            timestamp: U256::from(0),
            block_hash: B256::ZERO,
            parent_beacon_block_root: Some(B256::ZERO),
        };

        // EIP-2935 should succeed but not modify state
        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());
        assert!(!db.accounts.contains_key(&HISTORY_STORAGE_ADDRESS));

        // EIP-4788 should return Ok(()): genesis block
        let result = system_calls.apply_eip4788(&config, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_spec_id_checks() {
        assert!(!SpecId::SHANGHAI.is_cancun_active());
        assert!(SpecId::CANCUN.is_cancun_active());
        assert!(SpecId::PRAGUE.is_cancun_active());

        assert!(!SpecId::CANCUN.is_prague_active());
        assert!(SpecId::PRAGUE.is_prague_active());
    }

    #[test]
    fn test_apply_all_system_calls() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(100),
            timestamp: U256::from(1700000000),
            block_hash: B256::repeat_byte(0xab),
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        let result = system_calls.apply_system_calls(&config, &mut db);
        assert!(result.is_ok());

        // Both contracts should be present
        assert!(db.accounts.contains_key(&HISTORY_STORAGE_ADDRESS));
        assert!(db.accounts.contains_key(&BEACON_ROOTS_ADDRESS));
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Test with block number that wraps around the ring buffer
        let hash = B256::repeat_byte(0xff);
        let block_number = HISTORY_SERVE_WINDOW + 99; // Will wrap to slot 99

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(block_number),
            timestamp: U256::from(1234567890),
            block_hash: hash,
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        let account = db.accounts.get(&HISTORY_STORAGE_ADDRESS).unwrap();

        // EIP-2935: slot = (block_number - 1) % HISTORY_SERVE_WINDOW = 8289 % 8191 = 98
        let expected_slot = U256::from(98u64);
        assert!(account.storage.contains_key(&expected_slot));
    }

    #[test]
    fn test_preserves_existing_account_state_eip2935() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Pre-populate the account with balance and nonce (simulating SELFDESTRUCT or prior txs)
        let existing_balance = U256::from(1_000_000_000_000_000_000u128); // 1 ETH
        let existing_nonce = 42u64;
        let existing_account = Account {
            info: AccountInfo {
                nonce: existing_nonce,
                balance: existing_balance,
                code_hash: B256::ZERO,
                code: None,
            },
            transaction_id: 0,
            storage: EvmStorage::default(),
            status: AccountStatus::LoadedAsNotExisting,
        };
        db.accounts
            .insert(HISTORY_STORAGE_ADDRESS, existing_account);

        let hash = B256::repeat_byte(0xab);
        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(100),
            timestamp: U256::from(1234567890),
            block_hash: hash,
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        // Verify account state was preserved
        let account = db.accounts.get(&HISTORY_STORAGE_ADDRESS).unwrap();
        assert_eq!(account.info.balance, existing_balance);
        assert_eq!(account.info.nonce, existing_nonce);
    }

    #[test]
    fn test_preserves_existing_account_state_eip4788() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Pre-populate the account with balance and nonce (simulating SELFDESTRUCT or prior txs)
        let existing_balance = U256::from(2_000_000_000_000_000_000u128); // 2 ETH
        let existing_nonce = 99u64;
        let existing_account = Account {
            info: AccountInfo {
                nonce: existing_nonce,
                balance: existing_balance,
                code_hash: B256::ZERO,
                code: None,
            },
            transaction_id: 0,
            storage: EvmStorage::default(),
            status: AccountStatus::LoadedAsNotExisting,
        };
        db.accounts.insert(BEACON_ROOTS_ADDRESS, existing_account);

        let beacon_root = B256::repeat_byte(0xcd);
        let config = SystemCallsConfig {
            spec_id: SpecId::CANCUN,
            block_number: U256::from(100),
            timestamp: U256::from(1700000000),
            block_hash: B256::ZERO,
            parent_beacon_block_root: Some(beacon_root),
        };

        let result = system_calls.apply_eip4788(&config, &mut db);
        assert!(result.is_ok());

        // Verify account state was preserved
        let account = db.accounts.get(&BEACON_ROOTS_ADDRESS).unwrap();
        assert_eq!(account.info.balance, existing_balance);
        assert_eq!(account.info.nonce, existing_nonce);
    }

    #[test]
    fn test_block_hash_cache_populated() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let block_hash = B256::repeat_byte(0xab);
        let config = SystemCallsConfig {
            spec_id: SpecId::SHANGHAI, // Pre-Prague, only caching should happen
            block_number: U256::from(100),
            timestamp: U256::from(1234567890),
            block_hash,
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_system_calls(&config, &mut db);
        assert!(result.is_ok());

        // Verify block hash was cached for BLOCKHASH opcode
        let cached_hash = db.block_hash_cache.borrow().get(&100).copied();
        assert_eq!(
            cached_hash,
            Some(block_hash),
            "Parent block hash should be cached at block 100"
        );
    }

    #[test]
    fn test_block_hash_cache_not_populated_for_genesis() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::SHANGHAI,
            block_number: U256::from(0), // Genesis
            timestamp: U256::from(0),
            block_hash: B256::repeat_byte(0xab),
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_system_calls(&config, &mut db);
        assert!(result.is_ok());

        // Genesis has no parent, so nothing should be cached
        assert!(
            db.block_hash_cache.borrow().is_empty(),
            "No hash should be cached for genesis"
        );
    }

    #[test]
    fn test_block_hash_cache_sequential_blocks() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Simulate processing blocks 1, 2, 3
        for block_num in 1..=3u64 {
            let hash = B256::repeat_byte(u8::try_from(block_num).unwrap());
            let config = SystemCallsConfig {
                spec_id: SpecId::SHANGHAI,
                block_number: U256::from(block_num),
                timestamp: U256::from(1234567890 + block_num),
                block_hash: hash,
                parent_beacon_block_root: None,
            };
            system_calls.apply_system_calls(&config, &mut db).unwrap();
        }

        // Verify all block hashes are cached correctly
        let cache = db.block_hash_cache.borrow();
        assert_eq!(cache.get(&1), Some(&B256::repeat_byte(1))); // Block 1's hash
        assert_eq!(cache.get(&2), Some(&B256::repeat_byte(2))); // Block 2's hash
        assert_eq!(cache.get(&3), Some(&B256::repeat_byte(3))); // Block 3's hash
    }
}
