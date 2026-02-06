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

use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    address,
    keccak256,
};
use assertion_executor::db::BlockHashStore;
use eip_system_calls::{
    SystemContract,
    eip2935::{HISTORY_STORAGE_CODE, Eip2935},
    eip4788::{
        Eip4788,
        HISTORY_BUFFER_LENGTH,
        BEACON_ROOTS_CODE
    },
};
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
    /// Parent block hash, as we applied EIP-2935, before the tx execution.
    pub block_hash: Option<B256>,
    /// Parent beacon block root, required for EIP-4788.
    /// This comes from the consensus layer
    pub parent_beacon_block_root: Option<B256>,
}

impl SystemCallsConfig {
    /// Creates a new configuration from block environment and additional data.
    pub fn new(
        spec_id: SpecId,
        block_number: U256,
        timestamp: U256,
        block_hash: Option<B256>,
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

    /// Applies pre-transaction system calls (EIP-4788 and EIP-2935) to a database.
    /// It is  designed for `ForkDb` which doesn't implement `BlockHashStore`.
    /// It applies EIP-4788 (beacon root) and EIP-2935 (block hash storage) so that
    /// transactions executing in this iteration can query the system contracts.
    pub fn apply_pre_tx_system_calls<DB: DatabaseRef + DatabaseCommit>(
        &self,
        config: &SystemCallsConfig,
        db: &mut DB,
    ) -> Result<(), SystemCallError> {
        // Apply EIP-4788 first (Cancun+)
        if config.spec_id.is_cancun_active() {
            self.apply_eip4788(config, db)?;
            debug!(
                target = "system_calls",
                block_number = %config.block_number,
                timestamp = %config.timestamp,
                "Applied EIP-4788 beacon root update (pre-tx)"
            );
        }

        // Apply EIP-2935 (Prague+)
        if config.spec_id.is_prague_active() {
            self.apply_eip2935(config, db)?;
            debug!(
                target = "system_calls",
                block_number = %config.block_number,
                "Applied EIP-2935 block hash update (pre-tx)"
            );
        }

        Ok(())
    }

    /// Caches the block hash for BLOCKHASH opcode lookups.
    ///
    /// This should be called at commit time on the `OverlayDb` after the iteration
    /// is finalized. It doesn't modify EVM state, only the block hash cache.
    pub fn cache_block_hash<DB: BlockHashStore>(
        &self,
        block_number: U256,
        block_hash: B256,
        db: &DB,
    ) {
        if block_number > U256::ZERO {
            let block_num: u64 = block_number.saturating_to::<u64>();
            db.store_block_hash(block_num, block_hash);
            trace!(
                target = "system_calls",
                block_number = %block_num,
                %block_hash,
                "Block hash cached for BLOCKHASH opcode"
            );
        }
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
        if config.block_number == U256::ZERO {
            trace!(
                target = "system_calls",
                "Skipping EIP-2935 for genesis block"
            );
            return Ok(());
        }

        // Skip if no parent block hash provided
        let Some(block_hash) = config.block_hash else {
            warn!(
                target = "system_calls",
                "Skipping EIP-2935: no parent block hash provided"
            );
            return Ok(());
        };

        let block_number: u64 = config.block_number.saturating_to();
        // EIP-2935: At block N, store parent hash (block N-1) at slot (N-1) % HISTORY_SERVE_WINDOW
        let parent_slot = Eip2935::slot(block_number);

        debug!(
            target = "system_calls",
            block_number = %config.block_number,
            %block_number,
            %parent_slot,
            %block_hash,
            "Applying EIP-2935 block hash update"
        );

        // Fetch existing account info, or create default with system contract code
        let mut existing_info = db
            .basic_ref(Eip2935::ADDRESS)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash: Eip2935::code_hash(),
                    code: Some(Bytecode::new_raw(Eip2935::bytecode().clone())),
                }
            });

        // Overwrite the existing code in case the `basic_ref` is cached to none
        existing_info.code = Some(Bytecode::new_raw(HISTORY_STORAGE_CODE.clone()));
        existing_info.code_hash = keccak256(HISTORY_STORAGE_CODE.clone());

        // Build storage updates
        let mut storage = EvmStorage::default();
        let current_value = Eip2935::hash_to_value(block_hash);

        // Store parent block hash at slot
        storage.insert(
            parent_slot,
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
        state.insert(Eip2935::ADDRESS, account);
        db.commit(state);

        trace!(
            target = "system_calls",
            address = %Eip2935::ADDRESS,
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

        let timestamp: u64 = config.timestamp.saturating_to();
        let timestamp_slot = Eip4788::timestamp_slot(timestamp);
        let root_slot = Eip4788::root_slot(timestamp);

        debug!(
            target = "system_calls",
            block_number = %config.block_number,
            %timestamp,
            %timestamp_slot,
            %parent_beacon_root,
            "Applying EIP-4788 beacon root update"
        );

        // Fetch existing account info, or create default with system contract code
        let mut existing_info = db
            .basic_ref(Eip4788::ADDRESS)
            .ok()
            .flatten()
            .unwrap_or_else(|| {
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 1,
                    code_hash: Eip4788::code_hash(),
                    code: Some(Bytecode::new_raw(Eip4788::bytecode().clone())),
                }
            });

        // Overwrite the existing code in case the `basic_ref` is cached to none
        existing_info.code = Some(Bytecode::new_raw(BEACON_ROOTS_CODE.clone()));
        existing_info.code_hash = keccak256(BEACON_ROOTS_CODE.clone());

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
            EvmStorageSlot::new_changed(U256::ZERO, Eip4788::root_to_value(parent_beacon_root), 0),
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
        state.insert(Eip4788::ADDRESS, account);
        db.commit(state);

        trace!(
            target = "system_calls",
            address = %Eip4788::ADDRESS,
            "EIP-4788 state committed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eip_system_calls::eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_SERVE_WINDOW};
    use eip_system_calls::eip4788::BEACON_ROOTS_ADDRESS;
    use assertion_executor::db::{
        DatabaseCommit,
        RollbackDb,
        VersionDb,
    };
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
            block_hash: Some(hash),
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        // Verify contract was created
        let account = db.accounts.get(&Eip2935::ADDRESS);
        assert!(account.is_some(), "History storage contract should exist");

        let account = account.unwrap();

        // Verify storage slot
        // EIP-2935: slot = (block_number - 1) % HISTORY_SERVE_WINDOW = 98 % 8191 = 98
        let slot = U256::from(98u64);

        assert!(account.storage.contains_key(&slot), "Slot should exist");

        // Verify stored value
        let stored_hash = account.storage.get(&slot).unwrap();
        assert_eq!(stored_hash.present_value, Eip2935::hash_to_value(hash));
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
            block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(beacon_root),
        };

        let result = system_calls.apply_eip4788(&config, &mut db);
        assert!(result.is_ok());

        // Verify contract was created
        let account = db.accounts.get(&Eip4788::ADDRESS);
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
            block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(B256::ZERO),
        };

        // EIP-2935 should succeed but not modify state
        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());
        assert!(!db.accounts.contains_key(&Eip2935::ADDRESS));

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
    fn test_apply_pre_tx_system_calls() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(100),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::repeat_byte(0xab)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        let result = system_calls.apply_pre_tx_system_calls(&config, &mut db);
        assert!(result.is_ok());

        // Both contracts should be present
        assert!(db.accounts.contains_key(&Eip2935::ADDRESS));
        assert!(db.accounts.contains_key(&Eip4788::ADDRESS));
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let mut db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Test with block number that wraps around the ring buffer
        let hash = B256::repeat_byte(0xff);
        let block_number = HISTORY_SERVE_WINDOW + 100; // Will wrap to slot 99

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(block_number),
            timestamp: U256::from(1234567890),
            block_hash: Some(hash),
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        let account = db.accounts.get(&Eip2935::ADDRESS).unwrap();

        // EIP-2935: slot = (block_number - 1) % HISTORY_SERVE_WINDOW = 8290 % 8191 = 99
        let expected_slot = U256::from(99u64);
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
        db.accounts.insert(Eip2935::ADDRESS, existing_account);

        let hash = B256::repeat_byte(0xab);
        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(100),
            timestamp: U256::from(1234567890),
            block_hash: Some(hash),
            parent_beacon_block_root: None,
        };

        let result = system_calls.apply_eip2935(&config, &mut db);
        assert!(result.is_ok());

        // Verify account state was preserved
        let account = db.accounts.get(&Eip2935::ADDRESS).unwrap();
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
        db.accounts.insert(Eip4788::ADDRESS, existing_account);

        let beacon_root = B256::repeat_byte(0xcd);
        let config = SystemCallsConfig {
            spec_id: SpecId::CANCUN,
            block_number: U256::from(100),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(beacon_root),
        };

        let result = system_calls.apply_eip4788(&config, &mut db);
        assert!(result.is_ok());

        // Verify account state was preserved
        let account = db.accounts.get(&Eip4788::ADDRESS).unwrap();
        assert_eq!(account.info.balance, existing_balance);
        assert_eq!(account.info.nonce, existing_nonce);
    }

    #[test]
    fn test_block_hash_cache_populated() {
        let db = MockDb::default();
        let system_calls = SystemCalls::new();

        let block_hash = B256::repeat_byte(0xab);
        let block_number = U256::from(100);

        system_calls.cache_block_hash(block_number, block_hash, &db);

        // Verify block hash was cached for BLOCKHASH opcode
        let cached_hash = db.block_hash_cache.borrow().get(&100).copied();
        assert_eq!(
            cached_hash,
            Some(block_hash),
            "Block hash should be cached at block 100"
        );
    }

    #[test]
    fn test_block_hash_cache_not_populated_for_genesis() {
        let db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Genesis block (block 0) should not cache anything
        system_calls.cache_block_hash(U256::from(0), B256::repeat_byte(0xab), &db);

        // Genesis has no parent, so nothing should be cached
        assert!(
            db.block_hash_cache.borrow().is_empty(),
            "No hash should be cached for genesis"
        );
    }

    #[test]
    fn test_block_hash_cache_sequential_blocks() {
        let db = MockDb::default();
        let system_calls = SystemCalls::new();

        // Simulate caching block hashes for blocks 1, 2, 3
        for block_num in 1..=3u64 {
            let hash = B256::repeat_byte(u8::try_from(block_num).unwrap());
            system_calls.cache_block_hash(U256::from(block_num), hash, &db);
        }

        // Verify all block hashes are cached correctly
        let cache = db.block_hash_cache.borrow();
        assert_eq!(cache.get(&1), Some(&B256::repeat_byte(1))); // Block 1's hash
        assert_eq!(cache.get(&2), Some(&B256::repeat_byte(2))); // Block 2's hash
        assert_eq!(cache.get(&3), Some(&B256::repeat_byte(3))); // Block 3's hash
    }

    #[test]
    fn test_system_calls_via_state_mut_visible_to_reads() {
        let mock = MockDb::default();
        let mut version_db = VersionDb::new(mock);
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(100),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::repeat_byte(0xab)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xcd)),
        };

        system_calls
            .apply_pre_tx_system_calls(&config, version_db.state_mut())
            .unwrap();

        // EIP-2935 and EIP-4788 contracts readable through VersionDb
        assert!(
            version_db
                .basic_ref(HISTORY_STORAGE_ADDRESS)
                .unwrap()
                .is_some()
        );
        assert!(
            version_db
                .basic_ref(BEACON_ROOTS_ADDRESS)
                .unwrap()
                .is_some()
        );

        // Verify EIP-2935 storage (slot = (block_number - 1) % 8191 = 99)
        let slot = U256::from(99u64);
        let stored = version_db
            .storage_ref(HISTORY_STORAGE_ADDRESS, slot)
            .unwrap();
        assert_eq!(stored, U256::from_be_bytes(B256::repeat_byte(0xab).0));
    }

    #[test]
    fn test_system_calls_persist_after_tx_commit() {
        let mock = MockDb::default();
        let mut version_db = VersionDb::new(mock);
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(50),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::repeat_byte(0x11)),
            parent_beacon_block_root: Some(B256::repeat_byte(0x22)),
        };

        system_calls
            .apply_pre_tx_system_calls(&config, version_db.state_mut())
            .unwrap();

        // Commit a user transaction
        let user_addr = address!("1111111111111111111111111111111111111111");
        let mut tx_state = EvmState::default();
        tx_state.insert(
            user_addr,
            Account {
                info: AccountInfo {
                    balance: U256::from(1000),
                    nonce: 1,
                    code_hash: B256::ZERO,
                    code: None,
                },
                transaction_id: 0,
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        DatabaseCommit::commit(&mut version_db, tx_state);

        // System contracts still accessible after tx commit
        assert!(
            version_db
                .basic_ref(HISTORY_STORAGE_ADDRESS)
                .unwrap()
                .is_some()
        );
        assert!(
            version_db
                .basic_ref(BEACON_ROOTS_ADDRESS)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            version_db.basic_ref(user_addr).unwrap().unwrap().balance,
            U256::from(1000)
        );
    }

    #[test]
    fn test_rollback_loses_system_call_state() {
        // System calls applied via state_mut() are lost on rollback because
        // VersionDb rebuilds from base_state. In the engine, system calls
        // are reapplied on each new iteration after reorg.

        let mock = MockDb::default();
        let mut version_db = VersionDb::new(mock);
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(200),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::repeat_byte(0x33)),
            parent_beacon_block_root: Some(B256::repeat_byte(0x44)),
        };

        system_calls
            .apply_pre_tx_system_calls(&config, version_db.state_mut())
            .unwrap();

        // Commit two transactions
        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");

        let mut tx1 = EvmState::default();
        tx1.insert(
            addr1,
            Account {
                info: AccountInfo {
                    balance: U256::from(100),
                    nonce: 1,
                    code_hash: B256::ZERO,
                    code: None,
                },
                transaction_id: 0,
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        DatabaseCommit::commit(&mut version_db, tx1);

        let mut tx2 = EvmState::default();
        tx2.insert(
            addr2,
            Account {
                info: AccountInfo {
                    balance: U256::from(200),
                    nonce: 1,
                    code_hash: B256::ZERO,
                    code: None,
                },
                transaction_id: 0,
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        DatabaseCommit::commit(&mut version_db, tx2);

        version_db.rollback_to(1).unwrap();

        // System calls are LOST after rollback (rebuilds from base_state)
        assert!(
            version_db
                .basic_ref(HISTORY_STORAGE_ADDRESS)
                .unwrap()
                .is_none()
        );

        // tx1 visible, tx2 rolled back
        assert!(version_db.basic_ref(addr1).unwrap().is_some());
        assert!(version_db.basic_ref(addr2).unwrap().is_none());
    }

    #[test]
    fn test_different_timestamps_different_beacon_slots() {
        let mock = MockDb::default();
        let mut version_db = VersionDb::new(mock);
        let system_calls = SystemCalls::new();

        let ts1 = 1700000000u64;
        let ts2 = 1700000012u64; // skipped slot

        let config1 = SystemCallsConfig {
            spec_id: SpecId::CANCUN,
            block_number: U256::from(100),
            timestamp: U256::from(ts1),
            block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(B256::repeat_byte(0x11)),
        };
        system_calls
            .apply_pre_tx_system_calls(&config1, version_db.state_mut())
            .unwrap();

        let config2 = SystemCallsConfig {
            spec_id: SpecId::CANCUN,
            block_number: U256::from(100),
            timestamp: U256::from(ts2),
            block_hash: Some(B256::ZERO),
            parent_beacon_block_root: Some(B256::repeat_byte(0x22)),
        };
        system_calls
            .apply_pre_tx_system_calls(&config2, version_db.state_mut())
            .unwrap();

        // Both beacon roots at different slots
        let root_slot1 = U256::from((ts1 % HISTORY_BUFFER_LENGTH) + HISTORY_BUFFER_LENGTH);
        let root_slot2 = U256::from((ts2 % HISTORY_BUFFER_LENGTH) + HISTORY_BUFFER_LENGTH);

        let stored1 = version_db
            .storage_ref(BEACON_ROOTS_ADDRESS, root_slot1)
            .unwrap();
        let stored2 = version_db
            .storage_ref(BEACON_ROOTS_ADDRESS, root_slot2)
            .unwrap();
        assert_eq!(stored1, U256::from_be_bytes(B256::repeat_byte(0x11).0));
        assert_eq!(stored2, U256::from_be_bytes(B256::repeat_byte(0x22).0));
    }

    #[test]
    fn test_empty_block_system_calls_only() {
        let mock = MockDb::default();
        let mut version_db = VersionDb::new(mock);
        let system_calls = SystemCalls::new();

        let config = SystemCallsConfig {
            spec_id: SpecId::PRAGUE,
            block_number: U256::from(500),
            timestamp: U256::from(1700000000),
            block_hash: Some(B256::repeat_byte(0xee)),
            parent_beacon_block_root: Some(B256::repeat_byte(0xff)),
        };

        system_calls
            .apply_pre_tx_system_calls(&config, version_db.state_mut())
            .unwrap();

        // No tx commits
        assert_eq!(version_db.depth(), 0);

        // System contracts present
        assert!(
            version_db
                .basic_ref(HISTORY_STORAGE_ADDRESS)
                .unwrap()
                .is_some()
        );
        assert!(
            version_db
                .basic_ref(BEACON_ROOTS_ADDRESS)
                .unwrap()
                .is_some()
        );

        // Collapse finalizes to base state
        version_db.collapse_log();
        assert!(
            version_db
                .base_state()
                .basic_ref(HISTORY_STORAGE_ADDRESS)
                .unwrap()
                .is_some()
        );
    }
}
