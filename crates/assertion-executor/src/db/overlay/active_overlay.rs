use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        NotFoundError,
        overlay::{
            ForkDb,
            OverlayState,
        },
    },
    primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
        EvmState,
        U256,
    },
};
use dashmap::DashMap;

use std::{
    cell::UnsafeCell,
    sync::Arc,
};

use metrics::counter;
#[derive(Debug)]
/// An active overlay is a wrapper around the `overlaydb` meant to be used
/// when temporarily needing to change what database to use as the underlying.
///
/// It implements `DatabaseRef` over a Db implementing `Database`.
/// This access pattern uses `unsafe` code, but because we are not mutating the Db
/// data in any way it is perfectly safe. Additional safety is provided via an Arc.
///
/// The use of the active overlay may result in undefined behaviour if the `active_db`
/// is holding a refrance that is not valid anymore. There are no protections for this.
pub struct ActiveOverlay<Db> {
    active_db: Arc<UnsafeCell<Db>>,
    state: Arc<OverlayState>,
}

unsafe impl<Db> Send for ActiveOverlay<Db> {}
unsafe impl<Db> Sync for ActiveOverlay<Db> {}

impl<Db> Clone for ActiveOverlay<Db> {
    fn clone(&self) -> Self {
        Self {
            active_db: self.active_db.clone(),
            state: self.state.clone(),
        }
    }
}

impl<Db> ActiveOverlay<Db> {
    /// Creates a new `ActiveOverlay` given a `revm::DatabaseRef` and an `OverlayDb` cache.
    pub fn new(active_db: Arc<UnsafeCell<Db>>, state: Arc<OverlayState>) -> Self {
        Self { active_db, state }
    }

    /// Creates a new `forkdb` from the current overlay.
    pub fn fork(&self) -> ForkDb<ActiveOverlay<Db>> {
        ForkDb::new(self.clone())
    }
}

impl<Db: Database> DatabaseRef for ActiveOverlay<Db> {
    type Error = NotFoundError;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <Self as DatabaseRef>::Error> {
        //  Check cache
        if let Some(entry) = self.state.accounts.get(&address) {
            counter!("assex_active_overlay_db_basic_ref_hits").increment(1);
            return Ok(Some(entry.clone()));
        }

        // Query underlying DB (Unsafe Mutable Access)
        let result = unsafe {
            self.active_db
                .as_mut_unchecked()
                .basic(address)
                .map_err(|_| NotFoundError)?
        };

        if let Some(account_info) = result.as_ref() {
            // Cache the account
            self.state.accounts.insert(address, account_info.clone());

            // Cache code hash if present
            if let Some(code) = &account_info.code {
                self.state
                    .contracts
                    .insert(account_info.code_hash, code.clone());
            }
        }

        Ok(result)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, <Self as DatabaseRef>::Error> {
        if let Some(entry) = self.state.contracts.get(&code_hash) {
            counter!("assex_active_overlay_db_code_by_hash_ref_hits").increment(1);
            return Ok(entry.clone());
        }

        // Not in cache, query mandatory underlying DB
        // Map error if needed
        let bytecode = unsafe {
            self.active_db
                .as_mut_unchecked()
                .code_by_hash(code_hash)
                .map_err(|_| NotFoundError)?
        };

        // Found in DB, cache it
        self.state.contracts.insert(code_hash, bytecode.clone());
        Ok(bytecode)
    }

    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <Self as DatabaseRef>::Error> {
        // Check cache using tuple key (Address, U256)
        if let Some(value) = self.state.storage.get(&(address, slot)) {
            counter!("assex_active_overlay_db_storage_ref_hits").increment(1);
            // Convert B256 back to U256
            return Ok((*value).into());
        }

        let value_u256 = unsafe {
            self.active_db
                .as_mut_unchecked()
                .storage(address, slot)
                .map_err(|_| NotFoundError)?
        };

        // Compress U256 -> B256 for storage
        let value_b256: B256 = value_u256.to_be_bytes().into();
        self.state.storage.insert((address, slot), value_b256);

        Ok(value_u256)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, <Self as DatabaseRef>::Error> {
        if let Some(entry) = self.state.block_hashes.get(&number) {
            counter!("assex_active_overlay_db_block_hash_ref_hits").increment(1);
            return Ok(*entry);
        }

        let block_hash = unsafe {
            self.active_db
                .as_mut_unchecked()
                .block_hash(number)
                .map_err(|_| NotFoundError)?
        };

        self.state.block_hashes.insert(number, block_hash);
        Ok(block_hash)
    }
}

/// Implementation of `DatabaseCommit` for `ActiveOverlay`.
///
/// This implementation commits EVM state changes only to the overlay cache,
/// not to the underlying database. This allows temporary state modifications
/// that can be shared across multiple database instances through the same cache.
///
/// # Example
///
/// ```rust,ignore
/// use std::cell::UnsafeCell;
/// use std::sync::Arc;
///
/// let active_db = Arc::new(UnsafeCell::new(some_db));
/// let cache = Cache::new(1024);
/// let mut active_overlay = ActiveOverlay::new(active_db, cache);
///
/// // Commit state changes to the shared cache
/// active_overlay.commit(state_changes);
/// ```
impl<Db> DatabaseCommit for ActiveOverlay<Db> {
    fn commit(&mut self, changes: EvmState) {
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }

            // 1. Commit Basic Info
            self.state.accounts.insert(address, account.info.clone());

            // 2. Commit Code
            if let Some(code) = &account.info.code {
                self.state
                    .contracts
                    .insert(account.info.code_hash, code.clone());
            }

            // 3. Commit Storage (converting to B256)
            for (slot, storage_slot) in account.storage {
                let value_b256: B256 = storage_slot.present_value().to_be_bytes().into();
                self.state.storage.insert((address, slot), value_b256);
            }
        }
    }
}

impl<Db: Database> Database for ActiveOverlay<Db> {
    type Error = NotFoundError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
       DatabaseRef::basic_ref(self, address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        DatabaseRef::code_by_hash_ref(self, code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        DatabaseRef::storage_ref(self, address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        DatabaseRef::block_hash_ref(self, number)
    }
}

#[cfg(test)]
mod active_overlay_tests {
    use super::*;
    use crate::db::overlay::test_utils::{
        MockDb,
        mock_account_info,
    };
    use alloy_primitives::{
        U256,
        address,
        b256,
        bytes,
    };

    use crate::{
        db::overlay::TableKey,
        primitives::Bytecode,
    };

    use std::collections::HashMap;

    // Helper macro for accessing MockDb methods safely
    macro_rules! get_mock_db_field {
        ($arc_unsafe_cell:expr, $method:ident) => {
            unsafe { (*$arc_unsafe_cell.get()).$method() }
        };
    }

    // Test basic account fetching with cache interaction
    #[test]
    fn test_active_basic_hit_miss() {
        let addr1 = address!("a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1");
        let info1 = mock_account_info(U256::from(100), 1, None);
        let key1 = TableKey::Basic(addr1);

        let mut mock_db = MockDb::new();
        mock_db.insert_account(addr1, info1.clone());
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));

        // Create a cache instance (e.g., count-based)
        let overlay_cache = Arc::new(DashMap::new());

        // Create the ActiveOverlay
        let active_overlay = ActiveOverlay::new(mock_db_arc.clone(), overlay_cache);

        // 1. Initial state: Cache is empty
        assert!(!active_overlay.is_cached(&key1));
        assert_eq!(get_mock_db_field!(mock_db_arc, get_basic_calls), 0); // Use helper

        // 2. First read (cache miss): Fetches from underlying DB
        let result = active_overlay.basic_ref(addr1).unwrap();
        assert_eq!(result, Some(info1.clone()));
        assert_eq!(
            get_mock_db_field!(mock_db_arc, get_basic_calls), // Use helper
            1,
            "Underlying DB should be called on miss"
        );

        // 3. Check cache population (needs tasks to run)
        assert!(
            active_overlay.is_cached(&key1),
            "Data should be cached after miss"
        );

        // 4. Second read (cache hit): Gets from cache
        let result2 = active_overlay.basic_ref(addr1).unwrap();
        assert_eq!(result2, Some(info1.clone()));
        assert_eq!(
            get_mock_db_field!(mock_db_arc, get_basic_calls), // Use helper
            1,
            "Underlying DB should NOT be called on hit"
        );

        // 5. Read non-existent account
        let addr2 = address!("a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2");
        let key2 = TableKey::Basic(addr2);
        assert!(!active_overlay.is_cached(&key2));
        let result3 = active_overlay.basic_ref(addr2).unwrap();
        assert_eq!(result3, None);
        assert_eq!(
            get_mock_db_field!(mock_db_arc, get_basic_calls), // Use helper
            2,
            "Underlying DB should be called for non-existent acc"
        );
        // Absence is NOT cached
        assert!(!active_overlay.is_cached(&key2));
    }

    // Test storage fetching with cache interaction
    #[test]
    fn test_active_storage_hit_miss() {
        let addr1 = address!("b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1");
        let slot1 = U256::from(1);
        let value1 = U256::from(98765);
        let key1 = TableKey::Storage(addr1, slot1);

        let slot2 = U256::from(2); // Non-existent slot -> defaults to 0
        let key2 = TableKey::Storage(addr1, slot2);

        let mut mock_db = MockDb::new();
        mock_db.insert_storage(addr1, slot1, value1);
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));
        let overlay_cache = Arc::new(DashMap::new());
        let active_overlay = ActiveOverlay::new(mock_db_arc.clone(), overlay_cache);

        // 1. Initial state
        assert!(!active_overlay.is_cached(&key1));
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 0);

        // 2. First read (miss)
        let result = active_overlay.storage_ref(addr1, slot1).unwrap();
        assert_eq!(result, value1);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 1);
        assert!(active_overlay.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = active_overlay.storage_ref(addr1, slot1).unwrap();
        assert_eq!(result2, value1);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 1); // No new call

        // 4. Read non-existent slot (miss) -> should return U256::ZERO
        let result3 = active_overlay.storage_ref(addr1, slot2).unwrap();
        assert_eq!(result3, U256::ZERO);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 2);
        // Zero value IS cached
        assert!(active_overlay.is_cached(&key2));

        // 5. Read non-existent slot again (hit)
        let result4 = active_overlay.storage_ref(addr1, slot2).unwrap();
        assert_eq!(result4, U256::ZERO);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 2); // No new call
    }

    // Test code fetching with cache interaction
    #[test]
    fn test_active_code_hit_miss() {
        let code1_bytes = bytes!("30106000f3");
        let code1 = Bytecode::new_legacy(code1_bytes.clone());
        let hash1 = code1.hash_slow();
        let key1 = TableKey::CodeByHash(hash1);

        // Need an account associated with the code in the mock DB
        let addr1 = address!("c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1");
        let info1 = mock_account_info(U256::ZERO, 0, Some(code1.clone()));

        let hash_non_existent =
            b256!("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let key_non_existent = TableKey::CodeByHash(hash_non_existent);

        let mut mock_db = MockDb::new();
        mock_db.insert_account(addr1, info1);
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));
        let overlay_cache = Arc::new(DashMap::new());
        let active_overlay = ActiveOverlay::new(mock_db_arc.clone(), overlay_cache);

        // 1. Initial state
        assert!(!active_overlay.is_cached(&key1));
        assert_eq!(get_mock_db_field!(mock_db_arc, get_code_calls), 0);

        // 2. First read (miss)
        let result = active_overlay.code_by_hash_ref(hash1).unwrap();
        assert_eq!(result.original_bytes(), code1_bytes);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_code_calls), 1);
        assert!(active_overlay.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = active_overlay.code_by_hash_ref(hash1).unwrap();
        assert_eq!(result2.original_bytes(), code1_bytes);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_code_calls), 1); // No new call

        // 4. Read non-existent code (miss) -> Expect Error
        let result3 = active_overlay.code_by_hash_ref(hash_non_existent);
        assert!(result3.is_err());
        assert_eq!(get_mock_db_field!(mock_db_arc, get_code_calls), 2);
        // Error/absence not cached
        assert!(!active_overlay.is_cached(&key_non_existent));
    }

    // Test block hash fetching with cache interaction
    #[test]
    fn test_active_block_hash_hit_miss() {
        let num1: u64 = 200;
        let hash1 = b256!("d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1");
        let key1 = TableKey::BlockHash(num1);

        let num_non_existent: u64 = 201;
        let key_non_existent = TableKey::BlockHash(num_non_existent);

        let mut mock_db = MockDb::new();
        mock_db.insert_block_hash(num1, hash1);
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));
        let overlay_cache = Arc::new(DashMap::new());
        let active_overlay = ActiveOverlay::new(mock_db_arc.clone(), overlay_cache);

        // 1. Initial state
        assert!(!active_overlay.is_cached(&key1));
        assert_eq!(get_mock_db_field!(mock_db_arc, get_block_hash_calls), 0);

        // 2. First read (miss)
        let result = active_overlay.block_hash_ref(num1).unwrap();
        assert_eq!(result, hash1);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_block_hash_calls), 1);
        assert!(active_overlay.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = active_overlay.block_hash_ref(num1).unwrap();
        assert_eq!(result2, hash1);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_block_hash_calls), 1); // No new call

        // 4. Read non-existent block hash (miss) -> Expect Error
        let result3 = active_overlay.block_hash_ref(num_non_existent);
        assert!(result3.is_err());
        assert_eq!(get_mock_db_field!(mock_db_arc, get_block_hash_calls), 2);
        // Error/absence not cached
        assert!(!active_overlay.is_cached(&key_non_existent));
    }

    // Test interaction with a shared cache
    #[test]
    fn test_active_shared_cache() {
        let addr1 = address!("e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1");
        let info1 = mock_account_info(U256::from(500), 5, None);
        let key1 = TableKey::Basic(addr1);

        let addr2 = address!("e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2");
        let info2 = mock_account_info(U256::from(600), 6, None);
        let key2 = TableKey::Basic(addr2);

        // Underlying DB 1
        let mut mock_db1 = MockDb::new();
        mock_db1.insert_account(addr1, info1.clone());
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db1_arc = Arc::new(UnsafeCell::new(mock_db1));

        // Underlying DB 2
        let mut mock_db2 = MockDb::new();
        mock_db2.insert_account(addr2, info2.clone());
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db2_arc = Arc::new(UnsafeCell::new(mock_db2));

        // THE shared cache instance
        let shared_cache = Arc::new(DashMap::new());

        // Create two ActiveOverlays using DIFFERENT DBs but the SAME cache
        // Pass the correctly wrapped Arcs
        let active_overlay1 = ActiveOverlay::new(mock_db1_arc.clone(), shared_cache.clone());
        let active_overlay2 = ActiveOverlay::new(mock_db2_arc.clone(), shared_cache.clone());

        // Sanity check: initially empty
        assert!(!active_overlay1.is_cached(&key1));
        assert!(!active_overlay2.is_cached(&key2));

        // 1. Read addr1 via overlay1 (miss -> cache)
        let res1 = active_overlay1.basic_ref(addr1).unwrap(); // Should work now
        assert_eq!(res1, Some(info1.clone()));
        assert_eq!(get_mock_db_field!(mock_db1_arc, get_basic_calls), 1);
        assert_eq!(get_mock_db_field!(mock_db2_arc, get_basic_calls), 0);
        assert!(
            shared_cache.get(&key1).is_some(),
            "Cache should contain key1"
        );

        // 2. Read addr2 via overlay2 (miss -> cache)
        let res2 = active_overlay2.basic_ref(addr2).unwrap(); // Should work now
        assert_eq!(res2, Some(info2.clone()));
        assert_eq!(get_mock_db_field!(mock_db1_arc, get_basic_calls), 1);
        assert_eq!(get_mock_db_field!(mock_db2_arc, get_basic_calls), 1);
        assert!(
            shared_cache.get(&key2).is_some(),
            "Cache should contain key2"
        );
        assert_eq!(shared_cache.iter().count(), 2);

        // 3. Read addr1 via overlay2 (HIT in SHARED cache, even though DB2 doesn't have it)
        let res3 = active_overlay2.basic_ref(addr1).unwrap(); // Should work now
        assert_eq!(res3, Some(info1.clone())); // Got value from cache populated by overlay1
        assert_eq!(get_mock_db_field!(mock_db1_arc, get_basic_calls), 1); // No new DB calls
        assert_eq!(get_mock_db_field!(mock_db2_arc, get_basic_calls), 1); // No new DB calls

        // 4. Read addr2 via overlay1 (HIT in SHARED cache)
        let res4 = active_overlay1.basic_ref(addr2).unwrap(); // Should work now
        assert_eq!(res4, Some(info2.clone())); // Got value from cache populated by overlay2
        assert_eq!(get_mock_db_field!(mock_db1_arc, get_basic_calls), 1); // No new DB calls
        assert_eq!(get_mock_db_field!(mock_db2_arc, get_basic_calls), 1); // No new DB calls

        assert_eq!(shared_cache.iter().count(), 2);
    }

    // Test DatabaseCommit implementation
    #[test]
    fn test_active_database_commit() {
        use crate::primitives::{
            Account,
            AccountStatus,
            EvmState,
            EvmStorageSlot,
        };
        use std::collections::HashMap;

        let addr1 = address!("0000000000000000000000000000000000000001");
        let addr2 = address!("0000000000000000000000000000000000000002");

        let code_bytes = bytes!("608060405260aa8060106000396000f3fe");
        let code = Bytecode::new_raw(code_bytes.clone());
        let code_hash = code.hash_slow();

        // Create a mock database
        let mock_db = MockDb::new();
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));

        // Create a cache
        let cache = Arc::new(DashMap::new());

        // Create the active overlay
        let mut active_overlay = ActiveOverlay::new(mock_db_arc.clone(), cache.clone());

        // Create accounts with different states
        let account1 = Account {
            info: AccountInfo {
                balance: U256::from(1500),
                nonce: 3,
                code_hash,
                code: Some(code.clone()),
            },
            storage: HashMap::from_iter([
                (U256::from(5), EvmStorageSlot::new(U256::from(500))),
                (U256::from(6), EvmStorageSlot::new(U256::from(600))),
            ]),
            status: AccountStatus::Touched,
        };

        let account2 = Account {
            info: AccountInfo {
                balance: U256::from(2500),
                nonce: 4,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::from_iter([(U256::from(20), EvmStorageSlot::new(U256::from(2000)))]),
            status: AccountStatus::Touched,
        };

        // Create an untouched account that should be ignored
        let account3 = Account {
            info: AccountInfo {
                balance: U256::from(3500),
                nonce: 5,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::default(),
            status: AccountStatus::default(), // Not touched
        };

        let addr3 = address!("0000000000000000000000000000000000000003");

        let evm_state: EvmState =
            HashMap::from_iter([(addr1, account1), (addr2, account2), (addr3, account3)]);

        // Initial cache should be empty
        assert_eq!(cache.iter().count(), 0);

        // Commit the state changes
        active_overlay.commit(evm_state);

        // Verify account1's data was committed
        assert!(active_overlay.is_cached(&TableKey::Basic(addr1)));
        assert_eq!(
            active_overlay.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(1500)
        );
        assert_eq!(active_overlay.basic_ref(addr1).unwrap().unwrap().nonce, 3);

        // Verify code was committed
        assert!(active_overlay.is_cached(&TableKey::CodeByHash(code_hash)));
        assert_eq!(
            active_overlay
                .code_by_hash_ref(code_hash)
                .unwrap()
                .original_bytes(),
            code_bytes
        );

        // Verify storage was committed
        assert!(active_overlay.is_cached(&TableKey::Storage(addr1, U256::from(5))));
        assert!(active_overlay.is_cached(&TableKey::Storage(addr1, U256::from(6))));
        assert_eq!(
            active_overlay.storage_ref(addr1, U256::from(5)).unwrap(),
            U256::from(500)
        );
        assert_eq!(
            active_overlay.storage_ref(addr1, U256::from(6)).unwrap(),
            U256::from(600)
        );

        // Verify account2's data was committed
        assert!(active_overlay.is_cached(&TableKey::Basic(addr2)));
        assert_eq!(
            active_overlay.basic_ref(addr2).unwrap().unwrap().balance,
            U256::from(2500)
        );
        assert!(active_overlay.is_cached(&TableKey::Storage(addr2, U256::from(20))));
        assert_eq!(
            active_overlay.storage_ref(addr2, U256::from(20)).unwrap(),
            U256::from(2000)
        );

        // Verify account3 (untouched) was NOT committed
        assert!(!active_overlay.is_cached(&TableKey::Basic(addr3)));

        // Verify no underlying DB calls were made
        assert_eq!(get_mock_db_field!(mock_db_arc, get_basic_calls), 0);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_storage_calls), 0);
        assert_eq!(get_mock_db_field!(mock_db_arc, get_code_calls), 0);

        // Verify correct number of cache entries (2 accounts + 1 code + 3 storage slots)
        assert_eq!(cache.iter().count(), 6);
    }

    // Test DatabaseCommit with shared cache across multiple overlays
    #[test]
    fn test_active_database_commit_shared_cache() {
        use crate::primitives::{
            Account,
            AccountStatus,
            EvmState,
            EvmStorageSlot,
        };

        let addr1 = address!("f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1");
        let addr2 = address!("f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2");

        // Create two mock databases
        let mock_db1 = MockDb::new();
        let mock_db2 = MockDb::new();
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db1_arc = Arc::new(UnsafeCell::new(mock_db1));
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db2_arc = Arc::new(UnsafeCell::new(mock_db2));

        // Create a shared cache
        let shared_cache = Arc::new(DashMap::new());

        // Create two active overlays sharing the same cache
        let mut active_overlay1 = ActiveOverlay::new(mock_db1_arc.clone(), shared_cache.clone());
        let mut active_overlay2 = ActiveOverlay::new(mock_db2_arc.clone(), shared_cache.clone());

        // Create state for overlay1
        let account1 = Account {
            info: AccountInfo {
                balance: U256::from(1000),
                nonce: 1,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::from_iter([(U256::from(1), EvmStorageSlot::new(U256::from(100)))]),
            status: AccountStatus::Touched,
        };

        let state1: EvmState = HashMap::from_iter([(addr1, account1)]);

        // Create state for overlay2
        let account2 = Account {
            info: AccountInfo {
                balance: U256::from(2000),
                nonce: 2,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::from_iter([(U256::from(2), EvmStorageSlot::new(U256::from(200)))]),
            status: AccountStatus::Touched,
        };

        let state2: EvmState = HashMap::from_iter([(addr2, account2)]);

        // Commit state1 through overlay1
        active_overlay1.commit(state1);

        // Commit state2 through overlay2
        active_overlay2.commit(state2);

        // Both overlays should see both accounts through the shared cache
        assert_eq!(
            active_overlay1.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(1000)
        );
        assert_eq!(
            active_overlay1.basic_ref(addr2).unwrap().unwrap().balance,
            U256::from(2000)
        );
        assert_eq!(
            active_overlay2.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(1000)
        );
        assert_eq!(
            active_overlay2.basic_ref(addr2).unwrap().unwrap().balance,
            U256::from(2000)
        );

        // Storage should also be shared
        assert_eq!(
            active_overlay1.storage_ref(addr1, U256::from(1)).unwrap(),
            U256::from(100)
        );
        assert_eq!(
            active_overlay2.storage_ref(addr1, U256::from(1)).unwrap(),
            U256::from(100)
        );
        assert_eq!(
            active_overlay1.storage_ref(addr2, U256::from(2)).unwrap(),
            U256::from(200)
        );
        assert_eq!(
            active_overlay2.storage_ref(addr2, U256::from(2)).unwrap(),
            U256::from(200)
        );

        // Verify no underlying DB calls were made
        assert_eq!(get_mock_db_field!(mock_db1_arc, get_basic_calls), 0);
        assert_eq!(get_mock_db_field!(mock_db2_arc, get_basic_calls), 0);

        // Verify correct number of cache entries (2 accounts + 2 storage slots)
        assert_eq!(shared_cache.iter().count(), 4);
    }
}
