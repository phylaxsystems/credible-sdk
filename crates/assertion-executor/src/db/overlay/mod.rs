//! # overlay
//!
//! The overlay is a primitive used to cache EVM state in-memory from a on-disk store.
//! It can be tuned to store either a set number of entries or by size.
//!
//! The overlay has a buffer it uses before committing values to the underlying hashmap.
//! The data structure can be modeled as follows:
//! ```Buffer -> TinyLFU Hashmap -> Disk```
//! 
//! Eviction happens at the `TinyLFU` layer when committing the buffer. The buffer can either
//! be committed manually or when it becomes full. It is recommended to clear the buffer during
//! downtime, i.e., when calculating the state root.

#![allow(clippy::double_parens)]

use super::fork_db::ForkDb;
use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        NotFoundError,
    },
    primitives::{
        AccountInfo,
        Bytecode,
        EvmState,
    },
};
use active_overlay::ActiveOverlay;
use alloy_primitives::{
    Address,
    B256,
    U256,
};
use dashmap::DashMap;
use enum_as_inner::EnumAsInner;
use metrics::{
    counter,
    gauge,
};
use std::{
    cell::UnsafeCell,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;

pub mod active_overlay;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;

/// Enum to represent different table types
#[derive(Debug, PartialEq, Eq, Hash, Clone, EnumAsInner)]
pub enum TableKey {
    Basic(Address),
    CodeByHash(B256),
    /// Represents the code by hash table,
    /// where the first entry is the contract address and the second one is the slot.
    Storage(Address, U256),
    BlockHash(u64),
}

/// Enum representing different table values
#[derive(Debug, Clone, EnumAsInner)]
pub enum TableValue {
    Basic(AccountInfo),
    CodeByHash(Bytecode),
    Storage(B256),
    BlockHash(B256),
}

/// The `OverlayDb` is fast `TinyLFU`'d in memory cache for an on disk EVM database.
/// It optionally points to an on disk database that implements `revm::DatabaseRef`
/// and has a configurable cache.
///
/// The overlay can be used with either:
/// - `underlying_db` set in the overlay: where the overlay db is directly used,
/// - `underlying_db` set to `None` and spawning `ActiveOverlay`s: when you need to change the underlying db on the go,
/// - standalone: Without any underlying db.
#[derive(Debug)]
pub struct OverlayDb<Db> {
    underlying_db: Option<Arc<Db>>,
    pub overlay: Arc<DashMap<TableKey, TableValue>>,
}

impl<Db> Clone for OverlayDb<Db> {
    fn clone(&self) -> Self {
        Self {
            underlying_db: self.underlying_db.clone(),
            overlay: self.overlay.clone(),
        }
    }
}

impl<Db> Default for OverlayDb<Db> {
    fn default() -> Self {
        Self {
            underlying_db: None,
            overlay: Arc::new(DashMap::new()),
        }
    }
}

impl<Db> OverlayDb<Db> {
    /// Creates a new `OverlayDB` with the max cache size in bytes.
    pub fn new(underlying_db: Option<Arc<Db>>) -> Self {
        Self {
            underlying_db,
            overlay: Arc::new(DashMap::new()),
        }
    }

    /// Creates a new `OverlayDb` with the max capacity being determined by the number
    /// of elements inside of the cache instead of the size.
    pub fn new_with_len(underlying_db: Option<Arc<Db>>) -> Self {
        Self {
            underlying_db,
            overlay: Arc::new(DashMap::new()),
        }
    }

    ///Invalidates all cache entries.
    pub fn invalidate_all(&self) {
        self.overlay.clear();
    }

    /// Replaces underlying database refrance with a new one.
    /// Can be set to none which will ignore it completely.
    pub fn replace_underlying(&mut self, new_db: Option<Arc<Db>>) {
        self.underlying_db = new_db;
    }

    /// Creates a new forkdb from the current overlay.
    pub fn fork(&self) -> ForkDb<OverlayDb<Db>> {
        ForkDb::new(self.clone())
    }

    /// Creates an `ActiveOverlay` with the current overlay and a database refrance.
    ///
    /// The `ActiveOverlay` shares the same cache as this `OverlayDb` instance, meaning:
    /// - Changes committed to the `ActiveOverlay` will be visible in this `OverlayDb`
    /// - Changes made to this `OverlayDb` will be visible in the `ActiveOverlay`
    /// - Multiple `ActiveOverlay` instances created from the same `OverlayDb` will share state
    ///
    /// This is useful for scenarios where you need different underlying databases
    /// but want to share cached state across them.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let overlay_db = OverlayDb::new(None);
    /// let active_overlay = overlay_db.create_overlay(active_db);
    ///
    /// // Commit to active overlay
    /// active_overlay.commit(state_changes);
    ///
    /// // Changes are now visible in the parent overlay_db
    /// assert!(overlay_db.is_cached(&some_key));
    /// ```
    pub fn create_overlay<ActiveDbT>(
        &self,
        active_db: Arc<UnsafeCell<ActiveDbT>>,
    ) -> ActiveOverlay<ActiveDbT> {
        ActiveOverlay::new(active_db, self.overlay.clone())
    }

    /// Check if a value is present inside of the cache.
    /// Does not trigger a cache hit.
    pub fn is_cached(&self, key: &TableKey) -> bool {
        self.overlay.contains_key(key)
    }

    /// Returns the number of entries inside the cache.
    pub fn cache_entry_count(&self) -> u64 {
        self.overlay.len() as u64
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn spawn_monitoring_thread(&self) -> JoinHandle<()> {
        const MONITORING_INTERVAL: Duration = Duration::from_secs(10);

        let overlay_db = self.overlay.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(MONITORING_INTERVAL);
            loop {
                interval.tick().await;

                // Total count
                let total = overlay_db.len();
                gauge!("overlay_db_cache_total_entries").set(total as f64);

                let mut basic_count = 0u64;
                let mut storage_count = 0u64;
                let mut code_count = 0u64;
                let mut block_hash_count = 0u64;

                for entry in overlay_db.iter() {
                    match entry.key() {
                        TableKey::Basic(_) => basic_count += 1,
                        TableKey::Storage(_, _) => storage_count += 1,
                        TableKey::CodeByHash(_) => code_count += 1,
                        TableKey::BlockHash(_) => block_hash_count += 1,
                    }
                }

                gauge!("overlay_db_cache_basic_entries").set(basic_count as f64);
                gauge!("overlay_db_cache_storage_entries").set(storage_count as f64);
                gauge!("overlay_db_cache_code_entries").set(code_count as f64);
                gauge!("overlay_db_cache_block_hash_entries").set(block_hash_count as f64);
            }
        })
    }
}

impl<Db: DatabaseRef> DatabaseRef for OverlayDb<Db> {
    type Error = NotFoundError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key = TableKey::Basic(address);
        if let Some(value) = self.overlay.get(&key) {
            if let Some(account_info) = value.as_basic() {
                // Found in cache
                counter!("assex_overlay_db_basic_ref_hits").increment(1);
                let result = Some(account_info.clone());
                return Ok(result);
            }
            return Ok(None);
        }

        counter!("assex_overlay_db_basic_ref_misses").increment(1);

        // Not in cache, try underlying DB if it exists
        if let Some(db) = self.underlying_db.as_ref() {
            // Map potential underlying DB error to NotFoundError
            let result = db.basic_ref(address).map_err(|_| NotFoundError)?;

            if let Some(account_info) = result.as_ref() {
                // Found in DB, cache it
                self.overlay
                    .insert(key, TableValue::Basic(account_info.clone()));

                // If the code is present, already populate the cache with the code hash, so we save one call to the underlying DB
                if let Some(code) = account_info.code.as_ref() {
                    let code_byte = code.original_byte_slice();
                    let code_hash = TableKey::CodeByHash(revm::primitives::keccak256(code_byte));
                    let bytecode =
                        TableValue::CodeByHash(Bytecode::new_raw(code_byte.to_vec().into()));
                    self.overlay.insert(code_hash, bytecode);
                }
            }

            Ok(result) // Return the found info
        } else {
            // No underlying DB and not in cache
            Ok(None)
        }
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let key = TableKey::CodeByHash(code_hash);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache
            counter!("assex_overlay_db_code_by_hash_ref_hits").increment(1);
            let bytecode = value.as_code_by_hash().cloned().unwrap(); // unwrap safe, Clone Bytecode
            return Ok(bytecode);
        }

        counter!("assex_overlay_db_code_by_hash_ref_misses").increment(1);

        // Not in cache, try underlying DB
        if let Some(db) = self.underlying_db.as_ref() {
            // Underlying DB returns Result<Bytecode, Error>
            // Map error if needed
            let bytecode = db.code_by_hash_ref(code_hash).map_err(|_| NotFoundError)?;
            // Found in DB, cache it
            self.overlay
                .insert(key, TableValue::CodeByHash(bytecode.clone()));
            Ok(bytecode)
        } else {
            // No underlying DB and not in cache
            Err(NotFoundError) // Indicate not found
        }
    }

    fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        let key = TableKey::Storage(address, slot);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache, convert B256 back to U256
            counter!("assex_overlay_db_storage_ref_hits").increment(1);
            let value_u256: U256 = (*value.as_storage().unwrap()).into();
            return Ok(value_u256); // unwrap safe
        }

        counter!("assex_overlay_db_storage_ref_misses").increment(1);

        // Not in cache, try underlying DB
        if let Some(db) = self.underlying_db.as_ref() {
            // Underlying DB returns Result<U256, Error>
            let value_u256 = db.storage_ref(address, slot).map_err(|_| NotFoundError)?;
            // Found in DB, cache it as B256
            let value_b256: B256 = value_u256.to_be_bytes().into();
            self.overlay.insert(key, TableValue::Storage(value_b256));
            Ok(value_u256) // Return the U256 value
        } else {
            // No underlying DB, slot not cached. REVM expects U256::ZERO.
            Ok(U256::ZERO)
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let key = TableKey::BlockHash(number);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache
            counter!("assex_overlay_db_block_hash_ref_hits").increment(1);
            let block_hash = *value.as_block_hash().unwrap();
            return Ok(block_hash); // unwrap safe
        }

        counter!("assex_overlay_db_block_hash_ref_misses").increment(1);

        // Not in cache, try underlying DB
        if let Some(db) = self.underlying_db.as_ref() {
            // Underlying DB returns Result<B256, Error>
            let block_hash = db.block_hash_ref(number).map_err(|_| NotFoundError)?;
            // Found in DB, cache it
            self.overlay.insert(key, TableValue::BlockHash(block_hash));
            Ok(block_hash)
        } else {
            // No underlying DB and not in cache
            Err(NotFoundError) // Indicate not found
        }
    }
}

/// Implementation of `DatabaseCommit` for `OverlayDb`.
///
/// This implementation commits EVM state changes only to the overlay cache,
/// not to any underlying database. This is useful for:
/// - Temporary state modifications that shouldn't persist to disk
/// - Testing and simulation scenarios
/// - Building up state changes before a final commit to persistent storage
///
/// # Example
///
/// ```rust,ignore
/// use revm::primitives::{Account, AccountInfo, EvmState};
/// use std::collections::HashMap;
///
/// let mut overlay_db = OverlayDb::<SomeDb>::default();
/// let mut state = EvmState::new();
///
/// // Add an account with some balance
/// state.insert(
///     address!("0000000000000000000000000000000000000001"),
///     Account {
///         info: AccountInfo {
///             balance: U256::from(1000),
///             nonce: 1,
///             ..Default::default()
///         },
///         storage: HashMap::new(),
///         status: AccountStatus::Touched,
///     }
/// );
///
/// // Commit to overlay cache only
/// overlay_db.commit(state);
/// ```
impl<Db> DatabaseCommit for OverlayDb<Db> {
    fn commit(&mut self, changes: EvmState) {
        for (address, account) in changes {
            // Skip untouched accounts
            if !account.is_touched() {
                continue;
            }

            // Update account info
            let key = TableKey::Basic(address);
            self.overlay
                .insert(key, TableValue::Basic(account.info.clone()));

            // Update code if present
            if let Some(code) = &account.info.code {
                let code_key = TableKey::CodeByHash(account.info.code_hash);
                self.overlay
                    .insert(code_key, TableValue::CodeByHash(code.clone()));
            }

            // Update storage slots
            for (slot, storage_slot) in account.storage {
                let storage_key = TableKey::Storage(address, slot);
                let value_b256: B256 = storage_slot.present_value().to_be_bytes().into();
                self.overlay
                    .insert(storage_key, TableValue::Storage(value_b256));
            }
        }
    }
}

impl<Db> OverlayDb<Db> {
    pub fn commit_overlay_fork_db(&mut self, fork_db: ForkDb<OverlayDb<Db>>) {
        // Update account info
        for (address, account_info) in fork_db.basic {
            let key = TableKey::Basic(address);
            self.overlay
                .insert(key, TableValue::Basic(account_info.clone()));
        }

        // Update code if present
        for (address, code) in fork_db.code_by_hash {
            let code_key = TableKey::CodeByHash(address);
            self.overlay
                .insert(code_key, TableValue::CodeByHash(code.clone()));
        }

        // Update storage slots
        let storage = Arc::unwrap_or_clone( fork_db.storage);//xtra latentcy
        for (address, slot_map) in storage {
            for (slot, storage_slot) in slot_map.map {
                let storage_key = TableKey::Storage(address, slot);
                let value_b256: B256 = storage_slot.to_be_bytes().into();
                if slot_map.dont_read_from_inner_db {
                    self.overlay.remove(&storage_key);
                } else {
                    self.overlay
                        .insert(storage_key, TableValue::Storage(value_b256));
                }
            }
        }
    }
}

impl<Db: DatabaseRef> Database for OverlayDb<Db> {
    type Error = NotFoundError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let key = TableKey::Basic(address);
        if let Some(value) = self.overlay.get(&key) {
            match value.as_basic() {
                Some(account_info) => {
                    // Found in cache
                    return Ok(Some(account_info.clone()));
                }
                None => {
                    return Ok(None);
                }
            }
        }

        // Not in cache, try underlying DB if it exists
        match self.underlying_db.as_ref() {
            Some(db) => {
                // Map potential underlying DB error to NotFoundError
                match db.basic_ref(address).map_err(|_| NotFoundError)? {
                    Some(account_info) => {
                        // Found in DB, cache it
                        self.overlay
                            .insert(key, TableValue::Basic(account_info.clone()));
                        Ok(Some(account_info)) // Return the found info
                    }
                    None => {
                        // Not found in DB, do not cache absence explicitly here
                        Ok(None)
                    }
                }
            }
            None => {
                // No underlying DB and not in cache
                Ok(None)
            }
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let key = TableKey::CodeByHash(code_hash);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache
            return Ok(value.as_code_by_hash().cloned().unwrap()); // unwrap safe, Clone Bytecode
        }

        // Not in cache, try underlying DB
        match self.underlying_db.as_ref() {
            Some(db) => {
                // Underlying DB returns Result<Bytecode, Error>
                // Map error if needed
                let bytecode = db.code_by_hash_ref(code_hash).map_err(|_| NotFoundError)?;
                // Found in DB, cache it
                self.overlay
                    .insert(key, TableValue::CodeByHash(bytecode.clone()));
                Ok(bytecode)
            }
            None => {
                // No underlying DB and not in cache
                Err(NotFoundError) // Indicate not found
            }
        }
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let key = TableKey::Storage(address, index);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache, convert B256 back to U256
            return Ok((*value.as_storage().unwrap()).into()); // unwrap safe
        }

        // Not in cache, try underlying DB
        match self.underlying_db.as_ref() {
            Some(db) => {
                // Underlying DB returns Result<U256, Error>
                let value_u256 = db.storage_ref(address, index).map_err(|_| NotFoundError)?;
                // Found in DB (even if zero), cache it as B256
                let value_b256: B256 = value_u256.to_be_bytes().into();
                self.overlay.insert(key, TableValue::Storage(value_b256));
                Ok(value_u256) // Return the U256 value
            }
            None => {
                // No underlying DB, slot not cached. REVM expects U256::ZERO.
                Ok(U256::ZERO)
            }
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        let key = TableKey::BlockHash(number);
        if let Some(value) = self.overlay.get(&key) {
            // Found in cache
            return Ok(*value.as_block_hash().unwrap()); // unwrap safe
        }

        // Not in cache, try underlying DB
        match self.underlying_db.as_ref() {
            Some(db) => {
                // Underlying DB returns Result<B256, Error>
                let block_hash = db.block_hash_ref(number).map_err(|_| NotFoundError)?;
                // Found in DB, cache it
                self.overlay.insert(key, TableValue::BlockHash(block_hash));
                Ok(block_hash)
            }
            None => {
                // No underlying DB and not in cache
                Err(NotFoundError) // Indicate not found
            }
        }
    }
}

#[cfg(test)]
mod overlay_db_tests {
    use super::*;
    use crate::{
        db::overlay::test_utils::{
            MockDb,
            mock_account_info,
        },
        primitives::Bytecode,
    };
    use alloy_primitives::{
        address,
        b256,
        bytes,
    };

    use std::collections::HashMap;

    #[test]
    fn test_basic_hit_miss() {
        let addr1 = address!("0000000000000000000000000000000000000001");
        let info1 = mock_account_info(U256::from(100), 1, None);
        let key1 = TableKey::Basic(addr1);

        let mut mock_db = MockDb::new();
        mock_db.insert_account(addr1, info1.clone());
        let mock_db_arc = Arc::new(mock_db);

        // Use small capacity for testing potential eviction later
        let overlay_db = OverlayDb::new(Some(mock_db_arc.clone()));

        // 1. Initial state: Cache is empty
        assert!(!overlay_db.is_cached(&key1));
        assert_eq!(mock_db_arc.get_basic_calls(), 0);

        // 2. First read (cache miss): Fetches from underlying DB
        let result = overlay_db.basic_ref(addr1).unwrap();
        assert_eq!(result, Some(info1.clone()));
        assert_eq!(
            mock_db_arc.get_basic_calls(),
            1,
            "Underlying DB should be called on miss"
        );

        // 3. Check cache population
        assert!(
            overlay_db.is_cached(&key1),
            "Data should be cached after miss"
        );

        // 4. Second read (cache hit): Gets from cache
        let result2 = overlay_db.basic_ref(addr1).unwrap();
        assert_eq!(result2, Some(info1.clone()));
        assert_eq!(
            mock_db_arc.get_basic_calls(),
            1,
            "Underlying DB should NOT be called on hit"
        );

        // 5. Read non-existent account
        let addr2 = address!("0000000000000000000000000000000000000002");
        let key2 = TableKey::Basic(addr2);
        assert!(!overlay_db.is_cached(&key2));
        let result3 = overlay_db.basic_ref(addr2).unwrap();
        assert_eq!(result3, None);
        assert_eq!(
            mock_db_arc.get_basic_calls(),
            2,
            "Underlying DB should be called for non-existent acc"
        );
        // Absence is NOT cached by default in this implementation
        assert!(!overlay_db.is_cached(&key2));
    }

    #[test]
    fn test_storage_hit_miss() {
        let addr1 = address!("0000000000000000000000000000000000000011");
        let slot1 = U256::from(1);
        let value1 = U256::from(12345);
        let key1 = TableKey::Storage(addr1, slot1);

        let slot2 = U256::from(2); // Non-existent slot
        let key2 = TableKey::Storage(addr1, slot2);

        let mut mock_db = MockDb::new();
        mock_db.insert_storage(addr1, slot1, value1);
        let mock_db_arc = Arc::new(mock_db);

        let overlay_db = OverlayDb::new(Some(mock_db_arc.clone()));

        // 1. Initial state
        assert!(!overlay_db.is_cached(&key1));
        assert_eq!(mock_db_arc.get_storage_calls(), 0);

        // 2. First read (miss)
        let result = overlay_db.storage_ref(addr1, slot1).unwrap();
        assert_eq!(result, value1);
        assert_eq!(mock_db_arc.get_storage_calls(), 1);
        assert!(overlay_db.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = overlay_db.storage_ref(addr1, slot1).unwrap();
        assert_eq!(result2, value1);
        assert_eq!(mock_db_arc.get_storage_calls(), 1); // No new call

        // 4. Read non-existent slot (miss) - Should return default U256::ZERO
        let result3 = overlay_db.storage_ref(addr1, slot2).unwrap();
        assert_eq!(result3, U256::ZERO);
        assert_eq!(mock_db_arc.get_storage_calls(), 2);
        // Zero value *is* cached because the underlying db returned it,
        // and we store the B256 representation of U256::ZERO
        assert!(overlay_db.is_cached(&key2));

        // 5. Read non-existent slot again (hit)
        let result4 = overlay_db.storage_ref(addr1, slot2).unwrap();
        assert_eq!(result4, U256::ZERO);
        assert_eq!(mock_db_arc.get_storage_calls(), 2); // No new call
    }

    #[test]
    fn test_code_hit_miss() {
        let code1_bytes = bytes!("6080604052");
        let code1 = Bytecode::new_raw(code1_bytes.clone());
        let hash1 = code1.hash_slow();
        let key1 = TableKey::CodeByHash(hash1);

        let addr1 = address!("0000000000000000000000000000000000000021");
        let info1 = mock_account_info(U256::ZERO, 0, Some(code1.clone()));

        let hash_non_existent =
            b256!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        let key_non_existent = TableKey::CodeByHash(hash_non_existent);

        let mut mock_db = MockDb::new();
        // Inserting account also inserts code into mock_db.contracts
        mock_db.insert_account(addr1, info1);
        let mock_db_arc = Arc::new(mock_db);

        let overlay_db = OverlayDb::new(Some(mock_db_arc.clone()));

        // 1. Initial state
        assert!(!overlay_db.is_cached(&key1));
        assert_eq!(mock_db_arc.get_code_calls(), 0);

        // 2. First read (miss)
        let result = overlay_db.code_by_hash_ref(hash1).unwrap();
        assert_eq!(result.original_bytes(), code1_bytes);
        assert_eq!(mock_db_arc.get_code_calls(), 1);
        assert!(overlay_db.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = overlay_db.code_by_hash_ref(hash1).unwrap();
        assert_eq!(result2.original_bytes(), code1_bytes);
        assert_eq!(mock_db_arc.get_code_calls(), 1); // No new call

        // 4. Read non-existent code (miss)
        let result3 = overlay_db.code_by_hash_ref(hash_non_existent);
        assert!(result3.is_err()); // Expect error
        assert_eq!(mock_db_arc.get_code_calls(), 2);
        assert!(!overlay_db.is_cached(&key_non_existent)); // Errors/absence not cached
    }

    #[test]
    fn test_block_hash_hit_miss() {
        let num1: u64 = 100;
        let hash1 = b256!("1111000000000000000000000000000000000000000000000000000000001111");
        let key1 = TableKey::BlockHash(num1);

        let num_non_existent: u64 = 101;
        let key_non_existent = TableKey::BlockHash(num_non_existent);

        let mut mock_db = MockDb::new();
        mock_db.insert_block_hash(num1, hash1);
        let mock_db_arc = Arc::new(mock_db);

        let overlay_db = OverlayDb::new(Some(mock_db_arc.clone()));

        // 1. Initial state
        assert!(!overlay_db.is_cached(&key1));
        assert_eq!(mock_db_arc.get_block_hash_calls(), 0);

        // 2. First read (miss)
        let result = overlay_db.block_hash_ref(num1).unwrap();
        assert_eq!(result, hash1);
        assert_eq!(mock_db_arc.get_block_hash_calls(), 1);
        assert!(overlay_db.is_cached(&key1));

        // 3. Second read (hit)
        let result2 = overlay_db.block_hash_ref(num1).unwrap();
        assert_eq!(result2, hash1);
        assert_eq!(mock_db_arc.get_block_hash_calls(), 1); // No new call

        // 4. Read non-existent block hash (miss)
        let result3 = overlay_db.block_hash_ref(num_non_existent);
        assert!(result3.is_err());
        assert_eq!(mock_db_arc.get_block_hash_calls(), 2);
        assert!(!overlay_db.is_cached(&key_non_existent)); // Errors/absence not cached
    }

    #[test]
    fn test_no_underlying_db() {
        let addr1 = address!("0000000000000000000000000000000000000031");
        let slot1 = U256::from(1);
        let code_hash1 = b256!("cafebeefcafebeefcafebeefcafebeefcafebeefcafebeefcafebeefcafebeef");
        let block_num1: u64 = 50;

        // Create OverlayDb with NO underlying database
        let overlay_db: OverlayDb<MockDb> = OverlayDb::new(None); // Specify MockDb type arg

        // Read basic - should return None
        assert_eq!(overlay_db.basic_ref(addr1).unwrap(), None);

        // Read storage - should return U256::ZERO
        assert_eq!(overlay_db.storage_ref(addr1, slot1).unwrap(), U256::ZERO);

        // Read code - should return Error
        assert!(overlay_db.code_by_hash_ref(code_hash1).is_err());

        // Read block hash - should return Error
        assert!(overlay_db.block_hash_ref(block_num1).is_err());

        // Ensure nothing was cached
        assert!(!overlay_db.is_cached(&TableKey::Basic(addr1)));
        assert!(!overlay_db.is_cached(&TableKey::Storage(addr1, slot1)));
        assert!(!overlay_db.is_cached(&TableKey::CodeByHash(code_hash1)));
        assert!(!overlay_db.is_cached(&TableKey::BlockHash(block_num1)));
    }

    #[test]
    fn test_invalidate_all() {
        let addr1 = address!("0000000000000000000000000000000000000041");
        let info1 = mock_account_info(U256::from(200), 2, None);
        let key1 = TableKey::Basic(addr1);

        let mut mock_db = MockDb::new();
        mock_db.insert_account(addr1, info1.clone());
        let mock_db_arc = Arc::new(mock_db);

        let overlay_db = OverlayDb::new(Some(mock_db_arc.clone()));

        // Read to populate cache
        let _ = overlay_db.basic_ref(addr1).unwrap();
        assert!(overlay_db.is_cached(&key1));
        assert_eq!(overlay_db.cache_entry_count(), 1);

        // Invalidate
        overlay_db.invalidate_all();

        // Check cache is empty
        assert!(!overlay_db.is_cached(&key1));
        assert_eq!(overlay_db.cache_entry_count(), 0);

        // Read again (should be miss)
        let result = overlay_db.basic_ref(addr1).unwrap();
        assert_eq!(result, Some(info1));
        assert_eq!(mock_db_arc.get_basic_calls(), 2); // Called underlying again
        assert!(overlay_db.is_cached(&key1)); // Repopulated
    }

    #[test]
    fn test_replace_underlying() {
        let addr1 = address!("0000000000000000000000000000000000000061");
        let info1 = mock_account_info(U256::from(100), 1, None);
        let key1 = TableKey::Basic(addr1);

        let addr2 = address!("0000000000000000000000000000000000000062");
        let info2 = mock_account_info(U256::from(200), 2, None);
        let key2 = TableKey::Basic(addr2);

        // DB 1 has addr1
        let mut mock_db1 = MockDb::new();
        mock_db1.insert_account(addr1, info1.clone());
        let mock_db1_arc = Arc::new(mock_db1);

        // DB 2 has addr2
        let mut mock_db2 = MockDb::new();
        mock_db2.insert_account(addr2, info2.clone());
        let mock_db2_arc = Arc::new(mock_db2);

        let mut overlay_db = OverlayDb::new(Some(mock_db1_arc.clone()));

        // 1. Read from DB1 (miss -> cache)
        assert_eq!(overlay_db.basic_ref(addr1).unwrap(), Some(info1.clone()));
        assert!(overlay_db.is_cached(&key1));
        assert_eq!(mock_db1_arc.get_basic_calls(), 1);
        assert_eq!(mock_db2_arc.get_basic_calls(), 0);

        // 2. Replace underlying DB
        overlay_db.replace_underlying(Some(mock_db2_arc.clone()));

        // 3. Read addr1 again (cache hit - still has old value)
        assert_eq!(overlay_db.basic_ref(addr1).unwrap(), Some(info1.clone()));
        assert_eq!(mock_db1_arc.get_basic_calls(), 1); // No new call to DB1
        assert_eq!(mock_db2_arc.get_basic_calls(), 0); // No call to DB2 yet

        // 4. Read addr2 (miss -> reads from new DB2 -> cache)
        assert_eq!(overlay_db.basic_ref(addr2).unwrap(), Some(info2.clone()));
        assert!(overlay_db.is_cached(&key2));
        assert_eq!(mock_db1_arc.get_basic_calls(), 1);
        assert_eq!(mock_db2_arc.get_basic_calls(), 1); // Called DB2

        // 5. Invalidate cache
        overlay_db.invalidate_all();
        assert!(!overlay_db.is_cached(&key1));
        assert!(!overlay_db.is_cached(&key2));

        // 6. Read addr1 again (miss -> reads from DB2 -> not found)
        assert_eq!(overlay_db.basic_ref(addr1).unwrap(), None);
        assert_eq!(mock_db1_arc.get_basic_calls(), 1);
        assert_eq!(mock_db2_arc.get_basic_calls(), 2); // Called DB2 again
        assert!(!overlay_db.is_cached(&key1)); // Absence not cached

        // 7. Read addr2 again (miss -> reads from DB2 -> found)
        assert_eq!(overlay_db.basic_ref(addr2).unwrap(), Some(info2.clone()));
        assert_eq!(mock_db1_arc.get_basic_calls(), 1);
        assert_eq!(mock_db2_arc.get_basic_calls(), 3); // Called DB2 again
        assert!(overlay_db.is_cached(&key2)); // Cached again
    }

    #[test]
    fn test_fork_creation() {
        let overlay_db: OverlayDb<MockDb> = OverlayDb::default();
        let _fork_db = overlay_db.fork(); // Ensure it compiles and runs
    }

    #[test]
    fn test_active_overlay_creation() {
        let overlay_db: OverlayDb<MockDb> = OverlayDb::default();
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(MockDb::new()));
        let _active_overlay = overlay_db.create_overlay(mock_db_arc);
    }

    #[test]
    fn test_database_commit() {
        use crate::primitives::{
            Account,
            AccountStatus,
            EvmState,
            EvmStorageSlot,
        };
        use std::collections::HashMap;

        let addr1 = address!("0000000000000000000000000000000000000001");
        let addr2 = address!("0000000000000000000000000000000000000002");

        let code_bytes = bytes!("6080604052");
        let code = Bytecode::new_raw(code_bytes.clone());
        let code_hash = code.hash_slow();

        // Create accounts with different states
        let account1 = Account {
            info: AccountInfo {
                balance: U256::from(1000),
                nonce: 1,
                code_hash,
                code: Some(code.clone()),
            },
            storage: HashMap::from_iter([
                (U256::from(1), EvmStorageSlot::new(U256::from(100))),
                (U256::from(2), EvmStorageSlot::new(U256::from(200))),
            ]),
            status: AccountStatus::Touched,
        };

        let account2 = Account {
            info: AccountInfo {
                balance: U256::from(2000),
                nonce: 2,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::from_iter([(U256::from(10), EvmStorageSlot::new(U256::from(1000)))]),
            status: AccountStatus::Touched,
        };

        // Create an untouched account that should be ignored
        let account3 = Account {
            info: AccountInfo {
                balance: U256::from(3000),
                nonce: 3,
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

        let mut overlay_db: OverlayDb<MockDb> = OverlayDb::default();

        // Commit the state changes
        overlay_db.commit(evm_state);

        // Verify account1's data was committed
        assert!(overlay_db.is_cached(&TableKey::Basic(addr1)));
        assert_eq!(
            overlay_db.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(1000)
        );
        assert_eq!(overlay_db.basic_ref(addr1).unwrap().unwrap().nonce, 1);

        // Verify code was committed
        assert!(overlay_db.is_cached(&TableKey::CodeByHash(code_hash)));
        assert_eq!(
            overlay_db
                .code_by_hash_ref(code_hash)
                .unwrap()
                .original_bytes(),
            code_bytes
        );

        // Verify storage was committed
        assert!(overlay_db.is_cached(&TableKey::Storage(addr1, U256::from(1))));
        assert!(overlay_db.is_cached(&TableKey::Storage(addr1, U256::from(2))));
        assert_eq!(
            overlay_db.storage_ref(addr1, U256::from(1)).unwrap(),
            U256::from(100)
        );
        assert_eq!(
            overlay_db.storage_ref(addr1, U256::from(2)).unwrap(),
            U256::from(200)
        );

        // Verify account2's data was committed
        assert!(overlay_db.is_cached(&TableKey::Basic(addr2)));
        assert_eq!(
            overlay_db.basic_ref(addr2).unwrap().unwrap().balance,
            U256::from(2000)
        );
        assert!(overlay_db.is_cached(&TableKey::Storage(addr2, U256::from(10))));
        assert_eq!(
            overlay_db.storage_ref(addr2, U256::from(10)).unwrap(),
            U256::from(1000)
        );

        // Verify account3 (untouched) was NOT committed
        assert!(!overlay_db.is_cached(&TableKey::Basic(addr3)));

        // Run pending tasks to ensure all entries are properly cached
        assert_eq!(overlay_db.cache_entry_count(), 6); // 2 accounts + 1 code + 3 storage slots
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn test_active_overlay_commit_propagates_to_parent() {
        use crate::primitives::{
            Account,
            AccountStatus,
            EvmState,
            EvmStorageSlot,
        };
        use std::cell::UnsafeCell;

        let addr1 = address!("0000000000000000000000000000000000000010");
        let addr2 = address!("0000000000000000000000000000000000000020");

        let code_bytes = bytes!("6080604052600080fd");
        let code = Bytecode::new_raw(code_bytes.clone());
        let code_hash = code.hash_slow();

        // Create parent OverlayDb
        let parent_overlay_db: OverlayDb<MockDb> = OverlayDb::default();

        // Verify parent is initially empty
        assert_eq!(parent_overlay_db.cache_entry_count(), 0);
        assert!(!parent_overlay_db.is_cached(&TableKey::Basic(addr1)));
        assert!(!parent_overlay_db.is_cached(&TableKey::Basic(addr2)));

        // Create an ActiveOverlay from the parent
        let mock_db = MockDb::new();
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db_arc = Arc::new(UnsafeCell::new(mock_db));
        let mut active_overlay = parent_overlay_db.create_overlay(mock_db_arc);

        // Create accounts to commit
        let account1 = Account {
            info: AccountInfo {
                balance: U256::from(5000),
                nonce: 10,
                code_hash,
                code: Some(code.clone()),
            },
            storage: HashMap::from_iter([
                (U256::from(100), EvmStorageSlot::new(U256::from(1000))),
                (U256::from(200), EvmStorageSlot::new(U256::from(2000))),
            ]),
            status: AccountStatus::Touched,
        };

        let account2 = Account {
            info: AccountInfo {
                balance: U256::from(7500),
                nonce: 15,
                code_hash: b256!(
                    "0000000000000000000000000000000000000000000000000000000000000000"
                ),
                code: None,
            },
            storage: HashMap::from_iter([(U256::from(300), EvmStorageSlot::new(U256::from(3000)))]),
            status: AccountStatus::Touched,
        };

        let evm_state: EvmState = HashMap::from_iter([(addr1, account1), (addr2, account2)]);

        // Commit to the ActiveOverlay
        active_overlay.commit(evm_state);

        // Verify that changes are now visible in the PARENT OverlayDb
        assert!(parent_overlay_db.is_cached(&TableKey::Basic(addr1)));
        assert!(parent_overlay_db.is_cached(&TableKey::Basic(addr2)));
        assert!(parent_overlay_db.is_cached(&TableKey::CodeByHash(code_hash)));
        assert!(parent_overlay_db.is_cached(&TableKey::Storage(addr1, U256::from(100))));
        assert!(parent_overlay_db.is_cached(&TableKey::Storage(addr1, U256::from(200))));
        assert!(parent_overlay_db.is_cached(&TableKey::Storage(addr2, U256::from(300))));

        // Verify account data is accessible through parent OverlayDb
        assert_eq!(
            parent_overlay_db.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(5000)
        );
        assert_eq!(
            parent_overlay_db.basic_ref(addr1).unwrap().unwrap().nonce,
            10
        );
        assert_eq!(
            parent_overlay_db.basic_ref(addr2).unwrap().unwrap().balance,
            U256::from(7500)
        );
        assert_eq!(
            parent_overlay_db.basic_ref(addr2).unwrap().unwrap().nonce,
            15
        );

        // Verify code is accessible
        assert_eq!(
            parent_overlay_db
                .code_by_hash_ref(code_hash)
                .unwrap()
                .original_bytes(),
            code_bytes
        );

        // Verify storage is accessible
        assert_eq!(
            parent_overlay_db
                .storage_ref(addr1, U256::from(100))
                .unwrap(),
            U256::from(1000)
        );
        assert_eq!(
            parent_overlay_db
                .storage_ref(addr1, U256::from(200))
                .unwrap(),
            U256::from(2000)
        );
        assert_eq!(
            parent_overlay_db
                .storage_ref(addr2, U256::from(300))
                .unwrap(),
            U256::from(3000)
        );

        // Verify cache entry count matches expectations
        // 2 accounts + 1 code + 3 storage slots = 6 entries
        assert_eq!(parent_overlay_db.cache_entry_count(), 6);

        // Also verify that another ActiveOverlay created from the same parent
        // can see these committed changes
        let mock_db2 = MockDb::new();
        #[allow(clippy::arc_with_non_send_sync)]
        let mock_db2_arc = Arc::new(UnsafeCell::new(mock_db2));
        let active_overlay2 = parent_overlay_db.create_overlay(mock_db2_arc);

        assert_eq!(
            active_overlay2.basic_ref(addr1).unwrap().unwrap().balance,
            U256::from(5000)
        );
        assert_eq!(
            active_overlay2.storage_ref(addr1, U256::from(100)).unwrap(),
            U256::from(1000)
        );
    }
}
