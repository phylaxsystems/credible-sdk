use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        RollbackDb,
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
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
struct DatabaseTypes {
    basic: DashMap<Address, AccountInfo>,
    code_by_hash: DashMap<B256, Bytecode>,
    storage: DashMap<Address, StorageEntry>,
    block_hash: DashMap<u64, B256>,
}

#[derive(Debug, Clone, Default)]
struct StorageEntry {
    map: DashMap<U256, B256>,
    dont_read_from_inner_db: bool,
}

impl StorageEntry {
    fn mark_selfdestructed(&mut self) {
        self.dont_read_from_inner_db = true;
        self.map.clear();
    }
}

impl DatabaseTypes {
    fn get_basic(&self, address: Address) -> Option<AccountInfo> {
        self.basic.get(&address).map(|guard| guard.value().clone())
    }

    fn get_code(&self, code_hash: &B256) -> Option<Bytecode> {
        self.code_by_hash
            .get(code_hash)
            .map(|guard| guard.value().clone())
    }

    fn get_storage(&self, address: Address, slot: U256) -> Option<B256> {
        self.storage
            .get(&address)
            .and_then(|entry| entry.map.get(&slot).map(|guard| *guard.value()))
    }

    fn is_selfdestructed(&self, address: Address) -> bool {
        self.storage
            .get(&address)
            .is_some_and(|entry| entry.dont_read_from_inner_db)
    }

    fn get_block_hash(&self, number: u64) -> Option<B256> {
        self.block_hash.get(&number).map(|guard| *guard.value())
    }

    fn insert_basic(&mut self, address: Address, info: AccountInfo) {
        self.basic.insert(address, info);
    }

    fn insert_code(&mut self, code_hash: B256, code: Bytecode) {
        self.code_by_hash.insert(code_hash, code);
    }

    fn insert_storage(&mut self, address: Address, slot: U256, value: B256) {
        let entry = if let Some(entry) = self.storage.get_mut(&address) {
            entry
        } else {
            self.storage.insert(address, StorageEntry::default());
            self.storage.get_mut(&address).expect("entry must exist")
        };
        entry.map.insert(slot, value);
    }

    fn insert_selfdestructed(&mut self, address: Address) {
        let mut entry = if let Some(entry) = self.storage.get_mut(&address) {
            entry
        } else {
            self.storage.insert(address, StorageEntry::default());
            self.storage.get_mut(&address).expect("entry must exist")
        };
        entry.mark_selfdestructed();
    }

    #[allow(dead_code)]
    fn insert_block_hash(&mut self, number: u64, block_hash: B256) {
        self.block_hash.insert(number, block_hash);
    }

    fn apply_delta(&mut self, delta: &DatabaseTypes) {
        for entry in &delta.basic {
            self.basic.insert(*entry.key(), entry.value().clone());
        }
        for entry in &delta.code_by_hash {
            self.code_by_hash
                .insert(*entry.key(), entry.value().clone());
        }
        for entry in &delta.storage {
            let address = *entry.key();
            let delta_entry = entry.value();

            let mut storage_entry = if let Some(entry) = self.storage.get_mut(&address) {
                entry
            } else {
                self.storage.insert(address, StorageEntry::default());
                self.storage.get_mut(&address).expect("entry must exist")
            };

            let storage_entry = storage_entry.value_mut();
            storage_entry.dont_read_from_inner_db |= delta_entry.dont_read_from_inner_db;

            if delta_entry.dont_read_from_inner_db {
                storage_entry.map.clear();
            }

            for storage_kv in &delta_entry.map {
                storage_entry
                    .map
                    .insert(*storage_kv.key(), *storage_kv.value());
            }
        }
        for entry in &delta.block_hash {
            self.block_hash.insert(*entry.key(), *entry.value());
        }
    }
}

/// Versioned database that keeps a base state plus a changelog per commit.
/// Rolling back rebuilds the in-memory state by replaying the changelog from the
/// persisted base snapshot.
#[derive(Debug)]
pub struct VersionDb<Db> {
    inner_db: Arc<Db>,
    base_state: DatabaseTypes,
    state: DatabaseTypes,
    commit_log: Vec<DatabaseTypes>,
    commit_depth: usize,
}

impl<Db: Clone> Clone for VersionDb<Db> {
    fn clone(&self) -> Self {
        Self {
            inner_db: self.inner_db.clone(),
            base_state: self.base_state.clone(),
            state: self.state.clone(),
            commit_log: self.commit_log.clone(),
            commit_depth: self.commit_depth,
        }
    }
}

impl<Db> VersionDb<Db> {
    pub fn new(inner_db: Db) -> Self {
        let base_state = DatabaseTypes::default();
        Self {
            inner_db: Arc::new(inner_db),
            state: base_state.clone(),
            base_state,
            commit_log: Vec::new(),
            commit_depth: 0,
        }
    }

    /// Convert an `EvmState` into a typed delta optimized for storage in `DatabaseTypes`.
    fn build_delta(changes: EvmState) -> DatabaseTypes {
        let mut delta = DatabaseTypes::default();

        for (address, mut account) in changes {
            if !account.is_touched() {
                continue;
            }

            if account.is_selfdestructed() {
                delta.insert_selfdestructed(address);
            }

            delta.insert_basic(address, account.info.clone());

            if let Some(code) = account.info.code.take() {
                delta.insert_code(account.info.code_hash, code);
            }

            for (slot, storage_slot) in account.storage {
                let value_b256: B256 = storage_slot.present_value().to_be_bytes().into();
                delta.insert_storage(address, slot, value_b256);
            }
        }

        delta
    }
}

impl<Db: DatabaseRef> DatabaseRef for VersionDb<Db> {
    type Error = <Db as DatabaseRef>::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <Self as DatabaseRef>::Error> {
        if let Some(account_info) = self.state.get_basic(address) {
            return Ok(Some(account_info));
        }

        self.inner_db.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, <Self as DatabaseRef>::Error> {
        if let Some(code) = self.state.get_code(&code_hash) {
            return Ok(code);
        }

        self.inner_db.code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <Self as DatabaseRef>::Error> {
        if self.state.is_selfdestructed(address) {
            if let Some(raw_value) = self.state.get_storage(address, slot) {
                let value_u256: U256 = raw_value.into();
                return Ok(value_u256);
            }
            return Ok(U256::ZERO);
        }

        if let Some(raw_value) = self.state.get_storage(address, slot) {
            let value_u256: U256 = raw_value.into();
            return Ok(value_u256);
        }

        self.inner_db.storage_ref(address, slot)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, <Self as DatabaseRef>::Error> {
        if let Some(hash) = self.state.get_block_hash(number) {
            return Ok(hash);
        }

        self.inner_db.block_hash_ref(number)
    }
}

impl<Db: DatabaseRef> Database for VersionDb<Db> {
    type Error = <Db as DatabaseRef>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(&mut self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.storage_ref(address, slot)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

impl<Db> DatabaseCommit for VersionDb<Db> {
    fn commit(&mut self, changes: EvmState) {
        let delta = Self::build_delta(changes);
        self.state.apply_delta(&delta);
        self.commit_log.push(delta);
        self.commit_depth += 1;
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum VersionDbError {
    #[error("Rollback depth {attempted} exceeds current depth {max_depth}")]
    InvalidDepth { attempted: usize, max_depth: usize },
}

impl<Db> RollbackDb for VersionDb<Db> {
    type Err = VersionDbError;

    fn rollback_to(&mut self, depth: usize) -> Result<(), VersionDbError> {
        if depth > self.commit_log.len() {
            return Err(VersionDbError::InvalidDepth {
                attempted: depth,
                max_depth: self.commit_log.len(),
            });
        }

        self.state = self.base_state.clone();
        for delta in self.commit_log.iter().take(depth) {
            self.state.apply_delta(delta);
        }

        self.commit_log.truncate(depth);
        self.commit_depth = depth;
        Ok(())
    }

    fn collapse_log(&mut self) {
        self.base_state = self.state.clone();
        self.commit_log.clear();
        self.commit_depth = 0;
    }

    /// Number of commits currently tracked.
    fn depth(&self) -> usize {
        self.commit_depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::overlay::test_utils::{
            MockDb,
            mock_account_info,
        },
        primitives::{
            Account,
            AccountStatus,
            Bytecode,
            EvmStorage,
            EvmStorageSlot,
            U256,
            address,
            uint,
        },
    };
    use alloy_primitives::KECCAK256_EMPTY;

    #[test]
    fn commit_updates_versioned_state() {
        let mut version_db = VersionDb::new(MockDb::new());

        let mut changes = EvmState::default();
        changes.insert(
            address!("0000000000000000000000000000000000000001"),
            Account {
                info: mock_account_info(uint!(10_U256), 1, None),
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(changes);

        let balance = version_db
            .basic_ref(address!("0000000000000000000000000000000000000001"))
            .unwrap()
            .unwrap()
            .balance;

        assert_eq!(balance, uint!(10_U256));
        assert_eq!(version_db.depth(), 1);
        assert!(
            version_db
                .state
                .basic
                .contains_key(&address!("0000000000000000000000000000000000000001"))
        );
    }

    #[test]
    fn rollback_to_invalid_depth_errors() {
        let mut version_db = VersionDb::new(MockDb::new());

        let err = version_db.rollback_to(1).unwrap_err();
        assert_eq!(
            err,
            VersionDbError::InvalidDepth {
                attempted: 1,
                max_depth: 0
            }
        );
    }

    #[test]
    fn can_rollback_to_prior_commit() {
        let mut version_db = VersionDb::new(MockDb::new());

        let mut first = EvmState::default();
        first.insert(
            address!("0000000000000000000000000000000000000001"),
            Account {
                info: mock_account_info(uint!(10_U256), 1, None),
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(first);

        let mut second = EvmState::default();
        second.insert(
            address!("0000000000000000000000000000000000000001"),
            Account {
                info: mock_account_info(uint!(20_U256), 2, None),
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(second);

        assert_eq!(version_db.depth(), 2);
        assert_eq!(
            version_db
                .basic_ref(address!("0000000000000000000000000000000000000001"))
                .unwrap()
                .unwrap()
                .balance,
            uint!(20_U256)
        );

        version_db.rollback_to(1).unwrap();

        assert_eq!(
            version_db
                .basic_ref(address!("0000000000000000000000000000000000000001"))
                .unwrap()
                .unwrap()
                .balance,
            uint!(10_U256)
        );
        assert_eq!(version_db.depth(), 1);
    }

    #[test]
    fn selfdestructed_accounts_clear_storage_and_skip_inner_db() {
        let mut inner = MockDb::new();
        let address = address!("0000000000000000000000000000000000000001");
        let slot = U256::from(1);

        inner.insert_account(address, mock_account_info(uint!(1_U256), 0, None));
        inner.insert_storage(address, slot, uint!(5_U256));

        let mut version_db = VersionDb::new(inner);

        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(5_U256)
        );
        let storage_calls_after_first_read = version_db.inner_db.get_storage_calls();

        let mut changes = EvmState::default();
        changes.insert(
            address,
            Account {
                info: AccountInfo::default(),
                storage: EvmStorage::default(),
                status: AccountStatus::SelfDestructed | AccountStatus::Touched,
            },
        );
        version_db.commit(changes);

        assert_eq!(version_db.storage_ref(address, slot).unwrap(), U256::ZERO);
        assert_eq!(
            version_db.inner_db.get_storage_calls(),
            storage_calls_after_first_read
        );

        version_db.rollback_to(0).unwrap();
        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(5_U256)
        );
        assert!(version_db.inner_db.get_storage_calls() > storage_calls_after_first_read);
    }

    #[test]
    fn recreated_account_after_selfdestruct_does_not_fallback_to_inner_db() {
        let mut inner = MockDb::new();
        let address = address!("0000000000000000000000000000000000000001");
        let slot = U256::from(1);

        inner.insert_account(address, mock_account_info(uint!(1_U256), 0, None));
        inner.insert_storage(address, slot, uint!(5_U256));

        let mut version_db = VersionDb::new(inner);

        let mut initial_changes = EvmState::default();
        initial_changes.insert(
            address,
            Account {
                info: AccountInfo::default(),
                storage: EvmStorage::default(),
                status: AccountStatus::SelfDestructed | AccountStatus::Touched,
            },
        );
        version_db.commit(initial_changes);

        let storage_calls_after_selfdestruct = version_db.inner_db.get_storage_calls();

        let mut recreation_storage = EvmStorage::default();
        recreation_storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(9_U256),
                ..Default::default()
            },
        );
        let mut recreation_state = EvmState::default();
        recreation_state.insert(
            address,
            Account {
                info: mock_account_info(uint!(2_U256), 1, None),
                storage: recreation_storage,
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(recreation_state);

        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(9_U256)
        );
        assert_eq!(
            version_db.inner_db.get_storage_calls(),
            storage_calls_after_selfdestruct
        );
    }

    #[test]
    fn rollback_restores_prior_code_and_storage_per_depth() {
        let mut version_db = VersionDb::new(MockDb::new());
        let address = address!("0000000000000000000000000000000000000003");
        let slot = U256::from(1);

        let code_one = Bytecode::new_raw(vec![1u8, 2, 3].into());
        let code_two = Bytecode::new_raw(vec![4u8, 5, 6].into());

        let mut first_storage = EvmStorage::default();
        first_storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(11_U256),
                ..Default::default()
            },
        );
        let mut first = EvmState::default();
        first.insert(
            address,
            Account {
                info: mock_account_info(uint!(1_U256), 0, Some(code_one.clone())),
                storage: first_storage,
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(first);

        let mut second_storage = EvmStorage::default();
        second_storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(22_U256),
                ..Default::default()
            },
        );
        let mut second = EvmState::default();
        second.insert(
            address,
            Account {
                info: mock_account_info(uint!(2_U256), 1, Some(code_two.clone())),
                storage: second_storage,
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(second);

        let code_one_hash = code_one.hash_slow();
        let code_two_hash = code_two.hash_slow();

        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(22_U256)
        );
        assert_eq!(
            version_db.code_by_hash_ref(code_two_hash).unwrap(),
            code_two
        );

        version_db.rollback_to(1).unwrap();

        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(11_U256)
        );
        assert_eq!(
            version_db.code_by_hash_ref(code_one_hash).unwrap(),
            code_one
        );
        assert!(version_db.code_by_hash_ref(code_two_hash).is_err());
    }

    #[test]
    fn collapse_keeps_state_and_resets_log() {
        let mut version_db = VersionDb::new(MockDb::new());

        let mut first = EvmState::default();
        let mut first_storage = EvmStorage::default();
        first_storage.insert(
            U256::from(1),
            EvmStorageSlot {
                present_value: uint!(2_U256),
                ..Default::default()
            },
        );
        first.insert(
            address!("0000000000000000000000000000000000000001"),
            Account {
                info: mock_account_info(uint!(10_U256), 1, None),
                storage: first_storage,
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(first);

        // Collapse the log and treat current state as the new base.
        version_db.collapse_log();
        assert_eq!(version_db.depth(), 0);
        assert!(version_db.commit_log.is_empty());
        assert!(
            version_db
                .base_state
                .basic
                .contains_key(&address!("0000000000000000000000000000000000000001"))
        );

        // Apply a new commit on top of the collapsed base and roll it back to ensure the base is kept.
        let mut second = EvmState::default();
        second.insert(
            address!("0000000000000000000000000000000000000001"),
            Account {
                info: mock_account_info(uint!(20_U256), 2, None),
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(second);
        assert_eq!(version_db.depth(), 1);
        version_db.rollback_to(0).unwrap();

        // After rollback we should still see the collapsed base state.
        assert_eq!(
            version_db
                .basic_ref(address!("0000000000000000000000000000000000000001"))
                .unwrap()
                .unwrap()
                .balance,
            uint!(10_U256)
        );
        assert_eq!(
            version_db
                .storage_ref(
                    address!("0000000000000000000000000000000000000001"),
                    U256::from(1)
                )
                .unwrap(),
            uint!(2_U256)
        );
    }

    #[test]
    fn falls_back_to_inner_db_for_missing_values() {
        let mut inner = MockDb::new();
        inner.insert_account(
            address!("0000000000000000000000000000000000000002"),
            AccountInfo {
                balance: uint!(5_U256),
                nonce: 0,
                code_hash: KECCAK256_EMPTY,
                code: None,
            },
        );

        let version_db = VersionDb::new(inner);

        let balance = version_db
            .basic_ref(address!("0000000000000000000000000000000000000002"))
            .unwrap()
            .unwrap()
            .balance;

        assert_eq!(balance, uint!(5_U256));
    }
}
