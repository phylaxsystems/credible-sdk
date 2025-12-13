use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        RollbackDb,
        fork_db::ForkDb,
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

/// Versioned database that reuses `ForkDb` for state management and keeps a
/// shallow log of commits. Rolling back rebuilds state by replaying logged
/// changes on top of the base snapshot.
#[derive(Debug, Clone)]
pub struct VersionDb<Db> {
    base_state: ForkDb<Db>,
    state: ForkDb<Db>,
    commit_log: Vec<EvmState>,
}

impl<Db> VersionDb<Db> {
    pub fn new(inner_db: Db) -> Self {
        let state = ForkDb::new(inner_db);
        Self {
            base_state: state.clone(),
            state,
            commit_log: Vec::new(),
        }
    }

    // FIXME: we have unnecessary clones for state data across
    // the `VersionDb`. these should be removed and replaced
    // with proper interior mutability
    fn rebuild_state(&mut self, depth: usize) {
        self.state = self.base_state.clone();
        for delta in self.commit_log.iter().take(depth) {
            self.state.commit(delta.clone());
        }
        self.commit_log.truncate(depth);
    }
}

impl<Db: DatabaseRef> DatabaseRef for VersionDb<Db> {
    type Error = <Db as DatabaseRef>::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <Self as DatabaseRef>::Error> {
        self.state.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, <Self as DatabaseRef>::Error> {
        self.state.code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <Self as DatabaseRef>::Error> {
        self.state.storage_ref(address, slot)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, <Self as DatabaseRef>::Error> {
        self.state.block_hash_ref(number)
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
        self.state.commit(changes.clone());
        self.commit_log.push(changes);
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
        if depth >= self.commit_log.len() {
            return Err(VersionDbError::InvalidDepth {
                attempted: depth,
                max_depth: self.commit_log.len(),
            });
        }

        self.rebuild_state(depth);
        Ok(())
    }

    fn collapse_log(&mut self) {
        self.base_state = self.state.clone();
        self.commit_log.clear();
    }

    /// Number of commits currently tracked.
    fn depth(&self) -> usize {
        self.commit_log.len()
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
            AccountInfo,
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
                transaction_id: 0,
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
                transaction_id: 0,
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
                transaction_id: 0,
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
    fn selfdestruct_storage_written_same_tx_persists_post_cancun() {
        let mut version_db = VersionDb::new(MockDb::new());
        let address = address!("0000000000000000000000000000000000000004");
        let slot = U256::from(1);

        let mut storage = EvmStorage::default();
        storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(7_U256),
                ..Default::default()
            },
        );

        let mut changes = EvmState::default();
        changes.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: U256::ZERO,
                    ..Default::default()
                },
                transaction_id: 0,
                storage,
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(changes);

        // Post-Cancun: storage persists even with selfdestruct
        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(7_U256)
        );
        assert_eq!(version_db.depth(), 1);
    }

    #[test]
    fn selfdestruct_prior_storage_persists_post_cancun() {
        let mut version_db = VersionDb::new(MockDb::new());
        let address = address!("0000000000000000000000000000000000000005");
        let slot = U256::from(7);

        // First commit writes storage.
        let mut storage = EvmStorage::default();
        storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(42_U256),
                ..Default::default()
            },
        );
        let mut create = EvmState::default();
        create.insert(
            address,
            Account {
                info: mock_account_info(uint!(100_U256), 0, None),
                transaction_id: 0,
                storage,
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(create);

        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(42_U256)
        );

        // Second commit selfdestructs the same account.
        let mut destroy = EvmState::default();
        destroy.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: U256::ZERO,
                    ..Default::default()
                },
                transaction_id: 0,
                storage: EvmStorage::default(),
                status: AccountStatus::Touched,
            },
        );
        version_db.commit(destroy);

        // Post-Cancun: storage PERSISTS after selfdestruct
        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(42_U256)
        );
        assert_eq!(
            version_db.basic_ref(address).unwrap().unwrap().balance,
            U256::ZERO
        );
        assert_eq!(version_db.depth(), 2);
    }

    #[test]
    fn write_to_selfdestructed_account_updates_storage() {
        let mut inner = MockDb::new();
        let address = address!("0000000000000000000000000000000000000001");
        let slot = U256::from(1);

        inner.insert_account(address, mock_account_info(uint!(1_U256), 0, None));
        inner.insert_storage(address, slot, uint!(5_U256));

        let mut version_db = VersionDb::new(inner);

        // Selfdestruct
        let mut initial_changes = EvmState::default();
        initial_changes.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: U256::ZERO,
                    ..Default::default()
                },
                transaction_id: 0,
                storage: EvmStorage::default(),
                status: AccountStatus::SelfDestructed | AccountStatus::Touched,
            },
        );
        version_db.commit(initial_changes);

        // Post-Cancun: storage still readable
        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(5_U256)
        );

        // Write new storage value
        let mut new_storage = EvmStorage::default();
        new_storage.insert(
            slot,
            EvmStorageSlot {
                present_value: uint!(9_U256),
                ..Default::default()
            },
        );
        let mut new_state = EvmState::default();
        new_state.insert(
            address,
            Account {
                info: mock_account_info(uint!(2_U256), 1, None),
                transaction_id: 0,
                storage: new_storage,
                status: AccountStatus::Touched,
            },
        );

        version_db.commit(new_state);

        // New value is used
        assert_eq!(
            version_db.storage_ref(address, slot).unwrap(),
            uint!(9_U256)
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
                transaction_id: 0,
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
                transaction_id: 0,
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
                transaction_id: 0,
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
                transaction_id: 0,
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
