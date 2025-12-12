use crate::{
    db::{
        DatabaseCommit,
        DatabaseRef,
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
use rapidhash::fast::RandomState;
use revm::{
    Database,
    state::AccountStatus,
};
use std::{
    collections::{
        HashMap,
        hash_map::Entry,
    },
    sync::Arc,
};

/// Maps storage slots to their values.
#[derive(Debug, Clone, Default)]
pub struct ForkStorageMap {
    pub map: HashMap<U256, U256, RandomState>,
    pub dont_read_from_inner_db: bool,
}

/// Contains mutations on top of an existing database.
#[derive(Debug)]
pub struct ForkDb<ExtDb> {
    /// Maps addresses to storage slots and their history indexed by block.
    pub storage: HashMap<Address, ForkStorageMap, RandomState>,
    /// Maps addresses to their account info and indexes it by block.
    pub(super) basic: HashMap<Address, AccountInfo, RandomState>,
    /// Maps bytecode hashes to bytecode.
    pub(super) code_by_hash: HashMap<B256, Bytecode, RandomState>,
    /// Inner database.
    pub(super) inner_db: Arc<ExtDb>,
}

impl<ExtDb> Clone for ForkDb<ExtDb> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            basic: self.basic.clone(),
            code_by_hash: self.code_by_hash.clone(),
            inner_db: self.inner_db.clone(),
        }
    }
}

/// This implementation of `Database` is used to read from the fork db, it does not modify the internal
/// cache
impl<ExtDb: DatabaseRef> Database for ForkDb<ExtDb> {
    type Error = <ExtDb as DatabaseRef>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

impl<ExtDb: DatabaseRef> DatabaseRef for ForkDb<ExtDb> {
    type Error = <ExtDb as DatabaseRef>::Error;

    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <Self as DatabaseRef>::Error> {
        match self.basic.get(&address) {
            Some(b) => Ok(Some(b.clone())),
            None => Ok(self.inner_db.basic_ref(address)?),
        }
    }

    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <Self as DatabaseRef>::Error> {
        // Check fork's storage first, then fall back to inner db
        let value = match self.storage.get(&address) {
            Some(s) => {
                // If the slot is not present in the fork, we can skip reading from the inner db as it
                // won't be present either
                if s.dont_read_from_inner_db {
                    *s.map.get(&slot).unwrap_or(&U256::ZERO)
                } else if let Some(v) = s.map.get(&slot) {
                    *v
                } else {
                    self.inner_db.storage_ref(address, slot)?
                }
            }
            None => self.inner_db.storage_ref(address, slot)?,
        };

        Ok(value)
    }

    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, <Self as DatabaseRef>::Error> {
        match self.code_by_hash.get(&hash) {
            Some(code) => Ok(code.clone()),
            None => Ok(self.inner_db.code_by_hash_ref(hash)?),
        }
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, <Self as DatabaseRef>::Error> {
        self.inner_db.block_hash_ref(number)
    }
}

/// Post-Cancun `DatabaseCommit` implementation.
///
/// ## SELFDESTRUCT Behavior (EIP-6780)
///
/// Post-Cancun, SELFDESTRUCT only transfers balance to the beneficiary.
/// Code, storage, and nonce remain intact. We treat selfdestructed accounts
/// the same as any other touched account, just update the account info
/// (which will have balance = 0) and any storage changes.
impl<ExtDb> DatabaseCommit for ForkDb<ExtDb> {
    fn commit(&mut self, changes: EvmState) {
        for (address, account) in changes {
            // Note for self-destructed accounts (post-Cancun): if an account is self-destructed, it
            // means it was created within the same transaction as it was self-destructed.
            // Therefore, we can skip it from being written into the cache. We explicitly check for
            // self-destructed accounts here, in case the flag is set without the `Touched` flag
            if !account.is_touched()
                || account.status == AccountStatus::SelfDestructed
                || account.status == AccountStatus::SelfDestructedLocal
            {
                continue;
            }

            let is_created = account.is_created();

            if let Some(code) = &account.info.code {
                self.code_by_hash
                    .insert(account.info.code_hash, code.clone());
            }

            self.basic.insert(address, account.info.clone());

            if is_created || !account.storage.is_empty() {
                let mut storage_updates = account
                    .storage
                    .into_iter()
                    .map(|(k, v)| (k, v.present_value()))
                    .collect::<HashMap<_, _, RandomState>>();

                match self.storage.entry(address) {
                    Entry::Occupied(mut entry) => {
                        let storage_map = entry.get_mut();
                        if is_created {
                            storage_map.dont_read_from_inner_db = true;
                        }
                        storage_map.map.extend(storage_updates.drain());
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(ForkStorageMap {
                            map: storage_updates,
                            dont_read_from_inner_db: is_created,
                        });
                    }
                }
            }
        }
    }
}

impl<ExtDb> ForkDb<ExtDb> {
    /// Creates a new `ForkDb`.
    pub fn new(inner_db: ExtDb) -> Self {
        Self {
            storage: HashMap::default(),
            basic: HashMap::default(),
            code_by_hash: HashMap::default(),
            inner_db: Arc::new(inner_db),
        }
    }

    /// Replace the inner database with a new one.
    /// Returns the previous inner database.
    pub fn replace_inner_db(&mut self, new_db: Arc<ExtDb>) -> Arc<ExtDb> {
        let prev_inner_db = Arc::clone(&self.inner_db);

        self.inner_db = new_db;

        prev_inner_db
    }

    /// Inserts an account info into the fork db.
    /// This will overwrite any existing account info.
    pub fn insert_account_info(&mut self, address: Address, account_info: AccountInfo) {
        self.basic.insert(address, account_info);
    }

    pub fn invalidate(&mut self) {
        self.storage = HashMap::default();
        self.basic = HashMap::default();
        self.code_by_hash = HashMap::default();
    }
}

#[cfg(test)]
mod fork_db_tests {
    use super::*;
    use crate::db::overlay::{
        OverlayDb,
        TableKey,
        TableValue,
        test_utils::MockDb,
    };
    use revm::database::{
        CacheDB,
        EmptyDBTyped,
    };
    use std::{
        convert::Infallible,
        sync::Arc,
    };

    use crate::{
        db::DatabaseRef,
        primitives::{
            Account,
            AccountStatus,
            BlockChanges,
            EvmStorageSlot,
            U256,
            address,
            uint,
        },
        test_utils::random_bytes,
    };

    use revm::primitives::{
        KECCAK_EMPTY,
        keccak256,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_basic() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();

        let mut block_changes = BlockChanges::default();
        let mut evm_state = EvmState::default();

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo {
                    nonce: 0,
                    balance: uint!(1000_U256),
                    ..Default::default()
                },
                transaction_id: 0,
                storage: HashMap::from_iter([]),
                status: AccountStatus::Touched,
            },
        );

        let account_info = AccountInfo {
            nonce: 0,
            balance: uint!(1000_U256),
            ..Default::default()
        };

        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(account_info),
        );

        block_changes.state_changes = evm_state.clone();

        let mut fork_db = overlay_db.fork();

        assert_eq!(
            overlay_db
                .basic_ref(Address::ZERO)
                .unwrap()
                .unwrap()
                .balance,
            uint!(1000_U256)
        );

        assert_eq!(
            fork_db.basic_ref(Address::ZERO).unwrap().unwrap().balance,
            uint!(1000_U256)
        );

        evm_state.get_mut(&Address::ZERO).unwrap().info.balance = uint!(2000_U256);

        fork_db.commit(evm_state);

        assert_eq!(
            fork_db.basic_ref(Address::ZERO).unwrap().unwrap().balance,
            uint!(2000_U256)
        );
        assert_eq!(
            overlay_db
                .basic_ref(Address::ZERO)
                .unwrap()
                .unwrap()
                .balance,
            uint!(1000_U256)
        );
    }

    #[tokio::test]
    async fn test_storage() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();
        let mut evm_state = EvmState::default();

        let mut storage = HashMap::from_iter([]);

        let mut evm_storage_slot = EvmStorageSlot {
            original_value: uint!(0_U256),
            present_value: uint!(1_U256),
            transaction_id: 0,
            is_cold: false,
        };

        storage.insert(uint!(0_U256), evm_storage_slot.clone());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo::default(),
                transaction_id: 0,
                storage: storage.clone(),
                status: AccountStatus::Touched,
            },
        );
        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(AccountInfo::default()),
        );
        overlay_db.overlay.insert(
            TableKey::Storage(Address::ZERO),
            TableValue::Storage(ForkStorageMap {
                map: HashMap::from_iter([(uint!(0_U256), uint!(1_U256))]),
                dont_read_from_inner_db: false,
            }),
        );

        let mut fork_db = overlay_db.fork();

        assert_eq!(
            overlay_db
                .storage_ref(Address::ZERO, uint!(0_U256))
                .unwrap(),
            uint!(1_U256)
        );

        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            uint!(1_U256)
        );

        evm_storage_slot.original_value = uint!(1_U256);
        evm_storage_slot.present_value = uint!(2_U256);

        evm_state
            .get_mut(&Address::ZERO)
            .unwrap()
            .storage
            .insert(uint!(0_U256), evm_storage_slot);

        fork_db.commit(evm_state);

        assert_eq!(
            overlay_db
                .storage_ref(Address::ZERO, uint!(0_U256))
                .unwrap(),
            uint!(1_U256)
        );

        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            uint!(2_U256)
        );
    }

    #[tokio::test]
    async fn test_code_by_hash() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();
        let mut evm_state = EvmState::default();
        let mut fork_db = overlay_db.fork();

        let code_bytes = random_bytes::<32>();
        let code_hash = keccak256(code_bytes);
        let bytecode = Bytecode::new_raw(code_bytes.into());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo {
                    code_hash,
                    code: Some(bytecode.clone()),
                    ..Default::default()
                },
                transaction_id: 0,
                storage: HashMap::from_iter([]),
                status: AccountStatus::Touched,
            },
        );

        let account_info = AccountInfo {
            code_hash,
            code: Some(bytecode.clone()),
            ..Default::default()
        };

        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(account_info),
        );

        overlay_db.overlay.insert(
            TableKey::CodeByHash(bytecode.hash_slow()),
            TableValue::CodeByHash(bytecode.clone()),
        );

        assert_eq!(overlay_db.code_by_hash_ref(code_hash).unwrap(), bytecode);
        assert_eq!(fork_db.code_by_hash_ref(code_hash).unwrap(), bytecode);

        let mut evm_state = EvmState::default();

        let code_bytes = random_bytes::<64>();
        let code_hash = keccak256(code_bytes);
        let bytecode = Bytecode::new_raw(code_bytes.into());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo {
                    code_hash,
                    code: Some(bytecode.clone()),
                    ..Default::default()
                },
                transaction_id: 0,
                storage: HashMap::from_iter([]),
                status: AccountStatus::Touched,
            },
        );

        fork_db.commit(evm_state);

        assert_eq!(fork_db.code_by_hash_ref(code_hash).unwrap(), bytecode);
    }

    #[tokio::test]
    async fn test_block_hash() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();

        overlay_db
            .overlay
            .insert(TableKey::BlockHash(0), TableValue::BlockHash(KECCAK_EMPTY));

        assert_eq!(overlay_db.block_hash_ref(0), Ok(KECCAK_EMPTY));
    }

    #[tokio::test]
    async fn test_commit_self_destruct_post_cancun() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();

        let mut evm_state = EvmState::default();

        let evm_storage_slot = EvmStorageSlot {
            original_value: uint!(0_U256),
            present_value: uint!(1_U256),
            transaction_id: 0,
            is_cold: false,
        };

        let mut storage = HashMap::from_iter([]);
        storage.insert(uint!(0_U256), evm_storage_slot.clone());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo {
                    balance: uint!(1000_U256),
                    ..Default::default()
                },
                transaction_id: 0,
                storage: storage.clone(),
                status: AccountStatus::Touched,
            },
        );

        // Setup overlay with initial state
        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(AccountInfo {
                balance: uint!(1000_U256),
                ..Default::default()
            }),
        );
        overlay_db.overlay.insert(
            TableKey::Storage(Address::ZERO),
            TableValue::Storage(ForkStorageMap {
                map: HashMap::from_iter([(uint!(0_U256), evm_storage_slot.present_value())]),
                dont_read_from_inner_db: false,
            }),
        );

        let mut fork_db = overlay_db.fork();

        // Verify initial state
        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            uint!(1_U256)
        );
        assert_eq!(
            fork_db.basic_ref(Address::ZERO).unwrap().unwrap().balance,
            uint!(1000_U256)
        );

        // Commit selfdestruct
        let mut evm_state = EvmState::default();
        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo {
                    balance: U256::ZERO,
                    ..Default::default()
                },
                transaction_id: 0,
                storage: HashMap::from_iter([]),
                status: AccountStatus::SelfDestructed | AccountStatus::Touched,
            },
        );

        fork_db.commit(evm_state);

        // Post-Cancun: balance is zero
        assert_eq!(
            fork_db.basic_ref(Address::ZERO).unwrap().unwrap().balance,
            U256::ZERO
        );

        // Post-Cancun: storage PERSISTS (reads from inner db)
        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            uint!(1_U256),
        );

        // Original overlay unchanged
        assert_eq!(
            overlay_db
                .storage_ref(Address::ZERO, uint!(0_U256))
                .unwrap(),
            uint!(1_U256)
        );
        assert_eq!(
            overlay_db
                .basic_ref(Address::ZERO)
                .unwrap()
                .unwrap()
                .balance,
            uint!(1000_U256)
        );
    }

    #[tokio::test]
    async fn test_commit_created_account_sets_dont_read_flag() {
        let non_created_address = address!("0000000000000000000000000000000000000001");
        let non_created_slot = U256::from(7);
        let fallback_slot = U256::from(8);
        let fallback_value = U256::from(9);

        let mut mock_db = MockDb::new();
        mock_db.insert_storage(non_created_address, fallback_slot, fallback_value);
        let mock_db = Arc::new(mock_db);
        let overlay_db: OverlayDb<MockDb> = OverlayDb::new(Some(mock_db.clone()));
        let mut fork_db = overlay_db.fork();

        let mut evm_state = EvmState::default();
        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo::default(),
                transaction_id: 0,
                storage: HashMap::default(),
                status: AccountStatus::Created | AccountStatus::Touched,
            },
        );

        fork_db.commit(evm_state);

        let storage_entry = fork_db
            .storage
            .get(&Address::ZERO)
            .expect("storage entry inserted for created account");
        assert!(storage_entry.dont_read_from_inner_db);

        assert_eq!(
            fork_db.storage_ref(Address::ZERO, U256::from(5)).unwrap(),
            U256::ZERO
        );
        assert_eq!(
            mock_db.get_storage_calls(),
            0,
            "inner db should not be read when dont_read flag is set"
        );

        let mut evm_state = EvmState::default();
        evm_state.insert(
            non_created_address,
            Account {
                info: AccountInfo::default(),
                transaction_id: 0,
                storage: HashMap::from_iter([(
                    non_created_slot,
                    EvmStorageSlot::new(U256::from(1), 0),
                )]),
                status: AccountStatus::Touched,
            },
        );

        fork_db.commit(evm_state);

        let storage_entry = fork_db
            .storage
            .get(&non_created_address)
            .expect("storage entry inserted for touched account");
        assert!(
            !storage_entry.dont_read_from_inner_db,
            "dont_read flag should not be set for existing accounts"
        );

        assert_eq!(
            fork_db
                .storage_ref(non_created_address, fallback_slot)
                .unwrap(),
            fallback_value,
        );
        assert_eq!(
            mock_db.get_storage_calls(),
            1,
            "inner db should be read when dont_read flag is not set"
        );
    }
}
