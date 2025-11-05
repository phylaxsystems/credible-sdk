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
use revm::Database;
use std::{
    collections::HashMap,
    sync::Arc,
};

/// Maps storage slots to their values.
/// Also contains a flag to indicate if the account is self destructed.
/// Dont read from inner db is used to indicate that the account is not self destructed.
#[derive(Debug, Clone, Default)]
pub struct ForkStorageMap {
    pub map: HashMap<U256, U256>,
    pub dont_read_from_inner_db: bool,
}

/// Contains mutations on top of an existing database.
#[derive(Debug)]
pub struct ForkDb<ExtDb> {
    /// Maps addresses to storage slots and their history indexed by block.
    pub storage: HashMap<Address, ForkStorageMap>,
    /// Maps addresses to their account info and indexes it by block.
    pub(super) basic: HashMap<Address, AccountInfo>,
    /// Maps bytecode hashes to bytecode.
    pub(super) code_by_hash: HashMap<B256, Bytecode>,
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
        let value = match self.storage.get(&address) {
            Some(s) => {
                // If the account is self destructed, do not read from inner db.
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

impl<ExtDb> DatabaseCommit for ForkDb<ExtDb> {
    fn commit(&mut self, changes: EvmState) {
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                self.basic.insert(address, account.info.clone());

                let fork_storage_map = self.storage.entry(address).or_default();

                // Mark the account to not read from the inner database if it is self destructed.
                fork_storage_map.dont_read_from_inner_db = true;
                fork_storage_map.map.clear();

                continue;
            }

            if account.info.code.is_some() {
                self.code_by_hash
                    .insert(account.info.code_hash, account.info.code.clone().unwrap());
            }

            self.basic.insert(address, account.info.clone());
            match self.storage.get_mut(&address) {
                Some(s) => {
                    s.map.extend(
                        account
                            .storage
                            .into_iter()
                            .map(|(k, v)| (k, v.present_value())),
                    );
                }
                None => {
                    self.storage.insert(
                        address,
                        ForkStorageMap {
                            map: account
                                .storage
                                .into_iter()
                                .map(|(k, v)| (k, v.present_value()))
                                .collect(),
                            dont_read_from_inner_db: false,
                        },
                    );
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
        TableKey,
        TableValue,
    };
    use revm::database::{
        CacheDB,
        EmptyDBTyped,
    };
    use std::convert::Infallible;

    use crate::{
        db::{
            DatabaseRef,
            overlay::OverlayDb,
        },
        primitives::{
            Account,
            AccountStatus,
            BlockChanges,
            EvmStorageSlot,
            U256,
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
            is_cold: false,
        };

        storage.insert(uint!(0_U256), evm_storage_slot.clone());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo::default(),
                storage: storage.clone(),
                status: AccountStatus::Touched,
            },
        );
        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(AccountInfo::default()),
        );
        overlay_db.overlay.insert(
            TableKey::Storage(Address::ZERO, uint!(0_U256)),
            TableValue::Storage(uint!(1_U256).into()),
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
    async fn test_commit_self_destruct() {
        let overlay_db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();

        let mut evm_state = EvmState::default();

        let mut storage = HashMap::from_iter([]);

        let evm_storage_slot = EvmStorageSlot {
            original_value: uint!(0_U256),
            present_value: uint!(1_U256),
            is_cold: false,
        };

        storage.insert(uint!(0_U256), evm_storage_slot.clone());

        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo::default(),
                storage: storage.clone(),
                status: AccountStatus::Touched,
            },
        );
        overlay_db.overlay.insert(
            TableKey::Basic(Address::ZERO),
            TableValue::Basic(AccountInfo::default()),
        );
        overlay_db.overlay.insert(
            TableKey::Storage(Address::ZERO, uint!(0_U256)),
            TableValue::Storage(uint!(1_U256).into()),
        );

        let mut fork_db = overlay_db.fork();
        assert_eq!(
            overlay_db
                .storage_ref(Address::ZERO, uint!(0_U256))
                .unwrap(),
            evm_storage_slot.present_value()
        );
        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            evm_storage_slot.present_value()
        );

        let mut evm_state = EvmState::default();
        evm_state.insert(
            Address::ZERO,
            Account {
                info: AccountInfo::default(),
                storage: HashMap::from_iter([]),
                status: AccountStatus::SelfDestructed | AccountStatus::Touched,
            },
        );

        fork_db.commit(evm_state);

        assert_eq!(
            overlay_db
                .storage_ref(Address::ZERO, uint!(0_U256))
                .unwrap(),
            evm_storage_slot.present_value()
        );

        assert_eq!(
            fork_db.storage_ref(Address::ZERO, uint!(0_U256)).unwrap(),
            U256::ZERO
        );
    }
}
