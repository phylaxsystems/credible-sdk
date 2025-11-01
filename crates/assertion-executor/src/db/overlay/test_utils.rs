#![allow(clippy::missing_panics_doc)]
use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        NotFoundError,
        overlay::AccountInfo,
    },
    primitives::{
        Address,
        B256,
        Bytecode,
        EvmState,
        U256,
    },
};
use alloy_primitives::KECCAK256_EMPTY;
use revm::database::InMemoryDB;

use super::OverlayDb;
use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

impl<Db> OverlayDb<Db> {
    pub fn new_test() -> OverlayDb<InMemoryDB> {
        OverlayDb {
            underlying_db: Some(Arc::new(InMemoryDB::default())),
            overlay: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct MockDb {
    /// Simple storage for account info.
    accounts: HashMap<Address, AccountInfo>,
    /// Simple storage for contract code.
    contracts: HashMap<B256, Bytecode>,
    /// Simple storage for account storage slots.
    storage: HashMap<Address, HashMap<U256, U256>>,
    /// Simple storage for block hashes.
    block_hashes: HashMap<u64, B256>,
    /// Counter for calls to `basic_ref`
    basic_calls: Arc<Mutex<u64>>,
    /// Counter for calls to `code_by_hash_ref`
    code_calls: Arc<Mutex<u64>>,
    /// Counter for calls to `storage_ref`
    storage_calls: Arc<Mutex<u64>>,
    /// Counter for calls to `block_hash_ref`
    block_hash_calls: Arc<Mutex<u64>>,
}

#[allow(dead_code)]
impl MockDb {
    pub fn new() -> Self {
        MockDb::default()
    }

    // Methods to populate the mock database
    pub fn insert_account(&mut self, address: Address, info: AccountInfo) {
        if let Some(code) = &info.code {
            self.contracts.insert(info.code_hash, code.clone());
        }
        self.accounts.insert(address, info);
    }

    pub fn insert_storage(&mut self, address: Address, slot: U256, value: U256) {
        self.storage.entry(address).or_default().insert(slot, value);
    }

    pub fn insert_block_hash(&mut self, number: u64, hash: B256) {
        self.block_hashes.insert(number, hash);
    }

    // Methods to check call counts
    pub fn get_basic_calls(&self) -> u64 {
        *self.basic_calls.lock().unwrap()
    }
    pub fn get_code_calls(&self) -> u64 {
        *self.code_calls.lock().unwrap()
    }
    pub fn get_storage_calls(&self) -> u64 {
        *self.storage_calls.lock().unwrap()
    }
    pub fn get_block_hash_calls(&self) -> u64 {
        *self.block_hash_calls.lock().unwrap()
    }
}

impl DatabaseCommit for MockDb {
    fn commit(&mut self, changes: EvmState) {
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }
            if account.is_selfdestructed() {
                self.accounts.insert(address, account.info.clone());

                let storage = self.storage.entry(address).or_default();

                storage.clear();

                continue;
            }

            if account.info.code.is_some() {
                self.contracts
                    .insert(account.info.code_hash, account.info.code.clone().unwrap());
            }

            self.accounts.insert(address, account.info.clone());
            match self.storage.get_mut(&address) {
                Some(s) => {
                    s.extend(
                        account
                            .storage
                            .into_iter()
                            .map(|(k, v)| (k, v.present_value())),
                    );
                }
                None => {
                    self.storage.insert(
                        address,
                        account
                            .storage
                            .into_iter()
                            .map(|(k, v)| (k, v.present_value()))
                            .collect(),
                    );
                }
            }
        }
    }
}

impl DatabaseRef for MockDb {
    type Error = NotFoundError; // Simple error type for mock

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        *self.basic_calls.lock().unwrap() += 1;
        Ok(self.accounts.get(&address).cloned())
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        *self.code_calls.lock().unwrap() += 1;
        self.contracts.get(&code_hash).cloned().ok_or(NotFoundError)
    }

    fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        *self.storage_calls.lock().unwrap() += 1;
        Ok(self
            .storage
            .get(&address)
            .and_then(|s| s.get(&slot))
            .copied()
            .unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        *self.block_hash_calls.lock().unwrap() += 1;
        self.block_hashes.get(&number).copied().ok_or(NotFoundError)
    }
}

impl Database for MockDb {
    type Error = NotFoundError; // Simple error type for mock

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        *self.basic_calls.lock().unwrap() += 1;
        Ok(self.accounts.get(&address).cloned())
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        *self.code_calls.lock().unwrap() += 1;
        self.contracts.get(&code_hash).cloned().ok_or(NotFoundError)
    }

    fn storage(&mut self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        *self.storage_calls.lock().unwrap() += 1;
        Ok(self
            .storage
            .get(&address)
            .and_then(|s| s.get(&slot))
            .copied()
            .unwrap_or_default())
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        *self.block_hash_calls.lock().unwrap() += 1;
        self.block_hashes.get(&number).copied().ok_or(NotFoundError)
    }
}

// Helper function to create a simple AccountInfo
#[allow(dead_code)]
pub fn mock_account_info(balance: U256, nonce: u64, code: Option<Bytecode>) -> AccountInfo {
    AccountInfo {
        balance,
        nonce,
        code_hash: code.as_ref().map_or(KECCAK256_EMPTY, Bytecode::hash_slow),
        code,
    }
}
