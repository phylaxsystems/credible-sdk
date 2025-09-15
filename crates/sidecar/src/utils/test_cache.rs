use crate::cache::sources::{
    Source,
    SourceError,
};
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
};
use dashmap::DashMap;
use revm::{
    DatabaseRef,
    primitives::{
        HashMap,
        StorageKey,
        StorageValue,
    },
};
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        AtomicU64,
        Ordering,
    },
};

#[derive(Default, Debug)]
pub struct MockDB {
    basic_ref: HashMap<Address, AccountInfo>,
    pub basic_ref_counter: DashMap<Address, AtomicU64>,
    code_by_hash_ref: HashMap<B256, Bytecode>,
    pub code_by_hash_ref_counter: DashMap<B256, AtomicU64>,
    storage_ref: HashMap<(Address, StorageKey), StorageValue>,
    pub storage_ref_counter: DashMap<(Address, StorageKey), AtomicU64>,
    block_hash_ref: HashMap<u64, B256>,
    pub block_hash_ref_counter: DashMap<u64, AtomicU64>,
}

impl DatabaseRef for MockDB {
    type Error = SourceError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref_counter
            .entry(address)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        Ok(self.basic_ref.get(&address).cloned())
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.code_by_hash_ref_counter
            .entry(code_hash)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        self.code_by_hash_ref
            .get(&code_hash)
            .ok_or(Self::Error::CodeByHashNotFound)
            .cloned()
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        let key = (address, index);
        self.storage_ref_counter
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        self.storage_ref
            .get(&key)
            .ok_or(Self::Error::StorageNotFound)
            .cloned()
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref_counter
            .entry(number)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        self.block_hash_ref
            .get(&number)
            .ok_or(Self::Error::BlockNotFound)
            .cloned()
    }
}

#[derive(Debug)]
pub struct MockSequencerDB {
    pub mock_db: MockDB,
    pub is_synced: AtomicBool,
}

impl MockSequencerDB {
    pub fn build() -> Arc<Self> {
        Arc::new(Self {
            mock_db: MockDB::default(),
            is_synced: AtomicBool::new(true),
        })
    }
}

impl Source for MockSequencerDB {
    fn is_synced(&self, _current_block_number: u64) -> bool {
        self.is_synced.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "mock_sequencer_db"
    }
}

impl DatabaseRef for MockSequencerDB {
    type Error = SourceError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.mock_db.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.mock_db.code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.mock_db.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.mock_db.block_hash_ref(number)
    }
}

#[derive(Debug)]
pub struct MockBesuClientDB {
    pub mock_db: MockDB,
    pub is_synced: AtomicBool,
}

impl MockBesuClientDB {
    pub fn build() -> Arc<Self> {
        Arc::new(Self {
            mock_db: MockDB::default(),
            is_synced: AtomicBool::new(true),
        })
    }
}

impl DatabaseRef for MockBesuClientDB {
    type Error = SourceError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.mock_db.basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.mock_db.code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.mock_db.storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.mock_db.block_hash_ref(number)
    }
}

impl Source for MockBesuClientDB {
    fn is_synced(&self, _current_block_number: u64) -> bool {
        self.is_synced.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "mock_besu_client_db"
    }
}
