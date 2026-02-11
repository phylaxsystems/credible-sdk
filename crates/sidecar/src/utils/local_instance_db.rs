use crate::engine::system_calls::{
    SpecIdExt,
    SystemCallError,
    SystemCalls,
    SystemCallsConfig,
};
use parking_lot::RwLock;
use revm::{
    DatabaseCommit,
    DatabaseRef,
    primitives::{
        Address,
        B256,
        U256,
    },
    state::{
        AccountInfo,
        Bytecode,
    },
};

/// Thread-safe wrapper around a REVМ database that supports interior-mutability writes.
///
/// This is primarily used in tests to keep an in-memory canonical state store updated
/// while the engine cache overlay is invalidated/rebuilt (matching production behavior,
/// where canonical state lives in an external source).
#[derive(Debug)]
pub struct LocalInstanceDb<Db> {
    inner: RwLock<Db>,
}

impl<Db> LocalInstanceDb<Db> {
    pub fn new(inner: Db) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn with_read<R>(&self, f: impl FnOnce(&Db) -> R) -> R {
        let guard = self.inner.read();
        f(&*guard)
    }

    pub fn with_write<R>(&self, f: impl FnOnce(&mut Db) -> R) -> R {
        let mut guard = self.inner.write();
        f(&mut *guard)
    }
}

impl<Db: DatabaseRef> DatabaseRef for LocalInstanceDb<Db> {
    type Error = <Db as DatabaseRef>::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.with_read(|db| db.basic_ref(address))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.with_read(|db| db.code_by_hash_ref(code_hash))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.with_read(|db| db.storage_ref(address, index))
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.with_read(|db| db.block_hash_ref(number))
    }
}

impl<Db> LocalInstanceDb<Db>
where
    Db: DatabaseRef + DatabaseCommit,
{
    pub fn apply_system_calls_canonical(
        &self,
        system_calls: &SystemCalls,
        config: &SystemCallsConfig,
    ) -> Result<(), SystemCallError> {
        self.with_write(|db| {
            if config.spec_id.is_cancun_active() {
                system_calls.apply_eip4788(config, db)?;
            }
            if config.spec_id.is_prague_active() {
                system_calls.apply_eip2935(config, db)?;
            }
            Ok(())
        })
    }
}
