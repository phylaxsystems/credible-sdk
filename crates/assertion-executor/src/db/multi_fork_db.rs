use super::fork_db::ForkDb;

use crate::{
    db::{Database, DatabaseCommit, DatabaseRef},
    executor::{ASSERTION_CONTRACT, CALLER},
    inspectors::PRECOMPILE_ADDRESS,
    primitives::{AccountInfo, Address, B256, Bytecode, EvmState, JournalEntry, U256},
};

use std::{collections::HashMap, sync::Arc};

use revm::context::JournalInner;

/// Default persistent accounts.
/// Journaled state of these accounts will be persisted across forks.
const DEFAULT_PERSISTENT_ACCOUNTS: [Address; 3] = [ASSERTION_CONTRACT, CALLER, PRECOMPILE_ADDRESS];

/// A fork of the database. Contains the underlying database and
/// a reference to the [`JournaledState`].
#[derive(Debug, Clone)]
pub(crate) struct Fork<ExtDb> {
    /// The database.
    pub db: Arc<ExtDb>,
    /// Journaled state of the fork.
    /// None if the fork has not been initialized.
    pub journal: Option<JournalInner<JournalEntry>>,
}

/// Represents the various forms of forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForkId {
    PreTx,
    PostTx,
}

/// A multi-fork database for managing multiple forks.
/// init_journal must be set during initialization of the interpreter.
#[derive(Debug, Clone)]
pub struct MultiForkDb<ExtDb> {
    /// Active fork database.
    pub(crate) active_db: ForkDb<ExtDb>,
    /// Active fork id.
    pub(crate) active_fork_id: ForkId,
    /// Inactive forks. The key is the fork id.
    pub(crate) inactive_forks: HashMap<ForkId, Fork<ExtDb>>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ForkError {
    #[error("Fork not found.")]
    ForkNotFound(ForkId),
}

impl<ExtDb: DatabaseRef> MultiForkDb<ExtDb> {
    /// Creates a new MultiForkDb. Defaults to the post-tx state.
    pub fn new(pre_tx_db: ExtDb, post_tx_db: ExtDb) -> Self {
        let forks = HashMap::from_iter([(
            ForkId::PreTx,
            Fork {
                db: Arc::new(pre_tx_db),
                journal: None,
            },
        )]);

        Self {
            active_fork_id: ForkId::PostTx,
            active_db: ForkDb::new(post_tx_db),
            inactive_forks: forks,
        }
    }

    /// Switch the fork to the requested fork id.
    pub fn switch_fork(
        &mut self,
        fork_id: ForkId,
        active_journal: &mut JournalInner<JournalEntry>,
        init_journal: &JournalInner<JournalEntry>,
    ) -> Result<(), ForkError> {
        // If the fork is already active, do nothing.
        if fork_id == self.active_fork_id {
            return Ok(());
        }

        // Remove the target fork from the inactive forks.
        let mut target_fork = self
            .inactive_forks
            .remove(&fork_id)
            .ok_or(ForkError::ForkNotFound(fork_id))?; //Should never happen. Currently we only
        //have two fork modes but in the future we
        //might have more.
        //have two fork modes but in the future we
        //might have more.

        // If the fork does not have a journaled state, initialize it.
        let mut target_fork_journal = target_fork
            .journal
            .take()
            .unwrap_or_else(|| init_journal.clone());

        // Update the target forks journaled state with the active fork's journaled state.
        update_journal(
            DEFAULT_PERSISTENT_ACCOUNTS, //TODO: Add a cheatcode to allow for custom persistent accounts
            active_journal,
            &mut target_fork_journal,
        )?;

        // Update the active fork_id.
        let prev_fork_id = std::mem::replace(&mut self.active_fork_id, fork_id);

        // Replace the active db with the target fork db.
        let prev_fork_db = self.active_db.replace_inner_db(target_fork.db);

        // Replace the active journaled state with the target fork journaled state.
        let prev_journal = std::mem::replace(active_journal, target_fork_journal);

        self.inactive_forks.insert(
            prev_fork_id,
            Fork {
                db: prev_fork_db,
                journal: Some(prev_journal),
            },
        );

        Ok(())
    }
}
/// Clones the data of the given `accounts` from the `active_journal` into the `target_journal`.
pub(crate) fn update_journal(
    accounts: impl IntoIterator<Item = Address>,
    active_journal: &mut JournalInner<JournalEntry>,
    target_journal: &mut JournalInner<JournalEntry>,
) -> Result<(), ForkError> {
    for addr in accounts.into_iter() {
        merge_journal_data(addr, active_journal, target_journal);
    }

    // since all forks handle their state separately, the depth can drift
    // this is a handover where the target fork starts at the same depth where it was
    // selected. This ensures that there are no gaps in depth which could
    // otherwise cause issues with displaying traces.
    target_journal.depth = active_journal.depth;

    Ok(())
}

/// Clones the account data from the `active_journal`  into the `fork_journal`
fn merge_journal_data(
    addr: Address,
    active_journal: &mut JournalInner<JournalEntry>,
    fork_journal: &mut JournalInner<JournalEntry>,
) {
    if let Some(mut acc) = active_journal.state.get(&addr).cloned() {
        if let Some(fork_account) = fork_journal.state.get_mut(&addr) {
            // This will merge the fork's tracked storage with active storage and update values
            fork_account
                .storage
                .extend(std::mem::take(&mut acc.storage));

            // swap them so we can insert the account as whole in the next step
            std::mem::swap(&mut fork_account.storage, &mut acc.storage);
        }
        fork_journal.state.insert(addr, acc);
    }
}

impl<ExtDb: DatabaseRef> Database for MultiForkDb<ExtDb> {
    type Error = <ExtDb as DatabaseRef>::Error;
    fn basic(
        &mut self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <ExtDb as DatabaseRef>::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, <ExtDb as DatabaseRef>::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(
        &mut self,
        address: Address,
        index: U256,
    ) -> Result<U256, <ExtDb as DatabaseRef>::Error> {
        self.storage_ref(address, index)
    }

    #[inline]
    fn block_hash(&mut self, number: u64) -> Result<B256, <ExtDb as DatabaseRef>::Error> {
        self.block_hash_ref(number)
    }
}

impl<ExtDb: DatabaseRef> DatabaseRef for MultiForkDb<ExtDb> {
    type Error = <ExtDb as DatabaseRef>::Error;
    fn basic_ref(
        &self,
        address: Address,
    ) -> Result<Option<AccountInfo>, <ExtDb as DatabaseRef>::Error> {
        self.active_db.basic_ref(address)
    }
    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <ExtDb as DatabaseRef>::Error> {
        self.active_db.storage_ref(address, slot)
    }
    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, <ExtDb as DatabaseRef>::Error> {
        self.active_db.code_by_hash_ref(hash)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, <ExtDb as DatabaseRef>::Error> {
        self.active_db.block_hash_ref(number)
    }
}

impl<ExtDb: DatabaseCommit> DatabaseCommit for MultiForkDb<ExtDb> {
    fn commit(&mut self, changes: EvmState) {
        self.active_db.commit(changes)
    }
}

#[cfg(test)]
mod test_multi_fork {
    use super::*;
    use revm::database::InMemoryDB;

    use crate::{
        primitives::{Account, AccountStatus, Address, EvmState, uint},
        test_utils::random_bytes,
    };

    #[test]
    fn test_fork() {
        let pre_tx_fork_db = InMemoryDB::default();
        let mut post_tx_fork_db = InMemoryDB::default();

        let mut evm_state = EvmState::default();
        let address: Address = random_bytes::<20>().into();

        evm_state.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: uint!(1000_U256),
                    nonce: 0,
                    code_hash: Default::default(),
                    code: None,
                },
                storage: Default::default(),
                status: AccountStatus::Touched,
            },
        );

        post_tx_fork_db.commit(evm_state.clone());

        //Test options are preserved and underlying db is read correctly

        let mut db = MultiForkDb::new(pre_tx_fork_db, post_tx_fork_db);

        let mut journal = JournalInner::new();
        let init_journal = journal.clone();

        let pre_tx_fork = db.inactive_forks.get(&ForkId::PreTx).unwrap();

        assert!(pre_tx_fork.journal.is_none());

        assert!(!db.inactive_forks.contains_key(&ForkId::PostTx));
        assert_eq!(db.active_fork_id, ForkId::PostTx);

        assert_eq!(
            db.basic_ref(address).unwrap().unwrap().balance,
            uint!(1000_U256)
        );

        db.switch_fork(ForkId::PreTx, &mut journal, &init_journal)
            .unwrap();

        let post_tx_fork = db.inactive_forks.get(&ForkId::PostTx).unwrap();

        assert!(post_tx_fork.journal.is_some());

        assert!(!db.inactive_forks.contains_key(&ForkId::PreTx));
        assert_eq!(db.active_fork_id, ForkId::PreTx);

        assert_eq!(
            db.basic_ref(address)
                .unwrap_or_default()
                .unwrap_or_default()
                .balance,
            uint!(0_U256)
        );

        db.switch_fork(ForkId::PostTx, &mut journal, &init_journal)
            .unwrap();

        let pre_tx_fork = db.inactive_forks.get(&ForkId::PreTx).unwrap();
        assert!(pre_tx_fork.journal.is_some());

        assert!(!db.inactive_forks.contains_key(&ForkId::PostTx));
        assert_eq!(db.active_fork_id, ForkId::PostTx);

        assert_eq!(
            db.basic_ref(address).unwrap().unwrap().balance,
            uint!(1000_U256)
        );
    }

    #[test]
    fn test_journal_persistence() {
        // Test that journaled state is persisted across forks for
        let init_journal = JournalInner::new();
        let mut active_journal = init_journal.clone();

        let address: Address = random_bytes::<20>().into();
        active_journal.state.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: uint!(1000_U256),
                    nonce: 0,
                    code_hash: Default::default(),
                    code: None,
                },
                storage: Default::default(),
                status: AccountStatus::Touched,
            },
        );

        active_journal.state.insert(
            ASSERTION_CONTRACT,
            Account {
                info: AccountInfo {
                    balance: uint!(6900_U256),
                    nonce: 0,
                    code_hash: Default::default(),
                    code: None,
                },
                storage: Default::default(),
                status: AccountStatus::Touched,
            },
        );

        let pre_tx_fork_db = InMemoryDB::default();
        let post_tx_fork_db = InMemoryDB::default();

        let mut db = MultiForkDb::new(pre_tx_fork_db, post_tx_fork_db);

        db.switch_fork(ForkId::PreTx, &mut active_journal, &init_journal)
            .unwrap();

        assert_eq!(
            active_journal
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );
        assert!(!active_journal.state.contains_key(&address));

        let post_tx_journal = db
            .inactive_forks
            .get(&ForkId::PostTx)
            .unwrap()
            .journal
            .clone()
            .unwrap();
        assert_eq!(
            post_tx_journal
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );
        assert_eq!(
            post_tx_journal.state.get(&address).unwrap().info.balance,
            uint!(1000_U256)
        );
    }
}
