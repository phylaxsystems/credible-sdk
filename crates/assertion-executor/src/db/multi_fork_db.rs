use super::fork_db::ForkDb;

use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
    },
    executor::{
        ASSERTION_CONTRACT,
        CALLER,
    },
    inspectors::PRECOMPILE_ADDRESS,
    primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
        EvmState,
        JournaledState,
        U256,
    },
};

use std::{
    collections::HashMap,
    sync::Arc,
};

/// Default persistent accounts.
/// Journaled state of these accounts will be persisted across forks.
const DEFAULT_PERSISTENT_ACCOUNTS: [Address; 3] = [ASSERTION_CONTRACT, CALLER, PRECOMPILE_ADDRESS];

/// A fork of the database. Contains the underlying database and
/// a reference to the [`revm::JournaledState`].
#[derive(Debug, Clone)]
pub(crate) struct Fork<ExtDb> {
    /// The database.
    pub db: Arc<ExtDb>,
    /// Journaled state of the fork.
    /// None if the fork has not been initialized.
    pub journaled_state: Option<JournaledState>,
}

/// Represents the various forms of forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForkId {
    PreTx,
    PostTx,
}

/// A multi-fork database for managing multiple forks.
/// init_journaled_state must be set during initialization of the interpreter.
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
                journaled_state: None,
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
        active_journaled_state: &mut JournaledState,
        init_journaled_state: &JournaledState,
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

        // If the fork does not have a journaled state, initialize it.
        let mut target_fork_journaled_state = target_fork
            .journaled_state
            .take()
            .unwrap_or(init_journaled_state.clone());

        // Update the target forks journaled state with the active fork's journaled state.
        update_journaled_state(
            DEFAULT_PERSISTENT_ACCOUNTS, //TODO: Add a cheatcode to allow for custom persistent accounts
            active_journaled_state,
            &mut target_fork_journaled_state,
        )?;

        // Update the active fork_id.
        let prev_fork_id = std::mem::replace(&mut self.active_fork_id, fork_id);

        // Replace the active db with the target fork db.
        let prev_fork_db = self.active_db.replace_inner_db(target_fork.db);

        // Replace the active journaled state with the target fork journaled state.
        let prev_journaled_state =
            std::mem::replace(active_journaled_state, target_fork_journaled_state);

        self.inactive_forks.insert(
            prev_fork_id,
            Fork {
                db: prev_fork_db,
                journaled_state: Some(prev_journaled_state),
            },
        );

        Ok(())
    }
}
/// Clones the data of the given `accounts` from the `active_journaled_state` into the `target_journaled_state`.
pub(crate) fn update_journaled_state(
    accounts: impl IntoIterator<Item = Address>,
    active_journaled_state: &mut JournaledState,
    target_journaled_state: &mut JournaledState,
) -> Result<(), ForkError> {
    for addr in accounts.into_iter() {
        merge_journaled_state_data(addr, active_journaled_state, target_journaled_state);
    }

    // need to mock empty journal entries in case the current checkpoint is higher than the existing
    // journal entries
    while active_journaled_state.journal.len() > target_journaled_state.journal.len() {
        target_journaled_state.journal.push(Default::default());
    }

    // since all forks handle their state separately, the depth can drift
    // this is a handover where the target fork starts at the same depth where it was
    // selected. This ensures that there are no gaps in depth which could
    // otherwise cause issues with displaying traces.
    target_journaled_state.depth = active_journaled_state.depth;

    Ok(())
}

/// Clones the account data from the `active_journaled_state`  into the `fork_journaled_state`
fn merge_journaled_state_data(
    addr: Address,
    active_journaled_state: &JournaledState,
    fork_journaled_state: &mut JournaledState,
) {
    if let Some(mut acc) = active_journaled_state.state.get(&addr).cloned() {
        if let Some(fork_account) = fork_journaled_state.state.get_mut(&addr) {
            // This will merge the fork's tracked storage with active storage and update values
            fork_account
                .storage
                .extend(std::mem::take(&mut acc.storage));

            // swap them so we can insert the account as whole in the next step
            std::mem::swap(&mut fork_account.storage, &mut acc.storage);
        }
        fork_journaled_state.state.insert(addr, acc);
    }
}

impl<ExtDb: DatabaseRef> Database for MultiForkDb<ExtDb> {
    type Error = ExtDb::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.basic_ref(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.code_by_hash_ref(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.storage_ref(address, index)
    }

    #[inline]
    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_ref(number)
    }
}

impl<ExtDb: DatabaseRef> DatabaseRef for MultiForkDb<ExtDb> {
    type Error = ExtDb::Error;
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.active_db.basic_ref(address)
    }
    fn storage_ref(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        self.active_db.storage_ref(address, slot)
    }
    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, Self::Error> {
        self.active_db.code_by_hash_ref(hash)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
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
    use revm::InMemoryDB;

    use std::collections::HashSet;

    use crate::{
        primitives::{
            Account,
            AccountStatus,
            Address,
            EvmState,
            uint,
        },
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

        let mut journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::from_iter([]));
        let init_journaled_state = journaled_state.clone();

        let pre_tx_fork = db.inactive_forks.get(&ForkId::PreTx).unwrap();

        assert!(pre_tx_fork.journaled_state.is_none());

        assert!(!db.inactive_forks.contains_key(&ForkId::PostTx));
        assert_eq!(db.active_fork_id, ForkId::PostTx);

        assert_eq!(
            db.basic_ref(address).unwrap().unwrap().balance,
            uint!(1000_U256)
        );

        db.switch_fork(ForkId::PreTx, &mut journaled_state, &init_journaled_state)
            .unwrap();

        let post_tx_fork = db.inactive_forks.get(&ForkId::PostTx).unwrap();

        assert!(post_tx_fork.journaled_state.is_some());

        assert!(!db.inactive_forks.contains_key(&ForkId::PreTx));
        assert_eq!(db.active_fork_id, ForkId::PreTx);

        assert_eq!(
            db.basic_ref(address)
                .unwrap_or_default()
                .unwrap_or_default()
                .balance,
            uint!(0_U256)
        );

        db.switch_fork(ForkId::PostTx, &mut journaled_state, &init_journaled_state)
            .unwrap();

        let pre_tx_fork = db.inactive_forks.get(&ForkId::PreTx).unwrap();
        assert!(pre_tx_fork.journaled_state.is_some());

        assert!(!db.inactive_forks.contains_key(&ForkId::PostTx));
        assert_eq!(db.active_fork_id, ForkId::PostTx);

        assert_eq!(
            db.basic_ref(address).unwrap().unwrap().balance,
            uint!(1000_U256)
        );
    }

    #[test]
    fn test_journaled_state_persistence() {
        // Test that journaled state is persisted across forks for
        let init_journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::from_iter([]));
        let mut active_journaled_state = init_journaled_state.clone();

        let address: Address = random_bytes::<20>().into();
        active_journaled_state.state.insert(
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

        active_journaled_state.state.insert(
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

        db.switch_fork(
            ForkId::PreTx,
            &mut active_journaled_state,
            &init_journaled_state,
        )
        .unwrap();

        assert_eq!(
            active_journaled_state
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );
        assert!(!active_journaled_state.state.contains_key(&address));

        let post_tx_journaled_state = db
            .inactive_forks
            .get(&ForkId::PostTx)
            .unwrap()
            .journaled_state
            .clone()
            .unwrap();
        assert_eq!(
            post_tx_journaled_state
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );
        assert_eq!(
            post_tx_journaled_state
                .state
                .get(&address)
                .unwrap()
                .info
                .balance,
            uint!(1000_U256)
        );
    }
}
