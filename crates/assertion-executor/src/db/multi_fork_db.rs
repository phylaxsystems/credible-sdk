use crate::{
    constants::DEFAULT_PERSISTENT_ACCOUNTS,
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
    },
    inspectors::CallTracer,
    primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
        JournalEntry,
        U256,
    },
};

use std::collections::HashMap;

use revm::context::JournalInner;

/// A fork of the database. Contains the underlying database and
/// a reference to the [`Journal`].
#[derive(Debug, Clone)]
pub(crate) struct Fork<ExtDb> {
    /// The database.
    pub db: ExtDb,
    /// Journaled state of the fork.
    /// None if the fork is currently active (journal is in the EVM).
    pub journal: Option<JournalInner<JournalEntry>>,
}

/// Represents the various forms of forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForkId {
    PreTx,
    PostTx,
    PreCall(usize),
    PostCall(usize),
}

/// A multi-fork database for managing multiple forks.
#[derive(Debug, Clone)]
pub struct MultiForkDb<ExtDb> {
    /// All forks including active one. The key is the fork id.
    pub(crate) forks: HashMap<ForkId, Fork<ExtDb>>,
    /// Active fork id.
    pub(crate) active_fork_id: ForkId,
    /// Underlying database.
    pub(crate) underlying_db: ExtDb,
    /// Post tx journal.
    pub(crate) post_tx_journal: JournalInner<JournalEntry>,
}
#[derive(Debug, Clone, thiserror::Error)]
pub enum MultiForkError {
    #[error("Post tx journal not found.")]
    PostTxJournalNotFound,
    #[error("Target fork's journal should never be None at this point")]
    TargetForkJournalNotFound,
}

impl<ExtDb: Clone + DatabaseCommit> MultiForkDb<ExtDb> {
    /// Creates a new MultiForkDb. Default to the post-tx state is expected.
    pub fn new(pre_tx_db: ExtDb, post_tx_journal: &JournalInner<JournalEntry>) -> Self {
        let mut post_tx_db = pre_tx_db.clone();
        post_tx_db.commit(post_tx_journal.state.clone());

        let mut forks = HashMap::new();
        forks.insert(
            ForkId::PostTx,
            Fork {
                db: post_tx_db,
                journal: None, // Active fork has no journal (it's in the EVM)
            },
        );

        Self {
            forks,
            active_fork_id: ForkId::PostTx,
            underlying_db: pre_tx_db,
            post_tx_journal: post_tx_journal.clone(),
        }
    }
}

impl<ExtDb> MultiForkDb<ExtDb> {
    #[inline]
    pub(crate) fn active_fork(&self) -> &Fork<ExtDb> {
        &self.forks[&self.active_fork_id]
    }
}

impl<ExtDb: DatabaseRef> MultiForkDb<ExtDb> {
    /// Switch the fork to the requested fork id.
    pub fn switch_fork(
        &mut self,
        fork_id: ForkId,
        active_journal: &mut JournalInner<JournalEntry>,
        call_tracer: &CallTracer,
    ) -> Result<(), MultiForkError>
    where
        ExtDb: Clone + DatabaseCommit,
    {
        // If the fork is already active, do nothing.
        if fork_id == self.active_fork_id {
            return Ok(());
        }

        // Ensure the target fork exists, create if needed
        if !self.forks.contains_key(&fork_id) {
            let new_fork = match fork_id {
                ForkId::PreTx => {
                    self.create_fork(JournalInner {
                        spec: active_journal.spec,
                        ..Default::default()
                    })?
                }
                ForkId::PostTx => return Err(MultiForkError::PostTxJournalNotFound),
                ForkId::PreCall(call_id) => {
                    let mut pre_call_journal = self.post_tx_journal.clone();
                    pre_call_journal.depth += 1;
                    pre_call_journal.checkpoint_revert(call_tracer.pre_call_checkpoints[call_id]);
                    self.create_fork(pre_call_journal)?
                }
                ForkId::PostCall(call_id) => {
                    let mut post_call_journal = self.post_tx_journal.clone();
                    post_call_journal.depth += 1;
                    post_call_journal
                        .checkpoint_revert(call_tracer.post_call_checkpoints[call_id].unwrap());
                    self.create_fork(post_call_journal)?
                }
            };
            self.forks.insert(fork_id, new_fork);
        }

        // Update the target fork's journaled state with persistent accounts
        if let Some(target_fork) = self.forks.get_mut(&fork_id) {
            update_journal(
                DEFAULT_PERSISTENT_ACCOUNTS,
                active_journal,
                &mut target_fork.journal,
            )?;
        }

        // Save the current EVM journal back to the current active fork
        if let Some(current_active_fork) = self.forks.get_mut(&self.active_fork_id) {
            current_active_fork.journal = Some(std::mem::take(active_journal));
        }

        // Take the target fork's journal for the EVM (target fork becomes active with None)
        let target_fork = self.forks.get_mut(&fork_id).unwrap();
        *active_journal = target_fork
            .journal
            .take()
            .ok_or(MultiForkError::TargetForkJournalNotFound)?;

        self.active_fork_id = fork_id;

        Ok(())
    }

    fn create_fork(
        &mut self,
        journal: JournalInner<JournalEntry>,
    ) -> Result<Fork<ExtDb>, MultiForkError>
    where
        ExtDb: Clone + DatabaseCommit,
    {
        let mut fork_db = self.underlying_db.clone();
        fork_db.commit(journal.state.clone());

        let fork = Fork {
            db: fork_db,
            journal: Some(JournalInner {
                spec: journal.spec,
                ..Default::default()
            }),
        };

        Ok(fork)
    }
}

/// Clones the data of the given `accounts` from the `active_journal` into the `target_journal`.
pub(crate) fn update_journal(
    accounts: impl IntoIterator<Item = Address>,
    active_journal: &mut JournalInner<JournalEntry>,
    target_journal: &mut Option<JournalInner<JournalEntry>>,
) -> Result<(), MultiForkError> {
    if let Some(target_journal_inner) = target_journal {
        for addr in accounts.into_iter() {
            merge_journal_data(addr, active_journal, target_journal_inner);
        }

        // since all forks handle their state separately, the depth can drift
        // this is a handover where the target fork starts at the same depth where it was
        // selected. This ensures that there are no gaps in depth which could
        // otherwise cause issues with displaying traces.
        target_journal_inner.depth = active_journal.depth;
    }

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

impl<ExtDb: DatabaseRef + DatabaseCommit> DatabaseCommit for MultiForkDb<ExtDb> {
    fn commit(&mut self, changes: crate::primitives::EvmState) {
        if let Some(active_fork) = self.forks.get_mut(&self.active_fork_id) {
            active_fork.db.commit(changes);
        }
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
        self.active_fork().db.basic_ref(address)
    }
    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <ExtDb as DatabaseRef>::Error> {
        self.active_fork().db.storage_ref(address, slot)
    }
    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, <ExtDb as DatabaseRef>::Error> {
        self.active_fork().db.code_by_hash_ref(hash)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, <ExtDb as DatabaseRef>::Error> {
        self.active_fork().db.block_hash_ref(number)
    }
}

#[cfg(test)]
mod test_multi_fork {
    use super::*;
    use crate::constants::ASSERTION_CONTRACT;
    use revm::{
        database::InMemoryDB,
        interpreter::{
            CallInputs,
            CallValue,
        },
    };

    use crate::{
        primitives::{
            Account,
            AccountStatus,
            Address,
            Bytes,
            uint,
        },
        test_utils::random_bytes,
    };

    fn setup_fork_db(sender: Address) -> InMemoryDB {
        let mut pre_tx_fork_db = InMemoryDB::default();
        pre_tx_fork_db.insert_account_info(
            sender,
            AccountInfo {
                balance: U256::from(1000),
                ..Default::default()
            },
        );
        pre_tx_fork_db
    }

    fn switch_and_verify_fork_state(
        db: &mut MultiForkDb<InMemoryDB>,
        fork_id: ForkId,
        accounts: &[(Address, U256)],
        active_journal: &mut JournalInner<JournalEntry>,
        call_tracer: &CallTracer,
    ) {
        db.switch_fork(fork_id, active_journal, call_tracer)
            .unwrap();
        for (address, expected_balance) in accounts {
            let account_info = db.basic_ref(*address).unwrap().unwrap();
            assert_eq!(
                account_info.balance, *expected_balance,
                "Balance mismatch for address {:?}",
                address
            );
        }
        assert_eq!(db.active_fork_id, fork_id);
    }

    #[test]
    // Test with
    // * call 0 transfers 1000 from caller to address
    // * call 1 transfers 100 from caller to address

    fn test_fork_pre_and_post_call() {
        let sender: Address = random_bytes::<20>().into();
        let receiver: Address = random_bytes::<20>().into();
        let mut pre_tx_fork_db = setup_fork_db(sender.clone());
        let mut active_journal = JournalInner::new();

        let mut call_tracer = CallTracer::new();
        let call_inputs = CallInputs {
            caller: sender,
            target_address: receiver,
            gas_limit: 0,
            bytecode_address: receiver,
            value: CallValue::Transfer(U256::from(900)),
            scheme: revm::interpreter::CallScheme::Call,
            is_static: false,
            is_eof: false,
            input: revm::interpreter::CallInput::Bytes(Bytes::from("")),
            return_memory_offset: 0..0,
        };
        //Call 0
        active_journal.checkpoint();
        call_tracer.record_call_start(call_inputs.clone(), &Bytes::from(""), &mut active_journal);
        active_journal
            .load_account(&mut pre_tx_fork_db, sender)
            .unwrap();
        active_journal
            .transfer(&mut pre_tx_fork_db, sender, receiver, uint!(900_U256))
            .unwrap();
        call_tracer.record_call_end(&mut active_journal);
        active_journal.checkpoint_commit();

        let call_inputs = CallInputs {
            value: CallValue::Transfer(U256::from(100)),
            ..call_inputs
        };

        //Call 1
        active_journal.checkpoint();
        call_tracer.record_call_start(call_inputs, &Bytes::from(""), &mut active_journal);

        active_journal
            .load_account(&mut pre_tx_fork_db, sender)
            .unwrap();
        active_journal
            .transfer(&mut pre_tx_fork_db, sender, receiver, uint!(100_U256))
            .unwrap();

        call_tracer.record_call_end(&mut active_journal);
        active_journal.checkpoint_commit();

        let mut db = MultiForkDb::new(pre_tx_fork_db, &active_journal);

        // Test options are preserved and underlying db is read correctly
        let expected_states = [
            (ForkId::PreCall(0), vec![(sender, uint!(1000_U256))]),
            (
                ForkId::PostCall(0),
                vec![(sender, uint!(100_U256)), (receiver, uint!(900_U256))],
            ),
            (
                ForkId::PreCall(1),
                vec![(sender, uint!(100_U256)), (receiver, uint!(900_U256))],
            ),
            (
                ForkId::PostCall(1),
                vec![(sender, uint!(0_U256)), (receiver, uint!(1000_U256))],
            ),
        ];

        for (fork_id, balances) in expected_states.iter() {
            switch_and_verify_fork_state(
                &mut db,
                *fork_id,
                balances,
                &mut active_journal,
                &call_tracer,
            );
        }
    }

    #[test]
    // Test with pre tx db balance of 1000, and post of 0
    fn test_fork_pre_and_post() {
        let sender: Address = random_bytes::<20>().into();
        let receiver: Address = random_bytes::<20>().into();
        let mut pre_tx_fork_db = setup_fork_db(sender.clone());
        let mut active_journal = JournalInner::new();

        active_journal
            .load_account(&mut pre_tx_fork_db, sender)
            .unwrap();
        active_journal
            .transfer(&mut pre_tx_fork_db, sender, receiver, uint!(1000_U256))
            .unwrap();

        // Create a new MultiForkDb with the post tx db and the active journal
        let mut db = MultiForkDb::new(pre_tx_fork_db, &active_journal);

        //Test options are preserved and underlying db is read correctly

        assert!(!db.forks.contains_key(&ForkId::PreTx));
        assert!(db.forks.contains_key(&ForkId::PostTx)); // PostTx should exist from initialization

        //Assert currently on post
        assert_eq!(db.active_fork_id, ForkId::PostTx);
        switch_and_verify_fork_state(
            &mut db,
            ForkId::PostTx,
            &[(sender, uint!(0_U256)), (receiver, uint!(1000_U256))],
            &mut active_journal,
            &CallTracer::new(),
        );

        switch_and_verify_fork_state(
            &mut db,
            ForkId::PreTx,
            &[(sender, uint!(1000_U256))],
            &mut active_journal,
            &CallTracer::new(),
        );

        // assert that pre tx journal is empty
        assert_eq!(active_journal.state.len(), 0);
        // PreTx should exist after switch
        assert!(db.forks.contains_key(&ForkId::PreTx));
        assert_eq!(db.active_fork_id, ForkId::PreTx);

        db.switch_fork(ForkId::PostTx, &mut active_journal, &CallTracer::new())
            .unwrap();
    }

    #[test]
    fn test_journal_persistence() {
        // Test that journaled state is persisted across forks for

        let address: Address = random_bytes::<20>().into();

        let pre_tx_fork_db = setup_fork_db(address.clone());
        let mut active_journal = JournalInner::new();
        let mut db = MultiForkDb::new(pre_tx_fork_db, &active_journal);
        db.switch_fork(ForkId::PreTx, &mut active_journal, &CallTracer::new())
            .unwrap();

        assert_eq!(active_journal.state.len(), 0);
        // active_journal is now pre tx fork journal
        // insert the assertion contract into the journal
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
        active_journal.state.insert(
            address,
            Account {
                info: AccountInfo {
                    balance: uint!(1337_U256),
                    ..Default::default()
                },
                storage: Default::default(),
                status: AccountStatus::Touched,
            },
        );

        assert_eq!(active_journal.state.len(), 2);
        assert_eq!(
            active_journal
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );

        // switch to post tx fork
        db.switch_fork(ForkId::PostTx, &mut active_journal, &CallTracer::new())
            .unwrap();

        // active_journal is now post tx fork journal
        // assert that the assertion contract is still in the journal but address is not
        assert_eq!(
            active_journal
                .state
                .get(&ASSERTION_CONTRACT)
                .unwrap()
                .info
                .balance,
            uint!(6900_U256)
        );
        assert_eq!(active_journal.state.len(), 1);
    }
}
