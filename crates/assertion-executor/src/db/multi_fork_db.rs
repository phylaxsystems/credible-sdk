use crate::{
    db::{
        Database,
        DatabaseRef,
    },
    executor::{
        ASSERTION_CONTRACT,
        CALLER,
    },
    inspectors::{
        CallTracer,
        PRECOMPILE_ADDRESS,
    },
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

/// Default persistent accounts.
/// Journaled state of these accounts will be persisted across forks.
const DEFAULT_PERSISTENT_ACCOUNTS: [Address; 3] = [ASSERTION_CONTRACT, CALLER, PRECOMPILE_ADDRESS];

/// Represents the various forms of forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForkId {
    PreTx,
    PostTx,
    PreCall(usize),
    PostCall(usize),
}

/// A multi-fork database for managing multiple forks.
/// init_journal must be set during initialization of the interpreter.
#[derive(Debug, Clone)]
pub struct MultiForkDb<ExtDb> {
    /// Underlying database used for accounts not present in the journaled state.
    pub(crate) underlying_db: ExtDb,
    /// Active fork id.
    pub(crate) active_fork_id: ForkId,
    /// Inactive journals. The key is the fork id. The value is the journal to be applied on top of the database.
    pub(crate) inactive_journals: HashMap<ForkId, JournalInner<JournalEntry>>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum MultiForkError {
    #[error("Post tx journal not found.")]
    PostTxJournalNotFound,
}

impl<ExtDb: DatabaseRef> MultiForkDb<ExtDb> {
    /// Creates a new MultiForkDb. Default to the post-tx state is expected.
    pub fn new(pre_tx_db: ExtDb) -> Self {
        Self {
            active_fork_id: ForkId::PostTx,
            underlying_db: pre_tx_db,
            inactive_journals: HashMap::new(),
        }
    }

    /// Switch the fork to the requested fork id.
    pub fn switch_fork(
        &mut self,
        fork_id: ForkId,
        active_journal: &mut JournalInner<JournalEntry>,
        call_tracer: &CallTracer,
        init_journal: &JournalInner<JournalEntry>,
    ) -> Result<(), MultiForkError> {
        // If the fork is already active, do nothing.
        if fork_id == self.active_fork_id {
            return Ok(());
        }

        // Remove the target fork from the inactive forks.
        let mut target_fork_journal = match self.inactive_journals.remove(&fork_id) {
            Some(target_fork_journal) => target_fork_journal,
            None => {
                match fork_id {
                    ForkId::PreTx => init_journal.clone(), // Initialize to init journal if pre tx
                    ForkId::PostTx => return Err(MultiForkError::PostTxJournalNotFound), //Error post should always be present in inactive_journals if it is not currently the active journal,
                    ForkId::PreCall(call_id) => {
                        let mut pre_call_journal = self.get_post_tx_journal(active_journal)?;

                        //TODO: Here we increment the depth so we are sure it doesn't underflow when we revert once.
                        // Is there any reason we should store the depth with the checkpoint so that we can accurately set the depth
                        // when we revert it?. It seems like it's fine to have an innacurate depth in the journal here.
                        pre_call_journal.depth += 1;
                        pre_call_journal
                            .checkpoint_revert(call_tracer.pre_call_checkpoints[call_id]);

                        pre_call_journal
                    }

                    ForkId::PostCall(call_id) => {
                        let mut post_call_journal = self.get_post_tx_journal(active_journal)?;

                        //TODO: Should we store the depth with the checkpoint?
                        post_call_journal.depth += 1;
                        post_call_journal
                            .checkpoint_revert(call_tracer.post_call_checkpoints[call_id].unwrap());

                        post_call_journal
                    }
                }
            }
        };

        // Update the target forks journaled state with the active fork's journaled state.
        update_journal(
            DEFAULT_PERSISTENT_ACCOUNTS, //TODO: Add a cheatcode to allow for custom persistent accounts
            active_journal,
            &mut target_fork_journal,
        )?;

        // Update the active fork_id.
        let prev_fork_id = std::mem::replace(&mut self.active_fork_id, fork_id);

        // Replace the active journaled state with the target fork journaled state.
        let prev_journal = std::mem::replace(active_journal, target_fork_journal);

        self.inactive_journals.insert(prev_fork_id, prev_journal);

        Ok(())
    }
    fn get_post_tx_journal(
        &self,
        active_journal: &mut JournalInner<JournalEntry>,
    ) -> Result<JournalInner<JournalEntry>, MultiForkError> {
        let post_tx_journal = if self.active_fork_id == ForkId::PostTx {
            active_journal
        } else {
            self.inactive_journals
                .get(&ForkId::PostTx)
                .ok_or(MultiForkError::PostTxJournalNotFound)?
        }
        .clone();
        Ok(post_tx_journal)
    }
}

/// Clones the data of the given `accounts` from the `active_journal` into the `target_journal`.
pub(crate) fn update_journal(
    accounts: impl IntoIterator<Item = Address>,
    active_journal: &mut JournalInner<JournalEntry>,
    target_journal: &mut JournalInner<JournalEntry>,
) -> Result<(), MultiForkError> {
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
        self.underlying_db.basic_ref(address)
    }
    fn storage_ref(
        &self,
        address: Address,
        slot: U256,
    ) -> Result<U256, <ExtDb as DatabaseRef>::Error> {
        // Prevents reading from underlying database for persistent accounts that haven't been written to the storage.
        if DEFAULT_PERSISTENT_ACCOUNTS.contains(&address) {
            return Ok(U256::from(0));
        }
        self.underlying_db.storage_ref(address, slot)
    }
    fn code_by_hash_ref(&self, hash: B256) -> Result<Bytecode, <ExtDb as DatabaseRef>::Error> {
        self.underlying_db.code_by_hash_ref(hash)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, <ExtDb as DatabaseRef>::Error> {
        self.underlying_db.block_hash_ref(number)
    }
}

//impl<ExtDb: DatabaseCommit> DatabaseCommit for MultiForkDb<ExtDb> {
//    fn commit(&mut self, changes: EvmState) {
//        self.underlying_db.commit(changes)
//    }
//}

#[cfg(test)]
mod test_multi_fork {
    use super::*;
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

    #[test]
    // Test with
    // * call 0 transfers 1000 from caller to address
    // * call 1 transfers 100 from caller to address

    fn test_fork_pre_and_post_call() {
        let mut pre_tx_fork_db = InMemoryDB::default();

        let caller: Address = random_bytes::<20>().into();
        let account: Address = random_bytes::<20>().into();

        pre_tx_fork_db.insert_account_info(
            caller,
            AccountInfo {
                balance: U256::from(1100),
                ..Default::default()
            },
        );
        let mut db = MultiForkDb::new(pre_tx_fork_db);
        // Active journal contains post tx changes initially, as it is the default fork.
        let mut active_journal = JournalInner::new();

        let mut call_tracer = CallTracer::new();
        let call_inputs = CallInputs {
            caller,
            target_address: account,
            gas_limit: 0,
            bytecode_address: account,
            value: CallValue::Transfer(U256::from(1000)),
            scheme: revm::interpreter::CallScheme::Call,
            is_static: false,
            is_eof: false,
            input: revm::interpreter::CallInput::Bytes(Bytes::from("")),
            return_memory_offset: 0..0,
        };
        //Call 0
        active_journal.checkpoint();
        call_tracer.record_call_start(call_inputs.clone(), &Bytes::from(""), &mut active_journal);

        active_journal.load_account(&mut db, caller).unwrap();
        active_journal
            .transfer(&mut db, caller, account, uint!(1000_U256))
            .unwrap();

        call_tracer.record_call_end(&mut active_journal);
        active_journal.checkpoint_commit();

        let call_inputs = CallInputs {
            value: CallValue::Transfer(U256::from(100)),
            ..call_inputs
        };

        println!("creating checkpoint");
        //Call 1
        active_journal.checkpoint();
        call_tracer.record_call_start(call_inputs, &Bytes::from(""), &mut active_journal);

        active_journal.load_account(&mut db, caller).unwrap();
        active_journal
            .transfer(&mut db, caller, account, uint!(100_U256))
            .unwrap();

        call_tracer.record_call_end(&mut active_journal);
        active_journal.checkpoint_commit();

        //Test options are preserved and underlying db is read correctly

        let init_journal = JournalInner::new();

        db.switch_fork(
            ForkId::PreCall(0),
            &mut active_journal,
            &call_tracer,
            &init_journal,
        )
        .unwrap();

        //Assert currently on pre call 0
        assert_eq!(db.active_fork_id, ForkId::PreCall(0));
        assert_eq!(
            active_journal.account(caller).info.balance,
            uint!(1100_U256)
        );
        assert_eq!(active_journal.account(account).info.balance, uint!(0_U256));

        db.switch_fork(
            ForkId::PostCall(0),
            &mut active_journal,
            &call_tracer,
            &init_journal,
        )
        .unwrap();

        //Assert currently on post call 0
        assert_eq!(db.active_fork_id, ForkId::PostCall(0));
        assert_eq!(active_journal.account(caller).info.balance, uint!(100_U256));
        assert_eq!(
            active_journal.account(account).info.balance,
            uint!(1000_U256)
        );

        db.switch_fork(
            ForkId::PreCall(1),
            &mut active_journal,
            &call_tracer,
            &init_journal,
        )
        .unwrap();
        assert_eq!(active_journal.account(caller).info.balance, uint!(100_U256));
        assert_eq!(
            active_journal.account(account).info.balance,
            uint!(1000_U256)
        );

        db.switch_fork(
            ForkId::PostCall(1),
            &mut active_journal,
            &call_tracer,
            &init_journal,
        )
        .unwrap();

        assert_eq!(active_journal.account(caller).info.balance, uint!(0_U256));
        assert_eq!(
            active_journal.account(account).info.balance,
            uint!(1100_U256)
        );
    }

    #[test]
    // Test with pre tx db balance of 1000, and post of 0
    fn test_fork_pre_and_post() {
        let mut pre_tx_fork_db = InMemoryDB::default();

        let caller: Address = random_bytes::<20>().into();
        let account: Address = random_bytes::<20>().into();

        pre_tx_fork_db.insert_account_info(
            caller,
            AccountInfo {
                balance: U256::from(1000),
                ..Default::default()
            },
        );
        let mut db = MultiForkDb::new(pre_tx_fork_db);
        // Active journal contains post tx changes initially, as it is the default fork.
        let mut active_journal = JournalInner::new();

        active_journal.load_account(&mut db, caller).unwrap();
        active_journal
            .transfer(&mut db, caller, account, uint!(1000_U256))
            .unwrap();

        //Test options are preserved and underlying db is read correctly

        let init_journal = JournalInner::new();

        assert!(!db.inactive_journals.contains_key(&ForkId::PreTx));
        assert!(!db.inactive_journals.contains_key(&ForkId::PostTx));

        //Assert currently on post
        assert_eq!(db.active_fork_id, ForkId::PostTx);
        assert_eq!(active_journal.account(caller).info.balance, uint!(0_U256));
        assert_eq!(
            active_journal.account(account).info.balance,
            uint!(1000_U256)
        );

        db.switch_fork(
            ForkId::PreTx,
            &mut active_journal,
            &CallTracer::new(),
            &init_journal,
        )
        .unwrap();

        // should have post tx state in inactive
        let post_tx_journal: &JournalInner<JournalEntry> =
            db.inactive_journals.get(&ForkId::PostTx).unwrap();
        assert_eq!(
            post_tx_journal.account(account).info.balance,
            U256::from(1000)
        );
        assert_eq!(post_tx_journal.account(caller).info.balance, U256::from(0));

        //active journal should have pre transfer state
        active_journal.load_account(&mut db, account).unwrap();
        active_journal.load_account(&mut db, caller).unwrap();

        assert_eq!(active_journal.account(account).info.balance, uint!(0_U256));
        assert_eq!(
            active_journal.account(caller).info.balance,
            uint!(1000_U256)
        );
        assert!(!db.inactive_journals.contains_key(&ForkId::PreTx));
        assert_eq!(db.active_fork_id, ForkId::PreTx);

        db.switch_fork(
            ForkId::PostTx,
            &mut active_journal,
            &CallTracer::new(),
            &init_journal,
        )
        .unwrap();

        let pre_tx_fork = db.inactive_journals.get(&ForkId::PreTx).unwrap();
        assert_eq!(pre_tx_fork.account(caller).info.balance, U256::from(1000));
        assert_eq!(pre_tx_fork.account(account).info.balance, U256::from(0));
        assert_eq!(active_journal.account(caller).info.balance, U256::from(0));
        assert_eq!(
            active_journal.account(account).info.balance,
            U256::from(1000)
        );

        assert!(!db.inactive_journals.contains_key(&ForkId::PostTx));
        assert_eq!(db.active_fork_id, ForkId::PostTx);

        let balance = active_journal.account(account).info.balance;
        assert_eq!(balance, uint!(1000_U256));
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

        let mut db = MultiForkDb::new(pre_tx_fork_db);

        db.switch_fork(
            ForkId::PreTx,
            &mut active_journal,
            &CallTracer::new(),
            &init_journal,
        )
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

        let post_tx_journal = db.inactive_journals.get(&ForkId::PostTx).unwrap();
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
