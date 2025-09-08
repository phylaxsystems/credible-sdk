use crate::{
    db::{
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::{
            ForkId,
            MultiForkDb,
            MultiForkError,
        },
    },
    inspectors::{
        CallTracer,
        sol_primitives::PhEvm::{
            forkPostCallCall,
            forkPreCallCall,
        },
    },
    primitives::{
        Bytes,
        Journal,
    },
};

use alloy_primitives::ruint::FromUintError;
use alloy_sol_types::SolCall;
use revm::context::ContextTr;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ForkError {
    #[error("MultiForkDb error: {0}")]
    MultiForkDbError(#[from] MultiForkError),
    #[error("Error decoding cheatcode input: {0}")]
    DecodeError(#[from] alloy_sol_types::Error),
    #[error("ID exceeds usize")]
    IdExceedsUsize(#[from] FromUintError<usize>),
}

/// Fork to the state before the transaction.
pub fn fork_pre_tx<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PreTx, inner, call_tracer)?;
    Ok(Bytes::default())
}

/// Fork to the state after the transaction.
pub fn fork_post_tx<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PostTx, inner, call_tracer)?;
    Ok(Bytes::default())
}

/// Fork to the state before the call.
pub fn fork_pre_call<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call_id = forkPreCallCall::abi_decode(input_bytes)?.id;

    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PreCall(call_id.try_into()?), inner, call_tracer)?;
    Ok(Bytes::default())
}

/// Fork to the state after the call.
pub fn fork_post_call<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call_id = forkPostCallCall::abi_decode(input_bytes)?.id;

    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PostCall(call_id.try_into()?), inner, call_tracer)?;
    Ok(Bytes::default())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            MultiForkDb,
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        primitives::{
            AccountInfo,
            SpecId,
        },
        test_utils::{
            random_address,
            random_u256,
            run_precompile_test,
        },
    };
    use alloy_primitives::{
        Address,
        U256,
    };
    use revm::{
        JournalEntry,
        context::{
            ContextTr,
            JournalInner,
            JournalTr,
        },
        handler::MainnetContext,
        interpreter::Host,
        primitives::KECCAK_EMPTY,
    };

    fn create_test_context_with_mock_db(
        pre_tx_storage: Vec<(Address, U256, U256)>,
        post_tx_storage: Vec<(Address, U256, U256)>,
    ) -> (MultiForkDb<ForkDb<MockDb>>, JournalInner<JournalEntry>) {
        let mut pre_tx_db = MockDb::new();

        // Set up pre-tx state
        for (address, slot, value) in pre_tx_storage {
            pre_tx_db.insert_storage(address, slot, value);
            pre_tx_db.insert_account(
                address,
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                },
            );
        }

        let mut journal_inner = JournalInner::new();
        // Set up post-tx state
        for (address, slot, value) in post_tx_storage {
            journal_inner.load_account(&mut pre_tx_db, address).unwrap();
            journal_inner
                .sstore(&mut pre_tx_db, address, slot, value)
                .unwrap();
            journal_inner.touch(address); // Mark account as touched so changes get committed
        }

        let multi_fork_db = MultiForkDb::new(ForkDb::new(pre_tx_db), &journal_inner);

        (multi_fork_db, journal_inner)
    }

    #[test]
    fn test_fork_pre_state_with_mock_db() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(200);

        let (mut multi_fork_db, journal) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        // Test fork_pre_state function
        let result = fork_pre_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::default());

        context.journal().load_account(address).unwrap();

        // Verify that we're now on the pre-tx fork
        let storage_value = context.sload(address, slot).unwrap().data;
        assert_eq!(storage_value, pre_value);
    }

    #[test]
    fn test_fork_post_state_with_mock_db() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(300);
        let post_value = U256::from(400);

        let (mut multi_fork_db, test_journal) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Test fork_post_state function
        let result = fork_post_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::default());

        // Verify that we're now on the post-tx fork
        let storage_value = context.journal().sload(address, slot).unwrap().data;
        assert_eq!(storage_value, post_value);
    }

    #[test]
    fn test_fork_switching_between_states() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(500);
        let post_value = U256::from(600);

        let (mut multi_fork_db, test_journal) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Start with pre-tx state
        let result = fork_pre_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());
        let storage_value = context.db().storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, pre_value);

        // Switch to post-tx state
        let result = fork_post_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());
        let storage_value = context.db().storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, post_value);

        // Switch back to pre-tx state
        let result = fork_pre_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());
        let storage_value = context.db().storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, pre_value);
    }

    #[test]
    fn test_fork_with_multiple_accounts_and_storage() {
        let address1 = random_address();
        let address2 = random_address();
        let slot1 = random_u256();
        let slot2 = random_u256();

        let pre_values = vec![
            (address1, slot1, U256::from(10)),
            (address2, slot2, U256::from(20)),
        ];
        let post_values = vec![
            (address1, slot1, U256::from(30)),
            (address2, slot2, U256::from(40)),
        ];

        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(pre_values.clone(), post_values.clone());

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Test pre-tx state
        let result = fork_pre_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());

        let storage_value1 = context.db().storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db().storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(10));
        assert_eq!(storage_value2, U256::from(20));

        // Test post-tx state
        let result = fork_post_tx(&mut context, &CallTracer::new());
        assert!(result.is_ok());

        let storage_value1 = context.db().storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db().storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(30));
        assert_eq!(storage_value2, U256::from(40));
    }

    #[test]
    fn test_tx_fork_integration() {
        let result = run_precompile_test("TestFork");
        println!("result: {result:?}");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(
            result_and_state.result.is_success(),
            "{:#?}",
            result_and_state.result
        );
    }

    #[test]
    fn test_call_fork_integration() {
        let result = run_precompile_test("TestCallFrameForking");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(
            result_and_state.result.is_success(),
            "{:#?}",
            result_and_state.result
        );
    }

    #[test]
    fn test_call_fork_revert_integration() {
        let result = run_precompile_test("TestCallFrameForkingRevert");
        assert!(!result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(
            result_and_state.result.is_success(),
            "{:#?}",
            result_and_state.result
        );
    }
}
