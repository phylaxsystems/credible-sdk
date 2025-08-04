use crate::{
    db::{
        DatabaseRef,
        multi_fork_db::{
            ForkError,
            ForkId,
            MultiForkDb,
        },
    },
    inspectors::CallTracer,
    primitives::{
        Bytes,
        Journal,
        JournalEntry,
        JournalInner,
    },
};

use revm::context::ContextTr;

/// Fork to the state before the transaction.
pub fn fork_pre_state<'db, ExtDb: DatabaseRef + 'db, CTX>(
    init_journal: &JournalInner<JournalEntry>,
    context: &mut CTX,
    call_tracer: &CallTracer,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PreTx, inner, call_tracer, init_journal)?;
    Ok(Bytes::default())
}

/// Fork to the state after the transaction.
pub fn fork_post_state<'db, ExtDb: DatabaseRef + 'db, CTX>(
    init_journal: &JournalInner<JournalEntry>,
    context: &mut CTX,
    call_tracer: &CallTracer,
) -> Result<Bytes, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let Journal { database, inner } = context.journal();
    database.switch_fork(ForkId::PostTx, inner, call_tracer, init_journal)?;
    Ok(Bytes::default())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::primitives::{
        AccountInfo,
        SpecId,
    };
    use crate::{
        db::{
            MultiForkDb,
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
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
        context::{
            ContextTr,
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

        let mut journaled_inner = JournalInner::new();
        // Set up post-tx state
        for (address, slot, value) in post_tx_storage {
            let mut state_load = journaled_inner
                .load_account(&mut pre_tx_db, address)
                .unwrap();
            if state_load.is_cold {
                state_load.mark_warm();
            }
            journaled_inner
                .sstore(&mut pre_tx_db, address, slot, value)
                .unwrap();
        }

        let multi_fork_db = MultiForkDb::new(ForkDb::new(pre_tx_db));

        (multi_fork_db, journaled_inner)
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

        let init_journal = JournalInner::new();

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        // Test fork_pre_state function
        let result = fork_pre_state(&init_journal, &mut context, &CallTracer::new());
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

        let init_journal = test_journal.clone();

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Test fork_post_state function
        let result = fork_post_state(&init_journal, &mut context, &CallTracer::new());
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

        let init_journal = JournalInner::new();

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Start with pre-tx state
        let result = fork_pre_state(&init_journal, &mut context, &CallTracer::new());
        assert!(result.is_ok());
        context.journal().load_account(address).unwrap();
        let storage_value = context.sload(address, slot).unwrap().data;
        assert_eq!(storage_value, pre_value);

        // Switch to post-tx state
        let result = fork_post_state(&init_journal, &mut context, &CallTracer::new());
        assert!(result.is_ok());
        let storage_value = context.sload(address, slot).unwrap().data;
        assert_eq!(storage_value, post_value);

        // Switch back to pre-tx state
        let result = fork_pre_state(&init_journal, &mut context, &CallTracer::new());
        assert!(result.is_ok());
        let storage_value = context.sload(address, slot).unwrap().data;
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

        let init_journal = JournalInner::new();

        // Create EvmContext
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Test pre-tx state
        let result = fork_pre_state(&init_journal, &mut context, &CallTracer::new());
        assert!(result.is_ok());

        context.journal().load_account(address1).unwrap();
        context.journal().load_account(address2).unwrap();

        let storage_value1 = context.sload(address1, slot1).unwrap().data;
        let storage_value2 = context.sload(address2, slot2).unwrap().data;
        assert_eq!(storage_value1, U256::from(10));
        assert_eq!(storage_value2, U256::from(20));

        // Test post-tx state
        let result = fork_post_state(&init_journal, &mut context, &CallTracer::new());
        assert!(result.is_ok());

        let storage_value1 = context.sload(address1, slot1).unwrap().data;
        let storage_value2 = context.sload(address2, slot2).unwrap().data;
        assert_eq!(storage_value1, U256::from(30));
        assert_eq!(storage_value2, U256::from(40));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fork_inegration() {
        let result = run_precompile_test("TestFork").await;
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
