use crate::{
    db::multi_fork_db::{
        ForkError,
        ForkId,
        MultiForkDb,
    },
    primitives::{
        Bytes,
        JournaledState,
    },
};

use revm::{
    DatabaseRef,
    EvmContext,
    InnerEvmContext,
};

/// Fork to the state before the transaction.
pub fn fork_pre_state(
    init_journaled_state: &JournaledState,
    context: &mut EvmContext<&mut MultiForkDb<impl DatabaseRef>>,
) -> Result<Bytes, ForkError> {
    let InnerEvmContext {
        ref mut db,
        ref mut journaled_state,
        ..
    } = context.inner;

    db.switch_fork(ForkId::PreTx, journaled_state, init_journaled_state)?;
    Ok(Bytes::default())
}

/// Fork to the state after the transaction.
pub fn fork_post_state(
    init_journaled_state: &JournaledState,
    context: &mut EvmContext<&mut MultiForkDb<impl DatabaseRef>>,
) -> Result<Bytes, ForkError> {
    let InnerEvmContext {
        ref mut db,
        ref mut journaled_state,
        ..
    } = context.inner;

    db.switch_fork(ForkId::PostTx, journaled_state, init_journaled_state)?;
    Ok(Bytes::default())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            overlay::test_utils::MockDb,
            MultiForkDb,
        },
        primitives::JournaledState,
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
        primitives::{
            AccountInfo,
            BlockEnv,
            CfgEnv,
            Env,
            SpecId,
            TxEnv,
            KECCAK_EMPTY,
        },
        DatabaseRef,
        EvmContext,
    };
    use std::collections::HashSet;

    fn create_test_context_with_mock_db(
        pre_tx_storage: Vec<(Address, U256, U256)>,
        post_tx_storage: Vec<(Address, U256, U256)>,
    ) -> (MultiForkDb<MockDb>, JournaledState) {
        let mut pre_tx_db = MockDb::new();
        let mut post_tx_db = MockDb::new();

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

        // Set up post-tx state
        for (address, slot, value) in post_tx_storage {
            post_tx_db.insert_storage(address, slot, value);
            post_tx_db.insert_account(
                address,
                AccountInfo {
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                },
            );
        }

        let multi_fork_db = MultiForkDb::new(pre_tx_db, post_tx_db);
        let journaled_state = JournaledState::new(SpecId::LATEST, HashSet::default());

        (multi_fork_db, journaled_state)
    }

    #[test]
    fn test_fork_pre_state_with_mock_db() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(200);

        let (mut multi_fork_db, journaled_state) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        let init_journaled_state = journaled_state.clone();

        // Create EvmContext
        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(),
            tx: TxEnv::default(),
        });

        let mut context = EvmContext::new_with_env(&mut multi_fork_db, env);
        context.inner.journaled_state = journaled_state;

        // Test fork_pre_state function
        let result = fork_pre_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::default());

        // Verify that we're now on the pre-tx fork
        let storage_value = context.db.storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, pre_value);
    }

    #[test]
    fn test_fork_post_state_with_mock_db() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(300);
        let post_value = U256::from(400);

        let (mut multi_fork_db, journaled_state) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        let init_journaled_state = journaled_state.clone();

        // Create EvmContext
        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(),
            tx: TxEnv::default(),
        });

        let mut context = EvmContext::new_with_env(&mut multi_fork_db, env);
        context.inner.journaled_state = journaled_state;

        // Test fork_post_state function
        let result = fork_post_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::default());

        // Verify that we're now on the post-tx fork
        let storage_value = context.db.storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, post_value);
    }

    #[test]
    fn test_fork_switching_between_states() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(500);
        let post_value = U256::from(600);

        let (mut multi_fork_db, journaled_state) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        let init_journaled_state = journaled_state.clone();

        // Create EvmContext
        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(),
            tx: TxEnv::default(),
        });

        let mut context = EvmContext::new_with_env(&mut multi_fork_db, env);
        context.inner.journaled_state = journaled_state;

        // Start with pre-tx state
        let result = fork_pre_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());
        let storage_value = context.db.storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, pre_value);

        // Switch to post-tx state
        let result = fork_post_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());
        let storage_value = context.db.storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, post_value);

        // Switch back to pre-tx state
        let result = fork_pre_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());
        let storage_value = context.db.storage_ref(address, slot).unwrap();
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

        let (mut multi_fork_db, journaled_state) =
            create_test_context_with_mock_db(pre_values.clone(), post_values.clone());

        let init_journaled_state = journaled_state.clone();

        // Create EvmContext
        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(),
            tx: TxEnv::default(),
        });

        let mut context = EvmContext::new_with_env(&mut multi_fork_db, env);
        context.inner.journaled_state = journaled_state;

        // Test pre-tx state
        let result = fork_pre_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());

        let storage_value1 = context.db.storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db.storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(10));
        assert_eq!(storage_value2, U256::from(20));

        // Test post-tx state
        let result = fork_post_state(&init_journaled_state, &mut context);
        assert!(result.is_ok());

        let storage_value1 = context.db.storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db.storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(30));
        assert_eq!(storage_value2, U256::from(40));
    }

    #[tokio::test]
    async fn test_fork_integration() {
        let result = run_precompile_test("TestFork").await;
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
