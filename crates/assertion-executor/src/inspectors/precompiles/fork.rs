use crate::inspectors::precompiles::deduct_gas_and_check;
use crate::inspectors::precompiles::BASE_COST;
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
        CallTracer, phevm::PhevmOutcome, sol_primitives::PhEvm::{
            forkPostCallCall,
            forkPreCallCall,
        }
    },
    primitives::{
        Bytes,
        Journal,
    },
};

use alloy_primitives::{
    U256,
    ruint::FromUintError,
};
use alloy_sol_types::SolCall;
use revm::context::ContextTr;

const PERSISTENT_WRITE: u64 = 20;
const SET_FORK: u64 = 20;
const MEM_WRITE_COST: u64 = 3;
const EVM_WORD_BYTES: u64 = 32;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ForkError {
    #[error("MultiForkDb error: Fork pre call operation")]
    MultiForkPreCallDbError(#[source] MultiForkError),
    #[error("MultiForkDb error: Fork pre tx operation")]
    MultiForkPreTxDbError(#[source] MultiForkError),
    #[error("MultiForkDb error: Fork post call operation")]
    MultiForkPostCallDbError(#[source] MultiForkError),
    #[error("MultiForkDb error: Fork post tx operation")]
    MultiForkPostTxDbError(#[source] MultiForkError),
    #[error("Error decoding cheatcode input: {0}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("ID exceeds usize")]
    IdExceedsUsize(#[source] FromUintError<usize>),
    #[error("Cannot fork to call {call_id}: call is inside a reverted subtree")]
    CallInsideRevertedSubtree { call_id: usize },
    #[error("Call ID {call_id} is too large to be a valid index")]
    CallIdOverflow { call_id: U256 },
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

/// Fork to the state before the transaction.
pub fn fork_pre_tx<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    gas: u64,
) -> Result<PhevmOutcome, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    let journal = context.journal_mut();

    // No-op fork can be amortized in the base cost.
    if journal.database.is_active_fork(ForkId::PreTx) {
        return Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left));
    }

    // If the fork does not exist, price memory for creating it.
    if !journal.database.fork_exists(&ForkId::PreTx) {
        let bytes_written = journal
            .database
            .estimated_create_fork_bytes(ForkId::PreTx, &journal.inner, call_tracer)
            .map_err(ForkError::MultiForkPreTxDbError)?;
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        if let Some(rax) =
            deduct_gas_and_check(&mut gas_left, words_written * MEM_WRITE_COST, gas_limit)
        {
            return Err(ForkError::OutOfGas(rax));
        }
    }

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, PERSISTENT_WRITE + SET_FORK, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    journal
        .database
        .switch_fork(ForkId::PreTx, &mut journal.inner, call_tracer)
        .map_err(ForkError::MultiForkPreTxDbError)?;

    Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left))
}

/// Fork to the state after the transaction.
pub fn fork_post_tx<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    gas: u64
) -> Result<PhevmOutcome, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    let journal = context.journal_mut();

    // No-op fork can be amortized in the base cost.
    if journal.database.is_active_fork(ForkId::PostTx) {
        return Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left));
    }

    // If the fork does not exist, price memory for creating it.
    if !journal.database.fork_exists(&ForkId::PostTx) {
        let bytes_written = journal
            .database
            .estimated_create_fork_bytes(ForkId::PostTx, &journal.inner, call_tracer)
            .map_err(ForkError::MultiForkPostTxDbError)?;
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        if let Some(rax) =
            deduct_gas_and_check(&mut gas_left, words_written * MEM_WRITE_COST, gas_limit)
        {
            return Err(ForkError::OutOfGas(rax));
        }
    }

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, PERSISTENT_WRITE + SET_FORK, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    journal
        .database
        .switch_fork(ForkId::PostTx, &mut journal.inner, call_tracer)
        .map_err(ForkError::MultiForkPostTxDbError)?;

    Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left))
}

/// Fork to the state before the call.
pub fn fork_pre_call<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
    gas: u64
) -> Result<PhevmOutcome, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    let call_id = forkPreCallCall::abi_decode(input_bytes)
        .map_err(ForkError::DecodeError)?
        .id;

    let call_id_usize: usize = call_id
        .try_into()
        .map_err(|_| ForkError::CallIdOverflow { call_id })?;

    if !call_tracer.is_call_forkable(call_id_usize) {
        return Err(ForkError::CallInsideRevertedSubtree {
            call_id: call_id_usize,
        });
    }

    let journal = context.journal_mut();

    let fork_id = ForkId::PreCall(call_id.try_into().map_err(ForkError::IdExceedsUsize)?);

    // No-op fork can be amortized in the base cost.
    if journal.database.is_active_fork(fork_id) {
        return Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left));
    }
    
    // if the fork does not exist, price memory for creating it
    if !journal.database.fork_exists(&fork_id) {
        let bytes_written = journal
            .database
            .estimated_create_fork_bytes(fork_id, &journal.inner, call_tracer)
            .map_err(ForkError::MultiForkPreCallDbError)?;
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        if let Some(rax) =
            deduct_gas_and_check(&mut gas_left, words_written * MEM_WRITE_COST, gas_limit)
        {
            return Err(ForkError::OutOfGas(rax));
        }
    }

    // deduct gas for switching forks
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, PERSISTENT_WRITE + SET_FORK, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    journal
        .database
        .switch_fork(
            fork_id,
            &mut journal.inner,
            call_tracer,
        )
        .map_err(ForkError::MultiForkPreCallDbError)?;
    Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left))
}

/// Fork to the state after the call.
pub fn fork_post_call<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, ForkError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    let call_id = forkPostCallCall::abi_decode(input_bytes)
        .map_err(ForkError::DecodeError)?
        .id;

    let call_id_usize: usize = call_id
        .try_into()
        .map_err(|_| ForkError::CallIdOverflow { call_id })?;

    if !call_tracer.is_call_forkable(call_id_usize) {
        return Err(ForkError::CallInsideRevertedSubtree {
            call_id: call_id_usize,
        });
    }

    let journal = context.journal_mut();

    let fork_id = ForkId::PostCall(call_id.try_into().map_err(ForkError::IdExceedsUsize)?);

    // No-op fork can be amortized in the base cost.
    if journal.database.is_active_fork(fork_id) {
        return Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left));
    }

    // if the fork does not exist, price memory for creating it
    if !journal.database.fork_exists(&fork_id) {
        let bytes_written = journal
            .database
            .estimated_create_fork_bytes(fork_id, &journal.inner, call_tracer)
            .map_err(ForkError::MultiForkPostCallDbError)?;
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        if let Some(rax) =
            deduct_gas_and_check(&mut gas_left, words_written * MEM_WRITE_COST, gas_limit)
        {
            return Err(ForkError::OutOfGas(rax));
        }
    }

    // deduct gas for switching forks
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, PERSISTENT_WRITE + SET_FORK, gas_limit) {
        return Err(ForkError::OutOfGas(rax));
    }

    journal
        .database
        .switch_fork(
            fork_id,
            &mut journal.inner,
            call_tracer,
        )
        .map_err(ForkError::MultiForkPostCallDbError)?;
    Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left))
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
            Bytes,
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
            journaled_state::JournalCheckpoint,
        },
        handler::MainnetContext,
        interpreter::Host,
        primitives::KECCAK_EMPTY,
    };

    const TEST_GAS: u64 = 1_000_000;

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
                .sstore(&mut pre_tx_db, address, slot, value, false)
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
        let outcome = fork_pre_tx(&mut context, &CallTracer::default(), TEST_GAS).unwrap();
        assert_eq!(outcome.bytes(), &Bytes::default());

        context.journal_mut().load_account(address).unwrap();

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
        let outcome = fork_post_tx(&mut context, &CallTracer::default(), TEST_GAS).unwrap();
        assert_eq!(outcome.bytes(), &Bytes::default());
        assert_eq!(outcome.gas(), BASE_COST);

        // Verify that we're now on the post-tx fork
        let storage_value = context.journal_mut().sload(address, slot).unwrap().data;
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
        let result = fork_pre_tx(&mut context, &CallTracer::default(), TEST_GAS);
        assert!(result.is_ok(), "{result:?}");
        let storage_value = context.db().storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, pre_value);

        // Switch to post-tx state
        let result = fork_post_tx(&mut context, &CallTracer::default(), TEST_GAS);
        assert!(result.is_ok(), "{result:?}");
        let storage_value = context.db().storage_ref(address, slot).unwrap();
        assert_eq!(storage_value, post_value);

        // Switch back to pre-tx state
        let result = fork_pre_tx(&mut context, &CallTracer::default(), TEST_GAS);
        assert!(result.is_ok(), "{result:?}");
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
        let result = fork_pre_tx(&mut context, &CallTracer::default(), TEST_GAS);
        assert!(result.is_ok(), "{result:?}");

        let storage_value1 = context.db().storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db().storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(10));
        assert_eq!(storage_value2, U256::from(20));

        // Test post-tx state
        let result = fork_post_tx(&mut context, &CallTracer::default(), TEST_GAS);
        assert!(result.is_ok(), "{result:?}");

        let storage_value1 = context.db().storage_ref(address1, slot1).unwrap();
        let storage_value2 = context.db().storage_ref(address2, slot2).unwrap();
        assert_eq!(storage_value1, U256::from(30));
        assert_eq!(storage_value2, U256::from(40));
    }

    #[tokio::test]
    async fn test_tx_fork_integration() {
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

    #[tokio::test]
    async fn test_call_fork_integration() {
        let result = run_precompile_test("TestCallFrameForking");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(
            result_and_state.result.is_success(),
            "{:#?}",
            result_and_state.result
        );
    }

    #[tokio::test]
    async fn test_call_fork_revert_integration() {
        let result = run_precompile_test("TestCallFrameForkingRevert");
        assert!(!result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(
            result_and_state.result.is_success(),
            "{:#?}",
            result_and_state.result
        );
    }

    #[test]
    fn test_fork_post_call_gas_pricing_missing_existing_and_noop() {
        use crate::inspectors::sol_primitives::PhEvm::forkPostCallCall;

        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(123);

        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        // Prepare a tracer with a valid call id=0 and a post-call checkpoint that keeps the full
        // post-tx journal (checkpoint at the end).
        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(address);
        call_tracer.pre_call_checkpoints = vec![JournalCheckpoint {
            log_i: 0,
            journal_i: 0,
        }];
        call_tracer.post_call_checkpoints = vec![Some(JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        })];

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let calldata: Bytes = forkPostCallCall { id: U256::ZERO }.abi_encode().into();

        let expected_bytes_create = {
            let journal = context.journal_mut();
            journal
                .database
                .estimated_create_fork_bytes(ForkId::PostCall(0), &journal.inner, &call_tracer)
                .unwrap()
        };
        let expected_words_create = expected_bytes_create.div_ceil(EVM_WORD_BYTES);
        let expected_gas_create =
            BASE_COST + PERSISTENT_WRITE + SET_FORK + expected_words_create * MEM_WRITE_COST;

        let outcome = fork_post_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), expected_gas_create);

        // No-op on the already-active fork should be base-only.
        let outcome = fork_post_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST);

        // Switching away and back to an existing fork should not include create_fork memory cost.
        let outcome = fork_post_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);

        let outcome = fork_post_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);
    }

    #[test]
    fn test_fork_pre_call_gas_pricing_missing_existing_and_noop() {
        use crate::inspectors::sol_primitives::PhEvm::forkPreCallCall;

        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(456);

        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(address);
        call_tracer.pre_call_checkpoints = vec![JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        }];
        call_tracer.post_call_checkpoints = vec![Some(JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        })];

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let calldata: Bytes = forkPreCallCall { id: U256::ZERO }.abi_encode().into();

        let expected_bytes_create = {
            let journal = context.journal_mut();
            journal
                .database
                .estimated_create_fork_bytes(ForkId::PreCall(0), &journal.inner, &call_tracer)
                .unwrap()
        };
        let expected_words_create = expected_bytes_create.div_ceil(EVM_WORD_BYTES);
        let expected_gas_create =
            BASE_COST + PERSISTENT_WRITE + SET_FORK + expected_words_create * MEM_WRITE_COST;

        let outcome = fork_pre_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), expected_gas_create);

        let outcome = fork_pre_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST);

        let outcome = fork_post_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);

        let outcome = fork_pre_call(&mut context, &call_tracer, &calldata, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);
    }

    #[test]
    fn test_fork_tx_gas_pricing_noop_and_switch() {
        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(999);

        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        let call_tracer = CallTracer::default();
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Starts on PostTx (active): no-op.
        let outcome = fork_post_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST);

        // Switch to PreTx.
        let outcome = fork_pre_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);

        // No-op on PreTx.
        let outcome = fork_pre_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST);

        // Switch back to PostTx.
        let outcome = fork_post_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST + PERSISTENT_WRITE + SET_FORK);

        // No-op on PostTx.
        let outcome = fork_post_tx(&mut context, &call_tracer, TEST_GAS).unwrap();
        assert_eq!(outcome.gas(), BASE_COST);
    }

    fn assert_oog(
        result: Result<PhevmOutcome, ForkError>,
        gas_limit: u64,
        context: &str,
    ) {
        match result {
            Err(ForkError::OutOfGas(outcome)) => assert_eq!(
                outcome.gas(),
                gas_limit,
                "expected all gas consumed for OOG ({context})"
            ),
            other => panic!("expected OutOfGas for {context}, got {other:?}"),
        }
    }

    #[test]
    fn test_fork_gas_oog_at_base_cost() {
        use crate::inspectors::sol_primitives::PhEvm::{
            forkPostCallCall,
            forkPreCallCall,
        };

        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(123);
        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(address);
        call_tracer.pre_call_checkpoints = vec![JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        }];
        call_tracer.post_call_checkpoints = vec![Some(JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        })];

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let gas_limit = BASE_COST - 1;
        assert_oog(fork_pre_tx(&mut context, &call_tracer, gas_limit), gas_limit, "pre_tx base");

        // Reset to a known state (PostTx is active by default, but pre_tx above may have mutated).
        let gas_limit = BASE_COST - 1;
        assert_oog(
            fork_post_tx(&mut context, &call_tracer, gas_limit),
            gas_limit,
            "post_tx base",
        );

        let calldata_pre: Bytes = forkPreCallCall { id: U256::ZERO }.abi_encode().into();
        let gas_limit = BASE_COST - 1;
        assert_oog(
            fork_pre_call(&mut context, &call_tracer, &calldata_pre, gas_limit),
            gas_limit,
            "pre_call base",
        );

        let calldata_post: Bytes = forkPostCallCall { id: U256::ZERO }.abi_encode().into();
        let gas_limit = BASE_COST - 1;
        assert_oog(
            fork_post_call(&mut context, &call_tracer, &calldata_post, gas_limit),
            gas_limit,
            "post_call base",
        );
    }

    #[test]
    fn test_fork_gas_oog_at_switch_cost() {
        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(123);
        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        let call_tracer = CallTracer::default();
        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        // Enough for BASE_COST, but not enough for PERSISTENT_WRITE + SET_FORK.
        let gas_limit = BASE_COST + PERSISTENT_WRITE + SET_FORK - 1;
        assert_oog(
            fork_pre_tx(&mut context, &call_tracer, gas_limit),
            gas_limit,
            "pre_tx switch",
        );
    }

    #[test]
    fn test_fork_gas_oog_at_create_fork_memory_cost() {
        use crate::inspectors::sol_primitives::PhEvm::forkPreCallCall;

        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(123);
        let (mut multi_fork_db, test_journal) =
            create_test_context_with_mock_db(vec![], vec![(address, slot, post_value)]);

        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(address);
        call_tracer.pre_call_checkpoints = vec![JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        }];
        call_tracer.post_call_checkpoints = vec![Some(JournalCheckpoint {
            log_i: test_journal.logs.len(),
            journal_i: test_journal.journal.len(),
        })];

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let bytes_written = {
            let journal = context.journal_mut();
            journal
                .database
                .estimated_create_fork_bytes(ForkId::PreCall(0), &journal.inner, &call_tracer)
                .unwrap()
        };
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        assert!(words_written > 0, "expected non-zero create_fork write");
        let mem_cost = words_written * MEM_WRITE_COST;

        // Enough for base, but not enough for create_fork memory write pricing.
        let gas_limit = BASE_COST + mem_cost - 1;
        let calldata: Bytes = forkPreCallCall { id: U256::ZERO }.abi_encode().into();
        assert_oog(
            fork_pre_call(&mut context, &call_tracer, &calldata, gas_limit),
            gas_limit,
            "pre_call create_fork mem",
        );
    }
}
