pub mod config;

use std::{
    fmt::Debug,
    sync::atomic::AtomicU64,
};

use crate::{
    ExecutorConfig,
    constants::{
        ASSERTION_CONTRACT,
        CALLER,
        PRECOMPILE_ADDRESS,
    },
    db::{
        DatabaseCommit,
        DatabaseRef,
        fork_db::ForkDb,
        multi_fork_db::MultiForkDb,
    },
    error::{
        AssertionExecutionError,
        ExecutorError,
        ForkTxExecutionError,
    },
    evm::build_evm::evm_env,
    inspectors::{
        CallTracer,
        LogsAndTraces,
        PhEvmContext,
        PhEvmInspector,
    },
    primitives::{
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        AssertionContract,
        AssertionContractExecution,
        AssertionFnId,
        AssertionFunctionExecutionResult,
        AssertionFunctionResult,
        BlockEnv,
        Bytecode,
        EvmState,
        EvmStorage,
        FixedBytes,
        ResultAndState,
        TxEnv,
        TxKind,
        TxValidationResult,
        U256,
        bytes,
    },
    reprice_evm_storage,
    store::AssertionStore,
};

use revm::{
    Database,
    InspectEvm,
};

use rayon::prelude::{
    IntoParallelIterator,
    ParallelIterator,
};

use tracing::{
    debug,
    instrument,
    trace,
    warn,
};

#[derive(Debug, Clone)]
pub struct AssertionExecutor {
    pub config: ExecutorConfig,
    pub store: AssertionStore,
}

impl AssertionExecutor {
    /// Creates a new assertion executor.
    pub fn new(config: ExecutorConfig, store: AssertionStore) -> Self {
        Self { config, store }
    }
}

/// Used for tracing outputs about state changes
#[derive(Debug)]
struct StateChangeMetadata<'a> {
    #[allow(dead_code)]
    address: &'a Address,
    #[allow(dead_code)]
    balance: &'a U256,
    #[allow(dead_code)]
    has_code: bool,
    #[allow(dead_code)]
    storage: &'a EvmStorage,
}

#[derive(Debug)]
struct AssertionExecutionParams<'a, Active> {
    assertion_contract: &'a AssertionContract,
    fn_selector: &'a FixedBytes<4>,
    block_env: BlockEnv,
    multi_fork_db: MultiForkDb<ForkDb<Active>>,
    assertion_gas: &'a AtomicU64,
    assertions_ran: &'a AtomicU64,
    inspector: PhEvmInspector<'a>,
}

#[derive(Debug, Clone)]
pub struct ExecuteForkedTxResult {
    pub call_tracer: CallTracer,
    pub result_and_state: ResultAndState,
}

impl AssertionExecutor {
    /// Executes a transaction against an external revm database, and runs the appropriate
    /// assertions.
    ///
    /// We execute against an external database here to satisfy a requirement within op-talos, where
    /// transactions couldnt be properly commited if they weren't touched by the database beforehand.
    ///
    /// Returns the results of the assertions, as well as the state changes that should be
    /// committed if the assertions pass. Errors if the tx execution encounters an internal error.
    #[instrument(level = "debug", skip_all, target = "executor::validate_tx")]
    pub fn validate_transaction_ext_db<ExtDb, Active>(
        &mut self,
        block_env: BlockEnv,
        tx_env: TxEnv,
        fork_db: &mut ForkDb<Active>,
        external_db: &mut ExtDb,
    ) -> Result<TxValidationResult, ExecutorError<Active, ExtDb>>
    where
        ExtDb: Database + Sync + Send,
        ExtDb::Error: Send,
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
    {
        let tx_fork_db = fork_db.clone();

        // This call relies on From<EVMError<ExtDb::Error>> for ExecutorError<DB::Error>
        let forked_tx_result =
            self.execute_forked_tx_ext_db::<ExtDb>(block_env.clone(), tx_env, external_db)?;

        let exec_result = &forked_tx_result.result_and_state.result;
        if !exec_result.is_success() {
            debug!(target: "assertion-executor::validate_tx", "Transaction execution failed, skipping assertions");
            return Ok(TxValidationResult::new(
                true,
                forked_tx_result.result_and_state,
                vec![],
            ));
        }
        debug!(target: "assertion-executor::validate_tx", gas_used=exec_result.gas_used(), "Transaction execution succeeded.");

        let results = self.execute_assertions(block_env, tx_fork_db, &forked_tx_result)?;

        if results.is_empty() {
            debug!(target: "assertion-executor::validate_tx", "No assertions were executed");
            trace!(target: "assertion-executor::validate_tx", "Comitting state changes to fork db");
            fork_db.commit(forked_tx_result.result_and_state.state.clone());
            return Ok(TxValidationResult::new(
                true,
                forked_tx_result.result_and_state,
                vec![],
            ));
        }

        let invalid_assertions: Vec<AssertionFnId> = results
            .iter()
            .filter(|a| !a.assertion_fns_results.iter().all(|r| r.is_success()))
            .flat_map(|a| a.assertion_fns_results.iter().map(|r| r.id))
            .collect::<Vec<_>>();

        let valid = invalid_assertions.is_empty();

        if valid {
            debug!(target: "assertion-executor::validate_tx", gas_used = results.iter().map(|a| a.total_assertion_gas).sum::<u64>(), assertions_ran = results.iter().map(|a| a.total_assertion_funcs_ran).sum::<u64>(), "Tx validated");
            trace!(target: "assertion-executor::validate_tx", "Committing state changes to fork db");
            fork_db.commit(forked_tx_result.result_and_state.state.clone());
        } else {
            debug!(target: "assertion-executor::validate_tx", gas_used = results.iter().map(|a| a.total_assertion_gas).sum::<u64>(), assertions_ran = results.iter().map(|a| a.total_assertion_funcs_ran).sum::<u64>(), ?invalid_assertions, "Tx invalidated by assertions");
            trace!(
                target: "assertion-executor::validate_tx",
                "Not committing state changes to fork db"
            );
        }

        Ok(TxValidationResult::new(
            valid,
            forked_tx_result.result_and_state,
            results,
        ))
    }

    fn execute_assertions<Active>(
        &self,
        block_env: BlockEnv,
        tx_fork_db: ForkDb<Active>,
        forked_tx_result: &ExecuteForkedTxResult,
    ) -> Result<Vec<AssertionContractExecution>, AssertionExecutionError<Active>>
    where
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
    {
        let ExecuteForkedTxResult {
            call_tracer,
            result_and_state,
        } = forked_tx_result;
        let logs_and_traces = LogsAndTraces {
            tx_logs: result_and_state.result.logs(),
            call_traces: call_tracer,
        };

        let assertions = self
            .store
            .read(logs_and_traces.call_traces, U256::from(block_env.number))?;

        if assertions.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            target: "assertion-executor::execute_assertions",
            assertion_ids = ?assertions.iter().map(|a| format!("{:?}", a.assertion_contract.id)).collect::<Vec<_>>(),
            assertion_contract_count = assertions.len(),
            "Retrieved Assertion contracts from Assertion store"
        );

        let results: Result<Vec<AssertionContractExecution>, AssertionExecutionError<Active>> =
            assertions
                .into_par_iter()
                .map(
                    move |assertion_for_execution| -> Result<
                        AssertionContractExecution,
                        AssertionExecutionError<Active>,
                    > {
                        let phevm_context =
                            PhEvmContext::new(&logs_and_traces, assertion_for_execution.adopter);

                        self.run_assertion_contract(
                            &assertion_for_execution.assertion_contract,
                            &assertion_for_execution.selectors,
                            block_env.clone(),
                            tx_fork_db.clone(),
                            &phevm_context,
                        )
                    },
                )
                .collect();
        debug!(target: "assertion-executor::execute_assertions", ?results, "Assertion Execution Results");
        results
    }

    #[instrument(skip_all, fields(assertion_id=%assertion_contract.id), level="debug", target="assertion-executor::execute_assertions")]
    fn run_assertion_contract<Active>(
        &self,
        assertion_contract: &AssertionContract,
        fn_selectors: &[FixedBytes<4>],
        block_env: BlockEnv,
        mut tx_fork_db: ForkDb<Active>,
        context: &PhEvmContext,
    ) -> Result<AssertionContractExecution, AssertionExecutionError<Active>>
    where
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
    {
        let AssertionContract { id, .. } = assertion_contract;

        trace!(
            target: "assertion-executor::execute_assertions",
            assertion_contract_id = ?id,
            selector_count = fn_selectors.len(),
            selectors = ?fn_selectors.iter().map(|s| format!("{s:x?}")).collect::<Vec<_>>(),
            "Executing assertion contract"
        );

        self.insert_persistent_accounts(assertion_contract, &mut tx_fork_db);
        let multi_fork_db = MultiForkDb::new(tx_fork_db, context.post_tx_journal());

        let inspector = PhEvmInspector::new(context.clone());
        let assertion_gas = AtomicU64::new(0);
        let assertions_ran = AtomicU64::new(0);

        let current_span = tracing::Span::current();
        let results_vec = current_span.in_scope(|| {
            fn_selectors
                .into_par_iter()
                .map(
                    |fn_selector: &FixedBytes<4>| -> Result<AssertionFunctionResult, AssertionExecutionError<Active>> {
                        self.execute_assertion_fn(AssertionExecutionParams {
                            assertion_contract,
                            fn_selector,
                            block_env: block_env.clone(),
                            multi_fork_db: multi_fork_db.clone(),
                            assertion_gas: &assertion_gas,
                            assertions_ran: &assertions_ran,
                            inspector: inspector.clone(),
                        })
                    },
                )
                .collect::<Vec<Result<AssertionFunctionResult, AssertionExecutionError<Active>>>>()
        });

        debug!(target: "assertion-executor::execute_assertions", execution_results=?results_vec.iter().map(|result| format!("{result:?}")).collect::<Vec<_>>(), "Assertion Execution Results");
        let mut valid_results = vec![];
        for result in results_vec {
            valid_results.push(result?);
        }

        let rax = AssertionContractExecution {
            assertion_fns_results: valid_results,
            total_assertion_gas: assertion_gas.into_inner(),
            total_assertion_funcs_ran: assertions_ran.into_inner(),
        };

        Ok(rax)
    }

    #[instrument(
        skip_all,
        level = "debug",
        target = "assertion-executor::execute_assertions"
    )]
    fn execute_assertion_fn<Active>(
        &self,
        params: AssertionExecutionParams<'_, Active>,
    ) -> Result<AssertionFunctionResult, AssertionExecutionError<Active>>
    where
        Active: DatabaseRef + Sync + Send,
    {
        let AssertionExecutionParams {
            assertion_contract,
            fn_selector,
            block_env,
            mut multi_fork_db,
            assertion_gas,
            assertions_ran,
            inspector,
        } = params;

        let tx_env = TxEnv {
            kind: TxKind::Call(ASSERTION_CONTRACT),
            caller: CALLER,
            data: (*fn_selector).into(),
            gas_limit: self.config.assertion_gas_limit,
            gas_price: block_env.basefee.into(),
            nonce: 42,
            chain_id: Some(self.config.chain_id),
            ..Default::default()
        };

        trace!(target: "assertion-executor::execute_assertions", ?tx_env, ?block_env, "Assertion Function Execution environment");

        let env = evm_env(self.config.chain_id, self.config.spec_id, block_env.clone());

        let mut evm = crate::build_evm_by_features!(&mut multi_fork_db, &env, inspector);
        let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);

        reprice_evm_storage!(evm);

        trace!(target: "assertion-executor::execute_assertions", "Executing assertion function");
        let result_and_state = evm.inspect_with_tx(tx_env);

        let result = result_and_state
                        .map(|result_and_state| result_and_state.result)
                        .map_err(|e| {
                            warn!(target: "assertion-executor::execute_assertions", error = ?e, "Evm error executing assertions");
                            e
                        })?;

        assertion_gas.fetch_add(result.gas_used(), std::sync::atomic::Ordering::Relaxed);
        assertions_ran.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        trace!(target: "assertion-executor::execute_assertions", ?result, "Assertion execution result and state");
        Ok(AssertionFunctionResult {
            id: AssertionFnId {
                fn_selector: *fn_selector,
                assertion_contract_id: assertion_contract.id,
            },
            result: AssertionFunctionExecutionResult::AssertionExecutionResult(result),
            console_logs: evm.inspector.context.console_logs.clone(),
        })
    }

    /// Commits a transaction against a fork of the current state using an external DB.
    #[instrument(
        level = "trace",
        skip_all,
        target = "assertion-executor::execute_tx",
        fields(tx_env, block_env)
    )]
    pub fn execute_forked_tx_ext_db<ExtDb>(
        &self,
        block_env: BlockEnv,
        tx_env: TxEnv,
        external_db: &mut ExtDb,
    ) -> Result<ExecuteForkedTxResult, ForkTxExecutionError<ExtDb>>
    where
        ExtDb: Database + Sync + Send,
    {
        let mut call_tracer = CallTracer::default();
        let env = evm_env(self.config.chain_id, self.config.spec_id, block_env.clone());

        let mut evm = crate::build_evm_by_features!(external_db, &env, &mut call_tracer);
        let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);

        let result_and_state = evm.inspect_with_tx(tx_env).map_err(|e| {
            debug!(target: "assertion-executor::execute_tx", error = %e, "Evm error in execute_forked_tx");
            e
        })?;

        debug!(
            target: "assertion-executor::execute_tx",
            state_changes = ?{
                result_and_state.state.iter().map(|(address, state_change)| {
                    format!("{:?}", StateChangeMetadata {
                        address,
                        storage: &state_change.storage,
                        balance: &state_change.info.balance,
                        has_code: state_change.info.code.is_some(),
                    })
                }).collect::<Vec<_>>()
            },
            "Forked transaction state changes"
        );

        let call_tracer = std::mem::take(evm.inspector);
        std::mem::drop(evm);

        // Propogate potential errors from the inspector, if any
        if let Err(err) = call_tracer.result {
            return Err(ForkTxExecutionError::CallTracerError(err));
        }

        Ok(ExecuteForkedTxResult {
            call_tracer,
            result_and_state,
        })
    }

    /// Inserts pre-deployed assertion contract inside the multi-fork db.
    pub fn insert_persistent_accounts<Active>(
        &self,
        assertion_contract: &AssertionContract,
        tx_fork_db: &mut ForkDb<Active>,
    ) where
        Active: DatabaseRef + Sync + Send,
        Active::Error: Send,
    {
        let AssertionContract {
            deployed_code,
            code_hash,
            storage,
            account_status,
            id,
            ..
        } = assertion_contract;

        let assertion_account_info = AccountInfo {
            nonce: 1,
            // TODO(Odysseas) Why do we need to set the balance to max?
            balance: U256::MAX,
            code: Some(deployed_code.clone()),
            code_hash: *code_hash,
        };

        let assertion_account = Account {
            info: assertion_account_info,
            storage: storage.clone(),
            status: *account_status,
        };

        let caller_account = Account {
            info: AccountInfo {
                nonce: 42,
                balance: U256::MAX,
                ..Default::default()
            },
            status: AccountStatus::Touched,
            ..Default::default()
        };

        let precompile_account = Account {
            info: AccountInfo {
                nonce: 1,
                balance: U256::MAX,
                code: Some(Bytecode::new_raw(bytes!("DEAD"))),
                //Code needed to hit 'call(..)' fn of the inspector trait
                ..Default::default()
            },
            status: AccountStatus::Touched,
            ..Default::default()
        };

        let mut state = EvmState::default();

        for (address, account) in [
            (ASSERTION_CONTRACT, assertion_account),
            (CALLER, caller_account),
            (PRECOMPILE_ADDRESS, precompile_account),
        ] {
            state.insert(address, account);
        }

        tx_fork_db.commit(state);
        for address in [ASSERTION_CONTRACT, CALLER, PRECOMPILE_ADDRESS] {
            tx_fork_db
                .storage
                .entry(address)
                .or_default()
                .dont_read_from_inner_db = true;
        }
        trace!(
            target: "assertion-executor::insert_persistent_accounts",
            assertion_id = ?id,
            "Inserted assertion contract into assertion journal"
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            DatabaseRef,
            overlay::{
                OverlayDb,
                test_utils::MockDb,
            },
        },
        primitives::{
            BlockEnv,
            U256,
            uint,
        },
        store::{
            AssertionState,
            AssertionStore,
        },
        test_utils::*,
    };
    use revm::{
        context::JournalInner,
        database::{
            CacheDB,
            EmptyDBTyped,
        },
    };
    use std::convert::Infallible;

    // Define a concrete error type for tests if needed, or use Infallible
    type TestDbError = Infallible; // Or a custom test error enum

    // Define the DB type alias used in tests
    type TestDB = OverlayDb<CacheDB<EmptyDBTyped<TestDbError>>>;
    // Define the Fork DB type alias used in tests
    type TestForkDB = ForkDb<TestDB>;

    #[test]
    fn test_deploy_assertion_contract() {
        // Use the TestDB type
        let test_db: TestDB = OverlayDb::<CacheDB<EmptyDBTyped<TestDbError>>>::new_test();
        let mut fork_db: TestForkDB = test_db.fork();

        let assertion_store = AssertionStore::new_ephemeral().unwrap();

        // Build uses TestDB
        let executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);

        let counter_assertion = counter_assertion();

        // insert_assertion_contract is generic, works with MultiForkDb<ForkDb<TestDB>>
        executor.insert_persistent_accounts(&counter_assertion, &mut fork_db);
        let multi_fork_db = MultiForkDb::new(fork_db, &JournalInner::new());
        trace!(
            target: "assertion-executor::insert_persistent_accounts",
            assertion_id = ?counter_assertion.id,
            "Inserted assertion contract into assertion journal"
        );

        let account_info = multi_fork_db
            .basic_ref(ASSERTION_CONTRACT)
            .unwrap()
            .unwrap()
            .clone();

        assert_eq!(account_info.code.unwrap(), counter_assertion.deployed_code);
    }

    #[test]
    fn test_execute_forked_tx() {
        // Use the TestDB type
        let shared_db: TestDB = OverlayDb::<CacheDB<EmptyDBTyped<TestDbError>>>::new_test();

        let mut mock_db = MockDb::new();
        mock_db.insert_account(COUNTER_ADDRESS, counter_acct_info());

        let assertion_store = AssertionStore::new_ephemeral().unwrap();

        // Build uses TestDB
        let executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);

        // execute_forked_tx uses &mut ForkDb<TestDB>
        let result = executor
            .execute_forked_tx_ext_db(BlockEnv::default(), counter_call(), &mut mock_db)
            .unwrap();

        //Traces should contain the call to the counter contract
        assert_eq!(
            result
                .call_tracer
                .calls()
                .into_iter()
                .collect::<Vec<Address>>(),
            vec![COUNTER_ADDRESS]
        );

        // State changes should contain the counter contract and the caller accounts
        let _accounts = result
            .result_and_state
            .state
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        // Check storage on the TestForkDB
        assert_eq!(
            result
                .call_tracer
                .journal
                .state
                .get(&COUNTER_ADDRESS)
                .unwrap()
                .storage
                .get(&U256::ZERO)
                .cloned()
                .unwrap()
                .present_value,
            uint!(1_U256)
        );

        // Check storage on the original TestDB via executor.db
        assert_eq!(
            shared_db.storage_ref(COUNTER_ADDRESS, U256::ZERO),
            Ok(U256::ZERO)
        );
    }
    #[test]
    fn test_validate_tx() {
        // Use the TestDB type
        let test_db: TestDB = OverlayDb::<CacheDB<EmptyDBTyped<TestDbError>>>::new_test();

        let mut mock_db = MockDb::new();

        mock_db.insert_account(COUNTER_ADDRESS, counter_acct_info());

        let assertion_store = AssertionStore::new_ephemeral().unwrap();

        // Insert requires Bytes, use helper from test_utils
        let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
        assertion_store
            .insert(
                COUNTER_ADDRESS,
                // Assuming AssertionState::new_test takes Bytes or similar
                AssertionState::new_test(assertion_bytecode),
            )
            .unwrap();

        let config = ExecutorConfig::default();

        // Build uses TestDB
        let mut executor = AssertionExecutor::new(config.clone(), assertion_store);

        let basefee = 10;
        let number = 1;
        let block_env = BlockEnv {
            number,
            basefee,
            ..Default::default()
        };

        let tx = TxEnv {
            gas_price: basefee.into(),
            ..counter_call()
        };

        mock_db.insert_account(
            tx.caller,
            AccountInfo {
                balance: U256::MAX,
                ..Default::default()
            },
        );

        // Fork uses TestDB
        let mut fork_db: TestForkDB = test_db.fork();

        for (expected_state_before, expected_state_after, expected_result) in [
            (uint!(0_U256), uint!(1_U256), true),  // Counter is incremented
            (uint!(1_U256), uint!(1_U256), false), // Counter is not incremented as assertion fails
        ] {
            // Check storage on TestForkDB
            assert_eq!(
                fork_db.storage_ref(COUNTER_ADDRESS, U256::ZERO),
                Ok(expected_state_before),
                "Expected state before: {expected_state_before}",
            );

            let mut tx = tx.clone();
            tx.nonce = expected_state_before.try_into().unwrap();
            // validate_transaction uses &mut ForkDb<TestDB>
            let result = executor
                .validate_transaction_ext_db::<_, _>(
                    block_env.clone(),
                    tx,
                    &mut fork_db,
                    &mut mock_db,
                )
                .unwrap(); // Use unwrap or handle ExecutorError<TestDbError>

            assert_eq!(result.transaction_valid, expected_result);
            // Only assert gas if the transaction was meant to run assertions
            if result.transaction_valid || !expected_result {
                // If tx valid, or if tx invalid and we expected it to be invalid
                assert!(
                    result.total_assertions_gas() > 0,
                    "Assertions should have run gas"
                );
            }

            if result.transaction_valid {
                mock_db.commit(result.result_and_state.state.clone());
            }

            // Check storage on TestForkDB after potential commit
            assert_eq!(
                fork_db.storage_ref(COUNTER_ADDRESS, U256::ZERO),
                Ok(expected_state_after),
                "Expected state after: {expected_state_after}",
            );
        }

        // Check original TestDB via executor.db
        assert_eq!(test_db.basic_ref(ASSERTION_CONTRACT).unwrap(), None);
    }
}
