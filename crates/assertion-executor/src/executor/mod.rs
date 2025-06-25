pub mod config;

use std::{
    fmt::{
        Debug,
        Display,
    },
    sync::atomic::AtomicU64,
};

use crate::{
    build_evm::{
        new_phevm,
        new_tx_fork_evm,
    },
    db::{
        fork_db::ForkDb,
        multi_fork_db::MultiForkDb,
        DatabaseCommit,
        DatabaseRef,
        PhDB,
    },
    // Ensure ExecutorError is accessible
    error::ExecutorError,
    inspectors::{
        CallTracer,
        LogsAndTraces,
        PhEvmContext,
        PhEvmInspector,
    },
    primitives::{
        address,
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
        EVMError,
        EvmState,
        EvmStorage,
        FixedBytes,
        ResultAndState,
        TxEnv,
        TxKind,
        TxValidationResult,
        U256,
    },
    store::AssertionStore,
    ExecutorConfig,
};

use revm::Database;

use rayon::prelude::{
    IntoParallelIterator,
    ParallelIterator,
};

use tracing::{
    debug,
    error,
    instrument,
    trace,
    warn,
};

#[cfg(feature = "optimism")]
use crate::executor::config::create_optimism_fields;

/// Used to deploys the assertion contract to the forked db, and to call assertion functions.
pub const CALLER: Address = address!("00000000000000000000000000000000000001A4");

/// The address of the assertion contract.
/// This is a fixed address that is used to deploy assertion contracts.
/// Deploying assertion contracts via the caller address @ nonce 0 results in this address
pub const ASSERTION_CONTRACT: Address = address!("63f9abbe8aa6ba1261ef3b0cbfb25a5ff8eeed10");

#[derive(Debug, Clone)]
pub struct AssertionExecutor<DB> {
    pub db: DB,
    pub config: ExecutorConfig,
    pub store: AssertionStore,
}

impl<DB: PhDB> AssertionExecutor<DB> {
    /// Creates a new assertion executor.
    pub fn new(db: DB, config: ExecutorConfig, store: AssertionStore) -> Self {
        Self { db, config, store }
    }
}

// Define the result type using the main DB error
type ExecutorResult<T, DB> = Result<T, ExecutorError<<DB as DatabaseRef>::Error>>;

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
    context: &'a PhEvmContext<'a>,
}

impl<DB: PhDB> AssertionExecutor<DB>
where
    DB::Error: std::fmt::Debug + Send,
{
    /// Executes a transaction against an external revm database, and runs the appropriate
    /// assertions.
    ///
    /// We execute against an external database here to satisfy a requirement within op-talos, where
    /// transactions couldnt be properly commited if they weren't touched by the database beforehand.
    ///
    /// Returns the results of the assertions, as well as the state changes that should be
    /// committed if the assertions pass.
    #[instrument(level = "debug", skip_all, target = "executor::validate_tx")]
    pub fn validate_transaction_ext_db<'validation, ExtDb, Active>(
        &'validation mut self,
        block_env: BlockEnv,
        tx_env: TxEnv,
        fork_db: &mut ForkDb<Active>,
        external_db: &mut ExtDb,
    ) -> ExecutorResult<TxValidationResult, DB>
    where
        ExtDb: Database,
        ExtDb::Error: Display,
        Active: DatabaseRef + Sync + Send,
        Active::Error: Debug,
    {
        let pre_tx_db = fork_db.clone();
        let mut post_tx_db = fork_db.clone();

        // This call relies on From<EVMError<ExtDb::Error>> for ExecutorError<DB::Error>
        let (tx_traces, result_and_state) =
            self.execute_forked_tx_ext_db(block_env.clone(), tx_env, &mut post_tx_db, external_db)?;

        if !result_and_state.result.is_success() {
            debug!(target: "assertion-executor::validate_tx", "Transaction execution failed, skipping assertions");
            return Ok(TxValidationResult::new(true, result_and_state, vec![]));
        }
        debug!(target: "assertion-executor::validate_tx", triggers=?tx_traces.triggers(), gas_used=result_and_state.result.gas_used(), "Transaction execution succeeded.");

        let multi_fork_db = MultiForkDb::new(pre_tx_db, post_tx_db);

        let logs_and_traces = LogsAndTraces {
            tx_logs: result_and_state.result.logs(),
            call_traces: &tx_traces,
        };

        let results = self.execute_assertions(block_env, multi_fork_db, logs_and_traces)?;
        if results.is_empty() {
            debug!(target: "assertion-executor::validate_tx", "No assertions were executed");
            trace!(target: "assertion-executor::validate_tx", "Comitting state changes to fork db");
            fork_db.commit(result_and_state.state.clone());
            return Ok(TxValidationResult::new(true, result_and_state, vec![]));
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
            fork_db.commit(result_and_state.state.clone());
        } else {
            debug!(target: "assertion-executor::validate_tx", gas_used = results.iter().map(|a| a.total_assertion_gas).sum::<u64>(), assertions_ran = results.iter().map(|a| a.total_assertion_funcs_ran).sum::<u64>(), ?invalid_assertions, "Tx invalidated by assertions");
            trace!(
                target: "assertion-executor::validate_tx",
                "Not committing state changes to fork db"
            );
        }

        Ok(TxValidationResult::new(valid, result_and_state, results))
    }

    fn execute_assertions<'a, Active>(
        &'a self,
        block_env: BlockEnv,
        multi_fork_db: MultiForkDb<ForkDb<Active>>,
        logs_and_traces: LogsAndTraces<'a>,
    ) -> ExecutorResult<Vec<AssertionContractExecution>, DB>
    where
        Active: DatabaseRef + Sync + Send,
        Active::Error: Debug,
    {
        let assertions = self
            .store
            .read(logs_and_traces.call_traces, block_env.number)?;

        if assertions.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            target: "assertion-executor::execute_assertions",
            assertion_ids = ?assertions.iter().map(|a| format!("{:?}", a.assertion_contract.id)).collect::<Vec<_>>(),
            assertion_contract_count = assertions.len(),
            "Retrieved Assertion contracts from Assertion store"
        );

        let results: ExecutorResult<Vec<AssertionContractExecution>, DB> = assertions
            .into_par_iter()
            .map(
                move |assertion_for_execution| -> ExecutorResult<AssertionContractExecution, DB> {
                    let phevm_context =
                        PhEvmContext::new(&logs_and_traces, assertion_for_execution.adopter);

                    self.run_assertion_contract(
                        &assertion_for_execution.assertion_contract,
                        &assertion_for_execution.selectors,
                        block_env.clone(),
                        multi_fork_db.clone(),
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
        mut multi_fork_db: MultiForkDb<ForkDb<Active>>,
        context: &PhEvmContext,
    ) -> ExecutorResult<AssertionContractExecution, DB>
    where
        Active: DatabaseRef + Sync + Send,
        Active::Error: Debug,
    {
        let AssertionContract { id, .. } = assertion_contract;

        trace!(
            target: "assertion-executor::execute_assertions",
            assertion_contract_id = ?id,
            selector_count = fn_selectors.len(),
            selectors = ?fn_selectors.iter().map(|s| format!("{s:x?}")).collect::<Vec<_>>(),
            "Executing assertion contract"
        );

        self.insert_assertion_contract(assertion_contract, &mut multi_fork_db);

        let assertion_gas = AtomicU64::new(0);
        let assertions_ran = AtomicU64::new(0);

        let current_span = tracing::Span::current();

        let results_vec = current_span.in_scope(|| {
            fn_selectors
                .into_par_iter()
                .map(
                    |fn_selector: &FixedBytes<4>| -> ExecutorResult<AssertionFunctionResult, DB> {
                        self.execute_assertion_fn(AssertionExecutionParams {
                            assertion_contract,
                            fn_selector,
                            block_env: block_env.clone(),
                            multi_fork_db: multi_fork_db.clone(),
                            assertion_gas: &assertion_gas,
                            assertions_ran: &assertions_ran,
                            context,
                        })
                    },
                )
                .collect::<Vec<ExecutorResult<AssertionFunctionResult, DB>>>()
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
    ) -> ExecutorResult<AssertionFunctionResult, DB>
    where
        Active: DatabaseRef + Sync + Send,
        Active::Error: Debug,
    {
        let AssertionExecutionParams {
            assertion_contract,
            fn_selector,
            block_env,
            mut multi_fork_db,
            assertion_gas,
            assertions_ran,
            context,
        } = params;

        let inspector = PhEvmInspector::new(self.config.spec_id, &mut multi_fork_db, context);

        let tx_env = TxEnv {
            transact_to: TxKind::Call(ASSERTION_CONTRACT),
            caller: CALLER,
            data: (*fn_selector).into(),
            gas_limit: self.config.assertion_gas_limit,
            gas_price: block_env.basefee,
            #[cfg(feature = "optimism")]
            optimism: create_optimism_fields(),
            ..Default::default()
        };
        trace!(target: "assertion-executor::execute_assertions", ?tx_env, ?block_env, "Assertion Function Execution environment");

        let mut evm = new_phevm(
            tx_env,
            block_env,
            self.config.chain_id,
            self.config.spec_id,
            &mut multi_fork_db,
            inspector,
        );
        trace!(target: "assertion-executor::execute_assertions", "Executing assertion function");
        let result_and_state = evm.transact();

        let result = result_and_state
                        .map(|result_and_state| result_and_state.result)
                        .map_err(|e| {
                            warn!(target: "assertion-executor::execute_assertions", error = ?e, "Evm error executing assertions");
                            // TODO(0xgregthedev): This is bad, we are losing the typed error, we need to fix it. 
                            // Convert the error to the appropriate type
                            ExecutorError::TxError(EVMError::Custom(format!("{e:?}")))
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
        })
    }

    /// Commits a transaction against a fork of the current state using an external DB.
    #[instrument(
        level = "trace",
        skip_all,
        target = "assertion-executor::execute_tx",
        fields(tx_env, block_env)
    )]
    pub fn execute_forked_tx_ext_db<ExtDb, Active>(
        &self,
        block_env: BlockEnv,
        tx_env: TxEnv,
        fork_db: &mut ForkDb<Active>,
        external_db: &mut ExtDb,
    ) -> ExecutorResult<(CallTracer, ResultAndState), DB>
    where
        ExtDb: Database,
        ExtDb::Error: Display,
        Active: DatabaseRef,
    {
        let mut evm = new_tx_fork_evm(
            tx_env,
            block_env,
            self.config.chain_id,
            self.config.spec_id,
            external_db,
            CallTracer::default(),
        );

        let result_and_state = evm.transact().map_err(|e| {
            debug!(target: "assertion-executor::execute_tx", error = %e, "Evm error in execute_forked_tx");
            match e {
                EVMError::Transaction(e) => ExecutorError::TxError(EVMError::Transaction(e)),
                EVMError::Header(e) => ExecutorError::TxError(EVMError::Header(e)),
                EVMError::Precompile(e) => ExecutorError::TxError(EVMError::Precompile(e)),
                EVMError::Custom(e) => ExecutorError::TxError(EVMError::Custom(e)),
                _ => {
                    error!(target: "assertion-executor::execute_tx", error = %e, "Unknown error occurred");
                    ExecutorError::TxError(EVMError::Custom(format!("Error: {e}")))
                }
            }
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

        let call_tracer = std::mem::take(&mut evm.context.external);
        std::mem::drop(evm);

        // Commit changes to the ForkDb<Active>
        fork_db.commit(result_and_state.state.clone());
        Ok((call_tracer, result_and_state))
    }

    /// Inserts pre-deployed assertion contract inside the multi-fork db.
    pub fn insert_assertion_contract<Active>(
        &self,
        assertion_contract: &AssertionContract,
        multi_fork_db: &mut MultiForkDb<ForkDb<Active>>,
    ) where
        Active: DatabaseRef,
    {
        let AssertionContract {
            deployed_code,
            code_hash,
            storage,
            account_status,
            id,
            ..
        } = assertion_contract;

        let account_info = AccountInfo {
            nonce: 1,
            // TODO(Odysseas) Why do we need to set the balance to max?
            balance: U256::MAX,
            code: Some(deployed_code.clone()),
            code_hash: *code_hash,
        };

        let account = Account {
            info: account_info,
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

        let mut state = EvmState::default();
        state.insert(ASSERTION_CONTRACT, account);
        state.insert(CALLER, caller_account);
        multi_fork_db.commit(state);
        multi_fork_db
            .active_db
            .storage
            .entry(ASSERTION_CONTRACT)
            .or_default()
            .dont_read_from_inner_db = true;
        multi_fork_db
            .active_db
            .storage
            .entry(CALLER)
            .or_default()
            .dont_read_from_inner_db = true;

        trace!(
            target: "assertion-executor::insert_assertion_contract",
            assertion_id = ?id,
            "Inserted assertion contract into multi fork db"
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::overlay::test_utils::MockDb;
    use crate::{
        db::{
            overlay::OverlayDb,
            DatabaseRef,
        },
        primitives::{
            uint,
            BlockEnv,
            U256,
        },
        store::{
            AssertionState,
            AssertionStore,
        },
        test_utils::*,
    };
    use revm::db::CacheDB;
    use revm::db::EmptyDBTyped;
    use std::convert::Infallible;

    // Define a concrete error type for tests if needed, or use Infallible
    type TestDbError = Infallible; // Or a custom test error enum

    impl From<String> for ExecutorError<Infallible> {
        fn from(s: String) -> Self {
            ExecutorError::TxError(EVMError::Custom(s))
        }
    }
    // Define the DB type alias used in tests
    type TestDB = OverlayDb<CacheDB<EmptyDBTyped<TestDbError>>>;
    // Define the Fork DB type alias used in tests
    type TestForkDB = ForkDb<TestDB>;

    #[test]
    fn test_deploy_assertion_contract() {
        // Use the TestDB type
        let test_db: TestDB = OverlayDb::<CacheDB<EmptyDBTyped<TestDbError>>>::new_test();

        let assertion_store = AssertionStore::new_ephemeral().unwrap();

        // Build uses TestDB
        let executor = ExecutorConfig::default().build(test_db.clone(), assertion_store);

        // Forks use TestDB
        let mut multi_fork_db = MultiForkDb::new(test_db.fork(), test_db.fork());

        let counter_assertion = counter_assertion();

        // insert_assertion_contract is generic, works with MultiForkDb<ForkDb<TestDB>>
        executor.insert_assertion_contract(&counter_assertion, &mut multi_fork_db);

        let account_info = multi_fork_db
            .basic_ref(ASSERTION_CONTRACT)
            .unwrap()
            .unwrap();

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
        let executor = ExecutorConfig::default().build(shared_db.clone(), assertion_store);

        // Fork uses TestDB
        let mut fork_db: TestForkDB = shared_db.fork();

        // execute_forked_tx uses &mut ForkDb<TestDB>
        let (traces, result_and_state) = executor
            .execute_forked_tx_ext_db(
                BlockEnv::default(),
                counter_call(),
                &mut fork_db,
                &mut mock_db,
            )
            .unwrap();

        //Traces should contain the call to the counter contract
        assert_eq!(
            traces.calls().into_iter().collect::<Vec<Address>>(),
            vec![COUNTER_ADDRESS]
        );

        // State changes should contain the counter contract and the caller accounts
        let _accounts = result_and_state.state.keys().cloned().collect::<Vec<_>>();

        // Check storage on the TestForkDB
        assert_eq!(
            fork_db.storage_ref(COUNTER_ADDRESS, U256::ZERO),
            Ok(uint!(1_U256))
        );

        // Check storage on the original TestDB via executor.db
        assert_eq!(
            executor.db.storage_ref(COUNTER_ADDRESS, U256::ZERO),
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
        let mut executor = config.clone().build(test_db, assertion_store);

        let basefee = uint!(10_U256);
        let block_env = BlockEnv {
            number: uint!(1_U256),
            basefee,
            ..Default::default()
        };

        let tx = TxEnv {
            gas_price: basefee,
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
        let mut fork_db: TestForkDB = executor.db.fork();

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

            // validate_transaction uses &mut ForkDb<TestDB>
            let result = executor
                .validate_transaction_ext_db::<_, _>(
                    block_env.clone(),
                    tx.clone(),
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
        assert_eq!(executor.db.basic_ref(ASSERTION_CONTRACT).unwrap(), None);
    }
}
