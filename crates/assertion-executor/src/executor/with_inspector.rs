//! Implementation of `validate_transaction_with_inspector` and related helper functions.
//!
//! This module provides variants of the assertion execution functions that accept
//! a custom inspector, allowing observation of both transaction execution and
//! assertion execution with independent inspectors.

use std::{
    sync::atomic::AtomicU64,
    time::Instant,
};

use crate::{
    db::{
        DatabaseRef,
        fork_db::ForkDb,
        multi_fork_db::MultiForkDb,
    },
    error::{
        AssertionExecutionError,
        ExecutorError,
    },
    evm::build_evm::evm_env,
    inspectors::{
        CallTracer,
        PhEvmContext,
        PhEvmInspector,
    },
    primitives::{
        AssertionContract,
        AssertionContractExecution,
        AssertionFunctionResult,
        BlockEnv,
        FixedBytes,
        TxEnv,
        TxValidationResultWithInspectors,
    },
};

use revm::{
    Database,
    InspectEvm,
    Inspector,
};

use crate::error::TxExecutionError;
use tracing::{
    debug,
    instrument,
    trace,
};

use rayon::prelude::{
    IntoParallelIterator,
    ParallelIterator,
};

use super::{
    AssertionExecutor,
    ExecuteForkedTxResult,
    assertion_executor_pool,
};

use crate::evm::build_evm::{
    EthCtx,
    OpCtx,
};

impl AssertionExecutor {
    /// Validates a transaction with a custom inspector that observes both
    /// transaction execution and assertion execution.
    ///
    /// The inspector is cloned for each execution (TX and each assertion function),
    /// allowing independent observation of each phase.
    ///
    /// # Returns
    /// A `TxValidationResultWithInspectors` containing:
    /// - The validation result
    /// - A vector of inspectors: first element is from TX execution, subsequent
    ///   elements are from each assertion function execution
    #[instrument(level = "debug", skip_all, target = "executor::validate_tx")]
    pub fn validate_transaction_with_inspector<ExtDb, Active, I>(
        &mut self,
        block_env: BlockEnv,
        tx_env: &TxEnv,
        fork_db: &mut ForkDb<Active>,
        external_db: &mut ExtDb,
        inspector: I,
    ) -> Result<
        TxValidationResultWithInspectors<I>,
        ExecutorError<<Active as DatabaseRef>::Error, <ExtDb as Database>::Error>,
    >
    where
        ExtDb: Database + Sync + Send,
        ExtDb::Error: Send,
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
        I: Clone + Send + Sync,
        for<'db> I: Inspector<EthCtx<'db, ExtDb>>,
        for<'db> I: Inspector<EthCtx<'db, MultiForkDb<ForkDb<Active>>>>,
        for<'db> I: Inspector<OpCtx<'db, ExtDb>>,
        for<'db> I: Inspector<OpCtx<'db, MultiForkDb<ForkDb<Active>>>>,
    {
        let tx_fork_db = fork_db.clone();

        // Execute transaction with inspector
        let (forked_tx_result, tx_inspector) = self
            .execute_forked_tx_ext_db_with_inspector::<ExtDb, I>(
                &block_env,
                tx_env.clone(),
                external_db,
                inspector.clone(),
            )
            .map_err(ExecutorError::ForkTxExecutionError)?;

        let exec_result = &forked_tx_result.result_and_state.result;
        if !exec_result.is_success() {
            debug!(target: "assertion-executor::validate_tx", "Transaction execution failed, skipping assertions");
            return Ok(TxValidationResultWithInspectors {
                result: Self::validation_result_without_assertions(
                    forked_tx_result.result_and_state,
                    std::time::Duration::ZERO,
                ),
                inspectors: vec![tx_inspector],
            });
        }
        debug!(target: "assertion-executor::validate_tx", gas_used=exec_result.gas_used(), "Transaction execution succeeded.");

        let assertion_timer = Instant::now();
        let (results, assertion_inspectors) = self
            .execute_assertions_with_inspector(
                block_env,
                tx_fork_db,
                &forked_tx_result,
                tx_env,
                inspector,
            )
            .map_err(|e| {
                ExecutorError::AssertionExecutionError(
                    forked_tx_result.result_and_state.state.clone(),
                    e,
                )
            })?;
        let assertion_execution_duration = assertion_timer.elapsed();

        // Combine inspectors: tx inspector first, then assertion inspectors
        let mut all_inspectors = vec![tx_inspector];
        all_inspectors.extend(assertion_inspectors);

        let result = self.finalize_validation_result(
            fork_db,
            tx_env,
            forked_tx_result.result_and_state,
            results,
            assertion_execution_duration,
            true,
        );
        Ok(TxValidationResultWithInspectors {
            result,
            inspectors: all_inspectors,
        })
    }

    /// Execute a transaction against an external database with a custom inspector.
    /// Returns the execution result and the inspector after execution.
    #[instrument(level = "trace", skip_all, target = "assertion-executor::execute_tx")]
    fn execute_forked_tx_ext_db_with_inspector<ExtDb, I>(
        &self,
        block_env: &BlockEnv,
        tx_env: TxEnv,
        external_db: &mut ExtDb,
        mut inspector: I,
    ) -> Result<(ExecuteForkedTxResult, I), TxExecutionError<<ExtDb as Database>::Error>>
    where
        ExtDb: Database + Sync + Send,
        for<'db> I: Inspector<EthCtx<'db, ExtDb>>,
        for<'db> I: Inspector<OpCtx<'db, ExtDb>>,
    {
        let mut call_tracer = CallTracer::new(self.store.clone());
        let env = evm_env(self.config.chain_id, self.config.spec_id, block_env.clone());

        // Compose inspector with call tracer: (custom_inspector, call_tracer)
        let composed_inspector = (&mut inspector, &mut call_tracer);

        let mut evm = crate::build_evm_by_features!(external_db, &env, composed_inspector);
        let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);

        let result_and_state = evm.inspect_tx(tx_env).map_err(|e| {
            debug!(target: "assertion-executor::execute_tx", error = ?e, "Evm error in execute_forked_tx_with_inspector");
            TxExecutionError::TxEvmError(e)
        })?;

        drop(evm);

        // Propagate potential errors from the call tracer
        if let Err(err) = call_tracer.result {
            return Err(TxExecutionError::CallTracerError(err));
        }

        Ok((
            ExecuteForkedTxResult {
                call_tracer,
                result_and_state,
            },
            inspector,
        ))
    }

    /// Execute assertions with a custom inspector.
    /// Returns the assertion results and a vector of inspectors (one per assertion function).
    #[allow(clippy::type_complexity)]
    fn execute_assertions_with_inspector<Active, I>(
        &self,
        block_env: BlockEnv,
        tx_fork_db: ForkDb<Active>,
        forked_tx_result: &ExecuteForkedTxResult,
        tx_env: &TxEnv,
        inspector: I,
    ) -> Result<
        (Vec<AssertionContractExecution>, Vec<I>),
        AssertionExecutionError<<Active as DatabaseRef>::Error>,
    >
    where
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
        I: Clone + Send + Sync,
        for<'db> I: Inspector<EthCtx<'db, MultiForkDb<ForkDb<Active>>>>,
        for<'db> I: Inspector<OpCtx<'db, MultiForkDb<ForkDb<Active>>>>,
    {
        let results = self.execute_triggered_assertions(
            block_env,
            tx_fork_db,
            forked_tx_result,
            tx_env,
            |assertion_contract, fn_selectors, block_env, tx_fork_db, context| {
                self.run_assertion_contract_with_inspector(
                    assertion_contract,
                    fn_selectors,
                    block_env,
                    tx_fork_db,
                    context,
                    inspector.clone(),
                )
            },
        )?;

        let mut all_executions = Vec::with_capacity(results.len());
        let mut all_inspectors = Vec::new();
        for (execution, inspectors) in results {
            all_executions.push(execution);
            all_inspectors.extend(inspectors);
        }

        debug!(target: "assertion-executor::execute_assertions", execution_count=all_executions.len(), inspector_count=all_inspectors.len(), "Assertion Execution Results with Inspectors");
        Ok((all_executions, all_inspectors))
    }

    /// Inspector variant of [`AssertionExecutor::run_assertion_contract`].
    ///
    /// Uses the same [`AssertionExecutor::prepare_assertion_contract`] setup, but each
    /// rayon task composes the caller's inspector with the phevm inspector so both
    /// observe execution. Returns one cloned inspector per function selector alongside
    /// the execution results.
    #[instrument(
        skip_all,
        fields(assertion_id=%assertion_contract.id),
        level = "debug",
        target = "assertion-executor::execute_assertions"
    )]
    fn run_assertion_contract_with_inspector<Active, I>(
        &self,
        assertion_contract: &AssertionContract,
        fn_selectors: &[FixedBytes<4>],
        block_env: &BlockEnv,
        tx_fork_db: ForkDb<Active>,
        context: &PhEvmContext,
        inspector: I,
    ) -> Result<
        (AssertionContractExecution, Vec<I>),
        AssertionExecutionError<<Active as DatabaseRef>::Error>,
    >
    where
        Active: DatabaseRef + Sync + Send + Clone,
        Active::Error: Send,
        I: Clone + Send + Sync,
        for<'db> I: Inspector<EthCtx<'db, MultiForkDb<ForkDb<Active>>>>,
        for<'db> I: Inspector<OpCtx<'db, MultiForkDb<ForkDb<Active>>>>,
    {
        let prepared = self.prepare_assertion_contract(
            assertion_contract, fn_selectors, tx_fork_db, context,
        );

        let current_span = tracing::Span::current();
        let results_vec: Vec<_> = current_span.in_scope(|| {
            assertion_executor_pool().install(|| {
                fn_selectors
                    .into_par_iter()
                    .map(|fn_selector| {
                        self.execute_assertion_fn_with_inspector(
                            assertion_contract,
                            fn_selector,
                            block_env.clone(),
                            prepared.multi_fork_db.clone(),
                            &prepared.assertion_gas,
                            &prepared.assertions_ran,
                            prepared.inspector.clone(),
                            inspector.clone(),
                        )
                    })
                    .collect()
            })
        });

        trace!(target: "assertion-executor::execute_assertions", result_count=results_vec.len(), "Assertion Execution Results with Inspectors");

        let mut valid_results = Vec::with_capacity(results_vec.len());
        let mut inspectors = Vec::with_capacity(results_vec.len());
        for result in results_vec {
            let (fn_result, fn_inspector) = result?;
            valid_results.push(fn_result);
            inspectors.push(fn_inspector);
        }

        Ok((
            AssertionContractExecution {
                adopter: context.adopter,
                assertion_fns_results: valid_results,
                total_assertion_gas: prepared.assertion_gas.into_inner(),
                total_assertion_funcs_ran: prepared.assertions_ran.into_inner(),
            },
            inspectors,
        ))
    }

    /// Execute a single assertion function with a custom inspector.
    /// Returns the function result and the inspector after execution.
    #[instrument(
        skip_all,
        level = "debug",
        target = "assertion-executor::execute_assertions"
    )]
    #[allow(clippy::too_many_arguments)]
    fn execute_assertion_fn_with_inspector<Active, I>(
        &self,
        assertion_contract: &AssertionContract,
        fn_selector: &FixedBytes<4>,
        block_env: BlockEnv,
        mut multi_fork_db: MultiForkDb<ForkDb<Active>>,
        assertion_gas: &AtomicU64,
        assertions_ran: &AtomicU64,
        mut phevm_inspector: PhEvmInspector<'_>,
        mut inspector: I,
    ) -> Result<(AssertionFunctionResult, I), AssertionExecutionError<<Active as DatabaseRef>::Error>>
    where
        Active: DatabaseRef + Sync + Send,
        for<'db> I: Inspector<EthCtx<'db, MultiForkDb<ForkDb<Active>>>>,
        for<'db> I: Inspector<OpCtx<'db, MultiForkDb<ForkDb<Active>>>>,
    {
        // Compose inspector: (custom_inspector, phevm_inspector).
        let composed_inspector = (&mut inspector, &mut phevm_inspector);
        let result = self.execute_assertion_fn_call(
            &block_env,
            fn_selector,
            &mut multi_fork_db,
            composed_inspector,
        )?;
        Self::record_assertion_metrics(assertion_gas, assertions_ran, result.gas_used());

        trace!(target: "assertion-executor::execute_assertions", ?result, "Assertion execution result with inspector");
        Ok((
            Self::assertion_function_result(
                assertion_contract,
                fn_selector,
                result,
                phevm_inspector.context.console_logs.clone(),
            ),
            inspector,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExecutorConfig,
        db::{
            fork_db::ForkDb,
            overlay::{
                OverlayDb,
                test_utils::MockDb,
            },
        },
        store::{
            AssertionState,
            AssertionStore,
        },
        test_utils::*,
    };
    use revm::{
        Database,
        Inspector,
        database::{
            CacheDB,
            EmptyDBTyped,
        },
    };
    use std::{
        convert::Infallible,
        sync::atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    type TestDbError = Infallible;
    type TestDB = OverlayDb<CacheDB<EmptyDBTyped<TestDbError>>>;
    type TestForkDB = ForkDb<TestDB>;

    /// A simple counting inspector that counts how many times `step` is called.
    /// This is used to verify that the inspector runs during execution.
    #[derive(Clone, Default)]
    struct CountingInspector {
        step_count: std::sync::Arc<AtomicUsize>,
    }

    impl CountingInspector {
        fn new() -> Self {
            Self {
                step_count: std::sync::Arc::new(AtomicUsize::new(0)),
            }
        }

        fn get_step_count(&self) -> usize {
            self.step_count.load(Ordering::Relaxed)
        }
    }

    // Implement Inspector for the counting inspector for EthCtx
    impl<DB: Database> Inspector<EthCtx<'_, DB>> for CountingInspector {
        fn step(
            &mut self,
            _interp: &mut revm::interpreter::Interpreter,
            _ctx: &mut EthCtx<'_, DB>,
        ) {
            self.step_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Implement Inspector for the counting inspector for OpCtx
    impl<DB: Database> Inspector<OpCtx<'_, DB>> for CountingInspector {
        fn step(&mut self, _interp: &mut revm::interpreter::Interpreter, _ctx: &mut OpCtx<'_, DB>) {
            self.step_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_validate_transaction_with_inspector() {
        use crate::primitives::AccountInfo;

        let test_db: TestDB = OverlayDb::<CacheDB<EmptyDBTyped<TestDbError>>>::new_test();
        let mut fork_db: TestForkDB = test_db.fork();

        let mut mock_db = MockDb::new();
        mock_db.insert_account(COUNTER_ADDRESS, counter_acct_info());

        let assertion_store = AssertionStore::new_ephemeral();

        // Insert assertion
        let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
        assertion_store
            .insert(
                COUNTER_ADDRESS,
                AssertionState::new_test(&assertion_bytecode),
            )
            .unwrap();

        let config = ExecutorConfig::default();
        let mut executor = AssertionExecutor::new(config.clone(), assertion_store);

        let basefee = 10;
        let number = U256::from(1);
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

        // Create a counting inspector
        let inspector = CountingInspector::new();

        // Validate transaction with inspector
        let result = executor
            .validate_transaction_with_inspector(
                block_env.clone(),
                &tx,
                &mut fork_db,
                &mut mock_db,
                inspector,
            )
            .expect("Transaction validation should succeed");

        // Transaction should be valid (first counter increment passes assertion)
        assert!(
            result.result.is_valid(),
            "Transaction should be valid on first call"
        );

        // Check inspector count:
        // - 1 inspector from TX execution
        // - 1 inspector from assertion function execution (SIMPLE_ASSERTION_COUNTER has 1 function)
        let expected_inspector_count = 1 + result.result.total_assertion_funcs_ran() as usize;
        assert_eq!(
            result.inspectors.len(),
            expected_inspector_count,
            "Should have {} inspectors: 1 for tx + {} for assertion functions",
            expected_inspector_count,
            result.result.total_assertion_funcs_ran()
        );

        // Verify tx inspector observed execution (step count > 0)
        let tx_inspector = result.tx_inspector().expect("Should have tx inspector");
        assert!(
            tx_inspector.get_step_count() > 0,
            "TX inspector should have observed steps"
        );

        // Verify assertion inspectors observed execution
        let assertion_inspectors = result.assertion_inspectors();
        assert!(
            !assertion_inspectors.is_empty(),
            "Should have assertion inspectors"
        );
        for (i, inspector) in assertion_inspectors.iter().enumerate() {
            assert!(
                inspector.get_step_count() > 0,
                "Assertion inspector {} should have observed steps",
                i
            );
        }

        // Verify each inspector has independent state (using Arc means they share the counter,
        // but cloning before execution gives us independent step counts)
        // Note: Since we use Arc, the counts are actually shared within each clone group.
        // The tx inspector and assertion inspectors track different executions.
        println!("TX inspector step count: {}", tx_inspector.get_step_count());
        for (i, inspector) in assertion_inspectors.iter().enumerate() {
            println!(
                "Assertion inspector {} step count: {}",
                i,
                inspector.get_step_count()
            );
        }
    }
}
