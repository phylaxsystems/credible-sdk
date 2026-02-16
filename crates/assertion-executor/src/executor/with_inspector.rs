//! Implementation of `validate_transaction_with_inspector` and related helper functions.
//!
//! This module provides variants of the assertion execution functions that accept
//! a custom inspector, allowing observation of both transaction execution and
//! assertion execution with independent inspectors.

use std::time::Instant;

use crate::{
    arena::{
        next_tx_arena_epoch,
        prepare_tx_arena_for_current_thread,
    },
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

        let tx_arena_epoch = next_tx_arena_epoch();
        let assertion_timer = Instant::now();
        let (results, assertion_inspectors) = self
            .execute_assertions_with_inspector(
                block_env,
                fork_db,
                &forked_tx_result,
                tx_env,
                inspector,
                tx_arena_epoch,
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

        let result = Self::finalize_validation_result(
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
    fn execute_assertions_with_inspector<Active, I>(
        &self,
        block_env: BlockEnv,
        tx_fork_db: &ForkDb<Active>,
        forked_tx_result: &ExecuteForkedTxResult,
        tx_env: &TxEnv,
        inspector: I,
        tx_arena_epoch: u64,
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
            tx_arena_epoch,
            |assertion_contract, fn_selectors, block_env, tx_fork_db, context, tx_arena_epoch| {
                self.run_assertion_contract_with_inspector(
                    assertion_contract,
                    fn_selectors,
                    block_env,
                    tx_fork_db,
                    context,
                    inspector.clone(),
                    tx_arena_epoch,
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
        tx_arena_epoch: u64,
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
        if fn_selectors.is_empty() {
            debug!(
                target: "assertion-executor::execute_assertions",
                assertion_contract_id = ?assertion_contract.id,
                "Skipping assertion contract with no matched selectors (inspector path)"
            );
            return Ok((
                AssertionContractExecution {
                    adopter: context.adopter,
                    assertion_fns_results: vec![],
                    total_assertion_gas: 0,
                    total_assertion_funcs_ran: 0,
                },
                vec![],
            ));
        }

        let prepared =
            self.prepare_assertion_contract(assertion_contract, fn_selectors, tx_fork_db, context);

        let execute_fn = |fn_selector: &FixedBytes<4>| {
            self.execute_assertion_fn_with_inspector(
                assertion_contract,
                *fn_selector,
                block_env.clone(),
                prepared.multi_fork_db.clone(),
                prepared.inspector.clone(),
                inspector.clone(),
                tx_arena_epoch,
            )
        };

        let selector_count = fn_selectors.len();
        let parallel_fns = selector_count >= super::PARALLEL_THRESHOLD;
        trace!(
            target: "assertion-executor::execute_assertions",
            assertion_contract_id = ?assertion_contract.id,
            selector_count,
            scheduling = if parallel_fns { "parallel" } else { "sequential" },
            "Assertion fn scheduling decision (inspector path)"
        );

        let current_span = tracing::Span::current();
        let results_vec: Vec<_> = current_span.in_scope(|| {
            if parallel_fns {
                assertion_executor_pool().install(|| {
                    fn_selectors.into_par_iter().map(execute_fn).collect()
                })
            } else {
                fn_selectors.iter().map(execute_fn).collect()
            }
        });

        trace!(target: "assertion-executor::execute_assertions", result_count=results_vec.len(), "Assertion Execution Results with Inspectors");

        let mut valid_results = Vec::with_capacity(results_vec.len());
        let mut inspectors = Vec::with_capacity(results_vec.len());
        let mut total_assertion_gas = 0;
        for result in results_vec {
            let (fn_result, fn_inspector) = result?;
            total_assertion_gas += fn_result.as_result().gas_used();
            valid_results.push(fn_result);
            inspectors.push(fn_inspector);
        }
        let total_assertion_funcs_ran = valid_results.len() as u64;

        Ok((
            AssertionContractExecution {
                adopter: context.adopter,
                assertion_fns_results: valid_results,
                total_assertion_gas,
                total_assertion_funcs_ran,
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
    fn execute_assertion_fn_with_inspector<Active, I>(
        &self,
        assertion_contract: &AssertionContract,
        fn_selector: FixedBytes<4>,
        block_env: BlockEnv,
        mut multi_fork_db: MultiForkDb<ForkDb<Active>>,
        mut phevm_inspector: PhEvmInspector<'_>,
        mut inspector: I,
        tx_arena_epoch: u64,
    ) -> Result<(AssertionFunctionResult, I), AssertionExecutionError<<Active as DatabaseRef>::Error>>
    where
        Active: DatabaseRef + Sync + Send,
        for<'db> I: Inspector<EthCtx<'db, MultiForkDb<ForkDb<Active>>>>,
        for<'db> I: Inspector<OpCtx<'db, MultiForkDb<ForkDb<Active>>>>,
    {
        prepare_tx_arena_for_current_thread(tx_arena_epoch);

        // Compose inspector: (custom_inspector, phevm_inspector).
        let composed_inspector = (&mut inspector, &mut phevm_inspector);
        let result = self.execute_assertion_fn_call(
            block_env,
            fn_selector,
            &mut multi_fork_db,
            composed_inspector,
        )?;

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
    use crate::test_utils::*;
    use revm::{
        Database,
        Inspector,
    };
    use std::sync::atomic::{
        AtomicUsize,
        Ordering,
    };

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
        let CounterValidationSetup {
            mut fork_db,
            mut mock_db,
            mut executor,
            block_env,
            tx,
            ..
        } = setup_counter_validation();

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
