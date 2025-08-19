use crate::{
    constants::PRECOMPILE_ADDRESS,
    db::{
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::MultiForkDb,
    },
    inspectors::{
        inspector_result_to_call_outcome,
        precompiles::{
            assertion_adopter::get_assertion_adopter,
            calls::{
                GetCallInputsError,
                get_call_inputs,
            },
            console_log::ConsoleLogError,
            fork::{
                ForkError,
                fork_post_call,
                fork_post_tx,
                fork_pre_call,
                fork_pre_tx,
            },
            get_logs::get_logs,
            load::{
                LoadExternalSlotError,
                load_external_slot,
            },
            state_changes::{
                GetStateChangesError,
                get_state_changes,
            },
        },
        sol_primitives::{
            PhEvm,
            console,
        },
        tracer::CallTracer,
    },
    primitives::{
        Address,
        Bytes,
        FixedBytes,
        Journal,
        JournalEntry,
        JournalInner,
    },
};

use op_revm::OpContext;
use revm::{
    Inspector,
    context::ContextTr,
    interpreter::{
        CallInputs,
        CallOutcome,
        CallScheme,
        Gas,
    },
    primitives::Log,
};

use alloy_evm::eth::EthEvmContext;
use alloy_sol_types::SolCall;

#[derive(Debug, Clone)]
pub struct LogsAndTraces<'a> {
    pub tx_logs: &'a [Log],
    pub call_traces: &'a CallTracer,
}

#[derive(Debug, Clone)]
pub struct PhEvmContext<'a> {
    pub logs_and_traces: &'a LogsAndTraces<'a>,
    pub adopter: Address,
    pub console_logs: Vec<String>,
}

impl<'a> PhEvmContext<'a> {
    pub fn new(logs_and_traces: &'a LogsAndTraces<'a>, adopter: Address) -> Self {
        Self {
            logs_and_traces,
            adopter,
            console_logs: vec![],
        }
    }

    #[inline]
    pub fn post_tx_journal(&self) -> &JournalInner<JournalEntry> {
        &self.logs_and_traces.call_traces.journal
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrecompileError<ExtDb: DatabaseRef> {
    #[error("Precompile selector not found: {0:#?}")]
    SelectorNotFound(FixedBytes<4>),
    #[error("Unexpected error, should be Infallible: {0}")]
    UnexpectedError(#[from] std::convert::Infallible),
    #[error("Error getting state changes: {0}")]
    GetStateChangesError(#[from] GetStateChangesError),
    #[error("Error getting call inputs: {0}")]
    GetCallInputsError(#[from] GetCallInputsError),
    #[error("Error switching forks: {0}")]
    ForkError(#[from] ForkError),
    #[error("Error logging to console: {0}")]
    ConsoleLogError(#[from] ConsoleLogError),
    #[error("Error loading external slot: {0}")]
    LoadExternalSlotError(#[from] LoadExternalSlotError<ExtDb>),
}

/// PhEvmInspector is an inspector for supporting the PhEvm precompiles.
#[derive(Debug, Clone)]
pub struct PhEvmInspector<'a> {
    pub context: PhEvmContext<'a>,
}

impl<'a> PhEvmInspector<'a> {
    /// Create a new PhEvmInspector.
    pub fn new(context: PhEvmContext<'a>) -> Self {
        PhEvmInspector { context }
    }

    /// Execute precompile functions for the PhEvm.
    pub fn execute_precompile<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
        &mut self,
        context: &mut CTX,
        inputs: &mut CallInputs,
    ) -> Result<Bytes, PrecompileError<ExtDb>>
    where
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        let input_bytes = inputs.input.bytes(context);
        let result = match input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default()
        {
            PhEvm::forkPreTxCall::SELECTOR => {
                fork_pre_tx(context, self.context.logs_and_traces.call_traces)?
            }
            PhEvm::forkPostTxCall::SELECTOR => {
                fork_post_tx(context, self.context.logs_and_traces.call_traces)?
            }
            PhEvm::forkPreCallCall::SELECTOR => {
                fork_pre_call(
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                )?
            }
            PhEvm::forkPostCallCall::SELECTOR => {
                fork_post_call(
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                )?
            }
            PhEvm::loadCall::SELECTOR => load_external_slot(context, inputs)?,
            PhEvm::getLogsCall::SELECTOR => get_logs(&self.context)?,
            PhEvm::getAllCallInputsCall::SELECTOR => {
                let inputs = PhEvm::getAllCallInputsCall::abi_decode(&inputs.input.bytes(context))
                    .map_err(|err| PrecompileError::GetCallInputsError(err.into()))?;
                get_call_inputs(&self.context, inputs.target, inputs.selector, None)
                    .map_err(|err| PrecompileError::GetCallInputsError(err))?
            }
            PhEvm::getCallInputsCall::SELECTOR => {
                let inputs = PhEvm::getCallInputsCall::abi_decode(&inputs.input.bytes(context))
                    .map_err(|err| PrecompileError::GetCallInputsError(err.into()))?;
                get_call_inputs(
                    &self.context,
                    inputs.target,
                    inputs.selector,
                    Some(CallScheme::Call),
                )
                .map_err(|err| PrecompileError::GetCallInputsError(err))?
            }
            PhEvm::getStaticCallInputsCall::SELECTOR => {
                let inputs =
                    PhEvm::getStaticCallInputsCall::abi_decode(&inputs.input.bytes(context))
                        .map_err(|err| PrecompileError::GetCallInputsError(err.into()))?;
                get_call_inputs(
                    &self.context,
                    inputs.target,
                    inputs.selector,
                    Some(CallScheme::StaticCall),
                )
                .map_err(|err| PrecompileError::GetCallInputsError(err))?
            }
            PhEvm::getDelegateCallInputsCall::SELECTOR => {
                let inputs =
                    PhEvm::getDelegateCallInputsCall::abi_decode(&inputs.input.bytes(context))
                        .map_err(|err| PrecompileError::GetCallInputsError(err.into()))?;
                get_call_inputs(
                    &self.context,
                    inputs.target,
                    inputs.selector,
                    Some(CallScheme::DelegateCall),
                )
                .map_err(|err| PrecompileError::GetCallInputsError(err))?
            }
            PhEvm::getCallCodeInputsCall::SELECTOR => {
                let inputs = PhEvm::getCallCodeInputsCall::abi_decode(&inputs.input.bytes(context))
                    .map_err(|err| PrecompileError::GetCallInputsError(err.into()))?;
                get_call_inputs(
                    &self.context,
                    inputs.target,
                    inputs.selector,
                    Some(CallScheme::CallCode),
                )
                .map_err(|err| PrecompileError::GetCallInputsError(err))?
            }
            PhEvm::getStateChangesCall::SELECTOR => get_state_changes(&input_bytes, &self.context)?,
            PhEvm::getAssertionAdopterCall::SELECTOR => get_assertion_adopter(&self.context)?,
            console::logCall::SELECTOR => {
                #[cfg(feature = "phoundry")]
                return Ok(crate::inspectors::precompiles::console_log::console_log(
                    &input_bytes,
                    &mut self.context,
                )?);

                #[cfg(not(feature = "phoundry"))]
                return Ok(Default::default());
            }
            selector => Err(PrecompileError::SelectorNotFound(selector.into()))?,
        };

        Ok(result)
    }
}

/// Macro to implement Inspector trait for multiple context types.
/// This avoids duplicating the implementation and provides better maintainability.
macro_rules! impl_phevm_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<ExtDb: DatabaseRef + Clone + DatabaseCommit> Inspector<$context_type> for PhEvmInspector<'_> {
                fn call(&mut self, context: &mut $context_type, inputs: &mut CallInputs) -> Option<CallOutcome> {
                    if inputs.target_address == PRECOMPILE_ADDRESS {
                        let call_outcome = inspector_result_to_call_outcome(
                            self.execute_precompile(context, inputs),
                            Gas::new(inputs.gas_limit),
                            inputs.return_memory_offset.clone(),
                        );

                        return Some(call_outcome);
                    }
                    None
                }
            }
        )*
    };
}

// Implement Inspector for both context types using the macro
impl_phevm_inspector!(
    EthEvmContext<&mut MultiForkDb<ExtDb>>,
    OpContext<&mut MultiForkDb<ExtDb>>,
);

#[cfg(test)]
mod test {
    use crate::test_utils::run_precompile_test;

    use crate::inspectors::sol_primitives::Error;
    use alloy_sol_types::SolError;

    #[tokio::test]
    async fn test_invalid_selector_error_encoding() {
        let result = run_precompile_test("TestInvalidCall").await;
        assert!(!result.is_valid());
        assert_eq!(result.assertions_executions.len(), 1);
        let assertion_contract_execution = &result.assertions_executions[0];
        assert_eq!(assertion_contract_execution.assertion_fns_results.len(), 1);
        let assertion_fn_result = &assertion_contract_execution.assertion_fns_results[0];

        match &assertion_fn_result.result {
            crate::primitives::AssertionFunctionExecutionResult::AssertionExecutionResult(
                result,
            ) => {
                assert!(!result.is_success());
                let data = result.clone().into_output().unwrap();

                let error_string =
                    Error::abi_decode(data.iter().as_slice()).expect("Failed to decode error");

                assert_eq!(error_string.0, "Precompile selector not found: 0x1dcc85ae");
            }
            _ => {
                panic!("Expected AssertionExecutionResult(_), got: {assertion_fn_result:#?}");
            }
        }
    }
}
