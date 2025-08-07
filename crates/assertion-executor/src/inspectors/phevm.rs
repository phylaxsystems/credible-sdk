use crate::{
    db::{
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
        AccountInfo,
        Address,
        Bytecode,
        Bytes,
        FixedBytes,
        Journal,
        SpecId,
        U256,
        address,
        bytes,
    },
};

use op_revm::OpContext;
use revm::{
    Inspector,
    JournalEntry,
    context::{
        ContextTr,
        JournalInner,
    },
    interpreter::{
        CallInputs,
        CallOutcome,
        Gas,
    },
    primitives::Log,
};

use alloy_evm::eth::EthEvmContext;
use alloy_sol_types::SolCall;

/// Precompile address
/// address(uint160(uint256(keccak256("Kim Jong Un Sucks"))))
pub const PRECOMPILE_ADDRESS: Address = address!("4461812e00718ff8D80929E3bF595AEaaa7b881E");

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
    init_journal: JournalInner<JournalEntry>,
    pub context: PhEvmContext<'a>,
}

impl<'a> PhEvmInspector<'a> {
    /// Create a new PhEvmInspector.
    pub fn new(
        spec_id: SpecId,
        journal: &mut JournalInner<JournalEntry>,
        context: PhEvmContext<'a>,
    ) -> Self {
        insert_precompile_account(journal);

        let mut init_journal = JournalInner::new();
        init_journal.set_spec_id(spec_id);
        PhEvmInspector {
            init_journal,
            context,
        }
    }

    /// Execute precompile functions for the PhEvm.
    pub fn execute_precompile<'db, ExtDb: DatabaseRef + 'db, CTX>(
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
                fork_pre_tx(
                    &self.init_journal,
                    context,
                    self.context.logs_and_traces.call_traces,
                )?
            }
            PhEvm::forkPostTxCall::SELECTOR => {
                fork_post_tx(
                    &self.init_journal,
                    context,
                    self.context.logs_and_traces.call_traces,
                )?
            }
            PhEvm::forkPreCallCall::SELECTOR => {
                fork_pre_call(
                    &self.init_journal,
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                )?
            }
            PhEvm::forkPostCallCall::SELECTOR => {
                fork_post_call(
                    &self.init_journal,
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                )?
            }
            PhEvm::loadCall::SELECTOR => load_external_slot(context, inputs)?,
            PhEvm::getLogsCall::SELECTOR => get_logs(&self.context)?,
            PhEvm::getCallInputsCall::SELECTOR => get_call_inputs(inputs, context, &self.context)?,
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

/// Insert the precompile account into the database.
fn insert_precompile_account(journal: &mut JournalInner<JournalEntry>) {
    let precompile_account = AccountInfo {
        nonce: 1,
        balance: U256::MAX,
        code: Some(Bytecode::new_raw(bytes!("DEAD"))),
        //Code needed to hit 'call(..)' fn of the inspector trait
        ..Default::default()
    };

    journal
        .state
        .insert(PRECOMPILE_ADDRESS, precompile_account.into());
}

/// Macro to implement Inspector trait for multiple context types.
/// This avoids duplicating the implementation and provides better maintainability.
macro_rules! impl_phevm_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<ExtDb: DatabaseRef> Inspector<$context_type> for PhEvmInspector<'_> {
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
