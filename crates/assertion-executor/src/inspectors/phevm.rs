use crate::{
    db::PhDB,
    db::multi_fork_db::{
        ForkError,
        MultiForkDb,
    },
    inspectors::{
        inspector_result_to_call_outcome,
        precompiles::{
            assertion_adopter::get_assertion_adopter,
            calls::{
                GetCallInputsError,
                get_call_inputs,
            },
            fork::{
                fork_post_state,
                fork_pre_state,
            },
            load::load_external_slot,
            logs::get_logs,
            state_changes::{
                GetStateChangesError,
                get_state_changes,
            },
        },
        sol_primitives::PhEvm,
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
        CreateInputs,
        CreateOutcome,
        Gas,
        Interpreter,
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

#[derive(Debug)]
pub struct PhEvmContext<'a> {
    pub logs_and_traces: &'a LogsAndTraces<'a>,
    pub adopter: Address,
}

impl<'a> PhEvmContext<'a> {
    pub fn new(logs_and_traces: &'a LogsAndTraces<'a>, adopter: Address) -> Self {
        Self {
            logs_and_traces,
            adopter,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrecompileError {
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
}

/// PhEvmInspector is an inspector for supporting the PhEvm precompiles.
pub struct PhEvmInspector<'a> {
    init_journal: JournalInner<JournalEntry>,
    context: &'a PhEvmContext<'a>,
}

impl<'a> PhEvmInspector<'a> {
    /// Create a new PhEvmInspector.
    pub fn new<ExtDb: PhDB>(
        spec_id: SpecId,
        db: &mut MultiForkDb<ExtDb>,
        context: &'a PhEvmContext<'a>,
    ) -> Self {
        insert_precompile_account(db);

        let mut init_journal = JournalInner::new();
        init_journal.set_spec_id(spec_id);
        PhEvmInspector {
            init_journal,
            context,
        }
    }

    /// Execute precompile functions for the PhEvm.
    pub fn execute_precompile<'db, ExtDb: PhDB + 'db, CTX>(
        &self,
        context: &mut CTX,
        inputs: &mut CallInputs,
    ) -> Result<Bytes, PrecompileError>
    where
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        println!("Executing precompile");
        let input_bytes = inputs.input.bytes(context);
        let result = match input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default()
        {
            PhEvm::forkPreStateCall::SELECTOR => fork_pre_state(&self.init_journal, context)?,
            PhEvm::forkPostStateCall::SELECTOR => fork_post_state(&self.init_journal, context)?,
            PhEvm::loadCall::SELECTOR => load_external_slot(context, inputs)?,
            PhEvm::getLogsCall::SELECTOR => get_logs(self.context)?,
            PhEvm::getCallInputsCall::SELECTOR => get_call_inputs(inputs, context, self.context)?,
            PhEvm::getStateChangesCall::SELECTOR => get_state_changes(&input_bytes, self.context)?,
            PhEvm::getAssertionAdopterCall::SELECTOR => get_assertion_adopter(self.context)?,
            selector => Err(PrecompileError::SelectorNotFound(selector.into()))?,
        };

        Ok(result)
    }
}

/// Insert the precompile account into the database.
fn insert_precompile_account<DB: PhDB>(db: &mut MultiForkDb<DB>) {
    let precompile_account = AccountInfo {
        nonce: 1,
        balance: U256::MAX,
        code: Some(Bytecode::new_raw(bytes!("DEAD"))),
        ..Default::default()
    };

    db.active_db
        .insert_account_info(PRECOMPILE_ADDRESS, precompile_account);
    db.active_db
        .storage
        .entry(PRECOMPILE_ADDRESS)
        .or_default()
        .dont_read_from_inner_db = true;
}

/// Macro to implement Inspector trait for multiple context types.
/// This avoids duplicating the implementation and provides better maintainability.
macro_rules! impl_phevm_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<ExtDb: PhDB> Inspector<$context_type> for PhEvmInspector<'_> {
                fn initialize_interp(&mut self, _interp: &mut Interpreter, _context: &mut $context_type) {}
                fn step(&mut self, _interp: &mut Interpreter, _context: &mut $context_type) {}
                fn step_end(&mut self, _interp: &mut Interpreter, _context: &mut $context_type) {}
                fn call_end(&mut self, _context: &mut $context_type, _inputs: &CallInputs, _outcome: &mut CallOutcome) {}
                fn create_end(&mut self, _context: &mut $context_type, _inputs: &CreateInputs, _outcome: &mut CreateOutcome) {}
                fn selfdestruct(&mut self, _contract: Address, _target: Address, _value: U256) {}

                fn call(&mut self, context: &mut $context_type, inputs: &mut CallInputs) -> Option<CallOutcome> {
                    if inputs.target_address == PRECOMPILE_ADDRESS {
                        let call_outcome = inspector_result_to_call_outcome(
                            self.execute_precompile(context, inputs),
                            Gas::new(inputs.gas_limit),
                            inputs.return_memory_offset.clone(),
                        );

                        if call_outcome.result.is_revert() {
                            println!("PhEvm precompile revert: {:#?}", call_outcome.result.output);
                        }
                        return Some(call_outcome);
                    }
                    None
                }

                fn create(&mut self, _context: &mut $context_type, _inputs: &mut CreateInputs) -> Option<CreateOutcome> {
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
