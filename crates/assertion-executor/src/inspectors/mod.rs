mod phevm;
pub mod precompiles;
pub mod sol_primitives;
mod tracer;
mod trigger_recorder;

pub use phevm::{
    LogsAndTraces,
    PhEvmContext,
    PhEvmInspector,
};

pub use tracer::{
    CallTracer,
    CallTracerError,
};
pub use trigger_recorder::{
    TRIGGER_RECORDER,
    TriggerRecorder,
    TriggerType,
    insert_trigger_recorder_account,
};

use sol_primitives::Error;

pub use revm::inspector::NoOpInspector;

use revm::interpreter::{
    CallOutcome,
    Gas,
    InstructionResult,
    InterpreterResult,
};

use alloy_sol_types::SolError;

use std::ops::Range;

use crate::inspectors::phevm::PhevmOutcome;

/// Convert a result to a call outcome.
/// Uses the default require [`Error`] signature for encoding revert messages.
pub fn inspector_result_to_call_outcome<E: std::fmt::Display>(
    result: Result<PhevmOutcome, E>,
    memory_offset: Range<usize>,
) -> CallOutcome {
    match result {
        Ok(output) => {
            let (output, gas_used) = output.into_parts();
            CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Return,
                    output,
                    gas: Gas::new(gas_used),
                },
                memory_offset,
                was_precompile_called: false,
                precompile_call_logs: vec![],
            }
        }
        Err(e) => {
            CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Revert,
                    output: Error::abi_encode(&Error(e.to_string())).into(),
                    gas: Gas::new(0),
                },
                memory_offset,
                was_precompile_called: false,
                precompile_call_logs: vec![],
            }
        }
    }
}
