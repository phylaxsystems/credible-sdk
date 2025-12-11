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
    available_gas: u64,
    memory_offset: Range<usize>,
) -> CallOutcome {
    match result {
        Ok(output) => {
            let gas_remaining = available_gas.saturating_sub(output.gas());
            CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Return,
                    output: output.into_bytes(),
                    gas: Gas::new(gas_remaining),
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
                    gas: Gas::new(available_gas),
                },
                memory_offset,
                was_precompile_called: false,
                precompile_call_logs: vec![],
            }
        }
    }
}
