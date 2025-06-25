mod phevm;
pub mod precompiles;
pub mod sol_primitives;
mod tracer;
mod trigger_recorder;

pub use phevm::{
    LogsAndTraces,
    PhEvmContext,
    PhEvmInspector,
    PRECOMPILE_ADDRESS,
};

pub use tracer::CallTracer;
pub use trigger_recorder::{
    insert_trigger_recorder_account,
    TriggerRecorder,
    TriggerType,
};

use sol_primitives::Error;

use crate::primitives::Bytes;

use revm::interpreter::{
    CallOutcome,
    Gas,
    InstructionResult,
    InterpreterResult,
};

use alloy_sol_types::SolError;

use std::ops::Range;

/// Convert a result to a call outcome.
/// Uses the default require [`Error`] signature for encoding revert messages.
fn inspector_result_to_call_outcome<E: std::fmt::Display>(
    result: Result<Bytes, E>,
    gas: Gas,
    memory_offset: Range<usize>,
) -> CallOutcome {
    match result {
        Ok(output) => {
            CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Return,
                    output,
                    gas,
                },
                memory_offset,
            }
        }
        Err(e) => {
            CallOutcome {
                result: InterpreterResult {
                    result: InstructionResult::Revert,
                    output: Error::abi_encode(&Error { _0: e.to_string() }).into(),
                    gas,
                },
                memory_offset,
            }
        }
    }
}
