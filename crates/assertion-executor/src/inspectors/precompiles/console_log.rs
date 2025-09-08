use crate::{
    inspectors::{
        phevm::PhEvmContext,
        sol_primitives::Console::logCall,
    },
    primitives::Bytes,
};

use alloy_sol_types::SolCall;

#[derive(Debug, thiserror::Error)]
pub struct ConsoleLogError(#[from] alloy_sol_types::Error);

impl std::fmt::Display for ConsoleLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Inserts console log into [`PhEvmContext`]
pub fn console_log(
    input_bytes: &Bytes,
    context: &mut PhEvmContext,
) -> Result<Bytes, ConsoleLogError> {
    context
        .console_logs
        .push(logCall::abi_decode(input_bytes)?.message);
    Ok(Bytes::default())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::{
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        test_utils::random_address,
    };

    fn test_logging(input_bytes: &Bytes) -> Result<Bytes, ConsoleLogError> {
        let call_tracer = CallTracer::new();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let adopter = random_address();

        let mut context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter,
            console_logs: vec![],
        };
        console_log(input_bytes, &mut context)
    }
    #[test]
    fn test_decode_failure() {
        let result = test_logging(&Bytes::from("DEAD"));
        assert!(matches!(result, Err(ConsoleLogError(_))));
    }

    #[test]
    fn test_logging_success() {
        let call_tracer = CallTracer::new();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let adopter = random_address();

        let mut context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter,
            console_logs: vec![],
        };
        let result = console_log(
            &logCall {
                message: "Hello, world!".to_string(),
            }
            .abi_encode()
            .into(),
            &mut context,
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::new());
        assert_eq!(context.console_logs, vec!["Hello, world!"]);
    }
}
