use crate::{
    inspectors::{
        phevm::PhEvmContext,
        sol_primitives::Console::logCall,
    },
    primitives::Bytes,
};

use alloy_sol_types::SolCall;

#[derive(Debug, thiserror::Error)]
pub struct ConsoleLogError(#[source] alloy_sol_types::Error);

impl std::fmt::Display for ConsoleLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Inserts console log into [`PhEvmContext`].
///
/// # Errors
///
/// Returns an error if the log input cannot be decoded or run out of gas
pub fn console_log(
    input_bytes: &Bytes,
    context: &mut PhEvmContext,
) -> Result<Bytes, ConsoleLogError> {
    context.console_logs.push(
        logCall::abi_decode(input_bytes)
            .map_err(ConsoleLogError)?
            .message,
    );
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
        let call_tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let adopter = random_address();
        let tx_env = crate::primitives::TxEnv::default();

        let mut context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: &tx_env,
            assertion_spec: crate::inspectors::spec_recorder::AssertionSpec::Legacy,
            ofac_sanctions: std::sync::Arc::new(std::collections::HashSet::new()),
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
        let call_tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let adopter = random_address();
        let tx_env = crate::primitives::TxEnv::default();

        let mut context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: &tx_env,
            assertion_spec: crate::inspectors::spec_recorder::AssertionSpec::Legacy,
            ofac_sanctions: std::sync::Arc::new(std::collections::HashSet::new()),
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
