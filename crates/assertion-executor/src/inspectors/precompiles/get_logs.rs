use crate::{
    inspectors::{
        PhevmOutcome,
        phevm::PhEvmContext,
        precompiles::{
            MAX_ARRAY_RESPONSE_ITEMS,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm,
    },
    primitives::Bytes,
};

use alloy_primitives::Log;
use alloy_sol_types::SolType;
use bumpalo::Bump;

use super::BASE_COST;

#[derive(Debug, thiserror::Error)]
pub enum GetLogsError {
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("getLogs response has {count} logs, exceeds max {max}")]
    TooManyLogs { count: usize, max: usize },
}

const RESULT_ENCODING: u64 = 15;
const LOG_COST_PER_WORD: u64 = 8;
const ABI_ENCODE_COST: u64 = 6;

fn logs_size_bytes(logs: &[Log]) -> u64 {
    logs.iter().fold(0u64, |acc, log| {
        let topics_bytes = (log.data.topics().len() as u64).saturating_mul(32);
        let data_bytes = log.data.data.len() as u64;
        acc.saturating_add(20 + topics_bytes + data_bytes)
    })
}

fn encode_logs(logs: &[Log]) -> Bytes {
    crate::arena::with_current_tx_arena(|arena| {
        let mut sol_logs: Vec<PhEvm::Log, &Bump> = Vec::new_in(arena);
        for log in logs {
            sol_logs.push(PhEvm::Log {
                topics: log.topics().to_vec(),
                data: log.data.data.clone(),
                emitter: log.address,
            });
        }
        <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_encode(sol_logs.as_slice()).into()
    })
}

/// Get the log outputs.
///
/// # Errors
pub fn get_logs(context: &PhEvmContext, gas: u64) -> Result<PhevmOutcome, GetLogsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST + RESULT_ENCODING, gas_limit) {
        return Err(GetLogsError::OutOfGas(rax));
    }

    let logs = context.logs_and_traces.tx_logs;
    if logs.len() > MAX_ARRAY_RESPONSE_ITEMS {
        return Err(GetLogsError::TooManyLogs {
            count: logs.len(),
            max: MAX_ARRAY_RESPONSE_ITEMS,
        });
    }

    let sol_log_words: u64 = logs_size_bytes(logs).div_ceil(32);
    let sol_log_cost = sol_log_words.saturating_mul(LOG_COST_PER_WORD);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, sol_log_cost, gas_limit) {
        return Err(GetLogsError::OutOfGas(rax));
    }

    let encoded = context
        .logs_and_traces
        .call_traces
        .encoded_logs_or_init(|| encode_logs(logs))
        .clone();

    let encoded_words: u64 = (encoded.len() as u64).div_ceil(32);
    let encoded_cost = encoded_words.saturating_mul(ABI_ENCODE_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, encoded_cost, gas_limit) {
        return Err(GetLogsError::OutOfGas(rax));
    }

    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
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
            sol_primitives::PhEvm,
            tracer::CallTracer,
        },
        test_utils::{
            random_address,
            random_bytes,
            random_bytes32,
            run_precompile_test,
        },
    };
    use alloy_primitives::{
        Address,
        Bytes,
        Log,
        LogData,
    };

    fn expected_gas_for_logs(logs: &[Log]) -> u64 {
        const RESULT_ENCODING: u64 = 15;
        const LOG_COST_PER_WORD: u64 = 8;
        const ABI_ENCODE_COST: u64 = 6;

        let mut vec_size_bytes: u64 = 0;
        let sol_logs: Vec<PhEvm::Log> = logs
            .iter()
            .map(|log| {
                let sol_log = PhEvm::Log {
                    topics: log.topics().to_vec(),
                    data: log.data.data.clone(),
                    emitter: log.address,
                };
                vec_size_bytes += 20;
                vec_size_bytes += (sol_log.topics.len() as u64) * 32;
                vec_size_bytes += sol_log.data.len() as u64;
                sol_log
            })
            .collect();

        let encoded: Bytes =
            <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_encode(&sol_logs).into();

        BASE_COST
            + RESULT_ENCODING
            + LOG_COST_PER_WORD * vec_size_bytes.div_ceil(32)
            + ABI_ENCODE_COST * (encoded.len() as u64).div_ceil(32)
    }

    fn with_logs_context<F, R>(logs: &[Log], f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let call_tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: logs,
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
            original_tx_env: &tx_env,
            trigger_call_id: None,
        };
        f(&context)
    }

    #[test]
    fn test_get_logs_empty() {
        let result = with_logs_context(&[], |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();
        assert!(!encoded.is_empty());

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 0);
    }

    #[test]
    fn test_get_logs_gas_empty() {
        let logs = vec![];
        let expected_gas = expected_gas_for_logs(&logs);

        let outcome = with_logs_context(&logs, |context| get_logs(context, u64::MAX)).unwrap();

        assert_eq!(outcome.gas(), expected_gas);
    }

    #[test]
    fn test_get_logs_gas_single_log_rounding() {
        let address = random_address();
        let topic = random_bytes32();
        let data = random_bytes::<1>();

        let log = Log {
            address,
            data: LogData::new(vec![topic], Bytes::from(data)).unwrap(),
        };

        let expected_gas = expected_gas_for_logs(std::slice::from_ref(&log));
        let outcome = with_logs_context(&[log], |context| get_logs(context, u64::MAX)).unwrap();

        assert_eq!(outcome.gas(), expected_gas);
    }

    #[test]
    fn test_get_logs_gas_out_of_gas_returns_all_gas() {
        let address = random_address();
        let topic = random_bytes32();
        let data = random_bytes::<33>();

        let log = Log {
            address,
            data: LogData::new(vec![topic], Bytes::from(data)).unwrap(),
        };

        let expected_gas = expected_gas_for_logs(std::slice::from_ref(&log));
        let gas_limit = expected_gas - 1;

        let err = with_logs_context(&[log], |context| get_logs(context, gas_limit))
            .expect_err("expected OOG");
        match err {
            GetLogsError::OutOfGas(outcome) => {
                assert_eq!(outcome.gas(), gas_limit);
                assert!(outcome.bytes().is_empty());
            }
            other @ GetLogsError::TooManyLogs { .. } => {
                panic!("expected OutOfGas, got {other:?}");
            }
        }
    }

    #[test]
    fn test_get_logs_single_log() {
        let address = random_address();
        let topic = random_bytes32();
        let data = random_bytes::<64>();

        let log = Log {
            address,
            data: LogData::new(vec![topic], Bytes::from(data)).unwrap(),
        };

        let result = with_logs_context(std::slice::from_ref(&log), |context| {
            get_logs(context, u64::MAX)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        let decoded_log = &decoded_array[0];
        assert_eq!(decoded_log.emitter, address);
        assert_eq!(decoded_log.topics.len(), 1);
        assert_eq!(decoded_log.topics[0], topic);
        assert_eq!(decoded_log.data, Bytes::from(data));
    }

    #[test]
    fn test_get_logs_multiple_logs() {
        let address1 = random_address();
        let address2 = random_address();
        let topic1 = random_bytes32();
        let topic2 = random_bytes32();
        let data1 = random_bytes::<32>();
        let data2 = random_bytes::<64>();

        let logs = vec![
            Log {
                address: address1,
                data: LogData::new(vec![topic1], Bytes::from(data1)).unwrap(),
            },
            Log {
                address: address2,
                data: LogData::new(vec![topic2], Bytes::from(data2)).unwrap(),
            },
        ];

        let result = with_logs_context(&logs, |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 2);

        // Verify first log
        let decoded_log1 = &decoded_array[0];
        assert_eq!(decoded_log1.emitter, address1);
        assert_eq!(decoded_log1.topics.len(), 1);
        assert_eq!(decoded_log1.topics[0], topic1);
        assert_eq!(decoded_log1.data, Bytes::from(data1));

        // Verify second log
        let decoded_log2 = &decoded_array[1];
        assert_eq!(decoded_log2.emitter, address2);
        assert_eq!(decoded_log2.topics.len(), 1);
        assert_eq!(decoded_log2.topics[0], topic2);
        assert_eq!(decoded_log2.data, Bytes::from(data2));
    }

    #[test]
    fn test_get_logs_multiple_topics() {
        let address = random_address();
        let topic1 = random_bytes32();
        let topic2 = random_bytes32();
        let topic3 = random_bytes32();
        let data = random_bytes::<128>();

        let log = Log {
            address,
            data: LogData::new(vec![topic1, topic2, topic3], Bytes::from(data)).unwrap(),
        };

        let result = with_logs_context(&[log], |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        let decoded_log = &decoded_array[0];
        assert_eq!(decoded_log.emitter, address);
        assert_eq!(decoded_log.topics.len(), 3);
        assert_eq!(decoded_log.topics[0], topic1);
        assert_eq!(decoded_log.topics[1], topic2);
        assert_eq!(decoded_log.topics[2], topic3);
        assert_eq!(decoded_log.data, Bytes::from(data));
    }

    #[test]
    fn test_get_logs_no_topics() {
        let address = random_address();
        let data = random_bytes::<32>();

        let log = Log {
            address,
            data: LogData::new(vec![], Bytes::from(data)).unwrap(),
        };

        let result = with_logs_context(&[log], |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        let decoded_log = &decoded_array[0];
        assert_eq!(decoded_log.emitter, address);
        assert_eq!(decoded_log.topics.len(), 0);
        assert_eq!(decoded_log.data, Bytes::from(data));
    }

    #[test]
    fn test_get_logs_empty_data() {
        let address = random_address();
        let topic = random_bytes32();

        let log = Log {
            address,
            data: LogData::new(vec![topic], Bytes::new()).unwrap(),
        };

        let result = with_logs_context(&[log], |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        let decoded_log = &decoded_array[0];
        assert_eq!(decoded_log.emitter, address);
        assert_eq!(decoded_log.topics.len(), 1);
        assert_eq!(decoded_log.topics[0], topic);
        assert_eq!(decoded_log.data, Bytes::new());
    }

    #[test]
    fn test_get_logs_large_data() {
        let address = random_address();
        let topic = random_bytes32();
        let large_data = random_bytes::<1024>(); // 1KB of data

        let log = Log {
            address,
            data: LogData::new(vec![topic], Bytes::from(large_data)).unwrap(),
        };

        let result = with_logs_context(&[log], |context| get_logs(context, u64::MAX));
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        let decoded_log = &decoded_array[0];
        assert_eq!(decoded_log.emitter, address);
        assert_eq!(decoded_log.data, Bytes::from(large_data));
    }

    #[test]
    fn test_get_logs_common_cases_succeed() {
        let test_cases = vec![
            vec![],
            vec![Log {
                address: Address::ZERO,
                data: LogData::new(vec![], Bytes::new()).unwrap(),
            }],
        ];

        for logs in test_cases {
            let result = with_logs_context(&logs, |context| get_logs(context, u64::MAX));
            assert!(result.is_ok(), "get_logs should never fail");
        }
    }

    #[test]
    fn test_get_logs_errors_when_response_exceeds_bound() {
        let logs = vec![
            Log {
                address: Address::ZERO,
                data: LogData::new(vec![], Bytes::new()).unwrap(),
            };
            MAX_ARRAY_RESPONSE_ITEMS + 1
        ];

        let err = with_logs_context(&logs, |context| get_logs(context, u64::MAX))
            .expect_err("expected TooManyLogs");
        match err {
            GetLogsError::TooManyLogs { count, max } => {
                assert_eq!(count, MAX_ARRAY_RESPONSE_ITEMS + 1);
                assert_eq!(max, MAX_ARRAY_RESPONSE_ITEMS);
            }
            other @ GetLogsError::OutOfGas(_) => {
                panic!("expected TooManyLogs, got {other:?}");
            }
        }
    }

    #[test]
    fn test_get_logs_populates_tracer_encoding_cache() {
        let log = Log {
            address: Address::ZERO,
            data: LogData::new(vec![random_bytes32()], Bytes::from(random_bytes::<16>())).unwrap(),
        };
        let logs = [log];

        let call_tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = PhEvmContext::new(&logs_and_traces, Address::ZERO, &tx_env);

        assert!(!call_tracer.has_encoded_logs_cache());
        let _ = get_logs(&context, u64::MAX).expect("first getLogs should succeed");
        assert!(call_tracer.has_encoded_logs_cache());
        let _ = get_logs(&context, u64::MAX).expect("cached getLogs should succeed");
        assert!(call_tracer.has_encoded_logs_cache());
    }

    #[tokio::test]
    async fn test_get_logs_integration() {
        let result = run_precompile_test("TestGetLogs");
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
