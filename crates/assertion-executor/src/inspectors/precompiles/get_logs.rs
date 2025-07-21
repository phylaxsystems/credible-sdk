use crate::{
    inspectors::phevm::PhEvmContext,
    inspectors::sol_primitives::PhEvm,
    primitives::Bytes,
};

use alloy_sol_types::SolType;
use std::convert::Infallible;

/// Get the log outputs.
pub fn get_logs(context: &PhEvmContext) -> Result<Bytes, Infallible> {
    let sol_logs: Vec<PhEvm::Log> = context
        .logs_and_traces
        .tx_logs
        .iter()
        .map(|log| {
            PhEvm::Log {
                topics: log.topics().to_vec(),
                data: log.data.data.clone(),
                emitter: log.address,
            }
        })
        .collect();

    let encoded: Bytes =
        <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_encode(&sol_logs).into();

    Ok(encoded)
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

    fn with_logs_context<F, R>(logs: Vec<Log>, f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let call_tracer = CallTracer::new();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
        };
        f(&context)
    }

    #[test]
    fn test_get_logs_empty() {
        let result = with_logs_context(vec![], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();
        assert!(!encoded.is_empty());

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 0);
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

        let result = with_logs_context(vec![log.clone()], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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

        let result = with_logs_context(logs, get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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

        let result = with_logs_context(vec![log], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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

        let result = with_logs_context(vec![log], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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

        let result = with_logs_context(vec![log], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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

        let result = with_logs_context(vec![log], get_logs);
        assert!(result.is_ok());

        let encoded = result.unwrap();

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
    fn test_get_logs_never_fails() {
        // The function signature indicates it returns Result<Bytes, Infallible>
        // This means it should never fail, so let's verify that with edge cases

        let test_cases = vec![
            vec![],
            vec![Log {
                address: Address::ZERO,
                data: LogData::new(vec![], Bytes::new()).unwrap(),
            }],
        ];

        for logs in test_cases {
            let result = with_logs_context(logs, get_logs);
            assert!(result.is_ok(), "get_logs should never fail");
        }
    }

    #[tokio::test]
    async fn test_get_logs_integration() {
        let result = run_precompile_test("TestGetLogs").await;
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
