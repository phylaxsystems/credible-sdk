use crate::{
    inspectors::{
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm::{
            LogQuery,
            getLogsQueryCall,
        },
    },
    primitives::{
        Address,
        Bytes,
    },
};

use alloy_primitives::{
    FixedBytes,
    Log,
};
use alloy_sol_types::{
    SolCall,
    SolType,
};
use bumpalo::Bump;

use super::{
    BASE_COST as RESHIRAM_BASE_COST,
    snapshot_logs::{
        ForkWindowError,
        logs_for_fork,
    },
};

const LOG_SCAN_COST: u64 = 1;
const ABI_ENCODE_COST: u64 = 3;

#[derive(Debug, thiserror::Error)]
pub enum GetLogsQueryError {
    #[error("Error decoding getLogsQuery call: {0}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error(transparent)]
    ForkWindow(#[from] ForkWindowError),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

fn log_matches_query(log: &Log, query: &LogQuery) -> bool {
    if query.emitter != Address::ZERO && log.address != query.emitter {
        return false;
    }

    if query.signature != FixedBytes::ZERO {
        return log.topics().first().copied() == Some(query.signature);
    }

    true
}

/// Filter the selected fork window down to matching logs and ABI-encode them as
/// the `PhEvm.Log[]` result returned by the precompile.
fn filter_and_encode_logs(logs: &[Log], query: &LogQuery) -> Bytes {
    crate::arena::with_current_tx_arena(|arena| {
        // Reuse the transaction arena so temporary ABI-shaping allocations stay scoped
        // to the current transaction instead of hitting the general heap on each query.
        let mut sol_logs: Vec<crate::inspectors::sol_primitives::PhEvm::Log, &Bump> =
            Vec::new_in(arena);
        for log in logs {
            if !log_matches_query(log, query) {
                continue;
            }

            // Convert revm/alloy logs into the Solidity-facing PhEvm.Log shape that
            // the precompile returns over the ABI.
            sol_logs.push(crate::inspectors::sol_primitives::PhEvm::Log {
                topics: log.topics().to_vec(),
                data: log.data.data.clone(),
                emitter: log.address,
            });
        }

        // The ABI encoder still produces an owned byte buffer; the arena only covers
        // the intermediate vector used to assemble the filtered result set.
        <alloy_sol_types::sol_data::Array<crate::inspectors::sol_primitives::PhEvm::Log>>::abi_encode(
            sol_logs.as_slice(),
        )
        .into()
    })
}

/// Returns logs matching `query` from the immutable snapshot identified by `fork`.
///
/// # Errors
///
/// Returns an error if the calldata cannot be decoded, the requested fork is invalid, or gas is
/// insufficient.
pub fn get_logs_query(
    context: &PhEvmContext,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, GetLogsQueryError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, RESHIRAM_BASE_COST, gas_limit) {
        return Err(GetLogsQueryError::OutOfGas(rax));
    }

    let call = getLogsQueryCall::abi_decode(input_bytes).map_err(GetLogsQueryError::DecodeError)?;
    let logs = logs_for_fork(
        context.logs_and_traces.tx_logs,
        context.logs_and_traces.call_traces,
        &call.fork,
    )?;

    if let Some(rax) = deduct_gas_and_check(
        &mut gas_left,
        (logs.len() as u64).saturating_mul(LOG_SCAN_COST),
        gas_limit,
    ) {
        return Err(GetLogsQueryError::OutOfGas(rax));
    }

    let encoded = filter_and_encode_logs(logs, &call.query);
    let encoded_words = (encoded.len() as u64).div_ceil(32);
    if let Some(rax) = deduct_gas_and_check(
        &mut gas_left,
        encoded_words.saturating_mul(ABI_ENCODE_COST),
        gas_limit,
    ) {
        return Err(GetLogsQueryError::OutOfGas(rax));
    }

    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::{
            CallTracer,
            LogsAndTraces,
            sol_primitives::PhEvm::{
                self,
                ForkId as SolForkId,
            },
            spec_recorder::AssertionSpec,
        },
        primitives::U256,
        test_utils::{
            random_address,
            random_bytes32,
            run_precompile_test_with_spec,
        },
    };
    use alloy_primitives::{
        Bytes as AlloyBytes,
        LogData,
    };
    use revm::context::journaled_state::JournalCheckpoint;

    const TEST_GAS: u64 = 1_000_000;

    fn make_log(address: Address, topics: Vec<FixedBytes<32>>, data: &[u8]) -> Log {
        Log {
            address,
            data: LogData::new(topics, AlloyBytes::copy_from_slice(data)).unwrap(),
        }
    }

    fn with_logs_context<F, R>(logs: &[Log], call_tracer: &CallTracer, f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let logs_and_traces = LogsAndTraces {
            tx_logs: logs,
            call_traces: call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
            original_tx_env: &tx_env,
            assertion_spec: AssertionSpec::Reshiram,
        };
        f(&context)
    }

    fn encode_call(query: &LogQuery, fork_type: u8, call_index: usize) -> Bytes {
        getLogsQueryCall {
            query: query.clone(),
            fork: SolForkId {
                forkType: fork_type,
                callIndex: U256::from(call_index),
            },
        }
        .abi_encode()
        .into()
    }

    fn decode_logs(output: &Bytes) -> Vec<PhEvm::Log> {
        <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_decode(output).unwrap()
    }

    fn expected_gas(scope: &[Log], query: &LogQuery) -> u64 {
        let encoded = filter_and_encode_logs(scope, query);
        RESHIRAM_BASE_COST
            + scope.len() as u64
            + ABI_ENCODE_COST * (encoded.len() as u64).div_ceil(32)
    }

    #[test]
    fn test_get_logs_query_post_tx_all_logs_when_filters_are_zero() {
        let emitter_a = random_address();
        let emitter_b = random_address();
        let topic_a = random_bytes32();
        let topic_b = random_bytes32();
        let logs = vec![
            make_log(emitter_a, vec![topic_a], &[1u8; 32]),
            make_log(emitter_b, vec![topic_b], &[2u8; 16]),
        ];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 1, 0), TEST_GAS)
        })
        .unwrap();

        let decoded = decode_logs(outcome.bytes());
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].emitter, emitter_a);
        assert_eq!(decoded[1].emitter, emitter_b);
        assert_eq!(outcome.gas(), expected_gas(&logs, &query));
    }

    #[test]
    fn test_get_logs_query_filters_by_emitter_and_signature() {
        let emitter_a = random_address();
        let emitter_b = random_address();
        let target_signature = random_bytes32();
        let other_signature = random_bytes32();
        let logs = vec![
            make_log(emitter_a, vec![target_signature], &[3u8; 32]),
            make_log(emitter_b, vec![target_signature], &[4u8; 32]),
            make_log(emitter_a, vec![other_signature], &[5u8; 32]),
        ];
        let query = LogQuery {
            emitter: emitter_a,
            signature: target_signature,
        };

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 1, 0), TEST_GAS)
        })
        .unwrap();

        let decoded = decode_logs(outcome.bytes());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].emitter, emitter_a);
        assert_eq!(decoded[0].topics[0], target_signature);
        assert_eq!(outcome.gas(), expected_gas(&logs, &query));
    }

    #[test]
    fn test_get_logs_query_pre_tx_returns_empty_array() {
        let logs = vec![make_log(
            random_address(),
            vec![random_bytes32()],
            &[6u8; 32],
        )];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 0, 0), TEST_GAS)
        })
        .unwrap();

        let decoded = decode_logs(outcome.bytes());
        assert!(decoded.is_empty());
        assert_eq!(outcome.gas(), expected_gas(&logs[..0], &query));
    }

    #[test]
    fn test_get_logs_query_respects_call_boundaries() {
        let emitter_a = random_address();
        let emitter_b = random_address();
        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(random_address());
        call_tracer.set_last_call_checkpoints(
            JournalCheckpoint {
                log_i: 1,
                journal_i: 0,
            },
            Some(JournalCheckpoint {
                log_i: 3,
                journal_i: 0,
            }),
        );

        let logs = vec![
            make_log(emitter_a, vec![random_bytes32()], &[7u8; 8]),
            make_log(emitter_b, vec![random_bytes32()], &[8u8; 8]),
            make_log(emitter_a, vec![random_bytes32()], &[9u8; 8]),
            make_log(emitter_b, vec![random_bytes32()], &[10u8; 8]),
        ];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };

        let pre_call = with_logs_context(&logs, &call_tracer, |context| {
            get_logs_query(context, &encode_call(&query, 2, 0), TEST_GAS)
        })
        .unwrap();
        let post_call = with_logs_context(&logs, &call_tracer, |context| {
            get_logs_query(context, &encode_call(&query, 3, 0), TEST_GAS)
        })
        .unwrap();

        assert_eq!(decode_logs(pre_call.bytes()).len(), 1);
        assert_eq!(decode_logs(post_call.bytes()).len(), 3);
        assert_eq!(pre_call.gas(), expected_gas(&logs[..1], &query));
        assert_eq!(post_call.gas(), expected_gas(&logs[..3], &query));
    }

    #[test]
    fn test_get_logs_query_skips_topicless_logs_for_signature_filter() {
        let emitter = random_address();
        let signature = random_bytes32();
        let logs = vec![
            make_log(emitter, vec![], &[11u8; 32]),
            make_log(emitter, vec![signature], &[12u8; 32]),
        ];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature,
        };

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 1, 0), TEST_GAS)
        })
        .unwrap();

        let decoded = decode_logs(outcome.bytes());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].topics[0], signature);
    }

    #[test]
    fn test_get_logs_query_rejects_invalid_fork_type() {
        let logs = vec![make_log(
            random_address(),
            vec![random_bytes32()],
            &[13u8; 32],
        )];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };

        let err = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 9, 0), TEST_GAS)
        })
        .unwrap_err();

        assert!(matches!(
            err,
            GetLogsQueryError::ForkWindow(ForkWindowError::InvalidForkType { fork_type: 9 })
        ));
    }

    #[test]
    fn test_get_logs_query_rejects_reverted_call_snapshot() {
        let logs = vec![make_log(
            random_address(),
            vec![random_bytes32()],
            &[14u8; 32],
        )];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };

        let err = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 3, 0), TEST_GAS)
        })
        .unwrap_err();

        assert!(matches!(
            err,
            GetLogsQueryError::ForkWindow(ForkWindowError::CallInsideRevertedSubtree {
                call_id: 0
            })
        ));
    }

    #[test]
    fn test_get_logs_query_rejects_missing_post_call_checkpoint() {
        let logs = vec![make_log(
            random_address(),
            vec![random_bytes32()],
            &[15u8; 32],
        )];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };
        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(random_address());

        let err = with_logs_context(&logs, &call_tracer, |context| {
            get_logs_query(context, &encode_call(&query, 3, 0), TEST_GAS)
        })
        .unwrap_err();

        assert!(matches!(
            err,
            GetLogsQueryError::ForkWindow(ForkWindowError::PostCallCheckpointMissing {
                call_id: 0
            })
        ));
    }

    #[test]
    fn test_get_logs_query_oog_returns_all_gas() {
        let logs = vec![
            make_log(random_address(), vec![random_bytes32()], &[16u8; 64]),
            make_log(random_address(), vec![random_bytes32()], &[17u8; 64]),
        ];
        let query = LogQuery {
            emitter: Address::ZERO,
            signature: FixedBytes::ZERO,
        };
        let gas_limit = expected_gas(&logs, &query) - 1;

        let err = with_logs_context(&logs, &CallTracer::default(), |context| {
            get_logs_query(context, &encode_call(&query, 1, 0), gas_limit)
        })
        .unwrap_err();

        let GetLogsQueryError::OutOfGas(outcome) = err else {
            panic!("expected out of gas");
        };
        assert_eq!(outcome.gas(), gas_limit);
        assert!(outcome.bytes().is_empty());
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_get_logs_query() {
        let result = run_precompile_test_with_spec("TestGetLogsQuery", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "getLogsQuery should be allowed under Reshiram spec: {result:#?}"
        );
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_get_logs_query() {
        let result = run_precompile_test_with_spec("TestGetLogsQuery", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "getLogsQuery should be forbidden under Legacy spec: {result:#?}"
        );
    }
}
