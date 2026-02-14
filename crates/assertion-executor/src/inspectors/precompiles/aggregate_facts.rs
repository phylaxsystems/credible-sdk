#![allow(clippy::missing_errors_doc)]

use crate::inspectors::{
    phevm::{
        PhEvmContext,
        PhevmOutcome,
    },
    precompiles::{
        BASE_COST,
        MAX_ARRAY_RESPONSE_ITEMS,
        deduct_gas_and_check,
    },
    sol_primitives::PhEvm,
};

use alloy_primitives::{
    Address,
    FixedBytes,
    U256,
};
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use std::collections::{
    BTreeMap,
    BTreeSet,
    btree_map::Entry,
};

use super::call_facts::{
    call_matches_filter,
    call_type_to_scheme,
    candidate_call_indices,
};

#[derive(thiserror::Error, Debug)]
pub enum AggregateFactsError {
    #[error("Failed to decode aggregate facts input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("argIndex {arg_index} cannot be represented as usize")]
    InvalidArgIndex { arg_index: U256 },
    #[error("argIndex {arg_index} overflows offset computation")]
    ArgOffsetOverflow { arg_index: usize },
    #[error("topicIndex {topic_index} must be in [0,3]")]
    InvalidTopicIndex { topic_index: u8 },
    #[error("response has {count} unique keys, exceeds max {max}")]
    TooManyResults { count: usize, max: usize },
}

const PER_CALL_COST: u64 = 5;
const PER_LOG_COST: u64 = 3;

fn arg_start_offset(arg_index: U256, selector_prefix: usize) -> Result<usize, AggregateFactsError> {
    let arg_index_usize: usize = arg_index
        .try_into()
        .map_err(|_| AggregateFactsError::InvalidArgIndex { arg_index })?;
    let byte_offset =
        arg_index_usize
            .checked_mul(32)
            .ok_or(AggregateFactsError::ArgOffsetOverflow {
                arg_index: arg_index_usize,
            })?;
    selector_prefix
        .checked_add(byte_offset)
        .ok_or(AggregateFactsError::ArgOffsetOverflow {
            arg_index: arg_index_usize,
        })
}

fn read_zero_extended_word(data: &[u8], start: usize) -> [u8; 32] {
    let mut word = [0u8; 32];
    if start >= data.len() {
        return word;
    }
    let available = (data.len() - start).min(32);
    word[..available].copy_from_slice(&data[start..start + available]);
    word
}

fn read_call_arg_word(
    record: &crate::inspectors::tracer::CallRecord,
    start: usize,
) -> Option<[u8; 32]> {
    let revm::interpreter::CallInput::Bytes(calldata) = &record.inputs().input else {
        return None;
    };
    Some(read_zero_extended_word(calldata.as_ref(), start))
}

fn word_to_address(word: [u8; 32]) -> Address {
    Address::from_slice(&word[12..32])
}

/// `sumCallArgUintForAddress(...) -> uint256`
pub fn sum_call_arg_uint_for_address(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::sumCallArgUintForAddressCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let key_start = arg_start_offset(call.keyArgIndex, 4)?;
    let value_start = arg_start_offset(call.valueArgIndex, 4)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut total = U256::ZERO;

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let record = &call_records[idx];
            let Some(depth) = tracer.call_depth_at(idx) else {
                continue;
            };
            if !call_matches_filter(record, depth, &call.filter, scheme_filter) {
                continue;
            }

            let Some(key_word) = read_call_arg_word(record, key_start) else {
                continue;
            };
            if word_to_address(key_word) != call.key {
                continue;
            }

            let Some(value_word) = read_call_arg_word(record, value_start) else {
                continue;
            };
            total = total.wrapping_add(U256::from_be_bytes(value_word));
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    Ok(PhevmOutcome::new(
        total.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `uniqueCallArgAddresses(...) -> address[]`
pub fn unique_call_arg_addresses(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::uniqueCallArgAddressesCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let key_start = arg_start_offset(call.argIndex, 4)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut unique = BTreeSet::new();

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let record = &call_records[idx];
            let Some(depth) = tracer.call_depth_at(idx) else {
                continue;
            };
            if !call_matches_filter(record, depth, &call.filter, scheme_filter) {
                continue;
            }

            let Some(key_word) = read_call_arg_word(record, key_start) else {
                continue;
            };
            let key = word_to_address(key_word);
            if unique.len() >= MAX_ARRAY_RESPONSE_ITEMS && !unique.contains(&key) {
                return Err(AggregateFactsError::TooManyResults {
                    count: unique.len() + 1,
                    max: MAX_ARRAY_RESPONSE_ITEMS,
                });
            }
            unique.insert(key);
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let keys: Vec<Address> = unique.into_iter().collect();
    Ok(PhevmOutcome::new(
        keys.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `sumCallArgUintByAddress(...) -> AddressUint[]`
pub fn sum_call_arg_uint_by_address(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::sumCallArgUintByAddressCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let key_start = arg_start_offset(call.keyArgIndex, 4)?;
    let value_start = arg_start_offset(call.valueArgIndex, 4)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut grouped: BTreeMap<Address, U256> = BTreeMap::new();

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let record = &call_records[idx];
            let Some(depth) = tracer.call_depth_at(idx) else {
                continue;
            };
            if !call_matches_filter(record, depth, &call.filter, scheme_filter) {
                continue;
            }

            let Some(key_word) = read_call_arg_word(record, key_start) else {
                continue;
            };
            let key = word_to_address(key_word);
            let Some(value_word) = read_call_arg_word(record, value_start) else {
                continue;
            };
            let value = U256::from_be_bytes(value_word);
            let next_count = grouped.len() + 1;
            let is_full = grouped.len() >= MAX_ARRAY_RESPONSE_ITEMS;

            match grouped.entry(key) {
                Entry::Occupied(mut entry) => {
                    *entry.get_mut() = entry.get().wrapping_add(value);
                }
                Entry::Vacant(entry) => {
                    if is_full {
                        return Err(AggregateFactsError::TooManyResults {
                            count: next_count,
                            max: MAX_ARRAY_RESPONSE_ITEMS,
                        });
                    }
                    entry.insert(value);
                }
            }
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let entries: Vec<PhEvm::AddressUint> = grouped
        .into_iter()
        .map(|(key, value)| PhEvm::AddressUint { key, value })
        .collect();

    Ok(PhevmOutcome::new(
        entries.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

fn validate_topic_index(topic_index: u8) -> Result<usize, AggregateFactsError> {
    let idx = topic_index as usize;
    if idx > 3 {
        return Err(AggregateFactsError::InvalidTopicIndex { topic_index });
    }
    Ok(idx)
}

/// `sumEventUintForTopicKey(...) -> uint256`
pub fn sum_event_uint_for_topic_key(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::sumEventUintForTopicKeyCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let topic_idx = validate_topic_index(call.keyTopicIndex)?;
    let value_start = arg_start_offset(call.valueDataIndex, 0)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let logs = ph_context.logs_and_traces.tx_logs;
    let log_indices = tracer.log_indices_by_emitter_topic0(logs, call.emitter, call.topic0);
    let log_cost = (log_indices.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let mut total = U256::ZERO;
    for &idx in log_indices {
        let log = &logs[idx];
        let topics = log.data.topics();
        if topic_idx >= topics.len() || topics[topic_idx] != call.key {
            continue;
        }
        let value = U256::from_be_bytes(read_zero_extended_word(&log.data.data, value_start));
        total = total.wrapping_add(value);
    }

    Ok(PhevmOutcome::new(
        total.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `uniqueEventTopicValues(...) -> bytes32[]`
pub fn unique_event_topic_values(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::uniqueEventTopicValuesCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let topic_idx = validate_topic_index(call.topicIndex)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let logs = ph_context.logs_and_traces.tx_logs;
    let log_indices = tracer.log_indices_by_emitter_topic0(logs, call.emitter, call.topic0);
    let log_cost = (log_indices.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let mut unique = BTreeSet::new();
    for &idx in log_indices {
        let log = &logs[idx];
        let topics = log.data.topics();
        if topic_idx >= topics.len() {
            continue;
        }
        let value: FixedBytes<32> = topics[topic_idx];
        if unique.len() >= MAX_ARRAY_RESPONSE_ITEMS && !unique.contains(&value) {
            return Err(AggregateFactsError::TooManyResults {
                count: unique.len() + 1,
                max: MAX_ARRAY_RESPONSE_ITEMS,
            });
        }
        unique.insert(value);
    }

    let values: Vec<FixedBytes<32>> = unique.into_iter().collect();
    Ok(PhevmOutcome::new(
        values.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `sumEventUintByTopic(...) -> Bytes32Uint[]`
pub fn sum_event_uint_by_topic(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, AggregateFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let call = PhEvm::sumEventUintByTopicCall::abi_decode(input_bytes)
        .map_err(AggregateFactsError::DecodeError)?;
    let topic_idx = validate_topic_index(call.keyTopicIndex)?;
    let value_start = arg_start_offset(call.valueDataIndex, 0)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let logs = ph_context.logs_and_traces.tx_logs;
    let log_indices = tracer.log_indices_by_emitter_topic0(logs, call.emitter, call.topic0);
    let log_cost = (log_indices.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(AggregateFactsError::OutOfGas(rax));
    }

    let mut grouped: BTreeMap<FixedBytes<32>, U256> = BTreeMap::new();
    for &idx in log_indices {
        let log = &logs[idx];
        let topics = log.data.topics();
        if topic_idx >= topics.len() {
            continue;
        }
        let key: FixedBytes<32> = topics[topic_idx];
        let value = U256::from_be_bytes(read_zero_extended_word(&log.data.data, value_start));
        let next_count = grouped.len() + 1;
        let is_full = grouped.len() >= MAX_ARRAY_RESPONSE_ITEMS;

        match grouped.entry(key) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = entry.get().wrapping_add(value);
            }
            Entry::Vacant(entry) => {
                if is_full {
                    return Err(AggregateFactsError::TooManyResults {
                        count: next_count,
                        max: MAX_ARRAY_RESPONSE_ITEMS,
                    });
                }
                entry.insert(value);
            }
        }
    }

    let entries: Vec<PhEvm::Bytes32Uint> = grouped
        .into_iter()
        .map(|(key, value)| PhEvm::Bytes32Uint { key, value })
        .collect();
    Ok(PhevmOutcome::new(
        entries.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::inspectors::{
        phevm::{
            LogsAndTraces,
            PhEvmContext,
        },
        tracer::CallTracer,
    };
    use alloy_primitives::{
        Bytes,
        Log,
        LogData,
        address,
        b256,
    };
    use revm::{
        context::JournalInner,
        interpreter::{
            CallInput,
            CallInputs,
            CallScheme,
            CallValue,
        },
    };

    fn with_test_context<R>(
        tracer: &CallTracer,
        logs: &[Log],
        tx_env: &crate::primitives::TxEnv,
        f: impl FnOnce(&PhEvmContext) -> R,
    ) -> R {
        let logs_and_traces = LogsAndTraces {
            tx_logs: logs,
            call_traces: tracer,
        };
        let context = PhEvmContext::new(&logs_and_traces, Address::ZERO, tx_env);
        f(&context)
    }

    fn push_call(
        tracer: &mut CallTracer,
        journal: &mut JournalInner<revm::JournalEntry>,
        target: Address,
        selector: FixedBytes<4>,
        scheme: CallScheme,
        words: &[[u8; 32]],
    ) {
        let mut calldata = selector.as_slice().to_vec();
        for word in words {
            calldata.extend_from_slice(word);
        }

        let input: Bytes = calldata.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: target,
                known_bytecode: None,
                target_address: target,
                caller: Address::random(),
                value: CallValue::Transfer(U256::ZERO),
                scheme,
                is_static: false,
            },
            &input,
            journal,
        );
        tracer.record_call_end(journal, false);
    }

    fn address_word(addr: Address) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[12..32].copy_from_slice(addr.as_slice());
        word
    }

    fn uint_word(value: u64) -> [u8; 32] {
        U256::from(value).to_be_bytes::<32>()
    }

    fn default_filter() -> PhEvm::CallFilter {
        PhEvm::CallFilter {
            callType: 0,
            minDepth: 0,
            maxDepth: 0,
            topLevelOnly: false,
        }
    }

    #[test]
    fn test_sum_call_arg_uint_for_address() {
        let target = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let key = address!("2222222222222222222222222222222222222222");
        let other = address!("3333333333333333333333333333333333333333");

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(key), uint_word(7)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(other), uint_word(9)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(key), uint_word(11)],
        );

        let input = PhEvm::sumCallArgUintForAddressCall {
            target,
            selector,
            keyArgIndex: U256::ZERO,
            key,
            valueArgIndex: U256::from(1),
            filter: default_filter(),
        }
        .abi_encode();

        let tx_env = crate::primitives::TxEnv::default();
        let logs: Vec<Log> = vec![];
        with_test_context(&tracer, &logs, &tx_env, |context| {
            let outcome = sum_call_arg_uint_for_address(context, &input, u64::MAX).unwrap();
            let total = U256::abi_decode(outcome.bytes()).unwrap();
            assert_eq!(total, U256::from(18));
        });
    }

    #[test]
    fn test_unique_call_arg_addresses_sorted_and_deduped() {
        let target = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0xAA, 0xBB, 0xCC, 0xDD]);
        let a1 = address!("1111111111111111111111111111111111111112");
        let a2 = address!("1111111111111111111111111111111111111113");

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a2)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a1)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a2)],
        );

        let input = PhEvm::uniqueCallArgAddressesCall {
            target,
            selector,
            argIndex: U256::ZERO,
            filter: default_filter(),
        }
        .abi_encode();

        let tx_env = crate::primitives::TxEnv::default();
        let logs: Vec<Log> = vec![];
        with_test_context(&tracer, &logs, &tx_env, |context| {
            let outcome = unique_call_arg_addresses(context, &input, u64::MAX).unwrap();
            let keys = Vec::<Address>::abi_decode(outcome.bytes()).unwrap();
            assert_eq!(keys, vec![a1, a2]);
        });
    }

    #[test]
    fn test_sum_call_arg_uint_by_address_groups_values() {
        let target = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x10, 0x20, 0x30, 0x40]);
        let a1 = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let a2 = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a1), uint_word(3)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a2), uint_word(5)],
        );
        push_call(
            &mut tracer,
            &mut journal,
            target,
            selector,
            CallScheme::Call,
            &[address_word(a1), uint_word(7)],
        );

        let input = PhEvm::sumCallArgUintByAddressCall {
            target,
            selector,
            keyArgIndex: U256::ZERO,
            valueArgIndex: U256::from(1),
            filter: default_filter(),
        }
        .abi_encode();

        let tx_env = crate::primitives::TxEnv::default();
        let logs: Vec<Log> = vec![];
        with_test_context(&tracer, &logs, &tx_env, |context| {
            let outcome = sum_call_arg_uint_by_address(context, &input, u64::MAX).unwrap();
            let grouped = Vec::<PhEvm::AddressUint>::abi_decode(outcome.bytes()).unwrap();
            assert_eq!(grouped.len(), 2);
            assert_eq!(grouped[0].key, a1);
            assert_eq!(grouped[0].value, U256::from(10));
            assert_eq!(grouped[1].key, a2);
            assert_eq!(grouped[1].value, U256::from(5));
        });
    }

    #[test]
    fn test_event_aggregates() {
        let emitter = address!("9999999999999999999999999999999999999999");
        let topic0 = b256!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let key1 = b256!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let key2 = b256!("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");

        let logs = vec![
            Log {
                address: emitter,
                data: LogData::new(vec![topic0, key1], Bytes::from(uint_word(4).to_vec())).unwrap(),
            },
            Log {
                address: emitter,
                data: LogData::new(vec![topic0, key2], Bytes::from(uint_word(7).to_vec())).unwrap(),
            },
            Log {
                address: emitter,
                data: LogData::new(vec![topic0, key1], Bytes::from(uint_word(9).to_vec())).unwrap(),
            },
        ];

        let sum_for_key_input = PhEvm::sumEventUintForTopicKeyCall {
            emitter,
            topic0,
            keyTopicIndex: 1,
            key: key1,
            valueDataIndex: U256::ZERO,
        }
        .abi_encode();

        let unique_input = PhEvm::uniqueEventTopicValuesCall {
            emitter,
            topic0,
            topicIndex: 1,
        }
        .abi_encode();

        let grouped_input = PhEvm::sumEventUintByTopicCall {
            emitter,
            topic0,
            keyTopicIndex: 1,
            valueDataIndex: U256::ZERO,
        }
        .abi_encode();

        let tracer = CallTracer::default();
        let tx_env = crate::primitives::TxEnv::default();
        with_test_context(&tracer, &logs, &tx_env, |context| {
            let sum_for_key =
                sum_event_uint_for_topic_key(context, &sum_for_key_input, u64::MAX).unwrap();
            assert_eq!(
                U256::abi_decode(sum_for_key.bytes()).unwrap(),
                U256::from(13)
            );

            let unique = unique_event_topic_values(context, &unique_input, u64::MAX).unwrap();
            let unique_values = Vec::<FixedBytes<32>>::abi_decode(unique.bytes()).unwrap();
            assert_eq!(unique_values, vec![key1, key2]);

            let grouped = sum_event_uint_by_topic(context, &grouped_input, u64::MAX).unwrap();
            let grouped_values = Vec::<PhEvm::Bytes32Uint>::abi_decode(grouped.bytes()).unwrap();
            assert_eq!(grouped_values.len(), 2);
            assert_eq!(grouped_values[0].key, key1);
            assert_eq!(grouped_values[0].value, U256::from(13));
            assert_eq!(grouped_values[1].key, key2);
            assert_eq!(grouped_values[1].value, U256::from(7));
        });
    }

    #[test]
    fn test_unique_event_topic_values_invalid_topic_index() {
        let input = PhEvm::uniqueEventTopicValuesCall {
            emitter: Address::ZERO,
            topic0: FixedBytes::ZERO,
            topicIndex: 9,
        }
        .abi_encode();

        let tracer = CallTracer::default();
        let tx_env = crate::primitives::TxEnv::default();
        let logs: Vec<Log> = vec![];
        with_test_context(&tracer, &logs, &tx_env, |context| {
            let err = unique_event_topic_values(context, &input, u64::MAX).unwrap_err();
            assert!(matches!(
                err,
                AggregateFactsError::InvalidTopicIndex { topic_index: 9 }
            ));
        });
    }
}
