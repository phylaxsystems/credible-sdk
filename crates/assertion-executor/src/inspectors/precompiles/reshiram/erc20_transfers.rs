use crate::{
    arena::with_current_tx_arena,
    inspectors::{
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm::{
            self,
            ForkId as SolForkId,
            getErc20TransfersCall,
            getErc20TransfersForTokensCall,
        },
    },
    primitives::{
        Address,
        Bytes,
        U256,
        fixed_bytes,
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
use std::collections::HashSet;

use super::{
    BASE_COST as RESHIRAM_BASE_COST,
    snapshot_logs::{
        ForkWindowError,
        logs_for_fork,
    },
};

const LOG_SCAN_COST: u64 = 1;
const TOKEN_LOOKUP_COST: u64 = 1;
const MATCH_DECODE_COST: u64 = 12;
const ABI_ENCODE_COST: u64 = 3;

pub(crate) const TRANSFER_SIGNATURE: FixedBytes<32> =
    fixed_bytes!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

#[derive(Debug, thiserror::Error)]
pub enum Erc20TransfersError {
    #[error("Error decoding getErc20Transfers call: {0}")]
    DecodeSingle(#[source] alloy_sol_types::Error),
    #[error("Error decoding getErc20TransfersForTokens call: {0}")]
    DecodeMulti(#[source] alloy_sol_types::Error),
    #[error(transparent)]
    ForkWindow(#[from] ForkWindowError),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

/// Handles the `getErc20Transfers` precompile call for a single token.
///
/// - Decodes the ABI-encoded calldata and delegates to [`single_token_transfers_outcome`].
/// - Returns all ERC-20 Transfer events emitted by the specified token in the fork window.
pub fn get_erc20_transfers(
    context: &PhEvmContext,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, Erc20TransfersError> {
    let call = getErc20TransfersCall::abi_decode(input_bytes)
        .map_err(Erc20TransfersError::DecodeSingle)?;
    single_token_transfers_outcome(context, call.token, &call.fork, gas)
}

/// Handles the `getErc20TransfersForTokens` precompile call for multiple tokens.
///
/// - Decodes the ABI-encoded calldata containing an array of token addresses.
/// - Returns all ERC-20 Transfer events emitted by any of the specified tokens.
pub fn get_erc20_transfers_for_tokens(
    context: &PhEvmContext,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, Erc20TransfersError> {
    let call = getErc20TransfersForTokensCall::abi_decode(input_bytes)
        .map_err(Erc20TransfersError::DecodeMulti)?;
    multi_token_transfers_outcome(context, &call.tokens, &call.fork, gas)
}

/// Collects and ABI-encodes ERC-20 transfers for a single token within a fork window.
///
/// - Resolves the fork snapshot, scans logs, and filters by token address.
/// - Charges gas proportional to log scanning, match decoding, and ABI encoding.
/// - Shared by `getErc20Transfers` and `changedErc20BalanceDeltas`.
pub(crate) fn single_token_transfers_outcome(
    context: &PhEvmContext,
    token: Address,
    fork: &SolForkId,
    gas: u64,
) -> Result<PhevmOutcome, Erc20TransfersError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    charge_cost(&mut gas_left, gas_limit, RESHIRAM_BASE_COST)?;

    let logs = logs_for_fork(
        context.logs_and_traces.tx_logs,
        context.logs_and_traces.call_traces,
        fork,
    )?;
    charge_cost(
        &mut gas_left,
        gas_limit,
        (logs.len() as u64).saturating_mul(LOG_SCAN_COST),
    )?;

    let transfers = collect_erc20_transfers(logs, |emitter| emitter == token);
    charge_cost(
        &mut gas_left,
        gas_limit,
        (transfers.len() as u64).saturating_mul(MATCH_DECODE_COST),
    )?;

    let encoded = encode_erc20_transfers(&transfers);
    charge_cost(
        &mut gas_left,
        gas_limit,
        ((encoded.len() as u64).div_ceil(32)).saturating_mul(ABI_ENCODE_COST),
    )?;

    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

fn multi_token_transfers_outcome(
    context: &PhEvmContext,
    tokens: &[Address],
    fork: &SolForkId,
    gas: u64,
) -> Result<PhevmOutcome, Erc20TransfersError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    charge_cost(&mut gas_left, gas_limit, RESHIRAM_BASE_COST)?;

    let logs = logs_for_fork(
        context.logs_and_traces.tx_logs,
        context.logs_and_traces.call_traces,
        fork,
    )?;
    charge_cost(
        &mut gas_left,
        gas_limit,
        (logs.len() as u64).saturating_mul(LOG_SCAN_COST),
    )?;
    charge_cost(
        &mut gas_left,
        gas_limit,
        (logs.len() as u64)
            .saturating_mul(tokens.len() as u64)
            .saturating_mul(TOKEN_LOOKUP_COST),
    )?;

    let token_set: HashSet<Address> = tokens.iter().copied().collect();
    let transfers = collect_erc20_transfers(logs, |emitter| token_set.contains(&emitter));
    charge_cost(
        &mut gas_left,
        gas_limit,
        (transfers.len() as u64).saturating_mul(MATCH_DECODE_COST),
    )?;

    let encoded = encode_erc20_transfers(&transfers);
    charge_cost(
        &mut gas_left,
        gas_limit,
        ((encoded.len() as u64).div_ceil(32)).saturating_mul(ABI_ENCODE_COST),
    )?;

    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

fn charge_cost(
    gas_left: &mut u64,
    gas_limit: u64,
    gas_cost: u64,
) -> Result<(), Erc20TransfersError> {
    if let Some(rax) = deduct_gas_and_check(gas_left, gas_cost, gas_limit) {
        return Err(Erc20TransfersError::OutOfGas(rax));
    }

    Ok(())
}

/// Filters logs for ERC-20 Transfer events matching the given predicate.
///
/// - Iterates over logs and keeps only those where `token_matches(emitter)` is true.
/// - Decodes each matching log into an [`Erc20TransferData`], skipping malformed entries.
pub(crate) fn collect_erc20_transfers<F>(
    logs: &[Log],
    mut token_matches: F,
) -> Vec<PhEvm::Erc20TransferData>
where
    F: FnMut(Address) -> bool,
{
    let mut transfers = Vec::new();
    for log in logs {
        if !token_matches(log.address) {
            continue;
        }

        let Some(transfer) = decode_erc20_transfer(log) else {
            continue;
        };
        transfers.push(transfer);
    }

    transfers
}

fn decode_erc20_transfer(log: &Log) -> Option<PhEvm::Erc20TransferData> {
    let topics = log.topics();
    if topics.first().copied() != Some(TRANSFER_SIGNATURE) || topics.len() < 3 {
        return None;
    }

    if log.data.data.len() != 32 {
        return None;
    }

    Some(PhEvm::Erc20TransferData {
        token_addr: log.address,
        from: Address::from_slice(&topics[1][12..]),
        to: Address::from_slice(&topics[2][12..]),
        value: U256::from_be_slice(log.data.data.as_ref()),
    })
}

/// ABI-encodes a slice of [`Erc20TransferData`] as a Solidity `Erc20TransferData[]`.
///
/// - Uses the per-transaction bump arena for intermediate allocations.
/// - Returns the ABI-encoded bytes ready to be returned from a precompile.
pub(crate) fn encode_erc20_transfers(transfers: &[PhEvm::Erc20TransferData]) -> Bytes {
    with_current_tx_arena(|arena| {
        let mut sol_transfers: Vec<PhEvm::Erc20TransferData, &Bump> = Vec::new_in(arena);
        sol_transfers.extend(transfers.iter().cloned());
        <alloy_sol_types::sol_data::Array<PhEvm::Erc20TransferData>>::abi_encode(
            sol_transfers.as_slice(),
        )
        .into()
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::{
            CallTracer,
            LogsAndTraces,
            spec_recorder::AssertionSpec,
        },
        test_utils::{
            random_address,
            run_precompile_test_with_spec,
        },
    };
    use alloy_primitives::{
        Bytes as AlloyBytes,
        LogData,
    };

    const TEST_GAS: u64 = 1_000_000;

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

    fn address_topic(address: Address) -> FixedBytes<32> {
        let mut topic = [0u8; 32];
        topic[12..].copy_from_slice(address.as_slice());
        FixedBytes::from(topic)
    }

    fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        Log {
            address: token,
            data: LogData::new(
                vec![TRANSFER_SIGNATURE, address_topic(from), address_topic(to)],
                AlloyBytes::copy_from_slice(&value.to_be_bytes::<32>()),
            )
            .unwrap(),
        }
    }

    fn malformed_transfer_log(token: Address, from: Address, to: Address) -> Log {
        Log {
            address: token,
            data: LogData::new(
                vec![TRANSFER_SIGNATURE, address_topic(from), address_topic(to)],
                AlloyBytes::copy_from_slice(&[1u8; 31]),
            )
            .unwrap(),
        }
    }

    fn decode_transfers(output: &Bytes) -> Vec<PhEvm::Erc20TransferData> {
        <alloy_sol_types::sol_data::Array<PhEvm::Erc20TransferData>>::abi_decode(output).unwrap()
    }

    #[test]
    fn test_get_erc20_transfers_filters_single_token_and_skips_malformed_logs() {
        let token_a = random_address();
        let token_b = random_address();
        let from = random_address();
        let to = random_address();
        let logs = vec![
            transfer_log(token_a, from, to, U256::from(10)),
            transfer_log(token_b, from, to, U256::from(20)),
            malformed_transfer_log(token_a, from, to),
        ];

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            single_token_transfers_outcome(
                context,
                token_a,
                &SolForkId {
                    forkType: 1,
                    callIndex: U256::ZERO,
                },
                TEST_GAS,
            )
        })
        .unwrap();

        let transfers = decode_transfers(outcome.bytes());
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].token_addr, token_a);
        assert_eq!(transfers[0].from, from);
        assert_eq!(transfers[0].to, to);
        assert_eq!(transfers[0].value, U256::from(10));
    }

    #[test]
    fn test_get_erc20_transfers_for_tokens_deduplicates_input_but_keeps_log_order() {
        let token_a = random_address();
        let token_b = random_address();
        let alice = random_address();
        let bob = random_address();
        let carol = random_address();
        let logs = vec![
            transfer_log(token_b, alice, bob, U256::from(1)),
            transfer_log(token_a, bob, carol, U256::from(2)),
            transfer_log(token_b, carol, alice, U256::from(3)),
        ];

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            multi_token_transfers_outcome(
                context,
                &[token_a, token_b, token_a],
                &SolForkId {
                    forkType: 1,
                    callIndex: U256::ZERO,
                },
                TEST_GAS,
            )
        })
        .unwrap();

        let transfers = decode_transfers(outcome.bytes());
        assert_eq!(transfers.len(), 3);
        assert_eq!(transfers[0].token_addr, token_b);
        assert_eq!(transfers[1].token_addr, token_a);
        assert_eq!(transfers[2].token_addr, token_b);
    }

    #[test]
    fn test_get_erc20_transfers_for_empty_token_list_returns_empty() {
        let logs = vec![transfer_log(
            random_address(),
            random_address(),
            random_address(),
            U256::from(1),
        )];

        let outcome = with_logs_context(&logs, &CallTracer::default(), |context| {
            multi_token_transfers_outcome(
                context,
                &[],
                &SolForkId {
                    forkType: 1,
                    callIndex: U256::ZERO,
                },
                TEST_GAS,
            )
        })
        .unwrap();

        assert!(decode_transfers(outcome.bytes()).is_empty());
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_erc20_transfers() {
        let result =
            run_precompile_test_with_spec("TestErc20BalanceDeltas", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "ERC20 balance delta precompiles should work under Reshiram spec: {result:#?}"
        );
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_erc20_transfers() {
        let result = run_precompile_test_with_spec("TestErc20BalanceDeltas", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "ERC20 balance delta precompiles should be forbidden under Legacy spec: {result:#?}"
        );
    }
}
