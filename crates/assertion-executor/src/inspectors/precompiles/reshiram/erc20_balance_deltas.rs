use crate::{
    inspectors::{
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm::{
            self,
            changedErc20BalanceDeltasCall,
            reduceErc20BalanceDeltasCall,
        },
    },
    primitives::{
        Address,
        Bytes,
    },
};

use alloy_sol_types::SolCall;
use std::collections::HashMap;

use super::{
    BASE_COST as RESHIRAM_BASE_COST,
    erc20_transfers::{
        Erc20TransfersError,
        collect_erc20_transfers,
        encode_erc20_transfers,
        single_token_transfers_outcome,
    },
    snapshot_logs::logs_for_fork,
};

const LOG_SCAN_COST: u64 = 1;
const MATCH_DECODE_COST: u64 = 12;
const REDUCTION_COST: u64 = 2;
const ABI_ENCODE_COST: u64 = 3;

#[derive(Debug, thiserror::Error)]
pub enum Erc20BalanceDeltasError {
    #[error("Error decoding changedErc20BalanceDeltas call: {0}")]
    DecodeChanged(#[source] alloy_sol_types::Error),
    #[error("Error decoding reduceErc20BalanceDeltas call: {0}")]
    DecodeReduce(#[source] alloy_sol_types::Error),
    #[error(transparent)]
    Transfers(#[from] Erc20TransfersError),
    #[error(transparent)]
    ForkWindow(#[from] super::snapshot_logs::ForkWindowError),
    #[error("Overflow reducing ERC20 balance deltas for pair ({from}, {to})")]
    ReductionOverflow { from: Address, to: Address },
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

/// Handles the `changedErc20BalanceDeltas` precompile call.
///
/// - Decodes the ABI-encoded calldata and delegates to [`single_token_transfers_outcome`].
/// - Semantic alias of `getErc20Transfers` for balance-delta workflows.
/// - Returns all ERC-20 Transfer events for the given token in the requested fork window.
pub fn changed_erc20_balance_deltas(
    context: &PhEvmContext,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, Erc20BalanceDeltasError> {
    let call = changedErc20BalanceDeltasCall::abi_decode(input_bytes)
        .map_err(Erc20BalanceDeltasError::DecodeChanged)?;
    single_token_transfers_outcome(context, call.token, &call.fork, gas).map_err(Into::into)
}

/// Handles the `reduceErc20BalanceDeltas` precompile call.
///
/// - Collects ERC-20 Transfer events for a single token in the requested fork window.
/// - Reduces transfers into net balance deltas per unique `(from, to)` pair.
/// - Charges gas proportional to log scanning, matching, reduction, and ABI encoding.
pub fn reduce_erc20_balance_deltas(
    context: &PhEvmContext,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, Erc20BalanceDeltasError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    charge_cost(&mut gas_left, gas_limit, RESHIRAM_BASE_COST)?;

    let call = reduceErc20BalanceDeltasCall::abi_decode(input_bytes)
        .map_err(Erc20BalanceDeltasError::DecodeReduce)?;
    let logs = logs_for_fork(
        context.logs_and_traces.tx_logs,
        context.logs_and_traces.call_traces,
        &call.fork,
    )?;
    charge_cost(
        &mut gas_left,
        gas_limit,
        (logs.len() as u64).saturating_mul(LOG_SCAN_COST),
    )?;

    let transfers = collect_erc20_transfers(logs, |emitter| emitter == call.token);
    charge_cost(
        &mut gas_left,
        gas_limit,
        (transfers.len() as u64).saturating_mul(MATCH_DECODE_COST),
    )?;

    let reduced = reduce_transfers(&transfers)?;
    charge_cost(
        &mut gas_left,
        gas_limit,
        (transfers.len() as u64).saturating_mul(REDUCTION_COST),
    )?;

    let encoded = encode_erc20_transfers(&reduced);
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
) -> Result<(), Erc20BalanceDeltasError> {
    if let Some(rax) = deduct_gas_and_check(gas_left, gas_cost, gas_limit) {
        return Err(Erc20BalanceDeltasError::OutOfGas(rax));
    }

    Ok(())
}

/// Merges transfers sharing the same `(from, to)` pair by summing their values.
///
/// - Preserves the order in which each unique pair is first seen.
/// - Returns an error on arithmetic overflow for any pair.
fn reduce_transfers(
    transfers: &[PhEvm::Erc20TransferData],
) -> Result<Vec<PhEvm::Erc20TransferData>, Erc20BalanceDeltasError> {
    let mut reduced: Vec<PhEvm::Erc20TransferData> = Vec::new();
    let mut pair_indices: HashMap<(Address, Address), usize> = HashMap::new();

    for transfer in transfers {
        match pair_indices.get(&(transfer.from, transfer.to)).copied() {
            Some(index) => {
                let Some(sum) = reduced[index].value.checked_add(transfer.value) else {
                    return Err(Erc20BalanceDeltasError::ReductionOverflow {
                        from: transfer.from,
                        to: transfer.to,
                    });
                };
                reduced[index].value = sum;
            }
            None => {
                pair_indices.insert((transfer.from, transfer.to), reduced.len());
                reduced.push(transfer.clone());
            }
        }
    }

    Ok(reduced)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::sol_primitives::PhEvm,
        primitives::U256,
        test_utils::random_address,
    };

    #[test]
    fn test_reduce_transfers_preserves_first_seen_pair_order() {
        let token = random_address();
        let alice = random_address();
        let bob = random_address();
        let carol = random_address();

        let transfers = vec![
            PhEvm::Erc20TransferData {
                token_addr: token,
                from: alice,
                to: bob,
                value: U256::from(10),
            },
            PhEvm::Erc20TransferData {
                token_addr: token,
                from: bob,
                to: carol,
                value: U256::from(3),
            },
            PhEvm::Erc20TransferData {
                token_addr: token,
                from: alice,
                to: bob,
                value: U256::from(7),
            },
        ];

        let reduced = reduce_transfers(&transfers).unwrap();
        assert_eq!(reduced.len(), 2);
        assert_eq!(reduced[0].from, alice);
        assert_eq!(reduced[0].to, bob);
        assert_eq!(reduced[0].value, U256::from(17));
        assert_eq!(reduced[1].from, bob);
        assert_eq!(reduced[1].to, carol);
        assert_eq!(reduced[1].value, U256::from(3));
    }

    #[test]
    fn test_reduce_transfers_reverts_on_overflow() {
        let token = random_address();
        let alice = random_address();
        let bob = random_address();
        let transfers = vec![
            PhEvm::Erc20TransferData {
                token_addr: token,
                from: alice,
                to: bob,
                value: U256::MAX,
            },
            PhEvm::Erc20TransferData {
                token_addr: token,
                from: alice,
                to: bob,
                value: U256::from(1),
            },
        ];

        let err = reduce_transfers(&transfers).unwrap_err();
        assert!(matches!(
            err,
            Erc20BalanceDeltasError::ReductionOverflow { from, to }
                if from == alice && to == bob
        ));
    }
}
