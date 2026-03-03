use crate::{
    inspectors::{
        phevm::{
            PhEvmContext,
            PhevmOutcome,
        },
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm,
    },
    primitives::{
        Bytes,
        U256,
    },
};

use alloy_primitives::keccak256;
use alloy_sol_types::{
    SolCall,
    SolType,
    sol_data::{
        Array,
        Uint,
    },
};
use bumpalo::Bump;

use super::{
    BASE_COST,
    state_changes::{
        GetStateChangesError,
        get_differences,
    },
};

/// Get mapping state changes for a value-type key (address/uint/bool etc).
///
/// Decodes `getMappingStateChanges(address, bytes32, bytes32, uint256)`,
/// computes `keccak256(key ++ baseSlot) + offset`, then looks up state changes
/// for that derived slot.
///
/// # Errors
///
/// Returns an error if decoding fails, the journal is missing, or gas is exhausted.
pub fn get_mapping_state_changes_value_type(
    input_bytes: &[u8],
    context: &PhEvmContext,
    gas: u64,
) -> Result<PhevmOutcome, GetStateChangesError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(GetStateChangesError::OutOfGas(rax));
    }

    let event = PhEvm::getMappingStateChanges_0Call::abi_decode(input_bytes)
        .map_err(GetStateChangesError::CallDecodeError)?;

    let contract_address = event.contractAddress;
    let base_slot = event.baseSlot;
    let key = event.key;
    let offset = event.offset;

    // Derive slot: keccak256(key ++ baseSlot) + offset
    let mut preimage = [0u8; 64];
    preimage[..32].copy_from_slice(key.as_slice());
    preimage[32..].copy_from_slice(base_slot.as_slice());
    let derived_slot = U256::from_be_bytes(*keccak256(preimage)) + offset;

    let dif_bytes: Bytes = crate::arena::with_current_tx_arena(|arena| {
        let mut differences: Vec<U256, &Bump> = Vec::new_in(arena);
        get_differences(
            &context.logs_and_traces.call_traces.journal,
            contract_address,
            derived_slot,
            &mut gas_left,
            gas_limit,
            &mut differences,
        )?;

        Ok::<_, GetStateChangesError>(Array::<Uint<256>>::abi_encode(differences.as_slice()).into())
    })?;

    Ok(PhevmOutcome::new(dif_bytes, gas_limit - gas_left))
}

/// Get mapping state changes for a bytes/string key.
///
/// Decodes `getMappingStateChanges(address, bytes32, bytes)`,
/// computes `keccak256(key ++ baseSlot)`, then looks up state changes
/// for that derived slot.
///
/// # Errors
///
/// Returns an error if decoding fails, the journal is missing, or gas is exhausted.
pub fn get_mapping_state_changes_bytes_key(
    input_bytes: &[u8],
    context: &PhEvmContext,
    gas: u64,
) -> Result<PhevmOutcome, GetStateChangesError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(GetStateChangesError::OutOfGas(rax));
    }

    let event = PhEvm::getMappingStateChanges_1Call::abi_decode(input_bytes)
        .map_err(GetStateChangesError::CallDecodeError)?;

    let contract_address = event.contractAddress;
    let base_slot = event.baseSlot;
    let key = &event.key;

    // Derive slot: keccak256(key ++ baseSlot)
    let mut preimage = Vec::with_capacity(key.len() + 32);
    preimage.extend_from_slice(key);
    preimage.extend_from_slice(base_slot.as_slice());
    let derived_slot = U256::from_be_bytes(*keccak256(&preimage));

    let dif_bytes: Bytes = crate::arena::with_current_tx_arena(|arena| {
        let mut differences: Vec<U256, &Bump> = Vec::new_in(arena);
        get_differences(
            &context.logs_and_traces.call_traces.journal,
            contract_address,
            derived_slot,
            &mut gas_left,
            gas_limit,
            &mut differences,
        )?;

        Ok::<_, GetStateChangesError>(Array::<Uint<256>>::abi_encode(differences.as_slice()).into())
    })?;

    Ok(PhevmOutcome::new(dif_bytes, gas_limit - gas_left))
}
