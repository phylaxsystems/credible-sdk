//! # `precompiles`
//!
//! The `precompiles` mod contains the implementations of all the phevm precompiles.
//! Helper methods used across precompiles can be found here, while the rest of
//! the precompile implementations can be found as follows:
//!
//! - `load`: Loads storage from any account.
//! - `calls`: Returns the call inputs of a transaction.
//! - `fork`: Forks to pre and post tx states.
//! - `logs`: Returns the logs of a transaction.
//! - `state_changes`: Returns the state changes of a transaction.

use alloy_primitives::Bytes;

use super::phevm::PhevmOutcome;

pub mod assertion_adopter;
pub mod call_boundary;
pub mod call_facts;
pub mod calls;
pub mod console_log;
pub mod erc20_facts;
pub mod fork;
pub mod get_logs;
pub mod load;
pub mod slot_diffs;
pub mod state_changes;
pub mod tx_object;
pub mod write_policy;

pub use revm::interpreter::gas::COLD_SLOAD_COST;

/// Base cost of calling phevm precompiles
pub(crate) const BASE_COST: u64 = 15;

/// Deduct gas from the gas limit and check if we have OOG.
pub(super) fn deduct_gas_and_check(
    gas_left: &mut u64,
    gas_cost: u64,
    gas_limit: u64,
) -> Option<PhevmOutcome> {
    if *gas_left < gas_cost {
        *gas_left = 0;
        return Some(PhevmOutcome::new(Bytes::default(), gas_limit));
    }

    *gas_left -= gas_cost;

    None
}
