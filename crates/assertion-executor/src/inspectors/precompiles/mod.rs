//! # `precompiles`
//!
//! The `precompiles` mod contains the implementations of all the phevm precompiles.
//!
//! As a general note, when we reffer to `precompiles` we generally refer to functions we implement
//! that functionally act as precompiles, but are implemented via revm inspectors. We do this to get better transaction
//! introspection and more flexibility from the implementation.
//!
//! Precompiles are organized by assertion spec version:
//!
//! - [`legacy`]: Standard precompiles available at launch.
//! - [`reshiram`]: Adds better transaction introspection.

use alloy_primitives::Bytes;

use super::phevm::PhevmOutcome;

pub mod legacy;
pub mod reshiram;

pub use revm::interpreter::gas::COLD_SLOAD_COST;

/// Base cost of calling phevm precompiles
pub(crate) const BASE_COST: u64 = 15;

/// Deduct gas from the gas limit and check if we have OOG.
pub(crate) fn deduct_gas_and_check(
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
