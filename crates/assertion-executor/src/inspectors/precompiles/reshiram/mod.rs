//! # Reshiram Precompiles
//!
//! - `tx_object`: Returns the original transaction environment.
//! - `ofac`: Reverts for OFAC-sanctioned addresses.

pub(crate) use super::BASE_COST;

pub mod ofac;
pub mod tx_object;
