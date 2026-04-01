//! # Reshiram Precompiles
//!
//! - `load_state_at`: Reads immutable transaction snapshots without switching fork state.
//! - `tx_object`: Returns the original transaction environment.

pub(crate) use super::BASE_COST;

pub mod load_state_at;
pub mod tx_object;
