//! # Reshiram Precompiles
//!
//! - `load_state_at`: Reads immutable transaction snapshots without switching fork state.
//! - `staticcall_at`: Executes read-only calls against immutable transaction snapshots.
//! - `tx_object`: Returns the original transaction environment.

pub(crate) use super::BASE_COST;

pub mod load_state_at;
pub mod staticcall_at;
pub mod tx_object;
