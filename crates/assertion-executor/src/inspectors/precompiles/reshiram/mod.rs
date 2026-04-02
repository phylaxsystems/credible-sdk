//! # Reshiram Precompiles
//!
//! - `get_logs_query`: Returns filtered logs from immutable transaction snapshots.
//! - `erc20_transfers`: Returns decoded ERC20 Transfer events from immutable transaction snapshots.
//! - `erc20_balance_deltas`: Adds semantic aliases and pairwise transfer reduction.
//! - `load_state_at`: Reads immutable transaction snapshots without switching fork state.
//! - `staticcall_at`: Executes read-only calls against immutable transaction snapshots.
//! - `tx_object`: Returns the original transaction environment.

pub(crate) use super::BASE_COST;

pub mod erc20_balance_deltas;
pub mod erc20_transfers;
pub mod get_logs_query;
pub mod load_state_at;
pub mod snapshot_logs;
pub mod staticcall_at;
pub mod tx_object;
