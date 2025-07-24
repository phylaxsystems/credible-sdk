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

pub mod assertion_adopter;
pub mod calls;
pub mod console_log;
pub mod fork;
pub mod get_logs;
pub mod load;
pub mod state_changes;
