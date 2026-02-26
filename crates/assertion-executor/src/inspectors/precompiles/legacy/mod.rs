//! # Legacy Precompiles
//!
//! The `legacy` module contains the standard set of PhEvm precompiles available at launch.
//! These precompiles are exposed when an assertion registers with `AssertionSpec::Legacy`.
//!
//! ## Precompiles
//!
//! - `load`: Loads a storage slot from any account.
//! - `calls`: Returns the call inputs of a transaction, filtered by target and selector.
//! - `fork`: Forks to pre/post transaction and pre/post call states.
//! - `get_logs`: Returns the event logs emitted during a transaction.
//! - `state_changes`: Returns the storage state changes of a transaction.
//! - `assertion_adopter`: Returns the assertion adopter contract address.
//! - `console_log`: Enables console logging for debugging assertions.
//!
//! `AssertionSpec::Legacy`: crate::inspectors::spec_recorder::AssertionSpec::Legacy

pub(crate) use super::{
    BASE_COST,
    COLD_SLOAD_COST,
};

pub mod assertion_adopter;
pub mod calls;
pub mod console_log;
pub mod fork;
pub mod get_logs;
pub mod load;
pub mod state_changes;
