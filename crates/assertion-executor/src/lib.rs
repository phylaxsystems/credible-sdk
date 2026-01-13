#![feature(unsafe_cell_access)]
#![feature(test)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::similar_names)]
#![allow(clippy::ignore_without_reason)]
#![allow(clippy::unreadable_literal)]

mod error;
pub use error::{
    ExecutorError,
    ForkTxExecutionError,
    TxExecutionError,
};

mod executor;
pub use executor::{
    AssertionExecutor,
    config::ExecutorConfig,
};

pub mod constants;

pub mod primitives;

pub mod store;

pub mod inspectors;

pub mod db;

pub mod evm;

pub mod utils;

mod metrics;

#[cfg(any(test, feature = "test"))]
pub mod test_utils;
