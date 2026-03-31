#![feature(unsafe_cell_access)]
#![feature(test)]
#![feature(allocator_api)]
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

#[cfg(any(test, feature = "test"))]
// Perf benches reuse the executor's internal tx/setup artifacts, but those types
// should not be part of the production public API.
pub use executor::{
    BenchmarkAssertionSetupStats,
    ExecuteForkedTxResult,
};

mod arena;

pub mod constants;

pub mod primitives;

pub use primitives::TxValidationResultWithInspectors;

pub mod store;

pub mod inspectors;

pub mod db;

pub mod evm;

pub mod utils;

pub mod metrics;

#[cfg(any(test, feature = "test"))]
pub mod test_utils;
