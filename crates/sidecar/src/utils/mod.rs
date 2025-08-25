//! # `utils`
//!
//! Contains various shared utilities we use across the sidecar `mod`s:
//! - `test_util` test utilites to set up and run tests.
//! - `instance` test instance for running engine tests with mock transport

#[cfg(test)]
#[allow(dead_code)]
pub mod instance;
#[cfg(test)]
mod test_util;

#[cfg(test)]
pub use instance::LocalInstance;
#[cfg(test)]
pub use test_util::{
    TestDbError,
    engine_test,
};
