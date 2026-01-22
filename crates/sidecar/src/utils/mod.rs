//! # `utils`
//!
//! Contains various shared utilities we use across the sidecar `mod`s:
//! - `test_util` test utilites to set up and run tests.
//! - `instance` test instance for running engine tests with mock transport
//! - `test_drivers` transport driver implementations for testing

#[cfg(any(test, feature = "bench-utils"))]
#[allow(dead_code)]
pub mod instance;
pub(crate) mod macros;
pub mod profiling;
#[cfg(any(test, feature = "bench-utils"))]
pub mod local_instance_db;
#[cfg(any(test, feature = "bench-utils"))]
#[allow(dead_code)]
pub mod test_drivers;
#[cfg(any(test, feature = "bench-utils"))]
mod test_util;

#[cfg(any(test, feature = "bench-utils"))]
pub use test_util::{
    TestDbError,
    engine_test,
};

#[cfg(any(test, feature = "bench-utils"))]
pub use crate::utils::instance::LocalInstance;

pub enum ErrorRecoverability {
    Recoverable,
    Unrecoverable,
}

impl ErrorRecoverability {
    pub fn is_recoverable(&self) -> bool {
        match self {
            ErrorRecoverability::Recoverable => true,
            ErrorRecoverability::Unrecoverable => false,
        }
    }
}
