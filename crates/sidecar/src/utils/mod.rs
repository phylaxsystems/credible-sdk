//! # `utils`
//!
//! Contains various shared utilities we use across the sidecar `mod`s:
//! - `test_util` test utilites to set up and run tests.

#[cfg(test)]
mod test_util;

#[cfg(test)]
pub use test_util::TestDbError;
