use crate::{
    primitives::EVMError,
    store::AssertionStoreError,
};

use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError<DbError: Debug> {
    #[error("Expected value not found in database")]
    TxError(#[from] EVMError<DbError>),
    #[error("Failed to read assertions")]
    AssertionReadError(#[from] AssertionStoreError),
}
