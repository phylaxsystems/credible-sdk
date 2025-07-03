use crate::{primitives::EVMError, store::AssertionStoreError};

use std::fmt::Debug;

use revm::{Database, DatabaseRef};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError<Active: DatabaseRef, ExtDb: Database>
where
    ExtDb::Error: Debug,
{
    #[error("Fork tx execution error: {0}")]
    ForkTxExecutionError(#[from] ForkTxExecutionError<ExtDb>),
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[from] AssertionExecutionError<Active>),
}

#[derive(Error, Debug)]
pub enum ForkTxExecutionError<ExtDb: Database>
where
    ExtDb::Error: Debug,
{
    #[error("Evm error executing transaction: {0}")]
    TxEvmError(#[from] EVMError<ExtDb::Error>),
}

#[derive(Error, Debug)]
pub enum AssertionExecutionError<Active: DatabaseRef>
where
    Active::Error: Debug,
{
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[from] EVMError<Active::Error>),
    #[error("Assertion store error: {0}")]
    AssertionReadError(#[from] AssertionStoreError),
}
