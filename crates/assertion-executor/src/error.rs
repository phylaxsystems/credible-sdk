use crate::{
    primitives::EVMError,
    store::AssertionStoreError,
};

use std::fmt::Debug;

use revm::{
    Database,
    DatabaseRef,
};
use thiserror::Error;

#[derive(Error)]
pub enum ExecutorError<Active: DatabaseRef, ExtDb: Database>
where
    ExtDb::Error: Debug,
{
    #[error("Fork tx execution error: {0}")]
    ForkTxExecutionError(#[from] ForkTxExecutionError<ExtDb>),
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[from] AssertionExecutionError<Active>),
}

impl<Active: DatabaseRef, ExtDb: Database> Debug for ExecutorError<Active, ExtDb>
where
    ExtDb::Error: Debug,
    Active::Error: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForkTxExecutionError(e) => write!(f, "ForkTxExecutionError({e:?})"),
            Self::AssertionExecutionError(e) => write!(f, "AssertionExecutionError({e:?})"),
        }
    }
}

#[derive(Error)]
pub enum ForkTxExecutionError<ExtDb: Database>
where
    ExtDb::Error: Debug,
{
    #[error("Evm error executing transaction: {0}")]
    TxEvmError(#[from] EVMError<ExtDb::Error>),
}

impl<ExtDb: Database> Debug for ForkTxExecutionError<ExtDb>
where
    ExtDb::Error: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxEvmError(e) => write!(f, "TxEvmError({e:?})"),
        }
    }
}

#[derive(Error)]
pub enum AssertionExecutionError<Active: DatabaseRef>
where
    Active::Error: Debug,
{
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[from] EVMError<Active::Error>),
    #[error("Assertion store error: {0}")]
    AssertionReadError(#[from] AssertionStoreError),
}

impl<Active: DatabaseRef> Debug for AssertionExecutionError<Active>
where
    Active::Error: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AssertionExecutionError(e) => write!(f, "AssertionExecutionError({e:?})"),
            Self::AssertionReadError(e) => write!(f, "AssertionReadError({e:?})"),
        }
    }
}
