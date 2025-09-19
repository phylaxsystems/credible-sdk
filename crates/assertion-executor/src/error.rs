use crate::{
    inspectors::CallTracerError,
    primitives::EVMError,
    store::AssertionStoreError,
};

use std::fmt::Debug;

use crate::primitives::EvmState;
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
    ForkTxExecutionError(#[source] ForkTxExecutionError<ExtDb>),
    #[error("Assertion execution error")]
    AssertionExecutionError(EvmState, #[source] AssertionExecutionError<Active>),
}

impl<Active: DatabaseRef, ExtDb: Database> Debug for ExecutorError<Active, ExtDb>
where
    ExtDb::Error: Debug,
    Active::Error: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForkTxExecutionError(e) => write!(f, "ForkTxExecutionError({e:?})"),
            Self::AssertionExecutionError(_, e) => write!(f, "AssertionExecutionError({e:?})"),
        }
    }
}

#[derive(Error)]
pub enum ForkTxExecutionError<ExtDb: Database>
where
    ExtDb::Error: Debug,
{
    #[error("Evm error executing transaction: {0}")]
    TxEvmError(#[source] EVMError<ExtDb::Error>),
    #[error("Call tracer error: {0}")]
    CallTracerError(#[source] CallTracerError),
}

impl<ExtDb: Database> Debug for ForkTxExecutionError<ExtDb>
where
    ExtDb::Error: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxEvmError(e) => write!(f, "TxEvmError({e:?})"),
            Self::CallTracerError(e) => write!(f, "CallTracerError({e:?})"),
        }
    }
}

#[derive(Error)]
pub enum AssertionExecutionError<Active: DatabaseRef>
where
    Active::Error: Debug,
{
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[source] EVMError<Active::Error>),
    #[error("Assertion store error: {0}")]
    AssertionReadError(#[source] AssertionStoreError),
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
