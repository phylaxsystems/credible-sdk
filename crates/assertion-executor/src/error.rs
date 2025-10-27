use crate::{
    inspectors::CallTracerError,
    primitives::{
        EVMError,
        EvmState,
    },
    store::AssertionStoreError,
};
use revm::Database;
use std::fmt::Debug;
use thiserror::Error;

/// Unified transaction execution error that works with any database error type
#[derive(Error)]
pub enum TxExecutionError<DbErr>
where
    DbErr: Debug,
{
    #[error("Evm error executing transaction: {0}")]
    TxEvmError(#[source] EVMError<DbErr>),
    #[error("Call tracer error: {0}")]
    CallTracerError(#[source] CallTracerError),
}

impl<DbErr: Debug> Debug for TxExecutionError<DbErr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxEvmError(e) => write!(f, "TxEvmError({e:?})"),
            Self::CallTracerError(e) => write!(f, "CallTracerError({e:?})"),
        }
    }
}

/// Unified assertion execution error
#[derive(Error)]
pub enum AssertionExecutionError<DbErr>
where
    DbErr: Debug,
{
    #[error("Assertion execution error: {0}")]
    AssertionExecutionError(#[source] EVMError<DbErr>),
    #[error("Assertion store error: {0}")]
    AssertionReadError(#[source] AssertionStoreError),
}

impl<DbErr: Debug> Debug for AssertionExecutionError<DbErr> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AssertionExecutionError(e) => write!(f, "AssertionExecutionError({e:?})"),
            Self::AssertionReadError(e) => write!(f, "AssertionReadError({e:?})"),
        }
    }
}

#[derive(Error)]
pub enum ExecutorError<ActiveDbErr, ExtDbErr = ActiveDbErr>
where
    ActiveDbErr: Debug,
    ExtDbErr: Debug,
{
    #[error("Fork tx execution error: {0}")]
    ForkTxExecutionError(#[source] TxExecutionError<ExtDbErr>),
    #[error("Assertion execution error")]
    AssertionExecutionError(EvmState, #[source] AssertionExecutionError<ActiveDbErr>),
}

impl<ActiveDbErr, ExtDbErr> Debug for ExecutorError<ActiveDbErr, ExtDbErr>
where
    ActiveDbErr: Debug,
    ExtDbErr: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ForkTxExecutionError(e) => write!(f, "ForkTxExecutionError({e:?})"),
            Self::AssertionExecutionError(_, e) => write!(f, "AssertionExecutionError({e:?})"),
        }
    }
}

pub type ForkTxExecutionError<ExtDb> = TxExecutionError<<ExtDb as Database>::Error>;
