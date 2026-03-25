use crate::utils::ErrorRecoverability;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum StateWorkerError {
    /// The tokio runtime failed to build inside the OS thread.
    #[error("Failed to build tokio runtime: {0}")]
    RuntimeBuild(String),
    /// The state worker panicked. Panic payload is captured as a message string.
    #[error("State worker panicked: {0}")]
    Panicked(String),
    /// An I/O error occurred spawning the OS thread itself.
    #[error("Thread spawn I/O error: {0}")]
    ThreadSpawn(String),
    /// Failed to compute the start block from MDBX on restart.
    #[error("Failed to compute start block: {0}")]
    ComputeStartBlock(String),
    /// MDBX write failed during flush.
    #[error("MDBX write failed: {0}")]
    MdbxWrite(String),
    /// Block tracing failed (fetch or system-call application).
    #[error("Block trace failed: {0}")]
    Trace(String),
    /// State worker configuration is invalid or missing required fields.
    #[error("State worker config invalid: {0}")]
    Config(String),
}

impl From<&StateWorkerError> for ErrorRecoverability {
    fn from(e: &StateWorkerError) -> Self {
        match e {
            // Panics are transient — restart and let EthRpcSource cover the window
            StateWorkerError::Panicked(_) => ErrorRecoverability::Recoverable,
            // Runtime build failure is transient (resource pressure) — recoverable
            StateWorkerError::RuntimeBuild(_) => ErrorRecoverability::Recoverable,
            // Thread spawn failure (OS resource exhaustion) — recoverable
            StateWorkerError::ThreadSpawn(_) => ErrorRecoverability::Recoverable,
            // Start block computation failure is recoverable — MDBX may be temporarily locked
            StateWorkerError::ComputeStartBlock(_) => ErrorRecoverability::Recoverable,
            // MDBX write failures are recoverable — EthRpcSource covers the window
            StateWorkerError::MdbxWrite(_) => ErrorRecoverability::Recoverable,
            // Trace failures are recoverable — transient RPC or network errors
            StateWorkerError::Trace(_) => ErrorRecoverability::Recoverable,
            // Config validation failures are recoverable — config may be fixed on restart
            StateWorkerError::Config(_) => ErrorRecoverability::Recoverable,
        }
    }
}
