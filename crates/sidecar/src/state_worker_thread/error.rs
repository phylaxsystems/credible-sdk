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
        }
    }
}
