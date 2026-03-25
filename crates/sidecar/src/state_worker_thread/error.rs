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
        // All variants are recoverable: panics are transient, runtime/thread spawn
        // failures are resource pressure, MDBX/trace/config errors are transient or
        // fixable on restart. EthRpcSource covers the window in every case.
        let _ = e;
        ErrorRecoverability::Recoverable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_error_variants_are_recoverable() {
        let variants: Vec<StateWorkerError> = vec![
            StateWorkerError::RuntimeBuild("test".into()),
            StateWorkerError::Panicked("test".into()),
            StateWorkerError::ThreadSpawn("test".into()),
            StateWorkerError::ComputeStartBlock("test".into()),
            StateWorkerError::MdbxWrite("test".into()),
            StateWorkerError::Trace("test".into()),
            StateWorkerError::Config("test".into()),
        ];

        for variant in &variants {
            let recoverability = ErrorRecoverability::from(variant);
            assert!(
                recoverability.is_recoverable(),
                "Expected Recoverable for {variant}",
            );
        }
    }

    #[test]
    fn error_display_includes_message() {
        let err = StateWorkerError::RuntimeBuild("out of memory".into());
        let display = format!("{err}");
        assert!(
            display.contains("out of memory"),
            "Expected display to contain 'out of memory', got: {display}",
        );
    }

    #[test]
    fn error_clone_produces_equal_message() {
        let err = StateWorkerError::Panicked("segfault".into());
        let cloned = err.clone();
        assert_eq!(format!("{err}"), format!("{cloned}"));
    }
}
