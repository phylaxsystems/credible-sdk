//! Explicit in-process control messages for the embedded state worker.

/// Sidecar-owned control messages delivered to the embedded worker.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ControlMessage {
    /// Grants MDBX flush permission up to the latest committed block number.
    CommitHead(u64),
}
