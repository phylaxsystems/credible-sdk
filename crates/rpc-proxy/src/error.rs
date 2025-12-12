use std::net::AddrParseError;

use alloy_primitives::B256;
use thiserror::Error;

pub type Result<T, E = ProxyError> = std::result::Result<T, E>;

/// Top level error type for the proxy.
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("bind or socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("HTTP server error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("fingerprint error: {0}")]
    Fingerprint(#[from] crate::fingerprint::FingerprintError),
    #[error("fingerprint {0:#x} is currently pending evaluation")]
    PendingFingerprint(B256),
    #[error("fingerprint {0:#x} is currently denied")]
    DeniedFingerprint(B256),
}
