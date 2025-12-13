use std::{
    collections::HashSet,
    net::AddrParseError,
    time::Duration,
};

use alloy_primitives::B256;
use thiserror::Error;

use crate::{
    backpressure::OriginKey,
    fingerprint::AssertionInfo,
};

pub type Result<T, E = ProxyError> = std::result::Result<T, E>;

/// Top level error type for the proxy.
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid JSON-RPC parameters: {0}")]
    InvalidParams(String),
    #[error("bind or socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("HTTP server error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("upstream request error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("sidecar transport error: {0}")]
    SidecarTransport(String),
    #[error("fingerprint error: {0}")]
    Fingerprint(#[from] crate::fingerprint::FingerprintError),
    #[error("fingerprint {0:#x} is currently pending validation; retry after a short delay")]
    PendingFingerprint(B256),
    #[error("fingerprint {0:#x} is denied by assertions: {1:?}")]
    DeniedFingerprint(B256, HashSet<AssertionInfo>),
    #[error("{origin} is temporarily rate limited; retry after {retry_after:?}")]
    Backpressure {
        origin: OriginKey,
        retry_after: Duration,
    },
    #[error("upstream RPC error: {0}")]
    Upstream(String),
}
