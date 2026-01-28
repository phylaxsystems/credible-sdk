//! # Configuration for the `GrpcTransport`.

use crate::args::GrpcConfig;
use std::{
    net::{
        AddrParseError,
        SocketAddr,
    },
    time::Duration,
};

const DEFAULT_PENDING_RECEIVE_TTL_MS: Duration = Duration::from_secs(5);

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9090";

#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
    /// Max age for pending receive entries.
    pub pending_receive_ttl: Duration,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND_ADDR.parse().unwrap(),
            pending_receive_ttl: DEFAULT_PENDING_RECEIVE_TTL_MS,
        }
    }
}

impl TryFrom<GrpcConfig> for GrpcTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: GrpcConfig) -> Result<Self, Self::Error> {
        let pending_receive_ttl = if value.pending_receive_ttl_ms.is_zero() {
            Duration::from_millis(1)
        } else {
            value.pending_receive_ttl_ms
        };
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
            pending_receive_ttl,
        })
    }
}
