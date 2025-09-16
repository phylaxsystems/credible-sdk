//! # Configuration for the `GrpcTransport`.

use crate::args::HttpTransportArgs;
use std::net::{
    AddrParseError,
    SocketAddr,
};

#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9090".parse().unwrap(),
        }
    }
}

impl TryFrom<HttpTransportArgs> for GrpcTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: HttpTransportArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
        })
    }
}
