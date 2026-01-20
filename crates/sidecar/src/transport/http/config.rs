use crate::args::TransportConfig;
use std::{
    net::{
        AddrParseError,
        SocketAddr,
    },
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
    /// Max age for pending receive entries.
    pub pending_receive_ttl: Duration,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:50051".parse().unwrap(),
            pending_receive_ttl: Duration::from_secs(5),
        }
    }
}

impl TryFrom<TransportConfig> for HttpTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: TransportConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
            pending_receive_ttl: Duration::from_millis(value.pending_receive_ttl_ms.max(1)),
        })
    }
}
