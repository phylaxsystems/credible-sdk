use crate::args::file::TransportConfig;
use std::net::{
    AddrParseError,
    SocketAddr,
};

#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:50051".parse().unwrap(),
        }
    }
}

impl TryFrom<TransportConfig> for HttpTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: TransportConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
        })
    }
}
