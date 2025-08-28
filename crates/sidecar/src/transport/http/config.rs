use crate::args::HttpTransportArgs;
use std::net::{
    AddrParseError,
    SocketAddr,
};

#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
    /// Address of the driver
    pub driver_addr: SocketAddr,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8080".parse().unwrap(),
            driver_addr: "127.0.0.1:1337".parse().unwrap(),
        }
    }
}

impl TryFrom<HttpTransportArgs> for HttpTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: HttpTransportArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
            driver_addr: value.driver_addr.parse()?,
        })
    }
}
