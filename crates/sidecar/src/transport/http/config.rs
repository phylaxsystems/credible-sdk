use std::net::SocketAddr;

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
            driver_addr:"127.0.0.1:1337".parse().unwrap(),
        }
    }
}
