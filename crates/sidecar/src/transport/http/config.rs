use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct HttpTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
}

impl Default for HttpTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8080".parse().unwrap(),
        }
    }
}
