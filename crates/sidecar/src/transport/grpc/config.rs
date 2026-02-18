//! # Configuration for the `GrpcTransport`.

use serde::Deserialize;
use std::net::{
    AddrParseError,
    SocketAddr,
};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:9090";

const DEFAULT_HEALTH_BIND_ADDR: &str = "0.0.0.0:9547";

const DEFAULT_EVENT_ID_BUFFER_CAPACITY: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct GrpcTransportConfig {
    pub bind_addr: String,
    pub health_bind_addr: String,
    pub event_id_buffer_capacity: usize,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            health_bind_addr: DEFAULT_HEALTH_BIND_ADDR.to_string(),
            event_id_buffer_capacity: DEFAULT_EVENT_ID_BUFFER_CAPACITY,
        }
    }
}

impl GrpcTransportConfig {
    pub fn bind_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        self.bind_addr.parse()
    }

    pub fn health_socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        self.health_bind_addr.parse()
    }
}
