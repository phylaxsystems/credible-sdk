use std::net::{
    AddrParseError,
    SocketAddr,
};

#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
    /// Maximum message size for gRPC requests (in bytes)
    pub max_message_size: usize,
    /// Enable gRPC compression
    pub enable_compression: bool,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
    /// Keep-alive interval in seconds
    pub keep_alive_interval_secs: u64,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9090".parse().unwrap(),
            max_message_size: 16 * 1024 * 1024, // 16MB
            enable_compression: true,
            connection_timeout_secs: 30,
            keep_alive_interval_secs: 30,
        }
    }
}

/// Command line arguments for gRPC transport configuration
#[derive(Debug, Clone)]
pub struct GrpcTransportArgs {
    pub bind_addr: String,
    pub max_message_size: Option<usize>,
    pub enable_compression: Option<bool>,
    pub connection_timeout_secs: Option<u64>,
    pub keep_alive_interval_secs: Option<u64>,
}

impl TryFrom<GrpcTransportArgs> for GrpcTransportConfig {
    type Error = AddrParseError;

    fn try_from(args: GrpcTransportArgs) -> Result<Self, Self::Error> {
        Ok(Self {
            bind_addr: args.bind_addr.parse()?,
            max_message_size: args.max_message_size.unwrap_or(16 * 1024 * 1024),
            enable_compression: args.enable_compression.unwrap_or(true),
            connection_timeout_secs: args.connection_timeout_secs.unwrap_or(30),
            keep_alive_interval_secs: args.keep_alive_interval_secs.unwrap_or(30),
        })
    }
}
