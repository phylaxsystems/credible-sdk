use std::net::SocketAddr;

use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

use crate::{
    backpressure::BackpressureConfig,
    error::{
        ProxyError,
        Result,
    },
    fingerprint::CacheConfig,
};

/// Runtime configuration for the RPC proxy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Address the HTTP server listens on (e.g. `127.0.0.1:9546`).
    pub bind_addr: SocketAddr,
    /// JSON-RPC path exposed by the proxy. Defaults to `/rpc` to match the
    /// sequencer driver contracts.
    pub rpc_path: String,
    /// Upstream Ethereum JSON-RPC endpoint that ultimately executes accepted
    /// transactions.
    pub upstream_http: Url,
    /// Optional gRPC endpoint for communicating with the sidecar.
    pub sidecar_endpoint: Option<Url>,
    /// Fingerprint cache configuration.
    #[serde(default)]
    pub cache: CacheConfig,
    /// Sender/IP backpressure configuration.
    #[serde(default)]
    pub backpressure: BackpressureConfig,
    /// Dry-run mode: log what would be rejected but forward everything.
    /// Useful for validating cache hit rates in production without breaking traffic.
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum number of concurrent requests the proxy will handle globally.
    /// Prevents resource exhaustion during floods.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
}

fn default_max_concurrent_requests() -> usize {
    1000
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 9547)),
            rpc_path: "/rpc".into(),
            upstream_http: Url::parse("http://127.0.0.1:8545").expect("static URL"),
            sidecar_endpoint: None,
            cache: CacheConfig::default(),
            backpressure: BackpressureConfig::default(),
            dry_run: false,
            max_concurrent_requests: default_max_concurrent_requests(),
        }
    }
}

impl ProxyConfig {
    /// Validates a configuration loaded from CLI flags or disk.
    pub fn validate(self) -> Result<Self> {
        if self.rpc_path.is_empty() || !self.rpc_path.starts_with('/') {
            return Err(ProxyError::InvalidConfig(
                "rpc_path must start with '/'".to_string(),
            ));
        }

        Ok(self)
    }

    pub(crate) fn resolved_bind_addr(&self) -> Result<SocketAddr> {
        Ok(self.bind_addr)
    }
}
