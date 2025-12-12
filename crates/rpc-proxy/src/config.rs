use std::{
    net::SocketAddr,
    time::Duration,
};

use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

use crate::{
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
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 9547)),
            rpc_path: "/rpc".into(),
            upstream_http: Url::parse("http://127.0.0.1:8545").expect("static URL"),
            sidecar_endpoint: None,
            cache: CacheConfig::default(),
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
