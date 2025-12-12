use std::{
    net::SocketAddr,
    sync::Arc,
};

use ajj::Router;
use alloy_primitives::{
    B256,
    hex,
};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::{
    info,
    warn,
};

use crate::{
    config::ProxyConfig,
    error::{
        ProxyError,
        Result,
    },
    fingerprint::{
        CacheDecision,
        Fingerprint,
        FingerprintCache,
    },
};

/// Builder that wires configuration, state, and the ajj router together.
pub struct RpcProxyBuilder {
    config: ProxyConfig,
}

impl RpcProxyBuilder {
    pub fn new(config: ProxyConfig) -> Self {
        Self { config }
    }

    pub fn build(self) -> Result<RpcProxy> {
        let config = self.config.validate()?;
        let state = ProxyState::new(config.clone());
        let router = build_router(state.clone());
        Ok(RpcProxy { config, router })
    }
}

pub struct RpcProxy {
    config: ProxyConfig,
    router: Router<ProxyState>,
}

impl RpcProxy {
    pub async fn serve(self) -> Result<()> {
        let addr = self.config.resolved_bind_addr()?;
        let path = self.config.rpc_path.clone();
        info!(%addr, %path, "credible rpc proxy ready to listen");

        // TODO(integration): Complete axum integration for serving the router.
        // The ajj Router can be converted to an axum Router via into_axum(),
        // but the exact serving mechanism depends on axum version and features.
        // For now, this is a placeholder that will be completed when integrating
        // with the actual sequencer.
        //
        // Expected flow:
        // let listener = TcpListener::bind(addr).await?;
        // let app = self.router.into_axum(&path);
        // // Convert app to proper service and serve
        //
        // For testing, see the unit tests in fingerprint.rs which validate
        // the core cache and normalization logic.

        info!("Proxy serving is a TODO - implement axum integration");
        Ok(())
    }
}

fn build_router(state: ProxyState) -> Router<ProxyState> {
    Router::new()
        .route("eth_sendRawTransaction", send_raw_transaction)
        .with_state(state)
}

#[derive(Debug, Clone)]
pub struct ProxyState {
    config: ProxyConfig,
    cache: FingerprintCache,
}

impl ProxyState {
    fn new(config: ProxyConfig) -> Self {
        let cache = FingerprintCache::new(config.cache.clone());
        Self { config, cache }
    }

    async fn handle_send_raw_transaction(&self, params: Vec<String>) -> Result<String> {
        let raw_hex = params.first().ok_or_else(|| {
            ProxyError::InvalidParams("eth_sendRawTransaction expects a single hex payload".into())
        })?;
        let raw_bytes = decode_raw_tx(raw_hex)?;
        let fingerprint = Fingerprint::from_signed_tx(&raw_bytes)?;

        match self.cache.observe(&fingerprint) {
            CacheDecision::Forward => {
                let result = self.forward_downstream(raw_bytes).await;
                match &result {
                    Ok(_) => {
                        // Transaction forwarded successfully, release from pending
                        self.cache.release(&fingerprint);
                    }
                    Err(err) => {
                        warn!(fingerprint = ?fingerprint.hash, %err, "forwarding failed");
                        // Keep in pending - will be cleared when sidecar reports back
                        // or by TTL expiration of pending set (TODO: implement timeout)
                    }
                }
                result
            }
            CacheDecision::AwaitVerdict => {
                Err(ProxyError::PendingFingerprint(fingerprint.hash))
            }
            CacheDecision::Reject(assertions) => {
                Err(ProxyError::DeniedFingerprint(fingerprint.hash, assertions))
            }
        }
    }

    async fn forward_downstream(&self, raw_bytes: Vec<u8>) -> Result<String> {
        // Placeholder implementation. Future changes will stream the transaction to
        // the configured sequencer or in-process driver. For now we simply echo the
        // payload so integration tests can assert the proxy shaped the request.
        let encoded = format!("0x{}", hex::encode(raw_bytes));
        metrics::counter!("rpc_proxy_forward_total").increment(1);
        Ok(encoded)
    }
}

fn decode_raw_tx(raw_hex: &str) -> Result<Vec<u8>> {
    let stripped = raw_hex.trim_start_matches("0x");
    hex::decode(stripped)
        .map_err(|err| ProxyError::InvalidParams(format!("invalid raw transaction hex: {err}")))
}

async fn send_raw_transaction(
    params: Vec<String>,
    state: ProxyState,
) -> std::result::Result<String, RpcResponseError> {
    state
        .handle_send_raw_transaction(params)
        .await
        .map_err(|err| err.into())
}

/// Minimal JSON-RPC error payload that wraps [`ProxyError`]s for ajj.
#[derive(Debug, Clone, Serialize)]
pub struct RpcResponseError {
    code: i64,
    message: String,
}

impl From<ProxyError> for RpcResponseError {
    fn from(err: ProxyError) -> Self {
        let code = match err {
            ProxyError::PendingFingerprint(_) => -32001, // Custom: pending validation
            ProxyError::DeniedFingerprint(_, _) => -32002, // Custom: denied by assertion
            ProxyError::InvalidParams(_) => -32602,        // Standard: invalid params
            ProxyError::InvalidConfig(_) => -32600,        // Standard: invalid request
            _ => -32000,                                   // Standard: server error
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}
