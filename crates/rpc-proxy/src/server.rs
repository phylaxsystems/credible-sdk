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
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, %path, "credible rpc proxy listening");
        let axum = self.router.into_axum(&path);
        axum::serve(listener, axum).await?;
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
        Self {
            config,
            cache: FingerprintCache::new(),
        }
    }

    async fn handle_send_raw_transaction(&self, params: Vec<String>) -> Result<String> {
        let raw_hex = params.first().ok_or_else(|| {
            ProxyError::InvalidConfig("eth_sendRawTransaction expects a single hex payload".into())
        })?;
        let raw_bytes = decode_raw_tx(raw_hex)?;
        let fingerprint = Fingerprint::from_signed_tx(&raw_bytes)?;

        match self.cache.observe(&fingerprint) {
            CacheDecision::Forward => {
                let result = self.forward_downstream(raw_bytes).await;
                if let Err(err) = &result {
                    warn!(fingerprint = ?fingerprint.hash, %err, "forwarding failed");
                }
                self.cache.release(&fingerprint);
                result
            }
            CacheDecision::AwaitVerdict => Err(ProxyError::PendingFingerprint(fingerprint.hash)),
            CacheDecision::Reject => Err(ProxyError::DeniedFingerprint(fingerprint.hash)),
        }
    }

    async fn forward_downstream(&self, raw_bytes: Vec<u8>) -> Result<String> {
        // Placeholder implementation. Future changes will stream the transaction to
        // the configured sequencer or in-process driver. For now we simply echo the
        // payload so integration tests can assert the proxy shaped the request.
        let encoded = format!("0x{}", hex::encode(raw_bytes));
        Ok(encoded)
    }
}

fn decode_raw_tx(raw_hex: &str) -> Result<Vec<u8>> {
    let stripped = raw_hex.trim_start_matches("0x");
    hex::decode(stripped)
        .map_err(|err| ProxyError::InvalidConfig(format!("invalid raw transaction hex: {err}")))
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
            ProxyError::PendingFingerprint(_) => -32001,
            ProxyError::DeniedFingerprint(_) => -32002,
            ProxyError::InvalidConfig(_) => -32602,
            _ => -32000,
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}
