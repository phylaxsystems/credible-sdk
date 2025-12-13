use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};

use ajj::Router;
use alloy_primitives::hex;
use axum::Router as AxumRouter;
use futures::StreamExt;
use reqwest::Client;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use tokio::{
    signal,
    time::sleep,
};
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
    sidecar::{
        GrpcSidecarTransport,
        NoopSidecarTransport,
        SharedSidecarTransport,
        ShouldForwardVerdict,
    },
};

/// Builder that wires configuration, state, and the ajj router together.
pub struct RpcProxyBuilder {
    config: ProxyConfig,
    sidecar_transport: Option<SharedSidecarTransport>,
}

impl RpcProxyBuilder {
    pub fn new(config: ProxyConfig) -> Self {
        Self {
            config,
            sidecar_transport: None,
        }
    }

    pub fn with_sidecar_transport(mut self, transport: SharedSidecarTransport) -> Self {
        self.sidecar_transport = Some(transport);
        self
    }

    pub fn build(self) -> Result<RpcProxy> {
        let config = self.config.validate()?;
        let sidecar: SharedSidecarTransport = if let Some(custom) = self.sidecar_transport {
            custom
        } else if let Some(endpoint) = &config.sidecar_endpoint {
            Arc::new(GrpcSidecarTransport::new(endpoint.clone())?)
        } else {
            Arc::new(NoopSidecarTransport::default())
        };
        let state = ProxyState::new(config.clone(), sidecar.clone());
        let cache = state.cache.clone();
        let router = build_router();
        Ok(RpcProxy {
            config,
            router,
            state,
            cache,
            sidecar_transport: sidecar,
        })
    }
}

pub struct RpcProxy {
    config: ProxyConfig,
    router: Router<ProxyState>,
    state: ProxyState,
    cache: FingerprintCache,
    sidecar_transport: SharedSidecarTransport,
}

impl RpcProxy {
    pub async fn serve(self) -> Result<()> {
        let RpcProxy {
            config,
            router,
            state,
            cache,
            sidecar_transport,
        } = self;

        let path = config.rpc_path.clone();
        let pending_timeout = Duration::from_secs(config.cache.pending_timeout_secs);
        let addr = config.resolved_bind_addr()?;
        info!(%addr, %path, "credible rpc proxy starting");

        // Spawn background task to listen for invalidations from sidecar
        let cache_for_listener = cache.clone();
        let sidecar = sidecar_transport.clone();
        tokio::spawn(async move {
            run_invalidation_listener(sidecar, cache_for_listener).await;
        });

        // Spawn background task to sweep stale pending entries
        let cache_for_sweep = cache.clone();
        tokio::spawn(async move {
            run_pending_sweep(cache_for_sweep, pending_timeout).await;
        });

        // Convert ajj router to axum and serve on the configured listener.
        let router: AxumRouter<_> = router.into_axum(&path).with_state(state);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        info!(%addr, %path, "credible rpc proxy listening");

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        info!("credible rpc proxy shutdown complete");

        Ok(())
    }
}

fn build_router() -> Router<ProxyState> {
    Router::new().route("eth_sendRawTransaction", send_raw_transaction)
}

async fn run_invalidation_listener(sidecar: SharedSidecarTransport, cache: FingerprintCache) {
    let mut retry_delay = Duration::from_secs(1);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

    loop {
        match sidecar.subscribe_invalidations().await {
            Ok(mut stream) => {
                info!("sidecar invalidation stream connected");
                // Reset backoff on successful connection
                retry_delay = Duration::from_secs(1);
                metrics::counter!("rpc_proxy_sidecar_reconnect_total").increment(1);

                while let Some(event) = stream.next().await {
                    match event {
                        Ok(invalidation) => {
                            cache.record_failure(
                                &invalidation.fingerprint,
                                invalidation.assertion.clone(),
                            );
                            metrics::counter!("rpc_proxy_invalidations_total").increment(1);
                        }
                        Err(err) => {
                            warn!(%err, "failed to process invalidation event");
                            break;
                        }
                    }
                }

                warn!("sidecar invalidation stream disconnected, reconnecting");
            }
            Err(err) => {
                warn!(%err, delay=?retry_delay, "sidecar subscription failed, retrying with backoff");
                metrics::counter!("rpc_proxy_sidecar_connection_errors_total").increment(1);
            }
        }

        // Exponential backoff with cap
        sleep(retry_delay).await;
        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
    }
}

async fn run_pending_sweep(cache: FingerprintCache, timeout: Duration) {
    // Run sweep at half the timeout interval to catch stuck entries promptly
    let sweep_interval = timeout / 2;

    loop {
        sleep(sweep_interval).await;
        cache.sweep_stale_pending(timeout);
    }
}

async fn shutdown_signal() {
    if let Err(err) = signal::ctrl_c().await {
        warn!(%err, "failed to listen for shutdown signal");
    }
}

pub struct ProxyState {
    pub config: ProxyConfig,
    pub cache: FingerprintCache,
    sidecar: SharedSidecarTransport,
    http: Client,
    request_id: Arc<AtomicU64>,
}

impl Clone for ProxyState {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            cache: self.cache.clone(),
            sidecar: self.sidecar.clone(),
            http: self.http.clone(),
            request_id: self.request_id.clone(),
        }
    }
}

impl ProxyState {
    pub fn new(config: ProxyConfig, sidecar: SharedSidecarTransport) -> Self {
        let cache = FingerprintCache::new(config.cache.clone());
        let http = Client::new();
        Self {
            config,
            cache,
            sidecar,
            http,
            request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn handle_send_raw_transaction(&self, params: Vec<String>) -> Result<String> {
        let raw_hex = params
            .first()
            .ok_or_else(|| {
                ProxyError::InvalidParams(
                    "eth_sendRawTransaction expects a single hex payload".into(),
                )
            })?
            .to_string();
        let raw_bytes = decode_raw_tx(&raw_hex)?;
        let fingerprint = Fingerprint::from_signed_tx(&raw_bytes)?;

        match self.cache.observe(&fingerprint) {
            CacheDecision::Forward => {
                if let Err(err) = self.maybe_short_circuit(&fingerprint).await {
                    if self.config.dry_run {
                        warn!(
                            fingerprint = ?fingerprint.hash,
                            %err,
                            "DRY-RUN: would reject but forwarding anyway"
                        );
                        metrics::counter!("rpc_proxy_dry_run_rejections_total", "reason" => "short_circuit")
                            .increment(1);
                    } else {
                        return Err(err);
                    }
                }

                let result = self.forward_downstream(&raw_hex).await;
                match &result {
                    Ok(_) => {
                        // Transaction forwarded successfully, release from pending
                        self.cache.release(&fingerprint);
                    }
                    Err(err) => {
                        warn!(fingerprint = ?fingerprint.hash, %err, "forwarding failed");
                        // Keep in pending - will be cleared when sidecar reports back
                        // or by the pending-state timeout.
                    }
                }
                result
            }
            CacheDecision::AwaitVerdict => {
                if self.config.dry_run {
                    warn!(
                        fingerprint = ?fingerprint.hash,
                        "DRY-RUN: would reject (pending) but forwarding anyway"
                    );
                    metrics::counter!("rpc_proxy_dry_run_rejections_total", "reason" => "pending")
                        .increment(1);
                    // In dry-run, still forward to upstream
                    self.forward_downstream(&raw_hex).await
                } else {
                    Err(ProxyError::PendingFingerprint(fingerprint.hash))
                }
            }
            CacheDecision::Reject(assertions) => {
                if self.config.dry_run {
                    warn!(
                        fingerprint = ?fingerprint.hash,
                        ?assertions,
                        "DRY-RUN: would reject (denied) but forwarding anyway"
                    );
                    metrics::counter!("rpc_proxy_dry_run_rejections_total", "reason" => "denied")
                        .increment(1);
                    // In dry-run, still forward to upstream
                    self.forward_downstream(&raw_hex).await
                } else {
                    Err(ProxyError::DeniedFingerprint(fingerprint.hash, assertions))
                }
            }
        }
    }

    async fn maybe_short_circuit(&self, fingerprint: &Fingerprint) -> Result<()> {
        match self.sidecar.should_forward(fingerprint).await {
            Ok(ShouldForwardVerdict::Deny(assertion)) => {
                self.cache.record_failure(fingerprint, assertion.clone());
                let mut assertions = HashSet::new();
                assertions.insert(assertion);
                Err(ProxyError::DeniedFingerprint(fingerprint.hash, assertions))
            }
            Ok(ShouldForwardVerdict::Allow) | Ok(ShouldForwardVerdict::Unknown) => Ok(()),
            Err(err) => {
                warn!(%err, "should_forward check failed");
                Ok(())
            }
        }
    }

    async fn forward_downstream(&self, raw_hex: &str) -> Result<String> {
        let id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "eth_sendRawTransaction",
            "params": [raw_hex],
        });

        let response = self
            .http
            .post(self.config.upstream_http.clone())
            .json(&payload)
            .send()
            .await?;
        let body: UpstreamResponse = response.json().await?;

        if let Some(error) = body.error {
            return Err(ProxyError::Upstream(format!(
                "{} (code {})",
                error.message, error.code
            )));
        }

        let result = body
            .result
            .and_then(|value| value.as_str().map(|s| s.to_string()))
            .ok_or_else(|| ProxyError::Upstream("missing result from upstream".into()))?;

        metrics::counter!("rpc_proxy_forward_total").increment(1);
        Ok(result)
    }
}

fn decode_raw_tx(raw_hex: &str) -> Result<Vec<u8>> {
    let stripped = raw_hex.trim_start_matches("0x");
    hex::decode(stripped)
        .map_err(|err| ProxyError::InvalidParams(format!("invalid raw transaction hex: {err}")))
}

#[derive(Debug, Deserialize)]
struct UpstreamResponse {
    result: Option<Value>,
    error: Option<UpstreamError>,
}

#[derive(Debug, Deserialize)]
struct UpstreamError {
    code: i64,
    message: String,
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
            ProxyError::InvalidParams(_) => -32602,      // Standard: invalid params
            ProxyError::InvalidConfig(_) => -32600,      // Standard: invalid request
            _ => -32000,                                 // Standard: server error
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}
