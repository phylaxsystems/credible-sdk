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

use moka::sync::Cache;

use ajj::Router;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_primitives::{
    Address,
    B256,
    hex,
};
use axum::Router as AxumRouter;
use dashmap::DashMap;
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
    backpressure::{
        OriginBackpressure,
        OriginMetadata,
    },
    config::ProxyConfig,
    error::{
        ProxyError,
        Result,
    },
    fingerprint::{
        CacheDecision,
        Fingerprint,
        FingerprintCache,
        decode_envelope,
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

        let backpressure = state.backpressure.clone();
        let pending_origins = state.pending_origins.clone();
        let path = config.rpc_path.clone();
        let pending_timeout = Duration::from_secs(config.cache.pending_timeout_secs);
        let addr = config.resolved_bind_addr()?;
        info!(%addr, %path, "credible rpc proxy starting");

        // Spawn background task to listen for invalidations from sidecar
        let cache_for_listener = cache.clone();
        let sidecar = sidecar_transport.clone();
        let backpressure_for_listener = backpressure.clone();
        let pending_for_listener = pending_origins.clone();
        tokio::spawn(async move {
            run_invalidation_listener(
                sidecar,
                cache_for_listener,
                backpressure_for_listener,
                pending_for_listener,
            )
            .await;
        });

        // Spawn background task to sweep stale pending entries
        let cache_for_sweep = cache.clone();
        let pending_for_sweep = pending_origins.clone();
        tokio::spawn(async move {
            run_pending_sweep(cache_for_sweep, pending_timeout, pending_for_sweep).await;
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

async fn run_invalidation_listener(
    sidecar: SharedSidecarTransport,
    cache: FingerprintCache,
    backpressure: OriginBackpressure,
    pending_origins: Arc<DashMap<B256, OriginMetadata>>,
) {
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
                            if let Some((_, origin)) =
                                pending_origins.remove(&invalidation.fingerprint.hash)
                            {
                                backpressure.record_failure(&origin);
                            }
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

async fn run_pending_sweep(
    cache: FingerprintCache,
    timeout: Duration,
    pending_origins: Arc<DashMap<B256, OriginMetadata>>,
) {
    // Run sweep at half the timeout interval to catch stuck entries promptly
    let sweep_interval = timeout / 2;

    loop {
        sleep(sweep_interval).await;
        let stale = cache.sweep_stale_pending(timeout);
        for hash in stale {
            pending_origins.remove(&hash);
        }
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
    backpressure: OriginBackpressure,
    pending_origins: Arc<DashMap<B256, OriginMetadata>>,
    sidecar: SharedSidecarTransport,
    http: Client,
    request_id: Arc<AtomicU64>,
    /// Cache mapping transaction hash to recovered sender address.
    /// Avoids expensive ECDSA recovery (~500µs) on every request.
    sender_cache: Cache<B256, Option<Address>>,
}

impl Clone for ProxyState {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            cache: self.cache.clone(),
            backpressure: self.backpressure.clone(),
            pending_origins: self.pending_origins.clone(),
            sidecar: self.sidecar.clone(),
            http: self.http.clone(),
            request_id: self.request_id.clone(),
            sender_cache: self.sender_cache.clone(),
        }
    }
}

impl ProxyState {
    pub fn new(config: ProxyConfig, sidecar: SharedSidecarTransport) -> Self {
        let cache = FingerprintCache::new(config.cache.clone());
        let backpressure = OriginBackpressure::new(config.backpressure.clone());
        let pending_origins = Arc::new(DashMap::new());
        let http = Client::new();

        // Sender recovery cache: 5 min TTL, 100k max entries (~8MB memory)
        // Avoids expensive ECDSA recovery (~500µs) on duplicate/retry submissions
        let sender_cache = Cache::builder()
            .max_capacity(100_000)
            .time_to_live(Duration::from_secs(300))
            .build();

        Self {
            config,
            cache,
            backpressure,
            pending_origins,
            sidecar,
            http,
            request_id: Arc::new(AtomicU64::new(1)),
            sender_cache,
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
        let envelope = decode_envelope(&raw_bytes)?;
        let fingerprint = Fingerprint::from_envelope(&envelope)?;

        // Use cached sender recovery to avoid expensive ECDSA operations (~500µs).
        // Cache key is the transaction hash, which is cheap to compute.
        let tx_hash = *envelope.tx_hash();
        let sender = self
            .sender_cache
            .get_with(tx_hash, || {
                envelope
                    .recover_signer()
                    .ok()
                    .or_else(|| envelope.recover_signer_unchecked().ok())
            });

        // Track sender recovery failures for observability.
        // Note: If sender recovery fails, backpressure will not apply to this transaction
        // since OriginMetadata.is_empty() returns true. This is acceptable because:
        // 1. Invalid signatures are rare in production (wallets sign correctly)
        // 2. The sequencer will reject invalid transactions anyway
        // 3. We avoid false-positive rate limiting of legitimate edge cases
        if sender.is_none() {
            metrics::counter!("rpc_proxy_sender_recovery_failure_total").increment(1);
            warn!(fingerprint = ?fingerprint.hash, "sender recovery failed; backpressure bypassed");
        }

        let origin = OriginMetadata {
            sender,
            ip: None,
        };

        if let Some(throttled) = self.backpressure.check(&origin) {
            if self.config.dry_run {
                warn!(
                    origin = %throttled.origin,
                    retry_after = ?throttled.retry_after,
                    "DRY-RUN: would rate limit origin but forwarding anyway"
                );
                metrics::counter!("rpc_proxy_dry_run_rejections_total", "reason" => "backpressure")
                    .increment(1);
            } else {
                return Err(ProxyError::Backpressure {
                    origin: throttled.origin,
                    retry_after: throttled.retry_after,
                });
            }
        }

        match self.cache.observe(&fingerprint) {
            CacheDecision::Forward => {
                if let Err(err) = self.maybe_short_circuit(&fingerprint, &origin).await {
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

                self.pending_origins
                    .insert(fingerprint.hash, origin.clone());
                let result = self.forward_downstream(&raw_hex).await;

                // Keep in pending regardless of forward result.
                // Cleanup happens via:
                // 1. Invalidation listener on sidecar failure (records backpressure)
                // 2. Pending timeout sweep on success/timeout (no invalidation = valid)
                // Do NOT release early - we need to wait for sidecar verdict.
                if let Err(err) = &result {
                    warn!(fingerprint = ?fingerprint.hash, %err, "forwarding failed");
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

    async fn maybe_short_circuit(
        &self,
        fingerprint: &Fingerprint,
        origin: &OriginMetadata,
    ) -> Result<()> {
        match self.sidecar.should_forward(fingerprint).await {
            Ok(ShouldForwardVerdict::Deny(assertion)) => {
                self.backpressure.record_failure(origin);
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
            ProxyError::Backpressure { .. } => -32003,   // Custom: rate limited
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
