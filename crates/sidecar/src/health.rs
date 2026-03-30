//! Health endpoint server shared across transports.

use axum::{
    Json,
    Router,
    extract::State,
    http::StatusCode,
    routing::get,
};
use parking_lot::RwLock;
use serde::Serialize;
use std::{
    io,
    net::SocketAddr,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::{
    error,
    info,
    instrument,
};

const HEALTH_RESPONSE_BODY: &str = "OK";

#[derive(thiserror::Error, Debug)]
pub enum HealthServerError {
    #[error("failed to bind health server address: {addr}")]
    BindAddress {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("health server error on {addr}")]
    ServerError {
        addr: SocketAddr,
        #[source]
        source: io::Error,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerReadiness {
    Healthy,
    Degraded,
    #[default]
    Unavailable,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SourceReadinessSnapshot {
    pub name: String,
    pub ready: bool,
}

impl SourceReadinessSnapshot {
    pub fn new(name: impl Into<String>, ready: bool) -> Self {
        Self {
            name: name.into(),
            ready,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub fallback_active: bool,
    pub worker: WorkerReadiness,
    pub required_head: u64,
    pub minimum_synced_block: u64,
    pub sources: Vec<SourceReadinessSnapshot>,
}

#[derive(Debug, Default)]
pub struct HealthState {
    readiness: RwLock<ReadinessSnapshot>,
}

impl HealthState {
    pub fn update_readiness(&self, readiness: ReadinessSnapshot) {
        *self.readiness.write() = readiness;
    }

    pub fn readiness_snapshot(&self) -> ReadinessSnapshot {
        self.readiness.read().clone()
    }
}

fn readiness_status(snapshot: &ReadinessSnapshot) -> StatusCode {
    if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Health endpoint, used to signal sidecar readiness
#[derive(Debug)]
pub struct HealthServer {
    bind_addr: SocketAddr,
    health_state: Arc<HealthState>,
    shutdown_token: CancellationToken,
}

impl HealthServer {
    pub fn new(bind_addr: SocketAddr, health_state: Arc<HealthState>) -> Self {
        Self {
            bind_addr,
            health_state,
            shutdown_token: CancellationToken::new(),
        }
    }

    #[instrument(
        name = "health_server::run",
        skip(self),
        fields(bind_addr = %self.bind_addr),
        level = "debug"
    )]
    pub async fn run(&self) -> Result<(), HealthServerError> {
        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                error!(
                    bind_addr = %self.bind_addr,
                    error = ?e,
                    "Failed to bind health server listener"
                );
                HealthServerError::BindAddress {
                    addr: self.bind_addr,
                    source: e,
                }
            })?;

        info!(
            bind_addr = %self.bind_addr,
            "Health server starting"
        );

        let shutdown = self.shutdown_token.clone();
        axum::serve(listener, health_router(self.health_state.clone()))
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .map_err(|e| {
                error!(error = ?e, "Health server failed");
                HealthServerError::ServerError {
                    addr: self.bind_addr,
                    source: e,
                }
            })?;

        Ok(())
    }

    #[instrument(name = "health_server::stop", skip(self), level = "info")]
    pub fn stop(&mut self) {
        info!("Stopping health server");
        self.shutdown_token.cancel();
    }
}

#[instrument(name = "health_server::health", skip(health_state), level = "trace")]
async fn health(State(health_state): State<Arc<HealthState>>) -> (StatusCode, &'static str) {
    let snapshot = health_state.readiness_snapshot();
    (readiness_status(&snapshot), HEALTH_RESPONSE_BODY)
}

#[instrument(name = "health_server::ready", skip(health_state), level = "trace")]
async fn ready(
    State(health_state): State<Arc<HealthState>>,
) -> (StatusCode, Json<ReadinessSnapshot>) {
    let snapshot = health_state.readiness_snapshot();
    (readiness_status(&snapshot), Json(snapshot))
}

pub fn health_router(health_state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(health_state)
}

#[cfg(test)]
mod tests {
    use super::{
        HealthState,
        ReadinessSnapshot,
        SourceReadinessSnapshot,
        WorkerReadiness,
        health,
        HEALTH_RESPONSE_BODY,
        readiness_status,
    };
    use axum::{
        extract::State,
        http::StatusCode,
    };
    use std::sync::Arc;

    #[test]
    fn readiness_is_healthy_when_worker_is_degraded_but_rpc_fallback_is_ready() {
        let state = HealthState::default();
        state.update_readiness(ReadinessSnapshot {
            ready: true,
            fallback_active: true,
            worker: WorkerReadiness::Degraded,
            required_head: 128,
            minimum_synced_block: 120,
            sources: vec![
                SourceReadinessSnapshot::new("StateWorker", false),
                SourceReadinessSnapshot::new("EthRpcSource", true),
            ],
        });

        let snapshot = state.readiness_snapshot();

        assert_eq!(readiness_status(&snapshot), StatusCode::OK);
        assert!(snapshot.ready);
        assert!(snapshot.fallback_active);
        assert_eq!(snapshot.worker, WorkerReadiness::Degraded);
        assert_eq!(snapshot.sources[0].name, "StateWorker");
        assert!(!snapshot.sources[0].ready);
        assert_eq!(snapshot.sources[1].name, "EthRpcSource");
        assert!(snapshot.sources[1].ready);
    }

    #[test]
    fn readiness_is_unhealthy_when_no_source_can_serve_required_range() {
        let state = HealthState::default();
        state.update_readiness(ReadinessSnapshot {
            ready: false,
            fallback_active: false,
            worker: WorkerReadiness::Unavailable,
            required_head: 256,
            minimum_synced_block: 248,
            sources: vec![
                SourceReadinessSnapshot::new("StateWorker", false),
                SourceReadinessSnapshot::new("EthRpcSource", false),
            ],
        });

        let snapshot = state.readiness_snapshot();

        assert_eq!(readiness_status(&snapshot), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!snapshot.ready);
        assert!(!snapshot.fallback_active);
        assert_eq!(snapshot.worker, WorkerReadiness::Unavailable);
    }

    #[tokio::test]
    async fn health_endpoint_uses_readiness_status_when_no_source_is_ready() {
        let health_state = Arc::new(HealthState::default());
        health_state.update_readiness(ReadinessSnapshot {
            ready: false,
            fallback_active: false,
            worker: WorkerReadiness::Unavailable,
            required_head: 256,
            minimum_synced_block: 248,
            sources: vec![
                SourceReadinessSnapshot::new("StateWorker", false),
                SourceReadinessSnapshot::new("EthRpcSource", false),
            ],
        });

        let (status, body) = health(State(health_state)).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, HEALTH_RESPONSE_BODY);
    }
}
