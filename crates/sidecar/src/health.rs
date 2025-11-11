//! Health endpoint server shared across transports.

use axum::{
    Router,
    routing::get,
};
use std::{
    io,
    net::SocketAddr,
};
use tokio_util::sync::CancellationToken;
use tracing::{
    error,
    info,
    instrument,
};

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

/// Health endpoint, used to signal sidecar readiness
#[derive(Debug)]
pub struct HealthServer {
    bind_addr: SocketAddr,
    shutdown_token: CancellationToken,
}

impl HealthServer {
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
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
        axum::serve(listener, health_router())
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

#[instrument(name = "health_server::health", level = "trace")]
async fn health() -> &'static str {
    "OK"
}

pub fn health_router() -> Router {
    Router::new().route("/health", get(health))
}
