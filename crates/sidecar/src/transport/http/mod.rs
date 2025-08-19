//! HTTP JSON-RPC transport
use crate::{
    engine::queue::TransactionQueueSender,
    transport::{
        Transport,
        http::config::HttpTransportConfig,
    },
};
use axum::{
    Router,
    routing::get,
};
use std::{
    net::SocketAddr,
    sync::atomic::AtomicBool,
};
use tokio_util::sync::CancellationToken;

pub mod config;

#[derive(thiserror::Error, Debug)]
pub enum HttpTransportError {
    #[error("Error sending data to core engine via channel")]
    CoreSendError,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Client error: {0}")]
    ClientError(#[from] reqwest::Error),
}

/// Implementation of the HTTP `Transport`.
/// Contains server for accepting transaction events, and server to call
/// the driver when missing state.
///
/// ## Initializing
///
/// Initializing the transport should be done via the `new()` function.
/// One of the arguments is the `HttpTransportConfig`. The config contains
/// endpoints to which the transport should either listen on or connect to.
/// The transport will error on init if it tries to open a server on an occupied port.
///
/// ## Running
///
/// After initialization, the transport should be ran via `run`. It will proceed to spawn
/// multiple tasks needed for both the server, client and for its own operational purposes.
///
/// ## Cleaning up
///
/// The transport will attempt to clean up as well as it can when gracefully shutting down with
/// `stop`. This means severing client/server connections and performing database flushes if aplicable.
#[derive(Debug)]
#[allow(dead_code)]
pub struct HttpTransport {
    /// Core engine queue sender.
    tx_sender: TransactionQueueSender,
    /// HTTP client for outbound requests
    client: reqwest::Client,
    /// URL to use for making requests to the driver
    driver_url: SocketAddr,
    /// Server bind address
    bind_addr: SocketAddr,
    /// Shutdown cancellation token
    shutdown_token: CancellationToken,
    /// Signal if the transport has seen a blockenv, will respond to txs with errors if not
    has_blockenv: AtomicBool,
}

/// Health check endpoint
async fn health() -> &'static str {
    "OK"
}

/// Create health check routes
fn health_routes() -> Router {
    Router::new().route("/health", get(health))
}

/// Create transaction submission routes
fn transaction_routes() -> Router {
    // TODO: Add transaction submission endpoints
    Router::new()
}

impl Transport for HttpTransport {
    type Error = HttpTransportError;
    type Config = HttpTransportConfig;

    fn new(
        config: HttpTransportConfig,
        tx_sender: TransactionQueueSender,
    ) -> Result<Self, Self::Error> {
        let client = reqwest::Client::new();
        Ok(Self {
            tx_sender,
            client,
            bind_addr: config.bind_addr,
            driver_url: config.driver_addr,
            shutdown_token: CancellationToken::new(),
            has_blockenv: AtomicBool::new(false),
        })
    }

    async fn run(&self) -> Result<(), Self::Error> {
        let app = Router::new()
            .merge(health_routes())
            .merge(transaction_routes());

        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                HttpTransportError::ServerError(format!(
                    "Failed to bind to {}: {}",
                    self.bind_addr, e
                ))
            })?;

        tracing::info!("HTTP transport server starting on {}", self.bind_addr);

        let shutdown_token = self.shutdown_token.clone();
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
                .await
        });

        tokio::select! {
            result = server_task => {
                match result {
                    Ok(server_result) => {
                        server_result.map_err(|e| HttpTransportError::ServerError(format!("Server error: {}", e)))
                    }
                    Err(e) => {
                        Err(HttpTransportError::ServerError(format!("Server task error: {}", e)))
                    }
                }
            }
            // Add other tasks here as needed
        }
    }

    async fn stop(&mut self) -> Result<(), Self::Error> {
        tracing::info!("Stopping HTTP transport");
        self.shutdown_token.cancel();
        Ok(())
    }
}
