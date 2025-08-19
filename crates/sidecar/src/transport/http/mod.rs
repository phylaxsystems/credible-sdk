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
    routing::{
        get,
        post,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::AtomicBool,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

pub mod config;
pub mod server;

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
    has_blockenv: Arc<AtomicBool>,
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

/// Health check endpoint
// TODO: add readiness endpoint
#[instrument(name = "http_server::health", level = "trace")]
async fn health() -> &'static str {
    trace!("Health check requested");
    "OK"
}

/// Handle JSON-RPC requests for transactions
async fn handle_transaction_rpc(
    Json(request): Json<JsonRpcRequest>,
) -> Result<ResponseJson<JsonRpcResponse>, StatusCode> {
    let response = match request.method.as_str() {
        "sendTransaction" => {
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(serde_json::json!({
                    "transactionHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                })),
                error: None,
                id: request.id,
            }
        }
        _ => {
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: "Method not found".to_string(),
                }),
                id: request.id,
            }
        }
    };

    Ok(ResponseJson(response))
}

/// Create health check routes
fn health_routes() -> Router {
    Router::new().route("/health", get(health))
}

/// Create transaction submission routes
fn transaction_routes(state: server::ServerState) -> Router {
    Router::new()
        .route("/tx", post(server::handle_transaction_rpc))
        .with_state(state)
}

impl Transport for HttpTransport {
    type Error = HttpTransportError;
    type Config = HttpTransportConfig;

    #[instrument(name = "http_transport::new", skip_all, level = "debug")]
    fn new(
        config: HttpTransportConfig,
        tx_sender: TransactionQueueSender,
    ) -> Result<Self, Self::Error> {
        debug!(
            bind_addr = %config.bind_addr,
            driver_addr = %config.driver_addr,
            "Creating HTTP transport"
        );

        let client = reqwest::Client::new();
        Ok(Self {
            tx_sender,
            client,
            bind_addr: config.bind_addr,
            driver_url: config.driver_addr,
            shutdown_token: CancellationToken::new(),
            has_blockenv: Arc::new(AtomicBool::new(false)),
        })
    }

    #[instrument(name = "http_transport::run", skip(self), fields(bind_addr = %self.bind_addr), level = "info")]
    async fn run(&self) -> Result<(), Self::Error> {
        let state = server::ServerState::new(self.has_blockenv.clone());
        let app = Router::new()
            .merge(health_routes())
            .merge(transaction_routes(state));

        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                error!(
                    bind_addr = %self.bind_addr,
                    error = %e,
                    "Failed to bind HTTP transport listener"
                );
                HttpTransportError::ServerError(format!(
                    "Failed to bind to {}: {}",
                    self.bind_addr, e
                ))
            })?;

        info!(
            bind_addr = %self.bind_addr,
            "HTTP transport server starting"
        );

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
                        server_result.map_err(|e| {
                            error!(error = %e, "HTTP server error");
                            HttpTransportError::ServerError(format!("Server error: {}", e))
                        })
                    }
                    Err(e) => {
                        error!(error = %e, "HTTP server task error");
                        Err(HttpTransportError::ServerError(format!("Server task error: {}", e)))
                    }
                }
            }
            // Add other tasks here as needed
        }
    }

    #[instrument(name = "http_transport::stop", skip(self), level = "info")]
    async fn stop(&mut self) -> Result<(), Self::Error> {
        info!("Stopping HTTP transport");
        self.shutdown_token.cancel();
        Ok(())
    }
}
