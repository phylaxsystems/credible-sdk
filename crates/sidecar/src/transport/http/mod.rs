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
    routing::{get, post},
    extract::Json,
    response::Json as ResponseJson,
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
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
// TODO: this is a WIP and is not actually functional for now.
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
async fn health() -> &'static str {
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
fn transaction_routes() -> Router {
    Router::new()
        .route("/tx", post(handle_transaction_rpc))
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
