//! HTTP JSON-RPC transport
use crate::{
    engine::queue::TransactionQueueSender,
    transactions_state::TransactionsState,
    transport::{
        Transport,
        http::{
            block_context::BlockContext,
            config::HttpTransportConfig,
            tracing_middleware::tracing_middleware,
            transactions_results::QueryTransactionsResults,
        },
    },
    utils::ErrorRecoverability,
};
use axum::{
    Router,
    middleware,
    routing::{
        get,
        post,
    },
};
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

mod block_context;
pub mod config;
pub mod server;
mod tracing_middleware;
pub mod transactions_results;

#[derive(thiserror::Error, Debug)]
pub enum HttpTransportError {
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Failed to bind the address: {0}")]
    BindAddress(String),
    #[error("Client error: {0}")]
    ClientError(#[source] reqwest::Error),
}

impl From<&HttpTransportError> for ErrorRecoverability {
    fn from(e: &HttpTransportError) -> Self {
        match e {
            HttpTransportError::ServerError(_) | HttpTransportError::BindAddress(_) => {
                Self::Unrecoverable
            }
            HttpTransportError::ClientError(_) => Self::Recoverable,
        }
    }
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
pub struct HttpTransport {
    /// Core engine queue sender.
    tx_sender: TransactionQueueSender,
    /// Server bind address
    bind_addr: SocketAddr,
    /// Shutdown cancellation token
    shutdown_token: CancellationToken,
    /// Signal if the transport has seen a blockenv, will respond to txs with errors if not
    has_blockenv: Arc<AtomicBool>,
    /// Shared transaction results state
    transactions_results: QueryTransactionsResults,
    /// Block context for tracing
    block_context: BlockContext,
}

/// Health check endpoint
// TODO: add readiness endpoint
#[instrument(name = "http_server::health", level = "trace")]
async fn health() -> &'static str {
    trace!("Health check requested");
    "OK"
}

/// Create health check routes
fn health_routes() -> Router {
    Router::new().route("/health", get(health))
}

/// Create transaction submission routes
fn transaction_routes(state: server::ServerState, block_context: &BlockContext) -> Router {
    Router::new()
        .route("/tx", post(server::handle_transaction_rpc))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            block_context.clone(),
            tracing_middleware,
        ))
}

impl Transport for HttpTransport {
    type Error = HttpTransportError;
    type Config = HttpTransportConfig;

    #[instrument(name = "http_transport::new", skip_all, level = "debug")]
    fn new(
        config: HttpTransportConfig,
        tx_sender: TransactionQueueSender,
        state_results: Arc<TransactionsState>,
    ) -> Result<Self, Self::Error> {
        debug!(
            bind_addr = %config.bind_addr,
            "Creating HTTP transport"
        );
        Ok(Self {
            tx_sender,
            bind_addr: config.bind_addr,
            shutdown_token: CancellationToken::new(),
            has_blockenv: Arc::new(AtomicBool::new(false)),
            transactions_results: QueryTransactionsResults::new(state_results),
            block_context: BlockContext::default(),
        })
    }

    #[instrument(
        name = "http_transport::run",
        skip(self),
        fields(bind_addr = %self.bind_addr),
        level = "info"
    )]
    async fn run(&self) -> Result<(), Self::Error> {
        let state = server::ServerState::new(
            self.has_blockenv.clone(),
            self.tx_sender.clone(),
            self.transactions_results.clone(),
            self.block_context.clone(),
        );
        let app = Router::new()
            .merge(health_routes())
            .merge(transaction_routes(state, &self.block_context));

        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                error!(
                    bind_addr = %self.bind_addr,
                    error = ?e,
                    "Failed to bind HTTP transport listener"
                );
                HttpTransportError::BindAddress(self.bind_addr.to_string())
            })?;

        info!(
            bind_addr = %self.bind_addr,
            "HTTP transport server starting"
        );

        let shutdown_token = self.shutdown_token.clone();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
            .await
            .map_err(|e| {
                error!(error = ?e, "HTTP server failed");
                HttpTransportError::ServerError(e.to_string())
            })?;
        Ok(())
    }

    #[instrument(name = "http_transport::stop", skip(self), level = "info")]
    fn stop(&mut self) {
        info!("Stopping HTTP transport");
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use serde_json::json;
    use std::net::SocketAddr;
    use tracing::debug;

    #[crate::utils::engine_test(http)]
    async fn test_invalid_block_env_request(mut instance: crate::utils::LocalInstance) {
        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "number": "invalid_number",  // String instead of u64
                "beneficiary": "0x0000000000000000000000000000000000000000",
                "timestamp": 0u64,
                "gas_limit": 0u64,
                "basefee": 0u64,
                "difficulty": "0x0"
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains("Failed to decode transactions: Block env validation error: invalid type: string \\\"invalid_number\\\", expected u64\""));
    }

    #[crate::utils::engine_test(http)]
    async fn test_invalid_reorg_request(mut instance: crate::utils::LocalInstance) {
        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "invalid": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains("Failed to decode transactions: Missing transaction parameters"));
    }

    #[crate::utils::engine_test(http)]
    async fn test_invalid_transaction_request(mut instance: crate::utils::LocalInstance) {
        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendTransactions",
            "params": {
                "transactions": [
                    {
                        "txEnv": {
                            "caller": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
                            "gas_limit": 21000,
                            "gas_price": "1000",
                            "transact_to": "0x8ba1f109551bD432803012645Hac136c2D29",
                            "value": "0x0",
                            "data": "0x",
                            "chain_id": 1,
                            "access_list": []
                        }
                    }
                ]
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains("Failed to decode transactions: Invalid transaction format: invalid transactions array: invalid txEnv: invalid type: string \\\"1000\\\", expected u128\""));
    }

    async fn send_raw_request(
        request: serde_json::Value,
        address: SocketAddr,
    ) -> Result<String, String> {
        let mut last_error = String::new();
        let mut attempts = 0;

        while attempts < 3 {
            attempts += 1;

            match Client::new()
                .post(format!("http://{address}/tx"))
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        return Err(format!("HTTP error: {}", response.status()));
                    }

                    return Ok(response.text().await.unwrap_or_default());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {e}");
                    if attempts < 3 {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/3), retrying...", attempts);
                        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
                    }
                }
            }
        }

        Err(last_error)
    }
}
