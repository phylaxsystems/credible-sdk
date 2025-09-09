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
mod transactions_results;

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
                            HttpTransportError::ServerError(format!("Server error: {e}"))
                        })
                    }
                    Err(e) => {
                        error!(error = %e, "HTTP server task error");
                        Err(HttpTransportError::ServerError(format!("Server task error: {e}")))
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
