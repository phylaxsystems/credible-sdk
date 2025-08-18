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
    sync::{atomic::AtomicBool, Arc, Mutex},
};
use tokio::sync::oneshot;

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
    /// Shutdown signal sender
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
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
            shutdown_tx: Arc::new(Mutex::new(None)),
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
        
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);
        
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
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
        
        if let Some(shutdown_tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = shutdown_tx.send(());
        }
        
        Ok(())
    }
}
