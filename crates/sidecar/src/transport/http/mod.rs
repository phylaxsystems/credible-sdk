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
use std::net::SocketAddr;
use tokio::task::JoinHandle;

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
    /// Server task handle (set when running)
    server_handle: Option<JoinHandle<()>>,
}

// Health check endpoint
async fn health() -> &'static str {
    "OK"
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
            server_handle: None,
        })
    }

    async fn run(&self) -> Result<(), Self::Error> {
        // TODO: Add transaction submission endpoints
        let app = Router::new().route("/health", get(health));

        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                HttpTransportError::ServerError(format!(
                    "Failed to bind to {}: {}",
                    self.bind_addr, e
                ))
            })?;

        tracing::info!("HTTP transport server starting on {}", self.bind_addr);
        axum::serve(listener, app)
            .await
            .map_err(|e| HttpTransportError::ServerError(format!("Server error: {}", e)))?;

        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Self::Error> {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
        Ok(())
    }
}
