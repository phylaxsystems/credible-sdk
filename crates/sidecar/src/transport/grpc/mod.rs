//! gRPC transport for high-performance communication
use crate::{
    engine::queue::TransactionQueueSender,
    transactions_state::TransactionsState,
    transport::{
        Transport,
        grpc::{
            config::GrpcTransportConfig,
            server::GrpcTransportServer,
        },
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
use tonic::transport::Server;
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

pub mod config;
pub mod proto;
pub mod server;

#[cfg(test)]
mod tests;

#[derive(thiserror::Error, Debug)]
pub enum GrpcTransportError {
    #[error("Error sending data to core engine via channel")]
    CoreSendError,
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("gRPC transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
    #[error("gRPC status error: {0}")]
    StatusError(#[from] tonic::Status),
}

/// Implementation of the gRPC `Transport`.
/// Provides high-performance binary communication for transaction events and state queries.
///
/// ## Features
///
/// - Binary protobuf serialization for efficiency
/// - Streaming support for high-throughput scenarios
/// - Built-in compression and multiplexing
/// - Type-safe API with generated protobuf code
///
/// ## Initializing
///
/// Initializing the transport should be done via the `new()` function.
/// One of the arguments is the `GrpcTransportConfig`. The config contains
/// the bind address for the gRPC server.
///
/// ## Running
///
/// After initialization, the transport should be run via `run`. It will start
/// the gRPC server and handle incoming requests.
///
/// ## Cleaning up
///
/// The transport will attempt to clean up gracefully when shutting down with
/// `stop`. This means closing the gRPC server and flushing any pending operations.
#[derive(Debug)]
pub struct GrpcTransport {
    /// Core engine queue sender.
    tx_sender: TransactionQueueSender,
    /// Server bind address
    bind_addr: SocketAddr,
    /// Shutdown cancellation token
    shutdown_token: CancellationToken,
    /// Signal if the transport has seen a blockenv, will respond to txs with errors if not
    has_blockenv: Arc<AtomicBool>,
    /// Shared transaction results state
    transactions_state: Arc<TransactionsState>,
}

impl Transport for GrpcTransport {
    type Error = GrpcTransportError;
    type Config = GrpcTransportConfig;

    #[instrument(name = "grpc_transport::new", skip_all, level = "debug")]
    fn new(
        config: GrpcTransportConfig,
        tx_sender: TransactionQueueSender,
        state_results: Arc<TransactionsState>,
    ) -> Result<Self, Self::Error> {
        debug!(
            bind_addr = %config.bind_addr,
            "Creating gRPC transport"
        );
        Ok(Self {
            tx_sender,
            bind_addr: config.bind_addr,
            shutdown_token: CancellationToken::new(),
            has_blockenv: Arc::new(AtomicBool::new(false)),
            transactions_state: state_results,
        })
    }

    #[instrument(
        name = "grpc_transport::run",
        skip(self),
        fields(bind_addr = %self.bind_addr),
        level = "info"
    )]
    async fn run(&self) -> Result<(), Self::Error> {
        let server_impl = GrpcTransportServer::new(
            self.has_blockenv.clone(),
            self.tx_sender.clone(),
            self.transactions_state.clone(),
        );

        info!(
            bind_addr = %self.bind_addr,
            "gRPC transport server starting"
        );

        let shutdown_token = self.shutdown_token.clone();

        // Start gRPC server
        Server::builder()
            .add_service(proto::sidecar_service_server::SidecarServiceServer::new(
                server_impl,
            ))
            .serve_with_shutdown(self.bind_addr, async move {
                shutdown_token.cancelled().await;
                info!("gRPC transport received shutdown signal");
            })
            .await
            .map_err(GrpcTransportError::TransportError)?;

        info!("gRPC transport server stopped");
        Ok(())
    }

    #[instrument(name = "grpc_transport::stop", skip(self), level = "info")]
    async fn stop(&mut self) -> Result<(), Self::Error> {
        info!("Stopping gRPC transport");
        self.shutdown_token.cancel();
        Ok(())
    }
}
