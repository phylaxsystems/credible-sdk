//! # `transport`
//!
//! The transport module provides abstractions for communication between the sidecar and external
//! transaction sources (e.g., sequencers, builders, or test harnesses).
//!
//! ## Overview
//!
//! The sidecar operates as a listener-based system where external drivers send transactions and
//! block envs that drive the core engine to build blocks and validate assertions.
//! Transports act as the bridge between these external systems and the internal engine.
//!
//! ```text
//! ┌─────────────┐    ┌───────────┐    ┌────────────┐
//! │ External    │───▶│ Transport │───▶│ Core       │
//! │ Driver      │    │           │    │ Engine     │
//! │ (Sequencer) │    │           │    │            │
//! └─────────────┘    └───────────┘    └────────────┘
//! ```
//! ## API reference
//!
//! Below you'll be able to find the API reference for the sidecar as `JSON-RPC` calls. For protobuf, see the `grpc` mod.
#![doc = include_str!("./README.md")]

// For auto generated code and docs
#![allow(clippy::too_many_lines)]
#![allow(clippy::default_trait_access)]
#![allow(clippy::doc_markdown)]

mod common;
pub mod decoder;
pub mod grpc;
pub mod http;
pub mod mock;

use crate::{
    engine::queue::TransactionQueueSender,
    transactions_state::TransactionsState,
    utils::ErrorRecoverability,
};
use std::sync::Arc;

/// The `Transport` trait defines the interface for external communication adapters that
/// forward transactions and block environments to the core engine.
///
/// A transport should be implemented when implementing a new communication interface
/// between the sidecar and driver (or sequencer). A transport should be entirely self contained
/// spawning all services it needs to communicate inside of itself. The only way it should
/// interact with the rest of the sidecar code is via the `TransactionQueueSender` passed to
/// the driver on creation. The transport will be spawned alongside other components of the
/// sidecar and work alongside them.
///
/// The main transport task *should not* block. If the transport needs to block it is
/// recommended to spawn a new blocking task for whatever action that blocks is needed.
///
/// ## Associated Types
///
/// - `Error`: Transport-specific error type for connection and protocol failures
/// - `Config`: Configuration type for transport initialization
///
/// ## Lifecycle
///
/// 1. **Creation**: Use `new()` with configuration and queue sender, called whenever a new transport is created (generally on startup)
/// 2. **Execution**: Call `run()` to start the transport
/// 3. **Shutdown**: Call `stop()` for graceful cleanup
///
/// ## Communicating with the driver
///
/// The transport should have 2-way communication with the driver:
/// - to receive transactions,
/// - to query for missing state.
///
/// The transport should have an endpoint to receive transactions/blockenvs from the driver
/// it then converts to events it can send to the core engine.
///
/// If the core engine runs into state it does not have cached locally, it should query it via
/// the transport, which should query the driver and store the response into the sidecar cache.
///
/// ### Querying and storing state; Communicating with engine
///
/// ***TODO: expand docs on interacting with state when implementing the state mod***
///
/// ## Erroring
///
/// The `Error` type defined in the trait should be used for exiting the transport when it
/// encounters an error it cannot trivially recover from. The sidecar is designed to recover
/// from individual component crashes, so if the transport exits with an error, it will attempt
/// to restart it.
#[allow(async_fn_in_trait)]
pub trait Transport: Send + Sync {
    type Error: std::error::Error + Send;
    /// Optional config type. Intended to be used to configure the transport (i.e., ports, paths, etc...)
    type Config: Send;

    /// Create a new transport instance with the given configuration and queue sender.
    fn new(
        config: Self::Config,
        tx_sender: TransactionQueueSender,
        state_results: Arc<TransactionsState>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Start the transport. The transport is supposed to connect to the
    /// external driver and start as a background service.
    async fn run(&self) -> Result<(), Self::Error>;

    /// Graceful shutdown.
    async fn stop(&mut self) -> Result<(), Self::Error>;
}

use grpc::{
    GrpcTransport,
    GrpcTransportError,
};
use http::{
    HttpTransport,
    HttpTransportError,
};

#[derive(Debug, thiserror::Error)]
pub enum AnyTransportError {
    #[error("HTTP transport error: {0}")]
    Http(#[from] HttpTransportError),
    #[error("gRPC transport error: {0}")]
    Grpc(#[from] GrpcTransportError),
}

impl From<&AnyTransportError> for ErrorRecoverability {
    fn from(e: &AnyTransportError) -> Self {
        match e {
            AnyTransportError::Http(inner) => ErrorRecoverability::from(inner),
            AnyTransportError::Grpc(inner) => ErrorRecoverability::from(inner),
        }
    }
}

/// A simple enum wrapper to run any concrete transport behind a unified API.
#[derive(Debug)]
pub enum AnyTransport {
    Http(HttpTransport),
    Grpc(GrpcTransport),
}

impl AnyTransport {
    pub async fn run(&self) -> Result<(), AnyTransportError> {
        match self {
            AnyTransport::Http(t) => t.run().await.map_err(AnyTransportError::from),
            AnyTransport::Grpc(t) => t.run().await.map_err(AnyTransportError::from),
        }
    }

    pub async fn stop(&mut self) -> Result<(), AnyTransportError> {
        match self {
            AnyTransport::Http(t) => t.stop().await.map_err(AnyTransportError::from),
            AnyTransport::Grpc(t) => t.stop().await.map_err(AnyTransportError::from),
        }
    }
}
