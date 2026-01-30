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
//! See the `grpc` mod for the protobuf API reference.
#![doc = include_str!("./README.md")]
// For auto generated code and docs
#![allow(clippy::too_many_lines)]
#![allow(clippy::default_trait_access)]
#![allow(clippy::doc_markdown)]

mod event_id_deduplication;
pub mod grpc;
pub mod mock;
pub(crate) mod rpc_metrics;
pub mod transactions_results;

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
        event_id_buffer_capacity: usize,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Start the transport. The transport is supposed to connect to the
    /// external driver and start as a background service.
    async fn run(&self) -> Result<(), Self::Error>;

    /// Graceful shutdown.
    fn stop(&mut self);
}

use grpc::{
    GrpcTransport,
    GrpcTransportError,
};
