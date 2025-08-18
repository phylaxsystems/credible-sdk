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

pub mod mock;

use crate::engine::queue::TransactionQueueSender;

/// The `Transport` trait defines the interface for external communication adapters that
/// forward transactions and block environments to the core engine.
///
/// A transport should be implemented when implementing a new communication interface
/// between the sidecar and driver (or sequencer). A transport should be entirely self contained
/// spawning all services it needs to communicate inside of itself. The only way it should
/// interact with the rest of the sidecar code is via the `TransactionQueueSender` passed to
/// the driver on creation.
///
/// ## Associated Types
///
/// - `Error`: Transport-specific error type for connection and protocol failures
/// - `Config`: Configuration type for transport initialization
///
/// ## Lifecycle
///
/// 1. **Creation**: Use `new()` with configuration and queue sender
/// 2. **Execution**: Call `run()` to start the transport (typically in a background task)
/// 3. **Shutdown**: Call `stop()` for graceful cleanup
#[allow(async_fn_in_trait)]
pub trait Transport: Send + Sync {
    type Error: std::error::Error + Send;
    /// Optional config type. Intended to be used to configure the transport (i.e., ports, paths, etc...)
    type Config: Send;

    /// Create a new transport instance with the given configuration and queue sender.
    fn new(config: Self::Config, tx_sender: TransactionQueueSender) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Start the transport. The transport is supposed to connect to the
    /// external driver and start as a background service.
    async fn run(&self) -> Result<(), Self::Error>;

    /// Graceful shutdown.
    async fn stop(&mut self) -> Result<(), Self::Error>;
}
