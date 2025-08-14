//! `transport`
//!
//! The transport mod contains generics and primitives for the sidecar to communicate with external
//! builders.
//!
//! The sidecar is driven by external builders sending transactions and blockenvs which drives it to
//! *build blocks* and respond with results.

pub mod mock;

use crate::engine::queue::TransactionQueueSender;

/// The `Transport` trait defines what methods transports that want to send transactions
/// to the engine must implement.
///
/// Transports are used to establish communication between the sidecar and an external driver
/// of a chain (i.e., the sequencer). Transports are also used as a general entrypoint for
/// communication with the core engine.
#[allow(async_fn_in_trait)]
pub trait Transport: Send + Sync {
    type Error: std::error::Error + Send;
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
