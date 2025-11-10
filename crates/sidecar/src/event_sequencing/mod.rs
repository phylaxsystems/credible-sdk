use crate::{
    engine::{
        EngineError,
        queue::{
            TransactionQueueReceiver,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    utils::ErrorRecoverability,
};
use std::time::Instant;
use thiserror::Error;
use tracing::{
    error,
    info,
};

/// The event sequencing processes events from the transport layer and ensures the correct ordering
/// before sending the event to the core engine
pub struct EventSequencing {
    /// Channel on which the even sequencing receives events.
    tx_receiver: TransactionQueueReceiver,
    /// Channel on which the core engine receives events.
    tx_sender: TransactionQueueSender,
}

impl EventSequencing {
    /// Creates a new instance of the struct with the provided `tx_receiver` and `tx_sender`.
    ///
    /// # Arguments
    ///
    /// * `tx_receiver` - A `TransactionQueueReceiver` instance used to receive transactions.
    /// * `tx_sender` - A `TransactionQueueSender` instance used to send transactions.
    pub fn new(tx_receiver: TransactionQueueReceiver, tx_sender: TransactionQueueSender) -> Self {
        Self {
            tx_receiver,
            tx_sender,
        }
    }

    /// An asynchronous function that continuously processes events from a transaction queue channel
    /// and forwards them to a core engine for further handling.
    pub async fn run(&mut self) -> Result<(), EventSequencingError> {
        loop {
            // Use try_recv and yield when empty to be async-friendly
            let event = match self.tx_receiver.try_recv() {
                Ok(event) => event,
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // Channel is empty, yield to allow other tasks to run
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    error!(
                        target = "event_sequencing",
                        "Transaction queue channel disconnected"
                    );
                    return Err(EventSequencingError::ChannelClosed);
                }
            };

            if let Err(e) = self.tx_sender.send(event) {
                error!(
                    target = "event_sequencing",
                    error = ?e,
                    "Failed to send event to core engine"
                );
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum EventSequencingError {
    #[error("Transaction queue channel closed")]
    ChannelClosed,
}

impl From<&EventSequencingError> for ErrorRecoverability {
    fn from(e: &EventSequencingError) -> Self {
        match e {
            EventSequencingError::ChannelClosed => ErrorRecoverability::Recoverable,
        }
    }
}
