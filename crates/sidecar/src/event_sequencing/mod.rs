mod event_metadata;

use crate::{
    engine::{
        EngineError,
        queue::{
            TransactionQueueReceiver,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    event_sequencing::event_metadata::EventMetadata,
    utils::ErrorRecoverability,
};
use alloy::primitives::TxHash;
use std::{
    collections::{
        HashMap,
        HashSet,
        VecDeque,
    },
    time::Instant,
};
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
    /// The current head of the chain: The last committed head.
    current_head: u64,
    /// Context for each block.
    context: HashMap<u64, Context>,
}

/// The `Context` struct contains the context for a block. It is used to track the state of the block
/// and the events that have been dispatched during the block build.
struct Context {
    /// Sent events per iteration
    sent_events: HashMap<u64, VecDeque<EventMetadata>>,
    /// Keep track of pending events which haven't been sent yet to the engine
    dependency_graph: HashMap<EventMetadata, Vec<TxQueueContents>>,
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
            current_head: 0,
            context: HashMap::new(),
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
