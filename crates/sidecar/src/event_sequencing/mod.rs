mod event_metadata;
#[cfg(test)]
mod tests;

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
#[derive(Default)]
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

    /// Recursively sends an event and all its dependent events from the dependency graph.
    /// Gracefully handles block transitions when processing dependencies.
    fn send_event_recursive(&mut self, event: TxQueueContents) {
        let block_number = event.block_number();
        let event_metadata = EventMetadata::from(&event);

        // Temporarily remove context to satisfy the borrow checker
        let mut ctx = self.context.remove(&block_number).unwrap_or_default();

        // Send the event using existing logic
        self.send_event(event, &mut ctx);

        // Extract any events that were waiting for this event to complete
        let dependent_events = ctx
            .dependency_graph
            .remove(&event_metadata)
            .unwrap_or_default();

        // Put context back before recursing
        self.context.insert(block_number, ctx);

        // Recursively process all dependent events
        for dependent_event in dependent_events {
            self.send_event_recursive(dependent_event);
        }
    }

    fn send_event(&mut self, event: TxQueueContents, ctx: &mut Context) {
        let iteration_id = event.iteration_id();
        let event_metadata = EventMetadata::from(&event);

        let queue = ctx.sent_events.entry(iteration_id).or_default();

        // Handle reorg-specific validation
        if let TxQueueContents::Reorg(_tx_execution_id, _) = &event {
            match queue.pop_back() {
                Some(last_sent_event) if !last_sent_event.cancel_each_other(&event_metadata) => {
                    // If the reorg event is not cancelling the previous transaction, it means
                    // there was an error in our logic. This case should never happen, but we need to
                    // handle it gracefully.
                    error!(
                        target = "event_sequencing",
                        "Received reorg event which is not cancelling the previous transaction"
                    );
                    queue.push_back(last_sent_event);
                }
                // This case should never happen, but we need to handle it gracefully.
                None => {
                    error!(
                        target = "event_sequencing",
                        "Received reorg event without previous event"
                    );
                }
                _ => {} // Happy path: events cancel each other
            }
        }

        // Always send event and record metadata
        self.tx_sender.send(event);
        queue.push_back(event_metadata);
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
