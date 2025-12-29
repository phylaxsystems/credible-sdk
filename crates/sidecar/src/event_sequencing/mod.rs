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
    event_sequencing::event_metadata::{
        CompleteEventMetadata,
        EventMetadata,
    },
    utils::ErrorRecoverability,
};
use alloy::primitives::{
    TxHash,
    U256,
};
use std::{
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
        VecDeque,
    },
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::{
        Duration,
        Instant,
    },
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{
    error,
    info,
    warn,
};

/// Timeout for recv - threads will check for the shutdown flag at this interval
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// The event sequencing processes events from the transport layer and ensures the correct ordering
/// before sending the event to the core engine
pub struct EventSequencing {
    /// Channel on which the event sequencing receives events.
    tx_receiver: TransactionQueueReceiver,
    /// Channel on which the core engine receives events.
    tx_sender: TransactionQueueSender,
    /// A boolean to track if we received the first commit head
    first_commit_head_received: bool,
    /// The current head of the chain: The last committed head.
    current_head: U256,
    /// Context for each block.
    context: BTreeMap<U256, Context>,
}

/// The `Context` struct contains the context for a block. It is used to track the state of the block
/// and the events that have been dispatched during the block build.
#[derive(Debug, Default)]
struct Context {
    /// Sent events per iteration
    sent_events: HashMap<u64, VecDeque<EventMetadata>>,
    /// Keep track of pending events which haven't been sent yet to the engine
    dependency_graph: HashMap<EventMetadata, HashMap<CompleteEventMetadata, TxQueueContents>>,
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
            first_commit_head_received: false,
            current_head: U256::ZERO,
            context: BTreeMap::new(),
        }
    }

    /// Spawns event sequencing on a dedicated OS thread with blocking receive.
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        self,
        shutdown: Arc<AtomicBool>,
    ) -> Result<
        (
            JoinHandle<Result<(), EventSequencingError>>,
            oneshot::Receiver<Result<(), EventSequencingError>>,
        ),
        std::io::Error,
    > {
        let (tx, rx) = oneshot::channel();

        let handle = std::thread::Builder::new()
            .name("sidecar-sequencing".into())
            .spawn(move || {
                let mut sequencing = self;
                let result = sequencing.run_blocking(shutdown);
                let _ = tx.send(result.clone());
                result
            })?;

        Ok((handle, rx))
    }

    /// Blocking run loop with shutdown support
    #[allow(clippy::needless_pass_by_value)]
    fn run_blocking(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), EventSequencingError> {
        loop {
            // Check for the shutdown flag
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "event_sequencing", "Shutdown signal received");
                return Ok(());
            }

            // Use recv_timeout so we can periodically check the shutdown flag
            let event = match self.tx_receiver.recv_timeout(RECV_TIMEOUT) {
                Ok(event) => event,
                Err(flume::RecvTimeoutError::Timeout) => {
                    // No event, loop back and check for the shutdown flag
                    continue;
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    info!(target = "event_sequencing", "Channel disconnected");
                    return Err(EventSequencingError::ChannelClosed);
                }
            };

            // Check shutdown before processing
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "event_sequencing", "Shutdown signal received");
                return Ok(());
            }

            self.process_event(event)?;
        }
    }

    /// An asynchronous function that continuously processes events from a transaction queue channel
    /// and forwards them to a core engine for further handling.
    #[cfg(any(test, feature = "bench-utils"))]
    pub async fn run(&mut self) -> Result<(), EventSequencingError> {
        loop {
            let event = self.receive_event().await?;
            self.process_event(event)?;
        }
    }

    /// Receives an event from the transaction queue, yielding when empty.
    #[cfg(any(test, feature = "bench-utils"))]
    async fn receive_event(&mut self) -> Result<TxQueueContents, EventSequencingError> {
        loop {
            match self.tx_receiver.try_recv() {
                Ok(event) => return Ok(event),
                Err(flume::TryRecvError::Empty) => {
                    // Channel is empty, yield to allow other tasks to run
                    tokio::task::yield_now().await;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    error!(
                        target = "event_sequencing",
                        "Transaction queue channel disconnected"
                    );
                    return Err(EventSequencingError::ChannelClosed);
                }
            }
        }
    }

    /// Main event processing logic that routes to appropriate handlers.
    fn process_event(&mut self, event: TxQueueContents) -> Result<(), EventSequencingError> {
        let event_block_number = event.block_number();

        // Create EventMetadata once
        let event_metadata = EventMetadata::from(&event);
        info!(
            target = "event_sequencing",
            %event_block_number,
            current_head = %self.current_head,
            event_metadata = ?event_metadata,
            "Event received"
        );

        // Ignore events older than or equal to the current head
        if self.current_head >= event_block_number && self.first_commit_head_received {
            error!(
                target = "event_sequencing",
                %event_block_number,
                current_head = %self.current_head,
                "Received event for block older than current head"
            );
            return Ok(());
        }

        self.context.entry(event_block_number).or_default();

        // Commit head events are always sent immediately
        if event_metadata.is_commit_head() {
            self.first_commit_head_received = true;
            self.send_event_recursive(event, &event_metadata)?;
            return Ok(());
        }

        if self.current_head + U256::from(1) == event_block_number
            && self.first_commit_head_received
        {
            self.handle_current_context_event(event, event_metadata, event_block_number)?;
        } else if event_block_number > self.current_head + U256::from(1) {
            self.handle_future_context_event(event, event_metadata, event_block_number)?;
        }

        Ok(())
    }

    /// Handles events in the current execution context.
    fn handle_current_context_event(
        &mut self,
        event: TxQueueContents,
        event_metadata: EventMetadata,
        block_number: U256,
    ) -> Result<(), EventSequencingError> {
        let event_iteration_id = event.iteration_id();

        let context = self.get_context_mut(block_number, "handle_current_context_event")?;

        let Some(previous_sent_event) = context
            .sent_events
            .get_mut(&event_iteration_id)
            .and_then(|v| v.back())
            .cloned()
        else {
            // If we do not have a previously sent event in the iteration...
            if event_metadata.is_new_iteration() {
                self.send_event_recursive(event, &event_metadata)?;
            } else if let Some(previous_event) = event_metadata.calculate_previous_event() {
                // Transaction arrived before NewIteration - queue it!
                self.add_to_dependency_graph(block_number, previous_event, event_metadata, event)?;
            }
            return Ok(());
        };

        // Handle reorg events
        if event_metadata.is_reorg() {
            // Check if we can cancel the last sent event right now
            let context = self.get_context_mut(block_number, "handle_current_context_event")?;
            let can_cancel = context
                .sent_events
                .get(&event_iteration_id)
                .and_then(|q| q.back())
                .is_some_and(|last| last.cancel_each_other(&event_metadata));

            if can_cancel {
                // The last sent event is the TX we want to cancel
                self.send_event_recursive(event, &event_metadata)?;
            } else if let Some(previous_event) = event_metadata.calculate_previous_event() {
                self.add_to_dependency_graph(block_number, previous_event, event_metadata, event)?;
            }
            return Ok(());
        }

        let Some(previous_event) = event_metadata.calculate_previous_event() else {
            warn!(
                target = "event_sequencing",
                "Received new iteration event for block 0"
            );
            self.send_event_recursive(event, &event_metadata)?;
            return Ok(());
        };

        // Handle regular transactions and commit head events
        self.handle_sequential_event(
            event,
            event_metadata,
            &previous_sent_event,
            previous_event,
            event_iteration_id,
            block_number,
        )?;

        Ok(())
    }

    /// Handles sequential events (transactions and commit heads).
    fn handle_sequential_event(
        &mut self,
        event: TxQueueContents,
        event_metadata: EventMetadata,
        previous_sent_event: &EventMetadata,
        previous_event: EventMetadata,
        event_iteration_id: u64,
        block_number: U256,
    ) -> Result<(), EventSequencingError> {
        if *previous_sent_event == previous_event
            && Self::check_previous_send_event_against_current_event(
                previous_sent_event,
                &event_metadata,
            )
        {
            // Check if there's a pending reorg that would cancel this event
            let context = self.get_context_mut(block_number, "handle_sequential_event")?;
            let has_cancelling_reorg =
                context
                    .dependency_graph
                    .get(&previous_event)
                    .is_some_and(|dependents| {
                        dependents.keys().any(|existing| {
                            let existing_meta: EventMetadata = existing.into();
                            existing_meta.is_reorg()
                                && event_metadata.cancel_each_other(&existing_meta)
                        })
                    });

            if has_cancelling_reorg {
                // Remove the reorg from the dependency graph
                let context = self.get_context_mut(block_number, "handle_sequential_event")?;
                if let Some(dependents) = context.dependency_graph.get_mut(&previous_event) {
                    dependents.retain(|existing, _| {
                        let existing_meta: EventMetadata = existing.into();
                        !(existing_meta.is_reorg()
                            && event_metadata.cancel_each_other(&existing_meta))
                    });
                }
                // Don't send this TX, as it's been reorged
                return Ok(());
            }

            self.send_event_recursive(event, &event_metadata)?;
        } else {
            // Queue the event as a dependency
            self.add_to_dependency_graph(block_number, previous_event, event_metadata, event)?;
        }
        Ok(())
    }

    /// Checks if a previous sent event is valid against the current event.
    fn check_previous_send_event_against_current_event(
        previous_sent_event: &EventMetadata,
        current_event: &EventMetadata,
    ) -> bool {
        if current_event.is_transaction() && previous_sent_event.is_transaction() {
            previous_sent_event.tx_hash() == current_event.prev_tx_hash()
        } else if current_event.is_transaction() && previous_sent_event.is_new_iteration() {
            current_event.prev_tx_hash().is_none()
        } else {
            true
        }
    }

    /// Handles events in a future execution context by adding them to the dependency graph.
    fn handle_future_context_event(
        &mut self,
        event: TxQueueContents,
        event_metadata: EventMetadata,
        block_number: U256,
    ) -> Result<(), EventSequencingError> {
        if let Some(previous_event) = event_metadata.calculate_previous_event() {
            self.add_to_dependency_graph(block_number, previous_event, event_metadata, event)?;
        }
        Ok(())
    }

    /// Adds an event to the dependency graph, handling cancellation logic.
    fn add_to_dependency_graph(
        &mut self,
        block_number: U256,
        previous_event: EventMetadata,
        event_metadata: EventMetadata,
        event: TxQueueContents,
    ) -> Result<(), EventSequencingError> {
        info!(
            target = "event_sequencing",
            current_head = %self.current_head,
            event_metadata = ?event_metadata,
            "Event added to the dependency graph"
        );

        let context = self.get_context_mut(block_number, "add_to_dependency_graph")?;

        match context.dependency_graph.entry(previous_event) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let dependents = entry.get_mut();

                // Check and remove cancelling events in one pass
                let mut found_cancel = false;
                dependents.retain(|existing, _| {
                    if event_metadata.cancel_each_other(&existing.into()) {
                        found_cancel = true;
                        false // Remove this one
                    } else {
                        true // Keep
                    }
                });

                // Only insert if no cancellation occurred
                if !found_cancel {
                    dependents.insert(event_metadata.into(), event);
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(HashMap::from([(event_metadata.into(), event)]));
            }
        }

        Ok(())
    }

    /// Recursively sends an event and all its dependent events from the dependency graph.
    fn send_event_recursive(
        &mut self,
        event: TxQueueContents,
        event_metadata: &EventMetadata,
    ) -> Result<bool, EventSequencingError> {
        let block_number = event.block_number();
        let iteration_id = event.iteration_id();
        let is_commit_head = event_metadata.is_commit_head();

        // Clone tx_sender before getting mutable context to avoid borrow conflicts
        let tx_sender = self.tx_sender.clone();

        // Get context and extract what we need before sending
        let context = self.get_context_mut(block_number, "send_event_recursive")?;

        // Perform reorg validation inline to avoid borrow issues
        let should_send = if event_metadata.is_reorg() {
            let queue = context.sent_events.entry(iteration_id).or_default();
            if let Some(last_sent_event) = queue.pop_back() {
                if last_sent_event.cancel_each_other(event_metadata) {
                    // If the last event sent is canceled by the reorg, send the reorg event to
                    // the core engine and do not push back the last event sent to the `sent_events`
                    // queue.
                    true
                } else {
                    // If the last event sent is not canceled by the reorg, restore the last event
                    // and do not send the reorg event, as it will not cancel anything.
                    queue.push_back(last_sent_event);
                    false
                }
            } else {
                error!(
                    target = "event_sequencing",
                    "Received reorg event without previous event"
                );
                false
            }
        } else {
            true
        };

        // Send event
        if should_send {
            info!(
                target = "event_sequencing",
                event_metadata = ?event_metadata,
                "Event sent to the core engine"
            );
            tx_sender.send(event).map_err(|e| {
                error!(
                    target = "event_sequencing",
                    error = ?e,
                    "Failed to send event to core engine"
                );
                EventSequencingError::SendFailed
            })?;

            // Record the send event
            if !event_metadata.is_reorg() {
                context
                    .sent_events
                    .entry(iteration_id)
                    .or_default()
                    .push_back(event_metadata.clone());
            }
        }

        // Extract any events that were waiting for this event to complete
        let mut dependent_events = context
            .dependency_graph
            .remove(event_metadata)
            .unwrap_or_default();

        // Special handling for reorgs: after removing the cancelled TX, check if there are
        // events waiting for the new last event in sent_events
        if event_metadata.is_reorg()
            && should_send
            && let Some(new_last_event) = context
                .sent_events
                .get(&iteration_id)
                .and_then(|queue| queue.back())
                .cloned()
        {
            // Get any events that were waiting for the new last event
            if let Some(waiting_events) = context.dependency_graph.remove(&new_last_event) {
                dependent_events.extend(waiting_events);
            }
        }

        // Drop the mutable borrow before recursing
        let _ = context;

        let mut commit_head_found = false;
        // Recursively process all dependent events
        for (dependent_metadata, dependent_event) in dependent_events {
            // Validate prev_tx_hash if present
            if let (
                EventMetadata::Transaction {
                    tx_hash: sent_hash, ..
                },
                TxQueueContents::Tx(queue, _),
            ) = (event_metadata, &dependent_event)
                && let Some(prev_hash) = queue.prev_tx_hash
                && prev_hash != *sent_hash
            {
                // Skip this dependent - its prev_tx_hash doesn't match
                continue;
            }

            if self.send_event_recursive(dependent_event, &(&dependent_metadata).into())? {
                commit_head_found = true;
                break;
            }
        }

        // After all dependencies are processed, handle CommitHead logic
        if is_commit_head {
            self.handle_commit_head_completion(event_metadata)?;
        }

        Ok(is_commit_head || commit_head_found)
    }

    /// Handles the completion of a `CommitHead` event: cleanup and transition to the next block
    fn handle_commit_head_completion(
        &mut self,
        commit_head_event: &EventMetadata,
    ) -> Result<(), EventSequencingError> {
        let block_number = commit_head_event.block_number();
        info!(
            target = "event_sequencing",
            %block_number, "CommitHead completed, cleaning up context and moving to next block"
        );

        // Clean up context for the completed block
        self.context = self.context.split_off(&(block_number + U256::from(1)));

        // Update current_head to the next block
        self.current_head = block_number;

        // Try to start processing the next block if we have queued events
        self.try_start_next_block(commit_head_event)?;

        Ok(())
    }

    /// Attempts to start processing the next block if we have a `NewIteration` event queued
    fn try_start_next_block(
        &mut self,
        commit_head_event: &EventMetadata,
    ) -> Result<(), EventSequencingError> {
        let next_block = self.current_head + U256::from(1);

        // Check if we have events queued for the next block
        let Some(context) = self.context.get_mut(&next_block) else {
            // Nothing to run
            return Ok(());
        };

        let all_new_iterations = context
            .dependency_graph
            .get(commit_head_event)
            .map(|a| {
                a.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Drop the immutable borrow before we try to send
        let _ = context;

        // Start processing root NewIteration events
        for (meta, event) in all_new_iterations {
            info!(
                target = "event_sequencing",
                block = %next_block,
                "Starting next block processing with queued event"
            );
            self.send_event_recursive(event.clone(), &(&meta).into())?;
        }

        Ok(())
    }

    /// Helper method to get mutable context with consistent error handling.
    #[inline]
    fn get_context_mut(
        &mut self,
        block_number: U256,
        context_name: &'static str,
    ) -> Result<&mut Context, EventSequencingError> {
        self.context
            .get_mut(&block_number)
            .ok_or(EventSequencingError::MissingContext {
                block_number,
                context: context_name,
            })
    }
}

#[derive(Debug, Error, Clone)]
pub enum EventSequencingError {
    #[error("Transaction queue channel closed")]
    ChannelClosed,
    #[error("Missing context for block {block_number} in {context}")]
    MissingContext {
        block_number: U256,
        context: &'static str,
    },
    #[error("Failed to send event to core engine")]
    SendFailed,
}

impl From<&EventSequencingError> for ErrorRecoverability {
    fn from(e: &EventSequencingError) -> Self {
        match e {
            EventSequencingError::ChannelClosed
            | EventSequencingError::MissingContext { .. }
            | EventSequencingError::SendFailed => ErrorRecoverability::Recoverable,
        }
    }
}
