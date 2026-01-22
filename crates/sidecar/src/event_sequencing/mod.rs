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

#[derive(Clone, Copy)]
enum Origin {
    Main,
    After,
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
    /// Extracts the last `depth` transaction hashes from the sent events queue.
    ///
    /// Used to validate that a reorg request's `tx_hashes` match what was actually sent.
    /// The queue may contain non-transaction events (e.g., `NewIteration`), which are
    /// skipped when collecting hashes.
    ///
    /// Returns `None` if:
    /// - `depth` exceeds platform limits (`usize::MAX`)
    /// - There are fewer than `depth` transactions in the queue
    ///
    /// Returns hashes in chronological order (oldest first), matching the
    /// `ReorgRequest.tx_hashes` format.
    fn collect_tail_tx_hashes(queue: &VecDeque<EventMetadata>, depth: u64) -> Option<Vec<TxHash>> {
        let depth = usize::try_from(depth).ok()?;
        if depth == 0 {
            return Some(Vec::new());
        }

        // Collect hashes by iterating backwards (newest first), then reverse
        let mut hashes: Vec<_> = queue
            .iter()
            .rev()
            .filter_map(EventMetadata::tx_hash)
            .take(depth)
            .collect();

        if hashes.len() == depth {
            // Reverse to get chronological order (oldest first)
            hashes.reverse();
            Some(hashes)
        } else {
            None
        }
    }

    /// Validates a reorg request and applies it by removing events from the queue.
    ///
    /// Returns `true` if the reorg is valid and was applied, `false` otherwise.
    /// On success, cancelled events are pushed to `cancelled_events`.
    fn validate_and_apply_reorg(
        queue: &mut VecDeque<EventMetadata>,
        reorg_depth: u64,
        reorg_tx_hashes: &[TxHash],
        event_metadata: &EventMetadata,
        cancelled_events: &mut Vec<EventMetadata>,
    ) -> bool {
        // Validation 1: depth must be positive
        if reorg_depth == 0 {
            error!(
                target = "event_sequencing",
                "Received reorg event with zero depth"
            );
            return false;
        }

        let Ok(reorg_depth_usize) = usize::try_from(reorg_depth) else {
            error!(
                target = "event_sequencing",
                depth = reorg_depth,
                "Reorg depth exceeds platform limits"
            );
            return false;
        };

        // Validation 2: tx_hashes length must match depth
        if reorg_tx_hashes.len() != reorg_depth_usize {
            error!(
                target = "event_sequencing",
                depth = reorg_depth,
                tx_hashes_len = reorg_tx_hashes.len(),
                "Received reorg event with mismatched tx_hashes length"
            );
            return false;
        }

        let tx_count = queue.iter().filter(|e| e.is_transaction()).count();

        // Validation 3: must have enough transactions to reorg
        if tx_count < reorg_depth_usize {
            error!(
                target = "event_sequencing",
                "Received reorg event without enough transactions"
            );
            return false;
        }

        // Validation 4: tx_hashes must match the tail of sent transactions
        if Self::collect_tail_tx_hashes(queue, reorg_depth).as_deref() != Some(reorg_tx_hashes) {
            error!(
                target = "event_sequencing",
                "Received reorg event with tx_hashes that do not match sent events"
            );
            return false;
        }

        // Validation 5: last sent event must be cancellable by this reorg
        let Some(last_sent_event) = queue.pop_back() else {
            error!(
                target = "event_sequencing",
                "Received reorg event without previous event"
            );
            return false;
        };

        if !last_sent_event.cancel_each_other(event_metadata) {
            // Last event doesn't match - restore it and reject reorg
            queue.push_back(last_sent_event);
            return false;
        }

        // All validations passed - remove `depth` events from sent queue
        cancelled_events.push(last_sent_event);
        for _ in 1..reorg_depth_usize {
            if let Some(removed) = queue.pop_back() {
                cancelled_events.push(removed);
            }
        }
        true
    }

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

        // Handle reorg events.
        //
        // A reorg can arrive in two scenarios:
        // 1. AFTER the transactions it cancels have been sent - we can process immediately
        // 2. BEFORE the transactions it cancels - we queue it in the dependency graph
        //
        // For a depth-N reorg at index I, it cancels transactions from index (I-N+1) to I.
        // The reorg's `tx_execution_id.index` is the index of the LAST (newest) transaction
        // being cancelled.
        if event_metadata.is_reorg() {
            let reorg_depth = match &event_metadata {
                EventMetadata::Reorg { tx_hashes, .. } => tx_hashes.len() as u64,
                _ => 0,
            };

            // Check if we have enough sent transactions to cancel right now.
            // Two conditions must be met:
            // 1. We have at least `reorg_depth` transactions in the sent queue
            // 2. The most recent sent event matches the reorg's target (cancel_each_other)
            let context = self.get_context_mut(block_number, "handle_current_context_event")?;
            let can_cancel = context
                .sent_events
                .get(&event_iteration_id)
                .is_some_and(|queue| {
                    let tx_count =
                        u64::try_from(queue.iter().filter(|e| e.is_transaction()).count())
                            .unwrap_or(0);
                    tx_count >= reorg_depth
                        && queue
                            .back()
                            .is_some_and(|last| last.cancel_each_other(&event_metadata))
                });

            if can_cancel {
                // Scenario 1: All transactions to cancel have been sent. Process the reorg
                // immediately - `send_event_recursive` will validate hashes and remove events.
                self.send_event_recursive(event, &event_metadata)?;
            } else if let Some(previous_event) = event_metadata.calculate_previous_event() {
                // Scenario 2: Reorg arrived before its target transaction(s). Queue it in the
                // dependency graph. When the target transaction arrives, `handle_sequential_event`
                // will detect the pending reorg and cancel both events.
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
            block_number,
        )?;

        Ok(())
    }

    /// Handles sequential events (transactions and commit heads).
    ///
    /// This function handles the case where a reorg arrives BEFORE the transaction it cancels.
    /// When a transaction arrives, we check if there's a pending reorg in the dependency graph
    /// that would cancel it. If so, both the transaction and reorg are dropped (they cancel
    /// each other out).
    fn handle_sequential_event(
        &mut self,
        event: TxQueueContents,
        event_metadata: EventMetadata,
        previous_sent_event: &EventMetadata,
        previous_event: EventMetadata,
        block_number: U256,
    ) -> Result<(), EventSequencingError> {
        if *previous_sent_event == previous_event
            && Self::check_previous_send_event_against_current_event(
                previous_sent_event,
                &event_metadata,
            )
        {
            // Before sending this transaction, check if a reorg arrived earlier that
            // would cancel it. This handles the "reorg before transaction" scenario.
            //
            // Note: For depth-1 reorgs, this is straightforward - the reorg and transaction
            // cancel each other. For depth-N reorgs (N > 1), the reorg waits for its target
            // transaction (the last one it cancels). Earlier transactions in the reorg range
            // will have already been sent, and `send_event_recursive` handles removing them.
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
                // A reorg was waiting for this transaction. Remove both from the system:
                // - The reorg is removed from the dependency graph
                // - The transaction is not sent (dropped here)
                let context = self.get_context_mut(block_number, "handle_sequential_event")?;
                if let Some(dependents) = context.dependency_graph.get_mut(&previous_event) {
                    dependents.retain(|existing, _| {
                        let existing_meta: EventMetadata = existing.into();
                        !(existing_meta.is_reorg()
                            && event_metadata.cancel_each_other(&existing_meta))
                    });
                }
                return Ok(());
            }

            self.send_event_recursive(event, &event_metadata)?;
        } else {
            // Previous event hasn't been sent yet - queue this event as a dependency
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

    /// Helper: is a reorg *currently* eligible to fire (i.e. would cancel the last sent event)?
    fn is_reorg_ready_now(
        &self,
        block_number: U256,
        iteration_id: u64,
        reorg_meta: &EventMetadata,
    ) -> bool {
        self.context
            .get(&block_number)
            .and_then(|ctx| ctx.sent_events.get(&iteration_id))
            .and_then(|q| q.back())
            .is_some_and(|last| last.cancel_each_other(reorg_meta))
    }

    /// Helper: deterministic dependent ordering (important for tests + correctness).
    fn dependent_sort_key(meta: &CompleteEventMetadata) -> (u8, u64) {
        match meta {
            CompleteEventMetadata::NewIteration { iteration_id, .. } => (0, *iteration_id),
            CompleteEventMetadata::Transaction { index, .. } => (1, *index),
            // Reorg must come after the tx at the same index.
            CompleteEventMetadata::Reorg { index, .. } => (2, *index),
            CompleteEventMetadata::CommitHead { .. } => (3, 0),
        }
    }

    /// Recursively sends an event and all its dependent events from the dependency graph.
    ///
    /// For reorg events, this function performs validation before sending to the engine:
    /// 1. Validates `depth > 0`
    /// 2. Validates `tx_hashes.len() == depth`
    /// 3. Validates `tx_hashes` match the tail of sent transactions (integrity check)
    /// 4. Validates the last sent event can be cancelled by this reorg
    ///
    /// If validation passes, removes `depth` events from the sent queue and forwards
    /// the reorg to the engine for state rollback.
    /// IMPORTANT: We must not drop dependents that are not yet eligible (e.g., a reorg that cannot
    /// cancel the current last-sent tx, or a tx whose `prev_tx_hash` doesn't match). Those must be
    /// reinserted into the same bucket.
    #[allow(clippy::too_many_lines)]
    fn send_event_recursive(
        &mut self,
        event: TxQueueContents,
        event_metadata: &EventMetadata,
    ) -> Result<bool, EventSequencingError> {
        let block_number = event.block_number();
        let iteration_id = event.iteration_id();
        let is_commit_head = event_metadata.is_commit_head();
        let mut cancelled_events = Vec::new();

        // Clone tx_sender before getting mutable context to avoid borrow conflicts
        let tx_sender = self.tx_sender.clone();

        // Get context and extract what we need before sending
        let context = self.get_context_mut(block_number, "send_event_recursive")?;

        let (reorg_depth, reorg_tx_hashes) = match &event {
            TxQueueContents::Reorg(reorg) => {
                (reorg.depth() as u64, Some(reorg.tx_hashes.as_slice()))
            }
            _ => (0, None),
        };

        // Reorg validation: ensures the reorg request is valid before forwarding to engine.
        // This is defense-in-depth - the engine also validates, but catching issues here
        // prevents unnecessary work and provides clearer error attribution.
        let should_send = if event_metadata.is_reorg() {
            let Some(reorg_tx_hashes) = reorg_tx_hashes else {
                error!(target = "event_sequencing", "Reorg event metadata mismatch");
                return Ok(false);
            };
            let queue = context.sent_events.entry(iteration_id).or_default();

            Self::validate_and_apply_reorg(
                queue,
                reorg_depth,
                reorg_tx_hashes,
                event_metadata,
                &mut cancelled_events,
            )
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

        // We are going to drain dependency buckets, so we must be careful to reinsert leftovers.
        let origin_main = event_metadata.clone();

        // Drain dependents waiting on the current event
        let deps_main: HashMap<CompleteEventMetadata, TxQueueContents> = context
            .dependency_graph
            .remove(event_metadata)
            .unwrap_or_default();

        if event_metadata.is_reorg() && should_send {
            for cancelled in &cancelled_events {
                context.dependency_graph.remove(cancelled);
            }
        }

        // Special handling for reorgs: after removing the cancelled TX, check if there are
        // events waiting for the new last event in sent_events.
        let mut origin_after: Option<EventMetadata> = None;
        let mut deps_after: HashMap<CompleteEventMetadata, TxQueueContents> = HashMap::new();

        if event_metadata.is_reorg()
            && should_send
            && let Some(new_last_event) = context
                .sent_events
                .get(&iteration_id)
                .and_then(|queue| queue.back())
                .cloned()
        {
            origin_after = Some(new_last_event.clone());
            deps_after = context
                .dependency_graph
                .remove(&new_last_event)
                .unwrap_or_default();
        }

        // Drop the mutable borrow before recursing
        let _ = context;

        // Build a combined list of dependents with deterministic ordering.
        let mut combined: Vec<(Origin, CompleteEventMetadata, TxQueueContents)> = Vec::new();
        combined.extend(deps_main.into_iter().map(|(m, e)| (Origin::Main, m, e)));
        combined.extend(deps_after.into_iter().map(|(m, e)| (Origin::After, m, e)));

        combined.sort_by(|a, b| {
            let ka = Self::dependent_sort_key(&a.1);
            let kb = Self::dependent_sort_key(&b.1);
            ka.cmp(&kb)
        });

        // Leftovers that must be reinserted (NOT dropped).
        let mut leftovers_main: HashMap<CompleteEventMetadata, TxQueueContents> = HashMap::new();
        let mut leftovers_after: HashMap<CompleteEventMetadata, TxQueueContents> = HashMap::new();

        let mut commit_head_found = false;

        for (origin, dependent_metadata, dependent_event) in combined {
            let dep_meta: EventMetadata = (&dependent_metadata).into();

            // If this dependent is a reorg but it cannot cancel *right now*, keep it queued.
            if dep_meta.is_reorg()
                && !self.is_reorg_ready_now(block_number, iteration_id, &dep_meta)
            {
                match origin {
                    Origin::Main => {
                        leftovers_main.insert(dependent_metadata, dependent_event);
                    }
                    Origin::After => {
                        leftovers_after.insert(dependent_metadata, dependent_event);
                    }
                }
                continue;
            }

            // Validate prev_tx_hash if present; if it doesn't match, keep queued.
            if let (
                EventMetadata::Transaction {
                    tx_hash: sent_hash, ..
                },
                TxQueueContents::Tx(queue),
            ) = (event_metadata, &dependent_event)
                && let Some(prev_hash) = queue.prev_tx_hash
                && prev_hash != *sent_hash
            {
                match origin {
                    Origin::Main => {
                        leftovers_main.insert(dependent_metadata, dependent_event);
                    }
                    Origin::After => {
                        leftovers_after.insert(dependent_metadata, dependent_event);
                    }
                }
                continue;
            }

            // Try to send it.
            if self.send_event_recursive(dependent_event, &(&dependent_metadata).into())? {
                commit_head_found = true;
                // Remaining dependents are irrelevant if the commit head cleaned up context.
                break;
            }
        }

        // If a commit head was found downstream, or the context got cleaned up, do not reinsert.
        if is_commit_head || commit_head_found || !self.context.contains_key(&block_number) {
            // Handle CommitHead logic if the current event itself is a commit head.
            if is_commit_head {
                self.handle_commit_head_completion(event_metadata)?;
            }
            return Ok(is_commit_head || commit_head_found);
        }

        // Reinsert leftovers back into the same buckets (so we don't drop events).
        if !leftovers_main.is_empty()
            && let Some(ctx) = self.context.get_mut(&block_number)
        {
            ctx.dependency_graph
                .entry(origin_main)
                .or_default()
                .extend(leftovers_main);
        }

        if let Some(origin2) = origin_after
            && !leftovers_after.is_empty()
            && let Some(ctx) = self.context.get_mut(&block_number)
        {
            ctx.dependency_graph
                .entry(origin2)
                .or_default()
                .extend(leftovers_after);
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
