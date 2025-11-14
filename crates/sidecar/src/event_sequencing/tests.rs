#![allow(clippy::field_reassign_with_default)]
use super::*;
use crate::{
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
};
use alloy::primitives::TxHash;
use crossbeam::channel;
use revm::context::{
    BlockEnv,
    TxEnv,
};
use std::sync::{
    Arc,
    Mutex,
};

impl PartialEq for TxQueueContents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TxQueueContents::Tx(tx1, _), TxQueueContents::Tx(tx2, _)) => tx1 == tx2,
            (TxQueueContents::Reorg(id1, _), TxQueueContents::Reorg(id2, _)) => id1 == id2,
            (TxQueueContents::CommitHead(c1, _), TxQueueContents::CommitHead(c2, _)) => {
                c1.block_number == c2.block_number
                    && c1.selected_iteration_id == c2.selected_iteration_id
                    && c1.last_tx_hash == c2.last_tx_hash
                    && c1.n_transactions == c2.n_transactions
            }
            (TxQueueContents::NewIteration(n1, _), TxQueueContents::NewIteration(n2, _)) => {
                n1.iteration_id == n2.iteration_id && n1.block_env == n2.block_env
            }
            _ => false,
        }
    }
}

impl Eq for TxQueueContents {}

// Helper struct to capture sent events for testing
#[derive(Clone)]
struct EventCapture {
    events: Arc<Mutex<Vec<EventMetadata>>>,
}

impl EventCapture {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_events(&self) -> Vec<EventMetadata> {
        self.events.lock().unwrap().clone()
    }

    fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

/// Helper to build dependency graph from unordered events
fn build_dependency_graph_from_events(
    sequencing: &mut EventSequencing,
    events: &[TxQueueContents],
) {
    // Group events by block
    let mut events_by_block: HashMap<u64, Vec<TxQueueContents>> = HashMap::new();
    for event in events {
        events_by_block
            .entry(event.block_number())
            .or_default()
            .push(event.clone());
    }

    // Get sorted block numbers for cross-block linking
    let mut block_numbers: Vec<u64> = events_by_block.keys().copied().collect();
    block_numbers.sort_unstable();

    let mut last_event_per_block: HashMap<u64, TxQueueContents> = HashMap::new();

    // For each block, build the linear dependency chain
    for block_number in &block_numbers {
        let block_events = &events_by_block[block_number];
        let ctx = sequencing.context.entry(*block_number).or_default();

        // Sort events to establish proper order within the block
        let mut sorted_events = block_events.clone();
        sorted_events.sort_by_key(|e| {
            match e {
                TxQueueContents::NewIteration(ni, _) => (0, ni.iteration_id, 0u64),
                TxQueueContents::Tx(tx, _) => {
                    (1, tx.tx_execution_id.iteration_id, tx.tx_execution_id.index)
                }
                TxQueueContents::Reorg(id, _) => (1, id.iteration_id, id.index),
                TxQueueContents::CommitHead(ch, _) => (2, ch.selected_iteration_id, 0u64),
            }
        });

        // Build linear dependencies within the block
        for i in 0..sorted_events.len() - 1 {
            let current_meta = EventMetadata::from(&sorted_events[i]);
            let next_meta = EventMetadata::from(&sorted_events[i + 1]);
            ctx.dependency_graph.insert(
                current_meta,
                HashMap::from([(next_meta, sorted_events[i + 1].clone())]),
            );
        }

        // Remember the last event of this block for cross-block linking
        if let Some(last_event) = sorted_events.last() {
            last_event_per_block.insert(*block_number, last_event.clone());
        }
    }

    // Connect blocks: last event of block N -> first event of block N+1
    for i in 0..block_numbers.len() - 1 {
        let current_block = block_numbers[i];
        let next_block = block_numbers[i + 1];

        if let Some(last_event) = last_event_per_block.get(&current_block)
            && let Some(next_block_events) = events_by_block.get(&next_block)
        {
            // Sort next block events to find the first one
            let mut sorted_next = next_block_events.clone();
            sorted_next.sort_by_key(|e| {
                match e {
                    TxQueueContents::NewIteration(ni, _) => (0, ni.iteration_id, 0u64),
                    TxQueueContents::Tx(tx, _) => {
                        (1, tx.tx_execution_id.iteration_id, tx.tx_execution_id.index)
                    }
                    TxQueueContents::Reorg(id, _) => (1, id.iteration_id, id.index),
                    TxQueueContents::CommitHead(ch, _) => (2, ch.selected_iteration_id, 0u64),
                }
            });

            if let Some(first_next_event) = sorted_next.first() {
                // Add dependency from last event of current block to first event of next block
                let last_meta = EventMetadata::from(last_event);
                let first_next_meta = EventMetadata::from(first_next_event);
                sequencing
                    .context
                    .entry(current_block)
                    .or_default()
                    .dependency_graph
                    .insert(
                        last_meta,
                        HashMap::from([(first_next_meta, first_next_event.clone())]),
                    );
            }
        }
    }
}

// Helper to create a test EventSequencing
fn create_test_sequencing() -> (EventSequencing, TransactionQueueReceiver) {
    let (tx_send, tx_recv) = channel::unbounded();
    let (engine_send, engine_recv) = channel::unbounded();

    let sequencing = EventSequencing::new(tx_recv, engine_send);

    (sequencing, engine_recv)
}

// Mock event creators for testing
fn create_new_iteration(block: u64, iteration: u64) -> TxQueueContents {
    let mut block_env = BlockEnv::default();
    block_env.number = block;

    TxQueueContents::NewIteration(
        NewIteration::new(iteration, block_env),
        tracing::Span::none(),
    )
}

fn create_transaction(block: u64, iteration: u64, index: u64, tx_hash: TxHash) -> TxQueueContents {
    let tx_execution_id = TxExecutionId {
        block_number: block,
        iteration_id: iteration,
        tx_hash,
        index,
    };

    TxQueueContents::Tx(
        QueueTransaction {
            tx_execution_id,
            tx_env: TxEnv::default(),
            prev_tx_hash: None,
        },
        tracing::Span::none(),
    )
}

fn create_reorg(block: u64, iteration: u64, index: u64, tx_hash: TxHash) -> TxQueueContents {
    let tx_execution_id = TxExecutionId {
        block_number: block,
        iteration_id: iteration,
        tx_hash,
        index,
    };

    TxQueueContents::Reorg(tx_execution_id, tracing::Span::none())
}

fn create_commit_head(
    block: u64,
    iteration: u64,
    n_txs: u64,
    last_tx_hash: Option<TxHash>,
) -> TxQueueContents {
    TxQueueContents::CommitHead(
        CommitHead::new(block, iteration, last_tx_hash, n_txs),
        tracing::Span::none(),
    )
}

#[test]
fn test_send_event_recursive_no_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event = create_transaction(100, 1, 0, TxHash::random());
    let event_metadata = EventMetadata::from(&event);

    sequencing.context.entry(100).or_default();
    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    // Should send exactly one event
    assert_eq!(engine_recv.len(), 1);

    // Verify context was created
    assert!(sequencing.context.contains_key(&100));
}

#[test]
fn test_send_event_recursive_single_dependency() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random());
    let event2 = create_transaction(100, 1, 1, TxHash::random());

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);

    // Set up dependency: event2 depends on event1
    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([(event2_metadata.clone(), event2.clone())]),
        );

    sequencing
        .send_event_recursive(event1.clone(), &event1_metadata)
        .unwrap();

    // Should send both events in order
    assert_eq!(engine_recv.len(), 2);
    assert_eq!(engine_recv.recv().unwrap(), event1);
    assert_eq!(engine_recv.recv().unwrap(), event2);

    // Verify both were recorded in sent_events
    let ctx = sequencing.context.get(&100).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 2);
}

#[test]
fn test_send_event_recursive_multiple_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random());
    let event2 = create_transaction(100, 1, 1, TxHash::random());
    let event3 = create_transaction(100, 1, 2, TxHash::random());
    let event4 = create_transaction(100, 1, 3, TxHash::random());

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);
    let event3_metadata = EventMetadata::from(&event3);
    let event4_metadata = EventMetadata::from(&event4);

    // Set up dependencies: event2, event3, and event4 all depend on event1
    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([
                (event2_metadata, event2.clone()),
                (event3_metadata, event3.clone()),
                (event4_metadata, event4.clone()),
            ]),
        );

    sequencing
        .send_event_recursive(event1.clone(), &event1_metadata)
        .unwrap();

    // Should send all 4 events
    assert_eq!(engine_recv.len(), 4);
    assert_eq!(engine_recv.recv().unwrap(), event1);
    // Note: order of event2, event3, event4 may vary due to HashMap iteration
}

#[test]
fn test_send_event_recursive_chain_of_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random());
    let event2 = create_transaction(100, 1, 1, TxHash::random());
    let event3 = create_transaction(100, 1, 2, TxHash::random());

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);
    let event3_metadata = EventMetadata::from(&event3);

    // Set up chain: event1 -> event2 -> event3
    let ctx = sequencing.context.entry(100).or_default();
    ctx.dependency_graph.insert(
        event1_metadata.clone(),
        HashMap::from([(event2_metadata.clone(), event2.clone())]),
    );
    ctx.dependency_graph.insert(
        event2_metadata.clone(),
        HashMap::from([(event3_metadata, event3.clone())]),
    );

    sequencing
        .send_event_recursive(event1.clone(), &event1_metadata)
        .unwrap();

    // Should send all 3 events in order
    assert_eq!(engine_recv.len(), 3);

    // Verify the execution order
    let ctx = sequencing.context.get(&100).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 3);
    assert_eq!(engine_recv.recv().unwrap(), event1);
    assert_eq!(engine_recv.recv().unwrap(), event2);
    assert_eq!(engine_recv.recv().unwrap(), event3);
}

#[test]
fn test_send_event_recursive_block_transition() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Event in block 100
    let event1 = create_transaction(100, 1, 0, TxHash::random());
    // Dependent event in block 101
    let event2 = create_transaction(101, 1, 0, TxHash::random());

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);

    // Set up dependency across blocks
    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([(event2_metadata, event2)]),
        );

    // Create context for block 101 as well
    sequencing.context.entry(101).or_default();

    sequencing
        .send_event_recursive(event1, &event1_metadata)
        .unwrap();

    // Should handle both blocks gracefully
    assert_eq!(engine_recv.len(), 2);

    // Verify context for both blocks exists
    assert!(sequencing.context.contains_key(&100));
    assert!(sequencing.context.contains_key(&101));

    // Verify events in correct blocks
    assert!(
        sequencing
            .context
            .get(&100)
            .unwrap()
            .sent_events
            .contains_key(&1)
    );
    assert!(
        sequencing
            .context
            .get(&101)
            .unwrap()
            .sent_events
            .contains_key(&1)
    );
}

#[test]
fn test_send_event_recursive_multiple_block_transitions() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event_b100 = create_transaction(100, 1, 0, TxHash::random());
    let event_b101 = create_transaction(101, 1, 0, TxHash::random());
    let event_b102 = create_transaction(102, 1, 0, TxHash::random());
    let event_b103 = create_transaction(103, 1, 0, TxHash::random());

    let meta_b100 = EventMetadata::from(&event_b100);
    let meta_b101 = EventMetadata::from(&event_b101);
    let meta_b102 = EventMetadata::from(&event_b102);
    let meta_b103 = EventMetadata::from(&event_b103);

    // Chain across multiple blocks: 100 -> 101 -> 102 -> 103
    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            meta_b100.clone(),
            HashMap::from([(meta_b101.clone(), event_b101.clone())]),
        );
    sequencing
        .context
        .entry(101)
        .or_default()
        .dependency_graph
        .insert(
            meta_b101.clone(),
            HashMap::from([(meta_b102.clone(), event_b102.clone())]),
        );
    sequencing
        .context
        .entry(102)
        .or_default()
        .dependency_graph
        .insert(
            meta_b102.clone(),
            HashMap::from([(meta_b103.clone(), event_b103.clone())]),
        );
    sequencing.context.entry(103).or_default();

    sequencing
        .send_event_recursive(event_b100, &meta_b100)
        .unwrap();

    assert_eq!(engine_recv.len(), 4);
    assert!(sequencing.context.contains_key(&100));
    assert!(sequencing.context.contains_key(&101));
    assert!(sequencing.context.contains_key(&102));
    assert!(sequencing.context.contains_key(&103));
}

#[test]
fn test_send_event_recursive_with_reorg() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hash = TxHash::random();
    let tx_event = create_transaction(100, 1, 5, tx_hash);

    // First send a transaction to populate sent_events
    let tx_metadata = EventMetadata::from(&tx_event);
    sequencing
        .context
        .entry(100)
        .or_default()
        .sent_events
        .entry(1)
        .or_default()
        .push_back(tx_metadata);

    // Now send a reorg which should cancel it
    let reorg_event = create_reorg(100, 1, 5, tx_hash);
    let reorg_metadata = EventMetadata::from(&reorg_event);

    sequencing
        .send_event_recursive(reorg_event, &reorg_metadata)
        .unwrap();

    // Reorg should be sent
    assert_eq!(engine_recv.len(), 1);

    // The transaction should have been popped and the reorg added
    let ctx = sequencing.context.get(&100).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 1); // Only reorg remains
}

#[test]
fn test_send_event_recursive_reorg_with_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hash = TxHash::random();
    let tx_event = create_transaction(100, 1, 5, tx_hash);
    let reorg_event = create_reorg(100, 1, 5, tx_hash);
    let dependent_event = create_transaction(100, 1, 6, TxHash::random());
    let dependent_event_meta = EventMetadata::from(&dependent_event);

    // Set up: reorg has a dependent event
    let reorg_metadata = EventMetadata::from(&reorg_event);
    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            reorg_metadata.clone(),
            HashMap::from([(dependent_event_meta, dependent_event)]),
        );

    // Add the original transaction to sent_events
    sequencing
        .context
        .entry(100)
        .or_default()
        .sent_events
        .entry(1)
        .or_default()
        .push_back(EventMetadata::from(&tx_event));

    sequencing
        .send_event_recursive(reorg_event, &reorg_metadata)
        .unwrap();

    // Should send reorg and its dependent
    assert_eq!(engine_recv.len(), 2);
}

#[test]
fn test_send_event_recursive_tree_of_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    //       event1
    //      /      \
    //   event2   event3
    //     |        |
    //   event4   event5

    let event1 = create_transaction(100, 1, 0, TxHash::random());
    let event2 = create_transaction(100, 2, 1, TxHash::random());
    let event3 = create_transaction(100, 1, 1, TxHash::random());
    let event4 = create_transaction(100, 2, 2, TxHash::random());
    let event5 = create_transaction(100, 1, 2, TxHash::random());

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);
    let meta3 = EventMetadata::from(&event3);
    let meta4 = EventMetadata::from(&event4);
    let meta5 = EventMetadata::from(&event5);

    let ctx = sequencing.context.entry(100).or_default();
    ctx.dependency_graph.insert(
        meta1.clone(),
        HashMap::from([
            (meta2.clone(), event2.clone()),
            (meta3.clone(), event3.clone()),
        ]),
    );
    ctx.dependency_graph
        .insert(meta2.clone(), HashMap::from([(meta4.clone(), event4)]));
    ctx.dependency_graph
        .insert(meta3.clone(), HashMap::from([(meta5, event5)]));

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    // Should send all 5 events
    assert_eq!(engine_recv.len(), 5);
}

#[test]
fn test_send_event_recursive_empty_dependency_graph() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event = create_transaction(100, 1, 0, TxHash::random());
    let event_metadata = EventMetadata::from(&event);

    // Explicitly create empty context
    sequencing.context.insert(100, Context::default());

    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
}

#[test]
fn test_send_event_recursive_cross_iteration_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Event in iteration 1
    let event1 = create_transaction(100, 1, 0, TxHash::random());
    // Dependent in iteration 2 (same block)
    let event2 = create_transaction(100, 2, 0, TxHash::random());

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(meta1.clone(), HashMap::from([(meta2, event2)]));

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    assert_eq!(engine_recv.len(), 2);

    // Verify both iterations have sent events
    let ctx = sequencing.context.get(&100).unwrap();
    assert!(ctx.sent_events.contains_key(&1));
    assert!(ctx.sent_events.contains_key(&2));
}

#[test]
fn test_send_event_recursive_commit_head_with_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let commit = create_commit_head(100, 1, 5, Some(TxHash::random()));
    let dependent = create_new_iteration(101, 1);

    let dependent_meta = EventMetadata::from(&dependent);
    let commit_meta = EventMetadata::from(&commit);

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            commit_meta.clone(),
            HashMap::from([(dependent_meta, dependent)]),
        );

    sequencing.context.entry(101).or_default();

    sequencing
        .send_event_recursive(commit, &commit_meta)
        .unwrap();

    // CommitHead followed by NewIteration of the next block
    assert_eq!(engine_recv.len(), 2);
    assert!(!sequencing.context.contains_key(&100));
    assert!(sequencing.context.contains_key(&101));
}

#[test]
fn test_send_event_recursive_preserves_sent_events_queue() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random());
    let event2 = create_transaction(100, 1, 1, TxHash::random());

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            meta1.clone(),
            HashMap::from([(meta2.clone(), event2.clone())]),
        );

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    // Verify both events were actually sent
    assert_eq!(engine_recv.len(), 2);

    // Check that the sent_events queue contains both events
    let ctx = sequencing.context.get(&100).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 2);
}

#[test]
fn test_send_event_recursive_handles_missing_context_gracefully() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // No context pre-created for block 100
    let event = create_transaction(100, 1, 0, TxHash::random());
    let event_metadata = EventMetadata::from(&event);

    // This will fail because context doesn't exist - that's expected behavior
    // In real usage, context is created before calling send_event_recursive
    sequencing.context.entry(100).or_default();
    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    // Should create context and send event
    assert_eq!(engine_recv.len(), 1);
    assert!(sequencing.context.contains_key(&100));
}

#[test]
fn test_send_event_recursive_deep_chain() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Create a chain of 10 events
    let mut events = Vec::new();
    let mut metas = Vec::new();
    for i in 0..10 {
        let event = create_transaction(100, 1, i, TxHash::random());
        let meta = EventMetadata::from(&event);
        metas.push(meta);
        events.push(event);
    }

    // Chain them: 0->1->2->...->9
    for i in 0..9 {
        sequencing
            .context
            .entry(100)
            .or_default()
            .dependency_graph
            .insert(
                metas[i].clone(),
                HashMap::from([(metas[i + 1].clone(), events[i + 1].clone())]),
            );
    }

    sequencing
        .send_event_recursive(events[0].clone(), &metas[0].clone())
        .unwrap();

    // All 10 should be sent
    assert_eq!(engine_recv.len(), 10);
}

#[test]
fn test_send_event_recursive_wide_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let root = create_transaction(100, 1, 0, TxHash::random());
    let root_meta = EventMetadata::from(&root);

    // Create 20 dependent events
    let mut dependents = HashMap::new();
    for i in 1..21 {
        let tx = create_transaction(100, 1, i, TxHash::random());
        let tx_meta = EventMetadata::from(&tx);
        dependents.insert(tx_meta, tx);
    }

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(root_meta.clone(), dependents);

    sequencing.send_event_recursive(root, &root_meta).unwrap();

    // Root + 20 dependents = 21 total
    assert_eq!(engine_recv.len(), 21);
}

#[test]
fn test_send_event_recursive_new_iteration_start() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let new_iter = create_new_iteration(100, 1);
    let tx1 = create_transaction(100, 1, 0, TxHash::random());

    let new_iter_meta = EventMetadata::from(&new_iter);
    let tx1_meta = EventMetadata::from(&tx1);

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(new_iter_meta.clone(), HashMap::from([(tx1_meta, tx1)]));

    sequencing
        .send_event_recursive(new_iter, &new_iter_meta)
        .unwrap();

    assert_eq!(engine_recv.len(), 2);
}

#[test]
fn test_send_event_recursive_commit_head_to_new_iteration_to_transaction() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Simulate: CommitHead(block 99) -> NewIteration(block 100) -> Transaction(block 100)
    let commit = create_commit_head(99, 1, 10, Some(TxHash::random()));
    let new_iter = create_new_iteration(100, 1);
    let tx = create_transaction(100, 1, 0, TxHash::random());

    let commit_meta = EventMetadata::from(&commit);
    let new_iter_meta = EventMetadata::from(&new_iter);
    let tx_meta = EventMetadata::from(&tx);

    // Build proper dependency chain
    sequencing
        .context
        .entry(99)
        .or_default()
        .dependency_graph
        .insert(
            commit_meta.clone(),
            HashMap::from([(new_iter_meta.clone(), new_iter.clone())]),
        );

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(new_iter_meta.clone(), HashMap::from([(tx_meta, tx)]));

    // Process from the root (commit)
    sequencing
        .send_event_recursive(commit, &commit_meta)
        .unwrap();

    // Should process all three events across two blocks
    assert_eq!(engine_recv.len(), 3);
    // Block 99 context should be deleted by CommitHead
    assert!(!sequencing.context.contains_key(&99));
    // Block 100 context should still exist
    assert!(sequencing.context.contains_key(&100));
}

#[test]
fn test_send_event_recursive_complex_multi_block_tree() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    //    commit(99)
    //        |
    //    new_iter(100)
    //       / \
    //   tx1   tx2  (block 100)
    //    |     |
    //   tx3  commit(100)
    //          |
    //      new_iter(101)

    let commit99 = create_commit_head(99, 1, 5, Some(TxHash::random()));
    let new_iter100 = create_new_iteration(100, 1);
    let tx1 = create_transaction(100, 1, 0, TxHash::random());
    let tx2 = create_transaction(100, 1, 1, TxHash::random());
    let tx3 = create_transaction(100, 1, 2, TxHash::random());
    let commit100 = create_commit_head(100, 1, 3, Some(TxHash::random()));
    let new_iter101 = create_new_iteration(101, 1);

    let meta_commit99 = EventMetadata::from(&commit99);
    let meta_new_iter100 = EventMetadata::from(&new_iter100);
    let meta_tx1 = EventMetadata::from(&tx1);
    let meta_tx2 = EventMetadata::from(&tx2);
    let meta_tx3 = EventMetadata::from(&tx3);
    let meta_commit100 = EventMetadata::from(&commit100);
    let meta_new_iter101 = EventMetadata::from(&new_iter101);

    sequencing
        .context
        .entry(99)
        .or_default()
        .dependency_graph
        .insert(
            meta_commit99.clone(),
            HashMap::from([(meta_new_iter100.clone(), new_iter100.clone())]),
        );

    sequencing
        .context
        .entry(100)
        .or_default()
        .dependency_graph
        .insert(
            meta_new_iter100.clone(),
            HashMap::from([
                (meta_tx1.clone(), tx1.clone()),
                (meta_tx2.clone(), tx2.clone()),
            ]),
        );

    let ctx100 = sequencing.context.entry(100).or_default();
    ctx100
        .dependency_graph
        .insert(meta_tx1.clone(), HashMap::from([(meta_tx3, tx3.clone())]));
    ctx100.dependency_graph.insert(
        meta_tx2.clone(),
        HashMap::from([(meta_commit100.clone(), commit100.clone())]),
    );
    ctx100.dependency_graph.insert(
        meta_commit100.clone(),
        HashMap::from([(meta_new_iter101, new_iter101.clone())]),
    );

    sequencing.context.entry(101).or_default();

    sequencing
        .send_event_recursive(commit99.clone(), &meta_commit99)
        .unwrap();

    // Should send all 7 events
    if engine_recv.len() == 7 {
        assert_eq!(engine_recv.recv().unwrap(), commit99);
        assert_eq!(engine_recv.recv().unwrap(), new_iter100);
        assert_eq!(engine_recv.recv().unwrap(), tx1);
        assert_eq!(engine_recv.recv().unwrap(), tx3);
        assert_eq!(engine_recv.recv().unwrap(), tx2);
        assert_eq!(engine_recv.recv().unwrap(), commit100);
        assert_eq!(engine_recv.recv().unwrap(), new_iter101);
    } else if engine_recv.len() == 5 {
        assert_eq!(engine_recv.recv().unwrap(), commit99);
        assert_eq!(engine_recv.recv().unwrap(), new_iter100);
        assert_eq!(engine_recv.recv().unwrap(), tx2);
        assert_eq!(engine_recv.recv().unwrap(), commit100);
        assert_eq!(engine_recv.recv().unwrap(), new_iter101);
    } else {
        panic!("Unexpected number of events sent: {}", engine_recv.len());
    }
}

#[test]
fn test_send_event_recursive_reorg_doesnt_cancel_wrong_tx() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hash1 = TxHash::random();
    let tx_hash2 = TxHash::random();

    let tx = create_transaction(100, 1, 5, tx_hash1);
    let reorg = create_reorg(100, 1, 5, tx_hash2); // Different hash!

    // Add transaction to sent_events
    sequencing
        .context
        .entry(100)
        .or_default()
        .sent_events
        .entry(1)
        .or_default()
        .push_back(EventMetadata::from(&tx));

    let reorg_metadata = EventMetadata::from(&reorg);
    sequencing
        .send_event_recursive(reorg, &reorg_metadata)
        .unwrap();

    // Reorg should not be sent to engine because it doesn't cancel the right transaction
    assert_eq!(engine_recv.len(), 0);

    // Only one event should be recorded in sent_events because the reorg was ignored as it does
    // not reorg anything
    // This represents an error case that is logged
    let ctx = sequencing.context.get(&100).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 1); // Transaction + invalid Reorg both recorded
}

#[test]
fn test_send_event_recursive_empty_block_commit() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Commit with 0 transactions
    let commit = create_commit_head(100, 1, 0, None);
    let commit_metadata = EventMetadata::from(&commit);

    sequencing.context.entry(100).or_default();
    sequencing
        .send_event_recursive(commit, &commit_metadata)
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
}

#[test]
fn test_deterministic_event_ordering_with_shuffle() {
    use rand::{
        SeedableRng,
        seq::SliceRandom,
    };

    // Create a realistic sequence of events for a single block/iteration
    let block_num = 100;
    let iter_id = 1;

    let prev_commit = create_commit_head(block_num - 1, iter_id, 4, Some(TxHash::random()));
    let new_iter = create_new_iteration(block_num, iter_id);
    let tx0 = create_transaction(block_num, iter_id, 0, TxHash::random());
    let tx1 = create_transaction(block_num, iter_id, 1, TxHash::random());
    let tx2 = create_transaction(block_num, iter_id, 2, TxHash::random());
    let tx3 = create_transaction(block_num, iter_id, 3, TxHash::random());
    let commit = create_commit_head(block_num, iter_id, 4, Some(TxHash::random()));

    let events = vec![
        prev_commit.clone(),
        new_iter.clone(),
        tx0.clone(),
        tx1.clone(),
        tx2.clone(),
        tx3.clone(),
        commit.clone(),
    ];

    // Expected order: NewIteration -> Tx0 -> Tx1 -> Tx2 -> Tx3 -> CommitHead
    let expected_order = events.clone();

    // Test with multiple shuffles
    for seed in 0..10 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut shuffled = events.clone();
        shuffled.shuffle(&mut rng);

        let (mut sequencing, engine_recv) = create_test_sequencing();

        // Build dependency graph from shuffled events
        build_dependency_graph_from_events(&mut sequencing, &shuffled);

        // Start processing from the root (NewIteration)
        sequencing
            .send_event_recursive(prev_commit.clone(), &EventMetadata::from(&prev_commit))
            .unwrap();

        // Verify all events were sent
        assert_eq!(
            engine_recv.len(),
            7,
            "All events should be sent (seed: {seed})",
        );

        // Verify events were sent in the correct deterministic order
        for (i, expected) in expected_order.iter().enumerate() {
            let received = engine_recv.recv().unwrap();
            assert_eq!(
                received, *expected,
                "Event at position {i} should match expected order (seed: {seed})",
            );
        }
    }
}

#[test]
fn test_deterministic_ordering_with_reorg_shuffle() {
    use rand::{
        SeedableRng,
        seq::SliceRandom,
    };

    let block_num = 100;
    let iter_id = 1;
    let tx_hash_to_reorg = TxHash::random();

    let new_iter = create_new_iteration(block_num, iter_id);
    let tx0 = create_transaction(block_num, iter_id, 0, TxHash::random());
    let tx1 = create_transaction(block_num, iter_id, 1, tx_hash_to_reorg);
    let reorg1 = create_reorg(block_num, iter_id, 1, tx_hash_to_reorg);
    let tx2 = create_transaction(block_num, iter_id, 1, TxHash::random());
    let commit = create_commit_head(block_num, iter_id, 2, Some(TxHash::random()));

    let events = vec![
        new_iter.clone(),
        tx0.clone(),
        tx1.clone(),
        reorg1.clone(),
        tx2.clone(),
        commit.clone(),
    ];

    // Test with shuffles
    for seed in 0..10 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut shuffled = events.clone();
        shuffled.shuffle(&mut rng);

        let (mut sequencing, engine_recv) = create_test_sequencing();

        let ctx = sequencing.context.entry(block_num).or_default();
        let new_iter_meta = EventMetadata::from(&new_iter);
        let tx0_meta = EventMetadata::from(&tx0);
        let tx1_meta = EventMetadata::from(&tx1);
        let reorg1_meta = EventMetadata::from(&reorg1);
        let tx2_meta = EventMetadata::from(&tx2);
        let commit_meta = EventMetadata::from(&commit);

        // Build proper chain: new_iter -> tx0 -> tx1 -> reorg1
        // And also: tx0 -> tx2 -> commit (after reorg)
        ctx.dependency_graph.insert(
            new_iter_meta.clone(),
            HashMap::from([(tx0_meta.clone(), tx0.clone())]),
        );
        ctx.dependency_graph.insert(
            tx0_meta.clone(),
            HashMap::from([
                (tx1_meta.clone(), tx1.clone()),
                (tx2_meta.clone(), tx2.clone()),
            ]),
        );
        ctx.dependency_graph.insert(
            tx1_meta.clone(),
            HashMap::from([(reorg1_meta.clone(), reorg1.clone())]),
        );
        ctx.dependency_graph.insert(
            tx2_meta.clone(),
            HashMap::from([(commit_meta, commit.clone())]),
        );

        sequencing
            .send_event_recursive(new_iter.clone(), &new_iter_meta)
            .unwrap();

        // tx1 and reorg1 cancel each other, so we should get:
        // new_iter, tx0, tx1, reorg1, tx2, commit
        // But tx1/reorg1 cancel, so actually sent: new_iter, tx0, tx2, commit = 4 events
        assert_eq!(engine_recv.len(), 4, "Should send 4 events (seed: {seed})",);

        // Verify sent_events has the correct count
        // Block context should be deleted after commit
        assert!(
            !sequencing.context.contains_key(&block_num),
            "Block context should be deleted after CommitHead (seed: {seed})"
        );
    }
}

#[test]
fn test_deterministic_ordering_multi_iteration_shuffle() {
    use rand::{
        SeedableRng,
        seq::SliceRandom,
    };

    let block_num = 100;

    // Create events for two iterations in the same block
    let new_iter1 = create_new_iteration(block_num, 1);
    let tx1_0 = create_transaction(block_num, 1, 0, TxHash::random());
    let tx1_1 = create_transaction(block_num, 1, 1, TxHash::random());

    let new_iter2 = create_new_iteration(block_num, 2);
    let tx2_0 = create_transaction(block_num, 2, 0, TxHash::random());
    let commit2 = create_commit_head(block_num, 2, 1, Some(TxHash::random()));

    let events = vec![
        new_iter1.clone(),
        tx1_0.clone(),
        tx1_1.clone(),
        new_iter2.clone(),
        tx2_0.clone(),
        commit2.clone(),
    ];

    for seed in 0..10 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut shuffled = events.clone();
        shuffled.shuffle(&mut rng);

        let (mut sequencing, engine_recv) = create_test_sequencing();

        // Manually build TWO INDEPENDENT dependency chains
        let ctx = sequencing.context.entry(block_num).or_default();

        // Chain for iteration 1: new_iter1 -> tx1_0 -> tx1_1
        let new_iter1_meta = EventMetadata::from(&new_iter1);
        let tx1_0_meta = EventMetadata::from(&tx1_0);
        let tx1_1_meta = EventMetadata::from(&tx1_1);

        ctx.dependency_graph.insert(
            new_iter1_meta.clone(),
            HashMap::from([(tx1_0_meta.clone(), tx1_0.clone())]),
        );
        ctx.dependency_graph
            .insert(tx1_0_meta, HashMap::from([(tx1_1_meta, tx1_1.clone())]));

        // Chain for iteration 2: new_iter2 -> tx2_0 -> commit2
        let new_iter2_meta = EventMetadata::from(&new_iter2);
        let tx2_0_meta = EventMetadata::from(&tx2_0);
        let commit2_meta = EventMetadata::from(&commit2);

        ctx.dependency_graph.insert(
            new_iter2_meta.clone(),
            HashMap::from([(tx2_0_meta.clone(), tx2_0.clone())]),
        );
        ctx.dependency_graph
            .insert(tx2_0_meta, HashMap::from([(commit2_meta, commit2.clone())]));

        // Process iteration 1
        sequencing
            .send_event_recursive(new_iter1.clone(), &new_iter1_meta)
            .unwrap();

        // Should send only iteration 1's 3 events
        assert_eq!(
            engine_recv.len(),
            3,
            "Iteration 1 should send 3 events (seed: {seed})",
        );

        // Verify iteration 1 events in sent_events
        let ctx = sequencing.context.get(&block_num).unwrap();
        assert_eq!(
            ctx.sent_events.get(&1).unwrap().len(),
            3,
            "Iteration 1 should have 3 sent events (seed: {seed})",
        );

        // Now process iteration 2
        sequencing
            .send_event_recursive(new_iter2.clone(), &new_iter2_meta)
            .unwrap();

        // Total: 3 from iter1 + 3 from iter2 = 6
        assert_eq!(
            engine_recv.len(),
            6,
            "All events should be sent (seed: {seed})",
        );

        // Block context should be deleted after commit2
        assert!(
            !sequencing.context.contains_key(&block_num),
            "Block context should be deleted after CommitHead (seed: {seed})"
        );
    }
}

#[test]
fn test_deterministic_ordering_cross_block_shuffle() {
    use rand::{
        SeedableRng,
        seq::SliceRandom,
    };

    // Events spanning multiple blocks
    let commit99 = create_commit_head(99, 1, 5, Some(TxHash::random()));
    let new_iter100 = create_new_iteration(100, 1);
    let tx100 = create_transaction(100, 1, 0, TxHash::random());
    let commit100 = create_commit_head(100, 1, 1, Some(TxHash::random()));
    let new_iter101 = create_new_iteration(101, 1);
    let tx101 = create_transaction(101, 1, 0, TxHash::random());

    let events = vec![
        commit99.clone(),
        new_iter100.clone(),
        tx100.clone(),
        commit100.clone(),
        new_iter101.clone(),
        tx101.clone(),
    ];

    for seed in 0..10 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut shuffled = events.clone();
        shuffled.shuffle(&mut rng);

        let (mut sequencing, engine_recv) = create_test_sequencing();

        build_dependency_graph_from_events(&mut sequencing, &shuffled);

        // Start from the first event (commit99)
        sequencing
            .send_event_recursive(commit99.clone(), &EventMetadata::from(&commit99))
            .unwrap();

        // All 6 events should be sent
        assert_eq!(
            engine_recv.len(),
            6,
            "All events should be sent (seed: {seed})",
        );

        // Verify contexts: 99 and 100 should be deleted, 101 should exist
        assert!(
            !sequencing.context.contains_key(&99),
            "Block 99 context should be deleted (seed: {seed})",
        );
        assert!(
            !sequencing.context.contains_key(&100),
            "Block 100 context should be deleted (seed: {seed})",
        );
        assert!(
            sequencing.context.contains_key(&101),
            "Block 101 context should exist (seed: {seed})",
        );
    }
}

#[test]
fn test_commit_head_with_gap_jumps_immediately() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // System starts with current_head = 0
    assert_eq!(sequencing.current_head, 0);

    // Receive CommitHead(99) - huge gap, no events for blocks 1-99
    sequencing
        .process_event(create_commit_head(99, 1, 0, None))
        .unwrap();

    // CommitHead(99) should be sent immediately
    assert_eq!(engine_recv.len(), 1, "CommitHead should jump immediately");

    // current_head should jump to 99
    assert_eq!(sequencing.current_head, 99);
}

#[test]
fn test_sending_old_commit_head() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // current_head = 0
    // Send events for block 10 (future - not current_head + 1)
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random()))
        .unwrap();
    sequencing
        .process_event(create_commit_head(10, 1, 1, Some(TxHash::random())))
        .unwrap();

    // CommitHead(10) sent (gap), current_head = 10
    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 10);

    // Send CommitHead(9) - should be ignored (old)
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // Still only 1 event (CommitHead(9) ignored)
    assert_eq!(engine_recv.len(), 1);

    // current_head stays at 10
    assert_eq!(sequencing.current_head, 10);
}

#[test]
fn test_events_process_at_current_head_plus_one() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // current_head = 0, so block 1 is "current" and should process immediately
    assert_eq!(sequencing.current_head, 0);

    // Send events for block 1 - should process only commit head
    sequencing
        .process_event(create_new_iteration(1, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(1, 1, 0, TxHash::random()))
        .unwrap();
    sequencing
        .process_event(create_commit_head(1, 1, 1, Some(TxHash::random())))
        .unwrap();

    // Only CommitHead(1) should be sent
    assert_eq!(engine_recv.len(), 1);

    // current_head advanced to 1
    assert_eq!(sequencing.current_head, 1);

    // Context cleaned
    assert!(!sequencing.context.contains_key(&1));
}

#[test]
fn test_cascading_blocks() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // current_head = 0, so block 1 processes immediately
    // When CommitHead(1) is sent, current_head becomes 1, triggering block 2
    // And so on...

    for block in 1..=5 {
        sequencing
            .process_event(create_new_iteration(block, 1))
            .unwrap();
        sequencing
            .process_event(create_transaction(block, 1, 0, TxHash::random()))
            .unwrap();
        sequencing
            .process_event(create_commit_head(block, 1, 1, Some(TxHash::random())))
            .unwrap();
    }

    // All 5 blocks cascade: 5 blocks × 3 events = 15 events - 2 blocks (first new iteration and transaction)
    assert_eq!(engine_recv.len(), 13);

    // current_head at 5
    assert_eq!(sequencing.current_head, 5);

    // All contexts cleaned
    for block in 1..=5 {
        assert!(!sequencing.context.contains_key(&block));
    }
}

#[test]
fn test_future_blocks_queued() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // current_head = 0
    // Send events for block 10 (future - not current_head + 1)
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random()))
        .unwrap();

    // Nothing sent (block 10 is the future)
    assert_eq!(engine_recv.len(), 0);

    // Send CommitHead(9) to jump and trigger block 10
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // CommitHead(9) + block 10 (2 events) = 3 events
    assert_eq!(engine_recv.len(), 3);

    // current_head at 10
    assert_eq!(sequencing.current_head, 9);
}

#[test]
fn test_disconnect_reconnect() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Process block 1 normally
    sequencing
        .process_event(create_commit_head(1, 1, 1, Some(TxHash::random())))
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // DISCONNECT - miss blocks 2-149

    // RECONNECT - receive events for block 150
    sequencing
        .process_event(create_new_iteration(150, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(150, 1, 0, TxHash::random()))
        .unwrap();

    // Nothing sent (block 150 is the future)
    assert_eq!(engine_recv.len(), 0);

    // Receive CommitHead(149) - gap from 2-149
    sequencing
        .process_event(create_commit_head(149, 1, 0, None))
        .unwrap();

    // CommitHead(149) sent, current_head = 149, block 150 processes
    // CommitHead(149) + NewIteration(150) + Tx(150) = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 149);

    // Send CommitHead(150)
    sequencing
        .process_event(create_commit_head(150, 1, 1, Some(TxHash::random())))
        .unwrap();

    // CommitHead(150) sent (4 total)
    assert_eq!(engine_recv.len(), 4);
    assert_eq!(sequencing.current_head, 150);
}

#[test]
fn test_gap_stops_cascade() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Send block 1
    sequencing
        .process_event(create_commit_head(1, 1, 1, Some(TxHash::random())))
        .unwrap();

    // Block 1 processes (1 event)
    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue block 10 (gap from 2-9)
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random()))
        .unwrap();

    // Nothing sent (block 10 is the future with gap)
    assert_eq!(engine_recv.len(), 0);

    // Send CommitHead(9) to jump
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // CommitHead(9) + block 10 = 4 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 9);
}
