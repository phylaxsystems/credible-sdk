#![allow(clippy::field_reassign_with_default)]
use super::*;
use crate::{
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        ReorgRequest,
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
};
use alloy::primitives::TxHash;
use assertion_executor::primitives::{
    B256,
    Bytes,
    U256,
};
use revm::{
    context::{
        BlockEnv,
        TxEnv,
        tx::TxEnvBuilder,
    },
    primitives::TxKind,
};
use std::sync::{
    Arc,
    Mutex,
};

impl PartialEq for TxQueueContents {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TxQueueContents::Tx(tx1), TxQueueContents::Tx(tx2)) => tx1 == tx2,
            (TxQueueContents::Reorg(id1), TxQueueContents::Reorg(id2)) => id1 == id2,
            (TxQueueContents::CommitHead(c1), TxQueueContents::CommitHead(c2)) => {
                c1.block_number == c2.block_number
                    && c1.selected_iteration_id == c2.selected_iteration_id
                    && c1.last_tx_hash == c2.last_tx_hash
                    && c1.n_transactions == c2.n_transactions
            }
            (TxQueueContents::NewIteration(n1), TxQueueContents::NewIteration(n2)) => {
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
    let mut events_by_block: HashMap<U256, Vec<TxQueueContents>> = HashMap::new();
    for event in events {
        events_by_block
            .entry(event.block_number())
            .or_default()
            .push(event.clone());
    }

    // Get sorted block numbers for cross-block linking
    let mut block_numbers: Vec<U256> = events_by_block.keys().copied().collect();
    block_numbers.sort_unstable();

    let mut last_event_per_block: HashMap<U256, TxQueueContents> = HashMap::new();

    // For each block, build the linear dependency chain
    for block_number in &block_numbers {
        let block_events = &events_by_block[block_number];
        let ctx = sequencing.context.entry(*block_number).or_default();

        // Sort events to establish proper order within the block
        let mut sorted_events = block_events.clone();
        sorted_events.sort_by_key(|e| {
            match e {
                TxQueueContents::NewIteration(ni) => (0, ni.iteration_id, 0u64),
                TxQueueContents::Tx(tx) => {
                    (1, tx.tx_execution_id.iteration_id, tx.tx_execution_id.index)
                }
                TxQueueContents::Reorg(reorg) => {
                    (
                        1,
                        reorg.tx_execution_id.iteration_id,
                        reorg.tx_execution_id.index,
                    )
                }
                TxQueueContents::CommitHead(ch) => (2, ch.selected_iteration_id, 0u64),
            }
        });

        // Build linear dependencies within the block
        for i in 0..sorted_events.len() - 1 {
            let current_meta = EventMetadata::from(&sorted_events[i]);
            let next_meta = EventMetadata::from(&sorted_events[i + 1]);
            ctx.dependency_graph.insert(
                current_meta,
                HashMap::from([(next_meta.into(), sorted_events[i + 1].clone())]),
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
                    TxQueueContents::NewIteration(ni) => (0, ni.iteration_id, 0u64),
                    TxQueueContents::Tx(tx) => {
                        (1, tx.tx_execution_id.iteration_id, tx.tx_execution_id.index)
                    }
                    TxQueueContents::Reorg(reorg) => {
                        (
                            1,
                            reorg.tx_execution_id.iteration_id,
                            reorg.tx_execution_id.index,
                        )
                    }
                    TxQueueContents::CommitHead(ch) => (2, ch.selected_iteration_id, 0u64),
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
                        HashMap::from([(first_next_meta.into(), first_next_event.clone())]),
                    );
            }
        }
    }
}

// Helper to create a test EventSequencing
fn create_test_sequencing() -> (EventSequencing, TransactionQueueReceiver) {
    let (tx_send, tx_recv) = flume::unbounded();
    let (engine_send, engine_recv) = flume::unbounded();

    let sequencing = EventSequencing::new(tx_recv, engine_send);

    (sequencing, engine_recv)
}

// Mock event creators for testing
fn create_new_iteration(block: u64, iteration: u64) -> TxQueueContents {
    let mut block_env = BlockEnv::default();
    block_env.number = U256::from(block);

    TxQueueContents::NewIteration(NewIteration::new(
        iteration,
        block_env,
        Some(B256::ZERO),
        Some(B256::ZERO),
    ))
}

fn create_transaction(
    block: u64,
    iteration: u64,
    index: u64,
    tx_hash: TxHash,
    prev_tx_hash: Option<TxHash>,
) -> TxQueueContents {
    let tx_execution_id = TxExecutionId {
        block_number: U256::from(block),
        iteration_id: iteration,
        tx_hash,
        index,
    };

    TxQueueContents::Tx(QueueTransaction {
        tx_execution_id,
        tx_env: TxEnv::default(),
        prev_tx_hash,
    })
}

fn create_reorg_with_hashes(
    block: u64,
    iteration: u64,
    index: u64,
    tx_hashes: Vec<TxHash>,
) -> TxQueueContents {
    let tx_hash = *tx_hashes
        .last()
        .expect("reorg request should include at least one tx hash");
    let tx_execution_id = TxExecutionId {
        block_number: U256::from(block),
        iteration_id: iteration,
        tx_hash,
        index,
    };

    TxQueueContents::Reorg(ReorgRequest {
        tx_execution_id,
        tx_hashes,
    })
}

fn create_reorg(block: u64, iteration: u64, index: u64, tx_hash: TxHash) -> TxQueueContents {
    create_reorg_with_hashes(block, iteration, index, vec![tx_hash])
}

fn create_commit_head(
    block: u64,
    iteration: u64,
    n_txs: u64,
    last_tx_hash: Option<TxHash>,
) -> TxQueueContents {
    TxQueueContents::CommitHead(CommitHead::new(
        U256::from(block),
        iteration,
        last_tx_hash,
        n_txs,
        B256::ZERO,
        None,
        U256::ZERO,
    ))
}

#[test]
fn test_send_event_recursive_no_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event = create_transaction(100, 1, 0, TxHash::random(), None);
    let event_metadata = EventMetadata::from(&event);

    sequencing.context.entry(U256::from(100)).or_default();
    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    // Should send exactly one event
    assert_eq!(engine_recv.len(), 1);

    // Verify context was created
    assert!(sequencing.context.contains_key(&U256::from(100)));
}

#[test]
fn test_send_event_recursive_single_dependency() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event2 = create_transaction(100, 1, 1, TxHash::random(), None);

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);

    // Set up dependency: event2 depends on event1
    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([(CompleteEventMetadata::from(event2_metadata), event2.clone())]),
        );

    sequencing
        .send_event_recursive(event1.clone(), &event1_metadata)
        .unwrap();

    // Should send both events in order
    assert_eq!(engine_recv.len(), 2);
    assert_eq!(engine_recv.recv().unwrap(), event1);
    assert_eq!(engine_recv.recv().unwrap(), event2);

    // Verify both were recorded in sent_events
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 2);
}

#[test]
fn test_send_event_recursive_multiple_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event2 = create_transaction(100, 1, 1, TxHash::random(), None);
    let event3 = create_transaction(100, 1, 2, TxHash::random(), None);
    let event4 = create_transaction(100, 1, 3, TxHash::random(), None);

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);
    let event3_metadata = EventMetadata::from(&event3);
    let event4_metadata = EventMetadata::from(&event4);

    // Set up dependencies: event2, event3, and event4 all depend on event1
    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([
                (event2_metadata.into(), event2.clone()),
                (event3_metadata.into(), event3.clone()),
                (event4_metadata.into(), event4.clone()),
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

    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event2 = create_transaction(100, 1, 1, TxHash::random(), None);
    let event3 = create_transaction(100, 1, 2, TxHash::random(), None);

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);
    let event3_metadata = EventMetadata::from(&event3);

    // Set up chain: event1 -> event2 -> event3
    let ctx = sequencing.context.entry(U256::from(100)).or_default();
    ctx.dependency_graph.insert(
        event1_metadata.clone(),
        HashMap::from([(
            CompleteEventMetadata::from(event2_metadata.clone()),
            event2.clone(),
        )]),
    );
    ctx.dependency_graph.insert(
        event2_metadata.clone(),
        HashMap::from([(CompleteEventMetadata::from(event3_metadata), event3.clone())]),
    );

    sequencing
        .send_event_recursive(event1.clone(), &event1_metadata)
        .unwrap();

    // Should send all 3 events in order
    assert_eq!(engine_recv.len(), 3);

    // Verify the execution order
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
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
    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    // Dependent event in block 101
    let event2 = create_transaction(101, 1, 0, TxHash::random(), None);

    let event1_metadata = EventMetadata::from(&event1);
    let event2_metadata = EventMetadata::from(&event2);

    // Set up dependency across blocks
    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            event1_metadata.clone(),
            HashMap::from([(CompleteEventMetadata::from(event2_metadata), event2)]),
        );

    // Create context for block 101 as well
    sequencing.context.entry(U256::from(101)).or_default();

    sequencing
        .send_event_recursive(event1, &event1_metadata)
        .unwrap();

    // Should handle both blocks gracefully
    assert_eq!(engine_recv.len(), 2);

    // Verify context for both blocks exists
    assert!(sequencing.context.contains_key(&U256::from(100)));
    assert!(sequencing.context.contains_key(&U256::from(101)));

    // Verify events in correct blocks
    assert!(
        sequencing
            .context
            .get(&U256::from(100))
            .unwrap()
            .sent_events
            .contains_key(&1)
    );
    assert!(
        sequencing
            .context
            .get(&U256::from(101))
            .unwrap()
            .sent_events
            .contains_key(&1)
    );
}

#[test]
fn test_send_event_recursive_multiple_block_transitions() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event_b100 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event_b101 = create_transaction(101, 1, 0, TxHash::random(), None);
    let event_b102 = create_transaction(102, 1, 0, TxHash::random(), None);
    let event_b103 = create_transaction(103, 1, 0, TxHash::random(), None);

    let meta_b100 = EventMetadata::from(&event_b100);
    let meta_b101 = EventMetadata::from(&event_b101);
    let meta_b102 = EventMetadata::from(&event_b102);
    let meta_b103 = EventMetadata::from(&event_b103);

    // Chain across multiple blocks: 100 -> 101 -> 102 -> 103
    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            meta_b100.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(meta_b101.clone()),
                event_b101.clone(),
            )]),
        );
    sequencing
        .context
        .entry(U256::from(101))
        .or_default()
        .dependency_graph
        .insert(
            meta_b101.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(meta_b102.clone()),
                event_b102.clone(),
            )]),
        );
    sequencing
        .context
        .entry(U256::from(102))
        .or_default()
        .dependency_graph
        .insert(
            meta_b102.clone(),
            HashMap::from([(CompleteEventMetadata::from(meta_b103), event_b103.clone())]),
        );
    sequencing.context.entry(U256::from(103)).or_default();

    sequencing
        .send_event_recursive(event_b100, &meta_b100)
        .unwrap();

    assert_eq!(engine_recv.len(), 4);
    assert!(sequencing.context.contains_key(&U256::from(100)));
    assert!(sequencing.context.contains_key(&U256::from(101)));
    assert!(sequencing.context.contains_key(&U256::from(102)));
    assert!(sequencing.context.contains_key(&U256::from(103)));
}

#[test]
fn test_send_event_recursive_with_reorg() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hash = TxHash::random();
    let tx_event = create_transaction(100, 1, 5, tx_hash, None);

    // First send a transaction to populate sent_events
    let tx_metadata = EventMetadata::from(&tx_event);
    sequencing
        .context
        .entry(U256::from(100))
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
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 0); // Org event should be removed from sent_events
}

#[test]
fn test_send_event_recursive_reorg_depth_matches_tail() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hashes = [TxHash::random(), TxHash::random(), TxHash::random()];
    let tx0 = create_transaction(100, 1, 0, tx_hashes[0], None);
    let tx1 = create_transaction(100, 1, 1, tx_hashes[1], None);
    let tx2 = create_transaction(100, 1, 2, tx_hashes[2], None);

    let ctx = sequencing.context.entry(U256::from(100)).or_default();
    let sent = ctx.sent_events.entry(1).or_default();
    sent.push_back(EventMetadata::from(&tx0));
    sent.push_back(EventMetadata::from(&tx1));
    sent.push_back(EventMetadata::from(&tx2));

    let reorg_event = create_reorg_with_hashes(100, 1, 2, vec![tx_hashes[1], tx_hashes[2]]);
    let reorg_metadata = EventMetadata::from(&reorg_event);

    sequencing
        .send_event_recursive(reorg_event, &reorg_metadata)
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 1);
    assert!(matches!(
        sent.back(),
        Some(EventMetadata::Transaction { index: 0, .. })
    ));
}

#[test]
fn test_send_event_recursive_reorg_depth_hashes_mismatch() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hashes = [TxHash::random(), TxHash::random(), TxHash::random()];
    let tx0 = create_transaction(100, 1, 0, tx_hashes[0], None);
    let tx1 = create_transaction(100, 1, 1, tx_hashes[1], None);
    let tx2 = create_transaction(100, 1, 2, tx_hashes[2], None);

    let ctx = sequencing.context.entry(U256::from(100)).or_default();
    let sent = ctx.sent_events.entry(1).or_default();
    sent.push_back(EventMetadata::from(&tx0));
    sent.push_back(EventMetadata::from(&tx1));
    sent.push_back(EventMetadata::from(&tx2));

    let reorg_event = create_reorg_with_hashes(100, 1, 2, vec![TxHash::random(), tx_hashes[2]]);
    let reorg_metadata = EventMetadata::from(&reorg_event);

    sequencing
        .send_event_recursive(reorg_event, &reorg_metadata)
        .unwrap();

    assert_eq!(engine_recv.len(), 0);
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 3);
}

#[test]
fn test_send_event_recursive_reorg_with_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let tx_hash = TxHash::random();
    let tx_event = create_transaction(100, 1, 5, tx_hash, None);
    let reorg_event = create_reorg(100, 1, 5, tx_hash);
    let dependent_event = create_transaction(100, 1, 6, TxHash::random(), None);
    let dependent_event_meta = EventMetadata::from(&dependent_event);

    // Set up: reorg has a dependent event
    let reorg_metadata = EventMetadata::from(&reorg_event);
    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            reorg_metadata.clone(),
            HashMap::from([(dependent_event_meta.into(), dependent_event)]),
        );

    // Add the original transaction to sent_events
    sequencing
        .context
        .entry(U256::from(100))
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

    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event2 = create_transaction(100, 2, 1, TxHash::random(), None);
    let event3 = create_transaction(100, 1, 1, TxHash::random(), None);
    let event4 = create_transaction(100, 2, 2, TxHash::random(), None);
    let event5 = create_transaction(100, 1, 2, TxHash::random(), None);

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);
    let meta3 = EventMetadata::from(&event3);
    let meta4 = EventMetadata::from(&event4);
    let meta5 = EventMetadata::from(&event5);

    let ctx = sequencing.context.entry(U256::from(100)).or_default();
    ctx.dependency_graph.insert(
        meta1.clone(),
        HashMap::from([
            (CompleteEventMetadata::from(meta2.clone()), event2.clone()),
            (CompleteEventMetadata::from(meta3.clone()), event3.clone()),
        ]),
    );
    ctx.dependency_graph.insert(
        meta2.clone(),
        HashMap::from([(CompleteEventMetadata::from(meta4.clone()), event4)]),
    );
    ctx.dependency_graph.insert(
        meta3.clone(),
        HashMap::from([(CompleteEventMetadata::from(meta5), event5)]),
    );

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    // Should send all 5 events
    assert_eq!(engine_recv.len(), 5);
}

#[test]
fn test_send_event_recursive_empty_dependency_graph() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event = create_transaction(100, 1, 0, TxHash::random(), None);
    let event_metadata = EventMetadata::from(&event);

    // Explicitly create empty context
    sequencing
        .context
        .insert(U256::from(100), Context::default());

    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
}

#[test]
fn test_send_event_recursive_cross_iteration_dependencies() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Event in iteration 1
    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    // Dependent in iteration 2 (same block)
    let event2 = create_transaction(100, 2, 0, TxHash::random(), None);

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);

    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            meta1.clone(),
            HashMap::from([(CompleteEventMetadata::from(meta2), event2)]),
        );

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    assert_eq!(engine_recv.len(), 2);

    // Verify both iterations have sent events
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
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
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            commit_meta.clone(),
            HashMap::from([(CompleteEventMetadata::from(dependent_meta), dependent)]),
        );

    sequencing.context.entry(U256::from(101)).or_default();

    sequencing
        .send_event_recursive(commit, &commit_meta)
        .unwrap();

    // CommitHead followed by NewIteration of the next block
    assert_eq!(engine_recv.len(), 2);
    assert!(!sequencing.context.contains_key(&U256::from(100)));
    assert!(sequencing.context.contains_key(&U256::from(101)));
}

#[test]
fn test_send_event_recursive_preserves_sent_events_queue() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    let event1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let event2 = create_transaction(100, 1, 1, TxHash::random(), None);

    let meta1 = EventMetadata::from(&event1);
    let meta2 = EventMetadata::from(&event2);

    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            meta1.clone(),
            HashMap::from([(CompleteEventMetadata::from(meta2.clone()), event2.clone())]),
        );

    sequencing.send_event_recursive(event1, &meta1).unwrap();

    // Verify both events were actually sent
    assert_eq!(engine_recv.len(), 2);

    // Check that the sent_events queue contains both events
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 2);
}

#[test]
fn test_send_event_recursive_handles_missing_context_gracefully() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // No context pre-created for block 100
    let event = create_transaction(100, 1, 0, TxHash::random(), None);
    let event_metadata = EventMetadata::from(&event);

    // This will fail because context doesn't exist - that's expected behavior
    // In real usage, context is created before calling send_event_recursive
    sequencing.context.entry(U256::from(100)).or_default();
    sequencing
        .send_event_recursive(event, &event_metadata)
        .unwrap();

    // Should create context and send event
    assert_eq!(engine_recv.len(), 1);
    assert!(sequencing.context.contains_key(&U256::from(100)));
}

#[test]
fn test_send_event_recursive_deep_chain() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Create a chain of 10 events
    let mut events = Vec::new();
    let mut metas = Vec::new();
    for i in 0..10 {
        let event = create_transaction(100, 1, i, TxHash::random(), None);
        let meta = EventMetadata::from(&event);
        metas.push(meta);
        events.push(event);
    }

    // Chain them: 0->1->2->...->9
    for i in 0..9 {
        sequencing
            .context
            .entry(U256::from(100))
            .or_default()
            .dependency_graph
            .insert(
                metas[i].clone(),
                HashMap::from([(
                    CompleteEventMetadata::from(metas[i + 1].clone()),
                    events[i + 1].clone(),
                )]),
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

    let root = create_transaction(100, 1, 0, TxHash::random(), None);
    let root_meta = EventMetadata::from(&root);

    // Create 20 dependent events
    let mut dependents = HashMap::new();
    for i in 1..21 {
        let tx = create_transaction(100, 1, i, TxHash::random(), None);
        let tx_meta = EventMetadata::from(&tx);
        dependents.insert(CompleteEventMetadata::from(tx_meta), tx);
    }

    sequencing
        .context
        .entry(U256::from(100))
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
    let tx1 = create_transaction(100, 1, 0, TxHash::random(), None);

    let new_iter_meta = EventMetadata::from(&new_iter);
    let tx1_meta = EventMetadata::from(&tx1);

    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            new_iter_meta.clone(),
            HashMap::from([(tx1_meta.into(), tx1)]),
        );

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
    let tx = create_transaction(100, 1, 0, TxHash::random(), None);

    let commit_meta = EventMetadata::from(&commit);
    let new_iter_meta = EventMetadata::from(&new_iter);
    let tx_meta = EventMetadata::from(&tx);

    // Build proper dependency chain
    sequencing
        .context
        .entry(U256::from(99))
        .or_default()
        .dependency_graph
        .insert(
            commit_meta.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(new_iter_meta.clone()),
                new_iter.clone(),
            )]),
        );

    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            new_iter_meta.clone(),
            HashMap::from([(CompleteEventMetadata::from(tx_meta), tx)]),
        );

    // Process from the root (commit)
    sequencing
        .send_event_recursive(commit, &commit_meta)
        .unwrap();

    // Should process all three events across two blocks
    assert_eq!(engine_recv.len(), 3);
    // Block 99 context should be deleted by CommitHead
    assert!(!sequencing.context.contains_key(&U256::from(99)));
    // Block 100 context should still exist
    assert!(sequencing.context.contains_key(&U256::from(100)));
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
    let tx1 = create_transaction(100, 1, 0, TxHash::random(), None);
    let tx2 = create_transaction(100, 1, 1, TxHash::random(), None);
    let tx3 = create_transaction(100, 1, 2, TxHash::random(), None);
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
        .entry(U256::from(99))
        .or_default()
        .dependency_graph
        .insert(
            meta_commit99.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(meta_new_iter100.clone()),
                new_iter100.clone(),
            )]),
        );

    sequencing
        .context
        .entry(U256::from(100))
        .or_default()
        .dependency_graph
        .insert(
            meta_new_iter100.clone(),
            HashMap::from([
                (CompleteEventMetadata::from(meta_tx1.clone()), tx1.clone()),
                (CompleteEventMetadata::from(meta_tx2.clone()), tx2.clone()),
            ]),
        );

    let ctx100 = sequencing.context.entry(U256::from(100)).or_default();
    ctx100.dependency_graph.insert(
        meta_tx1.clone(),
        HashMap::from([(CompleteEventMetadata::from(meta_tx3), tx3.clone())]),
    );
    ctx100.dependency_graph.insert(
        meta_tx2.clone(),
        HashMap::from([(
            CompleteEventMetadata::from(meta_commit100.clone()),
            commit100.clone(),
        )]),
    );
    ctx100.dependency_graph.insert(
        meta_commit100.clone(),
        HashMap::from([(
            CompleteEventMetadata::from(meta_new_iter101),
            new_iter101.clone(),
        )]),
    );

    sequencing.context.entry(U256::from(101)).or_default();

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

    let tx = create_transaction(100, 1, 5, tx_hash1, None);
    let reorg = create_reorg(100, 1, 5, tx_hash2); // Different hash!

    // Add transaction to sent_events
    sequencing
        .context
        .entry(U256::from(100))
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
    let ctx = sequencing.context.get(&U256::from(100)).unwrap();
    let sent = ctx.sent_events.get(&1).unwrap();
    assert_eq!(sent.len(), 1); // Transaction + invalid Reorg both recorded
}

#[test]
fn test_send_event_recursive_empty_block_commit() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Commit with 0 transactions
    let commit = create_commit_head(100, 1, 0, None);
    let commit_metadata = EventMetadata::from(&commit);

    sequencing.context.entry(U256::from(100)).or_default();
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
    let tx0 = create_transaction(block_num, iter_id, 0, TxHash::random(), None);
    let tx1 = create_transaction(block_num, iter_id, 1, TxHash::random(), None);
    let tx2 = create_transaction(block_num, iter_id, 2, TxHash::random(), None);
    let tx3 = create_transaction(block_num, iter_id, 3, TxHash::random(), None);
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
    let tx0 = create_transaction(block_num, iter_id, 0, TxHash::random(), None);
    let tx1 = create_transaction(block_num, iter_id, 1, tx_hash_to_reorg, None);
    let reorg1 = create_reorg(block_num, iter_id, 1, tx_hash_to_reorg);
    let tx2 = create_transaction(block_num, iter_id, 1, TxHash::random(), None);
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

        let ctx = sequencing.context.entry(U256::from(block_num)).or_default();
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
            HashMap::from([(CompleteEventMetadata::from(tx0_meta.clone()), tx0.clone())]),
        );
        ctx.dependency_graph.insert(
            tx0_meta.clone(),
            HashMap::from([
                (CompleteEventMetadata::from(tx1_meta.clone()), tx1.clone()),
                (CompleteEventMetadata::from(tx2_meta.clone()), tx2.clone()),
            ]),
        );
        ctx.dependency_graph.insert(
            tx1_meta.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(reorg1_meta.clone()),
                reorg1.clone(),
            )]),
        );
        ctx.dependency_graph.insert(
            tx2_meta.clone(),
            HashMap::from([(CompleteEventMetadata::from(commit_meta), commit.clone())]),
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
            !sequencing.context.contains_key(&U256::from(block_num)),
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
    let tx1_0 = create_transaction(block_num, 1, 0, TxHash::random(), None);
    let tx1_1 = create_transaction(block_num, 1, 1, TxHash::random(), None);

    let new_iter2 = create_new_iteration(block_num, 2);
    let tx2_0 = create_transaction(block_num, 2, 0, TxHash::random(), None);
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
        let ctx = sequencing.context.entry(U256::from(block_num)).or_default();

        // Chain for iteration 1: new_iter1 -> tx1_0 -> tx1_1
        let new_iter1_meta = EventMetadata::from(&new_iter1);
        let tx1_0_meta = EventMetadata::from(&tx1_0);
        let tx1_1_meta = EventMetadata::from(&tx1_1);

        ctx.dependency_graph.insert(
            new_iter1_meta.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(tx1_0_meta.clone()),
                tx1_0.clone(),
            )]),
        );
        ctx.dependency_graph.insert(
            tx1_0_meta,
            HashMap::from([(CompleteEventMetadata::from(tx1_1_meta), tx1_1.clone())]),
        );

        // Chain for iteration 2: new_iter2 -> tx2_0 -> commit2
        let new_iter2_meta = EventMetadata::from(&new_iter2);
        let tx2_0_meta = EventMetadata::from(&tx2_0);
        let commit2_meta = EventMetadata::from(&commit2);

        ctx.dependency_graph.insert(
            new_iter2_meta.clone(),
            HashMap::from([(
                CompleteEventMetadata::from(tx2_0_meta.clone()),
                tx2_0.clone(),
            )]),
        );
        ctx.dependency_graph.insert(
            tx2_0_meta,
            HashMap::from([(CompleteEventMetadata::from(commit2_meta), commit2.clone())]),
        );

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
        let ctx = sequencing.context.get(&U256::from(block_num)).unwrap();
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
            !sequencing.context.contains_key(&U256::from(block_num)),
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
    let tx100 = create_transaction(100, 1, 0, TxHash::random(), None);
    let commit100 = create_commit_head(100, 1, 1, Some(TxHash::random()));
    let new_iter101 = create_new_iteration(101, 1);
    let tx101 = create_transaction(101, 1, 0, TxHash::random(), None);

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
            !sequencing.context.contains_key(&U256::from(99)),
            "Block 99 context should be deleted (seed: {seed})",
        );
        assert!(
            !sequencing.context.contains_key(&U256::from(100)),
            "Block 100 context should be deleted (seed: {seed})",
        );
        assert!(
            sequencing.context.contains_key(&U256::from(101)),
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
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
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
        .process_event(create_transaction(1, 1, 0, TxHash::random(), None))
        .unwrap();
    sequencing
        .process_event(create_commit_head(1, 1, 1, Some(TxHash::random())))
        .unwrap();

    // Only CommitHead(1) should be sent
    assert_eq!(engine_recv.len(), 1);

    // current_head advanced to 1
    assert_eq!(sequencing.current_head, 1);

    // Context cleaned
    assert!(!sequencing.context.contains_key(&U256::from(1)));
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
            .process_event(create_transaction(block, 1, 0, TxHash::random(), None))
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
        assert!(!sequencing.context.contains_key(&U256::from(block)));
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
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
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
        .process_event(create_transaction(150, 1, 0, TxHash::random(), None))
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
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
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

#[test]
fn test_multiple_gaps_with_queued_events() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue events for blocks 10, 20, 30 (multiple gaps)
    for block in [10u64, 20, 30] {
        sequencing
            .process_event(create_new_iteration(block, 1))
            .unwrap();
        sequencing
            .process_event(create_transaction(block, 1, 0, TxHash::random(), None))
            .unwrap();
    }

    // Nothing sent yet (all future blocks)
    assert_eq!(engine_recv.len(), 0);

    // Fill first gap with CommitHead(9)
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // CommitHead(9) + block 10 events = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 9);
    engine_recv.try_iter().for_each(drop);

    // Process block 10's commit to trigger the next gap
    sequencing
        .process_event(create_commit_head(10, 1, 1, Some(TxHash::random())))
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 10);
    engine_recv.try_iter().for_each(drop);

    // Fill second gap with CommitHead(19)
    sequencing
        .process_event(create_commit_head(19, 1, 0, None))
        .unwrap();

    // CommitHead(19) + block 20 events = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 19);
    engine_recv.try_iter().for_each(drop);

    // Fill second gap with CommitHead(29)
    sequencing
        .process_event(create_commit_head(29, 1, 1, Some(TxHash::random())))
        .unwrap();

    // CommitHead(29) + block 30 events = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 29);
}

#[test]
fn test_partial_block_queued_then_gap_filled_invalid_prev_hash() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue only NewIteration and first transaction for block 10 (incomplete)
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
        .unwrap();

    // Nothing sent (future block)
    assert_eq!(engine_recv.len(), 0);

    // Fill gap with CommitHead(9)
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // CommitHead(9) + NewIteration(10) + Tx(10,0) = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 9);
    engine_recv.try_iter().for_each(drop);

    // Now send more transactions for block 10
    sequencing
        .process_event(create_transaction(10, 1, 1, TxHash::random(), None))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 2, TxHash::random(), None))
        .unwrap();

    // These should not be sent as the prev hash is invalid
    assert_eq!(engine_recv.len(), 0);
}

#[test]
fn test_partial_block_queued_then_gap_filled_valid_prev_hash() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue only NewIteration and first transaction for block 10 (incomplete)
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    let tx1_hash = TxHash::random();
    sequencing
        .process_event(create_transaction(10, 1, 0, tx1_hash, None))
        .unwrap();

    // Nothing sent (future block)
    assert_eq!(engine_recv.len(), 0);

    // Fill gap with CommitHead(9)
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // CommitHead(9) + NewIteration(10) + Tx(10,0) = 3 events
    assert_eq!(engine_recv.len(), 3);
    assert_eq!(sequencing.current_head, 9);
    engine_recv.try_iter().for_each(drop);

    // Now send more transactions for block 10
    let tx2_hash = TxHash::random();
    sequencing
        .process_event(create_transaction(10, 1, 1, tx2_hash, Some(tx1_hash)))
        .unwrap();
    sequencing
        .process_event(create_transaction(
            10,
            1,
            2,
            TxHash::random(),
            Some(tx2_hash),
        ))
        .unwrap();

    // These should be sent immediately since block 10 is now current
    assert_eq!(engine_recv.len(), 2);
}

#[test]
fn test_reorg_in_future_block() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue events for block 10 including a reorg
    let tx_hash = TxHash::random();
    let tx_hash1 = TxHash::random();
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, tx_hash1, None))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 1, tx_hash, Some(tx_hash1)))
        .unwrap();
    sequencing
        .process_event(create_reorg(10, 1, 1, tx_hash))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 1, TxHash::random(), None))
        .unwrap();

    // Nothing sent (future block)
    assert_eq!(engine_recv.len(), 0);

    // Fill gap
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // Should process all events including reorg cancellation
    // CommitHead(9) + NewIteration(10) + Tx(0) + Tx(1) = 4 events (reorg cancels)
    assert_eq!(engine_recv.len(), 4);
}

#[test]
fn test_old_events_after_gap_jump() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Jump to block 50
    sequencing
        .process_event(create_commit_head(50, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 50);
    engine_recv.try_iter().for_each(drop);

    // Try to send events for old blocks (30, 40)
    sequencing
        .process_event(create_new_iteration(30, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(30, 1, 0, TxHash::random(), None))
        .unwrap();
    sequencing
        .process_event(create_new_iteration(40, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(40, 1, 0, TxHash::random(), None))
        .unwrap();

    // Nothing should be sent (all old blocks)
    assert_eq!(engine_recv.len(), 0);

    // current_head should remain at 50
    assert_eq!(sequencing.current_head, 50);
}

#[test]
fn test_old_commit_head_ignored() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Advance to block 9
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 9);
    engine_recv.try_iter().for_each(drop);

    // Send CommitHead(5) - should be ignored (old)
    sequencing
        .process_event(create_commit_head(5, 1, 0, None))
        .unwrap();

    assert_eq!(engine_recv.len(), 0);
    assert_eq!(sequencing.current_head, 9);
}

#[test]
fn test_multiple_iterations_in_future_block() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue two iterations for block 10
    // Iteration 1
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
        .unwrap();

    // Iteration 2
    sequencing
        .process_event(create_new_iteration(10, 2))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 2, 0, TxHash::random(), None))
        .unwrap();

    // Nothing sent (future block)
    assert_eq!(engine_recv.len(), 0);

    // Fill gap
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // Both iterations should process
    // CommitHead(9) + Iter1 events + Iter2 events
    assert_eq!(engine_recv.len(), 5);
    assert_eq!(sequencing.current_head, 9);
}

#[test]
fn test_interleaved_gap_filling() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue events for blocks 10, 15, 20 in mixed order
    sequencing
        .process_event(create_new_iteration(15, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(15, 1, 0, TxHash::random(), None))
        .unwrap();

    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
        .unwrap();

    sequencing
        .process_event(create_new_iteration(20, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(20, 1, 0, TxHash::random(), None))
        .unwrap();

    // Fill the gap to block 10
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // Only block 10 should process
    assert_eq!(engine_recv.len(), 3);
    engine_recv.try_iter().for_each(drop);

    // Fill gap to block 15
    sequencing
        .process_event(create_commit_head(14, 1, 0, None))
        .unwrap();

    // Block 15 should process
    assert_eq!(engine_recv.len(), 3);
}

#[test]
fn test_gap_with_only_commit_heads() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Queue only commit heads for future blocks (no transactions)
    sequencing
        .process_event(create_commit_head(10, 1, 0, None))
        .unwrap();
    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 10);
    engine_recv.try_iter().for_each(drop);

    sequencing
        .process_event(create_commit_head(20, 1, 0, None))
        .unwrap();
    assert_eq!(engine_recv.len(), 1);
    assert_eq!(sequencing.current_head, 20);
}

#[test]
fn test_alternating_future_and_current_events() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, 1);
    engine_recv.try_iter().for_each(drop);

    // Alternate between the current block (2) and future block (10)
    sequencing
        .process_event(create_new_iteration(2, 1))
        .unwrap();
    sequencing
        .process_event(create_new_iteration(10, 1))
        .unwrap();
    sequencing
        .process_event(create_transaction(2, 1, 0, TxHash::random(), None))
        .unwrap();
    sequencing
        .process_event(create_transaction(10, 1, 0, TxHash::random(), None))
        .unwrap();

    // Only block 2 events should be sent (2 events)
    assert_eq!(engine_recv.len(), 2);
    engine_recv.try_iter().for_each(drop);

    // Commit block 2
    sequencing
        .process_event(create_commit_head(2, 1, 1, Some(TxHash::random())))
        .unwrap();
    assert_eq!(sequencing.current_head, 2);
    engine_recv.try_iter().for_each(drop);

    // Fill the gap to trigger block 10
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // Block 10 events should now be sent
    assert_eq!(engine_recv.len(), 3);
}

#[crate::utils::engine_test(grpc)]
async fn test_large_block_number_jump(mut instance: LocalInstance<_>) {
    // Process the first few blocks normally
    for _ in 0..3 {
        instance.new_block().await.unwrap();
        instance
            .wait_for_processing(Duration::from_millis(10))
            .await;
    }

    // Simulate a large jump (like reconnecting after being offline)
    // Set the internal block number to simulate a gap
    let start_block = instance.block_number;
    instance.block_number = start_block + U256::from(100);

    // Continue processing - the system should handle the gap
    instance.new_block().await.unwrap();
    instance
        .send_successful_create_tx_dry(U256::ZERO, Bytes::default())
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Verify we're at the new block
    assert!(instance.block_number >= start_block + U256::from(100));
}

#[crate::utils::engine_test(grpc)]
async fn test_future_block_transaction_queuing(mut instance: LocalInstance<_>) {
    // MUST send CommitHead FIRST to initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Now send NewIteration for block 1 (current_head + 1 = 0 + 1 = 1)
    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send NewIteration for FUTURE block 10 (will be queued)
    let future_block_env = BlockEnv {
        number: U256::from(10),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, future_block_env)
        .await
        .unwrap();

    // Send transaction for future block 10 (should be queued, not executed)
    let tx_hash = TxHash::random();
    let tx_execution_id = TxExecutionId {
        block_number: U256::from(10),
        iteration_id: 1,
        tx_hash,
        index: 0,
    };

    let tx_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance.transport.set_prev_tx_hash(None);

    instance
        .transport
        .send_transaction(tx_execution_id, tx_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Verify that the transaction is NOT processed yet (queued for future block)
    assert!(
        instance.get_transaction_result(&tx_execution_id).is_none(),
        "Transaction for future block should not be processed yet"
    );

    // Advance through blocks 1-8
    for block in 1..9 {
        // Send NewIteration for the next block
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();

        // Commit current block
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
        instance.wait_for_processing(Duration::from_millis(5)).await;
    }

    assert!(
        instance.get_transaction_result(&tx_execution_id).is_none(),
        "Transaction for future block should not be processed yet"
    );

    // Send NewIteration for the next block
    let next_block_env = BlockEnv {
        number: U256::from(9),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, next_block_env)
        .await
        .unwrap();

    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(9);
    instance
        .transport
        .new_block(U256::from(9), 1, 0)
        .await
        .unwrap();
    instance.wait_for_processing(Duration::from_millis(5)).await;

    // Now the transaction should be processed!
    let result = instance
        .wait_for_transaction_processed(&tx_execution_id)
        .await;
    assert!(
        result.is_ok(),
        "Transaction should be processed after reaching block 10: {result:?}",
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_future_block_out_of_order_transactions(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Create 3 transactions for FUTURE block 5 (DON'T send NewIteration yet)
    let tx1_hash = TxHash::random();
    let tx2_hash = TxHash::random();
    let tx3_hash = TxHash::random();

    let tx1_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };
    let tx3_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash: tx3_hash,
        index: 2,
    };

    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();

    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();

    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();

    // Send transactions OUT OF ORDER: tx2, tx3, tx1 (for future block 5)
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();
    instance
        .transport
        .send_transaction(tx3_id, tx3_env)
        .await
        .unwrap();
    instance.transport.set_prev_tx_hash(None);
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Verify NONE are processed yet (all queued for future block)
    assert!(instance.get_transaction_result(&tx1_id).is_none());
    assert!(instance.get_transaction_result(&tx2_id).is_none());
    assert!(instance.get_transaction_result(&tx3_id).is_none());

    // Advance through blocks 1-5, sending NewIteration as we go
    for block in 1..=5 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .wait_for_processing(Duration::from_millis(10))
            .await;

        if block < 5 {
            // Commit blocks 1-4
            instance
                .eth_rpc_source_http_mock
                .send_new_head_with_block_number(block);
            instance
                .transport
                .new_block(U256::from(block), 1, 0)
                .await
                .unwrap();
            instance.wait_for_processing(Duration::from_millis(5)).await;

            // Transactions should still not be processed
            assert!(instance.get_transaction_result(&tx1_id).is_none());
            assert!(instance.get_transaction_result(&tx2_id).is_none());
            assert!(instance.get_transaction_result(&tx3_id).is_none());
        }
    }

    // After sending NewIteration(5), transactions should be processed IN ORDER
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx3_id)
        .await
        .unwrap();

    assert!(instance.get_transaction_result(&tx1_id).is_some());
    assert!(instance.get_transaction_result(&tx2_id).is_some());
    assert!(instance.get_transaction_result(&tx3_id).is_some());
}

#[crate::utils::engine_test(grpc)]
async fn test_multiple_future_blocks_interleaved(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Create transactions for future blocks 3, 5, 7 (NO NewIterations yet)
    let tx3_hash = TxHash::random();
    let tx5_hash = TxHash::random();
    let tx7_hash = TxHash::random();

    let tx3_id = TxExecutionId {
        block_number: U256::from(3),
        iteration_id: 1,
        tx_hash: tx3_hash,
        index: 0,
    };
    let tx5_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash: tx5_hash,
        index: 0,
    };
    let tx7_id = TxExecutionId {
        block_number: U256::from(7),
        iteration_id: 1,
        tx_hash: tx7_hash,
        index: 0,
    };

    // Send transactions INTERLEAVED: tx7, tx3, tx5 (all for different blocks)
    let tx7_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        // Nonces are per-sender and must be valid at *execution time*.
        // tx3 executes first (nonce 0), then tx5 (nonce 1), then tx7 (nonce 2).
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx7_id, tx7_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(None);
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx3_id, tx3_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(None);
    let tx5_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx5_id, tx5_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Verify all are queued
    assert!(instance.get_transaction_result(&tx3_id).is_none());
    assert!(instance.get_transaction_result(&tx5_id).is_none());
    assert!(instance.get_transaction_result(&tx7_id).is_none());

    // Advance through blocks 1-7, sending NewIteration as each becomes current
    for block in 1..=7 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .wait_for_processing(Duration::from_millis(10))
            .await;

        // Commit the block (except the last one which we check separately)
        if block < 7 {
            instance
                .eth_rpc_source_http_mock
                .send_new_head_with_block_number(block);
            instance
                .transport
                .new_block(U256::from(block), 1, 0)
                .await
                .unwrap();
            instance
                .wait_for_processing(Duration::from_millis(10))
                .await;
        }

        // Check which transactions should be processed by now
        match block {
            1..=2 => {
                assert!(instance.get_transaction_result(&tx3_id).is_none());
                assert!(instance.get_transaction_result(&tx5_id).is_none());
                assert!(instance.get_transaction_result(&tx7_id).is_none());
            }
            3 => {
                // After NewIteration(3), tx3 should be processed
                instance
                    .wait_for_transaction_processed(&tx3_id)
                    .await
                    .unwrap();
                assert!(instance.get_transaction_result(&tx3_id).is_some());
                assert!(instance.get_transaction_result(&tx5_id).is_none());
                assert!(instance.get_transaction_result(&tx7_id).is_none());
            }
            4 => {
                assert!(instance.get_transaction_result(&tx3_id).is_some());
                assert!(instance.get_transaction_result(&tx5_id).is_none());
                assert!(instance.get_transaction_result(&tx7_id).is_none());
            }
            5 => {
                // After NewIteration(5), tx5 should be processed
                instance
                    .wait_for_transaction_processed(&tx5_id)
                    .await
                    .unwrap();
                assert!(instance.get_transaction_result(&tx3_id).is_some());
                assert!(instance.get_transaction_result(&tx5_id).is_some());
                assert!(instance.get_transaction_result(&tx7_id).is_none());
            }
            6 => {
                assert!(instance.get_transaction_result(&tx3_id).is_some());
                assert!(instance.get_transaction_result(&tx5_id).is_some());
                assert!(instance.get_transaction_result(&tx7_id).is_none());
            }
            7 => {
                // After NewIteration(7), tx7 should be processed
                instance
                    .wait_for_transaction_processed(&tx7_id)
                    .await
                    .unwrap();
                assert!(instance.get_transaction_result(&tx3_id).is_some());
                assert!(instance.get_transaction_result(&tx5_id).is_some());
                assert!(instance.get_transaction_result(&tx7_id).is_some());
            }
            _ => {}
        }
    }
}

#[crate::utils::engine_test(grpc)]
async fn test_partial_transaction_chain_backwards(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Create a chain of 4 transactions
    let tx_hashes: Vec<TxHash> = (0..4).map(|_| TxHash::random()).collect();
    let tx_ids: Vec<TxExecutionId> = tx_hashes
        .iter()
        .enumerate()
        .map(|(i, &hash)| {
            TxExecutionId {
                block_number: U256::from(4),
                iteration_id: 1,
                tx_hash: hash,
                index: i as u64,
            }
        })
        .collect();

    // Send in this order: tx1, tx3, tx2, tx0
    //
    // The chain we want is: tx0 -> tx1 -> tx2 -> tx3.
    // Since we're sending backwards/interleaved, we manually set `prev_tx_hash`
    // to the expected predecessor hash for each tx.
    //
    // tx1 depends on tx0
    instance.transport.set_prev_tx_hash(Some(tx_hashes[0]));
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[1], tx1_env)
        .await
        .unwrap();

    // tx3 depends on tx2 (even though tx2 hasn't been sent yet)
    instance.transport.set_prev_tx_hash(Some(tx_hashes[2]));
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(3)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[3], tx3_env)
        .await
        .unwrap();

    // tx2 with prev = tx1 (override to tx1)
    instance.transport.set_prev_tx_hash(Some(tx_hashes[1]));
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[2], tx2_env)
        .await
        .unwrap();

    // tx0 with prev = None
    instance.transport.set_prev_tx_hash(None);
    let tx0_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[0], tx0_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Verify all are queued
    for tx_id in &tx_ids {
        assert!(instance.get_transaction_result(tx_id).is_none());
    }

    // Advance to block 4
    for block in 1..4 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
        instance.wait_for_processing(Duration::from_millis(5)).await;
    }

    // Verify all are queued
    for tx_id in &tx_ids {
        assert!(instance.get_transaction_result(tx_id).is_none());
    }

    // Send NewIteration(4) to trigger processing
    let block4_env = BlockEnv {
        number: U256::from(4),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block4_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // All should be processed
    for tx_id in &tx_ids {
        instance
            .wait_for_transaction_processed(tx_id)
            .await
            .unwrap();
        assert!(instance.get_transaction_result(tx_id).is_some());
    }
}

#[crate::utils::engine_test(grpc)]
async fn test_mixed_order_iterations(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Start with block 1 normally
    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();

    // Send a transaction for block 1 (immediate processing)
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // tx1 should be processed immediately
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    assert!(instance.get_transaction_result(&tx1_id).is_some());

    // Now send future block 5 with transactions (will be queued)
    let block5_env = BlockEnv {
        number: U256::from(5),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block5_env.clone())
        .await
        .unwrap();

    let tx5_hash = TxHash::random();
    let tx5_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash: tx5_hash,
        index: 0,
    };

    let tx5_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx5_id, tx5_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // tx5 should NOT be processed yet
    assert!(instance.get_transaction_result(&tx5_id).is_none());

    // Commit block 1 and advance through blocks 2-4
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(1);
    instance
        .transport
        .new_block(U256::from(1), 1, 1)
        .await
        .unwrap();
    instance.wait_for_processing(Duration::from_millis(5)).await;

    for block in 2..5 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
        instance.wait_for_processing(Duration::from_millis(5)).await;
    }

    // Resend NewIteration(5) to trigger processing
    instance
        .transport
        .new_iteration(1, block5_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Now tx5 should be processed
    instance
        .wait_for_transaction_processed(&tx5_id)
        .await
        .unwrap();
    assert!(instance.get_transaction_result(&tx5_id).is_some());
}

#[crate::utils::engine_test(grpc)]
async fn test_future_block_reorg_basic(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send transaction for FUTURE block 5
    let tx_hash = TxHash::random();
    let tx_id = TxExecutionId {
        block_number: U256::from(5),
        iteration_id: 1,
        tx_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_id, tx_env)
        .await
        .unwrap();

    // Send reorg for the same transaction (should be queued)
    instance.transport.reorg(tx_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Transaction should not be processed yet (future block)
    assert!(instance.get_transaction_result(&tx_id).is_none());

    // Advance through blocks to reach block 5
    for block in 1..=5 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .wait_for_processing(Duration::from_millis(10))
            .await;

        if block < 5 {
            instance
                .eth_rpc_source_http_mock
                .send_new_head_with_block_number(block);
            instance
                .transport
                .new_block(U256::from(block), 1, 0)
                .await
                .unwrap();
            instance.wait_for_processing(Duration::from_millis(5)).await;
        }
    }

    // After NewIteration(5), transaction and reorg should cancel each other
    // So no transaction result should exist
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        instance.get_transaction_result(&tx_id).is_none(),
        "Transaction should be cancelled by reorg"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_future_block_reorg_with_one_replacement(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send first transaction for future block 4
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();

    // Send reorg for first transaction
    instance.transport.reorg(tx1_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send replacement transaction with same index but different hash
    let tx2_hash = TxHash::random();
    let tx2_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Neither should be processed yet
    assert!(instance.get_transaction_result(&tx1_id).is_none());
    assert!(instance.get_transaction_result(&tx2_id).is_none());

    // Advance to block 4
    for block in 1..4 {
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance.transport.set_prev_tx_hash(None);
        instance.transport.set_n_transactions(0);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
    }

    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(4),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // After NewIteration(4):
    // - tx1 should be cancelled by reorg
    // - tx2 (replacement) should be processed
    instance
        .wait_for_transaction_processed(&tx2_id)
        .await
        .unwrap();
    assert!(
        instance.get_transaction_result(&tx1_id).is_none(),
        "Original transaction should not be processed"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_some(),
        "Replacement transaction should be processed"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_future_block_reorg_with_replacement_and_redundant_reorgs(
    mut instance: LocalInstance<_>,
) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // First transaction for future block 4
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    // Second transaction never sent for future block 4
    let tx2_hash = TxHash::random();
    let tx2_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };

    // Send first the reorg for first transaction
    instance.transport.reorg(tx1_id).await.unwrap();
    // Send the reorg for a second transaction which is never sent
    instance.transport.reorg(tx2_id).await.unwrap();

    // Send the transaction to be replaced
    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send replacement transaction with same index but different hash
    let tx3_hash = TxHash::random();
    let tx3_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx3_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx3_id, tx3_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // Neither should be processed yet
    assert!(instance.get_transaction_result(&tx1_id).is_none());
    assert!(instance.get_transaction_result(&tx2_id).is_none());
    assert!(instance.get_transaction_result(&tx3_id).is_none());

    // Advance to block 4 - each block needs NewIteration for the next block
    for block in 1..=3 {
        // NewIteration for the block we're about to build
        let block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, block_env)
            .await
            .unwrap();

        // Finalize the block
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance.transport.set_prev_tx_hash(None);
        instance.transport.set_n_transactions(0);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
    }

    // NewIteration for block 4
    let next_block_env = BlockEnv {
        number: U256::from(4),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, next_block_env)
        .await
        .unwrap();

    // After NewIteration(4):
    // - tx1 should be cancelled by reorg
    // - reorg tx2 sould be ignored
    // - tx3 (replacement) should be processed
    instance
        .wait_for_transaction_processed(&tx3_id)
        .await
        .unwrap();
    assert!(
        instance.get_transaction_result(&tx1_id).is_none(),
        "Original transaction should not be processed"
    );
    assert!(
        instance.get_transaction_result(&tx3_id).is_some(),
        "Replacement transaction should be processed"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_old_reorg_for_current_block_same_hash(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send tx1
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send tx2 (depends on tx1)
    let tx2_hash = TxHash::random();
    let tx2_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };

    instance.transport.set_prev_tx_hash(Some(tx1_hash));
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Both should be processed
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_id)
        .await
        .unwrap();
    assert!(instance.get_transaction_result(&tx1_id).is_some());
    assert!(instance.get_transaction_result(&tx2_id).is_some());

    // Send old reorg for tx1 (should be ignored)
    instance.transport.reorg(tx1_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Both transactions should still be processed (reorg ignored)
    assert!(
        instance.get_transaction_result(&tx1_id).is_some(),
        "tx1 should still be processed (old reorg ignored)"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_some(),
        "tx2 should still be processed (old reorg ignored)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_old_reorg_for_current_block_different_hash(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send tx1
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send tx2 (depends on tx1)
    let tx2_hash = TxHash::random();
    let tx2_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };

    instance.transport.set_prev_tx_hash(Some(tx1_hash));
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Both should be processed
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_id)
        .await
        .unwrap();

    // Send old reorg for tx1 with different hash (should be ignored)
    let tx1_different_hash = TxHash::random();
    let tx1_different_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_different_hash,
        index: 0,
    };
    instance.transport.reorg(tx1_different_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Original transactions should still be processed
    assert!(
        instance.get_transaction_result(&tx1_id).is_some(),
        "tx1 should still be processed (old reorg with different hash ignored)"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_some(),
        "tx2 should still be processed"
    );
    assert!(
        instance.get_transaction_result(&tx1_different_id).is_none(),
        "Different hash transaction should not exist"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_old_reorg_for_future_block_breaks_chain(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send a chain of transactions for future block 3: tx1 -> tx2 -> tx3
    let tx_hashes: Vec<TxHash> = (0..3).map(|_| TxHash::random()).collect();
    let tx_ids: Vec<TxExecutionId> = tx_hashes
        .iter()
        .enumerate()
        .map(|(i, &hash)| {
            TxExecutionId {
                block_number: U256::from(3),
                iteration_id: 1,
                tx_hash: hash,
                index: i as u64,
            }
        })
        .collect();

    // Send tx1
    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[0], tx1_env)
        .await
        .unwrap();

    // Send tx2 (depends on tx1)
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[1], tx2_env)
        .await
        .unwrap();

    // Send tx3 (depends on tx2)
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx_ids[2], tx3_env)
        .await
        .unwrap();

    // Send reorg for tx2 (breaks the chain)
    instance.transport.reorg(tx_ids[1]).await.unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // All should be queued (future block)
    for tx_id in &tx_ids {
        assert!(instance.get_transaction_result(tx_id).is_none());
    }

    // Advance to block 3
    for block in 1..=3 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .transport
            .new_iteration(1, next_block_env)
            .await
            .unwrap();
        instance
            .wait_for_processing(Duration::from_millis(10))
            .await;

        if block < 3 {
            instance
                .eth_rpc_source_http_mock
                .send_new_head_with_block_number(block);
            instance
                .transport
                .new_block(U256::from(block), 1, 0)
                .await
                .unwrap();
            instance.wait_for_processing(Duration::from_millis(5)).await;
        }
    }

    // After NewIteration(3):
    // - tx1 should be processed (no dependency issues)
    // - tx2 should be cancelled by reorg
    // - tx3 should NOT be processed (depends on cancelled tx2)
    instance
        .wait_for_transaction_processed(&tx_ids[0])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        instance.get_transaction_result(&tx_ids[0]).is_some(),
        "tx1 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx_ids[1]).is_none(),
        "tx2 should be cancelled by reorg"
    );
    assert!(
        instance.get_transaction_result(&tx_ids[2]).is_none(),
        "tx3 should not be processed (depends on cancelled tx2)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_old_reorg_future_block_with_replacement_when_current(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(2);
    instance
        .transport
        .new_block(U256::from(2), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send the first chain for future block 3: tx1 -> tx2 -> tx3
    let tx1_hash = TxHash::random();
    let tx2_hash_old = TxHash::random();
    let tx3_hash = TxHash::random();

    let tx1_id = TxExecutionId {
        block_number: U256::from(3),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2_id_old = TxExecutionId {
        block_number: U256::from(3),
        iteration_id: 1,
        tx_hash: tx2_hash_old,
        index: 1,
    };
    let tx3_id = TxExecutionId {
        block_number: U256::from(3),
        iteration_id: 1,
        tx_hash: tx3_hash,
        index: 2,
    };

    // Send tx1
    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send tx2 (old version, will be reorged)
    instance.transport.set_prev_tx_hash(Some(tx1_hash));
    let tx2_env_old = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id_old, tx2_env_old)
        .await
        .unwrap();

    // Send tx3 (depends on old tx2)
    instance.transport.set_prev_tx_hash(Some(tx2_hash_old));
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx3_id, tx3_env)
        .await
        .unwrap();

    // Send reorg for old tx2
    instance.transport.reorg(tx2_id_old).await.unwrap();

    // Send new tx2 (replacement with different hash)
    let tx2_hash_new = TxHash::random();
    let tx2_id_new = TxExecutionId {
        block_number: U256::from(3),
        iteration_id: 1,
        tx_hash: tx2_hash_new,
        index: 1,
    };

    instance.transport.set_prev_tx_hash(Some(tx1_hash));
    let tx2_env_new = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id_new, tx2_env_new)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    let block_env = BlockEnv {
        number: U256::from(3),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // After NewIteration(3):
    // - tx1 should be processed
    // - old tx2 should be cancelled by reorg
    // - new tx2 should be processed
    // - tx3 should NOT be processed (its prev_hash points to old tx2)
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_id_new)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        instance.get_transaction_result(&tx1_id).is_some(),
        "tx1 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx2_id_old).is_none(),
        "old tx2 should be cancelled by reorg"
    );
    assert!(
        instance.get_transaction_result(&tx2_id_new).is_some(),
        "new tx2 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx3_id).is_none(),
        "tx3 should not be processed (prev_hash points to reorged tx2)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_old_reorg_future_block_two_chains(mut instance: LocalInstance<_>) {
    // Initialize
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // First chain: tx1.1 -> tx1.2 -> tx1.3 (all for future block 4)
    let tx1_1_hash = TxHash::random();
    let tx1_2_hash = TxHash::random();
    let tx1_3_hash = TxHash::random();

    let tx1_1_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx1_1_hash,
        index: 0,
    };
    let tx1_2_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx1_2_hash,
        index: 1,
    };
    let tx1_3_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx1_3_hash,
        index: 2,
    };

    // Send the first chain
    instance.transport.set_prev_tx_hash(None);
    let tx1_1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_1_id, tx1_1_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(Some(tx1_1_id.tx_hash));
    let tx1_2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_2_id, tx1_2_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(Some(tx1_2_id.tx_hash));
    let tx1_3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_3_id, tx1_3_env)
        .await
        .unwrap();

    // Second chain (replacement): tx2.2 -> tx2.3 -> tx2.4
    // tx2.2 replaces tx1.2, tx2.3 replaces tx1.3, tx2.4 is new
    let tx2_2_hash = TxHash::random();
    let tx2_3_hash = TxHash::random();
    let tx2_4_hash = TxHash::random();

    let tx2_2_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx2_2_hash,
        index: 1,
    };
    let tx2_3_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx2_3_hash,
        index: 2,
    };
    let tx2_4_id = TxExecutionId {
        block_number: U256::from(4),
        iteration_id: 1,
        tx_hash: tx2_4_hash,
        index: 3,
    };

    // Send replacement chain (starts from tx1.1)
    instance.transport.set_prev_tx_hash(Some(tx1_1_hash));
    let tx2_2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_2_id, tx2_2_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(Some(tx2_2_hash));
    let tx2_3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_3_id, tx2_3_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(Some(tx2_3_hash));
    let tx2_4_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(3)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_4_id, tx2_4_env)
        .await
        .unwrap();

    // Send reorg for tx1.2 (invalidates the first chain from tx1.2 onwards)
    instance.transport.reorg(tx1_2_id).await.unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    instance.transport.reorg(tx1_3_id).await.unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    // All should be queued (future block)
    assert!(instance.get_transaction_result(&tx1_1_id).is_none());
    assert!(instance.get_transaction_result(&tx1_2_id).is_none());
    assert!(instance.get_transaction_result(&tx1_3_id).is_none());
    assert!(instance.get_transaction_result(&tx2_2_id).is_none());
    assert!(instance.get_transaction_result(&tx2_3_id).is_none());
    assert!(instance.get_transaction_result(&tx2_4_id).is_none());

    // Advance to block 4
    for block in 1..4 {
        let next_block_env = BlockEnv {
            number: U256::from(block),
            gas_limit: 50_000_000,
            ..Default::default()
        };
        instance
            .eth_rpc_source_http_mock
            .send_new_head_with_block_number(block);
        instance
            .transport
            .new_block(U256::from(block), 1, 0)
            .await
            .unwrap();
    }

    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send new iteration
    let next_block_env = BlockEnv {
        number: U256::from(4),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, next_block_env)
        .await
        .unwrap();

    // After NewIteration(4):
    // Processed: tx1.1 (no dependency issues), tx2.2, tx2.3, tx2.4 (valid chain)
    // Not processed: tx1.2 (reorged), tx1.3 (depends on reorged tx1.2)
    instance
        .wait_for_transaction_processed(&tx1_1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_2_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_3_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_4_id)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        instance.get_transaction_result(&tx1_1_id).is_some(),
        "tx1.1 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx1_2_id).is_none(),
        "tx1.2 should be cancelled by reorg"
    );
    assert!(
        instance.get_transaction_result(&tx1_3_id).is_none(),
        "tx1.3 should not be processed (depends on reorged tx1.2)"
    );
    assert!(
        instance.get_transaction_result(&tx2_2_id).is_some(),
        "tx2.2 should be processed (replacement)"
    );
    assert!(
        instance.get_transaction_result(&tx2_3_id).is_some(),
        "tx2.3 should be processed (replacement)"
    );
    assert!(
        instance.get_transaction_result(&tx2_4_id).is_some(),
        "tx2.4 should be processed (new transaction)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_non_sequential_prev_tx_hash_skips_transaction(mut instance: LocalInstance<_>) {
    // Test that tx3 can depend on tx1, effectively making tx2 independent
    // tx1 (index 0, prev_tx_hash = None)
    // tx2 (index 1, prev_tx_hash = None)
    // tx3 (index 1, prev_tx_hash = tx1) - skips tx2
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let tx1_hash = TxHash::random();
    let tx2_hash = TxHash::random();
    let tx3_hash = TxHash::random();

    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };
    let tx3_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx3_hash,
        index: 1,
    };

    // Send tx1
    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send tx2 (independent, prev_tx_hash = None)
    instance.transport.set_prev_tx_hash(None);
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();

    // Send tx3 (depends on tx1, skipping tx2)
    instance.transport.set_prev_tx_hash(Some(tx1_hash));
    let tx3_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx3_id, tx3_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx3_id)
        .await
        .unwrap();

    assert!(
        instance.get_transaction_result(&tx1_id).is_some(),
        "tx1 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_none(),
        "tx2 should not be processed"
    );
    assert!(
        instance.get_transaction_result(&tx3_id).is_some(),
        "tx3 should be processed (depends on tx1)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_invalid_prev_tx_hash_skips_transaction(mut instance: LocalInstance<_>) {
    // Test that a transaction with wrong prev_tx_hash is never processed
    // tx0 (index 0, prev_tx_hash = None)
    // tx1 (index 1, prev_tx_hash = wrong_hash) - should be skipped
    // tx2 (index 2, prev_tx_hash = tx0) - should process

    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let tx0_hash = TxHash::random();
    let tx1_hash = TxHash::random();
    let tx2_hash = TxHash::random();
    let wrong_hash = TxHash::random();

    let tx0_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx0_hash,
        index: 0,
    };
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 1,
    };
    let tx2_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 1,
    };

    // Send tx0
    instance.transport.set_prev_tx_hash(None);
    let tx0_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx0_id, tx0_env)
        .await
        .unwrap();

    // Send tx1 with WRONG prev_tx_hash
    instance.transport.set_prev_tx_hash(Some(wrong_hash));
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Send tx2 with the correct prev_tx_hash pointing to tx0
    instance.transport.set_prev_tx_hash(Some(tx0_hash));
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    instance
        .wait_for_transaction_processed(&tx0_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx2_id)
        .await
        .unwrap();

    assert!(
        instance.get_transaction_result(&tx0_id).is_some(),
        "tx0 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx1_id).is_none(),
        "tx1 should not be processed (wrong prev_tx_hash)"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_some(),
        "tx2 should be processed (depends on tx0)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_chain_with_invalid_middle_link(mut instance: LocalInstance<_>) {
    // Test chain tx0 -> tx1 -> tx2, but tx2 has wrong prev_tx_hash
    // tx0 (index 0, prev_tx_hash = None)
    // tx1 (index 1, prev_tx_hash = tx0)
    // tx2 (index 2, prev_tx_hash = tx0) - WRONG, should be tx1

    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let tx0_hash = TxHash::random();
    let tx1_hash = TxHash::random();
    let tx2_hash = TxHash::random();

    let tx0_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx0_hash,
        index: 0,
    };
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 1,
    };
    let tx2_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx2_hash,
        index: 2,
    };

    // Send all out of order
    instance.transport.set_prev_tx_hash(Some(tx0_hash));
    let tx2_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(2)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx2_id, tx2_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(Some(tx0_hash));
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance.transport.set_prev_tx_hash(None);
    let tx0_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx0_id, tx0_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    instance
        .wait_for_transaction_processed(&tx0_id)
        .await
        .unwrap();
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();

    assert!(
        instance.get_transaction_result(&tx0_id).is_some(),
        "tx0 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx1_id).is_some(),
        "tx1 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx2_id).is_none(),
        "tx2 should not be processed (wrong prev_tx_hash)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_dependency_arrives_with_wrong_hash(mut instance: LocalInstance<_>) {
    // tx1 (index 1) arrives first, expecting dependency with specific hash
    // tx0 (index 0) arrives with different hash
    // tx1 should never be processed

    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    let expected_tx0_hash = TxHash::random();
    let actual_tx0_hash = TxHash::random();
    let tx1_hash = TxHash::random();

    let tx0_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: actual_tx0_hash,
        index: 0,
    };
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 1,
    };

    // Send tx1 first, expecting a specific prev_tx_hash
    instance.transport.set_prev_tx_hash(Some(expected_tx0_hash));
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(1)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    // Now send tx0 with a different hash
    instance.transport.set_prev_tx_hash(None);
    let tx0_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx0_id, tx0_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(30))
        .await;

    instance
        .wait_for_transaction_processed(&tx0_id)
        .await
        .unwrap();

    assert!(
        instance.get_transaction_result(&tx0_id).is_some(),
        "tx0 should be processed"
    );
    assert!(
        instance.get_transaction_result(&tx1_id).is_none(),
        "tx1 should not be processed (expected different prev_tx_hash)"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_reorg_and_replacement_current_block(mut instance: LocalInstance<_>) {
    // Send commit head for block 0
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send new iteration for block 1
    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send TX1
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // TX1 should be processed
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();

    // Revert TX1
    instance.transport.reorg(tx1_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Send new TX1 (replacement with same index, different hash)
    let tx1_replacement_hash = TxHash::random();
    let tx1_replacement_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_replacement_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_replacement_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_replacement_id, tx1_replacement_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // The replacement TX should be executed
    instance
        .wait_for_transaction_processed(&tx1_replacement_id)
        .await
        .unwrap();
    assert!(
        instance
            .get_transaction_result(&tx1_replacement_id)
            .is_some(),
        "Replacement TX1 should be processed"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_reorg_enables_replacement_transaction(mut instance: LocalInstance<_>) {
    // Send commit head for block 0
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send new iteration for block 1
    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send TX1 (index 0)
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // TX1 should be processed
    instance
        .wait_for_transaction_processed(&tx1_id)
        .await
        .unwrap();

    // Send TX1.1 (index 0, replacement with different hash)
    let tx1_1_hash = TxHash::random();
    let tx1_1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_1_id, tx1_1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // TX1.1 should NOT be processed yet (waiting for TX1 to be reorged)
    assert!(
        instance.get_transaction_result(&tx1_1_id).is_none(),
        "TX1.1 should not be processed yet"
    );

    // Reorg TX1
    instance.transport.reorg(tx1_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // TX1.1 should now be executed
    instance
        .wait_for_transaction_processed(&tx1_1_id)
        .await
        .unwrap();
    assert!(
        instance.get_transaction_result(&tx1_1_id).is_some(),
        "TX1.1 should be processed after TX1 reorg"
    );
}

#[crate::utils::engine_test(grpc)]
async fn test_reorg_enables_replacement_transaction_with_reorg_first(
    mut instance: LocalInstance<_>,
) {
    // Send commit head for block 0
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(0);
    instance
        .transport
        .new_block(U256::from(0), 1, 0)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send new iteration for block 1
    let block1_env = BlockEnv {
        number: U256::from(1),
        gas_limit: 50_000_000,
        ..Default::default()
    };
    instance
        .transport
        .new_iteration(1, block1_env.clone())
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // Send a new iteration twice on purpose
    instance
        .transport
        .new_iteration(1, block1_env)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(10))
        .await;

    // TX1 (index 0)
    let tx1_hash = TxHash::random();
    let tx1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_hash,
        index: 0,
    };

    // Reorg first the reorg for TX1
    instance.transport.reorg(tx1_id).await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    instance.transport.set_prev_tx_hash(None);
    let tx1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_id, tx1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // Send TX1.1 (index 0, replacement with different hash)
    let tx1_1_hash = TxHash::random();
    let tx1_1_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash: tx1_1_hash,
        index: 0,
    };

    instance.transport.set_prev_tx_hash(None);
    let tx1_1_env = TxEnvBuilder::new()
        .caller(instance.default_account)
        .gas_limit(100_000)
        .gas_price(0)
        .kind(TxKind::Create)
        .value(U256::ZERO)
        .data(Bytes::default())
        .nonce(0)
        .build()
        .unwrap();
    instance
        .transport
        .send_transaction(tx1_1_id, tx1_1_env)
        .await
        .unwrap();

    instance
        .wait_for_processing(Duration::from_millis(20))
        .await;

    // TX1.1 should now be executed
    instance
        .wait_for_transaction_processed(&tx1_1_id)
        .await
        .unwrap();
    assert!(
        instance.get_transaction_result(&tx1_1_id).is_some(),
        "TX1.1 should be processed after TX1 reorg"
    );
    assert!(
        instance.get_transaction_result(&tx1_id).is_none(),
        "TX1.0 should not be processed"
    );
}

#[test]
fn test_reorg_arrives_before_tx_it_cancels_is_not_dropped_future_block() {
    let (mut sequencing, engine_recv) = create_test_sequencing();

    // Start at block 1
    sequencing
        .process_event(create_commit_head(1, 1, 0, None))
        .unwrap();
    assert_eq!(sequencing.current_head, U256::from(1));
    engine_recv.try_iter().for_each(drop);

    // We'll queue block 10, but we'll send reorg(index=1, tx1_old_hash) BEFORE tx1_old arrives.
    let tx0_hash = TxHash::random();
    let tx1_old_hash = TxHash::random();

    let new_iter_10 = create_new_iteration(10, 1);
    let tx0 = create_transaction(10, 1, 0, tx0_hash, None);

    // Reorg that is meant to cancel Tx1_old (index=1, tx_hash=tx1_old_hash)
    let reorg_tx1_old = create_reorg(10, 1, 1, tx1_old_hash);

    // Queue future block events (but DO NOT queue tx1_old yet)
    sequencing.process_event(new_iter_10).unwrap();
    sequencing.process_event(tx0).unwrap();
    sequencing.process_event(reorg_tx1_old).unwrap();

    // Nothing sent yet (still future block)
    assert_eq!(engine_recv.len(), 0);

    // Fill the gap: committing head 9 should start processing block 10
    sequencing
        .process_event(create_commit_head(9, 1, 0, None))
        .unwrap();

    // We expect: CommitHead(9) + NewIteration(10) + Tx0 = 3 events so far
    let mut sent: Vec<TxQueueContents> = engine_recv.try_iter().collect();
    assert_eq!(
        sent.len(),
        3,
        "expected CommitHead(9), NewIteration(10), Tx0 to be sent"
    );

    // Basic ordering checks
    match &sent[0] {
        TxQueueContents::CommitHead(q) => assert_eq!(q.block_number, U256::from(9)),
        other => panic!("expected CommitHead first, got: {other:?}"),
    }
    match &sent[1] {
        TxQueueContents::NewIteration(q) => assert_eq!(q.block_env.number, U256::from(10)),
        other => panic!("expected NewIteration second, got: {other:?}"),
    }
    match &sent[2] {
        TxQueueContents::Tx(q) => {
            assert_eq!(q.tx_execution_id.block_number, U256::from(10));
            assert_eq!(q.tx_execution_id.iteration_id, 1);
            assert_eq!(q.tx_execution_id.index, 0);
            assert_eq!(q.tx_execution_id.tx_hash, tx0_hash);
            assert_eq!(q.prev_tx_hash, None);
        }
        other => panic!("expected Tx0 third, got: {other:?}"),
    }

    // Now Tx1_old arrives AFTER Tx0 already got sent.
    // Correct behavior (if the reorg wasn't dropped): Tx1_old should be cancelled and NOT sent.
    let tx1_old = create_transaction(10, 1, 1, tx1_old_hash, Some(tx0_hash));
    sequencing.process_event(tx1_old).unwrap();

    // If the bug exists, Tx1_old will be sent (because the reorg was dropped when Tx0 fired).
    let leaked: Vec<TxQueueContents> = engine_recv.try_iter().collect();
    assert_eq!(leaked.len(), 0);
}
