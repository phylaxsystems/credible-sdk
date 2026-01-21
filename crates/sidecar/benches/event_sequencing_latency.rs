//! Targeted benchmark for `EventSequencing` end-to-end latency.
//!
//! Measures wall-clock time from enqueueing events into the sequencing input channel
//! to receiving the corresponding output events on the engine channel.

use alloy::primitives::{
    B256,
    TxHash,
    U256,
};
use criterion::{
    BatchSize,
    Criterion,
};
use revm::context::{
    BlockEnv,
    TxEnv,
};
use sidecar::{
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        ReorgRequest,
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    event_sequencing::{
        EventSequencing,
        EventSequencingError,
    },
    execution_ids::TxExecutionId,
};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Duration,
};

const CHAIN_LEN: usize = 64;
const TREE_BRANCHES: usize = 16;
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

fn tx_hash_from_u64(seed: u64) -> TxHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_be_bytes());
    TxHash::from(bytes)
}

fn make_seed(block: u64, iteration: u64, index: u64) -> u64 {
    (block << 32) ^ (iteration << 16) ^ index
}

fn create_new_iteration(block: u64, iteration: u64) -> TxQueueContents {
    let block_env = BlockEnv {
        number: U256::from(block),
        ..Default::default()
    };
    TxQueueContents::NewIteration(
        NewIteration::new(iteration, block_env),
        tracing::Span::none(),
    )
}

fn create_transaction(
    block: u64,
    iteration: u64,
    index: u64,
    tx_hash: TxHash,
    prev_tx_hash: Option<TxHash>,
) -> TxQueueContents {
    let tx_execution_id = TxExecutionId::new(U256::from(block), iteration, tx_hash, index);
    TxQueueContents::Tx(
        QueueTransaction {
            tx_execution_id,
            tx_env: TxEnv::default(),
            prev_tx_hash,
        },
        tracing::Span::none(),
    )
}

fn create_reorg(block: u64, iteration: u64, index: u64, tx_hash: TxHash) -> TxQueueContents {
    let tx_execution_id = TxExecutionId::new(U256::from(block), iteration, tx_hash, index);
    TxQueueContents::Reorg(
        ReorgRequest {
            tx_execution_id,
            tx_hashes: vec![tx_hash],
        },
        tracing::Span::none(),
    )
}

fn create_commit_head(
    block: u64,
    iteration: u64,
    n_txs: u64,
    last_tx_hash: Option<TxHash>,
) -> TxQueueContents {
    TxQueueContents::CommitHead(
        CommitHead::new(
            U256::from(block),
            iteration,
            last_tx_hash,
            n_txs,
            B256::ZERO,
            None,
            U256::ZERO,
        ),
        tracing::Span::none(),
    )
}

struct Scenario {
    to_send: Vec<TxQueueContents>,
    expected_output_events: usize,
}

struct SequencerHarness {
    input: TransactionQueueSender,
    output: TransactionQueueReceiver,
    shutdown: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Result<(), EventSequencingError>>>,
}

impl SequencerHarness {
    fn new() -> Self {
        let (input, input_recv) = flume::unbounded::<TxQueueContents>();
        let (engine_send, output) = flume::unbounded::<TxQueueContents>();

        let sequencing = EventSequencing::new(input_recv, engine_send);
        let shutdown = Arc::new(AtomicBool::new(false));

        let (handle, _rx) = sequencing
            .spawn(Arc::clone(&shutdown))
            .expect("failed to spawn EventSequencing thread");

        let harness = Self {
            input,
            output,
            shutdown,
            handle: Some(handle),
        };

        // Warmup: set `first_commit_head_received=true` and establish `current_head=0`.
        harness
            .send(create_commit_head(0, 0, 0, None))
            .expect("failed to send warmup CommitHead");
        harness.recv_exact(1);

        harness
    }

    fn send(&self, event: TxQueueContents) -> Result<(), Box<flume::SendError<TxQueueContents>>> {
        self.input.send(event).map_err(Box::new)
    }

    fn recv_exact(&self, n: usize) {
        for _ in 0..n {
            self.output
                .recv_timeout(RECV_TIMEOUT)
                .expect("timed out waiting for sequencer output");
        }
    }
}

impl Drop for SequencerHarness {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.send(create_commit_head(0, 0, 0, None));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn scenario_no_dependencies(block: u64) -> Scenario {
    let tx0_hash = tx_hash_from_u64(make_seed(block, 0, 0));

    Scenario {
        to_send: vec![
            create_new_iteration(block, 0),
            create_transaction(block, 0, 0, tx0_hash, None),
            create_commit_head(block, 0, 1, Some(tx0_hash)),
        ],
        expected_output_events: 3,
    }
}

fn scenario_with_dependencies(block: u64) -> Scenario {
    let tx0_hash = tx_hash_from_u64(make_seed(block, 0, 0));
    let tx1_hash = tx_hash_from_u64(make_seed(block, 0, 1));

    // TX1 arrives before TX0, so it must be queued until its dependency arrives.
    Scenario {
        to_send: vec![
            create_new_iteration(block, 0),
            create_transaction(block, 0, 1, tx1_hash, Some(tx0_hash)),
            create_transaction(block, 0, 0, tx0_hash, None),
            create_commit_head(block, 0, 2, Some(tx1_hash)),
        ],
        expected_output_events: 4,
    }
}

fn scenario_dependency_chain(block: u64) -> Scenario {
    let hashes: [TxHash; CHAIN_LEN] =
        std::array::from_fn(|i| tx_hash_from_u64(make_seed(block, 0, i as u64)));

    let mut to_send = Vec::with_capacity(CHAIN_LEN + 2);

    // Send the chain in reverse so every tx is queued in the dependency graph.
    for i in (0..CHAIN_LEN).rev() {
        let prev = if i == 0 { None } else { Some(hashes[i - 1]) };
        to_send.push(create_transaction(block, 0, i as u64, hashes[i], prev));
    }

    to_send.push(create_new_iteration(block, 0));
    to_send.push(create_commit_head(
        block,
        0,
        CHAIN_LEN as u64,
        Some(hashes[CHAIN_LEN - 1]),
    ));

    Scenario {
        to_send,
        expected_output_events: CHAIN_LEN + 2,
    }
}

fn scenario_reorg(block: u64) -> Scenario {
    let tx0_hash = tx_hash_from_u64(make_seed(block, 0, 0));
    let tx1_hash = tx_hash_from_u64(make_seed(block, 0, 1));

    Scenario {
        to_send: vec![
            create_new_iteration(block, 0),
            create_transaction(block, 0, 0, tx0_hash, None),
            create_transaction(block, 0, 1, tx1_hash, Some(tx0_hash)),
            create_reorg(block, 0, 1, tx1_hash),
            // After the reorg, TX0 is the last included tx.
            create_commit_head(block, 0, 1, Some(tx0_hash)),
        ],
        expected_output_events: 5,
    }
}

fn scenario_dependency_tree(base_head: u64) -> Scenario {
    let block_1 = base_head + 1;
    let block_2 = base_head + 2;

    let hashes: [TxHash; TREE_BRANCHES] =
        std::array::from_fn(|i| tx_hash_from_u64(make_seed(block_2, i as u64, 0)));

    // Future block events first (queued under CommitHead(block_1)).
    let mut to_send = Vec::with_capacity((TREE_BRANCHES * 2) + 3);
    for iteration in 0..(TREE_BRANCHES as u64) {
        let tx0_hash = hashes[usize::try_from(iteration).expect("iteration is within bounds")];
        // TX arrives before NewIteration, and both arrive before their parent CommitHead.
        to_send.push(create_transaction(block_2, iteration, 0, tx0_hash, None));
        to_send.push(create_new_iteration(block_2, iteration));
    }

    // Commit an empty block_1 to trigger sending queued NewIteration roots for block_2.
    to_send.push(create_new_iteration(block_1, 0));
    to_send.push(create_commit_head(block_1, 0, 0, None));

    // Commit block_2, selecting iteration 0.
    to_send.push(create_commit_head(block_2, 0, 1, Some(hashes[0])));

    Scenario {
        to_send,
        expected_output_events: (TREE_BRANCHES * 2) + 3,
    }
}

fn bench_event_sequencing_latency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("event_sequencing/latency");

    // Scenario 1: no dependencies (linear happy path).
    group.bench_function("no_dependencies", |b| {
        let harness = SequencerHarness::new();
        let mut block = 1u64;
        b.iter_batched(
            || {
                let scenario = scenario_no_dependencies(block);
                block += 1;
                scenario
            },
            |scenario| {
                for event in scenario.to_send {
                    harness.send(event).unwrap();
                }
                harness.recv_exact(scenario.expected_output_events);
            },
            BatchSize::SmallInput,
        );
    });

    // Scenario 2: a single TX arrives before its dependency (NewIteration).
    group.bench_function("with_dependencies", |b| {
        let harness = SequencerHarness::new();
        let mut block = 1u64;
        b.iter_batched(
            || {
                let scenario = scenario_with_dependencies(block);
                block += 1;
                scenario
            },
            |scenario| {
                for event in scenario.to_send {
                    harness.send(event).unwrap();
                }
                harness.recv_exact(scenario.expected_output_events);
            },
            BatchSize::SmallInput,
        );
    });

    // Scenario 3: chain of dependencies (reverse-send `CHAIN_LEN` txs).
    group.bench_function("dependency_chain", |b| {
        let harness = SequencerHarness::new();
        let mut block = 1u64;
        b.iter_batched(
            || {
                let scenario = scenario_dependency_chain(block);
                block += 1;
                scenario
            },
            |scenario| {
                for event in scenario.to_send {
                    harness.send(event).unwrap();
                }
                harness.recv_exact(scenario.expected_output_events);
            },
            BatchSize::SmallInput,
        );
    });

    // Scenario 4: reorg cancels the last TX in an iteration.
    group.bench_function("reorg", |b| {
        let harness = SequencerHarness::new();
        let mut block = 1u64;
        b.iter_batched(
            || {
                let scenario = scenario_reorg(block);
                block += 1;
                scenario
            },
            |scenario| {
                for event in scenario.to_send {
                    harness.send(event).unwrap();
                }
                harness.recv_exact(scenario.expected_output_events);
            },
            BatchSize::SmallInput,
        );
    });

    // Scenario 5: dependency tree (many NewIteration roots gated by a CommitHead).
    group.bench_function("dependency_tree", |b| {
        let harness = SequencerHarness::new();
        let mut base_head = 0u64;
        b.iter_batched(
            || {
                let scenario = scenario_dependency_tree(base_head);
                base_head += 2;
                scenario
            },
            |scenario| {
                for event in scenario.to_send {
                    harness.send(event).unwrap();
                }
                harness.recv_exact(scenario.expected_output_events);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn main() {
    let mut criterion = Criterion::default();
    bench_event_sequencing_latency(&mut criterion);
    criterion.final_summary();
}
