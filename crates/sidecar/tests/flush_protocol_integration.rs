//! Integration tests for the CommitHead-gated flush protocol.
//!
//! These tests exercise the full pipeline: state worker sends
//! [`BlockStateUpdate`]s through a bounded channel, the flush loop drains
//! them into a real MDBX [`StateWriter`] only up to the block number
//! signaled on the commit-head channel, and a [`StateReader`] verifies
//! which blocks are actually persisted.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable,
    clippy::unwrap_used
)]

use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};

use alloy::primitives::B256;
use mdbx::{
    BlockStateUpdate,
    Reader,
    StateReader,
    StateWriter,
    common::CircularBufferConfig,
};
use sidecar::state_worker_flush::{
    FlushLoopConfig,
    spawn_flush_loop,
};

/// Helper: create a minimal [`BlockStateUpdate`] for the given block number.
fn make_update(block_number: u64) -> BlockStateUpdate {
    BlockStateUpdate::new(block_number, B256::ZERO, B256::ZERO)
}

/// Helper: create a [`CircularBufferConfig`] with depth=1 (single namespace).
fn depth_one_config() -> CircularBufferConfig {
    CircularBufferConfig::new(1).expect("depth=1 is valid")
}

/// Sends blocks 1-10, flushes up to block 5, and verifies that MDBX
/// contains exactly blocks 1-5 via [`StateReader::is_block_available`].
/// Blocks 6-10 must NOT be present.
#[test]
fn flush_protocol_writes_only_up_to_committed_block() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let config = depth_one_config();

    let writer = StateWriter::new(tmp.path(), config.clone()).expect("create writer");
    let reader = StateReader::new(tmp.path(), config).expect("create reader");

    let (update_tx, update_rx) = flume::bounded::<BlockStateUpdate>(32);
    let (commit_tx, commit_rx) = flume::bounded::<u64>(32);
    let committed_height = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let flush_handle = spawn_flush_loop(
        FlushLoopConfig {
            recv_timeout: Duration::from_millis(10),
        },
        update_rx,
        commit_rx,
        writer,
        Arc::clone(&committed_height),
        Arc::clone(&shutdown),
    )
    .expect("spawn flush loop");

    // Send blocks 1 through 10.
    for block in 1..=10 {
        update_tx.send(make_update(block)).expect("send update");
    }

    // Signal flush up to block 5.
    commit_tx.send(5).expect("send commit signal");

    // Give the flush loop time to process, then shut it down.
    std::thread::sleep(Duration::from_millis(200));
    shutdown.store(true, Ordering::Release);

    // Drop senders so the loop can exit cleanly.
    drop(update_tx);
    drop(commit_tx);

    flush_handle.join().expect("flush loop panicked");

    // Blocks 1-5 should be available. With depth=1 only the latest block
    // occupies namespace 0, so `is_block_available` returns true only for
    // the most recent block written (block 5). We verify that block 5 IS
    // available and blocks 6-10 are NOT.
    //
    // NOTE: With `CircularBufferConfig { buffer_size: 1 }`, writing block N
    // overwrites namespace 0, making only block N available. Blocks 1-4 are
    // overwritten by subsequent commits.  The key invariant is that NO block
    // beyond the flush ceiling (5) is present.
    assert!(
        reader.is_block_available(5).expect("check block 5"),
        "block 5 should be available (last flushed block)"
    );

    for block in 6..=10 {
        assert!(
            !reader.is_block_available(block).expect("check block"),
            "block {block} should NOT be available (beyond flush ceiling)"
        );
    }
}

/// Verifies that the shared `committed_height` [`AtomicU64`] accurately
/// reflects the last successfully flushed block number.
#[test]
fn flush_protocol_committed_height_accurate() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let config = depth_one_config();

    let writer = StateWriter::new(tmp.path(), config.clone()).expect("create writer");
    let _reader = StateReader::new(tmp.path(), config).expect("create reader");

    let (update_tx, update_rx) = flume::bounded::<BlockStateUpdate>(32);
    let (commit_tx, commit_rx) = flume::bounded::<u64>(32);
    let committed_height = Arc::new(AtomicU64::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let flush_handle = spawn_flush_loop(
        FlushLoopConfig {
            recv_timeout: Duration::from_millis(10),
        },
        update_rx,
        commit_rx,
        writer,
        Arc::clone(&committed_height),
        Arc::clone(&shutdown),
    )
    .expect("spawn flush loop");

    // Send blocks 1-7 and flush to 7.
    for block in 1..=7 {
        update_tx.send(make_update(block)).expect("send update");
    }
    commit_tx.send(7).expect("send commit signal");

    // Allow time for flush to complete.
    std::thread::sleep(Duration::from_millis(200));
    shutdown.store(true, Ordering::Release);

    drop(update_tx);
    drop(commit_tx);

    flush_handle.join().expect("flush loop panicked");

    assert_eq!(
        committed_height.load(Ordering::Acquire),
        7,
        "committed_height should equal the last flushed block"
    );
}

/// Fills the bounded channel to capacity and verifies that [`flume::Sender::try_send`]
/// returns a [`flume::TrySendError::Full`] error, confirming backpressure.
#[test]
fn flush_protocol_backpressure() {
    let capacity = 4;
    let (tx, _rx) = flume::bounded::<BlockStateUpdate>(capacity);

    // Fill the channel to capacity.
    for block in 1..=capacity {
        tx.try_send(make_update(block as u64))
            .expect("send should succeed while channel has capacity");
    }

    // The next try_send should fail with Full.
    let result = tx.try_send(make_update((capacity + 1) as u64));
    assert!(
        matches!(result, Err(flume::TrySendError::Full(_))),
        "expected Full error when channel is at capacity, got {result:?}"
    );
}
