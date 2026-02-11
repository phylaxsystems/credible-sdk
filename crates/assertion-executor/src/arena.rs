use std::{
    cell::RefCell,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
};

use bumpalo::Bump;

const DEFAULT_TX_ARENA_CAPACITY_BYTES: usize = 256 * 1024;
const TX_ARENA_ALLOCATION_LIMIT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
struct WorkerTxArena {
    bump: Bump,
    active_tx_epoch: u64,
}

impl WorkerTxArena {
    fn new() -> Self {
        let bump = Bump::with_capacity(DEFAULT_TX_ARENA_CAPACITY_BYTES);
        bump.set_allocation_limit(Some(TX_ARENA_ALLOCATION_LIMIT_BYTES));

        Self {
            bump,
            active_tx_epoch: 0,
        }
    }

    fn prepare_for_tx(&mut self, tx_epoch: u64) {
        if self.active_tx_epoch == tx_epoch {
            return;
        }

        self.bump.reset();
        self.active_tx_epoch = tx_epoch;
    }
}

thread_local! {
    static WORKER_TX_ARENA: RefCell<WorkerTxArena> = RefCell::new(WorkerTxArena::new());
}

static NEXT_TX_ARENA_EPOCH: AtomicU64 = AtomicU64::new(1);

/// Returns a unique epoch for a transaction-level assertion execution pass.
pub(crate) fn next_tx_arena_epoch() -> u64 {
    NEXT_TX_ARENA_EPOCH.fetch_add(1, Ordering::Relaxed)
}

/// Ensures the current worker thread's arena is reset for the given tx epoch.
pub(crate) fn prepare_tx_arena_for_current_thread(tx_epoch: u64) {
    WORKER_TX_ARENA.with(|worker_arena| {
        worker_arena.borrow_mut().prepare_for_tx(tx_epoch);
    });
}

/// Access the current worker thread's bump arena.
pub(crate) fn with_current_tx_arena<R>(f: impl FnOnce(&Bump) -> R) -> R {
    WORKER_TX_ARENA.with(|worker_arena| {
        let worker_arena = worker_arena.borrow();
        f(&worker_arena.bump)
    })
}
