use assertion_executor::{
    db::multi_fork_db::{
        ForkId,
        MultiForkDb,
    },
    inspectors::CallTracer,
    primitives::JournalEntry,
};
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::{
    context::{
        JournalInner,
        journaled_state::JournalCheckpoint,
    },
    database::InMemoryDB,
};

/// Creates a minimal multi-fork setup with empty journals and a tracer that has
/// pre/post-call checkpoints seeded for fork creation.
fn setup_db_and_tracer() -> (
    MultiForkDb<InMemoryDB>,
    JournalInner<JournalEntry>,
    CallTracer,
) {
    let pre_tx_db = InMemoryDB::default();
    let post_tx_journal = JournalInner::new();
    let db = MultiForkDb::new(pre_tx_db, &post_tx_journal);
    let active_journal = post_tx_journal.clone();

    let mut call_tracer = CallTracer::default();
    let checkpoint = JournalCheckpoint {
        log_i: 0,
        journal_i: 0,
    };
    call_tracer.pre_call_checkpoints.push(checkpoint);
    call_tracer.post_call_checkpoints.push(Some(checkpoint));

    (db, active_journal, call_tracer)
}

fn bench_switch_noop_post_tx(c: &mut Criterion) {
    c.bench_function("switch_fork_post_tx_noop_10k", |b| {
        b.iter_batched(
            setup_db_and_tracer,
            |(mut db, mut active_journal, call_tracer)| {
                for _ in 0..10_000 {
                    db.switch_fork(ForkId::PostTx, &mut active_journal, &call_tracer)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_switch_pre_tx_toggle(c: &mut Criterion) {
    c.bench_function("switch_fork_pre_tx_toggle_10k", |b| {
        b.iter_batched(
            setup_db_and_tracer,
            |(mut db, mut active_journal, call_tracer)| {
                for _ in 0..10_000 {
                    db.switch_fork(ForkId::PreTx, &mut active_journal, &call_tracer)
                        .unwrap();
                    db.switch_fork(ForkId::PostTx, &mut active_journal, &call_tracer)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_switch_pre_call_toggle(c: &mut Criterion) {
    c.bench_function("switch_fork_pre_call_toggle_10k", |b| {
        b.iter_batched(
            setup_db_and_tracer,
            |(mut db, mut active_journal, call_tracer)| {
                for _ in 0..10_000 {
                    db.switch_fork(ForkId::PreCall(0), &mut active_journal, &call_tracer)
                        .unwrap();
                    db.switch_fork(ForkId::PostTx, &mut active_journal, &call_tracer)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_switch_post_call_toggle(c: &mut Criterion) {
    c.bench_function("switch_fork_post_call_toggle_10k", |b| {
        b.iter_batched(
            setup_db_and_tracer,
            |(mut db, mut active_journal, call_tracer)| {
                for _ in 0..10_000 {
                    db.switch_fork(ForkId::PostCall(0), &mut active_journal, &call_tracer)
                        .unwrap();
                    db.switch_fork(ForkId::PostTx, &mut active_journal, &call_tracer)
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_switch_noop_post_tx,
    bench_switch_pre_tx_toggle,
    bench_switch_pre_call_toggle,
    bench_switch_post_call_toggle
);
criterion_main!(benches);
