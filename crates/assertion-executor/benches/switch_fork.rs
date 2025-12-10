use alloy_primitives::map::{
    DefaultHashBuilder,
    HashMap,
};
use assertion_executor::{
    constants::DEFAULT_PERSISTENT_ACCOUNTS,
    db::multi_fork_db::{
        ForkId,
        MultiForkDb,
    },
    inspectors::CallTracer,
    primitives::{
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        EvmStorageSlot,
        JournalEntry,
        U256,
    },
};
use criterion::{
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
use std::time::{
    Duration,
    Instant,
};

fn seeded_state() -> (InMemoryDB, JournalInner<JournalEntry>) {
    let mut pre_tx_db = InMemoryDB::default();
    let mut post_tx_journal = JournalInner::new();

    // Seed more accounts and storage into both the pre-tx DB and the post-tx journal
    for i in 0..80000 {
        let mut addr_bytes = [0u8; 20];
        addr_bytes[18] = (i >> 8) as u8;
        addr_bytes[19] = i as u8;
        let addr = Address::from(addr_bytes);

        pre_tx_db.insert_account_info(
            addr,
            AccountInfo {
                balance: U256::from(10_000 + i as u64),
                ..Default::default()
            },
        );

        let mut storage = HashMap::with_hasher(DefaultHashBuilder::default());
        storage.insert(U256::from(1), EvmStorageSlot::new(U256::from(i as u64), 0));
        storage.insert(
            U256::from(2),
            EvmStorageSlot::new(U256::from(i as u64 + 1), 0),
        );
        storage.insert(
            U256::from(3),
            EvmStorageSlot::new(U256::from(i as u64 + 2), 0),
        );

        post_tx_journal.state.insert(
            addr,
            Account {
                info: AccountInfo {
                    balance: U256::from(20_000 + i as u64),
                    ..Default::default()
                },
                transaction_id: 0,
                storage,
                status: AccountStatus::Touched,
            },
        );
    }

    // Also include the persistent accounts in the journal so update_journal does work
    for (idx, addr) in DEFAULT_PERSISTENT_ACCOUNTS.iter().enumerate() {
        post_tx_journal.state.insert(
            *addr,
            Account {
                info: AccountInfo {
                    balance: U256::from(10_000 + idx as u64),
                    ..Default::default()
                },
                transaction_id: 0,
                storage: HashMap::default(),
                status: AccountStatus::Touched,
            },
        );
    }

    (pre_tx_db, post_tx_journal)
}

fn seeded_tracer() -> CallTracer {
    let mut call_tracer = CallTracer::default();
    let checkpoint = JournalCheckpoint {
        log_i: 0,
        journal_i: 0,
    };
    call_tracer.pre_call_checkpoints.push(checkpoint);
    call_tracer.post_call_checkpoints.push(Some(checkpoint));
    call_tracer
}

/// Prepare a DB with pre-created forks and a journal carrying state; return templates for cloning.
fn prepare_templates() -> (
    MultiForkDb<InMemoryDB>,
    JournalInner<JournalEntry>,
    CallTracer,
) {
    let (pre_tx_db, post_tx_journal) = seeded_state();
    let db = MultiForkDb::new(pre_tx_db, &post_tx_journal);
    let active_journal = post_tx_journal.clone();
    let tracer = seeded_tracer();

    (db, active_journal, tracer)
}

fn time_switches(
    c: &mut Criterion,
    name: &str,
    mut warmup: impl FnMut(&mut MultiForkDb<InMemoryDB>, &mut JournalInner<JournalEntry>, &CallTracer),
    mut body: impl FnMut(&mut MultiForkDb<InMemoryDB>, &mut JournalInner<JournalEntry>, &CallTracer),
) {
    c.bench_function(name, |b| {
        let (db_template, journal_template, tracer_template) = prepare_templates();
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let mut db = db_template.clone();
                let mut active_journal = journal_template.clone();
                let tracer = tracer_template.clone();

                warmup(&mut db, &mut active_journal, &tracer);

                let start = Instant::now();
                body(&mut db, &mut active_journal, &tracer);
                total += start.elapsed();
            }
            total
        });
    });
}

fn bench_switch_noop_post_tx(c: &mut Criterion) {
    time_switches(
        c,
        "switch_fork_post_tx_noop_10k",
        |_, _, _| {},
        |db, journal, tracer| {
            for _ in 0..10_000 {
                db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
            }
        },
    );
}

fn bench_switch_pre_tx_toggle(c: &mut Criterion) {
    time_switches(
        c,
        "switch_fork_pre_tx_toggle_10k",
        |_, _, _| {},
        |db, journal, tracer| {
            for _ in 0..10_000 {
                db.switch_fork(ForkId::PreTx, journal, tracer).unwrap();
                db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
            }
        },
    );
}

fn bench_switch_pre_tx_toggle_warm(c: &mut Criterion) {
    time_switches(
        c,
        "switch_fork_pre_tx_toggle_warm_10k",
        |db, journal, tracer| {
            // ensure fork exists and journals are aligned before measurement
            db.switch_fork(ForkId::PreTx, journal, tracer).unwrap();
            db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
        },
        |db, journal, tracer| {
            for _ in 0..10_000 {
                db.switch_fork(ForkId::PreTx, journal, tracer).unwrap();
                db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
            }
        },
    );
}

fn bench_switch_pre_call_toggle(c: &mut Criterion) {
    time_switches(
        c,
        "switch_fork_pre_call_toggle_10k",
        |_, _, _| {},
        |db, journal, tracer| {
            for _ in 0..10_000 {
                db.switch_fork(ForkId::PreCall(0), journal, tracer).unwrap();
                db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
            }
        },
    );
}

fn bench_switch_post_call_toggle(c: &mut Criterion) {
    time_switches(
        c,
        "switch_fork_post_call_toggle_10k",
        |_, _, _| {},
        |db, journal, tracer| {
            for _ in 0..10_000 {
                db.switch_fork(ForkId::PostCall(0), journal, tracer)
                    .unwrap();
                db.switch_fork(ForkId::PostTx, journal, tracer).unwrap();
            }
        },
    );
}

criterion_group!(
    benches,
    bench_switch_noop_post_tx,
    bench_switch_pre_tx_toggle,
    bench_switch_pre_tx_toggle_warm,
    bench_switch_pre_call_toggle,
    bench_switch_post_call_toggle
);
criterion_main!(benches);
