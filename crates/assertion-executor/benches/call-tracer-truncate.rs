use assertion_executor::{
    inspectors::CallTracer,
    primitives::{
        Address,
        Bytes,
        JournalEntry,
        JournalInner,
        U256,
    },
};
use std::hint::black_box;

use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::interpreter::{
    CallInput,
    CallInputs,
    CallScheme,
    CallValue,
};

const N_TRUNCATE_ENTRIES: usize = 15_000;
const SELECTOR: [u8; 4] = [0x12, 0x34, 0x56, 0x78];

fn make_call_inputs() -> CallInputs {
    CallInputs {
        input: CallInput::Bytes(Bytes::from(SELECTOR.to_vec())),
        return_memory_offset: 0..0,
        gas_limit: 0,
        bytecode_address: Address::ZERO,
        known_bytecode: None,
        target_address: Address::ZERO,
        caller: Address::ZERO,
        value: CallValue::Transfer(U256::ZERO),
        scheme: CallScheme::Call,
        is_static: false,
    }
}

fn build_tracer_with_entries_to_truncate(
    n_entries: usize,
) -> (CallTracer, JournalInner<JournalEntry>) {
    let mut tracer = CallTracer::default();
    let mut journal = JournalInner::new();

    // Mimic a root call (depth 0) that performs many child calls (depth 1),
    // then reverts at the end, causing the tracer to truncate the entire subtree.
    journal.depth = 0;
    tracer.record_call_start(make_call_inputs(), &SELECTOR, &mut journal);
    tracer.result.clone().unwrap();

    let n_children = n_entries.saturating_sub(1);
    for _ in 0..n_children {
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &SELECTOR, &mut journal);
        tracer.result.clone().unwrap();
        tracer.record_call_end(&mut journal, false);
        tracer.result.clone().unwrap();
    }

    journal.depth = 0;

    (tracer, journal)
}

fn build_tracer_with_deep_pending_calls(
    n_entries: usize,
) -> (CallTracer, JournalInner<JournalEntry>) {
    let mut tracer = CallTracer::default();
    let mut journal = JournalInner::new();

    // Worst-case for `truncate_from`: a large number of simultaneously-pending frames
    // (one per depth). Could be created by recursive calls.
    for depth in 0..n_entries {
        journal.depth = depth;
        tracer.record_call_start(make_call_inputs(), &SELECTOR, &mut journal);
        tracer.result.clone().unwrap();
    }

    (tracer, journal)
}

fn call_tracer_truncate_benchmark(c: &mut Criterion) {
    // Replicates a series of successful calls ending in a root-level revert.
    c.bench_function("call_tracer_truncate_15k", |b| {
        b.iter_batched(
            || build_tracer_with_entries_to_truncate(N_TRUNCATE_ENTRIES),
            |(mut tracer, mut journal)| {
                tracer.record_call_end(&mut journal, true);
                tracer.result.clone().unwrap();
                black_box(tracer.is_call_forkable(0));
            },
            BatchSize::LargeInput,
        );
    });

    // Replicates a series of recursive calls where the last call reverts and then bubbles up.
    c.bench_function("call_tracer_truncate_15k_deep_pending", |b| {
        b.iter_batched(
            || build_tracer_with_deep_pending_calls(N_TRUNCATE_ENTRIES),
            |(mut tracer, mut journal)| {
                loop {
                    tracer.record_call_end(&mut journal, true);
                    tracer.result.clone().unwrap();
                    black_box(tracer.is_call_forkable(0));
                    if journal.depth == 0 {
                        break;
                    }
                    journal.depth -= 1;
                }
            },
            BatchSize::LargeInput,
        );
    });

    // Replicates a series of successful calls ending in a root-level revert.
    c.bench_function("call_tracer_truncate_500", |b| {
        b.iter_batched(
            || build_tracer_with_entries_to_truncate(500),
            |(mut tracer, mut journal)| {
                tracer.record_call_end(&mut journal, true);
                tracer.result.clone().unwrap();
                black_box(tracer.is_call_forkable(0));
            },
            BatchSize::LargeInput,
        );
    });

    // Replicates a series of recursive calls where the last call reverts and then bubbles up.
    c.bench_function("call_tracer_truncate_500_deep_pending", |b| {
        b.iter_batched(
            || build_tracer_with_deep_pending_calls(500),
            |(mut tracer, mut journal)| {
                loop {
                    tracer.record_call_end(&mut journal, true);
                    tracer.result.clone().unwrap();
                    black_box(tracer.is_call_forkable(0));
                    if journal.depth == 0 {
                        break;
                    }
                    journal.depth -= 1;
                }
            },
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(benches, call_tracer_truncate_benchmark);
criterion_main!(benches);
