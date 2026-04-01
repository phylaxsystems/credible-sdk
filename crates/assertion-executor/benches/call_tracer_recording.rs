use assertion_executor::{
    ExecutorConfig,
    inspectors::CallTracer,
    primitives::{
        Address,
        Bytes,
        JournalEntry,
        JournalInner,
        U256,
        address,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::bytecode,
};
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
use std::hint::black_box;

const CALL_COUNT: usize = 1_024;
const REVERT_DEPTH: usize = 512;
const SELECTOR: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
const AVERAGE_ASSERTION_ARTIFACT: &str = "AverageAssertion.sol:AverageAssertion";

fn average_assertion_state() -> AssertionState {
    let bytecode = bytecode(AVERAGE_ASSERTION_ARTIFACT);
    AssertionState::new_active(&bytecode, &ExecutorConfig::default())
        .expect("create AverageAssertion state")
}

fn build_store(adopters: &[Address]) -> AssertionStore {
    let store = AssertionStore::new_ephemeral();
    let assertion_state = average_assertion_state();
    for adopter in adopters {
        store
            .insert(*adopter, assertion_state.clone())
            .expect("register adopter");
    }
    store
}

fn make_call_inputs(target: Address) -> CallInputs {
    CallInputs {
        input: CallInput::Bytes(Bytes::from(SELECTOR.to_vec())),
        return_memory_offset: 0..0,
        gas_limit: 0,
        bytecode_address: target,
        known_bytecode: None,
        target_address: target,
        caller: address!("000000000000000000000000000000000000c0de"),
        value: CallValue::Transfer(U256::ZERO),
        scheme: CallScheme::Call,
        is_static: false,
    }
}

fn repeated_targets(target: Address) -> Vec<Address> {
    vec![target; CALL_COUNT]
}

fn unique_targets() -> Vec<Address> {
    (0..CALL_COUNT)
        .map(|idx| {
            let mut bytes = [0u8; 20];
            bytes[16..20].copy_from_slice(
                &u32::try_from(idx)
                    .expect("bench call count fits in u32")
                    .to_be_bytes(),
            );
            Address::from(bytes)
        })
        .collect()
}

fn record_calls(store: AssertionStore, targets: Vec<Address>) {
    let mut tracer = CallTracer::new(store);
    let mut journal = JournalInner::<JournalEntry>::new();

    for target in targets {
        tracer.record_call_start(make_call_inputs(target), &SELECTOR, &mut journal);
        tracer.result.clone().expect("record call start");
        tracer.record_call_end(&mut journal, false);
        tracer.result.clone().expect("record call end");
    }

    black_box(tracer.call_records().len());
}

fn build_shallow_revert_tracer(depth: usize) -> (CallTracer, JournalInner<JournalEntry>) {
    let mut tracer = CallTracer::default();
    let mut journal = JournalInner::new();

    journal.depth = 0;
    tracer.record_call_start(make_call_inputs(Address::ZERO), &SELECTOR, &mut journal);
    tracer.result.clone().expect("record root");

    for _ in 0..depth.saturating_sub(1) {
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(Address::ZERO), &SELECTOR, &mut journal);
        tracer.result.clone().expect("record child");
        tracer.record_call_end(&mut journal, false);
        tracer.result.clone().expect("record child end");
    }

    journal.depth = 0;
    (tracer, journal)
}

fn build_deep_revert_tracer(depth: usize) -> (CallTracer, JournalInner<JournalEntry>) {
    let mut tracer = CallTracer::default();
    let mut journal = JournalInner::new();

    for current_depth in 0..depth {
        journal.depth = current_depth;
        tracer.record_call_start(make_call_inputs(Address::ZERO), &SELECTOR, &mut journal);
        tracer.result.clone().expect("record recursive frame");
    }

    (tracer, journal)
}

fn call_tracer_recording_benchmark(c: &mut Criterion) {
    let repeated_target = address!("00000000000000000000000000000000000000aa");
    let repeated = repeated_targets(repeated_target);
    let unique = unique_targets();

    // Repeated vs unique targets exercises the cache-friendly adopter path against the
    // worst case where every lookup misses any per-address reuse in the tracer.
    let mut group = c.benchmark_group("call_tracer_recording");

    group.bench_function("repeated_non_aa", |b| {
        b.iter_batched(
            || (build_store(&[]), repeated.clone()),
            |(store, targets)| record_calls(store, targets),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("repeated_aa", |b| {
        b.iter_batched(
            || (build_store(&[repeated_target]), repeated.clone()),
            |(store, targets)| record_calls(store, targets),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("unique_non_aa", |b| {
        b.iter_batched(
            || (build_store(&[]), unique.clone()),
            |(store, targets)| record_calls(store, targets),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("unique_aa", |b| {
        b.iter_batched(
            || (build_store(&unique), unique.clone()),
            |(store, targets)| record_calls(store, targets),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn call_tracer_revert_shape_benchmark(c: &mut Criterion) {
    // Revert truncation cost depends heavily on whether the revert unwinds one wide
    // frame or a deep recursive stack, so benchmark both shapes explicitly.
    let mut group = c.benchmark_group("call_tracer_revert_shapes");

    group.bench_function("shallow_revert_512", |b| {
        b.iter_batched(
            || build_shallow_revert_tracer(REVERT_DEPTH),
            |(mut tracer, mut journal)| {
                tracer.record_call_end(&mut journal, true);
                tracer.result.clone().expect("record shallow revert");
                black_box(tracer.is_call_forkable(0));
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("deep_recursive_revert_512", |b| {
        b.iter_batched(
            || build_deep_revert_tracer(REVERT_DEPTH),
            |(mut tracer, mut journal)| {
                loop {
                    tracer.record_call_end(&mut journal, true);
                    tracer.result.clone().expect("record deep revert");
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

    group.finish();
}

criterion_group!(
    benches,
    call_tracer_recording_benchmark,
    call_tracer_revert_shape_benchmark
);
criterion_main!(benches);
