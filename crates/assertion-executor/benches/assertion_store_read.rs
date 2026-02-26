use assertion_executor::{
    inspectors::{
        CallTracer,
        TriggerRecorder,
        TriggerType,
        spec_recorder::AssertionSpec,
    },
    primitives::{
        Address,
        FixedBytes,
        JournalEntry,
        JournalInner,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
};
use criterion::{
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
use std::collections::{
    HashMap,
    HashSet,
};
use tokio::runtime::Runtime;

fn build_traces(adopter: Address, trigger_selector: FixedBytes<4>) -> CallTracer {
    let mut tracer = CallTracer::default();
    let input_bytes = [&trigger_selector[..], &[0u8; 32][..]].concat();
    let call_inputs = CallInputs {
        input: CallInput::Bytes(input_bytes.clone().into()),
        gas_limit: 100_000,
        bytecode_address: adopter,
        known_bytecode: None,
        target_address: adopter,
        caller: adopter,
        is_static: false,
        scheme: CallScheme::Call,
        value: CallValue::default(),
        return_memory_offset: 0..0,
    };
    let mut journal_inner = JournalInner::<JournalEntry>::new();
    tracer.record_call_start(call_inputs, &input_bytes, &mut journal_inner);
    tracer.result.clone().expect("record call start");
    tracer.record_call_end(&mut journal_inner, false);
    tracer.result.clone().expect("record call end");
    tracer
}

fn build_assertion_state(
    trigger_selector: FixedBytes<4>,
    assertion_fn_selector: FixedBytes<4>,
) -> AssertionState {
    let mut triggers: HashMap<_, HashSet<_>> = HashMap::new();
    triggers.insert(
        TriggerType::Call { trigger_selector },
        HashSet::from([assertion_fn_selector]),
    );

    AssertionState {
        activation_block: 0,
        inactivation_block: None,
        assertion_contract: assertion_executor::primitives::AssertionContract::default(),
        trigger_recorder: TriggerRecorder { triggers },
        assertion_spec: AssertionSpec::Legacy,
    }
}

fn bench_assertion_store_read(c: &mut Criterion) {
    let adopter = Address::from([0x11u8; 20]);
    let trigger_selector = FixedBytes::from([0xAAu8, 0xBB, 0xCC, 0xDD]);
    let assertion_fn_selector = FixedBytes::from([0x01u8, 0x02, 0x03, 0x04]);
    let block = U256::from(1u64);

    let mut group = c.benchmark_group("assertion_store::read");

    group.bench_function("hit_existing_assertion", |b| {
        let runtime = Runtime::new().expect("create tokio runtime");
        let _enter_guard = runtime.enter();

        let store = AssertionStore::new_ephemeral();
        store
            .insert(
                adopter,
                build_assertion_state(trigger_selector, assertion_fn_selector),
            )
            .expect("insert assertion");
        let traces = build_traces(adopter, trigger_selector);

        b.iter(|| {
            let assertions = store
                .read(std::hint::black_box(&traces), std::hint::black_box(block))
                .expect("read assertions");
            std::hint::black_box(assertions);
        });
    });

    group.bench_function("miss_nonexistent_assertion", |b| {
        let runtime = Runtime::new().expect("create tokio runtime");
        let _enter_guard = runtime.enter();

        let store = AssertionStore::new_ephemeral();
        let traces = build_traces(adopter, trigger_selector);

        b.iter(|| {
            let assertions = store
                .read(std::hint::black_box(&traces), std::hint::black_box(block))
                .expect("read assertions");
            std::hint::black_box(assertions);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_assertion_store_read);
criterion_main!(benches);
