use Asm::*;
use assertion_executor::{
    db::{
        MultiForkDb,
        overlay::{
            OverlayDb,
            test_utils::MockDb,
        },
    },
    evm::build_evm::{
        build_optimism_evm,
        evm_env,
    },
    inspectors::{
        CallTracer,
        LogsAndTraces,
        PhEvmContext,
        PhEvmInspector,
    },
    primitives::{
        AccountInfo,
        Address,
        BlockEnv,
        Bytecode,
        EvmExecutionResult,
        HaltReason,
        JournalInner,
        SpecId,
        TxEnv,
        TxKind,
        U256,
        hex as hx,
        keccak256,
    },
    reprice_evm_storage,
};
use criterion::{
    BenchmarkGroup,
    Criterion,
    criterion_group,
    criterion_main,
    measurement::Measurement,
};
use evm_glue::{
    assembler::assemble_minimized,
    assembly::Asm,
    opcodes::Opcode::*,
};
use op_revm::OpTransaction;
use revm::ExecuteEvm;

fn register_op<M: Measurement>(
    group: &mut BenchmarkGroup<M>,
    op: Vec<Asm>,
    label: &str,
    test_op: bool,
) {
    let runtime_bytecode = assemble_inf_loop(op);

    unsafe { std::env::set_var("FOUNDRY_DISABLE_NIGHTLY_WARNING", "1") };

    // Execute the future, blocking the current thread until completion
    let db = OverlayDb::<MockDb>::new_test();

    // Insert runtime bytecode into the database
    let mut fork = db.fork();
    let addr = Address::random();

    fork.insert_account_info(
        addr,
        AccountInfo {
            nonce: 1,
            balance: U256::MAX,
            code_hash: keccak256(&runtime_bytecode),
            code: Some(Bytecode::new_legacy(runtime_bytecode.into())),
        },
    );

    let tx_env = OpTransaction::new(TxEnv {
        kind: TxKind::Call(addr),
        gas_limit: 500_000,
        ..Default::default()
    });

    let call_tracer = CallTracer::default();
    let logs_and_traces = LogsAndTraces {
        tx_logs: &[],
        call_traces: &call_tracer,
    };
    let phevm_context = PhEvmContext::new(&logs_and_traces, Address::ZERO);
    let inspector = PhEvmInspector::new(phevm_context);
    let env = evm_env(1, SpecId::default(), BlockEnv::default());
    let mut multi_fork_db = MultiForkDb::new(fork, &JournalInner::new());
    let mut evm = build_optimism_evm(&mut multi_fork_db, &env, inspector);
    reprice_evm_storage!(evm);

    // FIXME: This is a hacky abstraction to support testing the operation and benching it. An
    // abstraction that returns the evm has problems with lifetimes.
    if test_op {
        let result = evm.transact(tx_env).unwrap();
        match result.result {
            EvmExecutionResult::Halt { reason, .. } => {
                if let HaltReason::OutOfGas(..) = reason {
                    // Expected
                } else {
                    panic!("{label}: Unexpected halt reason: {reason:#?}");
                }
            }
            _ => {
                panic!("{label}: Unexpected result: {result:#?}");
            }
        }
    } else {
        group.bench_function(label, |b| b.iter(|| evm.transact(tx_env.clone()).unwrap()));
    }
}

// TODO: POP can be removed from operations ending in pop, if we populate the stack before the loop.
fn operations() -> Vec<(Vec<Asm>, &'static str)> {
    let mut operations = vec![];

    let tload_op = vec![Op(PUSH0), Op(TLOAD), Op(POP)];
    operations.push((tload_op, "TLOAD"));

    let tstore_op = vec![Op(PUSH0), Op(PUSH0), Op(TSTORE)];
    operations.push((tstore_op, "TSTORE"));

    let sload_op = vec![Op(PUSH0), Op(SLOAD), Op(POP)];
    operations.push((sload_op, "SLOAD"));

    let sstore_op = vec![Op(PUSH0), Op(PUSH0), Op(SSTORE)];
    operations.push((sstore_op, "SSTORE"));

    let keccak_op = vec![Op(PUSH1(hx!("20"))), Op(PUSH0), Op(SHA3), Op(POP)];
    operations.push((keccak_op, "KECCAK"));

    let add_op = vec![Op(PUSH0), Op(PUSH0), Op(ADD), Op(POP)];
    operations.push((add_op, "ADD"));

    let log0_op = vec![Op(PUSH0), Op(PUSH0), Op(LOG0)];
    operations.push((log0_op, "LOG0"));

    operations.push((ecrecover_call(), "ECRECOVER"));

    operations
}
fn ecrecover_call() -> Vec<Asm> {
    vec![
        // Store hash in memory
        Op(PUSH32(hx!(
            "456e9aea5e197a1f1af7a3e85a3212fa4049a3ba34c2289b4c860fc0b0c64ef3"
        ))), // hash
        Op(PUSH1([0])), // offset
        Op(MSTORE),
        // Store V in memory
        Op(PUSH1([28])), // V
        Op(PUSH1([32])), // offset
        Op(MSTORE),
        // Store R in Memory
        Op(PUSH32(hx!(
            "9242685bf161793cc25603c231bc2f568eb630ea16aa137d2664ac8038825608"
        ))), // R
        Op(PUSH1([32])), // offset
        Op(MSTORE),
        //
        Op(PUSH32(hx!(
            "4f8ae3bd7535248d0bd448298cc2e2071e56992d0774dc340c368ae950852ada"
        ))), // S
        Op(PUSH1([96])), // offset
        Op(MSTORE),
        // Call ecrecover
        Op(PUSH1([32])),                                                // output size
        Op(PUSH0),                                                      // output offset
        Op(PUSH1([128])),                                               // input size
        Op(PUSH0),                                                      // input offset
        Op(PUSH1([1])),                                                 // address
        Op(PUSH4(hx::decode("FFFFFFFF").unwrap().try_into().unwrap())), // gas
        Op(STATICCALL),
        Op(POP), // Ignore success flag
    ]
}

fn test_ecrecover() {
    let mut op = ecrecover_call();

    // Read the return data from memory
    op.push(Op(PUSH1([32])));
    op.push(Op(MLOAD));

    // Store the return data in storage
    op.push(Op(PUSH0));
    op.push(Op(SSTORE));

    let (_, runtime_bytecode) = assemble_minimized(&op, true).unwrap();

    unsafe {
        std::env::set_var("FOUNDRY_DISABLE_NIGHTLY_WARNING", "1");
    }

    // Execute the future, blocking the current thread until completion
    let db = OverlayDb::<MockDb>::new_test();

    // Insert runtime bytecode into the database
    let mut fork = db.fork();
    let addr = Address::random();
    fork.insert_account_info(
        addr,
        AccountInfo {
            nonce: 1,
            balance: U256::MAX,
            code_hash: keccak256(&runtime_bytecode),
            code: Some(Bytecode::new_legacy(runtime_bytecode.into())),
        },
    );

    let tx_env = OpTransaction::new(TxEnv {
        kind: TxKind::Call(addr),
        gas_limit: 500_000,
        ..Default::default()
    });

    let mut multi_fork_db = MultiForkDb::new(fork, &JournalInner::new());
    let call_tracer = CallTracer::default();
    let logs_and_traces = LogsAndTraces {
        tx_logs: &[],
        call_traces: &call_tracer,
    };
    let phevm_context = PhEvmContext::new(&logs_and_traces, addr);
    let inspector = PhEvmInspector::new(phevm_context);
    let env = evm_env(1, SpecId::default(), BlockEnv::default());
    let mut evm = build_optimism_evm(&mut multi_fork_db, &env, inspector);

    let result = evm.transact(tx_env).unwrap();
    let contract_state = result.state.get(&addr).unwrap();

    let storage_slot_0 = contract_state.storage.get(&U256::ZERO).unwrap();

    assert!(storage_slot_0.present_value != U256::ZERO);
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("worst-case-op");
    let operations = operations();
    test_ecrecover();

    for (op, label) in operations {
        register_op(&mut group, op.clone(), label, true);
        register_op(&mut group, op, label, false);
    }
}

// FIXME: Unrolling the loop would better isolate the worst case operation, but this is quick and
// mostly effective.
fn assemble_inf_loop(callback: Vec<Asm>) -> Vec<u8> {
    let mut runtime = vec![
        // Mark the start of the loop
        Op(JUMPDEST),
    ];

    runtime.extend(callback);

    runtime.extend(vec![
        // Jump to loop start
        Op(PUSH0),
        Op(JUMP),
    ]);

    let (_, runtime_bytecode) = assemble_minimized(&runtime, true).unwrap();
    runtime_bytecode
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
