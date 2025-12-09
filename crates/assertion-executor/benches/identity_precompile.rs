use assertion_executor::{
    evm::build_evm::{
        build_eth_evm,
        evm_env,
    },
    inspectors::NoOpInspector,
    primitives::{
        BlockEnv,
        Bytes,
        SpecId,
        TxEnv,
        TxKind,
        address,
    },
};
use criterion::{
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::{
    InspectEvm,
    database::InMemoryDB,
};

const IDENTITY_PRECOMPILE: alloy_primitives::Address =
    address!("0000000000000000000000000000000000000004");
const CALLER: alloy_primitives::Address = address!("000000000000000000000000000000000000c0de");

fn bench_identity_precompile_10k(c: &mut Criterion) {
    c.bench_function("identity_precompile_10k", |b| {
        b.iter(|| {
            let env = evm_env(1, SpecId::default(), BlockEnv::default());
            let mut db = InMemoryDB::default();
            let mut evm = build_eth_evm(&mut db, &env, NoOpInspector);

            let mut tx_env = TxEnv::default();
            tx_env.caller = CALLER.into();
            tx_env.kind = TxKind::Call(IDENTITY_PRECOMPILE.into());
            tx_env.data = Bytes::from(vec![1u8; 1024]);
            tx_env.gas_limit = 1_000_000;
            tx_env.gas_price = 0;

            for _ in 0..10_000 {
                evm.inspect_tx(tx_env.clone())
                    .expect("identity precompile call");
            }
        });
    });
}

criterion_group!(benches, bench_identity_precompile_10k);
criterion_main!(benches);
