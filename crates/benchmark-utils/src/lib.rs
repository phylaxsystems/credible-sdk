use std::collections::HashMap;

use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::{DatabaseCommit, ForkDb, InMemoryDB, OverlayDb},
    primitives::{
        Account, AccountInfo, AccountStatus, Address, BlockEnv, EvmState, TxEnv, TxKind, U256,
        address,
    },
    store::AssertionStore,
};

/// Prefunded benchmark account
pub const BENCH_ACCOUNT: Address = address!("feA6F304C546857b820cDed9127156BaD2543666");

/// Dead address for simple transfers
pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");

/// Simple transaction that just sends 1 wei to `DEAD_ADDRESS` from `BENCH_ACCOUNT`
pub fn eoa_tx() -> TxEnv {
    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(DEAD_ADDRESS),
        value: U256::from(1),
        gas_limit: 21000,
        gas_price: 1,
        ..TxEnv::default()
    }
}

/// Defines how a transaction bundle should look like.
#[derive(Debug)]
pub struct LoadDefinition {
    pub eoatx_amount: usize,
}

impl Default for LoadDefinition {
    fn default() -> Self {
        Self { eoatx_amount: 1 }
    }
}

impl LoadDefinition {
    pub fn create_tx_vec(&self) -> Vec<TxEnv> {
        let mut tx_vec = Vec::with_capacity(self.eoatx_amount);
        for i in 0..self.eoatx_amount {
            let mut tx = eoa_tx();
            tx.nonce = i as u64;
            tx_vec.push(tx);
        }
        tx_vec
    }
}

/// Contains everything needed to run a benchmark on
/// the assertion executor.
///
/// Includes an assex instance, database, and a bundle of transactions.
#[derive(Debug)]
pub struct BenchmarkPackage<Db> {
    pub executor: AssertionExecutor,
    pub db: Db,
    pub bundle: Vec<TxEnv>,
    pub block_env: BlockEnv,
}

impl BenchmarkPackage<ForkDb<OverlayDb<InMemoryDB>>> {
    pub fn new(load: LoadDefinition) -> Self {
        let store = AssertionStore::new_ephemeral();
        let executor = AssertionExecutor::new(ExecutorConfig::default(), store);

        // Create overlay without underlying db
        let mut overlay: OverlayDb<InMemoryDB> = OverlayDb::default();

        // Create prefunded account state and commit it
        let mut evm_state: EvmState = HashMap::default();
        evm_state.insert(
            BENCH_ACCOUNT,
            Account {
                info: AccountInfo {
                    balance: U256::MAX,
                    nonce: 0,
                    code_hash: Default::default(),
                    code: None,
                },
                transaction_id: 0,
                storage: HashMap::default(),
                status: AccountStatus::Touched,
            },
        );
        overlay.commit(evm_state);

        let fork_db = overlay.fork();
        let bundle = load.create_tx_vec();

        BenchmarkPackage {
            executor,
            db: fork_db,
            bundle,
            block_env: BlockEnv::default(),
        }
    }
}
