//! `benchmark-utils`
//!
//! This package is used to:
//! - Create a bundle of transactions representative of blocks on live networks,
//! - Create a *package* of everything needed to benchmark assex.
//!
//! The tx bundles is easily configurable by setting the # of tx to be included
//! and what % each of the tx should be. We have 3 groups:
//! - EOA sends,
//! - ERC20 sends,
//! - Uniswap v3 swaps.

use assertion_executor::{
    ExecutorError,
    TxExecutionError,
    db::NotFoundError,
    evm::build_evm::{
        build_eth_evm,
        evm_env,
    },
};
use revm::{
    ExecuteEvm,
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::{
        DatabaseCommit,
        ForkDb,
        InMemoryDB,
        OverlayDb,
    },
    primitives::{
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        BlockEnv,
        Bytecode,
        EvmState,
        TxEnv,
        TxKind,
        U256,
        address,
        keccak256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::{
        bytecode,
        deployed_bytecode,
    },
};

mod erc20;
mod uniswap_v3;

pub use erc20::{
    ERC20_AA_CONTRACT,
    ERC20_CONTRACT,
    erc20_aa_transfer_tx,
    erc20_transfer_tx,
};
pub use uniswap_v3::{
    UNI_V3_BENCH_AA_CONTRACT,
    UNI_V3_BENCH_CONTRACT,
    UNI_V3_DEPLOYER_ACCOUNT,
    UNI_V3_TOKEN_A,
    UNI_V3_TOKEN_B,
    uniswap_v3_aa_deploy_pool_tx,
    uniswap_v3_aa_swap_tx,
    uniswap_v3_deploy_pool_tx,
    uniswap_v3_factory_address,
    uniswap_v3_swap_tx,
};

#[derive(Debug)]
pub enum BenchmarkPackageError {
    EvmError(TxExecutionError<NotFoundError>),
    ExecutorError(ExecutorError<NotFoundError>),
    BenchmarkError,
}

/// Prefunded benchmark account
pub const BENCH_ACCOUNT: Address = address!("feA6F304C546857b820cDed9127156BaD2543666");

/// Dead address for simple transfers
pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");

/// `PayableReceiver` contract address for EOA AA transactions
pub const EOA_AA_CONTRACT: Address = address!("E0AaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa");

/// `PayableReceiver` artifact path
const PAYABLE_RECEIVER_ARTIFACT: &str = "PayableReceiver.sol:PayableReceiver";

/// Simple transaction that just sends 1 wei to the specified address from `BENCH_ACCOUNT`
#[must_use]
pub fn eoa_tx_to(target: Address) -> TxEnv {
    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(target),
        value: U256::from(1),
        gas_limit: 21000,
        gas_price: 1,
        ..TxEnv::default()
    }
}

/// Simple transaction that just sends 1 wei to `DEAD_ADDRESS` from `BENCH_ACCOUNT`
#[must_use]
pub fn eoa_tx() -> TxEnv {
    eoa_tx_to(DEAD_ADDRESS)
}

/// Simple transaction that sends 1 wei to an AA address (uses `EOA_AA_CONTRACT`)
/// Uses higher gas limit than `eoa_tx` since it calls a contract with code.
#[must_use]
pub fn eoa_aa_tx() -> TxEnv {
    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(EOA_AA_CONTRACT),
        value: U256::from(1),
        gas_limit: 30_000,
        gas_price: 1,
        ..TxEnv::default()
    }
}

/// Defines how a transaction bundle should look like.
#[derive(Debug, Clone, Copy)]
pub struct LoadDefinition {
    pub tx_amount: usize,
    pub eoa_percent: f64,
    pub erc20_percent: f64,
    pub uni_percent: f64,
    /// Percentage of contract-calling transactions (ERC20 + `UniV3`) that should
    /// target Assertion Adopter contracts (registered in the assertion store).
    /// Range: 0.0 to 100.0. Default: 0.0 (no AA transactions).
    pub aa_percent: f64,
}

/// Average loads for networks are the following:
/// | **Network**   | **EOA Transfers**  | **ERC-20 Transfers**  | **`DeFi` Protocol Calls**  | **Avg TXs/Block**    | **Avg Gas Used/Block**  |
/// | ------------- | ------------------ | --------------------- | -------------------------| -------------------- | ----------------------- |
/// | Ethereum L1   | ~10%               | ~30%                  | ~60%                     | ~330 (per 12s block) | ~30 million gas         |
/// | Arbitrum (L2) | ~5%                | ~20%                  | ~75%                     | ~40 (per 2s block)   | ~7 million Arbitrum gas |
/// | Optimism (L2) | ~8%                | ~25%                  | ~67%                     | ~60 (per 2s block)   | ~15–20 million OP gas   |
///
/// For the default impl below, we use the ethereum L1 as refrance
impl Default for LoadDefinition {
    fn default() -> Self {
        Self {
            tx_amount: 330,
            eoa_percent: 10.0,
            erc20_percent: 30.0,
            uni_percent: 60.0,
            aa_percent: 0.0,
        }
    }
}

impl LoadDefinition {
    /// Returns normalized percentages that sum to 100%.
    /// If total is 0, returns (0, 0, 0).
    fn normalized_percentages(&self) -> (f64, f64, f64) {
        let total = self.eoa_percent + self.erc20_percent + self.uni_percent;
        if total == 0.0 {
            return (0.0, 0.0, 0.0);
        }
        (
            self.eoa_percent / total * 100.0,
            self.erc20_percent / total * 100.0,
            self.uni_percent / total * 100.0,
        )
    }

    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn create_tx_vec(&self) -> Vec<TxEnv> {
        let mut tx_vec = Vec::with_capacity(self.tx_amount);

        let (eoa_pct, erc20_pct, uni_pct) = self.normalized_percentages();

        let eoa_count = (self.tx_amount as f64 * eoa_pct / 100.0).round() as usize;
        let erc20_count = (self.tx_amount as f64 * erc20_pct / 100.0).round() as usize;
        let uni_count = (self.tx_amount as f64 * uni_pct / 100.0).round() as usize;

        // Calculate how many of each tx type should target AAs
        let aa_ratio = self.aa_percent / 100.0;
        let eoa_aa_count = (eoa_count as f64 * aa_ratio).round() as usize;
        let erc20_aa_count = (erc20_count as f64 * aa_ratio).round() as usize;
        let uni_aa_count = (uni_count as f64 * aa_ratio).round() as usize;

        let mut nonce = 0u64;

        // EOA transfers - AA variant first (targets ERC20_AA_CONTRACT), then non-AA
        for _ in 0..eoa_aa_count {
            let mut tx = eoa_aa_tx();
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }
        for _ in eoa_aa_count..eoa_count {
            let mut tx = eoa_tx();
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }

        // ERC20 transfers - AA variant first, then non-AA
        for _ in 0..erc20_aa_count {
            let mut tx = erc20_aa_transfer_tx(DEAD_ADDRESS, U256::from(1));
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }
        for _ in erc20_aa_count..erc20_count {
            let mut tx = erc20_transfer_tx(DEAD_ADDRESS, U256::from(1));
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }

        // Uniswap swaps - AA variant
        // We include a single pool deployment+liquidity tx (if `uni_aa_count > 0`),
        // and the remaining `uni_aa_count - 1` txs are swaps against that pool.
        if uni_aa_count > 0 {
            let tx = uniswap_v3_aa_deploy_pool_tx(nonce);
            nonce += 1;
            tx_vec.push(tx);
        }
        for _ in 1..uni_aa_count {
            let tx = uniswap_v3_aa_swap_tx(nonce);
            nonce += 1;
            tx_vec.push(tx);
        }

        // Uniswap swaps - non-AA variant
        // We include a single pool deployment+liquidity tx (if there are non-AA uni txs),
        // and the remaining txs are swaps against that pool.
        let uni_non_aa_count = uni_count.saturating_sub(uni_aa_count);
        if uni_non_aa_count > 0 {
            let tx = uniswap_v3_deploy_pool_tx(nonce);
            nonce += 1;
            tx_vec.push(tx);
        }
        for _ in 1..uni_non_aa_count {
            let tx = uniswap_v3_swap_tx(nonce);
            nonce += 1;
            tx_vec.push(tx);
        }

        tx_vec
    }

    /// Returns whether AA contracts need to be deployed based on `aa_percent`
    #[must_use]
    pub fn needs_aa_contracts(&self) -> bool {
        self.aa_percent > 0.0
    }

    /// Returns whether the ERC20 AA contract is needed
    #[must_use]
    pub fn needs_erc20_aa(&self) -> bool {
        self.aa_percent > 0.0 && self.erc20_percent > 0.0
    }

    /// Returns whether the EOA AA contract is needed (`PayableReceiver` for ETH transfers)
    #[must_use]
    pub fn needs_eoa_aa(&self) -> bool {
        self.aa_percent > 0.0 && self.eoa_percent > 0.0
    }

    /// Returns whether the `UniV3` AA contracts are needed
    #[must_use]
    pub fn needs_uni_aa(&self) -> bool {
        self.aa_percent > 0.0 && self.uni_percent > 0.0
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

/// `AverageAssertion` artifact path
const AVERAGE_ASSERTION_ARTIFACT: &str = "AverageAssertion.sol:AverageAssertion";

/// Creates a real assertion state from the `AverageAssertion` contract.
/// This assertion exercises typical precompile operations (fork, load, state changes, arithmetic).
fn create_average_assertion_state(config: &ExecutorConfig) -> AssertionState {
    let assertion_bytecode = bytecode(AVERAGE_ASSERTION_ARTIFACT);
    AssertionState::new_active(&assertion_bytecode, config)
        .expect("Failed to create AverageAssertion state")
}

/// Inserts the `PayableReceiver` contract for EOA AA transactions.
fn insert_payable_receiver(evm_state: &mut EvmState) {
    let code = deployed_bytecode(PAYABLE_RECEIVER_ARTIFACT);
    let code_hash = keccak256(&code);

    evm_state.insert(
        EOA_AA_CONTRACT,
        Account {
            info: AccountInfo {
                balance: U256::ZERO,
                nonce: 1,
                code_hash,
                code: Some(Bytecode::new_legacy(code)),
            },
            transaction_id: 0,
            storage: HashMap::default(),
            status: AccountStatus::Touched,
        },
    );
}

impl BenchmarkPackage<ForkDb<OverlayDb<InMemoryDB>>> {
    /// Creates a new benchmark package with the given load definition.
    ///
    /// # Panics
    ///
    /// Panics if assertion state creation or registration fails.
    #[must_use]
    pub fn new(load: LoadDefinition) -> Self {
        let config = ExecutorConfig::default();
        let store = AssertionStore::new_ephemeral();

        // Register AA addresses in the store with real assertions based on what's needed
        if load.needs_eoa_aa() {
            store
                .insert(EOA_AA_CONTRACT, create_average_assertion_state(&config))
                .expect("Failed to register EOA AA contract");
        }
        if load.needs_erc20_aa() {
            store
                .insert(ERC20_AA_CONTRACT, create_average_assertion_state(&config))
                .expect("Failed to register ERC20 AA contract");
        }
        if load.needs_uni_aa() {
            store
                .insert(
                    UNI_V3_BENCH_AA_CONTRACT,
                    create_average_assertion_state(&config),
                )
                .expect("Failed to register UniV3 AA contract");
        }

        let executor = AssertionExecutor::new(config, store);

        // Create overlay with an in-memory underlying DB so basic lookups (e.g. empty code hash)
        // don't error during tx execution.
        let mut overlay: OverlayDb<InMemoryDB> =
            OverlayDb::new(Some(Arc::new(InMemoryDB::default())));

        let mut evm_state: EvmState = HashMap::default();

        // Create prefunded account state
        evm_state.insert(
            BENCH_ACCOUNT,
            Account {
                info: AccountInfo {
                    balance: U256::MAX,
                    nonce: 0,
                    code_hash: keccak256([]),
                    code: None,
                },
                transaction_id: 0,
                storage: HashMap::default(),
                status: AccountStatus::Touched,
            },
        );

        // Deploy EOA AA contract if needed (PayableReceiver for ETH transfers)
        if load.needs_eoa_aa() {
            insert_payable_receiver(&mut evm_state);
        }

        // Deploy ERC20 contracts if needed
        if load.erc20_percent > 0.0 {
            erc20::insert_benchmark_erc20(&mut evm_state);
        }
        // Deploy ERC20 AA contract if needed
        if load.needs_erc20_aa() {
            erc20::insert_benchmark_erc20_aa(&mut evm_state);
        }

        // Deploy UniV3 contracts if needed
        if load.uni_percent > 0.0 {
            uniswap_v3::insert_uniswap_v3_accounts(&mut evm_state);
        }
        if load.needs_uni_aa() {
            uniswap_v3::insert_uniswap_v3_aa_accounts(&mut evm_state);
        }

        overlay.commit(evm_state);

        let mut fork_db = overlay.fork();

        // Deploy UniV3 factories
        if load.uni_percent > 0.0 {
            uniswap_v3::deploy_uniswap_v3_factory(&executor, &mut fork_db);
        }
        if load.needs_uni_aa() {
            uniswap_v3::deploy_uniswap_v3_aa_factory(&executor, &mut fork_db);
        }

        let bundle = load.create_tx_vec();

        BenchmarkPackage {
            executor,
            db: fork_db,
            bundle,
            block_env: BlockEnv::default(),
        }
    }

    /// Executes all of the transactions by sending them to the executor in a loop,
    /// including assertion validation for AA transactions.
    /// Returns `()` if everything was a success or `BenchmarkError` if any tx
    /// failed to execute.
    ///
    /// # Errors
    ///
    /// Returns `BenchmarkPackageError::ExecutorError` if transaction/assertion execution fails,
    /// or `BenchmarkPackageError::BenchmarkError` if a transaction does not succeed.
    pub fn run(&mut self) -> Result<(), BenchmarkPackageError> {
        use assertion_executor::primitives::ExecutionResult;
        use revm::context_interface::result::SuccessReason;

        for tx in self.bundle.clone() {
            let result = self
                .executor
                .validate_transaction(self.block_env.clone(), &tx, &mut self.db, true)
                .map_err(BenchmarkPackageError::ExecutorError)?;

            match result.result_and_state.result {
                ExecutionResult::Success {
                    reason: SuccessReason::Stop | SuccessReason::Return,
                    ..
                } => {}
                _ => return Err(BenchmarkPackageError::BenchmarkError),
            }

            // Note: validate_transaction with commit=true already commits the state
        }

        Ok(())
    }

    /// Executes all transactions using vanilla revm without any assertion executor infrastructure.
    /// No `CallTracer`, no assertion store - just pure EVM execution with `NoOpInspector`.
    /// This provides a true baseline for comparing assertion overhead.
    ///
    /// # Errors
    ///
    /// Returns `BenchmarkPackageError::BenchmarkError` if a transaction does not succeed.
    pub fn run_vanilla(&mut self) -> Result<(), BenchmarkPackageError> {
        use assertion_executor::primitives::ExecutionResult;
        use revm::context_interface::result::SuccessReason;

        let env = evm_env(
            self.executor.config.chain_id,
            SpecId::CANCUN,
            self.block_env.clone(),
        );

        for tx in self.bundle.clone() {
            let mut evm = build_eth_evm(&mut self.db, &env, NoOpInspector);

            let result = evm
                .transact(tx)
                .map_err(|_| BenchmarkPackageError::BenchmarkError)?;

            match result.result {
                ExecutionResult::Success {
                    reason: SuccessReason::Stop | SuccessReason::Return,
                    ..
                } => {}
                _ => return Err(BenchmarkPackageError::BenchmarkError),
            }

            self.db.commit(result.state);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertion_executor::{
        db::DatabaseRef,
        primitives::{
            ExecutionResult,
            keccak256,
        },
    };
    use revm::context_interface::result::SuccessReason;

    #[tokio::test]
    async fn test_benchmark_package_with_erc20() {
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 50.0,
            erc20_percent: 50.0,
            uni_percent: 0.0,
            aa_percent: 0.0,
        };
        let package = BenchmarkPackage::new(load);

        // Query ERC20 contract - should have code deployed
        let erc20_account = package.db.basic_ref(ERC20_CONTRACT).unwrap().unwrap();
        assert!(
            erc20_account.code.is_some(),
            "ERC20 contract should have code"
        );
        assert!(
            !erc20_account.code.as_ref().unwrap().is_empty(),
            "ERC20 code should not be empty"
        );

        // Verify code hash matches
        let expected_code = erc20::mock_erc20_deployed_bytecode();
        let expected_hash = keccak256(&expected_code);
        assert_eq!(erc20_account.code_hash, expected_hash);

        // Query BENCH_ACCOUNT token balance from storage
        let balance_slot = erc20::erc20_balance_slot(BENCH_ACCOUNT);
        let balance = package
            .db
            .storage_ref(ERC20_CONTRACT, balance_slot)
            .unwrap();
        assert_eq!(
            balance,
            U256::MAX,
            "BENCH_ACCOUNT should have MAX token balance"
        );

        // Bundle should have 5 EOA + 5 ERC20 txs
        assert_eq!(package.bundle.len(), 10);
    }

    #[tokio::test]
    async fn test_benchmark_package_with_uniswap_v3_pool_deploy_and_swap() {
        let load = LoadDefinition {
            tx_amount: 2,
            eoa_percent: 0.0,
            erc20_percent: 0.0,
            uni_percent: 100.0,
            aa_percent: 0.0,
        };

        let mut package = BenchmarkPackage::new(load);

        // Factory should be deployed during setup
        let factory = uniswap_v3_factory_address();
        let factory_account = package.db.basic_ref(factory).unwrap().unwrap();
        assert!(factory_account.code.is_some());

        // Execute pool deploy tx then swap tx
        for tx in package.bundle.clone() {
            let result = package
                .executor
                .execute_forked_tx_ext_db(&package.block_env, tx, &mut package.db)
                .unwrap();

            match result.result_and_state.result {
                ExecutionResult::Success {
                    reason: SuccessReason::Stop | SuccessReason::Return,
                    ..
                } => {}
                other => panic!("tx failed: {other:?}"),
            }

            package.db.commit(result.result_and_state.state);
        }
    }

    #[test]
    fn test_normalized_percentages() {
        // Exact 100% - no change
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 50.0,
            erc20_percent: 30.0,
            uni_percent: 20.0,
            aa_percent: 0.0,
        };
        let (eoa, erc20, uni) = load.normalized_percentages();
        assert!((eoa - 50.0).abs() < f64::EPSILON);
        assert!((erc20 - 30.0).abs() < f64::EPSILON);
        assert!((uni - 20.0).abs() < f64::EPSILON);

        // Over 100% (150%) - should normalize
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 50.0,
            erc20_percent: 50.0,
            uni_percent: 50.0,
            aa_percent: 0.0,
        };
        let (eoa, erc20, uni) = load.normalized_percentages();
        assert!((eoa - 100.0 / 3.0).abs() < 0.001);
        assert!((erc20 - 100.0 / 3.0).abs() < 0.001);
        assert!((uni - 100.0 / 3.0).abs() < 0.001);

        // Under 100% (50%) - should normalize
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 25.0,
            erc20_percent: 25.0,
            uni_percent: 0.0,
            aa_percent: 0.0,
        };
        let (eoa, erc20, uni) = load.normalized_percentages();
        assert!((eoa - 50.0).abs() < f64::EPSILON);
        assert!((erc20 - 50.0).abs() < f64::EPSILON);
        assert!(uni.abs() < f64::EPSILON);

        // Zero total - returns zeros
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 0.0,
            erc20_percent: 0.0,
            uni_percent: 0.0,
            aa_percent: 0.0,
        };
        let (eoa, erc20, uni) = load.normalized_percentages();
        assert!(eoa.abs() < f64::EPSILON);
        assert!(erc20.abs() < f64::EPSILON);
        assert!(uni.abs() < f64::EPSILON);
    }

    #[test]
    fn test_create_tx_vec_normalizes_percentages() {
        // 150% total should normalize to equal thirds
        let load = LoadDefinition {
            tx_amount: 9,
            eoa_percent: 50.0,
            erc20_percent: 50.0,
            uni_percent: 50.0,
            aa_percent: 0.0,
        };
        let txs = load.create_tx_vec();
        // Each should get 3 txs (33.33% of 9 rounded)
        assert_eq!(txs.len(), 9);
    }

    #[tokio::test]
    async fn test_benchmark_package_with_aa_percent() {
        // Test with 50% AA transactions
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 0.0,
            erc20_percent: 100.0,
            uni_percent: 0.0,
            aa_percent: 50.0,
        };
        let package = BenchmarkPackage::new(load);

        // Should have both ERC20 contracts deployed
        let erc20_account = package.db.basic_ref(ERC20_CONTRACT).unwrap().unwrap();
        assert!(
            erc20_account.code.is_some(),
            "ERC20 contract should have code"
        );

        let erc20_aa_account = package.db.basic_ref(ERC20_AA_CONTRACT).unwrap().unwrap();
        assert!(
            erc20_aa_account.code.is_some(),
            "ERC20 AA contract should have code"
        );

        // Bundle should have 10 txs total (5 AA + 5 non-AA)
        assert_eq!(package.bundle.len(), 10);

        // Verify tx targets: first 5 should be AA, last 5 should be non-AA
        for (i, tx) in package.bundle.iter().enumerate() {
            let expected_target = if i < 5 {
                ERC20_AA_CONTRACT
            } else {
                ERC20_CONTRACT
            };
            match tx.kind {
                TxKind::Call(addr) => assert_eq!(addr, expected_target, "tx {i} target mismatch"),
                TxKind::Create => panic!("Expected Call transaction"),
            }
        }
    }

    #[tokio::test]
    async fn test_benchmark_package_with_eoa_aa_percent() {
        // Test EOA transactions with AA percentage
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 100.0,
            erc20_percent: 0.0,
            uni_percent: 0.0,
            aa_percent: 30.0,
        };
        let package = BenchmarkPackage::new(load);

        // EOA AA contract should be deployed (PayableReceiver for ETH transfers)
        let eoa_aa_account = package.db.basic_ref(EOA_AA_CONTRACT).unwrap().unwrap();
        assert!(
            eoa_aa_account.code.is_some(),
            "EOA AA contract should have code for EOA AA txs"
        );

        // Bundle should have 10 txs total (3 AA + 7 non-AA)
        assert_eq!(package.bundle.len(), 10);

        // Verify tx targets: first 3 should target EOA_AA_CONTRACT, last 7 should target DEAD_ADDRESS
        for (i, tx) in package.bundle.iter().enumerate() {
            let expected_target = if i < 3 { EOA_AA_CONTRACT } else { DEAD_ADDRESS };
            match tx.kind {
                TxKind::Call(addr) => assert_eq!(addr, expected_target, "tx {i} target mismatch"),
                TxKind::Create => panic!("Expected Call transaction"),
            }
        }
    }

    #[tokio::test]
    async fn test_benchmark_package_run_with_aa_assertions() {
        // Test that the run() method works with AA transactions and real assertions
        let load = LoadDefinition {
            tx_amount: 5,
            eoa_percent: 0.0,
            erc20_percent: 100.0,
            uni_percent: 0.0,
            aa_percent: 40.0, // 2 AA txs, 3 non-AA txs
        };
        let mut package = BenchmarkPackage::new(load);

        // Run all transactions - this should execute assertions for AA txs
        package.run().expect("Benchmark run should succeed");
    }

    #[tokio::test]
    async fn test_benchmark_package_run_mixed_load() {
        // Test running with a mixed load (EOA + ERC20 + UniV3)
        // Note: UniV3 pool has limited liquidity, so we limit the number of swaps
        // to avoid hitting price limits. 1 deploy + 2 swaps works well.
        let load = LoadDefinition {
            tx_amount: 6,
            eoa_percent: 17.0,   // 1 tx
            erc20_percent: 33.0, // 2 txs
            uni_percent: 50.0,   // 3 txs (1 deploy + 2 swaps)
            aa_percent: 0.0,
        };
        let mut package = BenchmarkPackage::new(load);

        // Run all transactions
        package
            .run()
            .expect("Benchmark run with mixed load should succeed");
    }
}
