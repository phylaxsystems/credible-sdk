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

use std::collections::HashMap;

use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::{DatabaseCommit, ForkDb, InMemoryDB, OverlayDb},
    primitives::{
        Account, AccountInfo, AccountStatus, Address, BlockEnv, Bytecode, Bytes, EvmState,
        EvmStorageSlot, TxEnv, TxKind, U256, address, keccak256,
    },
    store::AssertionStore,
    test_utils::deployed_bytecode,
};

/// Prefunded benchmark account
pub const BENCH_ACCOUNT: Address = address!("feA6F304C546857b820cDed9127156BaD2543666");

/// Dead address for simple transfers
pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");

/// ERC20 token contract address
pub const ERC20_CONTRACT: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");

/// MockERC20 artifact path
const MOCK_ERC20_ARTIFACT: &str = "MockERC20.sol:MockERC20";

/// ERC20 transfer function selector: transfer(address,uint256)
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

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

/// Create an ERC20 transfer transaction
pub fn erc20_transfer_tx(to: Address, amount: U256) -> TxEnv {
    // Encode: transfer(address,uint256)
    // selector (4 bytes) + address (32 bytes, left-padded) + amount (32 bytes)
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    data.extend_from_slice(&[0u8; 12]); // left-pad address to 32 bytes
    data.extend_from_slice(to.as_slice());
    data.extend_from_slice(&amount.to_be_bytes::<32>());

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(ERC20_CONTRACT),
        value: U256::ZERO,
        gas_limit: 60_000,
        gas_price: 1,
        data: Bytes::from(data),
        ..TxEnv::default()
    }
}

/// Calculate the storage slot for an ERC20 balance
/// MockERC20 has `_balanceOf` mapping at slot 4
fn erc20_balance_slot(owner: Address) -> U256 {
    // keccak256(abi.encode(owner, 4))
    let mut key = [0u8; 64];
    key[12..32].copy_from_slice(owner.as_slice()); // address left-padded to 32 bytes
    key[63] = 4; // slot 4
    U256::from_be_bytes(keccak256(key).0)
}

/// Defines how a transaction bundle should look like.
#[derive(Debug)]
pub struct LoadDefinition {
    pub tx_amount: usize,
    pub eoa_percent: f64,
    pub erc20_percent: f64,
    pub uni_percent: f64,
}

impl Default for LoadDefinition {
    fn default() -> Self {
        Self {
            tx_amount: 1,
            eoa_percent: 100.0,
            erc20_percent: 0.0,
            uni_percent: 0.0,
        }
    }
}

impl LoadDefinition {
    pub fn create_tx_vec(&self) -> Vec<TxEnv> {
        let mut tx_vec = Vec::with_capacity(self.tx_amount);

        let eoa_count = (self.tx_amount as f64 * self.eoa_percent / 100.0).round() as usize;
        let erc20_count = (self.tx_amount as f64 * self.erc20_percent / 100.0).round() as usize;
        let uni_count = (self.tx_amount as f64 * self.uni_percent / 100.0).round() as usize;

        let mut nonce = 0u64;

        // EOA transfers
        for _ in 0..eoa_count {
            let mut tx = eoa_tx();
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }

        // ERC20 transfers
        for _ in 0..erc20_count {
            let mut tx = erc20_transfer_tx(DEAD_ADDRESS, U256::from(1));
            tx.nonce = nonce;
            nonce += 1;
            tx_vec.push(tx);
        }

        // Uniswap swaps
        // TODO: Uniswap v3 requires adding contracts to testdata/mock-protocol:
        // - UniswapV3Factory
        // - SwapRouter
        // - At least one Pool with liquidity
        // - Two ERC20 tokens
        // For now, this panics if uni_percent > 0
        for _ in 0..uni_count {
            unimplemented!("Uniswap v3 swaps require adding Uniswap contracts to testdata");
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

        let mut evm_state: EvmState = HashMap::default();

        // Create prefunded account state
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

        // Deploy ERC20 contract if needed
        if load.erc20_percent > 0.0 {
            let code = deployed_bytecode(MOCK_ERC20_ARTIFACT);
            let code_hash = keccak256(&code);

            // Storage setup for MockERC20:
            // - Slot 4: _balanceOf mapping
            // - Slot 9: initialized = true
            let mut storage = HashMap::default();

            // Set initialized = true (slot 9)
            storage.insert(U256::from(9), EvmStorageSlot::new(U256::from(1), 0));

            // Set BENCH_ACCOUNT balance to MAX
            let balance_slot = erc20_balance_slot(BENCH_ACCOUNT);
            storage.insert(balance_slot, EvmStorageSlot::new(U256::MAX, 0));

            evm_state.insert(
                ERC20_CONTRACT,
                Account {
                    info: AccountInfo {
                        balance: U256::ZERO,
                        nonce: 1,
                        code_hash,
                        code: Some(Bytecode::new_legacy(code)),
                    },
                    transaction_id: 0,
                    storage,
                    status: AccountStatus::Touched,
                },
            );
        }

        // TODO: Deploy Uniswap v3 contracts if needed
        // This requires:
        // 1. Adding UniswapV3Factory, SwapRouter, and Pool contracts to testdata
        // 2. Deploying two ERC20 tokens
        // 3. Creating a pool with initial liquidity
        // 4. Setting up tick/liquidity state in pool storage
        if load.uni_percent > 0.0 {
            // Uniswap setup would go here once contracts are added
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use assertion_executor::db::DatabaseRef;

    #[tokio::test]
    async fn test_benchmark_package_eoa_only() {
        let load = LoadDefinition::default();
        let package = BenchmarkPackage::new(load);

        // Query BENCH_ACCOUNT - should have MAX balance
        let account = package.db.basic_ref(BENCH_ACCOUNT).unwrap().unwrap();
        assert_eq!(account.balance, U256::MAX);
        assert!(account.code.is_none());

        // ERC20 contract should not exist (erc20_percent = 0)
        let erc20 = package.db.basic_ref(ERC20_CONTRACT).unwrap();
        assert!(erc20.is_none());

        // Bundle should have 1 EOA tx
        assert_eq!(package.bundle.len(), 1);
    }

    #[tokio::test]
    async fn test_benchmark_package_with_erc20() {
        let load = LoadDefinition {
            tx_amount: 10,
            eoa_percent: 50.0,
            erc20_percent: 50.0,
            uni_percent: 0.0,
        };
        let package = BenchmarkPackage::new(load);

        // Query ERC20 contract - should have code deployed
        let erc20_account = package.db.basic_ref(ERC20_CONTRACT).unwrap().unwrap();
        assert!(erc20_account.code.is_some(), "ERC20 contract should have code");
        assert!(!erc20_account.code.as_ref().unwrap().is_empty(), "ERC20 code should not be empty");

        // Verify code hash matches
        let expected_code = deployed_bytecode(MOCK_ERC20_ARTIFACT);
        let expected_hash = keccak256(&expected_code);
        assert_eq!(erc20_account.code_hash, expected_hash);

        // Query BENCH_ACCOUNT token balance from storage
        let balance_slot = erc20_balance_slot(BENCH_ACCOUNT);
        let balance = package.db.storage_ref(ERC20_CONTRACT, balance_slot).unwrap();
        assert_eq!(balance, U256::MAX, "BENCH_ACCOUNT should have MAX token balance");

        // Bundle should have 5 EOA + 5 ERC20 txs
        assert_eq!(package.bundle.len(), 10);
    }
}
