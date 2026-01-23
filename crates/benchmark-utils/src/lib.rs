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

use std::{
    collections::HashMap,
    sync::Arc,
};

use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::{DatabaseCommit, ForkDb, InMemoryDB, OverlayDb},
    primitives::{
        Account, AccountInfo, AccountStatus, Address, BlockEnv, Bytecode, Bytes, EvmState,
        EvmStorageSlot, ExecutionResult, TxEnv, TxKind, U256, address, keccak256,
    },
    store::AssertionStore,
    test_utils::{bytecode, deployed_bytecode},
};
use revm::context_interface::result::Output;

/// Prefunded benchmark account
pub const BENCH_ACCOUNT: Address = address!("feA6F304C546857b820cDed9127156BaD2543666");

/// Prefunded deployer account used for Uniswap v3 setup txs
pub const UNI_V3_DEPLOYER_ACCOUNT: Address = address!("1111111111111111111111111111111111111111");

/// Dead address for simple transfers
pub const DEAD_ADDRESS: Address = address!("000000000000000000000000000000000000dEaD");

/// ERC20 token contract address
pub const ERC20_CONTRACT: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");

/// Uniswap v3 benchmark helper contract address (`UniV3Bench`)
pub const UNI_V3_BENCH_CONTRACT: Address = address!("2222222222222222222222222222222222222222");

/// Uniswap v3 token A contract address (MockERC20)
pub const UNI_V3_TOKEN_A: Address = address!("3333333333333333333333333333333333333333");

/// Uniswap v3 token B contract address (MockERC20)
pub const UNI_V3_TOKEN_B: Address = address!("4444444444444444444444444444444444444444");

/// MockERC20 artifact path
const MOCK_ERC20_ARTIFACT: &str = "MockERC20.sol:MockERC20";

/// Uniswap v3 factory artifact path
const UNISWAP_V3_FACTORY_ARTIFACT: &str = "UniswapV3Factory.sol:UniswapV3Factory";

/// UniV3Bench helper contract artifact path
const UNI_V3_BENCH_ARTIFACT: &str = "UniV3Bench.sol:UniV3Bench";

/// ERC20 transfer function selector: transfer(address,uint256)
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

/// UniV3Bench.deployPoolAndMint selector
const UNI_V3_DEPLOY_POOL_AND_MINT_SELECTOR: [u8; 4] = [0xf0, 0x36, 0x72, 0x6c];

/// UniV3Bench.swap selector
const UNI_V3_SWAP_SELECTOR: [u8; 4] = [0x39, 0xab, 0xe2, 0x19];

/// Default Uniswap v3 pool fee (0.3%)
const UNI_V3_POOL_FEE: u32 = 3_000;

/// Default Uniswap v3 initial price: 1:1 (sqrtPriceX96 = 2^96)
const UNI_V3_SQRT_PRICE_X96: u128 = 1u128 << 96;

/// Default Uniswap v3 liquidity parameters (tickSpacing for 0.3% fee is 60)
const UNI_V3_TICK_LOWER: i32 = -600;
const UNI_V3_TICK_UPPER: i32 = 600;
const UNI_V3_LIQUIDITY: u128 = 100_000;

/// TickMath.MIN_SQRT_RATIO + 1 (used as swap price limit for zeroForOne swaps)
const UNI_V3_MIN_SQRT_RATIO_PLUS_ONE: u64 = 4_295_128_740;

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

fn rlp_encode_u64(value: u64) -> Vec<u8> {
    if value == 0 {
        return vec![0x80];
    }

    if value < 0x80 {
        return vec![value as u8];
    }

    let mut buf = value.to_be_bytes().to_vec();
    while buf.first() == Some(&0) {
        buf.remove(0);
    }

    let mut out = Vec::with_capacity(1 + buf.len());
    out.push(0x80 + buf.len() as u8);
    out.extend_from_slice(&buf);
    out
}

fn contract_address(deployer: Address, nonce: u64) -> Address {
    // keccak256(rlp([deployer, nonce]))[12..]
    let mut payload = Vec::with_capacity(32);
    payload.push(0x94);
    payload.extend_from_slice(deployer.as_slice());
    payload.extend_from_slice(&rlp_encode_u64(nonce));

    let mut rlp = Vec::with_capacity(1 + payload.len());
    rlp.push(0xc0 + payload.len() as u8);
    rlp.extend_from_slice(&payload);

    let hash = keccak256(&rlp);
    Address::from_slice(&hash.0[12..])
}

pub fn uniswap_v3_factory_address() -> Address {
    contract_address(UNI_V3_DEPLOYER_ACCOUNT, 0)
}

fn abi_encode_address(value: Address, out: &mut Vec<u8>) {
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(value.as_slice());
}

fn abi_encode_uint(value: U256, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_be_bytes::<32>());
}

fn abi_encode_bool(value: bool, out: &mut Vec<u8>) {
    abi_encode_uint(if value { U256::from(1) } else { U256::ZERO }, out);
}

fn abi_encode_int(value: i128, out: &mut Vec<u8>) {
    let encoded = if value >= 0 {
        U256::from(value as u128)
    } else {
        (!U256::from((-value) as u128)) + U256::from(1)
    };
    out.extend_from_slice(&encoded.to_be_bytes::<32>());
}

pub fn uniswap_v3_deploy_pool_tx(nonce: u64) -> TxEnv {
    let mut data = Vec::with_capacity(4 + 32 * 8);
    data.extend_from_slice(&UNI_V3_DEPLOY_POOL_AND_MINT_SELECTOR);

    abi_encode_address(uniswap_v3_factory_address(), &mut data);
    abi_encode_address(UNI_V3_TOKEN_A, &mut data);
    abi_encode_address(UNI_V3_TOKEN_B, &mut data);
    abi_encode_uint(U256::from(UNI_V3_POOL_FEE), &mut data);
    abi_encode_uint(U256::from(UNI_V3_SQRT_PRICE_X96), &mut data);
    abi_encode_int(UNI_V3_TICK_LOWER as i128, &mut data);
    abi_encode_int(UNI_V3_TICK_UPPER as i128, &mut data);
    abi_encode_uint(U256::from(UNI_V3_LIQUIDITY), &mut data);

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(UNI_V3_BENCH_CONTRACT),
        value: U256::ZERO,
        gas_limit: 15_000_000,
        gas_price: 1,
        data: Bytes::from(data),
        nonce,
        ..TxEnv::default()
    }
}

pub fn uniswap_v3_swap_tx(nonce: u64) -> TxEnv {
    let mut data = Vec::with_capacity(4 + 32 * 7);
    data.extend_from_slice(&UNI_V3_SWAP_SELECTOR);

    abi_encode_address(uniswap_v3_factory_address(), &mut data);
    abi_encode_address(UNI_V3_TOKEN_A, &mut data);
    abi_encode_address(UNI_V3_TOKEN_B, &mut data);
    abi_encode_uint(U256::from(UNI_V3_POOL_FEE), &mut data);
    abi_encode_bool(true, &mut data); // zeroForOne
    abi_encode_int(1_000, &mut data); // amountSpecified (exact input)
    abi_encode_uint(U256::from(UNI_V3_MIN_SQRT_RATIO_PLUS_ONE), &mut data);

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(UNI_V3_BENCH_CONTRACT),
        value: U256::ZERO,
        gas_limit: 2_000_000,
        gas_price: 1,
        data: Bytes::from(data),
        nonce,
        ..TxEnv::default()
    }
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
        //
        // We include a single pool deployment+liquidity tx (if `uni_count > 0`),
        // and the remaining `uni_count - 1` txs are swaps against that pool.
        if uni_count > 0 {
            let tx = uniswap_v3_deploy_pool_tx(nonce);
            nonce += 1;
            tx_vec.push(tx);
        }

        for _ in 1..uni_count {
            let tx = uniswap_v3_swap_tx(nonce);
            nonce += 1;
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

        // Create overlay with an in-memory underlying DB so basic lookups (e.g. empty code hash)
        // don't error during tx execution.
        let mut overlay: OverlayDb<InMemoryDB> = OverlayDb::new(Some(Arc::new(InMemoryDB::default())));

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

        if load.uni_percent > 0.0 {
            // Uniswap v3 setup:
            // - Deploy `UniswapV3Factory` via a setup tx (needed for immutables in NoDelegateCall)
            // - Predeploy `UniV3Bench` helper contract
            // - Predeploy two MockERC20 tokens and fund `UniV3Bench` so it can mint/swap via callbacks

            // Prefund deployer
            evm_state.insert(
                UNI_V3_DEPLOYER_ACCOUNT,
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

            // Predeploy UniV3Bench helper contract
            let bench_code = deployed_bytecode(UNI_V3_BENCH_ARTIFACT);
            let bench_code_hash = keccak256(&bench_code);
            evm_state.insert(
                UNI_V3_BENCH_CONTRACT,
                Account {
                    info: AccountInfo {
                        balance: U256::ZERO,
                        nonce: 1,
                        code_hash: bench_code_hash,
                        code: Some(Bytecode::new_legacy(bench_code)),
                    },
                    transaction_id: 0,
                    storage: HashMap::default(),
                    status: AccountStatus::Touched,
                },
            );

            // Predeploy two MockERC20 tokens and fund UniV3Bench
            let token_code = deployed_bytecode(MOCK_ERC20_ARTIFACT);
            let token_code_hash = keccak256(&token_code);

            let mut token_a_storage = HashMap::default();
            token_a_storage.insert(U256::from(9), EvmStorageSlot::new(U256::from(1), 0));
            token_a_storage.insert(
                erc20_balance_slot(UNI_V3_BENCH_CONTRACT),
                EvmStorageSlot::new(U256::MAX, 0),
            );

            evm_state.insert(
                UNI_V3_TOKEN_A,
                Account {
                    info: AccountInfo {
                        balance: U256::ZERO,
                        nonce: 1,
                        code_hash: token_code_hash,
                        code: Some(Bytecode::new_legacy(token_code.clone())),
                    },
                    transaction_id: 0,
                    storage: token_a_storage,
                    status: AccountStatus::Touched,
                },
            );

            let mut token_b_storage = HashMap::default();
            token_b_storage.insert(U256::from(9), EvmStorageSlot::new(U256::from(1), 0));
            token_b_storage.insert(
                erc20_balance_slot(UNI_V3_BENCH_CONTRACT),
                EvmStorageSlot::new(U256::MAX, 0),
            );

            evm_state.insert(
                UNI_V3_TOKEN_B,
                Account {
                    info: AccountInfo {
                        balance: U256::ZERO,
                        nonce: 1,
                        code_hash: token_code_hash,
                        code: Some(Bytecode::new_legacy(token_code)),
                    },
                    transaction_id: 0,
                    storage: token_b_storage,
                    status: AccountStatus::Touched,
                },
            );
        }

        overlay.commit(evm_state);

        let mut fork_db = overlay.fork();

        if load.uni_percent > 0.0 {
            let factory_deploy_tx = TxEnv {
                caller: UNI_V3_DEPLOYER_ACCOUNT,
                kind: TxKind::Create,
                data: bytecode(UNISWAP_V3_FACTORY_ARTIFACT),
                gas_limit: 15_000_000,
                gas_price: 1,
                nonce: 0,
                ..TxEnv::default()
            };

            let result = executor
                .execute_forked_tx_ext_db(&BlockEnv::default(), factory_deploy_tx, &mut fork_db)
                .expect("UniswapV3Factory deployment tx should succeed");

            let deployed_factory = match &result.result_and_state.result {
                ExecutionResult::Success { output, .. } => match output {
                    Output::Create(_, Some(address)) => *address,
                    Output::Create(_, None) => panic!("Factory deployment did not return a created address"),
                    Output::Call(_) => panic!("Factory deployment returned call output"),
                },
                other => panic!("Factory deployment failed: {other:?}"),
            };

            let expected_factory = uniswap_v3_factory_address();
            assert_eq!(
                deployed_factory, expected_factory,
                "Factory deployed at unexpected address"
            );

            fork_db.commit(result.result_and_state.state);
        }

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
    use revm::context_interface::result::SuccessReason;

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

    #[tokio::test]
    async fn test_benchmark_package_with_uniswap_v3_pool_deploy_and_swap() {
        let load = LoadDefinition {
            tx_amount: 2,
            eoa_percent: 0.0,
            erc20_percent: 0.0,
            uni_percent: 100.0,
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
}
