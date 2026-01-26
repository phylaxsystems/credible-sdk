use std::collections::HashMap;

use assertion_executor::{
    AssertionExecutor,
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
        Bytes,
        EvmState,
        ExecutionResult,
        TxEnv,
        TxKind,
        U256,
        address,
        keccak256,
    },
    test_utils::{
        bytecode,
        deployed_bytecode,
    },
};
use revm::context_interface::result::Output;

use crate::{
    BENCH_ACCOUNT,
    erc20,
};

/// Prefunded deployer account used for Uniswap v3 setup txs (non-AA)
pub const UNI_V3_DEPLOYER_ACCOUNT: Address = address!("1111111111111111111111111111111111111111");

/// Prefunded deployer account used for Uniswap v3 setup txs (AA variant)
pub const UNI_V3_AA_DEPLOYER_ACCOUNT: Address =
    address!("1111111111111111111111111111111111111112");

/// Uniswap v3 benchmark helper contract address (`UniV3Bench`) - non-AA
pub const UNI_V3_BENCH_CONTRACT: Address = address!("2222222222222222222222222222222222222222");

/// Uniswap v3 benchmark helper contract address (`UniV3Bench`) - AA variant
pub const UNI_V3_BENCH_AA_CONTRACT: Address = address!("2222222222222222222222222222222222222223");

/// Uniswap v3 token A contract address (`MockERC20`)
pub const UNI_V3_TOKEN_A: Address = address!("3333333333333333333333333333333333333333");

/// Uniswap v3 token B contract address (`MockERC20`)
pub const UNI_V3_TOKEN_B: Address = address!("4444444444444444444444444444444444444444");

/// Uniswap v3 token A contract address (`MockERC20`) - for AA bench contract
pub const UNI_V3_AA_TOKEN_A: Address = address!("3333333333333333333333333333333333333334");

/// Uniswap v3 token B contract address (`MockERC20`) - for AA bench contract
pub const UNI_V3_AA_TOKEN_B: Address = address!("4444444444444444444444444444444444444445");

/// Uniswap v3 factory artifact path
const UNISWAP_V3_FACTORY_ARTIFACT: &str = "UniswapV3Factory.sol:UniswapV3Factory";

/// `UniV3Bench` helper contract artifact path
const UNI_V3_BENCH_ARTIFACT: &str = "UniV3Bench.sol:UniV3Bench";

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

/// `TickMath.MIN_SQRT_RATIO` + 1 (used as swap price limit for zeroForOne swaps)
const UNI_V3_MIN_SQRT_RATIO_PLUS_ONE: u64 = 4_295_128_740;

#[allow(clippy::cast_possible_truncation)]
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

#[allow(clippy::cast_possible_truncation)]
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

#[must_use]
pub fn uniswap_v3_factory_address() -> Address {
    contract_address(UNI_V3_DEPLOYER_ACCOUNT, 0)
}

pub fn uniswap_v3_aa_factory_address() -> Address {
    contract_address(UNI_V3_AA_DEPLOYER_ACCOUNT, 0)
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

#[allow(clippy::cast_sign_loss)]
fn abi_encode_int(value: i128, out: &mut Vec<u8>) {
    let encoded = if value >= 0 {
        U256::from(value.cast_unsigned())
    } else {
        (!U256::from((-value).cast_unsigned())) + U256::from(1)
    };
    out.extend_from_slice(&encoded.to_be_bytes::<32>());
}

/// Parameters for creating `UniV3` transactions
#[derive(Debug, Clone, Copy)]
pub struct UniV3TxParams {
    pub bench_contract: Address,
    pub factory: Address,
    pub token_a: Address,
    pub token_b: Address,
}

impl UniV3TxParams {
    /// Non-AA variant parameters
    pub fn non_aa() -> Self {
        Self {
            bench_contract: UNI_V3_BENCH_CONTRACT,
            factory: uniswap_v3_factory_address(),
            token_a: UNI_V3_TOKEN_A,
            token_b: UNI_V3_TOKEN_B,
        }
    }

    /// AA variant parameters
    pub fn aa() -> Self {
        Self {
            bench_contract: UNI_V3_BENCH_AA_CONTRACT,
            factory: uniswap_v3_aa_factory_address(),
            token_a: UNI_V3_AA_TOKEN_A,
            token_b: UNI_V3_AA_TOKEN_B,
        }
    }
}

pub fn uniswap_v3_deploy_pool_tx_with_params(nonce: u64, params: &UniV3TxParams) -> TxEnv {
    let mut data = Vec::with_capacity(4 + 32 * 8);
    data.extend_from_slice(&UNI_V3_DEPLOY_POOL_AND_MINT_SELECTOR);

    abi_encode_address(params.factory, &mut data);
    abi_encode_address(params.token_a, &mut data);
    abi_encode_address(params.token_b, &mut data);
    abi_encode_uint(U256::from(UNI_V3_POOL_FEE), &mut data);
    abi_encode_uint(U256::from(UNI_V3_SQRT_PRICE_X96), &mut data);
    abi_encode_int(i128::from(UNI_V3_TICK_LOWER), &mut data);
    abi_encode_int(i128::from(UNI_V3_TICK_UPPER), &mut data);
    abi_encode_uint(U256::from(UNI_V3_LIQUIDITY), &mut data);

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(params.bench_contract),
        value: U256::ZERO,
        gas_limit: 15_000_000,
        gas_price: 1,
        data: Bytes::from(data),
        nonce,
        ..TxEnv::default()
    }
}

pub fn uniswap_v3_swap_tx_with_params(nonce: u64, params: &UniV3TxParams) -> TxEnv {
    let mut data = Vec::with_capacity(4 + 32 * 7);
    data.extend_from_slice(&UNI_V3_SWAP_SELECTOR);

    abi_encode_address(params.factory, &mut data);
    abi_encode_address(params.token_a, &mut data);
    abi_encode_address(params.token_b, &mut data);
    abi_encode_uint(U256::from(UNI_V3_POOL_FEE), &mut data);
    abi_encode_bool(true, &mut data); // zeroForOne
    abi_encode_int(1_000, &mut data); // amountSpecified (exact input)
    abi_encode_uint(U256::from(UNI_V3_MIN_SQRT_RATIO_PLUS_ONE), &mut data);

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(params.bench_contract),
        value: U256::ZERO,
        gas_limit: 2_000_000,
        gas_price: 1,
        data: Bytes::from(data),
        nonce,
        ..TxEnv::default()
    }
}

#[must_use]
pub fn uniswap_v3_deploy_pool_tx(nonce: u64) -> TxEnv {
    uniswap_v3_deploy_pool_tx_with_params(nonce, &UniV3TxParams::non_aa())
}

#[must_use]
pub fn uniswap_v3_swap_tx(nonce: u64) -> TxEnv {
    uniswap_v3_swap_tx_with_params(nonce, &UniV3TxParams::non_aa())
}

#[must_use]
pub fn uniswap_v3_aa_deploy_pool_tx(nonce: u64) -> TxEnv {
    uniswap_v3_deploy_pool_tx_with_params(nonce, &UniV3TxParams::aa())
}

#[must_use]
pub fn uniswap_v3_aa_swap_tx(nonce: u64) -> TxEnv {
    uniswap_v3_swap_tx_with_params(nonce, &UniV3TxParams::aa())
}

fn insert_deployer_account(evm_state: &mut EvmState, deployer: Address) {
    evm_state.insert(
        deployer,
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
}

fn insert_bench_contract(evm_state: &mut EvmState, bench_contract: Address) {
    let bench_code = deployed_bytecode(UNI_V3_BENCH_ARTIFACT);
    let bench_code_hash = keccak256(&bench_code);
    evm_state.insert(
        bench_contract,
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
}

pub(crate) fn insert_uniswap_v3_accounts(evm_state: &mut EvmState) {
    insert_deployer_account(evm_state, UNI_V3_DEPLOYER_ACCOUNT);
    insert_bench_contract(evm_state, UNI_V3_BENCH_CONTRACT);

    // Predeploy two MockERC20 tokens and fund UniV3Bench (for callbacks)
    erc20::insert_mock_erc20_with_balance(
        evm_state,
        UNI_V3_TOKEN_A,
        UNI_V3_BENCH_CONTRACT,
        U256::MAX,
    );
    erc20::insert_mock_erc20_with_balance(
        evm_state,
        UNI_V3_TOKEN_B,
        UNI_V3_BENCH_CONTRACT,
        U256::MAX,
    );
}

pub(crate) fn insert_uniswap_v3_aa_accounts(evm_state: &mut EvmState) {
    insert_deployer_account(evm_state, UNI_V3_AA_DEPLOYER_ACCOUNT);
    insert_bench_contract(evm_state, UNI_V3_BENCH_AA_CONTRACT);

    // Predeploy two MockERC20 tokens and fund UniV3Bench AA (for callbacks)
    erc20::insert_mock_erc20_with_balance(
        evm_state,
        UNI_V3_AA_TOKEN_A,
        UNI_V3_BENCH_AA_CONTRACT,
        U256::MAX,
    );
    erc20::insert_mock_erc20_with_balance(
        evm_state,
        UNI_V3_AA_TOKEN_B,
        UNI_V3_BENCH_AA_CONTRACT,
        U256::MAX,
    );
}

fn deploy_factory_with_deployer(
    executor: &AssertionExecutor,
    db: &mut ForkDb<OverlayDb<InMemoryDB>>,
    deployer: Address,
    expected_factory: Address,
) {
    let factory_deploy_tx = TxEnv {
        caller: deployer,
        kind: TxKind::Create,
        data: bytecode(UNISWAP_V3_FACTORY_ARTIFACT),
        gas_limit: 15_000_000,
        gas_price: 1,
        nonce: 0,
        ..TxEnv::default()
    };

    let result = executor
        .execute_forked_tx_ext_db(&BlockEnv::default(), factory_deploy_tx, db)
        .expect("UniswapV3Factory deployment tx should succeed");

    let deployed_factory = match &result.result_and_state.result {
        ExecutionResult::Success { output, .. } => {
            match output {
                Output::Create(_, Some(address)) => *address,
                Output::Create(_, None) => {
                    panic!("Factory deployment did not return a created address")
                }
                Output::Call(_) => panic!("Factory deployment returned call output"),
            }
        }
        other => panic!("Factory deployment failed: {other:?}"),
    };

    assert_eq!(
        deployed_factory, expected_factory,
        "Factory deployed at unexpected address"
    );

    db.commit(result.result_and_state.state);
}

pub(crate) fn deploy_uniswap_v3_factory(
    executor: &AssertionExecutor,
    db: &mut ForkDb<OverlayDb<InMemoryDB>>,
) {
    deploy_factory_with_deployer(
        executor,
        db,
        UNI_V3_DEPLOYER_ACCOUNT,
        uniswap_v3_factory_address(),
    );
}

pub(crate) fn deploy_uniswap_v3_aa_factory(
    executor: &AssertionExecutor,
    db: &mut ForkDb<OverlayDb<InMemoryDB>>,
) {
    deploy_factory_with_deployer(
        executor,
        db,
        UNI_V3_AA_DEPLOYER_ACCOUNT,
        uniswap_v3_aa_factory_address(),
    );
}
