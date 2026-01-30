use std::collections::HashMap;

use assertion_executor::{
    primitives::{
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        Bytecode,
        Bytes,
        EvmState,
        EvmStorageSlot,
        TxEnv,
        TxKind,
        U256,
        address,
        keccak256,
    },
    test_utils::deployed_bytecode,
};

use crate::BENCH_ACCOUNT;

/// ERC20 token contract address (non-AA)
pub const ERC20_CONTRACT: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");

/// ERC20 token contract address (AA variant - registered in assertion store)
pub const ERC20_AA_CONTRACT: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeF");

/// `ERC20Mock` artifact path
pub(crate) const MOCK_ERC20_ARTIFACT: &str = "ERC20Mock.sol:ERC20Mock";

/// ERC20 transfer function selector: transfer(address,uint256)
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

/// Create an ERC20 transfer transaction targeting the specified contract
pub fn erc20_transfer_tx_to_contract(contract: Address, to: Address, amount: U256) -> TxEnv {
    // Encode: transfer(address,uint256)
    // selector (4 bytes) + address (32 bytes, left-padded) + amount (32 bytes)
    let mut data = Vec::with_capacity(68);
    data.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    data.extend_from_slice(&[0u8; 12]); // left-pad address to 32 bytes
    data.extend_from_slice(to.as_slice());
    data.extend_from_slice(&amount.to_be_bytes::<32>());

    TxEnv {
        caller: BENCH_ACCOUNT,
        kind: TxKind::Call(contract),
        value: U256::ZERO,
        gas_limit: 60_000,
        gas_price: 1,
        data: Bytes::from(data),
        ..TxEnv::default()
    }
}

/// Create an ERC20 transfer transaction (non-AA contract)
#[must_use]
pub fn erc20_transfer_tx(to: Address, amount: U256) -> TxEnv {
    erc20_transfer_tx_to_contract(ERC20_CONTRACT, to, amount)
}

/// Create an ERC20 transfer transaction (AA contract)
#[must_use]
pub fn erc20_aa_transfer_tx(to: Address, amount: U256) -> TxEnv {
    erc20_transfer_tx_to_contract(ERC20_AA_CONTRACT, to, amount)
}

/// Calculate the storage slot for an ERC20 balance.
///
/// `OpenZeppelin's` ERC20 has `_balances` mapping at slot 0.
/// See <https://github.com/OpenZeppelin/openzeppelin-contracts/blob/dde766bd542e4a1695fe8e4a07dc03b77305f367/contracts/token/ERC20/ERC20.sol#L30>
pub(crate) fn erc20_balance_slot(owner: Address) -> U256 {
    // keccak256(abi.encode(owner, 0))
    let mut key = [0u8; 64];
    key[12..32].copy_from_slice(owner.as_slice()); // address left-padded to 32 bytes
    U256::from_be_bytes(keccak256(key).0)
}

pub(crate) fn mock_erc20_deployed_bytecode() -> Bytes {
    deployed_bytecode(MOCK_ERC20_ARTIFACT)
}

pub(crate) fn insert_mock_erc20_with_balance(
    evm_state: &mut EvmState,
    token_address: Address,
    holder: Address,
    balance: U256,
) {
    let code = mock_erc20_deployed_bytecode();
    let code_hash = keccak256(&code);

    // Storage setup for OpenZeppelin ERC20:
    // - Slot 0: _balances mapping
    let mut storage = HashMap::default();

    // Set holder balance
    let balance_slot = erc20_balance_slot(holder);
    storage.insert(balance_slot, EvmStorageSlot::new(balance, 0));

    evm_state.insert(
        token_address,
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

pub(crate) fn insert_benchmark_erc20(evm_state: &mut EvmState) {
    insert_mock_erc20_with_balance(evm_state, ERC20_CONTRACT, BENCH_ACCOUNT, U256::MAX);
}

pub(crate) fn insert_benchmark_erc20_aa(evm_state: &mut EvmState) {
    insert_mock_erc20_with_balance(evm_state, ERC20_AA_CONTRACT, BENCH_ACCOUNT, U256::MAX);
}
