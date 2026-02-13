#![allow(clippy::missing_errors_doc)]

use crate::{
    db::{
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::{
            ForkId,
            MultiForkDb,
            MultiForkError,
        },
    },
    evm::build_evm::evm_env,
    inspectors::{
        phevm::{
            PhEvmContext,
            PhevmOutcome,
        },
        precompiles::{
            BASE_COST,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm,
    },
    primitives::{
        Address,
        BlockEnv,
        Bytes,
        Journal,
        SpecId,
        TxEnv,
        TxKind,
        U256,
    },
};

use alloy_primitives::I256;
use alloy_sol_types::{
    SolCall,
    SolValue,
    sol,
};
use revm::{
    ExecuteEvm,
    context::ContextTr,
    inspector::NoOpInspector,
};

sol! {
    interface IERC20ViewFacts {
        function balanceOf(address account) external view returns (uint256);
        function totalSupply() external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    interface IERC4626ViewFacts {
        function asset() external view returns (address);
        function totalAssets() external view returns (uint256);
        function totalSupply() external view returns (uint256);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Erc4626FactsError {
    #[error("Failed to decode ERC4626 facts input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Failed to decode ERC4626 view return data: {0:?}")]
    ReturnDecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("MultiForkDb error during ERC4626 view call")]
    MultiForkError(#[source] MultiForkError),
    #[error("ERC4626 view call execution failed: {0}")]
    ViewCallExecution(String),
    #[error("ERC4626 view call failed for target {target:?} at fork {fork_id:?}")]
    ViewCallFailed { target: Address, fork_id: ForkId },
    #[error("Vault asset changed across tx for vault {vault:?}: pre={pre:?}, post={post:?}")]
    VaultAssetChanged {
        vault: Address,
        pre: Address,
        post: Address,
    },
}

const MAX_VIEW_CALL_GAS: u64 = 200_000;

fn signed_diff(post: U256, pre: U256) -> I256 {
    I256::from_raw(post).wrapping_sub(I256::from_raw(pre))
}

fn tx_point_to_fork(point: PhEvm::TxPoint) -> ForkId {
    let point_u8: u8 = point.into();
    if point_u8 == 0 {
        ForkId::PreTx
    } else {
        ForkId::PostTx
    }
}

fn assets_per_share_bps(total_assets: U256, total_supply: U256) -> U256 {
    if total_supply.is_zero() {
        return U256::ZERO;
    }

    total_assets
        .wrapping_mul(U256::from(10_000u64))
        .wrapping_div(total_supply)
}

fn execute_view_call_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    target: Address,
    calldata: Bytes,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<Bytes, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let chain_id = ph_context.original_tx_env.chain_id.unwrap_or(1);

    let (mut db_clone, mut active_journal) = {
        let journal = evm_context.journal_mut();
        (journal.database.clone(), journal.inner.clone())
    };

    db_clone
        .switch_fork(
            fork_id,
            &mut active_journal,
            ph_context.logs_and_traces.call_traces,
        )
        .map_err(Erc4626FactsError::MultiForkError)?;

    let env = evm_env(chain_id, SpecId::default(), BlockEnv::default());
    let tx_gas_limit = (*gas_left).clamp(BASE_COST, MAX_VIEW_CALL_GAS);
    let tx_env = TxEnv {
        kind: TxKind::Call(target),
        caller: ph_context.adopter,
        data: calldata,
        gas_limit: tx_gas_limit,
        chain_id: Some(chain_id),
        ..Default::default()
    };

    let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);
    let mut evm = crate::build_evm_by_features!(&mut db_clone, &env, NoOpInspector);
    let result_and_state = evm
        .transact(tx_env)
        .map_err(|err| Erc4626FactsError::ViewCallExecution(format!("{err:?}")))?;

    let gas_used = result_and_state.result.gas_used();
    if let Some(rax) = deduct_gas_and_check(gas_left, gas_used, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    if !result_and_state.result.is_success() {
        return Err(Erc4626FactsError::ViewCallFailed { target, fork_id });
    }

    Ok(result_and_state.result.into_output().unwrap_or_default())
}

fn read_erc20_balance_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    token: Address,
    account: Address,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<U256, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = IERC20ViewFacts::balanceOfCall { account };
    let output = execute_view_call_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        fork_id,
        token,
        call.abi_encode().into(),
        gas_left,
        gas_limit,
    )?;

    let decoded = IERC20ViewFacts::balanceOfCall::abi_decode_returns(output.as_ref())
        .map_err(Erc4626FactsError::ReturnDecodeError)?;

    Ok(decoded)
}

fn read_erc20_supply_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    token: Address,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<U256, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = IERC20ViewFacts::totalSupplyCall {};
    let output = execute_view_call_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        fork_id,
        token,
        call.abi_encode().into(),
        gas_left,
        gas_limit,
    )?;

    let decoded = IERC20ViewFacts::totalSupplyCall::abi_decode_returns(output.as_ref())
        .map_err(Erc4626FactsError::ReturnDecodeError)?;

    Ok(decoded)
}

fn read_erc20_allowance_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    token: Address,
    owner: Address,
    spender: Address,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<U256, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = IERC20ViewFacts::allowanceCall { owner, spender };
    let output = execute_view_call_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        fork_id,
        token,
        call.abi_encode().into(),
        gas_left,
        gas_limit,
    )?;

    let decoded = IERC20ViewFacts::allowanceCall::abi_decode_returns(output.as_ref())
        .map_err(Erc4626FactsError::ReturnDecodeError)?;

    Ok(decoded)
}

fn read_vault_asset_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    vault: Address,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<Address, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = IERC4626ViewFacts::assetCall {};
    let output = execute_view_call_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        fork_id,
        vault,
        call.abi_encode().into(),
        gas_left,
        gas_limit,
    )?;

    let decoded = IERC4626ViewFacts::assetCall::abi_decode_returns(output.as_ref())
        .map_err(Erc4626FactsError::ReturnDecodeError)?;

    Ok(decoded)
}

fn read_vault_total_assets_at_fork<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    fork_id: ForkId,
    vault: Address,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<U256, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = IERC4626ViewFacts::totalAssetsCall {};
    let output = execute_view_call_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        fork_id,
        vault,
        call.abi_encode().into(),
        gas_left,
        gas_limit,
    )?;

    let decoded = IERC4626ViewFacts::totalAssetsCall::abi_decode_returns(output.as_ref())
        .map_err(Erc4626FactsError::ReturnDecodeError)?;

    Ok(decoded)
}

/// `erc4626TotalAssetsDiff(address vault) -> int256`
pub fn erc4626_total_assets_diff<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc4626TotalAssetsDiffCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;

    let pre = read_vault_total_assets_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let post = read_vault_total_assets_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let delta = signed_diff(post, pre);
    Ok(PhevmOutcome::new(
        delta.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc4626TotalSupplyDiff(address vault) -> int256`
pub fn erc4626_total_supply_diff<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc4626TotalSupplyDiffCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;

    let pre = read_erc20_supply_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let post = read_erc20_supply_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let delta = signed_diff(post, pre);
    Ok(PhevmOutcome::new(
        delta.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc4626VaultAssetBalanceDiff(address vault) -> int256`
pub fn erc4626_vault_asset_balance_diff<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc4626VaultAssetBalanceDiffCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;

    let pre_asset = read_vault_asset_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;
    let post_asset = read_vault_asset_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    if pre_asset != post_asset {
        return Err(Erc4626FactsError::VaultAssetChanged {
            vault: call.vault,
            pre: pre_asset,
            post: post_asset,
        });
    }

    let pre = read_erc20_balance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        post_asset,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let post = read_erc20_balance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        post_asset,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let delta = signed_diff(post, pre);
    Ok(PhevmOutcome::new(
        delta.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc4626AssetsPerShareDiffBps(address vault) -> int256`
pub fn erc4626_assets_per_share_diff_bps<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc4626AssetsPerShareDiffBpsCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;

    let pre_assets = read_vault_total_assets_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let post_assets = read_vault_total_assets_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let pre_supply = read_erc20_supply_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let post_supply = read_erc20_supply_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.vault,
        &mut gas_left,
        gas_limit,
    )?;

    let pre_bps = assets_per_share_bps(pre_assets, pre_supply);
    let post_bps = assets_per_share_bps(post_assets, post_supply);
    let delta_bps = signed_diff(post_bps, pre_bps);

    Ok(PhevmOutcome::new(
        delta_bps.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc20BalanceAt(address token, address account, TxPoint point) -> uint256`
pub fn erc20_balance_at<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc20BalanceAtCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;
    let balance = read_erc20_balance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        tx_point_to_fork(call.point),
        call.token,
        call.account,
        &mut gas_left,
        gas_limit,
    )?;

    Ok(PhevmOutcome::new(
        balance.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc20SupplyAt(address token, TxPoint point) -> uint256`
pub fn erc20_supply_at<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc20SupplyAtCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;
    let supply = read_erc20_supply_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        tx_point_to_fork(call.point),
        call.token,
        &mut gas_left,
        gas_limit,
    )?;

    Ok(PhevmOutcome::new(
        supply.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc20AllowanceAt(address token, address owner, address spender, TxPoint point) -> uint256`
pub fn erc20_allowance_at<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc20AllowanceAtCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;
    let allowance_ = read_erc20_allowance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        tx_point_to_fork(call.point),
        call.token,
        call.owner,
        call.spender,
        &mut gas_left,
        gas_limit,
    )?;

    Ok(PhevmOutcome::new(
        allowance_.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `erc20AllowanceDiff(address token, address owner, address spender) -> (int256, uint256, uint256)`
pub fn erc20_allowance_diff<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc4626FactsError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc4626FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc20AllowanceDiffCall::abi_decode(input_bytes)
        .map_err(Erc4626FactsError::DecodeError)?;
    let pre = read_erc20_allowance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PreTx,
        call.token,
        call.owner,
        call.spender,
        &mut gas_left,
        gas_limit,
    )?;
    let post = read_erc20_allowance_at_fork::<ExtDb, CTX>(
        evm_context,
        ph_context,
        ForkId::PostTx,
        call.token,
        call.owner,
        call.spender,
        &mut gas_left,
        gas_limit,
    )?;
    let diff = signed_diff(post, pre);

    Ok(PhevmOutcome::new(
        (diff, pre, post).abi_encode().into(),
        gas_limit - gas_left,
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assets_per_share_bps_zero_supply() {
        assert_eq!(
            assets_per_share_bps(U256::from(123u64), U256::ZERO),
            U256::ZERO
        );
    }

    #[test]
    fn test_assets_per_share_bps_computes_floor_ratio() {
        let bps = assets_per_share_bps(U256::from(250u64), U256::from(100u64));
        assert_eq!(bps, U256::from(25_000u64));
    }

    #[test]
    fn test_signed_diff_positive_and_negative() {
        let up = signed_diff(U256::from(20u64), U256::from(5u64));
        let down = signed_diff(U256::from(5u64), U256::from(20u64));

        assert_eq!(up, I256::try_from(15i64).unwrap());
        assert_eq!(down, I256::try_from(-15i64).unwrap());
    }

    #[test]
    fn test_tx_point_to_fork_mapping() {
        assert_eq!(tx_point_to_fork(PhEvm::TxPoint::PreTx), ForkId::PreTx);
        assert_eq!(tx_point_to_fork(PhEvm::TxPoint::PostTx), ForkId::PostTx);
    }
}
