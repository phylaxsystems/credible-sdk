use crate::inspectors::{
    phevm::{
        PhEvmContext,
        PhevmOutcome,
    },
    precompiles::{
        BASE_COST,
        deduct_gas_and_check,
    },
    sol_primitives::PhEvm,
};

use alloy_primitives::{
    Address,
    B256,
    I256,
    U256,
};
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use revm::primitives::Log;

#[derive(thiserror::Error, Debug)]
pub enum Erc20FactsError {
    #[error("Failed to decode ERC20 facts input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("Call ID {call_id} is out of bounds (total calls: {total})")]
    CallIdOutOfBounds { call_id: U256, total: usize },
}

/// ERC20 Transfer event topic0: keccak256("Transfer(address,address,uint256)")
const TRANSFER_TOPIC: B256 =
    alloy_primitives::b256!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

const PER_LOG_COST: u64 = 3;

/// Extract a Transfer event's (from, to, value) if the log matches the token address
/// and has the correct Transfer signature.
fn decode_transfer(log: &Log, token: Address) -> Option<(Address, Address, U256)> {
    if log.address != token {
        return None;
    }
    let topics = &log.data.topics();
    if topics.len() < 3 || topics[0] != TRANSFER_TOPIC {
        return None;
    }
    let from = Address::from_slice(&topics[1][12..]);
    let to = Address::from_slice(&topics[2][12..]);
    let value = U256::from_be_slice(&log.data.data);
    Some((from, to, value))
}

/// Compute net flow for an account from a set of Transfer events.
/// Returns sum of incoming transfers minus sum of outgoing transfers.
fn compute_net_flow(logs: &[Log], token: Address, account: Address) -> I256 {
    let mut net = I256::ZERO;
    for log in logs {
        if let Some((from, to, value)) = decode_transfer(log, token) {
            if to == account {
                net = net.wrapping_add(I256::from_raw(value));
            }
            if from == account {
                net = net.wrapping_sub(I256::from_raw(value));
            }
        }
    }
    net
}

/// `erc20BalanceDiff(address token, address account) -> int256`
///
/// Computes the ERC20 balance change for an account by scanning Transfer events.
/// For standard ERC20 tokens, this equals `balanceOf(account)` post-tx minus pre-tx.
///
/// NOTE: Uses Transfer event scanning rather than sub-EVM `balanceOf()` calls because
/// precompiles hold `&mut CTX` which borrows the MultiForkDb, preventing nested EVM
/// creation. This is accurate for standard ERC20 tokens but will NOT capture balance
/// changes from rebasing tokens, fee-on-transfer tokens, or tokens that modify
/// balances without emitting Transfer events.
pub fn erc20_balance_diff(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call = PhEvm::erc20BalanceDiffCall::abi_decode(input_bytes)
        .map_err(Erc20FactsError::DecodeError)?;

    let logs = ph_context.logs_and_traces.tx_logs;

    // Charge gas for log scanning
    let log_cost = (logs.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let delta = compute_net_flow(logs, call.token, call.account);
    let encoded = delta.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `erc20SupplyDiff(address token) -> int256`
///
/// Computes the ERC20 total supply change by scanning Transfer events.
/// Same architectural note as `erc20_balance_diff` — uses event scanning.
/// Mints (from address(0)) increase supply, burns (to address(0)) decrease it.
pub fn erc20_supply_diff(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call =
        PhEvm::erc20SupplyDiffCall::abi_decode(input_bytes).map_err(Erc20FactsError::DecodeError)?;

    let logs = ph_context.logs_and_traces.tx_logs;

    let log_cost = (logs.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    // Supply change = mints - burns
    // Mint: Transfer from address(0) → increases supply
    // Burn: Transfer to address(0) → decreases supply
    let mut delta = I256::ZERO;
    for log in logs {
        if let Some((from, to, value)) = decode_transfer(log, call.token) {
            if from == Address::ZERO {
                // Mint
                delta = delta.wrapping_add(I256::from_raw(value));
            }
            if to == Address::ZERO {
                // Burn
                delta = delta.wrapping_sub(I256::from_raw(value));
            }
        }
    }

    let encoded = delta.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `getERC20NetFlow(address token, address account) -> int256`
///
/// Computes the net ERC20 token flow for an account from all Transfer events
/// in the transaction. Positive means net received, negative means net sent.
pub fn get_erc20_net_flow(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call = PhEvm::getERC20NetFlowCall::abi_decode(input_bytes)
        .map_err(Erc20FactsError::DecodeError)?;

    let logs = ph_context.logs_and_traces.tx_logs;

    let log_cost = (logs.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let net_flow = compute_net_flow(logs, call.token, call.account);
    let encoded = net_flow.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `getERC20FlowByCall(address token, address account, uint256 callId) -> int256`
///
/// Computes the net ERC20 token flow for an account from Transfer events
/// that occurred within the scope of a specific call.
pub fn get_erc20_flow_by_call(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call = PhEvm::getERC20FlowByCallCall::abi_decode(input_bytes)
        .map_err(Erc20FactsError::DecodeError)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_id_usize: usize = call
        .callId
        .try_into()
        .map_err(|_| Erc20FactsError::CallIdOutOfBounds {
            call_id: call.callId,
            total: tracer.call_records().len(),
        })?;

    if call_id_usize >= tracer.call_records().len() {
        return Err(Erc20FactsError::CallIdOutOfBounds {
            call_id: call.callId,
            total: tracer.call_records().len(),
        });
    }

    let pre_checkpoint = tracer.get_pre_call_checkpoint(call_id_usize);
    let post_checkpoint = tracer.get_post_call_checkpoint(call_id_usize);

    // Filter tx_logs to those within the call's log checkpoint range
    let logs = ph_context.logs_and_traces.tx_logs;
    let start_log_i = pre_checkpoint
        .map(|c| c.log_i)
        .unwrap_or(0)
        .min(logs.len());
    let end_log_i = post_checkpoint
        .map(|c| c.log_i)
        .unwrap_or(logs.len())
        .min(logs.len());
    let scoped_len = end_log_i.saturating_sub(start_log_i);

    let log_cost = (scoped_len as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let scoped_logs = &logs[start_log_i..start_log_i + scoped_len];
    let net_flow = compute_net_flow(scoped_logs, call.token, call.account);
    let encoded = net_flow.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `didBalanceChange(address token, address account) -> bool`
///
/// Returns true if the ERC20 balance changed for an account,
/// based on Transfer event scanning.
pub fn did_balance_change(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call = PhEvm::didBalanceChangeCall::abi_decode(input_bytes)
        .map_err(Erc20FactsError::DecodeError)?;

    let logs = ph_context.logs_and_traces.tx_logs;

    let log_cost = (logs.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let delta = compute_net_flow(logs, call.token, call.account);
    let changed = delta != I256::ZERO;
    let encoded = changed.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `balanceDiff(address token, address account) -> int256`
///
/// Returns the ERC20 balance change for an account.
/// Equivalent to `erc20BalanceDiff` — computes net flow from Transfer events.
pub fn balance_diff(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, Erc20FactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let call = PhEvm::balanceDiffCall::abi_decode(input_bytes)
        .map_err(Erc20FactsError::DecodeError)?;

    let logs = ph_context.logs_and_traces.tx_logs;

    let log_cost = (logs.len() as u64).saturating_mul(PER_LOG_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, log_cost, gas_limit) {
        return Err(Erc20FactsError::OutOfGas(rax));
    }

    let delta = compute_net_flow(logs, call.token, call.account);
    let encoded = delta.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::inspectors::{
        phevm::{
            LogsAndTraces,
            PhEvmContext,
        },
        tracer::CallTracer,
    };
    use alloy_primitives::{
        Bytes,
        LogData,
        address,
    };
    use revm::{
        context::JournalInner,
        interpreter::{
            CallInput,
            CallInputs,
            CallScheme,
            CallValue,
        },
    };

    fn make_ph_context<'a>(
        logs_and_traces: &'a LogsAndTraces<'a>,
        tx_env: &'a crate::primitives::TxEnv,
    ) -> PhEvmContext<'a> {
        PhEvmContext::new(logs_and_traces, Address::ZERO, tx_env)
    }

    fn make_transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        let mut from_topic = B256::ZERO;
        from_topic[12..].copy_from_slice(from.as_slice());
        let mut to_topic = B256::ZERO;
        to_topic[12..].copy_from_slice(to.as_slice());

        Log {
            address: token,
            data: LogData::new(
                vec![TRANSFER_TOPIC, from_topic, to_topic],
                value.to_be_bytes::<32>().to_vec().into(),
            )
            .unwrap(),
        }
    }

    #[test]
    fn test_erc20_net_flow_single_transfer() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let sender = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let receiver = address!("cccccccccccccccccccccccccccccccccccc0001");

        let logs = vec![make_transfer_log(token, sender, receiver, U256::from(100))];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        // Check receiver's net flow (should be +100)
        let input = PhEvm::getERC20NetFlowCall {
            token,
            account: receiver,
        };
        let encoded = input.abi_encode();
        let result = get_erc20_net_flow(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(100i64).unwrap());

        // Check sender's net flow (should be -100)
        let input = PhEvm::getERC20NetFlowCall {
            token,
            account: sender,
        };
        let encoded = input.abi_encode();
        let result = get_erc20_net_flow(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(-100i64).unwrap());
    }

    #[test]
    fn test_erc20_net_flow_multiple_transfers() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let alice = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let bob = address!("cccccccccccccccccccccccccccccccccccc0001");

        let logs = vec![
            make_transfer_log(token, alice, bob, U256::from(50)),
            make_transfer_log(token, bob, alice, U256::from(30)),
        ];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        // Alice: -50 + 30 = -20
        let input = PhEvm::getERC20NetFlowCall {
            token,
            account: alice,
        };
        let encoded = input.abi_encode();
        let result = get_erc20_net_flow(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(-20i64).unwrap());
    }

    #[test]
    fn test_erc20_supply_diff_mint() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let receiver = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");

        // Mint: from address(0)
        let logs = vec![make_transfer_log(
            token,
            Address::ZERO,
            receiver,
            U256::from(1000),
        )];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::erc20SupplyDiffCall { token };
        let encoded = input.abi_encode();
        let result = erc20_supply_diff(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(1000i64).unwrap());
    }

    #[test]
    fn test_erc20_supply_diff_burn() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let sender = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");

        // Burn: to address(0)
        let logs = vec![make_transfer_log(
            token,
            sender,
            Address::ZERO,
            U256::from(500),
        )];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::erc20SupplyDiffCall { token };
        let encoded = input.abi_encode();
        let result = erc20_supply_diff(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(-500i64).unwrap());
    }

    #[test]
    fn test_erc20_balance_diff() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let other = address!("cccccccccccccccccccccccccccccccccccc0001");

        let logs = vec![
            make_transfer_log(token, other, account, U256::from(200)),
            make_transfer_log(token, account, other, U256::from(50)),
        ];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::erc20BalanceDiffCall { token, account };
        let encoded = input.abi_encode();
        let result = erc20_balance_diff(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        // 200 - 50 = 150
        assert_eq!(decoded, I256::try_from(150i64).unwrap());
    }

    #[test]
    fn test_erc20_flow_by_call() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let other = address!("cccccccccccccccccccccccccccccccccccc0001");
        let target = address!("dddddddddddddddddddddddddddddddddddd0001");
        let selector = alloy_primitives::FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        // Create logs: one transfer before the call, one during, one after
        let logs = vec![
            make_transfer_log(token, other, account, U256::from(10)),  // before call
            make_transfer_log(token, other, account, U256::from(100)), // during call
            make_transfer_log(token, other, account, U256::from(5)),   // after call
        ];

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let input_bytes: Bytes = selector.into();
        let inputs = CallInputs {
            input: CallInput::Bytes(input_bytes.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: target,
            known_bytecode: None,
            target_address: target,
            caller: account,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(inputs, &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Set checkpoints: call spans log indices 1..2
        tracer.set_last_call_checkpoints(
            revm::context::journaled_state::JournalCheckpoint {
                log_i: 1,
                journal_i: 0,
            },
            Some(revm::context::journaled_state::JournalCheckpoint {
                log_i: 2,
                journal_i: 0,
            }),
        );

        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::getERC20FlowByCallCall {
            token,
            account,
            callId: U256::ZERO,
        };
        let encoded = input.abi_encode();
        let result = get_erc20_flow_by_call(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        // Only the transfer during the call (index 1): +100
        assert_eq!(decoded, I256::try_from(100i64).unwrap());
    }

    #[test]
    fn test_erc20_flow_by_call_invalid_id() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::getERC20FlowByCallCall {
            token,
            account,
            callId: U256::from(99),
        };
        let encoded = input.abi_encode();
        let result = get_erc20_flow_by_call(&context, &encoded, 1_000_000);
        assert!(
            matches!(result, Err(Erc20FactsError::CallIdOutOfBounds { .. })),
            "Expected CallIdOutOfBounds, got {result:?}"
        );
    }

    #[test]
    fn test_did_balance_change_true() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let other = address!("cccccccccccccccccccccccccccccccccccc0001");

        let logs = vec![make_transfer_log(token, other, account, U256::from(100))];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::didBalanceChangeCall { token, account };
        let encoded = input.abi_encode();
        let result = did_balance_change(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded, "Balance should be reported as changed");
    }

    #[test]
    fn test_did_balance_change_false() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");

        // No Transfer events
        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::didBalanceChangeCall { token, account };
        let encoded = input.abi_encode();
        let result = did_balance_change(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Balance should not be reported as changed");
    }

    #[test]
    fn test_did_balance_change_net_zero() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let other = address!("cccccccccccccccccccccccccccccccccccc0001");

        // Send and receive same amount — net zero
        let logs = vec![
            make_transfer_log(token, other, account, U256::from(100)),
            make_transfer_log(token, account, other, U256::from(100)),
        ];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::didBalanceChangeCall { token, account };
        let encoded = input.abi_encode();
        let result = did_balance_change(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Net-zero balance change should return false");
    }

    #[test]
    fn test_balance_diff() {
        let token = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0001");
        let account = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb0001");
        let other = address!("cccccccccccccccccccccccccccccccccccc0001");

        let logs = vec![
            make_transfer_log(token, other, account, U256::from(200)),
            make_transfer_log(token, account, other, U256::from(50)),
        ];

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::balanceDiffCall { token, account };
        let encoded = input.abi_encode();
        let result = balance_diff(&context, &encoded, 1_000_000).unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(150i64).unwrap());
    }
}
