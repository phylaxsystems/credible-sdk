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
    inspectors::{
        phevm::{
            PhEvmContext,
            PhevmOutcome,
        },
        precompiles::{
            BASE_COST,
            COLD_SLOAD_COST,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm,
    },
    primitives::{
        Bytes,
        Journal,
        U256,
    },
};

use alloy_primitives::{
    Address,
    FixedBytes,
    I256,
};
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use revm::context::{
    ContextTr,
    JournalTr,
};

use super::call_facts::{
    call_matches_filter,
    call_type_to_scheme,
    candidate_call_indices,
};

#[derive(Debug, thiserror::Error)]
pub enum CallBoundaryError {
    #[error("Failed to decode call boundary input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("Call ID {call_id} is out of bounds (total calls: {total})")]
    CallIdOutOfBounds { call_id: U256, total: usize },
    #[error("Cannot fork to call {call_id}: call is inside a reverted subtree")]
    CallInsideRevertedSubtree { call_id: usize },
    #[error("MultiForkDb error during call boundary operation")]
    MultiForkError(#[source] MultiForkError),
    #[error("Database error loading account")]
    LoadAccountError,
}

const FORK_SWITCH_COST: u64 = 40; // PERSISTENT_WRITE (20) + SET_FORK (20)
const MEM_WRITE_COST: u64 = 3;
const EVM_WORD_BYTES: u64 = 32;

/// Helper to switch to a fork, pricing creation if needed.
fn switch_to_fork<ExtDb>(
    journal: &mut Journal<&mut MultiForkDb<ExtDb>>,
    fork_id: ForkId,
    tracer: &crate::inspectors::tracer::CallTracer,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<(), CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit,
{
    if !journal.database.fork_exists(&fork_id) {
        let bytes_written = journal
            .database
            .estimated_create_fork_bytes(fork_id, &journal.inner, tracer)
            .map_err(CallBoundaryError::MultiForkError)?;
        let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);
        if let Some(rax) = deduct_gas_and_check(gas_left, words_written * MEM_WRITE_COST, gas_limit)
        {
            return Err(CallBoundaryError::OutOfGas(rax));
        }
    }

    if let Some(rax) = deduct_gas_and_check(gas_left, FORK_SWITCH_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    journal
        .database
        .switch_fork(fork_id, &mut journal.inner, tracer)
        .map_err(CallBoundaryError::MultiForkError)?;

    Ok(())
}

#[inline]
fn resolve_forkable_call_id(
    ph_context: &PhEvmContext,
    call_id: U256,
) -> Result<usize, CallBoundaryError> {
    let tracer = ph_context.logs_and_traces.call_traces;
    let total = tracer.call_records().len();
    let call_id_usize: usize = call_id
        .try_into()
        .map_err(|_| CallBoundaryError::CallIdOutOfBounds { call_id, total })?;

    if !tracer.is_call_forkable(call_id_usize) {
        return Err(CallBoundaryError::CallInsideRevertedSubtree {
            call_id: call_id_usize,
        });
    }

    Ok(call_id_usize)
}

#[inline]
fn call_point_to_fork_id(call_id: usize, point: PhEvm::CallPoint) -> ForkId {
    let point_u8: u8 = point.into();
    if point_u8 == 0 {
        ForkId::PreCall(call_id)
    } else {
        ForkId::PostCall(call_id)
    }
}

/// Read a storage slot from the current fork state.
///
/// This mirrors `load` precompile semantics but stays local because call-boundary
/// operations need explicit fork-switch gas accounting and error mapping.
fn read_slot<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    target: alloy_primitives::Address,
    slot: alloy_primitives::B256,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<U256, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    evm_context
        .journal_mut()
        .load_account(target)
        .map_err(|_| CallBoundaryError::LoadAccountError)?;

    if let Some(rax) = deduct_gas_and_check(gas_left, COLD_SLOAD_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    Ok(evm_context
        .sload(target, slot.into())
        .unwrap_or_default()
        .data)
}

struct SlotDeltaQuery<'a> {
    target: Address,
    selector: FixedBytes<4>,
    slot: alloy_primitives::B256,
    filter: &'a PhEvm::CallFilter,
}

fn for_each_matching_slot_delta<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    query: SlotDeltaQuery<'_>,
    gas_left: &mut u64,
    gas_limit: u64,
    mut f: impl FnMut(I256) -> bool,
) -> Result<(), CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(query.filter.callType);

    if let Some(indices) = candidate_call_indices(tracer, query.target, query.selector) {
        for &idx in indices {
            let Some(depth) = tracer.call_depth_at(idx) else {
                continue;
            };
            if !call_matches_filter(&call_records[idx], depth, query.filter, scheme_filter) {
                continue;
            }

            let delta = slot_delta_for_call_id::<ExtDb, CTX>(
                evm_context,
                ph_context,
                query.target,
                query.slot,
                idx,
                gas_left,
                gas_limit,
            )?;
            if !f(delta) {
                break;
            }
        }
    }

    Ok(())
}

/// `loadAtCall(address target, bytes32 slot, uint256 callId, CallPoint point) -> bytes32`
///
/// Forks to the pre or post state of the specified call, reads the storage slot,
/// then restores the `PostTx` fork state. Returns the slot value.
///
/// `callId` should come from trigger context (per-call assertion execution) or
/// from prior call-query precompiles.
pub fn load_at_call<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    let call =
        PhEvm::loadAtCallCall::abi_decode(input_bytes).map_err(CallBoundaryError::DecodeError)?;

    let call_id = resolve_forkable_call_id(ph_context, call.callId)?;
    let tracer = ph_context.logs_and_traces.call_traces;
    let fork_id = call_point_to_fork_id(call_id, call.point);

    // Switch to target fork
    let journal = evm_context.journal_mut();
    switch_to_fork(journal, fork_id, tracer, &mut gas_left, gas_limit)?;

    // Read the slot
    let value = read_slot::<ExtDb, CTX>(
        evm_context,
        call.target,
        call.slot,
        &mut gas_left,
        gas_limit,
    )?;

    // Restore PostTx fork (the default state for assertion execution)
    let journal = evm_context.journal_mut();
    journal
        .database
        .switch_fork(ForkId::PostTx, &mut journal.inner, tracer)
        .map_err(CallBoundaryError::MultiForkError)?;

    let encoded = SolValue::abi_encode(&value).into();
    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

/// `slotDeltaAtCall(address target, bytes32 slot, uint256 callId) -> int256`
///
/// Reads the slot at pre and post call states, computes `int256(post) - int256(pre)`.
pub fn slot_delta_at_call<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    let call = PhEvm::slotDeltaAtCallCall::abi_decode(input_bytes)
        .map_err(CallBoundaryError::DecodeError)?;
    let call_id = resolve_forkable_call_id(ph_context, call.callId)?;
    let delta = slot_delta_for_call_id::<ExtDb, CTX>(
        evm_context,
        ph_context,
        call.target,
        call.slot,
        call_id,
        &mut gas_left,
        gas_limit,
    )?;
    let encoded: Bytes = delta.abi_encode().into();
    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

fn slot_delta_for_call_id<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    target: Address,
    slot: alloy_primitives::B256,
    call_id: usize,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<I256, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let tracer = ph_context.logs_and_traces.call_traces;
    if !tracer.is_call_forkable(call_id) {
        return Err(CallBoundaryError::CallInsideRevertedSubtree { call_id });
    }

    let pre_fork_id = ForkId::PreCall(call_id);
    let journal = evm_context.journal_mut();
    switch_to_fork(journal, pre_fork_id, tracer, gas_left, gas_limit)?;
    let pre_value = read_slot::<ExtDb, CTX>(evm_context, target, slot, gas_left, gas_limit)?;

    let post_fork_id = ForkId::PostCall(call_id);
    let journal = evm_context.journal_mut();
    switch_to_fork(journal, post_fork_id, tracer, gas_left, gas_limit)?;
    let post_value = read_slot::<ExtDb, CTX>(evm_context, target, slot, gas_left, gas_limit)?;

    let journal = evm_context.journal_mut();
    journal
        .database
        .switch_fork(ForkId::PostTx, &mut journal.inner, tracer)
        .map_err(CallBoundaryError::MultiForkError)?;

    Ok(I256::from_raw(post_value.wrapping_sub(pre_value)))
}

/// `allCallsSlotDeltaGE(address,bytes4,bytes32,int256,CallFilter) -> bool`
pub fn all_calls_slot_delta_ge<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    let call = PhEvm::allCallsSlotDeltaGECall::abi_decode(input_bytes)
        .map_err(CallBoundaryError::DecodeError)?;
    let mut ok = true;
    for_each_matching_slot_delta::<ExtDb, CTX>(
        evm_context,
        ph_context,
        SlotDeltaQuery {
            target: call.target,
            selector: call.selector,
            slot: call.slot,
            filter: &call.filter,
        },
        &mut gas_left,
        gas_limit,
        |delta| {
            if delta < call.minDelta {
                ok = false;
                return false;
            }
            true
        },
    )?;

    Ok(PhevmOutcome::new(
        ok.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `allCallsSlotDeltaLE(address,bytes4,bytes32,int256,CallFilter) -> bool`
pub fn all_calls_slot_delta_le<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    let call = PhEvm::allCallsSlotDeltaLECall::abi_decode(input_bytes)
        .map_err(CallBoundaryError::DecodeError)?;
    let mut ok = true;
    for_each_matching_slot_delta::<ExtDb, CTX>(
        evm_context,
        ph_context,
        SlotDeltaQuery {
            target: call.target,
            selector: call.selector,
            slot: call.slot,
            filter: &call.filter,
        },
        &mut gas_left,
        gas_limit,
        |delta| {
            if delta > call.maxDelta {
                ok = false;
                return false;
            }
            true
        },
    )?;

    Ok(PhevmOutcome::new(
        ok.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

/// `sumCallsSlotDelta(address,bytes4,bytes32,CallFilter) -> int256`
pub fn sum_calls_slot_delta<'db, ExtDb, CTX>(
    evm_context: &mut CTX,
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallBoundaryError>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallBoundaryError::OutOfGas(rax));
    }

    let call = PhEvm::sumCallsSlotDeltaCall::abi_decode(input_bytes)
        .map_err(CallBoundaryError::DecodeError)?;
    let mut total = I256::ZERO;
    for_each_matching_slot_delta::<ExtDb, CTX>(
        evm_context,
        ph_context,
        SlotDeltaQuery {
            target: call.target,
            selector: call.selector,
            slot: call.slot,
            filter: &call.filter,
        },
        &mut gas_left,
        gas_limit,
        |delta| {
            total = total.wrapping_add(delta);
            true
        },
    )?;

    Ok(PhevmOutcome::new(
        total.abi_encode().into(),
        gas_limit - gas_left,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            MultiForkDb,
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::{
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        primitives::{
            AccountInfo,
            SpecId,
        },
        test_utils::{
            random_address,
            random_u256,
        },
    };
    use alloy_primitives::{
        Address,
        B256,
        I256,
    };
    use alloy_sol_types::SolCall;
    use revm::{
        JournalEntry,
        context::{
            JournalInner,
            journaled_state::JournalCheckpoint,
        },
        handler::MainnetContext,
        primitives::KECCAK_EMPTY,
    };

    const TEST_GAS: u64 = 1_000_000;

    /// Creates a `MultiForkDb` with pre-tx and post-tx storage, plus a `CallTracer` with
    /// one call record whose checkpoint range covers the journal entries from the sstore.
    fn create_test_setup(
        address: Address,
        slot: U256,
        pre_value: U256,
        post_value: U256,
    ) -> (
        MultiForkDb<ForkDb<MockDb>>,
        JournalInner<JournalEntry>,
        CallTracer,
    ) {
        let mut pre_tx_db = MockDb::new();
        pre_tx_db.insert_storage(address, slot, pre_value);
        pre_tx_db.insert_account(
            address,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
            },
        );

        let mut journal_inner = JournalInner::new();
        journal_inner.load_account(&mut pre_tx_db, address).unwrap();
        journal_inner
            .sstore(&mut pre_tx_db, address, slot, post_value, false)
            .unwrap();
        journal_inner.touch(address);

        let multi_fork_db = MultiForkDb::new(ForkDb::new(pre_tx_db), &journal_inner);

        let mut call_tracer = CallTracer::default();
        call_tracer.insert_trace(address);
        // Pre-call checkpoint at journal_i=0 (before any sstore entries)
        // Post-call checkpoint at journal_i=end (after all sstore entries)
        call_tracer.set_last_call_checkpoints(
            JournalCheckpoint {
                log_i: 0,
                journal_i: 0,
            },
            Some(JournalCheckpoint {
                log_i: journal_inner.logs.len(),
                journal_i: journal_inner.journal.len(),
            }),
        );

        (multi_fork_db, journal_inner, call_tracer)
    }

    fn make_ph_context<'a>(
        logs_and_traces: &'a LogsAndTraces<'a>,
        tx_env: &'a crate::primitives::TxEnv,
    ) -> PhEvmContext<'a> {
        PhEvmContext::new(logs_and_traces, Address::ZERO, tx_env)
    }

    #[test]
    fn test_load_at_call_pre() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(200);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::loadAtCallCall {
            target: address,
            slot: B256::from(slot),
            callId: U256::ZERO,
            point: PhEvm::CallPoint::PreCall,
        };
        let encoded: Bytes = input.abi_encode().into();

        let result =
            load_at_call::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, pre_value, "PreCall should return pre-tx value");
    }

    #[test]
    fn test_load_at_call_post() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(200);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::loadAtCallCall {
            target: address,
            slot: B256::from(slot),
            callId: U256::ZERO,
            point: PhEvm::CallPoint::PostCall,
        };
        let encoded: Bytes = input.abi_encode().into();

        let result =
            load_at_call::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(
            decoded, post_value,
            "PostCall should return post-call value"
        );
    }

    #[test]
    fn test_load_at_call_invalid_id() {
        let address = random_address();
        let slot = random_u256();

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, U256::from(1), U256::from(2));

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        // Call ID 99 is out of bounds (only 1 call record at index 0)
        let input = PhEvm::loadAtCallCall {
            target: address,
            slot: B256::from(slot),
            callId: U256::from(99),
            point: PhEvm::CallPoint::PreCall,
        };
        let encoded: Bytes = input.abi_encode().into();

        let result = load_at_call::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS);
        assert!(
            matches!(
                result,
                Err(CallBoundaryError::CallInsideRevertedSubtree { .. })
            ),
            "Expected CallInsideRevertedSubtree for out-of-bounds call ID, got {result:?}"
        );
    }

    #[test]
    fn test_slot_delta_at_call() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(350);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::slotDeltaAtCallCall {
            target: address,
            slot: B256::from(slot),
            callId: U256::ZERO,
        };
        let encoded: Bytes = input.abi_encode().into();

        let result =
            slot_delta_at_call::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS)
                .unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        // delta = 350 - 100 = 250
        assert_eq!(decoded, I256::try_from(250i64).unwrap());
    }

    #[test]
    fn test_slot_delta_at_call_negative() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(500);
        let post_value = U256::from(200);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::slotDeltaAtCallCall {
            target: address,
            slot: B256::from(slot),
            callId: U256::ZERO,
        };
        let encoded: Bytes = input.abi_encode().into();

        let result =
            slot_delta_at_call::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS)
                .unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        // delta = 200 - 500 = -300
        assert_eq!(decoded, I256::try_from(-300i64).unwrap());
    }

    fn default_filter() -> PhEvm::CallFilter {
        PhEvm::CallFilter {
            callType: 0,
            minDepth: 0,
            maxDepth: 0,
            topLevelOnly: false,
            successOnly: false,
        }
    }

    #[test]
    fn test_all_calls_slot_delta_ge_true() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(350);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);
        let selector = call_tracer.call_records()[0].target_and_selector().selector;

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allCallsSlotDeltaGECall {
            target: address,
            selector,
            slot: B256::from(slot),
            minDelta: I256::try_from(200i64).unwrap(),
            filter: default_filter(),
        };
        let encoded: Bytes = input.abi_encode().into();
        let result =
            all_calls_slot_delta_ge::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS)
                .unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded);
    }

    #[test]
    fn test_all_calls_slot_delta_le_false() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(350);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);
        let selector = call_tracer.call_records()[0].target_and_selector().selector;

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allCallsSlotDeltaLECall {
            target: address,
            selector,
            slot: B256::from(slot),
            maxDelta: I256::try_from(200i64).unwrap(),
            filter: default_filter(),
        };
        let encoded: Bytes = input.abi_encode().into();
        let result =
            all_calls_slot_delta_le::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS)
                .unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded);
    }

    #[test]
    fn test_sum_calls_slot_delta() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(350);

        let (mut multi_fork_db, test_journal, call_tracer) =
            create_test_setup(address, slot, pre_value, post_value);
        let selector = call_tracer.call_records()[0].target_and_selector().selector;

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|journal| {
            journal.inner = test_journal;
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let ph_ctx = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumCallsSlotDeltaCall {
            target: address,
            selector,
            slot: B256::from(slot),
            filter: default_filter(),
        };
        let encoded: Bytes = input.abi_encode().into();
        let result =
            sum_calls_slot_delta::<ForkDb<MockDb>, _>(&mut context, &ph_ctx, &encoded, TEST_GAS)
                .unwrap();
        let decoded = I256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, I256::try_from(250i64).unwrap());
    }
}
