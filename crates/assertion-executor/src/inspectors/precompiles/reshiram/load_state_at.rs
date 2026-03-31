use crate::{
    db::{
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::{
            ForkId,
            MultiForkDb,
            MultiForkError,
            StorageRefFromForkError,
        },
    },
    inspectors::{
        CallTracer,
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::{
            COLD_SLOAD_COST,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm::{
            ForkId as SolForkId,
            loadStateAt_0Call,
            loadStateAt_1Call,
        },
    },
    primitives::{
        Address,
        Bytes,
        U256,
    },
};

use alloy_primitives::FixedBytes;
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use revm::context::{
    ContextTr,
    Journal,
};

use super::BASE_COST as RESHIRAM_BASE_COST;

const MEM_WRITE_COST: u64 = 3;
const EVM_WORD_BYTES: u64 = 32;

#[derive(Debug, thiserror::Error)]
pub enum LoadStateAtError<ExtDb: DatabaseRef> {
    #[error("Error decoding loadStateAt call: {0}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Invalid fork type: {fork_type}")]
    InvalidForkType { fork_type: u8 },
    #[error("Call ID {call_id} is too large to be a valid index")]
    CallIdOverflow { call_id: U256 },
    #[error("Cannot load state at call {call_id}: call is inside a reverted subtree")]
    CallInsideRevertedSubtree { call_id: usize },
    #[error("Error materializing snapshot fork: {0}")]
    MultiForkDb(#[source] MultiForkError),
    #[error("Error reading snapshot storage: {0}")]
    StorageRefFromFork(#[source] StorageRefFromForkError<ExtDb>),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

fn decode_fork_id<ExtDb: DatabaseRef>(
    fork: SolForkId,
    call_tracer: &CallTracer,
) -> Result<ForkId, LoadStateAtError<ExtDb>> {
    match fork.forkType {
        0 => Ok(ForkId::PreTx),
        1 => Ok(ForkId::PostTx),
        2 | 3 => {
            let call_id = fork.callIndex;
            let call_id_usize = call_id
                .try_into()
                .map_err(|_| LoadStateAtError::CallIdOverflow { call_id })?;

            if !call_tracer.is_call_forkable(call_id_usize) {
                return Err(LoadStateAtError::CallInsideRevertedSubtree {
                    call_id: call_id_usize,
                });
            }

            if fork.forkType == 2 {
                Ok(ForkId::PreCall(call_id_usize))
            } else {
                Ok(ForkId::PostCall(call_id_usize))
            }
        }
        fork_type => Err(LoadStateAtError::InvalidForkType { fork_type }),
    }
}

fn price_fork_creation_if_needed<ExtDb>(
    journal: &mut Journal<&mut MultiForkDb<ExtDb>>,
    fork_id: ForkId,
    call_tracer: &CallTracer,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<(), LoadStateAtError<ExtDb>>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit,
{
    if journal.database.fork_exists(&fork_id) {
        return Ok(());
    }

    let bytes_written = journal
        .database
        .estimated_create_fork_bytes(fork_id, &journal.inner, call_tracer)
        .map_err(LoadStateAtError::MultiForkDb)?;
    let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);

    if let Some(rax) = deduct_gas_and_check(gas_left, words_written * MEM_WRITE_COST, gas_limit) {
        return Err(LoadStateAtError::OutOfGas(rax));
    }

    Ok(())
}

/// Reads `slot` from `target` at the immutable transaction snapshot identified by `fork_id`.
///
/// This is the shared implementation behind both `loadStateAt` overloads. Callers are expected to
/// decode the ABI input first, resolve the user-facing `SolForkId` into the internal [`ForkId`],
/// and then delegate here with the final `target`.
///
/// Unlike the legacy fork precompiles, this helper never switches the active assertion execution
/// context. It lazily materializes the requested snapshot in [`MultiForkDb`] when needed, prices
/// that creation using the same byte-based estimate as legacy fork creation, and then performs a
/// read-only storage lookup against the snapshot fork's backing database. Any journaled writes made
/// by the currently running assertion stay local to the active execution context and are not
/// surfaced through this read path.
///
/// Gas accounting is:
/// - `BASE_COST` for selector dispatch
/// - snapshot creation cost when the requested fork has not been materialized yet
/// - `COLD_SLOAD_COST` for the storage read itself
///
/// # Errors
///
/// Returns an error if the requested snapshot cannot be materialized, the storage read fails, or
/// gas is insufficient.
fn load_state_at<'db, ExtDb, CTX>(
    context: &mut CTX,
    target: Address,
    slot: U256,
    fork_id: ForkId,
    call_tracer: &CallTracer,
    gas: u64,
) -> Result<PhevmOutcome, LoadStateAtError<ExtDb>>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, RESHIRAM_BASE_COST, gas_limit) {
        return Err(LoadStateAtError::OutOfGas(rax));
    }

    let journal = context.journal_mut();
    price_fork_creation_if_needed(journal, fork_id, call_tracer, &mut gas_left, gas_limit)?;

    journal
        .database
        .ensure_fork_exists(fork_id, &journal.inner, call_tracer)
        .map_err(LoadStateAtError::MultiForkDb)?;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, COLD_SLOAD_COST, gas_limit) {
        return Err(LoadStateAtError::OutOfGas(rax));
    }

    let value = journal
        .database
        .storage_ref_from_fork(fork_id, target, slot)
        .map_err(LoadStateAtError::StorageRefFromFork)?;

    let encoded: Bytes = SolValue::abi_encode(&FixedBytes::<32>::from(value.to_be_bytes())).into();
    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

/// Reads a storage slot from the assertion adopter at a requested snapshot fork.
///
/// # Errors
///
/// Returns an error if the calldata cannot be decoded, the requested snapshot cannot be
/// materialized, or gas is insufficient.
pub fn load_state_at_default_target<'db, ExtDb, CTX>(
    context: &mut CTX,
    phevm_context: &PhEvmContext,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, LoadStateAtError<ExtDb>>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = loadStateAt_0Call::abi_decode(input_bytes).map_err(LoadStateAtError::DecodeError)?;
    let fork_id = decode_fork_id::<ExtDb>(call.fork, call_tracer)?;

    load_state_at(
        context,
        phevm_context.adopter,
        call.slot.into(),
        fork_id,
        call_tracer,
        gas,
    )
}

/// Reads a storage slot from an arbitrary account at a requested snapshot fork.
///
/// # Errors
///
/// Returns an error if the calldata cannot be decoded, the requested snapshot cannot be
/// materialized, or gas is insufficient.
pub fn load_state_at_with_target<'db, ExtDb, CTX>(
    context: &mut CTX,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, LoadStateAtError<ExtDb>>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let call = loadStateAt_1Call::abi_decode(input_bytes).map_err(LoadStateAtError::DecodeError)?;
    let fork_id = decode_fork_id::<ExtDb>(call.fork, call_tracer)?;

    load_state_at(
        context,
        call.target,
        call.slot.into(),
        fork_id,
        call_tracer,
        gas,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::spec_recorder::AssertionSpec,
        test_utils::{
            random_address,
            random_u256,
            run_precompile_test_with_spec,
        },
    };
    use alloy_evm::eth::EthEvmContext;
    use revm::{
        JournalEntry,
        context::JournalInner,
        handler::MainnetContext,
        primitives::{
            KECCAK_EMPTY,
            hardfork::SpecId,
        },
    };

    const TEST_GAS: u64 = 1_000_000;

    fn create_test_context_with_mock_db(
        pre_tx_storage: Vec<(Address, U256, U256)>,
        post_tx_storage: Vec<(Address, U256, U256)>,
    ) -> (MultiForkDb<ForkDb<MockDb>>, JournalInner<JournalEntry>) {
        let mut pre_tx_db = MockDb::new();

        for (address, slot, value) in pre_tx_storage {
            pre_tx_db.insert_storage(address, slot, value);
            pre_tx_db.insert_account(
                address,
                crate::primitives::AccountInfo {
                    balance: U256::ZERO,
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                },
            );
        }

        let mut journal_inner = JournalInner::new();
        for (address, slot, value) in post_tx_storage {
            journal_inner.load_account(&mut pre_tx_db, address).unwrap();
            journal_inner
                .sstore(&mut pre_tx_db, address, slot, value, false)
                .unwrap();
            journal_inner.touch(address);
        }

        let multi_fork_db = MultiForkDb::new(ForkDb::new(pre_tx_db), &journal_inner);
        (multi_fork_db, journal_inner)
    }

    fn decode_bytes32(output: &Bytes) -> FixedBytes<32> {
        <FixedBytes<32> as SolValue>::abi_decode(output.as_ref()).unwrap()
    }

    #[test]
    fn test_load_state_at_reads_pre_and_post_tx_snapshots() {
        let address = random_address();
        let slot = random_u256();
        let pre_value = U256::from(100);
        let post_value = U256::from(200);

        let (mut multi_fork_db, journal) = create_test_context_with_mock_db(
            vec![(address, slot, pre_value)],
            vec![(address, slot, post_value)],
        );

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        let pre_calldata: Bytes = loadStateAt_1Call {
            target: address,
            slot: slot.into(),
            fork: SolForkId {
                forkType: 0,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let pre_outcome = load_state_at_with_target(
            &mut context,
            &CallTracer::default(),
            &pre_calldata,
            TEST_GAS,
        )
        .unwrap();
        assert_eq!(
            decode_bytes32(pre_outcome.bytes()),
            FixedBytes::from(pre_value.to_be_bytes())
        );

        let post_calldata: Bytes = loadStateAt_1Call {
            target: address,
            slot: slot.into(),
            fork: SolForkId {
                forkType: 1,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let post_outcome = load_state_at_with_target(
            &mut context,
            &CallTracer::default(),
            &post_calldata,
            TEST_GAS,
        )
        .unwrap();
        assert_eq!(
            decode_bytes32(post_outcome.bytes()),
            FixedBytes::from(post_value.to_be_bytes())
        );
    }

    #[test]
    fn test_load_state_at_ignores_assertion_local_writes() {
        let address = random_address();
        let slot = random_u256();
        let post_value = U256::from(200);
        let local_value = U256::from(999);

        let (mut multi_fork_db, mut journal) = create_test_context_with_mock_db(
            vec![(address, slot, U256::from(100))],
            vec![(address, slot, post_value)],
        );

        journal.load_account(&mut multi_fork_db, address).unwrap();
        journal
            .sstore(&mut multi_fork_db, address, slot, local_value, false)
            .unwrap();
        journal.touch(address);

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        let calldata: Bytes = loadStateAt_1Call {
            target: address,
            slot: slot.into(),
            fork: SolForkId {
                forkType: 1,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let outcome =
            load_state_at_with_target(&mut context, &CallTracer::default(), &calldata, TEST_GAS)
                .unwrap();

        assert_eq!(
            decode_bytes32(outcome.bytes()),
            FixedBytes::from(post_value.to_be_bytes())
        );
    }

    #[test]
    fn test_load_state_at_rejects_invalid_fork_type() {
        let address = random_address();
        let slot = random_u256();
        let (mut multi_fork_db, journal) = create_test_context_with_mock_db(
            vec![(address, slot, U256::from(1))],
            vec![(address, slot, U256::from(2))],
        );

        let mut context = EthEvmContext::new(&mut multi_fork_db, SpecId::default());
        context.journal_mut().inner = journal;

        let calldata: Bytes = loadStateAt_1Call {
            target: address,
            slot: slot.into(),
            fork: SolForkId {
                forkType: 9,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let err =
            load_state_at_with_target(&mut context, &CallTracer::default(), &calldata, TEST_GAS)
                .unwrap_err();
        assert!(matches!(
            err,
            LoadStateAtError::InvalidForkType { fork_type: 9 }
        ));
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_load_state_at() {
        let result = run_precompile_test_with_spec("TestLoadStateAt", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "loadStateAt should be allowed under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_load_state_at() {
        let result = run_precompile_test_with_spec("TestLoadStateAt", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "loadStateAt should be forbidden under Legacy spec"
        );
    }
}
