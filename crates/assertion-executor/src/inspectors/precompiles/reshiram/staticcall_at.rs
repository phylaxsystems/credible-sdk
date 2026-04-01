use crate::{
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::{
            ForkDbFromForkError,
            ForkId,
            MultiForkDb,
            MultiForkError,
        },
    },
    inspectors::{
        CallTracer,
        NoOpInspector,
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm::{
            ForkId as SolForkId,
            StaticCallResult,
            staticcallAtCall,
        },
    },
    primitives::{
        Address,
        Bytecode,
        Bytes,
        HaltReason,
        TxKind,
        U256,
    },
};

use alloy_evm::eth::EthEvmContext;
use alloy_primitives::B256;
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use op_revm::{
    OpContext,
    OpEvm,
    OpTransaction,
    precompiles::OpPrecompiles,
};
use revm::{
    MainnetEvm,
    context::{
        ContextTr,
        Journal,
        LocalContext,
    },
    context_interface::{
        Cfg,
        LocalContextTr,
        context::ContextError,
        journaled_state::JournalTr,
    },
    handler::{
        EthFrame,
        EvmTr,
        FrameResult,
        ItemOrResult,
        post_execution,
    },
    interpreter::{
        CallInput,
        CallInputs,
        CallScheme,
        CallValue,
        FrameInput,
        SharedMemory,
        interpreter::EthInterpreter,
        interpreter_action::FrameInit,
    },
    precompile::{
        PrecompileSpecId,
        Precompiles,
    },
};

use super::BASE_COST as RESHIRAM_BASE_COST;

type CtxDbError<CTX> = ContextError<<<CTX as ContextTr>::Db as Database>::Error>;

const MEM_WRITE_COST: u64 = 3;
const RETURN_WORD_COST: u64 = 3;
const EVM_WORD_BYTES: u64 = 32;

#[derive(Debug, thiserror::Error)]
pub enum StaticCallAtError<ExtDb: DatabaseRef> {
    #[error("Error decoding staticcallAt call: {0}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Invalid fork type: {fork_type}")]
    InvalidForkType { fork_type: u8 },
    #[error("Call ID {call_id} is too large to be a valid index")]
    CallIdOverflow { call_id: U256 },
    #[error("Cannot staticcall at call {call_id}: call is inside a reverted subtree")]
    CallInsideRevertedSubtree { call_id: usize },
    #[error("Error materializing snapshot fork: {0}")]
    MultiForkDb(#[source] MultiForkError),
    #[error("Requested fork {0:?} has not been materialized")]
    ForkDbFromFork(ForkDbFromForkError),
    #[error("Nested staticcall database error: {0}")]
    NestedDb(#[source] ExtDb::Error),
    #[error("Nested staticcall execution error: {0}")]
    NestedExecution(String),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

fn decode_fork_id<ExtDb: DatabaseRef>(
    fork: &SolForkId,
    call_tracer: &CallTracer,
) -> Result<ForkId, StaticCallAtError<ExtDb>> {
    match fork.forkType {
        0 => Ok(ForkId::PreTx),
        1 => Ok(ForkId::PostTx),
        2 | 3 => {
            let call_id = fork.callIndex;
            let call_id_usize = call_id
                .try_into()
                .map_err(|_| StaticCallAtError::CallIdOverflow { call_id })?;

            if !call_tracer.is_call_forkable(call_id_usize) {
                return Err(StaticCallAtError::CallInsideRevertedSubtree {
                    call_id: call_id_usize,
                });
            }

            if fork.forkType == 2 {
                Ok(ForkId::PreCall(call_id_usize))
            } else {
                Ok(ForkId::PostCall(call_id_usize))
            }
        }
        fork_type => Err(StaticCallAtError::InvalidForkType { fork_type }),
    }
}

fn price_fork_creation_if_needed<ExtDb>(
    journal: &mut Journal<&mut MultiForkDb<ExtDb>>,
    fork_id: ForkId,
    call_tracer: &CallTracer,
    gas_left: &mut u64,
    gas_limit: u64,
) -> Result<(), StaticCallAtError<ExtDb>>
where
    ExtDb: DatabaseRef + Clone + DatabaseCommit,
{
    if journal.database.fork_exists(&fork_id) {
        return Ok(());
    }

    let bytes_written = journal
        .database
        .estimated_create_fork_bytes(fork_id, &journal.inner, call_tracer)
        .map_err(StaticCallAtError::MultiForkDb)?;
    let words_written = bytes_written.div_ceil(EVM_WORD_BYTES);

    if let Some(rax) = deduct_gas_and_check(gas_left, words_written * MEM_WRITE_COST, gas_limit) {
        return Err(StaticCallAtError::OutOfGas(rax));
    }

    Ok(())
}

fn encode_result(ok: bool, data: Bytes) -> Bytes {
    StaticCallResult { ok, data }.abi_encode().into()
}

fn resolve_known_bytecode<CTX>(
    context: &mut CTX,
    target: Address,
) -> Result<Option<(B256, crate::primitives::Bytecode)>, CtxDbError<CTX>>
where
    CTX: ContextTr,
    CTX::Journal: JournalTr,
{
    let account = &context.journal_mut().load_account_with_code(target)?.info;

    if let Some(Bytecode::Eip7702(eip7702_bytecode)) = &account.code {
        let delegated_address = eip7702_bytecode.delegated_address;
        let delegated_account = &context
            .journal_mut()
            .load_account_with_code(delegated_address)?
            .info;
        Ok(Some((
            delegated_account.code_hash(),
            delegated_account.code.clone().unwrap_or_default(),
        )))
    } else {
        Ok(Some((
            account.code_hash(),
            account.code.clone().unwrap_or_default(),
        )))
    }
}

fn run_frame_loop<EVM>(
    evm: &mut EVM,
    first_frame_input: FrameInit,
) -> Result<FrameResult, CtxDbError<EVM::Context>>
where
    EVM: EvmTr<Frame = EthFrame<EthInterpreter>>,
    EVM::Frame: revm::handler::FrameTr<FrameInit = FrameInit, FrameResult = FrameResult>,
{
    let res = evm.frame_init(first_frame_input)?;

    if let ItemOrResult::Result(frame_result) = res {
        return Ok(frame_result);
    }

    loop {
        let call_or_result = evm.frame_run()?;

        let result = match call_or_result {
            ItemOrResult::Item(init) => {
                match evm.frame_init(init)? {
                    ItemOrResult::Item(_) => continue,
                    ItemOrResult::Result(result) => result,
                }
            }
            ItemOrResult::Result(result) => result,
        };

        if let Some(result) = evm.frame_return_result(result)? {
            return Ok(result);
        }
    }
}

fn execute_nested_staticcall<EVM>(
    evm: &mut EVM,
    caller: Address,
    target: Address,
    calldata: Bytes,
    gas_limit: u64,
) -> Result<revm::context_interface::result::ExecutionResult<HaltReason>, CtxDbError<EVM::Context>>
where
    EVM: EvmTr<Frame = EthFrame<EthInterpreter>>,
    EVM::Frame: revm::handler::FrameTr<FrameInit = FrameInit, FrameResult = FrameResult>,
    EVM::Context: ContextTr<Local: LocalContextTr, Journal: JournalTr>,
{
    let known_bytecode = resolve_known_bytecode(evm.ctx_mut(), target)?;

    let mut memory =
        SharedMemory::new_with_buffer(evm.ctx_ref().local().shared_memory_buffer().clone());
    memory.set_memory_limit(evm.ctx_ref().cfg().memory_limit());

    let frame_input = FrameInput::Call(Box::new(CallInputs {
        input: CallInput::Bytes(calldata),
        return_memory_offset: 0..0,
        gas_limit,
        bytecode_address: target,
        known_bytecode,
        target_address: target,
        caller,
        value: CallValue::Transfer(U256::ZERO),
        scheme: CallScheme::StaticCall,
        is_static: true,
    }));

    let frame_result = run_frame_loop(
        evm,
        FrameInit {
            depth: 0,
            memory,
            frame_input,
        },
    )?;

    core::mem::replace(evm.ctx_mut().error(), Ok(()))?;

    let result = post_execution::output::<_, HaltReason>(evm.ctx_mut(), frame_result);
    evm.ctx_mut().local_mut().clear();
    evm.frame_stack().clear();

    Ok(result)
}

pub(crate) trait NestedStaticCallRunner<
    ExtDb: DatabaseRef + Database<Error = <ExtDb as DatabaseRef>::Error> + Clone + DatabaseCommit,
>
{
    fn run_nested_staticcall(
        &self,
        fork_db: ExtDb,
        adopter: Address,
        target: Address,
        calldata: Bytes,
        gas_limit: u64,
    ) -> Result<
        revm::context_interface::result::ExecutionResult<HaltReason>,
        ContextError<<ExtDb as DatabaseRef>::Error>,
    >;
}

impl<ExtDb> NestedStaticCallRunner<ExtDb> for crate::evm::build_evm::EthCtx<'_, MultiForkDb<ExtDb>>
where
    ExtDb: DatabaseRef + Database<Error = <ExtDb as DatabaseRef>::Error> + Clone + DatabaseCommit,
{
    fn run_nested_staticcall(
        &self,
        mut fork_db: ExtDb,
        adopter: Address,
        target: Address,
        calldata: Bytes,
        gas_limit: u64,
    ) -> Result<
        revm::context_interface::result::ExecutionResult<HaltReason>,
        ContextError<<ExtDb as DatabaseRef>::Error>,
    > {
        let mut tx_env = self.tx().clone();
        tx_env.kind = TxKind::Call(target);
        tx_env.gas_limit = gas_limit;
        tx_env.value = U256::ZERO;
        tx_env.data = calldata.clone();

        let spec = self.cfg().spec;
        let nested_context = EthEvmContext {
            journaled_state: {
                let mut journal = Journal::new(&mut fork_db);
                journal.set_spec_id(spec);
                journal
            },
            block: self.block().clone(),
            cfg: self.cfg().clone(),
            tx: tx_env,
            chain: (),
            local: LocalContext::default(),
            error: Ok(()),
        };
        let eth_precompiles = revm::handler::EthPrecompiles {
            precompiles: Precompiles::new(PrecompileSpecId::from_spec_id(spec)),
            spec,
        };

        let mut evm = MainnetEvm::new_with_inspector(
            nested_context,
            NoOpInspector,
            revm::handler::instructions::EthInstructions::default(),
            eth_precompiles,
        );

        execute_nested_staticcall(&mut evm, adopter, target, calldata, gas_limit)
    }
}

impl<ExtDb> NestedStaticCallRunner<ExtDb> for crate::evm::build_evm::OpCtx<'_, MultiForkDb<ExtDb>>
where
    ExtDb: DatabaseRef + Database<Error = <ExtDb as DatabaseRef>::Error> + Clone + DatabaseCommit,
{
    fn run_nested_staticcall(
        &self,
        mut fork_db: ExtDb,
        adopter: Address,
        target: Address,
        calldata: Bytes,
        gas_limit: u64,
    ) -> Result<
        revm::context_interface::result::ExecutionResult<HaltReason>,
        ContextError<<ExtDb as DatabaseRef>::Error>,
    > {
        let mut tx_env = self.tx().base.clone();
        tx_env.kind = TxKind::Call(target);
        tx_env.gas_limit = gas_limit;
        tx_env.value = U256::ZERO;
        tx_env.data = calldata.clone();

        let nested_context = OpContext {
            journaled_state: {
                let mut journal = Journal::new(&mut fork_db);
                journal.set_spec_id(self.cfg().spec.into());
                journal
            },
            block: self.block().clone(),
            cfg: self.cfg().clone(),
            tx: OpTransaction::new(tx_env),
            chain: self.chain().clone(),
            local: LocalContext::default(),
            error: Ok(()),
        };
        let op_precompiles = OpPrecompiles::new_with_spec(self.cfg().spec);
        let mut evm = OpEvm::new(nested_context, NoOpInspector)
            .with_precompiles(op_precompiles)
            .0;

        execute_nested_staticcall(&mut evm, adopter, target, calldata, gas_limit)
    }
}

/// Executes a nested `STATICCALL` against an immutable snapshot fork.
///
/// This precompile never switches the active assertion fork. It lazily materializes the requested
/// snapshot in [`MultiForkDb`] if needed, clones that snapshot's backing DB, and runs the nested
/// call against the clone so no nested mutations can leak back into assertion execution.
pub(crate) fn staticcall_at<'db, ExtDb, CTX>(
    context: &mut CTX,
    phevm_context: &PhEvmContext,
    call_tracer: &CallTracer,
    input_bytes: &Bytes,
    gas: u64,
) -> Result<PhevmOutcome, StaticCallAtError<ExtDb>>
where
    ExtDb: DatabaseRef
        + Database<Error = <ExtDb as DatabaseRef>::Error>
        + Clone
        + DatabaseCommit
        + 'db,
    CTX: ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>
        + NestedStaticCallRunner<ExtDb>,
{
    let call = staticcallAtCall::abi_decode(input_bytes).map_err(StaticCallAtError::DecodeError)?;
    let fork_id = decode_fork_id::<ExtDb>(&call.fork, call_tracer)?;

    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, RESHIRAM_BASE_COST, gas_limit) {
        return Err(StaticCallAtError::OutOfGas(rax));
    }

    {
        let journal = context.journal_mut();
        price_fork_creation_if_needed(journal, fork_id, call_tracer, &mut gas_left, gas_limit)?;

        // Ensure we can allocate the nested call budget before materializing the snapshot.
        if let Some(rax) = deduct_gas_and_check(&mut gas_left, call.gas_limit, gas_limit) {
            return Err(StaticCallAtError::OutOfGas(rax));
        }
        gas_left += call.gas_limit;

        journal
            .database
            .ensure_fork_exists(fork_id, &journal.inner, call_tracer)
            .map_err(StaticCallAtError::MultiForkDb)?;
    }

    let fork_db = context
        .journal()
        .database
        .clone_fork_db(fork_id)
        .map_err(StaticCallAtError::ForkDbFromFork)?;

    let execution_result = context
        .run_nested_staticcall(
            fork_db,
            phevm_context.adopter,
            call.target,
            call.data.clone(),
            call.gas_limit,
        )
        .map_err(|err| {
            match err {
                ContextError::Db(db_err) => StaticCallAtError::NestedDb(db_err),
                ContextError::Custom(message) => StaticCallAtError::NestedExecution(message),
            }
        })?;

    let (ok, data, nested_gas_used) = match execution_result {
        revm::context_interface::result::ExecutionResult::Success {
            output, gas_used, ..
        } => (true, output.into_data(), gas_used),
        revm::context_interface::result::ExecutionResult::Revert { output, gas_used } => {
            (false, output, gas_used)
        }
        revm::context_interface::result::ExecutionResult::Halt { gas_used, .. } => {
            (false, Bytes::default(), gas_used)
        }
    };

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, nested_gas_used, gas_limit) {
        return Err(StaticCallAtError::OutOfGas(rax));
    }

    let return_encoding_cost = RETURN_WORD_COST * (data.len() as u64).div_ceil(EVM_WORD_BYTES);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, return_encoding_cost, gas_limit) {
        return Err(StaticCallAtError::OutOfGas(rax));
    }

    Ok(PhevmOutcome::new(
        encode_result(ok, data),
        gas_limit - gas_left,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::{
            LogsAndTraces,
            spec_recorder::AssertionSpec,
        },
        test_utils::{
            deployed_bytecode,
            random_address,
            run_precompile_test_with_spec,
        },
    };
    use alloy_primitives::keccak256;
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
    fn read_storage_selector() -> [u8; 4] {
        keccak256("readStorage()".as_bytes()).0[..4]
            .try_into()
            .unwrap()
    }

    fn create_ph_context<'a>(
        adopter: Address,
        tx_env: &'a crate::primitives::TxEnv,
        logs_and_traces: &'a LogsAndTraces<'a>,
    ) -> PhEvmContext<'a> {
        PhEvmContext {
            logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: tx_env,
            assertion_spec: AssertionSpec::Reshiram,
        }
    }

    fn create_test_context_with_target_code(
        target: Address,
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
                    code_hash: if address == target {
                        keccak256(deployed_bytecode("Target.sol:Target"))
                    } else {
                        KECCAK_EMPTY
                    },
                    code: if address == target {
                        Some(Bytecode::new_legacy(deployed_bytecode("Target.sol:Target")))
                    } else {
                        None
                    },
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

    fn decode_result(output: &Bytes) -> StaticCallResult {
        StaticCallResult::abi_decode(output.as_ref()).unwrap()
    }

    #[test]
    fn test_staticcall_at_reads_snapshot_view_function() {
        let target = random_address();
        let slot = U256::ZERO;
        let (mut multi_fork_db, journal) = create_test_context_with_target_code(
            target,
            vec![(target, slot, U256::from(11))],
            vec![(target, slot, U256::from(22))],
        );

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv {
            caller: random_address(),
            ..Default::default()
        };
        let phevm_context = create_ph_context(target, &tx_env, &logs_and_traces);

        let calldata: Bytes = staticcallAtCall {
            target,
            data: Bytes::from(read_storage_selector().to_vec()),
            gas_limit: 100_000,
            fork: SolForkId {
                forkType: 1,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let outcome =
            staticcall_at(&mut context, &phevm_context, &tracer, &calldata, TEST_GAS).unwrap();
        let result = decode_result(outcome.bytes());
        let value = U256::from_be_slice(result.data.as_ref());

        assert!(result.ok);
        assert_eq!(value, U256::from(22));
    }

    #[test]
    fn test_staticcall_at_oog_does_not_materialize_fork() {
        let target = random_address();
        let (mut multi_fork_db, journal) = create_test_context_with_target_code(
            target,
            vec![(target, U256::ZERO, U256::from(1))],
            vec![(target, U256::ZERO, U256::from(2))],
        );

        let mut context = MainnetContext::new(&mut multi_fork_db, SpecId::default());
        context.modify_journal(|current_journal| {
            current_journal.inner = journal;
        });

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv {
            caller: random_address(),
            ..Default::default()
        };
        let phevm_context = create_ph_context(target, &tx_env, &logs_and_traces);

        let calldata: Bytes = staticcallAtCall {
            target,
            data: Bytes::from(read_storage_selector().to_vec()),
            gas_limit: 100_000,
            fork: SolForkId {
                forkType: 0,
                callIndex: U256::ZERO,
            },
        }
        .abi_encode()
        .into();

        let err = staticcall_at(
            &mut context,
            &phevm_context,
            &tracer,
            &calldata,
            RESHIRAM_BASE_COST + 99_999,
        )
        .unwrap_err();

        assert!(matches!(err, StaticCallAtError::OutOfGas(_)));
        drop(context);
        assert!(!multi_fork_db.fork_exists(&ForkId::PreTx));
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_staticcall_at() {
        let result = run_precompile_test_with_spec("TestStaticCallAt", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "staticcallAt should be allowed under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_staticcall_at() {
        let result = run_precompile_test_with_spec("TestStaticCallAt", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "staticcallAt should be forbidden under Legacy spec"
        );
    }
}
