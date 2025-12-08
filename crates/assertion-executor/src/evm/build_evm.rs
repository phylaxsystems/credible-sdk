use crate::{
    db::Database,
    primitives::{
        BlockEnv,
        Journal,
        SpecId,
        TxEnv,
    },
};

use alloy_evm::{
    EvmEnv,
    eth::EthEvmContext,
};

use op_revm::{
    L1BlockInfo,
    OpContext,
    OpSpecId,
    precompiles::OpPrecompiles,
    transaction::OpTransaction,
};

use revm::{
    Context,
    Inspector,
    MainnetEvm,
    context::{
        CfgEnv,
        Evm,
        JournalTr,
        LocalContext,
    },
    handler::{
        EthFrame,
        EthPrecompiles,
        instructions::EthInstructions,
    },
    interpreter::{
        Gas,
        Host,
        InstructionContext,
        instructions::host::sstore,
        interpreter::EthInterpreter,
    },
    precompile::{
        PrecompileSpecId,
        Precompiles,
    },
};

/// Builds an EVM environment.
/// The `chain_id` is used to set the chain ID in the EVM environment.
/// The `spec_id` is used to set the spec ID in the EVM environment.
/// The `block_env` is used to set the block environment in the EVM environment.
pub fn evm_env<Spec>(chain_id: u64, spec_id: Spec, block_env: BlockEnv) -> EvmEnv<Spec>
where
    Spec: Default,
{
    let mut cfg_env = CfgEnv::default();

    #[cfg(feature = "phoundry")]
    {
        cfg_env.disable_eip3607 = true;
    }

    cfg_env.chain_id = chain_id;
    cfg_env.spec = spec_id;
    EvmEnv { cfg_env, block_env }
}

pub type OpCtx<'db, DB> = Context<
    BlockEnv,
    OpTransaction<TxEnv>,
    CfgEnv<OpSpecId>,
    &'db mut DB,
    Journal<&'db mut DB>,
    L1BlockInfo,
>;
type OpIns<'db, DB> = EthInstructions<EthInterpreter, OpCtx<'db, DB>>;
type OpEvm<'db, DB, I> = Evm<OpCtx<'db, DB>, I, OpIns<'db, DB>, OpPrecompiles, EthFrame>;

/// Builds an Optimism EVM, using all optimism related types.
/// Passes the `db` as a mutable reference to the inspector.
/// Any type that implements the inspector trait for the `OpCtx` can be used.
/// The `env` is used to configure the EVM.
pub fn build_optimism_evm<'db, DB, I>(
    db: &'db mut DB,
    env: &EvmEnv,
    inspector: I,
) -> OpEvm<'db, DB, I>
where
    DB: Database,
    I: Inspector<OpCtx<'db, DB>>,
{
    let op_cfg = env.cfg_env.clone().with_spec(op_revm::OpSpecId::ISTHMUS);

    let op_context: Context<
        BlockEnv,
        OpTransaction<TxEnv>,
        CfgEnv<OpSpecId>,
        &'db mut DB,
        Journal<&'db mut DB>,
        L1BlockInfo,
    > = OpContext {
        journaled_state: {
            let mut journal = Journal::new(db);
            journal.set_spec_id(env.cfg_env.spec);
            journal
        },
        block: env.block_env.clone(),
        cfg: op_cfg.clone(),
        tx: OpTransaction::default(),
        chain: L1BlockInfo::default(),
        local: LocalContext::default(),
        error: Ok(()),
    };
    let op_precompiles = OpPrecompiles::new_with_spec(op_cfg.spec);
    let evm = op_revm::OpEvm::new(op_context, inspector).with_precompiles(op_precompiles);
    evm.0
}

pub type EthCtx<'db, DB> =
    Context<BlockEnv, TxEnv, CfgEnv<SpecId>, &'db mut DB, Journal<&'db mut DB>, ()>;
pub type EthIns<'db, DB> = EthInstructions<EthInterpreter, EthCtx<'db, DB>>;
type EthEvm<'db, DB, I> = Evm<EthCtx<'db, DB>, I, EthIns<'db, DB>, EthPrecompiles, EthFrame>;

/// Builds a mainnet Ethereum EVM, using all mainnet related types.
/// Passes the `db` as a mutable reference to the inspector.
/// Any type that implements the inspector trait for the `EthCtx` can be used.
/// The `env` is used to configure the EVM.
pub fn build_eth_evm<'db, DB, I>(db: &'db mut DB, env: &EvmEnv, inspector: I) -> EthEvm<'db, DB, I>
where
    DB: Database,
    I: Inspector<EthCtx<'db, DB>>,
{
    let spec = env.cfg_env.spec;
    let eth_context = EthEvmContext {
        journaled_state: {
            let mut journal = Journal::new(db);
            journal.set_spec_id(spec);
            journal
        },
        block: env.block_env.clone(),
        cfg: env.cfg_env.clone(),
        tx: TxEnv::default(),
        chain: (),
        local: LocalContext::default(),
        error: Ok(()),
    };
    let eth_precompiles = EthPrecompiles {
        precompiles: Precompiles::new(PrecompileSpecId::from_spec_id(spec)),
        spec,
    };

    MainnetEvm::new_with_inspector(
        eth_context,
        inspector,
        EthInstructions::default(),
        eth_precompiles,
    )
}

/// Replaces `SSTORE` with functionally equivalent `ph_sstore`, but with
/// a fixed 100 gas cost.
/// Used for cheaper storage during assertion execution.
/// Storage values get thrown out during assertions so no need to bear
/// the cost of `SSTORE`.
///
/// We can potentially `SLOAD` slots which are not in memory, which is why
/// only `SSTORE` gets repriced.
#[macro_export]
macro_rules! reprice_evm_storage {
    ($evm:expr) => {{
        use revm::interpreter::Instruction;
        $evm.instruction.insert_instruction(
            revm::bytecode::opcode::SSTORE,
            Instruction::new($crate::evm::build_evm::ph_sstore, 0),
        );
    }};
}
/// Reprice the gas of an operation to a fixed cost.
/// Will still run out of gas if the operation spends all gas intentionally.
macro_rules! reprice_gas {
    ($context:expr, $operation:expr, $gas:expr) => {{
        // Spend the new expected gas. Will revert execution with an out-of-gas error if the gas
        // limit is exceeded.
        if !$context.interpreter.gas.record_cost($gas) {
            $context.interpreter.halt_oog();
            return;
        }

        // Cache the expected gas outcome, and replace the gas with the maximum value so that gas
        // limits are not enforced against other costs.
        let gas_ptr: *mut Gas = &mut $context.interpreter.gas;
        let saved_gas = std::mem::replace(unsafe { &mut *gas_ptr }, Gas::new(u64::MAX));

        $operation($context);

        unsafe { *gas_ptr = saved_gas };
    }};
}

/// Reprice the SSTORE operation to 100 gas.
/// Will still run out of gas if the operation spends all gas intentionally.
pub fn ph_sstore<H>(context: InstructionContext<'_, H, EthInterpreter>)
where
    H: Host + ?Sized,
{
    reprice_gas!(context, sstore::<EthInterpreter, H>, 100);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::MultiForkDb,
        inspectors::{
            CallTracer,
            LogsAndTraces,
            PhEvmContext,
            PhEvmInspector,
        },
        primitives::{
            AccountInfo,
            Address,
            BlockEnv,
            Bytecode,
            Bytes,
            EvmExecutionResult,
            TxEnv,
            TxKind,
            U256,
            keccak256,
        },
        test_utils::deployed_bytecode,
    };
    use revm::{
        ExecuteEvm,
        context::JournalInner,
        database::InMemoryDB,
    };

    fn insert_caller(db: &mut InMemoryDB, caller: Address) {
        db.insert_account_info(
            caller,
            AccountInfo {
                nonce: 0,
                balance: U256::MAX,
                code_hash: keccak256([]),
                code: None,
            },
        );
    }

    fn insert_test_contract(db: &mut InMemoryDB, address: Address, code: Bytes) {
        db.insert_account_info(
            address,
            AccountInfo {
                nonce: 1,
                balance: U256::ZERO,
                code_hash: keccak256(&code),
                code: Some(Bytecode::new_legacy(code)),
            },
        );
    }

    fn run_test(contract: &str, with_reprice: bool, gas_limit: Option<u64>) -> EvmExecutionResult {
        let address = Address::random();
        let caller = Address::random();

        let mut db = InMemoryDB::default();
        insert_caller(&mut db, caller);

        insert_test_contract(&mut db, address, deployed_bytecode(contract));

        let tx_env = TxEnv {
            kind: TxKind::Call(address),
            caller,
            gas_price: 1,
            gas_limit: gas_limit.unwrap_or(1_000_000),
            ..Default::default()
        };

        let mut multi_fork_db = MultiForkDb::new(db, &JournalInner::new());

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let phvem_context = PhEvmContext::new(&logs_and_traces, address);

        let inspector = PhEvmInspector::new(phvem_context);

        #[cfg(feature = "optimism")]
        let (mut evm, tx_env) = {
            let env = evm_env(1, SpecId::default(), BlockEnv::default());
            (
                build_optimism_evm(&mut multi_fork_db, &env, inspector),
                OpTransaction::new(tx_env),
            )
        };
        #[cfg(not(feature = "optimism"))]
        let (mut evm, tx_env) = {
            let env = evm_env(1, SpecId::default(), BlockEnv::default());
            (build_eth_evm(&mut multi_fork_db, &env, inspector), tx_env)
        };

        if with_reprice {
            crate::reprice_evm_storage!(evm);
        }

        evm.transact(tx_env).unwrap().result
    }

    fn test_diff(contract: &str, expected_gas: u64) {
        let with_reprice_result = run_test(contract, true, None);
        let without_reprice_result = run_test(contract, false, None);
        println!(
            "Gas used without reprice: {}, with reprice: {}",
            without_reprice_result.gas_used(),
            with_reprice_result.gas_used()
        );

        assert_eq!(
            without_reprice_result.gas_used() - with_reprice_result.gas_used(),
            expected_gas
        );
    }

    fn test_at_limit(contract: &str) {
        let no_reprice_gas = run_test(contract, false, None).gas_used();
        let with_reprice_result = run_test(contract, false, Some(no_reprice_gas));
        assert!(with_reprice_result.is_success());
    }
    fn test_under_limit(contract: &str) {
        let no_reprice_gas = run_test(contract, false, None).gas_used();
        let with_reprice_result = run_test(contract, false, Some(no_reprice_gas - 1));
        assert!(with_reprice_result.is_halt());
        assert_eq!(with_reprice_result.gas_used(), no_reprice_gas - 1);
    }

    #[test]
    fn test_sstore() {
        test_diff("StorageGas.sol:SSTOREGas", 22_000);
    }

    #[test]
    fn test_sstore_at_limit() {
        test_at_limit("StorageGas.sol:SSTOREGas");
    }

    #[test]
    fn test_sstore_under_limit() {
        test_under_limit("StorageGas.sol:SSTOREGas");
    }
}
