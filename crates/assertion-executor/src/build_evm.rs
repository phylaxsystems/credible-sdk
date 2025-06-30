use revm::{
    Context,
    Database,
    Evm,
    EvmContext,
    Handler,
    Inspector,
    inspector_handle_register,
    interpreter::{
        Gas,
        Host,
        Interpreter,
        gas,
        instructions::host::{
            sload,
            sstore,
        },
        opcode::{
            SLOAD,
            SSTORE,
        },
    },
    primitives::{
        BlockEnv,
        CfgEnv,
        Env,
        EnvWithHandlerCfg,
        HandlerCfg,
        Spec,
        SpecId,
        TxEnv,
        spec_to_generic,
    },
};

use crate::inspectors::{
    CallTracer,
    PhEvmInspector,
};

use std::sync::Arc;

/// Create a new EVM instance for the specified parameters.
/// This is used to configure the EVM in a consistent way, with a basic inspector handle register
/// and default op codes.
pub fn new_evm<'evm, 'db, DB: Database, I: Inspector<&'db mut DB>>(
    tx_env: TxEnv,
    block_env: BlockEnv,
    chain_id: u64,
    spec_id: SpecId,
    db: &'db mut DB,
    inspector: I,
) -> Evm<'evm, I, &'db mut DB> {
    let (context, handler_cfg) =
        new_ctx_and_handler_cfg(tx_env, block_env, chain_id, spec_id, db, inspector);

    let mut handler = Handler::new(handler_cfg);
    handler.append_handler_register_plain(inspector_handle_register);

    Evm::new(context, handler)
}

/// Create a new evm instance for transaction forking.
/// This uses the CallTracer inspector and captures the JournaledState at the end of the tx for use
/// by the PhEvmInspector.
pub fn new_tx_fork_evm<'evm, DB: Database>(
    tx_env: TxEnv,
    block_env: BlockEnv,
    chain_id: u64,
    spec_id: SpecId,
    db: &mut DB,
    inspector: CallTracer,
) -> Evm<'evm, CallTracer, &mut DB> {
    let (context, handler_cfg) =
        new_ctx_and_handler_cfg(tx_env, block_env, chain_id, spec_id, db, inspector);

    let mut handler: Handler<'_, Context<CallTracer, &mut DB>, CallTracer, &mut DB> =
        Handler::new(handler_cfg);

    handler.append_handler_register_plain(inspector_handle_register);

    let output = handler.post_execution.output.clone();
    handler.post_execution.output = Arc::new(move |context, result| {
        context.external.journaled_state = Some(context.evm.inner.journaled_state.clone());
        output(context, result)
    });

    Evm::new(context, handler)
}

/// Create a new PhEvm instance for the specified parameters.
/// This is used for executing assertions.
pub fn new_phevm<'evm, 'db, 'i, DB: Database>(
    tx_env: TxEnv,
    block_env: BlockEnv,
    chain_id: u64,
    spec_id: SpecId,
    db: &'db mut DB,
    inspector: PhEvmInspector<'i>,
) -> Evm<'evm, PhEvmInspector<'i>, &'db mut DB>
where
    PhEvmInspector<'i>: Inspector<&'db mut DB>,
{
    let (context, handler_cfg) =
        new_ctx_and_handler_cfg(tx_env, block_env, chain_id, spec_id, db, inspector);

    let mut handler: Handler<
        '_,
        Context<PhEvmInspector<'i>, &mut DB>,
        PhEvmInspector<'i>,
        &mut DB,
    > = Handler::new(handler_cfg);

    handler.append_handler_register_plain(inspector_handle_register);
    handler
        .instruction_table
        .insert(SLOAD, spec_to_generic!(spec_id, ph_sload::<_, SPEC>));

    handler
        .instruction_table
        .insert(SSTORE, spec_to_generic!(spec_id, ph_sstore::<_, SPEC>));

    Evm::new(context, handler)
}

/// Create a new context and handler configuration.
///
/// This is useful for creating a new EVM instance with a custom handler configuration.
/// The configuration is determined by the provided spec_id, and potentially
/// modified based on feature flags (e.g., `optimism`).
///
/// When the `optimism` feature is enabled, the handler configuration is modified
/// to enable Optimism-specific behavior (`is_optimism = true`).
fn new_ctx_and_handler_cfg<'db, DB: Database, I: Inspector<&'db mut DB>>(
    tx_env: TxEnv,
    block_env: BlockEnv,
    chain_id: u64,
    spec_id: SpecId,
    db: &'db mut DB,
    inspector: I,
) -> (Context<I, &'db mut DB>, HandlerCfg) {
    let mut cfg_env = CfgEnv::default();
    cfg_env.chain_id = chain_id;

    let env = Env {
        block: block_env,
        tx: tx_env,
        cfg: cfg_env,
    };

    // Make handler_cfg mutable as we might modify it.
    #[allow(unused_mut)]
    let EnvWithHandlerCfg {
        env,
        mut handler_cfg,
    } = EnvWithHandlerCfg::new_with_spec_id(Box::new(env), spec_id);

    #[cfg(feature = "optimism")]
    {
        handler_cfg.is_optimism = true;
    }

    let evm_context = EvmContext::new_with_env(db, env);
    let context = Context::new(evm_context, inspector);

    (context, handler_cfg)
}

/// Reprice the gas of an operation to a fixed cost.
/// Will still run out of gas if the operation spends all gas intentionally.
macro_rules! reprice_gas {
    ($interpreter:expr, $host:expr, $operation:expr, $gas:expr) => {{
        // Spend the new expected gas. Will revert execution with an out-of-gas error if the gas
        // limit is exceeded.
        gas!($interpreter, $gas);

        // Cache the expected gas outcome, and replace the gas with the maximum value so that gas
        // limits are not enforced against other costs.
        let gas = std::mem::replace(&mut $interpreter.gas, Gas::new(u64::MAX));
        $operation($interpreter, $host);

        $interpreter.gas = gas;
    }};
}

/// Reprice the SLOAD operation to 100 gas.
/// Will still run out of gas if the operation spends all gas intentionally.
pub fn ph_sload<H, S: Spec>(interpreter: &mut Interpreter, host: &mut H)
where
    H: Host + ?Sized,
{
    reprice_gas!(interpreter, host, sload::<H, S>, 100);
}

/// Reprice the SSTORE operation to 100 gas.
/// Will still run out of gas if the operation spends all gas intentionally.
pub fn ph_sstore<H, S: Spec>(interpreter: &mut Interpreter, host: &mut H)
where
    H: Host + ?Sized,
{
    reprice_gas!(interpreter, host, sstore::<H, S>, 100);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::MultiForkDb,
        inspectors::{
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
    use revm::db::InMemoryDB;

    #[cfg(feature = "optimism")]
    use crate::executor::config::create_optimism_fields;

    fn setup_phevm<'evm, 'db, 'i>(
        db: &'db mut MultiForkDb<InMemoryDB>,
        inspector: PhEvmInspector<'i>,
        tx_env: TxEnv,
        reprice_storage: bool,
    ) -> Evm<'evm, PhEvmInspector<'i>, &'db mut MultiForkDb<InMemoryDB>> {
        let block_env = BlockEnv {
            basefee: U256::from(1),
            ..Default::default()
        };
        let chain_id = 1;
        let spec_id = SpecId::default();

        if reprice_storage {
            new_phevm(tx_env, block_env, chain_id, spec_id, db, inspector)
        } else {
            new_evm(tx_env, block_env, chain_id, spec_id, db, inspector)
        }
    }

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
                code: Some(Bytecode::LegacyRaw(code)),
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
            transact_to: TxKind::Call(address),
            caller,
            gas_price: U256::from(1),
            gas_limit: gas_limit.unwrap_or(1_000_000),
            #[cfg(feature = "optimism")]
            optimism: create_optimism_fields(),
            ..Default::default()
        };

        let mut multi_fork_db = MultiForkDb::new(db.clone(), db);

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let phvem_context = PhEvmContext::new(&logs_and_traces, address);

        let inspector = PhEvmInspector::new(Default::default(), &mut multi_fork_db, &phvem_context);

        let mut evm = setup_phevm(&mut multi_fork_db, inspector, tx_env.clone(), with_reprice);
        evm.transact().unwrap().result
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
    fn test_sload() {
        test_diff("StorageGas.sol:SLOADGas", 2_000);
    }

    #[test]
    fn test_sload_at_limit() {
        test_at_limit("StorageGas.sol:SLOADGas");
    }

    #[test]
    fn test_sload_under_limit() {
        test_under_limit("StorageGas.sol:SLOADGas");
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
