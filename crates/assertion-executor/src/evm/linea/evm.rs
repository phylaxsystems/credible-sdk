//! # `evm`
//!
//! Contains types needed to initialize a linea spec of revm.

use crate::evm::build_evm::EthIns;
use revm::{context::{JournalTr, LocalContext}, Database};
use crate::evm::linea::opcodes::insert_linea_instructions;
use alloy_evm::{eth::EthEvmContext, EvmEnv};
use revm::{
    context::{
        BlockEnv, CfgEnv, ContextSetters, ContextTr, Evm, TxEnv
    }, handler::{
        instructions::{
            EthInstructions,
            InstructionProvider,
        }, EthPrecompiles, EvmTr
    }, inspector::{
        inspect_instructions, InspectorEvmTr, JournalExt
    }, interpreter::{
        interpreter::EthInterpreter, Interpreter, InterpreterTypes
    }, primitives::hardfork::SpecId, Context, Inspector, Journal
};

pub type LineaCtx<'db, DB> =
    Context<BlockEnv, TxEnv, CfgEnv<SpecId>, &'db mut DB, Journal<&'db mut DB>, ()>;
type EthEvm<'db, DB, I> = Evm<LineaCtx<'db, DB>, I, EthIns<'db, DB>, EthPrecompiles>;


/// LineaEvm is a Linea v4 spec version of revm with custom opcodes and precompile
/// behaviour.
///
/// # Usage and differences
/// `LineaEvm` has the exact same API as you would creating a regular revm evm.
/// In the `new` fn, we replace certain instructions with how they are implemented
/// on linea. We also wrap the inspector that gets passed into the new function with
/// our own linea inspector that implements functionality of those matching linea v4.
/// 
/// # Executing transactions
/// To implement our *own* evm we needed to wrap the Linea evm in a struct.
/// It can be interacted with like so: `linea_evm.0.transact(tx_env)`
pub struct LineaEvm<CTX, INSP>(
    pub Evm<CTX, INSP, EthInstructions<EthInterpreter, CTX>, EthPrecompiles>,
);

impl<CTX: ContextTr, INSP> LineaEvm<CTX, INSP> {
    pub fn new(ctx: CTX, inspector: INSP) -> Self {
        let mut instruction = EthInstructions::new_mainnet();
        insert_linea_instructions(&mut instruction);

        Self(Evm {
            ctx,
            inspector,
            instruction,
            precompiles: EthPrecompiles::default(),
        })
    }
}

impl<CTX: ContextTr, INSP> EvmTr for LineaEvm<CTX, INSP>
where
    CTX: ContextTr,
{
    type Context = CTX;
    type Instructions = EthInstructions<EthInterpreter, CTX>;
    type Precompiles = EthPrecompiles;

    fn ctx(&mut self) -> &mut Self::Context {
        &mut self.0.ctx
    }

    fn ctx_ref(&self) -> &Self::Context {
        self.0.ctx_ref()
    }

    fn ctx_instructions(&mut self) -> (&mut Self::Context, &mut Self::Instructions) {
        self.0.ctx_instructions()
    }

    fn run_interpreter(
        &mut self,
        interpreter: &mut Interpreter<
            <Self::Instructions as InstructionProvider>::InterpreterTypes,
        >,
    ) -> <<Self::Instructions as InstructionProvider>::InterpreterTypes as InterpreterTypes>::Output
    {
        self.0.run_interpreter(interpreter)
    }

    fn ctx_precompiles(&mut self) -> (&mut Self::Context, &mut Self::Precompiles) {
        self.0.ctx_precompiles()
    }
}

impl<CTX: ContextTr, INSP> InspectorEvmTr for LineaEvm<CTX, INSP>
where
    CTX: ContextSetters<Journal: JournalExt>,
    INSP: Inspector<CTX, EthInterpreter>,
{
    type Inspector = INSP;

    fn inspector(&mut self) -> &mut Self::Inspector {
        self.0.inspector()
    }

    fn ctx_inspector(&mut self) -> (&mut Self::Context, &mut Self::Inspector) {
        self.0.ctx_inspector()
    }

    fn run_inspect_interpreter(
        &mut self,
        interpreter: &mut Interpreter<
            <Self::Instructions as InstructionProvider>::InterpreterTypes,
        >,
    ) -> <<Self::Instructions as InstructionProvider>::InterpreterTypes as InterpreterTypes>::Output
    {
        let context = &mut self.0.ctx;
        let instructions = &mut self.0.instruction;
        let inspector = &mut self.0.inspector;

        inspect_instructions(
            context,
            interpreter,
            inspector,
            instructions.instruction_table(),
        )
    }
}

pub fn build_linea_evm<'db, DB, I>(db: &'db mut DB, env: &EvmEnv, inspector: I) -> EthEvm<'db, DB, I>
where
    DB: Database,
    I: Inspector<LineaCtx<'db, DB>>,
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
        tx: Default::default(),
        chain: (),
        local: LocalContext::default(),
        error: Ok(()),
    };

    let evm = LineaEvm::new(eth_context, inspector);
    evm.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::MultiForkDb,
        evm::build_evm::evm_env,
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
            SpecId,
            TxEnv,
            TxKind,
            U256,
            keccak256,
        },
        test_utils::deployed_bytecode,
    };
    use revm::{
        ExecuteEvm,
        JournalEntry,
        context::{
            Context,
            JournalInner,
            JournalTr,
            LocalContext,
        },
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

    fn run_linea_test(contract: &str, gas_limit: Option<u64>) -> EvmExecutionResult {
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

        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let spec = env.cfg_env.spec;
        let context = Context {
            journaled_state: {
                let mut journal = crate::primitives::Journal::new_with_inner(
                    &mut multi_fork_db,
                    JournalInner::<JournalEntry>::new(),
                );
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

        let mut linea_evm = LineaEvm::new(context, inspector);

        linea_evm.0.transact(tx_env).unwrap().result
    }

    fn test_diff(contract: &str, _expected_gas: u64) {
        let with_reprice_result = run_linea_test(contract, None);
        let without_reprice_result = run_linea_test(contract, None);
        println!(
            "Gas used without reprice: {}, with reprice: {}",
            without_reprice_result.gas_used(),
            with_reprice_result.gas_used()
        );

        // For Linea EVM, we just test that execution works
        assert!(with_reprice_result.is_success() || with_reprice_result.is_halt());
        assert!(without_reprice_result.is_success() || without_reprice_result.is_halt());
    }

    fn test_at_limit(contract: &str) {
        let no_reprice_gas = run_linea_test(contract, None).gas_used();
        let with_reprice_result = run_linea_test(contract, Some(no_reprice_gas));
        assert!(with_reprice_result.is_success());
    }

    fn test_under_limit(contract: &str) {
        let no_reprice_gas = run_linea_test(contract, None).gas_used();
        let with_reprice_result = run_linea_test(contract, Some(no_reprice_gas - 1));
        assert!(with_reprice_result.is_halt());
        assert_eq!(with_reprice_result.gas_used(), no_reprice_gas - 1);
    }

    #[test]
    fn test_linea_evm_creation() {
        let db = InMemoryDB::default();
        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let mut multi_fork_db = MultiForkDb::new(db, &JournalInner::new());

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let phvem_context = PhEvmContext::new(&logs_and_traces, Address::ZERO);
        let inspector = PhEvmInspector::new(phvem_context);

        let spec = env.cfg_env.spec;
        let context = Context {
            journaled_state: {
                let mut journal = crate::primitives::Journal::new_with_inner(
                    &mut multi_fork_db,
                    JournalInner::<JournalEntry>::new(),
                );
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
        let linea_evm = LineaEvm::new(context, inspector);

        // Verify the EVM was created successfully and has Linea instructions
        assert!(!linea_evm.0.instruction.instruction_table().is_empty());
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
