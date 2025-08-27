//! # `evm`
//!
//! Contains types needed to initialize a linea version of revm.

use crate::evm::linea::opcodes::insert_linea_instructions;
use revm::{
    Inspector,
    context::{
        ContextSetters,
        ContextTr,
        Evm,
    },
    handler::{
        EthPrecompiles,
        EvmTr,
        instructions::{
            EthInstructions,
            InstructionProvider,
        },
    },
    inspector::{
        InspectorEvmTr,
        JournalExt,
        inspect_instructions,
    },
    interpreter::{
        Interpreter,
        InterpreterTypes,
        interpreter::EthInterpreter,
    },
};

/// LineaEvm variant of the EVM.
pub struct LineaEvm<CTX, INSP>(
    pub Evm<CTX, INSP, EthInstructions<EthInterpreter, CTX>, EthPrecompiles>,
);

impl<CTX: ContextTr, INSP> LineaEvm<CTX, INSP> {
    pub fn new(ctx: CTX, inspector: INSP) -> Self {
        let mut instruction = EthInstructions::new_mainnet();
        insert_linea_instructions(&mut instruction).unwrap();

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
