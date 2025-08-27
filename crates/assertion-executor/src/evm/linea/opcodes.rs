//! # `opcodes`
//!
//! Contains setup fns and implementations of linea specific opcodes.

use revm::{
    handler::instructions::EthInstructions,
    interpreter::{
        Host,
        InterpreterTypes,
    },
};

/// Inserts linea specific instructions into an Eth instruction table.
/// We replace certain opcodes to match the linea spec of revm.
// FIXME: this is ideally implemented as an instructionprovider but this is easier
pub fn insert_linea_instructions<WIRE, HOST>(
    instructions: &mut EthInstructions<WIRE, HOST>,
) -> Result<(), ()>
where
    WIRE: InterpreterTypes,
    HOST: Host,
{
    unimplemented!()
}

