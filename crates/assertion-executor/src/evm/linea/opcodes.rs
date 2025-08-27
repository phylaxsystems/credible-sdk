//! # Linea specific `opcodes`
//!
//! Contains setup fns and implementations of linea specific opcodes. Linea contains a few changes
//! to opcode behavior. We target the final v4 spec of the linea evm. Compared to Osaka,
//! which the linea evm is based on, The changes are as follows:
//! - **BLOBBASEFEE** - Will always return the minimum value
//! - **BLOBHASH** - Will always return `0`
//! - **PREVRANDAO** - Use a formula similar to Ethereum, e.g. `L2_prevrandao XOR hash(signed(slot_id))`

use alloy_primitives::keccak256;
use revm::{
    bytecode::opcode::{
        BLOBBASEFEE,
        BLOBHASH,
        DIFFICULTY,
    },
    handler::instructions::EthInstructions,
    interpreter::{
        Host,
        Interpreter,
        InterpreterTypes,
        interpreter_types::StackTr,
    },
};

/// Inserts linea specific instructions into an Eth instruction table.
/// We replace certain opcodes to match the linea spec of revm.
// FIXME: this is ideally implemented as an instructionprovider but this is easier
pub fn insert_linea_instructions<WIRE, HOST>(
    instructions: &mut EthInstructions<WIRE, HOST>,
) -> ()
where
    WIRE: InterpreterTypes,
    HOST: Host,
{
    instructions.insert_instruction(BLOBHASH, linea_blob_hash);
    instructions.insert_instruction(BLOBBASEFEE, linea_blob_basefee);
    instructions.insert_instruction(DIFFICULTY, linea_difficulty);
}

/// Implements the linea version of the BLOBHASH instruction.
///
/// EIP-4844: Shard Blob Transactions - gets the hash of a transaction blob.
///
/// On Linea v4, we always return 0.
pub fn linea_blob_hash<WIRE: InterpreterTypes, HOST: Host>(
    interpreter: &mut Interpreter<WIRE>,
    _host: &mut HOST,
) {
    // On Linea v4, we always return 0
    // Pop the index from stack and push 0
    if let Some(_index) = interpreter.stack.pop() {
        let _ = interpreter.stack.push(revm::primitives::U256::ZERO);
    }
}

/// EIP-7516: BLOBBASEFEE opcode
///
/// The linea version of BLOBBASEFEE returns the minimum value
// FIXME: this is ambigous, we need to clarify what `minimum value means`. returning blob_gasprice for now
pub fn linea_blob_basefee<WIRE: InterpreterTypes, HOST: Host>(
    interpreter: &mut Interpreter<WIRE>,
    host: &mut HOST,
) {
    if let Some(_index) = interpreter.stack.pop() {
        let _ = interpreter.stack.push(host.blob_gasprice());
    }
}

/// Implements the linea DIFFICULTY/PREVRANDAO instruction.
///
/// The key differance between the regular prevrandao opcode and the linea
/// version is that on linea, we XOR prevrandao with the keccak of the current slot
// FIXME: hash is ambigous, we need to get clarity if hash as mentioned in docs is
// keccak or something more zk freindly. also clarify which slot and its type
pub fn linea_difficulty<WIRE: InterpreterTypes, HOST: Host>(
    interpreter: &mut Interpreter<WIRE>,
    host: &mut HOST,
) {
    // get the previous blocks prevrandao
    let prevrandao = host.prevrandao();
    if prevrandao.is_none() {
        // if first block/no prevrandao we can just ret 0
        let _ = interpreter.stack.push(revm::primitives::U256::ZERO);
    }

    // get the slot number
    let slot: i64 = host.block_number().try_into().unwrap();
    // keccak the slot
    let hashed_slot = keccak256(slot.to_be_bytes());
    // now xor them
    let linea_prevrandao = prevrandao.unwrap().bitxor(hashed_slot.into());

    if let Some(_index) = interpreter.stack.pop() {
        let _ = interpreter.stack.push(linea_prevrandao);
    }
}
