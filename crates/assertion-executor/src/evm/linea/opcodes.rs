//! # Linea specific `opcodes`
//!
//! Contains setup fns and implementations of linea specific opcodes. Linea contains a few changes
//! to opcode behavior. We target the final v4 spec of the linea evm. Compared to Osaka,
//! which the linea evm is based on, The changes are as follows:
//! - **BLOBBASEFEE** - Will always return the minimum value
//! - **BLOBHASH** - Will always return `0`
//! - **PREVRANDAO** - Use a formula similar to Ethereum, e.g. `L2_prevrandao XOR hash(signed(slot_id))`. Given by the sequencer, no revm opcode changes.

use revm::{
    bytecode::opcode::{
        BLOBBASEFEE,
        BLOBHASH,
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
// TODO: this is ideally implemented as an instructionprovider but this is easier
pub fn insert_linea_instructions<WIRE, HOST>(instructions: &mut EthInstructions<WIRE, HOST>)
where
    WIRE: InterpreterTypes,
    HOST: Host,
{
    instructions.insert_instruction(BLOBHASH, linea_blob_hash);
    instructions.insert_instruction(BLOBBASEFEE, linea_blob_basefee);
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
/// The linea version of BLOBBASEFEE returns the minimum value.
/// The minimum value is always `1_u256`.
pub fn linea_blob_basefee<WIRE: InterpreterTypes, HOST: Host>(
    interpreter: &mut Interpreter<WIRE>,
    _host: &mut HOST,
) {
    let _ = interpreter.stack.push(revm::primitives::U256::from(1));
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use revm::{
        interpreter::{
            InputsImpl,
            SharedMemory,
            Stack,
            host::DummyHost,
            interpreter::{
                EthInterpreter,
                ExtBytecode,
            },
        },
        primitives::hardfork::SpecId,
        state::Bytecode,
    };

    #[test]
    fn test_linea_blob_hash_returns_zero() {
        // Create a minimal stack for testing
        let mut stack = Stack::new();
        let _ = stack.push(U256::from(1));

        // Create interpreter with the stack
        let mut interpreter = create_test_interpreter(stack);

        let mut host = DummyHost;
        linea_blob_hash(&mut interpreter, &mut host);

        // Should have popped the index and pushed 0
        assert_eq!(interpreter.stack.len(), 1);
        assert_eq!(interpreter.stack.peek(0).unwrap(), U256::ZERO);
    }

    #[test]
    fn test_linea_blob_hash_with_different_indices() {
        // Test with different indices - all should return 0
        for index in [0u64, 1u64, 255u64] {
            let mut stack = Stack::new();
            let _ = stack.push(U256::from(index));

            let mut interpreter = create_test_interpreter(stack);
            let mut host = DummyHost;

            linea_blob_hash(&mut interpreter, &mut host);

            assert_eq!(interpreter.stack.len(), 1);
            assert_eq!(
                interpreter.stack.peek(0).unwrap(),
                U256::ZERO,
                "Index {index} should return 0"
            );
        }
    }

    #[test]
    fn test_linea_blob_basefee_behavior() {
        let stack = Stack::new();
        let mut interpreter = create_test_interpreter(stack);

        let mut host = DummyHost;

        linea_blob_basefee(&mut interpreter, &mut host);

        // Should have pushed the blob gas price from DummyHost
        assert_eq!(interpreter.stack.len(), 1);
        // DummyHost returns U256::ZERO for blob_gasprice by default
        assert_eq!(interpreter.stack.peek(0).unwrap(), U256::from(1));
    }

    #[test]
    fn test_linea_opcodes_execution_no_panic() {
        // Integration test to ensure all opcodes execute without panicking
        let mut stack = Stack::new();

        // Add an index for BLOBHASH
        let _ = stack.push(U256::from(1));

        let mut interpreter = create_test_interpreter(stack);
        let mut host = DummyHost;

        // Test BLOBHASH
        linea_blob_hash(&mut interpreter, &mut host);

        // Test BLOBBASEFEE
        linea_blob_basefee(&mut interpreter, &mut host);

        // Should have 3 values on stack now
        assert_eq!(interpreter.stack.len(), 2);

        // All should be 0 from DummyHost
        assert_eq!(interpreter.stack.peek(0).unwrap(), U256::from(1)); // BLOBBASEFEE
        assert_eq!(interpreter.stack.peek(1).unwrap(), U256::ZERO); // BLOBHASH
    }

    // Helper function to create a test interpreter
    fn create_test_interpreter(stack: Stack) -> revm::interpreter::Interpreter<EthInterpreter> {
        let memory = SharedMemory::new();
        let bytecode = ExtBytecode::new(Bytecode::default());
        let inputs = InputsImpl::default();
        let is_static = false;
        let is_eof = false;
        let spec_id = SpecId::PRAGUE;
        let gas_limit = 1000000u64;

        let mut interpreter = revm::interpreter::Interpreter::new(
            memory, bytecode, inputs, is_static, is_eof, spec_id, gas_limit,
        );

        // Replace the default stack with our test stack
        interpreter.stack = stack;
        interpreter
    }
}
