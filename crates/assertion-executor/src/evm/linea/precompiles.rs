//! # Linea `precompiles`
//!
//! Contains linea specific precompile implementations.
//!
//! Linea v4 implements the osaka spec precompiles with the following differences
//! - **BLAKE2f** - Compression function F used in the BLAKE2 cryptographic hashing algorithm - Not supported
//! - **MODEXP** - Arbitrary-precision exponentiation under modulo - Only supports arguments (base, exponent, modulus) that do not exceed 512-byte integers
//! - **Precompiles as transaction recipients** - Applicable to various use cases - Not supported. A transaction to address cannot be a precompile, i.e. an address in the range 0x01-0x09
//! - **RIPEMD-160** - A hash function - TBD (treat as removed)
//!
//! Instead of actually implementing custom precompiles, we extend the phevm insperctor
//! impl function to check if we are calling any of the precompiles above. If we are,
//! we need to process them according to the linea spec.

use crate::{
    db::Database,
    evm::linea::evm::LineaCtx,
    inspectors::CallTracer,
    primitives::bytes,
};
use revm::{
    Inspector,
    interpreter::{
        CallInputs,
        CallOutcome,
        CreateInputs,
        CreateOutcome,
        Gas,
        InterpreterResult,
    },
    precompile::{
        blake2::FUN,
        hash::RIPEMD160,
        modexp::BERLIN as MODEXP, // FIXME: this is bad because we cannot change with specid. real solution is a precompile provider
    },
};

/// `WrappedInspector` exists as a wrapper for merging the functionality of multiple
/// revm inspectors into one.
///
/// This struct fully implements the inspector traits, and when we begin inspection,
/// we first call into `INSP0`, and then `INSP1`.
///
/// This struct is primarily used to Wrap the phevm inspector, providing
#[derive(Debug)]
#[allow(dead_code)]
struct WrappedInspector<INSP0, INSP1> {
    inspector_0: INSP0,
    inspector_1: INSP1,
}

/// Called inside of an inspector to act as if we were calling into
/// linea precompiles.
///
/// The outcomes depending on the precompile are as follows:
/// - **BLAKE2f** - Always revert
/// - **MODEXP** - None (meaning pass) if args (base, exponent, modulus) are individually less than 512 bytes, otherwise revert
/// - **Precompiles as transaction recipients** - Always revert
/// - **RIPEMD-160** - Always revert
/// - Anything else - None (meaning pass)
pub fn execute_linea_precompile<DB: revm::Database>(
    ctx: &mut LineaCtx<'_, DB>,
    inputs: &mut CallInputs,
) -> Option<CallOutcome> {
    // let addr = inputs.target_address;
    
    // // Check if address is in the precompile range (0x01-0x09)
    // let addr_bytes = addr.into_array();
    // let is_precompile_range = addr_bytes[0..19] == [0; 19] && (1..=9).contains(&addr_bytes[19]);
    
    // // All addresses in 0x01-0x09 range are disallowed as transaction recipients
    // if is_precompile_range {
    //     return Some(CallOutcome::new(
    //         InterpreterResult::new(
    //             revm::interpreter::InstructionResult::Revert,
    //             bytes!(),
    //             Gas::new(inputs.gas_limit),
    //         ),
    //         inputs.return_memory_offset.clone(),
    //     ));
    // }

    // RIPEMD-160 (0x03) and BLAKE2f (0x09) are explicitly disallowed
    match inputs.target_address {
        addr if addr == *RIPEMD160.address() || addr == *FUN.address() => {
            Some(CallOutcome::new(
                InterpreterResult::new(
                    revm::interpreter::InstructionResult::Revert,
                    bytes!(),
                    Gas::new(inputs.gas_limit),
                ),
                inputs.return_memory_offset.clone(),
            ))
        },
        addr if addr == *MODEXP.address() => {
            // MODEXP only supports arguments (base, exponent, modulus) that do not exceed 512-byte integers
            const MAX_SIZE: usize = 512;
            
            // Parse the input to extract lengths
            let input = inputs.input.bytes(ctx);
            
            // Need at least 96 bytes for the header (3x32 bytes for lengths)
            if input.len() < 96 {
                // Let the precompile handle the error
                return None;
            }
            
            // Extract lengths from the input
            let mut base_len_bytes = [0u8; 32];
            base_len_bytes.copy_from_slice(&input[0..32]);
            let base_len = revm::primitives::U256::from_be_bytes(base_len_bytes);
            
            let mut exp_len_bytes = [0u8; 32];
            exp_len_bytes.copy_from_slice(&input[32..64]);
            let exp_len = revm::primitives::U256::from_be_bytes(exp_len_bytes);
            
            let mut mod_len_bytes = [0u8; 32];
            mod_len_bytes.copy_from_slice(&input[64..96]);
            let mod_len = revm::primitives::U256::from_be_bytes(mod_len_bytes);
            
            // Check if any of the arguments exceed 512 bytes
            if base_len > revm::primitives::U256::from(MAX_SIZE) 
                || exp_len > revm::primitives::U256::from(MAX_SIZE) 
                || mod_len > revm::primitives::U256::from(MAX_SIZE) {
                // Revert if any argument exceeds 512 bytes
                return Some(CallOutcome::new(
                    InterpreterResult::new(
                        revm::interpreter::InstructionResult::Revert,
                        bytes!(),
                        Gas::new(inputs.gas_limit),
                    ),
                    inputs.return_memory_offset.clone(),
                ));
            }
            
            // Arguments are within limits, allow the precompile to execute
            None
        }
        _ => None,
    }
}

// Manually implemented for linea
impl<DB: Database> Inspector<LineaCtx<'_, DB>> for CallTracer {
    fn call(
        &mut self,
        context: &mut LineaCtx<'_, DB>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let input_bytes = inputs.input.bytes(context);
        self.record_call_start(
            inputs.clone(),
            &input_bytes,
            &mut context.journaled_state.inner,
        );

        execute_linea_precompile(context, inputs)
    }
    fn call_end(
        &mut self,
        context: &mut LineaCtx<'_, DB>,
        _inputs: &CallInputs,
        _outcome: &mut CallOutcome,
    ) {
        self.journal = context.journaled_state.clone();
        self.record_call_end(&mut context.journaled_state.inner);
    }
    fn create_end(
        &mut self,
        context: &mut LineaCtx<'_, DB>,
        _inputs: &CreateInputs,
        _outcome: &mut CreateOutcome,
    ) {
        self.journal = context.journaled_state.clone();
    }
}
