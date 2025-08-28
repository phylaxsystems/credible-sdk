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
};
use revm::{
    Inspector,
    interpreter::{
        CallInputs,
        CallOutcome,
        CreateInputs,
        CreateOutcome,
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
) -> Option<CallOutcome> {
    unimplemented!()
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

        execute_linea_precompile(context)
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
