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

use revm::{interpreter::CallOutcome, Context};

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
/// - **MODEXP** - None if args (base, exponent, modulus) are individually less than 512 bytes, otherwise revert
/// - **Precompiles as transaction recipients** - Always revert
/// - **RIPEMD-160** - Always revert
pub fn execute_linea_precompile(ctx: Context) -> Option<CallOutcome> {
    unimplemented!()
}
