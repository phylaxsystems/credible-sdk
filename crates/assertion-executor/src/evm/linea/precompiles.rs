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
