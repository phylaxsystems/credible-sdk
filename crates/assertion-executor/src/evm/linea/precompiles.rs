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
    db::{
        Database,
        DatabaseCommit,
        DatabaseRef,
        MultiForkDb,
    },
    evm::linea::evm::LineaCtx,
    inspectors::{
        CallTracer,
        PhEvmInspector,
    },
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

/// Called inside of an inspector to act as if we were calling into
/// linea precompiles.
///
/// The outcomes depending on the precompile are as follows:
/// - **BLAKE2f** - Always revert
/// - **MODEXP** - None (meaning pass) if args (base, exponent, modulus) are individually less than 512 bytes, otherwise revert
/// - **Precompiles as transaction recipients** - N/A here, FIXME: this should be handled elsewhere as we cannot revert before we execute a call within an inspector.
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
        }
        addr if addr == *MODEXP.address() => {
            // MODEXP only supports arguments (base, exponent, modulus) that do not exceed 512-byte integers
            const LINEA_INPUT_SIZE_LIMIT: usize = 512;

            // Parse the input to extract lengths using the same method as REVM
            let input = inputs.input.bytes(ctx);

            // Need at least 96 bytes for the header (3x32 bytes for lengths)
            if input.len() < 96 {
                // Let the precompile handle the error
                return None;
            }

            // Extract the header
            let base_len = revm::primitives::U256::from_be_bytes(
                input
                    .get(0..32)
                    .unwrap_or(&[0u8; 32])
                    .try_into()
                    .unwrap_or([0u8; 32]),
            );
            let exp_len = revm::primitives::U256::from_be_bytes(
                input
                    .get(32..64)
                    .unwrap_or(&[0u8; 32])
                    .try_into()
                    .unwrap_or([0u8; 32]),
            );
            let mod_len = revm::primitives::U256::from_be_bytes(
                input
                    .get(64..96)
                    .unwrap_or(&[0u8; 32])
                    .try_into()
                    .unwrap_or([0u8; 32]),
            );

            let base_len = usize::try_from(base_len).unwrap_or(usize::MAX);
            let mod_len = usize::try_from(mod_len).unwrap_or(usize::MAX);
            let exp_len = usize::try_from(exp_len).unwrap_or(usize::MAX);

            // Check size limits like REVM does for OSAKA, but with our 512-byte limit
            if base_len > LINEA_INPUT_SIZE_LIMIT
                || mod_len > LINEA_INPUT_SIZE_LIMIT
                || exp_len > LINEA_INPUT_SIZE_LIMIT
            {
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

// Manually implemented for linea PhEvmInspector
impl<ExtDb: DatabaseRef + Clone + DatabaseCommit> Inspector<LineaCtx<'_, MultiForkDb<ExtDb>>>
    for PhEvmInspector<'_>
{
    fn call(
        &mut self,
        context: &mut LineaCtx<'_, MultiForkDb<ExtDb>>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        use crate::{
            constants::PRECOMPILE_ADDRESS,
            inspectors::inspector_result_to_call_outcome,
        };

        // First check for PhEvm precompiles
        if inputs.target_address == PRECOMPILE_ADDRESS {
            let call_outcome = inspector_result_to_call_outcome(
                self.execute_precompile(context, inputs),
                Gas::new(inputs.gas_limit),
                inputs.return_memory_offset.clone(),
            );
            return Some(call_outcome);
        }

        // Then check for Linea-specific precompile behavior
        execute_linea_precompile(context, inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::MultiForkDb,
        evm::{
            build_evm::evm_env,
            linea::evm::LineaCtx,
        },
        primitives::{
            AccountInfo,
            BlockEnv,
            Journal,
            SpecId,
            TxEnv,
            keccak256,
        },
    };
    use revm::{
        JournalEntry,
        context::{
            Context,
            JournalInner,
            LocalContext,
        },
        database::InMemoryDB,
        interpreter::{
            CallInput,
            CallScheme,
            CallValue,
        },
        primitives::{
            Address,
            Bytes,
            U256,
        },
    };
    use std::marker::PhantomData;

    fn create_test_context() -> (
        MultiForkDb<InMemoryDB>,
        LineaCtx<'static, MultiForkDb<InMemoryDB>>,
    ) {
        let mut db = InMemoryDB::default();

        // Insert a test account with balance
        db.insert_account_info(
            Address::from([0x01; 20]),
            AccountInfo {
                nonce: 0,
                balance: U256::MAX,
                code_hash: keccak256([]),
                code: None,
            },
        );

        let env = evm_env(1, SpecId::CANCUN, BlockEnv::default());
        let spec = env.cfg_env.spec;

        let multi_fork_db = MultiForkDb::new(db, &JournalInner::new());

        // Create context - need to leak to get 'static lifetime
        let multi_fork_db_ptr: *mut MultiForkDb<InMemoryDB> = Box::leak(Box::new(multi_fork_db));

        unsafe {
            let context = Context {
                journaled_state: {
                    let mut journal = Journal::new_with_inner(
                        &mut *multi_fork_db_ptr,
                        JournalInner::<JournalEntry>::new(),
                    );
                    journal.set_spec_id(spec);
                    journal
                },
                block: env.block_env.clone(),
                cfg: env.cfg_env.clone(),
                tx: TxEnv::default(),
                chain: PhantomData,
                local: LocalContext::default(),
                error: Ok(()),
            };

            ((*multi_fork_db_ptr).clone(), context)
        }
    }

    fn create_call_inputs(target: Address, input: Bytes) -> CallInputs {
        CallInputs {
            input: CallInput::Bytes(input),
            gas_limit: 100_000,
            target_address: target,
            bytecode_address: target,
            caller: Address::from([0x01; 20]),
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        }
    }

    #[test]
    fn test_blake2f_precompile_reverts() {
        let (_db, mut ctx) = create_test_context();
        let blake2f_address = *FUN.address();
        let mut inputs = create_call_inputs(blake2f_address, Bytes::default());

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        assert!(result.is_some());
        let outcome = result.unwrap();
        assert_eq!(
            outcome.result.result,
            revm::interpreter::InstructionResult::Revert
        );
        assert_eq!(outcome.result.output, bytes!());
        assert_eq!(outcome.result.gas.remaining(), 100_000);
    }

    #[test]
    fn test_ripemd160_precompile_reverts() {
        let (_db, mut ctx) = create_test_context();
        let ripemd_address = *RIPEMD160.address();
        let mut inputs = create_call_inputs(ripemd_address, Bytes::default());

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        assert!(result.is_some());
        let outcome = result.unwrap();
        assert_eq!(
            outcome.result.result,
            revm::interpreter::InstructionResult::Revert
        );
        assert_eq!(outcome.result.output, bytes!());
        assert_eq!(outcome.result.gas.remaining(), 100_000);
    }

    #[test]
    fn test_modexp_with_small_args_passes() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with small argument sizes (< 512 bytes each)
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // base_len = 32
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // exp_len = 32
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // mod_len = 32
        // Add actual data (96 bytes total)
        input.extend_from_slice(&[0u8; 96]);

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        // Should return None, allowing the precompile to execute normally
        assert!(result.is_none());
    }

    #[test]
    fn test_modexp_with_large_base_reverts() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with base_len > 512 bytes
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(513).to_be_bytes::<32>()); // base_len = 513 (exceeds limit)
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // exp_len = 32
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // mod_len = 32

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        assert!(result.is_some());
        let outcome = result.unwrap();
        assert_eq!(
            outcome.result.result,
            revm::interpreter::InstructionResult::Revert
        );
        assert_eq!(outcome.result.gas.remaining(), 100_000);
    }

    #[test]
    fn test_modexp_with_large_exponent_reverts() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with exp_len > 512 bytes
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // base_len = 32
        input.extend_from_slice(&U256::from(1024).to_be_bytes::<32>()); // exp_len = 1024 (exceeds limit)
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // mod_len = 32

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        assert!(result.is_some());
        let outcome = result.unwrap();
        assert_eq!(
            outcome.result.result,
            revm::interpreter::InstructionResult::Revert
        );
    }

    #[test]
    fn test_modexp_with_large_modulus_reverts() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with mod_len > 512 bytes
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // base_len = 32
        input.extend_from_slice(&U256::from(32).to_be_bytes::<32>()); // exp_len = 32
        input.extend_from_slice(&U256::from(600).to_be_bytes::<32>()); // mod_len = 600 (exceeds limit)

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        assert!(result.is_some());
        let outcome = result.unwrap();
        assert_eq!(
            outcome.result.result,
            revm::interpreter::InstructionResult::Revert
        );
    }

    #[test]
    fn test_modexp_exactly_at_limit_passes() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with all arguments exactly at 512 bytes
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(512).to_be_bytes::<32>()); // base_len = 512 (at limit)
        input.extend_from_slice(&U256::from(512).to_be_bytes::<32>()); // exp_len = 512 (at limit)
        input.extend_from_slice(&U256::from(512).to_be_bytes::<32>()); // mod_len = 512 (at limit)

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        // Should return None since all arguments are at the limit (not exceeding)
        assert!(result.is_none());
    }

    #[test]
    fn test_modexp_with_short_input_passes() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Create input with less than 96 bytes (header incomplete)
        let input = vec![0u8; 50];

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        // Should return None and let the precompile handle the error
        assert!(result.is_none());
    }

    #[test]
    fn test_other_precompiles_pass() {
        let (_db, mut ctx) = create_test_context();

        // Test ecrecover (0x01)
        let ecrecover_address =
            Address::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        let mut inputs = create_call_inputs(ecrecover_address, Bytes::default());
        let result = execute_linea_precompile(&mut ctx, &mut inputs);
        assert!(result.is_none());

        // Test SHA256 (0x02)
        let sha256_address =
            Address::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
        let mut inputs = create_call_inputs(sha256_address, Bytes::default());
        let result = execute_linea_precompile(&mut ctx, &mut inputs);
        assert!(result.is_none());

        // Test identity (0x04)
        let identity_address =
            Address::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4]);
        let mut inputs = create_call_inputs(identity_address, Bytes::default());
        let result = execute_linea_precompile(&mut ctx, &mut inputs);
        assert!(result.is_none());
    }

    #[test]
    fn test_non_precompile_addresses_pass() {
        let (_db, mut ctx) = create_test_context();

        // Test regular contract address
        let contract_address = Address::from([
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
            0xde, 0xf0, 0x12, 0x34, 0x56, 0x78,
        ]);
        let mut inputs = create_call_inputs(contract_address, Bytes::default());

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        // Should return None for non-precompile addresses
        assert!(result.is_none());
    }

    #[test]
    fn test_modexp_edge_cases() {
        let (_db, mut ctx) = create_test_context();
        let modexp_address = *MODEXP.address();

        // Test with zero-length arguments
        let mut input = Vec::new();
        input.extend_from_slice(&U256::from(0).to_be_bytes::<32>()); // base_len = 0
        input.extend_from_slice(&U256::from(0).to_be_bytes::<32>()); // exp_len = 0
        input.extend_from_slice(&U256::from(0).to_be_bytes::<32>()); // mod_len = 0

        let mut inputs = create_call_inputs(modexp_address, Bytes::from(input));

        let result = execute_linea_precompile(&mut ctx, &mut inputs);

        // Should return None since all arguments are within limits
        assert!(result.is_none());
    }
}
