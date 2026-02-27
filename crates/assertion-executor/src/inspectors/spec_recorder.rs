//! `spec_recorder` is used to get what assertion spec the assertion has selected, and to return this data to the executor.
//!
//! It is a simple inspector with a `call` implementation, where when the phevm precompile address is called,
//! with the `registerAssertionSpec` function, will store the desired assertion spec.

use crate::{
    inspectors::{
        inspector_result_to_call_outcome,
        phevm::PhevmOutcome,
        sol_primitives::{
            self,
            ISpecRecorder,
        },
    },
    primitives::{
        Address,
        Bytes,
        address,
    },
};
use alloy_evm::eth::EthEvmContext;
use alloy_sol_types::SolCall;
use op_revm::OpContext;
use revm::{
    Inspector,
    database::Database,
    interpreter::{
        CallInputs,
        CallOutcome,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::ops::Range;

/// Address of the spec recorder precompile.
// address(uint160(uint256(keccak256("SpecRecorder"))))
pub const SPEC_ADDRESS: Address = address!("3cf4a3c0a0af502eef5907cc92584b70f01d686d");

/// The assertion spec defines what subset of precompiles to expose during phevm execution.
///
/// All new specs derive and expose all precompiles from the old definitions, unless specified
/// otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AssertionSpec {
    /// Standard set of `PhEvm` precompiles available at launch.
    Legacy,
    /// Contains tx object precompiles.
    Reshiram,
    /// Unrestricted access to all available precompiles. May be untested and dangerous.
    /// Proceed with care.
    Experimental,
}

impl AssertionSpec {
    /// Returns `true` if the given precompile selector is allowed under this spec.
    ///
    /// Each spec is a superset of the previous one:
    /// - `Legacy`: all precompiles except Reshiram-only selectors
    /// - `Reshiram`: all precompiles except Experimental-only selectors
    /// - `Experimental`: unrestricted
    #[must_use]
    pub fn allows_selector(&self, selector: [u8; 4]) -> bool {
        match self {
            Self::Experimental => true,
            Self::Reshiram => !Self::is_experimental_only(selector),
            Self::Legacy => {
                !Self::is_reshiram_only(selector) && !Self::is_experimental_only(selector)
            }
        }
    }

    fn is_reshiram_only(selector: [u8; 4]) -> bool {
        use crate::inspectors::sol_primitives::PhEvm;
        use alloy_sol_types::SolCall;
        matches!(selector, PhEvm::getTxObjectCall::SELECTOR)
    }

    fn is_experimental_only(_selector: [u8; 4]) -> bool {
        false
    }
}

/// `AssertionSpecRecorder` is an inspector used on assertion deployment for assertions to
/// select their desired assertion spec.
///
/// The assertion spec defines what subset of assertion precompiles you will have access to.
#[derive(Debug, Clone, Default)]
pub struct AssertionSpecRecorder {
    pub context: Option<AssertionSpec>,
}

#[derive(thiserror::Error, Debug)]
pub enum RecordError {
    #[error("Failed to decode call inputs")]
    CallDecodeError(#[source] alloy_sol_types::Error),
    #[error("Fn selector not found")]
    FnSelectorNotFound,
    #[error("Assertion Spec already set")]
    SpecAlreadySet,
}

impl AssertionSpecRecorder {
    /// Records the assertion spec from a call to `registerAssertionSpec`.
    ///
    /// Returns a [`CallOutcome`] that reverts the call frame if the spec
    /// is already set, decoding fails, or the selector is not recognized.
    #[must_use]
    pub fn record_spec(
        &mut self,
        input_bytes: &[u8],
        gas_limit: u64,
        memory_offset: Range<usize>,
    ) -> CallOutcome {
        if self.context.is_some() {
            return inspector_result_to_call_outcome(
                Err::<PhevmOutcome, _>(RecordError::SpecAlreadySet),
                gas_limit,
                memory_offset,
            );
        }

        let result = match input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default()
        {
            ISpecRecorder::registerAssertionSpecCall::SELECTOR => {
                match ISpecRecorder::registerAssertionSpecCall::abi_decode(input_bytes) {
                    Ok(call) => {
                        match call.spec {
                            sol_primitives::AssertionSpec::Legacy => {
                                self.context = Some(AssertionSpec::Legacy);
                                Ok(Bytes::new())
                            }
                            sol_primitives::AssertionSpec::Reshiram => {
                                self.context = Some(AssertionSpec::Reshiram);
                                Ok(Bytes::new())
                            }
                            sol_primitives::AssertionSpec::Experimental => {
                                self.context = Some(AssertionSpec::Experimental);
                                Ok(Bytes::new())
                            }
                            sol_primitives::AssertionSpec::__Invalid => {
                                Err(RecordError::FnSelectorNotFound)
                            }
                        }
                    }
                    Err(e) => Err(RecordError::CallDecodeError(e)),
                }
            }
            _ => Err(RecordError::FnSelectorNotFound),
        };

        inspector_result_to_call_outcome(result.map(PhevmOutcome::from), gas_limit, memory_offset)
    }
}

macro_rules! impl_assertion_spec_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<DB: Database> Inspector<$context_type> for AssertionSpecRecorder {
                fn call(
                    &mut self,
                    context: &mut $context_type,
                    inputs: &mut CallInputs,
                ) -> Option<CallOutcome> {
                    if inputs.target_address == SPEC_ADDRESS {
                        let input_bytes = inputs.input.bytes(context);
                        return Some(self.record_spec(
                            &input_bytes,
                            inputs.gas_limit,
                            inputs.return_memory_offset.clone(),
                        ));
                    }
                    None
                }
            }
        )*
    };
}

impl_assertion_spec_inspector!(EthEvmContext<DB>, OpContext<DB>,);
