//! `spec_recorder` is used to get what assertion spec the assertion has selected, and to return this data to the executor.
//!
//! It is a simple inspector with a `call` implementation, where when the phevm precompile address is called,
//! with the `registerAssertionSpec` function, will store the desired assertion spec.

use crate::{
    inspectors::{
        inspector_result_to_call_outcome,
        phevm::PhevmOutcome,
    },
    primitives::{
        Address,
        address,
    },
};
use alloy_evm::eth::EthEvmContext;
use op_revm::OpContext;
use revm::{
    Inspector,
    database::Database,
    interpreter::{
        CallInputs,
        CallOutcome,
    },
};

/// Address of the spec recorder precompile.
// address(uint160(uint256(keccak256("cats dining table"))))
pub const SPEC_ADDRESS: Address = address!("984c47F4eE1770FBb8BbA655C058034652f48359");

/// The assertion spec defines what subset of precompiles to expose during phevm execution.
#[derive(Debug, Clone)]
pub enum AssertionSpec {
    /// Standard set of `PhEvm` precompiles available at launch.
    Legacy,
    /// Unrestricted access to all available precompiles. May be untested and dangerous.
    /// Proceed with care.
    Experimental,
}

/// `AssertionSpecRecored` is an inspector used on assertion deployment for assertions to
/// select their desired assertion spec.
///
/// The assertion spec defines what subset of assertion precompiles you will have access to.
#[derive(Debug, Clone)]
pub struct AssertionSpecRecorder {
    pub context: Option<AssertionSpec>,
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
                        unimplemented!()
                    }
                    None
                }
            }
        )*
    };
}

impl_assertion_spec_inspector!(EthEvmContext<DB>, OpContext<DB>,);
