use crate::{
    constants::PRECOMPILE_ADDRESS,
    db::{
        DatabaseCommit,
        DatabaseRef,
        multi_fork_db::MultiForkDb,
    },
    inspectors::{
        inspector_result_to_call_outcome,
        precompiles::{
            legacy::{
                assertion_adopter::get_assertion_adopter,
                calls::{
                    GetCallInputsError,
                    get_call_inputs,
                },
                console_log::ConsoleLogError,
                fork::{
                    ForkError,
                    fork_post_call,
                    fork_post_tx,
                    fork_pre_call,
                    fork_pre_tx,
                },
                get_logs::get_logs,
                load::{
                    LoadExternalSlotError,
                    load_external_slot,
                },
                state_changes::{
                    GetStateChangesError,
                    get_state_changes,
                },
            },
            reshiram::{
                ofac::{
                    OfacError,
                    revert_if_sanctioned,
                },
                tx_object::load_tx_object,
            },
        },
        sol_primitives::{
            PhEvm,
            console,
        },
        spec_recorder::AssertionSpec,
        tracer::CallTracer,
    },
    primitives::{
        Address,
        Bytes,
        FixedBytes,
        Journal,
        JournalEntry,
        JournalInner,
        TxEnv,
    },
};

use op_revm::OpContext;
use revm::{
    Inspector,
    context::ContextTr,
    interpreter::{
        CallInputs,
        CallOutcome,
        CallScheme,
    },
    primitives::Log,
};

use alloy_evm::eth::EthEvmContext;
use alloy_sol_types::SolCall;
use std::{
    collections::HashSet,
    sync::Arc,
};

/// Result of phevm precompile output.
/// Includes return data and gas deducted.
#[derive(Debug, Clone)]
pub struct PhevmOutcome {
    /// Resulting outcome of calling the precompile.
    bytes: Bytes,
    /// Gas spent calling the precompile.
    gas: u64,
}

impl PhevmOutcome {
    pub fn new(bytes: Bytes, gas: u64) -> Self {
        Self { bytes, gas }
    }

    pub fn gas(&self) -> u64 {
        self.gas
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn into_parts(self) -> (Bytes, u64) {
        (self.bytes, self.gas)
    }
}

impl From<Bytes> for PhevmOutcome {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes, 0)
    }
}

#[derive(Debug, Clone)]
pub struct LogsAndTraces<'a> {
    pub tx_logs: &'a [Log],
    pub call_traces: &'a CallTracer,
}

#[derive(Debug, Clone)]
pub struct PhEvmContext<'a> {
    pub logs_and_traces: &'a LogsAndTraces<'a>,
    pub adopter: Address,
    pub console_logs: Vec<String>,
    pub original_tx_env: &'a TxEnv,
    pub assertion_spec: AssertionSpec,
    pub ofac_sanctions: Arc<HashSet<Address>>,
}

impl<'a> PhEvmContext<'a> {
    #[must_use]
    pub fn new(
        logs_and_traces: &'a LogsAndTraces<'a>,
        adopter: Address,
        tx_env: &'a TxEnv,
        assertion_spec: AssertionSpec,
        ofac_sanctions: Arc<HashSet<Address>>,
    ) -> Self {
        Self {
            logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: tx_env,
            assertion_spec,
            ofac_sanctions,
        }
    }

    #[inline]
    #[must_use]
    pub fn post_tx_journal(&self) -> &JournalInner<JournalEntry> {
        &self.logs_and_traces.call_traces.journal
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrecompileError<ExtDb: DatabaseRef> {
    #[error("Precompile selector not found: {0:#?}")]
    SelectorNotFound(FixedBytes<4>),
    #[error("Precompile selector {selector:#?} not allowed under {spec:?} spec")]
    SelectorNotAllowed {
        selector: FixedBytes<4>,
        spec: AssertionSpec,
    },
    #[error("Unexpected error, should be Infallible: {0}")]
    UnexpectedError(#[source] std::convert::Infallible),
    #[error("Error getting state changes: {0}")]
    GetStateChangesError(#[source] GetStateChangesError),
    #[error("Error getting call inputs: {0}")]
    GetCallInputsError(#[source] GetCallInputsError),
    #[error("Error switching forks: {0}")]
    ForkError(#[source] ForkError),
    #[error("Error logging to console: {0}")]
    ConsoleLogError(#[source] ConsoleLogError),
    #[error("Error loading external slot: {0}")]
    LoadExternalSlotError(#[source] LoadExternalSlotError<ExtDb>),
    #[error("OFAC sanctions check failed: {0}")]
    OfacError(#[source] OfacError),
}

/// `PhEvmInspector` is an inspector for supporting the `PhEvm` precompiles.
#[derive(Debug, Clone)]
pub struct PhEvmInspector<'a> {
    pub context: PhEvmContext<'a>,
}

impl<'a> PhEvmInspector<'a> {
    /// Create a new `PhEvmInspector`.
    #[must_use]
    pub fn new(context: PhEvmContext<'a>) -> Self {
        PhEvmInspector { context }
    }

    /// Execute precompile functions for the `PhEvm`.
    ///
    /// # Errors
    ///
    /// Returns an error if the precompile selector is unknown or a precompile execution fails.
    pub fn execute_precompile<'db, ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db, CTX>(
        &mut self,
        context: &mut CTX,
        inputs: &mut CallInputs,
    ) -> Result<PhevmOutcome, PrecompileError<ExtDb>>
    where
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        let input_bytes = inputs.input.bytes(context);
        let selector: [u8; 4] = input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default();

        if !self.context.assertion_spec.allows_selector(selector) {
            return Err(PrecompileError::SelectorNotAllowed {
                selector: selector.into(),
                spec: self.context.assertion_spec.clone(),
            });
        }

        if let Some(outcome) =
            self.execute_fork_precompile::<ExtDb, CTX>(selector, context, inputs, &input_bytes)?
        {
            return Ok(outcome);
        }

        if let Some(outcome) = self.execute_call_inputs_precompile::<ExtDb, CTX>(
            selector,
            context,
            inputs,
            &input_bytes,
        )? {
            return Ok(outcome);
        }

        if let Some(outcome) =
            self.execute_misc_precompile::<ExtDb, CTX>(selector, context, inputs, &input_bytes)?
        {
            return Ok(outcome);
        }

        Err(PrecompileError::SelectorNotFound(selector.into()))
    }

    fn execute_fork_precompile<'db, ExtDb, CTX>(
        &mut self,
        selector: [u8; 4],
        context: &mut CTX,
        inputs: &mut CallInputs,
        input_bytes: &Bytes,
    ) -> Result<Option<PhevmOutcome>, PrecompileError<ExtDb>>
    where
        ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        let outcome = match selector {
            PhEvm::forkPreTxCall::SELECTOR => {
                fork_pre_tx(
                    context,
                    self.context.logs_and_traces.call_traces,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::ForkError)?
            }
            PhEvm::forkPostTxCall::SELECTOR => {
                fork_post_tx(
                    context,
                    self.context.logs_and_traces.call_traces,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::ForkError)?
            }
            PhEvm::forkPreCallCall::SELECTOR => {
                fork_pre_call(
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::ForkError)?
            }
            PhEvm::forkPostCallCall::SELECTOR => {
                fork_post_call(
                    context,
                    self.context.logs_and_traces.call_traces,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::ForkError)?
            }
            _ => return Ok(None),
        };

        Ok(Some(outcome))
    }

    fn execute_call_inputs_precompile<'db, ExtDb, CTX>(
        &self,
        selector: [u8; 4],
        context: &mut CTX,
        inputs: &mut CallInputs,
        input_bytes: &Bytes,
    ) -> Result<Option<PhevmOutcome>, PrecompileError<ExtDb>>
    where
        ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        let gas_limit = inputs.gas_limit;
        let outcome = match selector {
            PhEvm::getAllCallInputsCall::SELECTOR => {
                let call_inputs = Self::decode_call_inputs::<PhEvm::getAllCallInputsCall, ExtDb>(
                    &inputs.input.bytes(context),
                )?;
                self.run_get_call_inputs(call_inputs.target, call_inputs.selector, None, gas_limit)?
            }
            PhEvm::getCallInputsCall::SELECTOR => {
                let call_inputs =
                    Self::decode_call_inputs::<PhEvm::getCallInputsCall, ExtDb>(input_bytes)?;
                self.run_get_call_inputs(
                    call_inputs.target,
                    call_inputs.selector,
                    Some(CallScheme::Call),
                    gas_limit,
                )?
            }
            PhEvm::getStaticCallInputsCall::SELECTOR => {
                let call_inputs =
                    Self::decode_call_inputs::<PhEvm::getStaticCallInputsCall, ExtDb>(input_bytes)?;
                self.run_get_call_inputs(
                    call_inputs.target,
                    call_inputs.selector,
                    Some(CallScheme::StaticCall),
                    gas_limit,
                )?
            }
            PhEvm::getDelegateCallInputsCall::SELECTOR => {
                let call_inputs = Self::decode_call_inputs::<
                    PhEvm::getDelegateCallInputsCall,
                    ExtDb,
                >(input_bytes)?;
                self.run_get_call_inputs(
                    call_inputs.target,
                    call_inputs.selector,
                    Some(CallScheme::DelegateCall),
                    gas_limit,
                )?
            }
            PhEvm::getCallCodeInputsCall::SELECTOR => {
                let call_inputs =
                    Self::decode_call_inputs::<PhEvm::getCallCodeInputsCall, ExtDb>(input_bytes)?;
                self.run_get_call_inputs(
                    call_inputs.target,
                    call_inputs.selector,
                    Some(CallScheme::CallCode),
                    gas_limit,
                )?
            }
            _ => return Ok(None),
        };

        Ok(Some(outcome))
    }

    fn execute_misc_precompile<'db, ExtDb, CTX>(
        &mut self,
        selector: [u8; 4],
        context: &mut CTX,
        inputs: &mut CallInputs,
        input_bytes: &Bytes,
    ) -> Result<Option<PhevmOutcome>, PrecompileError<ExtDb>>
    where
        ExtDb: DatabaseRef + Clone + DatabaseCommit + 'db,
        CTX: ContextTr<
                Db = &'db mut MultiForkDb<ExtDb>,
                Journal = Journal<&'db mut MultiForkDb<ExtDb>>,
            >,
    {
        let outcome = match selector {
            PhEvm::loadCall::SELECTOR => {
                load_external_slot(context, inputs)
                    .map_err(PrecompileError::LoadExternalSlotError)?
            }
            PhEvm::getLogsCall::SELECTOR => {
                get_logs(&self.context, inputs.gas_limit)
                    .map_err(PrecompileError::UnexpectedError)?
            }
            PhEvm::getStateChangesCall::SELECTOR => {
                self.run_get_state_changes::<ExtDb>(input_bytes, inputs.gas_limit)?
            }
            PhEvm::getAssertionAdopterCall::SELECTOR => {
                get_assertion_adopter(&self.context).map_err(PrecompileError::UnexpectedError)?
            }
            PhEvm::getTxObjectCall::SELECTOR => load_tx_object(&self.context, inputs.gas_limit),
            PhEvm::revertIfSanctionedCall::SELECTOR => {
                revert_if_sanctioned(
                    input_bytes,
                    inputs.gas_limit,
                    self.context.ofac_sanctions.as_ref(),
                )
                .map_err(PrecompileError::OfacError)?
            }
            console::logCall::SELECTOR => {
                #[cfg(feature = "phoundry")]
                return Ok(Some(
                    crate::inspectors::precompiles::legacy::console_log::console_log(
                        input_bytes,
                        &mut self.context,
                    )
                    .map(PhevmOutcome::from)
                    .map_err(PrecompileError::ConsoleLogError),
                )?);

                #[cfg(not(feature = "phoundry"))]
                return Ok(Some(PhevmOutcome::from(Bytes::default())));
            }
            _ => return Ok(None),
        };

        Ok(Some(outcome))
    }

    fn decode_call_inputs<Call, ExtDb: DatabaseRef>(
        input_bytes: &Bytes,
    ) -> Result<Call, PrecompileError<ExtDb>>
    where
        Call: SolCall,
    {
        Call::abi_decode(input_bytes).map_err(|err| {
            PrecompileError::GetCallInputsError(
                GetCallInputsError::FailedToDecodeGetCallInputsCall(err),
            )
        })
    }

    fn run_get_call_inputs<ExtDb: DatabaseRef + Clone + DatabaseCommit>(
        &self,
        target: Address,
        selector: FixedBytes<4>,
        scheme: Option<CallScheme>,
        gas_limit: u64,
    ) -> Result<PhevmOutcome, PrecompileError<ExtDb>> {
        match get_call_inputs(&self.context, target, selector, scheme, gas_limit) {
            Ok(rax) | Err(GetCallInputsError::OutOfGas(rax)) => Ok(rax),
            Err(err) => Err(PrecompileError::GetCallInputsError(err)),
        }
    }

    fn run_get_state_changes<ExtDb: DatabaseRef + Clone + DatabaseCommit>(
        &self,
        input_bytes: &Bytes,
        gas_limit: u64,
    ) -> Result<PhevmOutcome, PrecompileError<ExtDb>> {
        match get_state_changes(input_bytes, &self.context, gas_limit) {
            Ok(rax) | Err(GetStateChangesError::OutOfGas(rax)) => Ok(rax),
            Err(err) => Err(PrecompileError::GetStateChangesError(err)),
        }
    }
}

/// Macro to implement Inspector trait for multiple context types.
/// This avoids duplicating the implementation and provides better maintainability.
macro_rules! impl_phevm_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<ExtDb: DatabaseRef + Clone + DatabaseCommit> Inspector<$context_type> for PhEvmInspector<'_> {
                fn call(&mut self, context: &mut $context_type, inputs: &mut CallInputs) -> Option<CallOutcome> {
                    if inputs.target_address == PRECOMPILE_ADDRESS {
                        let call_outcome = inspector_result_to_call_outcome(
                            self.execute_precompile(context, inputs),
                            inputs.gas_limit,
                            inputs.return_memory_offset.clone(),
                        );

                        return Some(call_outcome);
                    }
                    None
                }
            }
        )*
    };
}

// Implement Inspector for both context types using the macro
impl_phevm_inspector!(
    EthEvmContext<&mut MultiForkDb<ExtDb>>,
    OpContext<&mut MultiForkDb<ExtDb>>,
);

#[cfg(test)]
mod test {
    use crate::{
        inspectors::{
            sol_primitives::Error,
            spec_recorder::AssertionSpec,
        },
        test_utils::{
            run_precompile_test,
            run_precompile_test_with_spec,
        },
    };
    use alloy_sol_types::SolError;

    #[tokio::test]
    async fn test_invalid_selector_error_encoding() {
        let result = run_precompile_test("TestInvalidCall");
        assert!(!result.is_valid());
        assert_eq!(result.assertions_executions.len(), 1);
        let assertion_contract_execution = &result.assertions_executions[0];
        assert_eq!(assertion_contract_execution.assertion_fns_results.len(), 1);
        let assertion_fn_result = &assertion_contract_execution.assertion_fns_results[0];

        match &assertion_fn_result.result {
            crate::primitives::AssertionFunctionExecutionResult::AssertionExecutionResult(
                result,
            ) => {
                assert!(!result.is_success());
                let data = result.clone().into_output().unwrap();

                let error_string =
                    Error::abi_decode(data.iter().as_slice()).expect("Failed to decode error");

                assert_eq!(error_string.0, "Precompile selector not found: 0x1dcc85ae");
            }
            crate::primitives::AssertionFunctionExecutionResult::AssertionContractDeployFailure(
                _,
            ) => {
                panic!("Expected AssertionExecutionResult(_), got: {assertion_fn_result:#?}");
            }
        }
    }

    #[tokio::test]
    async fn test_legacy_spec_allows_legacy_precompiles() {
        let result = run_precompile_test_with_spec("TestSpecLegacy", AssertionSpec::Legacy);
        assert!(
            result.is_valid(),
            "Legacy precompiles should work under Legacy spec"
        );
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_get_tx_object() {
        let result = run_precompile_test_with_spec("TestSpecForbidTxObject", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "getTxObject should be forbidden under Legacy spec"
        );

        // Verify we get the SelectorNotAllowed error
        let assertion_fn_result = &result.assertions_executions[0].assertion_fns_results[0];
        match &assertion_fn_result.result {
            crate::primitives::AssertionFunctionExecutionResult::AssertionExecutionResult(
                result,
            ) => {
                assert!(!result.is_success());
                let data = result.clone().into_output().unwrap();
                let error_string =
                    Error::abi_decode(data.iter().as_slice())
                        .expect("Failed to decode error");
                assert!(
                    error_string.0.contains("not allowed under"),
                    "Error should mention spec restriction, got: {}",
                    error_string.0
                );
            }
            other @ crate::primitives::AssertionFunctionExecutionResult::AssertionContractDeployFailure(_) => panic!("Expected AssertionExecutionResult, got: {other:#?}"),
        }
    }

    #[tokio::test]
    async fn test_legacy_spec_forbids_ofac_check() {
        let result = run_precompile_test_with_spec("TestSpecForbidOfac", AssertionSpec::Legacy);
        assert!(
            !result.is_valid(),
            "revertIfSanctioned should be forbidden under Legacy spec"
        );

        let assertion_fn_result = &result.assertions_executions[0].assertion_fns_results[0];
        match &assertion_fn_result.result {
            crate::primitives::AssertionFunctionExecutionResult::AssertionExecutionResult(
                result,
            ) => {
                assert!(!result.is_success());
                let data = result.clone().into_output().unwrap();
                let error_string =
                    Error::abi_decode(data.iter().as_slice())
                        .expect("Failed to decode error");
                assert!(
                    error_string.0.contains("not allowed under"),
                    "Error should mention spec restriction, got: {}",
                    error_string.0
                );
            }
            other @ crate::primitives::AssertionFunctionExecutionResult::AssertionContractDeployFailure(_) => panic!("Expected AssertionExecutionResult, got: {other:#?}"),
        }
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_legacy_precompiles() {
        let result = run_precompile_test_with_spec("TestSpecLegacy", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "Legacy precompiles should work under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_get_tx_object() {
        let result =
            run_precompile_test_with_spec("TestSpecForbidTxObject", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "getTxObject should be allowed under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_ofac_check() {
        let result = run_precompile_test_with_spec("TestSpecForbidOfac", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "revertIfSanctioned should be allowed under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_reshiram_ofac_check_allows_unsanctioned_address() {
        let result = run_precompile_test_with_spec("TestOfacAllowed", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "Unsanctioned addresses should pass revertIfSanctioned"
        );
    }

    #[tokio::test]
    async fn test_reshiram_ofac_check_reverts_for_sanctioned_address() {
        let result = run_precompile_test_with_spec("TestOfacSanctioned", AssertionSpec::Reshiram);
        assert!(
            !result.is_valid(),
            "Sanctioned addresses should fail revertIfSanctioned"
        );

        let assertion_fn_result = &result.assertions_executions[0].assertion_fns_results[0];
        match &assertion_fn_result.result {
            crate::primitives::AssertionFunctionExecutionResult::AssertionExecutionResult(
                result,
            ) => {
                assert!(!result.is_success());
                let data = result.clone().into_output().unwrap();
                let error_string =
                    Error::abi_decode(data.iter().as_slice())
                        .expect("Failed to decode error");
                assert!(
                    error_string.0.contains("OFAC sanctions list"),
                    "Error should mention OFAC sanctions list, got: {}",
                    error_string.0
                );
            }
            other @ crate::primitives::AssertionFunctionExecutionResult::AssertionContractDeployFailure(_) => panic!("Expected AssertionExecutionResult, got: {other:#?}"),
        }
    }

    #[tokio::test]
    async fn test_reshiram_spec_allows_mixed_precompiles() {
        let result =
            run_precompile_test_with_spec("TestSpecReshiramWithLegacy", AssertionSpec::Reshiram);
        assert!(
            result.is_valid(),
            "Both Legacy and Reshiram precompiles should work under Reshiram spec"
        );
    }

    #[tokio::test]
    async fn test_experimental_spec_allows_legacy_precompiles() {
        let result = run_precompile_test_with_spec("TestSpecLegacy", AssertionSpec::Experimental);
        assert!(
            result.is_valid(),
            "Legacy precompiles should work under Experimental spec"
        );
    }

    #[tokio::test]
    async fn test_experimental_spec_allows_get_tx_object() {
        let result =
            run_precompile_test_with_spec("TestSpecForbidTxObject", AssertionSpec::Experimental);
        assert!(
            result.is_valid(),
            "getTxObject should be allowed under Experimental spec"
        );
    }

    #[tokio::test]
    async fn test_experimental_spec_allows_mixed_precompiles() {
        let result = run_precompile_test_with_spec(
            "TestSpecReshiramWithLegacy",
            AssertionSpec::Experimental,
        );
        assert!(
            result.is_valid(),
            "All precompiles should work under Experimental spec"
        );
    }

    #[tokio::test]
    async fn test_default_spec_allows_legacy_precompiles() {
        // run_precompile_test uses default spec (Legacy)
        let result = run_precompile_test("TestSpecLegacy");
        assert!(
            result.is_valid(),
            "Legacy precompiles should work with default (Legacy) spec"
        );
    }

    #[tokio::test]
    async fn test_default_spec_forbids_get_tx_object() {
        // run_precompile_test uses default spec (Legacy)
        let result = run_precompile_test("TestSpecForbidTxObject");
        assert!(
            !result.is_valid(),
            "getTxObject should be forbidden with default (Legacy) spec"
        );
    }
}
