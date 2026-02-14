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
            aggregate_facts::{
                AggregateFactsError,
                any_event,
                count_events,
                sum_call_arg_uint_by_address,
                sum_call_arg_uint_for_address,
                sum_event_data_uint,
                sum_event_uint_by_topic,
                sum_event_uint_for_topic_key,
                unique_call_arg_addresses,
                unique_event_topic_values,
            },
            assertion_adopter::get_assertion_adopter,
            call_boundary::{
                CallBoundaryError,
                all_calls_slot_delta_ge,
                all_calls_slot_delta_le,
                load_at_call,
                slot_delta_at_call,
                sum_calls_slot_delta,
            },
            call_facts::{
                CallFactsError,
                all_calls_by,
                any_call,
                caller_at,
                count_calls,
                get_touched_contracts,
                get_trigger_context,
                sum_arg_uint,
            },
            calls::{
                GetCallInputsError,
                get_call_inputs,
            },
            console_log::ConsoleLogError,
            erc20_facts::{
                Erc20FactsError,
                balance_diff,
                did_balance_change,
                erc20_balance_diff,
                erc20_supply_diff,
                get_erc20_flow_by_call,
                get_erc20_net_flow,
            },
            erc4626_facts::{
                Erc4626FactsError,
                erc20_allowance_at,
                erc20_allowance_at_call,
                erc20_allowance_delta_at_call,
                erc20_allowance_diff,
                erc20_balance_at,
                erc20_balance_delta_at_call,
                erc20_supply_at,
                erc20_supply_delta_at_call,
                erc4626_assets_per_share_diff_bps,
                erc4626_total_assets_delta_at_call,
                erc4626_total_assets_diff,
                erc4626_total_supply_delta_at_call,
                erc4626_total_supply_diff,
                erc4626_vault_asset_balance_delta_at_call,
                erc4626_vault_asset_balance_diff,
            },
            fork::{
                ForkError,
                fork_post_call,
                fork_post_tx,
                fork_pre_call,
                fork_pre_tx,
            },
            get_logs::{
                GetLogsError,
                get_logs,
            },
            load::{
                LoadExternalSlotError,
                load_external_slot,
            },
            slot_diffs::{
                SlotDiffsError,
                did_mapping_key_change,
                get_changed_slots,
                get_slot_diff,
                mapping_value_diff,
            },
            state_changes::{
                GetStateChangesError,
                get_state_changes,
            },
            tx_object::load_tx_object,
            write_policy::{
                WritePolicyError,
                all_slot_writes_by,
                any_slot_written,
            },
        },
        sol_primitives::{
            PhEvm,
            console,
        },
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

/// Index into `CallTracer.call_records`.
pub type TriggerCallId = usize;

#[derive(Debug, Clone)]
pub struct PhEvmContext<'a> {
    pub logs_and_traces: &'a LogsAndTraces<'a>,
    pub adopter: Address,
    pub console_logs: Vec<String>,
    pub original_tx_env: &'a TxEnv,
    /// The call ID (index into `CallTracer.call_records`) of the call that triggered
    /// this assertion. `None` if the trigger was not a call (e.g. storage/balance change).
    pub trigger_call_id: Option<TriggerCallId>,
}

impl<'a> PhEvmContext<'a> {
    #[must_use]
    pub fn new(
        logs_and_traces: &'a LogsAndTraces<'a>,
        adopter: Address,
        tx_env: &'a TxEnv,
    ) -> Self {
        Self {
            logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: tx_env,
            trigger_call_id: None,
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
    #[error("Unexpected error, should be Infallible: {0}")]
    UnexpectedError(#[source] std::convert::Infallible),
    #[error("Error getting logs: {0}")]
    GetLogsError(#[source] GetLogsError),
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
    #[error("Error in scalar call facts: {0}")]
    CallFactsError(#[source] CallFactsError),
    #[error("Error in write policy: {0}")]
    WritePolicyError(#[source] WritePolicyError),
    #[error("Error in call boundary: {0}")]
    CallBoundaryError(#[source] CallBoundaryError),
    #[error("Error in ERC20 facts: {0}")]
    Erc20FactsError(#[source] Erc20FactsError),
    #[error("Error in ERC4626 facts: {0}")]
    Erc4626FactsError(#[source] Erc4626FactsError),
    #[error("Error in slot diffs: {0}")]
    SlotDiffsError(#[source] SlotDiffsError),
    #[error("Error in aggregate facts: {0}")]
    AggregateFactsError(#[source] AggregateFactsError),
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
        let selector = input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default();

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

        // Fact/query precompiles are pure trace readers (no fork mutation).
        // Keep them after fork/call-input/misc dispatch for a clearer execution model.
        if let Some(outcome) =
            self.execute_fact_query_precompile::<ExtDb>(selector, inputs, &input_bytes)?
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
            PhEvm::loadAtCallCall::SELECTOR => {
                load_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::CallBoundaryError)?
            }
            PhEvm::slotDeltaAtCallCall::SELECTOR => {
                slot_delta_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::CallBoundaryError)?
            }
            PhEvm::allCallsSlotDeltaGECall::SELECTOR => {
                all_calls_slot_delta_ge(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::CallBoundaryError)?
            }
            PhEvm::allCallsSlotDeltaLECall::SELECTOR => {
                all_calls_slot_delta_le(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::CallBoundaryError)?
            }
            PhEvm::sumCallsSlotDeltaCall::SELECTOR => {
                sum_calls_slot_delta(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::CallBoundaryError)?
            }
            PhEvm::erc20BalanceAtCall::SELECTOR => {
                erc20_balance_at(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20SupplyAtCall::SELECTOR => {
                erc20_supply_at(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20AllowanceAtCall::SELECTOR => {
                erc20_allowance_at(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20AllowanceDiffCall::SELECTOR => {
                erc20_allowance_diff(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20BalanceDeltaAtCallCall::SELECTOR => {
                erc20_balance_delta_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20SupplyDeltaAtCallCall::SELECTOR => {
                erc20_supply_delta_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20AllowanceAtCallCall::SELECTOR => {
                erc20_allowance_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc20AllowanceDeltaAtCallCall::SELECTOR => {
                erc20_allowance_delta_at_call(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626TotalAssetsDiffCall::SELECTOR => {
                erc4626_total_assets_diff(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626TotalAssetsDeltaAtCallCall::SELECTOR => {
                erc4626_total_assets_delta_at_call(
                    context,
                    &self.context,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626TotalSupplyDiffCall::SELECTOR => {
                erc4626_total_supply_diff(context, &self.context, input_bytes, inputs.gas_limit)
                    .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626TotalSupplyDeltaAtCallCall::SELECTOR => {
                erc4626_total_supply_delta_at_call(
                    context,
                    &self.context,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626VaultAssetBalanceDiffCall::SELECTOR => {
                erc4626_vault_asset_balance_diff(
                    context,
                    &self.context,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626VaultAssetBalanceDeltaAtCallCall::SELECTOR => {
                erc4626_vault_asset_balance_delta_at_call(
                    context,
                    &self.context,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::Erc4626FactsError)?
            }
            PhEvm::erc4626AssetsPerShareDiffBpsCall::SELECTOR => {
                erc4626_assets_per_share_diff_bps(
                    context,
                    &self.context,
                    input_bytes,
                    inputs.gas_limit,
                )
                .map_err(PrecompileError::Erc4626FactsError)?
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
                get_logs(&self.context, inputs.gas_limit).map_err(PrecompileError::GetLogsError)?
            }
            PhEvm::getStateChangesCall::SELECTOR => {
                self.run_get_state_changes::<ExtDb>(input_bytes, inputs.gas_limit)?
            }
            PhEvm::getAssertionAdopterCall::SELECTOR => {
                get_assertion_adopter(&self.context).map_err(PrecompileError::UnexpectedError)?
            }
            PhEvm::getTxObjectCall::SELECTOR => load_tx_object(&self.context, inputs.gas_limit),
            console::logCall::SELECTOR => {
                #[cfg(feature = "phoundry")]
                return Ok(Some(
                    crate::inspectors::precompiles::console_log::console_log(
                        input_bytes,
                        &mut self.context,
                    )
                    .map(PhevmOutcome::from)
                    .map_err(PrecompileError::ConsoleLogError)?,
                ));

                #[cfg(not(feature = "phoundry"))]
                return Ok(Some(PhevmOutcome::from(Bytes::default())));
            }
            _ => return Ok(None),
        };

        Ok(Some(outcome))
    }

    /// Dispatches read-only query/fact precompiles.
    ///
    /// These selectors derive declarative facts from captured traces
    /// (calls, logs, slot diffs) and do not mutate fork state.
    #[allow(clippy::too_many_lines)]
    fn execute_fact_query_precompile<ExtDb: DatabaseRef>(
        &self,
        selector: [u8; 4],
        inputs: &CallInputs,
        input_bytes: &Bytes,
    ) -> Result<Option<PhevmOutcome>, PrecompileError<ExtDb>> {
        let gas_limit = inputs.gas_limit;
        let outcome = match selector {
            PhEvm::anyCall_0Call::SELECTOR => {
                any_call(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::anyCall_1Call::SELECTOR => {
                any_call(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::countCalls_0Call::SELECTOR => {
                count_calls(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::countCalls_1Call::SELECTOR => {
                count_calls(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::callerAtCall::SELECTOR => {
                caller_at(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::allCallsBy_0Call::SELECTOR => {
                all_calls_by(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::allCallsBy_1Call::SELECTOR => {
                all_calls_by(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::sumArgUint_0Call::SELECTOR => {
                sum_arg_uint(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::sumArgUint_1Call::SELECTOR => {
                sum_arg_uint(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::sumCallArgUintForAddressCall::SELECTOR => {
                sum_call_arg_uint_for_address(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::uniqueCallArgAddressesCall::SELECTOR => {
                unique_call_arg_addresses(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::sumCallArgUintByAddressCall::SELECTOR => {
                sum_call_arg_uint_by_address(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::sumEventUintForTopicKeyCall::SELECTOR => {
                sum_event_uint_for_topic_key(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::uniqueEventTopicValuesCall::SELECTOR => {
                unique_event_topic_values(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::sumEventUintByTopicCall::SELECTOR => {
                sum_event_uint_by_topic(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::getTouchedContractsCall::SELECTOR => {
                get_touched_contracts(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::countEventsCall::SELECTOR => {
                count_events(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::anyEventCall::SELECTOR => {
                any_event(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::sumEventDataUintCall::SELECTOR => {
                sum_event_data_uint(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::AggregateFactsError)?
            }
            PhEvm::anySlotWrittenCall::SELECTOR => {
                any_slot_written(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::WritePolicyError)?
            }
            PhEvm::allSlotWritesByCall::SELECTOR => {
                all_slot_writes_by(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::WritePolicyError)?
            }
            PhEvm::getTriggerContextCall::SELECTOR => {
                get_trigger_context(&self.context, gas_limit)
                    .map_err(PrecompileError::CallFactsError)?
            }
            PhEvm::erc20BalanceDiffCall::SELECTOR => {
                erc20_balance_diff(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::erc20SupplyDiffCall::SELECTOR => {
                erc20_supply_diff(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::getERC20NetFlowCall::SELECTOR => {
                get_erc20_net_flow(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::getERC20FlowByCallCall::SELECTOR => {
                get_erc20_flow_by_call(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::didBalanceChangeCall::SELECTOR => {
                did_balance_change(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::balanceDiffCall::SELECTOR => {
                balance_diff(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::Erc20FactsError)?
            }
            PhEvm::getChangedSlotsCall::SELECTOR => {
                get_changed_slots(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::SlotDiffsError)?
            }
            PhEvm::getSlotDiffCall::SELECTOR => {
                get_slot_diff(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::SlotDiffsError)?
            }
            PhEvm::didMappingKeyChangeCall::SELECTOR => {
                did_mapping_key_change(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::SlotDiffsError)?
            }
            PhEvm::mappingValueDiffCall::SELECTOR => {
                mapping_value_diff(&self.context, input_bytes, gas_limit)
                    .map_err(PrecompileError::SlotDiffsError)?
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
    use crate::test_utils::run_precompile_test;

    use crate::inspectors::sol_primitives::Error;
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
}
