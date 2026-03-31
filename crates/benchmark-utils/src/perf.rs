use std::str::FromStr;

use assertion_executor::{
    AssertionExecutor,
    BenchmarkAssertionSetupStats,
    ExecuteForkedTxResult,
    db::DatabaseCommit,
    evm::build_evm::{
        build_eth_evm,
        evm_env,
    },
    primitives::{
        Address,
        BlockEnv,
        ExecutionResult,
        FixedBytes,
        TxEnv,
        U256,
    },
    store::AssertionsForExecution,
};
use revm::{
    ExecuteEvm,
    context_interface::result::SuccessReason,
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
};

use crate::{
    BenchmarkDb,
    BenchmarkPackage,
    BenchmarkPackageError,
    LoadDefinition,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkMode {
    Vanilla,
    TraceOnly,
    StoreReadOnly,
    AssertionSetupOnly,
    AssertionsOnly,
    Full,
}

impl BenchmarkMode {
    pub const ALL: [Self; 6] = [
        Self::Vanilla,
        Self::TraceOnly,
        Self::StoreReadOnly,
        Self::AssertionSetupOnly,
        Self::AssertionsOnly,
        Self::Full,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vanilla => "vanilla",
            Self::TraceOnly => "trace_only",
            Self::StoreReadOnly => "store_read_only",
            Self::AssertionSetupOnly => "assertion_setup_only",
            Self::AssertionsOnly => "assertions_only",
            Self::Full => "full",
        }
    }
}

impl FromStr for BenchmarkMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "vanilla" => Ok(Self::Vanilla),
            "trace_only" => Ok(Self::TraceOnly),
            "store_read_only" => Ok(Self::StoreReadOnly),
            "assertion_setup_only" => Ok(Self::AssertionSetupOnly),
            "assertions_only" => Ok(Self::AssertionsOnly),
            "full" => Ok(Self::Full),
            _ => Err("unknown benchmark mode"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkPreset {
    AvgBlock0Aa,
    AvgBlock100Aa,
    Erc20TransactionAa,
    UniswapTransactionAa,
}

impl BenchmarkPreset {
    pub const ALL: [Self; 4] = [
        Self::AvgBlock0Aa,
        Self::AvgBlock100Aa,
        Self::Erc20TransactionAa,
        Self::UniswapTransactionAa,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AvgBlock0Aa => "avg_block_0_aa",
            Self::AvgBlock100Aa => "avg_block_100_aa",
            Self::Erc20TransactionAa => "erc20_transaction_aa",
            Self::UniswapTransactionAa => "uniswap_transaction_aa",
        }
    }

    #[must_use]
    pub fn load_definition(self) -> LoadDefinition {
        match self {
            Self::AvgBlock0Aa => LoadDefinition::default(),
            Self::AvgBlock100Aa => {
                LoadDefinition {
                    aa_percent: 100.0,
                    ..LoadDefinition::default()
                }
            }
            Self::Erc20TransactionAa => {
                LoadDefinition {
                    tx_amount: 1,
                    eoa_percent: 0.0,
                    erc20_percent: 100.0,
                    uni_percent: 0.0,
                    aa_percent: 100.0,
                }
            }
            Self::UniswapTransactionAa => {
                LoadDefinition {
                    tx_amount: 1,
                    eoa_percent: 0.0,
                    erc20_percent: 0.0,
                    uni_percent: 100.0,
                    aa_percent: 100.0,
                }
            }
        }
    }
}

impl FromStr for BenchmarkPreset {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "avg_block_0_aa" => Ok(Self::AvgBlock0Aa),
            "avg_block_100_aa" => Ok(Self::AvgBlock100Aa),
            "erc20_transaction_aa" => Ok(Self::Erc20TransactionAa),
            "uniswap_transaction_aa" => Ok(Self::UniswapTransactionAa),
            _ => Err("unknown benchmark preset"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkPhaseStats {
    pub transactions: usize,
    pub matched_assertion_contracts: usize,
    pub matched_assertion_selectors: usize,
    pub executed_assertion_functions: usize,
}

impl BenchmarkPhaseStats {
    fn accumulate_matches(&mut self, assertions: &[AssertionsForExecution]) {
        self.matched_assertion_contracts += assertions.len();
        self.matched_assertion_selectors +=
            assertions.iter().map(|a| a.selectors.len()).sum::<usize>();
    }
}

fn usize_from_u64(value: u64) -> usize {
    usize::try_from(value).expect("benchmark counters should fit in usize")
}

fn u32_from_usize(value: usize) -> u32 {
    u32::try_from(value).expect("synthetic benchmark fanout should fit in u32")
}

#[derive(Debug, Clone)]
struct PreparedStoreReadCase {
    // Store-read benchmarks should only pay the assertion-selection lookup, so the
    // traced tx is captured up front and replayed against the store in isolation.
    call_tracer: assertion_executor::inspectors::CallTracer,
}

#[derive(Debug, Clone)]
struct PreparedAssertionCase {
    // Assertion phase benchmarks need both the pre-tx fork state and the traced tx
    // outputs so they can replay setup/execution without re-running fixture loading.
    tx_env: TxEnv,
    pre_tx_fork_db: BenchmarkDb,
    forked_tx_result: ExecuteForkedTxResult,
    matched_assertions: Vec<AssertionsForExecution>,
}

#[derive(Debug)]
pub struct PreparedStoreReadPackage {
    // Prepared packages snapshot the expensive tx execution once and then let the
    // bench iterate only the phase being measured.
    executor: AssertionExecutor,
    block_number: U256,
    cases: Vec<PreparedStoreReadCase>,
}

#[derive(Debug)]
pub struct PreparedAssertionPhasePackage {
    // Assertion setup and assertion execution are measured separately but share the
    // same traced transaction inputs, so both phases read from the same prepared cases.
    executor: AssertionExecutor,
    block_env: BlockEnv,
    cases: Vec<PreparedAssertionCase>,
}

#[derive(Debug, Clone)]
pub struct SyntheticAssertionSetupCase {
    // Synthetic cases let the setup bench stress contract fanout and selector fanout
    // independently of whatever shape the real preset happened to match.
    executor: AssertionExecutor,
    block_env: BlockEnv,
    tx_env: TxEnv,
    pre_tx_fork_db: BenchmarkDb,
    forked_tx_result: ExecuteForkedTxResult,
    matched_assertions: Vec<AssertionsForExecution>,
}

fn ensure_success(result: &ExecutionResult) -> Result<(), BenchmarkPackageError> {
    match result {
        ExecutionResult::Success {
            reason: SuccessReason::Stop | SuccessReason::Return,
            ..
        } => Ok(()),
        _ => Err(BenchmarkPackageError::BenchmarkError),
    }
}

impl BenchmarkPackage<BenchmarkDb> {
    /// End-to-end benchmark path: execute txs and immediately run matched assertions.
    ///
    /// # Errors
    ///
    /// Returns an error if transaction execution, assertion selection, or assertion
    /// execution fails for any benchmarked transaction.
    pub fn run_full_phase(&mut self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();

        for tx in self.bundle.clone() {
            let result = self
                .executor
                .validate_transaction(self.block_env.clone(), &tx, &mut self.db, true)
                .map_err(BenchmarkPackageError::ExecutorError)?;

            ensure_success(&result.result_and_state.result)?;
            stats.transactions += 1;
            stats.matched_assertion_contracts += result.assertions_executions.len();
            let executed_assertion_functions = usize_from_u64(result.total_assertion_funcs_ran());
            stats.matched_assertion_selectors += executed_assertion_functions;
            stats.executed_assertion_functions += executed_assertion_functions;
        }

        Ok(stats)
    }

    /// Run the transaction bundle without tracing or assertions so the bench can
    /// compare executor overhead against plain EVM execution.
    ///
    /// # Errors
    ///
    /// Returns an error if any benchmark transaction fails plain EVM execution.
    pub fn run_vanilla_phase(&mut self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();
        let env = evm_env(
            self.executor.config.chain_id,
            SpecId::CANCUN,
            self.block_env.clone(),
        );

        for tx in self.bundle.clone() {
            let mut evm = build_eth_evm(&mut self.db, &env, NoOpInspector);
            let result = evm
                .transact(tx)
                .map_err(|_| BenchmarkPackageError::BenchmarkError)?;

            ensure_success(&result.result)?;
            self.db.commit(result.state);
            stats.transactions += 1;
        }

        Ok(stats)
    }

    /// Trace-only path: commit the tx state changes but skip all assertion selection and
    /// execution so the benchmark reflects transaction validation/tracing cost alone.
    ///
    /// # Errors
    ///
    /// Returns an error if tx execution or tracing fails for any benchmark transaction.
    pub fn run_trace_only(&mut self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();

        for tx in self.bundle.clone() {
            let traced = self
                .executor
                .execute_forked_tx_ext_db(&self.block_env, tx, &mut self.db)
                .map_err(BenchmarkPackageError::EvmError)?;

            ensure_success(&traced.result_and_state.result)?;
            self.db.commit(traced.result_and_state.state);
            stats.transactions += 1;
        }

        Ok(stats)
    }

    /// Execute the traced txs once and retain only the call tracer needed to benchmark
    /// assertion-store reads without paying tx execution again inside the bench loop.
    ///
    /// # Errors
    ///
    /// Returns an error if preparing the traced transactions fails.
    pub fn prepare_store_read(self) -> Result<PreparedStoreReadPackage, BenchmarkPackageError> {
        let mut package = self;
        let mut cases = Vec::with_capacity(package.bundle.len());

        for tx in package.bundle.clone() {
            let traced = package
                .executor
                .execute_forked_tx_ext_db(&package.block_env, tx, &mut package.db)
                .map_err(BenchmarkPackageError::EvmError)?;

            ensure_success(&traced.result_and_state.result)?;
            package.db.commit(traced.result_and_state.state);
            cases.push(PreparedStoreReadCase {
                call_tracer: traced.call_tracer,
            });
        }

        Ok(PreparedStoreReadPackage {
            executor: package.executor,
            block_number: U256::from(package.block_env.number),
            cases,
        })
    }

    /// Execute the traced txs once, capture the matched assertions, and retain the
    /// pre-tx fork snapshot so assertion setup and assertion execution can be benchmarked
    /// independently of the tx path that discovered them.
    ///
    /// # Errors
    ///
    /// Returns an error if tx tracing or assertion selection fails while preparing
    /// the assertion-phase benchmark cases.
    pub fn prepare_assertion_phases(
        self,
    ) -> Result<PreparedAssertionPhasePackage, BenchmarkPackageError> {
        let mut package = self;
        let mut cases = Vec::with_capacity(package.bundle.len());

        for tx_env in package.bundle.clone() {
            let pre_tx_fork_db = package.db.clone();
            let traced = package
                .executor
                .execute_forked_tx_ext_db(&package.block_env, tx_env.clone(), &mut package.db)
                .map_err(BenchmarkPackageError::EvmError)?;

            ensure_success(&traced.result_and_state.result)?;
            let matched_assertions = package
                .executor
                .store
                .read(&traced.call_tracer, U256::from(package.block_env.number))
                .map_err(BenchmarkPackageError::AssertionReadError)?;

            package.db.commit(traced.result_and_state.state.clone());
            cases.push(PreparedAssertionCase {
                tx_env,
                pre_tx_fork_db,
                forked_tx_result: traced,
                matched_assertions,
            });
        }

        Ok(PreparedAssertionPhasePackage {
            executor: package.executor,
            block_env: package.block_env,
            cases,
        })
    }
}

impl PreparedStoreReadPackage {
    /// Re-read the store from a prepared tracer so the benchmark only measures the
    /// selector/adopter lookup work.
    ///
    /// # Errors
    ///
    /// Returns an error if the prepared store lookup fails for any traced case.
    pub fn run(&self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();

        for case in &self.cases {
            let matches = self
                .executor
                .store
                .read(&case.call_tracer, self.block_number)
                .map_err(BenchmarkPackageError::AssertionReadError)?;
            stats.transactions += 1;
            stats.accumulate_matches(&matches);
        }

        Ok(stats)
    }
}

impl PreparedAssertionPhasePackage {
    /// Measure just the contract/selector preparation fanout by black-boxing the
    /// cloned setup artifacts before any assertion bytecode is executed.
    ///
    /// # Errors
    ///
    /// Returns an error if preparing any matched assertion contract fails.
    pub fn run_assertion_setup_only(&self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();

        for case in &self.cases {
            let BenchmarkAssertionSetupStats {
                assertion_contracts_prepared,
                selector_fanouts_prepared,
            } = self.executor.benchmark_prepare_selected_assertions(
                &self.block_env,
                &case.pre_tx_fork_db,
                &case.matched_assertions,
                &case.forked_tx_result,
                &case.tx_env,
            );

            stats.transactions += 1;
            stats.matched_assertion_contracts += assertion_contracts_prepared;
            stats.matched_assertion_selectors += selector_fanouts_prepared;
        }

        Ok(stats)
    }

    /// Measure assertion execution after setup inputs have already been captured by
    /// `prepare_assertion_phases`, so the bench does not repay store reads.
    ///
    /// # Errors
    ///
    /// Returns an error if executing any prepared assertion fails.
    pub fn run_assertions_only(&self) -> Result<BenchmarkPhaseStats, BenchmarkPackageError> {
        let mut stats = BenchmarkPhaseStats::default();

        for case in &self.cases {
            let executions = self
                .executor
                .benchmark_execute_selected_assertions(
                    &self.block_env,
                    &case.pre_tx_fork_db,
                    case.matched_assertions.clone(),
                    &case.forked_tx_result,
                    &case.tx_env,
                )
                .map_err(BenchmarkPackageError::ExecutorError)?;

            stats.transactions += 1;
            stats.accumulate_matches(&case.matched_assertions);
            stats.executed_assertion_functions += executions
                .iter()
                .map(|execution| usize_from_u64(execution.total_assertion_funcs_ran))
                .sum::<usize>();
        }

        Ok(stats)
    }
}

impl SyntheticAssertionSetupCase {
    #[must_use]
    pub fn run(&self) -> BenchmarkPhaseStats {
        let BenchmarkAssertionSetupStats {
            assertion_contracts_prepared,
            selector_fanouts_prepared,
        } = self.executor.benchmark_prepare_selected_assertions(
            &self.block_env,
            &self.pre_tx_fork_db,
            &self.matched_assertions,
            &self.forked_tx_result,
            &self.tx_env,
        );

        BenchmarkPhaseStats {
            transactions: 1,
            matched_assertion_contracts: assertion_contracts_prepared,
            matched_assertion_selectors: selector_fanouts_prepared,
            executed_assertion_functions: 0,
        }
    }
}

/// Build a synthetic assertion-setup benchmark case that reuses one real traced
/// transaction while varying assertion contract fanout and selector fanout.
///
/// # Errors
///
/// Returns an error if the preset cannot produce a real matched assertion to use
/// as the template for the synthetic setup case.
pub fn synthetic_assertion_setup_case(
    preset: BenchmarkPreset,
    contract_count: usize,
    selectors_per_contract: usize,
) -> Result<SyntheticAssertionSetupCase, BenchmarkPackageError> {
    // Start from one real matched assertion so the synthetic case still uses the same
    // assertion bytecode and tx context as the target preset.
    let prepared = BenchmarkPackage::new(preset.load_definition()).prepare_assertion_phases()?;
    let template_case = prepared
        .cases
        .iter()
        .find(|case| !case.matched_assertions.is_empty())
        .cloned()
        .ok_or(BenchmarkPackageError::BenchmarkError)?;

    let template_assertion = template_case
        .matched_assertions
        .first()
        .cloned()
        .ok_or(BenchmarkPackageError::BenchmarkError)?;

    let selectors = (0..selectors_per_contract)
        .map(|idx| FixedBytes::<4>::from(u32_from_usize(idx).to_be_bytes()))
        .collect::<Vec<_>>();
    let matched_assertions = (0..contract_count)
        .map(|idx| {
            let mut assertion = template_assertion.clone();
            let mut adopter_bytes = [0u8; 20];
            adopter_bytes[16..20].copy_from_slice(&u32_from_usize(idx).to_be_bytes());
            assertion.adopter = Address::from(adopter_bytes);
            assertion.selectors.clone_from(&selectors);
            assertion
        })
        .collect::<Vec<_>>();

    Ok(SyntheticAssertionSetupCase {
        executor: prepared.executor,
        block_env: prepared.block_env,
        tx_env: template_case.tx_env,
        pre_tx_fork_db: template_case.pre_tx_fork_db,
        forked_tx_result: template_case.forked_tx_result,
        matched_assertions,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkMode,
        BenchmarkPreset,
    };
    use std::str::FromStr;

    #[test]
    fn benchmark_modes_parse() {
        for mode in BenchmarkMode::ALL {
            assert_eq!(BenchmarkMode::from_str(mode.as_str()), Ok(mode));
        }
    }

    #[test]
    fn benchmark_presets_parse() {
        for preset in BenchmarkPreset::ALL {
            assert_eq!(BenchmarkPreset::from_str(preset.as_str()), Ok(preset));
        }
    }
}
