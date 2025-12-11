use crate::{
    inspectors::{
        CallTracer,
        TriggerRecorder,
        TriggerType,
    },
    primitives::{
        Address,
        AssertionContract,
        B256,
        FixedBytes,
        U256,
    },
    store::PendingModification,
};

use crate::{
    ExecutorConfig,
    primitives::Bytes,
    store::assertion_contract_extractor::{
        FnSelectorExtractorError,
        extract_assertion_contract,
    },
};

use std::sync::{
    Arc,
    Mutex,
};

use bincode::{
    deserialize as de,
    serialize as ser,
};

use serde::{
    Deserialize,
    Serialize,
};

use tracing::{
    debug,
    error,
};

use std::collections::{
    HashMap,
    HashSet,
};

use tokio::sync::watch;

#[derive(thiserror::Error, Debug)]
pub enum AssertionStoreError {
    #[error("Sled error")]
    SledError(#[source] std::io::Error),
    #[error("Bincode error")]
    BincodeError(#[source] bincode::Error),
    #[error("Block number exceeds u64")]
    BlockNumberExceedsU64,
}

/// Struct representing an assertion contract, matched fn selectors, and the adopter.
/// This is necessary context when running assertions.
#[derive(Debug, Clone)]
pub struct AssertionsForExecution {
    pub assertion_contract: AssertionContract,
    pub selectors: Vec<FixedBytes<4>>,
    pub adopter: Address,
}

/// Used to represent important tracing information
#[derive(Debug)]
struct AssertionsForExecutionMetadata<'a> {
    #[allow(dead_code)]
    assertion_id: &'a B256,
    #[allow(dead_code)]
    selectors: &'a Vec<FixedBytes<4>>,
    #[allow(dead_code)]
    adopter: &'a Address,
}

/// Struct representing a pending assertion modification that has not passed the timelock.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AssertionState {
    pub activation_block: u64,
    pub inactivation_block: Option<u64>,
    pub assertion_contract: AssertionContract,
    pub trigger_recorder: TriggerRecorder,
}

/// Used to represent important tracing information.
#[derive(Debug)]
struct AssertionStateMetadata<'a> {
    #[allow(dead_code)]
    activation_block: u64,
    #[allow(dead_code)]
    inactivation_block: Option<u64>,
    #[allow(dead_code)]
    assertion_id: &'a B256,
    #[allow(dead_code)]
    recorded_triggers: &'a std::collections::HashMap<TriggerType, HashSet<FixedBytes<4>>>,
}

impl AssertionState {
    /// Creates a new active assertion state.
    /// Will be active across all blocks.
    #[allow(clippy::result_large_err)]
    pub fn new_active(
        bytecode: &Bytes,
        executor_config: &ExecutorConfig,
    ) -> Result<Self, FnSelectorExtractorError> {
        let (contract, trigger_recorder) = extract_assertion_contract(bytecode, executor_config)?;
        Ok(Self {
            activation_block: 0,
            inactivation_block: None,
            assertion_contract: contract,
            trigger_recorder,
        })
    }

    #[cfg(any(test, feature = "test"))]
    #[allow(clippy::missing_panics_doc)]
    pub fn new_test(bytecode: &Bytes) -> Self {
        Self::new_active(bytecode, &ExecutorConfig::default()).unwrap()
    }

    /// Getter for the `assertion_contract_id`
    pub fn assertion_contract_id(&self) -> B256 {
        self.assertion_contract.id
    }
}

/// Configuration for the pruning background task
#[derive(Debug, Clone)]
pub struct PruneConfig {
    /// Interval between prune runs in milliseconds
    pub interval_ms: u64,
    /// Number of blocks to keep after inactivation (buffer for reorgs)
    pub retention_blocks: u64,
}

impl Default for PruneConfig {
    fn default() -> Self {
        Self {
            // 1 minute
            interval_ms: 60_000,
            retention_blocks: 100,
        }
    }
}

/// Tree name for the expiry index
const EXPIRY_INDEX_TREE: &str = "expiry_index";

/// Builds an expiry index key from components.
/// Format: [`inactivation_block` (8 bytes BE) | `adopter` (20 bytes) | `assertion_id` (32 bytes)]
/// Using big-endian for block number allows lexicographic ordering by block.
#[inline]
fn build_expiry_key(inactivation_block: u64, adopter: &Address, assertion_id: &B256) -> [u8; 60] {
    let mut key = [0u8; 60];
    key[0..8].copy_from_slice(&inactivation_block.to_be_bytes());
    key[8..28].copy_from_slice(adopter.as_slice());
    key[28..60].copy_from_slice(assertion_id.as_slice());
    key
}

/// Parses an expiry index key into its components.
#[inline]
fn parse_expiry_key(key: &[u8]) -> (u64, Address, B256) {
    let block = u64::from_be_bytes(key[0..8].try_into().unwrap());
    let adopter = Address::from_slice(&key[8..28]);
    let assertion_id = B256::from_slice(&key[28..60]);
    (block, adopter, assertion_id)
}

/// Inner state shared between the store and background task
struct AssertionStoreInner {
    db: sled::Db,
    expiry_tree: sled::Tree,
}

#[derive(Debug, Clone)]
pub struct AssertionStore {
    inner: Arc<Mutex<AssertionStoreInner>>,
    /// Shutdown signal sender - when dropped, signals the background task to stop
    _shutdown_tx: Arc<watch::Sender<bool>>,
    /// Handle to the background pruning task
    _prune_task: Arc<tokio::task::JoinHandle<()>>,
    /// Current block number (updated externally for pruning reference)
    current_block: Arc<std::sync::atomic::AtomicU64>,
    /// Prune configuration
    prune_config: PruneConfig,
}

impl std::fmt::Debug for AssertionStoreInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssertionStoreInner")
            .field("db", &"sled::Db")
            .field("expiry_tree", &"sled::Tree")
            .finish()
    }
}

impl AssertionStore {
    /// Create a new assertion store
    ///
    /// # Panics
    ///
    /// Will panics if the expiry tree cannot be opened in the sled DB
    pub fn new(db: sled::Db, prune_config: PruneConfig) -> Self {
        let expiry_tree = db
            .open_tree(EXPIRY_INDEX_TREE)
            .expect("Failed to open expiry index tree");

        let inner = Arc::new(Mutex::new(AssertionStoreInner { db, expiry_tree }));
        let current_block = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_tx = Arc::new(shutdown_tx);

        let task_inner = Arc::clone(&inner);
        let task_current_block = Arc::clone(&current_block);
        let task_config = prune_config.clone();

        let prune_task = tokio::spawn(async move {
            Self::prune_background_task(task_inner, task_current_block, task_config, shutdown_rx)
                .await;
        });

        Self {
            inner,
            _shutdown_tx: shutdown_tx,
            _prune_task: Arc::new(prune_task),
            current_block,
            prune_config,
        }
    }

    /// Creates a new assertion store without persistence.
    pub fn new_ephemeral() -> Result<Self, AssertionStoreError> {
        let db = sled::Config::tmp()
            .map_err(AssertionStoreError::SledError)?
            .open()
            .map_err(AssertionStoreError::SledError)?;
        Ok(Self::new(db, PruneConfig::default()))
    }

    /// Creates a new assertion store without persistence with custom prune config.
    pub fn new_ephemeral_with_config(
        prune_config: PruneConfig,
    ) -> Result<Self, AssertionStoreError> {
        let db = sled::Config::tmp()
            .map_err(AssertionStoreError::SledError)?
            .open()
            .map_err(AssertionStoreError::SledError)?;
        Ok(Self::new(db, prune_config))
    }

    /// Updates the current block number used for pruning decisions
    pub fn set_current_block(&self, block: u64) {
        self.current_block
            .store(block, std::sync::atomic::Ordering::Release);
    }

    /// Background task that periodically prunes expired assertions
    async fn prune_background_task(
        inner: Arc<Mutex<AssertionStoreInner>>,
        current_block: Arc<std::sync::atomic::AtomicU64>,
        config: PruneConfig,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(config.interval_ms));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let block = current_block.load(std::sync::atomic::Ordering::Acquire);
                    if block > config.retention_blocks {
                        let prune_before = block - config.retention_blocks;
                        if let Err(e) = Self::prune_expired_inner(&inner, prune_before) {
                            error!(
                                target: "assertion-executor::assertion_store",
                                error = ?e,
                                "Background prune task failed"
                            );
                        }
                    }
                }
                result = shutdown_rx.changed() => {
                    // Shutdown if either:
                    // 1. Channel closed (sender dropped)
                    // 2. Explicit shutdown signal
                    if result.is_err() || *shutdown_rx.borrow() {
                        debug!(
                            target: "assertion-executor::assertion_store",
                            "Pruning background task shutting down"
                        );
                        break;
                    }
                }
            }
        }
    }

    /// Inserts the given assertion into the store.
    /// If an assertion with the same `assertion_contract_id` already exists, it is replaced.
    /// Returns the previous assertion if it existed.
    pub fn insert(
        &self,
        assertion_adopter: Address,
        assertion: AssertionState,
    ) -> Result<Option<AssertionState>, AssertionStoreError> {
        debug!(
            target: "assertion-executor::assertion_store",
            assertion_adopter=?assertion_adopter,
            activation_block=?assertion.activation_block,
            triggers=?assertion.trigger_recorder.triggers,
            "Inserting assertion into store"
        );

        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut assertions: Vec<AssertionState> = inner
            .db
            .get(assertion_adopter)
            .map_err(AssertionStoreError::SledError)?
            .map(|a| de(&a))
            .transpose()
            .map_err(AssertionStoreError::BincodeError)?
            .unwrap_or_default();

        let assertion_id = assertion.assertion_contract_id();
        let position = assertions
            .iter()
            .position(|a| a.assertion_contract_id() == assertion_id);

        let assertion_inactivation_block = assertion.inactivation_block;

        let previous = if let Some(pos) = position {
            let old = std::mem::replace(&mut assertions[pos], assertion);
            // Remove the old expiry index if it had an inactivation block
            if let Some(old_inactivation) = old.inactivation_block {
                let old_key = build_expiry_key(old_inactivation, &assertion_adopter, &assertion_id);
                let _ = inner.expiry_tree.remove(old_key);
            }
            Some(old)
        } else {
            assertions.push(assertion);
            None
        };

        // Add a new expiry index if assertion has an inactivation block
        if let Some(inactivation_block) = assertion_inactivation_block {
            let key = build_expiry_key(inactivation_block, &assertion_adopter, &assertion_id);
            inner
                .expiry_tree
                .insert(key, [])
                .map_err(AssertionStoreError::SledError)?;
        }

        inner
            .db
            .insert(
                assertion_adopter,
                ser(&assertions).map_err(AssertionStoreError::BincodeError)?,
            )
            .map_err(AssertionStoreError::SledError)?;

        // Opportunistic prune on insert
        drop(inner);
        self.maybe_prune();

        Ok(previous)
    }

    /// Applies the given modifications to the store.
    pub fn apply_pending_modifications(
        &self,
        pending_modifications: Vec<PendingModification>,
    ) -> Result<(), AssertionStoreError> {
        let mut map = HashMap::<Address, Vec<PendingModification>>::new();

        for modification in pending_modifications {
            map.entry(modification.assertion_adopter())
                .or_default()
                .push(modification);
        }

        for (aa, mods) in map {
            self.apply_pending_modification(aa, &mods)?;
        }

        // Opportunistic prune after modifications
        self.maybe_prune();

        Ok(())
    }

    /// Opportunistically prunes if conditions are met
    fn maybe_prune(&self) {
        let block = self
            .current_block
            .load(std::sync::atomic::Ordering::Acquire);
        if block > self.prune_config.retention_blocks {
            let prune_before = block - self.prune_config.retention_blocks;
            let _ = Self::prune_expired_inner(&self.inner, prune_before);
        }
    }

    /// Prunes expired assertions up to the given block number.
    /// This is O(k) where k is the number of expired assertions, not total assertions.
    fn prune_expired_inner(
        inner: &Arc<Mutex<AssertionStoreInner>>,
        prune_before_block: u64,
    ) -> Result<usize, AssertionStoreError> {
        let inner_guard = inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Build the upper bound key (exclusive) - all keys with block < prune_before_block
        let upper_bound = build_expiry_key(prune_before_block + 1, &Address::ZERO, &B256::ZERO);

        // Collect keys to remove
        let expired_keys: Vec<_> = inner_guard
            .expiry_tree
            .range(..upper_bound)
            .filter_map(|r| r.ok().map(|(k, _)| k))
            .collect();

        let pruned_count = expired_keys.len();
        if pruned_count == 0 {
            return Ok(0);
        }

        // Group by adopter for batch updates
        let mut adopter_removals: HashMap<Address, Vec<B256>> = HashMap::new();

        for key in &expired_keys {
            let (_, adopter, assertion_id) = parse_expiry_key(key);
            adopter_removals
                .entry(adopter)
                .or_default()
                .push(assertion_id);
        }

        // Remove from the main store and expiry index
        for (adopter, assertion_ids) in adopter_removals {
            // Remove assertions from the adopter's list
            let assertions_serialized = inner_guard
                .db
                .get(adopter)
                .map_err(AssertionStoreError::SledError)?;

            if let Some(serialized) = assertions_serialized {
                let mut assertions: Vec<AssertionState> =
                    de(&serialized).map_err(AssertionStoreError::BincodeError)?;

                let original_len = assertions.len();
                assertions.retain(|a| !assertion_ids.contains(&a.assertion_contract_id()));

                if assertions.len() != original_len {
                    if assertions.is_empty() {
                        inner_guard
                            .db
                            .remove(adopter)
                            .map_err(AssertionStoreError::SledError)?;
                    } else {
                        inner_guard
                            .db
                            .insert(
                                adopter,
                                ser(&assertions).map_err(AssertionStoreError::BincodeError)?,
                            )
                            .map_err(AssertionStoreError::SledError)?;
                    }
                }
            }
        }

        // Remove all expired keys from the expiry index
        for key in expired_keys {
            let _ = inner_guard.expiry_tree.remove(key);
        }

        debug!(
            target: "assertion-executor::assertion_store",
            pruned_count,
            prune_before_block,
            "Pruned expired assertions"
        );

        Ok(pruned_count)
    }

    /// Reads the assertions for the given block from the store, given the traces.
    #[tracing::instrument(
        skip_all,
        name = "read_assertions_from_store",
        target = "assertion_store::read",
        fields(triggers, block_num=?block_num),
        level = "DEBUG"
    )]
    pub fn read(
        &self,
        traces: &CallTracer,
        block_num: U256,
    ) -> Result<Vec<AssertionsForExecution>, AssertionStoreError> {
        let block_num = block_num
            .try_into()
            .map_err(|_| AssertionStoreError::BlockNumberExceedsU64)?;

        // Update current block for pruning reference
        self.set_current_block(block_num);

        let mut assertions = Vec::new();

        let triggers = traces.triggers();
        tracing::Span::current().record("triggers", format!("{triggers:?}"));

        for (contract_address, triggers) in &triggers {
            let contract_assertions = self.read_adopter(contract_address, triggers, block_num)?;
            let assertions_for_execution: Vec<AssertionsForExecution> = contract_assertions
                .into_iter()
                .map(|(assertion_contract, selectors)| {
                    AssertionsForExecution {
                        assertion_contract,
                        selectors,
                        adopter: *contract_address,
                    }
                })
                .collect();

            assertions.extend(assertions_for_execution);
        }

        if assertions.is_empty() {
            debug!(
                target: "assertion-executor::assertion_store",
                ?triggers,
                "No assertions found based on triggers",
            );
        } else {
            debug!(
                target: "assertion-executor::assertion_store",
                assertions = ?assertions.iter().map(|assertion| format!("{:?}", AssertionsForExecutionMetadata {
                    assertion_id: &assertion.assertion_contract.id,
                    selectors: &assertion.selectors,
                    adopter: &assertion.adopter
                })).collect::<Vec<_>>(),
                ?triggers,
                "Assertions found based on triggers",
            );
        }

        Ok(assertions)
    }

    /// Returns `true` if the address has any active assertions associated with it.
    /// Used to check if a account is an assertion adopter.
    #[tracing::instrument(
        skip_all,
        name = "read_adopter_from_db",
        target = "assertion_store::has_assertions",
        fields(assertion_adopter=?assertion_adopter),
        level = "trace"
    )]
    pub fn has_assertions(&self, assertion_adopter: Address) -> Result<bool, AssertionStoreError> {
        match self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .db
            .contains_key(assertion_adopter)
        {
            Ok(has_assertions) => Ok(has_assertions),
            Err(err) => {
                error!(
                    target: "assertion_store::has_assertions",
                    ?err,
                    ?assertion_adopter,
                    "Failed to check if assertion adopter has active assertions"
                );
                Err(AssertionStoreError::SledError(err))
            }
        }
    }

    /// Reads the assertions for the given assertion adopter at the given block.
    /// Returns the assertions that are active at the given block.
    /// An assertion is considered active at a block if the `activation_block` is less than or equal
    /// to the given block, and the `inactivation_block` is greater than the given block.
    /// `assertion_adopter` is the address of the contract leveraging assertions.
    #[tracing::instrument(
        skip_all,
        name = "read_adopter_from_db",
        target = "assertion_store::read_adopter",
        fields(assertion_adopter=?assertion_adopter, triggers=?triggers, block=?block),
        level = "trace"
    )]
    fn read_adopter(
        &self,
        assertion_adopter: &Address,
        triggers: &HashSet<TriggerType>,
        block: u64,
    ) -> Result<Vec<(AssertionContract, Vec<FixedBytes<4>>)>, AssertionStoreError> {
        let assertion_states = tracing::trace_span!(
            "read_adopter_from_db",
            ?assertion_adopter,
            ?triggers,
            ?block
        )
        .in_scope(|| {
            self.inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .db
                .get(assertion_adopter)?
                .map(|a| de::<Vec<AssertionState>>(&a))
                .transpose()
        })
        .map_err(AssertionStoreError::BincodeError)?
        .unwrap_or_default();

        debug!(
            target: "assertion_store::read_adopter",
            ?assertion_adopter,
            assertion_states = ?assertion_states.iter().map(|a|
                format!("{:?}",
                AssertionStateMetadata {
                    assertion_id: &a.assertion_contract.id,
                    activation_block : a.activation_block,
                    inactivation_block : a.inactivation_block,
                    recorded_triggers : &a.trigger_recorder.triggers
                })).collect::<Vec<_>>(),
            "Assertion states in store for adopter.",
        );

        let active_assertion_contracts = assertion_states
            .into_iter()
            .filter(|a| {
                let inactive_block = match a.inactivation_block {
                    Some(inactive_block) => {
                        // If the inactive block is less than the active block, the end bound is
                        // ignored.
                        if inactive_block < a.activation_block {
                            u64::MAX
                        } else {
                            inactive_block
                        }
                    }
                    None => u64::MAX,
                };
                let in_bound_start = a.activation_block <= block;
                let in_bound_end = block < inactive_block;
                in_bound_start && in_bound_end
            })
            .map(|a| {
                // Get all function selectors from matching triggers
                let mut all_selectors = HashSet::new();
                let mut has_call_trigger = false;
                let mut has_storage_trigger = false;

                // Process specific triggers and detect trigger types
                for trigger in triggers {
                    if let Some(selectors) = a.trigger_recorder.triggers.get(trigger) {
                        all_selectors.extend(selectors.iter().copied());
                    }

                    // Check trigger type while we're iterating
                    match trigger {
                        TriggerType::Call { .. } => has_call_trigger = true,
                        TriggerType::StorageChange { .. } => has_storage_trigger = true,
                        _ => {}
                    }
                }

                // Add AllCalls selectors if needed
                if has_call_trigger
                    && let Some(selectors) = a.trigger_recorder.triggers.get(&TriggerType::AllCalls)
                {
                    all_selectors.extend(selectors.iter().copied());
                }

                // Add AllStorageChanges selectors if needed
                if has_storage_trigger
                    && let Some(selectors) = a
                        .trigger_recorder
                        .triggers
                        .get(&TriggerType::AllStorageChanges)
                {
                    all_selectors.extend(selectors.iter().copied());
                }
                // Convert HashSet to Vec to match the expected return type
                (a.assertion_contract, all_selectors.into_iter().collect())
            })
            .collect();

        Ok(active_assertion_contracts)
    }

    /// Applies the given assertion adopter modifications to the store.
    #[allow(clippy::too_many_lines)]
    fn apply_pending_modification(
        &self,
        assertion_adopter: Address,
        modifications: &[PendingModification],
    ) -> Result<(), AssertionStoreError> {
        loop {
            let inner = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            let assertions_serialized = inner
                .db
                .get(assertion_adopter)
                .map_err(AssertionStoreError::SledError)?;

            let mut assertions: Vec<AssertionState> = assertions_serialized
                .clone()
                .map(|a| de(&a))
                .transpose()
                .map_err(AssertionStoreError::BincodeError)?
                .unwrap_or_default();

            debug!(
                target: "assertion-executor::assertion_store",
                pending_modifations_len = assertions.len(),
                "Applying pending modifications"
            );

            // Track expiry index changes
            let mut expiry_additions: Vec<(u64, B256)> = Vec::new();
            let mut expiry_removals: Vec<(u64, B256)> = Vec::new();

            for modification in modifications {
                match modification {
                    PendingModification::Add {
                        assertion_contract,
                        trigger_recorder,
                        activation_block,
                        ..
                    } => {
                        debug!(
                            target: "assertion-executor::assertion_store",
                            ?assertion_contract,
                            ?trigger_recorder,
                            activation_block,
                            "Applying pending assertion addition"
                        );
                        let existing_state = assertions
                            .iter_mut()
                            .find(|a| a.assertion_contract_id() == assertion_contract.id);

                        match existing_state {
                            Some(state) => {
                                // Remove the old expiry if it had one
                                if let Some(old_inactivation) = state.inactivation_block {
                                    expiry_removals.push((old_inactivation, assertion_contract.id));
                                }
                                state.activation_block = *activation_block;
                                state.inactivation_block = None; // Re-activation clears inactivation
                            }
                            None => {
                                assertions.push(AssertionState {
                                    activation_block: *activation_block,
                                    inactivation_block: None,
                                    assertion_contract: assertion_contract.clone(),
                                    trigger_recorder: trigger_recorder.clone(),
                                });
                            }
                        }
                    }
                    PendingModification::Remove {
                        assertion_contract_id,
                        inactivation_block,
                        ..
                    } => {
                        debug!(
                            target: "assertion-executor::assertion_store",
                            ?assertion_contract_id,
                            inactivation_block,
                            "Applying pending assertion removal"
                        );
                        let existing_state = assertions
                            .iter_mut()
                            .find(|a| a.assertion_contract_id() == *assertion_contract_id);

                        match existing_state {
                            Some(state) => {
                                // Remove the old expiry if it had one
                                if let Some(old_inactivation) = state.inactivation_block {
                                    expiry_removals
                                        .push((old_inactivation, *assertion_contract_id));
                                }
                                state.inactivation_block = Some(*inactivation_block);
                                // Add a new expiry index
                                expiry_additions
                                    .push((*inactivation_block, *assertion_contract_id));
                            }
                            None => {
                                // The assertion was not found, so we add it with the inactivation_block set.
                                error!(
                                    target: "assertion-executor::assertion_store",
                                    ?assertion_contract_id,
                                    "Apply pending modifications error: Assertion not found for removal",
                                );
                            }
                        }
                    }
                }
            }

            let result = inner.db.compare_and_swap(
                assertion_adopter,
                assertions_serialized,
                Some(ser(&assertions).map_err(AssertionStoreError::BincodeError)?),
            );

            if let Ok(Ok(_)) = result {
                // Update expiry index after successful CAS
                for (block, id) in expiry_removals {
                    let key = build_expiry_key(block, &assertion_adopter, &id);
                    let _ = inner.expiry_tree.remove(key);
                }
                for (block, id) in expiry_additions {
                    let key = build_expiry_key(block, &assertion_adopter, &id);
                    let _ = inner.expiry_tree.insert(key, []);
                }
                break;
            }
            tracing::debug!(
                target: "assertion-executor::assertion_store",
                ?result,
                "Assertion store Compare and Swap failed, retrying"
            );
        }

        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    pub fn prune_now(&self, prune_before_block: u64) -> Result<usize, AssertionStoreError> {
        Self::prune_expired_inner(&self.inner, prune_before_block)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn assertion_contract_count(&self, assertion_adopter: Address) -> usize {
        let assertions = self.get_assertions_for_contract(assertion_adopter);
        assertions.len()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn get_assertions_for_contract(&self, assertion_adopter: Address) -> Vec<AssertionState> {
        let assertions_serialized = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .db
            .get(assertion_adopter)
            .unwrap_or(None);

        if let Some(assertions_serialized) = assertions_serialized {
            let assertions: Vec<AssertionState> = de(&assertions_serialized).unwrap_or_default();
            assertions
        } else {
            vec![]
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn expiry_index_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .expiry_tree
            .len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_truncation)]
    #![allow(clippy::cast_sign_loss)]
    #![allow(clippy::used_underscore_binding)]
    use revm::context::JournalInner;

    use super::*;
    use crate::primitives::{
        Address,
        JournalEntry,
    };
    use std::collections::HashSet;

    fn create_test_assertion(
        activation_block: u64,
        inactivation_block: Option<u64>,
    ) -> AssertionState {
        AssertionState {
            activation_block,
            inactivation_block,
            assertion_contract: AssertionContract {
                id: B256::random(),
                ..Default::default()
            },
            trigger_recorder: TriggerRecorder::default(),
        }
    }

    fn create_test_modification(
        active_at: u64,
        aa: Address,
        log_index: u64,
    ) -> PendingModification {
        create_test_modification_with_id(active_at, aa, log_index, B256::random())
    }

    fn create_test_modification_with_id(
        active_at: u64,
        aa: Address,
        log_index: u64,
        id: B256,
    ) -> PendingModification {
        PendingModification::Add {
            assertion_adopter: aa,
            assertion_contract: AssertionContract {
                id,
                ..Default::default()
            },
            trigger_recorder: TriggerRecorder::default(),
            activation_block: active_at,
            log_index,
        }
    }

    fn create_test_modification_remove(
        inactive_at: u64,
        aa: Address,
        log_index: u64,
        id: B256,
    ) -> PendingModification {
        PendingModification::Remove {
            log_index,
            assertion_adopter: aa,
            assertion_contract_id: id,
            inactivation_block: inactive_at,
        }
    }

    #[tokio::test]
    async fn test_has_assertions_false_when_absent() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        assert!(!store.has_assertions(aa).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_has_assertions_true_when_present() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        let assertion = create_test_assertion(0, None);
        store.insert(aa, assertion)?;

        assert!(store.has_assertions(aa).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_and_read() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        // Create a test assertion
        let assertion = create_test_assertion(100, None);
        let assertion_contract_id = assertion.assertion_contract_id();

        // Insert the assertion
        store.insert(aa, assertion.clone())?;

        // Create a call tracer that includes our AA
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa);

        // Read at block 150 (should be active)
        let assertions = store.read(&tracer, U256::from(150))?;
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].assertion_contract.id, assertion_contract_id);

        // Read at block 50 (should be inactive)
        let assertions = store.read(&tracer, U256::from(50))?;
        assert_eq!(assertions.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_pending_modifications() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        // Create two modifications
        let mod1 = create_test_modification(100, aa, 0);
        let mod2 = create_test_modification(200, aa, 0);

        // Apply modifications
        store.apply_pending_modifications(vec![mod1.clone(), mod2])?;

        // Create a call tracer that includes our AA
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa);

        // Read at block 150 (should see first assertion only)
        let assertions = store.read(&tracer, U256::from(150))?;
        assert_eq!(assertions.len(), 1);
        assert_eq!(
            assertions[0].assertion_contract.id,
            mod1.assertion_contract_id()
        );

        // Read at block 250 (should see both assertions)
        let assertions = store.read(&tracer, U256::from(250))?;
        assert_eq!(assertions.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_removal_modification() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        // Add an assertion
        let add_mod = create_test_modification(100, aa, 0);
        store.apply_pending_modifications(vec![add_mod.clone()])?;

        // Remove the assertion at block 200
        let remove_mod = PendingModification::Remove {
            log_index: 1,
            assertion_adopter: aa,
            assertion_contract_id: add_mod.assertion_contract_id(),
            inactivation_block: 200,
        };
        store.apply_pending_modifications(vec![remove_mod])?;

        // Create a call tracer
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa);

        // Check at different blocks
        let assertions = store.read(&tracer, U256::from(150))?;
        assert_eq!(assertions.len(), 1); // Active

        let assertions = store.read(&tracer, U256::from(250))?;
        assert_eq!(assertions.len(), 0); // Inactive

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_assertion_adopters() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa1 = Address::random();
        let aa2 = Address::random();

        // Create modifications for different AAs
        let mod1 = create_test_modification(100, aa1, 0);
        let mod2 = create_test_modification(100, aa2, 1);

        // Apply modifications
        store.apply_pending_modifications(vec![mod1.clone(), mod2])?;

        // Create a call tracer that includes both AAs
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa1);
        tracer.insert_trace(aa2);

        // Read at block 150 (should see both assertions)
        let assertions = store.read(&tracer, U256::from(150))?;
        assert_eq!(assertions.len(), 2);

        // Create a tracer with only aa1
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa1);

        // Should only see one assertion
        let assertions = store.read(&tracer, U256::from(150))?;
        assert_eq!(assertions.len(), 1);
        assert_eq!(
            assertions[0].assertion_contract.id,
            mod1.assertion_contract_id()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_update_same_assertion() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        let a_state = create_test_assertion(100, None);
        let id = a_state.assertion_contract_id();
        let _ = store.insert(aa, a_state);

        let mod1 = create_test_modification_remove(200, aa, 0, id);
        let mod2 = create_test_modification_with_id(300, aa, 1, id);

        // Apply modifications
        store.apply_pending_modifications(vec![mod1, mod2])?;

        // Create a call tracer that includes both AAs
        let mut tracer = CallTracer::default();
        tracer.insert_trace(aa);

        assert_eq!(store.assertion_contract_count(aa), 1);

        // Read at block 250 (should see no assertion)
        let assertions = store.read(&tracer, U256::from(250))?;
        assert_eq!(assertions.len(), 0);

        let assertions = store.read(&tracer, U256::from(350))?;
        assert_eq!(assertions.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_block_number_exceeds_u64() {
        let store = AssertionStore::new_ephemeral().unwrap();
        let mut tracer = CallTracer::default();
        tracer.insert_trace(Address::random());

        let result = store.read(&tracer, U256::MAX);
        assert!(matches!(
            result,
            Err(AssertionStoreError::BlockNumberExceedsU64)
        ));
    }

    #[allow(clippy::needless_pass_by_value)]
    fn setup_and_match(
        recorded_triggers: Vec<(TriggerType, HashSet<FixedBytes<4>>)>,
        journal_entries: Vec<JournalEntry>,
        assertion_adopter: Address,
    ) -> Result<Vec<AssertionsForExecution>, AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let mut trigger_recorder = TriggerRecorder::default();

        for (trigger, selectors) in &recorded_triggers {
            trigger_recorder
                .triggers
                .insert(trigger.clone(), selectors.clone());
        }

        let mut assertion = create_test_assertion(100, None);
        assertion.trigger_recorder = trigger_recorder;
        store.insert(assertion_adopter, assertion)?;

        let mut tracer = CallTracer::default();
        // insert_trace inserts (address, 0x00000000) in call_inputs to pretend a call
        tracer.insert_trace(assertion_adopter);

        for entry in journal_entries {
            tracer.journal.journal.push(entry);
        }

        store.read(&tracer, U256::from(100))
    }

    #[tokio::test]
    async fn test_read_adopter_with_all_triggers() -> Result<(), AssertionStoreError> {
        let aa = Address::random();

        let assertion_selector_call = FixedBytes::<4>::random();
        let assertion_selector_storage = FixedBytes::<4>::random();
        let assertion_selector_both = FixedBytes::<4>::random();
        let mut expected_selectors = vec![
            assertion_selector_call,
            assertion_selector_storage,
            assertion_selector_both,
        ];
        expected_selectors.sort();

        // Create recorded triggers for all calls and storage changes
        let recorded_triggers = vec![
            (
                TriggerType::AllCalls,
                vec![assertion_selector_call, assertion_selector_both]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::AllStorageChanges,
                vec![assertion_selector_storage, assertion_selector_both]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
        ];

        let journal_entries = vec![JournalEntry::StorageChanged {
            address: aa,
            key: U256::from(1),
            had_value: U256::from(0),
        }];

        let assertions = setup_and_match(recorded_triggers, journal_entries, aa)?;
        assert_eq!(assertions.len(), 1);
        let mut matched_selectors = assertions[0].selectors.clone();
        matched_selectors.sort();
        assert_eq!(matched_selectors, expected_selectors);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_adopter_with_specific_triggers() -> Result<(), AssertionStoreError> {
        let aa = Address::random();
        let assertion_selector_call = FixedBytes::<4>::random();
        let assertion_selector_storage = FixedBytes::<4>::random();
        let assertion_selector_balance = FixedBytes::<4>::random();

        let mut expected_selectors = vec![
            assertion_selector_call,
            assertion_selector_storage,
            assertion_selector_balance,
        ];
        expected_selectors.sort();

        let trigger_selector = FixedBytes::<4>::default();

        let recorded_triggers = vec![
            (
                TriggerType::Call { trigger_selector },
                vec![assertion_selector_call]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::StorageChange {
                    trigger_slot: U256::from(1).into(),
                },
                vec![assertion_selector_storage]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::BalanceChange,
                vec![assertion_selector_balance]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
        ];

        let journal_entries = vec![
            JournalEntry::StorageChanged {
                address: aa,
                key: U256::from(1),
                had_value: U256::from(0),
            },
            JournalEntry::BalanceTransfer {
                from: aa,
                to: Address::random(),
                balance: U256::from(1),
            },
        ];

        let assertions = setup_and_match(recorded_triggers, journal_entries, aa)?;
        assert_eq!(assertions.len(), 1);
        let mut matched_selectors = assertions[0].selectors.clone();
        matched_selectors.sort();
        assert_eq!(matched_selectors, expected_selectors);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_adopter_only_match_call_trigger() -> Result<(), AssertionStoreError> {
        let aa = Address::random();

        let assertion_selector_call = FixedBytes::<4>::random();
        let assertion_selector_all_storage = FixedBytes::<4>::random();
        let assertion_selector_balance = FixedBytes::<4>::random();
        let mut expected_selectors = vec![assertion_selector_call];
        expected_selectors.sort();

        let trigger_selector_call = FixedBytes::<4>::default();

        let recorded_triggers = vec![
            (
                TriggerType::Call {
                    trigger_selector: trigger_selector_call,
                },
                vec![assertion_selector_call]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::AllStorageChanges,
                vec![assertion_selector_all_storage]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::BalanceChange,
                vec![assertion_selector_balance]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
        ];

        let assertions = setup_and_match(recorded_triggers, vec![], aa)?;
        assert_eq!(assertions.len(), 1);
        let mut matched_selectors = assertions[0].selectors.clone();
        matched_selectors.sort();
        assert_eq!(matched_selectors, expected_selectors);

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_all_trigger_types_comprehensive() -> Result<(), AssertionStoreError> {
        let aa = Address::random();

        // Create unique selectors for each trigger type
        let selector_specific_call = FixedBytes::<4>::random();
        let selector_all_calls = FixedBytes::<4>::random();
        let selector_specific_storage = FixedBytes::<4>::random();
        let selector_all_storage = FixedBytes::<4>::random();
        let selector_balance = FixedBytes::<4>::random();

        let trigger_selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let trigger_slot = U256::from(42);

        // Create recorded triggers for ALL trigger types
        let recorded_triggers = [
            (
                TriggerType::Call { trigger_selector },
                vec![selector_specific_call]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::AllCalls,
                vec![selector_all_calls].into_iter().collect::<HashSet<_>>(),
            ),
            (
                TriggerType::StorageChange {
                    trigger_slot: trigger_slot.into(),
                },
                vec![selector_specific_storage]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::AllStorageChanges,
                vec![selector_all_storage]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::BalanceChange,
                vec![selector_balance].into_iter().collect::<HashSet<_>>(),
            ),
        ];

        // Create journal entries that trigger specific call, storage change, and balance change
        let journal_entries = vec![
            JournalEntry::StorageChanged {
                address: aa,
                key: trigger_slot,
                had_value: U256::from(0),
            },
            JournalEntry::StorageChanged {
                address: aa,
                key: U256::from(99), // Different slot to trigger AllStorageChanges
                had_value: U256::from(1),
            },
            JournalEntry::BalanceTransfer {
                from: aa,
                to: Address::random(),
                balance: U256::from(100),
            },
        ];

        let store = AssertionStore::new_ephemeral()?;
        let mut trigger_recorder = TriggerRecorder::default();

        for (trigger, selectors) in &recorded_triggers {
            trigger_recorder
                .triggers
                .insert(trigger.clone(), selectors.clone());
        }

        let mut assertion = create_test_assertion(100, None);
        assertion.trigger_recorder = trigger_recorder;
        store.insert(aa, assertion)?;

        let mut tracer = CallTracer::default();
        // Add call with the specific trigger selector
        tracer.record_call_start(
            revm::interpreter::CallInputs {
                input: revm::interpreter::CallInput::Bytes(Bytes::from(vec![
                    0x12, 0x34, 0x56, 0x78,
                ])),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: aa,
                known_bytecode: None,
                target_address: aa,
                caller: Address::random(),
                value: revm::interpreter::CallValue::Transfer(U256::from(100)),
                scheme: revm::interpreter::CallScheme::Call,
                is_static: false,
            },
            &[0x12, 0x34, 0x56, 0x78],
            &mut JournalInner::new(),
        );
        tracer.result.clone().unwrap();

        tracer.record_call_end(&mut JournalInner::new(), false);
        tracer.result.clone().unwrap();

        for entry in journal_entries {
            tracer.journal.journal.push(entry);
        }

        let assertions = store.read(&tracer, U256::from(100))?;
        assert_eq!(assertions.len(), 1);

        // All selectors should be included since we triggered:
        // - Call (specific trigger_selector)
        // - AllCalls (because we had a call)
        // - StorageChange (specific trigger_slot)
        // - AllStorageChanges (because we had storage changes)
        // - BalanceChange (because we had a balance transfer)
        let mut expected_selectors = vec![
            selector_specific_call,
            selector_all_calls,
            selector_specific_storage,
            selector_all_storage,
            selector_balance,
        ];
        expected_selectors.sort();

        let mut matched_selectors = assertions[0].selectors.clone();
        matched_selectors.sort();
        assert_eq!(matched_selectors, expected_selectors);

        Ok(())
    }

    #[tokio::test]
    async fn test_no_matching_triggers() -> Result<(), AssertionStoreError> {
        let aa = Address::random();

        // Create triggers that won't be matched
        let recorded_triggers = vec![
            (
                TriggerType::Call {
                    trigger_selector: FixedBytes::<4>::from([0xFF, 0xFF, 0xFF, 0xFF]),
                },
                vec![FixedBytes::<4>::random()]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
            (
                TriggerType::StorageChange {
                    trigger_slot: U256::from(999).into(),
                },
                vec![FixedBytes::<4>::random()]
                    .into_iter()
                    .collect::<HashSet<_>>(),
            ),
        ];

        // Create journal entries that DON'T match the triggers
        let journal_entries = vec![
            JournalEntry::StorageChanged {
                address: aa,
                key: U256::from(123), // Different slot
                had_value: U256::from(0),
            },
            JournalEntry::BalanceTransfer {
                from: aa,
                to: Address::random(),
                balance: U256::from(100),
            },
        ];

        let assertions = setup_and_match(recorded_triggers, journal_entries, aa)?;
        assert_eq!(assertions.len(), 1);

        // No selectors should match since triggers don't align
        assert_eq!(assertions[0].selectors.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_trigger_matching() -> Result<(), AssertionStoreError> {
        let aa = Address::random();

        let selector1 = FixedBytes::<4>::random();
        let selector2 = FixedBytes::<4>::random();
        let selector3 = FixedBytes::<4>::random();

        // Setup triggers where only some will match
        let recorded_triggers = vec![
            (
                TriggerType::Call {
                    trigger_selector: FixedBytes::<4>::from([0x11, 0x22, 0x33, 0x44]),
                },
                vec![selector1].into_iter().collect::<HashSet<_>>(),
            ),
            (
                TriggerType::StorageChange {
                    trigger_slot: U256::from(5).into(),
                },
                vec![selector2].into_iter().collect::<HashSet<_>>(),
            ),
            (
                TriggerType::BalanceChange,
                vec![selector3].into_iter().collect::<HashSet<_>>(),
            ),
        ];

        // Only trigger storage change and balance change, not the specific call
        let journal_entries = vec![
            JournalEntry::StorageChanged {
                address: aa,
                key: U256::from(5), // Matches the trigger
                had_value: U256::from(0),
            },
            JournalEntry::BalanceTransfer {
                from: aa,
                to: Address::random(),
                balance: U256::from(100),
            },
        ];

        let assertions = setup_and_match(recorded_triggers, journal_entries, aa)?;
        assert_eq!(assertions.len(), 1);

        // Should only have selectors for storage change and balance change
        let mut expected_selectors = vec![selector2, selector3];
        expected_selectors.sort();

        let mut matched_selectors = assertions[0].selectors.clone();
        matched_selectors.sort();
        assert_eq!(matched_selectors, expected_selectors);

        Ok(())
    }

    #[tokio::test]
    async fn test_expiry_index_created_on_removal() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        // Add an assertion
        let add_mod = create_test_modification(100, aa, 0);
        let assertion_id = add_mod.assertion_contract_id();
        store.apply_pending_modifications(vec![add_mod])?;

        // No expiry index yet (no inactivation block)
        assert_eq!(store.expiry_index_count(), 0);

        // Remove the assertion
        let remove_mod = create_test_modification_remove(200, aa, 1, assertion_id);
        store.apply_pending_modifications(vec![remove_mod])?;

        // Expiry index should now have one entry
        assert_eq!(store.expiry_index_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_prune_removes_expired_assertions() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000, // Long interval so background task doesn't interfere
            retention_blocks: 0,  // No retention for this test
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();

        // Add and then remove an assertion (inactivation at block 100)
        let add_mod = create_test_modification(50, aa, 0);
        let assertion_id = add_mod.assertion_contract_id();
        store.apply_pending_modifications(vec![add_mod])?;

        let remove_mod = create_test_modification_remove(100, aa, 1, assertion_id);
        store.apply_pending_modifications(vec![remove_mod])?;

        // Assertion still exists (just inactive)
        assert_eq!(store.assertion_contract_count(aa), 1);
        assert_eq!(store.expiry_index_count(), 1);

        // Prune at block 50 - should not remove (not yet expired)
        let pruned = store.prune_now(50)?;
        assert_eq!(pruned, 0);
        assert_eq!(store.assertion_contract_count(aa), 1);

        // Prune at block 100 - should remove (inactivation_block <= prune_before)
        let pruned = store.prune_now(100)?;
        assert_eq!(pruned, 1);
        assert_eq!(store.assertion_contract_count(aa), 0);
        assert_eq!(store.expiry_index_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_prune_keeps_active_assertions() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000,
            retention_blocks: 0,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();

        // Add an assertion with no inactivation block (always active)
        let assertion = create_test_assertion(50, None);
        store.insert(aa, assertion)?;

        // No expiry index (no inactivation block)
        assert_eq!(store.expiry_index_count(), 0);

        // Prune at a very high block - should not remove
        let pruned = store.prune_now(1_000_000)?;
        assert_eq!(pruned, 0);
        assert_eq!(store.assertion_contract_count(aa), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_prune_partial_removal() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000,
            retention_blocks: 0,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();

        // Add three assertions
        let id1 = B256::random();
        let id2 = B256::random();
        let id3 = B256::random();

        let mod1 = create_test_modification_with_id(10, aa, 0, id1);
        let mod2 = create_test_modification_with_id(10, aa, 1, id2);
        let mod3 = create_test_modification_with_id(10, aa, 2, id3);
        store.apply_pending_modifications(vec![mod1, mod2, mod3])?;

        // Remove assertion 1 at block 100, assertion 2 at block 200
        let remove1 = create_test_modification_remove(100, aa, 3, id1);
        let remove2 = create_test_modification_remove(200, aa, 4, id2);
        store.apply_pending_modifications(vec![remove1, remove2])?;

        assert_eq!(store.assertion_contract_count(aa), 3);
        assert_eq!(store.expiry_index_count(), 2);

        // Prune at block 150 - should only remove assertion 1
        let pruned = store.prune_now(150)?;
        assert_eq!(pruned, 1);
        assert_eq!(store.assertion_contract_count(aa), 2);
        assert_eq!(store.expiry_index_count(), 1);

        // Prune at block 250 - should remove assertion 2
        let pruned = store.prune_now(250)?;
        assert_eq!(pruned, 1);
        assert_eq!(store.assertion_contract_count(aa), 1);
        assert_eq!(store.expiry_index_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_prune_removes_empty_adopter_entry() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000,
            retention_blocks: 0,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();

        // Add and remove a single assertion
        let add_mod = create_test_modification(10, aa, 0);
        let assertion_id = add_mod.assertion_contract_id();
        store.apply_pending_modifications(vec![add_mod])?;

        let remove_mod = create_test_modification_remove(100, aa, 1, assertion_id);
        store.apply_pending_modifications(vec![remove_mod])?;

        assert!(store.has_assertions(aa)?);

        // Prune
        store.prune_now(100)?;

        // The adopter entry should be removed entirely
        assert!(!store.has_assertions(aa)?);

        Ok(())
    }

    #[tokio::test]
    async fn test_prune_multiple_adopters() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000,
            retention_blocks: 0,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa1 = Address::random();
        let aa2 = Address::random();

        // Add assertions for different adopters
        let mod1 = create_test_modification(10, aa1, 0);
        let id1 = mod1.assertion_contract_id();
        let mod2 = create_test_modification(10, aa1, 1);
        let mod3 = create_test_modification(10, aa2, 2);
        let id3 = mod3.assertion_contract_id();

        store.apply_pending_modifications(vec![mod1, mod2, mod3])?;

        // Remove one from each adopter at different blocks
        let remove1 = create_test_modification_remove(100, aa1, 3, id1);
        let remove3 = create_test_modification_remove(150, aa2, 4, id3);
        store.apply_pending_modifications(vec![remove1, remove3])?;

        assert_eq!(store.assertion_contract_count(aa1), 2);
        assert_eq!(store.assertion_contract_count(aa2), 1);
        assert_eq!(store.expiry_index_count(), 2);

        // Prune at block 150
        let pruned = store.prune_now(150)?;
        assert_eq!(pruned, 2); // Both expired

        assert_eq!(store.assertion_contract_count(aa1), 1);
        assert!(!store.has_assertions(aa2)?); // aa2 should be completely removed

        Ok(())
    }

    #[test]
    fn test_expiry_key_ordering() {
        // Verify that expiry keys are properly ordered by block number
        let aa = Address::random();
        let id = B256::random();

        let key1 = build_expiry_key(100, &aa, &id);
        let key2 = build_expiry_key(200, &aa, &id);
        let key3 = build_expiry_key(50, &aa, &id);

        // key3 (block 50) < key1 (block 100) < key2 (block 200)
        assert!(key3 < key1);
        assert!(key1 < key2);
    }

    #[test]
    fn test_expiry_key_roundtrip() {
        let block = 12345678u64;
        let aa = Address::random();
        let id = B256::random();

        let key = build_expiry_key(block, &aa, &id);
        let (parsed_block, parsed_aa, parsed_id) = parse_expiry_key(&key);

        assert_eq!(block, parsed_block);
        assert_eq!(aa, parsed_aa);
        assert_eq!(id, parsed_id);
    }

    #[tokio::test]
    async fn test_insert_updates_expiry_index() -> Result<(), AssertionStoreError> {
        let store = AssertionStore::new_ephemeral()?;
        let aa = Address::random();

        // Insert assertion with inactivation block
        let mut assertion = create_test_assertion(10, Some(100));
        store.insert(aa, assertion.clone())?;

        assert_eq!(store.expiry_index_count(), 1);

        // Update the same assertion with a different inactivation block
        assertion.inactivation_block = Some(200);
        store.insert(aa, assertion)?;

        // Should still have only one expiry index entry (old one removed, new one added)
        assert_eq!(store.expiry_index_count(), 1);

        // Prune at block 150 - should not remove (new inactivation is 200)
        let pruned = store.prune_now(150)?;
        assert_eq!(pruned, 0);
        assert_eq!(store.assertion_contract_count(aa), 1);

        // Prune at block 200 - should remove
        let pruned = store.prune_now(200)?;
        assert_eq!(pruned, 1);
        assert_eq!(store.assertion_contract_count(aa), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_reactivation_removes_expiry_index() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 100_000,
            retention_blocks: 0,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();
        let id = B256::random();

        // Add assertion
        let add_mod = create_test_modification_with_id(10, aa, 0, id);
        store.apply_pending_modifications(vec![add_mod])?;

        // Remove it (creates expiry index)
        let remove_mod = create_test_modification_remove(100, aa, 1, id);
        store.apply_pending_modifications(vec![remove_mod])?;
        assert_eq!(store.expiry_index_count(), 1);

        // Re-add it (should remove expiry index)
        let readd_mod = create_test_modification_with_id(200, aa, 2, id);
        store.apply_pending_modifications(vec![readd_mod])?;

        // Expiry index should be cleared (re-activation)
        assert_eq!(store.expiry_index_count(), 0);

        // Prune should not remove anything
        let pruned = store.prune_now(150)?;
        assert_eq!(pruned, 0);
        assert_eq!(store.assertion_contract_count(aa), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_background_task_prunes() -> Result<(), AssertionStoreError> {
        let config = PruneConfig {
            interval_ms: 50, // Very short interval for testing
            retention_blocks: 10,
        };
        let store = AssertionStore::new_ephemeral_with_config(config)?;
        let aa = Address::random();

        // Add and remove an assertion
        let add_mod = create_test_modification(10, aa, 0);
        let assertion_id = add_mod.assertion_contract_id();
        store.apply_pending_modifications(vec![add_mod])?;

        let remove_mod = create_test_modification_remove(100, aa, 1, assertion_id);
        store.apply_pending_modifications(vec![remove_mod])?;

        assert_eq!(store.assertion_contract_count(aa), 1);

        // Set current block high enough to trigger pruning (100 + 10 retention = 110)
        store.set_current_block(120);

        // Wait for background task to run
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // Should have been pruned by background task
        assert_eq!(store.assertion_contract_count(aa), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_background_task_shutdown_on_drop() {
        let config = PruneConfig {
            interval_ms: 10,
            retention_blocks: 0,
        };

        let task_handle = {
            let store = AssertionStore::new_ephemeral_with_config(config).unwrap();
            // Clone the task handle to check its state after drop
            Arc::clone(&store._prune_task)
        };
        // Store is dropped here, which should signal shutdown

        // Give the task time to receive shutdown signal and exit
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Task should have finished
        assert!(task_handle.is_finished());
    }
}
