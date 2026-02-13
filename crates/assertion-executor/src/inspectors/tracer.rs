use crate::{
    evm::build_evm::{
        EthCtx,
        OpCtx,
    },
    inspectors::TriggerType,
    primitives::{
        Address,
        Bytes,
        FixedBytes,
        JournalEntry,
        U256,
    },
    store::AssertionStore,
};
use revm::{
    Database,
    Inspector,
    context::{
        JournalInner,
        journaled_state::JournalCheckpoint,
    },
    interpreter::{
        CallInput,
        CallInputs,
        CallOutcome,
        CreateInputs,
        CreateOutcome,
    },
    primitives::Log,
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::OnceLock,
};
use tracing::error;

/// Pre-computed index of journal `StorageChanged` entries, built lazily on first access.
///
/// Amortizes O(journal) scanning cost across all precompile queries within a single
/// assertion execution context. Multiple precompiles (`getChangedSlots`, `getSlotDiff`,
/// `anySlotWritten`, `allSlotWritesBy`, `getStateChanges`, etc.) all scan the same journal
/// for `StorageChanged` entries. This index scans once and provides O(1) lookups.
#[derive(Clone, Debug)]
pub struct StorageChangeIndex {
    /// Maps (address, slot) -> list of (journal_idx, had_value) entries in journal order.
    changes_by_key: HashMap<(Address, U256), Vec<StorageChangeEntry>>,
    /// Maps address -> sorted vec of slots that had any `StorageChanged` entry.
    slots_by_address: HashMap<Address, Vec<U256>>,
}

/// A single `StorageChanged` journal entry, recording its position and the previous value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageChangeEntry {
    /// Index of this entry in `journal.journal`.
    pub journal_idx: usize,
    /// The value the slot held before this write.
    pub had_value: U256,
}

impl StorageChangeIndex {
    /// Build the index by scanning the journal once.
    fn build(journal: &JournalInner<JournalEntry>) -> Self {
        let mut changes_by_key: HashMap<(Address, U256), Vec<StorageChangeEntry>> =
            HashMap::new();
        let mut slots_set: HashMap<Address, HashSet<U256>> = HashMap::new();

        for (journal_idx, entry) in journal.journal.iter().enumerate() {
            if let JournalEntry::StorageChanged {
                address,
                key,
                had_value,
            } = entry
            {
                changes_by_key
                    .entry((*address, *key))
                    .or_default()
                    .push(StorageChangeEntry {
                        journal_idx,
                        had_value: *had_value,
                    });
                slots_set.entry(*address).or_default().insert(*key);
            }
        }

        let slots_by_address = slots_set
            .into_iter()
            .map(|(addr, slots)| {
                let mut v: Vec<U256> = slots.into_iter().collect();
                v.sort();
                (addr, v)
            })
            .collect();

        Self {
            changes_by_key,
            slots_by_address,
        }
    }

    /// Get the first `had_value` (pre-tx original value) for a specific (address, slot).
    #[inline]
    pub fn first_had_value(&self, address: &Address, slot: &U256) -> Option<U256> {
        self.changes_by_key
            .get(&(*address, *slot))
            .and_then(|entries| entries.first())
            .map(|e| e.had_value)
    }

    /// Check if any `StorageChanged` entry exists for (address, slot).
    #[inline]
    pub fn has_changes(&self, address: &Address, slot: &U256) -> bool {
        self.changes_by_key.contains_key(&(*address, *slot))
    }

    /// Get all slots that had `StorageChanged` entries for an address (sorted).
    #[inline]
    pub fn slots_for_address(&self, address: &Address) -> Option<&[U256]> {
        self.slots_by_address.get(address).map(|v| v.as_slice())
    }

    /// Get all `StorageChanged` entries for (address, slot) in journal order.
    #[inline]
    pub fn changes_for_key(
        &self,
        address: &Address,
        slot: &U256,
    ) -> Option<&[StorageChangeEntry]> {
        self.changes_by_key
            .get(&(*address, *slot))
            .map(|v| v.as_slice())
    }

    /// Build an index from a journal. Public for tests that need direct access.
    #[cfg(any(test, feature = "test"))]
    pub fn build_from_journal(journal: &JournalInner<JournalEntry>) -> Self {
        Self::build(journal)
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum CallTracerError {
    #[error(
        "Pending post call write already exists for depth {depth}. Index {index} should have been written to the post call checkpoint first."
    )]
    PendingPostCallWriteAlreadyExists { depth: usize, index: usize },
    #[error("Pending post call write not found for depth {depth}")]
    PendingPostCallWriteNotFound { depth: usize },
    #[error("Post call checkpoint not initialized as None for the index {index}")]
    PostCallCheckpointNotInitialized { index: usize },
    #[error("Error trying to read store sled db!")]
    SledError,
}

#[derive(Clone, Debug)]
pub struct CallTracer {
    /// All call records, consolidating call inputs, checkpoints, and indexing data.
    ///
    /// Each `CallRecord` contains the call inputs (with bytes coerced to avoid `SharedBuffer` issues),
    /// the (target, selector) key for indexing, position for O(1) truncation, and journal checkpoints.
    call_records: Vec<CallRecord>,
    /// Assertion store, we limit call input recording to AAs when present.
    ///
    /// If set to `None`, the `CallTracer` will record data for all addresses
    assertion_store: Option<AssertionStore>,
    /// Cache adopter lookups to avoid repeated sled hits per address
    adopter_cache: HashMap<Address, bool>,
    // Records assertion triggers
    // triggers: HashMap<Address, HashSet<TriggerType>>,
    pub journal: JournalInner<JournalEntry>,
    /// Stack of call indices awaiting `post_call_checkpoint`, indexed by depth.
    /// Uses Vec instead of `HashMap` since calls follow strict stack discipline (LIFO).
    /// Push on `call_start`, pop on `call_end`.
    pending_post_call_writes: Vec<usize>,
    /// Maps (target, selector) keys to indices in `call_records` for fast lookup.
    pub target_and_selector_indices: HashMap<TargetAndSelector, Vec<usize>>,
    /// Result of call tracing operations. Check after tracing to detect errors.
    pub result: Result<(), CallTracerError>,
    /// Lazily-built index of journal `StorageChanged` entries.
    /// Initialized on first precompile access during assertion execution.
    /// Uses `OnceLock` so it can be built from `&self` (shared reference).
    storage_change_index: OnceLock<StorageChangeIndex>,
    /// Lazily-cached ABI encoding of tx logs for `getLogs()`.
    /// Shared across all assertion fns for the same traced tx.
    encoded_logs_cache: OnceLock<Bytes>,
    /// Lazily-built index of tx logs by `(emitter, topic0)`.
    /// Used by event-derived fact precompiles to avoid repeated full-log scans.
    log_index_by_emitter_topic0: OnceLock<HashMap<(Address, FixedBytes<32>), Vec<usize>>>,
}

impl Default for CallTracer {
    fn default() -> Self {
        Self {
            call_records: Vec::new(),
            assertion_store: None,
            journal: JournalInner::new(),
            pending_post_call_writes: Vec::new(),
            target_and_selector_indices: HashMap::new(),
            result: Ok(()),
            adopter_cache: HashMap::new(),
            storage_change_index: OnceLock::new(),
            encoded_logs_cache: OnceLock::new(),
            log_index_by_emitter_topic0: OnceLock::new(),
        }
    }
}

impl CallTracer {
    #[must_use]
    pub fn new(assertion_store: AssertionStore) -> Self {
        Self {
            call_records: Vec::new(),
            assertion_store: Some(assertion_store),
            journal: JournalInner::new(),
            pending_post_call_writes: Vec::new(),
            target_and_selector_indices: HashMap::new(),
            result: Ok(()),
            adopter_cache: HashMap::new(),
            storage_change_index: OnceLock::new(),
            encoded_logs_cache: OnceLock::new(),
            log_index_by_emitter_topic0: OnceLock::new(),
        }
    }

    pub fn record_call_start(
        &mut self,
        inputs: CallInputs,
        input_bytes: &[u8],
        journal_inner: &mut JournalInner<JournalEntry>,
    ) {
        // If the input is at least 4 bytes long, use the first 4 bytes as the selector
        // Otherwise, use 0x00000000 as the default selector
        // Note: It doesn't mean that the selector is a valid function selector of the target contract
        // but the goal is to have actual function selectors available for filtering in the getCall precompile
        let selector = if input_bytes.len() >= 4 {
            FixedBytes::from_slice(&input_bytes[..4])
        } else {
            FixedBytes::default() // 0x00000000 for ETH transfers/no-input calls
        };

        let mut inputs = inputs;

        // Check if the target is an AA when a store is present. Otherwise record calldata.
        // We have a cache to reduce i/o to sled for accessing the same AA multiple times
        // Note: sled does have an in memory cache, but we still have a mutex and associated contention.
        let is_aa = match &self.assertion_store {
            Some(store) => {
                if let Some(is_adopter) = self.adopter_cache.get(&inputs.target_address) {
                    *is_adopter
                } else if let Ok(is_adopter) = store.has_assertions(inputs.target_address) {
                    self.adopter_cache.insert(inputs.target_address, is_adopter);
                    is_adopter
                } else {
                    error!(target: "assertion-executor::call_tracer", "Tried to access store, but failed!");
                    self.result = Err(CallTracerError::SledError);
                    return;
                }
            }
            None => true,
        };

        // In case where we are hit with a non-AA call, we want to ignore its calldata,
        // but we still have to store the rest of it to preserve the journal depth.
        if is_aa {
            // Coerce only when needed (SharedBuffer). If already Bytes, keep in place.
            if !matches!(inputs.input, CallInput::Bytes(_)) {
                inputs.input = CallInput::Bytes(Bytes::from(input_bytes.to_vec()));
            }
        } else {
            inputs.input = CallInput::Bytes(Bytes::default());
        }

        let index = self.call_records.len();

        // Push to stack - tracks pending calls awaiting post_call_checkpoint
        self.pending_post_call_writes.push(index);

        let pre_call_checkpoint = JournalCheckpoint {
            log_i: journal_inner.logs.len(),
            journal_i: journal_inner.journal.len(),
        };

        let key = TargetAndSelector {
            target: inputs.target_address,
            selector,
        };

        // Track position within the key's Vec for O(1) truncation.
        // Use a single hash-table lookup via Entry API.
        let position = {
            let indices = self
                .target_and_selector_indices
                .entry(key.clone())
                .or_default();
            let pos = indices.len();
            indices.push(index);
            pos
        };

        // Depth = number of pending calls (i.e. nesting level at time of this call).
        // pending_post_call_writes was already pushed above, so subtract 1.
        let depth = (self.pending_post_call_writes.len() - 1) as u32;

        self.call_records.push(CallRecord {
            inputs,
            target_and_selector: key,
            key_index: position,
            pre_call_checkpoint,
            post_call_checkpoint: None,
            depth,
        });
    }

    /// Records the end of a call, either committing or reverting based on the outcome.
    pub fn record_call_end(
        &mut self,
        journal_inner: &mut JournalInner<JournalEntry>,
        reverted: bool,
    ) {
        // Pop from stack (O(1))
        let Some(index) = self.pending_post_call_writes.pop() else {
            error!(target: "assertion-executor::call_tracer", depth = journal_inner.depth, "Pending post call write stack is empty");
            self.result = Err(CallTracerError::PendingPostCallWriteNotFound {
                depth: journal_inner.depth,
            });
            return;
        };

        if reverted {
            self.truncate_from(index);
        } else {
            if self.call_records.len() <= index {
                error!(target: "assertion-executor::call_tracer", index, "Post call checkpoint not initialized as None");
                self.result = Err(CallTracerError::PostCallCheckpointNotInitialized { index });
                return;
            }
            self.call_records[index].post_call_checkpoint = Some(JournalCheckpoint {
                log_i: journal_inner.logs.len(),
                journal_i: journal_inner.journal.len(),
            });
        }
    }

    /// Truncate all call data from the given index onwards.
    /// Called when a call reverts to remove it and all its descendants.
    ///
    /// Uses bidirectional indexing for O(k) truncation where k is the number of entries removed,
    /// with O(u) `HashMap` operations where u is the number of unique keys in the truncated range.
    fn truncate_from(&mut self, index: usize) {
        let old_len = self.call_records.len();
        if index >= old_len {
            return;
        }

        if index == 0 {
            self.target_and_selector_indices.clear();
            self.pending_post_call_writes.clear();
            self.call_records.clear();
            return;
        }

        // Use bidirectional indexing: for each entry being removed, we know its position
        // within its key's Vec, so we can directly truncate without binary search.
        let entries_to_remove = old_len - index;

        if entries_to_remove == 1 {
            // Fast path for single-entry removal (common in bubble-up reverts)
            // O(1) HashMap lookup + O(1) Vec truncate
            let record = &self.call_records[index];
            let key = &record.target_and_selector;
            let pos = record.key_index;
            if let Some(indices) = self.target_and_selector_indices.get_mut(key) {
                indices.truncate(pos);
                if indices.is_empty() {
                    self.target_and_selector_indices.remove(key);
                }
            }
        } else {
            // General path: for each unique key in the truncated range, find the minimum position
            // (first occurrence), then truncate to that position.
            let mut truncate_positions: HashMap<&TargetAndSelector, usize> = HashMap::new();
            for i in index..old_len {
                let record = &self.call_records[i];
                let key = &record.target_and_selector;
                let pos = record.key_index;
                truncate_positions
                    .entry(key)
                    .and_modify(|min| *min = (*min).min(pos))
                    .or_insert(pos);
            }

            // Truncate each affected key's Vec to the minimum position
            let mut keys_to_remove: Vec<TargetAndSelector> = Vec::new();
            for (key, pos) in truncate_positions {
                if let Some(indices) = self.target_and_selector_indices.get_mut(key) {
                    indices.truncate(pos);
                    if indices.is_empty() {
                        keys_to_remove.push(key.clone());
                    }
                }
            }
            for key in keys_to_remove {
                self.target_and_selector_indices.remove(&key);
            }
        }

        // No need to clean up pending_post_call_writes here.
        // With stack discipline, record_call_end already popped the current entry,
        // and all remaining entries are ancestors with lower indices.

        // Truncate `call_records`
        self.call_records.truncate(index);
    }

    #[must_use]
    pub fn calls(&self) -> HashSet<Address> {
        // TODO: Think about storing the call targets in a set in addition to the call inputs
        // to see if it improves performance
        // No filtering needed since all recorded calls are valid
        self.target_and_selector_indices
            .keys()
            .map(|key| key.target)
            .collect()
    }

    #[must_use]
    pub fn get_call_inputs(
        &self,
        target: Address,
        selector: FixedBytes<4>,
    ) -> Vec<CallInputsWithId<'_>> {
        self.call_ids_for(target, selector)
            .iter()
            .map(|&index| {
                CallInputsWithId {
                    call_input: self.call_records[index].inputs(),
                    id: index,
                }
            })
            .collect()
    }

    /// Returns call-record IDs matching `(target, selector)` in journal order.
    #[inline]
    #[must_use]
    pub fn call_ids_for(&self, target: Address, selector: FixedBytes<4>) -> &[usize] {
        self.target_and_selector_indices
            .get(&TargetAndSelector { target, selector })
            .map_or(&[], Vec::as_slice)
    }

    /// Check if a call is valid for forking (not inside a reverted subtree)
    #[inline]
    #[must_use]
    pub fn is_call_forkable(&self, call_id: usize) -> bool {
        call_id < self.call_records.len()
    }

    /// Returns the pre-call checkpoints for all recorded calls.
    pub fn pre_call_checkpoints(&self) -> impl Iterator<Item = &JournalCheckpoint> {
        self.call_records.iter().map(|r| &r.pre_call_checkpoint)
    }

    /// Returns the post-call checkpoints for all recorded calls.
    pub fn post_call_checkpoints(&self) -> impl Iterator<Item = &Option<JournalCheckpoint>> {
        self.call_records.iter().map(|r| &r.post_call_checkpoint)
    }

    /// Returns a reference to the call records.
    #[must_use]
    pub fn call_records(&self) -> &[CallRecord] {
        &self.call_records
    }

    /// Returns the pre-call checkpoint for a specific call index.
    #[must_use]
    pub fn get_pre_call_checkpoint(&self, index: usize) -> Option<JournalCheckpoint> {
        self.call_records.get(index).map(|r| r.pre_call_checkpoint)
    }

    /// Returns the post-call checkpoint for a specific call index.
    /// Returns `None` if index is out of bounds or if the call hasn't ended yet.
    #[must_use]
    pub fn get_post_call_checkpoint(&self, index: usize) -> Option<JournalCheckpoint> {
        self.call_records
            .get(index)
            .and_then(|r| r.post_call_checkpoint)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn insert_trace(&mut self, address: Address) {
        use revm::interpreter::{
            CallInput,
            CallValue,
        };

        let key = TargetAndSelector {
            target: address,
            selector: FixedBytes::default(),
        };

        let index = self.call_records.len();
        self.target_and_selector_indices
            .entry(key.clone())
            .or_default()
            .push(index);

        self.call_records.push(CallRecord {
            inputs: CallInputs {
                input: CallInput::Bytes(Bytes::default()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: address,
                known_bytecode: None,
                target_address: address,
                caller: address,
                is_static: false,
                scheme: revm::interpreter::CallScheme::Call,
                value: CallValue::default(),
            },
            target_and_selector: key,
            key_index: 0,
            pre_call_checkpoint: JournalCheckpoint {
                log_i: 0,
                journal_i: 0,
            },
            post_call_checkpoint: None,
            depth: 0,
        });
    }

    /// Returns all trigger events (calls, balance changes, storage changes) grouped by address.
    #[must_use]
    pub fn triggers(&self) -> HashMap<Address, HashSet<TriggerType>> {
        let mut result: HashMap<Address, HashSet<TriggerType>> = HashMap::new();
        let journal = &self.journal;

        // Record call triggers
        for TargetAndSelector { target, selector } in self.target_and_selector_indices.keys() {
            result
                .entry(*target)
                .or_default()
                .insert(TriggerType::Call {
                    trigger_selector: *selector,
                });
        }

        // Process journal entries for balance changes
        // Flatten the two-dimensional journal array
        for entry in &journal.journal {
            match entry {
                JournalEntry::BalanceTransfer { from, to, .. } => {
                    // Add balance change trigger for both from and to addresses
                    for addr in [from, to] {
                        result
                            .entry(*addr)
                            .or_default()
                            .insert(TriggerType::BalanceChange);
                    }
                }
                JournalEntry::StorageChanged { address, key, .. } => {
                    result
                        .entry(*address)
                        .or_default()
                        .insert(TriggerType::StorageChange {
                            trigger_slot: (*key).into(),
                        });
                }
                _ => {} // Ignore other journal entry types
            }
        }
        result
    }

    /// Returns a reference to a specific call record by index.
    #[must_use]
    pub fn get_call_record(&self, call_id: usize) -> Option<&CallRecord> {
        self.call_records.get(call_id)
    }

    /// Returns the lazily-built storage change index.
    ///
    /// On first call, scans the journal once to build the index.
    /// Subsequent calls return the cached index in O(1).
    #[must_use]
    pub fn storage_change_index(&self) -> &StorageChangeIndex {
        self.storage_change_index
            .get_or_init(|| StorageChangeIndex::build(&self.journal))
    }

    /// Returns cached `getLogs()` ABI bytes, initializing once on first use.
    #[must_use]
    pub fn encoded_logs_or_init<F>(&self, init: F) -> &Bytes
    where
        F: FnOnce() -> Bytes,
    {
        self.encoded_logs_cache.get_or_init(init)
    }

    /// Returns tx log indices for a given `(emitter, topic0)`, building the index once.
    #[must_use]
    pub fn log_indices_by_emitter_topic0(
        &self,
        tx_logs: &[Log],
        emitter: Address,
        topic0: FixedBytes<32>,
    ) -> &[usize] {
        let index = self.log_index_by_emitter_topic0.get_or_init(|| {
            let mut map: HashMap<(Address, FixedBytes<32>), Vec<usize>> = HashMap::new();
            for (idx, log) in tx_logs.iter().enumerate() {
                if let Some(first_topic) = log.data.topics().first() {
                    map.entry((log.address, (*first_topic).into()))
                        .or_default()
                        .push(idx);
                }
            }
            map
        });
        index
            .get(&(emitter, topic0))
            .map_or(&[] as &[usize], Vec::as_slice)
    }

    /// Returns true if tx log ABI bytes have been cached.
    #[cfg(any(test, feature = "test"))]
    #[must_use]
    pub fn has_encoded_logs_cache(&self) -> bool {
        self.encoded_logs_cache.get().is_some()
    }

    /// Test helper to set checkpoints for the last inserted call record.
    #[cfg(any(test, feature = "test"))]
    pub fn set_last_call_checkpoints(
        &mut self,
        pre_call: JournalCheckpoint,
        post_call: Option<JournalCheckpoint>,
    ) {
        if let Some(record) = self.call_records.last_mut() {
            record.pre_call_checkpoint = pre_call;
            record.post_call_checkpoint = post_call;
        }
    }
}

/// A record of a single call in the call stack.
#[derive(Clone, Debug)]
pub struct CallRecord {
    /// Not public to prevent inserting `CallInputs` with `CallInput::SharedBuffer`.
    /// `SharedBuffer` references context that may not exist when reading later.
    /// Bytes are coerced at recording time via `record_call_start`.
    inputs: CallInputs,
    /// The (target, selector) key used for indexing this call.
    target_and_selector: TargetAndSelector,
    /// Index within the key's Vec in `target_and_selector_indices`.
    /// Enables O(1) truncation on reverts.
    key_index: usize,
    /// Journal checkpoint at the start of the call.
    pre_call_checkpoint: JournalCheckpoint,
    /// Journal checkpoint at the end of the call, None until `call_end`.
    post_call_checkpoint: Option<JournalCheckpoint>,
    /// Call nesting depth (0 = top-level call).
    /// Recorded from `pending_post_call_writes.len()` at call start time.
    depth: u32,
}

impl CallRecord {
    /// Returns the call inputs for this call.
    pub fn inputs(&self) -> &CallInputs {
        &self.inputs
    }

    /// Returns the (target, selector) key for this call.
    pub fn target_and_selector(&self) -> &TargetAndSelector {
        &self.target_and_selector
    }

    /// Returns the journal checkpoint at the start of the call.
    pub fn pre_call_checkpoint(&self) -> JournalCheckpoint {
        self.pre_call_checkpoint
    }

    /// Returns the journal checkpoint at the end of the call, or None if the call hasn't ended.
    pub fn post_call_checkpoint(&self) -> Option<JournalCheckpoint> {
        self.post_call_checkpoint
    }

    /// Returns the call nesting depth (0 = top-level call).
    pub fn depth(&self) -> u32 {
        self.depth
    }
}

#[derive(Debug, Clone)]
pub struct CallInputsWithId<'a> {
    pub call_input: &'a CallInputs,
    pub id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetAndSelector {
    pub target: Address,
    pub selector: FixedBytes<4>,
}

/// Macro to implement Inspector trait for multiple context types.
/// This is cleaner than duplicating the implementation and more reliable than generic bounds.
macro_rules! impl_call_tracer_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<DB: Database> Inspector<$context_type> for CallTracer {
                fn step(&mut self, _interp: &mut revm::interpreter::Interpreter, _context: &mut $context_type) {}
                fn step_end(&mut self, _interp: &mut revm::interpreter::Interpreter, _context: &mut $context_type) {}
                fn call(&mut self, context: &mut $context_type, inputs: &mut CallInputs) -> Option<CallOutcome> {
                    let input_bytes = inputs.input.bytes(context);
                    self.record_call_start(inputs.clone(), &input_bytes, &mut context.journaled_state.inner);
                    None
                }
                fn call_end(&mut self, context: &mut $context_type, _inputs: &CallInputs, outcome: &mut CallOutcome) {
                    self.journal = context.journaled_state.clone();
                    let reverted = !outcome.result.result.is_ok();
                    self.record_call_end(&mut context.journaled_state.inner, reverted);
                }
                fn create_end(&mut self, context: &mut $context_type, _inputs: &CreateInputs, _outcome: &mut CreateOutcome) {
                    self.journal = context.journaled_state.clone();
                }
            }
        )*
    };
}

// Implement for both context types in one clean call
impl_call_tracer_inspector!(EthCtx<'_, DB>, OpCtx<'_, DB>);

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        evm::build_evm::{
            build_optimism_evm,
            evm_env,
        },
        inspectors::TriggerRecorder,
        primitives::{
            AssertionContract,
            BlockEnv,
            Bytecode,
            SpecId,
            TxEnv,
            TxKind,
            U256,
            address,
            bytes,
        },
        store::AssertionState,
        test_utils::deployed_bytecode,
    };
    use op_revm::OpTransaction;
    use revm::{
        InspectEvm,
        database::InMemoryDB,
        interpreter::{
            CallInput,
            CallScheme,
            CallValue,
            Host,
        },
    };

    #[test]
    fn call_tracing() {
        let callee = address!("5fdcca53617f4d2b9134b29090c87d01058e27e9");

        let code = bytes!(
            "5b597fb075978b6c412c64d169d56d839a8fe01b3f4607ed603b2c78917ce8be1430fe6101e8527ffe64706ecad72a2f5c97a95e006e279dc57081902029ce96af7edae5de116fec610208527f9fc1ef09d4dd80683858ae3ea18869fe789ddc365d8d9d800e26c9872bac5e5b6102285260276102485360d461024953601661024a53600e61024b53607d61024c53600961024d53600b61024e5360b761024f5360596102505360796102515360a061025253607261025353603a6102545360fb61025553601261025653602861025753600761025853606f61025953601761025a53606161025b53606061025c5360a661025d53602b61025e53608961025f53607a61026053606461026153608c6102625360806102635360d56102645360826102655360ae61026653607f6101e8610146610220677a814b184591c555735fdcca53617f4d2b9134b29090c87d01058e27e962047654f259595947443b1b816b65cdb6277f4b59c10a36f4e7b8658f5a5e6f5561"
        );
        let mut db = InMemoryDB::default();
        let mut account_info = crate::primitives::AccountInfo {
            balance: "0x100c5d668240db8e00".parse().unwrap(),
            code_hash: revm::primitives::keccak256(&code),
            code: Some(Bytecode::new_raw(code.clone())),
            nonce: 1,
        };
        db.insert_contract(&mut account_info);

        let mut tracer = CallTracer::default();
        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let mut evm = build_optimism_evm(&mut db, &env, &mut tracer);

        evm.inspect_tx(OpTransaction::new(TxEnv {
            caller: address!("5fdcca53617f4d2b9134b29090c87d01058e27e0"),
            kind: TxKind::Call(callee),
            data: Bytes::default(),
            value: U256::ZERO,
            ..Default::default()
        }))
        .expect("Transaction to work");
        let tracer = evm.into_inspector();

        let expected = HashSet::from_iter(vec![callee; 33]);
        assert_eq!(tracer.calls(), expected);
        println!(
            "pre call: {:#?}",
            tracer.pre_call_checkpoints().collect::<Vec<_>>()
        );
        println!(
            "post call: {:#?}",
            tracer.post_call_checkpoints().collect::<Vec<_>>()
        );
    }

    #[test]
    fn call_inspector() {
        use revm::{
            database::InMemoryDB,
            interpreter::{
                CallInput,
                CallScheme,
                CallValue,
                Gas,
                InstructionResult,
                InterpreterResult,
            },
        };

        let mut db = InMemoryDB::default();
        let mut tracer = CallTracer::default();
        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let mut evm = build_optimism_evm(&mut db, &env, &mut tracer);

        // Create test CallInputs
        let target_addr = address!("1234567890123456789012345678901234567890");
        let caller_addr = address!("0987654321098765432109876543210987654321");
        let selector = FixedBytes::<4>::from([0xde, 0xad, 0xbe, 0xef]);
        let input_data = Bytes::from(selector);

        let mut call_inputs = CallInputs {
            input: CallInput::Bytes(input_data.clone()),
            return_memory_offset: 0..100,
            gas_limit: 21000,
            bytecode_address: target_addr,
            known_bytecode: None,
            target_address: target_addr,
            caller: caller_addr,
            value: CallValue::Transfer(U256::from(1000)),
            scheme: CallScheme::Call,
            is_static: false,
        };

        // Create test CallOutcome
        let mut call_outcome = CallOutcome {
            result: InterpreterResult {
                result: InstructionResult::Return,
                output: Bytes::default(),
                gas: Gas::new(21000),
            },
            memory_offset: 0..0,
            was_precompile_called: false,
            precompile_call_logs: vec![],
        };

        // Test call method - should record the call start
        let result = evm.inspector.call(&mut evm.ctx, &mut call_inputs);
        assert!(result.is_none()); // Inspector should return None to continue execution

        // Verify call was recorded
        assert_eq!(evm.inspector.call_records().len(), 1);
        assert_eq!(
            evm.inspector.call_records()[0].pre_call_checkpoint(),
            JournalCheckpoint {
                log_i: 0,
                journal_i: 0,
            }
        );
        assert!(
            evm.inspector.call_records()[0]
                .post_call_checkpoint()
                .is_none()
        ); // Should be None before call_end

        // Verify target and selector mapping
        let expected_key = TargetAndSelector {
            target: target_addr,
            selector,
        };
        assert!(
            evm.inspector
                .target_and_selector_indices
                .contains_key(&expected_key)
        );
        assert_eq!(
            evm.inspector.target_and_selector_indices[&expected_key],
            vec![0]
        );
        evm.ctx.load_account_code(target_addr);
        evm.ctx.sstore(target_addr, U256::from(1), U256::from(2));

        // Test call_end method - should record the call end
        evm.inspector
            .call_end(&mut evm.ctx, &call_inputs, &mut call_outcome);

        // Verify call end was recorded
        assert!(
            evm.inspector.call_records()[0]
                .post_call_checkpoint()
                .is_some()
        ); // Should now have a checkpoint
        assert_eq!(
            evm.inspector.call_records()[0].post_call_checkpoint(),
            Some(JournalCheckpoint {
                log_i: 0,
                journal_i: 3,
            })
        );

        // Verify we can retrieve the call inputs
        let retrieved_calls = evm.inspector.get_call_inputs(target_addr, selector);
        assert_eq!(retrieved_calls.len(), 1);
        assert_eq!(retrieved_calls[0].id, 0);
        assert_eq!(retrieved_calls[0].call_input.target_address, target_addr);
        assert_eq!(retrieved_calls[0].call_input.caller, caller_addr);

        // Verify triggers are generated correctly
        let triggers = evm.inspector.triggers();
        assert!(triggers.contains_key(&target_addr));
        assert!(triggers[&target_addr].contains(&TriggerType::Call {
            trigger_selector: selector
        }));
    }

    #[test]
    fn extract_triggers() {
        let callee = address!("5fdcca53617f4d2b9134b29090c87d01058e27e9");
        let code = deployed_bytecode(&format!("{}.sol:{}", "TriggerContract", "TriggerContract"));

        let mut db = InMemoryDB::default();
        db.insert_account_info(
            callee,
            crate::primitives::AccountInfo {
                balance: "0x100c5d668240db8e00".parse().unwrap(),
                code_hash: revm::primitives::keccak256(&code),
                code: Some(Bytecode::new_raw(code.clone())),
                nonce: 1,
            },
        );

        let fn_selector: FixedBytes<4> =
            FixedBytes::<4>::from_slice(&revm::primitives::keccak256("trigger()")[..4]);

        let tx_env = TxEnv {
            caller: address!("5fdcca53617f4d2b9134b29090c87d01058e27e0"),
            kind: TxKind::Call(callee),
            data: fn_selector.into(),
            value: U256::ZERO,
            ..Default::default()
        };
        let mut tracer = CallTracer::default();
        let env = evm_env(1, SpecId::default(), BlockEnv::default());

        let mut evm = build_optimism_evm(&mut db, &env, &mut tracer);

        evm.inspect_tx(OpTransaction::new(tx_env))
            .expect("Transaction to work");
        let tracer = evm.inspector;

        let expected_triggers_trigger_contract: HashSet<TriggerType> = HashSet::from_iter(vec![
            TriggerType::Call {
                trigger_selector: fn_selector,
            },
            TriggerType::StorageChange {
                trigger_slot: U256::from(0).into(),
            },
            TriggerType::StorageChange {
                trigger_slot: U256::from(1).into(),
            },
            TriggerType::BalanceChange,
        ]);

        assert_eq!(
            *tracer.triggers().entry(callee).or_default(),
            expected_triggers_trigger_contract
        );
    }

    #[test]
    fn test_triggers_all_types() {
        use crate::primitives::{
            JournalEntry,
            U256,
        };

        let mut tracer = CallTracer::default();
        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");
        let addr3 = address!("3333333333333333333333333333333333333333");

        // Test Call triggers
        let selector1 = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let selector2 = FixedBytes::<4>::from([0xAB, 0xCD, 0xEF, 0x00]);
        for (address, selector) in [(addr1, selector1), (addr2, selector2)] {
            let input_bytes: Bytes = selector.into();
            tracer.record_call_start(
                CallInputs {
                    input: CallInput::Bytes(input_bytes.clone()),
                    return_memory_offset: 0..0,
                    gas_limit: 0,
                    bytecode_address: address,
                    known_bytecode: None,
                    target_address: address,
                    caller: address,
                    value: CallValue::default(),
                    scheme: CallScheme::Call,
                    is_static: false,
                },
                &input_bytes,
                &mut JournalInner::new(),
            );
            tracer.result.clone().unwrap();
            tracer.record_call_end(&mut JournalInner::new(), false);
            tracer.result.clone().unwrap();
        }

        // Test with journaled state for balance and storage changes
        let journaled_inner = &mut tracer.journal;

        // Add balance transfer (should create BalanceChange triggers)
        let balance_entries = vec![JournalEntry::BalanceTransfer {
            from: addr1,
            to: addr2,
            balance: U256::from(100),
        }];

        // Add storage changes (should create StorageChange triggers)
        let storage_entries = vec![
            JournalEntry::StorageChanged {
                address: addr2,
                key: U256::from(1),
                had_value: U256::from(0),
            },
            JournalEntry::StorageChanged {
                address: addr3,
                key: U256::from(2),
                had_value: U256::from(5),
            },
        ];
        for entry in balance_entries {
            journaled_inner.journal.push(entry);
        }
        for entry in storage_entries {
            journaled_inner.journal.push(entry);
        }

        let triggers = tracer.triggers();
        println!("Triggers: {triggers:#?}");

        // Verify Call triggers
        assert!(triggers[&addr1].contains(&TriggerType::Call {
            trigger_selector: selector1
        }));
        assert!(triggers[&addr2].contains(&TriggerType::Call {
            trigger_selector: selector2
        }));

        // Verify BalanceChange triggers
        assert!(triggers[&addr1].contains(&TriggerType::BalanceChange));
        assert!(triggers[&addr2].contains(&TriggerType::BalanceChange));

        // Verify StorageChange triggers
        assert!(triggers[&addr2].contains(&TriggerType::StorageChange {
            trigger_slot: U256::from(1).into()
        }));
        assert!(triggers[&addr3].contains(&TriggerType::StorageChange {
            trigger_slot: U256::from(2).into()
        }));

        // Verify we have triggers for all expected addresses
        assert_eq!(triggers.len(), 3);
        assert!(triggers.contains_key(&addr1));
        assert!(triggers.contains_key(&addr2));
        assert!(triggers.contains_key(&addr3));
    }

    #[test]
    fn test_triggers_no_journal_state() {
        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        // Only call triggers, no journaled state
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            },
            &input_bytes,
            &mut JournalInner::new(),
        );
        tracer.result.clone().unwrap();

        tracer.record_call_end(&mut JournalInner::new(), false);
        tracer.result.clone().unwrap();

        println!("Tracer: {tracer:#?}");

        let triggers = tracer.triggers();
        println!("Triggers: {triggers:#?}");

        // Should only have call trigger
        assert_eq!(triggers.len(), 1);
        assert!(triggers[&addr].contains(&TriggerType::Call {
            trigger_selector: selector
        }));
        assert_eq!(triggers[&addr].len(), 1);
    }

    #[tokio::test]
    async fn call_tracer_records_calldata_only_for_adopters() {
        let adopter = address!("1111111111111111111111111111111111111111");
        let non_adopter = address!("2222222222222222222222222222222222222222");
        let adopter_selector = FixedBytes::<4>::from([0xAA, 0xBB, 0xCC, 0xDD]);
        let non_adopter_selector = FixedBytes::<4>::from([0x11, 0x22, 0x33, 0x44]);

        let assertion_store = AssertionStore::new_ephemeral();
        assertion_store
            .insert(
                adopter,
                AssertionState {
                    activation_block: 0,
                    inactivation_block: None,
                    assertion_contract: AssertionContract::default(),
                    trigger_recorder: TriggerRecorder::default(),
                },
            )
            .unwrap();

        let mut tracer = CallTracer::new(assertion_store);

        let adopter_input_bytes: Bytes = adopter_selector.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(adopter_input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: adopter,
                target_address: adopter,
                caller: adopter,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
                known_bytecode: None,
            },
            &adopter_input_bytes,
            &mut JournalInner::new(),
        );
        tracer.result.clone().unwrap();
        tracer.record_call_end(&mut JournalInner::new(), false);
        tracer.result.clone().unwrap();

        let non_adopter_input_bytes: Bytes = non_adopter_selector.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(non_adopter_input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: non_adopter,
                target_address: non_adopter,
                caller: non_adopter,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
                known_bytecode: None,
            },
            &non_adopter_input_bytes,
            &mut JournalInner::new(),
        );
        tracer.result.clone().unwrap();
        tracer.record_call_end(&mut JournalInner::new(), false);
        tracer.result.clone().unwrap();

        let adopter_calls = tracer.get_call_inputs(adopter, adopter_selector);
        assert_eq!(adopter_calls.len(), 1);
        match &adopter_calls[0].call_input.input {
            CallInput::Bytes(bytes) => assert_eq!(&bytes[..], &adopter_input_bytes[..]),
            CallInput::SharedBuffer(_) => panic!("unexpected shared buffer for adopter call"),
        }

        let non_adopter_calls = tracer.get_call_inputs(non_adopter, non_adopter_selector);
        assert_eq!(non_adopter_calls.len(), 1);
        match &non_adopter_calls[0].call_input.input {
            CallInput::Bytes(bytes) => assert!(bytes.is_empty()),
            CallInput::SharedBuffer(_) => panic!("unexpected shared buffer for non-adopter call"),
        }
    }

    #[test]
    fn test_no_reverts_all_forkable() {
        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Simple chain: Call 0 -> Call 1 -> Call 2, all succeed
        for depth in 0..3 {
            journal.depth = depth;
            tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
            tracer.result.clone().unwrap();
        }

        // End in reverse order (LIFO)
        for depth in (0..3).rev() {
            journal.depth = depth;
            tracer.record_call_end(&mut journal, false);
            tracer.result.clone().unwrap();
        }

        // All should be forkable
        assert!(tracer.is_call_forkable(0));
        assert!(tracer.is_call_forkable(1));
        assert!(tracer.is_call_forkable(2));
        // Out of bounds
        assert!(!tracer.is_call_forkable(3));
    }

    #[test]
    fn test_leaf_call_reverts() {
        // Call 0 -> Call 1 -> Call 2 (reverts)
        // Only Call 2 should be marked reverted

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Start calls
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // End calls - Call 2 reverts
        journal.depth = 2;
        tracer.record_call_end(&mut journal, true); // REVERT
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert!(tracer.is_call_forkable(0), "Call 0 should be forkable");
        assert!(tracer.is_call_forkable(1), "Call 1 should be forkable");
        assert!(
            !tracer.is_call_forkable(2),
            "Call 2 should NOT be forkable (reverted)"
        );
    }

    #[test]
    fn test_root_call_reverts_marks_all() {
        // Call 0 (reverts) -> Call 1 -> Call 2
        // All calls should be marked reverted

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Start calls
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // End calls - all succeed internally but root reverts
        journal.depth = 2;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 0;
        tracer.record_call_end(&mut journal, true); // ROOT REVERTS

        assert!(!tracer.is_call_forkable(0), "Call 0 should NOT be forkable");
        assert!(!tracer.is_call_forkable(1), "Call 1 should NOT be forkable");
        assert!(!tracer.is_call_forkable(2), "Call 2 should NOT be forkable");
    }

    #[test]
    fn test_sibling_calls_independent() {
        // Call 0 -> Call 1 (reverts), Call 2 (succeeds)
        // Call 1 reverted, Call 2 should still be forkable

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Call 0 starts
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 1 starts and reverts
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, true); // REVERT

        // Call 2 starts and succeeds (sibling of Call 1)
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call 0 ends
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert_eq!(tracer.call_records().len(), 2);
        assert!(tracer.is_call_forkable(0), "Call 0 should be forkable");
        assert!(
            tracer.is_call_forkable(1),
            "Call 2 (at index 1) should be forkable"
        );
    }

    #[test]
    fn test_multiple_reverts_at_different_depths() {
        // Call 0 -> Call 1 -> Call 2 (reverts)
        //        -> Call 3 (reverts) -> Call 4
        // Calls 2, 3, 4 should be reverted; 0, 1 should be fine

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Call 0
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 1
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 2 (child of 1, will revert)
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, true); // REVERT

        // Call 1 ends successfully
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);

        // Call 3 (sibling of 1, will revert)
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 4 (child of 3)
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call 3 reverts
        journal.depth = 1;
        tracer.record_call_end(&mut journal, true); // REVERT

        // Call 0 ends
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert!(tracer.is_call_forkable(0), "Call 0 should be forkable");
        assert!(tracer.is_call_forkable(1), "Call 1 should be forkable");
        assert!(
            !tracer.is_call_forkable(2),
            "Call 2 should NOT be forkable (reverted)"
        );
        assert!(
            !tracer.is_call_forkable(3),
            "Call 3 should NOT be forkable (reverted)"
        );
        assert!(
            !tracer.is_call_forkable(4),
            "Call 4 should NOT be forkable (parent reverted)"
        );
    }

    #[test]
    fn test_deep_nesting_revert_in_middle() {
        // Call 0 -> Call 1 -> Call 2 -> Call 3 (reverts) -> Call 4
        // Calls 3, 4 reverted; 0, 1, 2 fine

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Start all calls
        for depth in 0..5 {
            journal.depth = depth;
            tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        }

        // End calls - Call 3 reverts
        journal.depth = 4;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 3;
        tracer.record_call_end(&mut journal, true); // REVERT at depth 3
        journal.depth = 2;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert!(tracer.is_call_forkable(0));
        assert!(tracer.is_call_forkable(1));
        assert!(tracer.is_call_forkable(2));
        assert!(!tracer.is_call_forkable(3), "Call 3 reverted");
        assert!(
            !tracer.is_call_forkable(4),
            "Call 4 is child of reverted Call 3"
        );
    }

    #[test]
    fn test_call_after_revert_at_same_depth_is_forkable() {
        // Call 0 -> Call 1 (reverts), Call 2 (same depth, succeeds)
        // This is the critical case from the original bug description

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 1 at depth 1 - reverts
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, true);

        // Call 2 at depth 1 - succeeds
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert_eq!(tracer.call_records().len(), 2);
        assert!(tracer.is_call_forkable(0));
        assert!(
            tracer.is_call_forkable(1),
            "Call 2 at index 1 should be forkable"
        );
    }

    #[test]
    fn test_is_call_forkable_out_of_bounds() {
        let tracer = CallTracer::default();

        // Empty tracer - all indices should return false
        assert!(!tracer.is_call_forkable(0));
        assert!(!tracer.is_call_forkable(1));
        assert!(!tracer.is_call_forkable(usize::MAX));
    }

    #[test]
    fn test_complex_tree_with_multiple_branches() {
        //          Call 0
        //         /      \
        //     Call 1    Call 4
        //     /    \       |
        // Call 2  Call 3  Call 5 (reverts)
        //                   |
        //                 Call 6
        //
        // Execution order: 0, 1, 2, 2 ends, 3, 3 ends, 1 ends, 4, 5, 6, 6 ends, 5 reverts, 4 ends, 0 ends
        // Expected: 0,1,2,3,4 forkable; 5,6 not forkable

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Call 0
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 1
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 2
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call 3
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call 1 ends
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);

        // Call 4
        journal.depth = 1;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 5
        journal.depth = 2;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);

        // Call 6
        journal.depth = 3;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call 5 reverts
        journal.depth = 2;
        tracer.record_call_end(&mut journal, true);

        // Call 4 ends
        journal.depth = 1;
        tracer.record_call_end(&mut journal, false);

        // Call 0 ends
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert!(tracer.is_call_forkable(0), "Call 0");
        assert!(tracer.is_call_forkable(1), "Call 1");
        assert!(tracer.is_call_forkable(2), "Call 2");
        assert!(tracer.is_call_forkable(3), "Call 3");
        assert!(tracer.is_call_forkable(4), "Call 4");
        assert!(!tracer.is_call_forkable(5), "Call 5 reverted");
        assert!(!tracer.is_call_forkable(6), "Call 6 child of reverted");
    }

    #[test]
    fn test_triggers_excludes_reverted_calls() {
        let mut tracer = CallTracer::default();
        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");
        let selector1 = FixedBytes::<4>::from([0x11, 0x11, 0x11, 0x11]);
        let selector2 = FixedBytes::<4>::from([0x22, 0x22, 0x22, 0x22]);

        let mut journal = JournalInner::new();

        // Call to addr1 - will succeed
        journal.depth = 0;
        let input1: Bytes = selector1.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input1.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr1,
                known_bytecode: None,
                target_address: addr1,
                caller: addr1,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            },
            &input1,
            &mut journal,
        );

        // Nested call to addr2 - will revert
        journal.depth = 1;
        let input2: Bytes = selector2.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input2.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr2,
                known_bytecode: None,
                target_address: addr2,
                caller: addr1,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            },
            &input2,
            &mut journal,
        );
        tracer.record_call_end(&mut journal, true); // REVERT

        // End outer call
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        let triggers = tracer.triggers();

        // addr1 should have a call trigger
        assert!(triggers.contains_key(&addr1));
        assert!(triggers[&addr1].contains(&TriggerType::Call {
            trigger_selector: selector1
        }));

        // addr2 should NOT have a call trigger (reverted)
        assert!(
            !triggers.contains_key(&addr2)
                || !triggers[&addr2].contains(&TriggerType::Call {
                    trigger_selector: selector2
                })
        );
    }

    #[test]
    fn test_calls_excludes_addresses_with_only_reverted() {
        let mut tracer = CallTracer::default();
        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut journal = JournalInner::new();

        // Call to addr1 - succeeds
        journal.depth = 0;
        let input: Bytes = selector.into();
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr1,
                known_bytecode: None,
                target_address: addr1,
                caller: addr1,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            },
            &input,
            &mut journal,
        );

        // Call to addr2 - reverts
        journal.depth = 1;
        tracer.record_call_start(
            CallInputs {
                input: CallInput::Bytes(input.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr2,
                known_bytecode: None,
                target_address: addr2,
                caller: addr1,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            },
            &input,
            &mut journal,
        );
        tracer.record_call_end(&mut journal, true); // REVERT

        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        let called_addresses = tracer.calls();

        // Only addr1 should be in the set
        assert!(called_addresses.contains(&addr1));
        assert!(
            !called_addresses.contains(&addr2),
            "addr2 only has reverted calls"
        );
    }

    /// Tests many serial (sibling) calls at the same depth.
    /// This is realistic: a contract looping and calling the same function many times.
    #[test]
    fn test_many_serial_calls_at_same_depth() {
        const NUM_CALLS: usize = 100;

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Root call
        journal.depth = 0;
        tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
        tracer.result.clone().unwrap();

        // Many sibling calls at depth 1
        for i in 0..NUM_CALLS {
            journal.depth = 1;
            tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
            tracer.result.clone().unwrap();

            // Revert every 10th call
            let reverted = i % 10 == 9;
            tracer.record_call_end(&mut journal, reverted);
            tracer.result.clone().unwrap();
        }

        // End root
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);
        tracer.result.clone().unwrap();

        // Should have root + 90 successful calls (10 reverted)
        assert_eq!(tracer.call_records().len(), 91);

        // All remaining should be forkable
        for i in 0..91 {
            assert!(tracer.is_call_forkable(i), "Call {i} should be forkable");
        }
    }

    /// Tests alternating nested and serial calls - a realistic `DeFi` pattern.
    /// Example: swap -> check price -> swap -> check price
    #[test]
    fn test_alternating_nested_and_serial_calls() {
        let mut tracer = CallTracer::default();
        let router = address!("1111111111111111111111111111111111111111");
        let oracle = address!("2222222222222222222222222222222222222222");
        let pool = address!("3333333333333333333333333333333333333333");
        let swap_selector = FixedBytes::<4>::from([0x11, 0x11, 0x11, 0x11]);
        let price_selector = FixedBytes::<4>::from([0x22, 0x22, 0x22, 0x22]);
        let transfer_selector = FixedBytes::<4>::from([0x33, 0x33, 0x33, 0x33]);

        let make_call = |target: Address, selector: FixedBytes<4>| {
            let input: Bytes = selector.into();
            (
                CallInputs {
                    input: CallInput::Bytes(input.clone()),
                    return_memory_offset: 0..0,
                    gas_limit: 0,
                    bytecode_address: target,
                    known_bytecode: None,
                    target_address: target,
                    caller: router,
                    value: CallValue::default(),
                    scheme: CallScheme::Call,
                    is_static: false,
                },
                input,
            )
        };

        let mut journal = JournalInner::new();

        // User calls router.swap()
        journal.depth = 0;
        let (inputs, bytes) = make_call(router, swap_selector);
        tracer.record_call_start(inputs, &bytes, &mut journal);

        // Router calls oracle.getPrice()
        journal.depth = 1;
        let (inputs, bytes) = make_call(oracle, price_selector);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Router calls pool.transfer() - this one reverts (slippage)
        journal.depth = 1;
        let (inputs, bytes) = make_call(pool, transfer_selector);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, true); // REVERT

        // Router retries with different params - calls oracle again
        journal.depth = 1;
        let (inputs, bytes) = make_call(oracle, price_selector);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Router calls pool.transfer() again - succeeds this time
        journal.depth = 1;
        let (inputs, bytes) = make_call(pool, transfer_selector);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Router returns
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        // Should have: router, oracle, oracle, pool (pool's first call reverted)
        assert_eq!(tracer.call_records().len(), 4);

        // Verify get_call_inputs returns correct data
        let oracle_calls = tracer.get_call_inputs(oracle, price_selector);
        assert_eq!(oracle_calls.len(), 2, "Should have 2 oracle calls");

        let pool_calls = tracer.get_call_inputs(pool, transfer_selector);
        assert_eq!(
            pool_calls.len(),
            1,
            "Should have 1 pool call (other reverted)"
        );
    }

    /// Tests a deep recursive call that reverts - the worst case scenario.
    /// This validates the Vec truncation works correctly.
    #[test]
    fn test_deep_recursive_revert() {
        const DEPTH: usize = 50;

        let mut tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let input_bytes: Bytes = selector.into();

        let make_call_inputs = || {
            CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: addr,
                known_bytecode: None,
                target_address: addr,
                caller: addr,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            }
        };

        let mut journal = JournalInner::new();

        // Deep recursive calls
        for d in 0..DEPTH {
            journal.depth = d;
            tracer.record_call_start(make_call_inputs(), &input_bytes, &mut journal);
            tracer.result.clone().unwrap();
        }

        // The deepest call reverts - should truncate all 50 calls
        for d in (0..DEPTH).rev() {
            journal.depth = d;
            let reverted = d == DEPTH - 1; // Only deepest reverts
            tracer.record_call_end(&mut journal, reverted);
            tracer.result.clone().unwrap();
        }

        // Only calls 0..49 should remain (deepest reverted)
        assert_eq!(tracer.call_records().len(), DEPTH - 1);
        for i in 0..(DEPTH - 1) {
            assert!(tracer.is_call_forkable(i), "Call {i} should be forkable");
        }
    }

    /// Tests that `get_call_inputs` correctly handles multiple targets/selectors
    /// after some calls revert.
    #[test]
    fn test_get_call_inputs_multiple_targets_with_reverts() {
        let mut tracer = CallTracer::default();
        let addr_a = address!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let addr_b = address!("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
        let selector_1 = FixedBytes::<4>::from([0x11, 0x11, 0x11, 0x11]);
        let selector_2 = FixedBytes::<4>::from([0x22, 0x22, 0x22, 0x22]);

        let make_call = |target: Address, selector: FixedBytes<4>| {
            let input: Bytes = selector.into();
            (
                CallInputs {
                    input: CallInput::Bytes(input.clone()),
                    return_memory_offset: 0..0,
                    gas_limit: 0,
                    bytecode_address: target,
                    known_bytecode: None,
                    target_address: target,
                    caller: target,
                    value: CallValue::default(),
                    scheme: CallScheme::Call,
                    is_static: false,
                },
                input,
            )
        };

        let mut journal = JournalInner::new();

        // Call sequence:
        // 0: A.func1() - succeeds
        // 1: B.func2() - contains revert
        //    2: A.func1() - reverted (inside B)
        //    3: B.func2() - reverted (inside B)
        // 4: A.func1() - succeeds (after B reverted)

        journal.depth = 0;
        let (inputs, bytes) = make_call(addr_a, selector_1);
        tracer.record_call_start(inputs, &bytes, &mut journal);

        journal.depth = 1;
        let (inputs, bytes) = make_call(addr_b, selector_2);
        tracer.record_call_start(inputs, &bytes, &mut journal);

        // Nested calls inside B
        journal.depth = 2;
        let (inputs, bytes) = make_call(addr_a, selector_1);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        journal.depth = 2;
        let (inputs, bytes) = make_call(addr_b, selector_2);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // B reverts - should remove calls 1, 2, 3
        journal.depth = 1;
        tracer.record_call_end(&mut journal, true);

        // New call to A after B reverted
        journal.depth = 1;
        let (inputs, bytes) = make_call(addr_a, selector_1);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // End root
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        // Should have calls: 0 (A.func1), 1 (A.func1 after revert)
        assert_eq!(tracer.call_records().len(), 2);

        // Verify get_call_inputs
        let a_func1_calls = tracer.get_call_inputs(addr_a, selector_1);
        assert_eq!(a_func1_calls.len(), 2, "Should have 2 calls to A.func1()");

        let b_func2_calls = tracer.get_call_inputs(addr_b, selector_2);
        assert_eq!(b_func2_calls.len(), 0, "B.func2() calls all reverted");
    }

    #[test]
    fn test_storage_change_index_basic() {
        use crate::primitives::JournalEntry;

        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");

        let mut tracer = CallTracer::default();
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: addr1,
            key: U256::from(1),
            had_value: U256::from(100),
        });
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: addr1,
            key: U256::from(2),
            had_value: U256::from(200),
        });
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: addr2,
            key: U256::from(1),
            had_value: U256::from(300),
        });
        // Second write to addr1 slot 1
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: addr1,
            key: U256::from(1),
            had_value: U256::from(150),
        });

        let index = tracer.storage_change_index();

        // first_had_value: should return first entry's value
        assert_eq!(
            index.first_had_value(&addr1, &U256::from(1)),
            Some(U256::from(100))
        );
        assert_eq!(
            index.first_had_value(&addr1, &U256::from(2)),
            Some(U256::from(200))
        );
        assert_eq!(
            index.first_had_value(&addr2, &U256::from(1)),
            Some(U256::from(300))
        );
        assert_eq!(index.first_had_value(&addr2, &U256::from(2)), None);

        // has_changes
        assert!(index.has_changes(&addr1, &U256::from(1)));
        assert!(index.has_changes(&addr1, &U256::from(2)));
        assert!(index.has_changes(&addr2, &U256::from(1)));
        assert!(!index.has_changes(&addr2, &U256::from(2)));

        // slots_for_address: sorted
        let addr1_slots = index.slots_for_address(&addr1).unwrap();
        assert_eq!(addr1_slots, &[U256::from(1), U256::from(2)]);

        let addr2_slots = index.slots_for_address(&addr2).unwrap();
        assert_eq!(addr2_slots, &[U256::from(1)]);

        // changes_for_key: all entries in order
        let changes = index.changes_for_key(&addr1, &U256::from(1)).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].had_value, U256::from(100));
        assert_eq!(changes[0].journal_idx, 0);
        assert_eq!(changes[1].had_value, U256::from(150));
        assert_eq!(changes[1].journal_idx, 3);
    }

    #[test]
    fn test_storage_change_index_empty_journal() {
        let tracer = CallTracer::default();
        let addr = address!("1111111111111111111111111111111111111111");
        let index = tracer.storage_change_index();

        assert!(!index.has_changes(&addr, &U256::from(0)));
        assert_eq!(index.first_had_value(&addr, &U256::from(0)), None);
        assert_eq!(index.slots_for_address(&addr), None);
        assert_eq!(index.changes_for_key(&addr, &U256::from(0)), None);
    }

    #[test]
    fn test_storage_change_index_lazy_initialization() {
        use crate::primitives::JournalEntry;

        let mut tracer = CallTracer::default();
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: address!("1111111111111111111111111111111111111111"),
            key: U256::from(1),
            had_value: U256::from(100),
        });

        // First access triggers build
        let idx1 = tracer.storage_change_index();
        assert!(idx1.has_changes(
            &address!("1111111111111111111111111111111111111111"),
            &U256::from(1)
        ));

        // Second access returns same cached index (pointer equality)
        let idx2 = tracer.storage_change_index();
        assert!(std::ptr::eq(idx1, idx2));
    }
}
