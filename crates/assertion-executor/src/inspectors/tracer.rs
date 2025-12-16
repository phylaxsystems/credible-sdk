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
};
use std::collections::{
    HashMap,
    HashSet,
};
use tracing::error;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TargetAndSelector {
    pub target: Address,
    pub selector: FixedBytes<4>,
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
    // Not public to prohibit inserting CallInputs with CallInput::SharedBuffer
    // If call_inputs with CallInput::SharedBuffer are inserted, then accessing the data without the previous context will be problematic
    // This is problematic as you would be required to pass the Context as it was,
    // at the start of the call,  to read the bytes of the CallInput,
    // if they are of the SharedBuffer variant.
    // You otherwise would no longer have access to this data in the future when
    // you want to read call_inputs from the tracer.
    // Because of this, we coerce the bytes at the time of recording the call.
    call_inputs: Vec<CallInputs>,
    /// Parallel to `call_inputs`: the `(target, selector)` key used for indexing each recorded call.
    ///
    /// This lets truncation remove indices without scanning all map entries.
    target_and_selector_by_call: Vec<TargetAndSelector>,
    /// Assertion store, we limit call input recording to AAs when present.
    ///
    /// If set to `None`, the `CallTracer` will record data for all addresses
    assertion_store: Option<AssertionStore>,
    /// Cache adopter lookups to avoid repeated sled hits per address
    adopter_cache: HashMap<Address, bool>,
    // Records assertion triggers
    // triggers: HashMap<Address, HashSet<TriggerType>>,
    pub journal: JournalInner<JournalEntry>,
    pub pre_call_checkpoints: Vec<JournalCheckpoint>,
    pub post_call_checkpoints: Vec<Option<JournalCheckpoint>>,
    // Map depth to the index of the call input that is awaiting a post_call_checkpoint
    pending_post_call_writes: HashMap<usize, usize>,
    pub target_and_selector_indices: HashMap<TargetAndSelector, Vec<usize>>,
    pub result: Result<(), CallTracerError>,
}
impl Default for CallTracer {
    fn default() -> Self {
        Self {
            call_inputs: Vec::new(),
            target_and_selector_by_call: Vec::new(),
            assertion_store: None,
            journal: JournalInner::new(),
            pre_call_checkpoints: Vec::new(),
            post_call_checkpoints: Vec::new(),
            pending_post_call_writes: HashMap::new(),
            target_and_selector_indices: HashMap::new(),
            result: Ok(()),
            adopter_cache: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CallInputsWithId<'a> {
    pub call_input: &'a CallInputs,
    pub id: usize,
}

impl CallTracer {
    pub fn new(assertion_store: AssertionStore) -> Self {
        Self {
            call_inputs: Vec::new(),
            target_and_selector_by_call: Vec::new(),
            assertion_store: Some(assertion_store),
            journal: JournalInner::new(),
            pre_call_checkpoints: Vec::new(),
            post_call_checkpoints: Vec::new(),
            pending_post_call_writes: HashMap::new(),
            target_and_selector_indices: HashMap::new(),
            result: Ok(()),
            adopter_cache: HashMap::new(),
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
        // unwrap
        if is_aa {
            // Coerce the bytes at the time of recording the call,
            // in case they are of the SharedBuffer variant
            inputs.input = CallInput::Bytes(Bytes::from(input_bytes.to_vec()));
        } else {
            inputs.input = CallInput::Bytes(Bytes::default());
        }

        let index = self.call_inputs.len();
        let depth = journal_inner.depth;
        if let Some(existing_index) = self.pending_post_call_writes.insert(depth, index) {
            error!(target: "assertion-executor::call_tracer", depth, existing_index, "Tried to insert pending_post_call_write, but pending post call write already exists.");
            self.result = Err(CallTracerError::PendingPostCallWriteAlreadyExists {
                depth,
                index: existing_index,
            });
            return;
        }

        let checkpoint = JournalCheckpoint {
            log_i: journal_inner.logs.len(),
            journal_i: journal_inner.journal.len(),
        };

        self.pre_call_checkpoints.push(checkpoint);
        self.post_call_checkpoints.push(None);

        let key = TargetAndSelector {
            target: inputs.target_address,
            selector,
        };

        self.target_and_selector_indices
            .entry(key.clone())
            .or_default()
            .push(index);

        self.call_inputs.push(inputs);
        self.target_and_selector_by_call.push(key);
    }

    pub fn record_call_end(
        &mut self,
        journal_inner: &mut JournalInner<JournalEntry>,
        reverted: bool,
    ) {
        if let Some(index) = self.pending_post_call_writes.remove(&journal_inner.depth) {
            if reverted {
                self.truncate_from(index);
            } else {
                if self.post_call_checkpoints.len() <= index {
                    error!(target: "assertion-executor::call_tracer", index, "Post call checkpoint not initialized as None");
                    self.result = Err(CallTracerError::PostCallCheckpointNotInitialized { index });
                    return;
                }
                self.post_call_checkpoints[index] = Some(JournalCheckpoint {
                    log_i: journal_inner.logs.len(),
                    journal_i: journal_inner.journal.len(),
                });
            }
        } else {
            error!(target: "assertion-executor::call_tracer", depth = journal_inner.depth, "Pending post call write not found");
            self.result = Err(CallTracerError::PendingPostCallWriteNotFound {
                depth: journal_inner.depth,
            });
        }
    }

    /// Truncate all call data from the given index onwards.
    /// Called when a call reverts to remove it and all its descendants.
    fn truncate_from(&mut self, index: usize) {
        let old_len = self.call_inputs.len();
        if index >= old_len {
            return;
        }

        if index == 0 {
            self.target_and_selector_indices.clear();
            self.pending_post_call_writes.clear();
            self.call_inputs.clear();
            self.pre_call_checkpoints.clear();
            self.post_call_checkpoints.clear();
            self.target_and_selector_by_call.clear();
            return;
        }

        // Remove indices for the calls we're dropping without scanning the entire map.
        //
        // Invariant: each per-key `Vec<usize>` is append-only and sorted, so truncating calls from
        // `index..old_len` corresponds to removing the last `count` indices for each affected key.
        // We batch consecutive calls that share the same key to avoid hashing per call.
        let mut dropped_counts: HashMap<TargetAndSelector, usize> = HashMap::new();
        let mut end = old_len;
        while end > index {
            let mut start = end - 1;
            let key = &self.target_and_selector_by_call[start];
            while start > index && &self.target_and_selector_by_call[start - 1] == key {
                start -= 1;
            }
            let run_len = end - start;
            *dropped_counts.entry(key.clone()).or_insert(0) += run_len;
            end = start;
        }

        let mut keys_to_remove: Vec<TargetAndSelector> = Vec::new();
        for (key, count) in dropped_counts {
            if let Some(indices) = self.target_and_selector_indices.get_mut(&key) {
                debug_assert!(count <= indices.len());
                indices.truncate(indices.len().saturating_sub(count));
                if indices.is_empty() {
                    keys_to_remove.push(key);
                }
            }
        }
        for key in keys_to_remove {
            self.target_and_selector_indices.remove(&key);
        }

        // Clean up any pending writes for children that haven't ended
        self.pending_post_call_writes
            .retain(|_, &mut idx| idx < index);

        // Truncate all vectors
        self.call_inputs.truncate(index);
        self.pre_call_checkpoints.truncate(index);
        self.post_call_checkpoints.truncate(index);
        self.target_and_selector_by_call.truncate(index);
    }

    pub fn calls(&self) -> HashSet<Address> {
        // TODO: Think about storing the call targets in a set in addition to the call inputs
        // to see if it improves performance
        // No filtering needed since all recorded calls are valid
        self.target_and_selector_indices
            .keys()
            .map(|key| key.target)
            .collect()
    }

    pub fn get_call_inputs(
        &self,
        target: Address,
        selector: FixedBytes<4>,
    ) -> Vec<CallInputsWithId<'_>> {
        // No filtering needed since all recorded calls are valid
        match self
            .target_and_selector_indices
            .get(&TargetAndSelector { target, selector })
        {
            Some(indices) => {
                indices
                    .iter()
                    .map(|&index| {
                        CallInputsWithId {
                            call_input: &self.call_inputs[index],
                            id: index,
                        }
                    })
                    .collect()
            }
            None => vec![],
        }
    }

    /// Check if a call is valid for forking (not inside a reverted subtree)
    #[inline]
    pub fn is_call_forkable(&self, call_id: usize) -> bool {
        call_id < self.call_inputs.len()
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

        self.target_and_selector_indices
            .entry(key.clone())
            .or_default()
            .push(self.call_inputs.len());
        self.call_inputs.push(CallInputs {
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
        });
        self.target_and_selector_by_call.push(key);
    }

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
}

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
        println!("pre call: {:#?}", tracer.pre_call_checkpoints);
        println!("post call: {:#?}", tracer.post_call_checkpoints);
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
        assert_eq!(evm.inspector.call_inputs.len(), 1);
        assert_eq!(evm.inspector.pre_call_checkpoints.len(), 1);
        assert_eq!(
            evm.inspector.pre_call_checkpoints[0],
            JournalCheckpoint {
                log_i: 0,
                journal_i: 0,
            }
        );
        assert_eq!(evm.inspector.post_call_checkpoints.len(), 1);
        assert!(evm.inspector.post_call_checkpoints[0].is_none()); // Should be None before call_end

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
        assert!(evm.inspector.post_call_checkpoints[0].is_some()); // Should now have a checkpoint
        assert_eq!(
            evm.inspector.post_call_checkpoints[0],
            Some(JournalCheckpoint {
                log_i: 0,
                journal_i: 3,
            })
        );
        assert!(evm.inspector.pending_post_call_writes.is_empty()); // Should be cleared after call_end

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

        let assertion_store = AssertionStore::new_ephemeral().unwrap();
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

        assert_eq!(tracer.call_inputs.len(), 2);
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

        assert_eq!(tracer.call_inputs.len(), 2);
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
}
