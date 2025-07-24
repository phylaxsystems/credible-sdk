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
};
use revm::{
    Database,
    Inspector,
    context::{
        JournalInner,
        journaled_state::JournalCheckpoint,
    },
    interpreter::{
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

/// Macro to implement Inspector trait for multiple context types.
/// This is cleaner than duplicating the implementation and more reliable than generic bounds.
macro_rules! impl_call_tracer_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<DB: Database> Inspector<$context_type> for CallTracer {
                fn call(&mut self, context: &mut $context_type, inputs: &mut CallInputs) -> Option<CallOutcome> {
                    let input_bytes = inputs.input.bytes(context);
                    self.record_call(inputs.clone(), &input_bytes);
                    None
                }
                fn call_end(&mut self, context: &mut $context_type, _inputs: &CallInputs, _outcome: &mut CallOutcome) {
                    self.journal = context.journaled_state.clone();
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CallInputsWithId {
    pub call_inputs: CallInputs,
    pub id: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CallTracer {
    // Not public to prohibit inserting CallInputs with CallInput::SharedBuffer
    // If call_inputs with CallInput::SharedBuffer are inserted, then accessing the data without the previous context will be problematic
    // This is problematic as you would be required to pass the Context as it was,
    // at the start of the call,  to read the bytes of the CallInput,
    // if they are of the SharedBuffer variant.
    // You otherwise would no longer have access to this data in the future when
    // you want to read call_inputs from the tracer.
    // Because of this, we coerce the bytes at the time of recording the call.
    call_inputs: HashMap<TargetAndSelector, Vec<CallInputsWithId>>,
    pub journal: JournalInner<JournalEntry>,
    pub pre_call_checkpoints: HashMap<u64, JournalCheckpoint>,
    pub post_call_checkpoints: HashMap<u64, JournalCheckpoint>,
    pub call_counter: u64,
}

impl CallTracer {
    pub fn new() -> Self {
        Self {
            call_inputs: HashMap::new(),
            journal: JournalInner::new(),
            pre_call_checkpoints: HashMap::new(),
            post_call_checkpoints: HashMap::new(),
            call_counter: 0,
        }
    }

    pub fn record_call(&mut self, inputs: CallInputs, input_bytes: &[u8]) {
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
        // Coerce the bytes at the time of recording the call, in case they are of the SharedBuffer variant
        inputs.input = revm::interpreter::CallInput::Bytes(Bytes::from(input_bytes.to_vec()));

        self.call_inputs
            .entry(TargetAndSelector {
                target: inputs.target_address,
                selector,
            })
            .or_default()
            .push(CallInputsWithId {
                call_inputs: inputs,
                id: self.call_counter,
            });
        self.call_counter += 1;
    }

    pub fn calls(&self) -> HashSet<Address> {
        // TODO: Think about storing the call targets in a set in addition to the call inputs
        // to see if it improves performance
        self.call_inputs.keys().map(|(addr, _)| *addr).collect()
    }

    pub fn call_inputs(&self) -> &HashMap<(Address, FixedBytes<4>), Vec<(CallInputs, u64)>> {
        &self.call_inputs
    }

    #[cfg(any(test, feature = "test"))]
    pub fn insert_trace(&mut self, address: Address) {
        self.call_inputs
            .insert((address, FixedBytes::default()), vec![]);
    }

    pub fn triggers(&self) -> HashMap<Address, HashSet<TriggerType>> {
        let mut result: HashMap<Address, HashSet<TriggerType>> = HashMap::new();
        let journal = &self.journal;

        // Record call triggers
        for (addr, selector) in self.call_inputs.keys() {
            result.entry(*addr).or_default().insert(TriggerType::Call {
                trigger_selector: *selector,
            });
        }

        // Process journal entries for balance changes
        // Flatten the two-dimensional journal array
        for entry in journal.journal.iter() {
            match entry {
                JournalEntry::BalanceTransfer {
                    from,
                    to,
                    balance: _,
                } => {
                    // Add balance change trigger for both from and to addresses
                    for addr in [from, to] {
                        result
                            .entry(*addr)
                            .or_default()
                            .insert(TriggerType::BalanceChange);
                    }
                }

                JournalEntry::StorageChanged {
                    address,
                    key,
                    had_value: _,
                } => {
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
        primitives::{
            BlockEnv,
            Bytecode,
            SpecId,
            TxEnv,
            TxKind,
            U256,
            address,
            bytes,
        },
        test_utils::deployed_bytecode,
    };
    use op_revm::OpTransaction;
    use revm::{
        InspectEvm,
        database::InMemoryDB,
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

        evm.inspect_with_tx(OpTransaction::new(TxEnv {
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

        evm.inspect_with_tx(OpTransaction::new(tx_env))
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

        let mut tracer = CallTracer::new();
        let addr1 = address!("1111111111111111111111111111111111111111");
        let addr2 = address!("2222222222222222222222222222222222222222");
        let addr3 = address!("3333333333333333333333333333333333333333");

        // Test Call triggers
        let selector1 = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let selector2 = FixedBytes::<4>::from([0xAB, 0xCD, 0xEF, 0x00]);
        tracer.call_inputs.insert((addr1, selector1), vec![]);
        tracer.call_inputs.insert((addr2, selector2), vec![]);

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
        let mut tracer = CallTracer::new();
        let addr = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        // Only call triggers, no journaled state
        tracer.call_inputs.insert((addr, selector), vec![]);

        let triggers = tracer.triggers();

        // Should only have call trigger
        assert_eq!(triggers.len(), 1);
        assert!(triggers[&addr].contains(&TriggerType::Call {
            trigger_selector: selector
        }));
        assert_eq!(triggers[&addr].len(), 1);
    }
}
