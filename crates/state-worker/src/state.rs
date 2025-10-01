//! Helpers that collapse per-transaction debug traces into per-account commits.
//!
//! The worker consumes Geth's `prestateTracer` output, merges the resulting
//! diffs into account-level snapshots, and exposes a compact representation that
//! mirrors the Redis schema the sidecar expects.

use alloy_rpc_types_trace::parity::Delta;
use std::collections::HashMap;

use alloy::primitives::{
    Address,
    B256,
    U256,
    keccak256,
};
use alloy_rpc_types_trace::parity::{
    StateDiff,
    TraceResultsWithTransactionHash,
};
use revm::primitives::KECCAK_EMPTY;

#[derive(Default)]
struct AccountSnapshot {
    /// Most recent balance observed in the trace stream; filled lazily.
    balance: Option<U256>,
    /// Most recent nonce observed in the trace stream; filled lazily.
    nonce: Option<u64>,
    /// Optional bytecode blob; preserved so we can emit code updates exactly once.
    code: Option<Vec<u8>>,
    /// Sparse storage slots touched by the block (slot -> value).
    storage_updates: HashMap<B256, B256>,
    /// Whether we saw a post-state for this account.
    touched: bool,
    /// Whether the account was deleted by the block.
    deleted: bool,
}

/// Aggregate of all state changes produced while processing a block.
pub struct BlockStateUpdate {
    pub block_number: u64,
    pub block_hash: B256,
    pub accounts: Vec<AccountCommit>,
}

/// Flattened representation of an account diff that mirrors the Redis schema.
pub struct AccountCommit {
    pub address: Address,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash: B256,
    pub code: Option<Vec<u8>>,
    pub storage: Vec<(B256, B256)>,
    pub deleted: bool,
}

impl BlockStateUpdate {
    /// Collapse the per-transaction pre-state traces into per-account updates.
    pub fn from_traces(
        block_number: u64,
        block_hash: B256,
        traces: Vec<TraceResultsWithTransactionHash>,
    ) -> Self {
        let mut accounts: HashMap<Address, AccountSnapshot> = HashMap::new();

        for trace in traces {
            if let Some(state_diff) = trace.full_trace.state_diff {
                process_diff(&mut accounts, &state_diff);
            }
        }

        let accounts = accounts
            .into_iter()
            .filter_map(|(address, snapshot)| snapshot.finalize(address))
            .collect();

        Self {
            block_number,
            block_hash,
            accounts,
        }
    }

    /// Consume the update and return its constituent parts for downstream
    /// writers. This keeps the write path ergonomic without cloning vectors.
    pub fn into_parts(self) -> (u64, B256, Vec<AccountCommit>) {
        (self.block_number, self.block_hash, self.accounts)
    }
}

impl AccountSnapshot {
    /// Convert the accumulated snapshot into a commit payload if anything
    /// meaningful changed. Returning `None` allows callsites to drop untouched
    /// snapshots without extra bookkeeping.
    fn finalize(mut self, address: Address) -> Option<AccountCommit> {
        if !self.touched && !self.deleted && self.storage_updates.is_empty() {
            return None;
        }

        let mut code_bytes = self.code.take().unwrap_or_default();
        let (balance, nonce) = if self.deleted {
            code_bytes.clear();
            (U256::ZERO, 0_u64)
        } else {
            (
                self.balance.unwrap_or_default(),
                self.nonce.unwrap_or_default(),
            )
        };

        let code_hash = if code_bytes.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code_bytes)
        };

        let code = if self.deleted || code_bytes.is_empty() {
            None
        } else {
            Some(code_bytes)
        };

        let storage = self.storage_updates.into_iter().collect();

        Some(AccountCommit {
            address,
            balance,
            nonce,
            code_hash,
            code,
            storage,
            deleted: self.deleted,
        })
    }
}

/// Merge a single transaction diff into the pending account snapshots.
fn process_diff(accounts: &mut HashMap<Address, AccountSnapshot>, state_diff: &StateDiff) {
    for (address, account_diff) in &state_diff.0 {
        let snapshot = accounts.entry(*address).or_default();

        // Handle balance changes
        match &account_diff.balance {
            Delta::Unchanged => {
                // No change to balance
            }
            Delta::Added(balance) => {
                // Account was created with this balance
                snapshot.balance = Some(*balance);
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Balance removed means account was deleted
                snapshot.balance = Some(U256::ZERO);
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Balance changed from one value to another
                snapshot.balance = Some(change.to);
                snapshot.touched = true;
            }
        }

        // Handle nonce changes
        match &account_diff.nonce {
            Delta::Unchanged => {
                // No change to nonce
            }
            Delta::Added(nonce) => {
                // Account was created with this nonce
                snapshot.nonce = Some(nonce.to::<u64>());
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Nonce removed means account was deleted
                snapshot.nonce = Some(0);
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Nonce changed from one value to another
                snapshot.nonce = Some(change.to.to::<u64>());
                snapshot.touched = true;
            }
        }

        // Handle code changes
        match &account_diff.code {
            Delta::Unchanged => {
                // No change to code
            }
            Delta::Added(code) => {
                // Contract was deployed
                snapshot.code = Some(code.to_vec());
                snapshot.touched = true;
            }
            Delta::Removed(_) => {
                // Code removed means contract was deleted
                snapshot.code = Some(Vec::new());
                snapshot.deleted = true;
            }
            Delta::Changed(change) => {
                // Code changed (unusual but theoretically possible)
                snapshot.code = Some(change.to.to_vec());
                snapshot.touched = true;
            }
        }

        // Handle storage changes
        for (slot, storage_delta) in &account_diff.storage {
            match storage_delta {
                Delta::Unchanged => {
                    // No change to this storage slot
                }
                Delta::Added(value) => {
                    // New storage slot was set
                    snapshot.storage_updates.insert(*slot, *value);
                    snapshot.touched = true;
                }
                Delta::Removed(_) => {
                    // Storage slot was deleted (set to zero)
                    snapshot.storage_updates.insert(*slot, B256::ZERO);
                    snapshot.touched = true;
                }
                Delta::Changed(change) => {
                    // Storage slot value changed
                    snapshot.storage_updates.insert(*slot, change.to);
                    snapshot.touched = true;
                }
            }
        }

        // Check if the account was fully destroyed
        // This happens when balance, nonce, and code are all Removed
        let is_destroyed = matches!(account_diff.balance, Delta::Removed(_))
            && matches!(account_diff.nonce, Delta::Removed(_))
            && matches!(account_diff.code, Delta::Removed(_));

        if is_destroyed {
            snapshot.deleted = true;
            // Zero out all storage that was touched
            for value in snapshot.storage_updates.values_mut() {
                *value = B256::ZERO;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Bytes,
        U64,
    };
    use alloy_rpc_types_trace::parity::{
        AccountDiff,
        ChangedType,
        Delta,
    };
    use std::collections::BTreeMap;

    // Helper function to create a StateDiff wrapped in TraceResultsWithTransactionHash
    fn create_trace_with_diff(
        tx_hash: B256,
        state_diff: StateDiff,
    ) -> TraceResultsWithTransactionHash {
        use alloy_rpc_types_trace::parity::TraceResults;

        TraceResultsWithTransactionHash {
            transaction_hash: tx_hash,
            full_trace: TraceResults {
                output: Bytes::new(),
                state_diff: Some(state_diff),
                trace: vec![],
                vm_trace: None,
            },
        }
    }

    #[test]
    fn test_simple_balance_transfer() {
        let sender = Address::repeat_byte(0x01);
        let recipient = Address::repeat_byte(0x02);
        let tx_hash = B256::repeat_byte(0xff);

        let mut state_diff = StateDiff::default();

        // Sender's balance decreases and nonce increases
        state_diff.0.insert(
            sender,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(1000u64),
                    to: U256::from(900u64),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(5),
                    to: U64::from(6),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        // Recipient's balance increases (new account)
        state_diff.0.insert(
            recipient,
            AccountDiff {
                balance: Delta::Added(U256::from(100u64)),
                nonce: Delta::Added(U64::from(0)),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(1, B256::ZERO, traces);

        assert_eq!(update.accounts.len(), 2);

        let accounts: HashMap<_, _> = update
            .accounts
            .into_iter()
            .map(|a| (a.address, a))
            .collect();

        // Check sender
        let sender_commit = accounts.get(&sender).unwrap();
        assert_eq!(sender_commit.balance, U256::from(900u64));
        assert_eq!(sender_commit.nonce, 6);
        assert!(!sender_commit.deleted);

        // Check recipient
        let recipient_commit = accounts.get(&recipient).unwrap();
        assert_eq!(recipient_commit.balance, U256::from(100u64));
        assert_eq!(recipient_commit.nonce, 0);
        assert!(!recipient_commit.deleted);
    }

    #[test]
    fn test_contract_deployment() {
        let deployer = Address::repeat_byte(0x10);
        let contract = Address::repeat_byte(0x20);
        let tx_hash = B256::repeat_byte(0xaa);
        let contract_code = vec![0x60, 0x60, 0x60, 0x40, 0x52];

        let mut state_diff = StateDiff::default();

        // Deployer's balance decreases, nonce increases
        state_diff.0.insert(
            deployer,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(10_000u64),
                    to: U256::from(9_000u64),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(0),
                    to: U64::from(1),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        // New contract is created
        state_diff.0.insert(
            contract,
            AccountDiff {
                balance: Delta::Added(U256::ZERO),
                nonce: Delta::Added(U64::from(1)), // Contracts start with nonce 1
                code: Delta::Added(contract_code.clone().into()),
                storage: BTreeMap::default(),
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(2, B256::ZERO, traces);

        let accounts: HashMap<_, _> = update
            .accounts
            .into_iter()
            .map(|a| (a.address, a))
            .collect();

        // Check contract
        let contract_commit = accounts.get(&contract).unwrap();
        assert_eq!(contract_commit.code, Some(contract_code.clone()));
        assert_eq!(contract_commit.code_hash, keccak256(&contract_code));
        assert_eq!(contract_commit.nonce, 1);
        assert!(!contract_commit.deleted);
    }

    #[test]
    fn test_storage_updates() {
        let contract = Address::repeat_byte(0x30);
        let tx_hash = B256::repeat_byte(0xbb);

        let slot1 = B256::from(U256::from(1));
        let slot2 = B256::from(U256::from(2));
        let slot3 = B256::from(U256::from(3));

        let mut storage = BTreeMap::new();

        // Slot 1: New value added
        storage.insert(slot1, Delta::Added(B256::from(U256::from(100))));

        // Slot 2: Value changed
        storage.insert(
            slot2,
            Delta::Changed(ChangedType {
                from: B256::from(U256::from(200)),
                to: B256::from(U256::from(300)),
            }),
        );

        // Slot 3: Value removed (set to zero)
        storage.insert(slot3, Delta::Removed(B256::from(U256::from(400))));

        let mut state_diff = StateDiff::default();
        state_diff.0.insert(
            contract,
            AccountDiff {
                balance: Delta::Unchanged,
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage,
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(3, B256::ZERO, traces);

        assert_eq!(update.accounts.len(), 1);
        let account = &update.accounts[0];

        // Convert storage to HashMap for easier testing
        let storage_map: HashMap<_, _> = account.storage.iter().copied().collect();

        assert_eq!(storage_map.get(&slot1), Some(&B256::from(U256::from(100))));
        assert_eq!(storage_map.get(&slot2), Some(&B256::from(U256::from(300))));
        assert_eq!(storage_map.get(&slot3), Some(&B256::ZERO));
    }

    #[test]
    fn test_account_deletion() {
        let account = Address::repeat_byte(0x40);
        let tx_hash = B256::repeat_byte(0xcc);

        let mut state_diff = StateDiff::default();

        // Account is being deleted
        state_diff.0.insert(
            account,
            AccountDiff {
                balance: Delta::Removed(U256::from(500u64)),
                nonce: Delta::Removed(U64::from(10)),
                code: Delta::Removed(vec![0x60, 0x80].into()),
                storage: BTreeMap::default(),
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(4, B256::ZERO, traces);

        assert_eq!(update.accounts.len(), 1);
        let account_commit = &update.accounts[0];

        assert!(account_commit.deleted);
        assert_eq!(account_commit.balance, U256::ZERO);
        assert_eq!(account_commit.nonce, 0);
        assert_eq!(account_commit.code, None);
        assert_eq!(account_commit.code_hash, KECCAK_EMPTY);
    }

    #[test]
    fn test_account_deletion_with_storage() {
        let account = Address::repeat_byte(0x50);
        let tx_hash = B256::repeat_byte(0xdd);

        let slot1 = B256::from(U256::from(1));
        let slot2 = B256::from(U256::from(2));

        let mut storage = BTreeMap::new();
        storage.insert(slot1, Delta::Removed(B256::from(U256::from(111))));
        storage.insert(slot2, Delta::Removed(B256::from(U256::from(222))));

        let mut state_diff = StateDiff::default();
        state_diff.0.insert(
            account,
            AccountDiff {
                balance: Delta::Removed(U256::from(1000u64)),
                nonce: Delta::Removed(U64::from(5)),
                code: Delta::Removed(vec![0x60].into()),
                storage,
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(5, B256::ZERO, traces);

        let account_commit = &update.accounts[0];
        assert!(account_commit.deleted);

        // All storage should be zeroed
        let storage_map: HashMap<_, _> = account_commit.storage.iter().copied().collect();
        assert_eq!(storage_map.get(&slot1), Some(&B256::ZERO));
        assert_eq!(storage_map.get(&slot2), Some(&B256::ZERO));
    }

    #[test]
    fn test_multiple_transactions_same_account() {
        let account = Address::repeat_byte(0x60);

        // Transaction 1: Balance changes from 1000 to 900
        let mut state_diff1 = StateDiff::default();
        state_diff1.0.insert(
            account,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(1000u64),
                    to: U256::from(900u64),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(0),
                    to: U64::from(1),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        // Transaction 2: Balance changes from 900 to 800
        let mut state_diff2 = StateDiff::default();
        state_diff2.0.insert(
            account,
            AccountDiff {
                balance: Delta::Changed(ChangedType {
                    from: U256::from(900u64),
                    to: U256::from(800u64),
                }),
                nonce: Delta::Changed(ChangedType {
                    from: U64::from(1),
                    to: U64::from(2),
                }),
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let traces = vec![
            create_trace_with_diff(B256::from(U256::from(1)), state_diff1),
            create_trace_with_diff(B256::from(U256::from(2)), state_diff2),
        ];

        let update = BlockStateUpdate::from_traces(6, B256::ZERO, traces);

        assert_eq!(update.accounts.len(), 1);
        let account_commit = &update.accounts[0];

        // Should have the final state
        assert_eq!(account_commit.balance, U256::from(800u64));
        assert_eq!(account_commit.nonce, 2);
    }

    #[test]
    fn test_unchanged_account_not_included() {
        let account = Address::repeat_byte(0x70);

        let mut state_diff = StateDiff::default();
        state_diff.0.insert(
            account,
            AccountDiff {
                balance: Delta::Unchanged,
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage: BTreeMap::default(),
            },
        );

        let traces = vec![create_trace_with_diff(B256::ZERO, state_diff)];
        let update = BlockStateUpdate::from_traces(7, B256::ZERO, traces);

        // Account should not be included since nothing changed
        assert_eq!(update.accounts.len(), 0);
    }

    #[test]
    fn test_empty_traces() {
        let traces = vec![];
        let update = BlockStateUpdate::from_traces(8, B256::ZERO, traces);
        assert_eq!(update.accounts.len(), 0);
    }

    #[test]
    fn test_traces_without_state_diff() {
        use alloy::primitives::Bytes;
        use alloy_rpc_types_trace::parity::TraceResults;

        let trace = TraceResultsWithTransactionHash {
            transaction_hash: B256::ZERO,
            full_trace: TraceResults {
                output: Bytes::new(),
                state_diff: None, // No state diff
                trace: vec![],
                vm_trace: None,
            },
        };

        let traces = vec![trace];
        let update = BlockStateUpdate::from_traces(9, B256::ZERO, traces);
        assert_eq!(update.accounts.len(), 0);
    }

    #[test]
    fn test_complex_storage_scenario() {
        let contract = Address::repeat_byte(0x80);
        let tx_hash = B256::repeat_byte(0xee);

        // Create 10 storage slots with various operations
        let mut storage = BTreeMap::new();
        for i in 0..10 {
            let slot = B256::from(U256::from(i));
            let delta = match i % 4 {
                0 => Delta::Added(B256::from(U256::from(i * 100))),
                1 => Delta::Removed(B256::from(U256::from(i * 100))),
                2 => {
                    Delta::Changed(ChangedType {
                        from: B256::from(U256::from(i * 100)),
                        to: B256::from(U256::from(i * 200)),
                    })
                }
                _ => Delta::Unchanged,
            };
            storage.insert(slot, delta);
        }

        let mut state_diff = StateDiff::default();
        state_diff.0.insert(
            contract,
            AccountDiff {
                balance: Delta::Unchanged,
                nonce: Delta::Unchanged,
                code: Delta::Unchanged,
                storage,
            },
        );

        let traces = vec![create_trace_with_diff(tx_hash, state_diff)];
        let update = BlockStateUpdate::from_traces(10, B256::ZERO, traces);

        let account = &update.accounts[0];
        let storage_map: HashMap<_, _> = account.storage.iter().copied().collect();

        // Check specific slots
        assert_eq!(
            storage_map.get(&B256::from(U256::from(0))),
            Some(&B256::from(U256::from(0)))
        );
        assert_eq!(
            storage_map.get(&B256::from(U256::from(1))),
            Some(&B256::ZERO)
        );
        assert_eq!(
            storage_map.get(&B256::from(U256::from(2))),
            Some(&B256::from(U256::from(400)))
        );
        // Slot 3 was unchanged, so shouldn't be in the map
        assert_eq!(storage_map.get(&B256::from(U256::from(3))), None);
    }

    #[test]
    fn test_into_parts() {
        let block_number = 12345u64;
        let block_hash = B256::repeat_byte(0xab);
        let traces = vec![];

        let update = BlockStateUpdate::from_traces(block_number, block_hash, traces);
        let (num, hash, accounts) = update.into_parts();

        assert_eq!(num, block_number);
        assert_eq!(hash, block_hash);
        assert_eq!(accounts.len(), 0);
    }
}
