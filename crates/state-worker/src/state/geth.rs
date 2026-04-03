//! Geth-specific trace parsing logic.
//!
//! Processes `prestateTracer` output from Geth nodes and converts
//! them into our normalized `AccountState` format.
//!
//! ## Post-Cancun SELFDESTRUCT (EIP-6780)
//!
//! Post-Cancun, SELFDESTRUCT on an existing account only transfers balance. Code, storage, and nonce
//! remain intact. The tracer reports only the balance change, no deletion.

use super::AccountSnapshot;
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use anyhow::anyhow;
use mdbx::{
    AccountState,
    AddressHash,
};
use serde::Deserialize;
use std::collections::{
    BTreeMap,
    HashMap,
};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum RawTraceResult {
    Success {
        result: RawPreStateFrame,
        #[serde(rename = "txHash")]
        tx_hash: Option<B256>,
    },
    Error {
        error: String,
        #[serde(rename = "txHash")]
        tx_hash: Option<B256>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum RawPreStateFrame {
    Default(RawPreStateMode),
    Diff(RawDiffMode),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct RawPreStateMode(pub BTreeMap<Address, RawAccountState>);

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDiffMode {
    pub post: BTreeMap<Address, RawAccountState>,
    pub pre: BTreeMap<Address, RawAccountState>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct RawAccountState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance: Option<U256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    #[serde(default, rename = "codeHash", skip_serializing_if = "Option::is_none")]
    pub code_hash: Option<B256>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub storage: BTreeMap<B256, B256>,
}

pub fn process_geth_traces(traces: Vec<RawTraceResult>) -> anyhow::Result<Vec<AccountState>> {
    let mut accounts: HashMap<AddressHash, AccountSnapshot> = HashMap::new();

    for trace in traces {
        match trace {
            RawTraceResult::Success { result, .. } => {
                process_frame(&mut accounts, result);
            }
            RawTraceResult::Error { tx_hash, .. } => {
                return Err(anyhow!(
                    "trace failed or unexpected tracer type (tx: {tx_hash:?})"
                ));
            }
        }
    }

    Ok(accounts
        .into_iter()
        .filter_map(|(address, snapshot)| snapshot.finalize(address))
        .collect())
}

fn process_frame(accounts: &mut HashMap<AddressHash, AccountSnapshot>, frame: RawPreStateFrame) {
    match frame {
        RawPreStateFrame::Default(prestate_mode) => {
            for (address, account_state) in prestate_mode.0 {
                let snapshot = accounts.entry(address.into()).or_default();

                if let Some(balance) = account_state.balance {
                    snapshot.balance = Some(balance);
                    snapshot.touched = true;
                }

                if let Some(nonce) = account_state.nonce {
                    snapshot.nonce = Some(nonce);
                    snapshot.touched = true;
                }

                if let Some(code) = account_state.code {
                    snapshot.code = Some(code);
                    snapshot.code_cleared = false;
                    snapshot.touched = true;
                }

                for (slot, value) in &account_state.storage {
                    let slot_hash = keccak256(slot.0);
                    let value_u256 = U256::from_be_bytes((*value).into());
                    snapshot.storage_updates.insert(slot_hash, value_u256);
                    snapshot.touched = true;
                }
            }
        }
        RawPreStateFrame::Diff(diff) => {
            process_diff_frame(accounts, &diff);
        }
    }
}

fn process_diff_frame(accounts: &mut HashMap<AddressHash, AccountSnapshot>, diff: &RawDiffMode) {
    // Process pre-state first as baseline for ACCOUNT FIELDS (balance, nonce, code).
    // These are properties of the account that persist if unchanged.
    //
    // NOTE: We do NOT process pre.storage here because:
    // - pre.storage contains OLD values (before the transaction)
    // - post.storage contains NEW values of slots that CHANGED
    // - We only want to track changed slots with their new values
    for (address, account) in &diff.pre {
        let snapshot = accounts.entry((*address).into()).or_default();

        // Set baseline values from pre-state (only if not already set by earlier tx)
        if snapshot.balance.is_none() {
            snapshot.balance = account.balance;
        }
        if snapshot.nonce.is_none() {
            snapshot.nonce = account.nonce;
        }
        if snapshot.code.is_none()
            && let Some(code) = &account.code
        {
            snapshot.code = Some(code.clone());
        }

        // Geth diff mode removes zero-valued storage entries, so deletions show up
        // as slots present in pre but missing in post. Infer those as deletions.
        if let Some(post_account) = diff.post.get(address) {
            for slot in account.storage.keys() {
                if !post_account.storage.contains_key(slot) {
                    let slot_hash = keccak256(slot.0);
                    snapshot.storage_updates.insert(slot_hash, U256::ZERO);
                    snapshot.touched = true;
                }
            }
        }
    }

    // Account deletions: present in pre, missing in post (Geth diff mode encoding).
    for address in diff.pre.keys() {
        if !diff.post.contains_key(address) {
            let snapshot = accounts.entry((*address).into()).or_default();
            snapshot.deleted = true;
            snapshot.code_cleared = false;
            snapshot.touched = true;
            snapshot.balance = None;
            snapshot.nonce = None;
            snapshot.code = None;
            snapshot.storage_updates.clear();
        }
    }

    // Process post-state to apply final values (overrides pre-state)
    for (address, account) in &diff.post {
        let snapshot = accounts.entry((*address).into()).or_default();
        snapshot.touched = true;
        snapshot.deleted = false;

        if let Some(balance) = account.balance {
            snapshot.balance = Some(balance);
        }

        if let Some(nonce) = account.nonce {
            snapshot.nonce = Some(nonce);
        }

        if let Some(code) = &account.code {
            snapshot.code = Some(code.clone());
            snapshot.code_cleared = false;
        } else if matches!(
            account.code_hash,
            Some(B256::ZERO | revm::primitives::KECCAK_EMPTY)
        ) {
            // Geth prestateTracer can encode EIP-7702 revocation either as
            // post.codeHash = 0x0 or as post.code = 0x with post.codeHash set to
            // the empty-code hash, while keeping the account in post-state with
            // updated nonce/balance.

            // The post.codeHash = 0x0 is a bug in Geth. Revert the check for 0x0 once this issue
            // https://github.com/ethereum/go-ethereum/issues/34648 is resolved
            snapshot.code = None;
            snapshot.code_cleared = true;
        }

        // Storage from post contains NEW values of changed slots
        for (slot, value) in &account.storage {
            let slot_hash = keccak256(slot.0);
            let value_u256 = U256::from_be_bytes((*value).into());
            snapshot.storage_updates.insert(slot_hash, value_u256);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        B256,
        Bytes,
    };
    use alloy_rpc_types_trace::geth::{
        AccountState as GethAccountState,
        DiffMode,
        PreStateFrame,
    };
    use anyhow::{
        Context,
        Result,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_trace_result(frame: PreStateFrame) -> RawTraceResult {
        RawTraceResult::Success {
            result: raw_frame_from_typed(frame),
            tx_hash: None,
        }
    }

    fn raw_frame_from_typed(frame: PreStateFrame) -> RawPreStateFrame {
        match frame {
            PreStateFrame::Default(mode) => {
                RawPreStateFrame::Default(RawPreStateMode(
                    mode.0
                        .into_iter()
                        .map(|(address, account)| (address, raw_account_from_typed(account)))
                        .collect(),
                ))
            }
            PreStateFrame::Diff(diff) => {
                RawPreStateFrame::Diff(RawDiffMode {
                    pre: diff
                        .pre
                        .into_iter()
                        .map(|(address, account)| (address, raw_account_from_typed(account)))
                        .collect(),
                    post: diff
                        .post
                        .into_iter()
                        .map(|(address, account)| (address, raw_account_from_typed(account)))
                        .collect(),
                })
            }
        }
    }

    fn raw_account_from_typed(account: GethAccountState) -> RawAccountState {
        RawAccountState {
            balance: account.balance,
            code: account.code,
            nonce: account.nonce,
            code_hash: None,
            storage: account.storage,
        }
    }

    #[test]
    fn test_selfdestruct_only_balance_changes() -> Result<()> {
        // Post-Cancun: SELFDESTRUCT only transfers balance, account remains in post
        let address = Address::from([0x42u8; 20]);

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(Bytes::from(vec![0x60, 0x80])),
                storage: BTreeMap::default(),
            },
        );

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: None,
                code: None,
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(!account.deleted);
        assert_eq!(account.balance, U256::ZERO);
        Ok(())
    }

    #[test]
    fn test_storage_updated() -> Result<()> {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let new_value = B256::from([0xBBu8; 32]);

        let mut next_state = std::collections::BTreeMap::new();
        let mut storage = std::collections::BTreeMap::new();
        storage.insert(slot, new_value);

        next_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(1),
                code: None,
                storage,
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            account.storage.get(&slot_hash),
            Some(&U256::from_be_bytes(new_value.0))
        );
        Ok(())
    }

    #[test]
    fn test_new_account_created() -> Result<()> {
        let address = Address::from([0x42u8; 20]);

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(0),
                code: Some(Bytes::from(vec![0x60, 0x80, 0x60, 0x40])),
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(!account.deleted);
        assert_eq!(account.balance, U256::from(1000));
        assert_eq!(account.nonce, 0);
        assert!(account.code.is_some());
        Ok(())
    }

    #[test]
    fn test_balance_transfer() -> Result<()> {
        let sender = Address::from([0x01u8; 20]);
        let receiver = Address::from([0x02u8; 20]);

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            sender,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: Some(6),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        next_state.insert(
            receiver,
            GethAccountState {
                balance: Some(U256::from(600)),
                nonce: None,
                code: None,
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 2);

        let sender_state = results
            .iter()
            .find(|a| a.balance == U256::from(900))
            .context("missing sender state")?;
        assert_eq!(sender_state.nonce, 6);
        assert!(!sender_state.deleted);

        let receiver_state = results
            .iter()
            .find(|a| a.balance == U256::from(600))
            .context("missing receiver state")?;
        assert!(!receiver_state.deleted);
        Ok(())
    }

    #[test]
    fn test_multiple_transactions() -> Result<()> {
        let address = Address::from([0x42u8; 20]);

        let mut next_state_first = std::collections::BTreeMap::new();
        next_state_first.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: Some(6),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        let diff_first = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state_first,
        };

        let mut next_state_second = std::collections::BTreeMap::new();
        next_state_second.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(800)),
                nonce: Some(7),
                code: None,
                storage: BTreeMap::default(),
            },
        );
        let diff_second = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state_second,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff_first)),
            make_trace_result(PreStateFrame::Diff(diff_second)),
        ])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert_eq!(account.balance, U256::from(800));
        assert_eq!(account.nonce, 7);
        Ok(())
    }

    #[test]
    fn test_empty_diff_returns_nothing() -> Result<()> {
        let diff = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert!(results.is_empty());
        Ok(())
    }

    #[test]
    fn test_code_preserved_when_only_storage_changes() -> Result<()> {
        // This test verifies the fix: when only storage changes, code should be
        // preserved from pre-state since it won't appear in post-state
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let old_value = B256::from([0xAAu8; 32]);
        let new_value = B256::from([0xBBu8; 32]);
        let code = Bytes::from(vec![0x60, 0x80, 0x60, 0x40]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot, old_value);

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(code.clone()),
                storage: prior_storage,
            },
        );

        let mut next_storage = std::collections::BTreeMap::new();
        next_storage.insert(slot, new_value);

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: None, // Not changed
                nonce: None,   // Not changed
                code: None,    // Not changed - but we need to preserve it!
                storage: next_storage,
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert_eq!(account.balance, U256::from(1000));
        assert_eq!(account.nonce, 5);
        assert_eq!(account.code, Some(code));

        let slot_hash = keccak256(slot.0);
        assert_eq!(
            account.storage.get(&slot_hash),
            Some(&U256::from_be_bytes(new_value.0))
        );
        Ok(())
    }

    #[test]
    fn test_storage_deletion_inferred_when_post_omits_zero() -> Result<()> {
        let address = Address::from([0x42u8; 20]);
        let slot = B256::from([0x01u8; 32]);
        let old_value = B256::from([0xAAu8; 32]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot, old_value);

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(5),
                code: Some(Bytes::from(vec![0x60, 0x80])),
                storage: prior_storage,
            },
        );

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(900)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(),
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        let slot_hash = keccak256(slot.0);
        assert_eq!(account.storage.get(&slot_hash), Some(&U256::ZERO));
        Ok(())
    }

    #[test]
    fn test_account_deletion_inferred_when_post_missing() -> Result<()> {
        let address = Address::from([0x24u8; 20]);
        let slot = B256::from([0x02u8; 32]);
        let old_value = B256::from([0x11u8; 32]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot, old_value);

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1)),
                nonce: Some(1),
                code: None,
                storage: prior_storage,
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(account.deleted);
        Ok(())
    }

    #[test]
    fn test_storage_mixed_add_change_delete() -> Result<()> {
        let address = Address::from([0x33u8; 20]);
        let slot_a = B256::from([0x0Au8; 32]);
        let slot_b = B256::from([0x0Bu8; 32]);
        let slot_c = B256::from([0x0Cu8; 32]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot_a, B256::from(U256::from(10)));
        prior_storage.insert(slot_b, B256::from(U256::from(20)));

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: Some(0),
                code: None,
                storage: prior_storage,
            },
        );

        let mut next_storage = std::collections::BTreeMap::new();
        next_storage.insert(slot_b, B256::from(U256::from(200)));
        next_storage.insert(slot_c, B256::from(U256::from(300)));

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: next_storage,
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: next_state,
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;
        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;

        let slot_a_digest = keccak256(slot_a.0);
        let slot_b_fingerprint = keccak256(slot_b.0);
        let slot_c_checksum = keccak256(slot_c.0);

        assert_eq!(account.storage.get(&slot_a_digest), Some(&U256::ZERO));
        assert_eq!(
            account.storage.get(&slot_b_fingerprint),
            Some(&U256::from(200))
        );
        assert_eq!(
            account.storage.get(&slot_c_checksum),
            Some(&U256::from(300))
        );
        Ok(())
    }

    #[test]
    fn test_storage_deletion_then_recreate_in_later_tx() -> Result<()> {
        let address = Address::from([0x55u8; 20]);
        let slot = B256::from([0x05u8; 32]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot, B256::from(U256::from(7)));

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::ZERO),
                nonce: Some(0),
                code: None,
                storage: prior_storage,
            },
        );

        let diff1 = DiffMode {
            pre: prior_state,
            post: std::collections::BTreeMap::new(),
        };

        let mut next_storage = std::collections::BTreeMap::new();
        next_storage.insert(slot, B256::from(U256::from(9)));

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: next_storage,
            },
        );

        let diff2 = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff1)),
            make_trace_result(PreStateFrame::Diff(diff2)),
        ])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        let slot_hash = keccak256(slot.0);
        assert_eq!(account.storage.get(&slot_hash), Some(&U256::from(9)));
        Ok(())
    }

    #[test]
    fn test_account_deletion_overrides_prior_storage_updates_in_block() -> Result<()> {
        let address = Address::from([0x21u8; 20]);
        let slot = B256::from([0x09u8; 32]);
        let value = B256::from([0xAAu8; 32]);

        let mut next_state_first = std::collections::BTreeMap::new();
        let mut storage_first = std::collections::BTreeMap::new();
        storage_first.insert(slot, value);
        next_state_first.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: None,
                storage: storage_first,
            },
        );

        let diff_first = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state_first,
        };

        let mut prior_state_second = std::collections::BTreeMap::new();
        prior_state_second.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: None,
                storage: std::collections::BTreeMap::new(),
            },
        );

        let diff_second = DiffMode {
            pre: prior_state_second,
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff_first)),
            make_trace_result(PreStateFrame::Diff(diff_second)),
        ])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(account.deleted);
        assert!(account.storage.is_empty());
        Ok(())
    }

    #[test]
    fn test_account_deleted_then_recreated_in_later_tx() -> Result<()> {
        let address = Address::from([0x22u8; 20]);
        let slot = B256::from([0x0Au8; 32]);
        let value = B256::from([0xBBu8; 32]);
        let code = Bytes::from(vec![0x60, 0x80]);

        let mut prior_state_first = std::collections::BTreeMap::new();
        prior_state_first.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(10)),
                nonce: Some(1),
                code: Some(code.clone()),
                storage: std::collections::BTreeMap::new(),
            },
        );

        let diff_first = DiffMode {
            pre: prior_state_first,
            post: std::collections::BTreeMap::new(),
        };

        let mut next_state_second = std::collections::BTreeMap::new();
        let mut storage_second = std::collections::BTreeMap::new();
        storage_second.insert(slot, value);
        next_state_second.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(50)),
                nonce: Some(2),
                code: Some(code.clone()),
                storage: storage_second,
            },
        );

        let diff_second = DiffMode {
            pre: std::collections::BTreeMap::new(),
            post: next_state_second,
        };

        let results = process_geth_traces(vec![
            make_trace_result(PreStateFrame::Diff(diff_first)),
            make_trace_result(PreStateFrame::Diff(diff_second)),
        ])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(!account.deleted);
        assert_eq!(account.balance, U256::from(50));
        assert_eq!(account.nonce, 2);
        let slot_hash = keccak256(slot.0);
        assert_eq!(
            account.storage.get(&slot_hash),
            Some(&U256::from_be_bytes(value.0))
        );
        assert_eq!(account.code, Some(code));
        Ok(())
    }

    #[test]
    fn test_account_deletion_clears_pre_storage_in_same_tx() -> Result<()> {
        let address = Address::from([0x44u8; 20]);
        let slot = B256::from([0x0Du8; 32]);

        let mut prior_storage = std::collections::BTreeMap::new();
        prior_storage.insert(slot, B256::from(U256::from(123)));

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(1),
                code: Some(Bytes::from(vec![0x60, 0x80])),
                storage: prior_storage,
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: std::collections::BTreeMap::new(),
        };

        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert!(account.deleted);
        assert!(account.storage.is_empty());
        assert_eq!(account.code_hash, B256::ZERO);
        Ok(())
    }

    #[test]
    fn test_code_change_overrides_pre_code() -> Result<()> {
        let address = Address::from([0x66u8; 20]);
        let old_code = Bytes::from(vec![0x60, 0x80]);
        let new_code = Bytes::from(vec![0x61, 0x01, 0x00]);

        let mut prior_state = std::collections::BTreeMap::new();
        prior_state.insert(
            address,
            GethAccountState {
                balance: Some(U256::from(1)),
                nonce: Some(1),
                code: Some(old_code),
                storage: BTreeMap::default(),
            },
        );

        let mut next_state = std::collections::BTreeMap::new();
        next_state.insert(
            address,
            GethAccountState {
                balance: None,
                nonce: None,
                code: Some(new_code.clone()),
                storage: BTreeMap::default(),
            },
        );

        let diff = DiffMode {
            pre: prior_state,
            post: next_state,
        };
        let results = process_geth_traces(vec![make_trace_result(PreStateFrame::Diff(diff))])?;

        assert_eq!(results.len(), 1);
        let account = results.first().context("expected one account result")?;
        assert_eq!(account.code, Some(new_code.clone()));
        assert_eq!(account.code_hash, keccak256(&new_code));
        assert!(!account.deleted);
        Ok(())
    }

    #[test]
    fn test_eip7702_revocation_trace_clears_delegation_code() -> Result<()> {
        let authority = Address::from_slice(
            &hex::decode("7ec0e5a95169d31d6fa7d44e3308872e090c5bd1")
                .context("authority hex should decode")?,
        );
        let old_code = Bytes::from(
            hex::decode("ef0100d313d93607c016a85e63e557a11ca5ab0b53ad83")
                .context("delegation code hex should decode")?,
        );

        let trace_json = json!([
            {
                "txHash": "0xd2f0012ecbcb7c50ea5415c94268c08bf951d7714209437458d6e6436ab0cd0c",
                "result": {
                    "pre": {
                        format!("{authority:#x}"): {
                            "balance": "0x0",
                            "nonce": 1,
                            "code": format!("0x{}", hex::encode(old_code.as_ref())),
                            "codeHash": "0x9eea9f41ed2b35e6234d1e1c14e88c1136f85d56ed1f32a7efc0096d998dad3d"
                        }
                    },
                    "post": {
                        format!("{authority:#x}"): {
                            "nonce": 2,
                            "codeHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
                        }
                    }
                }
            }
        ]);

        let traces: Vec<RawTraceResult> =
            serde_json::from_value(trace_json).context("trace json should deserialize")?;

        let results = process_geth_traces(traces)?;
        assert_eq!(results.len(), 1);

        let account = results.first().context("expected one account result")?;
        assert_eq!(account.nonce, 2);
        assert_eq!(account.balance, U256::ZERO);
        assert_eq!(account.code, None);
        assert_eq!(account.code_hash, revm::primitives::KECCAK_EMPTY);
        assert!(!account.deleted);
        Ok(())
    }

    #[test]
    fn test_eip7702_revocation_trace_empty_code_hash_clears_delegation_code() -> Result<()> {
        let authority = Address::from_slice(
            &hex::decode("7ec0e5a95169d31d6fa7d44e3308872e090c5bd1")
                .context("authority hex should decode")?,
        );
        let old_code = Bytes::from(
            hex::decode("ef0100d313d93607c016a85e63e557a11ca5ab0b53ad83")
                .context("delegation code hex should decode")?,
        );

        let trace_json = json!([
            {
                "txHash": "0xd2f0012ecbcb7c50ea5415c94268c08bf951d7714209437458d6e6436ab0cd0c",
                "result": {
                    "pre": {
                        format!("{authority:#x}"): {
                            "balance": "0x0",
                            "nonce": 1,
                            "code": format!("0x{}", hex::encode(old_code.as_ref())),
                            "codeHash": "0x9eea9f41ed2b35e6234d1e1c14e88c1136f85d56ed1f32a7efc0096d998dad3d"
                        }
                    },
                    "post": {
                        format!("{authority:#x}"): {
                            "nonce": 2,
                            "code": "0x",
                            "codeHash": format!("{:#x}", revm::primitives::KECCAK_EMPTY)
                        }
                    }
                }
            }
        ]);

        let traces: Vec<RawTraceResult> =
            serde_json::from_value(trace_json).context("trace json should deserialize")?;

        let results = process_geth_traces(traces)?;
        assert_eq!(results.len(), 1);

        let account = results.first().context("expected one account result")?;
        assert_eq!(account.nonce, 2);
        assert_eq!(account.balance, U256::ZERO);
        assert_eq!(account.code, None);
        assert_eq!(account.code_hash, revm::primitives::KECCAK_EMPTY);
        assert!(!account.deleted);
        Ok(())
    }
}
