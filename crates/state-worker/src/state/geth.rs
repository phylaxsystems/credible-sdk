//! Geth trace processing

use super::AccountSnapshot;
use alloy::primitives::{
    U256,
    keccak256,
};
use alloy_rpc_types_trace::geth::{
    PreStateFrame,
    TraceResult,
};
use state_store::common::{
    AccountState,
    AddressHash,
};
use std::collections::HashMap;

pub fn process_geth_traces(traces: Vec<TraceResult>) -> Vec<AccountState> {
    let mut accounts: HashMap<AddressHash, AccountSnapshot> = HashMap::new();

    for trace in traces {
        if let TraceResult::Success {
            result: alloy_rpc_types_trace::geth::GethTrace::PreStateTracer(frame),
            ..
        } = trace
        {
            process_frame(&mut accounts, frame);
        }
    }

    accounts
        .into_iter()
        .filter_map(|(address, snapshot)| snapshot.finalize(address))
        .collect()
}

fn process_frame(accounts: &mut HashMap<AddressHash, AccountSnapshot>, frame: PreStateFrame) {
    match frame {
        PreStateFrame::Default(prestate_mode) => {
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
                    snapshot.code = Some(code.to_vec());
                    snapshot.touched = true;
                }

                // Hash storage slots exactly like Parity does
                for (slot, value) in &account_state.storage {
                    let slot_hash = keccak256(slot.0);
                    let slot_key = U256::from_be_bytes(slot_hash.into());
                    let value_u256 = U256::from_be_bytes((*value).into());
                    snapshot.storage_updates.insert(slot_key, value_u256);
                    snapshot.touched = true;
                }
            }
        }
        PreStateFrame::Diff(diff) => {
            process_diff_frame(accounts, diff);
        }
    }
}

fn process_diff_frame(
    accounts: &mut HashMap<AddressHash, AccountSnapshot>,
    diff: alloy_rpc_types_trace::geth::DiffMode,
) {
    // Process post-state (final state after transaction)
    for (address, account) in &diff.post {
        let snapshot = accounts.entry((*address).into()).or_default();

        if let Some(balance) = account.balance {
            snapshot.balance = Some(balance);
            snapshot.touched = true;
        }

        if let Some(nonce) = account.nonce {
            snapshot.nonce = Some(nonce);
            snapshot.touched = true;
        }

        if let Some(code) = &account.code {
            snapshot.code = Some(code.to_vec());
            snapshot.touched = true;
        }

        // Hash storage slots exactly like Parity does
        for (slot, value) in &account.storage {
            let slot_hash = keccak256(slot.0);
            let slot_key = U256::from_be_bytes(slot_hash.into());
            let value_u256 = U256::from_be_bytes((*value).into());
            snapshot.storage_updates.insert(slot_key, value_u256);
            snapshot.touched = true;
        }
    }

    // Check for deletions (account in pre but not in post)
    for (address, pre_account) in diff.pre {
        if !diff.post.contains_key(&address) {
            let snapshot = accounts.entry(address.into()).or_default();
            snapshot.deleted = true;
            snapshot.balance = Some(U256::ZERO);
            snapshot.nonce = Some(0);
            snapshot.code = Some(Vec::new());

            // Process storage from pre-state before zeroing
            for slot in pre_account.storage.keys() {
                let slot_hash = keccak256(slot.0);
                let slot_key = U256::from_be_bytes(slot_hash.into());
                snapshot.storage_updates.insert(slot_key, U256::ZERO);
            }
        }
    }
}
