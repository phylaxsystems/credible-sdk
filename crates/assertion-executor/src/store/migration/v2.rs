//! V2 migration: adds `assertion_spec` field to `AssertionState`.
//!
//! Pre-1.1.0 (`AssertionStateV1`) did not have `assertion_spec`.
//! This migration reads all V1 entries from the default sled tree,
//! deserializes them with the old struct, and re-serializes with
//! `assertion_spec: AssertionSpec::Legacy`.

use bincode::{
    deserialize as de,
    serialize as ser,
};
use tracing::{
    error,
    info,
};

use crate::{
    inspectors::spec_recorder::AssertionSpec,
    store::{
        AssertionState,
        AssertionStoreError,
        migration::AssertionStateV1,
    },
};

pub fn migrate(db: &sled::Db) -> Result<(), AssertionStoreError> {
    let mut migrated = 0u64;

    for entry in db.iter() {
        let (key, value) = entry.map_err(AssertionStoreError::SledError)?;

        // Already in current format — skip
        if de::<Vec<AssertionState>>(&value).is_ok() {
            continue;
        }

        // Must be V1 format — fail hard if it isn't
        let v1_states = de::<Vec<AssertionStateV1>>(&value).map_err(|e| {
            error!(
                target: "assertion-executor::migration",
                key = ?key.as_ref(),
                error = %e,
                "Entry is neither V1 nor V2 — cannot migrate"
            );
            AssertionStoreError::BincodeError(e)
        })?;

        let v2_states: Vec<AssertionState> = v1_states
            .into_iter()
            .map(|v1| {
                AssertionState {
                    activation_block: v1.activation_block,
                    inactivation_block: v1.inactivation_block,
                    assertion_contract: v1.assertion_contract,
                    trigger_recorder: v1.trigger_recorder,
                    assertion_spec: AssertionSpec::Legacy,
                }
            })
            .collect();

        let serialized = ser(&v2_states).map_err(AssertionStoreError::BincodeError)?;
        db.insert(key, serialized)
            .map_err(AssertionStoreError::SledError)?;
        migrated += 1;
    }

    info!(
        target: "assertion-executor::migration",
        migrated,
        "V2 migration complete: added assertion_spec=Legacy to all entries"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        inspectors::TriggerRecorder,
        primitives::{
            AssertionContract,
            B256,
        },
    };

    #[test]
    fn v1_data_migrates_to_v2_with_legacy_spec() {
        let v1 = vec![AssertionStateV1 {
            activation_block: 42,
            inactivation_block: Some(100),
            assertion_contract: AssertionContract {
                id: B256::ZERO,
                ..Default::default()
            },
            trigger_recorder: TriggerRecorder::default(),
        }];
        let v1_bytes = ser(&v1).unwrap();

        // V1 bytes cannot deserialize as current AssertionState
        assert!(de::<Vec<AssertionState>>(&v1_bytes).is_err());

        // V1 bytes can deserialize as AssertionStateV1
        let parsed = de::<Vec<AssertionStateV1>>(&v1_bytes).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].activation_block, 42);
        assert_eq!(parsed[0].inactivation_block, Some(100));
    }

    #[test]
    fn v2_data_roundtrips() {
        let state = AssertionState {
            activation_block: 42,
            inactivation_block: Some(100),
            assertion_contract: AssertionContract {
                id: B256::ZERO,
                ..Default::default()
            },
            trigger_recorder: TriggerRecorder::default(),
            assertion_spec: AssertionSpec::Legacy,
        };
        let bytes = ser(&vec![state.clone()]).unwrap();
        let roundtrip: Vec<AssertionState> = de(&bytes).unwrap();
        assert_eq!(roundtrip[0], state);
    }
}
