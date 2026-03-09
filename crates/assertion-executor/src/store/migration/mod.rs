//! Versioned schema migrations for the assertion sled store.
//!
//! To add a new migration:
//! 1. Create `vN.rs` with a `pub fn migrate(db: &sled::Db)` function
//! 2. Add `mod vN;` below
//! 3. Add `(N, vN::migrate)` to `MIGRATIONS`
//! 4. Bump `CURRENT_SCHEMA_VERSION` to `N`
//! 5. Update the pinned test in `v(N-1).rs` to confirm old data still deserializes

mod v2;

use crate::store::AssertionStoreError;
use tracing::info;

const SCHEMA_VERSION_TREE: &str = "schema_version";
const CURRENT_SCHEMA_VERSION: u8 = 2;

/// Migration function signature.
type MigrateFn = fn(&sled::Db) -> Result<(), AssertionStoreError>;

/// Ordered list of migrations. Each entry is (`target_version`, `migrate_fn`).
/// Migrations run sequentially for any version below the target.
const MIGRATIONS: &[(u8, MigrateFn)] = &[(2, v2::migrate)];

/// Checks the schema version stored in sled and runs any pending migrations.
/// Called once at startup from `AssertionStore::new()`.
pub fn run(db: &sled::Db) -> Result<(), AssertionStoreError> {
    let version_tree = db
        .open_tree(SCHEMA_VERSION_TREE)
        .map_err(AssertionStoreError::SledError)?;

    let current: u8 = version_tree
        .get(b"v")
        .map_err(AssertionStoreError::SledError)?
        .map_or(1, |v| v[0]); // no marker = V1 (pre-versioning)

    if current >= CURRENT_SCHEMA_VERSION {
        return Ok(());
    }

    info!(
        target: "assertion-executor::migration",
        from = current,
        to = CURRENT_SCHEMA_VERSION,
        "Assertion store schema migration required"
    );

    for &(target_version, migrate_fn) in MIGRATIONS {
        if current < target_version {
            info!(
                target: "assertion-executor::migration",
                from = target_version - 1,
                to = target_version,
                "Running migration"
            );
            migrate_fn(db)?;

            version_tree
                .insert(b"v", [target_version])
                .map_err(AssertionStoreError::SledError)?;
            db.flush().map_err(AssertionStoreError::SledError)?;
        }
    }

    info!(
        target: "assertion-executor::migration",
        version = CURRENT_SCHEMA_VERSION,
        "Assertion store schema up to date"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        inspectors::{
            TriggerRecorder,
            spec_recorder::AssertionSpec,
        },
        primitives::{
            Address,
            AssertionContract,
            B256,
        },
        store::{
            AssertionState,
            migration::v2::AssertionStateV1,
        },
    };
    use bincode::{
        deserialize as de,
        serialize as ser,
    };

    /// Creates a unique temporary directory path for sled tests.
    fn test_db_path() -> std::path::PathBuf {
        let id = B256::random();
        std::env::temp_dir().join(format!("sled_migration_test_{id}"))
    }

    /// Removes the test db directory if it exists.
    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn migration_converts_v1_entries_to_v2() {
        let path = test_db_path();
        cleanup(&path);
        let adopter = Address::random();
        let assertion_id = B256::random();

        // Simulate a V1 database (pre-1.1.0)
        {
            let db = sled::open(&path).unwrap();
            let v1 = vec![AssertionStateV1 {
                activation_block: 42,
                inactivation_block: None,
                assertion_contract: AssertionContract {
                    id: assertion_id,
                    ..Default::default()
                },
                trigger_recorder: TriggerRecorder::default(),
            }];
            db.insert(adopter, ser(&v1).unwrap()).unwrap();
            db.flush().unwrap();
        }

        // Run migration
        {
            let db = sled::open(&path).unwrap();
            run(&db).unwrap();

            // Verify version was set
            let version_tree = db.open_tree(SCHEMA_VERSION_TREE).unwrap();
            let version = version_tree.get(b"v").unwrap().unwrap();
            assert_eq!(version[0], CURRENT_SCHEMA_VERSION);

            // Verify data was migrated
            let raw = db.get(adopter).unwrap().unwrap();
            let states: Vec<AssertionState> = de(&raw).unwrap();
            assert_eq!(states.len(), 1);
            assert_eq!(states[0].activation_block, 42);
            assert_eq!(states[0].assertion_contract.id, assertion_id);
            assert_eq!(states[0].assertion_spec, AssertionSpec::Legacy);
        }

        cleanup(&path);
    }

    #[test]
    fn migration_is_idempotent() {
        let path = test_db_path();
        cleanup(&path);
        let adopter = Address::random();

        // Write V2 data directly (simulating already-migrated or fresh deployment)
        {
            let db = sled::open(&path).unwrap();
            let state = vec![AssertionState {
                activation_block: 1,
                inactivation_block: None,
                assertion_contract: AssertionContract::default(),
                trigger_recorder: TriggerRecorder::default(),
                assertion_spec: AssertionSpec::Reshiram,
            }];
            db.insert(adopter, ser(&state).unwrap()).unwrap();

            // Set version to current
            let version_tree = db.open_tree(SCHEMA_VERSION_TREE).unwrap();
            version_tree.insert(b"v", [CURRENT_SCHEMA_VERSION]).unwrap();
            db.flush().unwrap();
        }

        // Run migration — should be a no-op
        {
            let db = sled::open(&path).unwrap();
            run(&db).unwrap();

            // Verify data was NOT changed (still Reshiram, not Legacy)
            let raw = db.get(adopter).unwrap().unwrap();
            let states: Vec<AssertionState> = de(&raw).unwrap();
            assert_eq!(states[0].assertion_spec, AssertionSpec::Reshiram);
        }

        cleanup(&path);
    }

    #[test]
    fn migration_skips_on_fresh_db_with_version() {
        let path = test_db_path();
        cleanup(&path);

        {
            let db = sled::open(&path).unwrap();
            let version_tree = db.open_tree(SCHEMA_VERSION_TREE).unwrap();
            version_tree.insert(b"v", [CURRENT_SCHEMA_VERSION]).unwrap();
            db.flush().unwrap();
        }

        // Should return immediately without error
        {
            let db = sled::open(&path).unwrap();
            run(&db).unwrap();
        }

        cleanup(&path);
    }
}
