use reth_db::mdbx::{
    DatabaseArguments,
    DatabaseEnv,
    DatabaseEnvKind,
};
use reth_db_api::{
    Database,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use reth_libmdbx::{
    DatabaseFlags,
    ffi::MDBX_dbi,
};
use std::path::Path;

const TX_CONTENT_HASHES_TABLE: &str = "tx_content_hashes";
const INCIDENT_REPORTS_TABLE: &str = "incident_reports";

/// Shared MDBX environment for the sidecar.
///
/// Holds two tables:
/// - `tx_content_hashes` — content-hash dedup cache (key = B256, value = u64 block number BE)
/// - `incident_reports`  — persistent incident reports for the transaction observer
pub struct SidecarDb {
    env: DatabaseEnv,
    content_hashes_dbi: MDBX_dbi,
    incident_reports_dbi: MDBX_dbi,
}

impl SidecarDb {
    /// Open (or create) the shared sidecar MDBX environment at `path`.
    pub fn open(path: &str) -> Result<Self, SidecarDbError> {
        if path.is_empty() {
            return Err(SidecarDbError::Open {
                reason: "sidecar db path is empty".to_string(),
            });
        }

        let path = Path::new(path);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                SidecarDbError::Open {
                    reason: format!("failed to create db directory: {e}"),
                }
            })?;
        }

        let args = DatabaseArguments::default();
        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            SidecarDbError::Open {
                reason: e.to_string(),
            }
        })?;

        let tx = env.tx_mut().map_err(|e| {
            SidecarDbError::Open {
                reason: e.to_string(),
            }
        })?;
        let content_hashes_db = tx
            .inner
            .create_db(Some(TX_CONTENT_HASHES_TABLE), DatabaseFlags::default())
            .map_err(|e| {
                SidecarDbError::Open {
                    reason: e.to_string(),
                }
            })?;
        let incident_reports_db = tx
            .inner
            .create_db(Some(INCIDENT_REPORTS_TABLE), DatabaseFlags::default())
            .map_err(|e| {
                SidecarDbError::Open {
                    reason: e.to_string(),
                }
            })?;
        let content_hashes_dbi = content_hashes_db.dbi();
        let incident_reports_dbi = incident_reports_db.dbi();
        tx.commit().map_err(|e| {
            SidecarDbError::Open {
                reason: e.to_string(),
            }
        })?;

        Ok(Self {
            env,
            content_hashes_dbi,
            incident_reports_dbi,
        })
    }

    pub fn env(&self) -> &DatabaseEnv {
        &self.env
    }

    pub fn content_hashes_dbi(&self) -> MDBX_dbi {
        self.content_hashes_dbi
    }

    pub fn incident_reports_dbi(&self) -> MDBX_dbi {
        self.incident_reports_dbi
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum SidecarDbError {
    #[error("Failed to open sidecar database: {reason}")]
    Open { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_db_api::transaction::{
        DbTx,
        DbTxMut,
    };
    use reth_libmdbx::WriteFlags;
    use tempfile::TempDir;

    #[test]
    fn open_creates_both_tables() {
        let tempdir = TempDir::new().unwrap();
        let db = SidecarDb::open(&tempdir.path().to_string_lossy()).unwrap();

        // Write to content_hashes
        let tx = db.env().tx_mut().unwrap();
        tx.inner
            .put(
                db.content_hashes_dbi(),
                [0u8; 32],
                42u64.to_be_bytes(),
                WriteFlags::empty(),
            )
            .unwrap();
        tx.commit().unwrap();

        // Write to incident_reports
        let tx = db.env().tx_mut().unwrap();
        tx.inner
            .put(
                db.incident_reports_dbi(),
                [1u8; 16],
                b"data",
                WriteFlags::empty(),
            )
            .unwrap();
        tx.commit().unwrap();

        // Read back
        let tx = db.env().tx().unwrap();
        let val: Option<Vec<u8>> = tx
            .inner
            .get(db.content_hashes_dbi(), [0u8; 32].as_slice())
            .unwrap();
        assert_eq!(val.as_deref(), Some(42u64.to_be_bytes().as_slice()));

        let val: Option<Vec<u8>> = tx
            .inner
            .get(db.incident_reports_dbi(), [1u8; 16].as_slice())
            .unwrap();
        assert_eq!(val.as_deref(), Some(b"data".as_slice()));
    }

    #[test]
    fn open_empty_path_fails() {
        let result = SidecarDb::open("");
        assert!(result.is_err());
    }
}
