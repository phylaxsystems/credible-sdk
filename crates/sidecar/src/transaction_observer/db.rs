use super::{
    IncidentReport,
    TransactionObserverError,
};
use rand::random;
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
    WriteFlags,
};
use std::path::Path;

const INCIDENT_REPORTS_TABLE: &str = "incident_reports";

/// Opens an MDBX database holding the incident reports.
/// Used as the source of truth for what incidents we are
/// yet to report on to the dapp API.
// TODO: i dont really like that we are opening a seprate database
// for this. we should have a global sidecar mdbx.
pub(super) struct IncidentDb {
    env: DatabaseEnv,
    dbi: reth_libmdbx::ffi::MDBX_dbi,
}

impl IncidentDb {
    pub(super) fn open(path: &str) -> Result<Self, TransactionObserverError> {
        if path.is_empty() {
            return Err(TransactionObserverError::DatabaseOpen {
                reason: "incident db path is empty".to_string(),
            });
        }

        let path = Path::new(path);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                TransactionObserverError::DatabaseOpen {
                    reason: format!("Failed to create db directory: {e}"),
                }
            })?;
        }

        let args = DatabaseArguments::default();
        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;

        let tx = env.tx_mut().map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;
        let db = tx
            .inner
            .create_db(Some(INCIDENT_REPORTS_TABLE), DatabaseFlags::default())
            .map_err(|e| {
                TransactionObserverError::DatabaseOpen {
                    reason: e.to_string(),
                }
            })?;
        let dbi = db.dbi();
        tx.commit().map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;

        Ok(Self { env, dbi })
    }

    /// Store an individual incident in an on disk persistant db.
    /// Incidents stored are synced to disk immediately.
    pub(super) fn store(&self, report: &IncidentReport) -> Result<(), TransactionObserverError> {
        // we need individual keys for incidents
        // we might see the same hash for multiple incidents so we just use
        // a random key
        //
        // we should never actually see duplicate incidents hit the observer
        let key = random::<u32>().to_be_bytes();

        let payload = bincode::serialize(report).map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;

        let tx = self.env.tx_mut().map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        let env = tx.inner.env().clone();
        tx.inner
            .put(self.dbi, &key, &payload, WriteFlags::empty())
            .map_err(|e| {
                TransactionObserverError::PersistFailed {
                    reason: e.to_string(),
                }
            })?;
        tx.commit().map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        env.sync(true).map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        Ok(())
    }

    /// Loads up to `limit` incident reports from a consistent snapshot.
    ///
    /// Returns (key, report) pairs so callers can delete on success.
    pub(super) fn load_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IncidentReport)>, TransactionObserverError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let tx = self.env.tx().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        let mut cursor = tx
            .inner
            .cursor_with_dbi(self.dbi)
            .map_err(|e| TransactionObserverError::PublishFailed {
                reason: e.to_string(),
            })?;

        let mut reports = Vec::new();
        let mut next = cursor
            .first::<Vec<u8>, Vec<u8>>()
            .map_err(|e| TransactionObserverError::PublishFailed {
                reason: e.to_string(),
            })?;
        while let Some((key, payload)) = next {
            let report = bincode::deserialize(&payload).map_err(|e| {
                TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                }
            })?;
            reports.push((key, report));
            if reports.len() >= limit {
                break;
            }
            next = cursor
                .next::<Vec<u8>, Vec<u8>>()
                .map_err(|e| TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                })?;
        }

        Ok(reports)
    }

    /// Deletes incident reports by key in a single write transaction.
    pub(super) fn delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), TransactionObserverError> {
        if keys.is_empty() {
            return Ok(());
        }

        let tx = self.env.tx_mut().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        for key in keys {
            tx.inner
                .del(self.dbi, key, None)
                .map_err(|e| TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                })?;
        }
        tx.commit().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        Ok(())
    }
}
