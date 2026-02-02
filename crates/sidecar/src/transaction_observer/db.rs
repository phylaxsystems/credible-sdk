use super::{
    IncidentReport,
    TransactionObserverError,
};
use crate::db::SidecarDb;
use rand::random;
use reth_db_api::{
    Database,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use reth_libmdbx::{
    WriteFlags,
    ffi::MDBX_dbi,
};
use std::sync::Arc;

const MAX_INCIDENT_BATCH: usize = 10_000;

/// MDBX-based storage for incident reports.
///
/// Uses the shared `SidecarDb` environment and the `incident_reports` table.
pub(super) struct IncidentDb {
    db: Arc<SidecarDb>,
    dbi: MDBX_dbi,
}

impl IncidentDb {
    /// Create a new `IncidentDb` backed by the shared `SidecarDb`.
    pub(super) fn new(db: Arc<SidecarDb>) -> Self {
        let dbi = db.incident_reports_dbi();
        Self { db, dbi }
    }

    /// Store an individual incident in an on disk persistent db.
    /// Incidents stored are synced to disk immediately.
    pub(super) fn store(&self, report: &IncidentReport) -> Result<(), TransactionObserverError> {
        // we need individual keys for incidents
        // we might see the same hash for multiple incidents so we just use
        // a random key
        //
        // we should never actually see duplicate incidents hit the observer
        let key = random::<u128>().to_be_bytes();

        let payload = bincode::serialize(report).map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;

        let tx = self.db.env().tx_mut().map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        tx.inner
            .put(self.dbi, key, &payload, WriteFlags::empty())
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
        self.db.env().sync(true).map_err(|e| {
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
        let limit = limit.min(MAX_INCIDENT_BATCH);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let tx = self.db.env().tx().map_err(|e| {
            TransactionObserverError::IoFailure {
                reason: e.to_string(),
            }
        })?;
        let mut cursor = tx.inner.cursor_with_dbi(self.dbi).map_err(|e| {
            TransactionObserverError::IoFailure {
                reason: e.to_string(),
            }
        })?;

        let mut reports = Vec::new();
        let mut next = cursor.first::<Vec<u8>, Vec<u8>>().map_err(|e| {
            TransactionObserverError::IoFailure {
                reason: e.to_string(),
            }
        })?;
        while let Some((key, payload)) = next {
            let report = bincode::deserialize(&payload).map_err(|e| {
                TransactionObserverError::DeserializeFailed {
                    reason: e.to_string(),
                }
            })?;
            reports.push((key, report));
            if reports.len() >= limit {
                break;
            }
            next = cursor.next::<Vec<u8>, Vec<u8>>().map_err(|e| {
                TransactionObserverError::IoFailure {
                    reason: e.to_string(),
                }
            })?;
        }

        Ok(reports)
    }

    /// Deletes incident reports by key in a single write transaction.
    pub(super) fn delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), TransactionObserverError> {
        if keys.is_empty() {
            return Ok(());
        }

        let tx = self.db.env().tx_mut().map_err(|e| {
            TransactionObserverError::IoFailure {
                reason: e.to_string(),
            }
        })?;
        for key in keys {
            tx.inner.del(self.dbi, key, None).map_err(|e| {
                TransactionObserverError::IoFailure {
                    reason: e.to_string(),
                }
            })?;
        }
        tx.commit().map_err(|e| {
            TransactionObserverError::IoFailure {
                reason: e.to_string(),
            }
        })?;
        Ok(())
    }
}
