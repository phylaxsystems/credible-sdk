//! Shovel-based consumer for the State Oracle assertion events.
//!
//! Instead of maintaining a direct WebSocket connection to an Ethereum node,
//! this module reads pre-indexed assertion events from a PostgreSQL database
//! populated by [Shovel](https://github.com/indexsupply/shovel).
//!
//! Shovel handles: chain following, `eth_getLogs`, event ABI decoding, reorg
//! detection and row pruning.  This consumer only needs to:
//!
//! 1. Query Postgres for new rows (poll or `LISTEN/NOTIFY`).
//! 2. Fetch assertion bytecode from the DA layer.
//! 3. Extract the assertion contract and build `PendingModification`s.
//! 4. Apply them to the `AssertionStore`.

use alloy::primitives::Bytes;
use tracing::{debug, error, info, trace, warn};

use crate::{
    ExecutorConfig,
    metrics,
    primitives::{Address, B256},
    store::{
        AssertionStore, AssertionStoreError, PendingModification, extract_assertion_contract,
        FnSelectorExtractorError,
    },
};

use assertion_da_client::{DaClient, DaClientError, DaFetchResponse};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the Shovel PG consumer.
#[derive(Debug, Clone)]
pub struct ShovelConsumerCfg {
    /// Postgres connection string (e.g. `postgres:///shovel`)
    pub pg_url: String,
    /// DA layer client – used to fetch assertion bytecode by id
    pub da_client: DaClient,
    /// Executor configuration for `extract_assertion_contract`
    pub executor_config: ExecutorConfig,
    /// The assertion store to write modifications into
    pub store: AssertionStore,
    /// Fallback polling interval in milliseconds
    pub poll_interval_ms: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ShovelConsumerError {
    #[error("Postgres error")]
    PostgresError(#[source] tokio_postgres::Error),
    #[error("DA Client error")]
    DaClientError(#[source] DaClientError),
    #[error("Assertion store error")]
    AssertionStoreError(#[source] AssertionStoreError),
    #[error("Failed to extract assertion contract")]
    FnSelectorExtractorError(#[source] FnSelectorExtractorError),
    #[error("Block number exceeds u64")]
    BlockNumberExceedsU64,
    #[error("Invalid row: {0}")]
    InvalidRow(String),
}

pub type ShovelConsumerResult<T = ()> = Result<T, ShovelConsumerError>;

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

/// Reads assertion events from a Shovel-populated Postgres table and feeds
/// them into the `AssertionStore`.
pub struct ShovelConsumer {
    pg_client: tokio_postgres::Client,
    da_client: DaClient,
    executor_config: ExecutorConfig,
    store: AssertionStore,
    poll_interval: tokio::time::Duration,
    /// Highest `(block_num, log_idx)` we have already processed.
    last_processed: Option<(i64, i64)>,
}

impl ShovelConsumer {
    /// Connect to Postgres and return a ready consumer.
    ///
    /// Spawns the `tokio-postgres` connection future on the runtime.
    pub async fn connect(cfg: ShovelConsumerCfg) -> ShovelConsumerResult<Self> {
        let (client, connection) = tokio_postgres::connect(&cfg.pg_url, tokio_postgres::NoTls)
            .await
            .map_err(ShovelConsumerError::PostgresError)?;

        // The connection must be driven to completion on its own task.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = ?e, "Postgres connection error");
            }
        });

        info!(pg_url = %cfg.pg_url, "Connected to Shovel Postgres");

        Ok(Self {
            pg_client: client,
            da_client: cfg.da_client,
            executor_config: cfg.executor_config,
            store: cfg.store,
            poll_interval: tokio::time::Duration::from_millis(cfg.poll_interval_ms),
            last_processed: None,
        })
    }

    /// Catch-up: process all existing rows in the `assertion_events` table.
    pub async fn sync(&mut self) -> ShovelConsumerResult {
        info!("Syncing: processing all existing assertion events from Postgres");
        metrics::set_syncing(true);

        let rows = self
            .pg_client
            .query(
                "SELECT ig_name, block_num, log_idx, assertion_adopter, assertion_id, activation_block \
                 FROM assertion_events \
                 ORDER BY block_num ASC, log_idx ASC",
                &[],
            )
            .await
            .map_err(ShovelConsumerError::PostgresError)?;

        let total = rows.len();
        if total == 0 {
            info!("No existing assertion events to process");
            metrics::set_syncing(false);
            return Ok(());
        }

        info!(total, "Processing existing assertion events");

        let mut mods = Vec::new();
        for row in &rows {
            if let Some(pending_mod) = self.process_row(row).await? {
                mods.push(pending_mod);
            }
        }

        if !mods.is_empty() {
            let count = mods.len() as u64;
            self.store
                .apply_pending_modifications(mods)
                .map_err(ShovelConsumerError::AssertionStoreError)?;
            metrics::record_assertions_moved(count);
            info!(count, "Applied pending modifications during sync");
        }

        metrics::set_syncing(false);
        info!("Sync complete");
        Ok(())
    }

    /// Steady-state: poll Postgres for new rows periodically.
    pub async fn run(&mut self) -> ShovelConsumerResult {
        info!(
            poll_interval_ms = self.poll_interval.as_millis(),
            "Starting Shovel consumer poll loop"
        );

        loop {
            tokio::time::sleep(self.poll_interval).await;
            self.poll_new_events().await?;
        }
    }

    /// Query for rows newer than `last_processed` and apply them.
    async fn poll_new_events(&mut self) -> ShovelConsumerResult {
        let rows = match self.last_processed {
            Some((block_num, log_idx)) => {
                self.pg_client
                    .query(
                        "SELECT ig_name, block_num, log_idx, assertion_adopter, assertion_id, activation_block \
                         FROM assertion_events \
                         WHERE (block_num, log_idx) > ($1, $2) \
                         ORDER BY block_num ASC, log_idx ASC",
                        &[&block_num, &log_idx],
                    )
                    .await
                    .map_err(ShovelConsumerError::PostgresError)?
            }
            None => {
                self.pg_client
                    .query(
                        "SELECT ig_name, block_num, log_idx, assertion_adopter, assertion_id, activation_block \
                         FROM assertion_events \
                         ORDER BY block_num ASC, log_idx ASC",
                        &[],
                    )
                    .await
                    .map_err(ShovelConsumerError::PostgresError)?
            }
        };

        if rows.is_empty() {
            return Ok(());
        }

        debug!(new_rows = rows.len(), "Fetched new assertion events");

        let mut mods = Vec::new();
        for row in &rows {
            if let Some(pending_mod) = self.process_row(row).await? {
                mods.push(pending_mod);
            }
        }

        if !mods.is_empty() {
            let count = mods.len() as u64;
            self.store
                .apply_pending_modifications(mods)
                .map_err(ShovelConsumerError::AssertionStoreError)?;
            metrics::record_assertions_moved(count);
            debug!(count, "Applied pending modifications");
        }

        Ok(())
    }

    /// Process a single row from the `assertion_events` table into a
    /// `PendingModification`.
    ///
    /// The `ig_name` column tells us whether this is an `AssertionAdded` or
    /// `AssertionRemoved` event (matching the Shovel integration name).
    async fn process_row(
        &mut self,
        row: &tokio_postgres::Row,
    ) -> ShovelConsumerResult<Option<PendingModification>> {
        let ig_name: &str = row.get("ig_name");
        let block_num: i64 = row.get("block_num");
        let log_idx: i64 = row.get("log_idx");

        // Update cursor
        self.last_processed = Some((block_num, log_idx));
        metrics::set_head_block(block_num as u64);

        let assertion_adopter_bytes: &[u8] = row.get("assertion_adopter");
        let assertion_id_bytes: &[u8] = row.get("assertion_id");

        let assertion_adopter = parse_address(assertion_adopter_bytes)?;
        let assertion_id = parse_b256(assertion_id_bytes)?;

        if ig_name.contains("assertion_added") || ig_name.contains("assertionadded") {
            // AssertionAdded event
            let activation_block: i64 = row.get("activation_block");

            trace!(
                ?assertion_adopter,
                ?assertion_id,
                activation_block,
                "Processing AssertionAdded row"
            );

            // Fetch bytecode from DA layer
            let DaFetchResponse {
                bytecode,
                encoded_constructor_args,
                ..
            } = self
                .da_client
                .fetch_assertion(assertion_id)
                .await
                .map_err(ShovelConsumerError::DaClientError)?;

            // Rebuild deployment bytecode
            let mut deployment_bytecode = bytecode.to_vec();
            deployment_bytecode.extend_from_slice(encoded_constructor_args.as_ref());
            let deployment_bytecode: Bytes = deployment_bytecode.into();

            let assertion_contract_res =
                extract_assertion_contract(&deployment_bytecode, &self.executor_config);

            match assertion_contract_res {
                Ok((assertion_contract, trigger_recorder)) => {
                    info!(
                        assertion_id = ?assertion_contract.id,
                        activation_block,
                        "AssertionAdded event processed",
                    );

                    metrics::record_assertions_seen(1);

                    Ok(Some(PendingModification::Add {
                        assertion_adopter,
                        assertion_contract,
                        trigger_recorder,
                        activation_block: activation_block as u64,
                        log_index: log_idx as u64,
                    }))
                }
                Err(err) => {
                    warn!(?err, "Failed to extract assertion contract");
                    Ok(None)
                }
            }
        } else if ig_name.contains("assertion_removed") || ig_name.contains("assertionremoved") {
            // AssertionRemoved event — activation_block column holds deactivation_block
            let inactivation_block: i64 = row.get("activation_block");

            info!(
                ?assertion_adopter,
                ?assertion_id,
                inactivation_block,
                "AssertionRemoved event processed",
            );

            metrics::record_assertions_seen(1);

            Ok(Some(PendingModification::Remove {
                assertion_adopter,
                assertion_contract_id: assertion_id,
                inactivation_block: inactivation_block as u64,
                log_index: log_idx as u64,
            }))
        } else {
            warn!(ig_name, "Unknown integration name, skipping row");
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_address(bytes: &[u8]) -> ShovelConsumerResult<Address> {
    if bytes.len() != 20 {
        return Err(ShovelConsumerError::InvalidRow(format!(
            "Expected 20-byte address, got {} bytes",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 20];
    buf.copy_from_slice(bytes);
    Ok(Address::from(buf))
}

fn parse_b256(bytes: &[u8]) -> ShovelConsumerResult<B256> {
    if bytes.len() != 32 {
        return Err(ShovelConsumerError::InvalidRow(format!(
            "Expected 32-byte hash, got {} bytes",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 32];
    buf.copy_from_slice(bytes);
    Ok(B256::from(buf))
}
