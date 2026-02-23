//! # `indexer`
//!
//! Contains the assertion syncer that polls an external event source (e.g. GraphQL API)
//! for assertion events, fetches bytecode from the DA layer, and updates the local
//! assertion store.

use crate::utils::ErrorRecoverability;
use alloy::primitives::Bytes;
use assertion_da_client::{
    DaClient,
    DaClientError,
    DaFetchResponse,
};
use assertion_executor::{
    ExecutorConfig,
    metrics,
    store::{
        AssertionAddedEvent,
        AssertionStore,
        AssertionStoreError,
        EventSource,
        EventSourceError,
        FnSelectorExtractorError,
        PendingModification,
        extract_assertion_contract,
    },
};
use std::time::Duration;
use tracing::{
    debug,
    info,
    warn,
};

/// Configuration for the assertion syncer.
pub struct SyncerCfg<S: EventSource> {
    pub event_source: S,
    pub store: AssertionStore,
    pub da_client: DaClient,
    pub executor_config: ExecutorConfig,
    pub poll_interval: Duration,
}

/// Polls an [`EventSource`] for new events,
/// fetches bytecode from the DA layer, extracts assertion contracts, and
/// applies modifications to the [`AssertionStore`].
struct AssertionSyncer<S: EventSource> {
    event_source: S,
    store: AssertionStore,
    da_client: DaClient,
    executor_config: ExecutorConfig,
    poll_interval: Duration,
    /// The last block number we have processed events for.
    last_synced_block: u64,
}

impl<S: EventSource> AssertionSyncer<S> {
    fn new(cfg: SyncerCfg<S>) -> Self {
        Self {
            event_source: cfg.event_source,
            store: cfg.store,
            da_client: cfg.da_client,
            executor_config: cfg.executor_config,
            poll_interval: cfg.poll_interval,
            last_synced_block: 0,
        }
    }

    /// Run the syncer loop indefinitely.
    async fn run(&mut self) -> Result<(), SyncerError> {
        info!(
            target = "sidecar::syncer",
            poll_interval_ms = self.poll_interval.as_millis(),
            "Starting assertion syncer"
        );

        loop {
            self.sync_once().await?;
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Perform a single sync cycle: fetch new events and apply them.
    async fn sync_once(&mut self) -> Result<(), SyncerError> {
        let since = self.last_synced_block;

        debug!(
            target = "sidecar::syncer",
            since_block = since,
            "Polling for new events"
        );

        // Check if the external indexer head moved backward ie possible reorg handling upstream
        if let Ok(Some(head)) = self.event_source.get_indexer_head().await
            && head < self.last_synced_block
        {
            warn!(
                target = "sidecar::syncer",
                indexer_head = head,
                last_synced_block = self.last_synced_block,
                "Event source head moved backward, possible reorg upstream. Skipping cycle."
            );
            metrics::record_event_source_head_regression();
            return Ok(());
        }

        // Fetch added and removed events concurrently
        let (added_result, removed_result) = tokio::join!(
            self.event_source.fetch_added_events(since),
            self.event_source.fetch_removed_events(since),
        );

        let added_events = added_result?;
        let removed_events = removed_result?;

        if added_events.is_empty() && removed_events.is_empty() {
            debug!(
                target = "sidecar::syncer",
                since_block = since,
                "No new events"
            );
            // Still update last_synced_block from the indexer head
            if let Ok(Some(head)) = self.event_source.get_indexer_head().await
                && head > self.last_synced_block
            {
                self.last_synced_block = head;
                self.store.set_current_block(head);
                metrics::set_head_block(head);
            }
            return Ok(());
        }

        info!(
            target = "sidecar::syncer",
            added_count = added_events.len(),
            removed_count = removed_events.len(),
            since_block = since,
            "Processing new events"
        );

        let mut max_block = since;
        let mut modifications = Vec::new();

        // Process added events -> PendingModification::Add
        for event in &added_events {
            max_block = max_block.max(event.block);
            metrics::record_assertions_seen(1);

            match self.process_added_event(event).await {
                Ok(Some(modification)) => modifications.push(modification),
                Ok(None) => {
                    warn!(
                        target = "sidecar::syncer",
                        assertion_id = ?event.assertion_id,
                        "Failed to extract assertion contract, skipping"
                    );
                }
                Err(e) => {
                    warn!(
                        target = "sidecar::syncer",
                        assertion_id = ?event.assertion_id,
                        error = ?e,
                        "Error processing added event, skipping"
                    );
                }
            }
        }

        // Process removed events -> PendingModification::Remove
        for event in &removed_events {
            max_block = max_block.max(event.block);

            modifications.push(PendingModification::Remove {
                assertion_adopter: event.assertion_adopter,
                assertion_contract_id: event.assertion_id,
                inactivation_block: event.deactivation_block,
                log_index: 0,
            });
        }

        // Apply all modifications to the assertion store
        if !modifications.is_empty() {
            let count = modifications.len() as u64;
            self.store.apply_pending_modifications(modifications)?;
            metrics::record_assertions_moved(count);
        }

        self.last_synced_block = max_block;
        self.store.set_current_block(max_block);
        metrics::set_head_block(max_block);
        metrics::set_syncing(false);

        Ok(())
    }

    /// Process a single `AssertionAdded` event:
    /// - Fetch bytecode from DA
    /// - Extract assertion contract + trigger recorder
    /// - Return `PendingModification::Add`
    async fn process_added_event(
        &self,
        event: &AssertionAddedEvent,
    ) -> Result<Option<PendingModification>, SyncerError> {
        let DaFetchResponse {
            bytecode,
            encoded_constructor_args,
            ..
        } = self.da_client.fetch_assertion(event.assertion_id).await?;

        // Rebuild deployment bytecode by appending constructor args
        let mut deployment_bytecode = bytecode.to_vec();
        deployment_bytecode.extend_from_slice(encoded_constructor_args.as_ref());
        let deployment_bytecode: Bytes = deployment_bytecode.into();

        match extract_assertion_contract(&deployment_bytecode, &self.executor_config) {
            Ok((assertion_contract, trigger_recorder)) => {
                info!(
                    target = "sidecar::syncer",
                    assertion_id = ?assertion_contract.id,
                    activation_block = event.activation_block,
                    "Processed AssertionAdded event"
                );
                Ok(Some(PendingModification::Add {
                    assertion_adopter: event.assertion_adopter,
                    assertion_contract,
                    trigger_recorder,
                    activation_block: event.activation_block,
                    log_index: 0,
                }))
            }
            Err(err) => {
                warn!(
                    target = "sidecar::syncer",
                    ?err,
                    "Failed to extract assertion contract"
                );
                Ok(None)
            }
        }
    }
}

/// Polls the configured event source for new assertion events,
/// fetches bytecode from the DA layer, and updates the assertion store.
pub async fn run_syncer<S: EventSource>(cfg: SyncerCfg<S>) -> Result<(), SyncerError> {
    let mut syncer = AssertionSyncer::new(cfg);
    syncer.run().await
}

impl From<&SyncerError> for ErrorRecoverability {
    fn from(e: &SyncerError) -> Self {
        match e {
            // Store errors are unrecoverable
            SyncerError::Store(_) => ErrorRecoverability::Unrecoverable,
            // Network, DA, and extraction errors are recoverable — will retry on next poll
            SyncerError::EventSource(_)
            | SyncerError::DaClient(_)
            | SyncerError::ExtractionError(_) => ErrorRecoverability::Recoverable,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncerError {
    #[error("Event source error: {0}")]
    EventSource(#[from] EventSourceError),
    #[error("DA client error: {0}")]
    DaClient(#[from] DaClientError),
    #[error("Assertion store error: {0}")]
    Store(#[from] AssertionStoreError),
    #[error("Assertion contract extraction error: {0}")]
    ExtractionError(#[from] FnSelectorExtractorError),
}
