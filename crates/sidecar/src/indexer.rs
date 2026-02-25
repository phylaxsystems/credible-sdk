//! # `indexer`
//!
//! Contains the assertion indexer that polls an external event source (e.g. GraphQL API)
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
        AssertionRemovedEvent,
        AssertionStore,
        AssertionStoreError,
        EventSource,
        EventSourceError,
        FnSelectorExtractorError,
        PendingModification,
        extract_assertion_contract,
    },
};
use futures_util::future::join_all;
use std::time::Duration;
use tracing::{
    debug,
    error,
    info,
    warn,
};

/// Maximum number of consecutive retries before declaring the indexer unreachable.
const MAX_RETRIES: u32 = 5;
/// Initial backoff duration between retries.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Maximum backoff duration between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Configuration for the indexer.
pub struct IndexerCfg<S: EventSource> {
    pub event_source: S,
    pub store: AssertionStore,
    pub da_client: DaClient,
    pub executor_config: ExecutorConfig,
    pub poll_interval: Duration,
}

/// Polls an [`EventSource`] for new events,
/// fetches bytecode from the DA layer, extracts assertion contracts, and
/// applies modifications to the [`AssertionStore`].
struct AssertionIndexer<S: EventSource> {
    event_source: S,
    store: AssertionStore,
    da_client: DaClient,
    executor_config: ExecutorConfig,
    poll_interval: Duration,
    /// The last block number we have processed events for.
    last_synced_block: u64,
}

impl<S: EventSource> AssertionIndexer<S> {
    fn new(cfg: IndexerCfg<S>) -> Self {
        Self {
            event_source: cfg.event_source,
            store: cfg.store,
            da_client: cfg.da_client,
            executor_config: cfg.executor_config,
            poll_interval: cfg.poll_interval,
            last_synced_block: 0,
        }
    }

    /// Run the indexer loop indefinitely.
    async fn run(&mut self) -> Result<(), IndexerError> {
        info!(
            target = "sidecar::indexer",
            poll_interval_ms = self.poll_interval.as_millis(),
            "Starting assertion indexer"
        );

        loop {
            self.sync_once().await?;
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Perform a single sync cycle: fetch new events and apply them.
    async fn sync_once(&mut self) -> Result<(), IndexerError> {
        let since = self.last_synced_block;

        debug!(
            target = "sidecar::indexer",
            since_block = since,
            "Polling for new events"
        );

        // Fetch indexer head with backoff retries
        let indexer_head = self.fetch_indexer_head_with_retry().await?;

        // Check if the external indexer head moved backward ie possible reorg handling upstream
        if let Some(head) = indexer_head
            && head < self.last_synced_block
        {
            warn!(
                target = "sidecar::indexer",
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
                target = "sidecar::indexer",
                since_block = since,
                "No new events"
            );
            // Still update last_synced_block from the indexer head
            if let Some(head) = indexer_head
                && head > self.last_synced_block
            {
                self.last_synced_block = head;
                self.store.set_current_block(head);
                metrics::set_head_block(head);
            }
            return Ok(());
        }

        info!(
            target = "sidecar::indexer",
            added_count = added_events.len(),
            removed_count = removed_events.len(),
            since_block = since,
            "Processing new events"
        );

        let mut max_block = self
            .process_events(&added_events, &removed_events, since)
            .await?;

        // Advance to the indexer head if it's ahead of the latest event block
        if let Some(head) = indexer_head {
            max_block = max_block.max(head);
        }
        self.last_synced_block = max_block;
        self.store.set_current_block(max_block);
        metrics::set_head_block(max_block);
        metrics::set_syncing(false);

        Ok(())
    }

    // Fetch the indexer head with exponential backoff retries.
    async fn fetch_indexer_head_with_retry(&self) -> Result<Option<u64>, IndexerError> {
        let mut backoff = INITIAL_BACKOFF;

        for attempt in 1..=MAX_RETRIES {
            match self.event_source.get_indexer_head().await {
                Ok(head) => return Ok(head),
                Err(e) => {
                    warn!(
                        target = "sidecar::indexer",
                        error = ?e,
                        attempt,
                        max_retries = MAX_RETRIES,
                        "Failed to fetch indexer head, retrying"
                    );
                    if attempt < MAX_RETRIES {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }
            }
        }

        error!(
            target = "sidecar::indexer",
            max_retries = MAX_RETRIES,
            "Indexer unreachable after exhausting retries"
        );
        Err(IndexerError::IndexerUnreachable)
    }

    /// Process added and removed events into store modifications.
    /// Returns the maximum block number seen across all events.
    async fn process_events(
        &self,
        added_events: &[AssertionAddedEvent],
        removed_events: &[AssertionRemovedEvent],
        since: u64,
    ) -> Result<u64, IndexerError> {
        let mut max_block = since;
        let mut modifications = Vec::new();

        // Process added events concurrently -> PendingModification::Add
        metrics::record_assertions_seen(added_events.len() as u64);
        let da_results = join_all(
            added_events
                .iter()
                .map(|event| self.process_added_event(event)),
        )
        .await;

        for (event, result) in added_events.iter().zip(da_results) {
            max_block = max_block.max(event.block);
            match result {
                Ok(Some(modification)) => modifications.push(modification),
                Ok(None) => {
                    warn!(
                        target = "sidecar::indexer",
                        assertion_id = ?event.assertion_id,
                        "Failed to extract assertion contract, skipping"
                    );
                }
                Err(e) => {
                    warn!(
                        target = "sidecar::indexer",
                        assertion_id = ?event.assertion_id,
                        error = ?e,
                        "Error processing added event, skipping"
                    );
                }
            }
        }

        // Process removed events -> PendingModification::Remove
        for event in removed_events {
            max_block = max_block.max(event.block);

            modifications.push(PendingModification::Remove {
                assertion_adopter: event.assertion_adopter,
                assertion_contract_id: event.assertion_id,
                inactivation_block: event.deactivation_block,
                log_index: event.log_index,
            });
        }

        // Apply all modifications to the assertion store
        if !modifications.is_empty() {
            let count = modifications.len() as u64;
            self.store.apply_pending_modifications(modifications)?;
            metrics::record_assertions_moved(count);
        }

        Ok(max_block)
    }

    /// Process a single `AssertionAdded` event:
    /// - Fetch bytecode from DA
    /// - Extract assertion contract + trigger recorder
    /// - Return `PendingModification::Add`
    async fn process_added_event(
        &self,
        event: &AssertionAddedEvent,
    ) -> Result<Option<PendingModification>, IndexerError> {
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
                    target = "sidecar::indexer",
                    assertion_id = ?assertion_contract.id,
                    activation_block = event.activation_block,
                    "Processed AssertionAdded event"
                );
                Ok(Some(PendingModification::Add {
                    assertion_adopter: event.assertion_adopter,
                    assertion_contract,
                    trigger_recorder,
                    activation_block: event.activation_block,
                    log_index: event.log_index,
                }))
            }
            Err(err) => {
                warn!(
                    target = "sidecar::indexer",
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
pub async fn run_indexer<S: EventSource>(cfg: IndexerCfg<S>) -> Result<(), IndexerError> {
    let mut indexer = AssertionIndexer::new(cfg);
    indexer.run().await
}

impl From<&IndexerError> for ErrorRecoverability {
    fn from(e: &IndexerError) -> Self {
        match e {
            // Store errors and indexer unreachable (retries exhausted) are unrecoverable
            IndexerError::Store(_) | IndexerError::IndexerUnreachable => {
                ErrorRecoverability::Unrecoverable
            }
            // Network, DA, and extraction errors are recoverable — will retry on next poll
            IndexerError::EventSource(_)
            | IndexerError::DaClient(_)
            | IndexerError::ExtractionError(_) => ErrorRecoverability::Recoverable,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("Event source error: {0}")]
    EventSource(#[from] EventSourceError),
    #[error("DA client error: {0}")]
    DaClient(#[from] DaClientError),
    #[error("Assertion store error: {0}")]
    Store(#[from] AssertionStoreError),
    #[error("Assertion contract extraction error: {0}")]
    ExtractionError(#[from] FnSelectorExtractorError),
    #[error("Indexer unreachable after {MAX_RETRIES} retries")]
    IndexerUnreachable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        B256,
        keccak256,
    };
    use assertion_executor::{
        store::{
            AssertionAddedEvent,
            AssertionRemovedEvent,
            AssertionStore,
            EventSource,
            EventSourceError,
        },
        test_utils,
    };
    use httpmock::MockServer;
    use std::sync::Mutex;

    struct MockEventSource {
        added_events: Mutex<Vec<AssertionAddedEvent>>,
        removed_events: Mutex<Vec<AssertionRemovedEvent>>,
        head: Mutex<Option<u64>>,
        head_error: Mutex<bool>,
    }

    impl MockEventSource {
        fn new() -> Self {
            Self {
                added_events: Mutex::new(Vec::new()),
                removed_events: Mutex::new(Vec::new()),
                head: Mutex::new(None),
                head_error: Mutex::new(false),
            }
        }

        fn set_events(&self, added: Vec<AssertionAddedEvent>, removed: Vec<AssertionRemovedEvent>) {
            *self.added_events.lock().unwrap() = added;
            *self.removed_events.lock().unwrap() = removed;
        }

        fn set_head(&self, block: u64) {
            *self.head.lock().unwrap() = Some(block);
        }

        fn set_head_error(&self, fail: bool) {
            *self.head_error.lock().unwrap() = fail;
        }
    }

    impl EventSource for MockEventSource {
        async fn fetch_added_events(
            &self,
            _since_block: u64,
        ) -> Result<Vec<AssertionAddedEvent>, EventSourceError> {
            Ok(self.added_events.lock().unwrap().drain(..).collect())
        }

        async fn fetch_removed_events(
            &self,
            _since_block: u64,
        ) -> Result<Vec<AssertionRemovedEvent>, EventSourceError> {
            Ok(self.removed_events.lock().unwrap().drain(..).collect())
        }

        async fn get_indexer_head(&self) -> Result<Option<u64>, EventSourceError> {
            if *self.head_error.lock().unwrap() {
                return Err(EventSourceError::RequestFailed(
                    "mock head error".to_string(),
                ));
            }
            Ok(*self.head.lock().unwrap())
        }
    }

    // Mock DA JSON-RPC endpoint returning the given bytecode and constructor args.
    fn mock_da_for_bytecode(
        server: &MockServer,
        _assertion_id: B256,
        bytecode: &[u8],
        constructor_args: &[u8],
    ) {
        let hex_bytecode = format!("0x{}", hex::encode(bytecode));
        let hex_args = format!("0x{}", hex::encode(constructor_args));

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .body_includes("da_get_assertion");
            then.status(200).json_body(serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "solidity_source": "",
                    "bytecode": hex_bytecode,
                    "prover_signature": "0x",
                    "encoded_constructor_args": hex_args,
                    "constructor_abi_signature": "constructor()"
                }
            }));
        });
    }

    // Build an indexer with ephemeral store, default config, and the given mock source.
    fn create_test_indexer(
        source: MockEventSource,
        da_url: &str,
    ) -> AssertionIndexer<MockEventSource> {
        let store = AssertionStore::new_ephemeral();
        let da_client = DaClient::new(da_url).unwrap();
        let cfg = IndexerCfg {
            event_source: source,
            store,
            da_client,
            executor_config: ExecutorConfig::default(),
            poll_interval: Duration::from_millis(10),
        };
        AssertionIndexer::new(cfg)
    }

    // Build an AssertionAddedEvent with assertion_id = keccak256(bytecode).
    fn make_added_event(
        bytecode: &[u8],
        adopter: Address,
        block: u64,
        activation_block: u64,
    ) -> AssertionAddedEvent {
        AssertionAddedEvent {
            block,
            log_index: 0,
            assertion_adopter: adopter,
            assertion_id: keccak256(bytecode),
            activation_block,
        }
    }

    // Load SimpleCounterAssertion deployment bytecode from test artifacts.
    fn counter_bytecode() -> Bytes {
        test_utils::bytecode(test_utils::SIMPLE_ASSERTION_COUNTER)
    }

    #[tokio::test]
    async fn test_indexer_creation() {
        let server = MockServer::start();
        let indexer = create_test_indexer(MockEventSource::new(), &server.base_url());
        assert_eq!(indexer.last_synced_block, 0);
    }

    #[tokio::test]
    async fn test_process_added_event() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0xAA);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let indexer = create_test_indexer(MockEventSource::new(), &server.base_url());

        let event = AssertionAddedEvent {
            block: 10,
            log_index: 0,
            assertion_adopter: adopter,
            assertion_id,
            activation_block: 15,
        };

        let result = indexer.process_added_event(&event).await.unwrap();
        let modification = result.expect("should produce Add modification");
        match modification {
            PendingModification::Add {
                assertion_adopter: aa,
                assertion_contract,
                activation_block,
                ..
            } => {
                assert_eq!(aa, adopter);
                assert_eq!(activation_block, 15);
                assert_eq!(assertion_contract.id, keccak256(&bc));
            }
            PendingModification::Remove { .. } => panic!("expected PendingModification::Add"),
        }
    }

    #[tokio::test]
    async fn test_process_added_event_with_constructor_args() {
        let bc = counter_bytecode();
        let constructor_args = [0u8; 32];
        // assertion_id = keccak(bytecode || constructor_args)
        let mut full_bytecode = bc.to_vec();
        full_bytecode.extend_from_slice(&constructor_args);
        let full_assertion_id = keccak256(&full_bytecode);

        let adopter = Address::repeat_byte(0xBB);
        let server = MockServer::start();
        mock_da_for_bytecode(&server, full_assertion_id, &bc, &constructor_args);

        let indexer = create_test_indexer(MockEventSource::new(), &server.base_url());

        let event = AssertionAddedEvent {
            block: 5,
            log_index: 0,
            assertion_adopter: adopter,
            assertion_id: full_assertion_id,
            activation_block: 10,
        };

        let result = indexer.process_added_event(&event).await.unwrap();
        let modification = result.expect("should produce Add modification");
        match &modification {
            PendingModification::Add {
                assertion_contract, ..
            } => {
                assert_eq!(assertion_contract.id, keccak256(&full_bytecode));
            }
            PendingModification::Remove { .. } => panic!("expected PendingModification::Add"),
        }
    }

    #[tokio::test]
    async fn test_process_removed_event() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0xCC);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 10)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();
        assert_eq!(indexer.store.get_assertions_for_contract(adopter).len(), 1);

        // cycle 2: remove the assertion
        indexer.event_source.set_events(
            vec![],
            vec![AssertionRemovedEvent {
                block: 20,
                log_index: 0,
                assertion_adopter: adopter,
                assertion_id,
                deactivation_block: 25,
            }],
        );
        indexer.event_source.set_head(20);
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].inactivation_block, Some(25));
    }

    #[tokio::test]
    async fn test_process_events_applied_to_store() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0xDD);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 12)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].activation_block, 12);
        assert_eq!(assertions[0].assertion_contract.id, keccak256(&bc));
    }

    #[tokio::test]
    async fn test_indexer_initial_sync() {
        let server = MockServer::start();
        let source = MockEventSource::new();
        source.set_head(0);

        let mut indexer = create_test_indexer(source, &server.base_url());
        assert_eq!(indexer.last_synced_block, 0);

        indexer.sync_once().await.unwrap();
        // head=0, no events => last_synced_block stays at 0
        assert_eq!(indexer.last_synced_block, 0);
    }

    #[tokio::test]
    async fn test_add_remove_across_cycles() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x01);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 10)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert!(assertions[0].inactivation_block.is_none());

        // cycle 2: remove the assertion
        indexer.event_source.set_events(
            vec![],
            vec![AssertionRemovedEvent {
                block: 20,
                log_index: 0,
                assertion_adopter: adopter,
                assertion_id,
                deactivation_block: 25,
            }],
        );
        indexer.event_source.set_head(20);
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].inactivation_block, Some(25));
    }

    #[tokio::test]
    async fn test_add_remove_same_cycle() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x02);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(
            vec![make_added_event(&bc, adopter, 10, 10)],
            vec![AssertionRemovedEvent {
                block: 10,
                log_index: 0,
                assertion_adopter: adopter,
                assertion_id,
                deactivation_block: 15,
            }],
        );
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].inactivation_block, Some(15));
    }

    #[tokio::test]
    async fn test_large_batch() {
        let bc = counter_bytecode();
        let batch_size = 200usize;

        let server = MockServer::start();
        let hex_bytecode = format!("0x{}", hex::encode(&bc));

        // One mock per request-id so DaClient's id-validation passes.
        // Match `"id":N}` with trailing `}` to avoid substring collisions (e.g. id:1 vs id:10).
        for req_id in 1..=batch_size {
            server.mock(|when, then| {
                when.method(httpmock::Method::POST)
                    .body_includes("da_get_assertion")
                    .body_includes(format!("\"id\":{req_id}}}"));
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "solidity_source": "",
                        "bytecode": hex_bytecode,
                        "prover_signature": "0x",
                        "encoded_constructor_args": "0x",
                        "constructor_abi_signature": "constructor()"
                    }
                }));
            });
        }

        // each adopter gets a unique address via the 2 low bytes
        let events: Vec<AssertionAddedEvent> = (0..batch_size)
            .map(|i| {
                let mut addr_bytes = [0u8; 20];
                addr_bytes[18] = u8::try_from(i >> 8).unwrap();
                addr_bytes[19] = u8::try_from(i & 0xFF).unwrap();
                make_added_event(&bc, Address::new(addr_bytes), 10, 10)
            })
            .collect();

        let source = MockEventSource::new();
        source.set_events(events, vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        for i in 0..batch_size {
            let mut addr_bytes = [0u8; 20];
            addr_bytes[18] = u8::try_from(i >> 8).unwrap();
            addr_bytes[19] = u8::try_from(i & 0xFF).unwrap();
            let assertions = indexer
                .store
                .get_assertions_for_contract(Address::new(addr_bytes));
            assert_eq!(assertions.len(), 1, "adopter {i} should have 1 assertion");
        }
    }

    #[tokio::test]
    async fn test_storage_is_persisted() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x03);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 10)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert!(!assertions[0].assertion_contract.deployed_code.is_empty());
    }

    #[tokio::test]
    async fn test_triggers_are_persisted() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x04);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 10)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert!(!assertions[0].trigger_recorder.triggers.is_empty());
    }

    #[tokio::test]
    async fn test_activation_block_is_expected() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x05);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 42)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].activation_block, 42);
        assert_eq!(assertions[0].inactivation_block, None);

        // cycle 2: remove, verify activation_block is preserved
        indexer.event_source.set_events(
            vec![],
            vec![AssertionRemovedEvent {
                block: 50,
                log_index: 0,
                assertion_adopter: adopter,
                assertion_id,
                deactivation_block: 99,
            }],
        );
        indexer.event_source.set_head(50);
        indexer.sync_once().await.unwrap();

        let assertions = indexer.store.get_assertions_for_contract(adopter);
        assert_eq!(assertions[0].activation_block, 42);
        assert_eq!(assertions[0].inactivation_block, Some(99));
    }

    #[tokio::test]
    async fn test_malformed_assertion_skipped() {
        // 0xDEADBEEF is not valid EVM bytecode
        let garbage = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let assertion_id = keccak256(&garbage);
        let adopter = Address::repeat_byte(0x06);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &garbage, &[]);

        let source = MockEventSource::new();
        source.set_events(
            vec![AssertionAddedEvent {
                block: 10,
                log_index: 0,
                assertion_adopter: adopter,
                assertion_id,
                activation_block: 10,
            }],
            vec![],
        );
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();

        // indexer skips malformed assertion but still advances
        assert!(
            indexer
                .store
                .get_assertions_for_contract(adopter)
                .is_empty()
        );
        assert_eq!(indexer.last_synced_block, 10);
    }

    #[tokio::test]
    async fn test_head_regression_skips_cycle() {
        let bc = counter_bytecode();
        let adopter = Address::repeat_byte(0x07);
        let assertion_id = keccak256(&bc);

        let server = MockServer::start();
        mock_da_for_bytecode(&server, assertion_id, &bc, &[]);

        let source = MockEventSource::new();
        source.set_events(vec![make_added_event(&bc, adopter, 10, 10)], vec![]);
        source.set_head(10);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();
        assert_eq!(indexer.last_synced_block, 10);

        // regress head from 10 to 5
        indexer.event_source.set_head(5);
        indexer.event_source.set_events(
            vec![make_added_event(&bc, Address::repeat_byte(0x08), 11, 11)],
            vec![],
        );
        indexer.sync_once().await.unwrap();

        // events were not processed, block didn't advance
        assert_eq!(indexer.last_synced_block, 10);
        assert!(
            indexer
                .store
                .get_assertions_for_contract(Address::repeat_byte(0x08))
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_empty_cycle_advances_head() {
        let server = MockServer::start();
        let source = MockEventSource::new();
        source.set_head(50);

        let mut indexer = create_test_indexer(source, &server.base_url());
        indexer.sync_once().await.unwrap();
        assert_eq!(indexer.last_synced_block, 50);
    }

    #[tokio::test]
    async fn test_indexer_unreachable_after_max_retries() {
        tokio::time::pause();

        let server = MockServer::start();
        let source = MockEventSource::new();
        source.set_head_error(true);

        let mut indexer = create_test_indexer(source, &server.base_url());
        let result = indexer.sync_once().await;

        assert!(
            matches!(result, Err(IndexerError::IndexerUnreachable)),
            "Expected IndexerUnreachable, got: {result:?}"
        );
    }
}
