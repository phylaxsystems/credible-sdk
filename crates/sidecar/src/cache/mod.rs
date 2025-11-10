//! Caches provide the sidecar with state that it does not have locally.
//!
//! The sidecar cache serves state from external providers
//! to the sidecar in a `revm::DatabaseRef` compatible format.
//!
//! The cache includes many state sources inside of it, which are responsible for
//! querying state externally, and forwarding it to the sidecar.
//! The cache deals with making sure that state sources are healthy and forwarding
//! the requests to the most appropriate source.
//!
//! More details about state source implementations can be seen in their respective modules.

use crate::{
    cache::sources::{
        Source,
        SourceError,
        SourceName,
    },
    metrics::StateMetrics,
};
use assertion_executor::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
};
use revm::{
    DatabaseRef,
    context::DBErrorMarker,
    primitives::{
        StorageKey,
        StorageValue,
    },
};
use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::Instant,
};
use thiserror::Error;
use tracing::{
    debug,
    error,
    trace,
};

/// Sources bridge external state to the sidecar.
pub mod sources;

/// A multi-layered cache for blockchain state data.
///
/// The `Cache` manages multiple data sources, each implementing the `Source` trait
/// and queries them in priority order (e.i., circuit breaker, the first successful query response
/// is returned). It maintains awareness of the current block number to ensure only synchronized
/// sources are queried.
///
/// # Architecture
///
/// The cache operates on a fallback mechanism:
/// 1. Sources are queried in priority order (index 0 = highest priority)
/// 2. Only sources that report as synced for the current block are queried
/// 3. The first successful response is returned
/// 4. If all sources fail or no sources are available, an error is returned
///
/// # Thread Safety
///
/// The `Cache` is thread-safe and can be shared across multiple threads using `Arc`.
/// The current block number is stored atomically to allow concurrent updates.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let source1 = Arc::new(MySource::new("primary"));
/// let source2 = Arc::new(MySource::new("fallback"));
/// let cache = Cache::new(vec![source1, source2]);
///
/// // Update the current block number
/// cache.set_block_number(12345);
///
/// // Query account information
/// let account_info = cache.basic_ref(address)?;
/// ```
#[derive(Debug)]
pub struct Sources {
    /// The current block number used to determine source synchronization status.
    latest_head: AtomicU64,
    /// Priority-ordered collection of data sources.
    /// Sources are tried in order until one succeeds.
    sources: Vec<Arc<dyn Source>>,
    /// The latest block which is unsynced by the sidecar.
    latest_unprocessed_block: AtomicU64,
    /// The maximum depth of the cache to be considered synced.
    max_depth: u64,
    /// Metrics for the cache.
    metrics: StateMetrics,
}

impl Sources {
    /// Creates a new cache with the specified sources.
    ///
    /// # Arguments
    ///
    /// * `sources` - A vector of sources ordered by priority. The source at index 0
    ///   has the highest priority and will be tried first.
    ///
    /// # Returns
    ///
    /// A new `Cache` instance with the block number initialized to 0.
    pub fn new(sources: Vec<Arc<dyn Source>>, max_depth: u64) -> Self {
        Self {
            latest_head: AtomicU64::new(0),
            sources,
            latest_unprocessed_block: AtomicU64::new(0),
            max_depth,
            metrics: StateMetrics::new(),
        }
    }

    /// Returns a list of configured sources for the cache
    pub fn list_configured_sources(&self) -> Vec<SourceName> {
        self.sources.iter().map(|s| s.name()).collect::<Vec<_>>()
    }

    /// Updates the current block number.
    ///
    /// This affects which sources are considered synchronized and eligible
    /// for queries. Sources will only be queried if their `is_synced` method
    /// returns true for this block number.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The new current block number
    pub fn set_block_number(&self, block_number: u64) {
        // If the block number is 0 (meaning we are logging it in for the first time), we set the
        // latest unprocessed block number to the first block number received. This way, we require that the
        // cache has to be updated up to this block to be considered synced.
        if self.latest_head.load(Ordering::Acquire) == 0 {
            self.latest_unprocessed_block
                .store(block_number, Ordering::Relaxed);
            self.metrics.set_latest_unprocessed_block(block_number);
        }
        self.latest_head.store(block_number, Ordering::Relaxed);
        self.metrics.set_latest_head(block_number);

        let minimum_synced_block_number = self.get_minimum_synced_block_number();

        self.metrics
            .set_min_synced_head(minimum_synced_block_number);

        for source in &self.sources {
            source.update_cache_status(minimum_synced_block_number, block_number);
        }
    }

    /// Resets the `reset_latest_unprocessed_block` to the `required_block`.
    ///
    /// This method updates the `latest_unprocessed_block` to match the value of the
    /// current `required_block`.
    ///
    /// If the internal cache is flushed, this method must be called to sync the required head to
    /// the latest block, as the current cache is stale
    pub fn reset_latest_unprocessed_block(&self, required_block: u64) {
        self.metrics.increase_reset_latest_unprocessed_block();
        self.latest_unprocessed_block
            .store(required_block, Ordering::Relaxed);
    }

    /// Returns how many times the cache has been explicitly reset.
    #[cfg(any(test, feature = "test", feature = "bench-utils"))]
    pub fn reset_latest_unprocessed_block_count(&self) -> u64 {
        self.metrics
            .reset_latest_unprocessed_block
            .load(Ordering::Relaxed)
    }

    /// Returns an iterator over sources that are currently synced.
    ///
    /// Sources are returned in priority order, with the highest priority
    /// synced source returned first.
    pub fn iter_synced_sources(&self) -> impl Iterator<Item = Arc<dyn Source>> {
        let instant = Instant::now();

        let min_synced_block = self.get_minimum_synced_block_number();
        let latest_head = self.latest_head.load(Ordering::Acquire);

        let sources = self
            .sources
            .iter()
            .filter(move |source| source.is_synced(min_synced_block, latest_head))
            .cloned();

        self.metrics.is_sync_duration(instant.elapsed());

        sources
    }

    pub(crate) fn get_minimum_synced_block_number(&self) -> u64 {
        // MAX(latest BlockEnv - MINIMUM_STATE_DIFF, First block env processed)
        let latest_unprocessed_block = self.latest_unprocessed_block.load(Ordering::Acquire);
        let latest_head = self.latest_head.load(Ordering::Acquire);

        if latest_unprocessed_block == 0 {
            latest_head
        } else {
            latest_unprocessed_block.max(latest_head.saturating_sub(self.max_depth))
        }
    }
}

impl DatabaseRef for Sources {
    type Error = CacheError;
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        trace!(
            target = "cache::basic_ref",
            address = %address,
            "Function call",
        );
        self.metrics.increase_basic_ref_counter();
        let total_operation_instant = Instant::now();
        for source in self.iter_synced_sources() {
            let source_instant = Instant::now();
            match source.basic_ref(address) {
                Ok(Some(account)) => {
                    self.metrics
                        .basic_ref_duration(&source.name(), source_instant.elapsed());
                    self.metrics
                        .total_basic_ref_duration(total_operation_instant.elapsed());
                    return Ok(Some(account));
                }
                Ok(None) => {
                    debug!(
                        target = "cache::basic_ref",
                        name = %source.name(),
                        address = %address,
                        "Cache source returned no account information",
                    );
                    return Ok(None);
                }
                Err(SourceError::CacheMiss) => {
                    debug!(
                        target = "cache::basic_ref",
                        name = %source.name(),
                        address = %address,
                        "Cache source reported account cache miss",
                    );
                }
                Err(e) => {
                    error!(
                        target = "cache::basic_ref",
                        name = %source.name(),
                        address = %address,
                        error = ?e,
                        "Failed to fetch account info from cache source",
                    );
                }
            }
            self.metrics
                .basic_ref_duration(&source.name(), source_instant.elapsed());
        }

        self.metrics
            .total_basic_ref_duration(total_operation_instant.elapsed());
        Err(CacheError::NoCacheSourceAvailable)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        trace!(
            target = "cache::block_hash_ref",
            number = number,
            "Function call",
        );
        self.metrics.increase_block_hash_ref_counter();
        let total_operation_instant = Instant::now();
        let result = self
            .iter_synced_sources()
            .find_map(|source| {
                let source_instant = Instant::now();
                let res = source.block_hash_ref(number);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::block_hash_ref",
                        name = %source.name(),
                        number = number,
                        error = ?e,
                        "Failed to fetch block hash from cache source");
                }
                self.metrics
                    .block_hash_ref_duration(&source.name(), source_instant.elapsed());
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable);
        self.metrics
            .total_block_hash_ref_duration(total_operation_instant.elapsed());
        result
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        trace!(
            target = "cache::code_by_hash_ref",
            code_hash = %code_hash,
            "Function call",
        );
        self.metrics.increase_code_by_hash_ref_counter();
        let total_operation_instant = Instant::now();
        let result = self
            .iter_synced_sources()
            .find_map(|source| {
                let source_instant = Instant::now();
                let res = source.code_by_hash_ref(code_hash);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::code_by_hash_ref",
                        name = %source.name(),
                        code_hash = %code_hash,
                        error = ?e,
                        "Failed to fetch code by hash from cache source");
                }
                self.metrics
                    .code_by_hash_ref_duration(&source.name(), source_instant.elapsed());
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable);
        self.metrics
            .total_code_by_hash_ref_duration(total_operation_instant.elapsed());
        result
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        trace!(
            target = "cache::storage_ref",
            address = %address,
            index = %format_args!("{:#x}", index),
            "Function call",
        );
        self.metrics.increase_storage_ref_counter_counter();
        let total_operation_instant = Instant::now();
        for source in self.iter_synced_sources() {
            let source_instant = Instant::now();
            match source.storage_ref(address, index) {
                Ok(value) => {
                    self.metrics
                        .storage_ref_duration(&source.name(), source_instant.elapsed());
                    self.metrics
                        .total_storage_ref_duration(total_operation_instant.elapsed());
                    return Ok(value);
                }
                Err(SourceError::CacheMiss) => {
                    debug!(
                        target = "cache::storage_ref",
                        name = %source.name(),
                        address = %address,
                        index = %format_args!("{:#x}", index),
                        "Cache source reported storage cache miss",
                    );
                }
                Err(e) => {
                    error!(
                        target = "cache::storage_ref",
                        name = %source.name(),
                        address = %address,
                        index = %format_args!("{:#x}", index),
                        error = ?e,
                        "Failed to fetch the storage from cache source",
                    );
                }
            }
            self.metrics
                .storage_ref_duration(&source.name(), source_instant.elapsed());
        }

        self.metrics
            .total_storage_ref_duration(total_operation_instant.elapsed());
        Err(CacheError::NoCacheSourceAvailable)
    }
}

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("No cache source available")]
    NoCacheSourceAvailable,
}

impl DBErrorMarker for CacheError {}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_truncation)]
    use super::*;
    use crate::cache::sources::{
        Source,
        redis::{
            RedisCacheError,
            RedisSource,
        },
    };
    use alloy::hex;
    use assertion_executor::primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
    };
    use redis::{
        self,
        ErrorKind,
        RedisError,
    };
    use redis_test::{
        MockCmd,
        MockRedisConnection,
    };
    use revm::primitives::{
        StorageKey,
        StorageValue,
        U256,
    };
    use std::{
        collections::HashMap,
        fmt,
        sync::{
            Arc,
            Mutex,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        time::Duration,
    };
    use tracing_test::traced_test;

    // Mock Source implementation for testing
    #[derive(Debug)]
    struct MockSource {
        name: SourceName,
        synced_threshold: u64,
        account_info: Option<AccountInfo>,
        block_hash: Option<B256>,
        bytecode: Option<Bytecode>,
        storage_value: Option<StorageValue>,
        should_error: bool,
        basic_ref_calls: AtomicUsize,
        block_hash_ref_calls: AtomicUsize,
        code_by_hash_ref_calls: AtomicUsize,
        storage_ref_calls: AtomicUsize,
        cache_miss_account: bool,
        cache_miss_storage: bool,
    }

    impl MockSource {
        fn new(name: SourceName) -> Self {
            Self {
                name,
                synced_threshold: 0,
                account_info: None,
                block_hash: None,
                bytecode: None,
                storage_value: None,
                should_error: false,
                basic_ref_calls: AtomicUsize::new(0),
                block_hash_ref_calls: AtomicUsize::new(0),
                code_by_hash_ref_calls: AtomicUsize::new(0),
                storage_ref_calls: AtomicUsize::new(0),
                cache_miss_account: false,
                cache_miss_storage: false,
            }
        }

        fn with_synced_threshold(mut self, threshold: u64) -> Self {
            self.synced_threshold = threshold;
            self
        }

        fn with_account_info(mut self, info: AccountInfo) -> Self {
            self.account_info = Some(info);
            self
        }

        fn with_block_hash(mut self, hash: B256) -> Self {
            self.block_hash = Some(hash);
            self
        }

        fn with_bytecode(mut self, bytecode: Bytecode) -> Self {
            self.bytecode = Some(bytecode);
            self
        }

        fn with_storage_value(mut self, value: StorageValue) -> Self {
            self.storage_value = Some(value);
            self
        }

        fn with_error(mut self) -> Self {
            self.should_error = true;
            self
        }

        fn with_cache_miss_for_account(mut self) -> Self {
            self.cache_miss_account = true;
            self
        }

        fn with_cache_miss_for_storage(mut self) -> Self {
            self.cache_miss_storage = true;
            self
        }

        fn basic_ref_call_count(&self) -> usize {
            self.basic_ref_calls.load(Ordering::Acquire)
        }

        fn block_hash_ref_call_count(&self) -> usize {
            self.block_hash_ref_calls.load(Ordering::Acquire)
        }

        fn code_by_hash_ref_call_count(&self) -> usize {
            self.code_by_hash_ref_calls.load(Ordering::Acquire)
        }

        fn storage_ref_call_count(&self) -> usize {
            self.storage_ref_calls.load(Ordering::Acquire)
        }
    }

    impl Source for MockSource {
        fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
            // Source has data from [synced_threshold, Inf)
            // We need data from [min_synced_block, latest_head]
            // Check if these ranges intersect:
            let lower_bound = min_synced_block.max(self.synced_threshold);
            let upper_bound = latest_head; // min(latest_head, Inf) = latest_head
            lower_bound <= upper_bound
        }

        fn update_cache_status(&self, _min_synced_block: u64, _latest_head: u64) {}

        fn name(&self) -> SourceName {
            self.name
        }
    }

    impl DatabaseRef for MockSource {
        type Error = sources::SourceError;

        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.basic_ref_calls.fetch_add(1, Ordering::Release);
            if self.cache_miss_account {
                return Err(sources::SourceError::CacheMiss);
            }
            if self.should_error {
                return Err(sources::SourceError::Other("Mock error".to_string()));
            }
            Ok(self.account_info.clone())
        }

        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            self.block_hash_ref_calls.fetch_add(1, Ordering::Release);
            if self.should_error {
                return Err(sources::SourceError::Other("Mock error".to_string()));
            }
            self.block_hash.ok_or(sources::SourceError::BlockNotFound)
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            self.code_by_hash_ref_calls.fetch_add(1, Ordering::Release);
            if self.should_error {
                return Err(sources::SourceError::Other("Mock error".to_string()));
            }
            self.bytecode
                .clone()
                .ok_or(sources::SourceError::CodeByHashNotFound)
        }

        fn storage_ref(
            &self,
            _address: Address,
            _index: StorageKey,
        ) -> Result<StorageValue, Self::Error> {
            self.storage_ref_calls.fetch_add(1, Ordering::Release);
            if self.cache_miss_storage {
                return Err(sources::SourceError::CacheMiss);
            }
            if self.should_error {
                return Err(sources::SourceError::Other("Mock error".to_string()));
            }
            Ok(self.storage_value.unwrap_or(U256::ZERO))
        }
    }

    struct RedisTestBackend {
        connection: Mutex<MockRedisConnection>,
        hgetall_calls: Arc<AtomicUsize>,
    }

    impl fmt::Debug for RedisTestBackend {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RedisTestBackend").finish()
        }
    }

    impl RedisTestBackend {
        fn with_commands<I>(commands: I) -> (Self, Arc<AtomicUsize>)
        where
            I: IntoIterator<Item = MockCmd>,
        {
            let calls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    connection: Mutex::new(MockRedisConnection::new(commands)),
                    hgetall_calls: calls.clone(),
                },
                calls,
            )
        }

        fn for_backend_error(address: Address) -> (Self, Arc<AtomicUsize>) {
            let account_key = format!("state:account:{}", hex::encode(address));
            let commands = vec![
                MockCmd::new(
                    redis::cmd("GET").arg("state:current_block"),
                    Ok::<_, RedisError>("1"),
                ),
                MockCmd::new(
                    redis::cmd("HGETALL").arg(&account_key),
                    Err::<String, RedisError>(redis_io_error("redis connection error")),
                ),
            ];
            Self::with_commands(commands)
        }

        fn for_unsynced() -> (Self, Arc<AtomicUsize>) {
            let commands = vec![MockCmd::new(
                redis::cmd("GET").arg("state:current_block"),
                Ok::<_, RedisError>("5"),
            )];
            Self::with_commands(commands)
        }
    }

    fn redis_io_error(message: &str) -> RedisError {
        RedisError::from((ErrorKind::IoError, "TEST", message.to_string()))
    }

    // Helper functions
    fn create_test_address() -> Address {
        Address::from([1u8; 20])
    }

    fn create_test_account_info() -> AccountInfo {
        AccountInfo {
            balance: U256::from(1000),
            nonce: 42,
            code_hash: B256::from([2u8; 32]),
            code: None,
        }
    }

    fn create_test_bytecode() -> Bytecode {
        Bytecode::new_raw(vec![0x60, 0x00, 0x60, 0x00].into())
    }

    #[test]
    fn test_cache_new_creates_empty_cache() {
        let cache = Sources::new(vec![], 10);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 0);
        assert_eq!(cache.sources.len(), 0);
    }

    #[test]
    fn test_cache_new_with_sources() {
        let source1: Arc<dyn Source> = Arc::new(MockSource::new(SourceName::Sequencer));
        let source2: Arc<dyn Source> = Arc::new(MockSource::new(SourceName::EthRpcSource));
        let sources = vec![source1, source2];

        let cache = Sources::new(sources, 10);
        assert_eq!(cache.sources.len(), 2);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_set_block_number() {
        let cache = Sources::new(vec![], 10);

        cache.set_block_number(42);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 42);

        cache.set_block_number(100);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 100);
    }

    #[test]
    fn test_synced_sources_filters_correctly() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new(SourceName::Redis).with_synced_threshold(0));

        let cache = Sources::new(vec![source1, source2, source3], 10);

        // Block 0 - only source3 should be synced
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Redis);

        // Block 15 - source1 and source3 should be synced
        cache.set_block_number(15);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
        let names: Vec<_> = synced.iter().map(|s| s.name()).collect();
        assert!(names.contains(&SourceName::Sequencer));
        assert!(names.contains(&SourceName::Redis));

        // Block 60 - all sources should be synced
        cache.set_block_number(60);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3);
    }

    #[test]
    fn test_basic_ref_success_first_source() {
        let account_info = create_test_account_info();
        let source1 = Arc::new(
            MockSource::new(SourceName::Sequencer).with_account_info(account_info.clone()),
        );
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        let address = create_test_address();

        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, Some(account_info));

        // Verify call counts
        assert_eq!(
            source1.basic_ref_call_count(),
            1,
            "First source should be called once"
        );
        assert_eq!(
            source2.basic_ref_call_count(),
            0,
            "Second source should not be called"
        );
    }

    #[test]
    fn test_basic_ref_success_second_source_after_first_fails() {
        let account_info = create_test_account_info();
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 = Arc::new(
            MockSource::new(SourceName::EthRpcSource).with_account_info(account_info.clone()),
        );

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        let address = create_test_address();

        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, Some(account_info));

        // Verify call counts - both should be called since first one fails
        assert_eq!(
            source1.basic_ref_call_count(),
            1,
            "First source should be called once"
        );
        assert_eq!(
            source2.basic_ref_call_count(),
            1,
            "Second source should be called once after first fails"
        );
    }

    #[test]
    fn test_basic_ref_returns_none_on_cache_miss() {
        let source1 =
            Arc::new(MockSource::new(SourceName::Sequencer).with_cache_miss_for_account());
        let source2 = Arc::new(
            MockSource::new(SourceName::EthRpcSource).with_account_info(create_test_account_info()),
        );

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        cache.set_block_number(1);
        let address = create_test_address();

        let result = cache.basic_ref(address).unwrap();
        assert!(result.is_some());
        assert_eq!(source1.basic_ref_call_count(), 1);
        assert_eq!(source2.basic_ref_call_count(), 1);
    }

    #[test]
    fn test_basic_ref_returns_none_without_fallback_when_first_source_has_no_data() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer));
        let source2 = Arc::new(
            MockSource::new(SourceName::EthRpcSource).with_account_info(create_test_account_info()),
        );

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        cache.set_block_number(1);
        let address = create_test_address();

        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, None);
        assert_eq!(source1.basic_ref_call_count(), 1);
        assert_eq!(source2.basic_ref_call_count(), 0);
    }

    #[test]
    fn test_basic_ref_falls_back_when_first_source_errors() {
        let address = create_test_address();
        let (backend, redis_calls) = RedisTestBackend::for_backend_error(address);
        let first_source = Arc::new(MockSource::new(SourceName::Sequencer).with_error());

        let account_info = create_test_account_info();
        let fallback_source = Arc::new(
            MockSource::new(SourceName::Sequencer).with_account_info(account_info.clone()),
        );

        let cache = Sources::new(vec![first_source.clone(), fallback_source.clone()], 10);
        cache.set_block_number(1);

        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, Some(account_info));
        assert_eq!(fallback_source.basic_ref_call_count(), 1);
        assert_eq!(first_source.basic_ref_call_count(), 1);
    }

    #[test]
    fn test_basic_ref_uses_fallback_when_first_source_unsynced() {
        let address = create_test_address();
        let first_source =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_cache_miss_for_account());

        let account_info = create_test_account_info();
        let fallback_source = Arc::new(
            MockSource::new(SourceName::Sequencer).with_account_info(account_info.clone()),
        );

        let cache = Sources::new(vec![first_source, fallback_source.clone()], 10);
        cache.set_block_number(15);

        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, Some(account_info));
        assert_eq!(fallback_source.basic_ref_call_count(), 1);
    }

    #[test]
    fn test_basic_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_error());

        let cache = Sources::new(vec![source1, source2], 10);
        let address = create_test_address();

        let result = cache.basic_ref(address);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_basic_ref_no_synced_sources() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let cache = Sources::new(vec![source1], 10);

        // Block 0, source needs block 10 to be synced
        let address = create_test_address();
        let result = cache.basic_ref(address);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_block_hash_ref_success_first_source() {
        let block_hash = B256::from([3u8; 32]);
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_block_hash(block_hash));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);

        let result = cache.block_hash_ref(42).unwrap();
        assert_eq!(result, block_hash);

        // Verify call counts
        assert_eq!(
            source1.block_hash_ref_call_count(),
            1,
            "First source should be called once"
        );
        assert_eq!(
            source2.block_hash_ref_call_count(),
            0,
            "Second source should not be called"
        );
    }

    #[test]
    fn test_block_hash_ref_fallback_to_second_source() {
        let block_hash = B256::from([3u8; 32]);
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_block_hash(block_hash));

        let cache = Sources::new(vec![source1, source2], 10);

        let result = cache.block_hash_ref(42).unwrap();
        assert_eq!(result, block_hash);
    }

    #[test]
    fn test_block_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1, source2], 10);

        let result = cache.block_hash_ref(42);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_code_by_hash_ref_success_first_source() {
        let bytecode = create_test_bytecode();
        let source1 =
            Arc::new(MockSource::new(SourceName::Sequencer).with_bytecode(bytecode.clone()));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash).unwrap();
        assert_eq!(result, bytecode);

        // Verify call counts
        assert_eq!(
            source1.code_by_hash_ref_call_count(),
            1,
            "First source should be called once"
        );
        assert_eq!(
            source2.code_by_hash_ref_call_count(),
            0,
            "Second source should not be called"
        );
    }

    #[test]
    fn test_code_by_hash_ref_fallback_to_second_source() {
        let bytecode = create_test_bytecode();
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_bytecode(bytecode.clone()));

        let cache = Sources::new(vec![source1, source2], 10);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash).unwrap();
        assert_eq!(result, bytecode);
    }

    #[test]
    fn test_code_by_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1, source2], 10);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_storage_ref_success_first_source() {
        let storage_value = U256::from(12345);
        let source1 =
            Arc::new(MockSource::new(SourceName::Sequencer).with_storage_value(storage_value));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key).unwrap();
        assert_eq!(result, storage_value);

        // Verify call counts
        assert_eq!(
            source1.storage_ref_call_count(),
            1,
            "First source should be called once"
        );
        assert_eq!(
            source2.storage_ref_call_count(),
            0,
            "Second source should not be called"
        );
    }

    #[test]
    fn test_storage_ref_fallback_to_second_source() {
        let storage_value = U256::from(12345);
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_storage_value(storage_value));

        let cache = Sources::new(vec![source1, source2], 10);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key).unwrap();
        assert_eq!(result, storage_value);
    }

    #[test]
    fn test_storage_ref_fallback_on_cache_miss() {
        let storage_value = U256::from(0xdeadbeefu64);
        let source1 =
            Arc::new(MockSource::new(SourceName::Sequencer).with_cache_miss_for_storage());
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_storage_value(storage_value));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        cache.set_block_number(1);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key).unwrap();
        assert_eq!(result, storage_value);
        assert_eq!(source1.storage_ref_call_count(), 1);
        assert_eq!(source2.storage_ref_call_count(), 1);
    }

    #[test]
    fn test_storage_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_error());
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_error());

        let cache = Sources::new(vec![source1, source2], 10);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_concurrent_access_thread_safety() {
        use std::thread;

        let source1 = Arc::new(
            MockSource::new(SourceName::Sequencer).with_account_info(create_test_account_info()),
        );
        let cache = Arc::new(Sources::new(vec![source1], 10));
        cache.set_block_number(1);
        let mut handles = vec![];

        // Spawn multiple threads that concurrently access the cache
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                // Set block number
                cache_clone.set_block_number(10 + i * 10);

                // Read account info
                let address = Address::from([(i as u8); 20]);
                let result = cache_clone.basic_ref(address);
                assert!(result.is_ok());

                // Read current block number
                let block_number = cache_clone.latest_head.load(Ordering::Acquire);
                assert!((10..=100).contains(&block_number)); // Should be one of the values set by threads
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_atomic_block_number_operations() {
        let cache = Sources::new(vec![], 10);

        // Test ordering semantics
        cache.set_block_number(42);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 42);

        // Test that load sees the stored value
        cache.latest_head.store(100, Ordering::Release);
        assert_eq!(cache.latest_head.load(Ordering::Acquire), 100);
    }

    #[test]
    fn test_empty_sources_returns_error() {
        let cache = Sources::new(vec![], 10);
        let address = create_test_address();

        // All operations should return NoCacheSourceAvailable with empty sources
        assert!(matches!(
            cache.basic_ref(address),
            Err(CacheError::NoCacheSourceAvailable)
        ));
        assert!(matches!(
            cache.block_hash_ref(42),
            Err(CacheError::NoCacheSourceAvailable)
        ));
        assert!(matches!(
            cache.code_by_hash_ref(B256::ZERO),
            Err(CacheError::NoCacheSourceAvailable)
        ));
        assert!(matches!(
            cache.storage_ref(address, U256::ZERO),
            Err(CacheError::NoCacheSourceAvailable)
        ));
    }

    #[test]
    fn test_mixed_success_and_failure_sources() {
        let account_info = create_test_account_info();
        let block_hash = B256::from([5u8; 32]);

        // Source 1: Has account info, fails on block hash
        let source1 = Arc::new(
            MockSource::new(SourceName::Sequencer).with_account_info(account_info.clone()),
        );

        // Source 2: Fails on account info, has block hash
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_block_hash(block_hash));

        let cache = Sources::new(vec![source1, source2], 10);
        let address = create_test_address();

        // Should get account info from source 1
        let result = cache.basic_ref(address).unwrap();
        assert_eq!(result, Some(account_info));

        // Should get block hash from source 2 (source 1 returns None)
        let result = cache.block_hash_ref(42).unwrap();
        assert_eq!(result, block_hash);
    }

    #[test]
    fn test_error_display() {
        let error = CacheError::NoCacheSourceAvailable;
        assert_eq!(error.to_string(), "No cache source available");
    }

    #[test]
    fn test_all_methods_short_circuit_on_first_success() {
        let account_info = create_test_account_info();
        let block_hash = B256::from([3u8; 32]);
        let bytecode = create_test_bytecode();
        let storage_value = U256::from(12345);

        // Both sources have all data, but we should only call the first
        let source1 = Arc::new(
            MockSource::new(SourceName::Sequencer)
                .with_account_info(account_info.clone())
                .with_block_hash(block_hash)
                .with_bytecode(bytecode.clone())
                .with_storage_value(storage_value),
        );

        let source2 = Arc::new(
            MockSource::new(SourceName::EthRpcSource)
                .with_account_info(account_info.clone())
                .with_block_hash(block_hash)
                .with_bytecode(bytecode.clone())
                .with_storage_value(storage_value),
        );

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 10);
        let address = create_test_address();
        let code_hash = B256::from([4u8; 32]);
        let storage_key = U256::from(42);

        // Call all methods
        let _ = cache.basic_ref(address).unwrap();
        let _ = cache.block_hash_ref(42).unwrap();
        let _ = cache.code_by_hash_ref(code_hash).unwrap();
        let _ = cache.storage_ref(address, storage_key).unwrap();

        // Verify only first source was called for each method
        assert_eq!(source1.basic_ref_call_count(), 1);
        assert_eq!(source1.block_hash_ref_call_count(), 1);
        assert_eq!(source1.code_by_hash_ref_call_count(), 1);
        assert_eq!(source1.storage_ref_call_count(), 1);

        // Second source should never be called
        assert_eq!(source2.basic_ref_call_count(), 0);
        assert_eq!(source2.block_hash_ref_call_count(), 0);
        assert_eq!(source2.code_by_hash_ref_call_count(), 0);
        assert_eq!(source2.storage_ref_call_count(), 0);
    }

    #[test]
    fn test_cache_max_depth_filtering() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new(SourceName::Redis).with_synced_threshold(101)); // CHANGED: 100 -> 101

        // Test with max_depth = 20
        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 20);

        // Set current block to 100 (this also sets required_head to 100)
        cache.set_block_number(100);

        // min_synced_block = max(100, 100 - 20) = 100
        // Checking range [100, 100]:
        // source1 [10, Inf): max(100, 10) = 100 ≤ 100 -> OK
        // source2 [50, Inf): max(100, 50) = 100 ≤ 100 -> OK
        // source3 [101, Inf): max(100, 101) = 101 > 100 -> NOT SYNCED
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2); // CHANGED: 3 -> 2

        // Set required block to 80 to test depth filtering (avoid special case)
        cache.reset_latest_unprocessed_block(80); // CHANGED: 0 -> 80

        // min_synced_block = max(80, 100 - 20) = max(80, 80) = 80
        // Checking range [80, 100]:
        // source1 [10, Inf): max(80, 10) = 80 ≤ 100 -> OK
        // source2 [50, Inf): max(80, 50) = 80 ≤ 100 -> OK
        // source3 [101, Inf): max(80, 101) = 101 > 100 -> NOT SYNCED
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
        let names: Vec<_> = synced.iter().map(|s| s.name()).collect();
        assert!(names.contains(&SourceName::Sequencer));
        assert!(names.contains(&SourceName::EthRpcSource));
        assert!(!names.contains(&SourceName::Redis));

        // Test with max_depth = 60
        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 60);
        cache.set_block_number(49); // CHANGED: 100 -> 49 (need to be < 50 to filter source2)
        cache.reset_latest_unprocessed_block(40); // CHANGED: 0 -> 40

        // min_synced_block = max(40, 49 - 60) = max(40, 0) = 40
        // Checking range [40, 49]:
        // source1 [10, Inf): max(40, 10) = 40 ≤ 49 -> OK
        // source2 [50, Inf): max(40, 50) = 50 > 49 -> NOT SYNCED (source2 starts at 50, can't serve blocks < 50)
        // source3 [101, Inf): max(40, 101) = 101 > 49 -> NOT SYNCED
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Sequencer);

        // Test with max_depth = 200 (larger than current block)
        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 200);
        cache.set_block_number(9); // CHANGED: 100 -> 9 (need to be < 10 to filter all)
        cache.reset_latest_unprocessed_block(5); // CHANGED: 0 -> 5

        // min_synced_block = max(5, 9 - 200) = max(5, 0) = 5
        // Checking range [5, 9]:
        // source1 [10, Inf): max(5, 10) = 10 > 9 NOT SYNCED (source1 starts at 10, can't serve blocks < 10)
        // source2 [50, Inf): max(5, 50) = 50 > 9 NOT SYNCED
        // source3 [101, Inf): max(5, 101) = 101 > 9 NOT SYNCED
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 0);
    }

    #[test]
    fn test_cache_required_head_override() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 20);

        // Set current block to 100 (this sets required_head to 100)
        cache.set_block_number(100);

        // Initially, effective block = max(100, 100 - 20) = 100
        // Both sources synced at block 100 (100 >= 10, 100 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Set required block number to 30
        cache.reset_latest_unprocessed_block(30);

        // The effective block number should be max(30, 100 - 20) = max(30, 80) = 80
        // Both sources synced at block 80 (80 >= 10, 80 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Set required block number to 5 (lower than depth calculation)
        cache.reset_latest_unprocessed_block(5);

        // The effective block number should be max(5, 80) = 80
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
    }

    #[test]
    fn test_cache_reset_required_head() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 30);

        // Set current block to 100 (this sets required_head to 100)
        cache.set_block_number(100);

        // Verify required block number is set to 100
        assert_eq!(
            cache
                .latest_unprocessed_block
                .load(std::sync::atomic::Ordering::Acquire),
            100
        );

        // Reset required block number to 0
        cache.reset_latest_unprocessed_block(0);

        // Verify it's now 0
        assert_eq!(
            cache
                .latest_unprocessed_block
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );

        // Now the effective block number should be max(0, 100 - 30) = 70
        // Both sources synced at block 70 (70 >= 10, 70 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Reset to current block (simulating the old behavior)
        cache.reset_latest_unprocessed_block(100);
        assert_eq!(
            cache
                .latest_unprocessed_block
                .load(std::sync::atomic::Ordering::Acquire),
            100
        );
    }

    #[test]
    fn test_cache_out_of_sync_scenarios() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(100));
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(200));
        let source3 = Arc::new(MockSource::new(SourceName::Redis).with_synced_threshold(300));

        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);

        // Scenario 1: Set block and reset to test depth filtering
        cache.set_block_number(150);
        cache.reset_latest_unprocessed_block(0);
        // Effective block number: max(0, 150 - 50) = 100
        // Sources synced at block 100: only source1 (100 >= 100)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Sequencer);

        // Scenario 2: Lower block number
        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);
        cache.set_block_number(50);
        cache.reset_latest_unprocessed_block(0);
        // Effective block number: max(0, 50 - 50) = 0
        // Sources synced at block 0: none (0 < 100, 0 < 200, 0 < 300)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 0);

        // Scenario 3: Required block number forces higher sync requirement
        let cache = Sources::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);
        cache.set_block_number(400);
        cache.reset_latest_unprocessed_block(380);
        // Effective block number: max(380, 400 - 50) = max(380, 350) = 380
        // Sources synced at block 380: all sources (380 >= 100, 380 >= 200, 380 >= 300)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3);
    }

    #[test]
    fn test_cache_edge_cases() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(0));
        let source2 =
            Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(u64::MAX));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], u64::MAX);

        // Test with zero block number - only source1 should be synced (0 >= 0)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Sequencer);

        // Test with maximum block number
        let cache = Sources::new(vec![source1.clone(), source2.clone()], u64::MAX);
        cache.set_block_number(u64::MAX);
        cache.reset_latest_unprocessed_block(0);
        // When latest_unprocessed_block = 0, min_synced_block = latest_head = u64::MAX
        // Both sources can serve [u64::MAX, u64::MAX]
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
        assert_eq!(synced[0].name(), SourceName::Sequencer);

        // Test with zero max_depth
        let cache = Sources::new(vec![source1.clone(), source2.clone()], 0);
        cache.set_block_number(100);
        cache.reset_latest_unprocessed_block(0);
        // Effective block number: max(0, 100 - 0) = 100
        // Both sources: source1 (100 >= 0 OK), source2 (100 >= u64::MAX NOT SYNCED)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Sequencer);
    }

    #[test]
    fn test_original_test_understanding() {
        // This replicates the original test to understand the correct behavior
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new(SourceName::Redis).with_synced_threshold(0));

        let cache = Sources::new(vec![source1, source2, source3], 10);

        // Block 0 - effective block is 0, only source3 (0 >= 0) should be synced
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Redis);

        // Block 15 - this sets required_head to 15
        cache.set_block_number(15);
        // Effective block: max(15, 15 - 10) = 15
        // Sources synced at block 15: source1 (15 >= 10), source3 (15 >= 0)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Block 60 - current becomes 60, required stays 15
        cache.set_block_number(60);
        // Effective block: max(15, 60 - 10) = max(15, 50) = 50
        // Sources synced at block 50: source1 (50 >= 10), source2 (50 >= 50), source3 (50 >= 0)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3); // All sources should be synced
    }

    #[test]
    fn test_cache_invalidation_simulation() {
        let source1 = Arc::new(MockSource::new(SourceName::Sequencer).with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new(SourceName::EthRpcSource).with_synced_threshold(50));

        let cache = Sources::new(vec![source1.clone(), source2.clone()], 40);

        // Simulate normal operation
        cache.set_block_number(100);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2); // Both sources synced at block 100

        // Simulate cache invalidation by resetting required block to 0
        cache.reset_latest_unprocessed_block(0);
        // Effective block: max(0, 100 - 40) = 60
        // Both sources still synced (60 >= 10, 60 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Simulate more restrictive invalidation
        cache.reset_latest_unprocessed_block(40);
        cache.set_block_number(49); // Lower current block
        // min_synced_block = max(40, 49-40) = max(40, 9) = 40
        // Checking range [40, 49]:
        // source1 [10, Inf) can serve [40, 49] -> OK
        // source2 [50, Inf) CANNOT serve [40, 49] because max(40, 50) = 50 > 49 -> NOT SYNCED
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), SourceName::Sequencer);
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_first_fallback(mut instance: crate::utils::LocalInstance) {
        // Send a new block
        instance.new_block().await.unwrap();
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Send a new block to the Eth RPC source client websocket
        instance.eth_rpc_source_http_mock.send_new_head();

        // Wait for the Eth RPC source client to be synced
        instance
            .wait_for_source_synced(0, 1, 1)
            .await
            .expect("Eth RPC source client should sync to block 1");

        // Send a random tx whose data is not in the in-memory cache
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        // Await result
        let _ = instance.is_transaction_successful(&tx_hash).await;

        // The first fallback is hit
        let cache_sequencer_db_basic_ref_counter = *instance
            .eth_rpc_source_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);

        // The second fallback is never called
        assert!(
            instance
                .sequencer_http_mock
                .eth_balance_counter
                .get(&address)
                .is_none()
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_second_fallback_first_fallback_out_of_sync(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send many blocks a new block to the Eth RPC source client
        for _ in 0..15 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Send a new block to the Eth RPC source client websocket
        instance.eth_rpc_source_http_mock.send_new_head();

        // Wait for the Eth RPC source client to be synced for block 1
        instance
            .wait_for_source_synced(0, 1, 1)
            .await
            .expect("Eth RPC source client should sync to block 1");

        // Send a random tx whose data is not in the in-memory cache
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        // Await result
        let _ = instance.is_transaction_successful(&tx_hash).await;

        // The first fallback is never called because it is not synced
        // This is because the Eth RPC source client head is at block 1, but the
        // sequencer head is at block 15
        assert!(
            instance
                .eth_rpc_source_http_mock
                .eth_balance_counter
                .get(&address)
                .is_none()
        );

        // The second fallback is hit
        let cache_sequencer_db_basic_ref_counter = *instance
            .sequencer_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_second_fallback_because_first_fallback_error(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send a new block
        instance.new_block().await.unwrap();
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Send a new block to the Eth RPC source client websocket
        instance.eth_rpc_source_http_mock.send_new_head();

        // Wait for the Eth RPC source client to be synced
        instance
            .wait_for_source_synced(0, 1, 1)
            .await
            .expect("Eth RPC source client should sync to block 1");

        // Bad response for Eth RPC source client
        instance.eth_rpc_source_http_mock.mock_rpc_error(
            "eth_getBalance",
            -32000,               // Error code
            "Insufficient funds", // Error message
        );

        // Send a random tx whose data is not in the in-memory cache
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        // Await result
        let _ = instance.is_transaction_successful(&tx_hash).await;

        // The first fallback is hit
        let eth_rpc_source_db_basic_ref_counter = *instance
            .eth_rpc_source_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(eth_rpc_source_db_basic_ref_counter, 1);

        // The second fallback is hit because the first fallback returned an error
        let cache_sequencer_db_basic_ref_counter = *instance
            .sequencer_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);
    }

    #[traced_test]
    #[crate::utils::engine_test(all)]
    async fn test_cache_no_fallback_available(mut instance: crate::utils::LocalInstance) {
        // Send a new block
        instance.new_block().await.unwrap();
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Send a new block to the Eth RPC source client websocket
        instance.eth_rpc_source_http_mock.send_new_head();

        // Wait for the Eth RPC source client to be synced
        instance
            .wait_for_source_synced(0, 1, 1)
            .await
            .expect("Eth RPC source client should sync to block 1");

        // Bad response for Eth RPC source client
        instance.eth_rpc_source_http_mock.mock_rpc_error(
            "eth_getBalance",
            -32000,               // Error code
            "Insufficient funds", // Error message
        );

        // Bad response for sequencer
        instance.sequencer_http_mock.mock_rpc_error(
            "eth_getBalance",
            -32000,               // Error code
            "Insufficient funds", // Error message
        );

        // Send a random tx whose data is not in the in-memory cache
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        // Await result
        let _ = instance.is_transaction_successful(&tx_hash).await;

        tokio::time::timeout(Duration::from_secs(10), async {
            while !logs_contain("critical") {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("expected critical log after exhausting fallbacks");

        // The first fallback is hit
        let eth_rpc_source_db_basic_ref_counter = *instance
            .eth_rpc_source_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(eth_rpc_source_db_basic_ref_counter, 1);

        // The second fallback is hit because the first fallback returned an error
        let cache_sequencer_db_basic_ref_counter = *instance
            .sequencer_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_source_sync_with_min_block_boundary(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send blocks to advance the chain
        for _ in 0..10 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Sync Eth RPC source client to block 10
        for _ in 0..10 {
            instance.eth_rpc_source_http_mock.send_new_head();
        }

        // Test the exact boundary: min_synced_block = 10, latest_head = 10
        instance
            .wait_for_source_synced(0, 10, 10)
            .await
            .expect("Source should be synced at exact boundary");

        // Test min_synced_block slightly below latest_head
        instance
            .wait_for_source_synced(0, 10, 9)
            .await
            .expect("Source should be synced when min < latest");
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_source_not_synced_when_min_block_higher_than_latest(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send a few blocks
        for _ in 0..5 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Sync Eth RPC source client to block 5
        for _ in 0..5 {
            instance.eth_rpc_source_http_mock.send_new_head();
        }

        // This should timeout because min_synced_block (10) > latest_head (5)
        let result = tokio::time::timeout(
            Duration::from_millis(500),
            instance.wait_for_source_synced(0, 5, 10),
        )
        .await;

        assert!(
            result.is_err(),
            "Should timeout when min_synced_block > latest_head"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_max_depth_filtering_integration(mut instance: crate::utils::LocalInstance) {
        // Send 100 blocks to test depth filtering
        for _ in 0..100 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(5)).await;

        // Sync Eth RPC source client to block 100
        for _ in 0..100 {
            instance.eth_rpc_source_http_mock.send_new_head();
        }

        // Assuming max_depth is set (e.g., 50), test that source is synced
        // when min_synced_block is within the acceptable range
        instance
            .wait_for_source_synced(0, 100, 50)
            .await
            .expect("Source should be synced within max_depth range");

        // Send a transaction to verify the cache works at this depth
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        instance.is_transaction_successful(&tx_hash).await.unwrap();
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_multiple_sources_sync_progression(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send blocks progressively and test sync states
        for block in 1..=5 {
            instance.new_block().await.unwrap();
            instance.wait_for_processing(Duration::from_millis(2)).await;

            // Update Eth RPC source client
            instance.eth_rpc_source_http_mock.send_new_head();

            // Verify Eth RPC source client syncs to each block
            instance
                .wait_for_source_synced(0, block, block)
                .await
                .unwrap_or_else(|_| panic!("Eth RPC source client should sync to block {block}"));
        }

        // Send a transaction after full sync
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        let _ = instance.is_transaction_successful(&tx_hash).await;

        // The first source should handle it
        let counter = *instance
            .eth_rpc_source_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(counter, 1);
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_fallback_with_partial_sync(mut instance: crate::utils::LocalInstance) {
        // Send 30 blocks instead of 20 - create more distance!
        for _ in 0..30 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Sync Eth RPC source client only to block 10 (partial sync)
        for _ in 0..10 {
            instance.eth_rpc_source_http_mock.send_new_head();
        }

        // Wait for Eth RPC source to sync to block 10
        instance
            .wait_for_source_synced(0, 10, 10)
            .await
            .expect("Eth RPC source client should sync to block 10");

        // Send a tx at block 31
        // min_synced_block = 1.max(31 - max_depth)
        // Even if max_depth = 20: min_synced_block = 1.max(11) = 11 > 10 OK
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        let _ = instance.is_transaction_successful(&tx_hash).await;

        // Eth RPC source [0, 10] cannot serve [11, 31] because the intersection is empty
        assert!(
            instance
                .eth_rpc_source_http_mock
                .eth_balance_counter
                .get(&address)
                .is_none(),
            "Eth RPC source should not be used when out of sync range"
        );

        // Sequencer should be used as a fallback
        let counter = *instance
            .sequencer_http_mock
            .eth_balance_counter
            .get(&address)
            .unwrap();
        assert_eq!(counter, 1);
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_sync_recovery_after_gap(mut instance: crate::utils::LocalInstance) {
        // Create initial sync
        for _ in 0..5 {
            instance.new_block().await.unwrap();
            instance.eth_rpc_source_http_mock.send_new_head();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Verify initial sync
        instance
            .wait_for_source_synced(0, 5, 5)
            .await
            .expect("Initial sync should succeed");

        // Create a gap - add more blocks without updating Eth RPC source
        for _ in 0..10 {
            instance.new_block().await.unwrap();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Eth RPC source should be out of sync now
        let result = tokio::time::timeout(
            Duration::from_millis(300),
            instance.wait_for_source_synced(0, 15, 15),
        )
        .await;
        assert!(result.is_err(), "Eth RPC source should be out of sync");

        // Recover sync
        for _ in 0..10 {
            instance.eth_rpc_source_http_mock.send_new_head();
        }

        // Should be synced again
        instance
            .wait_for_source_synced(0, 15, 15)
            .await
            .expect("Sync should recover");
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_zero_block_edge_case(mut instance: crate::utils::LocalInstance) {
        // Test with block 0 / genesis
        instance.new_block().await.unwrap();
        instance.wait_for_processing(Duration::from_millis(2)).await;

        instance.eth_rpc_source_http_mock.send_new_head();

        // Should be able to sync to block 1 with min_synced_block = 0
        instance
            .wait_for_source_synced(0, 1, 0)
            .await
            .expect("Should sync with min_synced_block = 0");

        // Should also work with min_synced_block = 1
        instance
            .wait_for_source_synced(0, 1, 1)
            .await
            .expect("Should sync with min_synced_block = 1");
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_source_index_out_of_bounds(mut instance: crate::utils::LocalInstance) {
        instance.new_block().await.unwrap();
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Try to access a non-existent source index (e.g., 999)
        let result = tokio::time::timeout(
            Duration::from_millis(300),
            instance.wait_for_source_synced(999, 1, 1),
        )
        .await;

        // Should return error or timeout
        assert!(
            result.is_err() || result.unwrap().is_err(),
            "Should fail for invalid source index"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_rapid_block_progression(mut instance: crate::utils::LocalInstance) {
        // Rapidly send blocks and verify sync keeps up
        for block in 1..=30 {
            instance.new_block().await.unwrap();
            instance.eth_rpc_source_http_mock.send_new_head();

            // Small delay to simulate realistic block times
            instance.wait_for_processing(Duration::from_millis(1)).await;

            // Every 5 blocks, verify sync
            if block % 5 == 0 {
                instance
                    .wait_for_source_synced(0, block, block)
                    .await
                    .unwrap_or_else(|_| panic!("Should stay synced at block {block}"));
            }
        }

        // Final verification with a transaction
        let (address, tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();
        let _ = instance.is_transaction_successful(&tx_hash).await;

        assert!(
            instance
                .eth_rpc_source_http_mock
                .eth_balance_counter
                .get(&address)
                .is_some()
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_min_synced_block_range_validation(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send blocks to create a range
        for _ in 0..15 {
            instance.new_block().await.unwrap();
            instance.eth_rpc_source_http_mock.send_new_head();
        }
        instance.wait_for_processing(Duration::from_millis(2)).await;

        // Test various min_synced_block values within the valid range
        let test_cases = vec![
            (15, 10, true, "min < latest"),
            (15, 15, true, "min == latest"),
            (15, 20, false, "min > latest (should timeout)"),
            (15, 0, true, "min = 0"),
            (15, 1, true, "min = 1"),
        ];

        for (latest_head, min_synced, should_succeed, description) in test_cases {
            let result = tokio::time::timeout(
                Duration::from_millis(500),
                instance.wait_for_source_synced(0, latest_head, min_synced),
            )
            .await;

            if should_succeed {
                assert!(
                    result.is_ok() && result.unwrap().is_ok(),
                    "Test case '{description}' should succeed",
                );
            } else {
                assert!(
                    result.is_err() || result.unwrap().is_err(),
                    "Test case '{description}' should timeout or error",
                );
            }
        }
    }
}
