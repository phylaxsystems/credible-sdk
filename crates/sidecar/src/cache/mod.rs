use crate::cache::sources::Source;
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
};
use thiserror::Error;
use tracing::error;

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
/// ```rust
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
pub struct Cache {
    /// The current block number used to determine source synchronization status.
    current_block_number: AtomicU64,
    /// Priority-ordered collection of data sources.
    /// Sources are tried in order until one succeeds.
    sources: Vec<Arc<dyn Source>>,
    /// The required block number for the cache to be considered synced.
    required_block_number: AtomicU64,
    /// The maximum depth of the cache to be considered synced.
    max_depth: u64,
}

impl Cache {
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
            current_block_number: AtomicU64::new(0),
            sources,
            required_block_number: AtomicU64::new(0),
            max_depth,
        }
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
        // required block number to the first block number received. This way, we require that the
        // cache has to be updated up to this block to be considered synced.
        if self.current_block_number.load(Ordering::Acquire) == 0 {
            self.required_block_number
                .store(block_number, Ordering::Relaxed);
        }
        self.current_block_number
            .store(block_number, Ordering::Relaxed);
    }

    /// Resets the `required_block_number` to the current `block_number`.
    ///
    /// This method updates the `required_block_number` to match the value of the
    /// current `block_number`.
    ///
    /// If the internal cache is flushed, this method must be called to sync the required head to
    /// the latest block, as the current cache is stale
    pub fn reset_required_block_number(&self, required_block_number: u64) {
        self.required_block_number
            .store(required_block_number, Ordering::Relaxed);
    }

    /// Returns an iterator over sources that are currently synced.
    ///
    /// Sources are returned in priority order, with the highest priority
    /// synced source returned first.
    fn iter_synced_sources(&self) -> impl Iterator<Item = Arc<dyn Source>> {
        // MAX(latest BlockEnv - MINIMUM_STATE_DIFF, First block env processed)
        let required_block_number = self.required_block_number.load(Ordering::Acquire);
        let current_block_number = self.current_block_number.load(Ordering::Acquire);
        let block_number =
            required_block_number.max(current_block_number.saturating_sub(self.max_depth));

        self.sources
            .iter()
            .filter(move |source| source.is_synced(block_number))
            .cloned()
    }
}

impl DatabaseRef for Cache {
    type Error = CacheError;
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.iter_synced_sources()
            .find_map(|source| {
                let res = source.basic_ref(address);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::basic_ref",
                        name = %source.name(),
                        address = %address,
                        error = %e,
                        "Failed to fetch account info from cache source");
                }
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.iter_synced_sources()
            .find_map(|source| {
                let res = source.block_hash_ref(number);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::block_hash_ref",
                        name = %source.name(),
                        number = number,
                        error = %e,
                        "Failed to fetch block hash from cache source");
                }
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.iter_synced_sources()
            .find_map(|source| {
                let res = source.code_by_hash_ref(code_hash);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::code_by_hash_ref",
                        name = %source.name(),
                        code_hash = %code_hash,
                        error = %e,
                        "Failed to fetch code by hash from cache source");
                }
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.iter_synced_sources()
            .find_map(|source| {
                let res = source.storage_ref(address, index);
                if let Err(e) = res.as_ref() {
                    error!(
                        target = "cache::storage_ref",
                        name = %source.name(),
                        address = %address,
                        index = %index,
                        error = %e,
                        "Failed to fetch the storage from cache source");
                }
                res.ok()
            })
            .ok_or(CacheError::NoCacheSourceAvailable)
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
    use crate::cache::sources::Source;
    use assertion_executor::primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
    };
    use revm::primitives::{
        StorageKey,
        StorageValue,
        U256,
    };
    use std::sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    };

    // Mock Source implementation for testing
    #[derive(Debug)]
    struct MockSource {
        name: &'static str,
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
    }

    impl MockSource {
        fn new(name: &'static str) -> Self {
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
        fn is_synced(&self, current_block_number: u64) -> bool {
            current_block_number >= self.synced_threshold
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    impl DatabaseRef for MockSource {
        type Error = sources::SourceError;

        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.basic_ref_calls.fetch_add(1, Ordering::Release);
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
            if self.should_error {
                return Err(sources::SourceError::Other("Mock error".to_string()));
            }
            Ok(self.storage_value.unwrap_or(U256::ZERO))
        }
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
        let cache = Cache::new(vec![], 10);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 0);
        assert_eq!(cache.sources.len(), 0);
    }

    #[test]
    fn test_cache_new_with_sources() {
        let source1: Arc<dyn Source> = Arc::new(MockSource::new("source1"));
        let source2: Arc<dyn Source> = Arc::new(MockSource::new("source2"));
        let sources = vec![source1, source2];

        let cache = Cache::new(sources, 10);
        assert_eq!(cache.sources.len(), 2);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_set_block_number() {
        let cache = Cache::new(vec![], 10);

        cache.set_block_number(42);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 42);

        cache.set_block_number(100);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 100);
    }

    #[test]
    fn test_synced_sources_filters_correctly() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new("source3").with_synced_threshold(0));

        let cache = Cache::new(vec![source1, source2, source3], 10);

        // Block 0 - only source3 should be synced
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source3");

        // Block 15 - source1 and source3 should be synced
        cache.set_block_number(15);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
        let names: Vec<_> = synced.iter().map(|s| s.name()).collect();
        assert!(names.contains(&"source1"));
        assert!(names.contains(&"source3"));

        // Block 60 - all sources should be synced
        cache.set_block_number(60);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3);
    }

    #[test]
    fn test_basic_ref_success_first_source() {
        let account_info = create_test_account_info();
        let source1 = Arc::new(MockSource::new("source1").with_account_info(account_info.clone()));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);
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
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_account_info(account_info.clone()));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);
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
    fn test_basic_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_error());

        let cache = Cache::new(vec![source1, source2], 10);
        let address = create_test_address();

        let result = cache.basic_ref(address);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_basic_ref_no_synced_sources() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let cache = Cache::new(vec![source1], 10);

        // Block 0, source needs block 10 to be synced
        let address = create_test_address();
        let result = cache.basic_ref(address);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_block_hash_ref_success_first_source() {
        let block_hash = B256::from([3u8; 32]);
        let source1 = Arc::new(MockSource::new("source1").with_block_hash(block_hash));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);

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
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_block_hash(block_hash));

        let cache = Cache::new(vec![source1, source2], 10);

        let result = cache.block_hash_ref(42).unwrap();
        assert_eq!(result, block_hash);
    }

    #[test]
    fn test_block_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1, source2], 10);

        let result = cache.block_hash_ref(42);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_code_by_hash_ref_success_first_source() {
        let bytecode = create_test_bytecode();
        let source1 = Arc::new(MockSource::new("source1").with_bytecode(bytecode.clone()));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);
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
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_bytecode(bytecode.clone()));

        let cache = Cache::new(vec![source1, source2], 10);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash).unwrap();
        assert_eq!(result, bytecode);
    }

    #[test]
    fn test_code_by_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1, source2], 10);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_storage_ref_success_first_source() {
        let storage_value = U256::from(12345);
        let source1 = Arc::new(MockSource::new("source1").with_storage_value(storage_value));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);
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
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_storage_value(storage_value));

        let cache = Cache::new(vec![source1, source2], 10);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key).unwrap();
        assert_eq!(result, storage_value);
    }

    #[test]
    fn test_storage_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_error());

        let cache = Cache::new(vec![source1, source2], 10);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_concurrent_access_thread_safety() {
        use std::thread;

        let source1 =
            Arc::new(MockSource::new("source1").with_account_info(create_test_account_info()));
        let cache = Arc::new(Cache::new(vec![source1], 10));
        let mut handles = vec![];

        // Spawn multiple threads that concurrently access the cache
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                // Set block number
                cache_clone.set_block_number(i * 10);

                // Read account info
                let address = Address::from([(i as u8); 20]);
                let result = cache_clone.basic_ref(address);
                assert!(result.is_ok());

                // Read current block number
                let block_number = cache_clone.current_block_number.load(Ordering::Acquire);
                assert!(block_number <= 90); // Should be one of the values set by threads
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
        let cache = Cache::new(vec![], 10);

        // Test ordering semantics
        cache.set_block_number(42);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 42);

        // Test that load sees the stored value
        cache.current_block_number.store(100, Ordering::Release);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 100);
    }

    #[test]
    fn test_empty_sources_returns_error() {
        let cache = Cache::new(vec![], 10);
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
        let source1 = Arc::new(MockSource::new("source1").with_account_info(account_info.clone()));

        // Source 2: Fails on account info, has block hash
        let source2 = Arc::new(MockSource::new("source2").with_block_hash(block_hash));

        let cache = Cache::new(vec![source1, source2], 10);
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
            MockSource::new("source1")
                .with_account_info(account_info.clone())
                .with_block_hash(block_hash)
                .with_bytecode(bytecode.clone())
                .with_storage_value(storage_value),
        );

        let source2 = Arc::new(
            MockSource::new("source2")
                .with_account_info(account_info.clone())
                .with_block_hash(block_hash)
                .with_bytecode(bytecode.clone())
                .with_storage_value(storage_value),
        );

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 10);
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
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new("source3").with_synced_threshold(100));

        // Test with max_depth = 20
        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 20);

        // Set current block to 100 (this also sets required_block_number to 100)
        cache.set_block_number(100);

        // Effective block: max(100, 100 - 20) = 100
        // All sources synced at block 100 (100 >= 10, 100 >= 50, 100 >= 100)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3);

        // Set required block to 0 to test depth filtering
        cache.reset_required_block_number(0);

        // Now effective block: max(0, 100 - 20) = 80
        // Sources: source1 (80 >= 10 ✓), source2 (80 >= 50 ✓), source3 (80 >= 100 ✗)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
        let names: Vec<_> = synced.iter().map(|s| s.name()).collect();
        assert!(names.contains(&"source1"));
        assert!(names.contains(&"source2"));
        assert!(!names.contains(&"source3"));

        // Test with max_depth = 60
        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 60);
        cache.set_block_number(100);
        cache.reset_required_block_number(0);

        // Effective block: max(0, 100 - 60) = 40
        // Sources: source1 (40 >= 10 ✓), source2 (40 >= 50 ✗), source3 (40 >= 100 ✗)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");

        // Test with max_depth = 200 (larger than current block)
        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 200);
        cache.set_block_number(100);
        cache.reset_required_block_number(0);

        // Effective block: max(0, 100 - 200) = 0
        // Sources: source1 (0 >= 10 ✗), source2 (0 >= 50 ✗), source3 (0 >= 100 ✗)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 0);
    }

    #[test]
    fn test_cache_required_block_number_override() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 20);

        // Set current block to 100 (this sets required_block_number to 100)
        cache.set_block_number(100);

        // Initially, effective block = max(100, 100 - 20) = 100
        // Both sources synced at block 100 (100 >= 10, 100 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Set required block number to 30
        cache.reset_required_block_number(30);

        // The effective block number should be max(30, 100 - 20) = max(30, 80) = 80
        // Both sources synced at block 80 (80 >= 10, 80 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Set required block number to 5 (lower than depth calculation)
        cache.reset_required_block_number(5);

        // The effective block number should be max(5, 80) = 80
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);
    }

    #[test]
    fn test_cache_reset_required_block_number() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 30);

        // Set current block to 100 (this sets required_block_number to 100)
        cache.set_block_number(100);

        // Verify required block number is set to 100
        assert_eq!(
            cache
                .required_block_number
                .load(std::sync::atomic::Ordering::Acquire),
            100
        );

        // Reset required block number to 0
        cache.reset_required_block_number(0);

        // Verify it's now 0
        assert_eq!(
            cache
                .required_block_number
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );

        // Now the effective block number should be max(0, 100 - 30) = 70
        // Both sources synced at block 70 (70 >= 10, 70 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Reset to current block (simulating the old behavior)
        cache.reset_required_block_number(100);
        assert_eq!(
            cache
                .required_block_number
                .load(std::sync::atomic::Ordering::Acquire),
            100
        );
    }

    #[test]
    fn test_cache_out_of_sync_scenarios() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(100));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(200));
        let source3 = Arc::new(MockSource::new("source3").with_synced_threshold(300));

        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);

        // Scenario 1: Set block and reset to test depth filtering
        cache.set_block_number(150);
        cache.reset_required_block_number(0);
        // Effective block number: max(0, 150 - 50) = 100
        // Sources synced at block 100: only source1 (100 >= 100)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");

        // Scenario 2: Lower block number
        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);
        cache.set_block_number(50);
        cache.reset_required_block_number(0);
        // Effective block number: max(0, 50 - 50) = 0
        // Sources synced at block 0: none (0 < 100, 0 < 200, 0 < 300)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 0);

        // Scenario 3: Required block number forces higher sync requirement
        let cache = Cache::new(vec![source1.clone(), source2.clone(), source3.clone()], 50);
        cache.set_block_number(400);
        cache.reset_required_block_number(380);
        // Effective block number: max(380, 400 - 50) = max(380, 350) = 380
        // Sources synced at block 380: all sources (380 >= 100, 380 >= 200, 380 >= 300)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 3);
    }

    #[test]
    fn test_cache_edge_cases() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(0));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(u64::MAX));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], u64::MAX);

        // Test with zero block number - only source1 should be synced (0 >= 0)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");

        // Test with maximum block number
        let cache = Cache::new(vec![source1.clone(), source2.clone()], u64::MAX);
        cache.set_block_number(u64::MAX);
        cache.reset_required_block_number(0);
        // Effective block number: max(0, u64::MAX - u64::MAX) = 0
        // Only source1 synced at block 0 (0 >= 0)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");

        // Test with zero max_depth
        let cache = Cache::new(vec![source1.clone(), source2.clone()], 0);
        cache.set_block_number(100);
        cache.reset_required_block_number(0);
        // Effective block number: max(0, 100 - 0) = 100
        // Both sources: source1 (100 >= 0 ✓), source2 (100 >= u64::MAX ✗)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");
    }

    #[test]
    fn test_original_test_understanding() {
        // This replicates the original test to understand the correct behavior
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));
        let source3 = Arc::new(MockSource::new("source3").with_synced_threshold(0));

        let cache = Cache::new(vec![source1, source2, source3], 10);

        // Block 0 - effective block is 0, only source3 (0 >= 0) should be synced
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source3");

        // Block 15 - this sets required_block_number to 15
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
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let source2 = Arc::new(MockSource::new("source2").with_synced_threshold(50));

        let cache = Cache::new(vec![source1.clone(), source2.clone()], 40);

        // Simulate normal operation
        cache.set_block_number(100);
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2); // Both sources synced at block 100

        // Simulate cache invalidation by resetting required block to 0
        cache.reset_required_block_number(0);
        // Effective block: max(0, 100 - 40) = 60
        // Both sources still synced (60 >= 10, 60 >= 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 2);

        // Simulate more restrictive invalidation
        cache.reset_required_block_number(0);
        cache.set_block_number(70); // Lower current block
        // Effective block: max(0, 70 - 40) = 30
        // Only source1 synced (30 >= 10, but 30 < 50)
        let synced: Vec<_> = cache.iter_synced_sources().collect();
        assert_eq!(synced.len(), 1);
        assert_eq!(synced[0].name(), "source1");
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_first_fallback(mut instance: crate::utils::LocalInstance) {
        // Send a random tx whose data is not in the in-memory cache
        let (address, _tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();

        // The first fallback is hit
        let cache_sequencer_db_basic_ref_counter = instance
            .cache_sequencer_db
            .mock_db
            .basic_ref_counter
            .get(&address)
            .unwrap()
            .load(Ordering::Relaxed);
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);

        // The second fallback is never called
        assert!(
            instance
                .cache_besu_client_db
                .mock_db
                .basic_ref_counter
                .get(&address)
                .is_none()
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_second_fallback(mut instance: crate::utils::LocalInstance) {
        // Make the first cache out of sync
        instance
            .cache_sequencer_db
            .is_synced
            .store(false, Ordering::Relaxed);

        // Send a random tx whose data is not in the in-memory cache
        let (address, _tx_hash) = instance.send_create_tx_with_cache_miss().await.unwrap();

        // The first fallback is never called
        assert!(
            instance
                .cache_sequencer_db
                .mock_db
                .basic_ref_counter
                .get(&address)
                .is_none()
        );

        // The second fallback is hit
        let cache_sequencer_db_basic_ref_counter = instance
            .cache_besu_client_db
            .mock_db
            .basic_ref_counter
            .get(&address)
            .unwrap()
            .load(Ordering::Relaxed);
        assert_eq!(cache_sequencer_db_basic_ref_counter, 1);
    }
}
