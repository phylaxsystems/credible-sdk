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
    pub fn new(sources: Vec<Arc<dyn Source>>) -> Self {
        Self {
            current_block_number: AtomicU64::new(0),
            sources,
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
        self.current_block_number
            .store(block_number, Ordering::Relaxed);
    }

    /// Returns an iterator over sources that are currently synced.
    ///
    /// Sources are returned in priority order, with the highest priority
    /// synced source returned first.
    fn iter_synced_sources(&self) -> impl Iterator<Item = Arc<dyn Source>> {
        self.sources
            .iter()
            .filter(move |source| source.is_synced(&self.current_block_number))
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
            AtomicU64,
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
        fn is_synced(&self, current_block_number: &AtomicU64) -> bool {
            current_block_number.load(Ordering::Acquire) >= self.synced_threshold
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
        let cache = Cache::new(vec![]);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 0);
        assert_eq!(cache.sources.len(), 0);
    }

    #[test]
    fn test_cache_new_with_sources() {
        let source1: Arc<dyn Source> = Arc::new(MockSource::new("source1"));
        let source2: Arc<dyn Source> = Arc::new(MockSource::new("source2"));
        let sources = vec![source1, source2];

        let cache = Cache::new(sources);
        assert_eq!(cache.sources.len(), 2);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_set_block_number() {
        let cache = Cache::new(vec![]);

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

        let cache = Cache::new(vec![source1, source2, source3]);

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

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);
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

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);
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

        let cache = Cache::new(vec![source1, source2]);
        let address = create_test_address();

        let result = cache.basic_ref(address);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_basic_ref_no_synced_sources() {
        let source1 = Arc::new(MockSource::new("source1").with_synced_threshold(10));
        let cache = Cache::new(vec![source1]);

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

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);

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

        let cache = Cache::new(vec![source1, source2]);

        let result = cache.block_hash_ref(42).unwrap();
        assert_eq!(result, block_hash);
    }

    #[test]
    fn test_block_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1, source2]);

        let result = cache.block_hash_ref(42);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_code_by_hash_ref_success_first_source() {
        let bytecode = create_test_bytecode();
        let source1 = Arc::new(MockSource::new("source1").with_bytecode(bytecode.clone()));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);
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

        let cache = Cache::new(vec![source1, source2]);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash).unwrap();
        assert_eq!(result, bytecode);
    }

    #[test]
    fn test_code_by_hash_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1, source2]);
        let code_hash = B256::from([4u8; 32]);

        let result = cache.code_by_hash_ref(code_hash);
        assert!(matches!(result, Err(CacheError::NoCacheSourceAvailable)));
    }

    #[test]
    fn test_storage_ref_success_first_source() {
        let storage_value = U256::from(12345);
        let source1 = Arc::new(MockSource::new("source1").with_storage_value(storage_value));
        let source2 = Arc::new(MockSource::new("source2"));

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);
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

        let cache = Cache::new(vec![source1, source2]);
        let address = create_test_address();
        let storage_key = U256::from(42);

        let result = cache.storage_ref(address, storage_key).unwrap();
        assert_eq!(result, storage_value);
    }

    #[test]
    fn test_storage_ref_no_cache_source_available() {
        let source1 = Arc::new(MockSource::new("source1").with_error());
        let source2 = Arc::new(MockSource::new("source2").with_error());

        let cache = Cache::new(vec![source1, source2]);
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
        let cache = Arc::new(Cache::new(vec![source1]));
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
        let cache = Cache::new(vec![]);

        // Test ordering semantics
        cache.set_block_number(42);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 42);

        // Test that load sees the stored value
        cache.current_block_number.store(100, Ordering::Release);
        assert_eq!(cache.current_block_number.load(Ordering::Acquire), 100);
    }

    #[test]
    fn test_empty_sources_returns_error() {
        let cache = Cache::new(vec![]);
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

        let cache = Cache::new(vec![source1, source2]);
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

        let cache = Cache::new(vec![source1.clone(), source2.clone()]);
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
}
