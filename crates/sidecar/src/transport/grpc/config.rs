//! # Configuration for the `GrpcTransport`.

use crate::args::TransportConfig;
use std::{
    net::{
        AddrParseError,
        SocketAddr,
    },
    time::Duration,
};

/// Configuration for the content-hash deduplication cache.
#[derive(Debug, Clone)]
pub struct DedupCacheConfig {
    /// Whether the dedup cache is enabled.
    pub enabled: bool,
    /// Eviction window in blocks.
    pub eviction_window: u64,
    /// Moka cache capacity.
    pub moka_capacity: u64,
    /// Bloom filter initial capacity.
    pub bloom_capacity: usize,
}

impl Default for DedupCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            eviction_window: 256,
            moka_capacity: 100_000,
            bloom_capacity: 100_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GrpcTransportConfig {
    /// Server bind address and port
    pub bind_addr: SocketAddr,
    /// Max age for pending receive entries.
    pub pending_receive_ttl: Duration,
    /// Content-hash dedup cache configuration.
    pub dedup_cache: DedupCacheConfig,
}

impl Default for GrpcTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9090".parse().unwrap(),
            pending_receive_ttl: Duration::from_secs(5),
            dedup_cache: DedupCacheConfig::default(),
        }
    }
}

impl TryFrom<TransportConfig> for GrpcTransportConfig {
    type Error = AddrParseError;
    fn try_from(value: TransportConfig) -> Result<Self, Self::Error> {
        let pending_receive_ttl = if value.pending_receive_ttl_ms.is_zero() {
            Duration::from_millis(1)
        } else {
            value.pending_receive_ttl_ms
        };
        Ok(Self {
            bind_addr: value.bind_addr.parse()?,
            pending_receive_ttl,
            dedup_cache: DedupCacheConfig {
                enabled: value.content_hash_dedup_enabled,
                eviction_window: value.content_hash_dedup_eviction_window,
                moka_capacity: value.content_hash_dedup_moka_capacity,
                bloom_capacity: value.content_hash_dedup_bloom_capacity,
            },
        })
    }
}
