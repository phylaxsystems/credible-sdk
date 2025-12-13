use std::{
    fmt,
    net::IpAddr,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use alloy_consensus::{
    TxEnvelope,
    transaction::SignerRecoverable,
};
use alloy_primitives::Address;
use dashmap::DashMap;
use serde::{
    Deserialize,
    Serialize,
};

/// Configuration for sender/IP backpressure enforcement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Maximum burst per origin before throttling kicks in.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Refill rate expressed as tokens per second.
    #[serde(default = "default_refill_tokens_per_second")]
    pub refill_tokens_per_second: f64,
    /// Base cooldown applied when an origin exhausts its bucket (milliseconds).
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    /// Maximum cooldown applied after repeated exhaustion (milliseconds).
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Maximum unique origins tracked before oldest entries start getting evicted.
    #[serde(default = "default_max_origins")]
    pub max_origins: u64,
    /// Enable/disable enforcement entirely.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

const fn default_max_tokens() -> u32 {
    20
}

const fn default_refill_tokens_per_second() -> f64 {
    5.0
}

const fn default_base_backoff_ms() -> u64 {
    1_000
}

const fn default_max_backoff_ms() -> u64 {
    30_000
}

const fn default_max_origins() -> u64 {
    20_000
}

const fn default_enabled() -> bool {
    true
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_tokens: default_max_tokens(),
            refill_tokens_per_second: default_refill_tokens_per_second(),
            base_backoff_ms: default_base_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            max_origins: default_max_origins(),
            enabled: default_enabled(),
        }
    }
}

impl BackpressureConfig {
    fn base_backoff(&self) -> Duration {
        Duration::from_millis(self.base_backoff_ms.max(1))
    }

    fn max_backoff(&self) -> Duration {
        Duration::from_millis(self.max_backoff_ms.max(self.base_backoff_ms.max(1)))
    }

    fn bucket_capacity(&self) -> f64 {
        self.max_tokens as f64
    }
}

/// Composite metadata extracted from the transport layer and transaction payload.
#[derive(Debug, Clone, Default)]
pub struct OriginMetadata {
    pub sender: Option<Address>,
    pub ip: Option<IpAddr>,
}

impl OriginMetadata {
    pub fn from_envelope(envelope: &TxEnvelope) -> Self {
        let sender = envelope.recover_signer().ok();
        Self { sender, ip: None }
    }

    pub fn is_empty(&self) -> bool {
        self.sender.is_none() && self.ip.is_none()
    }

    fn keys(&self) -> impl Iterator<Item = OriginKey> + '_ {
        self.sender
            .iter()
            .copied()
            .map(OriginKey::Sender)
            .chain(self.ip.iter().copied().map(OriginKey::Ip))
    }
}

/// Identifier for an origin subject to backpressure.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OriginKey {
    Sender(Address),
    Ip(IpAddr),
}

impl OriginKey {
    pub fn dimension(&self) -> &'static str {
        match self {
            OriginKey::Sender(_) => "sender",
            OriginKey::Ip(_) => "ip",
        }
    }
}

impl fmt::Display for OriginKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OriginKey::Sender(addr) => write!(f, "sender {}", addr),
            OriginKey::Ip(ip) => write!(f, "ip {}", ip),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ThrottledOrigin {
    pub origin: OriginKey,
    pub retry_after: Duration,
}

#[derive(Clone, Debug)]
pub struct OriginBackpressure {
    config: BackpressureConfig,
    buckets: Arc<DashMap<OriginKey, OriginBucket>>,
}

impl OriginBackpressure {
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(DashMap::new()),
        }
    }

    pub fn check(&self, metadata: &OriginMetadata) -> Result<(), ThrottledOrigin> {
        if !self.config.enabled || metadata.is_empty() {
            return Ok(());
        }

        for origin in metadata.keys() {
            if let Some(throttled) = self.evaluate_origin(origin.clone()) {
                metrics::counter!(
                    "rpc_proxy_backpressure_reject_total",
                    "dimension" => throttled.origin.dimension()
                )
                .increment(1);
                return Err(throttled);
            }
        }

        Ok(())
    }

    fn evaluate_origin(&self, origin: OriginKey) -> Option<ThrottledOrigin> {
        if self.config.max_tokens == 0 {
            return None;
        }

        let now = Instant::now();
        let decision = {
            let mut entry = self
                .buckets
                .entry(origin.clone())
                .or_insert_with(|| OriginBucket::new(now, self.config.bucket_capacity()));
            entry.refill(now, &self.config);

            if let Some(until) = entry.backoff_until {
                if until > now {
                    Some(ThrottledOrigin {
                        origin,
                        retry_after: until - now,
                    })
                } else {
                    entry.backoff_until = None;
                    None
                }
            } else if entry.tokens >= 1.0 {
                entry.tokens -= 1.0;
                entry.relax();
                None
            } else {
                let retry_after = entry.apply_penalty(now, &self.config);
                Some(ThrottledOrigin {
                    origin,
                    retry_after,
                })
            }
        };
        self.evict_if_needed();
        decision
    }

    fn evict_if_needed(&self) {
        if self.config.max_origins == 0 {
            return;
        }
        if self.buckets.len() > self.config.max_origins as usize {
            if let Some(key) = self.buckets.iter().next().map(|entry| entry.key().clone()) {
                self.buckets.remove(&key);
            }
        }
    }
}

#[derive(Debug)]
struct OriginBucket {
    tokens: f64,
    last_refill: Instant,
    penalty_level: u32,
    backoff_until: Option<Instant>,
}

impl OriginBucket {
    fn new(now: Instant, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: now,
            penalty_level: 0,
            backoff_until: None,
        }
    }

    fn refill(&mut self, now: Instant, config: &BackpressureConfig) {
        if self.tokens >= config.bucket_capacity() {
            self.tokens = config.bucket_capacity();
            self.last_refill = now;
            self.penalty_level = 0;
            return;
        }

        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed <= 0.0 || config.refill_tokens_per_second <= 0.0 {
            return;
        }

        self.tokens =
            (self.tokens + elapsed * config.refill_tokens_per_second).min(config.bucket_capacity());
        self.last_refill = now;
        if (self.tokens - config.bucket_capacity()).abs() < f64::EPSILON {
            self.penalty_level = 0;
        }
    }

    fn relax(&mut self) {
        self.penalty_level = self.penalty_level.saturating_sub(1);
    }

    fn apply_penalty(&mut self, now: Instant, config: &BackpressureConfig) -> Duration {
        self.penalty_level = self.penalty_level.saturating_add(1).min(16);
        let mut delay = config.base_backoff();
        let shift = self.penalty_level.saturating_sub(1).min(16);
        let multiplier = 1u32 << shift;
        delay = delay.saturating_mul(multiplier);
        delay = delay.min(config.max_backoff());

        self.tokens = 0.0;
        let until = now + delay;
        self.backoff_until = Some(until);
        delay
    }
}
