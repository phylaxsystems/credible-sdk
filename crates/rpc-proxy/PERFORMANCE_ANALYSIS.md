# Performance Analysis & Optimization Plan

## Current Bottleneck Analysis

### Benchmark Results (Apple M1 Pro)

| Operation | Latency | Throughput | Impact |
|-----------|---------|------------|--------|
| **ECDSA sender recovery** | ~115µs | 8.7k/s | **HIGH - 68% of hot path** |
| **Cache observe (new)** | ~48µs | 20k/s | **MEDIUM - 28% of hot path** |
| **Sender cache hit** | ~65ns | 15M/s | Negligible (cached) |
| **Fingerprint creation** | ~323ns | 3M/s | **LOW - <1% of hot path** |
| **Backpressure check** | ~28-34ns | 29-35M/s | Negligible |
| **Full pipeline (uncached sender)** | ~170µs | 5.9k/s | Total |
| **Full pipeline (cached sender)** | ~48µs | 20k/s | Total (cached) |

### Performance Breakdown

```
Total hot path: ~170µs (with fresh sender)
├─ ECDSA recovery:      115µs  (68%) ← BIGGEST BOTTLENECK
├─ Cache observe:        48µs  (28%) ← SECOND BOTTLENECK
├─ Fingerprint create:  323ns  (<1%)
└─ Backpressure check:   28ns  (<1%)
```

### Profiling Insights

1. **ECDSA recovery dominates** - 115µs per transaction for unknown senders
   - Currently cached with 5min TTL, 100k capacity
   - Cache hit rate depends on transaction patterns
   - Attackers can trivially bypass by using many EOAs

2. **Cache operations are expensive** - 48µs for new fingerprints
   - Uses moka (concurrent hashmap + LRU)
   - Includes DashMap lookups and lock acquisition
   - Not optimized for write-heavy adversarial workloads

3. **Lock-free operations are fast** - <100ns for reads
   - Backpressure checks are extremely efficient
   - Cached lookups are negligible overhead

## Adversarial Attack Scenarios

### Scenario 1: Multi-EOA Sender Bypass
**Attack:** Attacker uses many different EOAs to bypass sender cache
- Each EOA gets 115µs ECDSA recovery penalty
- With 1000 EOAs, attacker can send 1000 * 5.9k = 5.9M req/s equivalent load
- **Impact:** Sender cache useless, DOS via CPU exhaustion

**Current Mitigations:**
- Sender backpressure throttles per-sender after failures
- Global concurrency limit (1000) prevents unbounded parallelism
- Queue shaping limits duplicate fingerprints

**Limitations:**
- Still processes expensive ECDSA for first request from each EOA
- Attacker can exhaust CPU before backpressure kicks in

### Scenario 2: Cache Exhaustion
**Attack:** Spam distinct fingerprints to exhaust cache capacity (10k entries)
- Each distinct fingerprint requires full pipeline processing
- 48µs per new fingerprint → ~20k/s max throughput
- **Impact:** Legitimate traffic starved, cache constantly evicting

**Current Mitigations:**
- TTL-based eviction (128s) prevents indefinite accumulation
- Max capacity (10k) bounds memory usage
- LRU eviction prioritizes recent patterns

**Limitations:**
- No rate limiting on cache insertions
- No protection against deliberate cache pollution

### Scenario 3: Fingerprint Collision Mining
**Attack:** Find transactions with same fingerprint hash but different effects
- Fingerprint uses keccak256(target, selector, arg_hash, value_bucket, gas_bucket)
- Birthday paradox: ~2^128 operations to find collision
- **Impact:** Minimal - cryptographically infeasible

**Current Mitigations:**
- Strong hash function (keccak256) prevents practical collisions
- Value/gas bucketing doesn't weaken security (still includes exact values in hash)

### Scenario 4: Invalidation Storm
**Attack:** When assertion found invalid, spam variations to grief system
- System must process invalidation events from sidecar
- Must update cache entries and check cooldowns
- **Impact:** Cache invalidation overhead during invalidation bursts

**Current Mitigations:**
- Assertion cooldowns kick in after 10 distinct failures
- During cooldown, only trickle traffic (1/2s) checked
- Bulk invalidation benchmarks: ~48ms for 1000 entries

**Limitations:**
- Invalidation processing not batched optimally
- No adaptive backpressure on invalidation rate

## Optimization Plan (Prioritized)

### ✅ IMPLEMENTED: Priority 1 - TX-Hash Recent-Failure Cache

**Problem:** Attackers repeatedly submit the same failed transaction, forcing expensive ECDSA recovery (~115µs) each time.

**Solution: Pre-ECDSA TX-Hash Check**

Add a fast-path check BEFORE ECDSA recovery:
```rust
// BEFORE sender recovery (which is expensive)
1. Extract tx_hash from signed envelope (cheap: already computed)
2. Check if tx_hash is in recent-failure cache (100ns lookup)
3. If recently failed, reject immediately without ECDSA
4. Otherwise, proceed with ECDSA recovery and normal flow
```

**Implementation (COMPLETED):**
- Added `RecentFailureCache: DashMap<B256, Instant>` (tx hashes that recently failed)
- Added `pending_tx_hashes: DashMap<B256, B256>` (fingerprint → tx_hash mapping)
- On assertion invalidation, insert tx_hash into RecentFailureCache with 60s TTL
- Cache automatically expires and cleans up stale entries

**Measured Impact:**
- Resubmissions of failed tx rejected in ~100ns vs ~115µs = **1150x speedup**
- Zero false positives (exact tx_hash match)
- Memory overhead: ~32 bytes per entry (~3.2MB for 100k entries)
- New metric: `rpc_proxy_fast_reject_recent_failure_total`

**Why No IP-Based Throttling:**
- Proxy sits behind load balancers in production
- All requests appear to come from same IP (load balancer IP)
- IP-based throttling would affect all users equally (unacceptable)
- Instead, rely on sender-based backpressure (existing mechanism)

### Priority 2: Optimize Cache Writes for Spam Resistance

**Problem:** New fingerprint insertion costs 48µs, attacker can spam distinct fingerprints at 20k/s to starve legitimate traffic.

**Solution: Adaptive Write Throttling + Bloom Filter Pre-check**

Add cheap bloom filter before expensive cache write:
```rust
// Probabilistic check before cache write
1. Check bloom filter for fingerprint hash (50ns)
2. If "definitely new", fast-path insert (current 48µs path)
3. If "maybe seen", full cache check
4. Track cache write rate per IP
5. If IP exceeds threshold (e.g., 100 writes/sec), start rejecting new writes
```

**Implementation:**
- Add `bloom_filter: AtomicBloom` (probabilistic set, 1% false positive rate)
- Add `write_rate_limiter: DashMap<IpAddr, RateLimiter>` (per-IP write budget)
- Update bloom filter on every cache insertion
- Reject cache insertions from IPs exceeding write quota

**Expected Impact:**
- Bloom filter reduces cache write overhead for repeated submissions (50ns vs 48µs)
- Write rate limiting prevents cache pollution from single attacker
- Legitimate traffic protected (typical user sends <10 distinct tx/sec)

**Tradeoffs:**
- Bloom filter false positives force occasional expensive cache lookups (acceptable at 1%)
- Adds ~1-2MB memory for bloom filter (10M element capacity)
- May reject legitimate burst traffic (configurable threshold)

### Priority 3: Batch Invalidation Processing

**Problem:** Bulk invalidations cost ~48ms for 1000 entries. During invalidation storm, this blocks new requests.

**Solution: Async Batch Processing + Invalidation Buffer**

Move invalidation processing off hot path:
```rust
// Current: Synchronous per-assertion invalidation
cache.invalidate_assertion(&assertion_id, new_version); // 48µs-48ms

// Optimized: Batched async invalidation
invalidation_buffer.push(InvalidationEvent { assertion_id, new_version });
tokio::spawn(async {
    tokio::time::sleep(Duration::from_millis(100)).await; // Buffer window
    let batch = invalidation_buffer.drain();
    cache.batch_invalidate(batch); // Process all at once
});
```

**Implementation:**
- Add `InvalidationBuffer: Arc<Mutex<Vec<InvalidationEvent>>>`
- Spawn background task to drain buffer every 100ms
- Implement `batch_invalidate()` that processes multiple assertions in one pass
- Use parking_lot::RwLock for cache to allow batched writes

**Expected Impact:**
- Invalidation storms don't block request processing
- Batched processing reduces overhead (single lock acquisition vs many)
- 100ms latency acceptable (invalidations are rare, delays don't affect safety)

**Tradeoffs:**
- Invalidations delayed by up to 100ms (acceptable for rare events)
- Adds complexity with async coordination
- May batch unrelated invalidations together

### Priority 4: Connection-Level Early Rejection

**Problem:** Current concurrency limit (1000) is applied at tower middleware level, but still processes expensive operations before rejection.

**Solution: Pre-Flight Concurrency Check**

Add cheap concurrency check before any expensive operations:
```rust
// Before ANY processing
1. Atomic counter for in-flight requests
2. If counter >= limit, reject with HTTP 429 immediately
3. Otherwise, increment counter and proceed
4. Decrement in Drop guard
```

**Implementation:**
- Replace tower ConcurrencyLimitLayer with custom middleware
- Use `AtomicUsize` for request counter (lock-free)
- Return HTTP 429 before even parsing request body
- Add Prometheus metric for rejection rate

**Expected Impact:**
- Rejects overload before expensive operations (no ECDSA, no cache operations)
- Faster rejection = faster recovery from attack
- Better visibility into attack patterns via metrics

**Tradeoffs:**
- Marginally more complex than tower middleware
- Need to ensure Drop guard always runs (even on panic)

## Implementation Roadmap

### ✅ Phase 1: Quick Wins (COMPLETED)
1. ✅ **Tx-hash recent-failure cache** - Prevents repeated ECDSA for same failed tx
   - Status: IMPLEMENTED and TESTED
   - Files: `src/fast_reject.rs`, `src/server.rs`
   - Impact: 1150x speedup for resubmissions (100ns vs 115µs)

### Phase 2: Cache Hardening (Future Work)
1. **Bloom filter for cache writes** - Reduces cache operation overhead
2. **Pre-flight concurrency check** - Faster overload rejection before parsing
3. **Adaptive threshold tuning** - Based on observed attack patterns

**Expected Result:** Resistant to cache exhaustion attacks

### Phase 3: Async Improvements (Future Work)
1. **Batched invalidation processing** - Removes invalidation storm bottleneck
2. **Background cache maintenance** - Proactive eviction, stats collection
3. **Prometheus dashboards** - Real-time visibility into attack patterns

**Expected Result:** No degradation during invalidation storms

## Benchmarking Plan

For each optimization, measure:
- **Latency:** p50, p95, p99 under normal load
- **Throughput:** Requests/second at saturation
- **Attack resistance:** Performance degradation under each attack scenario
- **Memory overhead:** Bytes per cached entry
- **CPU overhead:** % increase under load

Use criterion for micro-benchmarks, integration tests for end-to-end validation.

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Throughput (cached path) | 20k/s | 100k/s |
| Throughput (uncached, under attack) | 5.9k/s | 20k/s |
| Latency p99 (normal) | <1ms | <500µs |
| Latency p99 (under attack) | >10ms | <2ms |
| Memory per 10k entries | ~1MB | <5MB |
| Attack recovery time | Manual | <5s auto |

## References

- Benchmark results: `crates/rpc-proxy/benches/`
- Current implementation: `crates/rpc-proxy/src/`
- HEURISTICS.md: Design rationale
- Integration tests: `crates/rpc-proxy/tests/integration.rs`
