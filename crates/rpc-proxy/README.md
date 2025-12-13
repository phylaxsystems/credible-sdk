# rpc-proxy

A JSON-RPC proxy that sits in front of the sequencer to prevent assertion-invalidated transactions from spamming the mempool. See `HEURISTICS.md` for the full design.

## Implementation Status

### ✅ Completed

- **Fingerprint normalization**: Transactions are normalized into deterministic fingerprints based on target, selector, argument hash, value bucket, and gas bucket
- **TTL cache with LRU eviction**: Uses `moka` for efficient caching with automatic expiration (default: 128s TTL, 10k max entries)
- **Assertion-aware tracking**: Each denied fingerprint tracks which assertion(s) caused it to fail, allowing selective cache invalidation when assertion versions update
- **Pending state management**: Fingerprints remain in "pending" state until the sidecar reports validation results, preventing duplicate transaction spam
- **Metrics instrumentation**: Prometheus counters for forwards, rejects (by reason), cache operations, and invalidations
- **Configurable cache**: Cache TTL and capacity can be configured via `ProxyConfig`
- **Comprehensive tests**: Unit tests for value/gas bucketing, fingerprint determinism, and cache state transitions
- **Error handling**: Proper JSON-RPC error codes with descriptive messages
- **HTTP forwarding**: Raw transactions are forwarded to the configured sequencer endpoint via `reqwest`, reusing the JSON-RPC envelope
- **Sidecar transport abstraction**: A `SidecarTransport` trait powers both the default in-process/noop transport and the new gRPC client (defined in `proto/heuristics.proto`)
- **Exponential backoff**: Invalidation listener reconnects with exponential backoff (1s → 60s max) on sidecar failures
- **Pending timeout**: Automatic cleanup of stuck pending entries (default: 30s timeout, swept every 15s)
- **Dry-run mode**: `--dry-run` flag logs rejections but forwards everything for production validation
- **Integration tests**: Wiremock-based tests for HTTP forwarding, cache behavior, and dry-run mode
- **Sender backpressure**: Token-bucket throttling per recovered sender activates only after repeated assertion invalidations, keeping spammy EOAs from monopolizing the proxy without touching honest traffic

### 🚧 TODO (in planned order)

1. **Assertion-level cooldowns + priority scoring**
   - Track assertion-level failure rates and apply global throttles when multiple fingerprints fail; adjust gas-price priority to penalize banned fingerprints.
2. **Benchmark harness**
   - Add Criterion/contender suites to measure normalization + cache latency; run Samply for wall-clock profiling once full pipeline is implemented.
3. **Persistence & observability**
   - Optional sled-backed cache to survive restarts, Prometheus/Grafana dashboards for cache stats, and documentation of benchmark results.

## Usage

```bash
# Run the proxy
cargo run --bin rpc-proxy -- \
  --listen 0.0.0.0:9547 \
  --upstream http://127.0.0.1:8545 \
  --sidecar-endpoint http://127.0.0.1:50051

# Run in dry-run mode (logs rejections but forwards everything)
cargo run --bin rpc-proxy -- \
  --listen 0.0.0.0:9547 \
  --upstream http://127.0.0.1:8545 \
  --sidecar-endpoint http://127.0.0.1:50051 \
  --dry-run

# Run tests
cargo test -p rpc-proxy

# Run integration tests
cargo test -p rpc-proxy --test integration
```

## Configuration

The proxy accepts configuration via CLI flags or environment variables:

- `--listen`: HTTP server bind address (default: `127.0.0.1:9547`)
- `--rpc-path`: JSON-RPC path (default: `/rpc`)
- `--upstream`: Upstream sequencer HTTP endpoint (default: `http://127.0.0.1:8545`)
- `--sidecar-endpoint`: Optional gRPC endpoint for sidecar communication
- `--dry-run`: Enable dry-run mode (logs rejections but forwards all transactions)

Cache behavior can be tuned via the `cache` field in `ProxyConfig`:
- `max_denied_entries`: Maximum fingerprints in denied cache (default: 10,000)
- `denied_ttl_secs`: Time-to-live for denied entries in seconds (default: 128s ≈ 64 L2 slots)
- `pending_timeout_secs`: Timeout for pending fingerprints in seconds (default: 30s)

Backpressure is configured via the `backpressure` block:
- `max_tokens`: Number of assertion invalidations per origin before throttling (default: 20)
- `refill_tokens_per_second`: Rate at which the invalidation budget refills (default: 5 / sec)
- `base_backoff_ms` / `max_backoff_ms`: Exponential cooldown window applied once the invalidation budget hits zero (default: 1s → 30s max)
- `max_origins`: Maximum unique origins tracked before old entries are evicted (default: 20k)
- `enabled`: Toggle enforcement without changing other thresholds
