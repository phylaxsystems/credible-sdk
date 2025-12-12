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

### 🚧 TODO (in planned order)

1. **Axum/HTTP serving**
   - The HTTP router is instantiated, but we still need to bind it to a listener using axum’s server utilities so the proxy can run as a standalone daemon.
2. **Sidecar gRPC integration polish**
   - Hook `GrpcSidecarTransport` into the runtime (spawn the invalidation stream, add retries/backoff, and surface health metrics).
3. **Pending-state timeout + backpressure**
   - Add configurable TTL for the pending set so stuck fingerprints clear automatically and introduce sender/IP rate limiting.
4. **Assertion-level cooldowns + priority scoring**
   - Track assertion-level failure rates and apply global throttles when multiple fingerprints fail; adjust gas-price priority to penalize banned fingerprints.
5. **Benchmark harness**
   - Add Criterion/contender suites to measure normalization + cache latency; run Samply for wall-clock profiling once full pipeline is implemented.
6. **Persistence & observability**
   - Optional sled-backed cache to survive restarts, Prometheus/Grafana dashboards for cache stats, and documentation of benchmark results.

## Usage

```bash
# Run the proxy
cargo run --bin rpc-proxy -- \
  --listen 0.0.0.0:9547 \
  --upstream http://127.0.0.1:8545 \
  --sidecar-endpoint http://127.0.0.1:50051

# Run tests
cargo test -p rpc-proxy
```

## Configuration

The proxy accepts configuration via CLI flags or environment variables:

- `--listen`: HTTP server bind address (default: `127.0.0.1:9547`)
- `--rpc-path`: JSON-RPC path (default: `/rpc`)
- `--upstream`: Upstream sequencer HTTP endpoint (default: `http://127.0.0.1:8545`)
- `--sidecar-endpoint`: Optional gRPC endpoint for sidecar communication

Cache behavior can be tuned via the `cache` field in `ProxyConfig`:
- `max_denied_entries`: Maximum fingerprints in denied cache (default: 10,000)
- `denied_ttl_secs`: Time-to-live for denied entries in seconds (default: 128s ≈ 64 L2 slots)
