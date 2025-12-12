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

### 🚧 TODO

- **gRPC integration**: Protobuf definitions and client implementation for:
  - `StreamInvalidations`: long-lived subscription to receive invalidated fingerprints from sidecar
  - `ShouldForward`: on-demand check for fingerprints not in local cache
- **Upstream forwarding**: Currently echoes transactions; needs HTTP client to forward to actual sequencer
- **Sender/IP backpressure**: Rate limiting per origin (IP/API token/address) with exponential backoff
- **Assertion-level cooldowns**: When many distinct fingerprints fail under the same assertion, throttle the assertion globally (with trickle-through for recovery)
- **Priority score adjustment**: Penalize gas price for banned fingerprints so sequencer doesn't select them solely based on tips
- **Pending timeout**: Evict fingerprints from pending set if they don't get a verdict within a reasonable time
- **Cache persistence**: Optional sled/SQLite backend to survive restarts
- **Observability dashboard**: Grafana panels for metrics

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
