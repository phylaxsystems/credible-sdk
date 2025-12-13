# Sidecar gRPC API Documentation

This document describes the gRPC API that sidecars must implement to communicate invalidation events to the RPC proxy.

## Overview

The RPC proxy acts as a **gRPC client** that connects to your sidecar (the **gRPC server**). When your sidecar detects that a transaction has failed assertion validation, it streams an invalidation event to all connected proxies.

```
┌─────────────┐                    ┌──────────────┐
│             │  gRPC connection   │              │
│  RPC Proxy  │◄───────────────────│   Sidecar    │
│  (client)   │  StreamInvalidations│  (server)   │
│             │                    │              │
└─────────────┘                    └──────────────┘
       │                                  │
       │                                  │
       ▼                                  ▼
  Caches denied                    Validates txs
  fingerprints                     against assertions
```

## Protocol Definition

The service is defined in `proto/heuristics.proto`:

```protobuf
service RpcProxyHeuristics {
  rpc StreamInvalidations(Empty) returns (stream Invalidation);
  rpc ShouldForward(ShouldForwardRequest) returns (ShouldForwardResponse);
}
```

## API Methods

### 1. StreamInvalidations (Server-Streaming RPC)

**Direction:** Sidecar → Proxy (continuous stream)

The proxy calls this method once on startup and keeps the connection open. Your sidecar streams `Invalidation` messages whenever a transaction fails assertion validation.

#### Request

```protobuf
message Empty {}
```

No request parameters. The proxy simply subscribes to the stream.

#### Response Stream

```protobuf
message Invalidation {
  Fingerprint fingerprint = 1;    // The denied transaction fingerprint
  Assertion assertion = 2;         // Which assertion caused the failure
  string adopter = 3;              // Optional: adopter identifier
  uint64 l2_block_number = 4;      // Block number where failure observed
  google.protobuf.Timestamp observed_at = 5;  // When failure was detected
}

message Fingerprint {
  bytes hash = 1;           // 32-byte keccak256 hash of fingerprint
  bytes target = 2;         // 20-byte contract address
  bytes selector = 3;       // 4-byte function selector
  bytes arg_hash16 = 4;     // 16-byte truncated hash of arguments
  uint64 value_bucket = 5;  // ETH value bucket (0, 1-10, 11-100, etc.)
  uint32 gas_bucket = 6;    // Gas limit bucket (rounded to nearest 10k)
}

message Assertion {
  bytes assertion_id = 1;        // 32-byte assertion identifier
  uint64 assertion_version = 2;  // Assertion version number
}
```

#### When to Send Invalidations

Send an invalidation when:
1. A transaction is executed on L2
2. Your assertion validator determines it violates an assertion
3. You want the proxy to cache and reject future similar transactions

#### Example (Rust)

```rust
use tokio::sync::broadcast;
use prost_types::Timestamp;

// In your sidecar server
let (invalidation_tx, _) = broadcast::channel(100);

// When a transaction fails assertion
let invalidation = Invalidation {
    fingerprint: Some(Fingerprint {
        hash: fingerprint_hash.to_vec(),
        target: contract_address.to_vec(),
        selector: vec![0xa9, 0x05, 0x9c, 0xbb], // transfer(address,uint256)
        arg_hash16: truncated_arg_hash.to_vec(),
        value_bucket: 0,
        gas_bucket: 100_000,
    }),
    assertion: Some(Assertion {
        assertion_id: assertion_id.to_vec(),
        assertion_version: 1,
    }),
    adopter: "my-protocol".to_string(),
    l2_block_number: 12345,
    observed_at: Some(Timestamp::from(SystemTime::now())),
};

// Broadcast to all connected proxies
invalidation_tx.send(Ok(invalidation))?;
```

### 2. ShouldForward (Unary RPC)

**Direction:** Proxy → Sidecar (per-transaction query)

**Status:** Optional - not currently used by the proxy.

This method allows the proxy to query your sidecar for real-time verdict on whether a transaction should be forwarded. Currently, the proxy uses only the invalidation stream for caching denied fingerprints.

#### Request

```protobuf
message ShouldForwardRequest {
  Fingerprint fingerprint = 1;
}
```

#### Response

```protobuf
message ShouldForwardResponse {
  enum Verdict {
    UNKNOWN = 0;  // No information, proxy decides
    DENY = 1;     // Sidecar says definitely deny
    ALLOW = 2;    // Sidecar says definitely allow
  }
  Verdict verdict = 1;
  Assertion assertion = 2;  // If DENY, which assertion failed
}
```

For now, you can implement this as:

```rust
async fn should_forward(&self, request: Request<ShouldForwardRequest>)
    -> Result<Response<ShouldForwardResponse>, Status>
{
    // Return UNKNOWN to let proxy's cache decide
    Ok(Response::new(ShouldForwardResponse {
        verdict: Verdict::Unknown as i32,
        assertion: None,
    }))
}
```

## Fingerprint Construction

The fingerprint uniquely identifies a "class" of transactions. It's constructed from:

1. **Target**: The contract address being called (20 bytes)
2. **Selector**: First 4 bytes of the function signature hash
3. **Arg Hash**: `keccak256(abi.encode(args))[0:16]` - first 16 bytes
4. **Value Bucket**: Logarithmic bucketing of ETH value
   - 0 → no ETH sent
   - 1-10 → bucket 1
   - 11-100 → bucket 2
   - etc.
5. **Gas Bucket**: Rounded to nearest 10,000

The 32-byte **hash** is: `keccak256(target || selector || arg_hash16 || value_bucket || gas_bucket)`

### Example: Computing Fingerprint Hash

```rust
use alloy_primitives::{keccak256, Address, U256};

fn compute_fingerprint_hash(
    target: Address,
    selector: [u8; 4],
    arg_hash16: [u8; 16],
    value_bucket: u64,
    gas_bucket: u32,
) -> [u8; 32] {
    let mut data = Vec::with_capacity(20 + 4 + 16 + 8 + 4);
    data.extend_from_slice(target.as_slice());
    data.extend_from_slice(&selector);
    data.extend_from_slice(&arg_hash16);
    data.extend_from_slice(&value_bucket.to_be_bytes());
    data.extend_from_slice(&gas_bucket.to_be_bytes());
    keccak256(&data).into()
}
```

## Running the Example

1. **Start the example sidecar server:**

```bash
cargo run --example sidecar_server
```

This starts a gRPC server on `0.0.0.0:50051` that simulates sending invalidation events every 10 seconds.

2. **Start the proxy (in another terminal):**

```bash
cargo run --bin rpc-proxy -- \
  --listen 0.0.0.0:9547 \
  --upstream http://127.0.0.1:8545 \
  --sidecar-endpoint http://127.0.0.1:50051
```

3. **Observe the logs:**

You should see:
- Sidecar: "Proxy connected to invalidation stream"
- Sidecar: "📤 Sending invalidation for fingerprint 0x..."
- Proxy: Receives and processes the invalidation, caches the fingerprint

## Implementation Checklist

When implementing your sidecar server:

- [ ] Implement `RpcProxyHeuristics` service trait
- [ ] Set up broadcast channel for invalidations (`tokio::sync::broadcast`)
- [ ] Compute fingerprints correctly (see above algorithm)
- [ ] Stream invalidations when transactions fail assertions
- [ ] Handle multiple connected proxies (broadcast to all)
- [ ] Implement reconnection logic (proxy retries with exponential backoff)
- [ ] Add metrics for invalidations sent
- [ ] Test with the example proxy

## Error Handling

### Connection Failures

The proxy implements exponential backoff when the sidecar connection fails:
- Initial retry: 1 second
- Max retry delay: 60 seconds
- Automatic reconnection on stream disconnect

Your sidecar should:
- Accept multiple simultaneous proxy connections
- Gracefully handle disconnections
- Not block on slow receivers (use broadcast channels)

### Invalid Messages

If your sidecar sends malformed invalidations:
- The proxy logs a warning and continues processing other events
- The stream continues (doesn't disconnect)
- Fix your sidecar implementation and redeploy

## Metrics

The proxy exposes these metrics related to sidecar communication:

| Metric | Type | Description |
|--------|------|-------------|
| `rpc_proxy_sidecar_reconnect_total` | Counter | Number of successful reconnections |
| `rpc_proxy_sidecar_connection_errors_total` | Counter | Number of connection failures |
| `rpc_proxy_invalidations_total` | Counter | Number of invalidations processed |

## Performance Considerations

### Broadcast Channel Capacity

Default: 100 pending invalidations

If your sidecar sends invalidations faster than proxies can process:
- Oldest messages are dropped (lagged subscribers)
- Increase channel capacity if needed
- Consider batching if you're sending thousands/sec

### Stream Efficiency

- Use server-streaming (not unary RPC per invalidation)
- Proxy maintains single long-lived connection
- No polling overhead
- Sub-millisecond propagation time

### Memory Usage

Each invalidation is ~200 bytes. With 100-message channel capacity:
- Per-proxy memory: ~20KB
- With 10 proxies: ~200KB

## Security Considerations

### Authentication

The current implementation does NOT include authentication. For production:

- Use mTLS (mutual TLS) for authentication
- Configure tonic with TLS certificates:

```rust
Server::builder()
    .tls_config(ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .client_ca_root(Certificate::from_pem(ca_cert)))?
    .add_service(service)
    .serve(addr)
    .await?;
```

### Rate Limiting

If you're concerned about malicious proxies:

- Implement per-connection rate limiting in your sidecar
- Track invalidation send rate
- Disconnect clients that request too frequently

## Troubleshooting

### Proxy can't connect

```
failed to connect to sidecar: connection refused
```

**Fix:** Ensure sidecar is running and listening on the correct address.

### Proxy disconnects immediately

```
sidecar stream error: status: Unimplemented
```

**Fix:** Implement the `StreamInvalidations` RPC method in your server.

### Invalidations not being cached

Check proxy logs for:
```
failed to process invalidation event: <error>
```

Common causes:
- Missing fingerprint or assertion fields
- Invalid byte lengths (hash must be 32 bytes, etc.)
- Selector not 4 bytes or arg_hash16 not 16 bytes

## Advanced: Assertion Version Updates

When you update an assertion, send an invalidation with the new version:

```rust
// Assertion v1 denied some fingerprints
// Now assertion v2 fixes the logic

// Send invalidation with new version
let invalidation = Invalidation {
    assertion: Some(Assertion {
        assertion_id: same_assertion_id,  // Same ID
        assertion_version: 2,              // New version!
    }),
    // ... same fingerprint ...
};
```

The proxy will:
1. Invalidate all cached fingerprints for assertion v1
2. Re-check them against the new assertion
3. This allows fixing false positives automatically

## References

- **Proto definition**: `proto/heuristics.proto`
- **Example server**: `examples/sidecar_server.rs`
- **Proxy client code**: `src/sidecar/grpc.rs`
- **Integration tests**: `tests/integration.rs`
- **Design rationale**: `HEURISTICS.md`
