# gRPC Transport

High-performance gRPC transport for the Credible Layer sidecar.

## Features

- **Binary Protocol**: Efficient protobuf serialization
- **Streaming Support**: Handle high-throughput transaction streams
- **Type Safety**: Generated Rust code from protobuf definitions
- **Compression**: Built-in gzip compression support
- **Multiplexing**: Multiple concurrent requests over single connection

## API Methods

### Core Methods

- `SendTransactions` - Send batch of transactions for processing
- `SendBlockEnv` - Set block environment context
- `GetTransactions` - Query transaction processing results
- `RevertTransaction` - Signal transaction reversion (from Besu plugins)
- `Health` - Health check endpoint

### Streaming Methods

- `SendTransactionsStream` - High-throughput streaming transaction submission

## Configuration

```toml
[transport.grpc]
bind_addr = "127.0.0.1:9090"
max_message_size = 16777216  # 16MB
enable_compression = true
connection_timeout_secs = 30
keep_alive_interval_secs = 30
```

## Usage Examples

### Basic Client (Any Language)

```bash
# Using grpcurl for testing
grpcurl -plaintext -d '{
  "transactions": [{
    "hash": "0x...",
    "tx_env": {
      "caller": "0x...",
      "gas_limit": 21000,
      "gas_price": "1000000000",
      "value": "0",
      "data": "0x",
      "nonce": 1,
      "chain_id": 1
    }
  }]
}' localhost:9090 sidecar.SidecarService/SendTransactions
```

### Rust Client

```rust
use tonic::Request;
use sidecar::sidecar_service_client::SidecarServiceClient;

let mut client = SidecarServiceClient::connect("http://127.0.0.1:9090").await?;

let request = Request::new(SendTransactionsRequest {
    transactions: vec![/* ... */],
});

let response = client.send_transactions(request).await?;
println!("Response: {:?}", response.into_inner());
```

### Java Client (for Besu plugins)

```java
SidecarServiceGrpc.SidecarServiceBlockingStub client =
    SidecarServiceGrpc.newBlockingStub(channel);

SendTransactionsRequest request = SendTransactionsRequest.newBuilder()
    .addTransactions(/* ... */)
    .build();

SendTransactionsResponse response = client.sendTransactions(request);
```

## Performance Benefits over HTTP

- **~2-5x faster** serialization with protobuf vs JSON
- **Connection reuse** - no HTTP overhead per request
- **Streaming support** - handle thousands of transactions per second
- **Type safety** - compile-time API validation
- **Compression** - automatic gzip compression for large payloads

## Protocol Buffer Schema

See `sidecar.proto` for the complete API definition.

## Error Handling

gRPC provides structured error codes:

- `FAILED_PRECONDITION` - No block environment set
- `INVALID_ARGUMENT` - Malformed transaction data
- `INTERNAL` - Server-side processing errors
- `UNAVAILABLE` - Server overloaded or down
