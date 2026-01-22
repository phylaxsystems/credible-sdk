# Transport

The transport module handles communication between the driver (external system) and the core engine. It provides an
abstraction layer that supports multiple transport mechanisms while maintaining a consistent interface.

## Architecture

The transport system consists of:

- **Transport Trait**: Defines the interface that all transports must implement
- **HTTP Transport**: JSON-RPC 2.0 over HTTP (default)
- **gRPC Transport**: Protocol Buffers over gRPC with streaming support
- **Message Queue**: Internal channel for communication with the core engine
- **Decoder**: Converts transport-specific messages to internal queue format

## Transport Implementations

### HTTP Transport

The HTTP transport provides a JSON-RPC 2.0 interface running on port 8080 by default.

**[See HTTP Transport Documentation](http/README.md)** for detailed API reference, transaction examples, and
implementation details.

### gRPC Transport

The gRPC transport provides a high-performance binary protocol with bidirectional streaming support.

**[See gRPC Transport Documentation](grpc/README.md)** for detailed API reference and implementation details.

#### Streaming Architecture

The gRPC transport uses a streaming-first architecture for optimal performance:

- **`StreamEvents`**: Bidirectional stream for sending events (commits, iterations, transactions, reorgs). The client
  sends events and receives acknowledgments for each event processed. Each event includes a `event_id` that is echoed
  back in the corresponding `StreamAck` for explicit request-response matching.
- **`SubscribeResults`**: Server-push stream for receiving transaction results as they complete. Results are pushed
  immediately when transactions finish executing.

#### Event Types

The `StreamEvents` RPC accepts the following event types:

| Event          | Description                                                                            |
|----------------|----------------------------------------------------------------------------------------|
| `CommitHead`   | Signals the start of a new block building round. Must be sent before any other events. |
| `NewIteration` | Initializes building for a new iteration ID with block environment.                    |
| `Transaction`  | Submits a transaction for execution.                                                   |
| `ReorgEvent`   | Signals a chain reorganization.                                                        |

#### Event Structure

Each event sent via `StreamEvents` is wrapped in an `Event` message containing:

- `event_id`: A client-provided `uint64` identifier for request-response matching
- One of the event types above (`commit_head`, `new_iteration`, `transaction`, or `reorg`)

#### Stream Acknowledgments

Each event processed by the server generates a `StreamAck` response containing:

| Field              | Type     | Description                                            |
|--------------------|----------|--------------------------------------------------------|
| `success`          | `bool`   | Whether the event was processed successfully           |
| `message`          | `string` | Error message (if `success` is false) or info message  |
| `events_processed` | `uint64` | Total number of events processed so far in this stream |
| `event_id`         | `uint64` | The `event_id` from the corresponding `Event`          |

The `event_id` allows clients to match acknowledgments to their original requests, which is especially useful when
sending events concurrently or when network ordering is not guaranteed.

#### Result Subscription

The `SubscribeResults` RPC provides real-time transaction results:

- Results are pushed immediately as transactions complete execution
- Optional `from_block` filter to receive only results from a specific block number onwards
- Multiple subscribers supported—each receives all results independently
- Handles slow subscribers gracefully with configurable buffer (lagged subscribers receive a warning)

## Choosing a Transport

| Use Case                    | Recommended Transport |
|-----------------------------|-----------------------|
| Debugging and development   | HTTP/JSON-RPC         |
| Human-readable requests     | HTTP/JSON-RPC         |
| Simple integrations         | HTTP/JSON-RPC         |
| High-performance production | gRPC                  |
| Real-time result streaming  | gRPC                  |
| Tight latency requirements  | gRPC                  |
| Testing                     | Mock                  |

Both transports provide identical functionality and semantics for core operations.

## Core API Overview

Both transports implement the same core API methods:

### Transaction Management

- **`sendTransactions`**: Submit transactions for execution
- **`getTransactions`**: Retrieve transaction results by their execution IDs
- **`getTransaction`**: Retrieve a single transaction result
- **`reorg`**: Handle chain reorganizations

### State Synchronization

- **`getAccounts`**: Retrieve account information
- **`getStorage`**: Retrieve storage values
- **`getBlockHashes`**: Retrieve block hashes by number
- **`getCodeByHash`**: Retrieve contract code by hash

For detailed API specifications and examples, refer to the transport-specific documentation linked above.

## Transaction Identification

### TxExecutionId

Every transaction in the sidecar is uniquely identified by a `TxExecutionId`:

- `block_number`: The block number this transaction belongs to
- `iteration_id`: An arbitrary identifier for this block creation attempt (chosen by the sequencer, never 0)
- `tx_hash`: The transaction hash (32-byte hex string)
- `index`: The index of the transaction in the block

**Purpose**: TxExecutionId allows tracking transactions across multiple block creation attempts by the sequencer. When a
sequencer creates multiple candidate blocks (iterations), only one will ultimately be selected. The `iteration_id`
differentiates between transactions in different candidate blocks for the same block number.

**Note**: The `iteration_id` is arbitrarily chosen by the sequencer and should not be assumed to be sequential. The
sequencer will never use 0 as an iteration_id.

**Example**: If a sequencer creates three candidate blocks for block 100, they might use `iteration_id` values like 42,
1001, and 2050. The sidecar can validate transactions from all three attempts, even though only one block will be
finalized.

### Transaction Structure

When sending transactions via `sendTransactions` or `StreamEvents`, each transaction consists of:

- `tx_execution_id`: The execution identifier (TxExecutionId)
- `tx_env`: The transaction environment data (TxEnv)
- `prev_tx_hash`: The hash of the previous transaction in the batch, or null for the first transaction

## Long-Polling vs Streaming Semantics

### HTTP Transport (Long-Polling)

The `getTransactions` and `getTransaction` methods use long-polling:

- Requests will block until results are available
- Configure appropriate timeouts on your client
- For transactions not yet received, the response includes them in the `not_found` array

### gRPC Transport (Streaming)

The `SubscribeResults` RPC provides real-time streaming:

- Results are pushed immediately as they become available
- No polling required—server pushes to client
- Supports filtering by `from_block` to skip historical results
- Connection stays open for continuous result delivery

The `GetTransactions` and `GetTransaction` unary RPCs are also available for one-off queries.

## Common Components

### Decoder

The decoder module (`decoder.rs`) is shared across transports and handles conversion from transport-specific formats to
internal queue messages. It supports:

- Transaction decoding with full TxEnv support
- Block environment decoding
- Reorg event handling
- Validation and error handling

### Message Queue

All transports communicate with the core engine through a unified message queue:

```rust, ignore
use sidecar::engine::queue::{CommitHead, NewIteration, QueueTransaction, ReorgRequest};

#[derive(Debug)]
pub enum TxQueueContents {
    CommitHead(CommitHead),
    NewIteration(NewIteration),
    Tx(QueueTransaction),
    Reorg(ReorgRequest),
}
```

## Transport Metrics

Both transports expose latency histograms so operators can watch request health in Prometheus/Grafana. Every handler
creates an `RpcRequestDuration` guard when the request starts and the guard records a value in
`sidecar_rpc_duration_*` when it drops:

| Transport | Metric Name                                 | Description                                                              |
|-----------|---------------------------------------------|--------------------------------------------------------------------------|
| HTTP      | `sidecar_rpc_duration_sendTransactions`     | JSON-RPC `sendTransactions` batches                                      |
| HTTP      | `sidecar_rpc_duration_sendEvents`           | JSON-RPC `sendEvents` batches                                            |
| HTTP      | `sidecar_rpc_duration_reorg`                | JSON-RPC `reorg` requests                                                |
| HTTP      | `sidecar_rpc_duration_getTransactions`      | JSON-RPC `getTransactions` long-polling calls                            |
| HTTP      | `sidecar_rpc_duration_getTransaction`       | JSON-RPC `getTransaction` calls                                          |
| Shared    | `sidecar_get_transaction_wait_duration`     | Time spent waiting for a transaction to be received while getTransaction |
| gRPC      | `sidecar_rpc_duration_StreamEvents`         | `StreamEvents` bidirectional streaming                                   |
| gRPC      | `sidecar_rpc_duration_GetTransactions`      | `GetTransactions` RPC                                                    |
| gRPC      | `sidecar_rpc_duration_GetTransaction`       | `GetTransaction` RPC                                                     |
| Shared    | `sidecar_fetch_transaction_result_duration` | Fetch + serialization latency for transaction results                    |

## Transaction Types

The sidecar supports all Ethereum transaction types through the `TxEnv` structure. This structure is used consistently
across all transport protocols (HTTP JSON-RPC, gRPC, etc.).

### Supported Types

- **Type 0 (Legacy)**: Original transaction format
- **Type 1 (EIP-2930)**: Access list transactions
- **Type 2 (EIP-1559)**: Fee market transactions with priority fees
- **Type 3 (EIP-4844)**: Blob transactions for data availability
- **Type 4 (EIP-7702)**: Transactions with authorization lists for EOA code

### TxEnv Field Reference

- `tx_type`: Transaction type (0-4). If omitted, will be auto-derived based on fields present
- `caller`: Transaction sender address (required)
- `gas_limit`: Maximum gas units for transaction (required)
- `gas_price`: Gas price in wei (for type 0,1) or max_fee_per_gas (for type 2+)
- `kind`: Target address for calls, or null/omitted for contract creation
- `value`: Wei amount to transfer (U256)
- `data`: Transaction data/calldata
- `nonce`: Transaction nonce
- `chain_id`: Chain ID (optional, but recommended)
- `access_list`: List of addresses and storage keys (type 1+, optional for type 2+)
- `gas_priority_fee`: Priority fee per gas (type 2+ only)
- `blob_hashes`: Blob versioned hashes (type 3 only)
- `max_fee_per_blob_gas`: Maximum fee per blob gas (type 3 only)
- `authorization_list`: List of authorizations (type 4 only)

**Best Practice**: Omit fields that are not relevant for the transaction type you're using. Don't include empty arrays,
null values, or zero values for fields that don't apply to your transaction type.

### Transaction Examples

These examples show the `TxEnv` structure for each transaction type. The exact wrapper format depends on your transport
protocol (JSON-RPC for HTTP, protobuf for gRPC).

#### Type 0: Legacy Transaction

Simple ETH transfer:

```json
{
  "tx_type": 0,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 21000,
  "gas_price": 20000000000,
  "kind": "0x8ba1f109551bD432803012645Ac136c9Ca2A1",
  "value": "1000000000000000000",
  "data": "0x",
  "nonce": 42,
  "chain_id": 1
}
```

Contract deployment:

```json
{
  "tx_type": 0,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 1000000,
  "gas_price": 20000000000,
  "kind": null,
  // null or omitted for contract creation
  "value": "0",
  "data": "0x608060405234801561001057600080fd5b50...",
  // Contract bytecode
  "nonce": 43,
  "chain_id": 1
}
```

#### Type 1: EIP-2930 Access List Transaction

```json
{
  "tx_type": 1,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 100000,
  "gas_price": 30000000000,
  "kind": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
  "value": "0",
  "data": "0xa9059cbb000000...",
  // transfer(address,uint256)
  "nonce": 44,
  "chain_id": 1,
  "access_list": [
    {
      "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      "storage_keys": [
        "0x0000000000000000000000000000000000000000000000000000000000000003",
        "0x0000000000000000000000000000000000000000000000000000000000000007"
      ]
    }
  ]
}
```

#### Type 2: EIP-1559 Fee Market Transaction

Simple transfer:

```json
{
  "tx_type": 2,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 21000,
  "gas_price": 50000000000,
  // max_fee_per_gas
  "kind": "0x8ba1f109551bD432803012645Ac136c9Ca2A1",
  "value": "2000000000000000000",
  "data": "0x",
  "nonce": 45,
  "chain_id": 1,
  "gas_priority_fee": 2000000000
  // max_priority_fee_per_gas
}
```

With access list (Type 2 supports all Type 1 features):

```json
{
  "tx_type": 2,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 100000,
  "gas_price": 50000000000,
  "kind": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "value": "0",
  "data": "0xa9059cbb000000...",
  "nonce": 46,
  "chain_id": 1,
  "gas_priority_fee": 2000000000,
  "access_list": [
    {
      "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      "storage_keys": [
        "0x0000000000000000000000000000000000000000000000000000000000000002"
      ]
    }
  ]
}
```

#### Type 3: EIP-4844 Blob Transaction

```json
{
  "tx_type": 3,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 210000,
  "gas_price": 50000000000,
  "kind": "0x8ba1f109551bD432803012645Ac136c9Ca2A1",
  // Must be a call
  "value": "0",
  "data": "0x",
  "nonce": 46,
  "chain_id": 1,
  "gas_priority_fee": 2000000000,
  "blob_hashes": [
    "0x01b0761f87b081d5cf10757ccc89f12be355c70e2e29df288b65b30710dcbcd1",
    "0x01d18b1c3a93b5f1a09f3545f69c2a0e5bc1f87e65b9c9bcf3d43e54a7e4e5f9"
  ],
  "max_fee_per_blob_gas": 3000000000
}
```

#### Type 4: EIP-7702 Authorization List Transaction

```json
{
  "tx_type": 4,
  "caller": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
  "gas_limit": 150000,
  "gas_price": 50000000000,
  "kind": "0x8ba1f109551bD432803012645Ac136c9Ca2A1",
  // Must be a call
  "value": "0",
  "data": "0x",
  "nonce": 47,
  "chain_id": 1,
  "gas_priority_fee": 2000000000,
  "authorization_list": [
    {
      "chain_id": 1,
      "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
      "nonce": 0,
      "v": 27,
      "r": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
      "s": "0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
    }
  ]
}
```

### Type Auto-Derivation

If `tx_type` is omitted, the system automatically derives it based on fields present (checked in order):

1. `authorization_list` present → Type 4 (EIP-7702)
2. `blob_hashes` or `max_fee_per_blob_gas` > 0 → Type 3 (EIP-4844)
3. `gas_priority_fee` present → Type 2 (EIP-1559)
4. `access_list` present (without gas_priority_fee) → Type 1 (EIP-2930)
5. Otherwise → Type 0 (Legacy)

### Value Encoding Notes

- **U256 values** (e.g., `value`): Can be numbers (up to safe integer), decimal strings, or hex strings
- **U128 values** (e.g., `gas_price`, `gas_priority_fee`): Similar encoding as U256
- **Addresses**: 20-byte hex strings with optional `0x` prefix
- **Hashes**: 32-byte hex strings with optional `0x` prefix
- **Data**: Hex strings with optional `0x` prefix

### gRPC Binary Encoding

For gRPC transport, numeric values are encoded as big-endian bytes for efficiency:

- **Addresses**: 20 bytes raw
- **Hashes**: 32 bytes raw
- **U128 values**: 16 bytes big-endian
- **U256 values**: 32 bytes big-endian

This eliminates hex encoding/decoding overhead for high-throughput scenarios.

## Configuration

Transport configuration can be set via command-line arguments or environment variables. See [
`src/args/mod.rs`](../args/mod.rs) for the complete list of configuration options.

### Transport Protocol Selection

- **CLI Flag**: `--transport.protocol <value>`
- **Environment Variable**: `TRANSPORT_PROTOCOL`
- **Values**: `http` or `grpc`
- **Default**: `grpc`

### HTTP Transport Configuration

- **Bind Address**:
    - **CLI Flag**: `--transport.bind-addr <address:port>`
    - **Environment Variable**: `TRANSPORT_BIND_ADDR`
    - **Default**: `0.0.0.0:50051`

### gRPC Transport Configuration

- **Bind Address**:
    - **CLI Flag**: `--transport.bind-addr <address:port>`
    - **Environment Variable**: `TRANSPORT_BIND_ADDR`
    - **Default**: `0.0.0.0:50051`

The gRPC transport uses optimized HTTP/2 settings for blockchain workloads.

### Health Endpoint Server

- **Bind Address**:
    - **Default**: `0.0.0.0:8080`
- Responds on `/health` with the current sidecar status.
