# Transport

The transport module handles communication between the driver (external system) and the core engine. It provides an
abstraction layer that supports multiple transport mechanisms while maintaining a consistent interface.

## Architecture

The transport system consists of:

- **Transport Trait**: Defines the interface that all transports must implement
- **HTTP Transport**: JSON-RPC 2.0 over HTTP (default)
- **gRPC Transport**: Protocol Buffers over gRPC
- **Message Queue**: Internal channel for communication with the core engine
- **Decoder**: Converts transport-specific messages to internal queue format

## Transport Implementations

### HTTP Transport

The HTTP transport provides a JSON-RPC 2.0 interface running on port 8080 by default.

**[See HTTP Transport Documentation](http/README.md)** for detailed API reference, transaction examples, and
implementation details.

### gRPC Transport

The gRPC transport provides a high-performance binary protocol for communication.

**[See gRPC Transport Documentation](grpc/README.md)** (currently under active development) for detailed API reference
and implementation details.

## Choosing a Transport

- **HTTP/JSON-RPC**: Best for debugging, human readability, and simple integrations
- **gRPC**: Best for high-performance, production deployments with tight latency requirements
- **Mock**: For testing purposes only

Both transports provide identical functionality and semantics.

## Core API Overview

Both transports implement the same core API methods:

### Transaction Management

- **`sendTransactions`**: Submit batch of transactions for execution
- **`getTransactions`**: Retrieve transaction results by their hashes
- **`getTransaction`**: Retrieve a single transaction result by hash
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

**Purpose**: TxExecutionId allows tracking transactions across multiple block creation attempts by the sequencer. When a
sequencer creates multiple candidate blocks (iterations), only one will ultimately be selected. The `iteration_id`
differentiates between transactions in different candidate blocks for the same block number.

**Note**: The `iteration_id` is arbitrarily chosen by the sequencer and should not be assumed to be sequential. The
sequencer will never use 0 as an iteration_id.

**Example**: If a sequencer creates three candidate blocks for block 100, they might use `iteration_id` values like 42,
1001, and 2050. The sidecar can validate transactions from all three attempts, even though only one block will be
finalized.

### Transaction Structure

When sending transactions via `sendTransactions`, each transaction consists of:

- `tx_execution_id`: The execution identifier (TxExecutionId)
- `tx_env`: The transaction environment data (TxEnv)

## Long-Polling Semantics

The `getTransactions` and `getTransaction` methods use long-polling:

- Requests will block until results are available
- Configure appropriate timeouts on your client
- For transactions not yet received, the response includes them in the `not_found` array

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
use sidecar::engine::queue::{CommitHead, NewIteration, QueueTransaction, TxExecutionId};
use tracing::Span;

#[derive(Debug)]
pub enum TxQueueContents {
    CommitHead(CommitHead, tracing::Span),
    NewIteration(NewIteration, tracing::Span),
    Tx(QueueTransaction, tracing::Span),
    Reorg(TxExecutionId, tracing::Span),
}
```

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

The gRPC transport is currently in development. Configuration options will be added as the implementation progresses.

### Health Endpoint Server

- **Bind Address**:
    - **Default**: `0.0.0.0:8080`
- Responds on `/health` with the current sidecar status.
