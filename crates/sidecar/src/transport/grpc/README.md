# gRPC Transport

The gRPC transport provides a high-performance binary protocol for communication between the driver and sidecar. **This
transport is currently under active development.**

## Protocol Definition

The complete gRPC service definition and message schemas are specified in [`sidecar.proto`](sidecar.proto).

## Core API Methods

The gRPC transport implements the `SidecarTransport` service with iteration lifecycle RPCs plus transaction/result APIs.

### `CommitHead`

Announces which iteration should be committed and provides metadata about the previously executed transactions.
Must be called before `NewIteration`.

- **Request:** `CommitHead`
- **Response:** `BasicAck`

### `NewIteration`

Starts building a new iteration by providing the block environment to target. Requires that the most recent request was a
`CommitHead`.

- **Request:** `NewIteration`
- **Response:** `BasicAck`

### `SendEvents`

Allows bundling multiple iteration events that must be applied sequentially. A typical new block announcement includes
`[CommitHead, NewIteration]`.

- **Request:** `SendEvents`
- **Response:** `BasicAck`

### `SendBlockEnv` (deprecated)

Legacy API that sends block environment and commit metadata in a single call. Prefer the event-based RPCs above.

- **Request:** `BlockEnvEnvelope`
- **Response:** `BasicAck`

### `SendTransactions`

Sends batch of transactions for processing. Requires prior BlockEnv.

- **Request:** `SendTransactionsRequest`
- **Response:** `SendTransactionsResponse`

### `Reorg`

Handles chain reorganization by removing specified transaction.

- **Request:** `ReorgRequest`
- **Response:** `BasicAck`

### `GetTransactions`

Retrieves transaction results by hash with long-polling support.

- **Request:** `GetTransactionsRequest`
- **Response:** `GetTransactionsResponse`

### `GetTransaction`

Retrieves a single transaction result by hash with long-polling support.

- **Request:** `GetTransactionRequest`
- **Response:** `GetTransactionResponse`
- **Response semantics:** Returns either `result` populated with a `TransactionResult` or `not_found` containing the
  original hash when the sidecar has no record of the request.

## Transaction Encoding

For comprehensive transaction type documentation and examples, see
the [Transport Transaction Types Documentation](../README.md#transaction-types).

### gRPC-Specific Encoding

Block data and transactions are encoded according to the protobuf schema:

- **CommitHead**: Carries iteration commit metadata (`last_tx_hash`, `n_transactions`, `selected_iteration_id`).
- **NewIteration**: Wraps a native `BlockEnv` message alongside the target iteration identifier.
- **Transaction** (in `SendEvents`): Shares the same layout as the standalone `Transaction` message so iteration batches
  can include queued transactions.
- **SendEvents**: Bundles a sequence of `CommitHead`, `NewIteration`, and `Transaction` messages for atomic submission.
- **BlockEnvEnvelope** *(deprecated)*: Legacy structure that combines commit metadata and block environment in one payload.
- **BlockEnv**: Uses strongly typed fields (e.g., `number`, `timestamp`, `gas_limit`) and string-encoded large
  integers (`difficulty`, `blob_gasprice`).

For transaction fields, protobuf still lacks native large-integer support, so:

- **U256/U128 values**: Encoded as decimal strings (e.g., `"1000000000000000000"`)
- **Addresses & Hashes**: Hex strings with optional `0x` prefix
- **Binary data**: Hex strings with optional `0x` prefix

The `CommitHead`, `NewIteration`, `SendEvents`, `TxExecutionId`, `BlockEnv`, and `TransactionEnv` messages in
[`sidecar.proto`](sidecar.proto) define the precise field layout and encoding expectations. `BlockEnvEnvelope` remains
for compatibility but is slated for removal.

## Example Usage

```rust
// Example: Sending a legacy transaction
let tx = Transaction {
tx_execution_id: Some(TxExecutionId {
block_number: 1234,
iteration_id: 2,
tx_hash: "0x1234...".to_string()
}),
tx_env: Some(TransactionEnv {
tx_type: 0, // Legacy
caller: "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4".to_string(),
gas_limit: 21000,
gas_price: "20000000000".to_string(), // U128 as decimal string
kind: "0x8ba1f109551bD432803012645Ac136c9Ca2A1".to_string(),
value: "1000000000000000000".to_string(), // U256 as decimal string
data: "0x".to_string(),
nonce: 42,
chain_id: Some(1),
..Default::default ()
}
};

let response = client.send_transactions(SendTransactionsRequest {
transactions: vec![tx],
}).await?;
```

## Configuration

The gRPC transport configuration is currently under development. Configuration options will be added as the
implementation progresses.
