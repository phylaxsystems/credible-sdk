# HTTP Transport

The HTTP transport provides a JSON-RPC 2.0 interface for communication between the driver and sidecar. It runs on port
8080 by default.

## Architecture

The HTTP transport consists of:

- **Server**: Axum-based HTTP server accepting JSON-RPC requests
- **Decoder**: Converts JSON-RPC messages to internal queue format (shared with parent transport module)
- **Tracing**: Request/response logging and monitoring
- **Block Context**: Block environment context management
- **Transaction Results**: Transaction result tracking and querying

## Endpoint

The HTTP transport exposes a single endpoint for all JSON-RPC requests:

- **URI**: `/tx`
- **Method**: `POST`
- **Content-Type**: `application/json`

## Message Formats

### Request Format

All JSON-RPC requests follow this structure:

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "methodName",
  "params": {
    /* method-specific parameters */
  }
}
```

### Response Format

Successful responses return results in this format:

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    /* method-specific result */
  }
}
```

### Error Format

Error responses include an error object with code and message:

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "error": {
    "code": -32000,
    "message": "Error description",
    "data": {
      /* optional additional error details */
    }
  }
}
```

## Core API Methods

The Core API provides essential methods for transaction execution and block management.

### `sendBlockEnv`

Sends block environment data to the sidecar. This must be called before any transactions can be processed, as the
sidecar needs to know which block it's building on top of.

**Request:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "sendBlockEnv",
  "params": {
    "number": 12345,
    "beneficiary": "0x742d35Cc6634C0532925a3b844B9c7e07e3E23eF4",
    "timestamp": 1625150400,
    "gas_limit": 30000000,
    "basefee": 1000000000,
    "difficulty": "0x0",
    "prevrandao": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    "blob_excess_gas_and_price": {
      "excess_blob_gas": 0,
      "blob_gasprice": 1
    },
    "last_tx_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
    "n_transactions": 100,
    "selected_iteration_id": 0
  }
}
```

**Response:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "status": "accepted",
    "request_count": 1,
    "message": "BlockEnv received"
  }
}
```

_Note: A successful response only means that the blockEnv has been received by the sidecar but may not be active yet as
it gets queued for the core engine to process the event._

### `sendTransactions`

Sends batch of transactions to sidecar for processing. Must include at least one transaction. Blocked until a BlockEnv
call is received, as we need information about what block we are executing txs on top of.

**Request:**

```json
{
  "id": 2,
  "jsonrpc": "2.0",
  "method": "sendTransactions",
  "params": {
    "transactions": [
      {
        "txEnv": {
          /* TxEnv object */
        },
        "txExecutionId": {
          "block_number": 1000,
          "iteration_id": 1,
          "tx_hash": "0x1234567890abcdef..."
        }
      }
      // Additional transactions...
    ]
  }
}
```

Each transaction in the array contains:

- `txEnv`: The transaction environment object (see [Transport docs](../README.md#transaction-types) for field reference
  and examples)
- `hash`: The transaction hash as a hex string

For detailed examples of all transaction types (Legacy, EIP-2930, EIP-1559, EIP-4844, EIP-7702), see
the [Transport Transaction Types Documentation](../README.md#transaction-examples).

**Response (success):**

```json
{
  "id": 2,
  "jsonrpc": "2.0",
  "result": {
    "status": "accepted",
    "request_count": 3,
    "message": "Successfully processed 3 requests"
  }
}
```

### `reorg`

Reorg the last sent transaction. Hash of the last sent transaction must be the same as `removedTxHash`.

**Request:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "reorg",
  "params": {
    "txExecutionId": {
      "block_number": 1000,
      "iteration_id": 1,
      "tx_hash": "0x1234567890abcdef..."
    }
  }
}
```

**Response:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "status": "accepted",
    "message": "Reorg processed"
  }
}
```

### `getTransactions` & `getTransaction`

Retrieves transaction results by hash. Can retrieve one or many transactions at once. Uses long-polling semantics.
Responds when all requested transactions are available. `results` field contains all transactions we have results for,
`not_found` contains txhashes the sidecar does not have stored.

**Request:**

```json
{
  "id": 3,
  "jsonrpc": "2.0",
  "method": "getTransactions",
  "params": [
    {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xabcd1234567890abcdef..."
      }
    },
    {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xefgh5678901234567890..."
      }
    },
    {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xijkl9012345678901234..."
      }
    }
  ]
}
```

**Response:**

```json
{
  "id": 3,
  "jsonrpc": "2.0",
  "result": {
    "results": [
      {
        "txExecutionId": {
          "block_number": 1000,
          "iteration_id": 1,
          "tx_hash": "0xabcd1234567890abcdef..."
        },
        "status": "success",
        "gas_used": 21000,
        "error": null
      },
      {
        "txExecutionId": {
          "block_number": 1000,
          "iteration_id": 1,
          "tx_hash": "0xefgh5678901234567890..."
        },
        "status": "assertion_failed",
        "gas_used": 18500,
        "error": null
      },
      {
        "txExecutionId": {
          "block_number": 1000,
          "iteration_id": 1,
          "tx_hash": "0xijkl9012345678901234..."
        },
        "status": "failed",
        "gas_used": null,
        "error": null
      }
    ],
    "not_found": []
  }
}
```

#### `getTransaction`

Same as `getTransactions`, but only works for one tx hash and returns a single result payload.

**Request:**

```json
{
  "id": 4,
  "jsonrpc": "2.0",
  "method": "getTransaction",
  "params": [
    {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xabcd1234567890abcdef..."
      }
    }
  ]
}
```

**Response (found):**

```json
{
  "id": 4,
  "jsonrpc": "2.0",
  "result": {
    "result": {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xabcd1234567890abcdef..."
      },
      "status": "success",
      "gas_used": 21000,
      "error": null
    }
  }
}
```

**Response (not found):**

```json
{
  "id": 4,
  "jsonrpc": "2.0",
  "result": {
    "not_found": {
      "txExecutionId": {
        "block_number": 1000,
        "iteration_id": 1,
        "tx_hash": "0xabcd1234567890abcdef..."
      }
    }
  }
}
```

**Transaction Status Values:**

- `"success"` - completed successfully, assertions passed. Also includes reverting transactions
- `"assertion_failed"` - transaction execution completed successfully, but assertions failed
- `"failed"` - execution failed (internal error, does not concern reverted transactions)
- `"reverted"` - transaction executed but reverted
- `"halted"` - transaction halted (invalid opcode, out of gas, etc...)

## Validation and Error Handling

**Common Decoder Errors:**

These are the actual errors returned by the transport decoder:

- `InvalidHash`: Transaction hash is not a valid 32-byte hex string
- `SchemaError`: JSON structure doesn't match expected format (e.g., invalid TxEnv deserialization)
- `MissingParams`: Required parameters are missing from the JSON-RPC request
- `NoTransactions`: Empty transactions array in `sendTransactions` request
- `InvalidAddress`: Invalid Ethereum address format
- `InvalidHex`: Invalid hexadecimal value in transaction data

**Transaction Type Validation Errors:**

When auto-deriving transaction types, these errors may occur:

- `MissingTargetForEip4844`: Type 3 (blob) transaction without a valid `kind` address (contract creation not allowed)
- `MissingTargetForEip7702`: Type 4 (EIP-7702) transaction without a valid `kind` address (contract creation not
  allowed)

## Error Codes

| Code   | Message                       | Description                            |
|--------|-------------------------------|----------------------------------------|
| -32000 | Transaction validation failed | Transaction execution failed           |
| -32001 | No BlockEnv                   | Received a transaction but no blockenv |
| -32002 | Invalid block data            | Block data validation failed           |
| -32003 | Invalid transaction           | Transaction data validation failed     |
| -32004 | State not found               | Requested state unavailable            |

## Configuration

The HTTP transport can be configured via command-line arguments or environment variables. See [
`src/args/mod.rs`](../../args/mod.rs) for the complete configuration structure.

### Bind Address

- **CLI Flag**: `--transport.bind-addr <address:port>`
- **Environment Variable**: `TRANSPORT_BIND_ADDR`
- **Default**: `0.0.0.0:50051`
- **Example**: `--transport.bind-addr 0.0.0.0:9000` or `TRANSPORT_BIND_ADDR=0.0.0.0:9000`
