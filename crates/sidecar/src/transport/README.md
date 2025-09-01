# Transport API

This document contains the API endpoints drivers/transports should implement to interface with the sidecar.
For docs regarding the transport trait and how/when to implement it, please consult the rustdoc.

# The API

Below you'll find what API methods the driver or sidecar should implement and how the response should be structured.
All methods below are formatted in JSON for ease of implementation, but other encodings, like CBOR, can be used.

The URI for the request is `/tx`.

## Message formats

### Request Format

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

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "error": {
    "code": -32000,
    "message": "Error description"
    // Optional
  }
}
```

## Core Driver API Methods

The following methods are for the core driver to send to the transport of the sidecar. When the driver starts working on
a new block it should send the sidecar a new `sendBlockEnv` and then it should send `sendTransactions` events with
transactions to be included in the block.

### `sendBlockEnv`

Marks the start of a new block and end of the previous. Sends block environment to sidecar.

**Request:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "method": "sendBlockEnv",
  "params": {
    "blockEnv": {
      "number": 12345,
      "beneficiary": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
      "timestamp": 1692816000,
      "gas_limit": 30000000,
      "basefee": 10000000,
      "difficulty": "0x0",
      "prevrandao": "0x1234567890abcdef...",
      "blob_excess_gas_and_price": {
        "excess_blob_gas": 1000,
        "blob_gasprice": 2000
      }
    }
  }
}
```

The field `blob_excess_gas_and_price` is only required for specifications Cancun or later.

**Response:**

```json
{
  "id": 1,
  "jsonrpc": "2.0",
  "result": {
    "success": true
  }
}
```

*Note: A successful response only means that the blockEnv has been received by the sidecar but may not be active yet as
it gets queued for the core engine to process the event.*

### `sendTransactions`

Sends batch of transactions to sidecar for processing. Must include at least one transaction. Blocked until a BlockEnv
call is received, as we need information about what block we are executing txs on top of. The full `TxEnv` struct must
be sent for each transaction. This is the minimum amount of data we need to execute a transaction. The transaction`hash`
is sent along with a `TxEnv` because we cannot construct a hash without a signature, which we dont need/want.

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
          "caller": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
          "gas_limit": 21000,
          "gas_price": "1000",
          "transact_to": "0x8ba1f109551bD432803012645Hac136c2D29",
          "value": "0x0",
          "data": "0x",
          "nonce": 42,
          "chain_id": 1,
          "access_list": []
        },
        "hash": "0xabcd1234567890abcdef..."
      },
      {
        "txEnv": {
          "caller": "0x8ba1f109551bD432803012645Hac136c2D29",
          "gas_limit": 50000,
          "gas_price": "2000",
          "value": "0x1bc16d674ec80000",
          "data": "0x60806040...",
          "nonce": 1,
          "chain_id": 1,
          "access_list": []
        },
        "hash": "0xefgh5678901234567890..."
      },
      {
        "txEnv": {
          "caller": "0x8ba1f109551bD432803012645Hac136c2D29",
          "gas_limit": 50000,
          "gas_price": "2000",
          "transact_to": "0x",
          "value": "0x1bc16d674ec80000",
          "data": "0x60806040...",
          "nonce": 1,
          "chain_id": 1,
          "access_list": []
        },
        "hash": "0xabcdef1234567890abcdef..."
      }
    ]
  }
}
```

The `transact_to` can be null, `0x` or "" if the transaction is a contract creation.

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

### `getTransactions`

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
    "0xabcd1234567890abcdef...",
    "0xefgh5678901234567890...",
    "0xijkl9012345678901234..."
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
        "hash": "0xabcd1234567890abcdef...",
        "status": "success",
        "gas_used": 21000,
        "error": null
      },
      {
        "hash": "0xefgh5678901234567890...",
        "status": "assertion_failed",
        "gas_used": 18500,
        "error": null
      },
      {
        "hash": "0xijkl9012345678901234...",
        "status": "failed",
        "gas_used": null,
        "error": null
      }
    ],
    "not_found": []
  }
}
```

**Transaction Status Values:**

- `"success"` - completed successfully, assertions passed. Also includes reverting transactions
- `"assertion_failed"` - transaction execution completed successfully, but assertions failed
- `"failed"` - execution failed (internal error, does not concern reverted transactions)
- `"reverted"` - transaction, executed but reverted
- `"halted"` - transaction halted (invalid opcode, out of gas, etc...)

### Semantics on message broadcast transports

When we cannot long wait for a `getTransactions` response, the flow of the call look like this:

```json
{
  "id": 2,
  "jsonrpc": "2.0",
  "method": "getTransactions",
  "params": {
    "hashes": [
      ...
    ]
  }
}
// ... waits ...
{
  "id": 2,
  "result": {
    "results": [
      ...
    ],
    "not_found": []
  }
}
```

`getTransactions` **should not** be used for pings to the sidecar/sequencer. Instead dedicated messages for this should
be defined according to transport needs.

## State Sync API

Provides state access with signatures identical to revm DatabaseRef interface. When the sidecar is missing state (state
not present in the sidecar evm cache), it should call into the driver(sequencer) with the methods below, with the driver
responding.

### `getAccounts`

Retrieves account information for multiple addresses.

**Request:**

```json
{
  "id": 4,
  "jsonrpc": "2.0",
  "method": "getAccounts",
  "params": {
    "addresses": [
      "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
      "0x8ba1f109551bD432803012645Hac136c2D29"
    ]
  }
}
```

**Response:**

```json
{
  "id": 4,
  "jsonrpc": "2.0",
  "result": {
    "accounts": [
      {
        "address": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
        "balance": "0x1bc16d674ec80000",
        "nonce": 42,
        "code_hash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        "code": "0x608060405234801561001057600080fd5b50..."
      },
      {
        "address": "0x8ba1f109551bD432803012645Hac136c2D29",
        "balance": "0x0",
        "nonce": 0,
        "code_hash": null,
        "code": null
      }
    ]
  }
}
```

### `getStorage`

Retrieves storage values for multiple address/slot combinations.

**Request:**

```json
{
  "id": 5,
  "jsonrpc": "2.0",
  "method": "getStorage",
  "params": {
    "requests": [
      {
        "address": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
        "slot": "0x0000000000000000000000000000000000000000000000000000000000000001"
      },
      {
        "address": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
        "slot": "0x0000000000000000000000000000000000000000000000000000000000000002"
      }
    ]
  }
}
```

**Response:**

```json
{
  "id": 5,
  "jsonrpc": "2.0",
  "result": {
    "storage": [
      {
        "address": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
        "slot": "0x0000000000000000000000000000000000000000000000000000000000000001",
        "value": "0x0000000000000000000000000000000000000000000000000000000000000042"
      },
      {
        "address": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
        "slot": "0x0000000000000000000000000000000000000000000000000000000000000002",
        "value": "0x0000000000000000000000000000000000000000000000000000000000000000"
      }
    ]
  }
}
```

### `getBlockHashes`

Retrieves block hashes for multiple block numbers.

**Request:**

```json
{
  "id": 6,
  "jsonrpc": "2.0",
  "method": "getBlockHashes",
  "params": {
    "numbers": [
      12344,
      12343,
      12342
    ]
  }
}
```

**Response:**

```json
{
  "id": 6,
  "jsonrpc": "2.0",
  "result": {
    "hashes": [
      {
        "number": 12344,
        "hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
      },
      {
        "number": 12343,
        "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
      },
      {
        "number": 12342,
        "hash": "0x567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234"
      }
    ]
  }
}
```

### `getCodeByHash`

Retrieves bytecode for multiple code hashes.

**Request:**

```json
{
  "id": 7,
  "jsonrpc": "2.0",
  "method": "getCodeByHash",
  "params": {
    "code_hashes": [
      "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
      "0x60806040523480156100115760006000fd5b50600436106100465760e060020a60003504..."
    ]
  }
}
```

**Response:**

```json
{
  "id": 7,
  "jsonrpc": "2.0",
  "result": {
    "bytecodes": [
      {
        "code_hash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        "bytecode": "0x"
      },
      {
        "code_hash": "0x60806040523480156100115760006000fd5b50600436106100465760e060020a60003504...",
        "bytecode": "0x608060405234801561001057600080fd5b50600436106100465760e060020a60003504631c31f7a081146100485780634e70b1dc1461005c575b60006000fd5b61005a6004803603602081101561005e57600080fd5b810190808035906020019092919050505061007a565b005b34801561006857600080fd5b5061007161007e565b60405180821515815260200191505060405180910390f35b8060008190555050565b60008060005414905090565b00"
      }
    ]
  }
}
```

## Error Codes

| Code   | Message                       | Description                            |
|--------|-------------------------------|----------------------------------------|
| -32000 | Transaction validation failed | Transaction execution failed           |
| -32001 | No BlockEnv                   | Received a transaction but no blockenv |
| -32002 | Internal error                | Unexpected sidecar error               |
