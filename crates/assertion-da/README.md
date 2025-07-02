# `assertion-da`

Assertion DA functions as a temporary data availability layer for the Phylax Credible layer. It stores and makes assertions publicly available via a JSON-RPC interface.

## Running the `assertion-da`

The assertion-da server is part of the credible-sdk monorepo. To run it, you have several options:

### Using Make (Recommended)

```bash
# Run with Docker using the latest published image
make compose

# Run with Docker for development (includes debug_assertions feature)
make compose-dev

# Build Docker image from source
make docker-build

# Build Docker image for development
make docker-build-dev
```

### Using Cargo

```bash
# Run directly from the monorepo root
cargo run --release --bin assertion-da -- --private-key <PRIVATE_KEY>

# Run with debug assertions enabled
cargo run --release --bin assertion-da --features debug_assertions -- --private-key <PRIVATE_KEY>

# Build the binary
cargo build --release --bin assertion-da
```

### Using Docker Compose

```bash
# Set the required environment variable
export DA_PRIVATE_KEY=0x...

# Run the service
docker compose -f etc/docker-compose.yaml up

# Or for development (allows bytecode submissions)
docker compose -f etc/docker-compose-dev.yaml up
```

**Note:** The `private-key` flag is required so the Assertion DA can sign the Assertion bytecode.

### Usage

```bash
Usage: assertion-da [OPTIONS] --private-key <PRIVATE_KEY>

Options:
      --db-path <DB_PATH>          Path of the database, defaults to /usr/local/assertions [env: DA_DB_PATH=]
      --cache-size <CACHE_SIZE>    Cache size in bytes [env: DA_CACHE_SIZE=] [default: 1000000]
      --listen-addr <LISTEN_ADDR>  Api server address [env: DA_LISTEN_ADDR=] [default: 127.0.0.1:5001]
      --private-key <PRIVATE_KEY>  Private key for the assertion DA [env: DA_PRIVATE_KEY=0x...]
      --log-level <LOG_LEVEL>      Log level [env: DA_LOG_LEVEL=] [default: info]
  -h, --help                       Print help
  -V, --version                    Print version
```

### Tracing subsriber env vars

- `OTEL_EXPORTER_OTLP_ENDPOINT` - optional. The endpoint to send traces to,
  should be some valid URL. If not specified, then [`OtelConfig::load`]
  will return [`None`].
- `OTEL_LEVEL` - optional. Specifies the minimum [`tracing::Level`] to
  export. Defaults to [`tracing::Level::DEBUG`].
- `OTEL_TIMEOUT` - optional. Specifies the timeout for the exporter in
  **milliseconds**. Defaults to 1000ms, which is equivalent to 1 second.
- `OTEL_ENVIRONMENT_NAME` - optional. Value for the `deployment.environment.
  name` resource key according to the OTEL conventions.
- `TRACING_METRICS_PORT` - Which port to bind the the exporter to. If the variable is missing or unparseable, it defaults to 9000.
- `TRACING_LOG_JSON` - If set, will enable JSON logging.

To view the tracing you need a tracing collector. For example use jager like so:
```
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

And view on `http://localhost:16686`.

## Development Setup

For development purposes, you can enable an HTTP method which allows the user to directly pass the bytecode of an assertion, without needing to compile.

### Enable HTTP Testing Feature

#### Using Cargo

To enable the HTTP testing feature with Cargo:

```bash
# Run with HTTP testing feature enabled
cargo run --features debug_assertions

# Build with HTTP testing feature enabled
cargo build --features debug_assertions
```

#### Using Docker

To enable the HTTP testing feature with Docker:

1. Run Docker Compose with latest published image: `make compose-dev`
2. Build docker image from source: `make docker-build-dev`

When the HTTP testing feature is enabled, you can use the additional JSON-RPC method `da_submit_assertion` to directly submit bytecode:

```bash
curl -X POST -H "Content-Type: application/json" http://localhost:5001 -d '{
    "jsonrpc": "2.0",
    "method": "da_submit_assertion",
    "params": ["0x608060405234801561001057600080fd5b5060b28061001f6000396000f3fe6080604052348015600f57600080fd5b506004361060285760003560e01c8063c19d93fb14602d575b600080fd5b60336047565b604051603e91906067565b60405180910390f35b60008054905090565b6000819050919050565b6061816050565b82525050565b6000602082019050607a6000830184605a565b9291505056fea2646970667358221220b89aea11d53811bce021b7c95b5fdb7ec5be444e8a48a43f62dda9a4fb2ba9e564736f6c63430008110033"],
    "id": 1
}'
```

## JSON-RPC API

The API follows the JSON-RPC 2.0 specification. The DA has a **max body request size of 10MB**.

### `da_submit_solidity_assertion`

Submits and compiles a Solidity assertion.

#### `da_submit_solidity_assertion` Request

```json
{
  "jsonrpc": "2.0",
  "method": "da_submit_solidity_assertion",
  "params": [
    {
      "solidity_source": "contract MyAssertion { ... }",
      "compiler_version": "0.8.17",
      "assertion_contract_name": "MyAssertion",
      "constructor_args": [],
      "constructor_abi_signature": "constructor()"
    }
  ],
  "id": 1
}
```

Parameters:

- `solidity_source`: Solidity source code
- `compiler_version`: Solidity compiler version to use (e.g., "0.8.17")
- `assertion_contract_name`: Name of the assertion contract in the source code
- `constructor_args`: Arguments for the constructor of the assertion contract
- `constructor_abi_signature`: ABI signature of the assertion contract's constructor

#### `da_submit_solidity_assertion` Success Response

```json
{
  "jsonrpc": "2.0",
  "result": {
    "id": "0x...",
    "prover_signature": "0x..."
  },
  "id": 1
}
```

#### `da_submit_solidity_assertion` Error Response

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32603,
    "message": "Internal error"
  },
  "id": 1
}
```

### `da_get_assertion`

Returns the assertion data by ID.

#### `da_get_assertion` Request

```json
{
  "jsonrpc": "2.0",
  "method": "da_get_assertion",
  "params": ["0xabcd1234"],
  "id": 1
}
```

Parameters:

1. `id`: Hex-encoded assertion ID

#### `da_get_assertion` Success Response

```json
{
  "jsonrpc": "2.0",
  "result": {
    "solidity_source": "contract MyAssertion { ... }",
    "bytecode": "0x...",
    "signature": "0x..."
    "encoded_constructor_args": "",
    "constructor_abi_signature": "constructor()"
  },
  "id": 1
}
```

### Error Codes

The API uses the following error codes:

| Code   | Description                                  |
| ------ | -------------------------------------------- |
| -32601 | Method not found                             |
| -32602 | Invalid params (missing required parameters) |
| -32603 | Internal error (compilation failure)         |
| -32604 | Internal error (signature failure)           |
| -32605 | Internal error (failed to decode hex ID)     |
| 404    | Assertion not found                          |

## Example Usage

Using curl:

```bash
# Submit a Solidity assertion
curl -X POST -H "Content-Type: application/json" http://localhost:5001 -d '{
    "jsonrpc": "2.0",
    "method": "da_submit_solidity_assertion",
    "params": [
        {
            "solidity_source": "// SPDX-License-Identifier: MIT\npragma solidity ^0.8.0;\n\ncontract MyAssertion {\n    function check() public pure returns (bool) {\n        return true;\n    }\n}",
            "compiler_version": "0.8.17",
            "assertion_contract_name": "MyAssertion",
            "assertion_contract_args": ["5"],
            "assertion_contract_abi_signature": "constructor(uint256)"

        }
    ],
    "id": 1
}'

# Get an assertion
curl -X POST -H "Content-Type: application/json" http://localhost:5001 -d '{
    "jsonrpc": "2.0",
    "method": "da_get_assertion",
    "params": [
        {
            "solidity_source": "contract MyAssertion { ... }",
            "bytecode": "0x...",
            "signature": "0x..."
            "encoded_constructor_args": "0x0000000000000000000000000000000000000000000000000000000000000005",
            "constructor_abi_signature": "constructor(uint256)"
        },
    ],
    "id": 1
}'
```

## Limitations

- Only one contract per Solidity source file is supported
- Compilation is performed using Docker containers
- Supported Solidity versions depend on available [ethereum/solc](https://hub.docker.com/r/ethereum/solc) images
