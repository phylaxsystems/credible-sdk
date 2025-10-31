# `sidecar`

The sidecar is driven by a rollup sequencer(or driver) that validates transactions against credible layer assertions.
The sequencer sends transactions either in bulk or transaction-by-transaction, and the sidecar approves or denies
transactions for inclusion. See the associated sidecar spec for more info.

## Configuring the sidecar

The sidecar can be configured either via env vars or via cli flags. **Distributed tracing/metrics endpoints must be
configured via env vars!**

### Tracing and metrics

Tracing and metrics is configured bia the rust-tracing crate. These are the enviroment vars to set to configure
tracing/metrics collection:

- `OTEL_EXPORTER_OTLP_ENDPOINT` - optional. The endpoint to send traces to,
  should be some valid URL. If not specified, then [`OtelConfig::load`]
  will return [`None`].
- `OTEL_LEVEL` - optional. Specifies the minimum [`tracing::Level`] to
  export. Defaults to [`tracing::Level::DEBUG`].
- `OTEL_TIMEOUT` - optional. Specifies the timeout for the exporter in
  **milliseconds**. Defaults to 1000ms, which is equivalent to 1 second.
- `OTEL_ENVIRONMENT_NAME` - optional. Value for the `deployment.environment.
name` resource key according to the OTEL conventions.
- `OTEL_SERVICE_NAME` - optional. Value for the `service.name` resource key
  according to the OTEL conventions. If set, this will override the default
  service name taken from `CARGO_PKG_NAME`.
- `TRACING_METRICS_PORT` - Which port to bind the the exporter to. If the variable is missing or unparseable, it
  defaults to 9000.
- `TRACING_LOG_JSON` - If set, will enable JSON logging.

### Sidecar config

The sidecar accepts configuration through a combination of command-line arguments (or environment variables) and a
configuration file in a JSON format.
All configuration follows a structured naming pattern with the following prefixes:

- `chain.*` - Rollup chain configuration
- `credible.*` - Credible layer specific settings
- `transport.*` - Transport layer configuration
- `telemetry.*` - Telemetry and monitoring settings
- `state.*` - State source configuration

Run `cargo run -p sidecar -- --help` to see all available options.

```
      --config-file-path <CHAIN_ID>
          Path to the configuration file [env: CONFIG_FILE_PATH=] 
          
Chain:
      --chain.spec-id <SPEC_ID>
          What EVM specification to use. Only latest for now [env: CHAIN_SPEC_ID=] [default: Cancun] [possible values: latest]

      --chain.chain-id <CHAIN_ID>
          Chain ID [env: CHAIN_CHAIN_ID=] [default: 1337]
```

The configuration file is a JSON file with the following schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Sidecar Configuration",
  "description": "Configuration schema for the Credible layer sidecar",
  "type": "object",
  "required": [
    "chain",
    "credible",
    "transport",
    "state"
  ],
  "properties": {
    "chain": {
      "type": "object",
      "description": "Chain configuration for EVM specification and network",
      "required": [
        "spec_id",
        "chain_id"
      ],
      "properties": {
        "spec_id": {
          "type": "string",
          "description": "EVM specification identifier to use for transaction execution",
          "enum": [
            "FRONTIER",
            "FRONTIER_THAWING",
            "HOMESTEAD",
            "DAO_FORK",
            "TANGERINE",
            "SPURIOUS_DRAGON",
            "BYZANTIUM",
            "CONSTANTINOPLE",
            "PETERSBURG",
            "ISTANBUL",
            "MUIR_GLACIER",
            "BERLIN",
            "LONDON",
            "ARROW_GLACIER",
            "GRAY_GLACIER",
            "MERGE",
            "SHANGHAI",
            "CANCUN",
            "PRAGUE",
            "LATEST"
          ],
          "examples": [
            "CANCUN"
          ]
        },
        "chain_id": {
          "type": "integer",
          "description": "Chain ID for the network",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            1,
            11155111,
            137,
            42161
          ]
        }
      },
      "additionalProperties": false
    },
    "credible": {
      "type": "object",
      "description": "Credible execution engine configuration",
      "required": [
        "assertion_gas_limit",
        "assertion_da_rpc_url",
        "indexer_rpc_url",
        "indexer_db_path",
        "assertion_store_db_path",
        "block_tag",
        "state_oracle",
        "state_oracle_deployment_block",
        "transaction_results_max_capacity"
      ],
      "properties": {
        "assertion_gas_limit": {
          "type": "integer",
          "description": "Gas limit for assertion execution",
          "minimum": 1,
          "maximum": 9007199254740991,
          "examples": [
            30000000
          ]
        },
        "overlay_cache_capacity": {
          "type": "integer",
          "description": "Overlay cache capacity in elements (optional)",
          "minimum": 0,
          "examples": [
            1000
          ]
        },
        "overlay_cache_invalidation_every_block": {
          "type": "boolean",
          "description": "Whether the overlay cache has to be invalidated every block"
        },
        "cache_capacity_bytes": {
          "type": "integer",
          "description": "Sled cache capacity used in FsDb, in bytes (256MB default)",
          "minimum": 0,
          "examples": [
            268435456
          ]
        },
        "flush_every_ms": {
          "type": "integer",
          "description": "How often in milliseconds the FsDb will be flushed to disk (5 seconds default)",
          "minimum": 0,
          "examples": [
            5000
          ]
        },
        "assertion_da_rpc_url": {
          "type": "string",
          "description": "HTTP URL of the assertion DA",
          "format": "uri",
          "pattern": "^https?://",
          "examples": [
            "http://localhost:8545",
            "https://mainnet.infura.io/v3/YOUR-PROJECT-ID"
          ]
        },
        "indexer_rpc_url": {
          "type": "string",
          "description": "WebSocket URL the RPC store will use to index assertions",
          "format": "uri",
          "pattern": "^wss?://",
          "examples": [
            "ws://localhost:8546",
            "wss://mainnet.infura.io/ws/v3/YOUR-PROJECT-ID"
          ]
        },
        "indexer_db_path": {
          "type": "string",
          "description": "Path to the indexer database (separate from main assertion store)",
          "minLength": 1,
          "examples": [
            "/tmp/indexer.db",
            "/var/lib/sidecar/indexer.db"
          ]
        },
        "assertion_store_db_path": {
          "type": "string",
          "description": "Path to the RPC store database",
          "minLength": 1,
          "examples": [
            "/tmp/store.db",
            "/var/lib/sidecar/store.db"
          ]
        },
        "block_tag": {
          "type": "string",
          "description": "Block tag to use for indexing assertions",
          "enum": [
            "latest",
            "earliest",
            "pending",
            "safe",
            "finalized"
          ],
          "examples": [
            "latest"
          ]
        },
        "state_oracle": {
          "type": "string",
          "description": "Contract address of the state oracle contract, used to query assertion info",
          "pattern": "^0x[a-fA-F0-9]{40}$",
          "examples": [
            "0x1234567890123456789012345678901234567890"
          ]
        },
        "state_oracle_deployment_block": {
          "type": "integer",
          "description": "Block number of the state oracle deployment",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            100,
            18000000
          ]
        },
        "transaction_results_max_capacity": {
          "type": "integer",
          "description": "Maximum capacity for transaction results cache",
          "minimum": 1,
          "examples": [
            10000
          ]
        },
        "cache_checker_ws_url": {
          "type": "string",
          "description": "Cache checker client websocket URL (only when cache_validation feature is enabled)",
          "format": "uri",
          "pattern": "^wss?://",
          "examples": [
            "ws://localhost:8549"
          ]
        }
      },
      "additionalProperties": false
    },
    "transport": {
      "type": "object",
      "description": "Transport protocol configuration",
      "required": [
        "protocol",
        "bind_addr"
      ],
      "properties": {
        "protocol": {
          "type": "string",
          "description": "Select which transport protocol to run",
          "enum": [
            "http",
            "grpc"
          ],
          "examples": [
            "http"
          ]
        },
        "bind_addr": {
          "type": "string",
          "description": "Server bind address and port",
          "pattern": "^([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|[a-zA-Z0-9.-]+):[0-9]{1,5}$",
          "examples": [
            "127.0.0.1:3000",
            "0.0.0.0:8080",
            "localhost:9000"
          ]
        }
      },
      "additionalProperties": false
    },
    "state": {
      "type": "object",
      "description": "State source configuration",
      "required": [
        "redis_namespace",
        "redis_depth",
        "minimum_state_diff",
        "sources_sync_timeout_ms",
        "sources_monitoring_period_ms"
      ],
      "properties": {
        "sequencer_url": {
          "type": "string",
          "description": "Sequencer bind address and port (optional)",
          "format": "uri",
          "pattern": "^https?://",
          "examples": [
            "http://localhost:8547",
            "http://sequencer-service:8547"
          ]
        },
        "besu_client_ws_url": {
          "type": "string",
          "description": "Besu client WebSocket bind address and port (optional)",
          "format": "uri",
          "pattern": "^wss?://",
          "examples": [
            "ws://localhost:8548",
            "ws://besu-service:8548"
          ]
        },
        "besu_client_http_url": {
          "type": "string",
          "description": "Besu client HTTP bind address and port (optional)",
          "format": "uri",
          "pattern": "^https?://",
          "examples": [
            "http://localhost:8548",
            "http://besu-service:8548"
          ]
        },
        "redis_url": {
          "type": "string",
          "description": "Redis bind address and port (optional)",
          "format": "uri",
          "pattern": "^redis://",
          "examples": [
            "redis://localhost:6379",
            "redis://redis-service:6379"
          ]
        },
        "redis_namespace": {
          "type": "string",
          "description": "Namespace prefix for Redis keys",
          "minLength": 1,
          "examples": [
            "sidecar",
            "credible",
            "custom_namespace"
          ]
        },
        "redis_depth": {
          "type": "integer",
          "description": "Redis state depth - how many blocks behind head Redis will have the data from",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            100,
            250,
            500
          ]
        },
        "minimum_state_diff": {
          "type": "integer",
          "description": "Minimum state diff to consider a cache synced",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            10
          ]
        },
        "sources_sync_timeout_ms": {
          "type": "integer",
          "description": "Maximum time (ms) the engine will wait for a state source to report as synced before failing a transaction",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            30000
          ]
        },
        "sources_monitoring_period_ms": {
          "type": "integer",
          "description": "Period (ms) the engine will check if the state sources are synced",
          "minimum": 0,
          "maximum": 9007199254740991,
          "examples": [
            1000
          ]
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

The default configuration can be found in [default_config.json](default_config.json):

```json
{
  "chain": {
    "spec_id": "CANCUN",
    "chain_id": 1
  },
  "credible": {
    "assertion_gas_limit": 3000000,
    "overlay_cache_capacity": 100000,
    "overlay_cache_invalidation_every_block": true,
    "cache_capacity_bytes": 256000000,
    "flush_every_ms": 5000,
    "assertion_da_rpc_url": "http://127.0.0.1:5001",
    "indexer_rpc_url": "ws://127.0.0.1:8546",
    "indexer_db_path": ".local/sidecar-host/indexer_database",
    "assertion_store_db_path": ".local/sidecar-host/assertion_store_database",
    "block_tag": "latest",
    "state_oracle": "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b",
    "state_oracle_deployment_block": 0,
    "transaction_results_max_capacity": 1000
  },
  "transport": {
    "protocol": "grpc",
    "bind_addr": "0.0.0.0:50051"
  },
  "state": {
    "sequencer_url": "http://127.0.0.1:8545",
    "besu_client_ws_url": "ws://127.0.0.1:8546",
    "besu_client_http_url": "http://127.0.0.1:8545",
    "minimum_state_diff": 100,
    "sources_sync_timeout_ms": 1000,
    "sources_monitoring_period_ms": 500
  }
}
```

## Running the sidecar

The sidecar is a binary in the credible-sdk workspace, you can run it from the cli like so:

```cargo run -p sidecar```

Alternatively, you can run a sidecar locally with all services needed to get it running + an observability stack via:

```make run-sidecar-host```

### Dockerfile

Build:
`docker build -f dockerfile/Dockerfile.sidecar -t sidecar .`

Run:
`docker run sidecar`

If you need to pass arguments to the sidecar binary:
`docker run sidecar [your-sidecar-args]`

If the sidecar needs to expose ports (you'll need to check what port it uses), add -p flag:
`docker run -p <host-port>:<container-port> sidecar`
