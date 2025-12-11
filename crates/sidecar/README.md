# `sidecar`

The sidecar is driven by a rollup sequencer(or driver) that validates transactions against credible layer assertions.
The sequencer sends transactions either in bulk or transaction-by-transaction, and the sidecar approves or denies
transactions for inclusion. See the associated sidecar spec for more info.

## Configuring the sidecar

The sidecar can be configured either via env vars or via cli flags. **Distributed tracing/metrics endpoints must be
configured via env vars!**

### Tracing and metrics

Tracing and metrics are configured via the rust-tracing crate. These are the environment vars to set to configure
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
- `TRACING_METRICS_PORT` - Which port to bind the exporter to. If the variable is missing or unparseable, it
  defaults to 9000.
- `TRACING_LOG_JSON` - If set, will enable JSON logging.

#### Transaction result metrics

The transports emit a few histograms so operators can distinguish between a slow client wait and slow engine fetch:

- `sidecar_get_transaction_wait_duration` - Emitted when either transport waits for a tx to arrive (HTTP long-poll path
  and the shared pending receiver helper)
- `sidecar_fetch_transaction_result_duration` - HTTP/gRPC: time spent waiting on the result after the transaction has
  been queued for being processed by the core engine

All durations are reported in seconds to the configured metrics backend.

### Sidecar config

The sidecar accepts configuration through a combination of command-line arguments (or environment variables) and a
configuration file in a JSON format.
All configuration follows a structured naming pattern with the following prefixes:

- `chain.*` - Rollup chain configuration
- `credible.*` - Credible layer specific settings
- `transport.*` - Transport layer configuration
- `telemetry.*` - Telemetry and monitoring settings
- `state.*` - State source configuration

Currently the binary only exposes the configuration file selector. Run `cargo run -p sidecar -- --help` to confirm the
supported flags. To override individual settings, update the JSON configuration (either the embedded default or a custom
file passed via `--config-file-path`).

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
        "transaction_results_max_capacity",
        "assertion_store_prune_config_interval_ms",
        "assertion_store_prune_config_retention_blocks"
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
        },
        "assertion_store_prune_config_interval_ms": {
          "type": "integer",
          "description": "Interval between prune runs in milliseconds for the assertion store",
          "examples": [
            60000,
            120000
          ]
        },
        "assertion_store_prune_config_retention_blocks": {
          "type": "integer",
          "description": "Number of blocks to keep after inactivation (buffer for reorgs) for the assertion store",
          "examples": [
            0,
            10
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
        "bind_addr",
        "health_bind_addr",
        "event_id_buffer_capacity"
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
        },
        "health_bind_addr": {
          "type": "string",
          "description": "Bind address for the always-on health probe server",
          "pattern": "^([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|[a-zA-Z0-9.-]+):[0-9]{1,5}$",
          "examples": [
            "127.0.0.1:3001",
            "0.0.0.0:9547"
          ]
        },
        "event_id_buffer_capacity": {
          "type": "integer",
          "description": "Maximum number of events ID in the transport layer buffer before dropping new events.",
          "examples": [
            "1000"
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
        "eth_rpc_source_ws_url": {
          "type": "string",
          "description": "Eth RPC source WebSocket bind address and port (optional)",
          "format": "uri",
          "pattern": "^wss?://",
          "examples": [
            "ws://localhost:8548",
            "ws://besu-service:8548"
          ]
        },
        "eth_rpc_source_http_url": {
          "type": "string",
          "description": "Eth RPC source client HTTP bind address and port (optional)",
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
    "bind_addr": "0.0.0.0:50051",
    "health_bind_addr": "0.0.0.0:9547"
  },
  "state": {
    "sequencer_url": "http://127.0.0.1:8545",
    "eth_rpc_source_ws_url": "ws://127.0.0.1:8546",
    "eth_rpc_source_http_url": "http://127.0.0.1:8545",
    "redis_namespace": "sidecar",
    "redis_depth": 3,
    "minimum_state_diff": 100,
    "sources_sync_timeout_ms": 1000,
    "sources_monitoring_period_ms": 500
  }
}
```

## Running the sidecar

The sidecar is a binary in the credible-sdk workspace, you can run it from the cli like so:

```cargo run -p sidecar```

And with logging + default config + sequencer:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf \
RUST_LOG=debug \
cargo run --locked --release -p sidecar -- --config-file-path crates/sidecar/default_config.json

docker compose -f docker/maru-besu-sidecar/docker-compose.yml up -d --scale credible-sidecar=0

# bring it down
docker compose -f docker/maru-besu-sidecar/docker-compose.yml down -v
```

Alternatively, you can run a sidecar locally with all services needed to get it running + an observability stack via:

```make run-sidecar-host```

#### Linux

On linux you might need to edit:

```yaml
  extra_hosts:
    - "credible-sidecar:host-gateway"
```

in the dockerfile to:

```yaml
extra_hosts:
  - "credible-sidecar:192.168.0.10"
```

or whatever your local network device ip is.

### Dockerfile

Build:
`docker build -f dockerfile/Dockerfile.sidecar -t sidecar .`

Run:
`docker run sidecar`

If you need to pass arguments to the sidecar binary:
`docker run sidecar [your-sidecar-args]`

If the sidecar needs to expose ports (you'll need to check what port it uses), add -p flag:
`docker run -p <host-port>:<container-port> sidecar`

### Profiling

For profiling you can use the following:

```bash
# If running in userspace you will need to relax security features
sudo sysctl kernel.kptr_restrict=0
sudo sysctl kernel.perf_event_paranoid=-1
sudo sysctl kernel.perf_event_max_sample_rate=100000

# Build with a sidecar w/ additionald debug info
# The traces will not come out nice without this
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile debug-perf

# This will collect perf.data. tune the `-c` flag for more or less detailed collection
RUST_LOG=info perf record -c 100000 -g target/debug-perf/sidecar --config-file-path crates/sidecar/default_config.json

# This will convert the perf data into a profile readable by flamegraph, firefox profiler, etc...
perf script -i perf.data > perf.script
```

Note: you will need to be on linux to run perf.

Alternatively you can also try using dtrace/cargo-flamegraph, but the setup might not work due to weird env caputuring
docker networking. YMMV.

### Hardware requirements

For **production usage** on a real network the sidecar should be ran with at least the following:

- CPU: 16 physical cores, AMD Zen 3 performance equivalent or higher
- Storage: 512gb+ recommended, fast local (not networked) PCIE NVME SSDs preffered to keep I/O latency low. Budget
  sustained IOPS and 2x storage to keep SSD reads fast.
- RAM: 128gb recommended, RAM allocation should be enough to store the entire chain *state*(not full blocks, just state)
  in memory.
- Networking: Keep RTT to the sequencer sub-millisecond by colocating in the same AZ/cluster (co-scheduling on the same
  k8s node/pod is ideal). Use a minimum of 10Gbps between the validator and sidecar and VPC-peer them; avoid routing
  over the public internet. Ensure stable, low-jitter egress to your DA RPC (HTTP) and indexer RPC (WS) endpoints—prefer
  private endpoints or allowlisted static egress IPs.

For **local testing** unless you are benchmarking performance the sidecar is fairly light-weight and doesnt have hard
hardware requirements.
