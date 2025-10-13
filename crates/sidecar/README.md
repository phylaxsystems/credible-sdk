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

The sidecar accepts configuration through command-line arguments and environment variables. All configuration follows a
structured naming pattern with the following prefixes:

- `chain.*` - Rollup chain configuration
- `credible.*` - Credible layer specific settings
- `transport.*` - Transport layer configuration
- `telemetry.*` - Telemetry and monitoring settings
- `state.*` - State source configuration

Run `cargo run -p sidecar -- --help` to see all available options.

```
Chain:
      --chain.spec-id <SPEC_ID>
          What EVM specification to use. Only latest for now [env: CHAIN_SPEC_ID=] [default: Cancun] [possible values: latest]

      --chain.chain-id <CHAIN_ID>
          Chain ID [env: CHAIN_CHAIN_ID=] [default: 1337]
          
State:
      --state.sequencer-url <SEQUENCER_URL>
          Sequencer RPC node URL and port [env: STATE_SEQUENCER_URL=] [default: http://127.0.0.1:8545]

      --state.besu-client-ws-url <BESU_CLIENT_WS_URL>
          Besu client websocket URL [env: STATE_BESU_CLIENT_WS_URL=] [default: ws://127.0.0.1:8546]
          
      --state.redis-url <REDIS_URL>
          Redis URL [env: STATE_REDIS_URL=] [default: disabled]

      --state.minimum-state-diff <MINIMUM_STATE_DIFF>
          Minimum state diff to consider a block valid [env: STATE_MINIMUM_STATE_DIFF=] [default: 100]
          
      --state.sources-sync-timeout-ms <SOURCES_SYNC_TIMEOUT_MS>
          Maximum time (ms) the engine will wait for a state source to report as synced before failing a transaction [env: STATE_SOURCES_SYNC_TIMEOUT_MS=] [default: 1000]
          
      --state.sources-monitoring-period-ms <SSOURCES_MONITORING_PERIOD_MS>
          Period (ms) the engine will check if the state sources are synced [env: STATE_SOURCES_MONITORING_PERIOD_MS=] [default: 500]

Credible:
      --credible.assertion-gas-limit <ASSERTION_GAS_LIMIT>
          Gas limit for assertion execution [env: CREDIBLE_ASSERTION_GAS_LIMIT=] [default: 3000000]

      --credible.overlay-cache-capacity <OVERLAY_CACHE_CAPACITY>
          Overlay cache capacity, 1gb default [env: CREDIBLE_OVERLAY_CACHE_CAPACITY=] [default: 100000]

      --credible.cache-capacity-bytes <CACHE_CAPACITY_BYTES>
          Sled cache capacity, used in the FsDb, 256mb default [env: CREDIBLE_CACHE_CAPACITY_BYTES=] [default: 256000000]

      --credible.flush-every-ms <FLUSH_EVERY_MS>
          How often in ms will the FsDb be flushed to disk, 5 sec default [env: CREDIBLE_FLUSH_EVERY_MS=] [default: 5000]

      --credible.assertion-da-rpc-url <ASSERTION_DA_RPC_URL>
          HTTP URL of the assertion DA [env: CREDIBLE_ASSERTION_DA_RPC_URL=] [default: http://127.0.0.1:5001]

      --credible.indexer-rpc-url <INDEXER_RPC_URL>
          WS URL the RPC store will use to index assertions [env: CREDIBLE_INDEXER_RPC_URL=] [default: ws://127.0.0.1:8546]

      --credible.indexer-db-path <INDEXER_DB_PATH>
          Path to the indexer database (separate from assertion store) [env: CREDIBLE_INDEXER_DB_PATH=] [default: .local/sidecar-host/indexer_database]

      --credible.assertion-store-db-path <ASSERTION_STORE_DB_PATH>
          Path to the assertion store database [env: CREDIBLE_ASSERTION_STORE_DB_PATH=] [default: .local/sidecar-host/assertion_store_database]

      --credible.block-tag <BLOCK_TAG>
          Block tag to use for indexing assertions [env: CREDIBLE_BLOCK_TAG=] [default: latest] [possible values: latest, safe, finalized]

      --credible.state-oracle <STATE_ORACLE>
          Contract address of the state oracle contract, used to query assertion info [env: CREDIBLE_STATE_ORACLE=] [default: 0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b]

      --credible.state-oracle-deployment-block <STATE_ORACLE_DEPLOYMENT_BLOCK>
          Block number of the state oracle deployment [env: CREDIBLE_STATE_ORACLE_DEPLOYMENT_BLOCK=] [default: 0]

      --credible.transaction-results-max-capacity <TRANSACTION_RESULTS_MAX_CAPACITY>
          Maximum capacity for transaction results cache [env: CREDIBLE_TRANSACTION_RESULTS_MAX_CAPACITY=] [default: 1000000]

Transport:
      --transport.protocol <TRANSPORT_PROTOCOL>
          Which transport protocol to run [env: TRANSPORT_PROTOCOL=] [default: grpc] [possible values: http, grpc]

      --transport.bind-addr <BIND_ADDR>
          Server bind address and port [env: TRANSPORT_BIND_ADDR=] [default: 0.0.0.0:50051]
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
