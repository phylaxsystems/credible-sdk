# `sidecar`

**This is a dummy binary to set up infra so we are ready to integrate when the sidecar is ready for use**.

The sidecar is driven by a rollup sequencer(or driver) that validates transactions against credible layer assertions. The sequencer sends transactions either in bulk or transaction-by-transaction, and the sidecar approves or denies transactions for inclusion. See the associated sidecar spec for more info.

## Configuring the sidecar

The sidecar can be configured either via env vars or via cli flags. **Distributed tracing/metrics endpoints must be configured via env vars!**

### Tracing and metrics

Tracing and metrics is configured bia the rust-tracing crate. These are the enviroment vars to set to configure tracing/metrics collection:

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
- `TRACING_METRICS_PORT` - Which port to bind the the exporter to. If the variable is missing or unparseable, it defaults to 9000.
- `TRACING_LOG_JSON` - If set, will enable JSON logging.

### Sidecar config

```
Options:
      --telemetry.sampling-ratio <SAMPLING_RATIO>
          Inverted sampling frequency in blocks. 1 - each block, 100 - every 100th block [env: SAMPLING_RATIO=] [default: 100]
      --ae.soft-timeout-ms <SOFT_TIMEOUT_MS>
          Soft timeout for credible block building in milliseconds [env: AE_BLOCK_TIME_LIMIT=] [default: 650]
      --ae.assertion-gas-limit <ASSERTION_GAS_LIMIT>
          Gas limit for assertion execution [env: AE_ASSERTION_GAS_LIMIT=] [default: 3000000]
      --ae.overlay_cache_capacity_bytes <OVERLAY_CACHE_CAPACITY_BYTES>
          Overlay cache capacity, 1gb default [env: AE_CACHE_CAPACITY_BYTES=] [default: 1024000000]
      --ae.db_path <DB_PATH>
          Path to the `assertion-executor` database [env: AE_DB_PATH=] [default: ae_database]
      --ae.cache_capacity_bytes <CACHE_CAPACITY_BYTES>
          Sled cache capacity, used in the `FsDb`, 256mb default [env: AE_CACHE_CAPACITY_BYTES=] [default: 256000000]
      --ae.flush_every_ms <FLUSH_EVERY_MS>
          How often in ms will the `FsDb` be flushed to disk, 5 sec default [env: AE_FLUSH_EVERY_MS=] [default: 5000]
      --ae.fs_compression_level <ZSTD_COMPRESSION_LEVEL>
          `FsDb` compression level, default to 3 [env: AE_FS_COMPRESSION_LEVEL=] [default: 3]
      --ae.rpc_url <INDEXER_RPC>
          WS URL the RPC store will use to index assertions [env: AE_RPC_STORE_URL=] [default: ws://localhost:8546]
      --ae.rpc_da_url <RPC_DA_URL>
          HTTP URL of the assertion DA [env: AE_RPC_DA_URL=] [default: http://localhost:5001]
      --ae.rpc_store_db_path <RPC_STORE_DB>
          Path to the rpc store db [env: AE_RPC_STORE_DB_PATH=] [default: rpc_store_database]
      --ae.block_tag <BLOCK_TAG>
          Block tag to use for indexing assertions [env: AE_BLOCK_TAG=] [default: finalized] [possible values: latest, finalized, safe]
      --ae.oracle_contract <ORACLE_CONTRACT>
          Contract address of the state oracle contract, used to query assertion info [env: AE_ORACLE_CONTRACT=] [default: 0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b]
  -h, --help
          Print help

Rollup:
      --chain.chain-block-time <CHAIN_BLOCK_TIME>
          chain block time in milliseconds [env: CHAIN_BLOCK_TIME=] [default: 1000]
      --chain.extra-block-deadline-secs <EXTRA_BLOCK_DEADLINE_SECS>
          How much time extra to wait for the block building job to complete and not get garbage collected [default: 20]
      --chain.spec-id <SPEC_ID>
          What EVM specification to use. Only latest for now [env: CHAIN_SPEC_ID=] [default: latest] [possible values: latest]
      --chain.transport-url <RPC_URL>
          Transport JSON-RPC server URL and port [env: CHAIN_RPC_URL=] [default: http://127.0.0.1:8545]
```

## Running the sidecar

The sidecar is a binary in the credible-sdk workspace, you can run it from the cli like so:

```cargo run -p sidecar```

### Dockerfile

