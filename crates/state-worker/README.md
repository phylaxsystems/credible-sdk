# `state-worker`

The `state-worker` is a component that pulls in state from a blockchain and commits it to
an external mdbx database.

The `state-worker` subscribes to the `newHeads` subscription over WS and then for every new block.

- It uses `debug_traceByBlockHash` and `debug_traceByBlockNumber` to get the state changes made in a block.

The changes are stored in a `revm::DatabaseRef` compatible format so we can consume the mdbx cache directly
in the sidecar by calling into it.

## Using and configuring the `state-worker`

The state worker requires `--ws-url` for the Ethereum WebSocket endpoint, `--mdbx-path` for the MDBX
disk path, and `--file-to-genesis` to preload the initial state from a file. Optional flags include:

- `--start-block` to override the resume position derived from `state:meta:latest_block`.
- `--trace-timeout-secs` to tune the timeout for `debug_traceBlockBy*` calls (default 30 seconds).
