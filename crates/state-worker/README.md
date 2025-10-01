# `state-worker`

The `state-worker` is a component that pulls in state from a blockchain and commits it to
an external redis database.

The `state-worker` subscribes to the `newHeads` subscription over WS and then for every new block
it uses `trace_replayBlockTransactions` to get the state changes made in a block which later get collapsed
into a single block which gets commited to redis.

The changes are stored in a `revm::DatabaseRef` compatible format so we can consume the redis cache directly
in the sidecar by calling into it.

## Redis state representation

The redis state is represented as follows:

```
state:account:{address}     → {balance, nonce, code_hash}
state:storage:{address}     → {slot1: value1, slot2: value2, ...}
state:code:{code_hash}      → hex-encoded bytecode
state:current_block         → latest synced block number
state:block_hash:{number}   → block hash
```

## Using and configuring the `state-worker`

The state worker requires `--ws-url` for the Ethereum WebSocket endpoint and `--redis-url` for the Redis
database. Optional flags include:

- `--redis-namespace` to change the key namespace (defaults to `state`).
- `--start-block` to override the resume position derived from `state:current_block`.
- `--trace-timeout-secs` to tune the timeout for `debug_traceBlockBy*` calls (default 30 seconds).
