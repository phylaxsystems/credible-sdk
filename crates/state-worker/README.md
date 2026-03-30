# `state-worker`

`state-worker` traces execution-layer state and writes block-level diffs into MDBX.

In the ENG-2233 architecture, that logic is primarily hosted inside `sidecar`. The standalone `state-worker` binary is
retained as bootstrap and recovery tooling, not as the default production deployment topology.

The worker subscribes to `newHeads` over WebSocket and traces each block with the Geth debug APIs.

The resulting changes are stored in a `revm::DatabaseRef`-compatible MDBX layout so sidecar can consume the durable
cache directly.

## When to use the standalone binary

Use the standalone binary for:

- seeding a fresh MDBX database before handing control to sidecar,
- manual catch-up or repair workflows during recovery,
- debugging worker behavior in isolation from the rest of the sidecar runtime.

Do not use it as a permanent companion process for production sidecar deployments. Running the standalone binary
against the same MDBX that a production sidecar manages would bypass the integrated commit-gated visibility contract.

## Using and configuring the `state-worker`

The standalone worker requires:

- `--ws-url` for the Ethereum WebSocket endpoint used for `newHeads` and tracing,
- `--mdbx-path` for the MDBX disk path to populate,
- `--file-to-genesis` to preload the initial state from a genesis file.

Optional flags include:

- `--start-block` to override the resume position derived from MDBX metadata,
- `--state-depth` to set the circular-buffer depth (default `3`).

Because the standalone worker writes directly to MDBX, operators should treat its output as durable bootstrap or
recovery state. Once sidecar takes over, durable MDBX visibility is authoritative only after sidecar has granted flush
permission and durably flushed the corresponding block.

## Open rollout question

This repository does not identify any remaining external consumers that still require standalone `state-worker`
operation in production. If any such consumers exist outside the repo, they need explicit migration planning before the
sidecar-only production topology can be enforced operationally.
