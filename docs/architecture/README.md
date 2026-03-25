# Architecture Docs

These docs are split so agents do not have to load the entire system description at once.

## Read routing

Read `system-overview.md` if you need:

- the global topology,
- cross-component data flow,
- the high-level schemas,
- the architectural boundaries between runtime, storage, DA, and CLI flows.

Read `state-pipeline.md` if you need:

- `crates/state-worker`,
- `crates/mdbx`,
- `scripts/geth_snapshot`,
- block ingestion, persistence, circular-buffer semantics, or snapshot hydration.

Read `sidecar.md` if you need:

- `crates/sidecar`,
- transport, event sequencing, engine behavior, reorg handling, the assertion indexer, or incident reporting.

Read `assertion-executor.md` if you need:

- `crates/assertion-executor`,
- tx validation semantics,
- trigger matching,
- assertion extraction,
- `OverlayDb` / `ForkDb` / `VersionDb` / `MultiForkDb`.

Read `assertion-da.md` if you need:

- `crates/assertion-da/da-client`,
- `crates/assertion-da/da-core`,
- `crates/assertion-da/da-server`,
- JSON-RPC submission/fetch behavior,
- Dockerized Solidity compilation,
- DA storage and readiness behavior.

Read `pcl.md` if you need:

- `crates/pcl/cli`,
- `crates/pcl/common`,
- `crates/pcl/core`,
- `crates/pcl/phoundry`,
- auth, build, test, store, submit, or apply workflows.

## Global system shape

At a high level:

1. `state-worker` ingests state from an execution node and writes block state into MDBX.
2. `geth_snapshot` can hydrate MDBX from a one-shot dump.
3. `sidecar` consumes driver events, builds speculative per-block state, executes txs, and validates them.
4. `assertion-executor` is the EVM/assertion engine used by `sidecar`.
5. `sidecar` also indexes assertions from external events plus Assertion DA fetches.
6. `assertion-da` is the artifact-availability service for assertion source and bytecode.
7. `pcl` is the user/operator CLI stack for auth, build/test, DA storage, and app submission flows.

## Invariants worth preserving

- `sidecar` only accepts tx execution after a valid iteration and first commit head.
- `sidecar` treats `CommitHead` as the canonicalization point; before that, iteration state is speculative.
- `VersionDb` commit log depth must stay aligned with tx ordering, including txs that produced no state change.
- `mdbx` readers must verify the namespace still maps to the requested block.
- `mdbx` writer assumes each account appears at most once in a block update.
- `state-worker` system-call writes are merged into traced block updates before commit.
- assertion trigger matching is block-bounded and adopter-bounded.
- inactive assertions are retained until prune horizon, not removed immediately.
- `geth_snapshot` treats parse errors as fatal state-integrity failures.

## Current limitations

- `state-worker` does not implement chain reorg handling.
- `sidecar` relies on driver events and iteration cache invalidation rather than reconstructing historical canonical state locally.
- `MdbxSource` serves only the buffer window available from the state-worker DB.
- `geth_snapshot` bootstraps all namespaces with the same block on initial hydrate; that is expected, not corruption.
- assertion indexing currently depends on an external event source plus DA source being reachable and consistent.
- Assertion DA submission depends on Dockerized Solidity compilation, so Docker health is part of the effective control plane.
- PCL relies on local `CliConfig` as the staging boundary between DA storage and app submission; removing that boundary would change current workflow semantics.

## Where to start when modifying behavior

If changing tx execution semantics:

- start in `docs/architecture/sidecar.md`
- then read `docs/architecture/assertion-executor.md`

If changing state-source behavior:

- start in `docs/architecture/sidecar.md`
- then read `docs/architecture/state-pipeline.md`

If changing persisted state format or retention behavior:

- start in `docs/architecture/state-pipeline.md`

If changing ingestion correctness:

- start in `docs/architecture/state-pipeline.md`

If changing Assertion DA behavior:

- start in `docs/architecture/assertion-da.md`

If changing PCL workflows:

- start in `docs/architecture/pcl.md`
