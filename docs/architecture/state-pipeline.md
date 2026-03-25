# State Pipeline

This file covers `crates/state-worker`, `crates/mdbx`, and `scripts/geth_snapshot`.

## State production schema

```text
execution node
  |
  +--> newHeads subscription ------------------------------+
  |                                                        |
  +--> debug_trace_block_by_number(prestateTracer diff)    |
                                                           v
                                                   state-worker
                                                           |
                                                           +--> trace collapse
                                                           |
                                                           +--> system call merge
                                                           |
                                                           v
                                                  BlockStateUpdate
                                                           |
                                                           v
                                                    mdbx::StateWriter
                                                           |
                                                           v
                                                         MDBX
```

## `crates/mdbx`

### What it is

`mdbx` is a custom blockchain-state storage layer over `reth`'s MDBX bindings.

It exposes:

- `StateReader`
- `StateWriter`
- a `Reader` trait
- a `Writer` trait

The storage model is a circular buffer of block states.

### Data model

The database tables are:

- `NamespaceBlocks`
- `NamespacedAccounts`
- `NamespacedStorage`
- `Bytecodes`
- `BlockMetadata`
- `StateDiffs`
- `Metadata`

Core idea:

- a block maps to a namespace via `block_number % buffer_size`,
- that namespace stores the full reconstructed state for the currently assigned block,
- diffs for blocks are stored separately so rotated namespaces can be rebuilt.

### Circular buffer semantics

This is the most important non-obvious behavior.

When a new block lands in namespace `N`:

1. the writer computes the current block's write batch from the diff,
2. it loads the namespace's current base state,
3. if the namespace already contains an old block, it applies intermediate diffs from the old block + 1 up to the new block - 1,
4. it applies the new block diff,
5. it atomically writes the resulting namespace contents and metadata.

This means the namespace holds a reconstructed full state for its assigned block even though only diffs are persisted across blocks.

### Read model

Reads are snapshot-consistent because MDBX MVCC is used.

Reader behavior:

- verify that the requested block is still the current occupant of its namespace,
- if not, return `BlockNotFound`,
- if yes, all account, storage, and code reads for that namespace are considered consistent.

This is why `verify_block_available` is critical. Namespace identity alone is not enough.

### Write model

`StateWriter::commit_block` is atomic and durable.

Pipeline:

1. validate unique accounts in `BlockStateUpdate`,
2. convert the update to a binary diff representation,
3. build a write batch in parallel,
4. load intermediate diffs and copied base state,
5. overlay batches in order,
6. sort and deduplicate,
7. execute all writes in one MDBX transaction,
8. update the diff table and metadata,
9. delete metadata and diff entries that fell out of the buffer.

Important details:

- duplicate accounts in a single block update are an error,
- bytecode is namespaced by `(namespace, code_hash)`, not globally by code hash alone,
- `BlockMetadata` and `StateDiffs` are pruned once older than the buffer horizon,
- namespace contents are overwritten through rotation rather than separately pruned.

### Bootstrap behavior

MDBX supports snapshot bootstrap through:

- `bootstrap_from_snapshot`
- `begin_bootstrap`
- `bootstrap_from_iterator`

Streaming bootstrap writes every account to all namespaces for the chosen starting block. This is intentional: immediately after bootstrap, every namespace points to the same snapshot block until live writes rotate the buffer forward.

`get_available_block_range` therefore computes the range from `NamespaceBlocks`, not from `latest_block - buffer_size + 1`, because after bootstrap the theoretical range would be wrong.

### Metadata repair

`StateWriter::fix_block_metadata` can rewrite:

- `Metadata.latest_block`
- all `NamespaceBlocks`
- the keyed `BlockMetadata` entry

This is used by `geth_snapshot` when snapshot hydration started with placeholder metadata and the true block metadata becomes known later.

## `crates/state-worker`

### What it is

`state-worker` is the continuous chain-state ingestor that produces the MDBX state that `sidecar` can consume.

It is designed for:

- replay from the persisted latest block,
- catch-up to head,
- then steady-state head following.

It assumes a Geth-like execution node with debug APIs.

### Startup behavior

On startup it:

1. installs tracing and rustls provider,
2. parses CLI and env config,
3. connects to the WS provider,
4. validates Geth version if the client identifies as Geth,
5. opens `StateWriter`,
6. reads and parses a genesis JSON file,
7. builds `SystemCalls` from genesis fork timestamps,
8. constructs a `GethTraceProvider`,
9. enters a restart-on-failure loop.

Geth version must be at least `1.16.6` if the client is Geth. Earlier versions incorrectly report post-Cancun `SELFDESTRUCT` in `prestateTracer` diff mode.

### Ingestion model

`StateWorker::run` has two phases:

- catch-up: sequentially process blocks until caught up to current head,
- streaming: subscribe to `newHeads` and process new blocks in order.

Important current limitation:

- missing blocks during streaming are treated as an error and cause retry,
- already-processed stale headers are skipped,
- chain reorg handling is not implemented.

### Block processing

For each block:

1. fetch block state via the trace provider,
2. apply EIP-2935 and EIP-4788 state updates,
3. commit the block to MDBX,
4. update metrics.

Block `0` is special:

- if the DB is empty and a genesis file was provided, the worker hydrates block `0` from the genesis file instead of tracing it.

### Trace semantics

`GethTraceProvider` uses `debug_trace_block_by_number` with Geth built-in `PreStateTracer` in `diff_mode = true`.

Trace conversion logic in `state::geth`:

- accumulates touched accounts across all tx traces in the block,
- carries forward baseline pre-state account fields,
- uses post-state for final changed values,
- infers storage deletions when a slot exists in pre but not post,
- infers account deletion when an account exists in pre but not post,
- correctly treats post-Cancun `SELFDESTRUCT` as balance-only semantics.

The final output is a block-level `BlockStateUpdate` keyed by address hash and slot hash.

### System calls

State-worker computes EIP system-contract state independently from traces and merges it into the block update.

This is intentionally defensive:

- if tracing already includes those writes, merge logic overwrites consistently,
- if tracing omits them, state-worker still persists expected canonical state.

If system-call computation fails, the worker logs a warning and continues instead of failing the block. The assumption in code is that traces may already contain the writes.

See [eips.md](eips.md) for full EIP-2935 and EIP-4788 storage layout, slot calculations, and the differences between sidecar and state-worker execution models.

## `scripts/geth_snapshot`

### What it is

`geth_snapshot` is the one-shot bootstrap and hydration CLI for MDBX.

It can:

- read directly from a Geth datadir by shelling out to `geth`,
- read from newline-delimited JSON dumps,
- write JSONL output,
- stream state into MDBX using `StateWriter::begin_bootstrap`,
- fix block metadata on an existing MDBX database.

### Input modes

There are two mutually exclusive inputs:

- live Geth via `--datadir`,
- JSONL via `--json`.

If reading from Geth:

- backend can be `snapshot`, `trie`, or `auto`,
- `auto` tries snapshot first then trie and has specialized fallback logic.

### Parsing model

The parser is intentionally fail-hard.

If any account JSON or account contents are malformed, the tool aborts with a fatal integrity error instead of continuing with partial state.

Important parsing behavior:

- balances can be hex or decimal,
- storage values from Geth dumps are RLP-decoded before turning into `U256`,
- code hash is validated against provided code,
- missing code requires code hash to be zero or `KECCAK256_EMPTY`.

### Streaming write model

The tool does not accumulate the full state in memory.

Main path:

1. optional JSON output sink is opened,
2. optional MDBX bootstrap session is started with placeholder metadata,
3. each parsed account is written immediately to output sinks,
4. after input completes, final block metadata is applied and bootstrap is finalized.

For JSON input, parsing and writing are parallelized with a bounded channel:

- parser thread reads and deserializes,
- main thread writes accounts to sinks.

### Geth fallback behavior

`run_with_fallback` is the heart of the live dump mode.

Behavior:

- `snapshot` backend runs `geth snapshot dump`,
- `trie` backend runs `geth dump --iterative --incompletes`,
- `auto` tries snapshot first, then trie if snapshot failure looks retryable.

There is special detection for pruned historical state:

- parse snapshot-head mismatch error,
- parse missing trie node or state-not-available error,
- if they correlate to the same missing root, emit a clear archive or pruning diagnosis.

### Metadata repair mode

`--fix-metadata` opens an existing MDBX DB and rewrites block metadata without re-hydrating state.

This is used when a bootstrap was done with incomplete metadata and later needs exact block number, hash, and state root alignment.
