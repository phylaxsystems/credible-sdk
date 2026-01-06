# Geth State Reader

This utility streams account/state data from a local Geth datadir into a
Redis instance using the same schema as `state-worker` (`state:{idx}:account:*`,
`state:{idx}:storage:*`, `state:{idx}:code:*`, `state:{idx}:block`,
`state:meta:latest_block`, `state:block_hash:*`, `state:state_root:*`,
`state:state_dump_indices`).

## Prerequisites

- Geth datadir available locally (e.g. `geth/geth`)
- Redis server (for local testing you can run `redis-server --port 6380`)
- [`uv`](https://docs.astral.sh/uv/) for Python dependency management

Install dependencies (once):

```bash
cd scripts/geth_state_reader
uv sync
```

## Usage

```bash
cd /path/to/credible-sdk
uv run python scripts/geth_state_reader/main.py \
  --datadir geth/geth \
  --block-number 0 \
  --state_worker-url state_worker://127.0.0.1:6380/0 \
  --state_worker-namespace state \
  --state_worker-buffer-size 3 \
  --json-output state_dump.json \
  --json-output-enabled
```

Flags:

- `--datadir` (required): path to the Geth datadir (folder that contains `chaindata`).
- `--block-number` / `--block-hash`: pin the block to dump (default is latest).
- `--start-key`: hashed address (Keccak-256) to start iteration from.
- `--limit`: max account count to process (0 or omitted for all).
- `--redis-url`: Redis connection URL (required to write state). When set you must also pass `--block-number`.
- `--redis-namespace`: Redis namespace prefix (defaults to `state`).
- `--redis-pipeline-size`: number of accounts to buffer before flushing Redis pipelines (defaults to 1 for immediate
  writes).
- `--redis-buffer-size`: number of namespace slots in the circular buffer (defaults to `3`, matching the state worker's
  default `--state-depth`).
- `--json-output` + `--json-output-enabled`: enable newline-delimited JSON mirroring.
- `--geth-dump-backend`: choose between `snapshot`, `trie`, or `auto` (default) for the
  backing `geth` command. `snapshot` avoids `missing trie node` errors on the modern
  path database scheme.

### Pruned datadirs / missing historical state

When both `geth snapshot dump` and `geth dump` report errors similar to
`head doesn't match snapshot ...` and `missing trie node ... is not available`, the
datadir no longer contains the historical state needed for the requested block
(Geth keeps snapshots only for roughly `HEAD-127` unless it is synced with
`--gcmode=archive`). The script now detects this pattern and emits an actionable
error indicating which state roots are available. If you hit this failure,
either re-sync the datadir as an archive node or choose a block that is still
covered by the snapshot horizon.
