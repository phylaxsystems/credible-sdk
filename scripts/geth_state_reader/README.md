# Geth State Reader

This utility streams account/state data from a local Geth datadir into a
Redis instance using the same schema as `state-worker` (`state:account:*`,
`state:storage:*`, `state:code:*`, `state:current_block`, `state:block_hash:*`).

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
  --redis-url redis://127.0.0.1:6380/0 \
  --redis-namespace state \
  --json-output state_dump.json \
  --json-output-enabled
```

Flags:

- `--datadir` (required): path to the Geth datadir (folder that contains `chaindata`).
- `--block-number` / `--block-hash`: pin the block to dump (default is latest).
- `--start-key`: hashed address (Keccak-256) to start iteration from.
- `--limit`: max account count to process (0 or omitted for all).
- `--redis-url`: Redis connection URL (required to write state).
- `--redis-namespace`: Redis namespace prefix (defaults to `state`).
- `--redis-pipeline-size`: number of commands per pipeline flush (default 1000).
- `--json-output` + `--json-output-enabled`: enable newline-delimited JSON mirroring.

## Verification

After running the script, verify Redis contents:

```bash
redis-cli -u redis://127.0.0.1:6380/0 \
  HGETALL state:account:00bf49f440a1cd0527e4d06e2765654c0f56452257516d793a9b8d604dcfdf2a
redis-cli -u redis://127.0.0.1:6380/0 \
  HGETALL state:storage:0d6aea581b220579a2b99819299dd32c7c28a420018ecb0bde93af007ad89a31
redis-cli -u redis://127.0.0.1:6380/0 GET state:block_hash:0
redis-cli -u redis://127.0.0.1:6380/0 GET state:current_block
```

The values should match the output from `geth dump --incompletes --limit ...`.
