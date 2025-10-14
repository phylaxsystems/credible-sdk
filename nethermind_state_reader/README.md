# Nethermind State Reader

This project provides a small Python CLI (managed with `uv`) that can open a Nethermind
RocksDB state database, enumerate all live accounts for a given state root, and (optionally)
explore each account's storage trie.

## Prerequisites

- Python 3.9+ (handled automatically by `uv`)
- The `uv` package manager
- Access to Nethermind RocksDB directories:
  - `state/0` (the state trie column family)
  - `headers` (used to discover state roots)

## Installation

```bash
uv sync
```

## Usage

```bash
uv run python main.py \
  --state-db /path/to/nethermind_db/state/0 \
  --headers-db /path/to/nethermind_db/headers \
  --include-storage \
  --limit 5
```

This emits newline-delimited JSON objects, each describing an account at the most recent
block whose state root is present in the database. Storage slots (if requested) are included
under a `storage` key.

### Useful Flags

- `--block-number <N>` — pin the traversal to a specific block.
- `--include-storage` — gather storage slots for each account.
- `--storage-limit <M>` — cap the number of storage slots collected per account.
- `--limit <K>` — limit the number of accounts emitted.
- `--output FILE` — write JSON lines to a file instead of stdout.
- `--verbose` — print progress information to stderr.
- `--no-verify` — skip hash validation when indexing nodes (faster, but unsafe).

## Output

Each JSON object looks like:

```json
{
  "address_hash": "0x…",
  "nonce": 3,
  "balance": "1234567890",
  "storage_root": "0x…",
  "code_hash": "0x…",
  "storage": [
    {
      "slot_hash": "0x…",
      "value": "0x…",
      "value_int": 1
    }
  ]
}
```

`address_hash` and `slot_hash` are Keccak-256 hashes of the address/slot keys (the
pre-images are not stored in Nethermind's state database).

