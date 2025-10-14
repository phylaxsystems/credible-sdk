# Nethermind State Reader


- To use you need access to Nethermind RocksDB directories:
  - `state/0` (the state trie column family)
  - `headers` (used to discover state roots)

## Usage

```bash
uv sync

uv run python main.py \
  --state-db /path/to/nethermind_db/state/0 \
  --headers-db /path/to/nethermind_db/headers \
  --limit 5
```

This emits newline-delimited JSON objects, each describing an account at the most recent
block whose state root is present in the database. Storage slots are included under a
`storage` key. Slot numbers are stored as keccak hashes.

### Flags

- `--block-number <N>` — pin the traversal to a specific block.
- `--[no-]include-storage` — gather storage slots for each account (enabled by default).
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
