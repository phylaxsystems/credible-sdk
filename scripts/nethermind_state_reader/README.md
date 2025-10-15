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

## Redis export

To mirror the extracted state into Redis, provide a connection URL (and optionally the code
database so that contract bytecode can be stored):

```bash
uv run python main.py \
      --state-db ../nethermind_db/linea-mainnet_archive/state/0 \
      --headers-db ../nethermind_db/linea-mainnet_archive/headers \
      --code-db ../nethermind_db/linea-mainnet_archive/code \
      --redis-url redis://localhost:6379/0 \
      --no-json-output \
      --no-verify \
      --redis-pipeline-size 50000
```

Keys are written using the following schema:

- `state:account:{address_hash}` → Redis hash containing `balance`, `nonce`, `code_hash`.
- `state:storage:{address_hash}` → Redis hash `{slot_hash: value, ...}` for non-empty slots.
- `state:code:{code_hash}` → hex-encoded bytecode when available.
- `state:current_block` → latest synced block number.
- `state:block_hash:{number}` → corresponding block hash (hex string).

Nethermind stores all trie keys and code references as Keccak-256 hashes, so:

- `{address_hash}` is the Keccak-256 of the 20-byte address (i.e., `0xF06B7BD371e46e96DF807d45ED1298BeeE8894BA`).
- `{slot_hash}` is the Keccak-256 of the 32-byte storage slot index.
- `{code_hash}` is the Keccak-256 of the raw contract bytecode.

To look up a known address or storage slot, hash the canonical value first and use the
resulting `0x`-prefixed digest in the Redis key/value lookups. The original preimages are
not persisted by Nethermind.

### Flags

- `--block-number <N>` — pin the traversal to a specific block.
- `--[no-]include-storage` — gather storage slots for each account (enabled by default).
- `--storage-limit <M>` — cap the number of storage slots collected per account.
- `--limit <K>` — limit the number of accounts emitted.
- `--output FILE` — write JSON lines to a file instead of stdout.
- `--[no-]json-output` — toggle JSON emission (enabled by default).
- `--redis-url URL` — stream results into Redis using the schema above.
- `--redis-pipeline-size N` — number of commands to batch before flushing to Redis.
- `--code-db PATH` — path to the code RocksDB (required to resolve contract bytecode).
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
