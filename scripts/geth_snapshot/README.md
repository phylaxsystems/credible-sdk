# Geth snapshot

A Rust CLI tool for dumping Ethereum state from Geth into an MDBX database. Supports streaming ingestion to minimize
memory usage and can read from either a live Geth node or a pre-dumped JSON file.

## Features

- **Dual input sources**: Read state directly from Geth or from a pre-dumped JSON file
- **Streaming architecture**: Processes accounts one-by-one without loading entire state into memory
- **Parallel processing**: Parser and writer run in separate threads when reading from JSON
- **Smart error diagnosis**: Detects pruned state scenarios by correlating errors from both backends
- **Metadata fixing**: Update block metadata on existing databases without re-hydrating

## Installation

```bash
cargo build --release
```

## Usage

### Dump from Geth (live)

```bash
# Basic usage - dumps state at block 18000000 to MDBX
cargo run -- --datadir /path/to/geth/chaindata --block-number 18000000

# Specify output path and verbose logging
cargo run -- --datadir /path/to/geth/chaindata --block-number 18000000 \
    --mdbx-path ./state-db --verbose

# Force a specific backend (snapshot or trie)
cargo run -- --datadir /path/to/geth/chaindata --block-number 18000000 \
    --geth-dump-backend snapshot

# Dump to JSON file instead of/in addition to MDBX
cargo run -- --datadir /path/to/geth/chaindata --block-number 18000000 \
    --json-output state-dump.jsonl
```

### Dump from pre-existing JSON file

When hydrating from a JSON file, you **must provide both `--block-number` and `--block-hash`** for a complete and
accurate database. The JSON dump only contains account data and state root — block number and hash are not included.

```bash
# Correct: provide both block number and block hash
cargo run -- --json state-dump.jsonl \
    --block-number 18000000 \
    --block-hash 0x1234...abcd \
    --mdbx-path ./state-db
```

#### Where to get the block hash

Since the JSON dump doesn't include block hash, you'll need to obtain it separately:

```bash
# Using cast (foundry)
cast block 18000000 --field hash

# Using curl + RPC
curl -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x112A880", false],"id":1}' \
    http://localhost:8545

# From Etherscan
# https://etherscan.io/block/18000000
```

### Fix metadata on existing database

```bash
cargo run -- --fix-metadata --block-number 18000000 \
    --block-hash 0x1234...abcd \
    --state-root 0xabcd...1234 \
    --mdbx-path ./state-db
```

## Command Line Options

| Option                | Description                                           | Default                                             |
|-----------------------|-------------------------------------------------------|-----------------------------------------------------|
| `--datadir`           | Path to Geth data directory                           | Required if `--json` not provided                   |
| `--json`              | Path to JSON input file (use `-` for stdin)           | Required if `--datadir` not provided                |
| `--block-number`      | Block number to dump                                  | Required                                            |
| `--block-hash`        | Block hash (hex)                                      | Extracted from geth stderr, or `0x0` for JSON input |
| `--geth-bin`          | Path to geth binary                                   | `geth`                                              |
| `--geth-dump-backend` | Backend: `auto`, `snapshot`, or `trie`                | `auto`                                              |
| `--start-key`         | Starting key for iteration (resume support)           | None                                                |
| `--limit`             | Limit number of accounts                              | None                                                |
| `--mdbx-path`         | MDBX database output path                             | `state`                                             |
| `--buffer-size`       | State worker buffer size (circular buffer namespaces) | `3`                                                 |
| `--json-output`       | JSON output file (use `-` for stdout)                 | None                                                |
| `--verbose` / `-v`    | Enable verbose logging                                | `false`                                             |
| `--fix-metadata`      | Fix metadata only without re-hydrating                | `false`                                             |
| `--state-root`        | State root (hex), used with `--fix-metadata`          | None                                                |

## JSON Format

The tool expects newline-delimited JSON (JSONL) with one account per line:

```json
{
  "key": "0x00000...",
  "balance": "0x1234",
  "nonce": 5,
  "codeHash": "0xc5d2...",
  "code": "0x6080...",
  "storage": {
    "0xabc...": "0x01"
  }
}
{
  "key": "0x00001...",
  "balance": "0x0",
  "nonce": 0,
  "codeHash": "0xc5d2..."
}
{
  "root": "0xabcdef..."
}
```

Each account object contains:

- `key`: Keccak256 hash of the account address (required)
- `balance`: Account balance in hex or decimal
- `nonce`: Account nonce
- `codeHash`: Hash of contract code
- `code`: Contract bytecode (hex)
- `storage`: Storage slots as dict or list (RLP-encoded values)

The final line contains the state root: `{"root": "0x..."}`.

## Metadata Handling

### From Geth (live dump)

When dumping directly from Geth, metadata is captured automatically:

| Field        | Source                                                     |
|--------------|------------------------------------------------------------|
| Block number | CLI `--block-number`                                       |
| Block hash   | Extracted from geth stderr, or CLI `--block-hash` override |
| State root   | Extracted from geth JSON output                            |

### From JSON file

When hydrating from a pre-dumped JSON file, metadata is **not fully available** in the dump:

| Field        | Source                             | In JSON dump? |
|--------------|------------------------------------|---------------|
| Block number | CLI `--block-number`               | ❌ No          |
| Block hash   | CLI `--block-hash`                 | ❌ No          |
| State root   | Extracted from `{"root":...}` line | ✅ Yes         |

**For 100% accurate MDBX state, you must provide both `--block-number` and `--block-hash` when using `--json`.**

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Input Source  │     │  Parser Thread   │     │  Writer Thread  │
│  (geth/JSON)    │────▶│  (deserialize)   │────▶│  (MDBX/JSON)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                              │
                              ▼
                        Bounded Channel
                         (1000 items)
```

- Accounts are streamed and processed one at a time
- Memory usage stays constant regardless of state size
- JSON parsing runs in parallel with database writes

## Error Handling

### Pruned State Detection

When dumping from a non-archive Geth node, historical state may have been pruned. The tool uses two backends with
automatic fallback:

1. **Snapshot backend**: Fast, but only has recent state (~128 blocks)
2. **Trie backend**: Can access older state, but requires the trie nodes to exist

When both backends fail, the tool correlates their errors to detect pruned state:

```
Snapshot error: "head doesn't match snapshot: have 0xAAA..., want 0xBBB..."
Trie error:     "missing trie node 0xBBB..."
                                   ^^^^^^
                                   Same hash = state was pruned!
```

When this correlation is detected, instead of showing two cryptic errors, you get an actionable message:

```
Geth pruned the historical state needed for block 17000000:
snapshot data only exists for head root 0xAAA...,
while the requested block requires state root 0xBBB...
and the trie backend reported missing node 0xBBB...
Re-sync the datadir with --gcmode=archive or select a block
within the snapshot horizon (typically HEAD-127 or newer).
```

**Solutions for pruned state:**

- Use a block within the snapshot horizon (typically HEAD-127 or newer)
- Re-sync Geth with `--gcmode=archive` to retain all historical state
- Use a pre-dumped JSON file from when the state was available

### Parse Errors

Any JSON parsing failure is treated as fatal to ensure state integrity:

```
FATAL: Failed to parse account JSON at line 12345.
State integrity compromised!
```

This is intentional — partial or corrupted state is worse than no state.

## Examples

### Full workflow: Dump from Geth to JSON, then hydrate MDBX

```bash
# Step 1: Dump state to JSON file (capture block hash from output!)
cargo run -- --datadir ~/.ethereum --block-number 18500000 \
    --json-output mainnet-18500000.jsonl --verbose 2>&1 | tee dump.log

# Step 2: Extract block hash from geth output
BLOCK_HASH=$(grep "block=" dump.log | sed 's/.*hash=\(0x[a-f0-9]*\).*/\1/')

# Step 3: Hydrate MDBX from JSON (can be done on different machine)
cargo run -- --json mainnet-18500000.jsonl \
    --block-number 18500000 \
    --block-hash $BLOCK_HASH \
    --mdbx-path ./mainnet-state --verbose
```

### Resume interrupted dump

```bash
# If dump was interrupted at account 0xabc123...
cargo run -- --datadir ~/.ethereum --block-number 18500000 \
    --start-key 0xabc123... --mdbx-path ./state-db
```