# EIPs in the Credible SDK

This document captures how the credible-sdk handles Ethereum Improvement Proposals (EIPs), extracted from the codebase for developer reference.

---

## Overview

The credible-sdk implements two EIP system contracts that require state modifications at the **start of each block**, before any user transactions are processed. These are critical for maintaining state parity between the sidecar and mainnet.

Both EIPs use a **ring buffer** pattern with a shared `SystemContract` trait defined in the `eip-system-calls` crate (`crates/eip-system-calls/`).

---

## EIP-2935: Historical Block Hashes in State

**Spec:** https://eips.ethereum.org/EIPS/eip-2935
**Activated:** Prague hardfork

### Purpose

Stores the last 8191 block hashes in a ring buffer at a system contract, enabling smart contracts to access historical block hashes beyond the 256-block limit of the `BLOCKHASH` opcode.

### Contract Details

| Property | Value |
|---|---|
| Address | `0x0000F90827F1C53a10cb7A02335B175320002935` |
| Ring buffer size | 8191 slots |
| Default nonce | 1 |
| Default balance | 0 |

### Storage Layout

Single ring buffer:
- **Slot `(block_number - 1) % 8191`**: stores the parent block hash

At block N, the hash of block N-1 is written to slot `(N-1) % 8191`. When block 8192 is processed, it overwrites slot 1 (since `8192 % 8191 = 1`).

### Behavior

- **Genesis block (block 0)**: skipped, no state written
- **Missing parent hash**: sidecar logs a warning and skips; state-worker returns an error
- Fork activation check: `spec_id >= SpecId::PRAGUE` (sidecar) or `timestamp >= prague_time` (state-worker)

### Key Code Paths

- **Shared logic:** `eip-system-calls/src/eip2935.rs` -- `Eip2935` marker type, `parent_hash_slot()`, `hash_to_value()`
- **Sidecar:** `sidecar/src/engine/system_calls.rs` -- `apply_eip2935()` writes directly to `revm` database via `DatabaseCommit`
- **State-worker:** `state-worker/src/system_calls.rs` -- `compute_eip2935_state()` computes state changes as `AccountState` records for MDBX storage

---

## EIP-4788: Beacon Block Root in the EVM

**Spec:** https://eips.ethereum.org/EIPS/eip-4788
**Activated:** Cancun hardfork

### Purpose

Stores beacon chain block roots for trust-minimized consensus layer access from the EVM. This allows smart contracts to verify consensus layer state without relying on an oracle.

### Contract Details

| Property | Value |
|---|---|
| Address | `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02` |
| Ring buffer size | 8191 slots (dual) |
| Default nonce | 1 |
| Default balance | 0 |

### Storage Layout

Dual ring buffer:
- **Slot `timestamp % 8191`**: stores the timestamp (for verification/lookup)
- **Slot `(timestamp % 8191) + 8191`**: stores the beacon block root

The timestamp is stored alongside the root so callers can verify that the root corresponds to the expected timestamp.

### Behavior

- **Genesis block (block 0)**: skipped, but if a non-zero beacon root is provided, returns an error (`GenesisNonZeroBeaconRoot`)
- **Missing beacon root (post-genesis)**: returns `MissingParentBeaconBlockRoot` error -- this is a hard error, not a skip
- **Applied before EIP-2935** in execution order
- Fork activation check: `spec_id >= SpecId::CANCUN` (sidecar) or `timestamp >= cancun_time` (state-worker)

### Key Code Paths

- **Shared logic:** `eip-system-calls/src/eip4788.rs` -- `Eip4788` marker type, `timestamp_slot()`, `root_slot()`, `root_to_value()`
- **Sidecar:** `sidecar/src/engine/system_calls.rs` -- `apply_eip4788()` writes to `revm` database
- **State-worker:** `state-worker/src/system_calls.rs` -- `compute_eip4788_state()` computes `AccountState` records

---

## Architecture: Sidecar vs State-Worker

The two consumers of these EIPs have different execution models:

### Sidecar (`apply_pre_tx_system_calls`)

- Sits at **block construction time** -- processes the block as it's being built
- Operates on a **live `revm` database** (`ForkDb`)
- Calls `apply_eip4788()` then `apply_eip2935()` before transaction execution
- Writes state directly via `DatabaseCommit` (builds `Account` with `EvmStorage` slots)
- Also provides `cache_block_hash()` for the `BLOCKHASH` opcode cache (separate from EIP-2935 storage)
- Uses `SpecId` enum for hardfork activation checks
- **Block reference**: receives `block_env.number` (current block N). The fork DB already reflects the parent block's final state (block N-1), so `db.basic_ref(address)` implicitly reads state at block N-1 without needing an explicit block offset

### State-Worker (`compute_system_call_states`)

- Sits at **block replay/indexing time** -- processes already-finalized blocks from the chain
- Operates on **MDBX persistent storage** (full historical state database)
- Computes state changes as `AccountState` records (doesn't execute EVM)
- Storage keys are **keccak256-hashed** slot indices (trie key format), unlike sidecar which uses raw slot indices
- Uses fork activation timestamps instead of `SpecId`
- **Block reference**: receives `block_number` (current block N), but must **explicitly read existing account state from block N-1** via `fetch_existing_or_default()` which does `prev_block = block_number.saturating_sub(1)` and calls `reader.get_full_account(address_hash, prev_block)`. This is necessary because MDBX stores state indexed by block number, so the worker must specify which block's state to read from

### Key Difference: Block State Access

The sidecar and state-worker sit at different points in the block lifecycle, which affects how they access "previous" state:

| Component | When it runs | DB type | How it reads block N-1 state |
|---|---|---|---|
| Sidecar | During block construction | In-memory fork DB | Implicitly -- DB already reflects parent state |
| State-Worker | After block finalization | MDBX historical DB | Explicitly -- reads `block_number - 1` |

Both receive the **current block number N** for slot calculations (e.g., `parent_hash_slot(N)` computes `(N-1) % 8191`), but the way they access the existing account state differs because of where they sit in the pipeline.

### Key Difference: Storage Key Format

| Component | Storage Key | Format |
|---|---|---|
| Sidecar | Raw slot index | `U256` (e.g., `parent_slot` directly) |
| State-Worker | `keccak256(slot.to_be_bytes())` | `B256` (trie-compatible key) |

This difference reflects that the sidecar operates at the EVM level (raw storage slots) while the state-worker operates at the Merkle trie level (hashed keys).

---

## Shared Abstractions (`eip-system-calls` crate)

```rust
/// Trait for system contracts that use ring buffer storage.
pub trait SystemContract {
    const ADDRESS: Address;
    const RING_BUFFER_SIZE: u64;
    fn bytecode() -> &'static Bytes;
    fn code_hash() -> B256; // keccak256(bytecode)
}
```

Utility function:
- `b256_to_storage(value: B256) -> U256` -- converts 32-byte hash to U256 storage format

Dependencies: `alloy-eips` (provides contract addresses, bytecode, and constants like `HISTORY_SERVE_WINDOW`)

---

## Execution Order

Per block, system calls are applied in this order:
1. **EIP-4788** (beacon root) -- if Cancun is active
2. **EIP-2935** (block hash) -- if Prague is active
3. User transactions execute
4. Block hash cached for `BLOCKHASH` opcode (sidecar only, at commit time)
