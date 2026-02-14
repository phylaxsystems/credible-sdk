# Assertion Rework Master Plan (Consolidated)

## Goal

Minimize assertion LoC while making assertions:
1. Declarative.
2. Business-logic independent.
3. Fast to author and review.

This plan is biased toward the Euler roadmap objective: remove assertion plumbing first.

## Latest Implementation Status (2026-02-13)

### Completed in this pass

1. Repaired strict lint/CI blockers in `assertion-executor` (format + pedantic clippy).
2. Tightened call-tracer hot path again by removing checked conversion overhead in per-call depth packing.
3. Kept the new declarative precompile paths lint-clean with scoped, explicit lint allowances where noise-only.
4. Restored this master plan file to keep progress tracking in-repo.
5. Added `cargo-deny` advisory handling for `RUSTSEC-2026-0002` (transitive tooling path via Foundry debugger stack).
6. Implemented Phase 13 per-call token facts in executor:
   - `erc20BalanceDeltaAtCall`, `erc20SupplyDeltaAtCall`,
   - `erc20AllowanceAtCall`, `erc20AllowanceDeltaAtCall`,
   - `erc4626TotalAssetsDeltaAtCall`, `erc4626TotalSupplyDeltaAtCall`,
   - `erc4626VaultAssetBalanceDeltaAtCall`.
7. Synced canonical `PhEvm.sol` into `credible-std` (Phase 12 interface follow-up).

### Validation results

1. `cargo fmt --all --check` passes.
2. `cargo clippy --workspace --all-targets --no-default-features --features test -- -D warnings -D clippy::pedantic` passes.
3. `cargo deny check advisories` passes.
4. `cargo test -p assertion-executor@1.0.8 --lib` passes (`291 passed`).
5. `cargo test -p assertion-executor@1.0.8`:
: unit tests pass; integration `indexer_int_tests` still require local artifact `../../lib/credible-layer-contracts/shell/deploy_create_x.sh`.

### Benchmarks (call tracer)

`call-tracer-truncate` (criterion, median values):

1. Branch (`feat/new-cheatcodes-design`): `15k=172.09µs`, `15k_deep_pending=356.33µs`, `500=15.85µs`, `500_deep_pending=22.35µs`.
2. Main (`origin/main`): `15k=220.44µs`, `15k_deep_pending=515.81µs`, `500=14.25µs`, `500_deep_pending=26.10µs`.

Net: large-trace and deep-pending cases are significantly faster; small `500` case still trails main.

## What We Keep From Current Stack

Current `PhEvm` gives us:
1. Forks (`forkPreTx`, `forkPostTx`, `forkPreCall`, `forkPostCall`)
2. Raw calls (`getCallInputs`, `getAllCallInputs`)
3. Raw logs (`getLogs`)
4. Slot reads and per-slot change streams (`load`, `getStateChanges`)

This is enough for correctness, but not enough for low-LoC authoring.

## Decision Summary

To reduce LoC across all repos, implement cheatcodes in three lanes:
1. Call facts (so we stop manually walking/de-duping call traces).
2. State-write facts (so we stop forking per call and manual slot bookkeeping).
3. Transfer/mapping facts (so we stop reconstructing keys/accounts from business logic).

No single API solves all projects. We need a small concrete set with priorities.

Additionally, we need a binding/interface sync lane:
1. Keep precompile interfaces (`PhEvm`, `TriggerRecorder`) canonical in one place.
2. Auto-generate/sync Rust bindings and Solidity stdlib interfaces from that source.
3. Enforce sync in CI and auto-open PRs in downstream repos when definitions change.

## Executor-Aware Constraints (Critical)

The executor model changes what "good API design" means for performance:
1. Sidecar processes txs sequentially, so per-tx latency is critical.
2. Triggered assertion contracts/functions fan out in parallel.
3. Each assertion function runs with isolated cloned context.

Implication:
1. If an assertion function scans/decode-loops arrays, that cost is multiplied across all triggered functions.
2. We should treat array-returning cheatcodes as escape hatches, not defaults.
3. Default APIs must be scalar or quantified and backed by tx-level shared indexes built once.

Design rules:
1. Scalar first (`bool`, `count`, `sum`, `delta`) and deterministic known-key checks.
2. Push trace iteration/filtering/ABI decode into Rust runtime.
3. Build tx facts once, share read-only across all assertion function executions.
4. Prioritize known-key and event-derived key sets in v1.

## Fresh Pattern Review (ROI-Driven)

Cross-repo snapshot from current assertions:

| Project | calls | forks | logs | loads | loops | Dominant pain |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `aave-v3-origin` | 54 | 0 | 1 | 0 | 55 | call-input decoding loops |
| `ethereum-vault-connector` | 16 | 4 | 3 | 0 | 64 | nested call reconstruction + dedupe |
| `malda-lending` | 27 | 29 | 0 | 0 | 24 | per-call fork loops |
| `morpho-vault-2` | 33 | 49 | 4 | 0 | 42 | repeated call+fork gate checks |
| `myx-v2-assertions` | 10 | 8 | 0 | 0 | 10 | multi-call accounting reconstruction |
| `lagoon-v0` | 1 | 0 | 10 | 13 | 11 | log scans + slot/layout coupling |
| `etherex-assertions` | 5 | 0 | 0 | 6 | 6 | auth policies expressed imperatively |
| `ctf-exchange` | 26 | 3 | 2 | 0 | 44 | key reconstruction from call inputs |

Strict ROI conclusion:
1. First killer is call-loop elimination APIs.
2. Second killer is fork-loop elimination APIs.
3. Third killer is transfer/state policy one-liners.
4. Mapping key tracking is targeted, not baseline.

## Concrete Cheatcodes To Implement

### P0 (Must Ship First)

These are the strict highest-ROI APIs for removing repeated trace scans across parallel assertion functions.

### P0-A: Tx-level shared facts (runtime architecture)

Build once per tx, read many times from all assertion functions:
1. Call index by `(target, selector, callType, depth, success)`.
2. Storage write index by `(target, slot)`.
3. Log index by `(emitter, topic0)`.
4. Normalized transfer index by `(token, account, callId)`.
5. Changed slot index by `(target, slot)`.

This is the core performance lever. Without it, every assertion function replays parts of the same trace.

### P0-B: Scalar and quantified call facts (default API)

```solidity
struct CallFilter {
    bool successOnly;
    bool topLevelOnly;
    uint8 callType; // 0 any, 1 call, 2 delegatecall, 3 staticcall, 4 callcode
    uint32 minDepth;
    uint32 maxDepth;
}

struct TriggerContext {
    uint256 callId;
    address caller;
    address target;
    address codeAddress;
    bytes4 selector;
    uint32 depth;
}

function getTriggerContext() external view returns (TriggerContext memory);
function anyCall(address target, bytes4 selector, CallFilter calldata filter) external view returns (bool);
function countCalls(address target, bytes4 selector, CallFilter calldata filter) external view returns (uint256);
function allCallsBy(address target, bytes4 selector, address allowedCaller, CallFilter calldata filter)
    external
    view
    returns (bool);
function sumArgUint(address target, bytes4 selector, uint8 argIndex, CallFilter calldata filter)
    external
    view
    returns (uint256);
```

### P0-B2: Trigger-time filters (reduce parallel fan-out at source)

Apply call-shape filters at trigger registration so irrelevant calls do not schedule assertion functions.

```solidity
struct TriggerFilter {
    bool successOnly;
    bool topLevelOnly;
    uint8 callType; // 0 any, 1 call, 2 delegatecall, 3 staticcall, 4 callcode
    uint32 minDepth;
    uint32 maxDepth;
}

function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector, TriggerFilter calldata filter) external view;
function registerCallTriggers(bytes4 fnSelector, bytes4[] calldata triggerSelectors, TriggerFilter calldata filter)
    external
    view;
```

Why:
1. Prevents assertion-function over-triggering under parallel execution.
2. Eliminates repeated in-assertion filtering/skip logic (delegatecall/success/depth checks).
3. Reduces scheduler and precompile load before assertion code runs.

### P0-C: Scalar call-boundary facts (replace fork loops)

```solidity
enum CallPoint { PRE, POST }

function loadAtCall(address target, bytes32 slot, uint256 callId, CallPoint point)
    external
    view
    returns (bytes32);
function callerAt(uint256 callId) external view returns (address);
function slotDeltaAtCall(address target, bytes32 slot, uint256 callId) external view returns (int256);
```

### P0-D: Scalar write/token policy facts (replace loops over writes/logs/transfers)

```solidity
function anySlotWritten(address target, bytes32 slot) external view returns (bool);
function allSlotWritesBy(address target, bytes32 slot, address allowedCaller) external view returns (bool);

function erc20BalanceDiff(address token, address account) external view returns (int256);
function erc20SupplyDiff(address token) external view returns (int256);
function getERC20NetFlow(address token, address account) external view returns (int256);
function getERC20FlowByCall(address token, address account, uint256 callId) external view returns (int256);

function erc4626TotalAssetsDiff(address vault) external view returns (int256);
function erc4626TotalSupplyDiff(address vault) external view returns (int256);
function erc4626VaultAssetBalanceDiff(address vault) external view returns (int256);
function erc4626AssetsPerShareDiffBps(address vault) external view returns (int256 bps);
```

### P0-D2: Keyed/grouped aggregates (remove Solidity dedupe maps)

Push key discovery/dedup/aggregation into runtime for call- and event-derived keys.

```solidity
struct AddressUint {
    address key;
    uint256 value;
}

struct Bytes32Uint {
    bytes32 key;
    uint256 value;
}

function sumCallArgUintForAddress(
    address target,
    bytes4 selector,
    uint8 keyArgIndex,
    address key,
    uint8 valueArgIndex,
    CallFilter calldata filter
) external view returns (uint256);

function uniqueCallArgAddresses(address target, bytes4 selector, uint8 argIndex, CallFilter calldata filter)
    external
    view
    returns (address[] memory);

function sumCallArgUintByAddress(
    address target,
    bytes4 selector,
    uint8 keyArgIndex,
    uint8 valueArgIndex,
    CallFilter calldata filter
) external view returns (AddressUint[] memory);

function sumEventUintForTopicKey(address emitter, bytes32 topic0, uint8 keyTopicIndex, bytes32 key, uint8 valueDataIndex)
    external
    view
    returns (uint256);

function uniqueEventTopicValues(address emitter, bytes32 topic0, uint8 topicIndex)
    external
    view
    returns (bytes32[] memory);

function sumEventUintByTopic(address emitter, bytes32 topic0, uint8 keyTopicIndex, uint8 valueDataIndex)
    external
    view
    returns (Bytes32Uint[] memory);
```

Why:
1. Removes hand-written hash sets/accumulators in assertions (EVC/CTF/MYX patterns).
2. Keeps array APIs bounded by unique keys, not raw trace length.
3. Preserves scalar fast path (`sum...ForAddress`, `sum...ForTopicKey`) for common checks.

### P0-E: Array APIs stay as escape hatches only

Keep these for rare cases/debug tooling, but do not make them default for new assertions:
1. `getCalls(...)`, `getAllCalls(...)`
2. `getStorageWrites(...)`
3. `getLogsByEmitter(...)`, `getLogsByTopic0(...)`, `getLogsFiltered(...)`
4. `getAssetTransfers(...)`

Why this is P0:
1. Moves iteration/filtering into runtime once per tx.
2. Eliminates repeated decode/dedupe/fork loops in high-volume repos.
3. Keeps assertion functions small under parallel fan-out.
4. Preserves escape hatches without making them the dominant pattern.

### P1 (State/Mapping Ergonomics)

These convert common known-key invariants into one-liners.

1. Slot diffs:

```solidity
function getChangedSlots(address target) external view returns (bytes32[] memory);
function getSlotDiff(address target, bytes32 slot)
    external
    view
    returns (bytes32 pre, bytes32 post, bool changed);
```

2. Deterministic mapping-key checks (known key):

```solidity
function didMappingKeyChange(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset)
    external
    view
    returns (bool);

function mappingValueDiff(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset)
    external
    view
    returns (bytes32 pre, bytes32 post, bool changed);
```

3. Deterministic balance key checks:

```solidity
function didBalanceChange(address token, address account) external view returns (bool);
function balanceDiff(address token, address account) external view returns (int256);
```

Why P1 matters:
1. Most real invariants are about known addresses/keys.
2. No preimage tricks needed.
3. Directly replaces many `getCallInputs`-based key reconstruction patterns.
4. v1 key coverage comes from deterministic known keys and event/call-derived key sets.

## Runtime Performance Tasks (Executor-Side)

These changes are required so declarative APIs are actually faster in production:
1. Skip empty-selector assertion executions in store read path.
2. Early return from contract execution when `fn_selectors` is empty.
3. Adaptive parallelism:
   - no rayon fan-out for tiny workloads,
   - cap nested fan-out overhead for small selector sets.
4. Precompute tx indexes once (calls/writes/logs/transfers/slots) and reuse across all assertion functions.
5. Replace repeated full-journal scans in precompiles with indexed lookups.
6. Replace repeated full-log re-encoding with filtered/indexed encoders.
7. Add bounded response options for array APIs (limit/offset) to avoid worst-case decode costs.
8. Add trigger-time call-shape filtering in trigger matching to prune assertion-function fan-out early.
9. Add keyed/grouped aggregate indexes to remove repeated per-assertion dedupe/aggregation work.

If we do not ship this runtime layer, new cheatcodes may reduce LoC but still regress latency.

## Repo-Level ROI Mapping

1. `ethereum-vault-connector`:
   - Highest ROI: P0 call facts + event-derived account extraction + P1 known-key diffs.
2. `malda-lending`:
   - Highest ROI: P0 call facts + `loadAtCall`, plus P1 known-key diffs.
3. `morpho-vault-2`, `aave-v3-origin`, `myx-v2-assertions`:
   - Highest ROI: P0 call facts + storage/asset facts.
4. `lagoon-v0`:
   - Highest ROI: filtered logs + slot diff facts.
5. `etherex-assertions`:
   - Highest ROI: storage write provenance and slot diff helpers.

## Plain-English To Trivial Expression

Design rule:
1. If the invariant can be stated in one sentence, it should map to 1-3 lines.
2. Assertion code should read like policy, not trace replay.

To make that real, we need both:
1. Runtime cheatcodes (facts).
2. `credible-std` expression helpers (predicates built on those facts).

Suggested expression helpers:
1. `allCallsDeltaGE(target, selector, slot, minDelta, filter)`
2. `allCallsDeltaLE(target, selector, slot, maxDelta, filter)`
3. `allSlotWritesBy(target, slot, allowedCaller)`
4. `sumFlowsEq(token, accountA, accountB, expectedNet)`
5. `withinBps(actual, expected, bps)`

| Plain English invariant | Trivial expression pattern |
| --- | --- |
| Only ProxyAdmin may upgrade implementation | `require(allSlotWritesBy(proxy, IMPLEMENTATION_SLOT, proxyAdmin), \"unauthorized_upgrade\");` |
| If borrow happened, all borrow calls must be by trusted entrypoint | `require(allCallsBy(pool, BORROW_SEL, trusted, successOnly()), \"unauthorized_borrow_path\");` |
| Total outflow this tx must not exceed limit | `require(getERC20NetFlow(asset, vault) >= -int256(limit), \"outflow_limit\");` |
| Every successful call to `f` must keep field `X` non-decreasing | `require(allCallsDeltaGE(target, F_SEL, SLOT_X, 0, successOnly()), \"x_decreased\");` |
| Sum of transfer amounts equals accounting delta | `require(sumArgUint(target, F_SEL, amountArg, successOnly()) == uint256(erc20SupplyDiff(token)), \"conservation\");` |
| No writes to critical slot in this tx | `require(!anySlotWritten(target, CRITICAL_SLOT), \"critical_slot_written\");` |
| Mapping key `k` must not change | `require(!didMappingKeyChange(target, BASE_SLOT, k, 0), \"key_changed\");` |
| If key changed, value must stay within bounds | `(bytes32 pre, bytes32 post, bool changed) = mappingValueDiff(...); if (changed) require(uint256(post) <= MAX, \"bound\");` |
| Triggered call must preserve permission gate | `TriggerContext memory t = getTriggerContext(); require(_gateHolds(t.callId), \"gate_violation\");` |
| Only expected keys may change | `require(allKeysInSet(changedKeysFromEvents(...), allowlist), \"unexpected_key_change\");` |

Recommended helper layer in `credible-std`:
1. `successOnly()`, `topLevelOnly()`, `delegateCallOnly()` filter constructors.
2. `allSlotWritesBy(...)`, `allCallsDeltaGE(...)`, `allCallsDeltaLE(...)` policy wrappers.
3. `withinBps(actual, expected, bps)` comparator to remove repeated tolerance boilerplate.

## Example Before/After LoC Reductions

All reductions below are estimated and based on current files/ranges.

| Project | Assertion (before) | Before LoC | After LoC (est.) | Reduction |
| --- | --- | ---: | ---: | ---: |
| `ethereum-vault-connector` | `assertions/src/AccountHealthAssertion.a.sol:123` | ~200 | ~28 | ~86% |
| `malda-lending` | `assertions/src/OutflowLimiterAssertion.a.sol:60` | ~55 | ~20 | ~64% |
| `lagoon-v0` | `assertions/src/SyncDepositModeAssertion_v0.5.0.a.sol:120` | ~55 | ~20 | ~64% |
| `etherex-assertions` | `assertions/src/AccessHubUpgradeAssertion.a.sol:41` | ~35 | ~14 | ~60% |
| `aave-v3-origin` | `assertions/src/production/BorrowingInvariantAssertions.a.sol:257` | ~36 | ~16 | ~56% |
| `morpho-vault-2` | `assertions/src/VaultV2ConfigAssertions.a.sol:97` | ~61 | ~22 | ~64% |
| `myx-v2-assertions` | `assertions/src/CollateralAccountingAssertion.a.sol:46` | ~133 | ~34 | ~74% |

Summary:
1. Main wins come from removing call/log/fork replay boilerplate.
2. Scalar/quantified APIs produce the largest readability and performance improvements.
3. Array APIs remain transitional escape hatches for custom cases.

## Simple Additions We Should Also Do (Common ERC20 + Repeated Patterns)

These are low-complexity/high-frequency helpers that appear in almost every repo.

Observed frequency across current assertion suites:
1. `balanceOf(...)` calls: `58`
2. `totalSupply(...)` calls: `28`
3. `allowance(...)` calls: `8`
4. Manual `Transfer` signature/log handling: `6` hot spots

### A) ERC20 pre/post one-liners

```solidity
enum TxPoint { PRE_TX, POST_TX }

function erc20BalanceAt(address token, address account, TxPoint point) external view returns (uint256);
function erc20SupplyAt(address token, TxPoint point) external view returns (uint256);
function erc20AllowanceAt(address token, address owner, address spender, TxPoint point) external view returns (uint256);

function erc20BalanceDiff(address token, address account) external view returns (int256);
function erc20SupplyDiff(address token) external view returns (int256);
function erc20AllowanceDiff(address token, address owner, address spender)
    external
    view
    returns (int256 diff, uint256 pre, uint256 post);
```

Why:
1. Removes repetitive fork/read boilerplate.
2. Makes 60%+ of simple token assertions one-liners.

### B) ERC20 transfer views (filtered and normalized)

```solidity
function getERC20NetFlow(address token, address account) external view returns (int256);
function getERC20FlowByCall(address token, address account, uint256 callId) external view returns (int256);
function getERC20Transfers(address token) external view returns (AssetTransfer[] memory); // escape hatch
```

Why:
1. Avoids manual topic/data decoding.
2. Replaces event-join logic in accounting assertions.

### C) Common allowance/approve patterns

```solidity
function assertAllowanceSetByApprove(
    address token,
    address owner,
    address spender,
    uint256 expected
) external view returns (bool);

function assertAllowanceMonotonicAfterTransferFrom(
    address token,
    address owner,
    address spender
) external view returns (bool);
```

Why:
1. Morpho-style allowance assertions are currently repetitive.
2. These cover a recurring pattern with almost no protocol coupling.

### D) ERC4626 quick helpers (also very common)

```solidity
function erc4626SharePriceAt(address vault, TxPoint point) external view returns (uint256);
function erc4626SharePriceDiffBps(address vault) external view returns (int256 bps);
function erc4626AssetsPerShareAt(address vault, TxPoint point) external view returns (uint256);
function erc4626TotalAssetsDiff(address vault) external view returns (int256);
function erc4626TotalSupplyDiff(address vault) external view returns (int256);
function erc4626VaultAssetBalanceDiff(address vault) external view returns (int256);
function erc4626AssetsPerShareDiffBps(address vault) external view returns (int256 bps);
```

Why:
1. EVC/Lagoon/Morpho repeatedly compute share-price style invariants.
2. Delta-first helpers avoid repeated `forkPreTx/forkPostTx` boilerplate.
3. Avoids each assertion re-implementing tiny variations of the same formula.
4. Keep `preview*`-based checks non-default to avoid business-logic coupling.

### E) Generic policy helpers that always show up

```solidity
function allCallsBy(address target, bytes4 selector, address allowedCaller) external view returns (bool);
function anySlotWritten(address target, bytes32 slot) external view returns (bool);
function allSlotWritesBy(address target, bytes32 slot, address allowedCaller) external view returns (bool);
```

Why:
1. Upgrade/auth assertions in Etherex and config assertions in other repos are mostly this pattern.
2. Turns imperative loops into declarative policy checks.

### Practical Priority

If we want immediate wins without waiting for the full roadmap:
1. Implement tx-level shared indexes + scalar quantifiers first (P0-A through P0-D2).
2. Then A + B (ERC20 boilerplate killer on top of shared indexes).
3. Then E (auth/policy one-liners).
4. Then D (ERC4626 convenience).
5. Keep C as a stdlib predicate layer on top of scalar call/token facts.

## Strict Highest-ROI Rollout

1. Ship executor fast-paths first:
   - Skip empty-selector assertion executions.
   - Early return when `fn_selectors` is empty.
   - Adaptive parallelism thresholds for tiny workloads.
2. Ship shared tx indexes:
   - calls, writes, logs, transfers, changed slots
   - single build per tx, read-only across all assertion functions
3. Ship Trigger Gate Pack:
   - filtered trigger registration (`successOnly`, `topLevelOnly`, call-type, depth bounds)
   - multi-selector filtered registration
4. Ship Loop-Elimination Pack:
   - `getTriggerContext`
   - `anyCall` / `countCalls` / `allCallsBy` / `sumArgUint`
   - `CallFilter` presets (`successOnly`, `topLevelOnly`, call-type filter)
   - `loadAtCall` / `callerAt` / `slotDeltaAtCall`
5. Ship Keyed Aggregate Pack:
   - `sumCallArgUintForAddress`, `uniqueCallArgAddresses`, `sumCallArgUintByAddress`
   - `sumEventUintForTopicKey`, `uniqueEventTopicValues`, `sumEventUintByTopic`
6. Ship Token Facts Pack:
   - `erc20BalanceDiff`, `erc20SupplyDiff`, `erc20AllowanceDiff`
   - `getERC20NetFlow`, `getERC20FlowByCall`
   - `erc4626TotalAssetsDiff`, `erc4626TotalSupplyDiff`, `erc4626VaultAssetBalanceDiff`, `erc4626AssetsPerShareDiffBps`
7. Ship Policy Pack:
   - `allSlotWritesBy`, `anySlotWritten`
   - filtered logs helpers
   - keep `getStorageWrites` as escape hatch
8. Rewrite top ROI assertions first:
   - `ethereum-vault-connector/assertions/src/AccountHealthAssertion.a.sol`
   - `malda-lending/assertions/src/OutflowLimiterAssertion.a.sol`
   - `morpho-vault-2/assertions/src/VaultV2ConfigAssertions.a.sol`
   - `aave-v3-origin/assertions/src/production/BorrowingInvariantAssertions.a.sol`
9. Ship P1 deterministic mapping/balance diffs for known-key invariants.
10. Add lint gate:
   - fail on new manual `for (...)` loops over `getCallInputs/getLogs` unless allowlisted.
11. Add API guardrails:
   - mark array-returning APIs as non-default in docs/examples
   - provide scalar alternative in every new helper namespace

## Success Metrics

1. >= 60% reduction in explicit call/log loops.
2. >= 70% reduction in explicit `forkPreCall/forkPostCall` use.
3. Zero new assertions that re-implement protocol formulas unless justified.
4. No new assertions require manual key-reconstruction loops for key coverage.
5. >= 30% reduction in p95 `assertion_execution_duration` per tx.
6. >= 50% reduction in precompile bytes returned per assertion function (scalar-first effect).
7. No measurable regression in tx throughput under high assertion fan-out.
8. >= 50% reduction in triggered assertion-function executions with zero relevant calls.
9. Zero new manual dedupe/accumulator maps for call/event keyed invariants where keyed aggregate APIs exist.

## Implementation Progress

### Phase 0: Baseline -- DONE
- Baseline recorded: 214 tests passing (1 pre-existing failure: MockAssertion.json artifact)
- Benchmark baselines: eoa_transaction_aa=44.824µs, erc20_transaction_aa=67.632µs, uniswap_transaction_aa=387.06µs

### Phase 1: Selector-Pruning in Store Read Path -- DONE
- Changed `read_adopter()` from `.map()` to `.filter_map()` to skip assertions with no matched selectors
- Added `default_test_trigger_recorder()` test helper and new `test_empty_selector_assertions_pruned_from_read` test
- All 213 tests pass (1 pre-existing failure)

### Phase 2: Zero-Work Fast Paths in Executor -- DONE
- Added early return for empty `fn_selectors` in `run_assertion_contract` (both mod.rs and with_inspector.rs)
- All 213 tests pass

### Phase 3: Adaptive Parallelism -- DONE
- Added `PARALLEL_THRESHOLD = 2` constant
- `execute_triggered_assertions` and `run_assertion_contract` use sequential execution when workload < threshold
- Extracted shared closures for sequential/parallel code paths
- All 213 tests pass

### Phase 4: Perf Instrumentation -- DONE
- Added debug/trace logging for scheduling decisions (assertion_count, total_selectors, scheduling mode)
- Non-invasive instrumentation using `tracing` macros
- All 213 tests pass

### Phase 5: Broader Validation Gate -- DONE
- Benchmark results after all changes:
  - eoa_transaction_aa: 34.411µs (-23% from baseline)
  - erc20_transaction_aa: statistically significant -43% improvement in one run
  - uniswap_transaction_aa: 339.26µs (-12% from baseline)

### Phase 6: Canonical Interface Source -- DONE
- Created `crates/assertion-executor/interfaces/PhEvm.sol` and `ITriggerRecorder.sol` as canonical sources
- Updated `sol_primitives.rs` to use file-path `sol!()` form
- Added 3 selector stability tests (14 PhEvm methods, 5 ITriggerRecorder methods, overload distinctness)
- All 213 tests pass

### Phase 7: Declarative Cheatcodes (P0 Scalar Facts) -- DONE
- Implemented 14 new scalar-first cheatcodes across 6 slices:
  - Slice 1: `anyCall`, `countCalls`, `callerAt` with `CallFilter` struct + depth tracking in CallTracer
  - Slice 2: `allCallsBy`, `sumArgUint`
  - Slice 3: `anySlotWritten`, `allSlotWritesBy` (journal-based write provenance)
  - Slice 4: `loadAtCall`, `slotDeltaAtCall` (fork-switching call-boundary reads)
  - Slice 5: `getTriggerContext` (trigger call ID threading from store → executor → precompile)
  - Slice 6: `erc20BalanceDiff`, `erc20SupplyDiff`, `getERC20NetFlow`, `getERC20FlowByCall` (Transfer event scanning)
- 4 new precompile modules: `call_facts.rs`, `write_policy.rs`, `call_boundary.rs`, `erc20_facts.rs`
- New dispatch stage `execute_scalar_facts_precompile` in phevm.rs
- All 247 tests pass (up from 213 baseline, includes fix for previously broken MockAssertion.json test)
- Phase 7 completion update (post hardening): ERC4626 helper cheatcodes implemented.
  - Added `erc4626TotalAssetsDiff`, `erc4626TotalSupplyDiff`, `erc4626VaultAssetBalanceDiff`, `erc4626AssetsPerShareDiffBps`.
  - Implemented via bounded fork-aware view calls on cloned MultiForkDb state (`erc4626_facts.rs`) to keep assertions declarative and business-logic independent.
  - Current suite: 288 tests passed (including new ERC4626 helper unit coverage).

### Phase 8: Declarative Cheatcodes (P1 Mapping + Diff Facts) -- DONE
- Implemented 6 new deterministic known-key cheatcodes:
  - `getChangedSlots(address)` — journal-scanned, returns only net-changed slots (sorted, deterministic)
  - `getSlotDiff(address, bytes32)` — returns (pre, post, changed) tuple from journal first-had-value + state
  - `didMappingKeyChange(address, bytes32, bytes32, uint256)` — keccak256(key++baseSlot)+fieldOffset slot computation
  - `mappingValueDiff(address, bytes32, bytes32, uint256)` — returns (pre, post, changed) for mapping slot
  - `didBalanceChange(address, address)` — Transfer event net-flow != 0
  - `balanceDiff(address, address)` — equivalent to erc20BalanceDiff
- New module: `slot_diffs.rs` (getChangedSlots, getSlotDiff, didMappingKeyChange, mappingValueDiff)
- Extended `erc20_facts.rs` (didBalanceChange, balanceDiff)
- 263 tests pass (16 new)

### Phase 9: Assertion Rewrite + Helper Layer -- DEFERRED (cross-repo)
- Requires modifying `credible-std` (separate repo) for helper wrappers/presets.
- Requires rewriting assertions in downstream repos (EVC, Malda, Morpho, Aave, etc.).
- Canonical PhEvm.sol interface (44 methods) is ready for sync.
- Deferred until Phase 10 cross-repo sync is in place.

### Phase 10: Cross-Repo Interface Sync Automation -- DEFERRED
- `credible-std` is a separate repo; automation should run cross-repo via CI workflows and PR bots.
- In `credible-sdk`, only submodule pointer bumps should be done after upstream `credible-std` updates.

### Phase 11: Runtime Performance Hardening Gate -- DONE
- **StorageChangeIndex**: DONE (`8ac766e`). Lazily-built index on `CallTracer` via `OnceLock` pre-indexes all `StorageChanged` journal entries by `(address, slot)`. Replaces O(journal) scans with O(1) lookups in `slot_diffs`, `state_changes`, and `write_policy` precompiles. 266 tests pass.
- **Correctness + Robustness Fix Pack (from review findings)**: DONE.
  - [x] Fix `CallTracer` constructors to initialize `storage_change_index` (`tracer.rs`).
  - [x] Fix `sumArgUint` unchecked arithmetic overflow/panic on large `argIndex` (`call_facts.rs`).
  - [x] Fix `anySlotWritten` double-charge gas accounting bug (`write_policy.rs`).
  - [x] Harden `allSlotWritesBy` writer attribution to select innermost call deterministically (reverse start-order match), including nested same-pre checkpoint edge case (`write_policy.rs`).
  - [x] Replace `allSlotWritesBy` O(matching_writes * call_records) path with one-pass call-window attribution (`write_policy.rs`).
  - [x] Harden `getERC20FlowByCall` against malformed checkpoint bounds (`erc20_facts.rs`).
  - [x] Propagate call IDs for `AllCalls` selectors and dedupe per-selector call IDs (`assertion_store.rs`).
  - [x] Remove repeated filtered `Vec` materialization in `anyCall/countCalls/allCallsBy/sumArgUint` (`call_facts.rs`).
  - [x] Cache per-trigger selector call-id extraction once per adopter read (`assertion_store.rs`).
  - [x] Multi-call execution semantics in executor (run selector once per triggering call ID, non-call selectors once).
  - [x] Remove selector cloning/formatting allocation in executor preparation path (`executor/mod.rs`, `executor/with_inspector.rs`).
  - [x] Add/adjust tests for multi-call trigger context semantics (normal + inspector paths).
  - [x] Fix failing full package doctest in `store/indexer.rs` example (`connect_ws`, remove stale `.boxed()`).
  - [x] Commit fix pack + update status with commit IDs and validation commands.
    - Commits:
      - `cd6a6e2` — executor/precompile/store hardening + multi-call trigger context support.
      - `bc272fd` — plan checklist progress tracking.
      - `5fddeb0` — one-pass `allSlotWritesBy` attribution + executor selector-prep allocation reduction.
      - `c496d62` — indexer doctest fix for current provider API.
      - `2b6f6f9` — Phase 11 checklist/status refresh with benchmark outcomes.
    - Validation run:
      - `cargo test -p assertion-executor@1.0.8 --no-default-features --features "optimism test" --lib` (268 passed).
      - `cargo bench -p assertion-executor@1.0.8 --bench assertion_store_read -- --quick`.
      - `cargo bench -p assertion-executor@1.0.8 --bench executor_transaction_perf -- --quick`.
      - `cargo bench -p assertion-executor@1.0.8 --bench executor_avg_block_perf -- --quick`.
  - [x] Add regression test for nested same-pre checkpoint write attribution (`write_policy.rs`):
    - `test_all_slot_writes_by_nested_same_pre_attributed_to_innermost`
  - [x] Add adversarial write-policy tests:
    - `test_all_slot_writes_by_multiple_writes_mixed_callers`
    - `test_all_slot_writes_by_write_outside_any_call`
  - [x] Re-run full (non-quick) benchmark suites and record outcome:
    - `cargo bench -p assertion-executor@1.0.8 --bench assertion_store_read`
    - `cargo bench -p assertion-executor@1.0.8 --bench executor_transaction_perf`
    - `cargo bench -p assertion-executor@1.0.8 --bench executor_avg_block_perf`
    - `cargo bench -p assertion-executor@1.0.8 --bench call-tracer-truncate`
    - `cargo bench -p assertion-executor@1.0.8 --bench worst-case-op`
    - Summary (latest runs on this branch):
      - `assertion_store_read`: stable/no regression.
      - `executor_transaction_perf`: broad improvement (EOA/ERC20/Uniswap scenarios improved or within noise).
      - `executor_avg_block_perf`: vanilla/0_aa/100_aa improved; 3_aa no significant change.
      - `call-tracer-truncate`: mixed but neutral-to-positive on reruns (no reproducible heavy-case regression).
      - `worst-case-op`: noisy mixed microbench profile; no consistently reproducible regression across reruns.
  - [x] Full package validation includes doctests:
    - `cargo test -p assertion-executor@1.0.8 --no-default-features --features "optimism test"` (288 unit tests passed, doctests passed).
- **Performance Extension Pack (remaining Phase 11 items)**: DONE.
  - [x] Commit:
    - `c7388e8` — completed extension pack implementation (log encoding cache, bounded array responses, trigger-time filters, keyed/grouped aggregates, tx log index reuse) plus validation updates.
  - [x] Item 6 (log re-encoding caching):
    - Added tx-scope `getLogs()` ABI-encoding cache on `CallTracer` (`encoded_logs_cache`) and used it from `get_logs.rs`.
    - Added regression test: `test_get_logs_populates_tracer_encoding_cache`.
  - [x] Item 7 (bounded array responses):
    - Added hard bound `MAX_ARRAY_RESPONSE_ITEMS = 4096`.
    - Enforced bounded responses for array-returning hot precompiles:
      - `getLogs` (`TooManyLogs`)
      - `getCallInputs` (`TooManyCallInputs`)
      - `getStateChanges` (`TooManyStateChanges`)
      - `getChangedSlots` (`TooManyChangedSlots`)
    - Added bound regression tests for each precompile path.
  - [x] Item 8 (trigger-time call-shape filtering):
    - Extended canonical `ITriggerRecorder.sol` with `TriggerFilter` plus filtered overloads:
      - `registerCallTrigger(bytes4, TriggerFilter)`
      - `registerCallTrigger(bytes4, bytes4, TriggerFilter)`
    - Added runtime trigger variants:
      - `TriggerType::CallFiltered`
      - `TriggerType::AllCallsFiltered`
    - Updated `read_adopter` matching to support filtered semantics with per-call trigger IDs.
    - Added fast path for legacy (non-filtered) trigger sets to preserve existing hot-path performance.
    - Added unit tests:
      - `test_call_filtered_trigger_selects_matching_call_ids`
      - `test_all_calls_filtered_trigger_uses_depth_filter`
  - [x] Item 9 (keyed/grouped aggregate indexes and APIs):
    - Added 6 declarative aggregate cheatcodes in canonical `PhEvm.sol`:
      - `sumCallArgUintForAddress`
      - `uniqueCallArgAddresses`
      - `sumCallArgUintByAddress`
      - `sumEventUintForTopicKey`
      - `uniqueEventTopicValues`
      - `sumEventUintByTopic`
    - New precompile module: `aggregate_facts.rs` with grouped/summed call/event implementations and tests.
    - Added tx log index on `CallTracer` (`log_index_by_emitter_topic0`) and reused it for:
      - event aggregate cheatcodes
      - ERC20 fact precompiles (`erc20_facts.rs`) to avoid repeated full-log scans.
  - [x] Item 1-3 verification:
    - Empty-selector short-circuits already present.
    - Adaptive parallelism already present.
  - [x] Focused benchmark runs completed:
    - `cargo bench -p assertion-executor@1.0.8 --bench assertion_store_read`
    - `cargo bench -p assertion-executor@1.0.8 --bench executor_transaction_perf`
    - `cargo bench -p assertion-executor@1.0.8 --bench executor_avg_block_perf`
    - `cargo bench -p assertion-executor@1.0.8 --bench call-tracer-truncate`
    - `cargo bench -p assertion-executor@1.0.8 --bench worst-case-op` (rerun twice to check noisy outliers)
    - Summary (latest `main` vs branch compare run with Criterion baseline `main_ref`):
      - `assertion_store_read`: mixed, with material hit-path regression (~+29%) and miss-path improvement (~-3.4%).
      - `executor_transaction_perf`: mixed; large wins on AA and several ERC20 paths, regressions in `erc20_vanilla` and `uniswap_transaction_aa`.
      - `executor_avg_block_perf`: mostly neutral-to-regressed (`avg_block_3_aa` and `avg_block_100_aa` regressions).
      - `call-tracer-truncate`: mixed with notable regressions on `15k` / `500`, improvement on `15k_deep_pending`.
      - `worst-case-op`: mostly stable/noise; `KECCAK` and `SSTORE` improved, `LOG0` regressed.
  - [x] Regression remediation follow-up (current branch):
    - `CallTracer` hot-path changes:
      - avoid calldata re-copy when input is already `CallInput::Bytes`
      - single-lookup `(target,selector)` index insertion in `record_call_start`
      - added borrowed call-id accessor `call_ids_for(target, selector)`
    - `AssertionStore` hot-path changes:
      - switched trigger call-id prep from allocated `Vec<usize>` to borrowed slices
      - lazy all-call union materialization (only when `AllCalls`/`AllCallsFiltered` exists)
      - single-call/simple-trigger fast path in `read_adopter`
      - skip sort/dedup for selector call-id vectors of size `<= 1`
      - empty-store short-circuit in `read` via conservative `has_any_assertions` hint
    - Validation:
      - `cargo test -p assertion-executor@1.0.8 --no-default-features --features "optimism test"` (288 passed)
      - `cargo bench -p assertion-executor@1.0.8 --bench assertion_store_read -- --baseline main_ref`
      - `cargo bench -p assertion-executor@1.0.8 --bench call-tracer-truncate -- --baseline main_ref2`
    - Latest snapshots:
      - `assertion_store_read` vs `main_ref`: `hit_existing_assertion` near-neutral (`~+2.8%`), `miss_nonexistent_assertion` strongly improved (`~-95%`).
      - `call-tracer-truncate` vs fresh `main_ref2` (same-window A/B): `15k` improved (`~-6.1%`), `500` improved (`~-44.1%`), `500_deep_pending` improved (`~-25.7%`), `15k_deep_pending` regressed (`~+12.2%`) and remains noisy across reruns.
  - [x] Post-pass benchmark/setup optimization (2026-02-14):
    - Added artifact bytecode cache in `test_utils.rs` (`OnceLock<RwLock<HashMap<...>>>`) to avoid repeated artifact JSON reads/decodes in bench setup paths (`bytecode`, `deployed_bytecode`).
    - Added unit tests for cache correctness:
      - `artifact_bytecode_cache_matches_fresh_decode`
      - `public_bytecode_helpers_return_cached_values`
    - Evaluated and dropped `SmallVec` trigger-call storage experiment (no clear perf gain, extra complexity).
    - Added sequential invocation fast paths in executor hot paths (`executor/mod.rs`):
      - `execute_triggered_assertions`: sequential branch now uses explicit loop instead of iterator `map(...).collect()`.
      - `run_assertion_contract`: sequential branch now executes selectors directly without materializing `selector_executions` vector.
      - Replaced expensive trace formatting allocation with count-only trace field.
    - Parallelism policy unlock (hot-path driven):
      - Added a dedicated fn-level parallelization policy (`should_parallelize_assertion_fns`) with conservative threshold (`>= 8` work items).
      - Kept outer contract-level parallelism unchanged; reduced over-scheduling on short selector/call-trigger batches.
      - Applied consistently to both executor paths (`executor/mod.rs`, `executor/with_inspector.rs`).
    - Cleanup/reviewability pass:
      - Reduced duplicated selector execution code in sequential paths via shared local execution closures.
      - Added explicit inline comments around trigger-context and multi-call semantics in executor hot paths.
      - Kept allocation-heavy selector expansion only on the parallel path.
    - Samply profiling loop (`executor_avg_block_performance/avg_block_100_aa`):
      - Confirmed artifact cache removed prior `serde_json`/`read_artifact` setup hotspot.
      - New dominant user-space hotspots moved to executor runtime path (`AssertionExecutor::validate_transaction`, `execute_triggered_assertions`, `execute_assertions`) and REVM inspect loop.
    - Validation:
      - `cargo test -p assertion-executor@1.0.8 --lib` (306 passed)
      - Focused same-window A/B medians (main `c68b725` vs current branch):
        - `executor_avg_block_performance/avg_block_100_aa`: `23.904 ms` -> `20.991 ms` (`-12.19%`, major improvement)
        - `assertion_store::read/hit_existing_assertion`: `661.65 ns` -> `687.52 ns` (`+3.91%`, small regression)
        - `executor_transaction_performance/erc20_vanilla`: `14.669 us` -> `10.553 us` (`-28.06%`, major improvement)
      - Post-cleanup local rerun snapshots (current branch, Criterion continuation):
        - `executor_avg_block_performance/avg_block_100_aa`: `[21.377 ms 21.523 ms 21.771 ms]` (no significant change)
        - `assertion_store::read/hit_existing_assertion`: `[686.29 ns 689.18 ns 692.11 ns]` (no significant change)

## Execution Appendix (Condensed)

The long-form execution checklist has been intentionally compacted to keep this file reviewable.
Phase status remains tracked above under `Implementation Progress`.

Execution guardrails:
1. Implement in `credible-sdk`; keep unrelated paths untouched.
2. Commit per phase with explicit test/bench commands and observed deltas.
3. Prefer scalar/indexed query paths over array scan paths in hot precompiles.
4. Keep `credible-sdk` canonical interfaces (`interfaces/PhEvm.sol`, `interfaces/ITriggerRecorder.sol`) as source of truth for cross-repo sync.

Definition of done:
1. No correctness regressions in assertion-executor test suites.
2. No material perf regressions in targeted benches.
3. Remaining deferred phases are explicitly marked as deferred with owner/context.
3. Benchmarks show neutral-to-positive impact on tx validation latency.
4. New behavior is covered by unit tests.
5. Declarative cheatcodes from P0 and P1 are implemented and used in rewritten top-ROI assertions.
6. Array-loop-based assertion patterns are no longer the default authoring path.
7. Interface drift checks and auto-PR sync are defined for cross-repo rollout (Phase 10).
8. Runtime hardening gate (Phase 11) passes with indexed precompile paths and measured latency improvements.
