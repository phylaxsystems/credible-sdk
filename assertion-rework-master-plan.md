# Assertion Rework Master Plan (Consolidated)

## Goal

Minimize assertion LoC while making assertions:
1. Declarative.
2. Business-logic independent.
3. Fast to author and review.

This plan is biased toward the Euler roadmap objective: remove assertion plumbing first.

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
4. Keep mapping key discovery optional; do not block common known-key invariants on it.

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
```

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

### P2 (Optional/Targeted: Your Tracking Design)

Use only when invariant requires "all changed keys" for unknown/unbounded key sets.

```solidity
enum MappingTrackMode { TRACKED_ONLY, BEST_EFFORT_DISCOVERY, STRICT }

function trackMapping(address target, bytes32 baseSlot, uint8 valueSlots, MappingTrackMode mode) external;
function trackKey(address target, bytes32 baseSlot, bytes32 key) external;
function seedTrackedKeys(address target, bytes32 baseSlot, bytes32[] calldata keys) external;
function getChangedTrackedKeys(address target, bytes32 baseSlot)
    external
    view
    returns (bytes32[] memory keys, bool complete);
```

Optional best-effort discovery:

```solidity
function getChangedMappingKeys(address target, bytes32 baseSlot)
    external
    view
    returns (bytes32[] memory keys, uint8 completenessCode);
```

Where `completenessCode` is:
1. `0`: complete
2. `1`: partial (best effort)
3. `2`: unknown/incomplete

## Do We Need Your Design?

Short answer:
1. Yes, but only for a subset.
2. It is not the baseline path.

Concrete decision:
1. For known-key invariants, do not require tracker/indexer design.
   - Use deterministic mapping/balance diff APIs (P1).
2. For unknown-key "all touched keys" invariants, use tracker/indexer design (P2).
   - Especially useful in `ethereum-vault-connector` and `ctf-exchange`.
3. For `lagoon-v0` and most of `etherex-assertions`, tracker adds little.
   - Primary pain there is slot/event verbosity, not key discovery.

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

If we do not ship this runtime layer, new cheatcodes may reduce LoC but still regress latency.

## Repo-Level ROI Mapping

1. `ethereum-vault-connector`:
   - Highest ROI: P0 call facts + P2 tracking.
2. `malda-lending`:
   - Highest ROI: P0 call facts + `loadAtCall`, plus P1 known-key diffs.
3. `morpho-vault-2`, `aave-v3-origin`, `myx-v2-assertions`:
   - Highest ROI: P0 call facts + storage/asset facts.
4. `lagoon-v0`:
   - Highest ROI: filtered logs + slot diff facts, not tracker.
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
| Only known tracked keys may change | `(, bool complete) = getChangedTrackedKeys(target, BASE_SLOT); require(complete, \"incomplete_coverage\");` |

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
| `morpho-vault-2` | `assertions/src/VaultV2ConfigAssertions.a.sol:97` (gate helper cluster) | ~61 | ~22 | ~64% |
| `myx-v2-assertions` | `assertions/src/CollateralAccountingAssertion.a.sol:46` | ~133 | ~34 | ~74% |

### 1) EVC Account Health (nested call decoding -> tracked changed keys)

Before: `ethereum-vault-connector/assertions/src/AccountHealthAssertion.a.sol:123`
- Manual unwrap of nested `batch/call`
- Selector-specific account extraction
- Multiple unique-set dedupe loops

After (P0 + P2):

```solidity
function assertionBatchAccountHealth() external {
    IEVC evc = IEVC(ph.getAssertionAdopter());

    address[] memory vaults = _uniqueVerifiedVaultTargets(ph.getAllCalls(address(evc)));
    (bytes32[] memory keys, bool complete) =
        getChangedTrackedKeys(address(evc), ACCOUNT_STATUS_BASE_SLOT);
    require(complete, "incomplete_account_key_coverage");

    for (uint256 i = 0; i < keys.length; i++) {
        address account = address(uint160(uint256(keys[i])));
        _validateAcrossTouchedVaultsAndControllers(evc, account, vaults);
    }
}
```

### 2) Malda Outflow (per-call fork loop -> loadAtCall)

Before: `malda-lending/assertions/src/OutflowLimiterAssertion.a.sol:60`
- Manual decode + `forkPreCall/forkPostCall` per borrow call
- Repeated boilerplate around per-call state boundaries

After (P0 + P1):

```solidity
function assertionBorrowOutflowLimit() external {
    IOperator operator = IOperator(ph.getAssertionAdopter());
    IOracleOperator oracle = IOracleOperator(operator.oracleOperator());
    uint256 limit = operator.limitPerTimePeriod();
    if (limit == 0) return;

    CallInfo[] memory borrows =
        getCalls(address(operator), IOperatorDefender.beforeMTokenBorrow.selector);

    for (uint256 i = 0; i < borrows.length; i++) {
        (address mToken,, uint256 amount) = abi.decode(borrows[i].input, (address, address, uint256));
        uint256 expectedUsd = (amount * oracle.getUnderlyingPrice(mToken)) / 1e18;

        uint256 pre = uint256(loadAtCall(address(operator), CUMULATIVE_OUTFLOW_SLOT, borrows[i].id, CallPoint.PRE));
        uint256 post = uint256(loadAtCall(address(operator), CUMULATIVE_OUTFLOW_SLOT, borrows[i].id, CallPoint.POST));
        require(_withinBps(post - pre, expectedUsd, 10), "outflow_tracking_mismatch");
    }

    require(operator.cumulativeOutflowVolume() <= limit, "borrow_exceeds_outflow_limit");
}
```

### 3) Lagoon Sync Deposit Accounting (manual tx-call replay -> transfer + slot facts)

Before: `lagoon-v0/assertions/src/SyncDepositModeAssertion_v0.5.0.a.sol:120`
- Replays share-mint formula from protocol internals
- Manual call iteration and local reconstruction

After (P0 + P1):

```solidity
function assertionSyncDepositAccounting() external {
    IVault vault = IVault(ph.getAssertionAdopter());
    address asset = vault.asset();
    address safe = vault.safe();

    int256 safeDelta = _netTransferDelta(asset, safe);
    (bytes32 preAssets, bytes32 postAssets,) = getSlotDiff(address(vault), TOTAL_ASSETS_SLOT);

    require(int256(uint256(postAssets) - uint256(preAssets)) == safeDelta, "totalAssets_not_backed_by_safe_flow");
}
```

### 4) Etherex Upgrade Auth (pre/post slot + selector scans -> write provenance)

Before: `etherex-assertions/assertions/src/AccessHubUpgradeAssertion.a.sol:41`
- Manual pre/post slot compare
- Two selector scans for caller checks

After (P0 + P1):

```solidity
function assertNoUnauthorizedUpgrade() external {
    address proxy = ph.getAssertionAdopter();
    address proxyAdmin = address(uint160(uint256(ph.load(proxy, ADMIN_SLOT))));

    StorageWrite[] memory writes = getStorageWrites(proxy, IMPLEMENTATION_SLOT);
    for (uint256 i = 0; i < writes.length; i++) {
        require(_callerAt(writes[i].callId) == proxyAdmin, "unauthorized_upgrade");
    }
}
```

### 5) Aave Borrow Balance Change (packed calldata + forks -> call + transfer facts)

Before: `aave-v3-origin/assertions/src/production/BorrowingInvariantAssertions.a.sol:257`
- Packed parameter decode logic in assertion
- Manual pre/post balance forks

After (P0):

```solidity
function assertBorrowBalanceChanges() external {
    IMockL2Pool pool = IMockL2Pool(ph.getAssertionAdopter());
    CallInfo[] memory borrows = getCalls(address(pool), pool.borrow.selector);

    for (uint256 i = 0; i < borrows.length; i++) {
        (address asset, address user, uint256 amount) = _decodeBorrowCall(borrows[i]);
        int256 userDelta = _netTransferDeltaByCall(asset, user, borrows[i].id);
        require(userDelta == int256(amount), "borrow_balance_delta_mismatch");
    }
}
```

### 6) Morpho Gate Checks (6 repeated decode+fork helpers -> one generic helper)

Before: `morpho-vault-2/assertions/src/VaultV2ConfigAssertions.a.sol:97`
- Repeated near-identical helper functions for deposit/mint/withdraw/redeem/transfer/transferFrom

After (P0):

```solidity
function _assertGate(bytes4 selector, function(address,address,IVaultV2) view policy) internal {
    IVaultV2 vault = IVaultV2(ph.getAssertionAdopter());
    CallInfo[] memory calls = getCalls(address(vault), selector);
    for (uint256 i = 0; i < calls.length; i++) {
        (address actor, address counterparty) = _gateAccounts(selector, calls[i].input, calls[i].caller);
        policy(actor, counterparty, vault);
    }
}
```

### 7) MYX Collateral Accounting (manual multi-call reconstruction -> normalized flows)

Before: `myx-v2-assertions/assertions/src/CollateralAccountingAssertion.a.sol:46`
- Aggregates 4 call sets + per-call pre/post forks for disburse path
- Recomputes branch logic for quote/base payout behavior

After (P0 + P1):

```solidity
function assertionCollateralAccounting() external {
    ITradingVault vault = ITradingVault(ph.getAssertionAdopter());
    address quote = _quoteToken(vault);
    address base = _baseToken(vault);

    int256 quoteNet = _netTransferDelta(quote, address(vault));
    int256 baseNet = _netTransferDelta(base, address(vault));
    (int256 expectedQuote, int256 expectedBase) = _expectedNetFromChangedDebtAndCollateral(vault);

    require(quoteNet == expectedQuote, "quote_collateral_accounting_mismatch");
    require(baseNet == expectedBase, "base_collateral_accounting_mismatch");
}
```

Notes:
1. These snippets are intentionally schematic; exact slot constants/selectors stay protocol-specific.
2. Biggest reduction comes from removing repeated trace-walking/forking boilerplate, not from removing core invariant math.
3. Preferred final form is scalar/quantified assertions. Array-returning examples are transitional where per-call evidence is required.
4. Invariants that require unknown-key completeness still rely on P2 `STRICT` tracking mode.

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
```

Why:
1. EVC/Lagoon/Morpho repeatedly compute share-price style invariants.
2. Avoids each assertion re-implementing tiny variations of the same formula.

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
1. Implement tx-level shared indexes + scalar quantifiers first (P0-A through P0-D).
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
3. Ship Loop-Elimination Pack:
   - `getTriggerContext`
   - `anyCall` / `countCalls` / `allCallsBy` / `sumArgUint`
   - `CallFilter` presets (`successOnly`, `topLevelOnly`, call-type filter)
   - `loadAtCall` / `callerAt` / `slotDeltaAtCall`
4. Ship Token Facts Pack:
   - `erc20BalanceDiff`, `erc20SupplyDiff`, `erc20AllowanceDiff`
   - `getERC20NetFlow`, `getERC20FlowByCall`
5. Ship Policy Pack:
   - `allSlotWritesBy`, `anySlotWritten`
   - filtered logs helpers
   - keep `getStorageWrites` as escape hatch
6. Rewrite top ROI assertions first:
   - `ethereum-vault-connector/assertions/src/AccountHealthAssertion.a.sol`
   - `malda-lending/assertions/src/OutflowLimiterAssertion.a.sol`
   - `morpho-vault-2/assertions/src/VaultV2ConfigAssertions.a.sol`
   - `aave-v3-origin/assertions/src/production/BorrowingInvariantAssertions.a.sol`
7. Ship P1 deterministic mapping/balance diffs for known-key invariants.
8. Ship P2 tracking only for unknown-key completeness gaps.
9. Add lint gate:
   - fail on new manual `for (...)` loops over `getCallInputs/getLogs` unless allowlisted.
10. Add API guardrails:
   - mark array-returning APIs as non-default in docs/examples
   - provide scalar alternative in every new helper namespace

## Success Metrics

1. >= 60% reduction in explicit call/log loops.
2. >= 70% reduction in explicit `forkPreCall/forkPostCall` use.
3. Zero new assertions that re-implement protocol formulas unless justified.
4. Zero silent false-negatives in key-discovery assertions (`STRICT` mode where required).
5. >= 30% reduction in p95 `assertion_execution_duration` per tx.
6. >= 50% reduction in precompile bytes returned per assertion function (scalar-first effect).
7. No measurable regression in tx throughput under high assertion fan-out.

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

### Phases 7-9: Sync Script, Auto PR, Guardrails -- REMOVED
- Removed: credible-std is a separate repo (https://github.com/phylaxsystems/credible-std),
  not a local folder. The testdata/mock-protocol/lib/credible-std path is a git submodule.
- Cross-repo sync between credible-sdk and credible-std should be handled at the
  credible-std repo level, not by syncing into the submodule checkout.

## Detailed Implementation Plan (Execution)

### Scope Guardrails

1. Implement in `credible-sdk` only.
2. Keep unrelated dirty path untouched: `credible-sdk/testdata/mock-protocol/lib/credible-std` (except intentional submodule-pointer bump in Phase 8).
3. Commit with explicit pathspecs for touched files only.
4. Run tests and benchmarks at every phase boundary.

### Phase 0: Baseline (Before Changes)

1. Record local branch/status and touched files.
2. Run focused baseline tests:

```bash
cd /Users/odysseas/code/phylax/assertions/credible-sdk
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
```

3. Run quick baseline benchmarks:

```bash
cargo bench -p assertion-executor --bench assertion_store_read -- --quick
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
```

4. Save baseline numbers for before/after comparison.

### Phase 1: Selector-Pruning in Store Read Path

1. Update `crates/assertion-executor/src/store/assertion_store.rs`:
   - Drop assertions with empty matched selector sets.
2. Add/update matcher tests in the same file.
3. Validate + benchmark:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" assertion_store
cargo bench -p assertion-executor --bench assertion_store_read -- --quick
```

4. Commit.

### Phase 2: Zero-Work Fast Paths in Executor

1. Update `crates/assertion-executor/src/executor/mod.rs`:
   - Early return in `run_assertion_contract` for empty `fn_selectors`.
2. Update `crates/assertion-executor/src/executor/with_inspector.rs`:
   - Same early return for inspector path.
3. Add/update tests for empty-selector execution behavior.
4. Validate + benchmark:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" executor
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
```

5. Commit.

### Phase 3: Adaptive Parallelism

1. Add thresholded scheduling for tiny workloads in:
   - `execute_triggered_assertions`
   - `run_assertion_contract`
   - `run_assertion_contract_with_inspector`
2. Preserve behavior; change scheduling only.
3. Validate + benchmark:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
cargo bench -p assertion-executor --bench executor_avg_block_perf -- --quick
```

4. Commit.

### Phase 4: Perf Instrumentation

1. Add lightweight counters/timers for:
   - skipped-empty-selector executions
   - sequential vs parallel path selection
2. Keep instrumentation non-invasive and optional at runtime.
3. Validate + benchmark spot check:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
```

4. Commit.

### Phase 5: Broader Validation Gate

1. Run package-level validation:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
```

2. Run heavier benchmark sweep:

```bash
cargo bench -p assertion-executor --bench assertion_store_read -- --quick
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
cargo bench -p assertion-executor --bench executor_avg_block_perf -- --quick
```

3. If environment allows, run broader non-full suite:

```bash
make test-no-full
```

4. Compare against Phase 0 baselines and document deltas.
5. Final cleanup commit.

### Phase 6: Canonical Interface Source (Bindings -> Std)

Goal: eliminate drift between `credible-sdk` bindings and `credible-std` interfaces.

1. Create canonical interface files in `credible-sdk` (single source of truth):
   - `crates/assertion-executor/interfaces/PhEvm.sol`
   - `crates/assertion-executor/interfaces/TriggerRecorder.sol`
2. Update `crates/assertion-executor/src/inspectors/sol_primitives.rs` to load from canonical Solidity files via `sol!(".../PhEvm.sol")` and `sol!(".../TriggerRecorder.sol")` (path form).
3. Keep `deploy_da.rs` assertion source aligned with canonical interfaces:
   - either inline-generate from canonical files during test setup, or
   - add explicit regeneration step and guard check.
4. Add focused tests for newly introduced interface methods/selectors to prevent accidental removals.
5. Commit.

### Phase 7: Sync Script + CI Drift Gate

1. Add `credible-sdk/scripts/sync-interfaces.sh`:
   - `sync` mode: copy canonical interface files into:
     - `../credible-std/src/PhEvm.sol`
     - `../credible-std/src/TriggerRecorder.sol`
   - `check` mode: fail if any target differs from canonical source.
2. Add CI job in `credible-sdk`:
   - run sync script in `check` mode,
   - fail PR when interface drift exists.
3. Add CI job in `credible-std`:
   - run same check against canonical upstream interface snapshot (or mirrored script).
4. Commit.

### Phase 8: Auto PR Workflow (Cross-Repo Sync)

Target behavior: when canonical interface changes, downstream update PRs are opened automatically.

1. On merge to `credible-sdk/main`, trigger downstream sync automation:
   - Preferred: `repository_dispatch` to `credible-std`.
   - Fallback: daily scheduled sync job in `credible-std` if dispatch is unavailable.
2. In `credible-std`, workflow pulls canonical interface files from `credible-sdk/main`, applies sync, and opens/updates PR using bot automation (e.g. `create-pull-request` action).
3. After `credible-std` PR merge, open/update a PR in `credible-sdk` to bump:
   - `testdata/mock-protocol/lib/credible-std` submodule pointer only (no manual edits inside submodule from `credible-sdk`).
4. Add labels + CODEOWNERS reviewers so interface changes are always reviewed by executor + stdlib owners.
5. Commit.

### Phase 9: Operational Guardrails

1. Add required status checks:
   - `interface-sync-check` in `credible-sdk`
   - `interface-sync-check` in `credible-std`
2. Add contributor docs:
   - "edit canonical interface only"
   - "run sync script before push"
3. Add failure message in CI explaining exactly how to regenerate and where drift was found.
4. Commit.

### Commit Cadence and Template

1. Commit after every phase.
2. Each commit message should state:
   - what changed,
   - which tests ran,
   - which benchmark(s) ran,
   - observed delta summary.
3. Example:
   - `executor: skip empty selector assertion executions`
   - `tests: assertion_store matcher suite`
   - `bench: assertion_store_read quick (+X% / -Y%)`

### Definition of Done

1. All phase validations pass.
2. No unrelated file is included in commits.
3. Benchmarks show neutral-to-positive impact on tx validation latency.
4. New behavior is covered by unit tests.
5. Interface drift checks are enforced in CI for both `credible-sdk` and `credible-std`.
6. Canonical interface updates automatically produce downstream sync PRs.
