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
| `morpho-vault-2` | `assertions/src/VaultV2ConfigAssertions.a.sol:97` (gate helper cluster) | ~61 | ~22 | ~64% |
| `myx-v2-assertions` | `assertions/src/CollateralAccountingAssertion.a.sol:46` | ~133 | ~34 | ~74% |

### 1) EVC Account Health (nested call decoding -> event-derived account set)

Before: `ethereum-vault-connector/assertions/src/AccountHealthAssertion.a.sol:123`
- Manual unwrap of nested `batch/call`
- Selector-specific account extraction
- Multiple unique-set dedupe loops

After (P0 + P1):

```solidity
function assertionBatchAccountHealth() external {
    IEVC evc = IEVC(ph.getAssertionAdopter());

    address[] memory vaults = _uniqueVerifiedVaultTargetsFromCallWithContextLogs(address(evc));
    address[] memory accounts = _uniqueAccountsFromCallWithContextLogs(address(evc));

    for (uint256 i = 0; i < accounts.length; i++) {
        address account = accounts[i];
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
4. Key coverage in this roadmap uses known-key and event/call-derived key sets.

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
- Still TODO from plan: keyed aggregates (`sumCallArgUintForAddress`, etc.), ERC4626 helpers, trigger-time filtered registration

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
- Canonical PhEvm.sol interface (34 methods) is ready for sync.
- Deferred until Phase 10 cross-repo sync is in place.

### Phase 10: Cross-Repo Interface Sync Automation -- DEFERRED
- `credible-std` is a separate repo; automation should run cross-repo via CI workflows and PR bots.
- In `credible-sdk`, only submodule pointer bumps should be done after upstream `credible-std` updates.

### Phase 11: Runtime Performance Hardening Gate -- IN PROGRESS
- **StorageChangeIndex**: DONE (`8ac766e`). Lazily-built index on `CallTracer` via `OnceLock` pre-indexes all `StorageChanged` journal entries by `(address, slot)`. Replaces O(journal) scans with O(1) lookups in `slot_diffs`, `state_changes`, and `write_policy` precompiles. 266 tests pass.
- Remaining items (not yet started):
  - Items 1-2 (empty-selector short-circuits): Already exist in executor path (verified by code review).
  - Item 3 (adaptive parallelism): Already implemented in Phases 3-4.
  - Item 6 (log re-encoding caching): Not started.
  - Item 7 (bounded array responses): Not started.
  - Item 8 (trigger-time call-shape filtering): Not started.
  - Item 9 (keyed/grouped aggregate indexes): Not started.
  - Focused perf benchmarks: Blocked on benchmarking infrastructure.

## Detailed Implementation Plan (Execution)

### Scope Guardrails

1. Implement in `credible-sdk` only.
2. Keep unrelated dirty path untouched: `credible-sdk/testdata/mock-protocol/lib/credible-std` (except intentional submodule-pointer bump in Phase 10).
3. Commit with explicit pathspecs for touched files only.
4. Run tests and benchmarks at every phase boundary.

### Phase Map (What Ships When)

1. Phases 0-5: executor performance foundations (already completed).
2. Phase 6: canonical interface source in `credible-sdk` (completed).
3. Phase 7: P0 declarative cheatcodes (this is where core new cheatcodes are implemented).
4. Phase 8: P1 mapping and diff cheatcodes.
5. Phase 9: rewrite assertions + add stdlib helper wrappers and authoring guardrails.
6. Phase 10: cross-repo interface sync CI/auto-PR automation (deferred/outside this repo).
7. Phase 11: runtime performance hardening gate for declarative cheatcodes.

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

### Phase 7: Declarative Cheatcodes P0 (Core)

1. Implement tx-level shared indexes reused across all assertion functions in the same tx:
   - call index, write index, log index, normalized transfer index, changed-slot index.
2. Add `PhEvm` P0 scalar APIs:
   - call facts: `anyCall`, `countCalls`, `allCallsBy`, `sumArgUint`, `getTriggerContext`
   - keyed aggregates: `sumCallArgUintForAddress`, `uniqueCallArgAddresses`, `sumCallArgUintByAddress`,
     `sumEventUintForTopicKey`, `uniqueEventTopicValues`, `sumEventUintByTopic`
   - call-boundary facts: `loadAtCall`, `callerAt`, `slotDeltaAtCall`
   - write/token facts: `anySlotWritten`, `allSlotWritesBy`, `erc20BalanceDiff`, `erc20SupplyDiff`,
     `getERC20NetFlow`, `getERC20FlowByCall`, `erc4626TotalAssetsDiff`, `erc4626TotalSupplyDiff`,
     `erc4626VaultAssetBalanceDiff`, `erc4626AssetsPerShareDiffBps`
3. Add `ITriggerRecorder` filtered trigger registration APIs and `Assertion.sol` helper wrappers.
4. Keep array-returning APIs as non-default escape hatches.
5. Validate + benchmark:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
cargo bench -p assertion-executor --bench executor_avg_block_perf -- --quick
```

6. Commit.

### Phase 8: Declarative Cheatcodes P1 (Known-Key First)

1. Implement known-key deterministic diff APIs:
   - `getChangedSlots`, `getSlotDiff`
   - `didMappingKeyChange`, `mappingValueDiff`
   - `didBalanceChange`, `balanceDiff`
2. Validate + benchmark:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
cargo bench -p assertion-executor --bench executor_transaction_perf -- --quick
```

3. Commit.

### Phase 9: Assertion Rewrite + Authoring Guardrails

1. Add `credible-std` helper wrappers/presets for declarative usage:
   - filter presets and policy predicates (`successOnly`, `topLevelOnly`, `allCallsDeltaGE`, etc.).
2. Rewrite highest-ROI assertions first:
   - `ethereum-vault-connector/assertions/src/AccountHealthAssertion.a.sol`
   - `malda-lending/assertions/src/OutflowLimiterAssertion.a.sol`
   - `morpho-vault-2/assertions/src/VaultV2ConfigAssertions.a.sol`
   - `aave-v3-origin/assertions/src/production/BorrowingInvariantAssertions.a.sol`
3. Add lint/docs guardrails to discourage manual loops over `getCallInputs/getLogs` without allowlist justification.
4. Validate:

```bash
cargo test -p assertion-executor --no-default-features --features "optimism test" --lib
```

5. Commit.

### Phase 10: Cross-Repo Interface Sync CI/Auto-PR (Deferred)

1. In `credible-std` repo, add workflow to sync canonical interface files from `credible-sdk` and open PRs.
2. In `credible-sdk`, only bump `testdata/mock-protocol/lib/credible-std` submodule pointer after upstream merge.
3. Add required CI checks in each repo for drift prevention.

### Phase 11: Runtime Performance Hardening Gate (Final)

Goal: lock in latency wins from declarative cheatcodes under parallel execution.

1. Build shared tx facts once, reuse across all assertion functions in the tx:
   - `Arc<TxFacts>` on `PhEvmContext`.
   - Fact indexes: first call by target, call range boundaries, write index, state-change index, log index, normalized transfer index.
2. Replace O(journal * calls) write attribution with one-pass call-window attribution:
   - derive `journal_idx -> innermost_call_id` via call start/end boundaries.
   - use that for `allSlotWritesBy` and any caller-attributed write checks.
3. Remove per-query full scans from hot precompiles:
   - `anySlotWritten`, `allSlotWritesBy`, `getStateChanges` use indexed lookups.
   - `anyCall`/`countCalls`/`allCallsBy`/`sumArgUint` use iterator/short-circuit paths (no intermediate `Vec` for scalar checks).
4. Reduce per-function clone amplification in executor path:
   - make `MultiForkDb` carry immutable post-tx journal as shared `Arc` and lazily clone only when creating forks.
   - avoid per-contract linear `call_records()` scan for trigger context by using a precomputed target->first-call map (or exact matched trigger call id metadata).
5. Cache expensive encodings on tx scope:
   - cache `getLogs` ABI encoding once per tx.
   - optionally cache filtered array encodings for escape-hatch APIs when identical queries repeat.
6. Add focused perf suites (when benchmarks are available again):
   - `precompile_call_facts_hot`
   - `precompile_write_policy_hot`
   - `precompile_state_changes_hot`
   - `tx_facts_build`
   - `executor_clone_overhead`
7. Validation gate:
   - no remaining O(journal * calls) paths.
   - no per-assertion full-journal scan in hot scalar precompiles.
   - trigger context lookup is O(1) per assertion contract.
   - benchmark deltas documented and neutral-to-positive before merge.

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
5. Declarative cheatcodes from P0 and P1 are implemented and used in rewritten top-ROI assertions.
6. Array-loop-based assertion patterns are no longer the default authoring path.
7. Interface drift checks and auto-PR sync are defined for cross-repo rollout (Phase 10).
8. Runtime hardening gate (Phase 11) passes with indexed precompile paths and measured latency improvements.
