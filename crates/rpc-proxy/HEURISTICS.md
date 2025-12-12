# RPC Proxy Heuristics

This note documents the heuristics that the Credible RPC proxy will enforce before a transaction ever reaches the sequencer. It is informed by the public docs at https://docs.phylax.systems (esp. the *Credible Layer → Assertions* section and the *Triggers* page, which describe how assertions register call/storage/balance triggers before the sidecar executes them) and by `crates/assertion-executor/ARCHITECTURE.md` in this repo. Those sources confirm that an assertion invalidation is deterministic per `(assertion_id, version, trigger, calldata/value)` while the driver only learns about it after the sidecar simulates the tx, which creates the spam vector we are closing here.

## Goals and threat model
- Attackers can mint EOAs/contracts that share calldata/value and bid extreme gas prices so their invalidating transactions forever sit at the top of the sequencer’s mempool; every iteration the sidecar/driver wastes cycles rejecting them.
- An attacker can vary sender, nonce, value and even tip caps to bypass naive duplicate checks, yet the failure is rooted in the same assertion trigger discovered via the `triggers()` metadata.
- The proxy must be driver-agnostic: it simply fronts JSON-RPC (e.g. `eth_sendRawTransaction`) and relays everything else untouched. Signals flow over a lightweight gRPC stream from the sidecar, so any rollup can consume the proxy without forking its sequencer.

## Deployment assumptions
- **First hop inside the sequencer perimeter**: upstream gateways (Infura, Alchemy, custom load balancers) terminate outside our control. The proxy runs on the same LAN or host as the sequencer so every transaction passes through it exactly once before touching the local mempool.
- **Opaque upstream identity**: per-client context from upstream providers is unreliable, so heuristics key on the normalized payload first and only use immediate peer metadata (remote IP/TLS cert) for secondary backpressure.
- **Protocol transparency**: the proxy must behave like a regular Ethereum JSON-RPC endpoint for non-mutating calls—status codes, errors, and retries all match public infrastructure so wallets and upstream relays don’t need custom logic.
- **Fail-open policy with guardrails**: if the gRPC link to the sidecar is down, the proxy still forwards traffic but keeps per-origin rate limits active so it doesn’t become an unthrottled spam relay.

## Architecture highlights
1. **Bidirectional signals**: the proxy subscribes to the sidecar’s invalidation feed (execution fingerprints and assertion metadata) and can also ask the sidecar for a "pre-validation" verdict if a submission is not yet known; the sidecar only needs to answer "already invalid" vs "unknown" to keep latency low.
2. **Normalized fingerprint cache**: we normalise every submission before forwarding—strip signature, bucket gas values, canonicalise calldata selectors as described in the Phylax Triggers doc—so equivalent payloads converge.
3. **Fast-path RPC handling**: everything except the mutable send endpoints is just proxied through. For the protected endpoints we enforce heuristics, emit metrics, and only forward to the sequencer if all checks pass.

## Heuristics to implement

The overarching design principle is *zero collateral damage*. Every control below is scoped so that a single malicious fingerprint can be throttled indefinitely while honest traffic keeps flowing through the proxy without added latency. In particular:

- Caches live entirely in the proxy and do not sit in the driver’s critical path.
- Rejections are fingerprint-specific; different calldata/value combinations continue unthrottled even when another fingerprint is being spammed.
- Rate limits are per-origin (IP/API token) and decay automatically so legitimate users recover as soon as they stop tripping the thresholds.

### 1. Execution fingerprint bans (TTL cache)
- Schema: `(assertion_id, assertion_version, adopter, to_address, calldata_root, value_bucket, block_anchor)`. Trigger info from the sidecar is recorded for observability, but matching is purely on the normalized transaction form since the proxy cannot infer trigger kinds ahead of time.
- When the sidecar reports a `status=invalidated` for a fingerprint, the proxy stores it with a short TTL (e.g. 64 slots) plus a generation counter. Any new submission whose normalized fingerprint matches is rejected locally with a JSON-RPC error containing the assertion hash so wallets know which rule fired.
- Eviction: flush entries when (a) a newer assertion version is indexed, (b) a `CommitHead` indication tells us the L2 block anchor moved, or (c) TTL expires. This mirrors the cache invalidation guidance in `assertion-executor` (new triggers after `triggers()` reruns).

### 2. Assertion-level cooldowns
- **Fingerprint-first**: throttling starts at the fingerprint level and never touches unrelated traffic. Only once we observe many *distinct* fingerprints failing under the same assertion (indicating a genuine incident or intentionally broad triggers) do we activate a per-assertion cooldown, and even then we continue to forward a trickle (e.g. 1 per block) so fixes self-heal the cache.
- Operators can opt individual assertions out of cooldowns or raise thresholds if their UX expects frequent, benign reverts. The proxy records the assertion metadata for transparency but does not block a whole assertion unless data proves it is universally failing.

### 3. Sender/IP backpressure
- Per the docs, assertions do not care which EOA called the target, so we combine per-IP, per-auth token, and per-address counters. After `N` invalidations or `M` RPC submissions/minute, the proxy exponentially backs off further sends (HTTP 429) even if fingerprints differ. Honest users rarely trip this because valid assertions will never revert repeatedly.
- Keep counters in a time-decaying structure (token bucket) so legitimate traffic recovers automatically.

### 4. Gas-price normalization & queue shaping
- Before forwarding to the sequencer’s mempool, we compute a "priority score" that subtracts a large penalty for fingerprints present in the ban/cooldown maps, regardless of stated `maxFeePerGas`. That ensures the driver does not keep selecting spam solely due to high tips.
- Limit queued submissions per fingerprint to one outstanding RPC call; subsequent matches wait until the first either lands (and succeeds) or is rejected.

### 5. Pre-forward sidecar probe
- Optional fast-path: for a submission that is not in our caches, send a `ShouldForward(fingerprint)` gRPC query. The sidecar answers `ALLOW` if it has never seen the payload or if an assertion fix landed, `DENY` if it already executed the same combination recently. This allows us to stop spam even between invalidation broadcasts.
- The query is read-only (no execution), so it does not add measurable CPU load to the sidecar; it simply checks the in-memory execution history that already exists for metrics (`sidecar_transactions_simulated_failure`).

### 6. Transport fairness & observability
- Cap the number of concurrent `eth_sendRawTransaction` calls per connection and drop payloads when latency exceeds an SLA; this prevents resource starvation during floods.
- Emit Prometheus metrics for every heuristic (e.g. `rpc_proxy_fingerprint_reject_total`, `rpc_proxy_assertion_cooldown_active`, `rpc_proxy_sender_backoff_total`). These mirror the counters already exposed by the sidecar so operators can correlate events.

## Operational considerations
- **Consistency with assertion data**: because triggers derive from on-chain `triggers()` outputs, the proxy must also watch the DA/indexer tip so it can reset caches when the assertion store pulls a new version. We can reuse the same SQLite/sled DB the sidecar already persists.
- **Failure modes**: if the gRPC channel drops, the proxy should fail-open but emit alerts—the sequencer immediately notices because invalidations stop arriving. Once the channel recovers we replay outstanding fingerprints before accepting new sends.
- **Rollout**: start in monitor-only mode (log rejections but still forward) to calibrate thresholds. Once the hit-rate proves accurate, flip to enforcement per endpoint.

These heuristics strike a balance between deterministic enforcement (fingerprint bans, assertion cooldowns) and adaptive safeguards (sender backoff, transport caps). Together, they give us the "revert protection" layer the docs describe without binding the solution to any specific driver implementation.

## Fingerprint normalization

The proxy never stores raw calldata; it derives a compact, reproducible fingerprint from transaction fields the RPC layer can observe:

1. **Target**: `to` address lower-cased; transactions without a `to` (EOA deployments) bypass heuristics entirely because assertions monitor deployed contracts.
2. **Selector**: first four bytes of calldata (or `0x00000000` for empty calldata).
3. **Argument hash**: keccak of calldata bytes after the selector, truncated to 16 bytes to keep keys small. This makes appending junk arguments ineffective.
4. **Value bucket**: integer bucket of `msg.value` using powers of two (0, (0,1e12], (1e12,1e15], …) so tiny perturbations collide. Protocols that rely on precise value matching can opt into a finer bucket.
5. **Gas bucket (optional)**: floor(`gas_limit / 50k`) so wildly different gas limits form distinct fingerprints while small tweaks collide.
6. **Assertion metadata**: `assertion_id` + `assertion_version` from the sidecar’s invalidation stream is appended when we learn which assertion fired; before that, the fingerprint is just the tuple above.

The canonical fingerprint string is:

```
fingerprint = keccak256(
    target || selector || arg_hash16 || value_bucket_u64 || gas_bucket_u32
)
```

This hash is what we key the TTL cache on; we also store the individual fields for observability/metrics. Any RPC submission that normalizes to the same tuple will be throttled even if the attacker changes gas price, nonce, or sender.

## gRPC messages

We extend the sidecar transport with two lightweight services:

```proto
service RpcProxyHeuristics {
  rpc StreamInvalidations(google.protobuf.Empty) returns (stream Invalidation);
  rpc ShouldForward(ShouldForwardRequest) returns (ShouldForwardResponse);
}

message Fingerprint {
  bytes hash = 1;                // keccak256 as defined above
  bytes target = 2;              // 20-byte address
  bytes selector = 3;            // 4-byte function selector
  bytes arg_hash16 = 4;          // 16-byte truncated keccak
  uint64 value_bucket = 5;
  uint32 gas_bucket = 6;
}

message Invalidation {
  Fingerprint fingerprint = 1;
  bytes assertion_id = 2;        // 32-byte hash
  uint64 assertion_version = 3;
  string adopter = 4;
  string trigger_description = 5; // optional human-readable context
  uint64 l2_block_number = 6;
  google.protobuf.Timestamp observed_at = 7;
}

message ShouldForwardRequest {
  Fingerprint fingerprint = 1;
}

message ShouldForwardResponse {
  enum Verdict {
    UNKNOWN = 0; // proxy should forward
    DENY = 1;    // identical tx failed recently
    ALLOW = 2;   // explicitly allowed despite historical failures (e.g. cache reset)
  }
  Verdict verdict = 1;
  bytes assertion_id = 2;        // echoed when DENY
  uint64 assertion_version = 3;
}
```

- `StreamInvalidations` is a long-lived subscription the proxy opens at startup. Every time the sidecar rejects a transaction, it emits an `Invalidation` with the normalized fingerprint and assertion metadata. The proxy updates its caches immediately—well before the driver can consider the same payload again.
- `ShouldForward` is an on-demand check when the proxy sees a fingerprint it has no opinion on. The sidecar only inspects its in-memory execution history (no re-simulation) and responds with `DENY` if that fingerprint already failed recently, otherwise `UNKNOWN/ALLOW`.

Versioning: if the proto evolves, both sides must include a `schema_version` field in the messages so we can phase-roll upgrades without downtime. Each response also carries the assertion version so the proxy can expire bans automatically when the assertion store syncs a new deployment.
