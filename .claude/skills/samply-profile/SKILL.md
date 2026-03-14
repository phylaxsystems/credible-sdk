---
name: samply-profile
description: Profile Rust CPU hotspots with samply, read flamegraphs, and validate before/after wins
---

# samply-profile

Use this when a Rust performance task needs a repeatable loop:

`profile -> inspect flamegraph/call tree -> optimize one hotspot -> validate -> re-profile`

## Goal

Measure real CPU hotspots first, then keep only changes that improve the same workload without breaking correctness.

## Build

Profile optimized code with symbols.

Repo-specific default:

```bash
cargo build -p benchmark-utils --profile debug-perf --bin benchmark-utils-perf
```

General fallback:

```bash
RUSTFLAGS="-C debuginfo=1" cargo build --release
```

macOS first attach run:

```bash
samply setup -y
```

## Workload Discipline

- Keep the workload fixed for the whole before/after comparison.
- Use representative inputs, not toy data.
- Use `samply` to find hotspots and Criterion to confirm repeatable deltas.
- Prefer one optimization per iteration.

In this repo, use the perf driver in `benchmark-utils-perf`:

- Presets:
  - `avg_block_0_aa`
  - `avg_block_100_aa`
  - `erc20_transaction_aa`
  - `uniswap_transaction_aa`
- Modes:
  - `vanilla`
  - `trace_only`
  - `store_read_only`
  - `assertion_setup_only`
  - `assertions_only`
  - `full`

## Record

Repo-local command:

```bash
make perf-profile PRESET=avg_block_100_aa MODE=full ITERS=1
```

On macOS, this should run `samply` via `sudo`.

Manual fallback:

```bash
mkdir -p artifacts/profiles
sudo env PATH="$PATH" samply record --save-only -o artifacts/profiles/<preset>-<mode>.json.gz -- ./target/debug-perf/benchmark-utils-perf --preset <preset> --mode <mode> --iters 1
```

If you want the capture to be more portable across machines, prefer:

```bash
sudo env PATH="$PATH" samply record --save-only --unstable-presymbolicate -o artifacts/profiles/<preset>-<mode>.json.gz -- ./target/debug-perf/benchmark-utils-perf --preset <preset> --mode <mode> --iters 1
```

Attach mode for a running process:

```bash
samply record -p <pid> -d 30 --save-only -o artifacts/profiles/profile.json.gz
```

## View

```bash
make perf-load FILE=artifacts/profiles/<preset>-<mode>.json.gz
```

Or directly:

```bash
samply load artifacts/profiles/<preset>-<mode>.json.gz
```

This opens Firefox Profiler, which is the flamegraph / call-tree UI for `samply`.

## Read the Profile

Use the flamegraph for prioritization and the call tree for confirmation.

Flamegraph rules:

- Width = total time spent in the function, including children.
- Y-axis = stack depth.
- Color has no semantic meaning.
- Wide blocks near the top are usually the best first targets.
- Plateaus often point to deep stacks that can be simplified.
- Repeated patterns usually indicate loops or recursion.

Call tree rules:

- High `Self` time means the function body itself is expensive.
- High `Total` time with low `Self` means callees or call frequency dominate.
- Prefer hotspots in project crates over runtime, libc, or profiler frames.

Common profile signatures:

- `alloc::`, `clone`, `Vec::push`, `memcpy`, `memmove`:
  reduce allocation growth, copying, and ownership churn.
- `Mutex`, `RwLock`, `parking_lot`, `pthread_mutex`:
  reduce lock scope, lock frequency, or contention.
- repeated stack shapes:
  collapse repeated work, cache results, or avoid recursion where practical.

## Loop

1. Pick one representative workload and keep it fixed.
2. Capture a baseline profile in `artifacts/profiles/`.
3. Open the profile and inspect flamegraph plus call tree.
4. Pick one hotspot with clear impact.
5. Implement one targeted change.
6. Run correctness checks.
7. Re-profile the exact same workload.
8. Compare runtime and hotspot share.
9. Record the result in `artifacts/profiles/summary.md`.
10. Keep or revert based on measured improvement.

## Validation

For this repo, validate both correctness and benchmark deltas:

```bash
cargo test -p assertion-executor --lib
cargo bench --manifest-path crates/assertion-executor/Cargo.toml --features test --benches
```

Use the targeted bench that matches the hotspot, plus at least one end-to-end workload.

## Guardrails

- Do not optimize without a fresh profile.
- Do not batch unrelated perf edits into one iteration.
- Do not trust the flamegraph alone; confirm with call tree and before/after timings.
- If the profile is ambiguous, narrow the workload or add a focused benchmark first.
- If asked only for profiling or analysis, report findings and ask before changing code.

## References

- Repo skill: `/Users/odysseas/.codex/skills/rust-samply-profiling-loop/SKILL.md`
- OneUptime guide: `https://oneuptime.com/blog/post/2026-01-07-rust-profiling-perf-flamegraph/view`
