# benchmarks

this mod contains benchmarks for the sidecar. the worst case compute bench tests scenarios where the assertions are maximally cpu intensive.
the generic tests io. benches can be ran like this:
```
SIDECAR_BENCH_TRANSPORT=mock \
    RUST_LOG=error \
    cargo bench --features bench-utils --bench worst_case_compute
```
where `SIDECAR_BENCH_TRANSPORT` can be set to whatever transport you want to run benchmarks for.

## profiling with tokio-console

the benchmarks can be profiled via tokio console by running

```
SIDECAR_BENCH_TRANSPORT=grpc \
    RUSTFLAGS="--cfg tokio_unstable" \
    RUST_LOG=tokio::metrics=info,tokio::task=trace,tokio::runtime=trace,tokio::resource=trace \
    cargo bench --features bench-utils,tokio-console --bench worst_case_compute
```
