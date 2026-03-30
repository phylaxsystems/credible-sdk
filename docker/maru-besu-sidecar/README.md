# Maru + Linea Besu + Credible Sidecar stack

This directory was generated via `linea-monorepo/docker/package-maru-besu-sidecar.sh`
to allow running the full stack from within the credible-sdk repository.

## Usage

1. Ensure Docker and Docker Compose are available.
2. From this directory run:

   ```bash
   docker compose up -d
   ```

   This starts the supported local topology: one `credible-sidecar` process with the state-worker
   embedded inside it. Do not launch a separate `state-worker` service alongside this stack.

3. Use the sidecar readiness endpoint to check whether the integrated worker is healthy or whether
   the sidecar is serving from fallback state sources:

   ```bash
   curl http://localhost:9547/ready
   ```

   `/health` remains a liveness endpoint. `/ready` is the operator-facing signal and can report a
   degraded-but-ready state when the embedded worker is unavailable but `eth-rpc` fallback still
   covers the required range.

4. Grafana is at http://localhost:3000/

## Running the sidecar on the host

To profile or debug the sidecar outside of Docker while keeping the rest of the stack in containers (requires a local Rust toolchain on macOS):

```bash
make run-sidecar-host
```

This target will:

- Start (or reuse) the compose stack with the `credible-sidecar` service scaled to zero.
- Launch the `sidecar` using `cargo run --release` on the host so you can rebuild and iterate quickly.
- Reuse the stack config at `docker/maru-besu-sidecar/config/credible-sidecar/grpc.config.json`.
- Override the integrated-worker MDBX path, genesis path, and RPC endpoints for host-local paths under `.local/sidecar-host/` so you still run a single sidecar runtime with an embedded worker.

You can override endpoints or other CLI arguments by exporting any of the `SIDECAR_*` environment variables defined in `scripts/run-sidecar-host.sh` before running the command. Additional arguments passed to the script can be appended by exporting `SIDECAR_EXTRA_ARGS` or by invoking the script directly.

Common examples:

```bash
SIDECAR_RUST_LOG=info make run-sidecar-host      # adjust log level
SIDECAR_BESU_CLIENT_WS_URL=ws://127.0.0.1:8546 make run-sidecar-host
```

If you already have the compose stack running, set `SIDECAR_SKIP_COMPOSE=true` to leave it untouched.

To tear the stack down and remove the associated volumes:

```bash
make down-sidecar-host
```

## Smoke Check

Before handing stack changes off, run:

```bash
./scripts/run-sidecar-host.sh --smoke-check
```

That validates the mounted sidecar config is well-formed JSON and that
`docker/maru-besu-sidecar/docker-compose.yml` renders successfully via `docker compose config`.
