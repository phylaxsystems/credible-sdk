# Maru + Linea Besu + Credible Sidecar stack

This directory was generated via `linea-monorepo/docker/package-maru-besu-sidecar.sh`
to allow running the full stack from within the credible-sdk repository.

## Usage

1. Ensure Docker and Docker Compose are available.
2. From this directory run:

   ```bash
   docker compose up -d
   ```

3. Grafana is at http://localhost:3000/

## Running the sidecar on the host

To profile or debug the sidecar outside of Docker while keeping the rest of the stack in containers (requires a local Rust toolchain on macOS):

```bash
make run-sidecar-host
```

This target will:

- Start (or reuse) the compose stack with the `credible-sidecar` service scaled to zero.
- Launch the `sidecar` using `cargo run --release` on the host so you can rebuild and iterate quickly.
- Place the runtime data (assertion/indexer databases) under `.local/sidecar-host/`, then launch with endpoints rewired to `localhost` so it can reach the in-cluster services.

You can override endpoints or other CLI arguments by exporting any of the `SIDECAR_*` environment variables defined in `scripts/run-sidecar-host.sh` before running the command. Additional arguments passed to the script can be appended by exporting `SIDECAR_EXTRA_ARGS` or by invoking the script directly.

Common examples:

```bash
SIDECAR_RUST_LOG=info make run-sidecar-host      # adjust log level
SIDECAR_TRANSPORT_PROTOCOL=http make run-sidecar-host
```

If you already have the compose stack running, set `SIDECAR_SKIP_COMPOSE=true` to leave it untouched.

To tear the stack down and remove the associated volumes:

```bash
make down-sidecar-host
```
