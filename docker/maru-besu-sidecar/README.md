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

- Start the compose stack with the `credible-sidecar` service scaled to zero.
- Build a native `sidecar` binary (using Docker on Linux and `cargo` directly on macOS).
- Place the runtime data (assertion/indexer databases) under `.local/sidecar-host/`, then launch with endpoints rewired to `localhost` so it can reach the in-cluster services. The compiled binary is emitted to `target/sidecar-host/sidecar`.

You can override endpoints or other CLI arguments by exporting any of the `SIDECAR_*` environment variables defined in `scripts/run-sidecar-host.sh` before running the command. Additional arguments passed to the script can be appended by exporting `SIDECAR_EXTRA_ARGS` or by invoking the script directly.
