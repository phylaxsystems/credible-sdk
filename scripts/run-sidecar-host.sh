#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/target/sidecar-host"
LOCAL_DB_DIR="${ROOT_DIR}/.local/sidecar-host"
BIN_PATH="${BIN_DIR}/sidecar"

mkdir -p "${BIN_DIR}"
mkdir -p "${LOCAL_DB_DIR}"

OS_NAME="$(uname -s)"

case "${OS_NAME}" in
  Linux)
    IMAGE_NAME="credible-sidecar-host-builder"
    docker build \
      --file "${ROOT_DIR}/dockerfile/Dockerfile.sidecar.host" \
      --tag "${IMAGE_NAME}" \
      "${ROOT_DIR}"

    CONTAINER_ID="$(docker create "${IMAGE_NAME}")"
    trap 'docker rm -f "${CONTAINER_ID}" >/dev/null 2>&1 || true' EXIT
    docker cp "${CONTAINER_ID}:/dist/sidecar" "${BIN_PATH}"
    docker rm "${CONTAINER_ID}" >/dev/null
    trap - EXIT
    ;;
  Darwin)
    cargo build --manifest-path "${ROOT_DIR}/Cargo.toml" --locked --release --bin sidecar
    cp "${ROOT_DIR}/target/release/sidecar" "${BIN_PATH}"
    ;;
  *)
    echo "Unsupported OS '${OS_NAME}'. Only macOS and Linux are supported." >&2
    exit 1
    ;;
esac

DEFAULT_ASSERTION_STORE_DB_PATH="${SIDECAR_ASSERTION_STORE_DB_PATH:-${LOCAL_DB_DIR}/assertion_store_database}"
DEFAULT_INDEXER_DB_PATH="${SIDECAR_INDEXER_DB_PATH:-${LOCAL_DB_DIR}/indexer_database}"

mkdir -p "$(dirname "${DEFAULT_ASSERTION_STORE_DB_PATH}")"
mkdir -p "$(dirname "${DEFAULT_INDEXER_DB_PATH}")"

export RUST_LOG="${RUST_LOG:-debug}"
export OTEL_LEVEL="${OTEL_LEVEL:-debug}"
export TRACING_LOG_JSON="${TRACING_LOG_JSON:-true}"
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-credible-sidecar}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4317}"
export OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-grpc}"
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="${OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:-grpc}"
export TRACING_METRICS_PORT="${TRACING_METRICS_PORT:-9000}"

DEFAULT_ARGS=(
  "--transport.protocol=${SIDECAR_TRANSPORT_PROTOCOL:-grpc}"
  "--transport.bind-addr=${SIDECAR_BIND_ADDR:-0.0.0.0:50051}"
  "--chain.spec-id=${SIDECAR_CHAIN_SPEC_ID:-Cancun}"
  "--chain.chain-id=${SIDECAR_CHAIN_ID:-1337}"
  "--credible.assertion-da-rpc-url=${SIDECAR_ASSERTION_DA_RPC_URL:-http://127.0.0.1:5001}"
  "--credible.assertion-gas-limit=${SIDECAR_ASSERTION_GAS_LIMIT:-3000000}"
  "--credible.block-tag=${SIDECAR_BLOCK_TAG:-latest}"
  "--credible.indexer-rpc-url=${SIDECAR_INDEXER_RPC_URL:-ws://127.0.0.1:8546}"
  "--credible.assertion-store-db-path=${DEFAULT_ASSERTION_STORE_DB_PATH}"
  "--credible.indexer-db-path=${DEFAULT_INDEXER_DB_PATH}"
  "--state.sequencer-url=${SIDECAR_SEQUENCER_URL:-http://127.0.0.1:8545}"
  "--state.besu-client-ws-url=${SIDECAR_BESU_CLIENT_WS_URL:-ws://127.0.0.1:8546}"
)

if [[ -n "${SIDECAR_EXTRA_ARGS:-}" ]]; then
  IFS=' ' read -r -a EXTRA_ARGS <<< "${SIDECAR_EXTRA_ARGS}"
else
  EXTRA_ARGS=()
fi

exec "${BIN_PATH}" "${DEFAULT_ARGS[@]}" "${EXTRA_ARGS[@]}" "$@"
