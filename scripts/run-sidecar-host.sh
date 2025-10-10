#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/maru-besu-sidecar/docker-compose.yml"

if [[ "${SIDECAR_SKIP_COMPOSE:-false}" != "true" ]]; then
  docker compose -f "${COMPOSE_FILE}" up -d --scale credible-sidecar=0
fi

DEFAULT_ASSERTION_STORE_DB_PATH=".local/sidecar-host/assertion_store_database"
DEFAULT_INDEXER_DB_PATH=".local/sidecar-host/indexer_database"

ASSERTION_STORE_DB_PATH="${SIDECAR_ASSERTION_STORE_DB_PATH:-${CREDIBLE_ASSERTION_STORE_DB_PATH:-${DEFAULT_ASSERTION_STORE_DB_PATH}}}"
INDEXER_DB_PATH="${SIDECAR_INDEXER_DB_PATH:-${CREDIBLE_INDEXER_DB_PATH:-${DEFAULT_INDEXER_DB_PATH}}}"

ensure_parent_dir() {
  local raw_path="$1"
  local parent_dir
  parent_dir="$(dirname "${raw_path}")"

  if [[ "${raw_path}" == /* ]]; then
    mkdir -p "${parent_dir}"
  else
    mkdir -p "${ROOT_DIR}/${parent_dir}"
  fi
}

ensure_parent_dir "${ASSERTION_STORE_DB_PATH}"
ensure_parent_dir "${INDEXER_DB_PATH}"

export CREDIBLE_ASSERTION_STORE_DB_PATH="${ASSERTION_STORE_DB_PATH}"
export CREDIBLE_INDEXER_DB_PATH="${INDEXER_DB_PATH}"

maybe_export() {
  local source="$1"
  local target="$2"
  local value="${!source:-}"

  if [[ -n "${value}" ]]; then
    export "${target}=${value}"
  fi
}

if [[ -n "${SIDECAR_RUST_LOG:-}" ]]; then
  export RUST_LOG="${SIDECAR_RUST_LOG}"
fi
if [[ -n "${SIDECAR_OTEL_LEVEL:-}" ]]; then
  export OTEL_LEVEL="${SIDECAR_OTEL_LEVEL}"
fi
if [[ -n "${SIDECAR_TRACING_LOG_JSON:-}" ]]; then
  export TRACING_LOG_JSON="${SIDECAR_TRACING_LOG_JSON}"
fi
if [[ -n "${SIDECAR_OTEL_SERVICE_NAME:-}" ]]; then
  export OTEL_SERVICE_NAME="${SIDECAR_OTEL_SERVICE_NAME}"
fi
if [[ -n "${SIDECAR_OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]]; then
  export OTEL_EXPORTER_OTLP_ENDPOINT="${SIDECAR_OTEL_EXPORTER_OTLP_ENDPOINT}"
fi
if [[ -n "${SIDECAR_OTEL_EXPORTER_OTLP_PROTOCOL:-}" ]]; then
  export OTEL_EXPORTER_OTLP_PROTOCOL="${SIDECAR_OTEL_EXPORTER_OTLP_PROTOCOL}"
fi
if [[ -n "${SIDECAR_OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:-}" ]]; then
  export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="${SIDECAR_OTEL_EXPORTER_OTLP_TRACES_PROTOCOL}"
fi
if [[ -n "${SIDECAR_TRACING_METRICS_PORT:-}" ]]; then
  export TRACING_METRICS_PORT="${SIDECAR_TRACING_METRICS_PORT}"
fi

export RUST_LOG="${RUST_LOG:-debug}"
export OTEL_LEVEL="${OTEL_LEVEL:-debug}"
export TRACING_LOG_JSON="${TRACING_LOG_JSON:-true}"
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-credible-sidecar}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4318}"
export OTEL_EXPORTER_OTLP_PROTOCOL="${OTEL_EXPORTER_OTLP_PROTOCOL:-http/protobuf}"
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL="${OTEL_EXPORTER_OTLP_TRACES_PROTOCOL:-http/protobuf}"
export TRACING_METRICS_PORT="${TRACING_METRICS_PORT:-9000}"

maybe_export SIDECAR_TRANSPORT_PROTOCOL TRANSPORT_PROTOCOL
maybe_export SIDECAR_BIND_ADDR TRANSPORT_BIND_ADDR
maybe_export SIDECAR_CHAIN_SPEC_ID CHAIN_SPEC_ID
maybe_export SIDECAR_CHAIN_ID CHAIN_CHAIN_ID
maybe_export SIDECAR_ASSERTION_DA_RPC_URL CREDIBLE_ASSERTION_DA_RPC_URL
maybe_export SIDECAR_ASSERTION_GAS_LIMIT CREDIBLE_ASSERTION_GAS_LIMIT
maybe_export SIDECAR_BLOCK_TAG CREDIBLE_BLOCK_TAG
maybe_export SIDECAR_INDEXER_RPC_URL CREDIBLE_INDEXER_RPC_URL
maybe_export SIDECAR_SEQUENCER_URL STATE_SEQUENCER_URL
maybe_export SIDECAR_BESU_CLIENT_WS_URL STATE_BESU_CLIENT_WS_URL

if [[ -n "${SIDECAR_EXTRA_ARGS:-}" ]]; then
  IFS=' ' read -r -a EXTRA_ARGS <<< "${SIDECAR_EXTRA_ARGS}"
else
  EXTRA_ARGS=()
fi

CARGO_RUN_ARGS=(
  cargo
  run
  --manifest-path
  "${ROOT_DIR}/Cargo.toml"
  --locked
  --release
  --bin
  sidecar
)

cd "${ROOT_DIR}"

if (( ${#EXTRA_ARGS[@]} > 0 || $# > 0 )); then
  CARGO_RUN_ARGS+=(--)
fi

exec "${CARGO_RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" "$@"
