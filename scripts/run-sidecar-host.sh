#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/maru-besu-sidecar/docker-compose.yml"
STACK_CONFIG_FILE="${ROOT_DIR}/docker/maru-besu-sidecar/config/credible-sidecar/grpc.config.json"

SHOULD_CLEANUP=false

run_smoke_check() {
  if command -v jq >/dev/null 2>&1; then
    jq empty "${STACK_CONFIG_FILE}" >/dev/null
  elif command -v python3 >/dev/null 2>&1; then
    python3 -m json.tool "${STACK_CONFIG_FILE}" >/dev/null
  else
    printf '%s\n' "error: smoke check requires either jq or python3 to validate JSON" >&2
    return 1
  fi

  docker compose -f "${COMPOSE_FILE}" config >/dev/null
  printf '%s\n' "Smoke check passed: sidecar config is valid JSON and docker compose renders cleanly."
}

if [[ "${1:-}" == "--smoke-check" ]]; then
  shift
  run_smoke_check "$@"
  exit 0
fi

if [[ "${SIDECAR_SKIP_COMPOSE:-false}" != "true" ]]; then
  docker compose -f "${COMPOSE_FILE}" up -d --scale credible-sidecar=0
fi

DEFAULT_ASSERTION_STORE_DB_PATH=".local/sidecar-host/assertion_store_database"
DEFAULT_TRANSACTION_OBSERVER_DB_PATH=".local/sidecar-host/transaction_observer_database"
DEFAULT_STATE_WORKER_MDBX_PATH=".local/sidecar-host/state_worker.mdbx"

ASSERTION_STORE_DB_PATH="${SIDECAR_ASSERTION_STORE_DB_PATH:-${CREDIBLE_ASSERTION_STORE_DB_PATH:-${DEFAULT_ASSERTION_STORE_DB_PATH}}}"
TRANSACTION_OBSERVER_DB_PATH="${SIDECAR_TRANSACTION_OBSERVER_DB_PATH:-${DEFAULT_TRANSACTION_OBSERVER_DB_PATH}}"
STATE_WORKER_MDBX_PATH="${SIDECAR_STATE_WORKER_MDBX_PATH:-${DEFAULT_STATE_WORKER_MDBX_PATH}}"

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
ensure_parent_dir "${TRANSACTION_OBSERVER_DB_PATH}"
ensure_parent_dir "${STATE_WORKER_MDBX_PATH}"

resolve_path() {
  local raw_path="$1"
  local parent
  local name

  if [[ "${raw_path}" == /* ]]; then
    parent="$(dirname "${raw_path}")"
    name="$(basename "${raw_path}")"
    (
      cd "${parent}"
      printf "%s/%s\n" "$(pwd -P)" "${name}"
    )
  else
    parent="$(dirname "${raw_path}")"
    name="$(basename "${raw_path}")"
    (
      cd "${ROOT_DIR}/${parent}"
      printf "%s/%s\n" "$(pwd -P)" "${name}"
    )
  fi
}

DEFAULT_ASSERTION_STORE_DB_ABS="$(resolve_path "${DEFAULT_ASSERTION_STORE_DB_PATH}")"
DEFAULT_TRANSACTION_OBSERVER_DB_ABS="$(resolve_path "${DEFAULT_TRANSACTION_OBSERVER_DB_PATH}")"
DEFAULT_STATE_WORKER_MDBX_ABS="$(resolve_path "${DEFAULT_STATE_WORKER_MDBX_PATH}")"
ASSERTION_STORE_DB_ABS="$(resolve_path "${ASSERTION_STORE_DB_PATH}")"
TRANSACTION_OBSERVER_DB_ABS="$(resolve_path "${TRANSACTION_OBSERVER_DB_PATH}")"
STATE_WORKER_MDBX_ABS="$(resolve_path "${STATE_WORKER_MDBX_PATH}")"

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

if [[ -n "${SIDECAR_BIND_ADDR:-}" && -z "${SIDECAR_TRANSPORT_BIND_ADDR:-}" ]]; then
  export SIDECAR_TRANSPORT_BIND_ADDR="${SIDECAR_BIND_ADDR}"
fi

BESU_WS_URL="${SIDECAR_BESU_CLIENT_WS_URL:-${SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL:-ws://127.0.0.1:8546}}"
BESU_HTTP_URL="${SIDECAR_BESU_CLIENT_HTTP_URL:-${SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL:-http://127.0.0.1:8545}}"
STATE_WORKER_DEPTH_VALUE="${SIDECAR_STATE_WORKER_DEPTH:-3}"

export SIDECAR_ASSERTION_STORE_DB_PATH="${SIDECAR_ASSERTION_STORE_DB_PATH:-${ASSERTION_STORE_DB_ABS}}"
export CREDIBLE_ASSERTION_STORE_DB_PATH="${SIDECAR_ASSERTION_STORE_DB_PATH}"
export SIDECAR_TRANSACTION_OBSERVER_DB_PATH="${SIDECAR_TRANSACTION_OBSERVER_DB_PATH:-${TRANSACTION_OBSERVER_DB_ABS}}"
export SIDECAR_ASSERTION_DA_RPC_URL="${SIDECAR_ASSERTION_DA_RPC_URL:-http://127.0.0.1:5001}"
export SIDECAR_EVENT_SOURCE_URL="${SIDECAR_EVENT_SOURCE_URL:-http://127.0.0.1:8080/graphql}"
export SIDECAR_AEGES_URL="${SIDECAR_AEGES_URL:-http://127.0.0.1:8080}"

export SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL="${SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL:-${BESU_WS_URL}}"
export SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL="${SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL:-${BESU_HTTP_URL}}"
export SIDECAR_STATE_WORKER_MDBX_PATH="${SIDECAR_STATE_WORKER_MDBX_PATH:-${STATE_WORKER_MDBX_ABS}}"
export SIDECAR_STATE_WORKER_DEPTH="${SIDECAR_STATE_WORKER_DEPTH:-${STATE_WORKER_DEPTH_VALUE}}"
export SIDECAR_STATE_SOURCES="${SIDECAR_STATE_SOURCES:-$(printf '[{"type":"mdbx","mdbx_path":"%s","depth":%s},{"type":"eth-rpc","ws_url":"%s","http_url":"%s"}]' "${SIDECAR_STATE_WORKER_MDBX_PATH}" "${SIDECAR_STATE_WORKER_DEPTH}" "${SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL}" "${SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL}")}"

if [[ -n "${SIDECAR_EXTRA_ARGS:-}" ]]; then
  IFS=' ' read -r -a EXTRA_ARGS <<< "${SIDECAR_EXTRA_ARGS}"
else
  EXTRA_ARGS=()
fi
POSITIONAL_ARGS=("$@")

CHAIN_ID_VALUE="${SIDECAR_CHAIN_ID:-${CHAIN_CHAIN_ID:-1337}}"
chain_id_cli_present=false

SPEC_ID_VALUE="${SIDECAR_CHAIN_SPEC_ID:-${CHAIN_SPEC_ID:-Cancun}}"
spec_id_cli_present=false
config_file_cli_present=false

check_chain_id_args() {
  local awaiting_value="false"
  for arg in "$@"; do
    if [[ "${awaiting_value}" == "true" ]]; then
      chain_id_cli_present=true
      awaiting_value="false"
      continue
    fi

    case "${arg}" in
      --chain.chain-id)
        awaiting_value="true"
        ;;
      --chain.chain-id=*)
        chain_id_cli_present=true
        ;;
    esac
  done
}

check_spec_id_args() {
  local awaiting_value="false"
  for arg in "$@"; do
    if [[ "${awaiting_value}" == "true" ]]; then
      spec_id_cli_present=true
      awaiting_value="false"
      continue
    fi

    case "${arg}" in
      --chain.spec-id)
        awaiting_value="true"
        ;;
      --chain.spec-id=*)
        spec_id_cli_present=true
        ;;
    esac
  done
}

check_config_file_args() {
  local awaiting_value="false"
  for arg in "$@"; do
    if [[ "${awaiting_value}" == "true" ]]; then
      config_file_cli_present=true
      awaiting_value="false"
      continue
    fi

    case "${arg}" in
      --config-file-path)
        awaiting_value="true"
        ;;
      --config-file-path=*)
        config_file_cli_present=true
        ;;
    esac
  done
}

check_chain_id_args "${EXTRA_ARGS[@]}"
check_chain_id_args "${POSITIONAL_ARGS[@]}"
check_spec_id_args "${EXTRA_ARGS[@]}"
check_spec_id_args "${POSITIONAL_ARGS[@]}"
check_config_file_args "${EXTRA_ARGS[@]}"
check_config_file_args "${POSITIONAL_ARGS[@]}"

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

SIDECAR_ARGS=()

if [[ "${config_file_cli_present}" != "true" ]]; then
  SIDECAR_ARGS+=(
    --config-file-path
    "${STACK_CONFIG_FILE}"
  )
fi

if [[ "${spec_id_cli_present}" != "true" ]]; then
  SIDECAR_ARGS+=(
    --chain.spec-id
    "${SPEC_ID_VALUE}"
  )
fi

if [[ "${chain_id_cli_present}" != "true" ]]; then
  SIDECAR_ARGS+=(
    --chain.chain-id
    "${CHAIN_ID_VALUE}"
  )
fi

SIDECAR_ARGS+=("${EXTRA_ARGS[@]}")
SIDECAR_ARGS+=("${POSITIONAL_ARGS[@]}")

cleanup_db_path() {
  local resolved_path="$1"
  local default_resolved="$2"

  if [[ "${resolved_path}" != "${default_resolved}" ]]; then
    return
  fi

  if [[ -z "${resolved_path}" ]]; then
    return
  fi

  if [[ ! -e "${resolved_path}" ]]; then
    return
  fi

  rm -rf -- "${resolved_path}"
}

cleanup() {
  if [[ "${SHOULD_CLEANUP}" != "true" ]]; then
    return
  fi

  cleanup_db_path "${ASSERTION_STORE_DB_ABS}" "${DEFAULT_ASSERTION_STORE_DB_ABS}"
  cleanup_db_path "${TRANSACTION_OBSERVER_DB_ABS}" "${DEFAULT_TRANSACTION_OBSERVER_DB_ABS}"
  cleanup_db_path "${STATE_WORKER_MDBX_ABS}" "${DEFAULT_STATE_WORKER_MDBX_ABS}"

  local sidecar_host_dir="${ROOT_DIR}/.local/sidecar-host"
  if [[ -d "${sidecar_host_dir}" ]]; then
    rmdir "${sidecar_host_dir}" 2>/dev/null || true
  fi

  local local_dir="${ROOT_DIR}/.local"
  if [[ -d "${local_dir}" ]]; then
    rmdir "${local_dir}" 2>/dev/null || true
  fi
}

trap cleanup EXIT

CARGO_RUN_ARGS+=(--)

SHOULD_CLEANUP=true

set +e
"${CARGO_RUN_ARGS[@]}" "${SIDECAR_ARGS[@]}"
EXIT_CODE=$?
set -e

exit "${EXIT_CODE}"
