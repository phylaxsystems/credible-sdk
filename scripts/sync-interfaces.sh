#!/usr/bin/env bash
set -euo pipefail

# sync-interfaces.sh - Synchronize canonical Solidity interfaces
#
# Usage:
#   ./scripts/sync-interfaces.sh sync   - Copy canonical files to credible-std
#   ./scripts/sync-interfaces.sh check  - Check if credible-std is in sync (CI mode)
#
# Canonical source (single source of truth):
#   crates/assertion-executor/interfaces/PhEvm.sol
#   crates/assertion-executor/interfaces/ITriggerRecorder.sol
#
# Downstream targets:
#   testdata/mock-protocol/lib/credible-std/src/PhEvm.sol
#   testdata/mock-protocol/lib/credible-std/src/TriggerRecorder.sol
#
# Note: The canonical ITriggerRecorder.sol uses the I prefix for Rust binding
# compatibility. The downstream TriggerRecorder.sol uses the name without I
# prefix for Solidity convention. The sync strips the interface name prefix
# when comparing/copying.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CANONICAL_DIR="$REPO_ROOT/crates/assertion-executor/interfaces"
DOWNSTREAM_DIR="$REPO_ROOT/testdata/mock-protocol/lib/credible-std/src"

# Interface mappings as parallel arrays (bash 3 compatible)
CANONICAL_NAMES=("PhEvm.sol" "ITriggerRecorder.sol")
DOWNSTREAM_NAMES=("PhEvm.sol" "TriggerRecorder.sol")

# Transform canonical file for downstream (rename interface if needed)
transform_for_downstream() {
    local canonical_file="$1"
    local canonical_name="$2"
    local downstream_name="$3"

    if [[ "$canonical_name" == "$downstream_name" ]]; then
        cat "$canonical_file"
    else
        local canonical_iface="${canonical_name%.sol}"
        local downstream_iface="${downstream_name%.sol}"
        sed "s/${canonical_iface}/${downstream_iface}/g" "$canonical_file"
    fi
}

cmd_check() {
    local has_drift=0

    for i in "${!CANONICAL_NAMES[@]}"; do
        local canonical_name="${CANONICAL_NAMES[$i]}"
        local downstream_name="${DOWNSTREAM_NAMES[$i]}"
        local canonical_file="$CANONICAL_DIR/$canonical_name"
        local downstream_file="$DOWNSTREAM_DIR/$downstream_name"

        if [[ ! -f "$canonical_file" ]]; then
            echo "ERROR: Canonical file missing: $canonical_file"
            has_drift=1
            continue
        fi

        if [[ ! -f "$downstream_file" ]]; then
            echo "ERROR: Downstream file missing: $downstream_file"
            echo "  Run: ./scripts/sync-interfaces.sh sync"
            has_drift=1
            continue
        fi

        # Compare: transform canonical to downstream form and diff
        if ! diff -q <(transform_for_downstream "$canonical_file" "$canonical_name" "$downstream_name") "$downstream_file" > /dev/null 2>&1; then
            echo "DRIFT DETECTED: $canonical_name -> $downstream_name"
            echo "  Canonical: $canonical_file"
            echo "  Downstream: $downstream_file"
            echo "  Run: ./scripts/sync-interfaces.sh sync"
            echo ""
            diff <(transform_for_downstream "$canonical_file" "$canonical_name" "$downstream_name") "$downstream_file" || true
            echo ""
            has_drift=1
        else
            echo "OK: $canonical_name -> $downstream_name (in sync)"
        fi
    done

    if [[ $has_drift -ne 0 ]]; then
        echo ""
        echo "Interface drift detected. Fix with:"
        echo "  ./scripts/sync-interfaces.sh sync"
        exit 1
    fi

    echo ""
    echo "All interfaces are in sync."
}

cmd_sync() {
    for i in "${!CANONICAL_NAMES[@]}"; do
        local canonical_name="${CANONICAL_NAMES[$i]}"
        local downstream_name="${DOWNSTREAM_NAMES[$i]}"
        local canonical_file="$CANONICAL_DIR/$canonical_name"
        local downstream_file="$DOWNSTREAM_DIR/$downstream_name"

        if [[ ! -f "$canonical_file" ]]; then
            echo "ERROR: Canonical file missing: $canonical_file"
            exit 1
        fi

        echo "Syncing: $canonical_name -> $downstream_name"
        transform_for_downstream "$canonical_file" "$canonical_name" "$downstream_name" > "$downstream_file"
    done

    echo "Sync complete."
}

case "${1:-}" in
    check)
        cmd_check
        ;;
    sync)
        cmd_sync
        ;;
    *)
        echo "Usage: $0 {sync|check}"
        echo ""
        echo "  sync   - Copy canonical interface files to credible-std"
        echo "  check  - Check if credible-std interfaces are in sync (for CI)"
        exit 1
        ;;
esac
