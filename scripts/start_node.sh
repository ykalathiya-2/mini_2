#!/usr/bin/env bash
# Launch one Mini 2 node by name (A..I). Reads overlay.yaml to decide
# whether to run the C++ or Python implementation. Runs in the foreground
# so you can Ctrl-C it cleanly.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

NODE=${1:-}
if [[ -z "$NODE" ]]; then
  echo "usage: $0 <NODE_NAME>  (e.g., A)" >&2
  exit 2
fi

OVERLAY=${MINI2_OVERLAY:-$ROOT/config/overlay.yaml}
DATA_DIR=${MINI2_DATA_DIR:-$ROOT/data/partitions}

# Pick impl from the YAML via a grep (avoids a YAML dep in bash).
IMPL=$(awk -v n="$NODE" '
  $1 == n":" { print $0; exit }
' "$OVERLAY" | sed -nE 's/.*impl:[[:space:]]*([a-z]+).*/\1/p')

if [[ -z "$IMPL" ]]; then
  echo "could not find impl for node $NODE in $OVERLAY" >&2
  exit 2
fi

export MINI2_OVERLAY="$OVERLAY"
export MINI2_DATA_DIR="$DATA_DIR"

echo "[launcher] node=$NODE impl=$IMPL overlay=$OVERLAY data=$DATA_DIR"

case "$IMPL" in
  cpp)
    exec "$ROOT/build/cpp/mini2_node" \
      --name "$NODE" \
      --overlay "$OVERLAY" \
      --data-dir "$DATA_DIR"
    ;;
  python|py)
    if [[ -f "$ROOT/.venv/bin/activate" ]]; then
      # shellcheck source=/dev/null
      source "$ROOT/.venv/bin/activate"
    elif [[ -f "/Users/spartan/mini_1/.venv/bin/activate" ]]; then
      # Fall back to the Mini 1 venv we already populated with grpc.
      # shellcheck source=/dev/null
      source "/Users/spartan/mini_1/.venv/bin/activate"
    fi
    cd "$ROOT"
    exec python -m py.server.node \
      --name "$NODE" \
      --overlay "$OVERLAY" \
      --data-dir "$DATA_DIR"
    ;;
  *)
    echo "unknown impl '$IMPL' for node $NODE" >&2
    exit 2
    ;;
esac
