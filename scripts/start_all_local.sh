#!/usr/bin/env bash
# Bring up all 9 processes on localhost. Each runs in its own background
# process; logs go to logs/run-<UTC-ts>/<node>.log. A `logs/latest`
# symlink always points at the most recent run dir for tooling.
#
# Usage:
#   ./scripts/start_all_local.sh                            # use config/overlay.yaml
#   MINI2_OVERLAY=config/topo/star.yaml ./scripts/start_all_local.sh
#   ./scripts/start_all_local.sh A B C                      # only these nodes

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

# Per-run timestamped log dir so consecutive runs don't clobber each other.
RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
RUN_DIR="$ROOT/logs/run-$RUN_TS"
mkdir -p "$RUN_DIR"
ln -sfn "run-$RUN_TS" "$ROOT/logs/latest"

# Forwarded to start_node.sh / binaries / telemetry writers.
export MINI2_RUN_DIR="$RUN_DIR"

# Let the user override start order / subset.
NODES=("${@:-A B C D E F G H I}")
if [[ ${#NODES[@]} -eq 1 && "${NODES[0]}" == *" "* ]]; then
  read -ra NODES <<< "${NODES[0]}"
fi

# Start leaves first so gateway's forward RPCs succeed immediately.
ORDER=(I H G F E D C B A)
SELECTED=()
for n in "${ORDER[@]}"; do
  for want in "${NODES[@]}"; do
    if [[ "$want" == "$n" ]]; then SELECTED+=("$n"); fi
  done
done

PIDS_FILE="$RUN_DIR/pids.txt"
: > "$PIDS_FILE"
{
  echo "ts_utc=$RUN_TS"
  echo "overlay=${MINI2_OVERLAY:-$ROOT/config/overlay.yaml}"
  echo "host=$(hostname)"
  echo "nodes=${SELECTED[*]}"
} > "$RUN_DIR/META.txt"

for n in "${SELECTED[@]}"; do
  echo "[start_all] starting $n -> $RUN_DIR/$n.log"
  (
    exec "$SCRIPT_DIR/start_node.sh" "$n" > "$RUN_DIR/$n.log" 2>&1
  ) &
  echo "$n $!" >> "$PIDS_FILE"
  sleep 0.2
done

# stop_all.sh reads $LOGDIR/pids.txt; keep that link compatible.
ln -sfn "run-$RUN_TS/pids.txt" "$ROOT/logs/pids.txt"

echo
echo "[start_all] launched ${#SELECTED[@]} processes."
echo "[start_all] run dir: $RUN_DIR"
echo "[start_all] PIDs:    $PIDS_FILE"
echo "[start_all] use scripts/stop_all.sh to stop them"
