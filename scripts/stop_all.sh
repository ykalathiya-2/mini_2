#!/usr/bin/env bash
# Stop every node we launched via start_all_local.sh.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
LOGDIR=${MINI2_LOG_DIR:-$ROOT/logs}
PIDS_FILE="$LOGDIR/pids.txt"
if [[ ! -f "$PIDS_FILE" ]]; then
  echo "no $PIDS_FILE — nothing to stop"
  exit 0
fi
while read -r name pid; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "stopping $name (pid $pid)"
    kill -TERM "$pid" 2>/dev/null || true
  fi
done < "$PIDS_FILE"
sleep 1
while read -r name pid; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "force-killing $name (pid $pid)"
    kill -KILL "$pid" 2>/dev/null || true
  fi
done < "$PIDS_FILE"
rm -f "$PIDS_FILE"
# Also best-effort kill anything else bound to our ports.
for port in 50051 50052 50053 50054 50055 50056 50057 50058 50059; do
  pids=$(lsof -ti :$port 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    echo "killing leftover on :$port ($pids)"
    kill -KILL $pids 2>/dev/null || true
  fi
done
echo "done"
