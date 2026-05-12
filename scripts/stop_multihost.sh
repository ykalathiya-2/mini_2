#!/usr/bin/env bash
# Tear down a multi-host run started by start_multihost.sh. Reads the
# local PIDs from logs/latest/pids.txt and the remote PIDs from the
# matching dir on the remote host.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

REMOTE_HOST=${MINI2_REMOTE_HOST:-192.168.50.1}
REMOTE_USER=${MINI2_REMOTE_USER:-yash}
REMOTE_DIR=${MINI2_REMOTE_DIR:-mini_2}
SSH_OPTS=${MINI2_SSH_OPTS:-"-o BatchMode=yes -o ConnectTimeout=10"}

LATEST="$ROOT/logs/latest"
if [[ ! -d "$LATEST" ]]; then
  echo "no logs/latest — nothing to stop"; exit 0
fi
RUN_NAME=$(basename "$(readlink -f "$LATEST" 2>/dev/null || readlink "$LATEST")")
[[ -z "$RUN_NAME" ]] && RUN_NAME=$(basename "$(stat -f '%Y' "$LATEST" 2>/dev/null || echo "$LATEST")")

# Local kill — same logic as stop_all.sh.
LOCAL_PIDS="$LATEST/pids.txt"
if [[ -f "$LOCAL_PIDS" ]]; then
  while read -r name pid; do
    [[ -z "${pid:-}" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      echo "stopping local $name (pid $pid)"
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done < "$LOCAL_PIDS"
fi

# Remote kill.
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "bash -c '
  set -e
  RUN_DIR=~/${REMOTE_DIR}/logs/${RUN_NAME}
  if [ -f \"\$RUN_DIR/pids.txt\" ]; then
    while read name pid; do
      [ -z \"\$pid\" ] && continue
      if kill -0 \"\$pid\" 2>/dev/null; then
        echo \"stopping remote \$name (pid \$pid)\"
        kill -TERM \"\$pid\" 2>/dev/null || true
      fi
    done < \"\$RUN_DIR/pids.txt\"
  fi
  # Best-effort kill any leftovers on our ports.
  for p in 50056 50057 50058 50059; do
    pids=\$(ss -ltnp 2>/dev/null | awk -v port=\":\$p\" \"\\\$4 ~ port {print}\" | grep -oE \"pid=[0-9]+\" | sed s/pid=// || true)
    [ -n \"\$pids\" ] && kill -KILL \$pids 2>/dev/null || true
  done
'" </dev/null

sleep 1

# Force-kill any local survivors.
if [[ -f "$LOCAL_PIDS" ]]; then
  while read -r name pid; do
    [[ -z "${pid:-}" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      echo "force-killing local $name (pid $pid)"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done < "$LOCAL_PIDS"
fi
for port in 50051 50052 50053 50054 50055; do
  pids=$(lsof -ti :$port 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    echo "killing local leftover on :$port ($pids)"
    kill -KILL $pids 2>/dev/null || true
  fi
done

# Pull Fedora logs into the local run dir so everything is in one place.
LOCAL_RUN="$ROOT/logs/$RUN_NAME"
if [[ -d "$LOCAL_RUN" ]]; then
  echo "[stop_multihost] syncing remote logs -> $LOCAL_RUN/"
  rsync -aH --ignore-existing \
    "${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}/logs/${RUN_NAME}/" \
    "$LOCAL_RUN/" 2>/dev/null \
    || echo "[stop_multihost] warn: rsync failed (logs may be incomplete)"
fi

echo "done"
