#!/usr/bin/env bash
# Multi-host launcher for the 5/4 split.
#
# Local host  (this Mac):           A B C D E
# Remote host (the Arch box):       F G H I
#
# Behaviour:
#   1. Picks an overlay (default config/overlay.yaml; --topo NAME picks
#      config/topo/NAME.yaml from the generator's output).
#   2. Creates a fresh logs/run-<UTC-ts>/ dir on BOTH hosts.
#   3. Starts F G H I on the remote via ssh; each writes to its remote
#      run dir and a pidfile.
#   4. Starts A B C D E locally; each writes to the local run dir.
#   5. Drops a META.txt on both sides describing the run.
#
# The bench harness drives this; you can also use it interactively:
#   ./scripts/start_multihost.sh --topo star
#   ./scripts/stop_multihost.sh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

REMOTE_HOST=${MINI2_REMOTE_HOST:-192.168.50.1}
REMOTE_USER=${MINI2_REMOTE_USER:-yash}
REMOTE_DIR=${MINI2_REMOTE_DIR:-mini_2}
SSH_OPTS=${MINI2_SSH_OPTS:-"-o BatchMode=yes -o ConnectTimeout=10"}

LOCAL_NODES=(A B C D E)
REMOTE_NODES=(F G H I)

OVERLAY_PATH=""
for ((i = 1; i <= $#; i++)); do
  arg=${!i}
  case "$arg" in
    --topo)
      i=$((i + 1)); name=${!i}
      OVERLAY_PATH="config/topo/${name}.yaml"
      ;;
    --overlay)
      i=$((i + 1)); OVERLAY_PATH=${!i}
      ;;
    -h|--help)
      sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done
[[ -z "$OVERLAY_PATH" ]] && OVERLAY_PATH="config/overlay.yaml"

if [[ ! -f "$ROOT/$OVERLAY_PATH" ]]; then
  echo "[start_multihost] overlay not found: $ROOT/$OVERLAY_PATH" >&2
  echo "[start_multihost] generate it first: .venv/bin/python -m py.topology.generator --host-h1 ... --host-h2 ..." >&2
  exit 2
fi
echo "[start_multihost] overlay = $OVERLAY_PATH"

RUN_TS=$(date -u +%Y%m%dT%H%M%SZ)
LOCAL_RUN_DIR="$ROOT/logs/run-$RUN_TS"
REMOTE_RUN_DIR="logs/run-$RUN_TS"   # relative to ~/$REMOTE_DIR
mkdir -p "$LOCAL_RUN_DIR"
ln -sfn "run-$RUN_TS" "$ROOT/logs/latest"
ln -sfn "run-$RUN_TS/pids.txt" "$ROOT/logs/pids.txt"

# Sanity: SSH up.
if ! ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "echo ok" >/dev/null 2>&1; then
  echo "[start_multihost] ssh to ${REMOTE_USER}@${REMOTE_HOST} failed; deploy first." >&2
  exit 1
fi

# Push the chosen overlay to the remote (so both sides agree on the same
# copy even if local edits haven't been re-deployed). Cheap, single file.
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ~/${REMOTE_DIR}/$(dirname "$OVERLAY_PATH")"
rsync -aH "$ROOT/$OVERLAY_PATH" \
  "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/${OVERLAY_PATH}"
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ~/${REMOTE_DIR}/${REMOTE_RUN_DIR}"

# META so post-mortem knows how this run was launched.
{
  echo "ts_utc=$RUN_TS"
  echo "overlay=$OVERLAY_PATH"
  echo "local_host=$(hostname)"
  echo "remote_host=${REMOTE_USER}@${REMOTE_HOST}"
  echo "local_nodes=${LOCAL_NODES[*]}"
  echo "remote_nodes=${REMOTE_NODES[*]}"
} > "$LOCAL_RUN_DIR/META.txt"
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" \
  "cat > ~/${REMOTE_DIR}/${REMOTE_RUN_DIR}/META.txt" < "$LOCAL_RUN_DIR/META.txt"

LOCAL_PIDS="$LOCAL_RUN_DIR/pids.txt"
: > "$LOCAL_PIDS"
REMOTE_PIDS_FILE="${REMOTE_RUN_DIR}/pids.txt"
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "rm -f ~/${REMOTE_DIR}/${REMOTE_PIDS_FILE}; touch ~/${REMOTE_DIR}/${REMOTE_PIDS_FILE}"

# Allow MINI2_DATA_DIR / MINI2_WORKERS / chunk env vars to flow through to
# both local and remote nodes. If MINI2_DATA_DIR is an absolute path under
# this repo (e.g. /Users/.../data/partitions_trip_distance), translate it
# relative to the remote repo root for the remote nodes.
DATA_DIR_LOCAL="${MINI2_DATA_DIR:-$ROOT/data/partitions}"
DATA_DIR_REL="${DATA_DIR_LOCAL#$ROOT/}"
DATA_DIR_REMOTE="\$PWD/${DATA_DIR_REL}"
EXTRA_ENV=""
for v in MINI2_WORKERS MINI2_INITIAL_ROWS MINI2_MAX_ROWS MINI2_TARGET_CHUNK_MS; do
  if [[ -n "${!v:-}" ]]; then
    EXTRA_ENV+=" export $v=\"${!v}\";"
  fi
done

# --- Remote launches: leaves first --------------------------------------
echo "[start_multihost] launching ${REMOTE_NODES[*]} on ${REMOTE_USER}@${REMOTE_HOST}"
echo "[start_multihost] data_dir = $DATA_DIR_LOCAL  (remote: $DATA_DIR_REMOTE)"
REMOTE_NODES_ORDER=(I H G F)
for n in "${REMOTE_NODES_ORDER[@]}"; do
  ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "bash -c '
    set -e
    cd ~/${REMOTE_DIR}
    export MINI2_OVERLAY=\"\$PWD/${OVERLAY_PATH}\"
    export MINI2_RUN_DIR=\"\$PWD/${REMOTE_RUN_DIR}\"
    export MINI2_DATA_DIR=\"${DATA_DIR_REMOTE}\"
    ${EXTRA_ENV}
    nohup ./scripts/start_node.sh ${n} > \"\$MINI2_RUN_DIR/${n}.log\" 2>&1 &
    echo \"${n} \$!\" >> \"\$MINI2_RUN_DIR/pids.txt\"
    disown || true
  '" </dev/null
  sleep 0.3
done

# --- Local launches -----------------------------------------------------
export MINI2_OVERLAY="$ROOT/$OVERLAY_PATH"
export MINI2_RUN_DIR="$LOCAL_RUN_DIR"
export MINI2_DATA_DIR="$DATA_DIR_LOCAL"

LOCAL_ORDER=(E D C B A)
for n in "${LOCAL_ORDER[@]}"; do
  echo "[start_multihost] starting local $n -> $LOCAL_RUN_DIR/$n.log"
  ( exec "$SCRIPT_DIR/start_node.sh" "$n" > "$LOCAL_RUN_DIR/$n.log" 2>&1 ) &
  echo "$n $!" >> "$LOCAL_PIDS"
  sleep 0.2
done

echo
echo "[start_multihost] launched 9 processes (5 local, 4 remote)"
echo "[start_multihost] local  run dir: $LOCAL_RUN_DIR"
echo "[start_multihost] remote run dir: ${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}/${REMOTE_RUN_DIR}"
echo "[start_multihost] use scripts/stop_multihost.sh to tear down"
