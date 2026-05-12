#!/usr/bin/env bash
# Deploy mini_2 to the remote (Arch) host via rsync, then build the C++
# binaries there natively.
#
# Usage:
#   ./scripts/deploy.sh                           # full deploy
#   ./scripts/deploy.sh --skip-data               # don't rsync the partition
#                                                 # CSVs (saves ~1.3 GB)
#   ./scripts/deploy.sh --skip-build              # rsync only, no remote build
#
# Env:
#   MINI2_REMOTE_HOST   ip/hostname of the remote (default 192.168.50.1)
#   MINI2_REMOTE_USER   ssh user                  (default yash)
#   MINI2_REMOTE_DIR    remote install path       (default ~/mini_2)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

REMOTE_HOST=${MINI2_REMOTE_HOST:-192.168.50.1}
REMOTE_USER=${MINI2_REMOTE_USER:-yash}
REMOTE_DIR=${MINI2_REMOTE_DIR:-mini_2}     # relative to remote $HOME
SSH_OPTS=${MINI2_SSH_OPTS:-"-o BatchMode=yes -o ConnectTimeout=10"}

SKIP_DATA=0
SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --skip-data)  SKIP_DATA=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

echo "[deploy] target ${REMOTE_USER}@${REMOTE_HOST}:~/${REMOTE_DIR}"

# Quick reachability check.
if ! ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "echo connected" >/dev/null 2>&1; then
  echo "[deploy] ERROR: ssh ${REMOTE_USER}@${REMOTE_HOST} failed." >&2
  echo "[deploy]   Check: passwordless ssh works ('ssh-copy-id ${REMOTE_USER}@${REMOTE_HOST}')," >&2
  echo "[deploy]   that sshd is running on the remote, and host/user env are right." >&2
  exit 1
fi

# Make sure the remote dir exists.
ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ~/${REMOTE_DIR}/{logs,data/partitions,build}"

# rsync source. Excludes mini_2-local artifacts that don't belong on the remote.
RSYNC_EXCLUDES=(
  --exclude '.git/'
  --exclude '.venv/'
  --exclude '.DS_Store'
  --exclude '__pycache__/'
  --exclude 'build/'
  --exclude 'logs/'
  --exclude 'data/2017_Yellow_Taxi_Trip_Data_*.csv'   # the 14GB master CSV
  --exclude 'data/partitions/'                       # legacy 20M baseline partitions
  --exclude 'data/partitions_*/'                     # per-scheme 70M partitions (kept by sharder)
)
echo "[deploy] rsync source -> ~/${REMOTE_DIR}"
rsync -aH --delete "${RSYNC_EXCLUDES[@]}" \
  "$ROOT/" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"

# Partitions: only the slices owned by remote nodes (F, G, H, I in the 5/4
# split). Saves bandwidth.
if [[ $SKIP_DATA -eq 0 ]]; then
  echo "[deploy] rsync partitions {F,G,H,I}.csv -> ~/${REMOTE_DIR}/data/partitions/"
  rsync -aH \
    "$ROOT/data/partitions/F.csv" \
    "$ROOT/data/partitions/G.csv" \
    "$ROOT/data/partitions/H.csv" \
    "$ROOT/data/partitions/I.csv" \
    "$ROOT/data/partitions/manifest.json" \
    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/data/partitions/"
fi

# Build remotely. We assume Arch has gcc/cmake; if grpc/protobuf are missing
# the user can install them via:
#   sudo pacman -S grpc protobuf abseil-cpp cmake python python-pip
if [[ $SKIP_BUILD -eq 0 ]]; then
  echo "[deploy] remote build (C++)"
  ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "bash -c '
    set -euo pipefail
    cd ~/${REMOTE_DIR}
    if ! command -v cmake >/dev/null; then
      echo \"cmake missing on remote — install: sudo pacman -S cmake gcc grpc protobuf abseil-cpp\" >&2
      exit 2
    fi
    cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build build/cpp -j
    test -x build/cpp/mini2_node && echo \"[remote] built mini2_node OK\"
  '"

  echo "[deploy] remote python deps"
  ssh $SSH_OPTS "${REMOTE_USER}@${REMOTE_HOST}" "bash -c '
    set -euo pipefail
    cd ~/${REMOTE_DIR}
    if [ ! -d .venv ]; then
      python3 -m venv .venv
    fi
    .venv/bin/pip install --quiet --upgrade pip
    .venv/bin/pip install --quiet grpcio grpcio-tools protobuf pyyaml
    .venv/bin/python -m grpc_tools.protoc -I proto \
      --python_out=proto_gen/python --grpc_python_out=proto_gen/python \
      proto/mini2.proto
    echo \"[remote] python venv ready\"
  '"
fi

echo "[deploy] done."
