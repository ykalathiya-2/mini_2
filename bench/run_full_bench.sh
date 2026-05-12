#!/usr/bin/env bash
# Drive the full Mini 2 benchmark suite. Four orthogonal axes are swept one
# at a time (not full cross-product).
#
# Defaults: tree topology, trip_distance scheme, 8 worker threads, dynamic
# chunk size. Each sweep varies one of those while holding the rest fixed.
#
# Usage:
#   bench/run_full_bench.sh local                  # localhost
#   bench/run_full_bench.sh multihost              # Mac + Fedora over LAN
#   bench/run_full_bench.sh multihost scheme topo  # only those sweeps

set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$ROOT"

MODE=${1:-multihost}
shift || true
SWEEPS=("$@")
if [[ ${#SWEEPS[@]} -eq 0 ]]; then
  SWEEPS=(scheme topo chunk threads)
fi

OUT=bench/results
PY=python3

DEF_TOPO=tree
DEF_SCHEME=trip_distance
DEF_THREADS=8
DEF_CHUNK=0
DEF_REPEATS=2

run() {
  local label=$1; shift
  echo
  echo "============================================================="
  echo "  $label"
  echo "============================================================="
  $PY bench/topo_bench.py --mode "$MODE" --out-dir "$OUT" --repeats "$DEF_REPEATS" "$@"
}

for s in "${SWEEPS[@]}"; do
  case "$s" in
    scheme)
      # Show smart-routing benefit. All workloads, single concurrency.
      run "SWEEP: clustering scheme (4 schemes)" \
        --topos "$DEF_TOPO" \
        --schemes round_robin,trip_distance,pu_location_id,pickup_datetime \
        --threads $DEF_THREADS --chunks $DEF_CHUNK --concurrency 1
      ;;
    topo)
      # Topology effect on hop chain. Use trip_distance (best scheme).
      # Skip 'broad' (it dominates runtime and topology effect is small).
      run "SWEEP: topology (4 topologies)" \
        --topos tree,star,chain,grid \
        --schemes "$DEF_SCHEME" \
        --threads $DEF_THREADS --chunks $DEF_CHUNK --concurrency 1 \
        --workloads narrow,small,medium
      ;;
    chunk)
      # Chunk-size knee curve. Just medium workload — narrow doesn't have
      # enough rows to differentiate, broad takes too long.
      run "SWEEP: chunk size (6 sizes)" \
        --topos "$DEF_TOPO" \
        --schemes "$DEF_SCHEME" \
        --threads $DEF_THREADS \
        --chunks 0,256,1024,2048,4096,8192 \
        --concurrency 1 \
        --workloads small,medium
      ;;
    threads)
      # Multithreading curve under concurrent requests.
      run "SWEEP: worker threads (5 counts)" \
        --topos "$DEF_TOPO" \
        --schemes "$DEF_SCHEME" \
        --threads 1,2,4,8,16 --chunks $DEF_CHUNK \
        --concurrency 1,4 \
        --workloads small,medium
      ;;
    *)
      echo "unknown sweep: $s" >&2; exit 2 ;;
  esac
done

echo
echo "All sweeps complete; per-cell files in $OUT/"
