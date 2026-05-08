#!/usr/bin/env python3
"""Topology bench harness.

For each named topology in --topos, this:

  1. Generates / picks the overlay yaml (config/topo/<name>.yaml).
  2. Brings up the cluster (localhost or multi-host depending on --mode).
  3. Waits for all nodes to be ready.
  4. Runs the workload (selectivity sweep + concurrency sweep).
  5. Tears down.
  6. Records per-(topology, workload) metrics into bench/results/<topo>.json.

The workload uses the existing C++ client binary so we don't have to
re-implement the gRPC pacing logic; the client already emits one JSON line
per request via --json. We aggregate those.

Usage:
    .venv/bin/python bench/topo_bench.py --mode local --topos star,tree
    .venv/bin/python bench/topo_bench.py --mode multihost --topos all
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "build" / "cpp" / "mini2_client"
TOPO_DIR = ROOT / "config" / "topo"
RESULTS_DIR = ROOT / "bench" / "results"

ALL_TOPOS = ["star", "tree", "binary_tree", "chain", "ring", "grid", "mesh", "random_k"]

# Selectivity workloads — derived for the 20M-row dataset:
# trip_distance histograms in NYC taxi data are heavy-tailed; these ranges
# give roughly the listed match fraction. Confirmed empirically via earlier
# runs (e.g. [5,6] -> ~545k = ~2.7%).
WORKLOADS = [
    {"name": "narrow",  "column": "trip_distance", "low": 5.0,    "high": 5.05,  "expect_pct": 0.1},
    {"name": "small",   "column": "trip_distance", "low": 5.0,    "high": 6.0,   "expect_pct": 2.7},
    {"name": "medium",  "column": "trip_distance", "low": 1.0,    "high": 3.0,   "expect_pct": 30.0},
    {"name": "broad",   "column": "trip_distance", "low": 0.0,    "high": 30.0,  "expect_pct": 95.0},
]

CONCURRENCY_LEVELS = [1, 2, 4]


def run_client_json(overlay: Path, *args: str, timeout: float = 90.0) -> List[dict]:
    cmd = [str(CLIENT),
           "--overlay", str(overlay),
           "--json", "--quiet",
           *args]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    out = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    if not out and res.returncode != 0:
        sys.stderr.write(f"[bench] client FAILED rc={res.returncode}\n")
        sys.stderr.write(res.stderr[:500] + "\n")
    return out


def jains_index(xs: List[float]) -> float:
    if not xs:
        return 0.0
    s  = sum(xs)
    s2 = sum(x * x for x in xs)
    return (s * s) / (len(xs) * s2) if s2 > 0 else 1.0


def percentiles(xs: List[float]) -> dict:
    if not xs:
        return {"min": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0, "std": 0}
    s = sorted(xs)
    n = len(s)
    def pct(p):
        if n == 1: return s[0]
        k = max(0, min(n - 1, int(round((p / 100.0) * (n - 1)))))
        return s[k]
    return {
        "min":  round(s[0], 2),
        "p50":  round(pct(50), 2),
        "p95":  round(pct(95), 2),
        "p99":  round(pct(99), 2),
        "max":  round(s[-1], 2),
        "mean": round(statistics.mean(xs), 2),
        "std":  round(statistics.pstdev(xs), 2) if n > 1 else 0.0,
    }


def cluster_up_local(overlay: Path) -> Path:
    """Bring up the cluster on localhost via start_all_local.sh.
    Returns the run_dir path."""
    env = os.environ.copy()
    env["MINI2_OVERLAY"] = str(overlay)
    res = subprocess.run(
        [str(ROOT / "scripts" / "start_all_local.sh")],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=30,
    )
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise RuntimeError(f"start_all_local.sh failed rc={res.returncode}")
    # Pull the run dir from the script's output.
    run_dir: Optional[Path] = None
    for line in res.stdout.splitlines():
        if "run dir:" in line:
            run_dir = Path(line.split("run dir:")[1].strip())
            break
    return run_dir or (ROOT / "logs" / "latest")


def cluster_up_multihost(overlay: Path) -> Path:
    res = subprocess.run(
        [str(ROOT / "scripts" / "start_multihost.sh"),
         "--overlay", str(overlay.relative_to(ROOT))],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise RuntimeError(f"start_multihost.sh failed rc={res.returncode}")
    run_dir: Optional[Path] = None
    for line in res.stdout.splitlines():
        if "local  run dir:" in line:
            run_dir = Path(line.split("local  run dir:")[1].strip())
            break
    return run_dir or (ROOT / "logs" / "latest")


def cluster_down(mode: str) -> None:
    script = "stop_multihost.sh" if mode == "multihost" else "stop_all.sh"
    subprocess.run([str(ROOT / "scripts" / script)],
                   cwd=ROOT, capture_output=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                   timeout=30)


def wait_ready(overlay: Path, max_wait_s: float = 15.0) -> bool:
    """Send a Heartbeat to gateway A; succeeds once it accepts client RPCs."""
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        rs = run_client_json(overlay, "--column", "trip_distance",
                             "--low", "1e9", "--high", "1e9",
                             "--limit-chunks", "1", timeout=5.0)
        if rs and rs[0].get("ok"):
            return True
        time.sleep(0.5)
    return False


def run_workload(overlay: Path, w: dict, concurrency: int, repeats: int = 3) -> dict:
    samples = []
    raw_runs = []
    for _ in range(repeats):
        rs = run_client_json(
            overlay,
            "--column", w["column"],
            "--low",  str(w["low"]),
            "--high", str(w["high"]),
            "--concurrency", str(concurrency),
        )
        for r in rs:
            if r.get("ok"):
                samples.append(r["ms_total"])
        raw_runs.append(rs)

    times = samples
    return {
        "concurrency": concurrency,
        "repeats":     repeats,
        "samples":     len(samples),
        "rows_mean":   int(statistics.mean(r["rows"] for run in raw_runs for r in run if r.get("ok"))) if samples else 0,
        "ms":          percentiles(times),
        "first_chunk_ms": percentiles([r["ms_first_chunk"] for run in raw_runs for r in run if r.get("ok")]),
        "jains_index": round(jains_index(times), 4) if times else None,
    }


def bench_topology(name: str, mode: str) -> dict:
    overlay = TOPO_DIR / f"{name}.yaml"
    if not overlay.exists():
        raise FileNotFoundError(f"missing topology overlay: {overlay}")

    print(f"\n=== topology: {name} ===")
    cluster_down(mode)
    time.sleep(1)

    if mode == "multihost":
        run_dir = cluster_up_multihost(overlay)
    else:
        run_dir = cluster_up_local(overlay)
    if not wait_ready(overlay):
        cluster_down(mode)
        return {"topology": name, "mode": mode, "error": "cluster did not become ready"}

    out: Dict = {
        "topology": name,
        "mode":     mode,
        "overlay":  str(overlay.relative_to(ROOT)),
        "run_dir":  str(run_dir),
        "started":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "workloads": [],
    }
    for w in WORKLOADS:
        for c in CONCURRENCY_LEVELS:
            print(f"  {w['name']:>7} concurrency={c} ... ", end="", flush=True)
            r = run_workload(overlay, w, c)
            r["workload"] = w["name"]
            r["range"]    = [w["low"], w["high"]]
            out["workloads"].append(r)
            ms = r["ms"]
            print(f"rows={r['rows_mean']:>7}  mean={ms['mean']:>7.1f}ms  p95={ms['p95']:>7.1f}ms  jains={r['jains_index']}")

    cluster_down(mode)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "multihost"], default="local")
    ap.add_argument("--topos", default="all",
                    help="comma-separated subset, or 'all'")
    ap.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    args = ap.parse_args()

    targets = ALL_TOPOS if args.topos == "all" else args.topos.split(",")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if not CLIENT.exists():
        sys.stderr.write(f"client missing: {CLIENT}\n  build first: cmake --build build/cpp -j\n")
        return 2

    summary = {"mode": args.mode, "topos": [], "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    for name in targets:
        try:
            result = bench_topology(name, args.mode)
        except Exception as e:
            result = {"topology": name, "mode": args.mode, "error": str(e)}
        path = args.out_dir / f"{name}.json"
        path.write_text(json.dumps(result, indent=2))
        summary["topos"].append({"name": name, "path": str(path)})
        print(f"  -> wrote {path}")

    (args.out_dir / "_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nbench complete; per-topology files in {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
