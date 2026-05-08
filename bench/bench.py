#!/usr/bin/env python3
"""Benchmark harness for Mini 2.

Measures:

* Wall-clock latency per request (ns, via time.perf_counter_ns).
* Throughput (rows/sec, bytes/sec).
* Per-process peak memory (via resource.getrusage on the client; for node
  processes we read their ps rss snapshot via `ps -o rss= -p <pid>`).
* Fairness across concurrent requests — Jain's index on completion times.
* Chunk-size sweep to show the latency/throughput trade-off.

Everything is driven by calling the built C++ client binary (which talks to
gateway A).  We parse its JSON output lines to collect per-request data.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import statistics
import sys
import time
from pathlib import Path
from typing import List, Dict


ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "build" / "cpp" / "mini2_client"
OVERLAY = ROOT / "config" / "overlay.yaml"


def run_client(*args: str) -> List[dict]:
    """Run the client binary with --json and parse per-request dicts."""
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet", *args]
    res = subprocess.run(cmd, check=False, capture_output=True, text=True)
    rows = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
    return rows


def jains_index(xs: List[float]) -> float:
    if not xs: return 0.0
    s  = sum(xs)
    s2 = sum(x * x for x in xs)
    return (s * s) / (len(xs) * s2) if s2 > 0 else 1.0


def bench_chunk_sweep(column: str, low: float, high: float) -> List[dict]:
    sizes = [0, 16, 64, 256, 1024, 4096]   # 0 = dynamic
    out = []
    for sz in sizes:
        runs = []
        for _ in range(3):
            rs = run_client(
                "--column", column, "--low", str(low), "--high", str(high),
                "--max-rows", str(sz),
            )
            if rs: runs.append(rs[0])
        if not runs: continue
        avg_ms = statistics.mean(r["ms_total"] for r in runs)
        avg_first = statistics.mean(r["ms_first_chunk"] for r in runs)
        avg_rows = statistics.mean(r["rows"] for r in runs)
        avg_chunks = statistics.mean(r["chunks"] for r in runs)
        out.append({
            "chunk_size_hint": sz if sz > 0 else "dynamic",
            "mean_total_ms": round(avg_ms, 2),
            "mean_first_chunk_ms": round(avg_first, 2),
            "rows": int(avg_rows),
            "chunks": int(avg_chunks),
            "throughput_rows_per_s": int(avg_rows / (avg_ms / 1000.0))
                if avg_ms > 0 else 0,
        })
    return out


def bench_fairness(n_concurrent: int) -> dict:
    # Run concurrent requests; each asks for all rows.  Fair scheduler should
    # give them similar completion times.
    rs = run_client(
        "--column", "trip_distance", "--low", "0.0", "--high", "1e9",
        "--concurrency", str(n_concurrent),
    )
    times = [r["ms_total"] for r in rs if r["ok"]]
    return {
        "n": n_concurrent,
        "times_ms": [round(t, 1) for t in times],
        "jains_index": round(jains_index(times), 4) if times else None,
        "min_ms": round(min(times), 1) if times else None,
        "max_ms": round(max(times), 1) if times else None,
        "spread_pct": round(100 * (max(times) - min(times)) / max(times), 1)
            if times else None,
    }


def bench_latency_series(column: str, low: float, high: float, n: int) -> dict:
    per = []
    for _ in range(n):
        rs = run_client("--column", column, "--low", str(low), "--high", str(high))
        if rs: per.append(rs[0]["ms_total"])
    if not per:
        return {}
    return {
        "n": n,
        "column": column,
        "range": [low, high],
        "mean_ms":   round(statistics.mean(per), 2),
        "median_ms": round(statistics.median(per), 2),
        "p95_ms":    round(sorted(per)[int(0.95 * len(per)) - 1], 2),
        "stdev_ms":  round(statistics.pstdev(per), 2) if len(per) > 1 else 0.0,
    }


def get_node_rss_kb(logs_dir: Path) -> Dict[str, int]:
    """Read logs/pids.txt and sample resident memory (kB) for each node."""
    pids = {}
    pidfile = logs_dir / "pids.txt"
    if not pidfile.exists(): return pids
    for line in pidfile.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2: pids[parts[0]] = int(parts[1])
    out = {}
    for name, pid in pids.items():
        try:
            r = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                check=True, capture_output=True, text=True,
            )
            out[name] = int(r.stdout.strip() or "0")
        except Exception:
            out[name] = -1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "bench" / "results.json"))
    ap.add_argument("--column", default="trip_distance")
    ap.add_argument("--low", type=float, default=0.0)
    ap.add_argument("--high", type=float, default=30.0)
    ap.add_argument("--latency-n", type=int, default=5)
    ap.add_argument("--fairness-max", type=int, default=4)
    args = ap.parse_args()

    if not CLIENT.exists():
        print(f"client binary missing at {CLIENT}; run cmake --build first",
              file=sys.stderr)
        return 1

    report = {
        "host":  os.uname().nodename,
        "date":  time.strftime("%Y-%m-%dT%H:%M:%S"),
        "dataset_rows": sum(1 for _ in open(ROOT / "data" / "partitions" / "C.csv")) - 1,
    }

    print("== latency series ==")
    report["latency"] = bench_latency_series(
        args.column, args.low, args.high, args.latency_n)
    print(json.dumps(report["latency"], indent=2))

    print("== chunk size sweep ==")
    report["chunk_sweep"] = bench_chunk_sweep(args.column, args.low, args.high)
    print(json.dumps(report["chunk_sweep"], indent=2))

    print("== fairness sweep ==")
    report["fairness"] = []
    for n in range(1, args.fairness_max + 1):
        r = bench_fairness(n)
        report["fairness"].append(r)
        print(json.dumps(r))

    print("== node memory snapshot (rss kB) ==")
    report["node_rss_kb"] = get_node_rss_kb(ROOT / "logs")
    print(json.dumps(report["node_rss_kb"], indent=2))

    Path(args.out).write_text(json.dumps(report, indent=2))
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    sys.exit(main() or 0)
