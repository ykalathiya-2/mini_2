#!/usr/bin/env python3
"""Topology / clustering / threads / chunk-size bench harness for Mini 2.

For each (topology, clustering scheme, thread count, chunk hint) tuple,
brings up the cluster, drives a workload through the C++ client, samples
node RAM during the run, and aggregates per-request and per-node metrics.

Modes:
    --mode local        all nodes on this host (no LAN, fast smoke)
    --mode multihost    nodes split across the Mac+Arch LAN per scripts/

Sweeps (each independent unless `--single-shot`):
    --topos       comma list from {tree,star,chain,grid,ring,binary_tree,...}
    --schemes     comma list from {round_robin,trip_distance,pu_location_id,
                                   pickup_datetime}
    --threads     comma list of integer worker counts (default "16")
    --chunks      comma list of max-rows hints; 0 = dynamic (default "0")

Per run output:
    bench/results/<topology>__<scheme>__t<threads>__c<chunk>.json
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "build" / "cpp" / "mini2_client"
TOPO_DIR = ROOT / "config" / "topo"
RESULTS_DIR = ROOT / "bench" / "results"

ALL_TOPOS = ["star", "tree", "binary_tree", "chain", "ring", "grid", "mesh", "random_k"]
ALL_SCHEMES = ["round_robin", "trip_distance", "consistent_hash", "pu_location_id", "pickup_datetime"]

# Workloads that exercise different selectivities. The right column for each
# is set up to align with the scheme being tested — by default trip_distance,
# but the runner will override the column when the scheme is location/time
# based so we get meaningful smart-routing wins on those schemes too.
WORKLOADS = [
    {"name": "narrow",  "column": "trip_distance", "low": 5.0,  "high": 5.05, "expect_pct": 0.1},
    {"name": "small",   "column": "trip_distance", "low": 5.0,  "high": 6.0,  "expect_pct": 2.7},
    {"name": "medium",  "column": "trip_distance", "low": 1.0,  "high": 3.0,  "expect_pct": 30.0},
    {"name": "broad",   "column": "trip_distance", "low": 0.0,  "high": 30.0, "expect_pct": 95.0},
]
# Concurrency levels swept for each (topo, scheme, threads, chunk) cell.
CONCURRENCY_LEVELS = [1, 2, 4]


def jains_index(xs: List[float]) -> float:
    if not xs: return 0.0
    s  = sum(xs)
    s2 = sum(x * x for x in xs)
    return (s * s) / (len(xs) * s2) if s2 > 0 else 1.0


def percentiles(xs: List[float]) -> dict:
    if not xs:
        return {"min": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0, "std": 0}
    s = sorted(xs); n = len(s)
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


def run_client_json(overlay: Path, *args: str, timeout: float = 600.0) -> List[dict]:
    cmd = [str(CLIENT), "--overlay", str(overlay), "--json", "--quiet", *args]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"[bench] client TIMEOUT after {timeout}s\n")
        return []
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


# --- RAM sampling --------------------------------------------------------

def sample_node_rss(pids: Dict[str, int], remote_user_host: Optional[str] = None
                   ) -> Dict[str, int]:
    """Sample resident memory (kB) for every (node -> pid) once."""
    local = {n: p for n, p in pids.items() if not n.startswith("R:")}
    out: Dict[str, int] = {}
    if local:
        # ps in one shot for all local pids (faster than per-pid).
        try:
            args = ["ps", "-o", "pid=,rss="] + ["-p"] + [str(p) for p in local.values()]
            r = subprocess.run(args, capture_output=True, text=True, timeout=2.0)
            pid_to_rss = {}
            for line in r.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        pid_to_rss[int(parts[0])] = int(parts[1])
                    except ValueError:
                        pass
            for n, p in local.items():
                out[n] = pid_to_rss.get(p, -1)
        except Exception:
            for n in local: out[n] = -1
    return out


class RamSampler(threading.Thread):
    """Samples RSS of node pids in `pidfile` every `interval_s` seconds while
    `running` is true. Final result is per-node max+mean."""
    def __init__(self, pidfile: Path, interval_s: float = 0.5,
                 remote_pidfile: Optional[Path] = None,
                 remote_user_host: Optional[str] = None):
        super().__init__(daemon=True)
        self.pidfile = pidfile
        self.remote_pidfile = remote_pidfile
        self.remote_user_host = remote_user_host
        self.interval_s = interval_s
        self.running = True
        self.samples: Dict[str, List[int]] = {}

    def _read_pids(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if self.pidfile.exists():
            for line in self.pidfile.read_text().splitlines():
                parts = line.split()
                if len(parts) == 2:
                    try: out[parts[0]] = int(parts[1])
                    except ValueError: pass
        return out

    def _read_remote_pids(self) -> Dict[str, int]:
        if not self.remote_pidfile or not self.remote_user_host: return {}
        try:
            r = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", self.remote_user_host,
                 f"cat {self.remote_pidfile}"],
                capture_output=True, text=True, timeout=3.0)
            out = {}
            for line in r.stdout.splitlines():
                parts = line.split()
                if len(parts) == 2:
                    try: out[parts[0]] = int(parts[1])
                    except ValueError: pass
            return out
        except Exception:
            return {}

    def run(self):
        while self.running:
            local_pids  = self._read_pids()
            remote_pids = self._read_remote_pids()
            local_rss = sample_node_rss(local_pids)
            for n, kb in local_rss.items():
                if kb >= 0:
                    self.samples.setdefault(n, []).append(kb)
            # Remote: one ssh call to get all rss values at once.
            if remote_pids:
                pids_csv = ",".join(str(p) for p in remote_pids.values())
                try:
                    r = subprocess.run(
                        ["ssh", "-o", "BatchMode=yes", self.remote_user_host,
                         f"ps -o pid=,rss= -p {pids_csv}"],
                        capture_output=True, text=True, timeout=3.0)
                    pid_to_rss = {}
                    for line in r.stdout.splitlines():
                        parts = line.split()
                        if len(parts) >= 2:
                            try: pid_to_rss[int(parts[0])] = int(parts[1])
                            except ValueError: pass
                    for n, p in remote_pids.items():
                        kb = pid_to_rss.get(p, -1)
                        if kb >= 0:
                            self.samples.setdefault(n, []).append(kb)
                except Exception:
                    pass
            time.sleep(self.interval_s)

    def summary(self) -> dict:
        out = {}
        for n, vals in self.samples.items():
            if vals:
                out[n] = {
                    "max_kb":    max(vals),
                    "mean_kb":   int(sum(vals) / len(vals)),
                    "samples":   len(vals),
                }
        return out


# --- cluster lifecycle ---------------------------------------------------

def cluster_up(mode: str, overlay: Path, scheme: str,
               threads: int, chunk_initial: Optional[int]
               ) -> Path:
    """Bring up the cluster. Honors the requested clustering scheme by
    setting MINI2_DATA_DIR to the matching partitions_<scheme>/ subdir,
    threads via MINI2_WORKERS, chunk initial via MINI2_INITIAL_ROWS."""
    env = os.environ.copy()
    env["MINI2_OVERLAY"] = str(overlay)
    env["MINI2_DATA_DIR"] = str(ROOT / "data" / f"partitions_{scheme}")
    env["MINI2_WORKERS"]  = str(threads)
    if chunk_initial is not None:
        env["MINI2_INITIAL_ROWS"] = str(chunk_initial)
    # In local mode the topo yamls carry the multi-host IPs (192.168.50.x);
    # override every node's host to 127.0.0.1 via the overlay loader's
    # MINI2_HOST_<NAME> hook so all 9 processes can talk via loopback.
    if mode == "local":
        for n in "ABCDEFGHI":
            env[f"MINI2_HOST_{n}"] = "127.0.0.1"
    script = "start_multihost.sh" if mode == "multihost" else "start_all_local.sh"
    res = subprocess.run(
        [str(ROOT / "scripts" / script)],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=60,
    )
    if res.returncode != 0:
        sys.stderr.write(res.stderr)
        raise RuntimeError(f"{script} failed rc={res.returncode}")
    run_dir: Optional[Path] = None
    for line in res.stdout.splitlines():
        if "run dir:" in line and "remote" not in line.lower():
            run_dir = Path(line.split("run dir:")[1].strip())
            break
        if "local  run dir:" in line:
            run_dir = Path(line.split("local  run dir:")[1].strip())
            break
    return run_dir or (ROOT / "logs" / "latest")


def cluster_down(mode: str) -> None:
    script = "stop_multihost.sh" if mode == "multihost" else "stop_all.sh"
    subprocess.run([str(ROOT / "scripts" / script)],
                   cwd=ROOT, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, timeout=30)


def wait_ready(overlay: Path, max_wait_s: float = 30.0) -> bool:
    """Wait until A is responsive AND a small but non-empty query returns
    rows from all-eligible owners. The empty-result probe was insufficient
    on multihost: it would return ok=True from A even while remote nodes
    were still loading their CSVs, leading to forward-during-warmup
    failures that silently dead-marked owners.

    We use a real query whose total row count tells us all owners replied:
    trip_distance ∈ [5.0, 5.05] over 70M rows yields ~176k rows, single
    digit % per owner — if the count is ≥ 100k we know all 9 producers
    contributed."""
    deadline = time.time() + max_wait_s
    # Initial fixed wait: gives Fedora time to load 4× ~500MB CSVs.
    time.sleep(6.0)
    while time.time() < deadline:
        rs = run_client_json(overlay, "--column", "trip_distance",
                             "--low", "5.0", "--high", "5.05",
                             timeout=15.0)
        if rs and rs[0].get("ok") and rs[0].get("rows", 0) >= 100_000:
            return True
        time.sleep(1.0)
    return False


# --- workload runner -----------------------------------------------------

def workload_for_scheme(scheme: str) -> List[dict]:
    """Return workloads that actually exercise the scheme's clustering
    column. trip_distance ranges work for the trip_distance scheme; for
    others we override the column so smart-routing has a chance to fire."""
    if scheme == "trip_distance":
        return WORKLOADS
    if scheme == "pu_location_id":
        # NYC zone IDs run 1..265; pick a few non-overlapping ranges.
        return [
            {"name": "single_zone", "column": "pu_location_id", "low": 41, "high": 41},
            {"name": "small_range", "column": "pu_location_id", "low": 1,  "high": 50},
            {"name": "wide_range",  "column": "pu_location_id", "low": 1,  "high": 150},
            {"name": "all_zones",   "column": "pu_location_id", "low": 0,  "high": 999},
        ]
    if scheme == "pickup_datetime":
        # 2017 epoch ranges for narrow/medium/broad.
        # Jan 1 2017 = 1483228800; Mar 1 = 1488326400; Jul 1 = 1498867200; Dec 31 = 1514678400
        return [
            {"name": "one_month",   "column": "pickup_datetime", "low": 1483228800, "high": 1485907199},  # Jan
            {"name": "two_months",  "column": "pickup_datetime", "low": 1483228800, "high": 1488326399},
            {"name": "half_year",   "column": "pickup_datetime", "low": 1483228800, "high": 1498867199},
            {"name": "all_year",    "column": "pickup_datetime", "low": 1483228800, "high": 1514678399},
        ]
    return WORKLOADS  # round_robin uses the trip_distance baseline workloads


def run_workload(overlay: Path, w: dict, concurrency: int, max_rows_hint: int = 0,
                 repeats: int = 2) -> dict:
    samples: List[float] = []
    raw_runs: List[List[dict]] = []
    for _ in range(repeats):
        args = [
            "--column", w["column"],
            "--low",  str(w["low"]),
            "--high", str(w["high"]),
            "--concurrency", str(concurrency),
        ]
        if max_rows_hint > 0:
            args += ["--max-rows", str(max_rows_hint)]
        rs = run_client_json(overlay, *args)
        for r in rs:
            if r.get("ok"):
                samples.append(r["ms_total"])
        raw_runs.append(rs)

    flat = [r for run in raw_runs for r in run if r.get("ok")]
    if not flat:
        return {"concurrency": concurrency, "repeats": repeats, "samples": 0,
                "error": "no successful runs"}

    # Vnode-fanout stats: per-query hit/eligible from the gateway. Older
    # client builds don't emit these fields, default to 0.
    hits     = [r.get("vnodes_hit", 0)      for r in flat]
    eligible = [r.get("vnodes_eligible", 0) for r in flat]
    return {
        "concurrency":    concurrency,
        "repeats":        repeats,
        "samples":        len(samples),
        "rows_mean":      int(statistics.mean(r["rows"] for r in flat)),
        "chunks_mean":    round(statistics.mean(r["chunks"] for r in flat), 1),
        "bytes_mean":     int(statistics.mean(r["bytes"] for r in flat)),
        "bytes_per_chunk_avg": int(statistics.mean(r["bytes_avg_per_chunk"] for r in flat)),
        "bytes_per_chunk_min": int(min(r["bytes_min_per_chunk"] for r in flat)),
        "bytes_per_chunk_max": int(max(r["bytes_max_per_chunk"] for r in flat)),
        "throughput_rps": int(statistics.mean(r["rows"] for r in flat)
                              / max(0.001, statistics.mean(samples) / 1000)),
        "throughput_mbps": round(statistics.mean(r["bytes"] for r in flat) / 1e6
                                 / max(0.001, statistics.mean(samples) / 1000), 1),
        "ms":             percentiles(samples),
        "first_chunk_ms": percentiles([r["ms_first_chunk"] for r in flat]),
        "jains_index":    round(jains_index(samples), 4),
        "vnodes_hit_mean":      round(statistics.mean(hits), 2)     if hits else 0,
        "vnodes_eligible_mean": round(statistics.mean(eligible), 2) if eligible else 0,
    }


# --- top-level driver ----------------------------------------------------

def bench_one(name: str, mode: str, scheme: str, threads: int,
              chunk_initial: int, concurrency_levels: List[int],
              repeats: int, workload_filter: Optional[set] = None,
              remote_user_host: Optional[str] = None) -> dict:
    overlay = TOPO_DIR / f"{name}.yaml"
    if not overlay.exists():
        raise FileNotFoundError(f"missing topology overlay: {overlay}")

    print(f"\n=== {name}  scheme={scheme}  threads={threads}  chunk={chunk_initial} ===")
    cluster_down(mode)
    time.sleep(1)

    chunk_arg = chunk_initial if chunk_initial > 0 else None
    run_dir = cluster_up(mode, overlay, scheme, threads, chunk_arg)
    if not wait_ready(overlay):
        cluster_down(mode)
        return {"topology": name, "scheme": scheme, "threads": threads,
                "chunk_initial": chunk_initial, "error": "cluster not ready"}

    pidfile = run_dir / "pids.txt"
    remote_pidfile = None
    if mode == "multihost" and run_dir.parent.name.startswith("logs") is False:
        # remote run dir mirrors local; harness emits both.
        remote_pidfile = Path(str(run_dir).replace(str(ROOT) + "/", "")) / "pids.txt"

    sampler = RamSampler(pidfile, interval_s=0.5,
                         remote_pidfile=remote_pidfile,
                         remote_user_host=remote_user_host)
    sampler.start()

    out = {
        "topology":    name, "mode": mode, "scheme": scheme,
        "threads":     threads, "chunk_initial": chunk_initial,
        "overlay":     str(overlay.relative_to(ROOT)),
        "run_dir":     str(run_dir),
        "started":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "workloads":   [],
    }
    for w in workload_for_scheme(scheme):
        if workload_filter is not None and w["name"] not in workload_filter:
            continue
        for c in concurrency_levels:
            print(f"  {w['name']:>11} c={c}  ", end="", flush=True)
            r = run_workload(overlay, w, c,
                             max_rows_hint=chunk_initial if chunk_initial > 0 else 0,
                             repeats=repeats)
            r["workload"] = w["name"]
            r["range"]    = [w["low"], w["high"]]
            out["workloads"].append(r)
            if "ms" in r:
                ms = r["ms"]
                print(f"rows={r['rows_mean']:>7}  chunks={r['chunks_mean']:>5.0f}  "
                      f"mean={ms['mean']:>7.1f}ms  p95={ms['p95']:>7.1f}ms  "
                      f"thr={r['throughput_rps']/1000:>5.0f}kr/s  "
                      f"vnodes={r.get('vnodes_hit_mean', 0):.1f}/"
                      f"{r.get('vnodes_eligible_mean', 0):.1f}  "
                      f"jains={r['jains_index']}")
            else:
                print(f"FAILED: {r.get('error', '?')}")

    sampler.running = False
    sampler.join(timeout=2.0)
    out["node_ram"] = sampler.summary()
    cluster_down(mode)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode",    choices=["local", "multihost"], default="local")
    ap.add_argument("--topos",   default="tree")
    ap.add_argument("--schemes", default="trip_distance")
    ap.add_argument("--threads", default="16")
    ap.add_argument("--chunks",  default="0")
    ap.add_argument("--concurrency", default="1,4",
                    help="comma list of concurrency levels (default 1,4)")
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--workloads", default="all",
                    help="comma list of workload names (e.g. narrow,medium,broad) or 'all'")
    ap.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    ap.add_argument("--remote-host", default=os.environ.get("MINI2_REMOTE_USER", "yash")
                    + "@" + os.environ.get("MINI2_REMOTE_HOST", "192.168.50.1"))
    args = ap.parse_args()

    topos   = ALL_TOPOS    if args.topos   == "all" else args.topos.split(",")
    schemes = ALL_SCHEMES  if args.schemes == "all" else args.schemes.split(",")
    threads = [int(x) for x in args.threads.split(",")]
    chunks  = [int(x) for x in args.chunks.split(",")]
    concurrency_levels = [int(x) for x in args.concurrency.split(",")]
    workload_filter = None if args.workloads == "all" else set(args.workloads.split(","))

    args.out_dir.mkdir(parents=True, exist_ok=True)

    if not CLIENT.exists():
        sys.stderr.write(f"client missing: {CLIENT}\n  build first: cmake --build build/cpp -j\n")
        return 2

    summary = {
        "mode":     args.mode,
        "started":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "matrix":   {"topos": topos, "schemes": schemes, "threads": threads, "chunks": chunks},
        "results":  [],
    }
    for name in topos:
        for scheme in schemes:
            for t in threads:
                for c in chunks:
                    try:
                        result = bench_one(name, args.mode, scheme, t, c,
                                           concurrency_levels=concurrency_levels,
                                           repeats=args.repeats,
                                           workload_filter=workload_filter,
                                           remote_user_host=args.remote_host
                                           if args.mode == "multihost" else None)
                    except Exception as e:
                        result = {"topology": name, "scheme": scheme,
                                  "threads": t, "chunk_initial": c,
                                  "error": str(e)}
                    fname = f"{name}__{scheme}__t{t}__c{c}.json"
                    p = args.out_dir / fname
                    p.write_text(json.dumps(result, indent=2))
                    summary["results"].append({"file": fname,
                                               "topology": name,
                                               "scheme": scheme,
                                               "threads": t,
                                               "chunk_initial": c})
                    print(f"  -> {p}")

    (args.out_dir / "_summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nbench complete; per-cell files in {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
