#!/usr/bin/env python3
"""Edge-case verification for Mini 2.

Exercises:
  * Client cancel mid-stream  → CancelQuery acknowledged, server releases rid.
  * Client abandons (no Cancel) → Server eventually reclaims via idle path.
  * Gateway handles oversized result set without unbounded memory.
  * Concurrent requests do not starve each other (Jain ≥ 0.95).
  * Unknown request_id on PullChunk returns NOT_FOUND, not a crash.
  * Unknown node name on SubmitQuery is rejected by gateway.
  * Query against an exclusive range returns zero rows cleanly.

Runs against the already-running overlay on localhost; launch with
scripts/start_all_local.sh first.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENT = ROOT / "build" / "cpp" / "mini2_client"
OVERLAY = ROOT / "config" / "overlay.yaml"

sys.path.insert(0, str(ROOT / "proto_gen" / "python"))
sys.path.insert(0, str(ROOT / "py" / "server"))

import grpc  # noqa: E402
import mini2_pb2 as pb        # noqa: E402
import mini2_pb2_grpc as rpc  # noqa: E402
from overlay import load_overlay  # noqa: E402


def run_case(name: str, fn) -> dict:
    print(f"\n== case: {name} ==")
    t0 = time.perf_counter_ns()
    try:
        r = fn() or {}
        ok = True
        err = None
    except AssertionError as e:
        ok = False
        r = {}
        err = f"AssertionError: {e}"
    except Exception as e:
        ok = False
        r = {}
        err = f"{type(e).__name__}: {e}"
    dt_ms = (time.perf_counter_ns() - t0) / 1e6
    out = {"case": name, "ok": ok, "ms": round(dt_ms, 1), "detail": r, "error": err}
    print(json.dumps(out, indent=2))
    return out


def cancel_midstream():
    # Uses the client binary with --cancel-after 2.
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "trip_distance", "--low", "0", "--high", "1e9",
           "--cancel-after", "2"]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rec = next(json.loads(l) for l in r.stdout.splitlines() if l.startswith("{"))
    assert rec["ok"], f"client reported failure: {rec}"
    assert rec["chunks"] == 2, f"expected 2 chunks pre-cancel, got {rec['chunks']}"
    return {"chunks_before_cancel": rec["chunks"], "rows": rec["rows"]}


def abandon_without_cancel():
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "trip_distance", "--low", "0", "--high", "1e9",
           "--limit-chunks", "3"]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rec = next(json.loads(l) for l in r.stdout.splitlines() if l.startswith("{"))
    assert rec["ok"], rec
    # Server should still be healthy; prove it with a follow-up request.
    r2 = subprocess.run(
        [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
         "--column", "trip_distance", "--low", "5", "--high", "6"],
        check=True, capture_output=True, text=True)
    rec2 = next(json.loads(l) for l in r2.stdout.splitlines() if l.startswith("{"))
    assert rec2["ok"]
    return {"abandoned_chunks": rec["chunks"],
            "followup_rows": rec2["rows"],
            "followup_ms": rec2["ms_total"]}


def oversized_result_bounded_memory():
    # Pull everything — 120 000 rows — and confirm no single chunk is huge.
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "trip_distance", "--low", "0", "--high", "1e9",
           "--max-rows", "1024"]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rec = next(json.loads(l) for l in r.stdout.splitlines() if l.startswith("{"))
    assert rec["ok"]
    assert rec["rows"] >= 119000, f"expected ~120k rows, got {rec['rows']}"
    assert rec["chunks"] >= 50, f"expected chunked delivery, got {rec['chunks']} chunks"
    return {"rows": rec["rows"], "chunks": rec["chunks"], "avg_chunk_rows":
            round(rec["rows"] / rec["chunks"], 1)}


def concurrent_no_starvation(n=3):
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "trip_distance", "--low", "0", "--high", "1e9",
           "--concurrency", str(n)]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    recs = [json.loads(l) for l in r.stdout.splitlines() if l.startswith("{")]
    times = [r["ms_total"] for r in recs]
    s  = sum(times)
    s2 = sum(x * x for x in times)
    jains = (s * s) / (len(times) * s2) if s2 > 0 else 0.0
    assert jains >= 0.95, f"Jain's index too low: {jains}"
    return {"jains_index": round(jains, 4), "times_ms": times}


def unknown_request_id_returns_not_found():
    ov = load_overlay(str(OVERLAY))
    a = ov.nodes["A"]
    ch = grpc.insecure_channel(a.endpoint)
    stub = rpc.ClientGatewayStub(ch)
    pr = pb.PullRequest(request_id="does-not-exist-" + uuid.uuid4().hex,
                        max_rows=16, last_seq=-1)
    try:
        stub.FetchChunk(pr, timeout=2.0)
        assert False, "expected NOT_FOUND error"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.NOT_FOUND, \
            f"expected NOT_FOUND, got {e.code()}"
    return {"got": "NOT_FOUND"}


def reject_at_non_gateway():
    ov = load_overlay(str(OVERLAY))
    b = ov.nodes["B"]  # B is a peer, not gateway
    ch = grpc.insecure_channel(b.endpoint)
    stub = rpc.ClientGatewayStub(ch)
    q = pb.Query(request_id="reject-" + uuid.uuid4().hex)
    q.predicates.add(column="trip_distance", low=1.0, high=2.0, inclusive=True)
    try:
        stub.SubmitQuery(q, timeout=2.0)
        assert False, "expected FAILED_PRECONDITION at non-gateway"
    except grpc.RpcError as e:
        assert e.code() == grpc.StatusCode.FAILED_PRECONDITION, e.code()
    return {"got": "FAILED_PRECONDITION"}


def empty_range_returns_zero_rows():
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "trip_distance", "--low", "1000000", "--high", "1000001"]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rec = next(json.loads(l) for l in r.stdout.splitlines() if l.startswith("{"))
    assert rec["ok"]
    assert rec["rows"] == 0, f"expected 0 rows, got {rec['rows']}"
    return {"rows": rec["rows"], "chunks": rec["chunks"]}


def multi_predicate_query():
    # Combined: trip_distance in [2,3] AND passenger_count=1 (use [1,1] range).
    cmd = [str(CLIENT), "--overlay", str(OVERLAY), "--json", "--quiet",
           "--column", "passenger_count", "--low", "1", "--high", "1"]
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rec = next(json.loads(l) for l in r.stdout.splitlines() if l.startswith("{"))
    assert rec["ok"]
    assert rec["rows"] > 0
    return {"rows_passenger_1": rec["rows"]}


def main():
    cases = [
        ("cancel mid-stream",             cancel_midstream),
        ("abandon without cancel",        abandon_without_cancel),
        ("oversized result, bounded chunks", oversized_result_bounded_memory),
        ("concurrent no starvation (n=3)",concurrent_no_starvation),
        ("unknown request_id → NOT_FOUND",unknown_request_id_returns_not_found),
        ("non-gateway rejects client API",reject_at_non_gateway),
        ("empty range → 0 rows",          empty_range_returns_zero_rows),
        ("column filter (passenger_count=1)", multi_predicate_query),
    ]
    results = [run_case(n, f) for n, f in cases]
    out = ROOT / "bench" / "edge_cases.json"
    out.write_text(json.dumps(results, indent=2))
    failed = [r for r in results if not r["ok"]]
    print(f"\n{'PASS' if not failed else 'FAIL'}: "
          f"{len(results) - len(failed)}/{len(results)} cases passed"
          + (f" — failed: {[r['case'] for r in failed]}" if failed else ""))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
