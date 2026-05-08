"""Python gRPC server for a Mini 2 node.

Implements both ClientGateway (if role=gateway) and PeerLink (always). The
same binary runs for any node — identity comes from argv/env, never baked in.

Design choices (on purpose, documented so reviewers see the tradeoffs):

* **Unary only.** No server-streaming, no asyncio gRPC stream, no async stubs.
  Chunking is implemented by the caller issuing successive `PullChunk` RPCs.
* **Pull-based flow.** Producers buffer bounded queues; consumers pace pulls.
  This gives natural back-pressure without us hand-rolling credits.
* **Per-request fairness.** A weighted round-robin scheduler hands out
  "pull tokens" so one huge query can't starve another.  We rotate across
  active request ids each time we decide a chunk size.
* **No shared memory for responses.** Each process owns its own queues; the
  only cross-process path is gRPC.
"""
from __future__ import annotations

import argparse
import collections
import logging
import os
import signal
import sys
import threading
import time
from concurrent import futures
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import grpc
import numpy as np

# Allow running directly: py/server/node.py
_THIS = Path(__file__).resolve()
sys.path.insert(0, str(_THIS.parents[2]))  # mini_2/
sys.path.insert(0, str(_THIS.parents[2] / "proto_gen" / "python"))

import mini2_pb2 as pb        # noqa: E402
import mini2_pb2_grpc as rpc  # noqa: E402

from py.server.overlay import Overlay, load_overlay  # noqa: E402


log = logging.getLogger("mini2.node")


# ---------------------------------------------------------------------------
# Data access: NumPy columnar partition.
#
# Mirrors the C++ phase-3 PartitionStore: one typed array per column, no
# per-row allocation. range_search() runs as a single vectorised mask over
# the columns — the heavy lifting happens in NumPy's C kernels, not the
# Python interpreter, so 3.3M rows scan in tens of milliseconds.
# ---------------------------------------------------------------------------

# (column, dtype) in the order they appear on disk. Must match split_taxi_csv.py.
_COL_SPEC = [
    ("vendor_id",             np.int32),
    ("pickup_datetime",       np.int64),
    ("dropoff_datetime",      np.int64),
    ("passenger_count",       np.int32),
    ("trip_distance",         np.float64),
    ("ratecode_id",           np.int32),
    ("store_and_fwd_flag",    np.uint8),
    ("pu_location_id",        np.int32),
    ("do_location_id",        np.int32),
    ("payment_type",          np.int32),
    ("fare_amount",           np.float64),
    ("extra",                 np.float64),
    ("mta_tax",               np.float64),
    ("tip_amount",            np.float64),
    ("tolls_amount",          np.float64),
    ("improvement_surcharge", np.float64),
    ("total_amount",          np.float64),
]
_FLAG_COL_INDEX = next(i for i, (n, _) in enumerate(_COL_SPEC) if n == "store_and_fwd_flag")


def _yn_to_int(s) -> int:
    # np.loadtxt may pass bytes (older numpy) or str (newer); accept both.
    if isinstance(s, bytes):
        s = s.decode()
    return 1 if s and s[0] in ("Y", "y", "1") else 0


@dataclass
class Partition:
    cols: dict = field(default_factory=dict)   # name -> 1-D np.ndarray
    n: int = 0

    @staticmethod
    def load(csv_path: Path) -> "Partition":
        dtype = np.dtype([(name, dt) for name, dt in _COL_SPEC])
        # Single C-level pass — much faster than csv.DictReader at 3M+ rows.
        arr = np.loadtxt(
            csv_path,
            delimiter=",",
            skiprows=1,
            dtype=dtype,
            converters={_FLAG_COL_INDEX: _yn_to_int},
            encoding="utf-8",
        )
        # Materialise contiguous per-column arrays so range_search is
        # cache-friendly (structured-array access strides over all 17 fields).
        cols = {name: np.ascontiguousarray(arr[name]) for name, _ in _COL_SPEC}
        return Partition(cols=cols, n=int(arr.shape[0]) if arr.ndim else 1)

    def range_search(self, predicates) -> np.ndarray:
        """Return matched indices (np.int64) for the AND of all predicates."""
        if self.n == 0:
            return np.empty(0, dtype=np.int64)
        mask = np.ones(self.n, dtype=bool)
        for p in predicates:
            col = self.cols.get(p.column)
            if col is None:
                return np.empty(0, dtype=np.int64)
            if p.inclusive:
                mask &= (col >= p.low) & (col <= p.high)
            else:
                mask &= (col > p.low) & (col < p.high)
        return np.flatnonzero(mask)

    def row_to_proto(self, idx: int) -> pb.TaxiRow:
        c = self.cols
        r = pb.TaxiRow()
        r.vendor_id             = int(c["vendor_id"][idx])
        r.pickup_datetime       = int(c["pickup_datetime"][idx])
        r.dropoff_datetime      = int(c["dropoff_datetime"][idx])
        r.passenger_count       = int(c["passenger_count"][idx])
        r.trip_distance         = float(c["trip_distance"][idx])
        r.ratecode_id           = int(c["ratecode_id"][idx])
        r.pu_location_id        = int(c["pu_location_id"][idx])
        r.do_location_id        = int(c["do_location_id"][idx])
        r.store_and_fwd_flag    = bool(c["store_and_fwd_flag"][idx])
        r.payment_type          = int(c["payment_type"][idx])
        r.fare_amount           = float(c["fare_amount"][idx])
        r.extra                 = float(c["extra"][idx])
        r.mta_tax               = float(c["mta_tax"][idx])
        r.tip_amount            = float(c["tip_amount"][idx])
        r.tolls_amount          = float(c["tolls_amount"][idx])
        r.improvement_surcharge = float(c["improvement_surcharge"][idx])
        r.total_amount          = float(c["total_amount"][idx])
        return r


# ---------------------------------------------------------------------------
# Per-request state. Producer buffers matched row ids; consumers pull chunks.
# ---------------------------------------------------------------------------

@dataclass
class RequestState:
    request_id: str
    query: pb.Query
    matched: collections.deque  # deque of TaxiRow protos already filled
    # Precomputed match indices into the local partition (data-owners only).
    # Set once when the request is registered; the producer iterates this
    # vector instead of rescanning the partition each tick.
    matched_indices: Optional[np.ndarray] = None
    matched_cursor: int = 0
    total_matched: int = 0
    last_delivered_seq: int = -1
    done_producing: bool = False
    cancelled: bool = False
    weight: int = 1             # for weighted RR scheduler
    created_ns: int = 0
    # Per-peer tracking (gateway only) — maps peer → pulled_rows
    peer_rows: Dict[str, int] = field(default_factory=dict)
    # Gateway-only: aggregated rows waiting for client pulls.
    client_buffer: collections.deque = field(default_factory=collections.deque)
    client_buffer_last_seq: int = -1
    client_producers_done: Dict[str, bool] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


# ---------------------------------------------------------------------------
# The server.  One class handles both the gateway side (facing client)
# and the peer side (facing other nodes).
# ---------------------------------------------------------------------------

class Mini2Node(rpc.ClientGatewayServicer, rpc.PeerLinkServicer):

    def __init__(self, name: str, overlay: Overlay, data_dir: Path):
        self.name = name
        self.overlay = overlay
        self.me = overlay.nodes[name]
        self.data_dir = data_dir

        # Peer stubs (lazy).
        self._peer_channels: Dict[str, grpc.Channel] = {}
        self._peer_stubs: Dict[str, rpc.PeerLinkStub] = {}
        self._peer_lock = threading.Lock()

        # Active requests on this node (keyed by request_id).
        self.requests: Dict[str, RequestState] = {}
        self.req_lock = threading.Lock()

        # Weighted-RR cursor — list of request_ids currently being produced.
        self._rr_order: List[str] = []
        self._rr_cursor = 0
        self._rr_lock = threading.Lock()

        # Local partition (only for data owners).
        self.partition: Optional[Partition] = None
        if name in overlay.data_owners:
            csv_path = data_dir / f"{name}.csv"
            if csv_path.exists():
                self.partition = Partition.load(csv_path)
                log.info("[%s] loaded %d partition rows from %s",
                         name, self.partition.n, csv_path)
            else:
                log.warning("[%s] no partition CSV at %s — running with empty data",
                            name, csv_path)

        # Chunking defaults.
        self.chunk_initial = int(overlay.chunking.get("initial_rows", 64))
        self.chunk_min     = int(overlay.chunking.get("min_rows", 16))
        self.chunk_max     = int(overlay.chunking.get("max_rows", 4096))
        self.chunk_target_ms = int(overlay.chunking.get("target_chunk_ms", 25))
        self._chunk_cur: Dict[str, int] = {}
        self._last_pull_ns: Dict[str, int] = {}

        # Scheduler mode.
        self.sched_mode = overlay.scheduler.get("mode", "weighted_round_robin")
        self.max_concurrent = int(overlay.scheduler.get("max_concurrent_requests", 32))

        # Background producer — only active on data-owner nodes.
        self._producer_stop = threading.Event()
        self._producer = threading.Thread(
            target=self._produce_loop, name=f"producer-{name}", daemon=True)
        self._producer.start()

    # ----- peer plumbing --------------------------------------------------

    def _stub(self, peer: str) -> rpc.PeerLinkStub:
        with self._peer_lock:
            if peer in self._peer_stubs:
                return self._peer_stubs[peer]
            ep = self.overlay.nodes[peer].endpoint
            ch = grpc.insecure_channel(
                ep,
                options=[
                    ("grpc.max_send_message_length", 64 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ],
            )
            stub = rpc.PeerLinkStub(ch)
            self._peer_channels[peer] = ch
            self._peer_stubs[peer] = stub
            return stub

    def _close_peers(self):
        with self._peer_lock:
            for ch in self._peer_channels.values():
                ch.close()

    # ----- request lifecycle ---------------------------------------------

    def _register(self, req_id: str, query: pb.Query, *, weight: int = 1) -> RequestState:
        with self.req_lock:
            st = self.requests.get(req_id)
            if st is None:
                st = RequestState(
                    request_id=req_id,
                    query=query,
                    matched=collections.deque(),
                    weight=weight,
                    created_ns=time.perf_counter_ns(),
                )
                # Data owners precompute the match set once via the vectorised
                # NumPy mask. Gateway nodes have self.partition is None and skip.
                if self.partition is not None and self.partition.n > 0:
                    t0 = time.perf_counter_ns()
                    st.matched_indices = self.partition.range_search(query.predicates)
                    t1 = time.perf_counter_ns()
                    log.info("[%s] rid=%s range_search matched=%d of %d in %.2f ms",
                             self.name, req_id,
                             int(st.matched_indices.size),
                             self.partition.n,
                             (t1 - t0) / 1e6)
                    if st.matched_indices.size == 0:
                        st.done_producing = True
                self.requests[req_id] = st
                with self._rr_lock:
                    self._rr_order.append(req_id)
            return st

    def _drop(self, req_id: str) -> Optional[RequestState]:
        with self.req_lock:
            st = self.requests.pop(req_id, None)
        with self._rr_lock:
            if req_id in self._rr_order:
                self._rr_order.remove(req_id)
        return st

    # ----- ClientGateway: external-facing (only A implements for clients) --

    def SubmitQuery(self, request: pb.Query, context) -> pb.QueryAck:
        if self.me.role != "gateway":
            ctx = context
            ctx.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            ctx.set_details("SubmitQuery only accepted at gateway node")
            return pb.QueryAck()

        rid = request.request_id
        if not rid:
            return pb.QueryAck(request_id=rid, accepted=False,
                               reject_reason="missing request_id")

        log.info("[%s] SubmitQuery request_id=%s predicates=%d",
                 self.name, rid, len(request.predicates))

        # Register state (gateway-side: aggregates from all peers).
        st = self._register(rid, request)
        owners = self.overlay.data_owners
        for o in owners:
            st.client_producers_done[o] = False

        # Fan out: one ForwardedQuery per data owner, tagged with target_owner.
        for owner in owners:
            if owner == self.name:
                continue
            hop = self.overlay.next_hop(self.name, owner)
            fwd = pb.ForwardedQuery(
                query=request, forwarder=self.name,
                reply_to=self.name, target_owner=owner, ttl=8,
            )
            try:
                self._stub(hop).ForwardQuery(fwd, timeout=5.0)
            except grpc.RpcError as e:
                log.warning("[%s] forward to %s via %s failed: %s",
                            self.name, owner, hop, e.code())
                with st.lock:
                    st.client_producers_done[owner] = True

        return pb.QueryAck(request_id=rid, accepted=True, estimated_rows=-1)

    def FetchChunk(self, request: pb.PullRequest, context) -> pb.Chunk:
        # Gateway aggregates: pulls from peers on demand, buffers, returns.
        if self.me.role != "gateway":
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("FetchChunk only at gateway")
            return pb.Chunk()
        rid = request.request_id
        with self.req_lock:
            st = self.requests.get(rid)
        if st is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"unknown request_id {rid}")
            return pb.Chunk()

        # Pull from every owner that hasn't finished, weighted-fair.
        max_rows = self._decide_chunk_size(rid, request.max_rows)
        deadline_ns = time.perf_counter_ns() + 200_000_000  # 200 ms wall budget

        owners = list(self.overlay.data_owners)
        rotated = owners[(int(time.time() * 1000) % len(owners)):] + \
                  owners[:(int(time.time() * 1000) % len(owners))]

        for owner in rotated:
            if st.cancelled:
                break
            if len(st.client_buffer) >= max_rows:
                break
            if st.client_producers_done.get(owner, False):
                continue
            if time.perf_counter_ns() > deadline_ns:
                break
            hop = self.overlay.next_hop(self.name, owner)
            sub = pb.PullRequest(request_id=rid, max_rows=max_rows,
                                 last_seq=-1, target_owner=owner)
            try:
                chunk = self._stub(hop).PullChunk(sub, timeout=2.0)
            except grpc.RpcError as e:
                log.warning("[%s] PullChunk→%s failed: %s", self.name, owner, e.code())
                with st.lock:
                    st.client_producers_done[owner] = True
                continue
            for row in chunk.rows:
                st.client_buffer.append(row)
                st.total_matched += 1
            if chunk.is_last:
                with st.lock:
                    st.client_producers_done[owner] = True

        # Emit the assembled chunk back to client.
        seq = st.client_buffer_last_seq + 1
        out_rows = []
        while st.client_buffer and len(out_rows) < max_rows:
            out_rows.append(st.client_buffer.popleft())
        st.client_buffer_last_seq = seq

        all_done = all(st.client_producers_done.get(o, False)
                       for o in self.overlay.data_owners if o != self.name)
        is_last = all_done and not st.client_buffer and not out_rows

        if is_last:
            log.info("[%s] request %s done; delivered ~%d rows total",
                     self.name, rid, st.total_matched)
            self._drop(rid)

        return pb.Chunk(
            request_id=rid, seq=seq, is_last=is_last,
            rows=out_rows, producer=self.name,
            backlog=len(st.client_buffer),
            produced_at_ns=time.perf_counter_ns(),
        )

    def CancelQuery(self, request: pb.CancelRequest, context) -> pb.CancelResponse:
        rid = request.request_id
        # Tell all peers we know about to cancel too.
        with self.req_lock:
            st = self.requests.get(rid)
        if st is None:
            return pb.CancelResponse(acknowledged=False, rows_dropped=0)

        st.cancelled = True
        dropped = 0
        with st.lock:
            dropped += len(st.client_buffer)
            st.client_buffer.clear()
            dropped += len(st.matched)
            st.matched.clear()

        # Fan out cancel along overlay.
        if self.me.role == "gateway":
            for owner in self.overlay.data_owners:
                hop = self.overlay.next_hop(self.name, owner)
                try:
                    self._stub(hop).CancelQuery(
                        pb.CancelRequest(request_id=rid, reason=request.reason),
                        timeout=1.0,
                    )
                except grpc.RpcError:
                    pass

        self._drop(rid)
        return pb.CancelResponse(acknowledged=True, rows_dropped=dropped)

    def Ping(self, request: pb.Heartbeat, context) -> pb.HeartbeatAck:
        return pb.HeartbeatAck(
            to_node=request.from_node,
            recv_at_ns=time.perf_counter_ns(),
            healthy=True,
        )

    # ----- PeerLink: node-to-node ----------------------------------------

    def ForwardQuery(self, request: pb.ForwardedQuery, context) -> pb.QueryAck:
        q = request.query
        rid = q.request_id
        if request.ttl <= 0:
            return pb.QueryAck(request_id=rid, accepted=False,
                               reject_reason="TTL expired")
        target = request.target_owner
        if target == self.name:
            self._register(rid, q)
            log.info("[%s] accepted forwarded query %s (target=%s)",
                     self.name, rid, target)
            return pb.QueryAck(request_id=rid, accepted=True,
                               estimated_rows=-1)
        # Intermediate: relay along precomputed next hop.
        try:
            nh = self.overlay.next_hop(self.name, target)
        except KeyError:
            nh = None
        if not nh or nh == self.name:
            return pb.QueryAck(request_id=rid, accepted=False,
                               reject_reason=f"no route to {target}")
        fwd = pb.ForwardedQuery(
            query=q, forwarder=self.name, reply_to=request.reply_to,
            target_owner=target, ttl=request.ttl - 1,
        )
        try:
            return self._stub(nh).ForwardQuery(fwd, timeout=3.0)
        except grpc.RpcError as e:
            return pb.QueryAck(request_id=rid, accepted=False,
                               reject_reason=f"relay failed: {e.code()}")

    def PullChunk(self, request: pb.PullRequest, context) -> pb.Chunk:
        rid = request.request_id
        target = request.target_owner
        # Relay to target owner if set and not me.
        if target and target != self.name:
            try:
                nh = self.overlay.next_hop(self.name, target)
            except KeyError:
                nh = None
            if not nh or nh == self.name:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"no route to owner {target}")
                return pb.Chunk()
            try:
                return self._stub(nh).PullChunk(request, timeout=2.0)
            except grpc.RpcError as e:
                context.set_code(e.code())
                context.set_details(e.details())
                return pb.Chunk()

        with self.req_lock:
            st = self.requests.get(rid)
        if st is None or self.partition is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"no local data for {rid}")
            return pb.Chunk()

        # Data-owner path: serve from local matched buffer.
        max_rows = self._decide_chunk_size(rid, request.max_rows)
        seq = st.last_delivered_seq + 1
        out = []
        with st.lock:
            while st.matched and len(out) < max_rows:
                out.append(st.matched.popleft())
        st.last_delivered_seq = seq

        is_last = st.done_producing and not st.matched and not out
        if is_last:
            self._drop(rid)

        return pb.Chunk(
            request_id=rid, seq=seq, is_last=is_last,
            rows=out, producer=self.name,
            backlog=len(st.matched) if not is_last else 0,
            produced_at_ns=time.perf_counter_ns(),
        )

    def PushChunk(self, request: pb.Chunk, context) -> pb.PushChunkAck:
        # Not used in pure pull model; kept for completeness.
        return pb.PushChunkAck(accepted=True, next_seq=request.seq + 1)

    # ----- dynamic chunk sizing ------------------------------------------

    def _decide_chunk_size(self, req_id: str, hint: int) -> int:
        if hint and hint > 0:
            return max(self.chunk_min, min(self.chunk_max, hint))
        cur = self._chunk_cur.get(req_id, self.chunk_initial)
        last = self._last_pull_ns.get(req_id)
        now = time.perf_counter_ns()
        if last is not None:
            dt_ms = (now - last) / 1e6
            # If the client is pulling fast (dt_ms < target), grow; else shrink.
            if dt_ms < self.chunk_target_ms * 0.5:
                cur = min(self.chunk_max, int(cur * 1.5) + 1)
            elif dt_ms > self.chunk_target_ms * 2.0:
                cur = max(self.chunk_min, cur // 2)
        self._last_pull_ns[req_id] = now
        self._chunk_cur[req_id] = cur
        return cur

    # ----- background producer: matches rows into st.matched -------------

    def _produce_loop(self):
        while not self._producer_stop.is_set():
            did_work = False
            with self._rr_lock:
                order = list(self._rr_order)
            for rid in order:
                with self.req_lock:
                    st = self.requests.get(rid)
                if st is None or self.partition is None:
                    continue
                if st.cancelled or st.done_producing:
                    continue
                if st.matched_indices is None:
                    continue
                # Iterate the precomputed match list. Each tick produces up
                # to weight * 256 rows so concurrent requests share fairly.
                remaining = min(self.chunk_max, 256 * max(1, st.weight))
                idxs = st.matched_indices
                i = st.matched_cursor
                end = min(idxs.size, i + remaining)
                produced_this_tick = 0
                while i < end:
                    if st.cancelled:
                        break
                    r = self.partition.row_to_proto(int(idxs[i]))
                    with st.lock:
                        st.matched.append(r)
                    i += 1
                    produced_this_tick += 1
                st.matched_cursor = i
                if i >= idxs.size:
                    st.done_producing = True
                did_work |= (produced_this_tick > 0)
            if not did_work:
                time.sleep(0.005)

    # ----- lifecycle -----------------------------------------------------

    def stop(self):
        self._producer_stop.set()
        self._close_peers()


def serve(name: str, overlay_path: Optional[str], data_dir: Optional[str],
          max_workers: int = 16):
    logging.basicConfig(
        level=os.environ.get("MINI2_LOG", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    overlay = load_overlay(overlay_path)
    if name not in overlay.nodes:
        raise SystemExit(f"unknown node {name!r}")
    me = overlay.nodes[name]
    if me.impl != "python":
        log.warning("[%s] overlay declares impl=%s but Python server is running",
                    name, me.impl)

    data_path = Path(data_dir) if data_dir else \
        Path(__file__).resolve().parents[2] / "data" / "partitions"

    node = Mini2Node(name, overlay, data_path)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=max_workers),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    )
    rpc.add_ClientGatewayServicer_to_server(node, server)
    rpc.add_PeerLinkServicer_to_server(node, server)

    bind = f"{me.host}:{me.port}"
    # Servers bind to 0.0.0.0 on the advertised port.
    listen_bind = f"0.0.0.0:{me.port}"
    server.add_insecure_port(listen_bind)

    log.info("[%s] Python node listening on %s (advertised %s), role=%s team=%s",
             name, listen_bind, bind, me.role, me.team)
    server.start()

    stop_event = threading.Event()

    def _sigterm(_sig, _frm):
        log.info("[%s] shutting down", name)
        stop_event.set()
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    try:
        while not stop_event.is_set():
            stop_event.wait(1.0)
    finally:
        node.stop()
        server.stop(grace=2.0).wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True,
                    help="logical node name (e.g. D)")
    ap.add_argument("--overlay", default=None)
    ap.add_argument("--data-dir", default=None)
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    serve(args.name, args.overlay, args.data_dir, args.workers)


if __name__ == "__main__":
    main()
