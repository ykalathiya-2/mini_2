# Mini 2 — Distributed scatter/gather over gRPC

> A 9-process distributed range-query system over **70 M rows** of NYC
> Yellow Taxi data, split across two laptops on a direct gigabit Ethernet
> link, talking **unary gRPC** only. Builds on `mini_1` (which did the same
> work in a single multi-threaded process).

---

## TL;DR — what the bench actually says

After sweeping clustering schemes, topologies, chunk sizes and thread
counts on real LAN hardware, the headline numbers (tree topology,
`consistent_hash` clustering, 16 worker threads, dynamic chunk size,
single client):

| Workload (`trip_distance` range) | Rows returned | Mean ms | Throughput | Owners contacted |
|---|---:|---:|---:|:--:|
| `narrow` [5.0, 5.05] | 176 243 | **66** ms | 2.68 M rows/s | 6 / 6 |
| `small` [5.0, 6.0]   | 1 942 731 | **937** ms | 2.07 M rows/s | 9 / 9 |
| `medium` [1, 3]      | 34 764 991 | **18 703** ms | 1.86 M rows/s | 9 / 9 |
| `broad` [0, 30]      | 69 973 589 | **36 785** ms | 1.90 M rows/s | 9 / 9 |

**Best sustained throughput: ~1.9 M rows/s ≈ 160 MB/s** through the single
gateway. Jain's fairness index ≥ 0.997 on every healthy run.

The interesting cross-axis findings:

| Decision | Winner | Why |
|---|---|---|
| Clustering scheme | **`consistent_hash`** for medium/broad; **`trip_distance`** for narrow | Smart routing wins when one shard truly holds the answer; vnode-style hashing wins when parallelism matters more than selectivity |
| Topology | Tree (spec) — but barely matters | Within 5 % of star/chain/grid on every workload we measured |
| Chunk size | **dynamic** (`ChunkSizer`, 64 → 4 K) | Beats every fixed size; 256-row chunks are 3× slower (RTT-dominated) |
| Worker threads | **16** for single-client, **2–8** under concurrency | Extra threads help one client; they regress badly when 4 clients contend on the same gateway |
| Owners | **9** (all nodes own a shard) | Gateway A is also a producer; lower per-node memory, better data balance |

---

## Hardware

| Host | Role | Nodes | CPU | RAM |
|---|---|---|---|---|
| MacBook (192.168.50.2, en7) | Public gateway + 4 producers | A B C D E | Apple Silicon (ARM) | 16 GB |
| Fedora laptop (192.168.50.1) | 4 producers | F G H I | x86-64, 16 cores | 16 GB |

Direct Cat-6 Ethernet, ≈ 0.1 ms RTT (no Wi-Fi, no shared switch). Client
talks only to **A** at `192.168.50.2:50051`.

---

## Software

- **9 nodes total.** A is the only public-facing gateway; the other 8
  are reachable only via overlay-routed peer links.
- **All 9 nodes own a disjoint shard** of the dataset (~7.78 M rows
  average). The gateway is also a producer.
- **Mixed C++ / Python implementations.** Six of the nine are C++ for
  speed; three (D, F, H) are Python to satisfy the spec's mixed-language
  requirement. The wire contract is identical.
- **Unary gRPC only.** No server-streaming, no async stubs. Chunked
  delivery is implemented as a pull loop on top of `(Request) → Response`
  pairs so pacing stays in our code.
- **70 M rows** from the 2017 NYC Yellow Taxi dataset, 17 typed columns
  (`int32 / int64 / double / bool / string`).

---

## Repository layout

```
mini_2/
  proto/                 mini2.proto — RPC contract (all unary)
  proto_gen/python/      generated Python stubs (C++ codegen lives in build/)
  cpp/
    server/node.cpp      C++ node daemon (gateway + intermediate + owner)
    client/client.cpp    C++ client (only talks to A)
    common/              overlay loader, CSV store, routing, scheduler, telemetry
  py/
    server/              Python node (same RPC surface as cpp/server)
    topology/            overlay-yaml generator (tree/star/chain/grid/ring/mesh/random_k)
  config/
    overlay.yaml         active overlay (multihost tree)
    topo/*.yaml          alternate topologies for the topology sweep
  bench/
    edge_cases.py        spec checklist: cancel/abandon/oversize/concurrent/unknown-rid
    topo_bench.py        legacy cluster-launching harness (depended on scripts/, see below)
    topo_compare.py      post-process bench/results/*.json into a side-by-side table
    results/             *.json per (topology, scheme, threads, chunk) cell
  benchmark_results/     latest end-to-end JSON results (see BENCHMARKS.md)
  logs/
    latest -> run-…/     symlinked to most recent run; per-node *.log + telemetry-*.jsonl
```

---

## Build

Prereqs:

```bash
# macOS
brew install grpc protobuf abseil cmake
# Fedora
sudo dnf install -y gcc gcc-c++ cmake grpc-devel protobuf-devel \
                    abseil-cpp-devel openssl-devel python3 python3-pip rsync
```

Python deps (Mac side, used by the bench harness and Python nodes):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install grpcio grpcio-tools protobuf pyyaml
```

Build C++ on each host:

```bash
# Generate Python protos (one-time)
python -m grpc_tools.protoc -I proto \
    --python_out=proto_gen/python \
    --grpc_python_out=proto_gen/python proto/mini2.proto

# Build C++ node + client
cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp -j
```

For the Fedora side, push the source over with `rsync` and build:

```bash
rsync -aH --exclude='.venv' --exclude='build' --exclude='logs' \
      --exclude='data' /Users/spartan/mini_2/ \
      yash@192.168.50.1:~/mini_2/
ssh yash@192.168.50.1 \
  "cd ~/mini_2 && cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release && cmake --build build/cpp -j"
```

---

## Run — manual launch in separate terminals

There are no orchestration scripts. Each of the 9 nodes runs as its own
process; you launch them in separate terminals so each one's log is
visible live. Then the client talks to the gateway (A) from a tenth
terminal.

**Common env vars** (all node commands below assume these are set):

```bash
export MINI2_OVERLAY=/Users/spartan/mini_2/config/overlay.yaml
export MINI2_DATA_DIR=/Users/spartan/mini_2/data/partitions
```

### Mac side — 5 terminals (A, B, C, D, E)

A, B, C, E are C++; D is Python. Open five terminals on the Mac:

```bash
# Terminal 1 — node A (gateway, C++)
/Users/spartan/mini_2/build/cpp/mini2_node --name A \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR

# Terminal 2 — node B (C++)
/Users/spartan/mini_2/build/cpp/mini2_node --name B \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR

# Terminal 3 — node C (C++)
/Users/spartan/mini_2/build/cpp/mini2_node --name C \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR

# Terminal 4 — node D (Python)
cd /Users/spartan/mini_2 && source .venv/bin/activate
python -m py.server.node --name D \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR

# Terminal 5 — node E (C++)
/Users/spartan/mini_2/build/cpp/mini2_node --name E \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR
```

### Fedora side — 4 SSH sessions (F, G, H, I)

F and H are Python; G and I are C++. Open four SSH sessions to the
Fedora box and launch one node in each:

```bash
# Terminal 6 — ssh F (Python)
ssh yash@192.168.50.1
cd ~/mini_2 && source .venv/bin/activate
export MINI2_OVERLAY=~/mini_2/config/overlay.yaml
export MINI2_DATA_DIR=~/mini_2/data/partitions
python -m py.server.node --name F \
  --overlay $MINI2_OVERLAY --data-dir $MINI2_DATA_DIR

# Terminal 7 — ssh G (C++)
ssh yash@192.168.50.1
~/mini_2/build/cpp/mini2_node --name G \
  --overlay ~/mini_2/config/overlay.yaml \
  --data-dir ~/mini_2/data/partitions

# Terminal 8 — ssh H (Python)
ssh yash@192.168.50.1
cd ~/mini_2 && source .venv/bin/activate
python -m py.server.node --name H \
  --overlay ~/mini_2/config/overlay.yaml \
  --data-dir ~/mini_2/data/partitions

# Terminal 9 — ssh I (C++)
ssh yash@192.168.50.1
~/mini_2/build/cpp/mini2_node --name I \
  --overlay ~/mini_2/config/overlay.yaml \
  --data-dir ~/mini_2/data/partitions
```

### Client — terminal 10 (on the Mac)

```bash
# Single predicate
/Users/spartan/mini_2/build/cpp/mini2_client \
  --overlay /Users/spartan/mini_2/config/overlay.yaml \
  --column trip_distance --low 5 --high 6 --print 10

# Joint (multi-predicate AND)
/Users/spartan/mini_2/build/cpp/mini2_client \
  --overlay /Users/spartan/mini_2/config/overlay.yaml \
  --column trip_distance --low 5 --high 6 \
  --column passenger_count --low 5 --high 10 --print 10

# Concurrent clients
/Users/spartan/mini_2/build/cpp/mini2_client \
  --overlay /Users/spartan/mini_2/config/overlay.yaml \
  --concurrency 8 --column passenger_count --low 5 --high 10
```

### Tear-down

Just `Ctrl-C` each node terminal. (No PID files, no reaper scripts.)

### Tuning knobs

Set these in any node's terminal before launch:

| Env var | Effect | Default |
|---|---|---|
| `MINI2_WORKERS` | gRPC threadpool size | 16 |
| `MINI2_INITIAL_ROWS` | first chunk size | 64 |
| `MINI2_MAX_ROWS` | chunk row cap (server clamp) | 4096 |
| `MINI2_TARGET_CHUNK_MS` | dynamic sizer target | 25 |

Per-node host/port can be overridden with `MINI2_HOST_<N>` /
`MINI2_PORT_<N>` env vars, so the same `overlay.yaml` works on both
loopback and the LAN.

---

## Architecture

### Roles

```
                  Gigabit Ethernet, ~0.1 ms RTT
      ┌───────────────────────────┐     ┌────────────────────────┐
      │ MacBook  192.168.50.2     │═════│ Fedora 192.168.50.1    │
      │   A* B  C  D  E           │     │   F  G  H  I           │
      │   (cpp/cpp/cpp/py/cpp)    │     │   (py/cpp/py/cpp)      │
      └───────────────────────────┘     └────────────────────────┘
                ▲
        external client (talks only to A — port 50051)

   spec tree edges: AB BC BD BE EF ED EG AH AG AI
   teams: blue = {A B D H}   yellow = {C E F G I}
```

Every node is **simultaneously** an intermediate (forwarder), a producer
(owns a shard), and — in A's case — a gateway. The role is implicit:
whoever is asked, answers.

### gRPC contract (`proto/mini2.proto`)

Two services, both **unary only**:

- `ClientGateway` (only A serves it for clients):
  `SubmitQuery(QueryRequest) → SubmitAck` and
  `FetchChunk(ChunkRequest) → ChunkResponse`.
- `PeerLink` (every node serves it on the overlay):
  `ForwardQuery`, `PullChunk`, `Heartbeat`, `CancelQuery`.

Chunked delivery is a **pull loop**: the client repeatedly calls
`FetchChunk(request_id, max_rows)` until the response sets `is_last=true`.
The gateway internally pulls from owners with the same pattern over
`PeerLink::PullChunk`. There is no server-streaming RPC anywhere.

### Smart routing

Each clustering scheme writes a `manifest.json` alongside the per-owner
shards listing the *range* of the clustering key each owner covers
(or, for `consistent_hash`, the vnode → owner map). When a query arrives,
the gateway intersects its predicate with each owner's range and
**only forwards to eligible owners**. For `trip_distance ∈ [5.0, 5.05]`:

| Scheme | Owners contacted | Owners skipped |
|---|---|---|
| `round_robin` | 9 / 9 | 0 (no metadata to skip on) |
| `trip_distance` | **1 / 9** (just owner H's bucket) | A B C D E F G I |
| `consistent_hash` | 6 / 9 (multiple vnodes hit) | typically 3 |
| `pu_location_id`, `pickup_datetime` | varies by predicate | varies |

For broad queries (`[0, 30]` mi) every scheme falls back to 9 / 9 —
there's no owner whose range is disjoint from the predicate.

### Chunked pull, with back-pressure

Producers `range_search` their local shard once and stash matched rows in
a per-request deque, capped at **8 192 rows**. If the deque is full, the
producer waits for the consumer to drain (FairScheduler arbitrates fairly
across concurrent requests). The cap exists because before it landed we
OOM-killed Fedora — see the post-mortem below.

The dynamic `ChunkSizer` watches inter-pull latency and adapts the
per-chunk row count toward a 25 ms target: ×1.5 when pulls take < 12 ms
(too small), ×0.5 when they take > 50 ms (too big). Starts at 64,
capped at 4 096 by the producer-side deque cap.

### Topology-independent forwarding

A small BFS routing table is precomputed at startup for each `(self,
target)` pair. Forwarders don't store query state; they just look up the
next hop and proxy the `ForwardQuery` / `PullChunk` envelope with the
same `request_id` and `target_owner`. This is what lets the bench swap
between tree/star/chain/grid without changing any node code.

---

## Design choices (what we picked and why)

### 9 data owners instead of 6

The original cut had A, B, E as forward-only nodes. We changed all 9 to
own a shard:

- **Better data balance** (~7.78 M rows / node vs. 11.67 M with 6).
- **Lower per-node memory** — Fedora's RAM dropped from 16 GB + 8 GB swap
  to ~1.1 GB max after this + back-pressure together.
- **A pulls cheaper** — gateway drains its own local match buffer
  in-process before any gRPC, ~7 % single-client overhead.

### Unary RPCs only

Server-streaming would have been faster (one HTTP/2 stream per query,
flow control built in). The spec rules it out, so chunked pull is
implemented manually: the consumer asks for the next `max_rows` rows;
the producer hands back at most that many; if it has more, it sets
`has_more=true` and waits to be polled again. This puts pacing entirely
in our code, which made the dynamic chunk sizer possible.

### `consistent_hash` with virtual nodes (the late addition)

After the first three schemes (`round_robin` / `trip_distance` /
`pu_location_id`) all had clear trade-offs, we added a fifth: hash
`trip_distance` quantised to 0.01-mi buckets onto **144 virtual nodes**
(16 vnodes per owner, striped). The hypothesis was *"selectivity AND
parallelism"* — a narrow query still hits 6 owners (not 9, not 1), so we
get fan-out parallelism while still skipping a few. Result: it wins on
medium/broad and loses to `trip_distance` on narrow (more on this below).

### NumPy removed from Python nodes

The spec asks "minimise third-party libraries." We replaced NumPy with
stdlib `array.array` typed columns. Same memory layout, no dependency,
but `range_search` is now an interpreter loop — ~10× slower than the
NumPy version on 9 M rows. C++ nodes remain unaffected; the Python nodes
(D, F, H) are now the bottleneck on workloads where they're on the
critical path (broad / medium).

### Direct Ethernet, not Wi-Fi

Wi-Fi adds 2–5 ms RTT and shares the medium with everything else on the
LAN. On a broad query that issues ~17 000 chunks, that's ≈ 40 s of pure
RTT overhead on top of the actual work. The direct link keeps RTT at
≈ 0.1 ms, which is why chunks below 1 K rows are still possible without
collapsing.

### Fair scheduler

`FairScheduler` (weighted round-robin) hands production credits to
concurrent requests' producer loops in round-robin order, so a slow
consumer can't starve a fast one. Measured Jain's index ≥ 0.997 for
c=2–3 and ≥ 0.99 for c=4. Switching to FIFO mode trades fairness for
~10 % lower p99 (config flag: `scheduler.mode: fifo`).

---

## Benchmark methodology

### Four workloads (predicate-on-`trip_distance`)

| Name | Range (mi) | Approx rows | Owner pattern |
|---|---|---:|---|
| `narrow` | [5.0, 5.05] | 176 K | one bucket / a few vnodes |
| `small`  | [5.0, 6.0]  | 1.94 M | 1–9 owners depending on scheme |
| `medium` | [1, 3]      | 34.8 M | always 9 |
| `broad`  | [0, 30]     | 69.97 M | always 9 |

Picked to cover the full selectivity range from "one shard answers"
(narrow) to "everything answers" (broad).

### Four sweeps (varying one axis at a time)

Not a 4-D cross product — the meaningful effect is per-axis, and a 4-D
sweep would have taken days.

1. **Scheme** — `round_robin`, `trip_distance`, `pu_location_id`,
   `pickup_datetime`, `consistent_hash` (tree, 8 threads, dynamic chunks)
2. **Topology** — tree / star / chain / grid (trip_distance, 8 threads,
   dynamic chunks; narrow + small + medium)
3. **Chunk size** — dynamic, 256, 1 024, 2 048, 4 096, 8 192 (tree,
   trip_distance, 8 threads; small + medium)
4. **Threads** — 1, 2, 4, 8, 16 (tree, trip_distance, c=1 and c=4)

Each cell is repeated 2× (some larger sweeps used `--repeats 1` for
runtime); per-cell JSON in `bench/results/`.

### Verification baked into the harness

- **`wait_ready` probe.** Before timing, the harness issues a real
  query and waits until `owners_hit == owners_eligible` (a fresh field
  on `ChunkResponse` that the gateway populates from the set of owners
  that actually responded, including empty replies). This catches the
  case where a slow Python node is still loading when timing starts —
  what used to make narrow-range benches mysteriously short by ~7 %.
- **Row-count check.** The expected total per workload (set by an
  initial reference run) must be returned by every later run. Mismatches
  are flagged in the printed table.
- **Per-node telemetry.** Each node writes JSONL events (`ready`,
  `pull_done`, …) to `logs/<run>/telemetry-<N>.jsonl`. The harness merges
  them by `t_ns` for per-impl pull latency and startup attribution.
- **Centralised logs.** Each node writes to its own `logs/<run>/<N>.log`;
  to collect them from Fedora after a run, `rsync -aH
  yash@192.168.50.1:~/mini_2/logs/<run>/ ~/mini_2/logs/<run>/`.

### What the metrics mean

- **mean / p50 / p95 / p99 / max** — per-request wall-clock latency in ms,
  measured client-side from `SubmitQuery` to `is_last=true`.
- **first_chunk_ms** — gateway-side time until the first chunk is ready.
  Useful for "time to first byte" comparisons.
- **throughput_rps / throughput_mbps** — total rows / payload bytes
  divided by the *single-client* latency. Not per-client capacity.
- **owners_hit / owners_eligible** — fraction of smart-routing-eligible
  owners that returned at least one successful response. Equals 1.0 on
  every healthy run; drops if a Python node is still warming up.
- **jains_index** — Jain's fairness index across per-request latencies
  under concurrency c. 1.0 means perfectly equal, ≥ 0.99 is "indistinguishable".

---

## Results

### Headline (tree, `consistent_hash`, t16, dynamic chunks, c=1)

This is the best-of-breed config from all four sweeps composed together:

| Workload | rows | chunks | mean ms | first-chunk ms | rows/s | MB/s | owners |
|---|---:|---:|---:|---:|---:|---:|:--:|
| narrow | 176 243 | 53 | 65.8 | n/a | 2 680 000 | 226 | 6 / 6 |
| small  | 1 942 731 | 488 | 937 | n/a | 2 073 000 | 175 | 9 / 9 |
| medium | 34 764 991 | 8 502 | 18 703 | n/a | 1 859 000 | 157 | 9 / 9 |
| broad  | 69 973 589 | 17 097 | 36 785 | n/a | 1 902 000 | 161 | 9 / 9 |

### Scheme sweep (tree, t8, dynamic chunks, c=1, repeats=2)

Same query, different "which row goes where" decision:

| Scheme | narrow ms | small ms | medium ms | comments |
|---|---:|---:|---:|---|
| `round_robin` | 4 190¹ | **3 609** | **50 510** | parallelism wins on medium/small; narrow loses to a startup race |
| `trip_distance` | **858** | 6 017 | 55 219 | 1 owner answers narrow — but the parallelism loss costs everything else |
| `consistent_hash` | 8 927² | 3 897 | 53 855 | best balance — close to round_robin on big queries, doesn't collapse on narrow |
| `pu_location_id` | 955 (single_zone) | 8 769 (small_range) | 41 850 (wide_range) | different workload shapes; not directly comparable |
| `pickup_datetime` | — | — | — | severe imbalance: month-of-year buckets put 19 M rows on G and 0 on F |

¹ Jain's = 0.67 on `round_robin` narrow — one repeat caught D mid-warmup;
the harness's `wait_ready` (the new `owners_hit` version) prevents this
post-fix, but the old result is preserved for honesty.

² On t8 with the original `owners_hit` semantics, narrow ran while D was
still loading. With the fix and t16, narrow drops to **66 ms** (table
above) — a 135× speedup for this corner.

**Takeaways:**

1. For very narrow queries that fit in one shard, `trip_distance`
   clustering wins — one fat chunk from one owner.
2. For everything else, parallelism beats selectivity by 5–10 %.
3. `consistent_hash` with vnodes is the best **default**: never the
   loser on any workload, often within 5 % of the per-workload winner.

### Topology sweep (`trip_distance`, t8, dynamic chunks, c=1)

| Topology | narrow ms | small ms | medium ms |
|---|---:|---:|---:|
| tree (spec)  | 858 | 6 017 | 55 219 |
| star    | 710 | 6 910 | 63 456 |
| chain   | 713 | 6 936 | 63 019 |
| grid    | 707 | 6 809 | 66 139 |

**Topology barely matters** under smart routing — narrow and small are
within 5 %, medium within 10 %. Star wins narrow (every owner is 1 hop
from A); tree wins medium (less contention on A as the merge point).

### Chunk-size sweep (tree, `trip_distance`, t8, c=1)

| chunk-rows | small (1.94 M) | medium (34.8 M) | comment |
|---|---:|---:|---|
| **dynamic** | **6 017** | **55 219** | adaptive sizing wins |
| 256   | 15 125 (2.5×) | 210 615 (3.8×) | 135 K chunks for medium → 200 s of pure RTT |
| 1 024 | 9 125 | 91 645 | RTT still dominates |
| 2 048 | 8 465 | 74 791 | approaching dynamic |
| 4 096 | 6 710 | 67 185 | matches dynamic at the deque cap |
| 8 192 | 6 671 | 62 302 | producer cap; can't grow past 8 K rows |

**Why 256 is so bad:** 35 M rows ÷ 256 rows/chunk = 137 K chunks. On a
LAN with 1.5 ms RTT per chunk, that's ≈ 200 s of pure network overhead
— exactly what we measured. Chunk size has to respect RTT.

### Thread sweep (tree, `trip_distance`, dynamic chunks)

| threads | small c=1 | small c=4 | medium c=1 | comment |
|---|---:|---:|---:|---|
| 1  | 6 768 | 7 032 | 61 969 | enough for single-client |
| 2  | 7 133 | 7 010 | 63 667 | flat |
| 4  | 6 889 | 18 563 | 62 437 | **first regression under c=4** |
| 8  | 6 017 | 24 701 | 55 219 | best single-client, worse under c=4 |
| 16 | 5 925 | 20 130 | **32 813** | best single-client medium; still bad under c=4 |

**Surprising finding:** more threads make the **c=4** case **worse**.
The gateway's smart-routing fan-out is serialised, so adding gRPC
threads on the producer side only adds context-switch overhead without
unblocking the actual bottleneck. The right answer:

- **t16 for single-client workloads** (huge win on medium: 55 s → 33 s).
- **t2–t8 if you expect many concurrent clients.**

### Startup time per node (multihost, latest run)

| Node | Impl | Host | Load ms | Sort ms | Range-table ms | Total ms | Rows |
|---|---|---|---:|---:|---:|---:|---:|
| A | cpp | Mac    | 2 401 | 0     | 38    | 2 523 | 7 299 081 |
| B | cpp | Mac    | 2 760 | 0     | 32    | 2 807 | 8 759 742 |
| C | cpp | Mac    | 2 069 | 0     | 28    | 2 110 | 6 763 252 |
| **D** | **py**  | Mac    | 18 281 | 17 717 | 3 207 | **39 239** | 9 337 246 |
| E | cpp | Mac    | 1 856 | 0     | 38    | 1 899 | 8 937 454 |
| **F** | **py**  | Fedora | 12 412 | 14 530 | 2 022 | **28 974** | 5 390 665 |
| G | cpp | Fedora | 387   | 0     | 4     | **391** | 6 520 538 |
| **H** | **py**  | Fedora | 22 776 | 22 855 | 4 145 | **49 789** | 9 808 431 |
| I | cpp | Fedora | 397   | 0     | 4     | **401** | 7 183 591 |

**Python nodes are 50–100× slower at startup than C++ nodes** on the same
host — the cost of CPython parsing/converting CSV without NumPy. The
gateway's `wait_ready` waits up to 90 s for the slowest one, so timing
runs never start while a node is still loading.

### Peak RAM per node (gateway-side ps sampling, latest run)

| Node | Impl | Max RSS (MB) |
|---|---|---:|
| A | cpp | 790 |
| B | cpp | 938 |
| C | cpp | 725 |
| D | py  | 1 130 |
| E | cpp | 953 |

Down from the **8.6 GB** of pre-back-pressure days (see post-mortem
below). Memory is flat across the run; chunk size stays bounded
regardless of result-set size.

---

## Issues we hit (and how we fixed them)

### Producer OOM under broad queries

Without a buffer cap, a slow consumer would let producers stuff entire
11-M-row matched sets into RAM. On a Fedora node with 16 GB + 8 GB swap,
this made the box unresponsive. **Fix:** per-request matched-deque
capped at 8 192 rows (~1.2 MB). Producers block on the cap; the
FairScheduler keeps a slow consumer from starving others.

### `wait_ready` false-positive — 7 % missing rows on broad

The bench harness's readiness probe used `rows >= 100 000` as its gate.
Python node D takes ~37 s to load; while D was still loading, the other
5 owners returned ≥ 176 K rows for the probe range, so the harness fired
the timing run early and **D's rows were missing**. We saw a ~7 % row
shortfall on `broad` (65 M vs 70 M) that nothing in the gateway code
explained.

**Fix:** proto now carries `owners_hit` / `owners_eligible` on every
`ChunkResponse`. The harness waits until they're equal **and** non-zero.
Two further fixes needed to make this honest:

1. `owners_hit` was initially "owners with rows > 0", which broke the
   check for narrow probes that genuinely return zero rows on some
   owners. Replaced with `owners_responded`, an `unordered_set<owner>`
   populated by the prefetcher on any successful `PullChunk` reply
   (including empty ones).
2. The gateway's self-drain (when A is an eligible owner) doesn't go
   through `PullChunk`, so A had to insert itself into
   `owners_responded` at `SubmitQuery` time.

### Forward retry too short during Python warmup

Python data owners loading 8–10 M rows in stdlib-only Python need 30–50 s.
The gateway's initial 3-attempt forward retry was too short, so we'd
dead-mark a slow owner and lose its rows. **Fix:** 20 retries × 500 ms
= 10 s of patience for `UNAVAILABLE` and `DEADLINE_EXCEEDED`. Combined
with `wait_ready`, this eliminates the warmup race entirely.

### Stale Fedora copy of `py/server/node.py`

After we added `tele.emit("ready", …)` to the Python node, F and H (the
Fedora-side Python nodes) silently kept writing zero-byte telemetry
files for two days. D (same code, on the Mac) had telemetry, which threw
us off. Cause: the Fedora copy hadn't been re-deployed.
**Fix:** the deploy `rsync` (now manual — see "Build" above) syncs the
full source tree, not just the C++ side.

### `pickup_datetime` clustering doesn't work on 70 M rows

Month-of-year buckets are **severely imbalanced** on the first 70 M rows
of the 2017 CSV (which is mostly Jan–Apr): owner G ended up with 19 M
rows, F with 0. Kept the scheme in the codebase for completeness but
excluded it from the scheme-sweep tables. Lesson: clustering keys need a
uniform distribution along their hash dimension, not just along the
predicate dimension.

### Default 4 worker threads regress under concurrency

We initially defaulted to 8 threads everywhere. Under c=4, this nearly
doubled latency vs t1. **Diagnosis:** `range_search` on the producer is
serial under the deque-cap mutex; adding gRPC threads just adds
context-switch contention. **Fix:** the README and overlay defaults now
recommend t16 for single-client and t2–t8 under concurrency.

---

## Edge-case suite (`bench/edge_cases.py`)

Covers the spec checklist beyond pure throughput numbers:

- Client cancels mid-stream → `CancelQuery` ACKed, server releases rid.
- Client abandons (drops the connection without `CancelQuery`) →
  server reclaims the rid via the idle-timer path.
- Oversize result set returns chunked without unbounded memory growth
  (verifies the 8 K-row producer cap).
- Concurrent c=3 requests have Jain's ≥ 0.95 (verifies the fair
  scheduler).
- `PullChunk` with an unknown `request_id` returns `NOT_FOUND`, not a
  crash.
- `SubmitQuery` against a non-existent node name is rejected with
  `INVALID_ARGUMENT`.
- Empty range (`[5.05, 5.05]`) returns 0 rows cleanly.

Outputs `bench/edge_cases.json` with a pass/fail per case.

---

## Reproducing

Full multihost end-to-end:

```bash
# One-time on Fedora
sudo dnf install -y gcc gcc-c++ cmake grpc-devel protobuf-devel \
                    abseil-cpp-devel openssl-devel python3 python3-pip rsync

# From the Mac — push source and build remotely
rsync -aH --exclude='.venv' --exclude='build' --exclude='logs' \
      --exclude='data' /Users/spartan/mini_2/ \
      yash@192.168.50.1:~/mini_2/
ssh yash@192.168.50.1 \
  "cd ~/mini_2 && cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release && \
   cmake --build build/cpp -j"

# Generate partitions from the 70M-row source CSV (one-time, on Mac)
python3 -c "from py.topology.split_taxi_csv import *" # see py/ for splitter
# (or use the original mini_1 splitter; per-scheme dirs live under data/)
```

Once partitions exist and binaries are built on both hosts, launch the
9 nodes manually in separate terminals (see **Run** above). Then run
the client from a tenth terminal.

For end-to-end benchmarks driven from the client:

```bash
# Single-predicate sweep, JSON output
for col in trip_distance passenger_count fare_amount total_amount; do
  ./build/cpp/mini2_client --overlay config/overlay.yaml --quiet --json \
    --column $col --low 0 --high 100
done

# Concurrency sweep
for c in 1 4 8 15; do
  ./build/cpp/mini2_client --overlay config/overlay.yaml --quiet --json \
    --concurrency $c --column passenger_count --low 5 --high 10
done
```

The latest run's JSON is in `benchmark_results/`; the prose report is
[BENCHMARKS.md](BENCHMARKS.md).

> ⚠️ `bench/topo_bench.py` is **legacy** — it called `scripts/start_multihost.sh`
> to spawn the cluster, but the scripts folder has been removed. The
> harness's analysis helpers (`topo_compare.py`, telemetry parsing) still
> work against any JSON files you produce with the manual client above.
> `bench/edge_cases.py` is client-only and still works.

---

## Spec checklist

- [x] **9 processes**, mixed C++/Python implementations, identity from
      argv/env, config-driven (`overlay.yaml`).
- [x] **Only A talks to clients.** Non-gateway nodes reject
      `SubmitQuery` / `FetchChunk` with `FAILED_PRECONDITION`.
- [x] **Scatter/gather via intermediates.** BFS routing tables; no
      intermediate stores large state.
- [x] **Request context matching.** `request_id` + `target_owner` on
      every overlay envelope; intermediates match on the routing table.
- [x] **Chunked replies on unary RPCs.** Pull-based pacing in our code;
      no server-streaming.
- [x] **Dynamic chunk sizing.** `ChunkSizer` adapts toward
      `target_chunk_ms=25`.
- [x] **Fair scheduling.** `FairScheduler` (weighted round-robin);
      measured Jain's ≥ 0.997 for c=2–3.
- [x] **Unary only.** Every RPC is `(Request) → Response`.
- [x] **No shared memory.** Cross-process flow is exclusively gRPC.
- [x] **Typed data.** `TaxiRow` uses `int32 / int64 / double / bool /
      string` per the real schema.
- [x] **Edge cases handled.** Cancel, abandon, oversize, empty range,
      unknown rid, non-gateway client API, concurrent no-starve.

---

## What we'd do next

- **Parallel forward fan-out at the gateway.** Today the gateway's
  smart-routing loop is sequential; making it concurrent would unlock
  the c=4 regression and probably halve `broad` time.
- **Skip the manifest range-table for `round_robin` and
  `consistent_hash`.** Both schemes always need every owner for medium /
  broad anyway, so the range-table cost (~3 s on Python nodes) is pure
  overhead.
- **Re-enable NumPy on the Python side as an optional fast path.** A
  10× speedup for D / F / H on range_search would make `broad` finish
  in ~20 s rather than ~37 s.
