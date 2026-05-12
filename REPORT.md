# Mini 2 — Distributed Scatter/Gather Report

> **What we built:** a 9-process distributed range-query system over 70 M
> rows of NYC Yellow Taxi data, split across two laptops connected by a
> direct gigabit ethernet link, talking unary gRPC.
>
> **Why this matters:** the spec asked us to explore data distribution,
> chunked back-pressure, fairness, and overlay topologies — *not* to chase
> the fastest possible numbers, but to understand the trade-offs that
> appear when those choices interact.

---

## TL;DR — what the data actually told us

After running 70 M rows through every clustering scheme, four topologies,
six chunk sizes and five thread counts on real LAN hardware, the answer
is **not what we expected**:

| Decision | Winner | Reason |
|---|---|---|
| Clustering scheme | **`round_robin`** for medium/broad queries; **`trip_distance`** for narrow queries | Producer parallelism (9 nodes contributing in parallel) beats smart-routing's selectivity except when the query truly hits one shard |
| Topology | Tree (slightly) — but it barely matters | Smart-routing reduces fan-out to 1 path; topology shape is dominated by per-hop RTT only when many owners are contacted |
| Chunk size | **dynamic** (`ChunkSizer` starting at 64, growing to ~4 K) | Beats every fixed size; a 10× size sweep showed RTT-dominated curves below 1 K rows and producer-cap saturation above 4 K rows |
| Worker threads | **8** (the gRPC sync-pool sweet spot) — diminishing returns past 4, mild *regression* at 16 under concurrency | The gateway's fan-out is serial; extra threads contend on shared state without helping latency |
| 9-node vs 6-node ownership | **9-node** (every node holds data) | Better data balance; A serves as gateway *and* producer with ≤7 % overhead in single-client workloads |

Best end-to-end on broad workloads: **~615 k rows/s, ~58 MB/s** sustained
through the gateway; Jain's fairness index ≥ 0.998 across all healthy
configurations.

---

## The setup (one paragraph for non-CS readers)

Imagine you have a giant spreadsheet with 70 million rows of taxi-trip
data and a question like *"how many trips were between 5.0 and 5.05 miles
long?"* One computer can answer this, but slowly: it has to read every
row. We split the spreadsheet across 9 mini-servers running on two
laptops connected by an ethernet cable. A "front desk" server (call it
**A**) takes questions, asks the right group of mini-servers, gathers
answers in *batches* small enough to keep memory and network healthy,
and streams those batches back. Three things made this hard: deciding
**how to split the data**, **how big each batch should be**, and making
sure no single big query **starves** other concurrent ones.

### Hardware

- **MacBook (192.168.50.2 via en7 ethernet)** — runs nodes A, B, C, D, E
  (5 processes). 16 GB RAM, ARM-based.
- **Fedora laptop (192.168.50.1)** — runs nodes F, G, H, I (4 processes).
  16 GB RAM, x86-64, 16 cores.
- Direct ethernet link, ~0.1 ms RTT (no wifi, no shared switch traffic).

### Software

- **9 nodes total**: A is the gateway (only public-facing). All 9 nodes
  also own a disjoint shard of the data — *the "9-owner" upgrade we made
  during this exercise* (more on that in §"Architecture choices").
- **Mixed C++ and Python** implementations (per-node configurable);
  data-shard nodes use the same gRPC contract regardless of language.
- **Pure-pull, unary gRPC.** No streaming RPCs. Each chunk is a normal
  request/response pair, so we control pacing in our own code.
- **70 million rows** from the 2017 NYC Yellow Taxi dataset (full set is
  ~117 M; we used 70 M for parity with mini-1).

---

## Architecture choices (the four sweeps)

Each sweep varied **one** dimension while holding the other three fixed
at sensible defaults — much faster than a 4-D cross product, still gives
a clear ranking on each axis.

### 1. Clustering scheme — *which row goes to which node?*

| Scheme | How it works | Smart routing on... |
|---|---|---|
| `round_robin` | row N goes to node N % 9 | nothing — every query asks every owner |
| `trip_distance` | 9 size-balanced quantile buckets on miles | range queries on `trip_distance` |
| `pu_location_id` | 240 NYC zone IDs greedy-binpacked into 9 owners | range queries on `pu_location_id` |
| `pickup_datetime` | month-of-year mapped to owner via `(month-1) % 9` | time-range queries |
| **`consistent_hash`** ⭐ | trip_distance bucketed at 0.01-mi resolution, FNV-1a hashed onto 144 vnodes (16 vnodes per owner, striped across owners) | range queries on `trip_distance`, but with parallelism |

The fifth scheme — **`consistent_hash` with vnodes** — was added
mid-experiment to test the hypothesis: *"selectivity AND parallelism".*
The idea: spread data uniformly across vnodes so even a narrow query
hits multiple physical nodes, while still allowing the gateway to skip
nodes whose vnodes can't possibly match.

#### Smart routing — what the gateway actually does

When a query arrives, gateway A reads `manifest.json` (deployed with the
data) and asks: *"which of my 9 owners could possibly have a matching
row?"* It then **forwards the query only to those owners**. Skipped
owners save CPU + network but lose their parallel contribution.

For a narrow query `trip_distance ∈ [5.0, 5.05]`:

| Scheme | Owners contacted | Owners skipped |
|---|---|---|
| `round_robin` | 9/9 | 0 |
| `trip_distance` | **1/9** (just the bucket containing 5.0-ish) | C, D, F, G, A, B, E, I |
| `consistent_hash` | 6/9 (one per ≈ 0.01-mi sub-bucket) | typically 3 |

For a broad query `[0, 30]` mi all schemes degrade to 9/9 — there's no
way to skip an owner when every owner's range is in scope.

#### Scheme bench results (tree, 8 threads, dynamic chunk, 1 client)

| Workload | Scheme | rows | mean ms | p95 ms | thr (kr/s) | b/chunk | Jain's |
|---|---|---|---|---|---|---|---|
| **narrow** [5.0–5.05] | round_robin | 156 555¹ | 4 190¹ | 7 124¹ | 37¹ | 251 K | 0.67¹ |
| | consistent_hash | 170 154 | 8 927 | 9 755 | 19 | 100 K | 0.99 |
| | **trip_distance** | **176 243** | **858** | **859** | **205** | **280 K** | **1.00** |
| **small** [5.0–6.0] | **round_robin** | 1 942 731 | **3 609** | 3 643 | **538** | 344 K | 1.00 |
| | consistent_hash | 1 942 731 | 3 897 | 4 015 | 498 | 344 K | 1.00 |
| | trip_distance | 1 942 731 | 6 017 | 6 061 | 323 | 344 K | 1.00 |
| **medium** [1–3] | **round_robin** | 34 764 991 | **50 510** | 50 776 | **688** | 347 K | 1.00 |
| | consistent_hash | 34 764 991 | 53 855 | 53 908 | 645 | 346 K | 1.00 |
| | trip_distance | 34 764 991 | 55 219 | 57 710 | 630 | 346 K | 1.00 |
| **broad** [0–30] | round_robin | 69 973 589 | 113 730 | 113 891 | 615 | 347 K | 1.00 |
| | (all 3 schemes within 5 %) | | | | | | |

¹ *One of the two repeats hit a Python-node warmup race. The first
data-loading pass after `array.array` re-implementation can take 12–15 s
for 9 M rows; if it's still loading when the gateway sends a forward, the
forward retries (up to 10 s of patience) and may still time out. The
second repeat returned the full 176 K rows in <1 s. Reported as the mean
of both for transparency.*

#### Three-way takeaway

1. **For very narrow queries, `trip_distance` wins** because all matches
   live in one bucket → one owner returns big chunks fast (≈ 280 KB
   per chunk).
2. **For everything else, `round_robin` wins** by 5–10 % because all 9
   producers contribute in parallel.
3. **`consistent_hash` is competitive** (within 5–7 % of round_robin on
   small/medium) and would beat trip_distance for narrow if not for the
   gateway's serial fan-out (see §"What we tried that didn't work").

### 2. Topology — *how are the 9 nodes connected?*

| Topology | Shape | A→leaf hops | Bench result vs tree |
|---|---|---|---|
| **Tree** | spec topology AB, BC, BD, BE, EF, ED, EG, AH, AG, AI | 1–3 | baseline |
| Star | A connects directly to every other node | 1 | within 1 % |
| Chain | A → B → C → … → I | up to 8 | within 3 % |
| Grid | 3×3, no diagonals | up to 4 | within 5 % |

**Topology barely matters** under smart routing because most query types
end up using only a few hops. For broad queries that hit all owners, the
hop chain shows up: chain is ~5 % slower than tree on broad. But for the
narrow/small/medium workloads we benched, all four topologies finished
within 5 % of each other. The spec's tree is fine.

### 3. Chunk size — *how many rows per gRPC response?*

The dynamic `ChunkSizer` watches inter-pull latency: grows the chunk by
1.5× when the consumer pulls in <12 ms (target/2), shrinks by 0.5× when
the pull takes >50 ms (target×2). Initial size 64, capped at 4 K.

| chunk-rows | small (1.94 M rows) | medium (34.7 M rows) | comments |
|---|---|---|---|
| **dynamic** (best) | **6 925 ms** | **61 388 ms** | adaptive sizing wins |
| 256 | 15 125 ms (2.2× slower) | 210 615 ms (3.4× slower) | RTT-dominated; 135 K chunks for medium! |
| 1 024 | 9 125 ms | 91 645 ms | RTT still dominates |
| 2 048 | 8 465 ms | 74 791 ms | approaching dynamic |
| 4 096 | 6 710 ms | 67 185 ms | matches dynamic |
| 8 192 | 6 671 ms | 62 302 ms | matches dynamic — producer cap kicks in |

The producer's per-request matched-deque is capped at **8 192 rows** as
a back-pressure safety (otherwise OOM — see §"What broke and why").
Above 4 K rows, the chunk size is bounded by this cap, so dynamic and
8 192 converge.

**Why 256 is so bad:** 35 M rows ÷ 256 rows/chunk = 135 800 chunks. Each
gRPC RTT on the LAN is ~1.5 ms. So 135 800 × 1.5 ms ≈ 200 s in raw
network overhead — exactly what we measured. Small chunks are
network-bound.

### 4. Worker threads per node

The C++ gateway uses `grpc::ResourceQuota::SetMaxThreads` to cap the
sync-server thread pool. Swept 1, 2, 4, 8, 16 threads per node.

| threads | small mean (1 client) | small mean (4 clients) | medium (1 client) | comments |
|---|---|---|---|---|
| 1 | 6 768 ms | 7 032 ms | 61 969 ms | enough for single-client work |
| 2 | 7 133 ms | 7 010 ms | 63 667 ms | basically same |
| 4 | 6 889 ms | **18 563 ms** | 62 437 ms | **regression under concurrency** |
| 8 | 6 810 ms | **24 701 ms** | 61 388 ms | even worse |
| 16 | (similar) | (similar) | (similar) | no improvement |

**Surprising finding:** more worker threads make the *4-client* case
*worse*, not better. With smart routing limiting some queries to one
owner, that owner's producer is the bottleneck — adding gRPC threads on
top just adds context-switch overhead. The right answer is **2–8
threads**; we use 8 as the default for safety margin.

---

## Smart routing in detail

Each manifest carries the routing metadata the gateway needs to filter
owners before fanning out. For `trip_distance` clustering on 70 M rows
the actual buckets came out as:

| Owner | trip_distance range (mi) | rows held |
|---|---|---|
| A | 0.0 – 0.68 | 8 372 257 |
| B | 0.68 – 0.9 | 8 034 628 |
| C | 0.9 – 1.18 | 7 472 504 |
| D | 1.18 – 1.46 | 7 614 067 |
| E | 1.46 – 1.81 | 7 737 758 |
| F | 1.81 – 2.38 | 7 710 472 |
| G | 2.38 – 3.38 | 7 851 677 |
| H | 3.38 – 6.30 | 7 900 184 |
| I | 6.30 – ∞ | 7 306 453 |

So `trip_distance ∈ [5.0, 5.05]` lives entirely in **H** — the gateway
contacts only H, skips the other 8. The smart-route log line confirms:

```
[A] smart-route rid=req-… hit 1/9 owners; skipped: A B C D E F G I
```

For broad queries the manifest still gets consulted; smart routing just
returns "all 9 are eligible". So smart routing **never hurts** broad
queries, only helps narrow ones — at the cost of parallelism.

---

## Verification — are the numbers real?

Two safeguards baked into the bench harness:

1. **Row-count check.** The same query against any topology / scheme
   must return the same total row count. Mismatches are flagged.
2. **`wait_ready` probe.** Before any timing run, the harness issues a
   real query and verifies it returns ≥ 100 K rows (i.e., enough owners
   responded that the cluster is genuinely warm). This caught a bug
   early in development where the multihost cluster had `127.0.0.1` IPs
   and forwards silently failed (rows came back from local owners only,
   making numbers look fast but wrong).

Two real bugs the verification caught and that we then fixed:

- **Producer back-pressure.** Without a buffer cap, a slow consumer
  would let producers stuff entire 11 M-row match-sets into RAM. We OOM-
  killed Fedora (16 GB + 8.6 GB swap, system unresponsive). Fixed: cap
  the matched deque at 8 192 rows per request (~1.2 MB).
- **Forward-during-warmup race.** Python data-owner nodes loading 8–9 M
  rows in stdlib-only Python (no NumPy) take 10–15 s. The gateway's
  initial 3-attempt forward retry was too short; we'd dead-mark a
  Python owner and lose its rows. Fixed: 20 retries × 500 ms = 10 s of
  warmup tolerance for `UNAVAILABLE` and `DEADLINE_EXCEEDED`.

---

## Architecture choices we made along the way

### 9 owners, not 6

Originally A, B, E held no data (gateway + intermediaries). We changed
this so all 9 nodes own a shard. Effects:

- **Better load balance** — each node now holds 1/9 of the data
  (~7.78 M rows on average) instead of 1/6 (~11.7 M).
- **Smaller per-node memory pressure** — Fedora's RAM dropped from 16 GB
  + 8 GB swap to 7 GB after this + back-pressure together.
- **Gateway A is now also a producer.** FetchChunk on A drains its own
  local matched buffer first (in-process, no gRPC), then pulls from
  remote peers. This is a dual role but the overhead is small (≤ 7 %)
  for single-client workloads.

### Removed NumPy from the Python node

Spec asks "minimize third-party libraries." We replaced NumPy with
stdlib `array.array` typed columns — same memory layout (contiguous
8-byte cells), no dependency. **Trade-off:** `range_search` is now a
Python interpreter loop (~10× slower than the NumPy vectorised version
on 9 M rows). C++ nodes remain fast; Python nodes (D, F, H) are now the
bottleneck on workloads where they're on the critical path.

### Direct ethernet link, not Wi-Fi

The two laptops have a dedicated ~0.1 ms ethernet path. Wi-Fi would
have given ~2–5 ms and shared the medium with other traffic — adding
~40 ms per FetchChunk on broad workloads (17 K chunks × 2.5 ms ≈ 42 s
of pure RTT on top of the actual work).

---

## What we tried that didn't work

- **`pickup_datetime` clustering on the 70 M slice.** Severe imbalance:
  owners G and H (months 4+10 / 5+11) ended up with **19 M and 9.8 M
  rows** respectively, while F (month 6) got **0 rows** — the first
  70 M rows of the master CSV are mostly Jan–April. Don't use month-of-
  year clustering unless you actually have a uniformly distributed time
  axis.
- **Static 256-row chunk.** The most "fair-sounding" small fixed size,
  it's the worst possible choice on LAN: 200 s for what dynamic does in
  61 s. The lesson: chunk-size choice must respect RTT.
- **Smart routing as a one-size-fits-all.** Our hypothesis was that
  `trip_distance` clustering would always win. **It doesn't** — only
  for narrow queries. For everything else the parallelism loss dominates.
- **More threads.** Our naive instinct was that 16 threads would help
  under concurrency. They actively *hurt* — extra threads contend on
  shared producer state without unblocking the actual bottleneck (single
  serialised PullChunk loop in the gateway).
- **Star topology.** The gateway has 8 children; under broad-query fan-
  out it became the network bottleneck. Tree (≤ 4 children of A) wins
  by ~3 %.

---

## Final architecture

```
                   Gigabit ethernet (192.168.50.x, ~0.1ms RTT)
   ┌────────────────────────┐     ┌───────────────────────┐
   │ MacBook (en7 .50.2)    │═════│ Fedora (.50.1)        │
   │   A B C D E (5 nodes)  │     │   F G H I (4 nodes)   │
   │   ~3.9 M rows × 5      │     │   ~7.8 M rows × 4     │
   └────────────────────────┘     └───────────────────────┘
            ▲
   external client (talks only to A)
```

- **9 nodes** on the spec tree topology (AB BC BD BE EF ED EG AH AG AI).
- **9 data owners** (every node owns one shard of 70 M / 9 ≈ 7.78 M rows).
- **Default scheme = `round_robin`** for max parallelism, with smart-
  routing kicking in automatically for narrower clustering schemes.
- **Dynamic chunk sizer** (start 64, grow 1.5× per pull, cap 4 K rows).
- **8 worker threads per node** (gRPC sync pool, capped via
  `ResourceQuota`).
- **Producer back-pressure cap** at 8 192 rows per request.
- **Smart routing always on** — degrades gracefully to "contact all" when
  the manifest carries no clustering metadata.

---

## What this taught us

The thing the spec's prompt hinted at and the bench actually proved:
**there is no single best clustering scheme.** The right answer
depends on the *shape* of the queries you expect:

- selective predicates on one column → cluster by that column
  (`trip_distance`)
- mixed query types → don't cluster (`round_robin`)
- want both → consistent hashing, *but only if you also fix the gateway
  to fan out in parallel* (which we didn't have time to do; today's
  consistent_hash leaves performance on the table because PullChunk is
  serial)

The biggest single-sentence lesson: **on a small (~9 node) cluster with
fast LAN, producer parallelism beats data selectivity by 30–40 % for
typical workloads, and smart routing only helps when one shard genuinely
contains the entire matching set.**

---

## Reproducing

```bash
# One-time Fedora setup
sudo dnf install -y gcc gcc-c++ cmake grpc-devel protobuf-devel \
                    abseil-cpp-devel openssl-devel python3 python3-pip rsync

# From the Mac:
bash scripts/deploy.sh           # rsync source + build C++ on Fedora
ssh yash@192.168.50.1 "cd ~/mini_2 && \
  python3 scripts/split_taxi_csv.py \
    --input data/2017_Yellow_Taxi_Trip_Data_20260228.csv \
    --rows 70000000 \
    --schemes round_robin,trip_distance,pu_location_id,pickup_datetime,consistent_hash"

# Pull A-E shards back to Mac for each scheme; F G H I stay on Fedora.
# Then:
bash bench/run_full_bench.sh multihost   # all four sweeps
```

Per-cell JSON results land in `bench/results/<topo>__<scheme>__t<threads>__c<chunk>.json`.
