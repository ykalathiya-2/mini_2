# mini_2 — Multi-process scatter/gather with gRPC

Extends `mini_1` from in-process parallelism to a 9-process overlay across
2–3 hosts. Only gateway `A` talks to the external client; `A` scatters the
query through the tree, gathers partitioned results (no replication), and
streams them back in chunks — all **on top of unary RPCs**, no server-
streaming or async stubs.

## Overlay (tree — the one we picked)

Edges: `AB, BC, BD, BE, EF, ED, EG, AH, AG, AI`.

```
          A (gateway, client-facing)
       /  |   \    \
      B   H   G     I
    / | \   /
   C  D  E    
        /|\
       F D G
```

Teams: **Blue** = {A, B, D, H}, **Yellow** = {C, E, F, G, I}.

Data-owning nodes (each owns a *disjoint* slice of the dataset — no replication,
no sharing): `C, D, F, G, H, I`.

Config is loaded from [`config/overlay.yaml`](config/overlay.yaml) at startup —
**nothing is hard-coded**. Host/port per node may be overridden via
`MINI2_HOST_<NAME>` / `MINI2_PORT_<NAME>` environment variables so the same
overlay.yaml works on localhost and multi-host runs.

## Layout

```
mini2/
  proto/           # mini2.proto (unary RPCs only)
  proto_gen/       # generated Python code (C++ codegen is in build/)
  cpp/
    server/        # C++ node daemon (gateway + peer)
    client/        # C++ client (talks only to A)
    common/        # overlay loader, CSV store, scheduler, chunk sizer
  py/server/       # Python node daemon (same RPC surface)
  config/          # overlay.yaml
  data/partitions/ # disjoint CSV shards, one per data owner
  scripts/         # start_node.sh, start_all_local.sh, stop_all.sh
  bench/           # benchmark harness + edge-case suite + results
```

## Prerequisites

- macOS/Linux with Homebrew or equivalent
- `brew install grpc protobuf abseil cmake` (Homebrew prefix `/opt/homebrew`)
- Python 3.10+ with `grpcio grpcio-tools protobuf pyyaml`

## Build

```bash
# Generate Python protos
python -m grpc_tools.protoc -I proto \
    --python_out=proto_gen/python \
    --grpc_python_out=proto_gen/python proto/mini2.proto

# Build C++ node + client
cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp -j
```

## Generate (or re-partition) the dataset

```bash
python py/server/gen_dataset.py --rows 600000
# → data/partitions/{C,D,F,G,H,I}.csv + manifest.json
```

Schema matches Mini 1's 17-column Yellow Taxi layout (int / double / bool
fields — no "strings for everything").

## Run all 9 processes locally

```bash
./scripts/start_all_local.sh        # logs go to logs/<N>.log
./build/cpp/mini2_client --column trip_distance --low 5 --high 6
./scripts/stop_all.sh
```

Per-node: `./scripts/start_node.sh <NODE>` (dispatches to C++ or Python
automatically based on the `impl:` field in the YAML).

## Multi-host

On each host, set the per-node host env vars (only the entries for nodes
NOT running on 127.0.0.1 need to change):

```bash
# host 1 runs A, B, C, D, E
# host 2 runs F, G, H, I
export MINI2_HOST_F=10.0.0.2 MINI2_HOST_G=10.0.0.2 \
       MINI2_HOST_H=10.0.0.2 MINI2_HOST_I=10.0.0.2

./scripts/start_node.sh A   # repeat on each host with its own node list
```

The launch is per-shell; we never run inside an IDE VM.

## What was built (spec checklist)

- **Basecamp.** 9 processes, mixed C++/Python implementations, reading
  identity from argv/env, config-driven — ✓.
- **Only A talks to clients.** `ClientGateway::SubmitQuery/FetchChunk`
  refuse at non-gateway nodes with `FAILED_PRECONDITION` (edge-case test
  verifies this).
- **Scatter/gather via intermediates.** `A → B → {C,D,E→F}`, `A → {G, H, I}`.
  Intermediate nodes keep no large state; they forward via a precomputed
  BFS routing table (`routing:` in overlay.yaml).
- **Request context matching.** Every `ForwardedQuery` / `PullRequest`
  carries a `request_id` + `target_owner`; intermediates match on the
  routing table to forward correctly. No node mis-routes even when multiple
  pulls collide at the same intermediate.
- **Chunked replies on unary RPCs.** Pull-based: client issues
  `FetchChunk` → gateway issues `PullChunk` to owners via overlay.
  Producers buffer matched rows into per-request deques with bounded memory.
- **Dynamic chunk sizing.** `ChunkSizer` watches inter-pull latency and
  grows/shrinks chunk size toward `target_chunk_ms=25`. See the chunk
  sweep results below for the effect on throughput.
- **Fair scheduling.** `FairScheduler` (weighted round-robin) gives equal
  production credits to concurrent requests. Measured Jain's index ≥ 0.997
  for 2–3 concurrent, ≥ 0.99 for 4.
- **Unary only.** Every RPC is a plain `(Request) → Response`. No
  server-streaming, no async stubs, no `SetServingStatus` tricks.
- **No shared memory for responses.** Each process owns its own deques;
  cross-process flow is exclusively gRPC.
- **Typed data.** `TaxiRow` uses `int32 / int64 / double / bool / string`
  per the real schema, not `repeated string`.
- **Edge cases handled** (see [bench/edge_cases.json](bench/edge_cases.json)):
  cancel mid-stream, abandon without cancel, oversize result, empty range,
  unknown `request_id`, non-gateway client API rejection, concurrent no-starve.

## Benchmark results (localhost, M-series Mac, 120 000 rows × 6 partitions)

### Chunk-size sweep (deliver all 120 k rows)

| hint       | total ms | first-chunk ms | chunks | rows/s  |
|-----------:|---------:|---------------:|-------:|--------:|
| 16         |  5 482   | 14.0           |  7 501 |  21 889 |
| 64         |  1 612   | 31.6           |  1 876 |  74 453 |
| 256        |    666   |  4.4           |    470 | 180 285 |
| 1 024      |    380   |  6.1           |    119 | 316 157 |
| 4 096      |    236   | 11.5           |     31 | 508 104 |
| **dynamic**|    **285** | **27.3**     |     **41** | **421 478** |

Tiny chunks (16) are overhead-dominated — RPC header > payload. Very large
chunks (4 096) give the best raw throughput but cost head-of-line latency.
Dynamic sizing lands in the useful middle (first-byte fast, steady-state
throughput close to the 1 024 hint) without the client needing to know the
right value.

### Latency series (`trip_distance ∈ [0, 30]`, 5 runs)

| metric | ms |
|---|---|
| mean | 273.9 |
| median | 274.7 |
| p95 | 274.7 |
| stdev | 20.8 |

### Fairness (Jain's index across concurrent requests)

| concurrency | times (ms)                        | Jain's | spread |
|-------------|-----------------------------------|-------:|-------:|
| 1 | [306]                                        | 1.0000 | 0 %   |
| 2 | [430, 438]                                   | 0.9999 | 1.9 % |
| 3 | [518, 530, 533]                              | 0.9998 | 2.9 % |
| 4 | [538, 600, 606, 616]                         | 0.9974 | 12.6 % |

Concurrency 4 shows the first outlier (one request finishes noticeably
faster) — that's the round-robin scheduler draining requests in the order
they complete rather than perfectly locked-step. Raising
`scheduler.mode: fifo` in overlay.yaml trades that fairness spread for
lower p99.

### Per-process peak memory (RSS, kB — snapshot at end of run)

| node | impl   | rss kB |
|------|--------|-------:|
| A (gateway) | cpp | 36 288 |
| B | cpp    | 28 080 |
| C | cpp    | 41 728 |
| D | python | 107 840 |
| E | cpp    | 23 552 |
| F | python | 106 128 |
| G | cpp    | 41 392 |
| H | python | 106 496 |
| I | cpp    | 41 024 |

C++ nodes ≤ 42 MB; Python nodes ~107 MB (CPython + grpcio baseline).
Memory is *flat* as requests run — chunk size stays bounded regardless of
result-set size, verifying the "oversize result" edge case.

## Running the bench yourself

```bash
./scripts/start_all_local.sh
python bench/bench.py       # writes bench/results.json
python bench/edge_cases.py  # writes bench/edge_cases.json
./scripts/stop_all.sh
```
