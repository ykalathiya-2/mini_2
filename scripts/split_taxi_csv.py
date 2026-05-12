#!/usr/bin/env python3
"""Stream the 2017 Yellow Taxi master CSV, take the first N rows (default 20M),
and split disjointly across the six data-owning nodes (C, D, F, G, H, I).

Supports multiple clustering schemes simultaneously (single read pass over
the input):

    round_robin       row_index % 6                          (baseline)
    trip_distance     size-balanced quantile buckets         (smart routing dim)
    pu_location_id    zones grouped to balance row counts    (spatial)
    pickup_datetime   month-of-year % 6                      (temporal)

Output layout (one tree per scheme):

    data/partitions_round_robin/{C,D,F,G,H,I}.csv + manifest.json
    data/partitions_trip_distance/...
    data/partitions_pu_location_id/...
    data/partitions_pickup_datetime/...

Each manifest carries the scheme's bucket boundaries so the gateway can do
smart routing (e.g. "trip_distance in [5,6] -> only node H").

Schema notes:
- No row_id column (saves 8 bytes/row + a column).
- pickup_datetime / dropoff_datetime are converted from
  "YYYY Mon DD HH:MM:SS AM/PM" to Unix epoch seconds (int64, UTC).
- Columns renamed CamelCase -> snake_case to match mini_2 proto.
- store_and_fwd_flag stays as 'Y'/'N' (the C++ reader maps to bool).
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Dict, List

OWNERS = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]
ALL_SCHEMES = ["round_robin", "trip_distance", "pu_location_id",
               "pickup_datetime", "consistent_hash"]

# Consistent hashing tuning. We bucket trip_distance at 0.01-mi granularity
# (multiplier 100). Each physical node hosts VNODES_PER_OWNER virtual nodes;
# vnodes are striped across owners so a random hash hits roughly all owners.
# The same constants are mirrored in cpp/common/routing.cpp — change both
# together or routing will be wrong.
HASH_BUCKET_MULTIPLIER = 100
VNODES_PER_OWNER       = 16
TOTAL_VNODES           = VNODES_PER_OWNER * len(OWNERS)


def fnv1a32(x: int) -> int:
    """32-bit FNV-1a hash of an integer's little-endian byte representation.
    Deterministic across runs; matches the C++ implementation in routing.cpp."""
    h = 2166136261
    for _ in range(4):
        h ^= x & 0xff
        h = (h * 16777619) & 0xffffffff
        x >>= 8
    return h

OUT_HEADER = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def parse_dt(s: str) -> int:
    parts = s.split()
    yr = int(parts[0]); mo = MONTHS[parts[1]]; da = int(parts[2])
    hh, mm, ss = parts[3].split(":")
    hh = int(hh); mm = int(mm); ss = int(ss)
    if parts[4] == "PM" and hh != 12: hh += 12
    elif parts[4] == "AM" and hh == 12: hh = 0
    return calendar.timegm((yr, mo, da, hh, mm, ss, 0, 0, 0))


def num(s: str) -> str:
    if "," in s:
        return s.replace(",", "")
    return s


def compute_boundaries(input_path: Path, sample_size: int = 1_000_000) -> dict:
    """One sample pass to learn data distribution. Used for size-balanced
    trip_distance buckets and pu_location_id node assignment."""
    print(f"[boundaries] sampling first {sample_size:,} rows...", file=sys.stderr)
    distances: List[float] = []
    loc_counts: Dict[int, int] = {}
    t0 = time.time()
    with open(input_path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_idx = {name: i for i, name in enumerate(header)}
        idx_dist = col_idx["trip_distance"]
        idx_pu   = col_idx["PULocationID"]
        for i, row in enumerate(reader):
            if i >= sample_size:
                break
            try:
                d = float(num(row[idx_dist]))
                if 0 < d < 1000:
                    distances.append(d)
                pu = int(num(row[idx_pu]))
                loc_counts[pu] = loc_counts.get(pu, 0) + 1
            except (ValueError, IndexError):
                continue

    distances.sort()
    n = len(distances)
    # 8 dividers -> 9 buckets, each ~equal in count
    dist_dividers = [distances[int(n * k / 9)] for k in range(1, 9)]

    # Greedy bin packing for locations: assign each (large->small) zone to the
    # currently smallest bucket. Yields ~equal row counts across nodes.
    sorted_locs = sorted(loc_counts.items(), key=lambda kv: -kv[1])
    loc_to_node: Dict[int, int] = {}
    bucket_load = [0] * len(OWNERS)
    for loc_id, cnt in sorted_locs:
        idx = bucket_load.index(min(bucket_load))
        loc_to_node[loc_id] = idx
        bucket_load[idx] += cnt

    print(f"[boundaries] dist dividers (mi): {[round(x,3) for x in dist_dividers]}",
          file=sys.stderr)
    print(f"[boundaries] {len(loc_to_node)} unique zones; "
          f"per-node load (sample): {bucket_load}", file=sys.stderr)
    print(f"[boundaries] sample took {time.time()-t0:.1f}s", file=sys.stderr)
    return {
        "trip_distance_dividers": dist_dividers,
        "pu_location_to_node":    {str(k): v for k, v in loc_to_node.items()},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  type=Path,
                    default=Path("data/2017_Yellow_Taxi_Trip_Data_20260228.csv"))
    ap.add_argument("--out",    type=Path, default=Path("data"))
    ap.add_argument("--rows",   type=int, default=70_000_000)
    ap.add_argument("--schemes", default=",".join(ALL_SCHEMES),
                    help="comma-separated subset of " + ",".join(ALL_SCHEMES))
    ap.add_argument("--progress", type=int, default=1_000_000)
    args = ap.parse_args()

    schemes = [s.strip() for s in args.schemes.split(",") if s.strip()]
    for s in schemes:
        if s not in ALL_SCHEMES:
            print(f"unknown scheme: {s}", file=sys.stderr); sys.exit(2)

    if not args.input.exists():
        print(f"input not found: {args.input}", file=sys.stderr); sys.exit(1)

    boundaries = compute_boundaries(args.input)
    dist_dividers   = boundaries["trip_distance_dividers"]
    loc_to_node_idx = {int(k): v for k, v in boundaries["pu_location_to_node"].items()}

    # Per-scheme owner ranges (used by the gateway for smart routing).
    # For trip_distance: [low, high] of dividers per owner index.
    # For pu_location_id: list of zone IDs each owner owns.
    # For pickup_datetime: month set per owner.
    dist_ranges = []
    for i in range(len(OWNERS)):
        lo = -1.0 if i == 0 else dist_dividers[i - 1]
        hi = float("inf") if i == len(OWNERS) - 1 else dist_dividers[i]
        dist_ranges.append([lo, hi])

    loc_owners: Dict[int, List[int]] = {i: [] for i in range(len(OWNERS))}
    for zone, idx in loc_to_node_idx.items():
        loc_owners[idx].append(zone)

    # Months 1..12 distributed across 9 owners using owner = (month-1) % 9.
    # Owners 0..2 get 2 months each (Jan+Oct, Feb+Nov, Mar+Dec); owners 3..8
    # get 1 month each (Apr..Sep).
    month_to_owner: Dict[int, int] = {m: (m - 1) % len(OWNERS) for m in range(1, 13)}
    month_owners: Dict[int, List[int]] = {i: [] for i in range(len(OWNERS))}
    for m, owner_idx in month_to_owner.items():
        month_owners[owner_idx].append(m)

    # Open output files for every (scheme, owner).
    files: Dict[str, Dict[str, "object"]] = {}
    writers: Dict[str, Dict[str, csv.writer]] = {}
    counts:  Dict[str, Dict[str, int]] = {}
    for scheme in schemes:
        d = args.out / f"partitions_{scheme}"
        d.mkdir(parents=True, exist_ok=True)
        files[scheme] = {o: open(d / f"{o}.csv", "w", newline="") for o in OWNERS}
        writers[scheme] = {o: csv.writer(f, quoting=csv.QUOTE_NONE, escapechar="\\")
                           for o, f in files[scheme].items()}
        for w in writers[scheme].values():
            w.writerow(OUT_HEADER)
        counts[scheme] = {o: 0 for o in OWNERS}

    # Per-row routing functions (closures so we don't recompute per-row).
    n_owners = len(OWNERS)
    def owner_round_robin(kept, _row): return OWNERS[kept % n_owners]

    def owner_consistent_hash(_kept, row):
        try:
            d = float(row[4])  # trip_distance
        except (ValueError, TypeError):
            return OWNERS[0]
        bucket = int(d * HASH_BUCKET_MULTIPLIER)
        if bucket < 0: bucket = 0
        vnode = fnv1a32(bucket) % TOTAL_VNODES
        return OWNERS[vnode // VNODES_PER_OWNER]

    def owner_trip_distance(_kept, row):
        d = float(row[4])  # trip_distance is index 4 in OUT_HEADER
        for i, b in enumerate(dist_dividers):
            if d <= b:
                return OWNERS[i]
        return OWNERS[n_owners - 1]

    def owner_pu_location(_kept, row):
        try:
            loc = int(row[7])  # pu_location_id is index 7
        except (ValueError, TypeError):
            return OWNERS[0]
        return OWNERS[loc_to_node_idx.get(loc, 0)]

    _epoch = dt.datetime(1970, 1, 1)
    def owner_pickup_datetime(_kept, row):
        try:
            ts = int(row[1])  # pickup_datetime epoch seconds at index 1
            month = dt.datetime.utcfromtimestamp(ts).month
        except Exception:
            return OWNERS[0]
        return OWNERS[month_to_owner.get(month, 0)]

    fns = {
        "round_robin":       owner_round_robin,
        "trip_distance":     owner_trip_distance,
        "pu_location_id":    owner_pu_location,
        "pickup_datetime":   owner_pickup_datetime,
        "consistent_hash":   owner_consistent_hash,
    }
    active_fns = [(s, fns[s]) for s in schemes]

    print(f"[split] schemes = {schemes}", file=sys.stderr)
    print(f"[split] target rows = {args.rows:,}", file=sys.stderr)
    t_start = time.time()

    with open(args.input, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_idx = {name: i for i, name in enumerate(header)}
        idx_vendor = col_idx["VendorID"]
        idx_pu_dt  = col_idx["tpep_pickup_datetime"]
        idx_do_dt  = col_idx["tpep_dropoff_datetime"]
        idx_pass   = col_idx["passenger_count"]
        idx_dist   = col_idx["trip_distance"]
        idx_rate   = col_idx["RatecodeID"]
        idx_fwd    = col_idx["store_and_fwd_flag"]
        idx_pu     = col_idx["PULocationID"]
        idx_do     = col_idx["DOLocationID"]
        idx_ptype  = col_idx["payment_type"]
        idx_fare   = col_idx["fare_amount"]
        idx_extra  = col_idx["extra"]
        idx_mta    = col_idx["mta_tax"]
        idx_tip    = col_idx["tip_amount"]
        idx_tolls  = col_idx["tolls_amount"]
        idx_imp    = col_idx["improvement_surcharge"]
        idx_total  = col_idx["total_amount"]

        kept = 0
        skipped = 0
        for row in reader:
            if kept >= args.rows:
                break
            try:
                pu_ts = parse_dt(row[idx_pu_dt])
                do_ts = parse_dt(row[idx_do_dt])
            except Exception:
                skipped += 1
                continue

            out_row = [
                num(row[idx_vendor]),
                pu_ts,
                do_ts,
                num(row[idx_pass]),
                num(row[idx_dist]),
                num(row[idx_rate]),
                row[idx_fwd],
                num(row[idx_pu]),
                num(row[idx_do]),
                num(row[idx_ptype]),
                num(row[idx_fare]),
                num(row[idx_extra]),
                num(row[idx_mta]),
                num(row[idx_tip]),
                num(row[idx_tolls]),
                num(row[idx_imp]),
                num(row[idx_total]),
            ]
            for scheme, fn in active_fns:
                owner = fn(kept, out_row)
                writers[scheme][owner].writerow(out_row)
                counts[scheme][owner] += 1
            kept += 1
            if kept % args.progress == 0:
                rate = kept / max(1e-3, time.time() - t_start)
                print(f"  ... {kept:,} rows split  ({rate/1000:.0f} kr/s)",
                      file=sys.stderr)

    for scheme in schemes:
        for f in files[scheme].values():
            f.close()

    # Manifests, one per scheme. Includes routing metadata so the gateway can
    # decide which owners can satisfy a given query without contacting them.
    for scheme in schemes:
        manifest = {
            "scheme": scheme,
            "total_rows": kept,
            "skipped_malformed": skipped,
            "owners": OWNERS,
            "rows_per_owner": counts[scheme],
            "source": str(args.input),
            "header": OUT_HEADER,
            "datetime_encoding": "unix_epoch_seconds_utc_int64",
        }
        if scheme == "trip_distance":
            manifest["dist_dividers"] = dist_dividers
            manifest["owner_dist_range"] = {
                OWNERS[i]: dist_ranges[i] for i in range(len(OWNERS))
            }
        elif scheme == "pu_location_id":
            manifest["owner_zones"] = {
                OWNERS[i]: sorted(loc_owners[i]) for i in range(len(OWNERS))
            }
        elif scheme == "pickup_datetime":
            manifest["owner_months"] = {
                OWNERS[i]: month_owners[i] for i in range(len(OWNERS))
            }
        elif scheme == "consistent_hash":
            manifest["hash_bucket_multiplier"] = HASH_BUCKET_MULTIPLIER
            manifest["vnodes_per_owner"]       = VNODES_PER_OWNER
            manifest["total_vnodes"]           = TOTAL_VNODES
            manifest["hash_column"]            = "trip_distance"
            manifest["hash"]                   = "fnv1a32"
        out_path = args.out / f"partitions_{scheme}" / "manifest.json"
        out_path.write_text(json.dumps(manifest, indent=2))

    elapsed = time.time() - t_start
    print(f"\n[split] {kept:,} rows -> {len(schemes)} schemes "
          f"in {elapsed:.1f}s ({skipped:,} skipped)", file=sys.stderr)
    for scheme in schemes:
        per = counts[scheme]
        spread = max(per.values()) - min(per.values())
        print(f"  {scheme:20s}  rows={per}  spread={spread}", file=sys.stderr)


if __name__ == "__main__":
    main()
