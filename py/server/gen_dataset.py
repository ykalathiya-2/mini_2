#!/usr/bin/env python3
"""Generate a synthetic NYC-taxi-shaped dataset and partition it across
the six data-owning nodes (C, D, F, G, H, I) disjointly.

Writes one CSV per owner into data/partitions/<node>.csv and a manifest
in data/partitions/manifest.json so every process can look up its own slice
at startup.

Seed is fixed for reproducibility so Mini 1 parity can be compared.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from pathlib import Path

HEADER = [
    "row_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "pu_location_id",
    "do_location_id",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
]


def synth_row(rng: random.Random, row_id: int) -> list:
    vendor = rng.choice([1, 2])
    passengers = rng.choices([1, 2, 3, 4, 5, 6], weights=[70, 12, 7, 5, 3, 3])[0]
    dist = round(max(0.1, rng.lognormvariate(0.7, 0.6)), 2)
    rate = rng.choices([1, 2, 3, 4, 5, 6], weights=[93, 4, 1, 1, 0.5, 0.5])[0]
    pu = rng.randint(1, 265)
    do = rng.randint(1, 265)
    fwd = rng.random() < 0.005
    ptype = rng.choices([1, 2, 3, 4], weights=[70, 27, 2, 1])[0]
    fare = round(2.5 + dist * rng.uniform(2.0, 3.2), 2)
    extra = round(rng.choice([0.0, 0.0, 0.5, 1.0]), 2)
    mta = 0.5
    tolls = round(rng.choice([0.0, 0.0, 0.0, 5.76, 6.12, 11.52]), 2)
    improv = 0.3
    tip = 0.0 if ptype != 1 else round(fare * rng.uniform(0.0, 0.28), 2)
    total = round(fare + extra + mta + tolls + improv + tip, 2)
    month = rng.randint(1, 12)
    day = rng.randint(1, 28)
    hour = rng.randint(0, 23)
    minute = rng.randint(0, 59)
    pu_time = f"2017-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
    trip_minutes = max(1, int(dist * 3 + rng.uniform(0, 10)))
    do_minute = (minute + trip_minutes) % 60
    do_hour = (hour + (minute + trip_minutes) // 60) % 24
    do_time = f"2017-{month:02d}-{day:02d} {do_hour:02d}:{do_minute:02d}:00"

    return [
        row_id,
        vendor,
        pu_time,
        do_time,
        passengers,
        dist,
        rate,
        pu,
        do,
        "Y" if fwd else "N",
        ptype,
        fare,
        extra,
        mta,
        tip,
        tolls,
        improv,
        total,
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=600_000,
                    help="total synthetic rows to generate")
    ap.add_argument("--out", type=Path,
                    default=Path(__file__).resolve().parents[2] / "data" / "partitions")
    ap.add_argument("--owners", nargs="+",
                    default=["C", "D", "F", "G", "H", "I"])
    ap.add_argument("--seed", type=int, default=0xC0FFEE)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(args.seed)

    owners = args.owners
    # Partition by row_id % len(owners) — fully disjoint, easy to reason about.
    files = {o: open(args.out / f"{o}.csv", "w", newline="") for o in owners}
    writers = {o: csv.writer(f) for o, f in files.items()}
    counts = {o: 0 for o in owners}
    for w in writers.values():
        w.writerow(HEADER)

    for i in range(args.rows):
        owner = owners[i % len(owners)]
        writers[owner].writerow(synth_row(rng, i))
        counts[owner] += 1

    for f in files.values():
        f.close()

    manifest = {
        "total_rows": args.rows,
        "owners": owners,
        "rows_per_owner": counts,
        "seed": args.seed,
        "header": HEADER,
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"Wrote {args.rows} rows across {len(owners)} partitions → {args.out}")
    for o in owners:
        print(f"  {o}: {counts[o]} rows ({args.out / f'{o}.csv'})")


if __name__ == "__main__":
    main()
