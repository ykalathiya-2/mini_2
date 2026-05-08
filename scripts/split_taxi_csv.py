#!/usr/bin/env python3
"""Stream the 2017 Yellow Taxi master CSV, take the first N rows (default 20M),
and split disjointly across the six data-owning nodes (C, D, F, G, H, I).

Per Mini 2 design choices:
- No row_id column (saves 8 bytes/row + a column).
- pickup_datetime / dropoff_datetime are converted from
  "YYYY Mon DD HH:MM:SS AM/PM" to Unix epoch seconds (int64, UTC).
- Columns renamed CamelCase -> snake_case to match mini_2 proto.
- store_and_fwd_flag stays as 'Y'/'N' (the C++ reader maps to bool).

Output: data/partitions/{C,D,F,G,H,I}.csv (no quoting, numeric-friendly)
        data/partitions/manifest.json
"""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import sys
from pathlib import Path

OWNERS = ["C", "D", "F", "G", "H", "I"]

# Output schema (17 cols, no row_id, datetimes as int64 epoch seconds).
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

# Column-name remap from the master CSV's header to OUT_HEADER.
RENAME = {
    "VendorID":              "vendor_id",
    "tpep_pickup_datetime":  "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "RatecodeID":            "ratecode_id",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "PULocationID":          "pu_location_id",
    "DOLocationID":          "do_location_id",
    "payment_type":          "payment_type",
    "fare_amount":           "fare_amount",
    "extra":                 "extra",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount":          "total_amount",
}

MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def parse_dt(s: str) -> int:
    """Parse '2017 Sep 25 11:04:31 PM' -> Unix epoch seconds (UTC).

    Hand-rolled (not strptime) because we run this 40M times; ~3x faster.
    Treats the timestamp as UTC — we only need consistency for range queries,
    not wall-clock accuracy.
    """
    # Year is fixed-width 4 digits; the rest is space-separated, day may be
    # 1 or 2 chars. Use split() to be robust.
    parts = s.split()
    # parts: ['2017', 'Sep', '25', '11:04:31', 'PM']
    yr = int(parts[0])
    mo = MONTHS[parts[1]]
    da = int(parts[2])
    hh, mm, ss = parts[3].split(":")
    hh = int(hh); mm = int(mm); ss = int(ss)
    if parts[4] == "PM" and hh != 12:
        hh += 12
    elif parts[4] == "AM" and hh == 12:
        hh = 0
    return calendar.timegm((yr, mo, da, hh, mm, ss, 0, 0, 0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path,
                    default=Path("data/2017_Yellow_Taxi_Trip_Data_20260228.csv"))
    ap.add_argument("--out", type=Path, default=Path("data/partitions"))
    ap.add_argument("--rows", type=int, default=20_000_000,
                    help="number of data rows to keep (excludes header)")
    ap.add_argument("--progress", type=int, default=1_000_000,
                    help="log progress every N rows")
    args = ap.parse_args()

    if not args.input.exists():
        print(f"input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    args.out.mkdir(parents=True, exist_ok=True)

    files = {o: open(args.out / f"{o}.csv", "w", newline="") for o in OWNERS}
    # QUOTE_NONE + escapechar='\\' guarantees no field will be wrapped in quotes;
    # we strip thousands-separator commas below so values are pure numbers.
    writers = {o: csv.writer(f, quoting=csv.QUOTE_NONE, escapechar="\\") for o, f in files.items()}
    counts = {o: 0 for o in OWNERS}
    for w in writers.values():
        w.writerow(OUT_HEADER)

    def num(s: str) -> str:
        # Some source rows use thousands separators inside quoted numerics
        # (e.g. "8,008.5"). Strip commas so output is plain numeric CSV.
        if "," in s:
            return s.replace(",", "")
        return s

    with open(args.input, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_idx = {name: i for i, name in enumerate(header)}
        # Validate every input column we need is present.
        for src in RENAME:
            if src not in col_idx:
                print(f"input CSV missing column: {src}", file=sys.stderr)
                sys.exit(2)

        # Pre-compute source indices in OUT_HEADER order so the hot loop is
        # a flat list lookup, not a dict lookup per field.
        idx_vendor   = col_idx["VendorID"]
        idx_pu_dt    = col_idx["tpep_pickup_datetime"]
        idx_do_dt    = col_idx["tpep_dropoff_datetime"]
        idx_pass     = col_idx["passenger_count"]
        idx_dist     = col_idx["trip_distance"]
        idx_rate     = col_idx["RatecodeID"]
        idx_fwd      = col_idx["store_and_fwd_flag"]
        idx_pu       = col_idx["PULocationID"]
        idx_do       = col_idx["DOLocationID"]
        idx_ptype    = col_idx["payment_type"]
        idx_fare     = col_idx["fare_amount"]
        idx_extra    = col_idx["extra"]
        idx_mta      = col_idx["mta_tax"]
        idx_tip      = col_idx["tip_amount"]
        idx_tolls    = col_idx["tolls_amount"]
        idx_imp      = col_idx["improvement_surcharge"]
        idx_total    = col_idx["total_amount"]

        n_owners = len(OWNERS)
        kept = 0
        skipped = 0
        for i, row in enumerate(reader):
            if kept >= args.rows:
                break
            try:
                pu_ts = parse_dt(row[idx_pu_dt])
                do_ts = parse_dt(row[idx_do_dt])
            except Exception:
                skipped += 1
                continue

            owner = OWNERS[kept % n_owners]
            writers[owner].writerow([
                num(row[idx_vendor]),
                pu_ts,
                do_ts,
                num(row[idx_pass]),
                num(row[idx_dist]),
                num(row[idx_rate]),
                row[idx_fwd],          # 'Y' or 'N'
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
            ])
            counts[owner] += 1
            kept += 1
            if kept % args.progress == 0:
                print(f"  ... {kept:,} rows split", file=sys.stderr)

    for f in files.values():
        f.close()

    manifest = {
        "total_rows": kept,
        "skipped_malformed": skipped,
        "owners": OWNERS,
        "rows_per_owner": counts,
        "source": str(args.input),
        "header": OUT_HEADER,
        "datetime_encoding": "unix_epoch_seconds_utc_int64",
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"wrote {kept:,} rows across {n_owners} partitions ({skipped:,} skipped) -> {args.out}")
    for o in OWNERS:
        print(f"  {o}: {counts[o]:,} rows ({args.out / f'{o}.csv'})")


if __name__ == "__main__":
    main()
