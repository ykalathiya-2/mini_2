#pragma once
// SoA partition store + phase-3 style parallel CSV loader and range scan.
//
// Layout matches the TaxiRow proto exactly so fill() is a column-by-column copy.
// All datetime fields are int64 Unix epoch seconds (UTC) — they are converted
// once at split time, so this loader only deals with numeric input.

#include <cstdint>
#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "mini2.pb.h"

namespace mini2 {

struct PartitionStore {
    // Numeric columns only — no per-row strings, no row_id.
    std::vector<int32_t> vendor_id;
    std::vector<int64_t> pickup_datetime;        // unix epoch seconds (UTC)
    std::vector<int64_t> dropoff_datetime;
    std::vector<int32_t> passenger_count;
    std::vector<double>  trip_distance;
    std::vector<int32_t> ratecode_id;
    std::vector<int32_t> pu_location_id;
    std::vector<int32_t> do_location_id;
    std::vector<uint8_t> store_and_fwd_flag;
    std::vector<int32_t> payment_type;
    std::vector<double>  fare_amount;
    std::vector<double>  extra;
    std::vector<double>  mta_tax;
    std::vector<double>  tip_amount;
    std::vector<double>  tolls_amount;
    std::vector<double>  improvement_surcharge;
    std::vector<double>  total_amount;
    std::vector<uint8_t> valid;                  // 1 if parse succeeded

    // Per-column (min, max) summary. Built once at startup by
    // compute_column_ranges(). Used as a vnode-level pre-filter: if a
    // query's predicate window doesn't intersect the column's [min, max],
    // this partition can't produce any matches and is skipped without
    // running the full range_search.
    std::map<std::string, std::pair<double, double>> column_ranges;

    std::size_t size() const { return valid.size(); }
    bool empty() const { return valid.empty(); }
    void resize(std::size_t n);

    // Fills a TaxiRow proto from index i.
    void fill(std::size_t i, ::mini2::TaxiRow& out) const;

    // Single-row predicate eval (used by smoke tests / fallback paths).
    bool matches(std::size_t i, const ::mini2::Query& q) const;

    // Parallel range scan — returns matched indices in original order.
    // Pre-resolves column pointers once so the inner loop has no string
    // comparisons. Uses OpenMP across rows.
    std::vector<std::size_t> range_search(const ::mini2::Query& q) const;

    // Computes (min, max) for every numeric column. OpenMP-parallel over
    // rows with a per-column reduction. Cheap relative to CSV load
    // (~1 column-scan vs. the full row parse) and runs once at startup.
    void compute_column_ranges();

    // Vnode-level pre-filter: returns false if any predicate's window
    // [low, high] lies entirely outside this partition's column range,
    // which means range_search would return zero rows. Returns true if
    // no info is available (column_ranges empty / unknown column) so
    // callers fall through to the full scan.
    bool predicate_can_match(const ::mini2::Query& q) const;
};

// mmap + two-pass OpenMP parallel CSV loader.
//   pass 1: each thread counts newlines in its byte slice (memchr/SIMD-fast)
//   pass 2: prefix-sum gives each thread its write offset; threads parse
//           rows directly into the SoA columns with no synchronisation.
PartitionStore load_partition_csv(const std::string& path);

} // namespace mini2
