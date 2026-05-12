#include "csv_store.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <limits>
#include <fcntl.h>
#include <stdexcept>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace mini2 {

void PartitionStore::resize(std::size_t n) {
    vendor_id.resize(n);
    pickup_datetime.resize(n);
    dropoff_datetime.resize(n);
    passenger_count.resize(n);
    trip_distance.resize(n);
    ratecode_id.resize(n);
    pu_location_id.resize(n);
    do_location_id.resize(n);
    store_and_fwd_flag.resize(n);
    payment_type.resize(n);
    fare_amount.resize(n);
    extra.resize(n);
    mta_tax.resize(n);
    tip_amount.resize(n);
    tolls_amount.resize(n);
    improvement_surcharge.resize(n);
    total_amount.resize(n);
    valid.resize(n);
}

void PartitionStore::fill(std::size_t i, ::mini2::TaxiRow& out) const {
    out.set_vendor_id(vendor_id[i]);
    out.set_pickup_datetime(pickup_datetime[i]);
    out.set_dropoff_datetime(dropoff_datetime[i]);
    out.set_passenger_count(passenger_count[i]);
    out.set_trip_distance(trip_distance[i]);
    out.set_ratecode_id(ratecode_id[i]);
    out.set_pu_location_id(pu_location_id[i]);
    out.set_do_location_id(do_location_id[i]);
    out.set_store_and_fwd_flag(store_and_fwd_flag[i] != 0);
    out.set_payment_type(payment_type[i]);
    out.set_fare_amount(fare_amount[i]);
    out.set_extra(extra[i]);
    out.set_mta_tax(mta_tax[i]);
    out.set_tip_amount(tip_amount[i]);
    out.set_tolls_amount(tolls_amount[i]);
    out.set_improvement_surcharge(improvement_surcharge[i]);
    out.set_total_amount(total_amount[i]);
}

// ---------------------------------------------------------------------------
// Column resolver: maps a column name to a (base ptr, kind) pair so the
// hot loop can read a value without any string compares.
// ---------------------------------------------------------------------------

namespace {

enum class ColKind : uint8_t { I32, I64, F64, U8 };

struct ColRef {
    const void* data = nullptr;
    ColKind kind     = ColKind::I32;
};

ColRef resolve_col(const PartitionStore& s, const std::string& col) {
    if (col == "vendor_id")             return {s.vendor_id.data(),             ColKind::I32};
    if (col == "pickup_datetime")       return {s.pickup_datetime.data(),       ColKind::I64};
    if (col == "dropoff_datetime")      return {s.dropoff_datetime.data(),      ColKind::I64};
    if (col == "passenger_count")       return {s.passenger_count.data(),       ColKind::I32};
    if (col == "trip_distance")         return {s.trip_distance.data(),         ColKind::F64};
    if (col == "ratecode_id")           return {s.ratecode_id.data(),           ColKind::I32};
    if (col == "pu_location_id")        return {s.pu_location_id.data(),        ColKind::I32};
    if (col == "do_location_id")        return {s.do_location_id.data(),        ColKind::I32};
    if (col == "store_and_fwd_flag")    return {s.store_and_fwd_flag.data(),    ColKind::U8};
    if (col == "payment_type")          return {s.payment_type.data(),          ColKind::I32};
    if (col == "fare_amount")           return {s.fare_amount.data(),           ColKind::F64};
    if (col == "extra")                 return {s.extra.data(),                 ColKind::F64};
    if (col == "mta_tax")               return {s.mta_tax.data(),               ColKind::F64};
    if (col == "tip_amount")            return {s.tip_amount.data(),            ColKind::F64};
    if (col == "tolls_amount")          return {s.tolls_amount.data(),          ColKind::F64};
    if (col == "improvement_surcharge") return {s.improvement_surcharge.data(), ColKind::F64};
    if (col == "total_amount")          return {s.total_amount.data(),          ColKind::F64};
    return {};
}

inline double read_val(const ColRef& r, std::size_t i) {
    switch (r.kind) {
        case ColKind::F64: return static_cast<const double*>(r.data)[i];
        case ColKind::I64: return static_cast<double>(static_cast<const int64_t*>(r.data)[i]);
        case ColKind::I32: return static_cast<double>(static_cast<const int32_t*>(r.data)[i]);
        case ColKind::U8:  return static_cast<double>(static_cast<const uint8_t*>(r.data)[i]);
    }
    return 0.0;
}

} // anonymous

bool PartitionStore::matches(std::size_t i, const ::mini2::Query& q) const {
    if (i >= valid.size() || !valid[i]) return false;
    for (const auto& p : q.predicates()) {
        ColRef r = resolve_col(*this, p.column());
        if (!r.data) return false;
        double v = read_val(r, i);
        if (p.inclusive()) {
            if (!(v >= p.low() && v <= p.high())) return false;
        } else {
            if (!(v >  p.low() && v <  p.high())) return false;
        }
    }
    return true;
}

std::vector<std::size_t> PartitionStore::range_search(const ::mini2::Query& q) const {
    const std::size_t n = valid.size();
    if (n == 0) return {};

    struct ResolvedPred {
        ColRef col;
        double low;
        double high;
        bool   inclusive;
    };
    std::vector<ResolvedPred> preds;
    preds.reserve(q.predicates().size());
    for (const auto& p : q.predicates()) {
        ColRef cr = resolve_col(*this, p.column());
        if (!cr.data) return {};        // unknown column -> empty result
        preds.push_back({cr, p.low(), p.high(), p.inclusive()});
    }

    const uint8_t* valid_ptr = valid.data();

#ifdef _OPENMP
    int nt = omp_get_max_threads();
#else
    int nt = 1;
#endif

    // Per-thread output buffers. With `schedule(static)` each thread covers a
    // contiguous range, so within a thread the indices are already ascending.
    // Concatenating in tid order gives globally-sorted output without an
    // O(n log n) sort.
    std::vector<std::vector<std::size_t>> per_thread(nt);

    #pragma omp parallel num_threads(nt)
    {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        auto& local = per_thread[tid];
        local.reserve(n / static_cast<std::size_t>(nt) / 16 + 64);

        #pragma omp for schedule(static) nowait
        for (std::size_t i = 0; i < n; ++i) {
            if (!valid_ptr[i]) continue;
            bool match = true;
            for (const auto& pr : preds) {
                double v = read_val(pr.col, i);
                bool hit = pr.inclusive ? (v >= pr.low && v <= pr.high)
                                        : (v >  pr.low && v <  pr.high);
                if (!hit) { match = false; break; }
            }
            if (match) local.push_back(i);
        }
    }

    std::size_t total = 0;
    for (auto& v : per_thread) total += v.size();
    std::vector<std::size_t> result;
    result.reserve(total);
    for (auto& v : per_thread) {
        result.insert(result.end(), v.begin(), v.end());
    }
    return result;
}

// ---------------------------------------------------------------------------
// Vnode range-table construction + predicate pre-filter.
// ---------------------------------------------------------------------------

namespace {

template <typename T>
std::pair<double, double> minmax_col(const std::vector<T>& col,
                                     const std::vector<uint8_t>& valid) {
    const std::size_t n = col.size();
    double lo =  std::numeric_limits<double>::infinity();
    double hi = -std::numeric_limits<double>::infinity();
    #pragma omp parallel for reduction(min:lo) reduction(max:hi) schedule(static)
    for (std::size_t i = 0; i < n; ++i) {
        if (!valid[i]) continue;
        double v = static_cast<double>(col[i]);
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    return {lo, hi};
}

} // anonymous

void PartitionStore::compute_column_ranges() {
    if (valid.empty()) return;

    const auto& V = valid;
    column_ranges["vendor_id"]              = minmax_col(vendor_id, V);
    column_ranges["pickup_datetime"]        = minmax_col(pickup_datetime, V);
    column_ranges["dropoff_datetime"]       = minmax_col(dropoff_datetime, V);
    column_ranges["passenger_count"]        = minmax_col(passenger_count, V);
    column_ranges["trip_distance"]          = minmax_col(trip_distance, V);
    column_ranges["ratecode_id"]            = minmax_col(ratecode_id, V);
    column_ranges["pu_location_id"]         = minmax_col(pu_location_id, V);
    column_ranges["do_location_id"]         = minmax_col(do_location_id, V);
    column_ranges["store_and_fwd_flag"]     = minmax_col(store_and_fwd_flag, V);
    column_ranges["payment_type"]           = minmax_col(payment_type, V);
    column_ranges["fare_amount"]            = minmax_col(fare_amount, V);
    column_ranges["extra"]                  = minmax_col(extra, V);
    column_ranges["mta_tax"]                = minmax_col(mta_tax, V);
    column_ranges["tip_amount"]             = minmax_col(tip_amount, V);
    column_ranges["tolls_amount"]           = minmax_col(tolls_amount, V);
    column_ranges["improvement_surcharge"]  = minmax_col(improvement_surcharge, V);
    column_ranges["total_amount"]           = minmax_col(total_amount, V);

    // Drop columns where every row was invalid (lo stayed +inf).
    for (auto it = column_ranges.begin(); it != column_ranges.end(); ) {
        if (it->second.first > it->second.second) it = column_ranges.erase(it);
        else ++it;
    }
}

bool PartitionStore::predicate_can_match(const ::mini2::Query& q) const {
    if (column_ranges.empty()) return true;
    for (const auto& p : q.predicates()) {
        auto it = column_ranges.find(p.column());
        if (it == column_ranges.end()) continue;
        double lo = it->second.first;
        double hi = it->second.second;
        // [lo, hi] must intersect the predicate window. If the predicate is
        // strictly below the partition's min or strictly above its max, no
        // row in this partition can satisfy it.
        if (p.high() < lo || p.low() > hi) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// CSV parsing helpers (mmap-friendly, zero per-row alloc).
// ---------------------------------------------------------------------------

namespace {

inline std::string_view next_field(const char*& pos, const char* end) {
    const char* start = pos;
    const char* comma = static_cast<const char*>(
        std::memchr(pos, ',', static_cast<std::size_t>(end - pos)));
    if (comma) {
        pos = comma + 1;
        return {start, static_cast<std::size_t>(comma - start)};
    }
    pos = end;
    return {start, static_cast<std::size_t>(end - start)};
}

inline bool parse_i32(std::string_view sv, int32_t& out) {
    if (sv.empty()) { out = 0; return true; }
    auto [p, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out, 10);
    return ec == std::errc{} && p == sv.data() + sv.size();
}
inline bool parse_i64(std::string_view sv, int64_t& out) {
    if (sv.empty()) { out = 0; return true; }
    auto [p, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out, 10);
    return ec == std::errc{} && p == sv.data() + sv.size();
}
inline bool parse_f64(std::string_view sv, double& out) {
    if (sv.empty()) { out = 0.0; return true; }
    auto [p, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), out);
    return ec == std::errc{} && p == sv.data() + sv.size();
}

} // anonymous

PartitionStore load_partition_csv(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("cannot open CSV: " + path);

    struct stat st{};
    if (fstat(fd, &st) != 0) { ::close(fd); throw std::runtime_error("fstat failed: " + path); }
    auto file_size = static_cast<std::size_t>(st.st_size);
    if (file_size == 0) { ::close(fd); return {}; }

    const char* mapped = static_cast<const char*>(
        ::mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    ::close(fd);
    if (mapped == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);

#ifdef MADV_SEQUENTIAL
    ::madvise(const_cast<char*>(mapped), file_size, MADV_SEQUENTIAL);
#endif

    const char* file_end = mapped + file_size;

    // Skip header line.
    const char* header_end = static_cast<const char*>(
        std::memchr(mapped, '\n', file_size));
    if (!header_end) {
        ::munmap(const_cast<char*>(mapped), file_size);
        throw std::runtime_error("CSV has no header newline: " + path);
    }
    const char* data_start = header_end + 1;
    std::size_t data_size  = static_cast<std::size_t>(file_end - data_start);

#ifdef _OPENMP
    int nt = omp_get_max_threads();
#else
    int nt = 1;
#endif

    // Line-aligned chunk boundaries: chunk[i] is the first byte of a line.
    std::vector<const char*> chunk_starts(nt + 1);
    chunk_starts[0] = data_start;
    for (int i = 1; i < nt; ++i) {
        const char* raw = data_start + (data_size * static_cast<std::size_t>(i)
                                        / static_cast<std::size_t>(nt));
        if (raw >= file_end) { chunk_starts[i] = file_end; continue; }
        const char* nl = static_cast<const char*>(
            std::memchr(raw, '\n', static_cast<std::size_t>(file_end - raw)));
        chunk_starts[i] = nl ? nl + 1 : file_end;
    }
    chunk_starts[nt] = file_end;

    // Pass 1: count newlines per chunk.
    std::vector<std::size_t> counts(nt, 0);
    #pragma omp parallel num_threads(nt)
    {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        const char* p   = chunk_starts[tid];
        const char* end = chunk_starts[tid + 1];
        std::size_t nl = 0;
        while (p < end) {
            const char* found = static_cast<const char*>(
                std::memchr(p, '\n', static_cast<std::size_t>(end - p)));
            if (!found) break;
            ++nl;
            p = found + 1;
        }
        counts[tid] = nl;
    }

    std::vector<std::size_t> offsets(nt + 1, 0);
    for (int i = 0; i < nt; ++i) offsets[i + 1] = offsets[i] + counts[i];

    PartitionStore store;
    store.resize(offsets[nt]);

    // Pass 2: parse directly into SoA columns at each thread's offset.
    #pragma omp parallel num_threads(nt)
    {
#ifdef _OPENMP
        int tid = omp_get_thread_num();
#else
        int tid = 0;
#endif
        const char* pos  = chunk_starts[tid];
        const char* cend = chunk_starts[tid + 1];
        std::size_t idx  = offsets[tid];
        std::size_t cap  = offsets[tid + 1];

        while (pos < cend && idx < cap) {
            const char* nl = static_cast<const char*>(
                std::memchr(pos, '\n', static_cast<std::size_t>(cend - pos)));
            const char* line_end = nl ? nl : cend;
            if (line_end == pos) { pos = line_end + 1; continue; }

            const char* row_end = line_end;
            if (row_end > pos && *(row_end - 1) == '\r') --row_end;

            const char* p = pos;
            const char* e = row_end;
            auto fld = [&]() { return next_field(p, e); };

            bool ok = true;
            ok &= parse_i32(fld(), store.vendor_id[idx]);
            ok &= parse_i64(fld(), store.pickup_datetime[idx]);
            ok &= parse_i64(fld(), store.dropoff_datetime[idx]);
            ok &= parse_i32(fld(), store.passenger_count[idx]);
            ok &= parse_f64(fld(), store.trip_distance[idx]);
            ok &= parse_i32(fld(), store.ratecode_id[idx]);

            // store_and_fwd_flag is a single char: 'Y' or 'N'.
            std::string_view fwd = fld();
            store.store_and_fwd_flag[idx] = (!fwd.empty() && (fwd.front() == 'Y' || fwd.front() == 'y')) ? 1 : 0;

            ok &= parse_i32(fld(), store.pu_location_id[idx]);
            ok &= parse_i32(fld(), store.do_location_id[idx]);
            ok &= parse_i32(fld(), store.payment_type[idx]);
            ok &= parse_f64(fld(), store.fare_amount[idx]);
            ok &= parse_f64(fld(), store.extra[idx]);
            ok &= parse_f64(fld(), store.mta_tax[idx]);
            ok &= parse_f64(fld(), store.tip_amount[idx]);
            ok &= parse_f64(fld(), store.tolls_amount[idx]);
            ok &= parse_f64(fld(), store.improvement_surcharge[idx]);
            ok &= parse_f64(fld(), store.total_amount[idx]);

            store.valid[idx] = ok ? 1 : 0;
            ++idx;
            pos = line_end + 1;
        }
    }

    // mmap can be released — every value has been copied into vectors.
    ::munmap(const_cast<char*>(mapped), file_size);
    return store;
}

} // namespace mini2
