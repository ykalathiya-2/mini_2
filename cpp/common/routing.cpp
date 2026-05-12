#include "routing.hpp"

#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace mini2 {
namespace {

// --- minimal hand-rolled JSON reader -------------------------------------
// We only need to read manifest.json's known shape (a flat object with a few
// nested maps + arrays of numbers). Pulling in nlohmann/json would work but
// the spec says "minimize third-party libraries"; this keeps the dep
// surface tiny and matches the YAML-lite parser already in overlay.cpp.

struct Tok {
    enum Kind { Lbrace, Rbrace, Lbracket, Rbracket, Colon, Comma,
                Str, Num, True, False, Null, End };
    Kind        k;
    std::string s;
    double      num = 0.0;
};

struct Lex {
    const std::string& src;
    std::size_t        i = 0;
    explicit Lex(const std::string& s) : src(s) {}
    void ws() {
        while (i < src.size()
               && (src[i] == ' ' || src[i] == '\n' || src[i] == '\r'
                   || src[i] == '\t'))
            ++i;
    }
    Tok next() {
        ws();
        if (i >= src.size()) return {Tok::End, ""};
        char c = src[i];
        switch (c) {
            case '{': ++i; return {Tok::Lbrace,   "{"};
            case '}': ++i; return {Tok::Rbrace,   "}"};
            case '[': ++i; return {Tok::Lbracket, "["};
            case ']': ++i; return {Tok::Rbracket, "]"};
            case ':': ++i; return {Tok::Colon,    ":"};
            case ',': ++i; return {Tok::Comma,    ","};
            case '"': {
                ++i;
                std::string s;
                while (i < src.size() && src[i] != '"') {
                    if (src[i] == '\\' && i + 1 < src.size()) {
                        ++i;
                        switch (src[i]) {
                            case 'n': s += '\n'; break;
                            case 't': s += '\t'; break;
                            case '"': s += '"';  break;
                            default:  s += src[i];
                        }
                        ++i;
                    } else {
                        s += src[i++];
                    }
                }
                if (i < src.size()) ++i;
                return {Tok::Str, s};
            }
            default: {
                // number, true, false, null
                if (c == 't' && src.substr(i, 4) == "true")  { i += 4; return {Tok::True,  "true"}; }
                if (c == 'f' && src.substr(i, 5) == "false") { i += 5; return {Tok::False, "false"}; }
                if (c == 'n' && src.substr(i, 4) == "null")  { i += 4; return {Tok::Null,  "null"}; }
                std::size_t s = i;
                if (src[i] == '-' || src[i] == '+') ++i;
                while (i < src.size()
                       && ((src[i] >= '0' && src[i] <= '9')
                           || src[i] == '.' || src[i] == 'e' || src[i] == 'E'
                           || src[i] == '+' || src[i] == '-'))
                    ++i;
                std::string n = src.substr(s, i - s);
                Tok t{Tok::Num, n};
                try { t.num = std::stod(n); } catch (...) { t.num = 0; }
                return t;
            }
        }
    }
};

// Forward decls for recursive descent.
struct Value;
using Object = std::map<std::string, Value>;
using Array  = std::vector<Value>;
struct Value {
    int          kind = 0;   // 0=null,1=str,2=num,3=bool,4=obj,5=arr
    std::string  s;
    double       n    = 0;
    bool         b    = false;
    Object       obj;
    Array        arr;
};

Value parse_value(Lex& lex);

Value parse_object(Lex& lex) {
    Value v; v.kind = 4;
    Tok t = lex.next();
    if (t.k == Tok::Rbrace) return v;
    while (true) {
        if (t.k != Tok::Str) break;
        std::string key = t.s;
        Tok colon = lex.next();
        if (colon.k != Tok::Colon) break;
        Value child = parse_value(lex);
        v.obj[key] = std::move(child);
        Tok next = lex.next();
        if (next.k == Tok::Rbrace) break;
        if (next.k != Tok::Comma) break;
        t = lex.next();
    }
    return v;
}

Value parse_array(Lex& lex) {
    Value v; v.kind = 5;
    // Peek for empty-array case without copying Lex (it holds a reference).
    std::size_t saved = lex.i;
    Tok first = lex.next();
    if (first.k == Tok::Rbracket) return v;
    lex.i = saved;
    while (true) {
        Value child = parse_value(lex);
        v.arr.push_back(std::move(child));
        Tok next = lex.next();
        if (next.k == Tok::Rbracket) break;
        if (next.k != Tok::Comma)    break;
    }
    return v;
}

Value parse_value(Lex& lex) {
    Tok t = lex.next();
    Value v;
    switch (t.k) {
        case Tok::Lbrace:   return parse_object(lex);
        case Tok::Lbracket: return parse_array(lex);
        case Tok::Str:      v.kind = 1; v.s = t.s; return v;
        case Tok::Num:      v.kind = 2; v.n = t.num; return v;
        case Tok::True:     v.kind = 3; v.b = true;  return v;
        case Tok::False:    v.kind = 3; v.b = false; return v;
        default:            v.kind = 0; return v;
    }
}

Value parse_root(const std::string& src) {
    Lex lex(src);
    return parse_value(lex);
}

int month_of_epoch(int64_t epoch_s) {
    std::time_t t = static_cast<std::time_t>(epoch_s);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    return tm.tm_mon + 1;  // 1..12
}

// 32-bit FNV-1a hash of an integer's little-endian byte representation.
// Mirrors scripts/split_taxi_csv.py:fnv1a32 — keep them in sync.
uint32_t fnv1a32(uint32_t x) {
    uint32_t h = 2166136261u;
    for (int i = 0; i < 4; ++i) {
        h ^= (x & 0xffu);
        h *= 16777619u;
        x >>= 8;
    }
    return h;
}

} // anonymous

OwnerMetadata load_owner_metadata(const std::string& data_dir) {
    OwnerMetadata meta;
    std::string path = data_dir + "/manifest.json";
    std::ifstream f(path);
    if (!f) return meta;
    std::stringstream ss; ss << f.rdbuf();
    auto root = parse_root(ss.str());
    if (root.kind != 4) return meta;

    if (root.obj.count("scheme") && root.obj["scheme"].kind == 1)
        meta.scheme = root.obj["scheme"].s;

    if (root.obj.count("owner_dist_range")
        && root.obj["owner_dist_range"].kind == 4) {
        for (auto& [owner, val] : root.obj["owner_dist_range"].obj) {
            if (val.kind == 5 && val.arr.size() == 2
                && val.arr[0].kind == 2 && val.arr[1].kind == 2) {
                meta.dist_range[owner] = {val.arr[0].n, val.arr[1].n};
            }
        }
    }
    if (root.obj.count("owner_zones")
        && root.obj["owner_zones"].kind == 4) {
        for (auto& [owner, val] : root.obj["owner_zones"].obj) {
            if (val.kind != 5) continue;
            std::vector<int> zs;
            zs.reserve(val.arr.size());
            for (auto& z : val.arr) {
                if (z.kind == 2) zs.push_back(static_cast<int>(z.n));
            }
            std::sort(zs.begin(), zs.end());
            meta.zones[owner] = std::move(zs);
        }
    }
    if (root.obj.count("owner_months")
        && root.obj["owner_months"].kind == 4) {
        for (auto& [owner, val] : root.obj["owner_months"].obj) {
            if (val.kind != 5) continue;
            std::vector<int> ms;
            for (auto& m : val.arr) {
                if (m.kind == 2) ms.push_back(static_cast<int>(m.n));
            }
            std::sort(ms.begin(), ms.end());
            meta.months[owner] = std::move(ms);
        }
    }
    // Consistent-hash metadata.
    if (root.obj.count("hash_bucket_multiplier")
        && root.obj["hash_bucket_multiplier"].kind == 2) {
        meta.hash_bucket_multiplier =
            static_cast<int>(root.obj["hash_bucket_multiplier"].n);
    }
    if (root.obj.count("vnodes_per_owner")
        && root.obj["vnodes_per_owner"].kind == 2) {
        meta.vnodes_per_owner =
            static_cast<int>(root.obj["vnodes_per_owner"].n);
    }
    if (root.obj.count("total_vnodes")
        && root.obj["total_vnodes"].kind == 2) {
        meta.total_vnodes = static_cast<int>(root.obj["total_vnodes"].n);
    }
    if (root.obj.count("hash_column")
        && root.obj["hash_column"].kind == 1) {
        meta.hash_column = root.obj["hash_column"].s;
    }
    if (root.obj.count("owners")
        && root.obj["owners"].kind == 5) {
        for (auto& v : root.obj["owners"].arr) {
            if (v.kind == 1) meta.owners_order.push_back(v.s);
        }
    }
    return meta;
}

std::vector<std::string> eligible_owners(
    const std::vector<std::string>& all_owners,
    const Query&                    query,
    const OwnerMetadata&            meta)
{
    if (meta.empty() || query.predicates_size() == 0) return all_owners;

    // Find a predicate matching the manifest's clustering column (if any).
    auto find_pred = [&](const std::string& column) -> const RangePredicate* {
        for (int i = 0; i < query.predicates_size(); ++i) {
            if (query.predicates(i).column() == column)
                return &query.predicates(i);
        }
        return nullptr;
    };

    if (meta.scheme == "trip_distance") {
        const auto* pred = find_pred("trip_distance");
        if (!pred) return all_owners;
        double low = pred->low(), high = pred->high();
        std::vector<std::string> out;
        for (auto& o : all_owners) {
            auto it = meta.dist_range.find(o);
            if (it == meta.dist_range.end()) { out.push_back(o); continue; }
            double lo = it->second.first, hi = it->second.second;
            // [lo,hi] intersects [low,high] iff !(hi < low || lo > high)
            if (!(hi < low || lo > high)) out.push_back(o);
        }
        return out;
    }
    if (meta.scheme == "pu_location_id") {
        const auto* pred = find_pred("pu_location_id");
        if (!pred) return all_owners;
        int low  = static_cast<int>(pred->low());
        int high = static_cast<int>(pred->high());
        std::vector<std::string> out;
        for (auto& o : all_owners) {
            auto it = meta.zones.find(o);
            if (it == meta.zones.end()) { out.push_back(o); continue; }
            // Owner is eligible if any of its zones falls in [low, high].
            bool hit = false;
            for (int z : it->second) {
                if (z >= low && z <= high) { hit = true; break; }
            }
            if (hit) out.push_back(o);
        }
        return out;
    }
    if (meta.scheme == "consistent_hash") {
        // Routing: enumerate every bucket in the predicate range, hash to a
        // vnode, map vnode to its striped owner. Cap the iteration to avoid
        // unbounded work — for ranges that span more than `total_vnodes *
        // 4` buckets we fall back to "all owners" (every vnode is hit
        // anyway with high probability).
        if (meta.vnodes_per_owner == 0 || meta.total_vnodes == 0
            || meta.owners_order.empty())
            return all_owners;
        const auto* pred = find_pred(meta.hash_column);
        if (!pred) return all_owners;
        long long b_lo = static_cast<long long>(
            pred->low()  * meta.hash_bucket_multiplier);
        long long b_hi = static_cast<long long>(
            pred->high() * meta.hash_bucket_multiplier);
        if (b_lo > b_hi) std::swap(b_lo, b_hi);
        if (b_lo < 0) b_lo = 0;
        long long span = b_hi - b_lo + 1;
        long long cap  = (long long)meta.total_vnodes * 4;
        if (span > cap) return all_owners;
        std::set<std::string> hit;
        const int n_owners = (int)meta.owners_order.size();
        for (long long b = b_lo; b <= b_hi; ++b) {
            uint32_t v = fnv1a32(static_cast<uint32_t>(b))
                       % static_cast<uint32_t>(meta.total_vnodes);
            int owner_idx = (int)v / meta.vnodes_per_owner;
            if (owner_idx >= n_owners) owner_idx = n_owners - 1;
            hit.insert(meta.owners_order[owner_idx]);
            if ((int)hit.size() >= n_owners) break;  // already saturated
        }
        std::vector<std::string> out;
        for (auto& o : all_owners) if (hit.count(o)) out.push_back(o);
        return out.empty() ? all_owners : out;
    }
    if (meta.scheme == "pickup_datetime") {
        const auto* pred = find_pred("pickup_datetime");
        if (!pred) return all_owners;
        int m_lo = month_of_epoch(static_cast<int64_t>(pred->low()));
        int m_hi = month_of_epoch(static_cast<int64_t>(pred->high()));
        if (m_lo > m_hi) std::swap(m_lo, m_hi);
        std::vector<std::string> out;
        for (auto& o : all_owners) {
            auto it = meta.months.find(o);
            if (it == meta.months.end()) { out.push_back(o); continue; }
            bool hit = false;
            for (int m : it->second) {
                if (m >= m_lo && m <= m_hi) { hit = true; break; }
            }
            if (hit) out.push_back(o);
        }
        return out;
    }
    return all_owners;
}

} // namespace mini2
