#include "overlay.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace mini2 {

namespace {

// Strip trailing \r, leading whitespace counters.
int leading_spaces(const std::string& s) {
    int i = 0;
    while (i < (int)s.size() && s[i] == ' ') ++i;
    return i;
}

std::string rtrim(std::string s) {
    while (!s.empty() && (s.back() == '\r' || s.back() == '\n' || s.back() == ' '))
        s.pop_back();
    return s;
}

std::string strip_comment(const std::string& s) {
    auto p = s.find('#');
    if (p == std::string::npos) return s;
    // Allow '#' inside quoted strings — our config has none, so simple path is fine.
    return s.substr(0, p);
}

// Trim a string on both sides.
std::string trim(std::string s) {
    auto notsp = [](int c){ return !std::isspace(c); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notsp));
    s.erase(std::find_if(s.rbegin(), s.rend(), notsp).base(), s.end());
    return s;
}

// Remove matched outer quotes.
std::string dequote(std::string s) {
    s = trim(s);
    if (s.size() >= 2 &&
        ((s.front() == '"' && s.back() == '"') ||
         (s.front() == '\'' && s.back() == '\''))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

// Parses a flow-style mapping like {host: 127.0.0.1, port: 50051, ...}.
std::map<std::string, std::string> parse_flow_map(const std::string& body) {
    std::map<std::string, std::string> out;
    std::string inner = body;
    // Strip leading '{' / trailing '}'.
    auto lb = inner.find('{');
    auto rb = inner.rfind('}');
    if (lb == std::string::npos || rb == std::string::npos || rb <= lb)
        throw std::runtime_error("bad flow map: " + body);
    inner = inner.substr(lb + 1, rb - lb - 1);

    std::string tok;
    std::vector<std::string> parts;
    for (char c : inner) {
        if (c == ',') { parts.push_back(tok); tok.clear(); }
        else tok.push_back(c);
    }
    if (!tok.empty()) parts.push_back(tok);

    for (auto& p : parts) {
        auto colon = p.find(':');
        if (colon == std::string::npos) continue;
        auto k = trim(p.substr(0, colon));
        auto v = dequote(trim(p.substr(colon + 1)));
        out[k] = v;
    }
    return out;
}

std::vector<std::string> parse_flow_seq(const std::string& body) {
    std::vector<std::string> out;
    auto lb = body.find('[');
    auto rb = body.rfind(']');
    if (lb == std::string::npos || rb == std::string::npos) return out;
    std::string inner = body.substr(lb + 1, rb - lb - 1);
    std::string tok;
    for (char c : inner) {
        if (c == ',') { out.push_back(dequote(trim(tok))); tok.clear(); }
        else tok.push_back(c);
    }
    if (!trim(tok).empty()) out.push_back(dequote(trim(tok)));
    return out;
}

void apply_env_overrides(Overlay& o) {
    for (auto& [name, spec] : o.nodes) {
        if (const char* h = std::getenv(("MINI2_HOST_" + name).c_str()))
            spec.host = h;
        if (const char* p = std::getenv(("MINI2_PORT_" + name).c_str()))
            spec.port = std::atoi(p);
    }
}

} // anonymous namespace

std::vector<std::string> Overlay::neighbors(const std::string& n) const {
    std::vector<std::string> out;
    for (auto& [a, b] : edges) {
        if (a == n) out.push_back(b);
        else if (b == n) out.push_back(a);
    }
    return out;
}

std::string Overlay::next_hop(const std::string& src, const std::string& dst) const {
    auto it = routing.find(src);
    if (it == routing.end()) throw std::runtime_error("no routing for " + src);
    auto jt = it->second.find(dst);
    if (jt == it->second.end()) throw std::runtime_error("no route " + src + "->" + dst);
    return jt->second;
}

// Hand-rolled parser for the specific shape of our overlay.yaml.
// Supported at top level: nodes:, edges:, routing:, data_owners:, chunking:, scheduler:
Overlay load_overlay(const std::string& path) {
    std::string p = path;
    if (p.empty()) {
        if (const char* env = std::getenv("MINI2_OVERLAY")) p = env;
    }
    if (p.empty()) p = "config/overlay.yaml";

    std::ifstream f(p);
    if (!f) throw std::runtime_error("cannot open overlay: " + p);

    std::vector<std::string> lines;
    for (std::string line; std::getline(f, line); )
        lines.push_back(rtrim(strip_comment(line)));

    Overlay out;

    auto line_is_blank = [](const std::string& s) {
        return trim(s).empty();
    };

    // Top-level sections live at column 0.
    for (std::size_t i = 0; i < lines.size(); ) {
        const std::string& line = lines[i];
        if (line_is_blank(line)) { ++i; continue; }
        if (leading_spaces(line) != 0) { ++i; continue; }

        auto colon = line.find(':');
        if (colon == std::string::npos) { ++i; continue; }
        std::string key = trim(line.substr(0, colon));
        std::string rest = trim(line.substr(colon + 1));

        ++i;
        if (key == "nodes") {
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                std::string& nl = lines[i];
                auto c = nl.find(':');
                auto nm = trim(nl.substr(0, c));
                auto body = trim(nl.substr(c + 1));
                auto kv = parse_flow_map(body);
                NodeSpec ns;
                ns.name = nm;
                ns.host = kv["host"];
                ns.port = std::atoi(kv["port"].c_str());
                ns.team = kv["team"];
                ns.role = kv["role"];
                ns.impl = kv["impl"];
                auto vh = kv.find("virtual_host");
                if (vh != kv.end() && !vh->second.empty()) ns.virtual_host = vh->second;
                out.nodes[nm] = ns;
                ++i;
            }
        } else if (key == "edges") {
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                // "  - [A, B]"
                auto& el = lines[i];
                auto dash = el.find('-');
                if (dash == std::string::npos) { ++i; continue; }
                auto seq = parse_flow_seq(el.substr(dash + 1));
                if (seq.size() == 2) out.edges.emplace_back(seq[0], seq[1]);
                ++i;
            }
        } else if (key == "routing") {
            // Two nested layers. "A:" at indent 2, then either flow map
            // "{A: A, B: B, ...}" or block listing with indent 4.
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                if (leading_spaces(lines[i]) != 2) { ++i; continue; }
                auto& rl = lines[i];
                auto c = rl.find(':');
                std::string src = trim(rl.substr(0, c));
                std::string body = trim(rl.substr(c + 1));
                std::map<std::string, std::string> row;
                if (!body.empty() && body.front() == '{') {
                    row = parse_flow_map(body);
                    ++i;
                } else {
                    ++i;
                    while (i < lines.size() &&
                           (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 4)) {
                        if (line_is_blank(lines[i])) { ++i; continue; }
                        auto& il = lines[i];
                        auto c2 = il.find(':');
                        if (c2 != std::string::npos) {
                            row[trim(il.substr(0, c2))] =
                                dequote(trim(il.substr(c2 + 1)));
                        }
                        ++i;
                    }
                }
                out.routing[src] = std::move(row);
            }
        } else if (key == "data_owners") {
            if (!rest.empty()) {
                out.data_owners = parse_flow_seq(rest);
            }
        } else if (key == "chunking") {
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                auto& cl = lines[i];
                auto c = cl.find(':');
                std::string k = trim(cl.substr(0, c));
                std::string v = trim(cl.substr(c + 1));
                if      (k == "initial_rows")     out.chunking.initial_rows     = std::atoi(v.c_str());
                else if (k == "min_rows")         out.chunking.min_rows         = std::atoi(v.c_str());
                else if (k == "max_rows")         out.chunking.max_rows         = std::atoi(v.c_str());
                else if (k == "target_chunk_ms")  out.chunking.target_chunk_ms  = std::atoi(v.c_str());
                ++i;
            }
        } else if (key == "scheduler") {
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                auto& sl = lines[i];
                auto c = sl.find(':');
                std::string k = trim(sl.substr(0, c));
                std::string v = dequote(trim(sl.substr(c + 1)));
                if (k == "mode") out.scheduler.mode = v;
                else if (k == "max_concurrent_requests")
                    out.scheduler.max_concurrent_requests = std::atoi(v.c_str());
                ++i;
            }
        } else if (key == "network") {
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2)) {
                if (line_is_blank(lines[i])) { ++i; continue; }
                auto& nl = lines[i];
                auto c = nl.find(':');
                std::string k = trim(nl.substr(0, c));
                std::string v = dequote(trim(nl.substr(c + 1)));
                if      (k == "simulate")              out.network.simulate              = (v == "true" || v == "True" || v == "1");
                else if (k == "inter_host_latency_ms") out.network.inter_host_latency_ms = std::atof(v.c_str());
                else if (k == "intra_host_latency_ms") out.network.intra_host_latency_ms = std::atof(v.c_str());
                ++i;
            }
        } else {
            // Unknown top-level key — skip its children.
            while (i < lines.size() && (line_is_blank(lines[i]) || leading_spaces(lines[i]) >= 2))
                ++i;
        }
    }

    apply_env_overrides(out);
    if (out.nodes.empty())
        throw std::runtime_error("overlay parsed but nodes map is empty: " + p);
    return out;
}

} // namespace mini2
