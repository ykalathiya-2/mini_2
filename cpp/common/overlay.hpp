#pragma once
// Tiny YAML-ish loader for config/overlay.yaml.  We parse a strict subset of
// the file by hand so we don't pull in a YAML dependency — the format is
// fixed and documented in overlay.yaml itself.
//
// A proper YAML lib would be nicer, but the spec says "minimize third-party
// libraries" and the format is under our control.

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini2 {

struct NodeSpec {
    std::string name;
    std::string host;
    int         port = 0;
    std::string team;
    std::string role;
    std::string impl;
    // Virtual-host id used by the latency interceptor: RPCs whose peer
    // sits on a different virtual_host get inter_host_latency added.
    std::string virtual_host = "h1";

    std::string endpoint() const { return host + ":" + std::to_string(port); }
};

struct NetworkSim {
    bool   simulate                 = false;
    double inter_host_latency_ms    = 0.0;
    double intra_host_latency_ms    = 0.0;
};

struct Chunking {
    int initial_rows = 64;
    int min_rows     = 16;
    int max_rows     = 4096;
    int target_chunk_ms = 25;
};

struct Scheduler {
    std::string mode = "weighted_round_robin";
    int max_concurrent_requests = 32;
};

struct Overlay {
    std::map<std::string, NodeSpec> nodes;
    std::vector<std::pair<std::string,std::string>> edges;
    // routing[src][dst] = next_hop
    std::map<std::string, std::map<std::string, std::string>> routing;
    std::vector<std::string> data_owners;
    Chunking   chunking;
    Scheduler  scheduler;
    NetworkSim network;

    std::vector<std::string> neighbors(const std::string& n) const;
    std::string next_hop(const std::string& src, const std::string& dst) const;
    bool has_node(const std::string& n) const { return nodes.count(n) > 0; }
};

// Loads overlay.yaml. Throws std::runtime_error on missing/malformed input.
// Honors MINI2_OVERLAY, MINI2_HOST_<NAME>, MINI2_PORT_<NAME> env overrides.
Overlay load_overlay(const std::string& path = "");

} // namespace mini2
