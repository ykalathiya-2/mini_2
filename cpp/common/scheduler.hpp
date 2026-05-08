#pragma once
// Dynamic chunk sizer + weighted round-robin fair scheduler across
// concurrent request IDs on one node.
//
// This is the hand-rolled "above-unary" flow-control layer the spec asks for.

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace mini2 {

class ChunkSizer {
public:
    ChunkSizer(int initial, int min_rows, int max_rows, int target_ms)
        : initial_(initial), min_(min_rows), max_(max_rows),
          target_ms_(target_ms) {}

    // Called each time a consumer pulls. Returns the rows-per-chunk to serve.
    // If the consumer is pulling faster than target_ms, chunk grows; if slower,
    // it shrinks.  This gives us adaptive first-byte-fast + steady-state-big.
    int decide(const std::string& request_id, int client_hint);

    void forget(const std::string& request_id);

private:
    int initial_, min_, max_, target_ms_;
    struct State {
        int  cur       = 0;
        std::chrono::steady_clock::time_point last{};
        bool has_last  = false;
    };
    std::mutex mu_;
    std::unordered_map<std::string, State> s_;
};

// Weighted round-robin scheduler: every "tick" returns the next request_id
// that should get a production slot.  Supports weights so VIP requests get
// more CPU.  Fairness metric can be computed from observed completion times.
class FairScheduler {
public:
    explicit FairScheduler(std::string mode = "weighted_round_robin")
        : mode_(std::move(mode)) {}

    void add(const std::string& request_id, int weight = 1);
    void remove(const std::string& request_id);
    // Returns "" if no active requests.
    std::string next();
    std::size_t size() const;

private:
    std::string mode_;
    struct Entry { std::string rid; int weight; int credits; };
    mutable std::mutex mu_;
    std::vector<Entry> q_;
    std::size_t cursor_ = 0;
};

} // namespace mini2
