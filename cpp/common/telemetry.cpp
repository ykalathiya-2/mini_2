#include "telemetry.hpp"

#include <cstdlib>
#include <filesystem>

namespace mini2 {

Telemetry& Telemetry::instance() {
    static Telemetry t;
    return t;
}

void Telemetry::init(const std::string& node) {
    std::lock_guard<std::mutex> lk(mu_);
    if (inited_) return;
    node_ = node;
    const char* env = std::getenv("MINI2_RUN_DIR");
    std::string base = env ? env : "logs/run-default";
    std::filesystem::create_directories(base);
    std::string path = base + "/telemetry-" + node + ".jsonl";
    f_.open(path, std::ios::app);
    // unitbuf flushes after each `<<` so tail -f works and a crash doesn't
    // lose the last few events.
    f_ << std::unitbuf;
    inited_ = true;
}

void Telemetry::emit_raw(const std::string& event, const std::string& kv_json) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!inited_) return;
    auto now_mono = std::chrono::steady_clock::now().time_since_epoch();
    auto now_wall = std::chrono::system_clock::now().time_since_epoch();
    long long t_ns    = std::chrono::duration_cast<std::chrono::nanoseconds>(now_mono).count();
    long long wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now_wall).count();
    f_ << "{\"t_ns\":"   << t_ns
       << ",\"wall_ns\":" << wall_ns
       << ",\"node\":\""  << node_ << "\""
       << ",\"event\":\"" << event << "\"";
    if (!kv_json.empty()) {
        f_ << "," << kv_json;
    }
    f_ << "}\n";
}

void Telemetry::Event::emit() const {
    Telemetry::instance().emit_raw(name_, buf_);
}

} // namespace mini2
