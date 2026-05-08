#pragma once
// Per-node telemetry writer (mirror of py/server/telemetry.py).
// Emits one JSON object per line to $MINI2_RUN_DIR/telemetry-<node>.jsonl.
// Thread-safe; the underlying ofstream is held under a single mutex.

#include <chrono>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>

namespace mini2 {

class Telemetry {
public:
    static Telemetry& instance();

    // Must be called once at startup. Picks $MINI2_RUN_DIR; falls back to
    // ./logs/run-default/. Idempotent.
    void init(const std::string& node);

    // Emit an event with arbitrary k=v fields. Field values must be valid
    // JSON literals (numbers, true/false, "quoted strings"). The helper
    // overloads below take care of that for common types.
    void emit_raw(const std::string& event, const std::string& kv_json);

    // Convenience builder so call sites don't manually format JSON.
    class Event {
    public:
        explicit Event(const std::string& name) : name_(name) {}
        Event& f(const std::string& k, long long v)  { add_kv(k, std::to_string(v));        return *this; }
        Event& f(const std::string& k, int v)        { add_kv(k, std::to_string(v));        return *this; }
        Event& f(const std::string& k, std::size_t v){ add_kv(k, std::to_string(v));        return *this; }
        Event& f(const std::string& k, double v)     { add_kv(k, std::to_string(v));        return *this; }
        Event& f(const std::string& k, bool v)       { add_kv(k, v ? "true" : "false");     return *this; }
        Event& f(const std::string& k, const std::string& v) {
            std::string esc;
            esc.reserve(v.size() + 2);
            esc += '"';
            for (char c : v) {
                if (c == '"' || c == '\\') { esc += '\\'; esc += c; }
                else if (c == '\n')        { esc += "\\n"; }
                else                       { esc += c; }
            }
            esc += '"';
            add_kv(k, esc);
            return *this;
        }
        Event& f(const std::string& k, const char* v) { return f(k, std::string(v)); }
        void emit() const;
    private:
        void add_kv(const std::string& k, const std::string& v) {
            if (!buf_.empty()) buf_ += ',';
            buf_ += '"'; buf_ += k; buf_ += "\":"; buf_ += v;
        }
        std::string name_;
        std::string buf_;
    };

private:
    Telemetry() = default;
    Telemetry(const Telemetry&) = delete;
    Telemetry& operator=(const Telemetry&) = delete;

    std::ofstream f_;
    std::mutex    mu_;
    std::string   node_;
    bool          inited_ = false;
};

} // namespace mini2
