// Mini 2 — C++ client.  Talks ONLY to gateway node A.
// Flow:
//   1. SubmitQuery(rid, predicates)  → QueryAck
//   2. Loop: FetchChunk(rid)         → Chunk until is_last=true
//   3. Optional CancelQuery(rid)
//
// Demonstrates: request/response pacing, dynamic chunking, cancel, and
// fair concurrency via --concurrency N running N requests in parallel threads.

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "mini2.grpc.pb.h"
#include "overlay.hpp"

using grpc::ClientContext;
using grpc::Status;

namespace {

std::string rand_id() {
    static std::atomic<uint64_t> ctr{0};
    std::ostringstream ss;
    ss << "req-" << std::chrono::steady_clock::now().time_since_epoch().count()
       << "-" << ++ctr;
    return ss.str();
}

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Format Unix epoch seconds (UTC) as "YYYY-MM-DD HH:MM:SS".
std::string fmt_dt(int64_t epoch_s) {
    std::time_t t = static_cast<std::time_t>(epoch_s);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    return buf;
}

// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" as UTC epoch seconds.
int64_t parse_iso_dt(const std::string& s) {
    std::tm tm{};
    if (s.size() == 10) {
        std::sscanf(s.c_str(), "%d-%d-%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday);
    } else {
        std::sscanf(s.c_str(), "%d-%d-%d %d:%d:%d",
                    &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                    &tm.tm_hour, &tm.tm_min, &tm.tm_sec);
    }
    tm.tm_year -= 1900;
    tm.tm_mon  -= 1;
    return static_cast<int64_t>(timegm(&tm));
}

struct CliArgs {
    std::string overlay_path;
    std::string gateway_name = "A";
    std::string column = "trip_distance";
    double low = 0.0, high = 1e9;
    bool  inclusive = true;
    int   concurrency = 1;
    int   max_rows_hint = 0; // 0 = dynamic
    bool  cancel_after = false;
    int   cancel_after_chunks = 2;
    bool  quiet = false;
    int   limit_chunks = -1; // stop after this many chunks (-1 = run to last)
    bool  json = false;      // emit per-request summary as JSON
    int   print_rows = 0;    // pretty-print first N matched rows
};

CliArgs parse(int argc, char** argv) {
    CliArgs a;
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        auto need = [&](const std::string& k){ if (i + 1 >= argc) throw std::runtime_error("need value for " + k); return std::string(argv[++i]); };
        if      (s == "--overlay")      a.overlay_path = need(s);
        else if (s == "--gateway")      a.gateway_name = need(s);
        else if (s == "--column")       a.column = need(s);
        else if (s == "--low")          a.low  = std::stod(need(s));
        else if (s == "--high")         a.high = std::stod(need(s));
        else if (s == "--low-dt")       a.low  = static_cast<double>(parse_iso_dt(need(s)));
        else if (s == "--high-dt")      a.high = static_cast<double>(parse_iso_dt(need(s)));
        else if (s == "--print")        a.print_rows = std::stoi(need(s));
        else if (s == "--exclusive")    a.inclusive = false;
        else if (s == "--concurrency")  a.concurrency = std::stoi(need(s));
        else if (s == "--max-rows")     a.max_rows_hint = std::stoi(need(s));
        else if (s == "--cancel-after") { a.cancel_after = true; a.cancel_after_chunks = std::stoi(need(s)); }
        else if (s == "--limit-chunks") a.limit_chunks = std::stoi(need(s));
        else if (s == "--quiet" || s == "-q") a.quiet = true;
        else if (s == "--json")         a.json = true;
        else if (s == "--help" || s == "-h") {
            std::cout << "mini2_client [--overlay PATH] [--gateway A] [--column C] "
                         "[--low N] [--high N] [--low-dt 'YYYY-MM-DD HH:MM:SS'] "
                         "[--high-dt 'YYYY-MM-DD HH:MM:SS'] [--exclusive] "
                         "[--concurrency N] [--max-rows N] [--cancel-after K] "
                         "[--limit-chunks K] [--print N] [--quiet] [--json]\n";
            std::exit(0);
        }
        else { std::cerr << "unknown arg: " << s << "\n"; std::exit(2); }
    }
    return a;
}

struct ReqResult {
    std::string rid;
    int64_t     start_ns = 0;
    int64_t     first_chunk_ns = 0;
    int64_t     end_ns = 0;
    int64_t     rows  = 0;
    int         chunks = 0;
    int64_t     bytes = 0;          // total wire bytes received in chunks
    int64_t     bytes_min = 0;      // smallest non-empty chunk
    int64_t     bytes_max = 0;      // largest chunk
    int         owners_hit = 0;     // owners that returned ≥1 row (last chunk)
    int         owners_eligible = 0; // owners post smart-routing (last chunk)
    bool        ok = false;
    std::string err;
};

ReqResult run_one(const CliArgs& a, const mini2::Overlay& ov,
                  std::shared_ptr<grpc::Channel> ch, int idx) {
    ReqResult r;
    r.rid = rand_id();
    mini2::ClientGateway::Stub stub(ch);

    mini2::Query q;
    q.set_request_id(r.rid);
    q.set_origin("client");
    auto* p = q.add_predicates();
    p->set_column(a.column);
    p->set_low(a.low);
    p->set_high(a.high);
    p->set_inclusive(a.inclusive);

    r.start_ns = now_ns();
    mini2::QueryAck ack;
    {
        ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        Status s = stub.SubmitQuery(&ctx, q, &ack);
        if (!s.ok() || !ack.accepted()) {
            r.err = s.ok() ? ack.reject_reason() : s.error_message();
            return r;
        }
    }
    if (!a.quiet)
        std::cerr << "[client#" << idx << "] submitted " << r.rid
                  << "; predicate " << a.column
                  << " in [" << a.low << "," << a.high << "]\n";

    // Pull loop.
    while (true) {
        mini2::PullRequest pr;
        pr.set_request_id(r.rid);
        pr.set_max_rows(a.max_rows_hint);
        pr.set_last_seq(r.chunks - 1);

        mini2::Chunk ch_msg;
        ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        Status s = stub.FetchChunk(&ctx, pr, &ch_msg);
        if (!s.ok()) {
            r.err = "FetchChunk: " + s.error_message();
            return r;
        }
        if (r.chunks == 0) r.first_chunk_ns = now_ns();
        r.chunks += 1;
        r.rows   += ch_msg.rows_size();
        int64_t this_bytes = static_cast<int64_t>(ch_msg.ByteSizeLong());
        r.bytes  += this_bytes;
        if (ch_msg.rows_size() > 0) {
            if (r.bytes_min == 0 || this_bytes < r.bytes_min) r.bytes_min = this_bytes;
            if (this_bytes > r.bytes_max) r.bytes_max = this_bytes;
        }
        if (!a.quiet) {
            std::cerr << "[client#" << idx << "] chunk seq=" << ch_msg.seq()
                      << " rows=" << ch_msg.rows_size()
                      << " backlog=" << ch_msg.backlog()
                      << " is_last=" << ch_msg.is_last()
                      << " producer=" << ch_msg.producer() << "\n";
        }
        // Pretty-print up to print_rows rows total (datetime as UTC ISO).
        if (a.print_rows > 0) {
            int already = static_cast<int>(r.rows - ch_msg.rows_size());
            int budget  = std::max(0, a.print_rows - already);
            int to_show = std::min(budget, ch_msg.rows_size());
            for (int j = 0; j < to_show; ++j) {
                const auto& row = ch_msg.rows(j);
                std::cout << "  vendor=" << row.vendor_id()
                          << " pickup="  << fmt_dt(row.pickup_datetime())
                          << " dropoff=" << fmt_dt(row.dropoff_datetime())
                          << " dist="    << row.trip_distance()
                          << " fare="    << row.fare_amount()
                          << " total="   << row.total_amount()
                          << " pu="      << row.pu_location_id()
                          << " do="      << row.do_location_id()
                          << "\n";
            }
        }

        if (a.cancel_after && r.chunks >= a.cancel_after_chunks) {
            mini2::CancelRequest cr; cr.set_request_id(r.rid); cr.set_reason("client test cancel");
            mini2::CancelResponse cresp;
            ClientContext cctx;
            cctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
            stub.CancelQuery(&cctx, cr, &cresp);
            if (!a.quiet)
                std::cerr << "[client#" << idx << "] cancelled; dropped="
                          << cresp.rows_dropped() << "\n";
            r.ok = true;
            r.end_ns = now_ns();
            return r;
        }

        if (a.limit_chunks >= 0 && r.chunks >= a.limit_chunks) {
            // Simulate disconnect by just returning without cancel; server
            // will reclaim on idle.
            r.ok = true;
            r.end_ns = now_ns();
            if (!a.quiet)
                std::cerr << "[client#" << idx << "] abandon after "
                          << r.chunks << " chunks (no cancel)\n";
            return r;
        }

        if (ch_msg.is_last()) {
            r.owners_hit      = ch_msg.owners_hit();
            r.owners_eligible = ch_msg.owners_eligible();
            break;
        }
    }
    r.ok = true;
    r.end_ns = now_ns();
    return r;
}

} // anonymous

int main(int argc, char** argv) {
    auto a = parse(argc, argv);
    auto ov = mini2::load_overlay(a.overlay_path);
    if (!ov.has_node(a.gateway_name)) {
        std::cerr << "unknown gateway node: " << a.gateway_name << '\n';
        return 2;
    }
    auto ep = ov.nodes.at(a.gateway_name).endpoint();
    std::cerr << "[client] connecting to " << a.gateway_name << " at " << ep << '\n';

    grpc::ChannelArguments cargs;
    cargs.SetMaxSendMessageSize(64 * 1024 * 1024);
    cargs.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    auto ch = grpc::CreateCustomChannel(
        ep, grpc::InsecureChannelCredentials(), cargs);

    // Healthcheck.
    {
        mini2::ClientGateway::Stub stub(ch);
        mini2::Heartbeat hb; hb.set_from_node("client"); hb.set_sent_at_ns(now_ns());
        mini2::HeartbeatAck ack;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        auto s = stub.Ping(&ctx, hb, &ack);
        if (!s.ok()) {
            std::cerr << "gateway unreachable: " << s.error_message() << '\n';
            return 1;
        }
    }

    std::vector<std::thread> threads;
    std::vector<ReqResult> results(a.concurrency);
    for (int i = 0; i < a.concurrency; ++i) {
        threads.emplace_back([&, i]{ results[i] = run_one(a, ov, ch, i); });
    }
    for (auto& t : threads) t.join();

    // Summary.
    int ok = 0, fail = 0;
    int64_t total_rows = 0;
    for (auto& r : results) {
        if (r.ok) ++ok; else ++fail;
        total_rows += r.rows;
    }
    std::cerr << "=== client summary: " << ok << " ok / " << fail
              << " fail, total_rows=" << total_rows << " ===\n";
    for (auto& r : results) {
        double ms_total = (r.end_ns - r.start_ns) / 1e6;
        double ms_first = (r.first_chunk_ns - r.start_ns) / 1e6;
        if (a.json) {
            int64_t bytes_avg = r.chunks > 0 ? r.bytes / r.chunks : 0;
            std::cout << "{\"rid\":\"" << r.rid << "\","
                      << "\"ok\":" << (r.ok ? "true" : "false") << ","
                      << "\"rows\":" << r.rows << ","
                      << "\"chunks\":" << r.chunks << ","
                      << "\"bytes\":" << r.bytes << ","
                      << "\"bytes_avg_per_chunk\":" << bytes_avg << ","
                      << "\"bytes_min_per_chunk\":" << r.bytes_min << ","
                      << "\"bytes_max_per_chunk\":" << r.bytes_max << ","
                      << "\"ms_total\":" << ms_total << ","
                      << "\"ms_first_chunk\":" << ms_first << ","
                      << "\"owners_hit\":" << r.owners_hit << ","
                      << "\"owners_eligible\":" << r.owners_eligible << ","
                      << "\"err\":\"" << r.err << "\"}\n";
        } else {
            std::cerr << "  " << r.rid
                      << " rows=" << r.rows
                      << " chunks=" << r.chunks
                      << " total_ms=" << ms_total
                      << " first_chunk_ms=" << ms_first
                      << (r.ok ? "" : (" ERR: " + r.err)) << "\n";
        }
    }
    return fail == 0 ? 0 : 1;
}
