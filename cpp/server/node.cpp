// Mini 2 — C++ gRPC node daemon.
//
// A single binary that runs as any node (A–I). Identity comes from --name.
// Implements two services:
//
//   ClientGateway : only meaningful when the node's role is "gateway" (A);
//                   external clients talk here.
//   PeerLink      : node-to-node; every process implements it.
//
// All RPCs are unary.  Chunking / flow-control / fairness are hand-rolled
// on top of PullChunk calls — no server-streaming, no async gRPC APIs.

#include <atomic>
#include <chrono>
#include <csignal>
#include <deque>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "mini2.grpc.pb.h"
#include "csv_store.hpp"
#include "overlay.hpp"
#include "scheduler.hpp"

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

namespace {

// Small log helper — avoids pulling in an external logging lib.
std::mutex g_log_mu;
void log_line(const std::string& who, const std::string& msg) {
    std::lock_guard<std::mutex> lk(g_log_mu);
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    char buf[32]; std::strftime(buf, sizeof(buf), "%H:%M:%S", std::localtime(&t));
    std::cerr << buf << " [" << who << "] " << msg << '\n';
}

int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Per-request state.  Data-owners produce into `matched`; gateways aggregate
// into `client_buffer`.  Locks guard only the deques — production and pulling
// contend on them but nothing else.
struct RequestState {
    std::string request_id;
    ::mini2::Query query;
    std::deque<::mini2::TaxiRow> matched;     // producer-side buffer
    std::deque<::mini2::TaxiRow> client_buffer; // gateway aggregate buffer
    // Indices of rows in the local partition that match the query — computed
    // once via PartitionStore::range_search when the request is registered,
    // so the producer iterates a precomputed dense list instead of rescanning.
    std::vector<std::size_t> matched_indices;
    std::size_t matched_cursor = 0;
    std::size_t prod_cursor  = 0;             // legacy; unused on owners now
    int64_t     last_delivered_seq = -1;
    int64_t     client_last_seq    = -1;
    int64_t     total_matched      = 0;
    std::atomic<bool> cancelled{false};
    std::atomic<bool> done_producing{false};
    int         weight = 1;
    int64_t     created_ns = 0;
    // Gateway: which peers are finished producing?
    std::unordered_map<std::string, bool> client_producers_done;
    std::mutex  mu;
};

} // anonymous

namespace mini2 {

class Mini2ServiceImpl final
    : public ClientGateway::Service,
      public PeerLink::Service {
public:
    Mini2ServiceImpl(std::string name, Overlay ov, std::string data_dir)
        : name_(std::move(name)),
          overlay_(std::move(ov)),
          data_dir_(std::move(data_dir)),
          me_(overlay_.nodes.at(name_)),
          sizer_(overlay_.chunking.initial_rows,
                 overlay_.chunking.min_rows,
                 overlay_.chunking.max_rows,
                 overlay_.chunking.target_chunk_ms),
          sched_(overlay_.scheduler.mode),
          stop_producer_(false) {
        // Load partition if this node owns data.
        for (auto& o : overlay_.data_owners) {
            if (o == name_) {
                std::string path = data_dir_ + "/" + name_ + ".csv";
                if (std::filesystem::exists(path)) {
                    part_ = load_partition_csv(path);
                    log_line(name_, "loaded " + std::to_string(part_.size())
                             + " rows from " + path);
                } else {
                    log_line(name_, "no partition file at " + path);
                }
            }
        }
        producer_thread_ = std::thread([this]{ this->produce_loop(); });
    }

    ~Mini2ServiceImpl() {
        stop_producer_.store(true);
        if (producer_thread_.joinable()) producer_thread_.join();
    }

    // --- ClientGateway -----------------------------------------------------

    Status SubmitQuery(ServerContext*, const Query* req, QueryAck* resp) override {
        if (me_.role != "gateway") {
            return {StatusCode::FAILED_PRECONDITION,
                    "SubmitQuery only at gateway role"};
        }
        const auto& rid = req->request_id();
        if (rid.empty()) {
            resp->set_accepted(false);
            resp->set_reject_reason("missing request_id");
            return Status::OK;
        }
        log_line(name_, "SubmitQuery rid=" + rid
                 + " preds=" + std::to_string(req->predicates_size()));
        auto st = register_request(rid, *req, /*weight=*/1);
        for (auto& o : overlay_.data_owners) {
            if (o == name_) continue;
            std::lock_guard<std::mutex> lk(st->mu);
            st->client_producers_done[o] = false;
        }

        // Fan out ForwardedQuery per owner, with explicit target_owner so
        // intermediates relay along the correct path.
        for (auto& owner : overlay_.data_owners) {
            if (owner == name_) continue;
            auto hop = overlay_.next_hop(name_, owner);
            ForwardedQuery fq;
            *fq.mutable_query() = *req;
            fq.set_forwarder(name_);
            fq.set_reply_to(name_);
            fq.set_target_owner(owner);
            fq.set_ttl(8);
            QueryAck ack;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now()
                             + std::chrono::seconds(5));
            auto s = peer_stub(hop)->ForwardQuery(&ctx, fq, &ack);
            if (!s.ok()) {
                log_line(name_, "forward→" + owner + " via " + hop
                         + " failed: " + s.error_message());
                std::lock_guard<std::mutex> lk(st->mu);
                st->client_producers_done[owner] = true;
            }
        }
        resp->set_request_id(rid);
        resp->set_accepted(true);
        resp->set_estimated_rows(-1);
        return Status::OK;
    }

    Status FetchChunk(ServerContext*, const PullRequest* req,
                      Chunk* resp) override {
        if (me_.role != "gateway") {
            return {StatusCode::FAILED_PRECONDITION,
                    "FetchChunk only at gateway"};
        }
        auto st = get_request(req->request_id());
        if (!st) return {StatusCode::NOT_FOUND,
                         "unknown request_id " + req->request_id()};

        int max_rows = sizer_.decide(req->request_id(), req->max_rows());

        // Gather from owners (round-robin) until we have max_rows or time out.
        auto deadline = std::chrono::steady_clock::now()
                         + std::chrono::milliseconds(200);
        auto owners = overlay_.data_owners;
        std::shuffle(owners.begin(), owners.end(), rng_);
        for (auto& owner : owners) {
            if (st->cancelled.load()) break;
            {
                std::lock_guard<std::mutex> lk(st->mu);
                if ((int)st->client_buffer.size() >= max_rows) break;
            }
            if (st->client_producers_done[owner]) continue;
            if (std::chrono::steady_clock::now() > deadline) break;

            auto hop = overlay_.next_hop(name_, owner);
            PullRequest sub;
            sub.set_request_id(req->request_id());
            sub.set_max_rows(max_rows);
            sub.set_last_seq(-1);
            sub.set_target_owner(owner);
            Chunk piece;
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now()
                             + std::chrono::seconds(2));
            auto s = peer_stub(hop)->PullChunk(&ctx, sub, &piece);
            if (!s.ok()) {
                log_line(name_, "PullChunk→" + owner + " failed: "
                         + s.error_message());
                std::lock_guard<std::mutex> lk(st->mu);
                st->client_producers_done[owner] = true;
                continue;
            }
            {
                std::lock_guard<std::mutex> lk(st->mu);
                for (auto& r : piece.rows())
                    st->client_buffer.push_back(r);
                st->total_matched += piece.rows_size();
                if (piece.is_last()) st->client_producers_done[owner] = true;
            }
        }

        // Emit up to max_rows from the aggregate buffer.
        int64_t seq = ++st->client_last_seq;
        std::vector<TaxiRow> out;
        {
            std::lock_guard<std::mutex> lk(st->mu);
            while (!st->client_buffer.empty() && (int)out.size() < max_rows) {
                out.push_back(std::move(st->client_buffer.front()));
                st->client_buffer.pop_front();
            }
        }
        bool all_done = true;
        for (auto& o : overlay_.data_owners) {
            if (o == name_) continue;
            if (!st->client_producers_done[o]) { all_done = false; break; }
        }
        bool is_last;
        {
            std::lock_guard<std::mutex> lk(st->mu);
            is_last = all_done && st->client_buffer.empty() && out.empty();
        }

        resp->set_request_id(req->request_id());
        resp->set_seq(seq);
        resp->set_is_last(is_last);
        for (auto& r : out) *resp->add_rows() = std::move(r);
        resp->set_producer(name_);
        {
            std::lock_guard<std::mutex> lk(st->mu);
            resp->set_backlog(st->client_buffer.size());
        }
        resp->set_produced_at_ns(now_ns());

        if (is_last) {
            log_line(name_, "rid=" + req->request_id() + " done; total="
                     + std::to_string(st->total_matched));
            drop_request(req->request_id());
        }
        return Status::OK;
    }

    Status CancelQuery(ServerContext*, const CancelRequest* req,
                       CancelResponse* resp) override {
        auto st = get_request(req->request_id());
        if (!st) {
            resp->set_acknowledged(false);
            resp->set_rows_dropped(0);
            return Status::OK;
        }
        st->cancelled.store(true);
        int dropped = 0;
        {
            std::lock_guard<std::mutex> lk(st->mu);
            dropped += st->client_buffer.size() + st->matched.size();
            st->client_buffer.clear();
            st->matched.clear();
        }
        if (me_.role == "gateway") {
            for (auto& owner : overlay_.data_owners) {
                if (owner == name_) continue;
                auto hop = overlay_.next_hop(name_, owner);
                CancelRequest cr;
                cr.set_request_id(req->request_id());
                cr.set_reason(req->reason());
                CancelResponse cresp;
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now()
                                 + std::chrono::seconds(1));
                peer_stub(hop)->CancelQuery(&ctx, cr, &cresp);
            }
        }
        drop_request(req->request_id());
        resp->set_acknowledged(true);
        resp->set_rows_dropped(dropped);
        return Status::OK;
    }

    Status Ping(ServerContext*, const Heartbeat* req,
                HeartbeatAck* resp) override {
        resp->set_to_node(req->from_node());
        resp->set_recv_at_ns(now_ns());
        resp->set_healthy(true);
        return Status::OK;
    }

    // --- PeerLink: these handlers shadow the ClientGateway ones.  gRPC
    //     dispatches by service method name so there's no ambiguity.  We
    //     implement the same logic with per-node role guards.

    Status ForwardQuery(ServerContext*, const ForwardedQuery* req,
                        QueryAck* resp) override {
        const auto& q = req->query();
        if (req->ttl() <= 0) {
            resp->set_request_id(q.request_id());
            resp->set_accepted(false);
            resp->set_reject_reason("ttl expired");
            return Status::OK;
        }
        const std::string& target = req->target_owner();
        if (target == name_) {
            // I am the target data owner — register and start matching.
            register_request(q.request_id(), q);
            log_line(name_, "accepted forwarded query " + q.request_id()
                     + " (target=" + target + ")");
            resp->set_request_id(q.request_id());
            resp->set_accepted(true);
            resp->set_estimated_rows(-1);
            return Status::OK;
        }
        // Intermediate: relay toward target via the precomputed next hop.
        std::string nh;
        try { nh = overlay_.next_hop(name_, target); } catch (...) {}
        if (nh.empty() || nh == name_) {
            resp->set_request_id(q.request_id());
            resp->set_accepted(false);
            resp->set_reject_reason("no route to " + target);
            return Status::OK;
        }
        ForwardedQuery fwd;
        *fwd.mutable_query() = q;
        fwd.set_forwarder(name_);
        fwd.set_reply_to(req->reply_to());
        fwd.set_target_owner(target);
        fwd.set_ttl(req->ttl() - 1);
        QueryAck ack;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now()
                         + std::chrono::seconds(3));
        auto s = peer_stub(nh)->ForwardQuery(&ctx, fwd, &ack);
        if (!s.ok()) {
            resp->set_request_id(q.request_id());
            resp->set_accepted(false);
            resp->set_reject_reason("relay failed: " + s.error_message());
            return Status::OK;
        }
        *resp = ack;
        return Status::OK;
    }

    Status PullChunk(ServerContext*, const PullRequest* req,
                     Chunk* resp) override {
        const std::string& target = req->target_owner();
        // If target is set and it's not me, relay along the route.
        if (!target.empty() && target != name_) {
            std::string nh;
            try { nh = overlay_.next_hop(name_, target); } catch (...) {}
            if (nh.empty() || nh == name_) {
                return {StatusCode::NOT_FOUND, "no route to owner " + target};
            }
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now()
                             + std::chrono::seconds(2));
            return peer_stub(nh)->PullChunk(&ctx, *req, resp);
        }
        // Serve locally.
        auto st = get_request(req->request_id());
        if (!st || part_.empty()) {
            return {StatusCode::NOT_FOUND, "no local data for " + req->request_id()};
        }
        int max_rows = sizer_.decide(req->request_id(), req->max_rows());
        int64_t seq = ++st->last_delivered_seq;
        std::vector<TaxiRow> out;
        {
            std::lock_guard<std::mutex> lk(st->mu);
            while (!st->matched.empty() && (int)out.size() < max_rows) {
                out.push_back(std::move(st->matched.front()));
                st->matched.pop_front();
            }
        }
        bool done_prod = st->done_producing.load();
        bool is_last;
        {
            std::lock_guard<std::mutex> lk(st->mu);
            is_last = done_prod && st->matched.empty() && out.empty();
        }
        resp->set_request_id(req->request_id());
        resp->set_seq(seq);
        resp->set_is_last(is_last);
        for (auto& r : out) *resp->add_rows() = std::move(r);
        resp->set_producer(name_);
        {
            std::lock_guard<std::mutex> lk(st->mu);
            resp->set_backlog(st->matched.size());
        }
        resp->set_produced_at_ns(now_ns());
        if (is_last) drop_request(req->request_id());
        return Status::OK;
    }

    Status PushChunk(ServerContext*, const Chunk* req,
                     PushChunkAck* resp) override {
        // Unused in pull model — ack trivially for future mixed transports.
        resp->set_accepted(true);
        resp->set_next_seq(req->seq() + 1);
        return Status::OK;
    }

    // Node-side Ping is served by the ClientGateway::Ping above since the
    // message types are identical. Re-declare here so PeerLink's vtable is
    // fully filled.  Body is trivial.
    Status Ping(ServerContext* c, const Heartbeat* req,
                HeartbeatAck* resp, bool) {
        return Ping(c, req, resp);
    }

private:
    // Registers a request, creating its state if new.
    std::shared_ptr<RequestState> register_request(
            const std::string& rid, const Query& q, int weight = 1) {
        std::lock_guard<std::mutex> lk(req_mu_);
        auto it = requests_.find(rid);
        if (it != requests_.end()) return it->second;
        auto st = std::make_shared<RequestState>();
        st->request_id = rid;
        st->query = q;
        st->weight = weight;
        st->created_ns = now_ns();
        // Data-owners precompute the matched index list once via the parallel
        // range_search — the background producer just iterates this dense
        // vector, so per-row predicate evaluation never happens in steady
        // state.  Gateways have an empty partition and skip this.
        if (!part_.empty()) {
            int64_t t0 = now_ns();
            st->matched_indices = part_.range_search(q);
            int64_t t1 = now_ns();
            log_line(name_, "rid=" + rid
                     + " range_search matched=" + std::to_string(st->matched_indices.size())
                     + " of " + std::to_string(part_.size())
                     + " in " + std::to_string((t1 - t0) / 1'000'000) + "ms");
            if (st->matched_indices.empty()) {
                st->done_producing.store(true);
            }
        }
        requests_[rid] = st;
        sched_.add(rid, weight);
        return st;
    }

    std::shared_ptr<RequestState> get_request(const std::string& rid) {
        std::lock_guard<std::mutex> lk(req_mu_);
        auto it = requests_.find(rid);
        return it == requests_.end() ? nullptr : it->second;
    }

    void drop_request(const std::string& rid) {
        std::lock_guard<std::mutex> lk(req_mu_);
        requests_.erase(rid);
        sched_.remove(rid);
        sizer_.forget(rid);
    }

    ::mini2::PeerLink::Stub* peer_stub(const std::string& peer) {
        std::lock_guard<std::mutex> lk(peer_mu_);
        auto it = peer_stubs_.find(peer);
        if (it != peer_stubs_.end()) return it->second.get();
        auto ep = overlay_.nodes.at(peer).endpoint();
        grpc::ChannelArguments args;
        args.SetMaxSendMessageSize(64 * 1024 * 1024);
        args.SetMaxReceiveMessageSize(64 * 1024 * 1024);
        auto ch = grpc::CreateCustomChannel(
            ep, grpc::InsecureChannelCredentials(), args);
        auto stub = ::mini2::PeerLink::NewStub(ch);
        auto raw = stub.get();
        peer_stubs_[peer] = std::move(stub);
        peer_channels_[peer] = ch;
        return raw;
    }

    // Background producer: runs matching work for each active request.
    void produce_loop() {
        while (!stop_producer_.load()) {
            bool did_work = false;
            std::string rid = sched_.next();
            if (rid.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            auto st = get_request(rid);
            if (!st) { continue; }
            if (st->cancelled.load() || st->done_producing.load()) continue;
            if (part_.empty()) continue;

            const auto& rows = part_;
            const auto& idxs = st->matched_indices;
            std::size_t i = st->matched_cursor;
            std::size_t limit = std::min<std::size_t>(
                idxs.size(), i + 256 * std::max(1, st->weight));
            int produced = 0;
            for (; i < limit; ++i) {
                if (st->cancelled.load()) break;
                TaxiRow r;
                rows.fill(idxs[i], r);
                {
                    std::lock_guard<std::mutex> lk(st->mu);
                    st->matched.push_back(std::move(r));
                }
                ++produced;
            }
            st->matched_cursor = i;
            if (i >= idxs.size()) st->done_producing.store(true);
            if (produced > 0) did_work = true;
            if (!did_work) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    std::string name_;
    Overlay     overlay_;
    std::string data_dir_;
    NodeSpec    me_;
    PartitionStore part_;

    ChunkSizer    sizer_;
    FairScheduler sched_;

    std::mutex peer_mu_;
    std::unordered_map<std::string, std::unique_ptr<::mini2::PeerLink::Stub>> peer_stubs_;
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> peer_channels_;

    std::mutex req_mu_;
    std::unordered_map<std::string, std::shared_ptr<RequestState>> requests_;

    std::mt19937 rng_{0xC0FFEE};
    std::atomic<bool> stop_producer_;
    std::thread producer_thread_;
};

} // namespace mini2

namespace {
std::atomic<bool> g_shutdown{false};
void on_signal(int) { g_shutdown.store(true); }

std::string env_or(const char* key, const std::string& dflt) {
    if (const char* v = std::getenv(key)) return v;
    return dflt;
}

std::string pop_arg(int& i, int argc, char** argv, const std::string& name) {
    if (i + 1 >= argc)
        throw std::runtime_error("missing value for " + name);
    return argv[++i];
}
} // anonymous

int main(int argc, char** argv) {
    std::string name;
    std::string overlay_path;
    std::string data_dir;
    int workers = 16;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--name")          name         = pop_arg(i, argc, argv, a);
        else if (a == "--overlay")  overlay_path = pop_arg(i, argc, argv, a);
        else if (a == "--data-dir") data_dir     = pop_arg(i, argc, argv, a);
        else if (a == "--workers")  workers      = std::atoi(pop_arg(i, argc, argv, a).c_str());
        else if (a == "--help" || a == "-h") {
            std::cout << "usage: mini2_node --name <NODE> "
                         "[--overlay <yaml>] [--data-dir <dir>] [--workers N]\n";
            return 0;
        } else {
            std::cerr << "unknown arg: " << a << '\n';
            return 2;
        }
    }

    if (name.empty()) name = env_or("MINI2_NODE", "");
    if (name.empty()) { std::cerr << "--name is required\n"; return 2; }

    mini2::Overlay ov = mini2::load_overlay(overlay_path);
    if (!ov.has_node(name)) {
        std::cerr << "unknown node " << name << " in overlay\n";
        return 2;
    }
    auto me = ov.nodes.at(name);

    if (data_dir.empty()) {
        data_dir = env_or("MINI2_DATA_DIR", "data/partitions");
    }

    mini2::Mini2ServiceImpl service(name, ov, data_dir);

    grpc::ServerBuilder builder;
    std::string bind_addr = "0.0.0.0:" + std::to_string(me.port);
    builder.AddListeningPort(bind_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(static_cast<mini2::ClientGateway::Service*>(&service));
    builder.RegisterService(static_cast<mini2::PeerLink::Service*>(&service));
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "failed to bind " << bind_addr << '\n';
        return 1;
    }
    log_line(name, "listening on " + bind_addr
             + " role=" + me.role + " team=" + me.team);

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);
    while (!g_shutdown.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    log_line(name, "shutdown requested");
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    return 0;
}
