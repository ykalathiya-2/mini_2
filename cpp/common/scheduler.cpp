#include "scheduler.hpp"

namespace mini2 {

int ChunkSizer::decide(const std::string& rid, int hint) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& st = s_[rid];
    if (st.cur == 0) st.cur = initial_;
    auto now = std::chrono::steady_clock::now();
    if (hint > 0) {
        st.cur = std::max(min_, std::min(max_, hint));
        st.last = now;
        st.has_last = true;
        return st.cur;
    }
    if (st.has_last) {
        auto dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - st.last).count();
        if (dt_ms < target_ms_ / 2) {
            st.cur = std::min(max_, int(st.cur * 1.5) + 1);
        } else if (dt_ms > target_ms_ * 2) {
            st.cur = std::max(min_, st.cur / 2);
        }
    }
    st.last = now;
    st.has_last = true;
    return st.cur;
}

void ChunkSizer::forget(const std::string& rid) {
    std::lock_guard<std::mutex> lk(mu_);
    s_.erase(rid);
}

// ---------- FairScheduler --------------------------------------------------

void FairScheduler::add(const std::string& rid, int weight) {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& e : q_) if (e.rid == rid) { e.weight = weight; return; }
    q_.push_back({rid, weight, weight});
}

void FairScheduler::remove(const std::string& rid) {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto it = q_.begin(); it != q_.end(); ++it) {
        if (it->rid == rid) {
            q_.erase(it);
            if (cursor_ >= q_.size()) cursor_ = 0;
            return;
        }
    }
}

std::string FairScheduler::next() {
    std::lock_guard<std::mutex> lk(mu_);
    if (q_.empty()) return {};
    std::size_t tries = 0;
    while (tries <= q_.size()) {
        auto& e = q_[cursor_];
        if (mode_ == "fifo") {
            // FIFO: always pick front, never advance until removed.
            return q_.front().rid;
        }
        if (mode_ == "round_robin") {
            std::string rid = e.rid;
            cursor_ = (cursor_ + 1) % q_.size();
            return rid;
        }
        // weighted_round_robin
        if (e.credits > 0) {
            e.credits -= 1;
            std::string rid = e.rid;
            if (e.credits == 0) {
                e.credits = e.weight;
                cursor_ = (cursor_ + 1) % q_.size();
            }
            return rid;
        }
        e.credits = e.weight;
        cursor_ = (cursor_ + 1) % q_.size();
        ++tries;
    }
    return q_.front().rid;
}

std::size_t FairScheduler::size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
}

} // namespace mini2
