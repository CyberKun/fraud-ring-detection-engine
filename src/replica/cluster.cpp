#include "replica/cluster.hpp"
#include <fstream>
#include <iostream>

namespace fraud::replica {
using namespace std::chrono_literals;
namespace {
Boundary mark(const rpc::Mark &v) { return {v.index(), v.epoch(), v.lsn(), v.digest()}; }
void put(rpc::Mark *m, const Boundary &b) {
    m->set_index(b.index);
    m->set_epoch(b.epoch);
    m->set_lsn(b.lsn);
    m->set_digest(b.digest);
}
std::string identity(Id shard, const std::map<std::string, std::string> &members) {
    std::string s = std::to_string(shard);
    for (const auto &[id, address] : members)
        s += "/" + id;
    return s;
}
std::string session(std::string id) {
    std::string uuid;
    std::ifstream("/proc/sys/kernel/random/uuid") >> uuid;
    if (uuid.empty())
        throw storage::Error("cannot generate election session");
    return id + "/" + uuid;
}
} // namespace
Cluster::Cluster(Id shard, std::string id, std::map<std::string, std::string> members, std::string etcd,
                 std::filesystem::path dir, service::Options options, int ttl, int tick_ms,
                 Id checkpoint_frames)
    : view(options), shard_(shard), id_(id), session_(session(id)), ttl_(ttl), tick_ms_(tick_ms),
      checkpoint_(dir / "replica.snapshot"), checkpoint_frames_(checkpoint_frames),
      journal_(dir, identity(shard, members), id), election_(etcd, shard), writer_(options) {
    if (members.size() != 3 || !members.contains(id) || ttl < 2 || tick_ms < 1)
        throw std::invalid_argument("invalid group configuration");
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(storage::max_payload + 4096);
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    for (auto &[name, address] : members)
        if (name != id)
            peers_[name] = rpc::ReplicaPeer::NewStub(
                grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
    if (std::filesystem::exists(checkpoint_)) {
        auto image = storage::deserialize(storage::read_file(checkpoint_, storage::snapshot_cap));
        auto frames = std::span(journal_.frames()).first(journal_.state().commit.index);
        auto end = std::lower_bound(frames.begin(), frames.end(), image.lsn,
                                    [](const auto &f, Id lsn) { return f.back().lsn < lsn; });
        if (end == frames.end())
            throw Invariant("snapshot beyond durable commit");
        checkpoint_index_ = visible_ = std::distance(frames.begin(), end) + 1;
        view.load_checkpoint(image, frames.first(visible_));
    }
    publish(journal_.state().commit.index);
}
void Cluster::publish(Id index) {
    while (visible_ < index) {
        view.replay(journal_.frames().at(visible_));
        ++visible_;
    }
    if (visible_ == journal_.state().commit.index && visible_ - checkpoint_index_ >= checkpoint_frames_) {
        storage::atomic_file(checkpoint_, storage::serialize(view.image()));
        checkpoint_index_ = visible_;
    }
}
void Cluster::follow() {
    ready_ = false;
    if (visible_ > journal_.state().commit.index) {
        view.reset();
        visible_ = 0;
        publish(journal_.state().commit.index);
    }
}
void Cluster::handle(const Request &q, Reply &r) {
    std::lock_guard lock(mu_);
    if (q.shard() != shard_ || q.epoch() < journal_.state().promised)
        throw Rejected("stale epoch or shard");
    if (q.op() == rpc::PROMISE && q.epoch() > journal_.state().promised) {
        auto current = election_.lookup();
        if (current.epoch != q.epoch() || current.owner != q.owner())
            throw Rejected("candidate lease absent");
        follow();
        journal_.promise(q.epoch(), q.owner());
    }
    journal_.fence(q.epoch(), q.owner());
    if (q.owner() != session_)
        observed_tail_ = q.leader_lsn();
    switch (q.op()) {
    case rpc::PROMISE:
        break;
    case rpc::PROBE:
        put(r.mutable_boundary(), journal_.at(q.index()));
        break;
    case rpc::FETCH: {
        if (!q.index() || q.index() > journal_.state().tail.index)
            throw Rejected("missing frame");
        auto data = storage::encode(journal_.frames()[q.index() - 1]);
        r.set_frame(data.data(), data.size());
        put(r.mutable_boundary(), journal_.at(q.index()));
        break;
    }
    case rpc::APPEND: {
        auto &bytes = q.frame();
        storage::Frame frame;
        try {
            frame = storage::decode_frame({reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size()});
        } catch (const storage::Error &e) {
            throw Rejected(e.what());
        }
        view.account_wal_write(journal_.append(mark(q.previous()), frame));
        break;
    }
    case rpc::TRUNCATE:
        journal_.truncate(mark(q.previous()), mark(q.boundary()));
        break;
    case rpc::COMMIT:
        journal_.commit(mark(q.boundary()));
        publish(journal_.state().commit.index);
        break;
    default:
        throw Rejected("unknown replication operation");
    }
    auto s = journal_.state();
    put(r.mutable_tail(), s.tail);
    put(r.mutable_committed(), s.commit);
}
grpc::Status Cluster::Exchange(grpc::ServerContext *, const Request *q, Reply *r) {
    try {
        handle(*q, *r);
        return grpc::Status::OK;
    } catch (const storage::Error &e) {
        std::cerr << e.what() << std::endl;
        std::terminate();
    } catch (const std::exception &e) {
        return {grpc::StatusCode::FAILED_PRECONDITION, e.what()};
    }
}
Cluster::Request Cluster::request(const Lease &l, rpc::PeerOp op) {
    std::lock_guard lock(mu_);
    Request q;
    q.set_leader_lsn(journal_.state().tail.lsn);
    q.set_epoch(l.epoch);
    q.set_owner(l.owner);
    q.set_shard(shard_);
    q.set_op(op);
    return q;
}
Cluster::Reply Cluster::exchange(const std::string &peer, Request q) {
    Reply r;
    if (peer == id_) {
        handle(q, r);
        return r;
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 500ms);
    auto status = peers_.at(peer)->Exchange(&ctx, q, &r);
    if (!status.ok())
        throw Rejected(peer + ": " + status.error_message());
    return r;
}
void Cluster::verify(const Lease &l) {
    if (!election_.holds(l))
        throw Rejected("leader lease lost");
    std::lock_guard lock(mu_);
    journal_.fence(l.epoch, l.owner);
    if (lease_.epoch != l.epoch)
        throw Rejected("lease renewal interrupted");
}
Boundary Cluster::common(const std::string &peer, const Lease &l, const Boundary &remote) {
    auto local = exchange(id_, request(l, rpc::PROMISE));
    Id hi = std::min(remote.index, local.tail().index()), lo = 0;
    auto matches = [&](Id i) {
        auto q = request(l, rpc::PROBE);
        q.set_index(i);
        return mark(exchange(id_, q).boundary()) ==
               (i == remote.index ? remote : mark(exchange(peer, q).boundary()));
    };
    if (matches(hi))
        lo = hi;
    while (lo < hi) {
        Id mid = lo + (hi - lo + 1) / 2;
        if (matches(mid))
            lo = mid;
        else
            hi = mid - 1;
    }
    auto q = request(l, rpc::PROBE);
    q.set_index(lo);
    return mark(exchange(id_, q).boundary());
}
void Cluster::align(const std::string &peer, const Lease &l, const Boundary &remote) {
    auto base = common(peer, l, remote);
    if (base != remote) {
        auto q = request(l, rpc::TRUNCATE);
        put(q.mutable_previous(), remote);
        put(q.mutable_boundary(), base);
        exchange(peer, q);
    }
    auto last = exchange(id_, request(l, rpc::PROMISE)).tail().index();
    while (base.index < last) {
        auto f = request(l, rpc::FETCH);
        f.set_index(base.index + 1);
        auto frame = exchange(id_, f);
        auto q = request(l, rpc::APPEND);
        put(q.mutable_previous(), base);
        q.set_frame(std::move(*frame.mutable_frame()));
        base = mark(exchange(peer, q).tail());
        if (base != mark(frame.boundary()))
            throw Invariant("installed frame hash mismatch");
    }
}
void Cluster::reconcile(const Lease &l) {
    auto self = exchange(id_, request(l, rpc::PROMISE));
    std::map<std::string, Reply> voters;
    std::string best = id_;
    Boundary tip = mark(self.tail()), certified = mark(self.committed());
    for (auto &[id, stub] : peers_) {
        Reply r;
        try {
            r = exchange(id, request(l, rpc::PROMISE));
        } catch (const Rejected &) {
            continue;
        }
        voters[id] = r;
        auto other = mark(r.tail());
        if (std::tie(other.epoch, other.lsn) > std::tie(tip.epoch, tip.lsn)) {
            best = id;
            tip = other;
        } else if (other.epoch == tip.epoch && other.lsn == tip.lsn && other != tip)
            throw Invariant("conflicting same-term entry");
        auto c = mark(r.committed());
        if (c.index > certified.index)
            certified = c;
    }
    if (voters.empty())
        throw Rejected("no promise quorum");
    if (best != id_) {
        auto base = common(best, l, tip);
        auto cut = request(l, rpc::TRUNCATE);
        put(cut.mutable_previous(), mark(self.tail()));
        put(cut.mutable_boundary(), base);
        exchange(id_, cut);
        while (base.index < tip.index) {
            auto f = request(l, rpc::FETCH);
            f.set_index(base.index + 1);
            auto frame = exchange(best, f);
            auto q = request(l, rpc::APPEND);
            put(q.mutable_previous(), base);
            q.set_frame(std::move(*frame.mutable_frame()));
            base = mark(exchange(id_, q).tail());
            if (base != mark(frame.boundary()))
                throw Invariant("catch-up frame hash mismatch");
            std::lock_guard lock(mu_);
            ++fetched_;
        }
        if (base != tip)
            throw Invariant("catch-up tail mismatch");
    }
    verify(l);
    unsigned aligned = 0;
    for (auto &[id, r] : voters) {
        try {
            align(id, l, mark(r.tail()));
            ++aligned;
        } catch (const Rejected &) {
        }
    }
    if (!aligned)
        throw Rejected("no aligned majority");
    auto commit = request(l, rpc::COMMIT);
    put(commit.mutable_boundary(), certified);
    exchange(id_, commit);
    // Rebuild the writer outside the journal lock so the heartbeat can renew a
    // short lease even when installing a long inherited history.
    writer_.reset();
    for (Id i = 0; i < tip.index; ++i) {
        storage::Frame frame;
        {
            std::lock_guard lock(mu_);
            journal_.fence(l.epoch, l.owner);
            frame = journal_.frames().at(i);
        }
        writer_.replay(frame);
    }
    verify(l);
    std::lock_guard lock(mu_);
    journal_.fence(l.epoch, l.owner);
    if (lease_.epoch != l.epoch || journal_.state().tail != tip)
        throw Rejected("lease or log changed before write gate");
    failover_.observe(std::chrono::duration<double>(service::Clock::now() - acquired_).count());
    ready_ = true;
}
bool Cluster::replicate(const Lease &l, bool all) {
    auto self = exchange(id_, request(l, rpc::PROMISE));
    auto tip = mark(self.tail());
    bool confirmed = false;
    for (auto &[id, stub] : peers_) {
        try {
            auto remote = exchange(id, request(l, rpc::PROMISE));
            align(id, l, mark(remote.tail()));
            auto commit = request(l, rpc::COMMIT);
            put(commit.mutable_boundary(), tip.epoch == l.epoch ? tip : mark(self.committed()));
            exchange(id_, commit);
            exchange(id, commit);
            confirmed = true;
            if (!all)
                break;
        } catch (const Rejected &) {
        }
    }
    return confirmed;
}
service::Mutation Cluster::write(Id tenant, Command cmd, std::optional<ProducerSequence> source,
                                 rpc::AckMode mode, std::size_t bytes) {
    bytes = std::max(bytes, sizeof(Command));
    auto reservation = admission_.reserve(bytes, limits_.requests, limits_.bytes);
    if (!reservation) {
        ++shed_;
        throw std::overflow_error("leader admission limit");
    }
    try {
        std::lock_guard drive(drive_);
        Lease l;
        {
            std::lock_guard lock(mu_);
            if (!ready_)
                throw Rejected("NOT_LEADER: reconciliation gate closed");
            l = lease_;
            auto state = journal_.state();
            // A current-term tick can certify an inherited uncommitted suffix.
            bool seal = std::holds_alternative<Tick>(cmd) && state.tail.epoch < l.epoch;
            if (!seal && state.tail.lsn - state.commit.lsn >= limits_.lag) {
                ++shed_;
                throw std::overflow_error("replication lag watermark");
            }
        }
        try {
            verify(l);
        } catch (const Rejected &e) {
            throw Rejected(std::string("NOT_LEADER: ") + e.what());
        }
        Batch batch;
        Boundary tip;
        {
            std::lock_guard lock(mu_);
            journal_.fence(l.epoch, l.owner);
            if (!ready_ || lease_.epoch != l.epoch)
                throw Rejected("NOT_LEADER: lease changed before append");
            batch = writer_.propose(l.epoch, tenant, service::wall_ms(), cmd, source);
            if (batch.status == Status::ok)
                view.account_wal_write(journal_.append(journal_.state().tail, batch.records));
            else if (batch.status != Status::duplicate)
                return {batch.status, {l.epoch, 0}, false};
            tip = journal_.state().tail;
            // A duplicate may refer to an inherited, as yet uncertified suffix.
            if (mode == rpc::QUORUM && tip.index > journal_.state().commit.index && tip.epoch != l.epoch) {
                auto seal = writer_.propose(l.epoch, tenant, service::wall_ms(), Tick{}, {});
                view.account_wal_write(journal_.append(tip, seal.records));
                tip = journal_.state().tail;
            }
        }
        if (mode != rpc::ASYNC && !replicate(l, false))
            throw Rejected("no durable commit quorum");
        verify(l);
        {
            std::lock_guard lock(mu_);
            journal_.fence(l.epoch, l.owner);
            if (!ready_ || lease_.epoch != l.epoch)
                throw Rejected("lease changed before acknowledgment");
            publish(tip.index);
        }
        return {batch.status, {l.epoch, tip.lsn}, !batch.alerts.empty()};
    } catch (const storage::Error &e) {
        std::cerr << e.what() << std::endl;
        std::terminate();
    }
}
Id Cluster::leader_epoch() {
    std::lock_guard guard(mu_);
    return ready_ ? lease_.epoch : 0;
}
bool Cluster::owns_epoch(Id epoch) {
    Lease lease;
    {
        std::lock_guard guard(mu_);
        if (!ready_ || !epoch || lease_.epoch != epoch)
            return false;
        lease = lease_;
    }
    return election_.holds(lease);
}
std::unique_lock<std::mutex> Cluster::read_guard(Id min, std::string &role) {
    std::unique_lock lock(mu_);
    if (view.position().lsn < min)
        throw Rejected("replica below min_lsn");
    role = ready_ ? "LEADER" : "FOLLOWER";
    return lock;
}
void Cluster::health(rpc::ShardHealth &h, bool checksum) {
    std::lock_guard lock(mu_);
    auto s = journal_.state();
    auto pos = view.position();
    h.set_ingestion_rate(view.ingestion_rate());
    h.set_shard_id(shard_);
    h.set_replica_role(ready_ ? "LEADER" : "FOLLOWER");
    h.set_epoch(s.promised);
    h.set_applied_lsn(pos.lsn);
    h.set_commit_lsn(s.commit.lsn);
    h.set_last_entry_epoch(s.tail.epoch);
    h.set_last_lsn(s.tail.lsn);
    h.set_lag(std::max(s.tail.lsn, observed_tail_) - pos.lsn);
    h.set_truncations(journal_.truncations());
    h.set_fetched_frames(fetched_);
    if (checksum) {
        h.set_checksum(storage::logical_checksum(view.image()));
        h.set_log_hash(s.tail.digest);
    }
}
std::string Cluster::metrics() {
    std::lock_guard lock(mu_);
    auto pending = admission_.usage();
    return "ingestion_queue_depth " + std::to_string(pending.requests) + "\ningestion_pending_bytes " +
           std::to_string(pending.bytes) + "\n# TYPE shed_requests_total counter\nshed_requests_total " +
           std::to_string(shed_.load()) + "\n# TYPE replica_lsn_lag gauge\nreplica_lsn_lag " +
           std::to_string(std::max(journal_.state().tail.lsn, observed_tail_) - view.position().lsn) +
           "\n# HELP failover_duration_seconds Local lease acquisition to reconciled write gate.\n# TYPE "
           "failover_duration_seconds histogram\n" +
           failover_.render("failover_duration_seconds");
}
void Cluster::start() {
    heartbeat_ = std::jthread([this](std::stop_token stop) {
        Lease held;
        while (!stop.stop_requested()) {
            try {
                if (held.epoch && (!election_.renew(held) || !election_.holds(held)))
                    held = {};
                if (!held.epoch && !election_.lookup().epoch)
                    held = election_.acquire(session_, ttl_);
            } catch (const std::exception &) {
                held = {};
            }
            {
                std::lock_guard lock(mu_);
                if (lease_.epoch != held.epoch) {
                    follow();
                    acquired_ = service::Clock::now();
                    observed_tail_ = 0;
                }
                lease_ = held;
            }
            std::this_thread::sleep_for(200ms);
        }
    });
    worker_ = std::jthread([this](std::stop_token stop) {
        auto next = service::Clock::now() + std::chrono::milliseconds(tick_ms_);
        while (!stop.stop_requested()) {
            try {
                std::vector<Id> ticks;
                {
                    std::lock_guard drive(drive_);
                    Lease l;
                    bool ready;
                    {
                        std::lock_guard lock(mu_);
                        l = lease_;
                        ready = ready_;
                    }
                    if (l.epoch) {
                        if (!ready)
                            reconcile(l);
                        else {
                            verify(l);
                            replicate(l, true);
                        }
                        if (service::Clock::now() >= next) {
                            ticks = writer_.tenant_ids();
                            next = service::Clock::now() + std::chrono::milliseconds(tick_ms_);
                        }
                    }
                }
                for (auto id : ticks)
                    write(id, Tick{}, {}, rpc::QUORUM);
            } catch (const storage::Error &e) {
                std::cerr << e.what() << std::endl;
                std::terminate();
            } catch (
                const std::exception &) { /* Retry from a fresh promise quorum or on the next heartbeat. */
            }
            std::this_thread::sleep_for(100ms);
        }
    });
}
Cluster::~Cluster() {
    worker_.request_stop();
    heartbeat_.request_stop();
}
} // namespace fraud::replica
