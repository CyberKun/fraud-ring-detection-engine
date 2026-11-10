#pragma once

#include "core/timing_wheel.hpp"
#include <unordered_map>
#include <unordered_set>

namespace fraud {
struct Policy {
    std::size_t high{50}, low{35};
    Id min_accounts{5};
    double min_ratio{0.25};
    Time cooldown{60000};
    bool valid() const { return low < high && min_ratio >= 0 && min_ratio <= 1; }
};
struct Batch {
    Status status{Status::ok};
    std::vector<Event> records;
    std::vector<Alert> alerts;
};

// Single-writer tenant projection. apply_record consumes trusted ordered records.
// The service owns shard-wide LSN allocation and eventual durable publication.
template<DynamicGraphEngine Engine>
class Tenant {
    struct Vertex {
        Weight weight{};
        bool suppressed{};
        std::optional<Time> last_alert;
        std::set<Handle> live, structural;
    };
    Id tenant_, epoch_{}, lsn_{}, next_handle_{1}, total_volume_{};
    Time time_{};
    Policy policy_;
    Engine engine_;
    TimingWheel wheel_;
    bool rebuild_wheel_{};
    std::unordered_map<Id, Vertex> vertices_;
    std::unordered_map<Key, Handle, KeyHash> keys_;
    std::unordered_map<Handle, EdgeRecord> edges_;
    std::unordered_map<Id, Id> sequences_;

    static std::vector<Id> endpoints(Key k) {
        if (k.u == k.v) return {k.u};
        return {std::min(k.u,k.v), std::max(k.u,k.v)};
    }
    void ensure(Id v) {
        if (vertices_.try_emplace(v).second) engine_.set_vertex(v, {});
    }
    void link(EdgeRecord& e) {
        engine_.insert_edge(e.handle, e.key.u, e.key.v, e.expiry);
        for (Id v : endpoints(e.key)) vertices_.at(v).structural.insert(e.handle);
        e.shadow = false;
    }
    void unlink(EdgeRecord& e, Removal why) {
        engine_.delete_edge(e.handle, why);
        for (Id v : endpoints(e.key)) vertices_.at(v).structural.erase(e.handle);
        e.shadow = true;
    }
    bool filtered(Key k) const {
        return suppressed(k.u) || suppressed(k.v);
    }
    bool current(Expire x) const {
        auto it = edges_.find(x.handle);
        return it != edges_.end() && it->second.version == x.version;
    }
    void remove(Handle h, Removal why) {
        auto& e = edges_.at(h);
        if (!e.shadow) unlink(e, why);
        for (Id v : endpoints(e.key)) vertices_.at(v).live.erase(h);
        keys_.erase(e.key);
        edges_.erase(h);
    }
    void mutate(EdgeUpsert x, bool schedule) {
        Key k = x.key.normalized();
        if (auto it = keys_.find(k); it != keys_.end()) {
            auto& e = edges_.at(it->second);
            if (x.expiry <= e.expiry) return;
            e.expiry = x.expiry;
            ++e.version;
            if (schedule) wheel_.schedule({e.expiry,e.handle,e.version});
            if (!e.shadow) engine_.extend_ttl(e.handle,e.expiry);
            return;
        }
        auto h = next_handle_++;
        for (Id v : endpoints(k)) {
            ensure(v);
            vertices_.at(v).live.insert(h);
        }
        keys_.emplace(k,h);
        auto& e = edges_.emplace(h, EdgeRecord{k,h,1,x.expiry,true}).first->second;
        if (!filtered(k)) link(e);
        if (schedule) wheel_.schedule({e.expiry,h,1});
    }
    void mutate(EdgeDelete x, bool) {
        if (auto it = keys_.find(x.key.normalized()); it != keys_.end())
            remove(it->second, Removal::manual);
    }
    void mutate(Expire x, bool) {
        if (current(x)) remove(x.handle, Removal::expiry);
    }
    void mutate(VertexUpdate x, bool) {
        ensure(x.vertex);
        auto& w = vertices_.at(x.vertex).weight;
        total_volume_ -= w.volume;
        w = {x.weight.volume, x.weight.flagged && entity(x.vertex) == Entity::account};
        total_volume_ += w.volume;
        engine_.set_vertex(x.vertex, w);
    }
    void mutate(Tick, bool) {}
    void mutate(HubSuppress x, bool) {
        auto& v = vertices_.at(x.vertex);
        v.suppressed = true;
        while (!v.structural.empty()) unlink(edges_.at(*v.structural.begin()), Removal::suppression);
    }
    void mutate(HubRestore x, bool) {
        auto& v = vertices_.at(x.vertex);
        v.suppressed = false;
        for (Handle h : v.live) {
            auto& e = edges_.at(h);
            if (e.shadow && !filtered(e.key)) link(e);
        }
    }
    std::vector<Id> touched(const Payload& p) const {
        if (auto x = std::get_if<EdgeUpsert>(&p)) return endpoints(x->key);
        if (auto x = std::get_if<EdgeDelete>(&p)) return endpoints(x->key);
        if (auto x = std::get_if<VertexUpdate>(&p)) return {x->vertex};
        if (auto x = std::get_if<Expire>(&p)) {
            if (current(*x)) return endpoints(edges_.at(x->handle).key);
            return {};
        }
        if (std::holds_alternative<Tick>(p)) return {};
        Id hub = std::holds_alternative<HubSuppress>(p) ? std::get<HubSuppress>(p).vertex : std::get<HubRestore>(p).vertex;
        std::set<Id> members{hub};
        for (Handle h : vertices_.at(hub).live) {
            const auto k = edges_.at(h).key;
            if (!suppressed(k.u == hub ? k.v : k.u)) {
                members.insert(k.u);
                members.insert(k.v);
            }
        }
        return {members.begin(),members.end()};
    }
    bool qualifies(const std::optional<Aggregate>& a) const {
        return a && a->account_count >= policy_.min_accounts && a->flagged_ratio() >= policy_.min_ratio;
    }
    void apply(const Event& e, bool schedule, std::vector<Alert>& alerts) {
        auto affected = touched(e.payload);
        std::vector<bool> before;
        for (Id v : affected) before.push_back(qualifies(engine_.stats(v)));
        bool newly_flagged = false;
        if (auto x = std::get_if<VertexUpdate>(&e.payload)) {
            auto it = vertices_.find(x->vertex);
            newly_flagged = entity(x->vertex) == Entity::account && x->weight.flagged &&
                            (it == vertices_.end() || !it->second.weight.flagged);
        }
        std::visit([&](auto x) { mutate(x, schedule); }, e.payload);
        std::vector<bool> grouped(affected.size());
        for (std::size_t i = 0; i < affected.size(); ++i) {
            if (grouped[i]) continue;
            auto a = engine_.stats(affected[i]);
            if (!qualifies(a)) continue;
            bool crossed = newly_flagged;
            std::vector<Id> eligible;
            for (std::size_t j = i; j < affected.size(); ++j) {
                Id v = affected[j];
                if (j != i && !engine_.connected(affected[i],v)) continue;
                grouped[j] = true;
                crossed |= !before[j];
                auto last = vertices_.at(v).last_alert;
                if (!last || e.time - *last >= policy_.cooldown) eligible.push_back(v);
            }
            if (crossed && !eligible.empty()) {
                alerts.push_back({tenant_,e.epoch,e.lsn,eligible.front(),e.time,*a});
                for (Id v : eligible) vertices_.at(v).last_alert = e.time;
            }
        }
        if (e.source) sequences_[e.source->producer] = e.source->sequence;
        epoch_ = e.epoch;
        lsn_ = e.lsn;
        time_ = e.time;
    }
    bool acceptable(const Command& c, Time now) const {
        if (auto x = std::get_if<VertexUpdate>(&c)) {
            auto it = vertices_.find(x->vertex);
            Id old = it == vertices_.end() ? 0 : it->second.weight.volume;
            return valid(x->vertex) && x->weight.volume <= UINT64_MAX - (total_volume_ - old);
        }
        if (auto x = std::get_if<EdgeUpsert>(&c))
            return valid(x->key.u) && valid(x->key.v) && x->expiry > now;
        if (auto x = std::get_if<EdgeDelete>(&c)) return valid(x->key.u) && valid(x->key.v);
        return true;
    }
public:
    explicit Tenant(Id tenant, Policy policy = {}) : tenant_(tenant), policy_(policy) {}
    const Engine& engine() const { return engine_; }
    std::size_t active_degree(Id v) const {
        auto it = vertices_.find(v);
        return it == vertices_.end() ? 0 : it->second.live.size();
    }
    bool suppressed(Id v) const {
        auto it = vertices_.find(v);
        return it != vertices_.end() && it->second.suppressed;
    }
    std::optional<EdgeRecord> edge(Key k) const {
        auto it = keys_.find(k.normalized());
        if (it == keys_.end()) return {};
        return edges_.at(it->second);
    }
    std::optional<Subgraph> subgraph(Id u, std::size_t max_k) const {
        auto a = engine_.stats(u);
        if (!a) return {};
        auto ids = engine_.component_vertices(u,max_k);
        std::unordered_set<Id> selected(ids.begin(),ids.end());
        Subgraph out{{},{},a->vertex_count,ids.size() < a->vertex_count};
        for (Id v : ids) {
            const auto& vertex = vertices_.at(v);
            out.vertices.push_back({v,vertex.weight});
            for (Handle h : vertex.structural) {
                const auto& e = edges_.at(h);
                if (v == e.key.u && selected.contains(e.key.v)) out.edges.push_back(e);
            }
        }
        return out;
    }

    // first_lsn may skip positions used by other tenants on this shard.
    Batch submit(Id epoch, Id first_lsn, Time now, Command command,
                 std::optional<ProducerSequence> source = {}) {
        if (epoch < epoch_ || first_lsn <= lsn_ || now < time_) return {Status::stale_position};
        if (source && sequences_.contains(source->producer) &&
            sequences_.at(source->producer) >= source->sequence) return {Status::duplicate};
        if (!policy_.valid() || !acceptable(command, now)) return {Status::invalid_input};
        // Each expiration or input may generate at most two hub transitions.
        // Reserve enough LSN space before starting an indivisible batch.
        if ((UINT64_MAX - first_lsn) / 3 < edges_.size() + 1)
            return {Status::stale_position};
        if (rebuild_wheel_) {
            wheel_ = {};
            for (const auto& [h,e] : edges_) wheel_.schedule({e.expiry,h,e.version});
            rebuild_wheel_ = false;
        }
        Batch result;
        auto emit = [&](Payload p, std::optional<ProducerSequence> s = {}) {
            result.records.push_back({tenant_,epoch,first_lsn++,now,s,p});
            apply(result.records.back(),true,result.alerts);
        };
        auto transition = [&](Id v) {
            if (entity(v) == Entity::account) return;
            if (!suppressed(v) && active_degree(v) >= policy_.high) emit(HubSuppress{v});
            else if (suppressed(v) && active_degree(v) <= policy_.low) emit(HubRestore{v});
        };
        while (auto d = wheel_.pop_due(now)) {
            Expire x{d->handle,d->version};
            if (!current(x)) continue;
            auto sides = endpoints(edges_.at(d->handle).key);
            emit(x);
            for (Id v : sides) transition(v);
        }
        std::visit([&](auto x) {
            emit(x,source);
            if constexpr (requires { x.key; })
                for (Id v : endpoints(x.key)) transition(v);
        }, command);
        return result;
    }
    Status apply_record(const Event& e, std::vector<Alert>& alerts) {
        if (e.tenant != tenant_ || e.lsn <= lsn_ || e.epoch < epoch_ || e.time < time_)
            return Status::stale_position;
        // A repeated producer record still occupies its own position in the log.
        if (e.source && sequences_.contains(e.source->producer) &&
            sequences_.at(e.source->producer) >= e.source->sequence) {
            epoch_ = e.epoch;
            lsn_ = e.lsn;
            time_ = e.time;
            return Status::duplicate;
        }
        apply(e,false,alerts);
        rebuild_wheel_ = true;
        return Status::ok;
    }
};
} // namespace fraud
