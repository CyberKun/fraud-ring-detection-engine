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
struct SavedVertex { Id id; Weight weight; bool suppressed; std::optional<Time> cooldown; };
struct TenantImage {
    Id tenant,epoch,lsn,next_handle,total_volume;
    Time time;
    Policy policy;
    std::vector<SavedVertex> vertices;
    std::vector<EdgeRecord> edges;
    std::vector<Deadline> deadlines;
    std::vector<std::pair<Id,Id>> sequences;
};

// Single-writer tenant projection. apply_record consumes trusted ordered records.
// The service owns shard-wide LSN allocation and eventual durable publication.
// Live engines answer connectivity queries. A replay sink can instead consume
// the exact structural records (including key, version and TTL) for indexing.
template<class Engine>
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
        e.shadow = false;
        if constexpr (DynamicGraphEngine<Engine>)
            engine_.insert_edge(e.handle, e.key.u, e.key.v, e.expiry);
        else
            engine_.open_edge(e);
        for (Id v : endpoints(e.key)) vertices_.at(v).structural.insert(e.handle);
    }
    void unlink(EdgeRecord& e, Removal why) {
        if constexpr (DynamicGraphEngine<Engine>) engine_.delete_edge(e.handle, why);
        else engine_.close_edge(e.handle);
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
            if (!e.shadow) {
                if constexpr (DynamicGraphEngine<Engine>) engine_.extend_ttl(e.handle,e.expiry);
                else {
                    engine_.close_edge(e.handle);
                    engine_.open_edge(e);
                }
            }
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
        if constexpr (requires { engine_.remaining_vertices(); }) {
            if (auto x = std::get_if<VertexUpdate>(&c)) {
                if (!vertices_.contains(x->vertex) && !engine_.remaining_vertices()) return false;
            }
            if (auto x = std::get_if<EdgeUpsert>(&c)) {
                const auto& k = x->key;
                std::size_t needed = !vertices_.contains(k.u) + (k.u != k.v && !vertices_.contains(k.v));
                if (needed > engine_.remaining_vertices()) return false;
            }
        }
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
    Tenant(Id tenant, Policy policy, Engine engine)
        : tenant_(tenant), policy_(policy), engine_(std::move(engine)) {}
    const Engine& engine() const { return engine_; }
    std::size_t suppressed_count() const {
        return std::count_if(vertices_.begin(),vertices_.end(),[](auto& entry){ return entry.second.suppressed; });
    }
    TenantImage image() const {
        TenantImage s{tenant_,epoch_,lsn_,next_handle_,total_volume_,time_,policy_};
        for (const auto& [id,v]:vertices_) s.vertices.push_back({id,v.weight,v.suppressed,v.last_alert});
        std::sort(s.vertices.begin(),s.vertices.end(),[](auto& a,auto& b){return a.id<b.id;});
        for (const auto& [h,e]:edges_) {
            s.edges.push_back(e);
            s.deadlines.push_back({e.expiry,h,e.version});
        }
        std::sort(s.edges.begin(),s.edges.end(),[](auto& a,auto& b){return a.handle<b.handle;});
        std::sort(s.deadlines.begin(),s.deadlines.end());
        s.sequences.assign(sequences_.begin(),sequences_.end());
        std::sort(s.sequences.begin(),s.sequences.end());
        return s;
    }
    // Load into a fresh projection. The caller discards it on any error.
    bool restore(const TenantImage& s) {
        if (s.tenant!=tenant_ || !s.policy.valid() || !s.next_handle || !vertices_.empty() ||
            s.vertices.size()>engine_.remaining_vertices()) return false;
        policy_=s.policy;
        for (const auto& v:s.vertices) {
            if (!valid(v.id) || vertices_.contains(v.id) || v.weight.volume>UINT64_MAX-total_volume_ ||
                (v.cooldown && *v.cooldown>s.time) || (v.weight.flagged && entity(v.id)!=Entity::account) ||
                (v.suppressed && entity(v.id)==Entity::account)) return false;
            mutate(VertexUpdate{v.id,v.weight},false);
            vertices_.at(v.id).suppressed=v.suppressed;
            vertices_.at(v.id).last_alert=v.cooldown;
        }
        if (total_volume_!=s.total_volume || s.deadlines.size()!=s.edges.size()) return false;
        for (const auto& e:s.edges) {
            if (!e.handle || e.handle>=s.next_handle || !e.version || e.key!=e.key.normalized() ||
                !vertices_.contains(e.key.u) || !vertices_.contains(e.key.v) || filtered(e.key)!=e.shadow ||
                !keys_.emplace(e.key,e.handle).second || !edges_.emplace(e.handle,e).second) return false;
            for (Id v:endpoints(e.key)) vertices_.at(v).live.insert(e.handle);
            if (!e.shadow) link(edges_.at(e.handle));
        }
        std::optional<Deadline> prev;
        for (auto d:s.deadlines) {
            auto it=edges_.find(d.handle);
            if ((prev && d<=*prev) || it==edges_.end() || it->second.expiry!=d.expiry ||
                it->second.version!=d.version) return false;
            wheel_.schedule(d);
            prev=d;
        }
        for (auto [p,q]:s.sequences) if (!sequences_.emplace(p,q).second) return false;
        next_handle_=s.next_handle; epoch_=s.epoch; lsn_=s.lsn; time_=s.time;
        return true;
    }
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
        // Stable handle-ordered BFS also fixes the membership of truncated
        // results across different forest shapes and snapshot reconstruction.
        std::vector<Id> ids;
        std::unordered_set<Id> seen;
        if (max_k>=a->vertex_count) {
            ids=engine_.component_vertices(u,max_k);
            if (!ids.empty()) std::sort(ids.begin()+1,ids.end());
        } else if (max_k) { ids.push_back(u); seen.insert(u); }
        for (std::size_t i=0;i<ids.size() && max_k<a->vertex_count && ids.size()<max_k;++i) {
            Id v=ids[i];
            for (Handle h:vertices_.at(v).structural) {
                const auto k=edges_.at(h).key;
                Id other=k.u==v ? k.v : k.u;
                if (seen.insert(other).second) ids.push_back(other);
                if (ids.size()==max_k) break;
            }
        }
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
    struct DirectApply {
        template<class F> void operator()(const Payload&,F apply) const { apply(); }
    };
    template<class Observer=DirectApply>
    Batch submit(Id epoch, Id first_lsn, Time now, Command command,
                 std::optional<ProducerSequence> source = {}, Observer observe = {}) {
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
            observe(p,[&]{ apply(result.records.back(),true,result.alerts); });
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
        if constexpr (requires { engine_.remaining_vertices(); }) {
            if (auto x = std::get_if<EdgeUpsert>(&e.payload); x && !acceptable(*x,e.time))
                return Status::invalid_input;
            if (auto x = std::get_if<VertexUpdate>(&e.payload); x && !acceptable(*x,e.time))
                return Status::invalid_input;
        }
        if constexpr (DynamicGraphEngine<Engine>) {
            apply(e,false,alerts);
        } else {
            std::visit([&](auto x) { mutate(x,false); }, e.payload);
            if (e.source) sequences_[e.source->producer] = e.source->sequence;
            epoch_ = e.epoch;
            lsn_ = e.lsn;
            time_ = e.time;
        }
        rebuild_wheel_ = true;
        return Status::ok;
    }
};
} // namespace fraud
