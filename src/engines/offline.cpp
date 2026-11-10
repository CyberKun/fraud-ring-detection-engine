#include "engines/offline.hpp"
#include "engines/rollback_dsu.hpp"
#include <bit>
#include <limits>

namespace fraud {
namespace {
using Dense = RollbackDsu::Index;
using Slot = std::size_t;
using Action = std::size_t;
struct EdgeValue { EdgeRecord edge; Dense u,v; };
struct WeightValue { Dense v; Weight weight; };
struct Open { Slot start; Action action; };

// Even actions refer to weights, odd actions to structural edge records.
struct Timeline {
    std::vector<Time> times;
    std::vector<Id> ids;
    std::unordered_map<Id,Dense> dense;
    std::vector<WeightValue> weights;
    std::vector<EdgeValue> edges;
    std::vector<std::vector<Action>> buckets;
    Slot base{1};
    Dense intern(Id v) {
        auto [it,inserted] = dense.try_emplace(v,static_cast<Dense>(ids.size()));
        if (inserted) ids.push_back(v);
        return it->second;
    }
    void cover(Open op, Slot end) {
        Slot left = op.start + base, right = end + base;
        while (left < right) {
            if (left % 2) buckets[left++].push_back(op.action);
            if (right % 2) buckets[--right].push_back(op.action);
            left /= 2;
            right /= 2;
        }
    }
};

struct Capture {
    Timeline& line;
    std::vector<std::vector<Dense>>& changed;
    Slot slot{};
    std::unordered_map<Id,Open> weights;
    std::unordered_map<Handle,Open> edges;
    void set_vertex(Id v, Weight w) {
        Dense i = line.intern(v);
        auto it = weights.find(v);
        if (it != weights.end()) line.cover(it->second,slot);
        Action action = static_cast<Action>(2 * line.weights.size());
        weights[v] = {slot,action};
        line.weights.push_back({i,w});
        changed[slot].push_back(i);
    }
    void open_edge(const EdgeRecord& edge) {
        Dense u = line.dense.at(edge.key.u), v = line.dense.at(edge.key.v);
        Action action = static_cast<Action>(2 * line.edges.size() + 1);
        edges.emplace(edge.handle,Open{slot,action});
        line.edges.push_back({edge,u,v});
        changed[slot].push_back(u);
        changed[slot].push_back(v);
    }
    void close_edge(Handle h) {
        auto it = edges.find(h);
        const auto& edge = line.edges[it->second.action / 2];
        changed[slot].push_back(edge.u);
        changed[slot].push_back(edge.v);
        line.cover(it->second,slot);
        edges.erase(it);
    }
    void finish() {
        for (const auto& [id,op] : weights) line.cover(op,line.times.size());
        for (const auto& [handle,op] : edges) line.cover(op,line.times.size());
    }
};
// A small reference adapter lets Tenant own its sink while the builder
// controls the current slot and closes surviving intervals at the window end.
struct Sink {
    Capture*& capture;
    void set_vertex(Id v, Weight w) { capture->set_vertex(v,w); }
    void open_edge(const EdgeRecord& e) { capture->open_edge(e); }
    void close_edge(Handle h) { capture->close_edge(h); }
};

class HistoricalView {
    const Timeline& line_;
    std::vector<std::optional<Weight>> weight_;
    std::vector<std::vector<Action>> adjacency_;
public:
    RollbackDsu dsu;
    explicit HistoricalView(const Timeline& line)
        : line_(line), weight_(line.ids.size()), adjacency_(line.ids.size()), dsu(line.ids.size()) {}
    void apply(Action op) {
        if (op % 2) {
            const auto& e = line_.edges[op / 2];
            dsu.join(e.u,e.v);
            adjacency_[e.u].push_back(op);
            if (e.u != e.v) adjacency_[e.v].push_back(op);
        } else {
            const auto& w = line_.weights[op / 2];
            weight_[w.v] = w.weight;
            dsu.add_weight(w.v,entity(line_.ids[w.v]),w.weight);
        }
    }
    void remove(Action op) {
        if (op % 2) {
            const auto& e = line_.edges[op / 2];
            adjacency_[e.u].pop_back();
            if (e.u != e.v) adjacency_[e.v].pop_back();
        } else weight_[line_.weights[op / 2].v].reset();
    }
    std::vector<Id> component_vertices(Id seed, std::size_t max_k) const {
        auto it = line_.dense.find(seed);
        if (it == line_.dense.end() || !weight_[it->second]) return {};
        std::vector<Id> ids;
        for (Dense v : dsu.component_vertices(it->second,max_k)) ids.push_back(line_.ids[v]);
        return ids;
    }
    std::optional<AuditResult> audit(AuditQuery q, const std::vector<std::optional<Time>>& first) const {
        auto it = line_.dense.find(q.vertex);
        if (it == line_.dense.end() || !weight_[it->second]) return {};
        Dense seed = it->second;
        auto sum = dsu.stats(seed);
        Subgraph graph{{},{},sum.vertex_count,sum.vertex_count > q.max_nodes};
        auto members = component_vertices(q.vertex,q.max_nodes);
        std::unordered_set<Id> selected(members.begin(),members.end());
        for (Id id : members) {
            Dense v = line_.dense.at(id);
            graph.vertices.push_back({id,*weight_[v]});
            for (Action op : adjacency_[v]) {
                const auto& e = line_.edges[op / 2];
                if (v == e.u && selected.contains(e.edge.key.v)) graph.edges.push_back(e.edge);
            }
        }
        auto earliest = first[seed];
        if (earliest && *earliest > q.time) earliest.reset();
        std::optional<bool> connected;
        if (q.peer) {
            auto peer = line_.dense.find(*q.peer);
            connected = peer != line_.dense.end() && weight_[peer->second] &&
                        dsu.root(peer->second) == dsu.root(seed);
        }
        return AuditResult{sum,std::move(graph),earliest,connected};
    }
};

// Query traversal prunes disjoint branches. Index construction visits all real
// leaves once. Every applied action is undone in reverse order on return.
template<class Visitor>
void walk(const Timeline& line, HistoricalView& view, Slot node, Slot lo, Slot hi, Visitor& visit) {
    if (lo >= line.times.size() || !visit.includes(lo,hi)) return;
    auto snapshot = view.dsu.snapshot();
    const auto& ops = line.buckets[node];
    for (Action op : ops) view.apply(op);
    if (hi-lo == 1) visit.leaf(lo);
    else {
        Slot mid = lo + (hi-lo)/2;
        walk(line,view,node*2,lo,mid,visit);
        walk(line,view,node*2+1,mid,hi,visit);
    }
    view.dsu.rollback(snapshot);
    for (auto it = ops.rbegin(); it != ops.rend(); ++it) view.remove(*it);
}

struct Crossings {
    HistoricalView& view;
    const Timeline& line;
    const std::vector<std::vector<Dense>>& changed;
    Policy policy;
    bool includes(Slot,Slot) const { return true; }
    void leaf(Slot t) {
        // Any component whose qualification or membership changed contains a
        // touched endpoint. Tag its root once or more (min is idempotent).
        for (Dense v : changed[t]) {
            const auto& a = view.dsu.stats(v);
            if (a.account_count >= policy.min_accounts && a.flagged_ratio() >= policy.min_ratio)
                view.dsu.mark_component(v,line.times[t]);
        }
    }
};

struct Queries {
    HistoricalView& view;
    std::span<const AuditQuery> input;
    const std::vector<std::optional<Time>>& first;
    std::vector<std::optional<AuditResult>>& output;
    std::vector<std::pair<Slot,std::size_t>> ordered;
    std::size_t next{};
    bool includes(Slot lo,Slot hi) const {
        return next < ordered.size() && lo <= ordered[next].first && ordered[next].first < hi;
    }
    void leaf(Slot t) {
        while (next < ordered.size() && ordered[next].first == t) {
            auto i = ordered[next++].second;
            output[i] = view.audit(input[i],first);
        }
    }
};
} // namespace

struct OfflineEngine::Index {
    Timeline line;
    Time end;
    std::vector<std::optional<Time>> first;
};

std::optional<OfflineEngine> OfflineEngine::build(Id tenant, Policy policy,
                                                  std::span<const Event> records, Time end_ms) {
    return OfflineReplay(tenant,policy).next_window(records,end_ms);
}

struct OfflineReplay::State {
    Policy policy;
    Capture* capture{};
    Tenant<Sink> projection;
    std::vector<VertexView> vertices;
    std::vector<EdgeRecord> edges;
    std::unordered_map<Id,Time> first;
    Time start{};
    bool failed{};
    State(Id tenant, Policy p) : policy(p), projection(tenant,p,Sink{capture}) {}
};
OfflineReplay::OfflineReplay(Id tenant,Policy policy) : state_(std::make_unique<State>(tenant,policy)) {}
OfflineReplay::~OfflineReplay() = default;
std::optional<OfflineEngine> OfflineReplay::next_window(std::span<const Event> records, Time end_ms) {
    auto& s = *state_;
    // Each input record can introduce at most two external vertex IDs.
    if (s.failed || !s.policy.valid() || end_ms < s.start ||
        records.size() > (std::numeric_limits<Dense>::max()-s.vertices.size())/2) return {};
    auto index = std::make_shared<OfflineEngine::Index>();
    index->end = end_ms;
    auto& line = index->line;
    if (!s.vertices.empty() && s.start < end_ms) line.times.push_back(s.start);
    for (const auto& e : records) {
        if (e.time < s.start || e.time >= end_ms || (!line.times.empty() && e.time < line.times.back())) return {};
        if (line.times.empty() || e.time != line.times.back()) line.times.push_back(e.time);
    }
    line.base = std::bit_ceil(std::max<Slot>(1,line.times.size()));
    line.buckets.resize(2*line.base);
    {
        std::vector<std::vector<Dense>> changed(line.times.size());
        {
            Capture capture{line,changed};
            if (line.times.empty()) {
                s.start = end_ms;
                return OfflineEngine(std::move(index));
            }
            s.capture = &capture;
            for (auto v : s.vertices) capture.set_vertex(v.id,v.weight);
            for (const auto& e : s.edges) capture.open_edge(e);
            std::vector<Alert> unused;
            for (const auto& e : records) {
                if (line.times[capture.slot] != e.time) ++capture.slot;
                auto status = s.projection.apply_record(e,unused);
                if (status != Status::ok && status != Status::duplicate) {
                    s.failed = true;
                    s.capture = nullptr;
                    return {};
                }
            }
            capture.finish();
            s.capture = nullptr;
            s.vertices.clear();
            s.edges.clear();
            for (const auto& [id,op] : capture.weights)
                s.vertices.push_back({id,line.weights[op.action/2].weight});
            for (const auto& [handle,op] : capture.edges)
                s.edges.push_back(line.edges[op.action/2].edge);
        }
        HistoricalView view(line);
        Crossings crossings{view,line,changed,s.policy};
        walk(line,view,1,0,line.base,crossings);
        for (Dense i = 0; i < line.ids.size(); ++i) {
            auto first = view.dsu.singleton_first(i);
            if (first) s.first.try_emplace(line.ids[i],*first);
            auto it = s.first.find(line.ids[i]);
            if (it != s.first.end()) first = it->second;
            index->first.push_back(first);
        }
    }
    s.start = end_ms;
    return OfflineEngine(std::move(index));
}

std::vector<std::optional<AuditResult>> OfflineEngine::audit_many(std::span<const AuditQuery> queries) const {
    const auto& line = index_->line;
    HistoricalView view(line);
    std::vector<std::optional<AuditResult>> output(queries.size());
    Queries visitor{view,queries,index_->first,output};
    for (std::size_t i = 0; i < queries.size(); ++i) {
        Time t = queries[i].time;
        if (t >= index_->end) continue;
        auto after = std::upper_bound(line.times.begin(),line.times.end(),t);
        if (after != line.times.begin()) visitor.ordered.emplace_back(after-line.times.begin()-1,i);
    }
    std::sort(visitor.ordered.begin(),visitor.ordered.end());
    walk(line,view,1,0,line.base,visitor);
    return output;
}

std::optional<AuditResult> OfflineEngine::audit_at(Time t, Id v, std::size_t max_nodes) const {
    AuditQuery q{t,v,max_nodes};
    return std::move(audit_many(std::span(&q,1)).front());
}
bool OfflineEngine::connected_at(Time t, Id u, Id v) const {
    AuditQuery q{t,u,0,v};
    auto result = audit_many(std::span(&q,1));
    return result.front() && result.front()->connected_to_peer.value_or(false);
}
} // namespace fraud
