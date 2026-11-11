#include "engines/hdt.hpp"
#include <bit>
#include <cassert>
#include <limits>
#include <stdexcept>

namespace fraud {
HdtEngine::HdtEngine(Index capacity) : capacity_(capacity) {
    if (!capacity || capacity>std::numeric_limits<Index>::max()/3)
        throw std::invalid_argument("invalid HDT vertex capacity");
    unsigned count=std::bit_width(capacity);
    levels_.reserve(count);
    for (unsigned i=0;i<count;++i) levels_.emplace_back(i==0);
}
HdtEngine::Index HdtEngine::ensure(Id v) {
    auto [it,inserted]=ids_.try_emplace(v,static_cast<Index>(external_.size()));
    if (inserted) {
        external_.push_back(v);
        for (auto& level : levels_) level.forest.add_vertex();
        levels_.front().forest.set_weight(it->second,{1,entity(v)==Entity::account,0,0});
    }
    return it->second;
}
Status HdtEngine::set_vertex(Id v,Weight w) {
    if (!valid(v) || (!remaining_vertices() && !ids_.contains(v))) return Status::invalid_input;
    bool account=entity(v)==Entity::account;
    levels_.front().forest.set_weight(ensure(v),{1,account,w.volume,account && w.flagged});
    return Status::ok;
}
bool HdtEngine::level_connected(unsigned i,Id u,Id v) const {
    auto a=ids_.find(u), b=ids_.find(v);
    if (i>=levels_.size() || a==ids_.end() || b==ids_.end()) return false;
    const auto& f=levels_[i].forest;
    return f.root(a->second)==f.root(b->second);
}
bool HdtEngine::connected(Id u,Id v) const { return level_connected(0,u,v); }
std::optional<Aggregate> HdtEngine::stats(Id u) const {
    auto it=ids_.find(u);
    if (it==ids_.end()) return {};
    return levels_[0].forest.stats(it->second);
}
std::vector<Id> HdtEngine::component_vertices(Id u,std::size_t limit) const {
    std::vector<Id> result;
    auto it=ids_.find(u);
    if (it!=ids_.end())
        for (Index x : levels_[0].forest.component_vertices(it->second,limit)) result.push_back(external_[x]);
    return result;
}
std::optional<HdtEngine::EdgeInfo> HdtEngine::edge_info(Handle h) const {
    auto it=edges_.find(h);
    if (it==edges_.end()) return {};
    return EdgeInfo{it->second.level,it->second.tree()};
}
void HdtEngine::list(Handle h,const Edge& e) {
    auto& level=levels_[e.level];
    for (Index v : {e.u,e.v}) {
        auto& adj=level.incident[v];
        adj.insert(h);
        if (adj.size()==1) level.forest.set_incident(v,true);
    }
}
void HdtEngine::unlist(Handle h,const Edge& e) {
    auto& level=levels_[e.level];
    for (Index v : {e.u,e.v}) {
        auto it=level.incident.find(v);
        assert(it!=level.incident.end());
        it->second.erase(h);
        if (it->second.empty()) {
            level.incident.erase(it);
            level.forest.set_incident(v,false);
        }
    }
}
void HdtEngine::link_at(Handle h,Edge& e,unsigned i) {
    assert(e.darts.size()==i);
    auto& forest=levels_[i].forest;
    e.darts.push_back(forest.link(e.u,e.v,h,i==e.level));
    assert(forest.count(e.u)<=(capacity_>>i));
}
void HdtEngine::make_tree(Handle h,Edge& e) {
    for (unsigned i=0;i<=e.level;++i) link_at(h,e,i);
}
Status HdtEngine::insert_edge(Handle h,Id u,Id v,Time) {
    if (edges_.contains(h)) return Status::duplicate;
    if (!valid(u) || !valid(v)) return Status::invalid_input;
    std::size_t needed=(!ids_.contains(u))+(u!=v && !ids_.contains(v));
    if (needed>remaining_vertices()) return Status::invalid_input;
    auto& e=edges_.emplace(h,Edge{ensure(u),ensure(v),0,{}}).first->second;
    // A graph self-edge remains queryable in Tenant and never crosses a cut.
    if (u!=v) {
        if (connected(u,v)) list(h,e);
        else make_tree(h,e);
    }
    return Status::ok;
}
Status HdtEngine::extend_ttl(Handle h,Time) const {
    return edges_.contains(h) ? Status::ok : Status::invalid_input;
}
void HdtEngine::replace(Index u,Index v,unsigned removed_level) {
    ++counters_.searches;
    for (int level=static_cast<int>(removed_level);level>=0;--level) {
        unsigned i=static_cast<unsigned>(level);
        auto& layer=levels_[i];
        auto& forest=layer.forest;
        ++counters_.levels_traversed;
        Index small=forest.count(u)<=forest.count(v) ? u : v;
        // Promote the smaller tree before testing its non-tree edges. Every
        // internal candidate then has a connecting path in the next forest.
        while (auto h=forest.exact_edge(small)) {
            auto& e=edges_.at(*h);
            assert(e.level==i && i+1<levels_.size());
            forest.set_exact(e.darts[i],false);
            ++e.level;
            link_at(*h,e,i+1);
            ++counters_.tree_promotions;
            counters_.highest_level=std::max(counters_.highest_level,e.level);
        }
        while (auto x=forest.incident_vertex(small)) {
            Handle h=*layer.incident.at(*x).begin();
            auto& e=edges_.at(h);
            ++counters_.candidates;
            unlist(h,e);
            if (forest.root(e.u)!=forest.root(e.v)) {
                make_tree(h,e);
                ++counters_.replacements;
                counters_.lower_level_replacements+=i<removed_level;
                return;
            }
            assert(e.level==i && i+1<levels_.size());
            ++e.level;
            assert(levels_[e.level].forest.root(e.u)==levels_[e.level].forest.root(e.v));
            list(h,e);
            ++counters_.nontree_promotions;
            counters_.highest_level=std::max(counters_.highest_level,e.level);
        }
    }
}
Status HdtEngine::delete_edge(Handle h,Removal) {
    auto it=edges_.find(h);
    if (it==edges_.end()) return Status::invalid_input;
    auto& e=it->second;
    if (e.tree()) {
        for (unsigned i=0;i<=e.level;++i) levels_[i].forest.cut(e.darts[i]);
        replace(e.u,e.v,e.level);
    } else if (e.u!=e.v) unlist(h,e);
    edges_.erase(it);
    return Status::ok;
}
bool HdtEngine::validate() const {
    std::vector<std::size_t> darts(levels_.size());
    for (const auto& [h,e] : edges_) {
        if (e.level>=levels_.size()) return false;
        if (e.tree()) {
            if (e.darts.size()!=e.level+1) return false;
            for (unsigned i=0;i<=e.level;++i) {
                if (!levels_[i].forest.matches(e.darts[i],e.u,e.v,h,i==e.level)) return false;
                darts[i]+=2;
            }
        } else if (e.u!=e.v) {
            const auto& level=levels_[e.level];
            if (level.forest.root(e.u)!=level.forest.root(e.v)) return false;
            for (Index v : {e.u,e.v}) {
                auto it=level.incident.find(v);
                if (it==level.incident.end() || !it->second.contains(h)) return false;
            }
        }
    }
    for (unsigned i=0;i<levels_.size();++i) {
        const auto& level=levels_[i];
        if (level.forest.dart_count()!=darts[i] || !level.forest.validate(capacity_>>i)) return false;
        for (Index v=0;v<external_.size();++v)
            if (level.forest.has_incident(v)!=level.incident.contains(v)) return false;
        for (const auto& [v,adj] : level.incident) {
            if (adj.empty()) return false;
            for (Handle h : adj) {
                auto it=edges_.find(h);
                if (it==edges_.end()) return false;
                const auto& e=it->second;
                if (e.tree() || e.level!=i || (e.u!=v && e.v!=v)) return false;
            }
        }
    }
    return true;
}
} // namespace fraud
