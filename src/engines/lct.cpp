#include "engines/lct.hpp"
#include <cassert>
#include <limits>
#include <stdexcept>

namespace fraud {
namespace {
void add(Aggregate& a, const Aggregate& b) {
    a.vertex_count += b.vertex_count;
    a.account_count += b.account_count;
    a.volume += b.volume;
    a.flagged_accounts += b.flagged_accounts;
}
void subtract(Aggregate& a, const Aggregate& b) {
    a.vertex_count -= b.vertex_count;
    a.account_count -= b.account_count;
    a.volume -= b.volume;
    a.flagged_accounts -= b.flagged_accounts;
}
}
LctEngine::LctEngine(Index capacity) : capacity_(capacity) {
    if (!capacity || capacity > std::numeric_limits<Index>::max()/2)
        throw std::invalid_argument("invalid LCT vertex capacity");
    nodes_.resize(2*capacity-1);
    adj_.resize(capacity);
    marked_.resize(capacity);
    free_.reserve(capacity-1);
    ancestors_.reserve(nodes_.size());
    for (Index i = 2*capacity-1; i > capacity; --i) free_.push_back(i);
}
bool LctEngine::aux_root(Index x) const {
    Index p = node(x).parent;
    return !p || (node(p).child[0] != x && node(p).child[1] != x);
}
void LctEngine::pull(Index x) const {
    auto& n = node(x);
    n.sum = n.self;
    add(n.sum,n.virtual_sum);
    n.minimum = x > capacity_ ? x : 0;
    for (Index c : n.child) {
        if (!c) continue;
        add(n.sum,node(c).sum);
        Index m = node(c).minimum;
        if (m && (!n.minimum || node(m).key < node(n.minimum).key)) n.minimum = m;
    }
}
void LctEngine::reverse(Index x) const {
    if (!x) return;
    auto& n = node(x);
    std::swap(n.child[0],n.child[1]);
    n.reversed = !n.reversed;
}
void LctEngine::push(Index x) const {
    auto& n = node(x);
    if (n.reversed) {
        reverse(n.child[0]);
        reverse(n.child[1]);
        n.reversed = false;
    }
}
void LctEngine::rotate(Index x) const {
    Index p = node(x).parent, g = node(p).parent;
    int side = node(p).child[1] == x;
    Index middle = node(x).child[1-side];
    if (!aux_root(p)) node(g).child[node(g).child[1] == p] = x;
    node(x).parent = g;
    node(p).child[side] = middle;
    if (middle) node(middle).parent = p;
    node(x).child[1-side] = p;
    node(p).parent = x;
    pull(p);
    pull(x);
}
void LctEngine::splay(Index x) const {
    // Directions used by zig-zig/zig-zag must reflect every pending reroot.
    ancestors_.clear();
    for (Index a=x;; a=node(a).parent) {
        ancestors_.push_back(a);
        if (aux_root(a)) break;
    }
    for (auto it=ancestors_.rbegin(); it!=ancestors_.rend(); ++it) push(*it);
    while (!aux_root(x)) {
        Index p=node(x).parent;
        if (!aux_root(p)) {
            Index g=node(p).parent;
            bool aligned = (node(g).child[1] == p) == (node(p).child[1] == x);
            rotate(aligned ? p : x);
        }
        rotate(x);
    }
}
void LctEngine::access(Index x) const {
    Index preferred = 0;
    for (Index a=x; a; a=node(a).parent) {
        splay(a);
        auto& n = node(a);
        // The old preferred subtree becomes virtual, and the newly exposed
        // subtree leaves the virtual sum. Path minima exclude both virtual sets.
        if (n.child[1]) add(n.virtual_sum,node(n.child[1]).sum);
        if (preferred) subtract(n.virtual_sum,node(preferred).sum);
        n.child[1] = preferred;
        pull(a);
        preferred = a;
    }
    splay(x);
}
void LctEngine::makeroot(Index x) const { access(x); reverse(x); }
LctEngine::Index LctEngine::root(Index x) const {
    access(x);
    push(x);
    while (node(x).child[0]) {
        x = node(x).child[0];
        push(x);
    }
    splay(x);
    return x;
}
void LctEngine::link(Index u,Index v) {
    makeroot(u);
    access(v);
    node(u).parent = v;
    // A parent pointer outside the auxiliary tree is a virtual attachment.
    add(node(v).virtual_sum,node(u).sum);
    pull(v);
}
void LctEngine::cut(Index u,Index v) {
    makeroot(u);
    access(v);
    assert(node(v).child[0] == u && !node(u).child[1]);
    node(v).child[0] = 0;
    node(u).parent = 0;
    pull(v);
}
LctEngine::Index LctEngine::ensure(Id v) {
    auto [it,inserted] = ids_.try_emplace(v,static_cast<Index>(ids_.size()+1));
    if (inserted) {
        external_.push_back(v);
        node(it->second).self = {1,entity(v)==Entity::account,0,0};
        pull(it->second);
    }
    return it->second;
}
Status LctEngine::set_vertex(Id v, Weight w) {
    if (!valid(v) || (!ids_.contains(v) && !remaining_vertices())) return Status::invalid_input;
    Index x = ensure(v);
    access(x);
    node(x).self = {1,entity(v)==Entity::account,w.volume,entity(v)==Entity::account && w.flagged};
    pull(x);
    return Status::ok;
}
bool LctEngine::connected(Id u,Id v) const {
    auto a=ids_.find(u), b=ids_.find(v);
    if (a==ids_.end() || b==ids_.end()) return false;
    Index r = root(a->second);
    return r == root(b->second);
}
std::optional<Aggregate> LctEngine::stats(Id u) const {
    auto it=ids_.find(u);
    if (it==ids_.end()) return {};
    access(it->second);
    return node(it->second).sum;
}
std::vector<LctEngine::Index> LctEngine::members(Index seed,std::size_t limit) const {
    if (!limit) return {};
    std::vector<Index> queue{seed}, parents{0};
    for (std::size_t i=0; i<queue.size() && queue.size()<limit; ++i) {
        Index v=queue[i];
        for (Handle h : adj_[v-1].tree) {
            auto& e=edges_.at(h);
            Index other=e.u==v ? e.v : e.u;
            if (other == parents[i]) continue;
            queue.push_back(other);
            parents.push_back(v);
            if (queue.size()==limit) break;
        }
    }
    return queue;
}
std::vector<Id> LctEngine::component_vertices(Id v,std::size_t limit) const {
    std::vector<Id> result;
    auto it=ids_.find(v);
    if (it!=ids_.end())
        for (Index x : members(it->second,limit)) result.push_back(external_[x-1]);
    return result;
}
void LctEngine::extra(const Edge& e,bool insert) {
    for (Index v : {e.u,e.v}) {
        auto& set=adj_[v-1].extra;
        if (insert) set.insert(e.key.second);
        else set.erase(e.key.second);
    }
}
void LctEngine::attach(Edge& e) {
    assert(!free_.empty() && !e.dummy);
    e.dummy=free_.back();
    free_.pop_back();
    node(e.dummy).key=e.key;
    pull(e.dummy);
    link(e.u,e.dummy);
    link(e.dummy,e.v);
    adj_[e.u-1].tree.insert(e.key.second);
    adj_[e.v-1].tree.insert(e.key.second);
    counters_.peak_dummies=std::max<Id>(counters_.peak_dummies,dummy_count());
}
void LctEngine::detach(Edge& e) {
    cut(e.u,e.dummy);
    cut(e.dummy,e.v);
    adj_[e.u-1].tree.erase(e.key.second);
    adj_[e.v-1].tree.erase(e.key.second);
    node(e.dummy)={};
    free_.push_back(e.dummy);
    e.dummy=0;
}
void LctEngine::reconsider(Edge& e) {
    if (e.u==e.v) return;
    makeroot(e.u);
    access(e.v);
    Index m=node(e.v).minimum;
    if (m && node(m).key < e.key) {
        auto& old=edges_.at(node(m).key.second);
        detach(old);
        extra(old,true);
        extra(e,false);
        attach(e);
        ++counters_.swaps;
    }
}
Status LctEngine::insert_edge(Handle h,Id u,Id v,Time expiry) {
    if (edges_.contains(h)) return Status::duplicate;
    if (!valid(u) || !valid(v)) return Status::invalid_input;
    std::size_t needed = !ids_.contains(u) + static_cast<std::size_t>(v!=u && !ids_.contains(v));
    if (needed > remaining_vertices()) return Status::invalid_input;
    auto& e=edges_.emplace(h,Edge{ensure(u),ensure(v),0,{expiry,h}}).first->second;
    if (!connected(u,v)) attach(e);
    else {
        extra(e,true);
        reconsider(e);
    }
    return Status::ok;
}
Status LctEngine::extend_ttl(Handle h,Time expiry) {
    auto it=edges_.find(h);
    if (it==edges_.end()) return Status::invalid_input;
    auto& e=it->second;
    if (expiry<=e.key.first) return Status::ok;
    e.key.first=expiry;
    if (e.dummy) {
        access(e.dummy);
        node(e.dummy).key=e.key;
        pull(e.dummy);
    } else reconsider(e);
    return Status::ok;
}
void LctEngine::repair(Index u,Index v) {
    access(u);
    Id left=node(u).sum.vertex_count;
    access(v);
    auto small=members(left<=node(v).sum.vertex_count ? u : v,capacity_);
    ++counters_.repairs;
    counters_.repair_vertices+=small.size();
    for (Index x : small) marked_[x-1]=true;
    Edge* best=nullptr;
    for (Index x : small) {
        for (Handle h : adj_[x-1].extra) {
            auto& e=edges_.at(h);
            ++counters_.candidates;
            if (marked_[e.u-1]!=marked_[e.v-1] && (!best || best->key<e.key)) best=&e;
        }
    }
    for (Index x : small) marked_[x-1]=false;
    if (best) {
        extra(*best,false);
        attach(*best);
        ++counters_.promotions;
    }
}
Status LctEngine::delete_edge(Handle h,Removal reason) {
    auto it=edges_.find(h);
    if (it==edges_.end()) return Status::invalid_input;
    auto& e=it->second;
    if (e.dummy) {
        detach(e);
        if (reason!=Removal::expiry) repair(e.u,e.v);
    } else extra(e,false);
    edges_.erase(it);
    return Status::ok;
}
bool LctEngine::tree_edge(Handle h) const {
    auto it=edges_.find(h);
    return it!=edges_.end() && it->second.dummy!=0;
}
} // namespace fraud
