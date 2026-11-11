#include "engines/ett.hpp"
#include <cassert>

namespace fraud {
EulerTourForest::EulerTourForest(bool fraud_weights) : nodes_(1) {
    if (fraud_weights) weights_.resize(1);
}
EulerTourForest::Index EulerTourForest::allocate(Index from,Index to,Handle h,unsigned char flags) {
    Index x;
    if (free_.empty()) {
        x=static_cast<Index>(nodes_.size());
        nodes_.emplace_back();
        if (!weights_.empty()) weights_.emplace_back();
    } else {
        x=free_.back();
        free_.pop_back();
    }
    // SplitMix64 is a permutation, giving unique priorities for this forest.
    Id p=(++serial_)*0x9e3779b97f4a7c15ULL;
    p=(p^(p>>30))*0xbf58476d1ce4e5b9ULL;
    p=(p^(p>>27))*0x94d049bb133111ebULL;
    auto& n=nodes_[x];
    n.priority=p^(p>>31);
    n.from=from;
    n.to=to;
    n.handle=h;
    n.local_flags=flags;
    pull(x);
    return x;
}
void EulerTourForest::release(Index x) {
    assert(nodes_[x].size==1 && nodes_[x].from!=nodes_[x].to);
    nodes_[x]={};
    if (!weights_.empty()) weights_[x]={};
    free_.push_back(x);
}
void EulerTourForest::pull(Index x) {
    auto& n=nodes_[x];
    const auto& l=nodes_[n.left];
    const auto& r=nodes_[n.right];
    n.size=1+l.size+r.size;
    n.count=(n.from==n.to)+l.count+r.count;
    n.subtree_flags=n.local_flags|l.subtree_flags|r.subtree_flags;
    if (!weights_.empty()) {
        auto& w=weights_[x];
        const auto& a=weights_[n.left].sum;
        const auto& b=weights_[n.right].sum;
        w.sum={w.self.accounts+a.accounts+b.accounts,w.self.volume+a.volume+b.volume,
               w.self.flagged+a.flagged+b.flagged};
    }
}
void EulerTourForest::refresh(Index x) {
    for (;x;x=nodes_[x].parent) pull(x);
}
void EulerTourForest::add_vertex() {
    Index v=static_cast<Index>(loops_.size());
    loops_.push_back(allocate(v,v,0,0));
}
void EulerTourForest::set_weight(Index v,const Aggregate& w) {
    assert(!weights_.empty() && w.vertex_count==1);
    Index x=loops_[v];
    weights_[x].self={w.account_count,w.volume,w.flagged_accounts};
    refresh(x);
}
EulerTourForest::Index EulerTourForest::top(Index x) const {
    while (nodes_[x].parent) x=nodes_[x].parent;
    return x;
}
EulerTourForest::Index EulerTourForest::root(Index v) const { return top(loops_[v]); }
EulerTourForest::Index EulerTourForest::rank(Index x) const {
    Index pos=nodes_[nodes_[x].left].size;
    while (Index p=nodes_[x].parent) {
        if (nodes_[p].right==x) pos+=1+nodes_[nodes_[p].left].size;
        x=p;
    }
    return pos;
}
Aggregate EulerTourForest::stats(Index v) const {
    Index r=root(v);
    auto w=weights_.empty() ? Fraud{} : weights_[r].sum;
    return {nodes_[r].count,w.accounts,w.volume,w.flagged};
}
EulerTourForest::Index EulerTourForest::merge(Index a,Index b) {
    if (!a || !b) {
        Index r=a ? a : b;
        if (r) nodes_[r].parent=0;
        return r;
    }
    if (nodes_[a].priority>nodes_[b].priority) {
        Index child=merge(nodes_[a].right,b);
        nodes_[a].right=child;
        nodes_[child].parent=a;
    } else {
        Index child=merge(a,nodes_[b].left);
        nodes_[b].left=child;
        nodes_[child].parent=b;
        a=b;
    }
    nodes_[a].parent=0;
    pull(a);
    return a;
}
std::pair<EulerTourForest::Index,EulerTourForest::Index> EulerTourForest::split(Index x,Index prefix) {
    if (!x) return {};
    auto& n=nodes_[x];
    Index left_size=nodes_[n.left].size;
    n.parent=0;
    if (prefix<=left_size) {
        auto [a,b]=split(n.left,prefix);
        n.left=b;
        if (b) nodes_[b].parent=x;
        pull(x);
        return {a,x};
    }
    auto [a,b]=split(n.right,prefix-left_size-1);
    n.right=a;
    if (a) nodes_[a].parent=x;
    pull(x);
    return {x,b};
}
void EulerTourForest::reroot(Index v) {
    auto [a,b]=split(root(v),rank(loops_[v]));
    merge(b,a);
}
EulerTourForest::Darts EulerTourForest::link(Index u,Index v,Handle h,bool exact) {
    assert(root(u)!=root(v));
    reroot(u);
    reroot(v);
    Index left=root(u),right=root(v);
    Index uv=allocate(u,v,h,exact ? tree : 0);
    Index vu=allocate(v,u,h,exact ? tree : 0);
    merge(merge(left,uv),merge(right,vu));
    return {uv,vu};
}
void EulerTourForest::cut(Darts darts) {
    auto [uv,vu]=darts;
    Index i=rank(uv), j=rank(vu);
    if (i>j) std::swap(i,j);
    auto [before,tail]=split(top(uv),i);
    auto [first,rest]=split(tail,1);
    auto [inside,last]=split(rest,j-i-1);
    auto [second,after]=split(last,1);
    assert(inside && (before || after));
    merge(after,before);
    release(first);
    release(second);
}
void EulerTourForest::set_flag(Index x,unsigned char f) {
    if (nodes_[x].local_flags==f) return;
    nodes_[x].local_flags=f;
    refresh(x);
}
void EulerTourForest::set_exact(Darts darts,bool exact) {
    set_flag(darts.first,exact ? tree : 0);
    set_flag(darts.second,exact ? tree : 0);
}
void EulerTourForest::set_incident(Index v,bool present) {
    set_flag(loops_[v],present ? nontree : 0);
}
EulerTourForest::Index EulerTourForest::find_flag(Index v,Flag f) const {
    Index x=root(v);
    if (!(nodes_[x].subtree_flags & f)) return 0;
    while (!(nodes_[x].local_flags & f)) {
        Index left=nodes_[x].left;
        x=(nodes_[left].subtree_flags & f) ? left : nodes_[x].right;
        assert(x && (nodes_[x].subtree_flags & f));
    }
    return x;
}
std::optional<Handle> EulerTourForest::exact_edge(Index v) const {
    if (Index x=find_flag(v,tree)) return nodes_[x].handle;
    return {};
}
std::optional<EulerTourForest::Index> EulerTourForest::incident_vertex(Index v) const {
    if (Index x=find_flag(v,nontree)) return nodes_[x].from;
    return {};
}
void EulerTourForest::collect(Index x,Index seed,std::size_t limit,std::vector<Index>& result) const {
    if (!nodes_[x].count || result.size()==limit) return;
    const auto& n=nodes_[x];
    collect(n.left,seed,limit,result);
    if (n.from==n.to && n.from!=seed && result.size()<limit) result.push_back(n.from);
    collect(n.right,seed,limit,result);
}
std::vector<EulerTourForest::Index> EulerTourForest::component_vertices(Index v,std::size_t limit) const {
    if (!limit) return {};
    std::vector<Index> result{v};
    collect(root(v),v,limit,result);
    return result;
}
bool EulerTourForest::matches(Darts d,Index u,Index v,Handle h,bool exact) const {
    auto match=[&](Index x,Index from,Index to) {
        return x && x<nodes_.size() && nodes_[x].from==from && nodes_[x].to==to &&
               nodes_[x].handle==h && nodes_[x].local_flags==(exact ? tree : 0);
    };
    return match(d.first,u,v) && match(d.second,v,u);
}
bool EulerTourForest::continuous(Index x,Index& first,Index& previous) const {
    if (!x) return true;
    const auto& n=nodes_[x];
    if (!continuous(n.left,first,previous)) return false;
    if (!first) first=x;
    if (previous && nodes_[previous].to!=n.from) return false;
    previous=x;
    return continuous(n.right,first,previous);
}
bool EulerTourForest::contiguous_tour(Index v) const {
    Index first=0,previous=0;
    return continuous(root(v),first,previous) && nodes_[previous].to==nodes_[first].from;
}
bool EulerTourForest::validate(Index bound) const {
    std::size_t loops=0, empty=0;
    for (Index x=1;x<nodes_.size();++x) {
        const auto& n=nodes_[x];
        if (!n.size) { ++empty; continue; }
        bool self=n.from==n.to;
        if (self && (n.from>=loops_.size() || loops_[n.from]!=x)) return false;
        loops+=self;
        if (n.local_flags & ~(self ? nontree : tree)) return false;
        if (!n.parent && (n.count>bound || !n.count || n.size!=3*n.count-2)) return false;
        if (n.parent && nodes_[n.parent].left!=x && nodes_[n.parent].right!=x) return false;
        for (Index c : {n.left,n.right})
            if (c && (nodes_[c].parent!=x || nodes_[c].priority>=n.priority)) return false;
        const auto& a=nodes_[n.left];
        const auto& b=nodes_[n.right];
        if (n.count!=self+a.count+b.count || n.size!=1+a.size+b.size ||
            n.subtree_flags!=(n.local_flags|a.subtree_flags|b.subtree_flags)) return false;
        if (!weights_.empty()) {
            auto w=weights_[x];
            auto l=weights_[n.left].sum,r=weights_[n.right].sum;
            if (!self && w.self!=Fraud{}) return false;
            if (w.sum!=Fraud{w.self.accounts+l.accounts+r.accounts,w.self.volume+l.volume+r.volume,
                             w.self.flagged+l.flagged+r.flagged}) return false;
        }
    }
    return loops==loops_.size() && empty==free_.size();
}
} // namespace fraud
