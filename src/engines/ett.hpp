#pragma once

#include "core/types.hpp"
#include <utility>

namespace fraud {
// Implicit treap of cyclic Euler tours. All links are stable slab indices.
class EulerTourForest {
public:
    using Index = std::uint32_t;
    using Darts = std::pair<Index,Index>;
    enum Flag : unsigned char { tree = 1, nontree = 2 };
    explicit EulerTourForest(bool fraud_weights);
    void add_vertex();
    void set_weight(Index v, const Aggregate& weight);
    Index root(Index v) const;
    Index count(Index v) const { return nodes_[root(v)].count; }
    Aggregate stats(Index v) const;
    void reroot(Index v);
    Darts link(Index u,Index v,Handle h,bool exact);
    void cut(Darts darts);
    void set_exact(Darts darts,bool exact);
    void set_incident(Index v,bool incident);
    std::optional<Handle> exact_edge(Index v) const;
    std::optional<Index> incident_vertex(Index v) const;
    std::vector<Index> component_vertices(Index v,std::size_t limit) const;
    bool validate(Index bound) const;
    bool contiguous_tour(Index v) const;
    bool matches(Darts darts,Index u,Index v,Handle h,bool exact) const;
    bool has_incident(Index v) const { return nodes_[loops_[v]].local_flags & nontree; }
    std::size_t dart_count() const { return nodes_.size()-1-loops_.size()-free_.size(); }
    std::size_t slab_bytes() const {
        return nodes_.capacity()*sizeof(Node)+weights_.capacity()*sizeof(Weights)+
               (loops_.capacity()+free_.capacity())*sizeof(Index);
    }
private:
    struct Node {
        Index left{}, right{}, parent{}, size{}, count{}, from{}, to{};
        Id priority{}, handle{};
        unsigned char local_flags{}, subtree_flags{};
    };
    struct Fraud { Id accounts{}, volume{}, flagged{}; bool operator==(const Fraud&) const = default; };
    struct Weights { Fraud self,sum; };
    std::vector<Node> nodes_;
    std::vector<Weights> weights_; // Empty on every level above zero.
    std::vector<Index> loops_, free_;
    Id serial_{};
    Index allocate(Index from,Index to,Handle,unsigned char flags);
    void release(Index);
    void pull(Index);
    void refresh(Index);
    Index merge(Index,Index);
    std::pair<Index,Index> split(Index,Index prefix);
    Index top(Index) const;
    Index rank(Index) const;
    Index find_flag(Index,Flag) const;
    void set_flag(Index,unsigned char);
    void collect(Index,Index seed,std::size_t,std::vector<Index>&) const;
    bool continuous(Index,Index& first,Index& previous) const;
};
} // namespace fraud
