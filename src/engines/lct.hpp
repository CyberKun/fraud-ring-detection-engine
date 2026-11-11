#pragma once

#include "core/types.hpp"
#include "engines/memory.hpp"
#include <unordered_map>
#include <unordered_set>

namespace fraud {
class LctEngine {
public:
    explicit LctEngine(std::uint32_t capacity);
    Status set_vertex(Id, Weight);
    Status insert_edge(Handle, Id, Id, Time);
    Status extend_ttl(Handle, Time);
    Status delete_edge(Handle, Removal);
    bool connected(Id, Id) const;
    std::optional<Aggregate> stats(Id) const;
    std::vector<Id> component_vertices(Id, std::size_t) const;
    std::size_t remaining_vertices() const { return capacity_ - ids_.size(); }
    std::size_t dummy_count() const { return capacity_ - 1 - free_.size(); }
    std::size_t node_slots() const { return nodes_.size(); }
    std::size_t storage_bytes() const {
        auto bytes=sizeof(*this)+nodes_.capacity()*sizeof(Node)+adj_.capacity()*sizeof(Adj)
                  +(free_.capacity()+ancestors_.capacity())*sizeof(Index)+external_.capacity()*sizeof(Id)
                  +marked_.capacity()/8+table_bytes(ids_)+table_bytes(edges_);
        for (const auto& a:adj_) bytes+=table_bytes(a.tree)+table_bytes(a.extra);
        return bytes;
    }
    bool tree_edge(Handle h) const;
    struct Counters {
        Id repairs{}, repair_vertices{}, candidates{}, swaps{}, promotions{}, peak_dummies{};
    };
    const Counters& counters() const { return counters_; }
private:
    using Index = std::uint32_t;
    using Key = std::pair<Time,Handle>;
    struct Node {
        Index parent{}, child[2]{}, minimum{};
        bool reversed{};
        Aggregate self, sum, virtual_sum;
        Key key{};
    };
    struct Edge { Index u,v,dummy; Key key; };
    struct Adj {
        std::unordered_set<Handle> tree, extra;
    };
    Index capacity_;
    // Slot identifiers are one-based. Zero is null and consumes no slab slot.
    mutable std::vector<Node> nodes_;
    mutable std::vector<Index> ancestors_;
    std::vector<Index> free_;
    std::unordered_map<Id,Index> ids_;
    std::vector<Id> external_;
    std::vector<Adj> adj_;
    std::unordered_map<Handle,Edge> edges_;
    std::vector<bool> marked_;
    Counters counters_;

    Node& node(Index i) const { return nodes_[i-1]; }
    bool aux_root(Index) const;
    void pull(Index) const;
    void reverse(Index) const;
    void push(Index) const;
    void rotate(Index) const;
    void splay(Index) const;
    void access(Index) const;
    void makeroot(Index) const;
    Index root(Index) const;
    void link(Index,Index);
    void cut(Index,Index);
    Index ensure(Id);
    void attach(Edge&);
    void detach(Edge&);
    void extra(const Edge&,bool);
    void reconsider(Edge&);
    void repair(Index,Index);
    std::vector<Index> members(Index,std::size_t) const;
};
static_assert(DynamicGraphEngine<LctEngine>);
} // namespace fraud
