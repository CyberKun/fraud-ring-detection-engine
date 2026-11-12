#pragma once

#include "engines/ett.hpp"
#include "engines/memory.hpp"
#include <set>
#include <unordered_map>

namespace fraud {
class HdtEngine {
public:
    using Index = EulerTourForest::Index;
    explicit HdtEngine(Index capacity);
    Status set_vertex(Id,Weight);
    Status insert_edge(Handle,Id,Id,Time);
    Status extend_ttl(Handle,Time) const;
    Status delete_edge(Handle,Removal);
    bool connected(Id,Id) const;
    std::optional<Aggregate> stats(Id) const;
    std::vector<Id> component_vertices(Id,std::size_t) const;
    std::size_t remaining_vertices() const { return capacity_-ids_.size(); }
    struct Counters {
        Id searches{}, levels_traversed{}, candidates{}, tree_promotions{}, nontree_promotions{},
           replacements{}, lower_level_replacements{};
        unsigned highest_level{};
    };
    const Counters& counters() const { return counters_; }
    static constexpr std::size_t edge_bytes() { return sizeof(Edge); }
    std::size_t slab_bytes() const {
        std::size_t bytes=0;
        for(const auto& level:levels_) bytes+=level.forest.slab_bytes();
        return bytes;
    }
    std::size_t storage_bytes() const {
        auto bytes=sizeof(*this)+levels_.capacity()*sizeof(Level)+external_.capacity()*sizeof(Id)
                  +table_bytes(ids_)+table_bytes(edges_);
        for (const auto& level:levels_) {
            bytes+=level.forest.slab_bytes()+table_bytes(level.incident);
            for (const auto& [v,handles]:level.incident) bytes+=table_bytes(handles);
        }
        for (const auto& [h,e]:edges_) bytes+=e.darts.capacity()*sizeof(EulerTourForest::Darts);
        return bytes;
    }
    struct EdgeInfo { unsigned level; bool tree; };
    std::optional<EdgeInfo> edge_info(Handle) const;
    bool level_connected(unsigned,Id,Id) const;
    bool validate() const;
private:
    struct Level {
        explicit Level(bool weighted) : forest(weighted) {}
        EulerTourForest forest;
        std::unordered_map<Index,std::set<Handle>> incident;
    };
    struct Edge {
        Index u,v;
        unsigned level{};
        std::vector<EulerTourForest::Darts> darts;
        bool tree() const { return !darts.empty(); }
    };
    Index capacity_;
    std::vector<Level> levels_;
    std::unordered_map<Id,Index> ids_;
    std::vector<Id> external_;
    std::unordered_map<Handle,Edge> edges_;
    Counters counters_;
    Index ensure(Id);
    void list(Handle,const Edge&);
    void unlist(Handle,const Edge&);
    void link_at(Handle,Edge&,unsigned);
    void make_tree(Handle,Edge&);
    void replace(Index,Index,unsigned);
};
static_assert(DynamicGraphEngine<HdtEngine>);
} // namespace fraud
