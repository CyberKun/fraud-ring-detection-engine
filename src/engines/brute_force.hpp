#pragma once

#include "core/types.hpp"
#include <unordered_map>

namespace fraud {
// Adjacency consists of contiguous (neighbor, handle) arrays per vertex.
class BruteForce {
    struct Arc { Id neighbor; Handle handle; };
    struct Vertex { Weight weight; std::vector<Arc> arcs; };
    std::unordered_map<Id, Vertex> vertices_;
    std::unordered_map<Handle, std::pair<Id,Id>> endpoints_;
public:
    Status set_vertex(Id u, Weight w);
    Status insert_edge(Handle h, Id u, Id v, Time);
    Status extend_ttl(Handle h, Time) const {
        return endpoints_.contains(h) ? Status::ok : Status::invalid_input;
    }
    Status delete_edge(Handle h, Removal);
    bool connected(Id u, Id v) const;
    std::optional<Aggregate> stats(Id u) const;
    std::vector<Id> component_vertices(Id u, std::size_t max_k) const;
};
static_assert(DynamicGraphEngine<BruteForce>);
} // namespace fraud
