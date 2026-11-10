#pragma once
#include "core/types.hpp"

namespace fraud::test {
inline void canonicalize(Subgraph& g) {
    std::sort(g.vertices.begin(),g.vertices.end(),[](auto a, auto b) { return a.id < b.id; });
    std::sort(g.edges.begin(),g.edges.end(),[](auto a, auto b) { return a.handle < b.handle; });
}
inline bool equal(Subgraph& a, Subgraph& b) {
    canonicalize(a);
    canonicalize(b);
    auto same_vertex = [](VertexView x, VertexView y) {
        return x.id == y.id && x.weight.volume == y.weight.volume && x.weight.flagged == y.weight.flagged;
    };
    auto same_edge = [](EdgeRecord x, EdgeRecord y) {
        return x.key == y.key && x.handle == y.handle && x.version == y.version &&
               x.expiry == y.expiry && x.shadow == y.shadow;
    };
    return a.truncated == b.truncated && a.total_vertices == b.total_vertices &&
           std::equal(a.vertices.begin(),a.vertices.end(),b.vertices.begin(),b.vertices.end(),same_vertex) &&
           std::equal(a.edges.begin(),a.edges.end(),b.edges.begin(),b.edges.end(),same_edge);
}
} // namespace fraud::test
