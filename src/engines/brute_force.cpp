#include "engines/brute_force.hpp"
#include <limits>
#include <unordered_set>

namespace fraud {
Status BruteForce::set_vertex(Id u, Weight w) {
    if (!valid(u)) return Status::invalid_input;
    w.flagged &= entity(u) == Entity::account;
    vertices_[u].weight = w;
    return Status::ok;
}
Status BruteForce::insert_edge(Handle h, Id u, Id v, Time) {
    if (!valid(u) || !valid(v)) return Status::invalid_input;
    if (!endpoints_.emplace(h, std::pair{u,v}).second) return Status::duplicate;
    vertices_[u].arcs.push_back({v,h});
    if (u != v) vertices_[v].arcs.push_back({u,h});
    return Status::ok;
}
Status BruteForce::delete_edge(Handle h, Removal) {
    auto it = endpoints_.find(h);
    if (it == endpoints_.end()) return Status::invalid_input;
    auto [u,v] = it->second;
    auto matches = [h](Arc a) { return a.handle == h; };
    std::erase_if(vertices_.at(u).arcs, matches);
    if (v != u) std::erase_if(vertices_.at(v).arcs, matches);
    endpoints_.erase(it);
    return Status::ok;
}
std::vector<Id> BruteForce::component_vertices(Id u, std::size_t max_k) const {
    if (max_k == 0 || !vertices_.contains(u)) return {};
    std::vector<Id> queue{u};
    std::unordered_set<Id> seen{u};
    for (std::size_t i = 0; i < queue.size() && queue.size() < max_k; ++i) {
        for (Arc a : vertices_.at(queue[i]).arcs) {
            if (seen.insert(a.neighbor).second) queue.push_back(a.neighbor);
            if (queue.size() == max_k) break;
        }
    }
    return queue;
}
bool BruteForce::connected(Id u, Id v) const {
    auto members = component_vertices(u, std::numeric_limits<std::size_t>::max());
    return std::find(members.begin(), members.end(), v) != members.end();
}
std::optional<Aggregate> BruteForce::stats(Id u) const {
    if (!vertices_.contains(u)) return {};
    Aggregate result;
    for (Id v : component_vertices(u, std::numeric_limits<std::size_t>::max())) {
        const auto w = vertices_.at(v).weight;
        ++result.vertex_count;
        result.account_count += entity(v) == Entity::account;
        result.volume += w.volume;
        result.flagged_accounts += w.flagged;
    }
    return result;
}
} // namespace fraud
