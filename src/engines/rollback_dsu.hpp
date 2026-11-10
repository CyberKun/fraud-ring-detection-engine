#pragma once

#include "core/types.hpp"
#include <numeric>
#include <utility>

namespace fraud {
// Structural state and weights rewind at each DFS boundary. Earliest timestamps
// accumulate across the chronological traversal and survive rollback.
class RollbackDsu {
public:
    using Index = std::uint32_t;
    explicit RollbackDsu(std::size_t n) : nodes_(n) {
        for (Index i = 0; i < n; ++i) {
            nodes_[i].parent = i;
            nodes_[i].next = i;
        }
    }
    Index root(Index v) const {
        while (nodes_[v].parent != v) v = nodes_[v].parent;
        return v;
    }
    const Aggregate& stats(Index v) const { return nodes_[root(v)].sum; }
    std::size_t snapshot() const { return undo_.size(); }
    void join(Index a, Index b) {
        a = root(a);
        b = root(b);
        if (a == b) return;
        if (nodes_[a].size < nodes_[b].size) std::swap(a,b);
        auto& parent = nodes_[a];
        auto& child = nodes_[b];
        undo_.push_back({a,b,parent.sum,parent.first});
        // A mark made before this merge applies only to the original members.
        parent.first.reset();
        child.parent = a;
        parent.size += child.size;
        std::swap(parent.next,child.next);
        parent.sum.vertex_count += child.sum.vertex_count;
        parent.sum.account_count += child.sum.account_count;
        parent.sum.volume += child.sum.volume;
        parent.sum.flagged_accounts += child.sum.flagged_accounts;
    }
    void add_weight(Index v, Entity kind, Weight w) {
        Index a = root(v);
        auto& sum = nodes_[a].sum;
        undo_.push_back({a,a,sum,{}});
        ++sum.vertex_count;
        sum.account_count += kind == Entity::account;
        sum.volume += w.volume;
        sum.flagged_accounts += kind == Entity::account && w.flagged;
    }
    void mark_component(Index v, Time t) { remember(nodes_[root(v)].first,t); }
    std::optional<Time> singleton_first(Index v) const { return nodes_[v].first; }
    void rollback(std::size_t checkpoint) {
        while (undo_.size() > checkpoint) {
            auto change = undo_.back();
            undo_.pop_back();
            auto& parent = nodes_[change.a];
            parent.sum = change.sum;
            if (change.a == change.b) continue;
            auto& child = nodes_[change.b];
            child.parent = change.b;
            parent.size -= child.size;
            std::swap(parent.next,child.next);
            // Marks made while joined reach both halves. Nested rollback
            // eventually distributes them to the singleton members.
            if (parent.first) remember(child.first,*parent.first);
            if (change.previous_first) remember(parent.first,*change.previous_first);
        }
    }
    std::vector<Index> component_vertices(Index seed, std::size_t max_k) const {
        std::vector<Index> out;
        Index v = seed;
        while (out.size() < max_k) {
            out.push_back(v);
            v = nodes_[v].next;
            if (v == seed) break;
        }
        return out;
    }
private:
    struct Node {
        Index parent{}, size{1}, next{};
        Aggregate sum;
        std::optional<Time> first;
    };
    struct Change {
        Index a,b;
        Aggregate sum;
        std::optional<Time> previous_first;
    };
    static void remember(std::optional<Time>& first, Time time) {
        if (!first || time < *first) first = time;
    }
    std::vector<Node> nodes_;
    std::vector<Change> undo_;
};
} // namespace fraud
