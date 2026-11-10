#pragma once

#include "core/tenant.hpp"
#include <memory>
#include <span>

namespace fraud {
struct AuditQuery { Time time; Id vertex; std::size_t max_nodes; std::optional<Id> peer{}; };
struct AuditResult {
    Aggregate stats;
    Subgraph subgraph;
    std::optional<Time> first_alert_timestamp_ms;
    std::optional<bool> connected_to_peer;
};

// Immutable after build. Input is a bounded, ordered stream of complete,
// trusted tenant WAL batches beginning at empty state. end_ms is exclusive.
class OfflineEngine {
public:
    static std::optional<OfflineEngine> build(Id tenant, Policy policy,
                                              std::span<const Event> records, Time end_ms);
    std::optional<AuditResult> audit_at(Time t, Id v, std::size_t max_nodes) const;
    bool connected_at(Time t, Id u, Id v) const;
    std::vector<std::optional<AuditResult>> audit_many(std::span<const AuditQuery> queries) const;
private:
    friend class OfflineReplay;
    struct Index;
    explicit OfflineEngine(std::shared_ptr<const Index> index) : index_(std::move(index)) {}
    std::shared_ptr<const Index> index_;
};

// Successive disjoint windows over one continuous WAL. Retains policy state
// and first crossings between windows, so consumed segment trees can be freed.
class OfflineReplay {
public:
    OfflineReplay(Id tenant, Policy policy);
    ~OfflineReplay();
    std::optional<OfflineEngine> next_window(std::span<const Event> records, Time end_ms);
private:
    struct State;
    std::unique_ptr<State> state_;
};
} // namespace fraud
