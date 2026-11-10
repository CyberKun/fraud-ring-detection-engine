#pragma once

#include "core/types.hpp"
#include <map>
#include <set>

namespace fraud {
struct Deadline {
    Time expiry;
    Handle handle;
    Id version;
    auto operator<=>(const Deadline&) const = default;
};
class TimingWheel {
    // Sparse absolute slots handle idle gaps and multiple wheel revolutions.
    static constexpr Time slot_ms = 100;
    std::map<Time, std::set<Deadline>> slots_;
public:
    void schedule(Deadline d) { slots_[d.expiry / slot_ms].insert(d); }
    std::optional<Deadline> pop_due(Time now) {
        if (slots_.empty()) return {};
        auto slot = slots_.begin();
        auto entry = slot->second.begin();
        if (entry->expiry > now) return {};
        auto d = *entry;
        slot->second.erase(entry);
        if (slot->second.empty()) slots_.erase(slot);
        return d;
    }
};
} // namespace fraud
