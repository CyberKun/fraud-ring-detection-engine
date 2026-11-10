#pragma once

#include <algorithm>
#include <compare>
#include <concepts>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace fraud {
using Id = std::uint64_t;
using Handle = std::uint64_t;
using Time = std::uint64_t;
enum class Entity : Id { account, card, device };
constexpr Id id_mask = (Id{1} << 62) - 1;
constexpr Entity entity(Id id) { return static_cast<Entity>(id >> 62); }
constexpr bool valid(Id id) { return entity(id) <= Entity::device; }
constexpr std::optional<Id> encode(Entity kind, Id local) {
    if (kind > Entity::device || local > id_mask) return {};
    return (static_cast<Id>(kind) << 62) | local;
}

struct Weight { Id volume{}; bool flagged{}; };
struct Aggregate {
    Id vertex_count{}, account_count{}, volume{}, flagged_accounts{};
    double flagged_ratio() const {
        return account_count ? static_cast<double>(flagged_accounts) / account_count : 0.;
    }
    bool operator==(const Aggregate&) const = default;
};
struct Key {
    Id u, v;
    std::uint32_t type;
    auto operator<=>(const Key&) const = default;
    Key normalized() const { return {std::min(u,v), std::max(u,v), type}; }
};
struct KeyHash {
    std::size_t operator()(Key k) const noexcept {
        auto mix = [](Id n) {
            n = (n ^ (n >> 30)) * 0xbf58476d1ce4e5b9ULL;
            return (n ^ (n >> 27)) * 0x94d049bb133111ebULL;
        };
        return mix(k.u ^ mix(k.v ^ mix(k.type)));
    }
};
struct EdgeUpsert { Key key; Time expiry; };
struct EdgeDelete { Key key; };
struct VertexUpdate { Id vertex; Weight weight; };
struct Tick {};
struct Expire { Handle handle; Id version; };
struct HubSuppress { Id vertex; };
struct HubRestore { Id vertex; };
using Payload = std::variant<EdgeUpsert, EdgeDelete, VertexUpdate, Tick, Expire, HubSuppress, HubRestore>;
using Command = std::variant<EdgeUpsert, EdgeDelete, VertexUpdate, Tick>;
struct ProducerSequence { Id producer, sequence; };
struct Event {
    Id tenant, epoch, lsn;
    Time time;
    std::optional<ProducerSequence> source;
    Payload payload;
};
enum class Removal { manual, expiry, suppression };
enum class Status { ok, invalid_input, stale_position, duplicate };
struct EdgeRecord { Key key; Handle handle; Id version; Time expiry; bool shadow; };
struct VertexView { Id id; Weight weight; };
struct Subgraph {
    std::vector<VertexView> vertices;
    std::vector<EdgeRecord> edges;
    Id total_vertices;
    bool truncated;
};
struct Alert { Id tenant, epoch, lsn, vertex; Time time; Aggregate stats; };

template<class E>
concept DynamicGraphEngine = requires(E e, const E reader, Id u, Id v, Handle h, Weight w, Time t, std::size_t k) {
    { e.set_vertex(u,w) } -> std::same_as<Status>;
    { e.insert_edge(h,u,v,t) } -> std::same_as<Status>;
    { e.extend_ttl(h,t) } -> std::same_as<Status>;
    { e.delete_edge(h,Removal::manual) } -> std::same_as<Status>;
    { reader.connected(u,v) } -> std::same_as<bool>;
    { reader.stats(u) } -> std::same_as<std::optional<Aggregate>>;
    { reader.component_vertices(u,k) } -> std::same_as<std::vector<Id>>;
};
} // namespace fraud
