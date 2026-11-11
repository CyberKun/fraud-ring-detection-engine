#include "router/directory.hpp"
#include "replica/gateway.hpp"
#include <charconv>
#include <nlohmann/json.hpp>
namespace fraud::routing {
using namespace replica::gateway;
using json = nlohmann::json;
namespace {
Id number(std::string_view s) {
    Id n{};
    auto [end, error] = std::from_chars(s.data(), s.data() + s.size(), n);
    if (error != std::errc{} || end != s.data() + s.size())
        throw std::invalid_argument("invalid topology ID");
    return n;
}
} // namespace
int Topology::partition(Id tenant) const {
    if (auto it = pins.find(tenant); it != pins.end())
        return it->second;
    // FNV-1a over the eight low-to-high bytes of the unsigned tenant ID.
    Id hash = 14695981039346656037ull;
    for (unsigned i = 0; i < 8; ++i) {
        hash ^= (tenant >> (8 * i)) & 255;
        hash *= 1099511628211ull;
    }
    return static_cast<int>(hash % partitions.size());
}
std::shared_ptr<const Topology> Directory::get(bool refresh) {
    using namespace std::chrono;
    std::lock_guard guard(mutex_);
    if (!refresh && current_ && steady_clock::now() - checked_ < milliseconds(150))
        return current_;
    json map = {{"key", b64("/shard_map")}};
    json keys = {{"key", b64("/shards/")}, {"range_end", b64("/shards0")}};
    auto txn = post(endpoint_ + "/v3/kv/txn",
                    {{"success", json::array({{{"request_range", map}}, {{"request_range", keys}}})}});
    auto revision = number(txn.at("header").at("revision").get<std::string>());
    if (current_ && revision < current_->revision)
        throw std::runtime_error("etcd revision regressed");
    if (!current_ || current_->revision != revision) {
        auto next = std::make_shared<Topology>();
        auto &ranges = txn.at("responses");
        auto &entry = ranges.at(0).at("response_range").at("kvs").at(0);
        auto config = json::parse(unbase(entry.at("value")));
        auto &partitions = config.at("partitions");
        if (!partitions.is_array())
            throw std::invalid_argument("expected partition array");
        for (auto &shard : partitions) {
            if (!shard.is_number_unsigned() || shard.get<Id>() > UINT32_MAX)
                throw std::invalid_argument("invalid shard id");
            next->partitions.push_back(shard.get<Id>());
        }
        if (next->partitions.empty() || next->partitions.size() > INT32_MAX)
            throw std::invalid_argument("partition count");
        for (auto &[key, value] : config.at("members").items()) {
            auto addresses = value.get<std::vector<std::string>>();
            if (addresses.size() != 3 || std::find(addresses.begin(), addresses.end(), "") != addresses.end())
                throw std::invalid_argument("expected three member addresses");
            next->members.emplace(number(key), std::move(addresses));
        }
        for (auto shard : next->partitions)
            next->members.at(shard);
        if (config.contains("pins"))
            for (auto &[key, value] : config.at("pins").items()) {
                if (!value.is_number_unsigned() || value.get<Id>() >= next->partitions.size())
                    throw std::invalid_argument("pin outside partition range");
                next->pins.emplace(number(key), static_cast<int>(value.get<Id>()));
            }
        for (auto &kv : ranges.at(1).at("response_range").value("kvs", json::array())) {
            auto key = unbase(kv.at("key"));
            if (!key.ends_with("/leader"))
                continue;
            Id shard = number(std::string_view(key).substr(8, key.size() - 15));
            if (!next->members.contains(shard))
                continue;
            auto owner = unbase(kv.at("value"));
            auto member = number(owner.substr(0, owner.find('/')));
            next->leaders[shard] = {next->members.at(shard).at(member),
                                    number(kv.at("create_revision").get<std::string>())};
        }
        next->revision = revision;
        next->map_revision = number(entry.at("mod_revision").get<std::string>());
        current_ = std::move(next);
    }
    checked_ = steady_clock::now();
    return current_;
}
} // namespace fraud::routing
