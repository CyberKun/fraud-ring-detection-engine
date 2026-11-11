#pragma once
#include "core/types.hpp"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace fraud::routing {
struct Topology {
    Id revision{}, map_revision{};
    std::vector<Id> partitions;
    std::map<Id, int> pins;
    std::map<Id, std::vector<std::string>> members;
    struct Leader {
        std::string address;
        Id epoch;
    };
    std::map<Id, Leader> leaders;
    int partition(Id tenant) const;
    Id shard(Id tenant) const { return partitions.at(partition(tenant)); }
};
class Directory {
  public:
    explicit Directory(std::string endpoint) : endpoint_(std::move(endpoint)) {}
    std::shared_ptr<const Topology> get(bool refresh = false);

  private:
    std::string endpoint_;
    std::mutex mutex_;
    std::shared_ptr<const Topology> current_;
    std::chrono::steady_clock::time_point checked_{};
};
} // namespace fraud::routing
