#pragma once
#include "core/types.hpp"
#include <string>

namespace fraud::replica {
struct Lease {
    std::string owner, lease;
    Id epoch{};
};
class Election {
  public:
    Election(std::string endpoint, Id shard);
    Lease lookup() const;
    Lease acquire(const std::string &owner, int ttl) const;
    bool renew(const Lease &) const;
    bool holds(const Lease &) const;

  private:
    std::string endpoint_, key_;
};
} // namespace fraud::replica
