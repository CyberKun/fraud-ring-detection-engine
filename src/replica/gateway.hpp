#pragma once
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace fraud::replica::gateway {
std::string b64(const std::string &);
std::string unbase(const std::string &);
nlohmann::json post(const std::string &url, nlohmann::json body);
} // namespace fraud::replica::gateway
