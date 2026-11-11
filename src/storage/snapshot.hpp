#pragma once
#include "core/tenant.hpp"
#include "storage/wal.hpp"

namespace fraud::storage {
struct Image {
    Id epoch{1},lsn{},time{};
    Policy policy;
    std::uint32_t capacity{};
    std::vector<TenantImage> tenants;
    std::uint32_t boundary_crc{};
};
Bytes serialize(const Image&);
Image deserialize(std::span<const std::uint8_t>);
std::string logical_checksum(const Image&);
constexpr std::size_t snapshot_cap=256u*1024*1024;
}
