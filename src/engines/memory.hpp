#pragma once
#include <cstddef>

namespace fraud {
// Count owned element storage and hash buckets. Allocator headers, padding
// outside the stored value, and implementation-specific tree links are excluded.
template<class Table>
std::size_t table_bytes(const Table& table) {
    auto bytes=table.size()*sizeof(typename Table::value_type);
    if constexpr (requires { table.bucket_count(); }) bytes+=table.bucket_count()*sizeof(void*);
    return bytes;
}
}
