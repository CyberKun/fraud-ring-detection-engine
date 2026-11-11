#pragma once
#include "core/types.hpp"
#include <filesystem>
#include <functional>
#include <span>
#include <stdexcept>

namespace fraud::storage {
using Bytes = std::vector<std::uint8_t>;
using Frame = std::vector<Event>;
using Sink = std::function<void(const Frame&)>;
struct Error : std::runtime_error { using runtime_error::runtime_error; };
constexpr std::uint32_t max_payload=16*1024*1024, max_records=65536;
std::uint32_t checksum(std::span<const std::uint8_t>);
Bytes encode(const Frame&);
Frame decode_frame(std::span<const std::uint8_t>);
struct Recovery { Id lsn{},epoch{},time{},frames{},truncated{}; };
Recovery recover(const std::filesystem::path&, Recovery previous, bool repair_tail, const Sink&);
void sync_directory(const std::filesystem::path&);
void atomic_file(const std::filesystem::path&,std::span<const std::uint8_t>);
Bytes read_file(const std::filesystem::path&,std::size_t cap);
struct WriteStats { std::size_t bytes{}; double sync_seconds{}; };
class Wal {
public:
    explicit Wal(const std::filesystem::path&);
    ~Wal();
    Wal(const Wal&)=delete;
    Wal& operator=(const Wal&)=delete;
    WriteStats append(std::span<const Frame>);
    void truncate(std::uint64_t bytes);
private:
    int fd_;
};
} // namespace fraud::storage
