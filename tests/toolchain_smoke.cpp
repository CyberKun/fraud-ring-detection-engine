#include <bit>
#include <concepts>
#include <cstdint>

static_assert(std::unsigned_integral<std::uint64_t>);
static_assert(std::bit_width(UINT64_MAX) == 64);

int main() { return 0; }
