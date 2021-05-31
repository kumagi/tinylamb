#ifndef PEDASOS_CONSTANTS_HPP
#define PEDASOS_CONSTANTS_HPP

#include <limits>

static constexpr size_t kPageSize = 1024 * 32;
static_assert(kPageSize <= std::numeric_limits<uint16_t>::max());

#endif  // PEDASOS_CONSTANTS_HPP
