/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_COMMON_JOIN_HASH_HPP
#define TINYLAMB_COMMON_JOIN_HASH_HPP

#include <cstdint>

namespace tinylamb {

inline constexpr uint64_t kJoinHashMultiplier1 = 0x9E3779B97F4A7C15ULL;
inline constexpr uint64_t kJoinHashMultiplier2 = 0xBF58476D1CE4E5B9ULL;
inline constexpr unsigned kJoinHashShift1 = 32;
inline constexpr unsigned kJoinHashShift2 = 29;

[[nodiscard]] inline uint64_t MixJoinHash(uint64_t hash) noexcept {
  hash *= kJoinHashMultiplier1;
  hash ^= hash >> kJoinHashShift1;
  hash *= kJoinHashMultiplier2;
  return hash ^ (hash >> kJoinHashShift2);
}

}  // namespace tinylamb

#endif
