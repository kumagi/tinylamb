/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_HASH_JOIN_MODE_HPP
#define TINYLAMB_HASH_JOIN_MODE_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "executor/query_memory.hpp"

namespace tinylamb {

// Physical hash-join strategy chosen by the planner (or forced in tests).
// kInMemory: build a full in-memory HT (may still reactively spill under
//            QueryMemoryBudget pressure).
// kHybrid:   keep one resident partition in memory; spill the rest and join
//            spilled partitions afterwards (DeWitt-style Hybrid Hash Join).
enum class HashJoinMode : uint8_t {
  kInMemory = 0,
  kHybrid = 1,
};

inline std::string_view HashJoinModeName(HashJoinMode mode) {
  switch (mode) {
    case HashJoinMode::kInMemory:
      return "HashJoin";
    case HashJoinMode::kHybrid:
      return "HybridHashJoin";
  }
  return "HashJoin";
}

// Rough per-row footprint used by the planner when TableStatistics lack width.
inline constexpr size_t kHashJoinRowBytesEstimate = 128;

// Prefer hybrid when the estimated build-side footprint exceeds the soft
// query-memory budget (80% of TINYLAMB_QUERY_MEMORY_BYTES).
[[nodiscard]] inline bool PreferHybridHashJoin(size_t estimated_build_bytes) {
  return !QueryMemoryBudget::Global().CanReserve(estimated_build_bytes);
}

// Partition count so a typical resident partition fits in ~half the remaining
// soft budget (clamped).
[[nodiscard]] inline size_t HybridPartitionCount(size_t estimated_build_bytes) {
  constexpr size_t kMin = 8;
  constexpr size_t kMax = 256;
  constexpr size_t kDefault = 32;
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  if (budget.Unlimited() || estimated_build_bytes == 0) {
    return kDefault;
  }
  const size_t soft =
      budget.Limit() / 5 * 4;  // match QueryMemoryBudget soft fraction
  const size_t used = budget.Used();
  const size_t remaining = used >= soft ? 0 : soft - used;
  const size_t target = std::max(remaining / 2, size_t{1} << 20);
  const size_t needed =
      estimated_build_bytes / target + (estimated_build_bytes % target != 0);
  return std::clamp(std::max(needed, kMin), kMin, kMax);
}

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_MODE_HPP
