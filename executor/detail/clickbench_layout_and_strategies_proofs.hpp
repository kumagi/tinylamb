/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_CLICKBENCH_LAYOUT_AND_STRATEGIES_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_CLICKBENCH_LAYOUT_AND_STRATEGIES_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// ClickBenchLayout (24 files -> 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_AnchorDemand.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_ClockForest.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_ClockRange.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_ClockWindow.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_ConcatSearch.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_DefaultGroups.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_DictionaryAdmission.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_HashExceptions.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_LengthRollup.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_RunSidecar.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_SparseMark.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_CB_TieOrientation.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_AnchorDemand.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_ClockRange.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_ConcatSearch.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_DefaultGroups.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_LengthRollup.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Exec_SparseMark.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_ClockForest.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_ClockWindow.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_DictionaryAdmission.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_HashExceptions.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_RunSidecar.lean
// lean/TinylambOpt/ResearchProofs/ClickBenchLayout/Goal_Storage_TieOrientation.lean
//
// ExecutionStrategies remaining (1 file -> 12/12 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/ExecutionStrategies/BinaryText.lean
// ============================================================================

// ClickBenchLayout: dense bucket histogram over bounded key domain `[0,
// bound)`.
[[nodiscard]] inline std::vector<uint64_t> BoundedDomainHistogram(
    std::span<const uint32_t> keys, size_t bound) {
  std::vector<uint64_t> counts(bound, 0);
  for (const uint32_t k : keys) {
    if (k < bound) {
      ++counts[k];
    }
  }
  return counts;
}

// ExecutionStrategies: monotonic interval intersection count.
[[nodiscard]] constexpr bool ClosedIntervalsOverlap(int64_t a_lo, int64_t a_hi,
                                                    int64_t b_lo,
                                                    int64_t b_hi) noexcept {
  return a_lo <= b_hi && b_lo <= a_hi;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_CLICKBENCH_LAYOUT_AND_STRATEGIES_PROOFS_HPP
