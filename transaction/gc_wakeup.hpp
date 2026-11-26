/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TRANSACTION_GC_WAKEUP_HPP
#define TINYLAMB_TRANSACTION_GC_WAKEUP_HPP

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>

namespace tinylamb::mvcc {

// ============================================================================
// EngineDecisions.MVCC.GcWakeup
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_GcWakeup.lean
//     (GcWakeup.Goal)
// After one GC sweep over an oldest-first committed version chain, another
// sweep can release an entry at `(snap, durable)` if and only if either:
//   1. `live.size() >= 2` and `live[1].ts <= snap`, OR
//   2. `!pending` and `!live.empty()` and `live.back().ts <= snap &&
//      live.back().lsn <= durable`.
// ============================================================================
struct GcVersionStamp {
  uint64_t ts{0};
  uint64_t lsn{0};
};

[[nodiscard]] inline bool IsGcChainWakeupDue(
    std::span<const GcVersionStamp> live_oldest_first, bool pending,
    uint64_t snap, uint64_t durable) noexcept {
  if (live_oldest_first.size() >= 2 && live_oldest_first[1].ts <= snap) {
    return true;
  }
  if (!pending && !live_oldest_first.empty()) {
    const GcVersionStamp& last = live_oldest_first.back();
    if (last.ts <= snap && last.lsn <= durable) {
      return true;
    }
  }
  return false;
}

// Aggregate wakeup fence across all chains retained by a full GC sweep.
struct GcSweepWakeupFence {
  uint64_t min_second_ts{std::numeric_limits<uint64_t>::max()};
  uint64_t min_tail_ts{std::numeric_limits<uint64_t>::max()};
  uint64_t min_tail_lsn{std::numeric_limits<uint64_t>::max()};
  bool has_unpinned_tail{false};

  void Observe(std::span<const GcVersionStamp> live_oldest_first,
               bool pending) noexcept {
    if (live_oldest_first.size() >= 2 &&
        live_oldest_first[1].ts < min_second_ts) {
      min_second_ts = live_oldest_first[1].ts;
    }
    if (!pending && !live_oldest_first.empty()) {
      has_unpinned_tail = true;
      const GcVersionStamp& last = live_oldest_first.back();
      min_tail_ts = std::min(last.ts, min_tail_ts);
      min_tail_lsn = std::min(last.lsn, min_tail_lsn);
    }
  }

  // Conservative necessary condition: if this returns false at `(snap,
  // durable)`, no observed chain can release any entry (`GcWakeup.Goal`).
  [[nodiscard]] bool MayReleaseAt(uint64_t snap,
                                  uint64_t durable) const noexcept {
    if (min_second_ts <= snap) {
      return true;
    }
    if (has_unpinned_tail && min_tail_ts <= snap && min_tail_lsn <= durable) {
      return true;
    }
    return false;
  }
};

}  // namespace tinylamb::mvcc

#endif  // TINYLAMB_TRANSACTION_GC_WAKEUP_HPP
