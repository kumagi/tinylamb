/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_OPTIMIZER_DECISIONS_HPP
#define TINYLAMB_PLAN_OPTIMIZER_DECISIONS_HPP

#include <algorithm>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace tinylamb::optimizer_decisions {

// ============================================================================
// 1. EngineDecisions.Optimizer.SharedBuild
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_SharedBuild.lean
//     (SharedBuild.Goal)
// Context-independent pruning between two memo alternatives with reusable
// builds: `a` dominates `b` across EVERY subset of already-available builds
// iff `a.private_cost + extra(b.builds, a.builds) <= b.private_cost`.
// ============================================================================
struct SharedBuildAlt {
  uint64_t private_cost{0};
  std::vector<uint64_t> builds;
};

template <typename PriceFn>
[[nodiscard]] uint64_t ExtraBuildCost(std::span<const uint64_t> available,
                                      std::span<const uint64_t> required,
                                      PriceFn&& price) {
  std::unordered_set<uint64_t> avail(available.begin(), available.end());
  std::unordered_set<uint64_t> seen;
  uint64_t total = 0;
  for (uint64_t id : required) {
    if (!avail.contains(id) && seen.insert(id).second) {
      total += static_cast<uint64_t>(price(id));
    }
  }
  return total;
}

template <typename PriceFn>
[[nodiscard]] bool SharedBuildDominates(const SharedBuildAlt& a,
                                        const SharedBuildAlt& b,
                                        PriceFn&& price) {
  const uint64_t worst_case_a =
      a.private_cost + ExtraBuildCost(b.builds, a.builds, price);
  return worst_case_a <= b.private_cost;
}

// ============================================================================
// 2. EngineDecisions.Optimizer.SearchFence
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_SearchFence.lean
//     (SearchFence.Goal)
// An interrupted group search retains finished costs (`done`) and lower bounds
// for unopened branches (`bounds`). Its negative-result certificate is
// `min(done ++ bounds)`, which rules out improving `cutoff` iff
// `!cert.has_value() || cutoff <= *cert`.
// ============================================================================
[[nodiscard]] inline std::optional<uint64_t> SearchFenceCertificate(
    std::span<const uint64_t> done_costs,
    std::span<const uint64_t> open_bounds) noexcept {
  std::optional<uint64_t> best;
  for (uint64_t v : done_costs) {
    if (!best.has_value() || v < *best) {
      best = v;
    }
  }
  for (uint64_t v : open_bounds) {
    if (!best.has_value() || v < *best) {
      best = v;
    }
  }
  return best;
}

[[nodiscard]] inline bool SearchFenceBlocksCutoff(
    std::optional<uint64_t> certificate, uint64_t cutoff) noexcept {
  return !certificate.has_value() || cutoff <= *certificate;
}

// ============================================================================
// 3. EngineDecisions.Optimizer.ProjectExcept
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_ProjectExcept.lean
//     (ProjectExcept.Goal)
// Exact local condition for pushing a non-injective projection below EXCEPT
// ALL: all multiplicity pairs `(left_mult, right_mult)` in the same projected
// fiber must have consistent sign (`all l <= r` OR `all r <= l`).
// ============================================================================
struct FiberMultiplicity {
  uint64_t left{0};
  uint64_t right{0};
};

[[nodiscard]] inline bool CanPushProjectionBelowExceptAll(
    std::span<const FiberMultiplicity> fiber) noexcept {
  bool all_le = true;
  bool all_ge = true;
  for (const FiberMultiplicity& p : fiber) {
    if (p.left > p.right) {
      all_le = false;
    }
    if (p.right > p.left) {
      all_ge = false;
    }
  }
  return all_le || all_ge;
}

// ============================================================================
// 4. EngineDecisions.Optimizer.JoinPrefix
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_JoinPrefix.lean
//     (JoinPrefix.Goal)
// Exact prefix length of ordered left rows needed to satisfy a downstream
// `OFFSET off LIMIT limit` across a one-to-many join given per-row lower bounds
// on post-residual match fanout (even when some rows have lower bound 0).
// ============================================================================
[[nodiscard]] inline size_t ComputeJoinPrefixDemand(
    std::span<const size_t> fanout_lower_bounds, size_t offset,
    size_t limit) noexcept {
  size_t remaining = offset + limit;
  size_t taken = 0;
  for (const size_t lb : fanout_lower_bounds) {
    if (remaining == 0) {
      break;
    }
    ++taken;
    remaining = (lb >= remaining) ? 0 : (remaining - lb);
  }
  return taken;
}

// ============================================================================
// 5. EngineDecisions.Optimizer.ProjectedOrder
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_ProjectedOrder.lean
//     (ProjectedOrder.Goal)
// Elide a suffix re-sort after a monotone non-injective projection `proj` on
// ordered runs `(leading_key, sorted_suffix_keys)` iff every adjacent pair of
// nonempty runs mapped into the same projected bucket has compatible endpoints
// (`prev.back() <= next.front()`).
// ============================================================================
struct OrderedKeyRun {
  int64_t leading_key{0};
  std::vector<int64_t> suffix_keys;
};

template <typename MonotoneProj>
[[nodiscard]] bool CanElideSuffixResortAfterProjection(
    std::span<const OrderedKeyRun> runs, MonotoneProj&& proj) {
  const OrderedKeyRun* prev = nullptr;
  for (const OrderedKeyRun& r : runs) {
    if (r.suffix_keys.empty()) {
      continue;
    }
    if (prev != nullptr) {
      if (proj(prev->leading_key) == proj(r.leading_key) &&
          prev->suffix_keys.back() > r.suffix_keys.front()) {
        return false;
      }
    }
    prev = &r;
  }
  return true;
}

// ============================================================================
// 6. EngineDecisions.Optimizer.JoinRevalidation
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Opt_JoinRevalidation.lean
//     (JoinRevalidation.Goal)
// Revalidate a prepared plan's unprojected INNER JOIN elimination certificate
// by checking only new active fact keys and keys whose dimension multiplicity
// changed since the previous certified snapshot.
// ============================================================================
[[nodiscard]] inline bool RevalidateOneMatchJoinElimination(
    std::span<const uint64_t> old_fact_keys,
    std::span<const uint64_t> new_fact_keys,
    std::span<const uint64_t> old_dim_keys,
    std::span<const uint64_t> new_dim_keys) {
  std::unordered_set<uint64_t> old_fact(old_fact_keys.begin(),
                                        old_fact_keys.end());
  std::unordered_map<uint64_t, size_t> old_dim_cnt;
  for (const uint64_t k : old_dim_keys) {
    ++old_dim_cnt[k];
  }
  std::unordered_map<uint64_t, size_t> new_dim_cnt;
  for (const uint64_t k : new_dim_keys) {
    ++new_dim_cnt[k];
  }
  std::unordered_set<uint64_t> checked;
  for (const uint64_t k : new_fact_keys) {
    if (!checked.insert(k).second) {
      continue;
    }
    const size_t old_c = old_dim_cnt.contains(k) ? old_dim_cnt[k] : 0;
    const size_t new_c = new_dim_cnt.contains(k) ? new_dim_cnt[k] : 0;
    const bool dirty = !old_fact.contains(k) || old_c != new_c;
    if (dirty && new_c != 1) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb::optimizer_decisions

#endif  // TINYLAMB_PLAN_OPTIMIZER_DECISIONS_HPP
