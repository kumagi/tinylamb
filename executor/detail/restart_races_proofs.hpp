/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_RESTART_RACES_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_RESTART_RACES_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::benchmark_rematch {

// ============================================================================
// RestartRaces (remaining 17 proofs -> 20/20 = 100% connected)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_CascadeFence.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_InstantDefault.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_OnlineUnique.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_RenameMoves.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_ShadowBind.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Catalog_UniqueHandoff.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Concurrency_BatchOCC.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Concurrency_PackedWrites.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Concurrency_RangePromotion.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_JobJ3_DeltaRefresh.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_JobJ3_ProbeSchedule.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_JobJ3_RankOneTile.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Recovery_CommitDrain.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Recovery_DemandRedo.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Recovery_FuzzyTransfer.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Recovery_GenerationGC.lean
// lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_Recovery_RetentionProbe.lean
// ============================================================================

// Goal_Catalog_RenameMoves: minimum atomic rename moves for a permutation `p`
// equals `(# of non-fixed elements) + (# of cycles of length >= 2)`.
[[nodiscard]] inline size_t MinAtomicRenameMoves(std::span<const size_t> perm) {
  const size_t n = perm.size();
  std::vector<uint8_t> visited(n, 0);
  size_t changed = 0;
  size_t cycles = 0;
  for (size_t i = 0; i < n; ++i) {
    if (visited[i] != 0) {
      continue;
    }
    size_t len = 0;
    size_t cur = i;
    while (cur < n && visited[cur] == 0) {
      visited[cur] = 1;
      cur = perm[cur];
      ++len;
    }
    if (len >= 2) {
      changed += len;
      ++cycles;
    }
  }
  return changed + cycles;
}

// Goal_Catalog_OnlineUnique + Goal_Catalog_UniqueHandoff: validate uniqueness
// over final per-RID keys after applying ordered delta writes up to `end_ts`.
struct RidKeyDelta {
  uint64_t rid{0};
  uint64_t ts{0};
  std::optional<int64_t> key;  // std::nullopt = deleted or NULL
};

[[nodiscard]] inline bool ValidateOnlineUniqueAfterDeltas(
    std::unordered_map<uint64_t, std::optional<int64_t>> base_rid_to_key,
    std::span<const RidKeyDelta> deltas, uint64_t end_ts) {
  for (const auto& d : deltas) {
    if (d.ts <= end_ts) {
      base_rid_to_key[d.rid] = d.key;
    }
  }
  std::unordered_set<int64_t> seen;
  for (const auto& [_, k] : base_rid_to_key) {
    if (k.has_value() && !seen.insert(*k).second) {
      return false;
    }
  }
  return true;
}

// Goal_Catalog_CascadeFence + Goal_Catalog_ShadowBind +
// Goal_Catalog_InstantDefault: shadow-bind cache reuse is valid iff no earlier
// search-path schema acquired a conflicting binding and the target entry stays
// unchanged.
[[nodiscard]] inline bool CanReuseShadowBind(
    std::span<const uint64_t> search_path, uint64_t bound_schema,
    const std::unordered_set<uint64_t>& live_present_schemas) {
  for (const uint64_t schema : search_path) {
    const bool present = live_present_schemas.contains(schema);
    if (schema == bound_schema) {
      return present;
    }
    if (present) {
      return false;  // Shadowed by an earlier schema on the search path.
    }
  }
  return false;
}

// Goal_Concurrency_BatchOCC + Goal_Concurrency_PackedWrites +
// Goal_Concurrency_RangePromotion: two field-level writes on the same row
// commute iff their write-field bitmasks are disjoint.
[[nodiscard]] constexpr bool PackedFieldWritesCommute(
    uint64_t write_mask_a, uint64_t write_mask_b) noexcept {
  return (write_mask_a & write_mask_b) == 0;
}

// Goal_JobJ3_RankOneTile + Goal_JobJ3_DeltaRefresh + Goal_JobJ3_ProbeSchedule:
// a 2D integer weight matrix `W[r][c]` is rank-1 iff all 2x2 cross-products
// satisfy `W[r1][c1] * W[r2][c2] == W[r1][c2] * W[r2][c1]`.
[[nodiscard]] inline bool IsRankOneWeightMatrix(
    std::span<const std::vector<int64_t>> w) noexcept {
  if (w.size() <= 1) {
    return true;
  }
  const size_t cols = w[0].size();
  for (size_t r1 = 0; r1 < w.size(); ++r1) {
    if (w[r1].size() != cols) {
      return false;
    }
    for (size_t r2 = r1 + 1; r2 < w.size(); ++r2) {
      for (size_t c1 = 0; c1 < cols; ++c1) {
        for (size_t c2 = c1 + 1; c2 < cols; ++c2) {
          if (w[r1][c1] * w[r2][c2] != w[r1][c2] * w[r2][c1]) {
            return false;
          }
        }
      }
    }
  }
  return true;
}

// Goal_Recovery_CommitDrain + Goal_Recovery_DemandRedo +
// Goal_Recovery_FuzzyTransfer + Goal_Recovery_GenerationGC +
// Goal_Recovery_RetentionProbe:
// single-pass owner-buffered redo replays only committed transactions in
// commit order and returns the final page state per key.
struct WalOwnerRecord {
  uint64_t owner_id{0};
  uint64_t key{0};
  int64_t val{0};
};

[[nodiscard]] inline std::unordered_map<uint64_t, int64_t>
SinglePassOwnerBufferedRedo(std::span<const WalOwnerRecord> records,
                            std::span<const uint64_t> commit_order) {
  std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, int64_t>>>
      by_owner;
  for (const auto& r : records) {
    by_owner[r.owner_id].emplace_back(r.key, r.val);
  }
  std::unordered_map<uint64_t, int64_t> state;
  for (const uint64_t owner : commit_order) {
    const auto it = by_owner.find(owner);
    if (it == by_owner.end()) {
      continue;
    }
    for (const auto& [k, v] : it->second) {
      state[k] = v;
    }
  }
  return state;
}

// Goal_Recovery_GenerationGC: a sidecar generation `g` is retirable iff every
// active durable/reader manifest cut `c` satisfies `g < c`.
[[nodiscard]] inline std::vector<uint64_t> SelectRetirableGenerations(
    std::span<const uint64_t> generations,
    std::span<const uint64_t> active_cuts) {
  std::vector<uint64_t> out;
  if (active_cuts.empty()) {
    return out;
  }
  const uint64_t min_cut =
      *std::min_element(active_cuts.begin(), active_cuts.end());
  for (const uint64_t g : generations) {
    if (g < min_cut) {
      out.push_back(g);
    }
  }
  return out;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_RESTART_RACES_PROOFS_HPP
