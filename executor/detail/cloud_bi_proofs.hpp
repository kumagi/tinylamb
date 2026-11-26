/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_CLOUD_BI_PROOFS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_CLOUD_BI_PROOFS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor/detail/analytical_operators_proofs.hpp"

namespace tinylamb::benchmark_rematch {

// ============================================================================
// CloudBI / DeleteFiles (Iceberg SQL positional & equality delete files)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/CloudBI/DeleteFiles.lean
// (sound: EqualityHorizon, PositionMask, OverlapCount, FooterCount,
//  ManifestHorizon, NullAwarePruning, FieldIdProbe, CompactOrdinal,
//  DeleteAwareLimit, SkippedOrdinalBase, BitmapEpoch)
// ============================================================================

struct IcebergPart {
  uint64_t spec_id{0};
  uint64_t part_id{0};
  constexpr bool operator==(const IcebergPart&) const = default;
};

struct IcebergRow {
  uint64_t file{0};
  uint64_t pos{0};
  IcebergPart part;
  uint64_t seq{0};
  std::optional<int64_t> key;
};

struct IcebergEqDelete {
  std::optional<IcebergPart> scope;  // std::nullopt = unpartitioned global
  uint64_t seq{0};
  std::optional<int64_t> key;
};

struct IcebergPosDelete {
  uint64_t file{0};
  uint64_t pos{0};
  IcebergPart part;
  uint64_t seq{0};
};

struct EqHorizonKey {
  std::optional<IcebergPart> scope;
  std::optional<int64_t> key;
  constexpr bool operator==(const EqHorizonKey&) const = default;
};

struct EqHorizonKeyHash {
  size_t operator()(const EqHorizonKey& k) const noexcept {
    size_t h = k.scope.has_value()
                   ? (std::hash<uint64_t>{}(k.scope->spec_id) ^
                      (std::hash<uint64_t>{}(k.scope->part_id) * 31ULL))
                   : 0x12345678ULL;
    const size_t kh = k.key.has_value() ? std::hash<int64_t>{}(*k.key)
                                        : 0x9e3779b97f4a7c15ULL;
    return h ^ (kh * 0x517cc1b727220a95ULL);
  }
};

using EqualityDeleteHorizon =
    std::unordered_map<EqHorizonKey, uint64_t, EqHorizonKeyHash>;

// EqualityHorizon.Goal + BitmapEpoch.Goal: collapse equality deletes into one
// maximum sequence number per (scope, key); row is deleted iff row.seq <
// max_seq.
[[nodiscard]] inline EqualityDeleteHorizon BuildEqualityDeleteHorizon(
    std::span<const IcebergEqDelete> deletes) {
  EqualityDeleteHorizon out;
  for (const auto& d : deletes) {
    const EqHorizonKey k{.scope = d.scope, .key = d.key};
    auto& cur = out[k];
    cur = std::max(cur, d.seq);
  }
  return out;
}

[[nodiscard]] inline bool IsDeletedByEqualityHorizon(
    const EqualityDeleteHorizon& horizon, const IcebergRow& row) {
  const EqHorizonKey local{.scope = row.part, .key = row.key};
  if (const auto it = horizon.find(local);
      it != horizon.end() && row.seq < it->second) {
    return true;
  }
  const EqHorizonKey global{.scope = std::nullopt, .key = row.key};
  if (const auto it = horizon.find(global);
      it != horizon.end() && row.seq < it->second) {
    return true;
  }
  return false;
}

// PositionMask.Goal + FooterCount.Goal: build deduplicated in-range position
// delete mask once per data file; metadata-only surviving count = n - |mask|.
[[nodiscard]] inline std::unordered_set<uint64_t> BuildFilePositionDeleteMask(
    uint64_t file_id, IcebergPart part, uint64_t file_seq, uint64_t row_count,
    std::span<const IcebergPosDelete> deletes) {
  std::unordered_set<uint64_t> mask;
  for (const auto& d : deletes) {
    if (d.file == file_id && d.part == part && file_seq <= d.seq &&
        d.pos < row_count) {
      mask.insert(d.pos);
    }
  }
  return mask;
}

[[nodiscard]] inline uint64_t MetadataOnlyPosDeleteCount(
    uint64_t row_count, uint64_t file_id, IcebergPart part, uint64_t file_seq,
    std::span<const IcebergPosDelete> deletes) {
  const auto mask =
      BuildFilePositionDeleteMask(file_id, part, file_seq, row_count, deletes);
  return row_count - mask.size();
}

// OverlapCount.Goal + CompactOrdinal.Goal + DeleteAwareLimit.Goal:
// count surviving rows by subtracting the union of pos/eq delete masks, and
// overfetch ordered scans by `k + dead_upper_bound` to guarantee `k` survivors.
[[nodiscard]] inline size_t SurvivingRowsAfterUnionDeletes(
    std::span<const IcebergRow> rows,
    std::span<const IcebergPosDelete> pos_deletes,
    const EqualityDeleteHorizon& eq_horizon) {
  size_t dead = 0;
  for (const auto& r : rows) {
    bool is_dead = IsDeletedByEqualityHorizon(eq_horizon, r);
    if (!is_dead) {
      for (const auto& p : pos_deletes) {
        if (r.file == p.file && r.pos == p.pos && r.part == p.part &&
            r.seq <= p.seq) {
          is_dead = true;
          break;
        }
      }
    }
    if (is_dead) {
      ++dead;
    }
  }
  return rows.size() - dead;
}

// NullAwarePruning.Goal: numeric bounds [lo, hi] can prune an equality-delete
// file only when non-NULL keys fall outside [lo, hi] AND either the data has no
// NULLs or the delete file has no NULL key.
[[nodiscard]] inline bool CanPruneEqDeleteByBounds(
    int64_t file_lo, int64_t file_hi, bool file_has_null,
    std::span<const IcebergEqDelete> eq_deletes) {
  for (const auto& d : eq_deletes) {
    if (!d.key.has_value()) {
      if (file_has_null) {
        return false;
      }
    } else if (*d.key >= file_lo && *d.key <= file_hi) {
      return false;
    }
  }
  return true;
}

// FieldIdProbe.Goal: resolve equality-delete field IDs into physical column
// offsets once per file schema.
[[nodiscard]] inline std::vector<std::optional<size_t>> ResolveFieldIdOffsets(
    std::span<const uint64_t> file_layout,
    std::span<const uint64_t> probe_fields) {
  std::unordered_map<uint64_t, size_t> pos;
  for (size_t i = 0; i < file_layout.size(); ++i) {
    pos.emplace(file_layout[i], i);
  }
  std::vector<std::optional<size_t>> offsets;
  offsets.reserve(probe_fields.size());
  for (const uint64_t fid : probe_fields) {
    const auto it = pos.find(fid);
    if (it != pos.end()) {
      offsets.push_back(it->second);
    } else {
      offsets.push_back(std::nullopt);
    }
  }
  return offsets;
}

// SkippedOrdinalBase.Goal: skipping whole row groups preserves absolute file
// ordinals by adding skipped group lengths to the base ordinal.
[[nodiscard]] inline std::vector<uint64_t> ComputeSurvivingGroupBaseOrdinals(
    std::span<const size_t> group_sizes, std::span<const uint8_t> keep_group) {
  std::vector<uint64_t> bases;
  uint64_t offset = 0;
  for (size_t i = 0; i < group_sizes.size() && i < keep_group.size(); ++i) {
    if (keep_group[i] != 0) {
      bases.push_back(offset);
    }
    offset += group_sizes[i];
  }
  return bases;
}

// ============================================================================
// CloudBI / Quotes (CostBench mixed dashboard/refresh/OHLC/spread)
// Lean proofs:
// lean/TinylambOpt/ResearchProofs/CloudBI/Quotes.lean
// (sound: StaleRollup, SparseRefresh, CloseTournament, CandleFringe,
//  SpreadMoments, FenceRead, ReclusterSwap, RetryRefresh, ReclusterBudget,
//  MaintenanceBreakEven)
// ============================================================================

struct QuoteTick {
  uint64_t batch{0};
  uint64_t ts{0};
  uint64_t tie{0};
  uint64_t sym{0};
  std::optional<int64_t> bid;
  std::optional<int64_t> ask;
};

struct CloseWinner {
  uint64_t ts{0};
  uint64_t tie{0};
  int64_t price{0};
  constexpr bool operator==(const CloseWinner&) const = default;
};

// StaleRollup.Goal: combine a stale dashboard rollup up to `watermark` with
// late-arriving committed batches in `(watermark, snapshot]`.
[[nodiscard]] inline NullableTotals MergeStaleRollupWithDelta(
    NullableTotals stale_rollup, std::span<const QuoteTick> rows, uint64_t sym,
    uint64_t lo_ts, uint64_t hi_ts, uint64_t watermark, uint64_t snapshot) {
  NullableTotals delta;
  for (const auto& r : rows) {
    if (r.sym == sym && lo_ts <= r.ts && r.ts < hi_ts && watermark < r.batch &&
        r.batch <= snapshot) {
      delta = AddTotals(delta, WeightedTotal(1, r.bid));
    }
  }
  return AddTotals(stale_rollup, delta);
}

// CloseTournament.Goal: associative tournament reduction for OHLC close price
// ordered by `(ts, tie)` over non-NULL bids.
[[nodiscard]] inline std::optional<CloseWinner> MergeCloseWinner(
    std::optional<CloseWinner> a, std::optional<CloseWinner> b) noexcept {
  if (!a.has_value()) {
    return b;
  }
  if (!b.has_value()) {
    return a;
  }
  if (b->ts > a->ts || (b->ts == a->ts && b->tie > a->tie)) {
    return b;
  }
  return a;
}

[[nodiscard]] inline std::optional<CloseWinner> ReduceCloseRun(
    std::span<const QuoteTick> run) noexcept {
  std::optional<CloseWinner> best;
  for (const auto& r : run) {
    if (!r.bid.has_value()) {
      continue;
    }
    best = MergeCloseWinner(
        best, CloseWinner{.ts = r.ts, .tie = r.tie, .price = *r.bid});
  }
  return best;
}

// SpreadMoments.Goal: compute exact spread count and sum `SUM(ask - bid)` over
// rows where both bid and ask are non-NULL without materializing differences.
struct SpreadPairTotals {
  uint64_t count{0};
  int64_t bid_sum{0};
  int64_t ask_sum{0};
  [[nodiscard]] constexpr int64_t SpreadSum() const noexcept {
    return ask_sum - bid_sum;
  }
};

[[nodiscard]] inline SpreadPairTotals ComputeSpreadMoments(
    std::span<const QuoteTick> rows) noexcept {
  SpreadPairTotals out;
  for (const auto& r : rows) {
    if (r.bid.has_value() && r.ask.has_value()) {
      ++out.count;
      out.bid_sum += *r.bid;
      out.ask_sum += *r.ask;
    }
  }
  return out;
}

// RetryRefresh.Goal: deduplicate retried batch deliveries by immutable batch ID
// before folding totals.
template <typename PayloadFn>
[[nodiscard]] NullableTotals FoldIdempotentRetryBatches(
    std::span<const uint64_t> delivered_batch_ids, PayloadFn&& payload_of) {
  std::unordered_set<uint64_t> seen;
  NullableTotals out;
  for (const uint64_t bid : delivered_batch_ids) {
    if (!seen.insert(bid).second) {
      continue;
    }
    for (const QuoteTick& t : payload_of(bid)) {
      out = AddTotals(out, WeightedTotal(1, t.bid));
    }
  }
  return out;
}

// MaintenanceBreakEven.Goal: choose maintained layout when query benefit
// `query * (time - s) >= m * fresh`.
[[nodiscard]] inline bool ShouldUseMaintainedLayout(
    uint64_t fresh, uint64_t query_count, uint64_t base_time,
    uint64_t maint_cost, uint64_t served_time) noexcept {
  if (served_time > base_time) {
    return false;
  }
  return query_count * (base_time - served_time) >= maint_cost * fresh;
}

}  // namespace tinylamb::benchmark_rematch

#endif  // TINYLAMB_EXECUTOR_DETAIL_CLOUD_BI_PROOFS_HPP
