/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TRANSACTION_MVCC_DECISIONS_HPP
#define TINYLAMB_TRANSACTION_MVCC_DECISIONS_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tinylamb::mvcc {

// ============================================================================
// 1. EngineDecisions.MVCC.PublicationHoles
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_PublicationHoles.lean
//     (PublicationHoles.Goal)
// Given newest-first sealed write sets with unique commit stamps, a read-only
// query at `snap` over `keys` may safely cross unpublished timestamps iff for
// every key `k` and every unpublished tx `t` with `t.stamp <= snap` touching
// `k`, a newer published tx with `stamp <= snap` already masks `t.stamp`.
// ============================================================================
struct SealedWriteTx {
  uint64_t stamp{0};
  std::vector<uint64_t> keys;
  bool published{false};
};

[[nodiscard]] inline bool IsReadFootprintCertified(
    std::span<const SealedWriteTx> txs_newest_first, uint64_t snap,
    std::span<const uint64_t> keys) {
  for (const uint64_t k : keys) {
    std::optional<uint64_t> newest_published;
    for (const SealedWriteTx& tx : txs_newest_first) {
      if (tx.stamp > snap) {
        continue;
      }
      if (std::ranges::find(tx.keys, k) == tx.keys.end()) {
        continue;
      }
      if (tx.published) {
        if (!newest_published.has_value()) {
          newest_published = tx.stamp;
        }
      } else if (!newest_published.has_value() ||
                 tx.stamp >= *newest_published) {
        return false;
      }
    }
  }
  return true;
}

// ============================================================================
// 2. EngineDecisions.MVCC.UndoPeak
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_UndoPeak.lean
//     (UndoPeak.Goal)
// The least sufficient free space on a page to guarantee all interleaved
// rollback schedules succeed is the sum of each owner's maximum prefix demand.
// ============================================================================
[[nodiscard]] inline uint64_t OwnerUndoPeakBytes(
    std::span<const int64_t> steps) noexcept {
  int64_t cur = 0;
  int64_t peak = 0;
  for (const int64_t delta : steps) {
    cur += delta;
    peak = std::max(peak, cur);
  }
  return peak > 0 ? static_cast<uint64_t>(peak) : 0;
}

[[nodiscard]] inline uint64_t TotalInterleavedUndoReserve(
    std::span<const std::vector<int64_t>> owner_steps) noexcept {
  uint64_t total = 0;
  for (const auto& steps : owner_steps) {
    total += OwnerUndoPeakBytes(steps);
  }
  return total;
}

// ============================================================================
// 3. EngineDecisions.MVCC.ChainSummary
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_ChainSummary.lean
//     (ChainSummary.Goal)
// Intern runs of identical payloads and identical unflushed WAL dependencies
// into separate timestamped arrays while preserving head_ts for FCW conflicts.
// ============================================================================
struct ChainVersionEntry {
  uint64_t ts{0};
  std::optional<int64_t> payload;
  uint64_t wal_end{0};
};

struct CompactedVersionChain {
  std::vector<ChainVersionEntry> payload_runs;
  std::vector<ChainVersionEntry> dep_runs;
  std::optional<uint64_t> head_ts;

  [[nodiscard]] bool HasWriteConflict(uint64_t snap) const noexcept {
    return head_ts.has_value() && snap < *head_ts;
  }

  [[nodiscard]] std::optional<int64_t> PayloadAt(uint64_t snap) const noexcept {
    std::optional<int64_t> out;
    for (const auto& v : payload_runs) {
      if (v.ts <= snap) {
        out = v.payload;
      } else {
        break;
      }
    }
    return out;
  }

  [[nodiscard]] uint64_t DependencyAt(uint64_t snap,
                                      uint64_t durable) const noexcept {
    uint64_t dep = 0;
    for (const auto& v : dep_runs) {
      if (v.ts <= snap) {
        dep = (v.wal_end <= durable) ? 0 : v.wal_end;
      } else {
        break;
      }
    }
    return dep;
  }
};

[[nodiscard]] inline CompactedVersionChain CompactVersionChain(
    std::span<const ChainVersionEntry> oldest_first, uint64_t durable) {
  CompactedVersionChain out;
  if (oldest_first.empty()) {
    return out;
  }
  out.head_ts = oldest_first.back().ts;
  out.payload_runs.push_back(oldest_first[0]);
  out.dep_runs.push_back(oldest_first[0]);
  auto eff_dep = [durable](const ChainVersionEntry& v) noexcept -> uint64_t {
    return v.wal_end <= durable ? 0 : v.wal_end;
  };
  for (size_t i = 1; i < oldest_first.size(); ++i) {
    if (oldest_first[i].payload != oldest_first[i - 1].payload) {
      out.payload_runs.push_back(oldest_first[i]);
    }
    if (eff_dep(oldest_first[i]) != eff_dep(oldest_first[i - 1])) {
      out.dep_runs.push_back(oldest_first[i]);
    }
  }
  return out;
}

// ============================================================================
// 4. EngineDecisions.MVCC.HistoricalCover
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_HistoricalCover.lean
//     (HistoricalCover.Goal)
// Reconstruct a historical covering scan from the current covering index hits
// plus the first before-image per dirty RID since the target snapshot.
// ============================================================================
struct CoveringImage {
  uint64_t key{0};
  int64_t val{0};
};

struct CoveringHit {
  uint64_t rid{0};
  uint64_t key{0};
  int64_t val{0};
  bool operator==(const CoveringHit&) const noexcept = default;
};

struct CoveringChange {
  uint64_t rid{0};
  std::optional<CoveringImage> before;
  std::optional<CoveringImage> after;
};

template <typename KeyPred>
[[nodiscard]] std::vector<CoveringHit> RepairHistoricalCoveringScan(
    KeyPred&& pred, std::span<const CoveringHit> current_hits,
    std::span<const CoveringChange> oldest_first_changes) {
  std::unordered_map<uint64_t, std::optional<CoveringImage>> first_before;
  std::vector<uint64_t> dirty_order;
  for (const CoveringChange& ch : oldest_first_changes) {
    if (first_before.emplace(ch.rid, ch.before).second) {
      dirty_order.push_back(ch.rid);
    }
  }
  std::vector<CoveringHit> out;
  out.reserve(current_hits.size() + dirty_order.size());
  for (const CoveringHit& hit : current_hits) {
    if (!first_before.contains(hit.rid)) {
      out.push_back(hit);
    }
  }
  for (const uint64_t rid : dirty_order) {
    const auto& img = first_before[rid];
    if (img.has_value() && pred(img->key)) {
      out.push_back({.rid = rid, .key = img->key, .val = img->val});
    }
  }
  return out;
}

// ============================================================================
// 5. EngineDecisions.MVCC.OverlayCache
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_OverlayCache.lean
//     (OverlayCache.Goal)
// Compact a sequence of staged edits (oldest-first) by retaining only the last
// edit per key (preserving explicit tombstones `std::nullopt`) to patch a
// transaction-private lookup cache over a fixed snapshot.
// ============================================================================
struct OverlayEdit {
  uint64_t key{0};
  std::optional<int64_t> value;  // std::nullopt = staged tombstone
};

[[nodiscard]] inline std::unordered_map<uint64_t, std::optional<int64_t>>
CompactOverlayDelta(std::span<const OverlayEdit> oldest_first_delta) {
  std::unordered_map<uint64_t, std::optional<int64_t>> compact;
  for (const OverlayEdit& e : oldest_first_delta) {
    compact[e.key] = e.value;
  }
  return compact;
}

// ============================================================================
// 6. EngineDecisions.MVCC.GuardedDelete
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_GuardedDelete.lean
//     (GuardedDelete.Goal)
// Exact commit-time validation of a lower-quota invariant under concurrent
// deletes and inserts: prevents write skew without whole-table locking.
// ============================================================================
[[nodiscard]] inline bool IsQuotaGuardedDeleteValid(
    std::span<const uint64_t> snapshot_ids,
    std::span<const uint64_t> own_deletes,
    std::span<const uint64_t> concurrent_deletes,
    std::span<const uint64_t> concurrent_inserts, size_t quota) {
  std::unordered_set<uint64_t> snap(snapshot_ids.begin(), snapshot_ids.end());
  std::unordered_set<uint64_t> own(own_deletes.begin(), own_deletes.end());
  size_t own_in_snap = 0;
  for (const uint64_t id : own) {
    if (snap.contains(id)) {
      ++own_in_snap;
    }
  }
  const size_t base_count = snap.size() - own_in_snap;
  std::unordered_set<uint64_t> conc_del(concurrent_deletes.begin(),
                                        concurrent_deletes.end());
  size_t lost = 0;
  for (const uint64_t id : conc_del) {
    if (snap.contains(id) && !own.contains(id)) {
      ++lost;
    }
  }
  size_t gained = 0;
  for (const uint64_t id : concurrent_inserts) {
    if (!own.contains(id)) {
      ++gained;
    }
  }
  return quota + lost <= base_count + gained;
}

// ============================================================================
// 7. EngineDecisions.MVCC.ExistsDurability
// Lean proof:
//   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_MVCC_ExistsDurability.lean
//     (ExistsDurability.Goal)
// The minimum WAL durability fence that makes a read-only EXISTS reply
// reproducible across all later WAL prefixes is the WAL end of the last
// committed batch that flipped the EXISTS truth value.
// ============================================================================
struct ExistsEditBatch {
  uint64_t wal_end{0};
  std::vector<std::pair<uint64_t, bool>> edits;  // (key, qualifies)
};

[[nodiscard]] inline uint64_t ExistsDurabilityFence(
    std::span<const uint64_t> initial_live,
    std::span<const ExistsEditBatch> wal_ordered_batches) {
  std::unordered_set<uint64_t> live(initial_live.begin(), initial_live.end());
  uint64_t fence = 0;
  for (const ExistsEditBatch& b : wal_ordered_batches) {
    const bool was_empty = live.empty();
    for (const auto& [k, qualifies] : b.edits) {
      if (qualifies) {
        live.insert(k);
      } else {
        live.erase(k);
      }
    }
    if (live.empty() != was_empty) {
      fence = b.wal_end;
    }
  }
  return fence;
}

}  // namespace tinylamb::mvcc

#endif  // TINYLAMB_TRANSACTION_MVCC_DECISIONS_HPP
