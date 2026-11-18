/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_FOREIGN_KEY_HPP
#define TINYLAMB_FOREIGN_KEY_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/status_or.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {
class SpillStore;
class SpillSegmentWriter;
class Table;
class Transaction;
class Value;

// One enforced FOREIGN KEY edge, resolved to table handles by the query
// layer (which owns catalog access).  Plain data so DML executors stay free
// of statement-layer types.
struct ForeignKeyCheck {
  size_t child_offset{0};   // Offset of the FK column in the target row.
  Table* parent{nullptr};   // Resolved parent table (owned by the txn ctx).
  size_t parent_offset{0};  // Offset of the referenced column in parent rows.
};

// Reverse edge for NO ACTION checks on the referenced side: deleting (or
// rekeying) a parent row is rejected while a child row references its key.
struct ReferencedByCheck {
  Table* child{nullptr};    // Resolved child table (owned by the txn ctx).
  size_t child_offset{0};   // Offset of the FK column in child rows.
  size_t parent_offset{0};  // Offset of the referenced key in target rows.
};

// Bounded-memory multiset of non-null parent keys encoded as a type tag plus
// Value::AsString.  Keeps small parent tables in a ProcessMemoryCharge-tracked
// hash table and spills large tables through ExternalSorter + a sparse-indexed
// SpillStore segment (`ooca-v2.md` §5.3).
class ForeignKeyCounts {
 public:
  ForeignKeyCounts();
  ~ForeignKeyCounts();
  ForeignKeyCounts(const ForeignKeyCounts&) = delete;
  ForeignKeyCounts& operator=(const ForeignKeyCounts&) = delete;
  ForeignKeyCounts(ForeignKeyCounts&& other) noexcept;
  ForeignKeyCounts& operator=(ForeignKeyCounts&& other) noexcept;

  static void SetBudgetBytesForTest(size_t bytes) noexcept;
  [[nodiscard]] static size_t EffectiveBudgetBytes() noexcept;

  [[nodiscard]] Status IncrementKey(const Value& key);
  [[nodiscard]] Status DecrementKey(const Value& key);
  [[nodiscard]] StatusOr<bool> Contains(const Value& key) const;
  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] size_t InMemoryEntries() const noexcept {
    return counts_.size() + delta_counts_.size();
  }

 private:
  friend StatusOr<ForeignKeyCounts> BuildForeignKeyCounts(Table& parent,
                                                          Transaction& txn,
                                                          size_t parent_offset);
  struct LeafMeta {
    std::string first_key;
    uint64_t offset{0};
    uint32_t length{0};
  };

  [[nodiscard]] Status SpillInMemoryToSortedSegment();
  [[nodiscard]] Status RebuildWithDeltas();
  [[nodiscard]] Status ApplySpilledDelta(std::string encoded, int64_t delta);
  [[nodiscard]] Status BuildSortedSegment(
      const std::function<Status(
          const std::function<Status(std::string_view, int64_t)>&)>& producer);
  [[nodiscard]] StatusOr<int64_t> LookupSpilledCount(
      std::string_view key) const;
  void ReleaseSpillSegment() noexcept;

  size_t budget_bytes_{0};
  bool spilled_{false};
  std::unordered_map<std::string, int64_t> counts_;
  size_t in_mem_bytes_{0};
  std::unordered_map<std::string, int64_t> delta_counts_;
  size_t delta_bytes_{0};
  ProcessMemoryCharge mem_charge_{MemoryPoolKind::kQuery, 0, false};
  std::shared_ptr<SpillStore> store_;
  uint64_t segment_id_{0};
  bool two_level_{false};
  std::vector<LeafMeta> top_metas_;
};

// Bounded-memory buffer for DML RowPositions (DELETE / MERGE / FK ignore sets):
// stays in a plain std::vector when below budget and streams batches to
// SpillStore once exceeding the query FK budget or under AlwaysSpillEnabled().
class SpillableRowPositionList {
 public:
  SpillableRowPositionList();
  ~SpillableRowPositionList();
  SpillableRowPositionList(const SpillableRowPositionList&) = delete;
  SpillableRowPositionList& operator=(const SpillableRowPositionList&) = delete;
  SpillableRowPositionList(SpillableRowPositionList&&) noexcept;
  SpillableRowPositionList& operator=(SpillableRowPositionList&&) noexcept;

  [[nodiscard]] Status PushBack(const RowPosition& pos);
  [[nodiscard]] bool Empty() const noexcept { return count_ == 0; }
  [[nodiscard]] size_t Size() const noexcept { return count_; }
  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] Status ForEach(
      const std::function<Status(const RowPosition&)>& fn) const;
  [[nodiscard]] const std::vector<RowPosition>& InMemory() const noexcept {
    return in_memory_;
  }

 private:
  [[nodiscard]] Status TransitionToSpill();
  [[nodiscard]] Status FlushBatchToSpill(const RowPosition* data, size_t n);

  size_t budget_bytes_{0};
  size_t count_{0};
  bool spilled_{false};
  std::vector<RowPosition> in_memory_;
  ProcessMemoryCharge mem_charge_{MemoryPoolKind::kQuery, 0, false};
  std::shared_ptr<SpillStore> store_;
  std::unique_ptr<SpillSegmentWriter> writer_;
};

// Bounded-memory buffer for DML pending updates `(Row new_row, RowPosition
// pos)`.
class SpillablePendingUpdateList {
 public:
  SpillablePendingUpdateList();
  ~SpillablePendingUpdateList();
  SpillablePendingUpdateList(const SpillablePendingUpdateList&) = delete;
  SpillablePendingUpdateList& operator=(const SpillablePendingUpdateList&) =
      delete;
  SpillablePendingUpdateList(SpillablePendingUpdateList&&) noexcept;
  SpillablePendingUpdateList& operator=(SpillablePendingUpdateList&&) noexcept;

  [[nodiscard]] Status PushBack(Row row, RowPosition pos);
  [[nodiscard]] bool Empty() const noexcept { return count_ == 0; }
  [[nodiscard]] size_t Size() const noexcept { return count_; }
  [[nodiscard]] bool Spilled() const noexcept { return spilled_; }
  [[nodiscard]] Status ForEach(
      const std::function<Status(const Row&, const RowPosition&)>& fn) const;

 private:
  [[nodiscard]] Status TransitionToSpill();
  [[nodiscard]] Status WriteEntryToSpill(const Row& row,
                                         const RowPosition& pos);

  size_t budget_bytes_{0};
  size_t in_mem_bytes_{0};
  size_t count_{0};
  bool spilled_{false};
  std::vector<std::pair<Row, RowPosition>> in_memory_;
  ProcessMemoryCharge mem_charge_{MemoryPoolKind::kQuery, 0, false};
  std::shared_ptr<SpillStore> store_;
  std::unique_ptr<SpillSegmentWriter> writer_;
  std::string scratch_;
};

// Encodes a non-null key value for ForeignKeyCounts.  NULL keys never match
// under SQL semantics and must not be encoded.
std::string ForeignKeyString(const Value& value);

// Seeds the parent-key multiset from a statement-snapshot scan of the
// parent table (projection-only read of the referenced column).
StatusOr<ForeignKeyCounts> BuildForeignKeyCounts(Table& parent,
                                                 Transaction& txn,
                                                 size_t parent_offset);

// True when a non-null child key has no match in the parent multiset.
StatusOr<bool> IsForeignKeyViolation(const ForeignKeyCounts& counts,
                                     const Value& key);

// Single-pass / ExternalSorter-backed batch check eliminating O(|pending| *
// |child|) nested scans: streams candidate parent keys and child-ignored
// RowPositions, returning the ForeignKeyString/display value of the first
// referenced parent key or std::nullopt if none are referenced.
StatusOr<std::optional<std::string>> FindReferencedParentKey(
    Table& child, Transaction& txn, size_t child_offset,
    const std::function<
        Status(const std::function<Status(const Value& parent_key)>& emit)>&
        for_each_candidate_key,
    const std::function<
        Status(const std::function<Status(const RowPosition& pos)>& emit)>&
        for_each_ignore_pos);

// True when a non-null parent key is still referenced by a row of the child
// table.  `ignore` holds positions already removed by the running statement
// (NO ACTION is statement-end semantics: rows the statement itself deletes
// do not keep the parent alive).  A failed child scan is an error, never a
// "not referenced" verdict.
StatusOr<bool> IsParentKeyReferenced(Table& child, Transaction& txn,
                                     size_t child_offset,
                                     const Value& parent_key,
                                     const std::vector<RowPosition>& ignore);

}  // namespace tinylamb

#endif
