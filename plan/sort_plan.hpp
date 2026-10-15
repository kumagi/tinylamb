/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_PLAN_SORT_PLAN_HPP
#define TINYLAMB_PLAN_SORT_PLAN_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "plan/plan.hpp"

namespace tinylamb {

struct SortKey {
  Expression expression;
  bool ascending{true};
  std::optional<bool> nulls_first;
  // Collation the key's comparison runs under, resolved from the key
  // expression (COLLATE(...)) by the implementation rules. kNone is the
  // engine default (binary byte order); a case-insensitive key folds its
  // text before the sort encoder writes the memcomparable form.
  Collation collation{Collation::kNone};
};

// Which physical shape orders the rows. Both shapes deliver the *same* logical
// order — identical keys, directions, NULL placement and collation — and both
// keep the sort executor's own spill selection as a last resort, so the choice
// between them is a memory decision only, never a different ORDER BY contract.
enum class SortStrategy : uint8_t { kBuffered, kExternalMerge };

// Working set a sort may hold before it hands rows to its spill path.
inline constexpr size_t kSortDefaultMemoryBudgetBytes = 64UL * 1024UL * 1024UL;
// Rows the merge keeps decoded per spilled run.
inline constexpr size_t kSortDefaultMergeWindowRows = 128;
// Row width the cost/spill model assumes when it converts a row count into
// bytes (cascades MemoryBudget::row_size_bytes).
inline constexpr size_t kSortEstimatedRowBytes = 64;

// Working-memory contract an implementation rule attaches to a sort shape and
// hands down to the operator. It is logical information: it says how much
// memory the shape may hold (and therefore how many rows of the estimated
// width that is), which is what makes the choice between the two shapes
// decidable from a row estimate alone.
struct SortMemoryPolicy {
  // Bytes the shape may hold before it has to hand rows to the spill path.
  // Matches the optimizer's MemoryBudget default so the rule's choice and the
  // operator's accounting speak the same unit. 0 means "no shape bound of its
  // own" (the operator then follows the process-wide budget).
  size_t memory_budget_bytes{kSortDefaultMemoryBudgetBytes};
  // Rows a merge window holds while spilled runs are merged.
  size_t merge_window_rows{kSortDefaultMergeWindowRows};
};

// Rows of the estimated width that fit in `memory`'s budget. The buffered
// shape is only offered at or below this capacity; above it the external merge
// shape takes over, which is the selection contract the rules and their tests
// pin down. An unbounded policy (0 bytes) never reaches the capacity, so it
// always keeps the input in memory and leaves the choice to the process budget.
[[nodiscard]] inline size_t SortRowCapacity(
    const SortMemoryPolicy& memory = SortMemoryPolicy{}) {
  if (memory.memory_budget_bytes == 0) {
    return std::numeric_limits<size_t>::max();
  }
  return std::max<size_t>(1,
                          memory.memory_budget_bytes / kSortEstimatedRowBytes);
}

// Which shape a row estimate gets. This is the single place the capacity
// comparison lives: the two implementation rules check their own side of it
// (they are exclusive halves of the choice) and MakeSortPlan acts on it.
[[nodiscard]] inline SortStrategy SortStrategyForRows(
    double estimated_rows,
    const SortMemoryPolicy& memory = SortMemoryPolicy{}) {
  // Written so a non-finite estimate (NaN from a degenerate cardinality
  // estimate) cannot push the choice to the spill-heavy shape by accident.
  return estimated_rows > static_cast<double>(SortRowCapacity(memory))
             ? SortStrategy::kExternalMerge
             : SortStrategy::kBuffered;
}

// Logical sort node. The base type carries the ORDER BY contract and reports
// it identically for every shape (IsOrderedBy / OrderCollation /
// EnforcesLimit / row counts), so a consumer cannot tell — and must not care —
// which shape produced the order. `StrategyName` names the shape for
// EXPLAIN/Dump only.
class SortPlan : public PlanBase {
 public:
  SortPlan(Plan child, std::vector<SortKey> keys)
      : child_(std::move(child)), keys_(std::move(keys)) {}
  SortPlan(Plan child, std::vector<SortKey> keys, SortMemoryPolicy memory)
      : child_(std::move(child)), keys_(std::move(keys)), memory_(memory) {}
  SortPlan(const SortPlan&) = delete;
  SortPlan(SortPlan&&) = delete;
  SortPlan& operator=(const SortPlan&) = delete;
  SortPlan& operator=(SortPlan&&) = delete;
  ~SortPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;

  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return child_->GetSchema();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return child_->EmitRowCount();
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  // Sorting reorders but never adds or removes rows, so a limit below a
  // Sort still shapes the final output exactly.
  [[nodiscard]] bool EnforcesLimit(size_t limit_count,
                                   size_t limit_offset) const override {
    return child_->EnforcesLimit(limit_count, limit_offset);
  }
  [[nodiscard]] bool EnforcesDistinct() const override {
    return child_->EnforcesDistinct();
  }

  [[nodiscard]] virtual SortStrategy Strategy() const {
    return SortStrategy::kBuffered;
  }
  // Shape name as it appears in Dump/ToString and in the memo's physical
  // operator. The base type keeps the historical "Sort".
  [[nodiscard]] virtual std::string StrategyName() const { return "Sort"; }
  [[nodiscard]] const SortMemoryPolicy& Memory() const { return memory_; }
  // Rows the buffered shape holds before it spills.
  [[nodiscard]] size_t RowCapacity() const { return SortRowCapacity(memory_); }

  [[nodiscard]] Collation OrderCollation() const override;
  [[nodiscard]] const std::vector<SortKey>& Keys() const { return keys_; }
  [[nodiscard]] const Plan& Child() const { return child_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;
  // "a ASC, b DESC" — the ORDER BY contract, shared by every shape.
  [[nodiscard]] std::string KeysToString() const;
  // " (buffered, budget=…, rows=…)" / " (spill runs, budget=…, window=…)";
  // empty for the base type so its rendering is unchanged.
  [[nodiscard]] virtual std::string MemorySuffix() const;

 private:
  Plan child_;
  std::vector<SortKey> keys_;
  SortMemoryPolicy memory_{};
};

// Sorts the whole input in memory. The shape the rules offer while the
// estimated input fits the memory budget; it still falls back to the
// executor's spill path if the estimate was wrong, because a wrong shape choice
// must never change the answer.
class BufferedSortPlan final : public SortPlan {
 public:
  BufferedSortPlan(Plan child, std::vector<SortKey> keys,
                   SortMemoryPolicy memory = SortMemoryPolicy{})
      : SortPlan(std::move(child), std::move(keys), memory) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] SortStrategy Strategy() const override {
    return SortStrategy::kBuffered;
  }
  [[nodiscard]] std::string StrategyName() const override {
    return "BufferedSort";
  }
  [[nodiscard]] std::string MemorySuffix() const override;
};

// Writes sorted runs and merges them back, so the working set stays bounded by
// the memory budget however large the input is. Chosen above the buffered row
// capacity; it reports the same ordering, collation and row counts as the
// buffered shape.
class ExternalMergeSortPlan final : public SortPlan {
 public:
  ExternalMergeSortPlan(Plan child, std::vector<SortKey> keys,
                        SortMemoryPolicy memory = SortMemoryPolicy{})
      : SortPlan(std::move(child), std::move(keys), memory) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] SortStrategy Strategy() const override {
    return SortStrategy::kExternalMerge;
  }
  [[nodiscard]] std::string StrategyName() const override {
    return "ExternalMergeSort";
  }
  [[nodiscard]] std::string MemorySuffix() const override;
  // Rows buffered before a sorted run is written out.
  [[nodiscard]] size_t SpillThresholdRows() const { return RowCapacity(); }
  [[nodiscard]] size_t MergeWindowRows() const {
    return Memory().merge_window_rows;
  }
};

// The sort shape an implementation rule picks for `estimated_rows` input
// rows: the buffered sort at or below the memory budget's row capacity, the
// external merge sort above it. The ORDER BY contract is identical either way,
// so this is the single place that encodes the choice.
[[nodiscard]] Plan MakeSortPlan(
    Plan child, std::vector<SortKey> keys, double estimated_rows,
    const SortMemoryPolicy& memory = SortMemoryPolicy{});

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_SORT_PLAN_HPP
