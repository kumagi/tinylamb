/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_AGGREGATION_PLAN_HPP
#define TINYLAMB_AGGREGATION_PLAN_HPP

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Which physical algorithm groups the rows. The three shapes deliver the same
// logical GROUP BY (the same keys, the same aggregates, the same NULL and
// duplicate-grouping semantics); they differ in the algorithm they run and in
// the properties they can promise, which is what `Strategy()` and the plan
// types below report.
enum class AggregationStrategy : uint8_t {
  kHash,    // one hash table over the group keys, no ordering required
  kSort,    // sorted runs over the group keys, then one pass over equal runs
  kStream,  // one pass, no hash table and no sort: valid only over an input
            // that already arrives in group-key order
};

// Working set a grouped shape may hold before it has to hand rows to its spill
// path.
inline constexpr size_t kAggregateDefaultMemoryBudgetBytes =
    64UL * 1024UL * 1024UL;
// Row width the cost/spill model assumes when it converts a row count into
// bytes (the aggregate counterpart of sort's kSortEstimatedRowBytes).
inline constexpr size_t kAggregateEstimatedRowBytes = 64;
// Partitions the hash shape writes its rows into before it re-groups them one
// partition at a time.
inline constexpr size_t kAggregateDefaultSpillPartitions = 32;
// Rows a merge window holds while spilled runs are merged back (the sort shape
// reuses the sort operator's window).
inline constexpr size_t kAggregateDefaultMergeWindowRows = 128;

// Memory contract an implementation rule attaches to a shape and hands down to
// the operator. It is logical information: it says how much memory the shape
// may hold (and therefore how many groups / rows of the estimated width that
// is), which is what makes the choice between the shapes decidable from a row
// estimate alone.
struct AggregateMemoryPolicy {
  // Bytes the shape may hold before it has to hand rows to its spill path.
  // Matches the optimizer's MemoryBudget default so the rule's choice and the
  // operator's accounting speak the same unit. 0 means "no shape bound of its
  // own" (the operator then follows the process-wide budget).
  size_t memory_budget_bytes{kAggregateDefaultMemoryBudgetBytes};
  // Partitions the hash shape splits its spilled rows across.
  size_t spill_partition_count{kAggregateDefaultSpillPartitions};
  // Rows a merge window holds while the sort shape merges spilled runs.
  size_t merge_window_rows{kAggregateDefaultMergeWindowRows};
};

// Groups of the estimated width that fit in `memory`'s budget. The hash shape
// partitions its input once the resident group table passes this capacity, the
// sort shape writes a sorted run at the same row count, and the stream shape
// never holds more than one group. An unbounded policy (0 bytes) never reaches
// the capacity, so the operator follows the process-wide budget instead.
[[nodiscard]] inline size_t AggregateGroupCapacity(
    const AggregateMemoryPolicy& memory = AggregateMemoryPolicy{}) {
  if (memory.memory_budget_bytes == 0) {
    return std::numeric_limits<size_t>::max();
  }
  return std::max<size_t>(
      1, memory.memory_budget_bytes / kAggregateEstimatedRowBytes);
}

// Partitions the hash shape may split spilled rows across. One partition means
// "buffer everything and group at the end", which is the degenerate but always
// correct shape of the same algorithm.
[[nodiscard]] inline size_t AggregateSpillPartitionCount(
    const AggregateMemoryPolicy& memory = AggregateMemoryPolicy{}) {
  return std::max<size_t>(1, memory.spill_partition_count);
}

// The GROUP BY payload every shape receives: the grouping keys, the aggregate
// target list, the HAVING filter, the estimated group count and the memory
// contract. One struct so the three shapes can never disagree on the payload.
struct AggregatePayload {
  std::vector<NamedExpression> group_keys{};
  std::vector<NamedExpression> aggregates{};
  // HAVING, evaluated against the finished group row (keys then aggregates).
  Expression having{};
  double estimated_groups{1.0};
  AggregateMemoryPolicy memory{};
};

// Logical aggregation node. The base type carries the whole GROUP BY contract
// (payload, output schema, row counts, parameter slots) and reports it
// identically for every shape, so a consumer cannot tell — and must not care —
// which algorithm produced a group. `StrategyName` names the shape for
// EXPLAIN/Dump and the shapes add the memory contract they own on top.
class AggregationPlan : public PlanBase {
 public:
  AggregationPlan(Plan child, std::vector<NamedExpression> aggregates,
                  AggregationStrategy strategy = AggregationStrategy::kHash);
  AggregationPlan(Plan child, AggregatePayload payload,
                  AggregationStrategy strategy);
  AggregationPlan(const AggregationPlan&) = delete;
  AggregationPlan(AggregationPlan&&) = delete;
  AggregationPlan& operator=(const AggregationPlan&) = delete;
  AggregationPlan& operator=(AggregationPlan&&) = delete;
  ~AggregationPlan() override = default;

  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;
  [[nodiscard]] const Table* ScanSource() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override;
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  // One row per group, and group keys are unique by construction, so the
  // output never carries a duplicate: a consumer must not add another DISTINCT.
  [[nodiscard]] bool EnforcesDistinct() const override { return true; }
  [[nodiscard]] AggregationStrategy Strategy() const { return strategy_; }
  // Shape name as it appears in Dump/ToString and in the memo's physical
  // operator. The base type keeps the historical per-strategy naming.
  [[nodiscard]] virtual std::string StrategyName() const;
  // The algorithm a shape runs, and the order it can promise, are what the
  // derived types override; a hash table promises none.
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override {
    static_cast<void>(expressions);
    static_cast<void>(ascending);
    return false;
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override {
    static_cast<void>(expressions);
    static_cast<void>(ascending);
    static_cast<void>(nulls_first);
    return false;
  }
  // The collation the delivered order was produced under. A shape that orders
  // by the group keys compares them left to right, so the delivered ordering
  // folds as soon as one key asks for it; a shape that promises no order keeps
  // the engine default.
  [[nodiscard]] Collation OrderCollation() const override;

  [[nodiscard]] const std::vector<NamedExpression>& GroupKeys() const {
    return group_keys_;
  }
  [[nodiscard]] const std::vector<NamedExpression>& Aggregates() const {
    return aggregates_;
  }
  [[nodiscard]] const Expression& Having() const { return having_; }
  [[nodiscard]] double EstimatedGroups() const { return estimated_groups_; }
  [[nodiscard]] const AggregateMemoryPolicy& Memory() const { return memory_; }
  [[nodiscard]] const Plan& Child() const { return child_; }
  // Groups the shape may hold before it spills.
  [[nodiscard]] size_t RowCapacity() const {
    return AggregateGroupCapacity(memory_);
  }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o, int indent) const override;
  // "k, name" — the GROUP BY key list, shared by every shape.
  [[nodiscard]] std::string GroupKeysToString() const;
  // " (hash table, budget=…B, groups=…)" / " (streaming, budget=0B)"; empty
  // for the base type so its rendering is unchanged.
  [[nodiscard]] virtual std::string MemorySuffix() const;

 protected:
  [[nodiscard]] Schema GenerateSchema() const;
  [[nodiscard]] TableStatistics GenerateOutputStats() const;

  Plan child_;
  std::vector<NamedExpression> group_keys_;
  std::vector<NamedExpression> aggregates_;
  Expression having_{};
  AggregationStrategy strategy_{AggregationStrategy::kHash};
  AggregateMemoryPolicy memory_{};
  // Declaration order matters: estimated_groups_/schema_/output_stats_ are
  // initialized from the payload in the constructor's initializer list.
  double estimated_groups_{1.0};
  Schema schema_;
  TableStatistics output_stats_;
};

// Groups every input row in one hash table. Unordered input is fine, and the
// shape promises no output order at all; the resident group table is what
// bounds its memory, so it partitions its input into spill files and re-groups
// one partition at a time when the table outgrows the memory budget.
class HashAggregatePlan final : public AggregationPlan {
 public:
  HashAggregatePlan(Plan child, AggregatePayload payload)
      : AggregationPlan(std::move(child), std::move(payload),
                        AggregationStrategy::kHash) {}
  HashAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kHash) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] std::string StrategyName() const override {
    return "HashAggregate";
  }
  [[nodiscard]] std::string MemorySuffix() const override;
  [[nodiscard]] size_t SpillPartitionCount() const {
    return AggregateSpillPartitionCount(Memory());
  }
};

// Sorts the input by the group keys and folds each run of equal keys in one
// sequential pass, so the only state it keeps is the current group (plus the
// sort's own runs). Chosen when the hash table would be too large, and it
// delivers its output in group-key order as a side effect of the sort.
class SortAggregatePlan final : public AggregationPlan {
 public:
  SortAggregatePlan(Plan child, AggregatePayload payload)
      : AggregationPlan(std::move(child), std::move(payload),
                        AggregationStrategy::kSort) {}
  SortAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kSort) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] std::string StrategyName() const override {
    return "SortAggregate";
  }
  [[nodiscard]] std::string MemorySuffix() const override;
  // The output is produced in group-key order: ascending, NULLs first, which is
  // the sort's own default for an ascending key.
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  [[nodiscard]] size_t SpillThresholdRows() const { return RowCapacity(); }
  [[nodiscard]] size_t MergeWindowRows() const {
    return Memory().merge_window_rows;
  }
};

// Folds one group at a time straight off the input: no hash table, no sort, no
// spill. That is only correct when the input already arrives in group-key
// order, which is the shape's contract — a rule may therefore offer it only
// over an ordered child, and the operator refuses an unordered input rather
// than emitting an order it never had.
class StreamAggregatePlan final : public AggregationPlan {
 public:
  StreamAggregatePlan(Plan child, AggregatePayload payload)
      : AggregationPlan(std::move(child), std::move(payload),
                        AggregationStrategy::kStream) {}
  StreamAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kStream) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] std::string StrategyName() const override {
    return "StreamAggregate";
  }
  [[nodiscard]] std::string MemorySuffix() const override;
  // One group is resident at a time, so the shape holds no group table: the
  // budget is the input's, not the aggregation's.
  [[nodiscard]] static size_t RowCapacity() { return 1; }
  // The output keeps the input's group-key order, which the input contract
  // fixes to ascending / NULLs first.
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  // True for a payload the streaming pass can evaluate row at a time without a
  // hash table (any payload, in fact: a scalar payload has exactly one group,
  // which makes the stream shape valid over any input order).
  [[nodiscard]] bool RequiresOrderedInput() const {
    return !group_keys_.empty();
  }
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATION_PLAN_HPP
