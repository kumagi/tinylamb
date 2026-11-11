/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GROUPED_AGGREGATE_HPP
#define TINYLAMB_GROUPED_AGGREGATE_HPP

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/spill_file.hpp"
#include "expression/named_expression.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Physical defaults for a directly constructed operator. The factory always
// hands the plan's AggregateMemoryPolicy down, so these only apply outside the
// plan path; the shape contract test pins the two sets together so they cannot
// drift.
inline constexpr size_t kGroupedAggregateDefaultMemoryBudgetBytes =
    64UL * 1024UL * 1024UL;
inline constexpr size_t kGroupedAggregateDefaultSpillPartitions = 32;
inline constexpr size_t kGroupedAggregateDefaultMergeWindowRows = 128;
inline constexpr size_t kGroupedAggregateEstimatedRowBytes = 64;

// Physical knobs the plan hands down (the operator-side form of the plan's
// AggregateMemoryPolicy). They never change the answer: they only decide where
// the rows live while the shape groups them.
struct GroupedAggregateOptions {
  // Bytes the shape may hold before it hands rows to its spill path. 0 = the
  // process-wide query memory budget decides, the historical behavior.
  size_t memory_budget_bytes{kGroupedAggregateDefaultMemoryBudgetBytes};
  // Partitions the hash shape splits spilled rows across.
  size_t spill_partition_count{kGroupedAggregateDefaultSpillPartitions};
  // Rows buffered before a sorted run is written (sort shape). 0 = the memory
  // budget alone decides.
  size_t spill_threshold_rows{0};
  // Rows a merge window holds while spilled runs are merged (sort shape).
  size_t merge_window_rows{kGroupedAggregateDefaultMergeWindowRows};
  // Workers folding the input in parallel (hash shape). >1 requires a payload
  // every worker can read positionally; the operator refuses to guess and
  // keeps the serial build otherwise.
  size_t worker_count{1};
  // The stream shape's contract: refuse an input that does not already arrive
  // in group-key order instead of emitting an order it never had.
  bool require_ordered_input{false};
};

// The group index every grouped shape shares: a hash map from the canonical
// group key to a slot in a flat key vector, with one accumulator state per
// (group, aggregate). Keys are canonicalized by the caller (NULL is one group,
// NaN one peer, -0 folds, a case-insensitive key folds), so all three shapes
// agree on what "the same group" means. The aggregate list is referenced, not
// copied: the owner fills it before the first row and never resizes it again.
class AggregateGroupTable {
 public:
  AggregateGroupTable(const Schema* input_schema,
                      const std::vector<const AggregateExpression*>* aggregates)
      : input_schema_(input_schema), aggregates_(aggregates) {}
  // The table owns its accumulator states, which are neither copyable nor
  // safely movable (each holds a distinct set and buffered rows), so a table
  // stays where it was constructed.
  AggregateGroupTable(const AggregateGroupTable&) = delete;
  AggregateGroupTable& operator=(const AggregateGroupTable&) = delete;
  AggregateGroupTable(AggregateGroupTable&&) = delete;
  AggregateGroupTable& operator=(AggregateGroupTable&&) = delete;
  ~AggregateGroupTable() = default;

  // Slot of a canonical key, creating the group when the key is new. The
  // vector overload probes without materializing a Row per input row.
  [[nodiscard]] StatusOr<size_t> FindOrCreate(
      const std::vector<Value>& canonical_key);
  [[nodiscard]] StatusOr<size_t> FindOrCreate(Row key);
  // Folds one input row into an existing group.
  [[nodiscard]] Status Accumulate(size_t group, const Row& row);
  // [group keys..., aggregate results...] for one group.
  [[nodiscard]] StatusOr<Row> Finish(size_t group) const;
  // Folds `other`'s groups into this one; a group seen by both merges its
  // accumulator state, a group seen by one is adopted. Deterministic in
  // `other`'s group order, so the merge order fixes the output group order.
  [[nodiscard]] Status Merge(const AggregateGroupTable& other);

  [[nodiscard]] size_t size() const { return keys_.size(); }
  [[nodiscard]] bool empty() const { return keys_.empty(); }
  [[nodiscard]] const Row& Key(size_t group) const { return keys_[group]; }
  [[nodiscard]] size_t AggregateCount() const { return aggregates_->size(); }
  // The accumulator state of one (group, aggregate) pair; the positional fast
  // path feeds it directly.
  [[nodiscard]] relational_detail::AggregateAccumulator& StateFor(
      size_t group, size_t index) {
    return states_[(group * aggregates_->size()) + index];
  }
  void Clear();

 private:
  void AddGroup(Row key);

  const Schema* input_schema_;
  const std::vector<const AggregateExpression*>* aggregates_;
  relational_detail::GroupKeyMap index_{};
  std::vector<Row> keys_;
  std::deque<relational_detail::AggregateAccumulator> states_{};
};

// Shared output cursor: every shape materializes its group rows first and then
// hands them out, so `Next` and `NextBatch` cannot disagree between the three
// shapes and a failure never yields a partial result.
class GroupedAggregateBase : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;

  [[nodiscard]] const Schema& OutputSchema() const { return output_schema_; }
  [[nodiscard]] const GroupedAggregateOptions& Options() const {
    return options_;
  }
  // Groups the shape produced (after HAVING).
  [[nodiscard]] size_t GroupCount() const { return output_rows_.size(); }

 protected:
  GroupedAggregateBase(Executor child, Schema input_schema,
                       std::vector<NamedExpression> group_keys,
                       std::vector<NamedExpression> aggregates,
                       Expression having, GroupedAggregateOptions options)
      : child_(std::move(child)),
        input_schema_(std::move(input_schema)),
        group_keys_(std::move(group_keys)),
        aggregates_(std::move(aggregates)),
        having_(std::move(having)),
        options_(options) {}

  // Builds output_rows_: group keys then aggregate results, HAVING applied.
  // Called at most once; a failure is latched and no row is emitted.
  virtual Status Build() = 0;

  [[nodiscard]] Status Materialize();
  // Whether a finished group survives HAVING, or nullopt-style false. The
  // filter is a predicate over the group's output row, the same scope a
  // Selection above this node sees, so it may name a group key or an aggregate
  // result column. Three-valued: only TRUE keeps the group.
  [[nodiscard]] StatusOr<bool> KeepsGroup(const Row& group_row) const;

  Executor child_;
  Schema input_schema_;
  Schema output_schema_;
  std::vector<NamedExpression> group_keys_;
  std::vector<NamedExpression> aggregates_;
  Expression having_{};
  GroupedAggregateOptions options_{};
  std::vector<Row> output_rows_;
  size_t cursor_{0};
  size_t input_rows_{0};
  bool materialized_{false};
};

// One hash table over the group keys. Any input order is fine and the shape
// promises no output order. The resident group table is what bounds its
// memory: when it outgrows the memory budget, the rows read so far are split
// across spill partitions by key hash and re-grouped one partition at a time,
// which keeps every group inside exactly one partition.
class HashAggregateExecutor final : public GroupedAggregateBase {
 public:
  HashAggregateExecutor(
      Executor child, Schema input_schema,
      std::vector<NamedExpression> group_keys,
      std::vector<NamedExpression> aggregates, Expression having = nullptr,
      GroupedAggregateOptions options = GroupedAggregateOptions{});
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] bool Spilled() const { return spilled_; }
  [[nodiscard]] size_t SpillPartitionCount() const { return spill_partitions_; }
  [[nodiscard]] size_t WorkerCount() const { return options_.worker_count; }
  // True when the payload allowed the worker-parallel build (plain column
  // keys and arguments, no row filter, no group context).
  [[nodiscard]] bool ParallelCapable() const { return parallel_capable_; }
  // Groups the build holds resident at most. This is the bound the serial build
  // honors -- the group table partitions its input once it reaches it. The
  // worker build is bounded by the distinct key count instead (it unions one
  // private table per worker), which is why a worker count and the partition
  // spill are two different answers to the same budget rather than two stages
  // of one pipeline.
  [[nodiscard]] size_t GroupCapacity() const { return group_capacity_; }
  // The most groups one of this shape's group tables ever held at once. This
  // shape's state IS the group table, so the peak is what its memory is spent
  // on: a resident build peaks at the distinct key count, and a partitioned
  // build peaks at the largest single partition's groups -- which is what the
  // partition count in the memory policy is there to keep small. The sort runs
  // keep exactly one group instead.
  [[nodiscard]] size_t PeakResidentGroups() const { return peak_groups_; }

 protected:
  Status Build() override;

 private:
  [[nodiscard]] Status BuildSerial();
  [[nodiscard]] Status BuildParallel();
  // Starts partitioning: writes `rows` into the partition their key hashes to.
  [[nodiscard]] Status StartPartitioning(const std::vector<Row>& rows);
  [[nodiscard]] Status AppendToPartition(const Row& row);
  [[nodiscard]] Status AccumulateRow(AggregateGroupTable* table,
                                     const Row& row);
  [[nodiscard]] Status EmitTable(AggregateGroupTable* table);
  // Records how many groups the table holds right now, so the resident peak is
  // observable from outside and the group table's own state can be pinned.
  void NoteResidentGroups(const AggregateGroupTable& table);
  // Positional fast path: plain column keys and a plain column / COUNT(*)
  // argument, which is what lets a worker fold a batch without evaluating an
  // expression (the evaluator is not thread safe).
  [[nodiscard]] Status AccumulatePositional(AggregateGroupTable* table,
                                            const DataChunk& chunk);

  std::vector<SortKeyOrder> key_orders_;
  std::vector<const AggregateExpression*> aggregate_nodes_;
  std::vector<std::optional<slot_t>> key_offsets_;
  std::vector<std::optional<slot_t>> aggregate_offsets_;
  std::vector<bool> aggregate_count_star_;
  AggregateGroupTable table_{&input_schema_, &aggregate_nodes_};
  std::vector<Row> buffer_;
  std::vector<SpillFile> partitions_;
  bool parallel_capable_{false};
  bool spilled_{false};
  size_t spill_partitions_{0};
  size_t group_capacity_{0};
  size_t peak_groups_{0};
};

// Sorts the input by the group keys and folds each run of equal keys in one
// sequential pass, so the only aggregation state is the run being read. The
// sort operator owns the runs and the spill, and its output order is the
// group-key order (ascending, NULLs first).
class SortAggregateExecutor final : public GroupedAggregateBase {
 public:
  SortAggregateExecutor(
      Executor child, Schema input_schema,
      std::vector<NamedExpression> group_keys,
      std::vector<NamedExpression> aggregates, Expression having = nullptr,
      GroupedAggregateOptions options = GroupedAggregateOptions{});
  void Dump(std::ostream& o, int indent) const override;
  // Rows buffered before a sorted run is written out.
  [[nodiscard]] size_t SpillThresholdRows() const {
    return options_.spill_threshold_rows != 0
               ? options_.spill_threshold_rows
               : options_.memory_budget_bytes /
                     kGroupedAggregateEstimatedRowBytes;
  }
  [[nodiscard]] bool Sorted() const { return sorted_; }
  // The most groups this shape ever held at once. The sort runs need no group
  // table at all: only the run being read is folded, so the peak is 1 however
  // many groups the input has. This is the state half of what separates the
  // two grouped algorithms.
  [[nodiscard]] size_t PeakResidentGroups() const { return peak_groups_; }

 protected:
  Status Build() override;

 private:
  [[nodiscard]] Status EmitGroup();

  std::vector<SortKeyOrder> key_orders_;
  std::vector<const AggregateExpression*> aggregate_nodes_;
  std::vector<std::optional<slot_t>> key_offsets_;
  AggregateGroupTable table_{&input_schema_, &aggregate_nodes_};
  Executor sorter_;
  Row current_key_;
  bool have_group_{false};
  bool sorted_{false};
  size_t peak_groups_{0};
};

// Folds one group at a time straight off the input: no hash table, no sort, no
// spill. Correct only over an input that already arrives in group-key order
// (ascending, NULLs first) — with no keys there is exactly one group, so any
// input order is fine. With require_ordered_input the operator verifies that
// order itself and refuses an unordered input rather than silently producing an
// order the shape never had.
class StreamAggregateExecutor final : public GroupedAggregateBase {
 public:
  StreamAggregateExecutor(
      Executor child, Schema input_schema,
      std::vector<NamedExpression> group_keys,
      std::vector<NamedExpression> aggregates, Expression having = nullptr,
      GroupedAggregateOptions options = GroupedAggregateOptions{});
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] bool InputOrderChecked() const { return order_checked_; }

 protected:
  Status Build() override;

 private:
  [[nodiscard]] Status CheckInputOrder(const Row& key);
  [[nodiscard]] Status EmitGroup();

  std::vector<SortKeyOrder> key_orders_;
  std::vector<const AggregateExpression*> aggregate_nodes_;
  std::vector<std::optional<slot_t>> key_offsets_;
  AggregateGroupTable table_{&input_schema_, &aggregate_nodes_};
  // The group's key (its first row's) drives the run boundary, while
  // `last_key_` is the previous row's key: the input-order contract is about
  // the row sequence, so it is checked against the row that came before.
  Row current_key_;
  Row last_key_;
  bool have_group_{false};
  bool have_last_{false};
  bool order_checked_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_GROUPED_AGGREGATE_HPP
