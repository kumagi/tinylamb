/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/grouped_aggregate.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/aggregation.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/collation.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// The group-key order every shape resolves through: the engine default for a
// grouping key (ascending, NULLs first) under the key's own collation. The sort
// shape sorts with this contract, the stream shape validates its input against
// it, and the hash shape promises no order at all — so the three shapes cannot
// disagree about which rows are peers.
std::vector<SortKeyOrder> KeyOrders(const std::vector<NamedExpression>& keys) {
  std::vector<SortKeyOrder> orders;
  orders.reserve(keys.size());
  for (const NamedExpression& key : keys) {
    orders.emplace_back(/*ascending=*/true, /*nulls_first=*/true,
                        CollationOfExpression(key.expression));
  }
  return orders;
}

ValueType ValueTypeOfTag(TypeTag tag) {
  switch (tag) {
    case TypeTag::kInteger:
    case TypeTag::kBigInt:
      return ValueType::kInt64;
    case TypeTag::kDouble:
      return ValueType::kDouble;
    case TypeTag::kVarChar:
      return ValueType::kVarChar;
    case TypeTag::kDate:
      return ValueType::kDate;
    case TypeTag::kArray:
      return ValueType::kArray;
    default:
      return ValueType::kVarChar;
  }
}

// Canonical group key of one row. Values are canonicalized exactly like the
// relational path's grouping loop (NULL folds to one group, NaN to one peer,
// -0 to +0, a case-insensitive key folded), so the hash index, the sort runs
// and the stream run boundary all agree on "the same group". A plain column key
// is read positionally; anything else is evaluated. The offsets are resolved
// once per operator, not per row.
std::optional<slot_t> KeyOffset(const NamedExpression& key,
                                const Schema& schema) {
  if (!key.expression || key.expression->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const int found =
      schema.Offset(key.expression->AsColumnValue().GetColumnName());
  if (found < 0 || std::cmp_greater_equal(found, schema.ColumnCount())) {
    return std::nullopt;
  }
  return static_cast<slot_t>(found);
}

std::vector<std::optional<slot_t>> KeyOffsets(
    const std::vector<NamedExpression>& keys, const Schema& schema) {
  std::vector<std::optional<slot_t>> offsets;
  offsets.reserve(keys.size());
  for (const NamedExpression& key : keys) {
    offsets.push_back(KeyOffset(key, schema));
  }
  return offsets;
}

Status EvaluateKeys(const std::vector<NamedExpression>& keys,
                    const Schema& schema,
                    const std::vector<std::optional<slot_t>>& offsets,
                    const Row& row, std::vector<Value>* scratch) {
  scratch->clear();
  scratch->reserve(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    const std::optional<slot_t>& offset = offsets[i];
    if (offset.has_value()) {
      scratch->push_back(
          relational_detail::CanonicalDistinctValue(row[*offset]));
      continue;
    }
    ASSIGN_OR_RETURN(Value, value,
                     (keys[i].expression->TryEvaluate(row, schema)));
    scratch->push_back(relational_detail::CanonicalDistinctValue(value));
  }
  return Status::kSuccess;
}

ValueType KeyColumnType(const NamedExpression& key, const Schema& input) {
  const std::optional<slot_t> offset = KeyOffset(key, input);
  if (offset.has_value()) {
    return input.GetColumn(*offset).Type();
  }
  try {
    return ValueTypeOfTag(key.expression->ResultType(input).GetType());
  } catch (...) {
    return ValueType::kVarChar;
  }
}

ValueType ResultColumnType(const NamedExpression& aggregate,
                           const Schema& input) {
  if (!aggregate.expression) {
    return ValueType::kInt64;
  }
  if (aggregate.expression->Type() != TypeTag::kAggregateExp) {
    try {
      return ValueTypeOfTag(aggregate.expression->ResultType(input).GetType());
    } catch (...) {
      return ValueType::kInt64;
    }
  }
  const auto& expression = aggregate.expression->AsAggregateExpression();
  switch (expression.GetType()) {
    case AggregationType::kAvg:
      return ValueType::kDouble;
    case AggregationType::kCount:
      return ValueType::kInt64;
    default:
      break;
  }
  if (!expression.Child()) {
    return ValueType::kInt64;
  }
  try {
    return ValueTypeOfTag(expression.Child()->ResultType(input).GetType());
  } catch (...) {
    return ValueType::kInt64;
  }
}

// The grouped output row shape, shared by the three shapes: the group keys in
// GROUP BY order, then the aggregate results in target-list order. The plan
// node states the same shape (plan/aggregation_plan.hpp), and it is the scope
// a HAVING filter is evaluated against, exactly like a Selection above it.
Schema GroupedOutputSchema(const Schema& input,
                           const std::vector<NamedExpression>& group_keys,
                           const std::vector<NamedExpression>& aggregates) {
  std::vector<Column> columns;
  columns.reserve(group_keys.size() + aggregates.size());
  for (size_t i = 0; i < group_keys.size(); ++i) {
    const std::string& name = group_keys[i].name;
    columns.emplace_back(
        name.empty() ? ("group_key_" + std::to_string(i)) : name,
        KeyColumnType(group_keys[i], input));
  }
  for (const NamedExpression& aggregate : aggregates) {
    columns.emplace_back(aggregate.name, ResultColumnType(aggregate, input));
  }
  return {"grouped_agg", std::move(columns)};
}

std::vector<const AggregateExpression*> AggregateNodes(
    const std::vector<NamedExpression>& aggregates) {
  std::vector<const AggregateExpression*> nodes;
  nodes.reserve(aggregates.size());
  for (const NamedExpression& aggregate : aggregates) {
    if (aggregate.expression &&
        aggregate.expression->Type() == TypeTag::kAggregateExp) {
      nodes.push_back(&aggregate.expression->AsAggregateExpression());
    } else {
      // A target-list item that is not an aggregate call contributes no
      // accumulator; the slot is still reserved so the output layout matches
      // the plan.
      nodes.push_back(nullptr);
    }
  }
  return nodes;
}

// Partition assignment for the hash shape's spill: a group key never spans two
// partitions, so re-grouping one partition at a time is still exact. NULL keys
// are legal here, so they get the same dedicated tag the parallel hash join
// uses ('\0' never collides with a real encoding prefix).
size_t PartitionOf(const std::vector<Value>& key, size_t partitions) {
  std::string encoded;
  for (const Value& value : key) {
    if (value.IsNull()) {
      encoded.push_back('\0');
    } else {
      encoded += value.EncodeMemcomparableFormat();
    }
  }
  return relational_detail::SpillPartitionOf(encoded, partitions);
}

bool SameGroup(const std::vector<SortKeyOrder>& orders, const Row& left,
               const Row& right) {
  if (left.values_.size() != right.values_.size()) {
    return false;
  }
  for (size_t i = 0; i < orders.size(); ++i) {
    if (orders[i].Compare(left[i], right[i]) != 0) {
      return false;
    }
  }
  return true;
}

}  // namespace

// ===================================================== AggregateGroupTable ===

void AggregateGroupTable::AddGroup(Row key) {
  keys_.push_back(std::move(key));
  for (const AggregateExpression* aggregate : *aggregates_) {
    states_.emplace_back(aggregate, input_schema_);
  }
}

StatusOr<size_t> AggregateGroupTable::FindOrCreate(
    const std::vector<Value>& canonical_key) {
  // The transparent lookup probes the scratch key vector directly, so a row
  // landing in an existing group costs no Row at all; only a new group pays
  // for one.
  const auto found = index_.find(canonical_key);
  if (found != index_.end()) {
    return found->second;
  }
  const size_t group = keys_.size();
  index_.emplace(Row(canonical_key), group);
  keys_.emplace_back(canonical_key);
  for (const AggregateExpression* aggregate : *aggregates_) {
    states_.emplace_back(aggregate, input_schema_);
  }
  return group;
}

StatusOr<size_t> AggregateGroupTable::FindOrCreate(Row key) {
  auto [iterator, inserted] = index_.emplace(key, keys_.size());
  if (inserted) {
    AddGroup(std::move(key));
  }
  return iterator->second;
}

Status AggregateGroupTable::Accumulate(size_t group, const Row& row) {
  for (size_t i = 0; i < aggregates_->size(); ++i) {
    const AggregateExpression* aggregate = (*aggregates_)[i];
    if (aggregate == nullptr) {
      continue;
    }
    relational_detail::AggregateAccumulator& state = StateFor(group, i);
    if (state.IsDone()) {
      continue;
    }
    if (aggregate->WhereFilter()) {
      ASSIGN_OR_RETURN(
          Value, keep,
          (aggregate->WhereFilter()->TryEvaluate(row, *input_schema_)));
      if (!relational_detail::Truthy(keep)) {
        continue;
      }
    }
    relational_detail::AggregateInput input;
    if (IsCountStar(*aggregate)) {
      input.value = Value(1);
    } else if (aggregate->Child()) {
      ASSIGN_OR_RETURN(Value, value,
                       (aggregate->Child()->TryEvaluate(row, *input_schema_)));
      input.value = std::move(value);
    }
    if (aggregate->Having() != AggregateHavingModifier::kNone &&
        aggregate->HavingCondition()) {
      ASSIGN_OR_RETURN(
          Value, condition,
          (aggregate->HavingCondition()->TryEvaluate(row, *input_schema_)));
      input.condition = std::move(condition);
    }
    for (const auto& term : aggregate->InnerOrderBy()) {
      ASSIGN_OR_RETURN(Value, key,
                       (term.expression->TryEvaluate(row, *input_schema_)));
      input.order_keys.push_back(std::move(key));
    }
    if (aggregate->SecondaryArg()) {
      ASSIGN_OR_RETURN(
          Value, auxiliary,
          (aggregate->SecondaryArg()->TryEvaluate(row, *input_schema_)));
      input.auxiliary = std::move(auxiliary);
    }
    for (const Expression& extra : aggregate->TrailingArgs()) {
      if (!extra) {
        continue;
      }
      ASSIGN_OR_RETURN(Value, trailing,
                       (extra->TryEvaluate(row, *input_schema_)));
      input.trailing_values.push_back(std::move(trailing));
    }
    RETURN_IF_FAIL(state.Add(std::move(input)));
  }
  return Status::kSuccess;
}

StatusOr<Row> AggregateGroupTable::Finish(size_t group) const {
  const Row& key = keys_[group];
  std::vector<Value> values;
  values.reserve(key.values_.size() + aggregates_->size());
  for (const Value& value : key.values_) {
    values.push_back(value);
  }
  for (size_t i = 0; i < aggregates_->size(); ++i) {
    if ((*aggregates_)[i] == nullptr) {
      values.emplace_back();
      continue;
    }
    const relational_detail::AggregateAccumulator& state =
        states_[(group * aggregates_->size()) + i];
    ASSIGN_OR_RETURN(Value, finished, state.TryFinish());
    values.push_back(std::move(finished));
  }
  return Row(std::move(values));
}

Status AggregateGroupTable::Merge(const AggregateGroupTable& other) {
  for (size_t group = 0; group < other.keys_.size(); ++group) {
    auto [iterator, inserted] =
        index_.emplace(other.keys_[group], keys_.size());
    if (inserted) {
      AddGroup(other.keys_[group]);
    }
    const size_t target = iterator->second;
    for (size_t i = 0; i < aggregates_->size(); ++i) {
      if ((*aggregates_)[i] == nullptr) {
        continue;
      }
      RETURN_IF_FAIL(StateFor(target, i).Merge(
          other.states_[(group * other.aggregates_->size()) + i]));
    }
  }
  return Status::kSuccess;
}

void AggregateGroupTable::Clear() {
  index_.clear();
  keys_.clear();
  states_.clear();
}

// ==================================================== GroupedAggregateBase ===

bool GroupedAggregateBase::Next(Row* dst, RowPosition* rp) {
  if (Materialize() != Status::kSuccess) {
    return false;
  }
  if (cursor_ >= output_rows_.size()) {
    return false;
  }
  *dst = output_rows_[cursor_++];
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  return true;
}

size_t GroupedAggregateBase::NextBatch(DataChunk* destination,
                                       size_t max_rows) {
  destination->Reset();
  if (max_rows == 0 || Materialize() != Status::kSuccess) {
    return 0;
  }
  size_t emitted = 0;
  while (cursor_ < output_rows_.size() && emitted < max_rows) {
    destination->Append(std::move(output_rows_[cursor_++]));
    ++emitted;
  }
  return emitted;
}

Status GroupedAggregateBase::Materialize() {
  if (materialized_) {
    return GetStatus();
  }
  materialized_ = true;
  output_rows_.clear();
  cursor_ = 0;
  const Status built = Build();
  if (built != Status::kSuccess) {
    FailWith(built);
  }
  return GetStatus();
}

StatusOr<bool> GroupedAggregateBase::KeepsGroup(const Row& group_row) const {
  if (!having_) {
    return true;
  }
  ASSIGN_OR_RETURN(Value, verdict,
                   (having_->TryEvaluate(group_row, output_schema_)));
  return relational_detail::Truthy(verdict);
}

// ===================================================== HashAggregateExecutor
// ==

HashAggregateExecutor::HashAggregateExecutor(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> group_keys,
    std::vector<NamedExpression> aggregates, Expression having,
    GroupedAggregateOptions options)
    : GroupedAggregateBase(std::move(child), std::move(input_schema),
                           std::move(group_keys), std::move(aggregates),
                           std::move(having), options),
      key_orders_(KeyOrders(this->group_keys_)) {
  output_schema_ =
      GroupedOutputSchema(input_schema_, this->group_keys_, this->aggregates_);
  aggregate_nodes_ = AggregateNodes(this->aggregates_);
  group_capacity_ =
      options_.memory_budget_bytes == 0
          ? std::numeric_limits<size_t>::max()
          : std::max<size_t>(1, options_.memory_budget_bytes /
                                    kGroupedAggregateEstimatedRowBytes);

  // Positional gate: plain column keys and, per aggregate, COUNT(*) or a plain
  // column argument with a mergeable state. Only then may a worker fold a batch
  // without evaluating an expression, because the evaluator is a
  // single-threaded contract (the same gate the relational path's parallel
  // grouping uses).
  key_offsets_ = KeyOffsets(this->group_keys_, this->input_schema_);
  parallel_capable_ = !this->group_keys_.empty();
  for (const std::optional<slot_t>& offset : key_offsets_) {
    parallel_capable_ = parallel_capable_ && offset.has_value();
  }
  aggregate_offsets_.reserve(this->aggregates_.size());
  aggregate_count_star_.reserve(this->aggregates_.size());
  for (const AggregateExpression* node : aggregate_nodes_) {
    const bool count_star = node != nullptr && IsCountStar(*node);
    aggregate_count_star_.push_back(count_star);
    std::optional<slot_t> offset;
    if (node != nullptr) {
      // The merge gate is the accumulator's own answer, not a second copy of
      // its rule: a probe state per aggregate answers "may a worker hand this
      // to the merge phase" exactly, with nothing to drift. A row filter has
      // no positional form, so it rules the parallel build out separately.
      const relational_detail::AggregateAccumulator probe(node, &input_schema_);
      if (!node->WhereFilter() && !node->SecondaryArg() &&
          node->TrailingArgs().empty() && probe.CanMerge() &&
          (count_star || !node->Distinct())) {
        if (count_star) {
          offset = static_cast<slot_t>(0);
        } else if (node->Child() != nullptr) {
          offset = KeyOffset(NamedExpression("", node->Child()), input_schema_);
        }
      }
    }
    aggregate_offsets_.push_back(offset);
    parallel_capable_ =
        parallel_capable_ && (node == nullptr || offset.has_value());
  }
  if (!parallel_capable_) {
    // Guessing would silently change the answer, so the operator keeps the
    // serial build and the plan's worker count is simply not honoured.
    options_.worker_count = 1;
  }
}

Status HashAggregateExecutor::AccumulateRow(AggregateGroupTable* table,
                                            const Row& row) {
  std::vector<Value> scratch;
  RETURN_IF_FAIL(
      EvaluateKeys(group_keys_, input_schema_, key_offsets_, row, &scratch));
  ASSIGN_OR_RETURN(size_t, group, table->FindOrCreate(scratch));
  return table->Accumulate(group, row);
}

Status HashAggregateExecutor::AppendToPartition(const Row& row) {
  std::vector<Value> scratch;
  RETURN_IF_FAIL(
      EvaluateKeys(group_keys_, input_schema_, key_offsets_, row, &scratch));
  return partitions_[PartitionOf(scratch, partitions_.size())].Append(row);
}

Status HashAggregateExecutor::StartPartitioning(const std::vector<Row>& rows) {
  partitions_.clear();
  partitions_.reserve(std::max<size_t>(1, options_.spill_partition_count));
  for (size_t i = 0; i < std::max<size_t>(1, options_.spill_partition_count);
       ++i) {
    partitions_.emplace_back();
  }
  for (const Row& row : rows) {
    RETURN_IF_FAIL(AppendToPartition(row));
  }
  spill_partitions_ = partitions_.size();
  spilled_ = true;
  return Status::kSuccess;
}

Status HashAggregateExecutor::EmitTable(AggregateGroupTable* table) {
  for (size_t group = 0; group < table->size(); ++group) {
    ASSIGN_OR_RETURN(Row, row, table->Finish(group));
    ASSIGN_OR_RETURN(bool, keep, KeepsGroup(row));
    if (!keep) {
      continue;
    }
    output_rows_.push_back(std::move(row));
  }
  return Status::kSuccess;
}

void HashAggregateExecutor::NoteResidentGroups(
    const AggregateGroupTable& table) {
  peak_groups_ = std::max(peak_groups_, table.size());
}

Status HashAggregateExecutor::AccumulatePositional(AggregateGroupTable* table,
                                                   const DataChunk& chunk) {
  for (size_t row = 0; row < chunk.Size(); ++row) {
    const Row image = chunk.RowAt(row);
    std::vector<Value> scratch;
    scratch.reserve(key_offsets_.size());
    for (const std::optional<slot_t>& offset : key_offsets_) {
      // Key offsets resolved at construction are always engaged here; an
      // expression without a positional form never reaches this row path.
      assert(offset.has_value());
      scratch.push_back(
          // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
          relational_detail::CanonicalDistinctValue(image[*offset]));
    }
    ASSIGN_OR_RETURN(size_t, group, table->FindOrCreate(scratch));
    for (size_t i = 0; i < aggregate_nodes_.size(); ++i) {
      if (aggregate_nodes_[i] == nullptr) {
        continue;
      }
      const std::optional<slot_t>& value_offset = aggregate_offsets_[i];
      assert(value_offset.has_value());
      const Value value =
          // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
          aggregate_count_star_[i] ? Value(1) : image[*value_offset];
      RETURN_IF_FAIL(table->StateFor(group, i).Add(value));
    }
  }
  return Status::kSuccess;
}

Status HashAggregateExecutor::Build() {
  if (options_.worker_count > 1) {
    return BuildParallel();
  }
  return BuildSerial();
}

Status HashAggregateExecutor::BuildSerial() {
  const size_t partitions = std::max<size_t>(1, options_.spill_partition_count);
  QueryMemoryCharge charge;
  DataChunk batch;
  bool partitioning = false;
  while (child_->NextBatch(&batch) != 0) {
    input_rows_ += batch.Size();
    for (size_t row = 0; row < batch.Size(); ++row) {
      const Row image = batch.RowAt(row);
      if (partitioning) {
        RETURN_IF_FAIL(AppendToPartition(image));
        continue;
      }
      const size_t bytes = EstimateRowBytes(image) + sizeof(RowPosition);
      const bool over_groups = table_.size() >= group_capacity_;
      const bool over_bytes =
          (options_.memory_budget_bytes != 0 &&
           charge.Bytes() + bytes > options_.memory_budget_bytes) ||
          !QueryMemoryBudget::Global().CanReserve(bytes);
      if (over_groups || over_bytes) {
        // The group table is this shape's memory, and the buffered rows exist
        // only so a later spill can re-group them partition by partition. Once
        // either bound is reached the input read so far is partitioned and the
        // rest of the input follows it there.
        buffer_.push_back(image);
        RETURN_IF_FAIL(StartPartitioning(buffer_));
        buffer_.clear();
        buffer_.shrink_to_fit();
        charge.ReleaseAll();
        table_.Clear();
        partitioning = true;
        continue;
      }
      RETURN_IF_FAIL(AccumulateRow(&table_, image));
      NoteResidentGroups(table_);
      buffer_.push_back(image);
      charge.Add(bytes);
    }
  }
  if (FailWithChildOf(*child_)) {
    return GetStatus();
  }
  if (!partitioning) {
    // A scalar payload has exactly one group even over an empty input.
    if (table_.empty() && group_keys_.empty()) {
      ASSIGN_OR_RETURN(size_t, scalar_group,
                       (table_.FindOrCreate(std::vector<Value>{})));
      static_cast<void>(scalar_group);
    }
    return EmitTable(&table_);
  }
  // The partitions are finished exactly once, after the last row: they are read
  // back one at a time below, and a spill file that cannot be written is the
  // shape's error rather than a short GROUP BY.
  for (SpillFile& partition : partitions_) {
    RETURN_IF_FAIL(partition.FinishWriting());
  }
  for (size_t index = 0; index < partitions; ++index) {
    auto local = std::make_unique<AggregateGroupTable>(&input_schema_,
                                                       &aggregate_nodes_);
    Status partition_error = Status::kSuccess;
    RETURN_IF_FAIL(partitions_[index].ForEachRow([&](const Row& row) {
      if (partition_error == Status::kSuccess) {
        partition_error = AccumulateRow(local.get(), row);
      }
    }));
    RETURN_IF_FAIL(partition_error);
    NoteResidentGroups(*local);
    RETURN_IF_FAIL(EmitTable(local.get()));
  }
  partitions_.clear();
  return Status::kSuccess;
}

Status HashAggregateExecutor::BuildParallel() {
  const size_t workers = options_.worker_count;
  std::vector<std::unique_ptr<AggregateGroupTable>> locals;
  locals.reserve(workers);
  for (size_t w = 0; w < workers; ++w) {
    locals.push_back(std::make_unique<AggregateGroupTable>(&input_schema_,
                                                           &aggregate_nodes_));
  }
  std::mutex input_mutex;
  std::condition_variable turn_cv;
  // Turn-taking under input_mutex: worker w may pull a batch only on its
  // turn, so batch i is always accumulated by worker i % workers.  The
  // per-worker partial contents are then a deterministic function of the
  // child's batch sequence, and the worker-order merge below turns the
  // whole aggregation into one — a DOUBLE partial sum can no longer depend
  // on which worker happened to win a mutex race (rank boundaries read
  // these bits).  Stop/error flags are written under input_mutex so a
  // waiter blocked on the turn predicate can never miss the wakeup.
  size_t turn = 0;
  bool input_exhausted = false;
  bool stopped = false;
  std::mutex error_mutex;
  Status error = Status::kSuccess;
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  const auto stop_under_lock = [&](Status failure) {
    {
      std::scoped_lock input_guard(input_mutex);
      stopped = true;
      turn_cv.notify_all();
    }
    std::scoped_lock error_guard(error_mutex);
    if (error == Status::kSuccess) {
      error = std::move(failure);
    }
  };
  for (size_t w = 0; w < workers; ++w) {
    try {
      threads.emplace_back([&, w] {
        // EXC-SHIM bridge: a child may still surface errors by throwing
        // (Phase 7 removes the last throwing executors); translate them into
        // the Status channel so no exception escapes this executor.
        try {
          DataChunk batch;
          while (true) {
            size_t rows = 0;
            {
              std::unique_lock input_guard(input_mutex);
              turn_cv.wait(input_guard, [&] {
                return stopped || input_exhausted || turn == w;
              });
              if (stopped || input_exhausted) {
                return;
              }
              rows = child_->NextBatch(&batch);
              if (rows == 0) {
                input_exhausted = true;
                turn_cv.notify_all();
                return;
              }
              input_rows_ += rows;
              turn = (turn + 1) % workers;
              turn_cv.notify_all();
            }
            const Status st = AccumulatePositional(locals[w].get(), batch);
            if (st != Status::kSuccess) {
              stop_under_lock(st);
              return;
            }
            std::scoped_lock error_guard(error_mutex);
            NoteResidentGroups(*locals[w]);
          }
        } catch (const std::exception& ex) {
          stop_under_lock(StatusError(StatusCode::kRuntimeError, ex.what()));
        }
      });
    } catch (...) {
      stop_under_lock(
          StatusError(StatusCode::kRuntimeError,
                      "failed to start grouped aggregation worker"));
      break;
    }
  }
  threads.clear();
  if (error == Status::kSuccess && child_->GetStatus() != Status::kSuccess) {
    error = child_->GetStatus();
  }
  if (error != Status::kSuccess) {
    return error;
  }
  // Merge in worker order, so the group order is a deterministic function of
  // the input and a mis-estimated worker count never changes the answer.
  for (const std::unique_ptr<AggregateGroupTable>& local : locals) {
    RETURN_IF_FAIL(table_.Merge(*local));
    NoteResidentGroups(table_);
  }
  return EmitTable(&table_);
}

void HashAggregateExecutor::Dump(std::ostream& o, int indent) const {
  o << "HashAggregate (groups=" << output_rows_.size()
    << ", input_rows=" << input_rows_
    << ", budget=" << options_.memory_budget_bytes << "B";
  if (options_.worker_count > 1) {
    o << ", workers=" << options_.worker_count;
  }
  o << ", spill=" << (spilled_ ? "partitioned" : "in-memory");
  if (spilled_) {
    o << ", partitions=" << spill_partitions_;
  }
  o << ")\n";
  child_->Dump(o, indent + 2);
}

// ===================================================== SortAggregateExecutor
// ==

SortAggregateExecutor::SortAggregateExecutor(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> group_keys,
    std::vector<NamedExpression> aggregates, Expression having,
    GroupedAggregateOptions options)
    : GroupedAggregateBase(std::move(child), std::move(input_schema),
                           std::move(group_keys), std::move(aggregates),
                           std::move(having), options),
      key_orders_(KeyOrders(this->group_keys_)) {
  output_schema_ =
      GroupedOutputSchema(input_schema_, this->group_keys_, this->aggregates_);
  aggregate_nodes_ = AggregateNodes(this->aggregates_);
  key_offsets_ = KeyOffsets(this->group_keys_, this->input_schema_);
}

Status SortAggregateExecutor::EmitGroup() {
  if (!have_group_) {
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(Row, row, table_.Finish(0));
  table_.Clear();
  have_group_ = false;
  peak_groups_ = std::max<size_t>(peak_groups_, 1);
  ASSIGN_OR_RETURN(bool, keep, KeepsGroup(row));
  if (keep) {
    output_rows_.push_back(std::move(row));
  }
  sorted_ = true;
  return Status::kSuccess;
}

Status SortAggregateExecutor::Build() {
  // The sort owns the runs and the spill; this shape only folds each run of
  // equal keys, so the working set is one group plus the sort's own window.
  std::vector<SortExecutor::Key> keys;
  keys.reserve(group_keys_.size());
  for (const NamedExpression& key : group_keys_) {
    keys.push_back(
        SortExecutor::Key{.expression = key.expression,
                          .ascending = true,
                          .nulls_first = std::nullopt,
                          .collation = CollationOfExpression(key.expression)});
  }
  SortExecutor::Options sort_options;
  sort_options.memory_budget_bytes = options_.memory_budget_bytes;
  sort_options.merge_window_rows = options_.merge_window_rows == 0
                                       ? SortExecutor::kDefaultMergeWindowRows
                                       : options_.merge_window_rows;
  sort_options.spill_threshold_rows = SpillThresholdRows();
  // A planned run threshold means the shape expects to spill, so it plans the
  // external merge; without one the sort keeps its own adaptive selection.
  sort_options.spill_mode = SpillThresholdRows() == 0
                                ? SortExecutor::SpillMode::kAuto
                                : SortExecutor::SpillMode::kExternalMerge;
  sorter_ =
      std::make_shared<SortExecutor>(child_, input_schema_, std::move(keys),
                                     /*worker_count=*/1, sort_options);
  sorted_ = true;
  DataChunk batch;
  while (sorter_->NextBatch(&batch) != 0) {
    input_rows_ += batch.Size();
    for (size_t row = 0; row < batch.Size(); ++row) {
      const Row image = batch.RowAt(row);
      std::vector<Value> scratch;
      RETURN_IF_FAIL(EvaluateKeys(group_keys_, input_schema_, key_offsets_,
                                  image, &scratch));
      const Row key(scratch);
      if (have_group_ && !SameGroup(key_orders_, current_key_, key)) {
        RETURN_IF_FAIL(EmitGroup());
      }
      if (!have_group_) {
        current_key_ = key;
        ASSIGN_OR_RETURN(size_t, opened,
                         (table_.FindOrCreate(current_key_.values_)));
        static_cast<void>(opened);
        have_group_ = true;
      }
      RETURN_IF_FAIL(table_.Accumulate(0, image));
    }
  }
  if (FailWithChildOf(*sorter_)) {
    return GetStatus();
  }
  RETURN_IF_FAIL(EmitGroup());
  // A scalar payload aggregates over an empty input into one group.  The
  // synthesis must not fire when rows WERE consumed but HAVING rejected the
  // single group: re-aggregating an empty state would emit a spurious row
  // (e.g. `SELECT COUNT(*) FROM t HAVING COUNT(*) = 0` over a non-empty t).
  if (group_keys_.empty() && output_rows_.empty() && input_rows_ == 0) {
    ASSIGN_OR_RETURN(size_t, scalar_group,
                     (table_.FindOrCreate(std::vector<Value>{})));
    static_cast<void>(scalar_group);
    have_group_ = true;
    RETURN_IF_FAIL(EmitGroup());
  }
  return Status::kSuccess;
}

void SortAggregateExecutor::Dump(std::ostream& o, int indent) const {
  o << "SortAggregate (groups=" << output_rows_.size()
    << ", input_rows=" << input_rows_
    << ", budget=" << options_.memory_budget_bytes
    << "B, run_rows=" << SpillThresholdRows()
    << ", window=" << options_.merge_window_rows << ")\n";
  child_->Dump(o, indent + 2);
}

// ================================================== StreamAggregateExecutor
// ===

StreamAggregateExecutor::StreamAggregateExecutor(
    Executor child, Schema input_schema,
    std::vector<NamedExpression> group_keys,
    std::vector<NamedExpression> aggregates, Expression having,
    GroupedAggregateOptions options)
    : GroupedAggregateBase(std::move(child), std::move(input_schema),
                           std::move(group_keys), std::move(aggregates),
                           std::move(having), options),
      key_orders_(KeyOrders(this->group_keys_)) {
  output_schema_ =
      GroupedOutputSchema(input_schema_, this->group_keys_, this->aggregates_);
  aggregate_nodes_ = AggregateNodes(this->aggregates_);
  key_offsets_ = KeyOffsets(this->group_keys_, this->input_schema_);
}

Status StreamAggregateExecutor::CheckInputOrder(const Row& key) {
  if (group_keys_.empty()) {
    // One group, so every input order is the group-key order.
    return Status::kSuccess;
  }
  // The contract is lexicographic over the whole key: the input must be
  // non-decreasing. SortKeyOrder (ascending, NULLS FIRST) already places a NULL
  // before every value, so a NULL that should have come first shows up here as
  // a backwards key. Only the first position that actually differs decides --
  // a key that advanced frees the positions after it.
  if (have_last_) {
    for (size_t i = 0; i < key.values_.size(); ++i) {
      const int compared = key_orders_[i].Compare(last_key_[i], key[i]);
      if (compared == 0) {
        continue;
      }
      if (compared > 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "StreamAggregate input is not ordered by the group "
                           "keys (key " +
                               std::to_string(i) + ")");
      }
      break;
    }
  }
  last_key_ = key;
  have_last_ = true;
  return Status::kSuccess;
}

Status StreamAggregateExecutor::EmitGroup() {
  if (!have_group_) {
    return Status::kSuccess;
  }
  ASSIGN_OR_RETURN(Row, row, table_.Finish(0));
  table_.Clear();
  have_group_ = false;
  ASSIGN_OR_RETURN(bool, keep, KeepsGroup(row));
  if (keep) {
    output_rows_.push_back(std::move(row));
  }
  return Status::kSuccess;
}

Status StreamAggregateExecutor::Build() {
  DataChunk batch;
  while (child_->NextBatch(&batch) != 0) {
    input_rows_ += batch.Size();
    for (size_t row = 0; row < batch.Size(); ++row) {
      const Row image = batch.RowAt(row);
      std::vector<Value> scratch;
      RETURN_IF_FAIL(EvaluateKeys(group_keys_, input_schema_, key_offsets_,
                                  image, &scratch));
      const Row key(scratch);
      if (options_.require_ordered_input) {
        RETURN_IF_FAIL(CheckInputOrder(key));
        order_checked_ = true;
      }
      if (have_group_ && !SameGroup(key_orders_, current_key_, key)) {
        RETURN_IF_FAIL(EmitGroup());
      }
      if (!have_group_) {
        current_key_ = key;
        ASSIGN_OR_RETURN(size_t, opened,
                         (table_.FindOrCreate(current_key_.values_)));
        static_cast<void>(opened);
        have_group_ = true;
      }
      RETURN_IF_FAIL(table_.Accumulate(0, image));
    }
  }
  if (FailWithChildOf(*child_)) {
    return GetStatus();
  }
  RETURN_IF_FAIL(EmitGroup());
  // A scalar payload aggregates over an empty input into one group.  The
  // synthesis must not fire when rows WERE consumed but HAVING rejected the
  // single group: re-aggregating an empty state would emit a spurious row
  // (e.g. `SELECT COUNT(*) FROM t HAVING COUNT(*) = 0` over a non-empty t).
  if (group_keys_.empty() && output_rows_.empty() && input_rows_ == 0) {
    ASSIGN_OR_RETURN(size_t, scalar_group,
                     (table_.FindOrCreate(std::vector<Value>{})));
    static_cast<void>(scalar_group);
    have_group_ = true;
    RETURN_IF_FAIL(EmitGroup());
  }
  return Status::kSuccess;
}

void StreamAggregateExecutor::Dump(std::ostream& o, int indent) const {
  o << "StreamAggregate (groups=" << output_rows_.size()
    << ", input_rows=" << input_rows_ << ", keys=" << group_keys_.size()
    << ", order=" << (order_checked_ ? "checked" : "trusted") << ")\n";
  child_->Dump(o, indent + 2);
}

}  // namespace tinylamb
