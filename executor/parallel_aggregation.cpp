/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_aggregation.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/aggregation.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

ParallelAggregationExecutor::ParallelAggregationExecutor(
    std::shared_ptr<ExecutorBase> child, Schema input_schema,
    std::vector<NamedExpression> aggregates, size_t worker_count)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)),
      worker_count_(std::max<size_t>(1, worker_count)) {
  inputs_.reserve(aggregates_.size());
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    AggregateInput input;
    // Aggregates carrying a FILTER(WHERE ...) predicate (pushed down by the
    // filter_aggregate_pushdown rule) must go through the generic path, which
    // evaluates the predicate per row. The typed fast paths (row count and
    // column scans) ignore WhereFilter and would silently over-count.
    const bool has_filter = static_cast<bool>(aggregate.WhereFilter());
    if (IsCountStar(aggregate) && !has_filter) {
      input.kind = AggregateInputKind::kRowCount;
      row_count_indices_.push_back(index);
    } else if (!aggregate.Distinct() && !has_filter &&
               aggregate.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0) {
        if (input_schema_.GetColumn(offset).Type() == ValueType::kDouble) {
          input.kind = AggregateInputKind::kDoubleColumn;
          input.column = static_cast<size_t>(offset);
          double_column_indices_.push_back(index);
        } else {
          input.kind = AggregateInputKind::kInt64Column;
          input.column = static_cast<size_t>(offset);
          int64_column_indices_.push_back(index);
        }
      } else {
        generic_indices_.push_back(index);
      }
    } else {
      generic_indices_.push_back(index);
    }
    inputs_.push_back(input);
  }
}

ParallelAggregationExecutor::PartialState
ParallelAggregationExecutor::MakeState() const {
  PartialState state;
  state.values.resize(aggregates_.size());
  state.counts.resize(aggregates_.size(), 0);
  state.distinct_values.resize(aggregates_.size());
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const AggregationType type =
        aggregates_[index].expression->AsAggregateExpression().GetType();
    if (type == AggregationType::kCount) {
      state.values[index] = Value(0);
    }
    if (type == AggregationType::kAvg) {
      state.values[index] = Value(0.0);
    }
  }
  return state;
}

void ParallelAggregationExecutor::AccumulateValue(PartialState* state,
                                                  size_t index,
                                                  const Value& value,
                                                  bool apply_distinct) const {
  if (value.IsNull()) {
    return;
  }
  const auto& aggregate =
      aggregates_[index].expression->AsAggregateExpression();
  if (aggregate.GetType() == AggregationType::kSum ||
      aggregate.GetType() == AggregationType::kAvg) {
    // SUM/AVG are numeric aggregates; reading the union of a non-numeric
    // value would be garbage.
    if (value.type != ValueType::kInt64 && value.type != ValueType::kDouble) {
      throw std::runtime_error("numeric value required");
    }
  }
  if (apply_distinct && aggregate.Distinct()) {
    const size_t bytes = EstimateValueBytes(value);
    QueryMemoryBudget::Global().ReserveForced(bytes);
    if (!state->distinct_values[index]
             .insert(relational_detail::CanonicalDistinctValue(value))
             .second) {
      QueryMemoryBudget::Global().Release(bytes);
      return;
    }
    state->distinct_charged_bytes += bytes;
  }
  switch (aggregate.GetType()) {
    case AggregationType::kSum:
      state->values[index] =
          state->values[index].IsNull() ? value : state->values[index] + value;
      break;
    case AggregationType::kAvg:
      state->values[index].value.double_value +=
          value.type == ValueType::kDouble
              ? value.value.double_value
              : static_cast<double>(value.value.int_value);
      ++state->counts[index];
      break;
    case AggregationType::kMin:
      if (state->values[index].IsNull() || value < state->values[index]) {
        state->values[index] = value;
      }
      break;
    case AggregationType::kMax:
      if (state->values[index].IsNull() || state->values[index] < value) {
        state->values[index] = value;
      }
      break;
    case AggregationType::kCount:
      ++state->values[index].value.int_value;
      break;
    case AggregationType::kLogicalAnd:
      if (state->values[index].IsNull()) {
        state->values[index] = Value(value.Truthy() ? int64_t{1} : int64_t{0});
      } else {
        state->values[index] = Value(
            (state->values[index].Truthy() && value.Truthy()) ? int64_t{1}
                                                              : int64_t{0});
      }
      break;
    case AggregationType::kLogicalOr:
      if (state->values[index].IsNull()) {
        state->values[index] = Value(value.Truthy() ? int64_t{1} : int64_t{0});
      } else {
        state->values[index] = Value(
            (state->values[index].Truthy() || value.Truthy()) ? int64_t{1}
                                                              : int64_t{0});
      }
      break;
    default:
      // Unsupported aggregate kinds use the generic executor path.
      break;
  }
}

void ParallelAggregationExecutor::Accumulate(PartialState* state,
                                             const DataChunk& chunk) const {
  // Index groups were precomputed in the constructor; the only per-chunk
  // state is this local scratch vector, so the hot path does not allocate.
  // (It must stay a local: a shared mutable member was written by every
  // worker thread concurrently, corrupting the fallback index lists.)
  std::vector<size_t> generic_scratch;
  generic_scratch.reserve(int64_column_indices_.size() +
                          double_column_indices_.size());
  for (const size_t index : row_count_indices_) {
    state->values[index].value.int_value += static_cast<int64_t>(chunk.Size());
  }
  for (const size_t index : int64_column_indices_) {
    const AggregateInput& input = inputs_[index];
    if (chunk.ColumnAt(input.column).Type() == ValueType::kInt64) {
      AccumulateInt64Column(state, index, chunk.ColumnAt(input.column));
    } else {
      generic_scratch.push_back(index);
    }
  }
  for (const size_t index : double_column_indices_) {
    const AggregateInput& input = inputs_[index];
    if (chunk.ColumnAt(input.column).Type() == ValueType::kDouble) {
      AccumulateDoubleColumn(state, index, chunk.ColumnAt(input.column));
    } else {
      generic_scratch.push_back(index);
    }
  }
  if (!generic_indices_.empty() || !generic_scratch.empty()) {
    AccumulateGeneric(state, chunk, generic_indices_, generic_scratch);
  }
}

void ParallelAggregationExecutor::AccumulateGeneric(
    PartialState* state, const DataChunk& chunk,
    const std::vector<size_t>& always_generic,
    const std::vector<size_t>& fallback) const {
  for (size_t row_index = 0; row_index < chunk.Size(); ++row_index) {
    std::optional<Row> materialized;
    size_t always_pos = 0;
    size_t fallback_pos = 0;
    while (always_pos < always_generic.size() ||
           fallback_pos < fallback.size()) {
      size_t index = 0;
      if (fallback_pos >= fallback.size() ||
          (always_pos < always_generic.size() &&
           always_generic[always_pos] < fallback[fallback_pos])) {
        index = always_generic[always_pos++];
      } else {
        index = fallback[fallback_pos++];
      }
      const auto& aggregate =
          aggregates_[index].expression->AsAggregateExpression();
      // FILTER(WHERE ...) predicate: skip the row for this aggregate when the
      // predicate is not TRUE (mirrors the serial AggregationExecutor).
      if (aggregate.WhereFilter()) {
        if (!materialized) {
          materialized = chunk.RowAt(row_index);
        }
        const Value keep =
            aggregate.WhereFilter()->Evaluate(*materialized, input_schema_);
        if (keep.IsNull() || !keep.Truthy()) {
          continue;
        }
      }
      Value value;
      if (IsCountStar(aggregate)) {
        value = Value(1);
      } else if (aggregate.Child()->Type() == TypeTag::kColumnValue) {
        const int offset = input_schema_.Offset(
            aggregate.Child()->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          value =
              chunk.ColumnAt(static_cast<size_t>(offset)).ValueAt(row_index);
        } else {
          if (!materialized) {
            materialized = chunk.RowAt(row_index);
          }
          value = aggregate.Child()->Evaluate(*materialized, input_schema_);
        }
      } else {
        if (!materialized) {
          materialized = chunk.RowAt(row_index);
        }
        value = aggregate.Child()->Evaluate(*materialized, input_schema_);
      }
      AccumulateValue(state, index, value, true);
    }
  }
}

void ParallelAggregationExecutor::AccumulateInt64Column(
    PartialState* state, size_t aggregate_index,
    const ColumnVector& column) const {
  const AggregationType type = aggregates_[aggregate_index]
                                   .expression->AsAggregateExpression()
                                   .GetType();
  const std::vector<int64_t>& data = column.IntegerData();
  switch (type) {
    case AggregationType::kCount: {
      int64_t& count = state->values[aggregate_index].value.int_value;
      for (size_t row = 0; row < column.Size(); ++row) {
        if (!column.IsNull(row)) {
          ++count;
        }
      }
      break;
    }
    case AggregationType::kSum: {
      // The serial executor raises "integer overflow on '+'"; the parallel
      // path must not silently wrap (UB) instead.
      int64_t sum = 0;
      bool any = false;
      bool overflow = false;
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        if (__builtin_add_overflow(sum, data[row], &sum)) {
          overflow = true;
        }
        any = true;
      }
      if (!any) {
        break;
      }
      Value& total = state->values[aggregate_index];
      if (overflow ||
          (!total.IsNull() &&
           __builtin_add_overflow(total.value.int_value, sum, &sum))) {
        throw std::runtime_error("integer overflow on '+'");
      }
      total = total.IsNull() ? Value(sum) : Value(sum);
      break;
    }
    case AggregationType::kAvg: {
      double& total = state->values[aggregate_index].value.double_value;
      int64_t& count = state->counts[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        total += static_cast<double>(data[row]);
        ++count;
      }
      break;
    }
    case AggregationType::kMin: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        if (best.IsNull() || data[row] < best.value.int_value) {
          best = Value(data[row]);
        }
      }
      break;
    }
    case AggregationType::kMax: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        if (best.IsNull() || best.value.int_value < data[row]) {
          best = Value(data[row]);
        }
      }
      break;
    }
    default:
      break;
  }
}

void ParallelAggregationExecutor::AccumulateDoubleColumn(
    PartialState* state, size_t aggregate_index,
    const ColumnVector& column) const {
  const AggregationType type = aggregates_[aggregate_index]
                                   .expression->AsAggregateExpression()
                                   .GetType();
  const std::vector<double>& data = column.DoubleData();
  switch (type) {
    case AggregationType::kCount: {
      int64_t& count = state->values[aggregate_index].value.int_value;
      for (size_t row = 0; row < column.Size(); ++row) {
        if (!column.IsNull(row)) {
          ++count;
        }
      }
      break;
    }
    case AggregationType::kSum: {
      double sum = 0.0;
      bool any = false;
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        sum += data[row];
        any = true;
      }
      if (!any) {
        break;
      }
      Value& total = state->values[aggregate_index];
      total =
          total.IsNull() ? Value(sum) : Value(total.value.double_value + sum);
      break;
    }
    case AggregationType::kAvg: {
      double& total = state->values[aggregate_index].value.double_value;
      int64_t& count = state->counts[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        total += data[row];
        ++count;
      }
      break;
    }
    case AggregationType::kMin: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        // GoogleSQL: any NaN in the group makes MIN/MAX NaN (matches the
        // serial typed path and the ground truth accumulator).
        if (std::isnan(data[row])) {
          best = Value(std::numeric_limits<double>::quiet_NaN());
          continue;
        }
        if (best.IsNull() || data[row] < best.value.double_value) {
          best = Value(data[row]);
        }
      }
      break;
    }
    case AggregationType::kMax: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        if (std::isnan(data[row])) {
          best = Value(std::numeric_limits<double>::quiet_NaN());
          continue;
        }
        if (best.IsNull() || best.value.double_value < data[row]) {
          best = Value(data[row]);
        }
      }
      break;
    }
    default:
      break;
  }
}

void ParallelAggregationExecutor::Merge(PartialState* destination,
                                        const PartialState& source) const {
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    if (aggregate.Distinct()) {
      for (const Value& value : source.distinct_values[index]) {
        AccumulateValue(destination, index, value, true);
      }
      continue;
    }
    switch (aggregate.GetType()) {
      case AggregationType::kSum:
        if (!source.values[index].IsNull()) {
          destination->values[index] =
              destination->values[index].IsNull()
                  ? source.values[index]
                  : destination->values[index] + source.values[index];
        }
        break;
      case AggregationType::kAvg:
        destination->values[index].value.double_value +=
            source.values[index].value.double_value;
        destination->counts[index] += source.counts[index];
        break;
      case AggregationType::kMin:
        // NaN-aware merge: a NaN partial must win (and a NaN destination
        // must not be replaced), matching the GoogleSQL group semantics.
        if (source.values[index].type == ValueType::kDouble &&
            std::isnan(source.values[index].value.double_value)) {
          destination->values[index] = source.values[index];
          break;
        }
        if (!source.values[index].IsNull() &&
            !(destination->values[index].type == ValueType::kDouble &&
              std::isnan(destination->values[index].value.double_value)) &&
            (destination->values[index].IsNull() ||
             source.values[index] < destination->values[index])) {
          destination->values[index] = source.values[index];
        }
        break;
      case AggregationType::kMax:
        if (source.values[index].type == ValueType::kDouble &&
            std::isnan(source.values[index].value.double_value)) {
          destination->values[index] = source.values[index];
          break;
        }
        if (!source.values[index].IsNull() &&
            !(destination->values[index].type == ValueType::kDouble &&
              std::isnan(destination->values[index].value.double_value)) &&
            (destination->values[index].IsNull() ||
             destination->values[index] < source.values[index])) {
          destination->values[index] = source.values[index];
        }
        break;
      case AggregationType::kCount:
        destination->values[index].value.int_value +=
            source.values[index].value.int_value;
        break;
      case AggregationType::kLogicalAnd:
        if (!source.values[index].IsNull()) {
          if (destination->values[index].IsNull()) {
            destination->values[index] = source.values[index];
          } else {
            destination->values[index] =
                Value((destination->values[index].Truthy() &&
                       source.values[index].Truthy())
                          ? int64_t{1}
                          : int64_t{0});
          }
        }
        break;
      case AggregationType::kLogicalOr:
        if (!source.values[index].IsNull()) {
          if (destination->values[index].IsNull()) {
            destination->values[index] = source.values[index];
          } else {
            destination->values[index] =
                Value((destination->values[index].Truthy() ||
                       source.values[index].Truthy())
                          ? int64_t{1}
                          : int64_t{0});
          }
        }
        break;
      default:
        // Non-streamable aggregate kinds are not assigned to this executor.
        break;
    }
  }
}

Row ParallelAggregationExecutor::Finalize(PartialState state) const {
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    if (aggregate.GetType() != AggregationType::kAvg) {
      continue;
    }
    if (state.counts[index] == 0) {
      state.values[index] = Value();
    } else {
      state.values[index].value.double_value /=
          static_cast<double>(state.counts[index]);
    }
  }
  return Row(std::move(state.values));
}

bool ParallelAggregationExecutor::Next(Row* destination,
                                       RowPosition* /*position*/) {
  if (errored_) {
    std::rethrow_exception(error_);
  }
  if (executed_) {
    return false;
  }
  std::vector<PartialState> partials;
  partials.reserve(worker_count_);
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    partials.push_back(MakeState());
  }

  std::mutex input_mutex;
  std::mutex error_mutex;
  std::exception_ptr error;
  std::atomic<bool> stopped{false};
  std::vector<std::jthread> workers;
  workers.reserve(worker_count_);
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    workers.emplace_back([&, worker] {
      try {
        DataChunk chunk;
        while (!stopped.load(std::memory_order_relaxed)) {
          size_t rows = 0;
          {
            std::scoped_lock input_guard(input_mutex);
            rows = child_->NextBatch(&chunk);
          }
          if (rows == 0) {
            break;
          }
          Accumulate(&partials[worker], chunk);
        }
      } catch (...) {
        stopped.store(true, std::memory_order_relaxed);
        std::scoped_lock error_guard(error_mutex);
        if (!error) {
          error = std::current_exception();
        }
      }
    });
  }
  workers.clear();
  if (error) {
    // Latch the failure: a later Next() must rethrow instead of returning a
    // well-formed but empty aggregate (COUNT=0 / SUM=NULL).
    errored_ = true;
    error_ = error;
    std::rethrow_exception(error);
  }

  PartialState merged = MakeState();
  for (const PartialState& partial : partials) {
    Merge(&merged, partial);
  }
  *destination = Finalize(std::move(merged));
  executed_ = true;
  return true;
}

size_t ParallelAggregationExecutor::NextBatch(DataChunk* destination,
                                              size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  Row row;
  if (!Next(&row, nullptr)) {
    return 0;
  }
  destination->Append(std::move(row));
  return 1;
}

void ParallelAggregationExecutor::Dump(std::ostream& out, int indent) const {
  out << "ParallelAggregationExecutor (" << worker_count_ << " workers) {";
  for (const NamedExpression& aggregate : aggregates_) {
    out << "\n"
        << Indent(indent + 2) << aggregate.name << ": "
        << *aggregate.expression;
  }
  out << "\n" << Indent(indent) << "}";
}

}  // namespace tinylamb
