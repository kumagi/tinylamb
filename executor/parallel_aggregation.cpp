/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_aggregation.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <condition_variable>
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
#include "common/platform/extended_float.hpp"
#include "executor/aggregation.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "table/lstable_bridge.hpp"
#include "type/row.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Long-double conversion mirroring the ground-truth accumulator: numeric
// values convert, anything else raises like the serial path's ToLongDouble.
StatusOr<platform::ExtendedFloat> StatInputToLongDouble(const Value& value) {
  if (value.type == ValueType::kDouble) {
    return static_cast<platform::ExtendedFloat>(value.value.double_value);
  }
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
    return static_cast<platform::ExtendedFloat>(value.value.int_value);
  }
  return StatusError(StatusCode::kInvalidArgument, "numeric value required");
}

}  // namespace

ParallelAggregationExecutor::ParallelAggregationExecutor(
    std::shared_ptr<ExecutorBase> child, Schema input_schema,
    std::vector<NamedExpression> aggregates, size_t worker_count)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)),
      worker_count_(std::max<size_t>(1, worker_count)) {
  // Two-input DISTINCT statistics (CORR/COVAR with DISTINCT) de-duplicate
  // leading/trailing pairs per row; worker-local dedup sets cannot be merged
  // without the pairs, so force single-worker execution for exactness.  The
  // serial AggregateAccumulator stays the reference for these shapes.
  for (const NamedExpression& named : aggregates_) {
    const auto& aggregate = named.expression->AsAggregateExpression();
    if (aggregate.Distinct() && IsTwoInputStatAggregate(aggregate.GetType())) {
      worker_count_ = 1;
      break;
    }
  }
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
    } else if (IsStatisticalAggregate(aggregate.GetType()) &&
               !aggregate.Distinct() && !has_filter &&
               aggregate.Child()->Type() == TypeTag::kColumnValue) {
      // Statistical aggregates keep long-double partial sums.  Raw numeric
      // storage for the child (and, for COVAR_*/CORR, the trailing argument
      // column) takes the batched fast path; anything else is accumulated
      // per row in the generic loop.
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      const auto& trailing_args = aggregate.TrailingArgs();
      const bool two_input = IsTwoInputStatAggregate(aggregate.GetType());
      int trailing_offset = -1;
      if (two_input && trailing_args.size() == 1 && trailing_args[0] &&
          trailing_args[0]->Type() == TypeTag::kColumnValue) {
        trailing_offset = input_schema_.Offset(
            trailing_args[0]->AsColumnValue().GetColumnName());
      }
      if (offset >= 0 &&
          (two_input ? trailing_offset >= 0 : trailing_args.empty())) {
        input.kind = AggregateInputKind::kStatColumn;
        input.column = static_cast<size_t>(offset);
        if (trailing_offset >= 0) {
          input.trailing_column = static_cast<size_t>(trailing_offset);
        }
        stat_column_indices_.push_back(index);
      } else {
        generic_indices_.push_back(index);
      }
    } else if (!aggregate.Distinct() && !has_filter &&
               (aggregate.GetType() == AggregationType::kCount ||
                aggregate.GetType() == AggregationType::kSum ||
                aggregate.GetType() == AggregationType::kAvg ||
                aggregate.GetType() == AggregationType::kMin ||
                aggregate.GetType() == AggregationType::kMax) &&
               aggregate.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0) {
        const ValueType col_type =
            input_schema_.GetColumn(static_cast<size_t>(offset)).Type();
        if (col_type == ValueType::kDouble) {
          input.kind = AggregateInputKind::kDoubleColumn;
          input.column = static_cast<size_t>(offset);
          double_column_indices_.push_back(index);
        } else if (col_type == ValueType::kInt64) {
          input.kind = AggregateInputKind::kInt64Column;
          input.column = static_cast<size_t>(offset);
          int64_column_indices_.push_back(index);
        } else {
          generic_indices_.push_back(index);
        }
      } else {
        generic_indices_.push_back(index);
      }
    } else if (!aggregate.Distinct() && !has_filter &&
               (aggregate.GetType() == AggregationType::kCount ||
                aggregate.GetType() == AggregationType::kSum ||
                aggregate.GetType() == AggregationType::kAvg ||
                aggregate.GetType() == AggregationType::kMin ||
                aggregate.GetType() == AggregationType::kMax) &&
               aggregate.Child()->Type() == TypeTag::kBinaryExp) {
      const auto& bin = aggregate.Child()->AsBinaryExpression();
      if ((bin.Op() == BinaryOperation::kMultiply ||
           bin.Op() == BinaryOperation::kAdd ||
           bin.Op() == BinaryOperation::kSubtract) &&
          bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right()->Type() == TypeTag::kColumnValue) {
        const int lhs_offset =
            input_schema_.Offset(bin.Left()->AsColumnValue().GetColumnName());
        const int rhs_offset =
            input_schema_.Offset(bin.Right()->AsColumnValue().GetColumnName());
        if (lhs_offset >= 0 && rhs_offset >= 0 &&
            input_schema_.GetColumn(static_cast<size_t>(lhs_offset)).Type() ==
                ValueType::kDouble &&
            input_schema_.GetColumn(static_cast<size_t>(rhs_offset)).Type() ==
                ValueType::kDouble) {
          input.kind = AggregateInputKind::kDoubleBinaryColumn;
          input.column = static_cast<size_t>(lhs_offset);
          input.trailing_column = static_cast<size_t>(rhs_offset);
          input.binary_op = bin.Op();
          double_binary_column_indices_.push_back(index);
        } else {
          generic_indices_.push_back(index);
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
  state.stat_sx.resize(aggregates_.size());
  state.stat_sxx.resize(aggregates_.size());
  state.stat_sy.resize(aggregates_.size());
  state.stat_syy.resize(aggregates_.size());
  state.stat_sxy.resize(aggregates_.size());
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

Status ParallelAggregationExecutor::AccumulateValue(PartialState* state,
                                                    size_t index,
                                                    const Value& value,
                                                    bool apply_distinct) const {
  if (value.IsNull()) {
    return Status::kSuccess;
  }
  const auto& aggregate =
      aggregates_[index].expression->AsAggregateExpression();
  if (aggregate.GetType() == AggregationType::kSum ||
      aggregate.GetType() == AggregationType::kAvg) {
    // SUM/AVG are numeric aggregates; reading the union of a non-numeric
    // value would be garbage.
    if (value.type != ValueType::kInt64 && value.type != ValueType::kDouble) {
      return StatusError(StatusCode::kInvalidArgument,
                         "numeric value required");
    }
  }
  if (apply_distinct && aggregate.Distinct()) {
    const size_t bytes = EstimateValueBytes(value);
    QueryMemoryBudget::Global().ReserveForced(bytes);
    if (!state->distinct_values[index]
             .insert(relational_detail::CanonicalDistinctValue(value))
             .second) {
      QueryMemoryBudget::Global().Release(bytes);
      return Status::kSuccess;
    }
    state->distinct_charged_bytes += bytes;
  }
  switch (aggregate.GetType()) {
    case AggregationType::kSum:
      if (state->values[index].IsNull()) {
        state->values[index] = value;
      } else {
        // Same Status (kIsInfinity) as the serial executor, and no exception
        // on the main-thread Merge replay, which runs outside any try/catch.
        ASSIGN_OR_RETURN(
            Value, next,
            state->values[index].TryArithmetic(value, BinaryOperation::kAdd));
        state->values[index] = std::move(next);
      }
      break;
    case AggregationType::kAvg:
      state->values[index].value.double_value +=
          value.type == ValueType::kDouble
              ? value.value.double_value
              : (value.IsUnsigned()
                     ? static_cast<double>(
                           static_cast<uint64_t>(value.value.int_value))
                     : static_cast<double>(value.value.int_value));
      ++state->counts[index];
      break;
    case AggregationType::kMin:
      // A NaN poisons the whole group (serial ground truth, aggregation.cpp):
      // `value < best` is false for NaN, so without the explicit adoption a
      // NaN arriving after the first value was silently dropped and the
      // result depended on arrival order.
      if (value.type == ValueType::kDouble &&
          std::isnan(value.value.double_value)) {
        state->values[index] = value;
        break;
      }
      if (state->values[index].IsNull() || value < state->values[index]) {
        state->values[index] = value;
      }
      break;
    case AggregationType::kMax:
      if (value.type == ValueType::kDouble &&
          std::isnan(value.value.double_value)) {
        state->values[index] = value;
        break;
      }
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
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::Accumulate(PartialState* state,
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
      RETURN_IF_FAIL(
          AccumulateInt64Column(state, index, chunk.ColumnAt(input.column)));
    } else {
      generic_scratch.push_back(index);
    }
  }
  for (const size_t index : double_column_indices_) {
    const AggregateInput& input = inputs_[index];
    if (chunk.ColumnAt(input.column).Type() == ValueType::kDouble) {
      RETURN_IF_FAIL(
          AccumulateDoubleColumn(state, index, chunk.ColumnAt(input.column)));
    } else {
      generic_scratch.push_back(index);
    }
  }
  for (const size_t index : double_binary_column_indices_) {
    const AggregateInput& input = inputs_[index];
    const ColumnVector& lhs = chunk.ColumnAt(input.column);
    const ColumnVector& rhs = chunk.ColumnAt(input.trailing_column);
    if (lhs.Type() == ValueType::kDouble && rhs.Type() == ValueType::kDouble) {
      RETURN_IF_FAIL(AccumulateDoubleBinaryColumn(state, index, lhs, rhs,
                                                  input.binary_op));
    } else if (lhs.Type() != ValueType::kNull &&
               rhs.Type() != ValueType::kNull) {
      generic_scratch.push_back(index);
    }
  }
  for (const size_t index : stat_column_indices_) {
    const AggregateInput& input = inputs_[index];
    const ColumnVector& child = chunk.ColumnAt(input.column);
    const ValueType child_type = child.Type();
    if (child_type != ValueType::kInt64 && child_type != ValueType::kDouble) {
      // An all-null kNull column contributes nothing (paired semantics keep
      // the result NULL); any other layout defers to the per-row loop.
      if (child_type != ValueType::kNull) {
        generic_scratch.push_back(index);
      }
      continue;
    }
    const ColumnVector* trailing = nullptr;
    if (!aggregates_[index]
             .expression->AsAggregateExpression()
             .TrailingArgs()
             .empty()) {
      trailing = &chunk.ColumnAt(input.trailing_column);
      const ValueType trailing_type = trailing->Type();
      if (trailing_type != ValueType::kInt64 &&
          trailing_type != ValueType::kDouble) {
        if (trailing_type != ValueType::kNull) {
          generic_scratch.push_back(index);
        }
        continue;
      }
    }
    RETURN_IF_FAIL(AccumulateStatColumns(state, index, child, trailing));
  }
  if (!generic_indices_.empty() || !generic_scratch.empty()) {
    RETURN_IF_FAIL(
        AccumulateGeneric(state, chunk, generic_indices_, generic_scratch));
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateGeneric(
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
        ASSIGN_OR_RETURN(Value, keep,
                         (aggregate.WhereFilter()->TryEvaluate(*materialized,
                                                               input_schema_)));
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
          ASSIGN_OR_RETURN(
              Value, child_value,
              (aggregate.Child()->TryEvaluate(*materialized, input_schema_)));
          value = std::move(child_value);
        }
      } else {
        if (!materialized) {
          materialized = chunk.RowAt(row_index);
        }
        ASSIGN_OR_RETURN(
            Value, child_value,
            (aggregate.Child()->TryEvaluate(*materialized, input_schema_)));
        value = std::move(child_value);
      }
      if (IsStatisticalAggregate(aggregate.GetType())) {
        if (value.IsNull()) {
          continue;
        }
        std::vector<Value> trailing_values;
        trailing_values.reserve(aggregate.TrailingArgs().size());
        for (const Expression& extra : aggregate.TrailingArgs()) {
          if (extra) {
            if (!materialized) {
              materialized = chunk.RowAt(row_index);
            }
            ASSIGN_OR_RETURN(
                Value, trailing_value,
                (extra->TryEvaluate(*materialized, input_schema_)));
            trailing_values.push_back(std::move(trailing_value));
          }
        }
        if (aggregate.Distinct()) {
          const size_t bytes = EstimateValueBytes(value);
          QueryMemoryBudget::Global().ReserveForced(bytes);
          if (!state->distinct_values[index]
                   .insert(relational_detail::CanonicalDistinctValue(value))
                   .second) {
            QueryMemoryBudget::Global().Release(bytes);
            continue;
          }
          state->distinct_charged_bytes += bytes;
        }
        RETURN_IF_FAIL(
            AccumulateStatValue(state, index, value, trailing_values));
        continue;
      }
      RETURN_IF_FAIL(AccumulateValue(state, index, value, true));
    }
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateInt64Column(
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
      if (column.IsUnsigned()) {
        uint64_t sum = 0;
        bool any = false;
        bool overflow = false;
        for (size_t row = 0; row < column.Size(); ++row) {
          if (column.IsNull(row)) {
            continue;
          }
          if (__builtin_add_overflow(sum, static_cast<uint64_t>(data[row]),
                                     &sum)) {
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
             __builtin_add_overflow(
                 static_cast<uint64_t>(total.value.int_value), sum, &sum))) {
          return StatusError(StatusCode::kIsInfinity, "uint64 overflow in SUM");
        }
        total = Value(static_cast<int64_t>(sum)).WithUnsigned();
        break;
      }
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
        return StatusError(StatusCode::kIsInfinity, "integer overflow on '+'");
      }
      total = Value(sum);
      break;
    }
    case AggregationType::kAvg: {
      double& total = state->values[aggregate_index].value.double_value;
      int64_t& count = state->counts[aggregate_index];
      for (size_t row = 0; row < column.Size(); ++row) {
        if (column.IsNull(row)) {
          continue;
        }
        total += column.IsUnsigned()
                     ? static_cast<double>(static_cast<uint64_t>(data[row]))
                     : static_cast<double>(data[row]);
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
        if (column.IsUnsigned()) {
          if (best.IsNull() ||
              static_cast<uint64_t>(data[row]) <
                  static_cast<uint64_t>(best.value.int_value)) {
            best = Value(data[row]).WithUnsigned();
          }
        } else {
          if (best.IsNull() || data[row] < best.value.int_value) {
            best = Value(data[row]);
          }
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
        if (column.IsUnsigned()) {
          if (best.IsNull() || static_cast<uint64_t>(best.value.int_value) <
                                   static_cast<uint64_t>(data[row])) {
            best = Value(data[row]).WithUnsigned();
          }
        } else {
          if (best.IsNull() || best.value.int_value < data[row]) {
            best = Value(data[row]);
          }
        }
      }
      break;
    }
    default:
      break;
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateDoubleColumn(
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
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateDoubleBinaryColumn(
    PartialState* state, size_t aggregate_index, const ColumnVector& lhs,
    const ColumnVector& rhs, BinaryOperation op) const {
  const AggregationType type = aggregates_[aggregate_index]
                                   .expression->AsAggregateExpression()
                                   .GetType();
  const std::vector<double>& lhs_data = lhs.DoubleData();
  const std::vector<double>& rhs_data = rhs.DoubleData();
  const auto eval = [op](double a, double b) noexcept -> double {
    switch (op) {
      case BinaryOperation::kMultiply:
        return a * b;
      case BinaryOperation::kAdd:
        return a + b;
      case BinaryOperation::kSubtract:
        return a - b;
      default:
        return 0.0;
    }
  };
  switch (type) {
    case AggregationType::kCount: {
      int64_t& count = state->values[aggregate_index].value.int_value;
      for (size_t row = 0; row < lhs.Size(); ++row) {
        if (!lhs.IsNull(row) && !rhs.IsNull(row)) {
          ++count;
        }
      }
      break;
    }
    case AggregationType::kSum: {
      double sum = 0.0;
      bool any = false;
      for (size_t row = 0; row < lhs.Size(); ++row) {
        if (lhs.IsNull(row) || rhs.IsNull(row)) {
          continue;
        }
        sum += eval(lhs_data[row], rhs_data[row]);
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
      for (size_t row = 0; row < lhs.Size(); ++row) {
        if (lhs.IsNull(row) || rhs.IsNull(row)) {
          continue;
        }
        total += eval(lhs_data[row], rhs_data[row]);
        ++count;
      }
      break;
    }
    case AggregationType::kMin: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < lhs.Size(); ++row) {
        if (lhs.IsNull(row) || rhs.IsNull(row)) {
          continue;
        }
        const double v = eval(lhs_data[row], rhs_data[row]);
        if (std::isnan(v)) {
          best = Value(std::numeric_limits<double>::quiet_NaN());
          continue;
        }
        if (best.IsNull() || v < best.value.double_value) {
          best = Value(v);
        }
      }
      break;
    }
    case AggregationType::kMax: {
      Value& best = state->values[aggregate_index];
      for (size_t row = 0; row < lhs.Size(); ++row) {
        if (lhs.IsNull(row) || rhs.IsNull(row)) {
          continue;
        }
        const double v = eval(lhs_data[row], rhs_data[row]);
        if (std::isnan(v)) {
          best = Value(std::numeric_limits<double>::quiet_NaN());
          continue;
        }
        if (best.IsNull() || best.value.double_value < v) {
          best = Value(v);
        }
      }
      break;
    }
    default:
      break;
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateLSTableBlock(
    PartialState* state, const ParallelScan& scan, const LSTablePageBlock& blk,
    const uint32_t* rows, size_t count) const {
  for (const size_t index : row_count_indices_) {
    state->values[index].value.int_value += static_cast<int64_t>(count);
  }
  for (const size_t index : int64_column_indices_) {
    const slot_t col_slot = scan.TableColumnSlot(inputs_[index].column);
    const LSTableColumnBlock& col = blk.columns[col_slot];
    const AggregationType type =
        aggregates_[index].expression->AsAggregateExpression().GetType();
    const int64_t* data = col.int_cells.data();
    switch (type) {
      case AggregationType::kCount: {
        int64_t& cnt = state->values[index].value.int_value;
        if (!col.has_null) {
          cnt += static_cast<int64_t>(count);
        } else if (rows == nullptr) {
          for (size_t r = 0; r < count; ++r) {
            if (!col.IsNull(r)) {
              ++cnt;
            }
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            if (!col.IsNull(rows[i])) {
              ++cnt;
            }
          }
        }
        break;
      }
      case AggregationType::kSum: {
        if (col.is_unsigned) {
          uint64_t sum = 0;
          bool any = false;
          bool overflow = false;
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (col.IsNull(r)) {
              continue;
            }
            if (__builtin_add_overflow(sum, static_cast<uint64_t>(data[r]),
                                       &sum)) {
              overflow = true;
            }
            any = true;
          }
          if (!any) {
            break;
          }
          Value& total = state->values[index];
          if (overflow ||
              (!total.IsNull() &&
               __builtin_add_overflow(
                   static_cast<uint64_t>(total.value.int_value), sum, &sum))) {
            return StatusError(StatusCode::kIsInfinity,
                               "uint64 overflow in SUM");
          }
          total = Value(static_cast<int64_t>(sum)).WithUnsigned();
          break;
        }
        int64_t sum = 0;
        bool any = false;
        bool overflow = false;
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          if (__builtin_add_overflow(sum, data[r], &sum)) {
            overflow = true;
          }
          any = true;
        }
        if (!any) {
          break;
        }
        Value& total = state->values[index];
        if (overflow ||
            (!total.IsNull() &&
             __builtin_add_overflow(total.value.int_value, sum, &sum))) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow on '+'");
        }
        total = Value(sum);
        break;
      }
      case AggregationType::kAvg: {
        double& total = state->values[index].value.double_value;
        int64_t& cnt = state->counts[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          total += col.is_unsigned
                       ? static_cast<double>(static_cast<uint64_t>(data[r]))
                       : static_cast<double>(data[r]);
          ++cnt;
        }
        break;
      }
      case AggregationType::kMin: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          if (col.is_unsigned) {
            if (best.IsNull() ||
                static_cast<uint64_t>(data[r]) <
                    static_cast<uint64_t>(best.value.int_value)) {
              best = Value(data[r]).WithUnsigned();
            }
          } else if (best.IsNull() || data[r] < best.value.int_value) {
            best = Value(data[r]);
          }
        }
        break;
      }
      case AggregationType::kMax: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          if (col.is_unsigned) {
            if (best.IsNull() || static_cast<uint64_t>(best.value.int_value) <
                                     static_cast<uint64_t>(data[r])) {
              best = Value(data[r]).WithUnsigned();
            }
          } else if (best.IsNull() || best.value.int_value < data[r]) {
            best = Value(data[r]);
          }
        }
        break;
      }
      default:
        break;
    }
  }
  for (const size_t index : double_column_indices_) {
    const slot_t col_slot = scan.TableColumnSlot(inputs_[index].column);
    const LSTableColumnBlock& col = blk.columns[col_slot];
    const AggregationType type =
        aggregates_[index].expression->AsAggregateExpression().GetType();
    const int64_t* data = col.int_cells.data();
    switch (type) {
      case AggregationType::kCount: {
        int64_t& cnt = state->values[index].value.int_value;
        if (!col.has_null) {
          cnt += static_cast<int64_t>(count);
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (!col.IsNull(r)) {
              ++cnt;
            }
          }
        }
        break;
      }
      case AggregationType::kSum: {
        double sum = 0.0;
        bool any = false;
        if (!col.has_null) {
          any = count > 0;
          if (rows == nullptr) {
            for (size_t r = 0; r < count; ++r) {
              sum += std::bit_cast<double>(data[r]);
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              sum += std::bit_cast<double>(data[rows[i]]);
            }
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (col.IsNull(r)) {
              continue;
            }
            sum += std::bit_cast<double>(data[r]);
            any = true;
          }
        }
        if (any) {
          Value& total = state->values[index];
          total = total.IsNull() ? Value(sum)
                                 : Value(total.value.double_value + sum);
        }
        break;
      }
      case AggregationType::kAvg: {
        double& total = state->values[index].value.double_value;
        int64_t& cnt = state->counts[index];
        if (!col.has_null) {
          if (rows == nullptr) {
            for (size_t r = 0; r < count; ++r) {
              total += std::bit_cast<double>(data[r]);
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              total += std::bit_cast<double>(data[rows[i]]);
            }
          }
          cnt += static_cast<int64_t>(count);
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (col.IsNull(r)) {
              continue;
            }
            total += std::bit_cast<double>(data[r]);
            ++cnt;
          }
        }
        break;
      }
      case AggregationType::kMin: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          const auto v = std::bit_cast<double>(data[r]);
          if (std::isnan(v)) {
            best = Value(std::numeric_limits<double>::quiet_NaN());
            continue;
          }
          if (best.IsNull() || v < best.value.double_value) {
            best = Value(v);
          }
        }
        break;
      }
      case AggregationType::kMax: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (col.IsNull(r)) {
            continue;
          }
          const auto v = std::bit_cast<double>(data[r]);
          if (std::isnan(v)) {
            best = Value(std::numeric_limits<double>::quiet_NaN());
            continue;
          }
          if (best.IsNull() || best.value.double_value < v) {
            best = Value(v);
          }
        }
        break;
      }
      default:
        break;
    }
  }
  for (const size_t index : double_binary_column_indices_) {
    const AggregateInput& input = inputs_[index];
    const slot_t lhs_slot = scan.TableColumnSlot(input.column);
    const slot_t rhs_slot = scan.TableColumnSlot(input.trailing_column);
    const LSTableColumnBlock& lhs_col = blk.columns[lhs_slot];
    const LSTableColumnBlock& rhs_col = blk.columns[rhs_slot];
    const int64_t* lhs_data = lhs_col.int_cells.data();
    const int64_t* rhs_data = rhs_col.int_cells.data();
    const AggregationType type =
        aggregates_[index].expression->AsAggregateExpression().GetType();
    const auto eval = [op = input.binary_op](double a,
                                             double b) noexcept -> double {
      switch (op) {
        case BinaryOperation::kMultiply:
          return a * b;
        case BinaryOperation::kAdd:
          return a + b;
        case BinaryOperation::kSubtract:
          return a - b;
        default:
          return 0.0;
      }
    };
    switch (type) {
      case AggregationType::kCount: {
        int64_t& cnt = state->values[index].value.int_value;
        if (!lhs_col.has_null && !rhs_col.has_null) {
          cnt += static_cast<int64_t>(count);
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (!lhs_col.IsNull(r) && !rhs_col.IsNull(r)) {
              ++cnt;
            }
          }
        }
        break;
      }
      case AggregationType::kSum: {
        double sum = 0.0;
        bool any = false;
        if (!lhs_col.has_null && !rhs_col.has_null) {
          any = count > 0;
          if (rows == nullptr) {
            for (size_t r = 0; r < count; ++r) {
              sum += eval(std::bit_cast<double>(lhs_data[r]),
                          std::bit_cast<double>(rhs_data[r]));
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              const uint32_t r = rows[i];
              sum += eval(std::bit_cast<double>(lhs_data[r]),
                          std::bit_cast<double>(rhs_data[r]));
            }
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (lhs_col.IsNull(r) || rhs_col.IsNull(r)) {
              continue;
            }
            sum += eval(std::bit_cast<double>(lhs_data[r]),
                        std::bit_cast<double>(rhs_data[r]));
            any = true;
          }
        }
        if (any) {
          Value& total = state->values[index];
          total = total.IsNull() ? Value(sum)
                                 : Value(total.value.double_value + sum);
        }
        break;
      }
      case AggregationType::kAvg: {
        double& total = state->values[index].value.double_value;
        int64_t& cnt = state->counts[index];
        if (!lhs_col.has_null && !rhs_col.has_null) {
          if (rows == nullptr) {
            for (size_t r = 0; r < count; ++r) {
              total += eval(std::bit_cast<double>(lhs_data[r]),
                            std::bit_cast<double>(rhs_data[r]));
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              const uint32_t r = rows[i];
              total += eval(std::bit_cast<double>(lhs_data[r]),
                            std::bit_cast<double>(rhs_data[r]));
            }
          }
          cnt += static_cast<int64_t>(count);
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r =
                rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
            if (lhs_col.IsNull(r) || rhs_col.IsNull(r)) {
              continue;
            }
            total += eval(std::bit_cast<double>(lhs_data[r]),
                          std::bit_cast<double>(rhs_data[r]));
            ++cnt;
          }
        }
        break;
      }
      case AggregationType::kMin: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (lhs_col.IsNull(r) || rhs_col.IsNull(r)) {
            continue;
          }
          const double v = eval(std::bit_cast<double>(lhs_data[r]),
                                std::bit_cast<double>(rhs_data[r]));
          if (std::isnan(v)) {
            best = Value(std::numeric_limits<double>::quiet_NaN());
            continue;
          }
          if (best.IsNull() || v < best.value.double_value) {
            best = Value(v);
          }
        }
        break;
      }
      case AggregationType::kMax: {
        Value& best = state->values[index];
        for (size_t i = 0; i < count; ++i) {
          const uint32_t r =
              rows == nullptr ? static_cast<uint32_t>(i) : rows[i];
          if (lhs_col.IsNull(r) || rhs_col.IsNull(r)) {
            continue;
          }
          const double v = eval(std::bit_cast<double>(lhs_data[r]),
                                std::bit_cast<double>(rhs_data[r]));
          if (std::isnan(v)) {
            best = Value(std::numeric_limits<double>::quiet_NaN());
            continue;
          }
          if (best.IsNull() || best.value.double_value < v) {
            best = Value(v);
          }
        }
        break;
      }
      default:
        break;
    }
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateStatColumns(
    PartialState* state, size_t aggregate_index, const ColumnVector& child,
    const ColumnVector* trailing) {
  const bool child_is_double = child.Type() == ValueType::kDouble;
  const std::vector<int64_t>* child_ints =
      child_is_double ? nullptr : &child.IntegerData();
  const std::vector<double>* child_doubles =
      child_is_double ? &child.DoubleData() : nullptr;
  const bool trailing_is_double =
      trailing != nullptr && trailing->Type() == ValueType::kDouble;
  const std::vector<int64_t>* trailing_ints =
      trailing != nullptr && !trailing_is_double ? &trailing->IntegerData()
                                                 : nullptr;
  const std::vector<double>* trailing_doubles =
      trailing != nullptr && trailing_is_double ? &trailing->DoubleData()
                                                : nullptr;
  platform::ExtendedFloat& sx = state->stat_sx[aggregate_index];
  platform::ExtendedFloat& sxx = state->stat_sxx[aggregate_index];
  platform::ExtendedFloat& sy = state->stat_sy[aggregate_index];
  platform::ExtendedFloat& syy = state->stat_syy[aggregate_index];
  platform::ExtendedFloat& sxy = state->stat_sxy[aggregate_index];
  int64_t& count = state->counts[aggregate_index];
  for (size_t row = 0; row < child.Size(); ++row) {
    if (child.IsNull(row)) {
      continue;
    }
    if (trailing != nullptr && trailing->IsNull(row)) {
      // Paired-row semantics: rows where either side is NULL are skipped.
      continue;
    }
    const platform::ExtendedFloat y =
        child_is_double
            ? static_cast<platform::ExtendedFloat>((*child_doubles)[row])
            : static_cast<platform::ExtendedFloat>((*child_ints)[row]);
    if (trailing == nullptr) {
      // Single-input forms (VAR_* / STDDEV_*) fold into the x slot exactly
      // like the ground-truth accumulator.
      sx += y;
      sxx += y * y;
    } else {
      const platform::ExtendedFloat x =
          trailing_is_double
              ? static_cast<platform::ExtendedFloat>((*trailing_doubles)[row])
              : static_cast<platform::ExtendedFloat>((*trailing_ints)[row]);
      sy += y;
      syy += y * y;
      sx += x;
      sxx += x * x;
      sxy += x * y;
    }
    ++count;
  }
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::AccumulateStatValue(
    PartialState* state, size_t aggregate_index, const Value& value,
    const std::vector<Value>& trailing_values) const {
  if (value.IsNull()) {
    return Status::kSuccess;
  }
  const AggregationType type = aggregates_[aggregate_index]
                                   .expression->AsAggregateExpression()
                                   .GetType();
  const bool two_input = IsTwoInputStatAggregate(type);
  Value other;
  if (two_input) {
    if (trailing_values.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         ToString(type) + " requires two arguments");
    }
    other = trailing_values[0];
    if (other.IsNull()) {
      return Status::kSuccess;
    }
  }
  ASSIGN_OR_RETURN(platform::ExtendedFloat, y, (StatInputToLongDouble(value)));
  platform::ExtendedFloat& sx = state->stat_sx[aggregate_index];
  platform::ExtendedFloat& sxx = state->stat_sxx[aggregate_index];
  platform::ExtendedFloat& sy = state->stat_sy[aggregate_index];
  platform::ExtendedFloat& syy = state->stat_syy[aggregate_index];
  platform::ExtendedFloat& sxy = state->stat_sxy[aggregate_index];
  if (two_input) {
    ASSIGN_OR_RETURN(platform::ExtendedFloat, x,
                     (StatInputToLongDouble(other)));
    sy += y;
    syy += y * y;
    sx += x;
    sxx += x * x;
    sxy += x * y;
  } else {
    sx += y;
    sxx += y * y;
  }
  ++state->counts[aggregate_index];
  return Status::kSuccess;
}

Status ParallelAggregationExecutor::Merge(PartialState* destination,
                                          const PartialState& source) const {
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    if (aggregate.Distinct()) {
      // Replay each worker's deduped set through the destination's OWN
      // distinct_values: a value present in several workers' sets must be
      // counted once globally.  Single-input statistical aggregates keep
      // long-double partial sums, so they replay through AccumulateStatValue
      // with the same (empty-trailing) shape the per-row path uses.
      // Two-input DISTINCT statistics (CORR/COVAR) never reach this executor
      // (AggregationPlan routes them to the serial path: their pairs cannot
      // be reconstructed from deduped leading values alone).
      if (IsStatisticalAggregate(aggregate.GetType())) {
        for (const Value& value : source.distinct_values[index]) {
          RETURN_IF_FAIL(AccumulateStatValue(destination, index, value, {}));
        }
      } else {
        for (const Value& value : source.distinct_values[index]) {
          RETURN_IF_FAIL(AccumulateValue(destination, index, value, true));
        }
      }
      continue;
    }
    if (IsStatisticalAggregate(aggregate.GetType())) {
      // Long-double partial sums and their paired-row counts combine
      // additively; FinalizeStat applies the division/square-root.
      destination->stat_sx[index] += source.stat_sx[index];
      destination->stat_sxx[index] += source.stat_sxx[index];
      destination->stat_sy[index] += source.stat_sy[index];
      destination->stat_syy[index] += source.stat_syy[index];
      destination->stat_sxy[index] += source.stat_sxy[index];
      destination->counts[index] += source.counts[index];
      continue;
    }
    switch (aggregate.GetType()) {
      case AggregationType::kSum:
        if (!source.values[index].IsNull()) {
          if (destination->values[index].IsNull()) {
            destination->values[index] = source.values[index];
          } else {
            relational_detail::AggregateAccumulator merged_sum(&aggregate,
                                                               &input_schema_);
            RETURN_IF_FAIL(merged_sum.Add(destination->values[index]));
            RETURN_IF_FAIL(merged_sum.Add(source.values[index]));
            ASSIGN_OR_RETURN(Value, total, merged_sum.TryFinish());
            destination->values[index] = std::move(total);
          }
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
            (destination->values[index].type != ValueType::kDouble ||
             !std::isnan(destination->values[index].value.double_value)) &&
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
            (destination->values[index].type != ValueType::kDouble ||
             !std::isnan(destination->values[index].value.double_value)) &&
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
  return Status::kSuccess;
}

Value ParallelAggregationExecutor::FinalizeStat(const PartialState& state,
                                                size_t index) const {
  // Mirrors AggregateAccumulator::Finish's statistical cases expression for
  // expression so both execution paths agree bit for bit.
  const AggregationType type =
      aggregates_[index].expression->AsAggregateExpression().GetType();
  const platform::ExtendedFloat sx = state.stat_sx[index];
  const platform::ExtendedFloat sxx = state.stat_sxx[index];
  const platform::ExtendedFloat sy = state.stat_sy[index];
  const platform::ExtendedFloat syy = state.stat_syy[index];
  const platform::ExtendedFloat sxy = state.stat_sxy[index];
  const int64_t count = state.counts[index];
  switch (type) {
    case AggregationType::kVarPop: {
      if (count == 0) {
        return {};
      }
      const platform::ExtendedFloat mean = sx / count;
      const platform::ExtendedFloat ssd = sxx - (count * mean * mean);
      return Value(static_cast<double>(ssd / count));
    }
    case AggregationType::kVarSamp: {
      if (count < 2) {
        return {};
      }
      const platform::ExtendedFloat mean = sx / count;
      const platform::ExtendedFloat ssd = sxx - (count * mean * mean);
      return Value(static_cast<double>(ssd / (count - 1)));
    }
    case AggregationType::kStddevPop: {
      if (count == 0) {
        return {};
      }
      const platform::ExtendedFloat mean = sx / count;
      const platform::ExtendedFloat ssd = sxx - (count * mean * mean);
      return Value(static_cast<double>(platform::Sqrt(ssd / count)));
    }
    case AggregationType::kStddevSamp: {
      if (count < 2) {
        return {};
      }
      const platform::ExtendedFloat mean = sx / count;
      const platform::ExtendedFloat ssd = sxx - (count * mean * mean);
      return Value(static_cast<double>(platform::Sqrt(ssd / (count - 1))));
    }
    case AggregationType::kCovarPop: {
      if (count == 0) {
        return {};
      }
      const platform::ExtendedFloat mx = sx / count;
      const platform::ExtendedFloat my = sy / count;
      const platform::ExtendedFloat sdd = sxy - (count * mx * my);
      return Value(static_cast<double>(sdd / count));
    }
    case AggregationType::kCovarSamp: {
      if (count < 2) {
        return {};
      }
      const platform::ExtendedFloat mx = sx / count;
      const platform::ExtendedFloat my = sy / count;
      const platform::ExtendedFloat sdd = sxy - (count * mx * my);
      return Value(static_cast<double>(sdd / (count - 1)));
    }
    case AggregationType::kCorr: {
      if (count < 2) {
        return {};
      }
      const platform::ExtendedFloat mx = sx / count;
      const platform::ExtendedFloat my = sy / count;
      const platform::ExtendedFloat sdd = sxy - (count * mx * my);
      const platform::ExtendedFloat vx = sxx - (count * mx * mx);
      const platform::ExtendedFloat vy = syy - (count * my * my);
      return Value(static_cast<double>(sdd / platform::Sqrt(vx * vy)));
    }
    case AggregationType::kRegrCount:
      return Value(count);
    case AggregationType::kRegrSlope:
    case AggregationType::kRegrIntercept:
    case AggregationType::kRegrR2:
    case AggregationType::kRegrAvgX:
    case AggregationType::kRegrAvgY:
    case AggregationType::kRegrSxx:
    case AggregationType::kRegrSyy:
    case AggregationType::kRegrSxy: {
      if (count == 0) {
        return {};
      }
      // x is the trailing argument, y the child: Σx lives in sx.
      const platform::ExtendedFloat mx = sx / count;
      const platform::ExtendedFloat my = sy / count;
      const platform::ExtendedFloat reg_sxy = sxy - (count * mx * my);
      const platform::ExtendedFloat reg_sxx = sxx - (count * mx * mx);
      const platform::ExtendedFloat reg_syy = syy - (count * my * my);
      switch (type) {
        case AggregationType::kRegrAvgX:
          return Value(static_cast<double>(mx));
        case AggregationType::kRegrAvgY:
          return Value(static_cast<double>(my));
        case AggregationType::kRegrSxx:
          return Value(static_cast<double>(reg_sxx));
        case AggregationType::kRegrSyy:
          return Value(static_cast<double>(reg_syy));
        case AggregationType::kRegrSxy:
          return Value(static_cast<double>(reg_sxy));
        case AggregationType::kRegrSlope:
          if (count < 2 || reg_sxx <= 0) {
            return {};
          }
          return Value(static_cast<double>(reg_sxy / reg_sxx));
        case AggregationType::kRegrIntercept:
          if (count < 2 || reg_sxx <= 0) {
            return {};
          }
          return Value(static_cast<double>(my - ((reg_sxy / reg_sxx) * mx)));
        case AggregationType::kRegrR2:
          if (count < 2 || reg_sxx <= 0 || reg_syy <= 0) {
            return {};
          }
          return Value(
              static_cast<double>((reg_sxy * reg_sxy) / (reg_sxx * reg_syy)));
        default:
          return {};
      }
    }
    default:
      return {};
  }
}

Row ParallelAggregationExecutor::Finalize(PartialState state) const {
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    if (IsStatisticalAggregate(aggregate.GetType())) {
      state.values[index] = FinalizeStat(state, index);
      continue;
    }
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

bool ParallelAggregationExecutor::Execute(Row* destination) {
  if (errored_) {
    return FailWith(error_);
  }
  if (executed_) {
    return false;
  }
  std::vector<PartialState> partials;
  partials.reserve(worker_count_);
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    partials.push_back(MakeState());
  }

  if (auto* scan = dynamic_cast<ParallelScan*>(child_.get());
      scan != nullptr && generic_indices_.empty() &&
      stat_column_indices_.empty()) {
    bool can_fold_lstable = true;
    for (const NamedExpression& named : aggregates_) {
      const auto& aggregate = named.expression->AsAggregateExpression();
      const AggregationType type = aggregate.GetType();
      if (aggregate.Distinct() ||
          (type != AggregationType::kCount && type != AggregationType::kSum &&
           type != AggregationType::kAvg && type != AggregationType::kMin &&
           type != AggregationType::kMax)) {
        can_fold_lstable = false;
        break;
      }
    }
    if (can_fold_lstable) {
      StatusOr<bool> folded = scan->TryFoldLSTableBlocks(
          worker_count_,
          [&](size_t worker, const LSTablePageBlock& blk, const uint32_t* rows,
              size_t count) -> Status {
            return AccumulateLSTableBlock(&partials[worker], *scan, blk, rows,
                                          count);
          });
      if (!folded.HasValue()) {
        errored_ = true;
        error_ = folded.GetStatus();
        return FailWith(error_);
      }
      if (folded.Value()) {
        PartialState merged = MakeState();
        for (const PartialState& partial : partials) {
          if (Status st = Merge(&merged, partial); st != Status::kSuccess) {
            errored_ = true;
            error_ = st;
            return FailWith(st);
          }
        }
        *destination = Finalize(merged);
        executed_ = true;
        return true;
      }
    }
  }

  std::mutex input_mutex;
  std::condition_variable turn_cv;
  // Turn-taking under input_mutex: worker w accumulates batches w, w+W, ... .
  // The per-worker partial states are then a deterministic function of the
  // child's batch sequence, so the worker-order Merge below is too — a
  // DOUBLE sum can no longer depend on which worker won a mutex race.
  // Stop/exhaustion flags are written under input_mutex so a waiter blocked
  // on the turn predicate can never miss the wakeup.
  size_t turn = 0;
  bool input_exhausted = false;
  bool stopped = false;
  std::mutex error_mutex;
  Status error = Status::kSuccess;
  std::vector<std::jthread> workers;
  workers.reserve(worker_count_);
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
  for (size_t worker = 0; worker < worker_count_; ++worker) {
    try {
      workers.emplace_back([&, worker] {
        // EXC-SHIM bridge: a child may still surface errors by throwing
        // (Phase 7 removes the last throwing executors); translate them into
        // the Status channel so no exception escapes this executor.
        try {
          DataChunk chunk;
          while (true) {
            size_t rows = 0;
            {
              std::unique_lock input_guard(input_mutex);
              turn_cv.wait(input_guard, [&] {
                return stopped || input_exhausted || turn == worker;
              });
              if (stopped || input_exhausted) {
                return;
              }
              rows = child_->NextBatch(&chunk);
              if (rows == 0) {
                input_exhausted = true;
                turn_cv.notify_all();
                return;
              }
              turn = (turn + 1) % worker_count_;
              turn_cv.notify_all();
            }
            if (Status st = Accumulate(&partials[worker], chunk);
                st != Status::kSuccess) {
              stop_under_lock(st);
              return;
            }
          }
        } catch (const std::exception& ex) {
          stop_under_lock(StatusError(StatusCode::kRuntimeError, ex.what()));
        }
      });
    } catch (const std::exception& ex) {
      stop_under_lock(StatusError(StatusCode::kRuntimeError, ex.what()));
      break;
    } catch (...) {
      stop_under_lock(StatusError(StatusCode::kRuntimeError,
                                  "failed to start aggregation worker"));
      break;
    }
  }
  workers.clear();
  if (error == Status::kSuccess && child_->GetStatus() != Status::kSuccess) {
    error = child_->GetStatus();
  }
  if (error != Status::kSuccess) {
    // Latch the failure: a later Next() must report it instead of returning a
    // well-formed but empty aggregate (COUNT=0 / SUM=NULL).
    errored_ = true;
    error_ = error;
    return FailWith(error);
  }

  PartialState merged = MakeState();
  for (const PartialState& partial : partials) {
    if (Status st = Merge(&merged, partial); st != Status::kSuccess) {
      errored_ = true;
      error_ = st;
      return FailWith(st);
    }
  }
  *destination = Finalize(merged);
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
  if (!Execute(&row)) {
    return 0;
  }
  destination->Append(std::move(row));
  return 1;
}

void ParallelAggregationExecutor::Dump(std::ostream& out, int indent) const {
  out << "ParallelAggregationExecutor (" << worker_count_ << " workers) {";
  for (const NamedExpression& aggregate : aggregates_) {
    out << "\n"
        << Indent(static_cast<size_t>(indent) + 2) << aggregate.name << ": "
        << *aggregate.expression;
  }
  out << "\n" << Indent(static_cast<size_t>(indent)) << "}";
  out << "\n" << Indent(static_cast<size_t>(indent) + 2);
  child_->Dump(out, indent + 2);
}

}  // namespace tinylamb
