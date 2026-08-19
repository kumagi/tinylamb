/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/parallel_aggregation.hpp"

#include <algorithm>
#include <atomic>
#include <exception>
#include <mutex>
#include <optional>
#include <ostream>
#include <thread>
#include <utility>

#include "expression/aggregate_expression.hpp"
#include "type/row.hpp"

namespace tinylamb {

ParallelAggregationExecutor::ParallelAggregationExecutor(
    std::shared_ptr<ExecutorBase> child, Schema input_schema,
    std::vector<NamedExpression> aggregates, size_t worker_count)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)),
      worker_count_(std::max<size_t>(1, worker_count)) {}

ParallelAggregationExecutor::PartialState
ParallelAggregationExecutor::MakeState() const {
  PartialState state;
  state.values.resize(aggregates_.size());
  state.counts.resize(aggregates_.size(), 0);
  state.distinct_values.resize(aggregates_.size());
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const AggregationType type =
        aggregates_[index].expression->AsAggregateExpression().GetType();
    if (type == AggregationType::kCount) state.values[index] = Value(0);
    if (type == AggregationType::kAvg) state.values[index] = Value(0.0);
  }
  return state;
}

void ParallelAggregationExecutor::AccumulateValue(
    PartialState* state, size_t index, const Value& value,
    bool apply_distinct) const {
  if (value.IsNull()) return;
  const auto& aggregate =
      aggregates_[index].expression->AsAggregateExpression();
  if (apply_distinct && aggregate.Distinct() &&
      !state->distinct_values[index].insert(value).second) {
    return;
  }
  switch (aggregate.GetType()) {
    case AggregationType::kSum:
      state->values[index] = state->values[index].IsNull()
                                 ? value
                                 : state->values[index] + value;
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
  }
}

void ParallelAggregationExecutor::Accumulate(PartialState* state,
                                             const DataChunk& chunk) const {
  for (size_t row_index = 0; row_index < chunk.Size(); ++row_index) {
    std::optional<Row> materialized;
    for (size_t index = 0; index < aggregates_.size(); ++index) {
      const auto& aggregate =
          aggregates_[index].expression->AsAggregateExpression();
      const bool count_star =
          aggregate.GetType() == AggregationType::kCount &&
          aggregate.Child()->Type() == TypeTag::kColumnValue &&
          aggregate.Child()->AsColumnValue().GetColumnName().name == "*";
      Value value;
      if (count_star) {
        value = Value(1);
      } else if (aggregate.Child()->Type() == TypeTag::kColumnValue) {
        const int offset = input_schema_.Offset(
            aggregate.Child()->AsColumnValue().GetColumnName());
        if (offset >= 0) {
          value = chunk.ColumnAt(static_cast<size_t>(offset))
                      .ValueAt(row_index);
        } else {
          if (!materialized) materialized = chunk.RowAt(row_index);
          value = aggregate.Child()->Evaluate(*materialized, input_schema_);
        }
      } else {
        if (!materialized) materialized = chunk.RowAt(row_index);
        value = aggregate.Child()->Evaluate(*materialized, input_schema_);
      }
      AccumulateValue(state, index, value, true);
    }
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
        if (!source.values[index].IsNull() &&
            (destination->values[index].IsNull() ||
             source.values[index] < destination->values[index])) {
          destination->values[index] = source.values[index];
        }
        break;
      case AggregationType::kMax:
        if (!source.values[index].IsNull() &&
            (destination->values[index].IsNull() ||
             destination->values[index] < source.values[index])) {
          destination->values[index] = source.values[index];
        }
        break;
      case AggregationType::kCount:
        destination->values[index].value.int_value +=
            source.values[index].value.int_value;
        break;
    }
  }
}

Row ParallelAggregationExecutor::Finalize(PartialState state) const {
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const auto& aggregate =
        aggregates_[index].expression->AsAggregateExpression();
    if (aggregate.GetType() != AggregationType::kAvg) continue;
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
  if (executed_) return false;
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
          if (rows == 0) break;
          Accumulate(&partials[worker], chunk);
        }
      } catch (...) {
        stopped.store(true, std::memory_order_relaxed);
        std::scoped_lock error_guard(error_mutex);
        if (!error) error = std::current_exception();
      }
    });
  }
  workers.clear();
  if (error) std::rethrow_exception(error);

  PartialState merged = MakeState();
  for (const PartialState& partial : partials) Merge(&merged, partial);
  *destination = Finalize(std::move(merged));
  executed_ = true;
  return true;
}

size_t ParallelAggregationExecutor::NextBatch(DataChunk* destination,
                                               size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) return 0;
  Row row;
  if (!Next(&row, nullptr)) return 0;
  destination->Append(std::move(row));
  return 1;
}

void ParallelAggregationExecutor::Dump(std::ostream& out, int indent) const {
  out << "ParallelAggregationExecutor (" << worker_count_ << " workers) {";
  for (const NamedExpression& aggregate : aggregates_) {
    out << "\n" << Indent(indent + 2) << aggregate.name << ": "
        << *aggregate.expression;
  }
  out << "\n" << Indent(indent) << "}";
}

}  // namespace tinylamb
