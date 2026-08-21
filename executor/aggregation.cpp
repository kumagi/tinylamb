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

#include "executor/aggregation.hpp"

#include <optional>
#include <unordered_set>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "executor/query_memory.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

AggregationExecutor::AggregationExecutor(
    std::shared_ptr<ExecutorBase> child, Schema input_schema,
    std::vector<NamedExpression> aggregates, size_t jit_threshold_rows)
    : child_(std::move(child)),
      input_schema_(std::move(input_schema)),
      aggregates_(std::move(aggregates)),
      jit_threshold_rows_(jit_threshold_rows) {
  if (aggregates_.size() == 1) {
    const auto& aggregate =
        aggregates_[0].expression->AsAggregateExpression();
    if (aggregate.GetType() == AggregationType::kSum && !aggregate.Distinct() &&
        aggregate.Child()->Type() == TypeTag::kColumnValue) {
      const int offset = input_schema_.Offset(
          aggregate.Child()->AsColumnValue().GetColumnName());
      if (offset >= 0 && input_schema_.GetColumn(offset).Type() ==
                             ValueType::kInt64) {
        jit_sum_eligible_ = true;
        jit_sum_column_ = static_cast<uint16_t>(offset);
      }
    }
  }
}

bool AggregationExecutor::Next(Row* dst, RowPosition* /*rp*/) {
  if (executed_) {
    return false;
  }
  if (jit_sum_eligible_) {
    int64_t total = 0;
    bool any = false;
    while (child_->NextBatch(&input_batch_) != 0) {
      rows_seen_ += input_batch_.Size();
      if (!jit_attempted_ && rows_seen_ >= jit_threshold_rows_) {
        jit_attempted_ = true;
        jit_sum_ = JitInt64Kernels::CompileSum();
      }
      const ColumnVector& column = input_batch_.ColumnAt(jit_sum_column_);
      if (jit_sum_ && input_batch_.ZoneMapAt(jit_sum_column_).NullCount() == 0) {
        total += jit_sum_->Sum(column.IntegerData().data(), column.Size());
        any = any || column.Size() != 0;
        ++jit_batches_;
      } else {
        for (size_t row = 0; row < column.Size(); ++row) {
          if (column.IsNull(row)) continue;
          total += column.ValueAt(row).value.int_value;
          any = true;
        }
      }
    }
    *dst = Row({any ? Value(total) : Value()});
    executed_ = true;
    return true;
  }
  std::vector<Value> results;
  results.resize(aggregates_.size());
  std::vector<int64_t> counts(aggregates_.size(), 0);
  std::vector<std::unordered_set<Value>> distinct_values(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kCount:
        results[i] = Value(0);
        break;
      case AggregationType::kAvg:
        results[i] = Value(0.0);
        break;
      case AggregationType::kSum:
      case AggregationType::kMin:
      case AggregationType::kMax:
        results[i] = Value();
        break;
    }
  }
  while (child_->NextBatch(&input_batch_) != 0) {
    for (size_t row_index = 0; row_index < input_batch_.Size(); ++row_index) {
      std::optional<Row> materialized;
      for (size_t i = 0; i < aggregates_.size(); ++i) {
        const auto& agg = aggregates_[i].expression->AsAggregateExpression();
        const bool count_star =
            agg.GetType() == AggregationType::kCount &&
            agg.Child()->Type() == TypeTag::kColumnValue &&
            agg.Child()->AsColumnValue().GetColumnName().name == "*";
        Value val;
        if (count_star) {
          val = Value(1);
        } else if (agg.Child()->Type() == TypeTag::kColumnValue) {
          const int offset = input_schema_.Offset(
              agg.Child()->AsColumnValue().GetColumnName());
          if (offset >= 0) {
            val = input_batch_.ColumnAt(static_cast<size_t>(offset))
                      .ValueAt(row_index);
          } else {
            if (!materialized) materialized = input_batch_.RowAt(row_index);
            val = agg.Child()->Evaluate(*materialized, input_schema_);
          }
        } else {
          if (!materialized) materialized = input_batch_.RowAt(row_index);
          val = agg.Child()->Evaluate(*materialized, input_schema_);
        }
        if (val.IsNull()) continue;
        if (agg.Distinct()) {
          const size_t bytes = EstimateValueBytes(val);
          QueryMemoryBudget::Global().ReserveForced(bytes);
          if (!distinct_values[i].insert(val).second) {
            QueryMemoryBudget::Global().Release(bytes);
            continue;
          }
        }
        switch (agg.GetType()) {
          case AggregationType::kSum:
            results[i] = results[i].IsNull() ? val : results[i] + val;
            break;
          case AggregationType::kAvg:
            results[i].value.double_value +=
                val.type == ValueType::kDouble
                    ? val.value.double_value
                    : static_cast<double>(val.value.int_value);
            ++counts[i];
            break;
          case AggregationType::kMin:
            if (results[i].IsNull() || val < results[i]) results[i] = val;
            break;
          case AggregationType::kMax:
            if (results[i].IsNull() || results[i] < val) results[i] = val;
            break;
          case AggregationType::kCount:
            ++results[i].value.int_value;
            break;
        }
      }
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kAvg:
        if (counts[i] == 0) {
          results[i] = Value();
        } else {
          results[i].value.double_value /= static_cast<double>(counts[i]);
        }
        break;
      default:
        // NOP
        break;
    }
  }

  *dst = Row(results);
  executed_ = true;
  return true;
}

size_t AggregationExecutor::NextBatch(DataChunk* destination,
                                      size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) return 0;
  Row row;
  if (!Next(&row, nullptr)) return 0;
  destination->Append(std::move(row));
  return 1;
}

void AggregationExecutor::Dump(std::ostream& o, int indent) const {
  o << "AggregationExecutor {";
  for (const auto& agg : aggregates_) {
    o << "\n" << Indent(indent + 2) << agg.name << ": " << *agg.expression;
  }
  o << "\n" << Indent(indent) << "}";
}

}  // namespace tinylamb
