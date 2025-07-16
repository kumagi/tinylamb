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

#include <limits>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "type/schema.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

AggregationExecutor::AggregationExecutor(
    std::shared_ptr<ExecutorBase> child,
    std::vector<NamedExpression> aggregates)
    : child_(std::move(child)), aggregates_(std::move(aggregates)) {}

bool AggregationExecutor::Next(Row* dst, RowPosition* rp) {
  if (executed_) {
    return false;
  }
  std::vector<Value> results;
  results.resize(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kCount:
        results[i] = Value(0);
        break;
      case AggregationType::kSum:
        results[i] = Value(0.0);
        break;
      case AggregationType::kAvg:
        results[i] = Value(0.0);
        break;
      case AggregationType::kMin:
        results[i] = Value(std::numeric_limits<double>::max());
        break;
      case AggregationType::kMax:
        results[i] = Value(std::numeric_limits<double>::lowest());
        break;
    }
  }
  Row row;
  int count = 0;
  while (child_->Next(&row, nullptr)) {
    count++;
    for (size_t i = 0; i < aggregates_.size(); ++i) {
      const auto& agg = aggregates_[i].expression->AsAggregateExpression();
      Value val = agg.Child()->Evaluate(row, Schema());
      switch (agg.GetType()) {
        case AggregationType::kSum:
          results[i].value.double_value += val.value.double_value;
          break;
        case AggregationType::kAvg:
          results[i].value.double_value += val.value.double_value;
          break;
        case AggregationType::kMin:
          if (val.value.double_value < results[i].value.double_value) {
            results[i].value.double_value = val.value.double_value;
          }
          break;
        case AggregationType::kMax:
          if (results[i].value.double_value < val.value.double_value) {
            results[i].value.double_value = val.value.double_value;
          }
          break;
        case AggregationType::kCount:
          // NOP
          break;
      }
    }
  }
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    const auto& agg = aggregates_[i].expression->AsAggregateExpression();
    switch (agg.GetType()) {
      case AggregationType::kCount:
        results[i] = Value(count);
        break;
      case AggregationType::kAvg:
        results[i].value.double_value /= count;
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

void AggregationExecutor::Dump(std::ostream& o, int indent) const {
  o << "AggregationExecutor {";
  for (const auto& agg : aggregates_) {
    o << "\n" << Indent(indent + 2) << agg.name << ": " << *agg.expression;
  }
  o << "\n" << Indent(indent) << "}";
}

}  // namespace tinylamb
