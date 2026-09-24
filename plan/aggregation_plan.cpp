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

#include "plan/aggregation_plan.hpp"

#include <cstddef>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

AggregationPlan::AggregationPlan(Plan child,
                                 std::vector<NamedExpression> aggregates,
                                 AggregationStrategy strategy)
    : child_(std::move(child)),
      aggregates_(std::move(aggregates)),
      strategy_(strategy),
      schema_(GenerateSchema()),
      output_stats_(GenerateOutputStats()) {}

const Schema& AggregationPlan::GetSchema() const { return schema_; }

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

const Table* AggregationPlan::ScanSource() const {
  return child_->ScanSource();
}

const TableStatistics& AggregationPlan::GetStats() const {
  return output_stats_;
}

size_t AggregationPlan::AccessRowCount() const {
  return child_->AccessRowCount();
}

size_t AggregationPlan::EmitRowCount() const { return output_stats_.Rows(); }

TableStatistics AggregationPlan::GenerateOutputStats() const {
  std::vector<ColumnStats> columns;
  columns.reserve(schema_.ColumnCount());
  for (size_t i = 0; i < schema_.ColumnCount(); ++i) {
    columns.emplace_back(schema_.GetColumn(i).Type());
  }
  TableStatistics stats(schema_);
  stats.Assign(1, std::move(columns));
  return stats;
}

std::string AggregationPlan::ToString() const {
  const char* name = "HashAggregate";
  if (strategy_ == AggregationStrategy::kSort) {
    name = "SortAggregate";
  } else if (strategy_ == AggregationStrategy::kStream) {
    name = "StreamAggregate";
  }
  std::string s = std::string(name) + " {";
  bool first = true;
  for (const auto& agg : aggregates_) {
    if (!first) {
      s += ", ";
    }
    first = false;
    s += agg.name + ": " + agg.expression->ToString();
  }
  s += "}";
  return s;
}

void AggregationPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(static_cast<size_t>(indent)) << ToString() << "\n";
  child_->Dump(o, indent + 2);
}

Schema AggregationPlan::GenerateSchema() const {
  std::vector<Column> columns;
  for (const auto& agg : aggregates_) {
    if (!agg.expression) {
      columns.emplace_back(agg.name, ValueType::kInt64);
      continue;
    }
    if (agg.expression->Type() != TypeTag::kAggregateExp) {
      ValueType type = ValueType::kInt64;
      try {
        const Type result = agg.expression->ResultType(child_->GetSchema());
        if (result.GetType() == TypeTag::kDouble) {
          type = ValueType::kDouble;
        } else if (result.GetType() == TypeTag::kVarChar) {
          type = ValueType::kVarChar;
        } else {
          type = ValueType::kInt64;
        }
      } catch (...) {
        type = ValueType::kInt64;
      }
      columns.emplace_back(agg.name, type);
      continue;
    }
    const auto& expression = agg.expression->AsAggregateExpression();
    ValueType type = ValueType::kInt64;
    if (expression.GetType() == AggregationType::kAvg) {
      type = ValueType::kDouble;
    } else if (expression.GetType() != AggregationType::kCount &&
               expression.Child()) {
      // SUM/MIN/MAX inherit their argument's evaluated type so that
      // arithmetic arguments (e.g. TPC-H Q1
      // sum(l_extendedprice*(1-l_discount))) do not silently claim Int64.
      // When the argument type cannot be resolved, keep the legacy Int64
      // default.
      const Type result = expression.Child()->ResultType(child_->GetSchema());
      switch (result.GetType()) {
        case TypeTag::kDouble:
          type = ValueType::kDouble;
          break;
        case TypeTag::kVarChar:
          type = ValueType::kVarChar;
          break;
        case TypeTag::kDate:
          type = ValueType::kDate;
          break;
        default:
          break;
      }
    }
    columns.emplace_back(agg.name, type);
  }
  return {"", columns};
}

bool AggregationPlan::ContainsParameterSlots() const {
  if (child_->ContainsParameterSlots()) {
    return true;
  }
  return std::ranges::any_of(aggregates_, [](const NamedExpression& aggregate) {
    return ContainsParameterSlot(aggregate.expression);
  });
}

bool AggregationPlan::InstallParameterSlots(
    const std::vector<Value>& parameters, size_t* cursor,
    std::vector<Expression>* slots) {
  if (!child_->InstallParameterSlots(parameters, cursor, slots)) {
    return false;
  }
  for (NamedExpression& aggregate : aggregates_) {
    if (!SlotizeInPlace(&aggregate.expression, parameters, cursor, slots)) {
      return false;
    }
  }
  return true;
}

}  // namespace tinylamb
