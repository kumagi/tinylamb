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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <thread>
#include <utility>
#include <string>
#include <ostream>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/parallel_thresholds.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

AggregationPlan::AggregationPlan(Plan child,
                                 std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      aggregates_(std::move(aggregates)),
      schema_(GenerateSchema()) {}

const Schema& AggregationPlan::GetSchema() const { return schema_; }

// EmitExecutor lives in the relational factory (executor/relational_factory.cpp).

const Table* AggregationPlan::ScanSource() const {
  return child_->ScanSource();
}

const TableStatistics& AggregationPlan::GetStats() const {
  return child_->GetStats();
}

size_t AggregationPlan::AccessRowCount() const {
  return child_->AccessRowCount();
}

size_t AggregationPlan::EmitRowCount() const { return 1; }

std::string AggregationPlan::ToString() const {
  std::string s = "Aggregation {";
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
  o << Indent(indent) << ToString() << "\n";
  child_->Dump(o, indent + 2);
}

Schema AggregationPlan::GenerateSchema() const {
  std::vector<Column> columns;
  for (const auto& agg : aggregates_) {
    const auto& expression = agg.expression->AsAggregateExpression();
    ValueType type = ValueType::kInt64;
    if (expression.GetType() == AggregationType::kAvg) {
      type = ValueType::kDouble;
    } else if (expression.GetType() != AggregationType::kCount) {
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

}  // namespace tinylamb
