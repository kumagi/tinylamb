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

#include <memory>
#include <utility>
#include <vector>

#include "executor/aggregation.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

AggregationPlan::AggregationPlan(Plan child,
                                 std::vector<NamedExpression> aggregates)
    : child_(std::move(child)),
      aggregates_(std::move(aggregates)),
      schema_(GenerateSchema()) {}

const Schema& AggregationPlan::GetSchema() const { return schema_; }

Executor AggregationPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<AggregationExecutor>(child_->EmitExecutor(ctx),
                                               aggregates_);
}

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
  for (const auto& agg : aggregates_) {
    s += agg.name + ": " + agg.expression->ToString() + ", ";
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
    columns.emplace_back(agg.name, ValueType::kInt64);
  }
  return {"", columns};
}

}  // namespace tinylamb
