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
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

ValueType KeyValueType(const Expression& expression, const Schema& input) {
  if (!expression) {
    return ValueType::kNull;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const int offset =
        input.Offset(expression->AsColumnValue().GetColumnName());
    if (offset >= 0 && std::cmp_less(offset, input.ColumnCount())) {
      return input.GetColumn(static_cast<size_t>(offset)).Type();
    }
  }
  try {
    switch (expression->ResultType(input).GetType()) {
      case TypeTag::kInteger:
      case TypeTag::kBigInt:
        return ValueType::kInt64;
      case TypeTag::kDouble:
        return ValueType::kDouble;
      case TypeTag::kDate:
        return ValueType::kDate;
      case TypeTag::kArray:
        return ValueType::kArray;
      default:
        return ValueType::kVarChar;
    }
  } catch (...) {
    return ValueType::kVarChar;
  }
}

// The group-key order both ordered shapes deliver: ascending with NULLs first,
// which is the engine default for an ascending key. A requirement is answered
// only when it asks for exactly that, and only for a prefix of the key list —
// an ordered output also delivers any shorter prefix of its own order.
bool DeliversAscendingNullsFirst(
    const std::vector<NamedExpression>& keys,
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>* nulls_first) {
  if (keys.empty() || expressions.empty() || expressions.size() > keys.size() ||
      ascending.size() != expressions.size()) {
    return false;
  }
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (!ascending[i] || !keys[i].expression ||
        keys[i].expression->ToString() != expressions[i]->ToString()) {
      return false;
    }
    if (nulls_first != nullptr && i < nulls_first->size() &&
        !(*nulls_first)[i].value_or(true)) {
      return false;
    }
  }
  return true;
}

}  // namespace

AggregationPlan::AggregationPlan(Plan child,
                                 std::vector<NamedExpression> aggregates,
                                 AggregationStrategy strategy)
    : AggregationPlan(std::move(child),
                      AggregatePayload{.aggregates = std::move(aggregates)},
                      strategy) {}

AggregationPlan::AggregationPlan(Plan child, AggregatePayload payload,
                                 AggregationStrategy strategy)
    : child_(std::move(child)),
      group_keys_(std::move(payload.group_keys)),
      aggregates_(std::move(payload.aggregates)),
      having_(std::move(payload.having)),
      strategy_(strategy),
      memory_(payload.memory),
      estimated_groups_(payload.estimated_groups),
      schema_(GenerateSchema()),
      output_stats_(GenerateOutputStats()) {}

const Schema& AggregationPlan::GetSchema() const { return schema_; }

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp); each shape has its own body there.

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
  // The group count the rule estimated, never more than the rows that can feed
  // it. A scalar payload has exactly one group even over an empty input, so it
  // keeps its historical single-row estimate.
  const size_t input_rows = child_->EmitRowCount();
  size_t rows = 0;
  if (estimated_groups_ > 0.0) {
    // A group count can never exceed the rows that feed it, but a child whose
    // own estimate is zero carries no information here, so the rule's estimate
    // stands rather than collapsing to an empty output.
    const double capped =
        input_rows == 0
            ? estimated_groups_
            : std::min(estimated_groups_, static_cast<double>(input_rows));
    rows = static_cast<size_t>(capped);
  }
  if (rows == 0 && group_keys_.empty()) {
    rows = 1;
  }
  stats.Assign(rows, std::move(columns));
  return stats;
}

std::string AggregationPlan::StrategyName() const {
  switch (strategy_) {
    case AggregationStrategy::kSort:
      return "SortAggregate";
    case AggregationStrategy::kStream:
      return "StreamAggregate";
    case AggregationStrategy::kHash:
      break;
  }
  return "HashAggregate";
}

std::string AggregationPlan::GroupKeysToString() const {
  std::string s;
  for (size_t i = 0; i < group_keys_.size(); ++i) {
    if (i > 0) {
      s += ", ";
    }
    s += group_keys_[i].expression->ToString();
  }
  return s;
}

std::string AggregationPlan::MemorySuffix() const { return {}; }

std::string AggregationPlan::ToString() const {
  std::string s = StrategyName();
  if (!group_keys_.empty()) {
    // The name before the first colon is what the memo reports as the physical
    // operator, so the shape name stays colon free.
    s += ": [" + GroupKeysToString() + "]";
  }
  s += " {";
  bool first = true;
  for (const auto& agg : aggregates_) {
    if (!first) {
      s += ", ";
    }
    first = false;
    s += agg.name + ": " + agg.expression->ToString();
  }
  s += "}";
  s += MemorySuffix();
  return s;
}

void AggregationPlan::Dump(std::ostream& o, int indent) const {
  o << Indent(static_cast<size_t>(indent)) << ToString() << "\n";
  child_->Dump(o, indent + 2);
}

Schema AggregationPlan::GenerateSchema() const {
  std::vector<Column> columns;
  columns.reserve(group_keys_.size() + aggregates_.size());
  for (const auto& key : group_keys_) {
    columns.emplace_back(key.name,
                         KeyValueType(key.expression, child_->GetSchema()));
  }
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
  if (having_ && ContainsParameterSlot(having_)) {
    return true;
  }
  const bool key_slot =
      std::ranges::any_of(group_keys_, [](const NamedExpression& key) {
        return ContainsParameterSlot(key.expression);
      });
  if (key_slot) {
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
  for (NamedExpression& key : group_keys_) {
    if (!SlotizeInPlace(&key.expression, parameters, cursor, slots)) {
      return false;
    }
  }
  for (NamedExpression& aggregate : aggregates_) {
    if (!SlotizeInPlace(&aggregate.expression, parameters, cursor, slots)) {
      return false;
    }
  }
  return !having_ || SlotizeInPlace(&having_, parameters, cursor, slots);
}

Collation AggregationPlan::OrderCollation() const {
  // A shape that delivers an order compares its group keys left to right, so
  // the delivered ordering is case-insensitive as soon as one key asks for it
  // -- the same rule a SortPlan reports. Without keys there is no order at all.
  Collation resolved = Collation::kNone;
  for (const NamedExpression& key : group_keys_) {
    resolved =
        ResolveCollation(resolved, CollationOfExpression(key.expression));
  }
  return resolved;
}

// ------------------------------------------------------------- hash shape ---

std::string HashAggregatePlan::MemorySuffix() const {
  std::ostringstream output;
  output << " (hash table, budget=" << Memory().memory_budget_bytes
         << "B, groups=" << RowCapacity() << ")";
  return output.str();
}

// ------------------------------------------------------------- sort shape ---

std::string SortAggregatePlan::MemorySuffix() const {
  std::ostringstream output;
  output << " (sort runs, budget=" << Memory().memory_budget_bytes
         << "B, run_rows=" << SpillThresholdRows()
         << ", window=" << MergeWindowRows() << ")";
  return output.str();
}

bool SortAggregatePlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                    const std::vector<bool>& ascending) const {
  return DeliversAscendingNullsFirst(GroupKeys(), expressions, ascending,
                                     nullptr);
}

bool SortAggregatePlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  return DeliversAscendingNullsFirst(GroupKeys(), expressions, ascending,
                                     &nulls_first);
}

// ---------------------------------------------------------- stream shape ---

std::string StreamAggregatePlan::MemorySuffix() const {
  // One group is resident, so the shape has no group table and no spill path.
  return " (streaming, groups=1)";
}

bool StreamAggregatePlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending) const {
  return DeliversAscendingNullsFirst(GroupKeys(), expressions, ascending,
                                     nullptr);
}

bool StreamAggregatePlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  return DeliversAscendingNullsFirst(GroupKeys(), expressions, ascending,
                                     &nulls_first);
}

}  // namespace tinylamb
