/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "plan/window_plan.hpp"

#include <cstddef>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Window outputs are Null-typed: the executor's column vectors adopt the
// first non-null computed type (DataChunk's inferred storage). The window
// call's own ResultType is a placeholder (always VARCHAR) that must not leak
// into schemas; dynamic evaluation stays the authority.
Schema BuildWindowSchema(const Schema& input,
                         const std::vector<NamedExpression>& outputs) {
  std::vector<Column> columns;
  columns.reserve(input.ColumnCount() + outputs.size());
  for (size_t i = 0; i < input.ColumnCount(); ++i) {
    columns.push_back(input.GetColumn(i));
  }
  for (const NamedExpression& item : outputs) {
    columns.emplace_back(ColumnName("", item.name), ValueType::kNull);
  }
  return {"", std::move(columns)};
}

TableStatistics MakeWindowStats(const Schema& schema, size_t rows) {
  std::vector<ColumnStats> columns;
  columns.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    columns.emplace_back(schema.GetColumn(i).Type());
  }
  TableStatistics stats(schema);
  stats.Assign(rows, std::move(columns));
  return stats;
}

}  // namespace

WindowPlan::WindowPlan(Plan child, std::vector<NamedExpression> window_outputs,
                       WindowMemoryPolicy memory)
    : child_(std::move(child)),
      window_outputs_(std::move(window_outputs)),
      schema_(BuildWindowSchema(child_ ? child_->GetSchema() : Schema(),
                                window_outputs_)),
      stats_(MakeWindowStats(schema_, child_ ? child_->EmitRowCount() : 0)),
      memory_(memory) {}

bool WindowPlan::CallsShareOnePartitionKey() const {
  // No call means nothing to spill for: the operator would copy the child
  // through, so the buffered shape (and its ordering claim) is the honest one.
  if (window_outputs_.empty()) {
    return false;
  }
  const std::vector<Expression>* shared = nullptr;
  for (const NamedExpression& item : window_outputs_) {
    if (item.expression == nullptr ||
        item.expression->Type() != TypeTag::kWindowFunctionExp) {
      return false;
    }
    const std::vector<Expression>& partition_by =
        item.expression->AsWindowFunctionCallExpression().partition_by;
    if (shared == nullptr) {
      shared = &partition_by;
      continue;
    }
    // Same expression list, not merely the same width: a row's spill file is
    // chosen from this key, so two calls may only share it if their partitions
    // are the same sets of rows. Two calls with different ORDER BY frames are
    // fine -- they sort inside the same partition.
    if (partition_by.size() != shared->size()) {
      return false;
    }
    for (size_t i = 0; i < partition_by.size(); ++i) {
      const std::string left = partition_by[i] == nullptr
                                   ? std::string()
                                   : partition_by[i]->ToString();
      const std::string right =
          (*shared)[i] == nullptr ? std::string() : (*shared)[i]->ToString();
      if (left != right) {
        return false;
      }
    }
  }
  return true;
}

bool WindowPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                             const std::vector<bool>& ascending) const {
  return IsOrderedBy(expressions, ascending, {});
}

bool WindowPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  // A spill-capable shape emits one spill file at a time, so it cannot promise
  // the child's order. Decided here, before the runtime row count is known: a
  // plan that might spill must never leave a claim that it might violate.
  if (SpillCapable()) {
    return expressions.empty() && ascending.empty();
  }
  if (child_ == nullptr) {
    return expressions.empty() && ascending.empty();
  }
  return child_->IsOrderedBy(expressions, ascending, nulls_first);
}

void WindowPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "Window: [";
  for (size_t i = 0; i < window_outputs_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << window_outputs_[i].name;
  }
  output << "]";
  // Only the spill shape is worth naming; the buffered one keeps the historical
  // rendering every EXPLAIN golden test knows.
  if (SpillCapable()) {
    output << " (spill: partitions=" << memory_.spill_partition_count
           << ", budget=" << memory_.memory_budget_bytes << ")";
  }
  output << "\n" << Indent(static_cast<size_t>(indent) + 2);
  if (child_ != nullptr) {
    child_->Dump(output, indent + 2);
  }
}

std::string WindowPlan::ToString() const {
  std::ostringstream output;
  output << "Window: [";
  for (size_t i = 0; i < window_outputs_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << window_outputs_[i].name;
  }
  output << "]";
  return output.str();
}

}  // namespace tinylamb
