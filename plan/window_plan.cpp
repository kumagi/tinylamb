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

WindowPlan::WindowPlan(Plan child, std::vector<NamedExpression> window_outputs)
    : child_(std::move(child)),
      window_outputs_(std::move(window_outputs)),
      schema_(BuildWindowSchema(child_ ? child_->GetSchema() : Schema(),
                                window_outputs_)),
      stats_(MakeWindowStats(schema_, child_ ? child_->EmitRowCount() : 0)) {}

bool WindowPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                             const std::vector<bool>& ascending) const {
  if (child_ == nullptr) {
    return expressions.empty();
  }
  return child_->IsOrderedBy(expressions, ascending);
}

void WindowPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "Window: [";
  for (size_t i = 0; i < window_outputs_.size(); ++i) {
    if (i > 0) {
      output << ", ";
    }
    output << window_outputs_[i].name;
  }
  output << "]\n" << Indent(static_cast<size_t>(indent) + 2);
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
