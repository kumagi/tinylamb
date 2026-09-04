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

#include "plan/projection_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"

namespace tinylamb {
namespace {

// Best-effort static result type for a projection expression.  Fails soft
// (kNull): dynamic evaluation stays the authority, and the declared type is
// only metadata for downstream planners.
ValueType StaticResultValueType(const Expression& expression,
                                const Schema& schema) {
  try {
    const Type type = expression->ResultType(schema);
    switch (type.GetType()) {
      case TypeTag::kInteger:
      case TypeTag::kBigInt:
        return ValueType::kInt64;
      case TypeTag::kDouble:
        return ValueType::kDouble;
      case TypeTag::kVarChar:
        return ValueType::kVarChar;
      case TypeTag::kDate:
        return ValueType::kDate;
      case TypeTag::kArray:
        return ValueType::kArray;
      default:
        return ValueType::kNull;
    }
  } catch (const std::exception&) {
    return ValueType::kNull;
  }
}

}  // namespace

ProjectionPlan::ProjectionPlan(Plan src,
                               std::vector<NamedExpression> project_columns)
    : src_(std::move(src)),
      columns_(std::move(project_columns)),
      output_schema_(CalcSchema()),
      stats_(src_->GetStats()) {}

ProjectionPlan::ProjectionPlan(Plan src,
                               const std::vector<ColumnName>& project_columns)
    : src_(std::move(src)), stats_(src_->GetStats()) {
  columns_.reserve(project_columns.size());
  for (const auto& col : project_columns) {
    columns_.emplace_back(col);
  }
  output_schema_ = CalcSchema();
}

Schema ProjectionPlan::CalcSchema() const {
  Schema original_schema = src_->GetSchema();

  std::vector<Column> cols;
  cols.reserve(columns_.size());
  for (size_t i = 0; i < columns_.size(); ++i) {
    const NamedExpression& item = columns_[i];
    // A column reference keeps the source column verbatim (type, unsigned
    // flag and constraints) under its output name.  Dropping the type here
    // made every projected column kNull, and downstream consumers that
    // specialize on the declared type (set-operation schemas, typed
    // aggregates, planning heuristics) silently took their generic path.
    const bool is_column_ref =
        item.expression && item.expression->Type() == TypeTag::kColumnValue;
    const ColumnName column_name =
        is_column_ref ? item.expression->AsColumnValue().GetColumnName()
                      : ColumnName();
    const int source_offset =
        is_column_ref ? original_schema.Offset(column_name) : -1;
    if (item.name.empty() && 0 <= source_offset) {
      cols.emplace_back(
          original_schema.GetColumn(static_cast<size_t>(source_offset)));
      continue;
    }
    if (!item.name.empty()) {
      Column out(item.name);
      if (0 <= source_offset) {
        const Column& source =
            original_schema.GetColumn(static_cast<size_t>(source_offset));
        out.SetType(source.Type());
        out.SetUnsigned(source.IsUnsigned());
      } else if (item.expression) {
        out.SetType(StaticResultValueType(item.expression, original_schema));
      }
      cols.emplace_back(std::move(out));
      continue;
    }
    if (is_column_ref) {
      if (0 <= source_offset) {
        Column source =
            original_schema.GetColumn(static_cast<size_t>(source_offset));
        source.Name() = column_name;
        cols.emplace_back(std::move(source));
        continue;
      }
      cols.emplace_back(column_name);
      continue;
    }
    Column fallback("$col" + std::to_string(i));
    if (item.expression) {
      fallback.SetType(StaticResultValueType(item.expression, original_schema));
    }
    cols.emplace_back(std::move(fallback));
  }
  return {"", cols};
}

// EmitExecutor lives in the relational factory
// (executor/relational_factory.cpp).

const Schema& ProjectionPlan::GetSchema() const { return output_schema_; }

size_t ProjectionPlan::AccessRowCount() const { return src_->AccessRowCount(); }

size_t ProjectionPlan::EmitRowCount() const { return src_->EmitRowCount(); }

bool ProjectionPlan::IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const {
  if (expressions.size() != ascending.size()) {
    return false;
  }
  std::vector<Expression> child_expressions;
  child_expressions.reserve(expressions.size());
  for (const Expression& expression : expressions) {
    Expression translated = expression;
    if (expression && expression->Type() == TypeTag::kColumnValue) {
      const std::string& name =
          expression->AsColumnValue().GetColumnName().name;
      for (const NamedExpression& column : columns_) {
        if (column.name == name) {
          translated = column.expression;
          break;
        }
      }
    }
    child_expressions.push_back(std::move(translated));
  }
  return src_->IsOrderedBy(child_expressions, ascending);
}

bool ProjectionPlan::IsOrderedBy(
    const std::vector<Expression>& expressions,
    const std::vector<bool>& ascending,
    const std::vector<std::optional<bool>>& nulls_first) const {
  if (expressions.size() != ascending.size()) {
    return false;
  }
  std::vector<Expression> child_expressions;
  child_expressions.reserve(expressions.size());
  for (const Expression& expression : expressions) {
    Expression translated = expression;
    if (expression && expression->Type() == TypeTag::kColumnValue) {
      const std::string& name =
          expression->AsColumnValue().GetColumnName().name;
      for (const NamedExpression& column : columns_) {
        if (column.name == name) {
          translated = column.expression;
          break;
        }
      }
    }
    child_expressions.push_back(std::move(translated));
  }
  // Renaming outputs never moves NULLs, so null placement passes through
  // positionally alongside the translated keys.
  return src_->IsOrderedBy(child_expressions, ascending, nulls_first);
}

void ProjectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Project: {";
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << columns_[i];
  }
  o << "} (estimated cost: " << AccessRowCount() << ")\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

std::string ProjectionPlan::ToString() const {
  std::string s = "Project: {";
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (0 < i) {
      s += ", ";
    }
    s += columns_[i].name;
  }
  s += "} (estimated cost: " + std::to_string(AccessRowCount()) + ")";
  return s;
}
}  // namespace tinylamb
