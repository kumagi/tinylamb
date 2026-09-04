/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/unnest_plan.hpp"

#include <cstddef>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

TableStatistics MakeStats(const Schema& schema, size_t rows) {
  std::vector<ColumnStats> columns;
  columns.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    columns.emplace_back(schema.GetColumn(i).Type());
  }
  TableStatistics stats(schema);
  stats.Assign(rows, std::move(columns));
  return stats;
}

Schema BuildUnnestSchema(const Plan& child, const std::string& alias,
                         const std::string& offset_alias, Schema schema) {
  if (schema.ColumnCount() > 0) {
    if (child && child->GetSchema().ColumnCount() > 0 &&
        schema.Offset(child->GetSchema().GetColumn(0).Name()) == -1) {
      return child->GetSchema() + schema;
    }
    return schema;
  }
  std::vector<Column> cols;
  const std::string rel_name = alias.empty() ? "unnest" : alias;
  cols.emplace_back(ColumnName(rel_name, rel_name), ValueType::kNull);
  if (!offset_alias.empty()) {
    cols.emplace_back(ColumnName(rel_name, offset_alias), ValueType::kInt64);
  }
  Schema unnest_schema(rel_name, std::move(cols));
  if (child && child->GetSchema().ColumnCount() > 0) {
    return child->GetSchema() + unnest_schema;
  }
  return unnest_schema;
}

}  // namespace

UnnestPlan::UnnestPlan(Plan child, Expression unnest_expr, std::string alias,
                       std::string offset_alias, Schema schema)
    : child_(std::move(child)),
      unnest_expr_(std::move(unnest_expr)),
      alias_(std::move(alias)),
      offset_alias_(std::move(offset_alias)),
      schema_(
          BuildUnnestSchema(child_, alias_, offset_alias_, std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

void UnnestPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent)
         << "UnnestPlan: " << (unnest_expr_ ? unnest_expr_->ToString() : "");
  if (!alias_.empty()) {
    output << " AS " << alias_;
  }
  if (!offset_alias_.empty()) {
    output << " WITH OFFSET " << offset_alias_;
  }
  output << "\n";
  if (child_) {
    child_->Dump(output, indent + 2);
  }
}

std::string UnnestPlan::ToString() const {
  std::ostringstream ss;
  Dump(ss, 0);
  return ss.str();
}

}  // namespace tinylamb
