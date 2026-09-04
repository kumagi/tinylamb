/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/recursive_cte_plan.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
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

Schema BuildRecursiveCteSchema(
    const Plan& child, const std::string& cte_name,
    const std::shared_ptr<const SelectStatement>& body,
    const std::optional<RecursiveDepthSpec>& depth_spec,
    Schema declared_schema) {
  if (declared_schema.ColumnCount() > 0) {
    return declared_schema;
  }
  if (child && child->GetSchema().ColumnCount() > 0) {
    return child->GetSchema();
  }
  if (body) {
    std::vector<Column> cols;
    for (const NamedExpression& item : body->SelectList()) {
      cols.emplace_back(ColumnName(cte_name, item.name), ValueType::kNull);
    }
    if (depth_spec.has_value()) {
      cols.emplace_back(ColumnName(cte_name, depth_spec->column),
                        ValueType::kInt64);
    }
    return {cte_name, std::move(cols)};
  }
  return declared_schema;
}

}  // namespace

RecursiveCtePlan::RecursiveCtePlan(Plan child, std::string cte_name,
                                   std::shared_ptr<const SelectStatement> body,
                                   std::optional<RecursiveDepthSpec> depth_spec,
                                   Schema schema)
    : child_(std::move(child)),
      cte_name_(std::move(cte_name)),
      body_(std::move(body)),
      depth_spec_(std::move(depth_spec)),
      schema_(BuildRecursiveCteSchema(child_, cte_name_, body_, depth_spec_,
                                      std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

RecursiveCtePlan::RecursiveCtePlan(Plan child, Plan recursive_child,
                                   std::string cte_name,
                                   std::shared_ptr<const SelectStatement> body,
                                   std::optional<RecursiveDepthSpec> depth_spec,
                                   Schema schema)
    : child_(std::move(child)),
      recursive_child_(std::move(recursive_child)),
      cte_name_(std::move(cte_name)),
      body_(std::move(body)),
      depth_spec_(std::move(depth_spec)),
      schema_(BuildRecursiveCteSchema(child_, cte_name_, body_, depth_spec_,
                                      std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

void RecursiveCtePlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "RecursiveCtePlan: [" << cte_name_ << "]";
  if (depth_spec_.has_value()) {
    output << " (depth_column: " << depth_spec_->column << ", range: ["
           << depth_spec_->lower << ", " << depth_spec_->upper << "])";
  }
  output << " (schema: " << schema_ << ")\n";
  if (child_) {
    child_->Dump(output, indent + 2);
  }
  if (recursive_child_) {
    recursive_child_->Dump(output, indent + 2);
  }
}

std::string RecursiveCtePlan::ToString() const {
  std::ostringstream ss;
  Dump(ss, 0);
  return ss.str();
}

}  // namespace tinylamb
