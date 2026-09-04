/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "plan/apply_plan.hpp"

#include <cstddef>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

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

Schema BuildApplySchema(const Plan& child, const Plan& inner_child,
                        const std::string& alias, JoinKind kind,
                        Schema declared_schema) {
  (void)alias;
  if (declared_schema.ColumnCount() > 0) {
    if (child && child->GetSchema().ColumnCount() > 0 &&
        declared_schema.Offset(child->GetSchema().GetColumn(0).Name()) == -1) {
      if (kind == JoinKind::kSemi || kind == JoinKind::kAnti) {
        return child->GetSchema();
      }
      return child->GetSchema() + declared_schema;
    }
    return declared_schema;
  }
  if (!child) {
    return declared_schema;
  }
  if (kind == JoinKind::kSemi || kind == JoinKind::kAnti) {
    return child->GetSchema();
  }
  if (inner_child && inner_child->GetSchema().ColumnCount() > 0) {
    return child->GetSchema() + inner_child->GetSchema();
  }
  return child->GetSchema();
}

}  // namespace

ApplyPlan::ApplyPlan(Plan child,
                     std::shared_ptr<const SelectStatement> inner_query,
                     std::string alias, Expression predicate, JoinKind kind,
                     Schema schema)
    : child_(std::move(child)),
      inner_query_(std::move(inner_query)),
      alias_(std::move(alias)),
      predicate_(std::move(predicate)),
      kind_(kind),
      schema_(BuildApplySchema(child_, inner_child_, alias_, kind_,
                               std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

ApplyPlan::ApplyPlan(Plan child, Plan inner_child, Expression predicate,
                     JoinKind kind, Schema schema)
    : child_(std::move(child)),
      inner_child_(std::move(inner_child)),
      predicate_(std::move(predicate)),
      kind_(kind),
      schema_(BuildApplySchema(child_, inner_child_, alias_, kind_,
                               std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

ApplyPlan::ApplyPlan(Plan child, Plan inner_child,
                     std::shared_ptr<const SelectStatement> inner_query,
                     std::string alias, Expression predicate, JoinKind kind,
                     Schema schema)
    : child_(std::move(child)),
      inner_child_(std::move(inner_child)),
      inner_query_(std::move(inner_query)),
      alias_(std::move(alias)),
      predicate_(std::move(predicate)),
      kind_(kind),
      schema_(BuildApplySchema(child_, inner_child_, alias_, kind_,
                               std::move(schema))),
      stats_(MakeStats(schema_, (child_ ? child_->EmitRowCount() : 1) * 10)) {}

void ApplyPlan::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "ApplyPlan: [kind: " << static_cast<int>(kind_)
         << ", alias: " << alias_ << "]";
  if (predicate_) {
    output << " ON " << predicate_->ToString();
  }
  output << " (schema: " << schema_ << ")\n";
  if (child_) {
    child_->Dump(output, indent + 2);
  }
  if (inner_child_) {
    inner_child_->Dump(output, indent + 2);
  }
}

std::string ApplyPlan::ToString() const {
  std::ostringstream ss;
  Dump(ss, 0);
  return ss.str();
}

}  // namespace tinylamb
