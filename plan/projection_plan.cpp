//
// Created by kumagi on 2022/03/01.
//

#include "plan/projection_plan.hpp"

#include <utility>

#include "executor/named_expression.hpp"
#include "executor/projection.hpp"
#include "expression/column_value.hpp"
#include "type/schema.hpp"

namespace tinylamb {

ProjectionPlan::ProjectionPlan(Plan src,
                               std::vector<NamedExpression> project_columns)
    : src_(std::move(src)), columns_(std::move(project_columns)) {}

std::unique_ptr<ExecutorBase> ProjectionPlan::EmitExecutor(
    TransactionContext& ctx) const {
  return std::make_unique<Projection>(columns_, src_.GetSchema(ctx),
                                      src_.EmitExecutor(ctx));
}
Schema ProjectionPlan::GetSchema(TransactionContext& ctx) const {
  Schema original_schema = src_.GetSchema(ctx);

  std::vector<Column> cols;
  cols.reserve(columns_.size());
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (!columns_[i].name.empty()) {
      cols.emplace_back(columns_[i].name);
      continue;
    }
    if (columns_[i].expression.Type() == TypeTag::kColumnValue) {
      auto* cv = dynamic_cast<ColumnValue*>(columns_[i].expression.exp_.get());
      cols.emplace_back(cv->col_name_);
      continue;
    }
    cols.emplace_back("$col" + std::to_string(i));
  }

  return {"", cols};
}

int ProjectionPlan::AccessRowCount(TransactionContext& ctx) const {
  return src_.EmitRowCount(ctx);
}

int ProjectionPlan::EmitRowCount(TransactionContext& ctx) const {
  return src_.EmitRowCount(ctx);
}

void ProjectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Project: {";
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (0 < i) o << ", ";
    o << columns_[i];
  }
  o << "}\n" << Indent(indent + 2);
  src_.Dump(o, indent + 2);
}
}  // namespace tinylamb