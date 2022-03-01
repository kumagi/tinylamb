//
// Created by kumagi on 2022/03/01.
//

#include "plan/projection_plan.hpp"

#include "executor/projection.hpp"
#include "type/schema.hpp"

namespace tinylamb {

ProjectionPlan::ProjectionPlan(Plan src, std::vector<size_t> project_columns)
    : src_(src), project_columns_(std::move(project_columns)) {}

std::unique_ptr<ExecutorBase> ProjectionPlan::EmitExecutor(
    TransactionContext& ctx) const {
  return std::make_unique<Projection>(project_columns_, src_.EmitExecutor(ctx));
}
Schema ProjectionPlan::GetSchema(TransactionContext& ctx) const {
  return src_.GetSchema(ctx).Extract(project_columns_);
}

void ProjectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Project: {";
  for (size_t i = 0; i < project_columns_.size(); ++i) {
    if (0 < i) o << ", ";
    o << project_columns_[i];
  }
  o << "}\n" << Indent(indent + 2);
  src_.Dump(o, indent + 2);
}
}  // namespace tinylamb