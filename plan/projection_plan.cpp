//
// Created by kumagi on 2022/03/01.
//

#include "plan/projection_plan.hpp"

#include "executor/projection.hpp"
#include "type/schema.hpp"

namespace tinylamb {

ProjectionPlan::ProjectionPlan(std::unique_ptr<PlanBase> src,
                               std::vector<size_t> project_columns)
    : src_(std::move(src)), project_columns_(std::move(project_columns)) {}

std::unique_ptr<ExecutorBase> ProjectionPlan::EmitExecutor(
    Transaction& txn) const {
  return std::make_unique<Projection>(project_columns_,
                                      src_->EmitExecutor(txn));
}
Schema ProjectionPlan::GetSchema() const {
  return src_->GetSchema().Extract(project_columns_);
}

}  // namespace tinylamb