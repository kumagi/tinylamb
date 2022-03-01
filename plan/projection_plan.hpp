//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <vector>

#include "plan.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
  ProjectionPlan(Plan src, std::vector<size_t> project_columns);

 public:
  ~ProjectionPlan() override = default;

  std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Plan;

  Plan src_;
  std::vector<size_t> project_columns_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
