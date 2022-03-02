//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <vector>

#include "executor/named_expression.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
  ProjectionPlan(Plan src, std::vector<NamedExpression> project_columns);

 public:
  ~ProjectionPlan() override = default;

  std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Plan;

  Plan src_;
  std::vector<NamedExpression> columns_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
