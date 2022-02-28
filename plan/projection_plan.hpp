//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <vector>

#include "plan/plan_base.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
 public:
  ~ProjectionPlan() override = default;
  ProjectionPlan(std::unique_ptr<PlanBase> src,
                 std::vector<size_t> project_columns);
  std::unique_ptr<ExecutorBase> EmitExecutor(Transaction& txn) const override;
  [[nodiscard]] Schema GetSchema() const override;

 private:
  std::unique_ptr<PlanBase> src_;
  std::vector<size_t> project_columns_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
