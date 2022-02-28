//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_SELECTION_PLAN_HPP
#define TINYLAMB_SELECTION_PLAN_HPP

#include "plan/plan_base.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class PlanBase;
class ExpressionBase;

class SelectionPlan : public PlanBase {
 public:
  ~SelectionPlan() override = default;
  SelectionPlan(std::unique_ptr<PlanBase>&& src,
                std::shared_ptr<ExpressionBase> exp)
      : src_(std::move(src)), exp_(exp) {}
  std::unique_ptr<ExecutorBase> EmitExecutor(Transaction& txn) const override;
  [[nodiscard]] Schema GetSchema() const override;

 private:
  std::unique_ptr<PlanBase> src_;
  std::shared_ptr<ExpressionBase> exp_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_SELECTION_PLAN_HPP
