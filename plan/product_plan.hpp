//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression_base.hpp"
#include "plan/plan.hpp"

namespace tinylamb {
class Plan;

class ProductPlan : public PlanBase {
  ProductPlan(Plan left_src, std::vector<size_t> left_cols, Plan right_src,
              std::vector<size_t> right_cols);
  ProductPlan(Plan left_src, Plan right_src);

 public:
  ~ProductPlan() override = default;
  std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  void Dump(std::ostream& o, int indent) const override;

 public:
  friend class Plan;
  Plan left_src_;
  Plan right_src_;
  std::vector<size_t> left_cols_;
  std::vector<size_t> right_cols_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
