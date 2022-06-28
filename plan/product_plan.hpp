//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression.hpp"
#include "plan/plan.hpp"

namespace tinylamb {
class ExecutorBase;

class ProductPlan : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<slot_t> left_cols, Plan right_src,
              std::vector<slot_t> right_cols);
  ProductPlan(Plan left_src, Plan right_src);

  ~ProductPlan() override = default;
  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Schema& GetSchema() const override;

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan left_src_;
  Plan right_src_;
  std::vector<slot_t> left_cols_;
  std::vector<slot_t> right_cols_;
  Schema output_schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
