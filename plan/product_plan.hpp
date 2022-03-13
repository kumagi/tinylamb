//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression_base.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {
class ExecutorBase;

class ProductPlan : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<size_t> left_cols, Plan right_src,
              std::vector<size_t> right_cols);
  ProductPlan(Plan left_src, Plan right_src);

  ~ProductPlan() override = default;
  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  [[nodiscard]] size_t AccessRowCount(TransactionContext& ctx) const override;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& ctx) const override;
  void Dump(std::ostream& o, int indent) const override;

 public:
  Plan left_src_;
  Plan right_src_;
  std::vector<size_t> left_cols_;
  std::vector<size_t> right_cols_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
