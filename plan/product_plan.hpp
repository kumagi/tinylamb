//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression_base.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {

class ProductPlan : public PlanBase {
 public:
  ~ProductPlan() override = default;
  ProductPlan(std::unique_ptr<PlanBase>&& left_src,
              std::vector<size_t> left_cols,
              std::unique_ptr<PlanBase>&& right_src,
              std::vector<size_t> right_cols)
      : left_src_(std::move(left_src)),
        right_src_(std::move(right_src)),
        left_cols_(std::move(left_cols)),
        right_cols_(std::move(right_cols)) {}
  std::unique_ptr<ExecutorBase> EmitExecutor(Transaction& txn) const override;
  [[nodiscard]] Schema GetSchema() const override;

 public:
  std::unique_ptr<PlanBase> left_src_;
  std::unique_ptr<PlanBase> right_src_;
  std::vector<size_t> left_cols_;
  std::vector<size_t> right_cols_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
