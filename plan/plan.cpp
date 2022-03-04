//
// Created by kumagi on 2022/03/02.
//

#include "plan.hpp"

#include <utility>

#include "executor/named_expression.hpp"
#include "expression/expression.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"

namespace tinylamb {

Plan Plan::FullScan(std::string_view table_name) {
  return Plan(new FullScanPlan(table_name));
}

Plan Plan::Product(Plan left_src, std::vector<size_t> left_cols, Plan right_src,
                   std::vector<size_t> right_cols) {
  return Plan(new ProductPlan(std::move(left_src), std::move(left_cols),
                              std::move(right_src), std::move(right_cols)));
}

Plan Plan::Product(Plan left_src, Plan right_src) {
  return Plan(new ProductPlan(std::move(left_src), std::move(right_src)));
}

Plan Plan::Projection(Plan src, std::vector<NamedExpression> cols) {
  return Plan(new ProjectionPlan(std::move(src), std::move(cols)));
}

Plan Plan::Selection(Plan src, Expression exp) {
  return Plan(new SelectionPlan(std::move(src), std::move(exp)));
}

int Plan::AccessRowCount(TransactionContext& ctx) const {
  return plan_->AccessRowCount(ctx);
}

int Plan::EmitRowCount(TransactionContext& ctx) const {
  return plan_->EmitRowCount(ctx);
}

}  // namespace tinylamb