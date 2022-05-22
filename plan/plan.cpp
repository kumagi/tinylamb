//
// Created by kumagi on 2022/03/14.
//
#include "plan/full_scan_plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/selection_plan.hpp"

namespace tinylamb {

Plan NewFullScanPlan(std::string_view table_name, const TableStatistics& ts) {
  return std::make_shared<FullScanPlan>(table_name, ts);
}

Plan NewProductPlan(const Plan& left_src, std::vector<slot_t> left_cols,
                    const Plan& right_src, std::vector<slot_t> right_cols) {
  return std::make_shared<ProductPlan>(left_src, std::move(left_cols),
                                       right_src, std::move(right_cols));
}

Plan NewSelectionPlan(const Plan& src, const Expression& exp,
                      const TableStatistics& stat) {
  return std::make_shared<SelectionPlan>(src, exp, stat);
}

Plan NewProductPlan(const Plan& left_src, const Plan& right_src) {
  return std::make_shared<ProductPlan>(left_src, right_src);
}

Plan NewProjectionPlan(const Plan& src,
                       std::vector<NamedExpression> project_columns) {
  return std::make_shared<ProjectionPlan>(src, std::move(project_columns));
}

Plan NewProjectionPlan(const Plan& src,
                       const std::vector<std::string>& project_columns) {
  return std::make_shared<ProjectionPlan>(src, project_columns);
}

}  // namespace tinylamb