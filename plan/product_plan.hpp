//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {
class ExecutorBase;

class ProductPlan : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols);
  ProductPlan(Plan left_src, Plan right_src);
  ProductPlan(const ProductPlan&) = delete;
  ProductPlan(ProductPlan&&) = delete;
  ProductPlan& operator=(const ProductPlan&) = delete;
  ProductPlan& operator=(ProductPlan&&) = delete;
  ~ProductPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan left_src_;
  Plan right_src_;
  std::vector<ColumnName> left_cols_;
  std::vector<ColumnName> right_cols_;
  Schema output_schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
