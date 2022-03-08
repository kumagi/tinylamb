//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_PLAN_HPP
#define TINYLAMB_PLAN_HPP
#include <memory>
#include <string_view>

#include "executor/named_expression.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {
class TableStatistics;
class TransactionContext;
class ExecutorBase;
class Expression;

class Plan {
  explicit Plan(PlanBase* plan) : plan_(plan) {}

 public:
  static Plan FullScan(std::string_view table_name, TableStatistics ts);
  static Plan Product(Plan left_src, std::vector<size_t> left_cols,
                      Plan right_src, std::vector<size_t> right_cols);
  static Plan Product(Plan left_src, Plan right_src);
  static Plan Projection(Plan src, std::vector<NamedExpression> cols);
  static Plan Selection(Plan src, Expression exp, TableStatistics ts);

  std::unique_ptr<ExecutorBase> EmitExecutor(TransactionContext& ctx) const {
    return plan_->EmitExecutor(ctx);
  }
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const {
    return plan_->GetSchema(ctx);
  }
  [[nodiscard]] size_t AccessRowCount(TransactionContext& txn) const;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& txn) const;
  void Dump(std::ostream& o, int indent) const { plan_->Dump(o, indent); }
  friend std::ostream& operator<<(std::ostream& o, const Plan& p) {
    p.Dump(o, 0);
    return o;
  }

 private:
  std::shared_ptr<PlanBase> plan_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_HPP
