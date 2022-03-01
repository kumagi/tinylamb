//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_PLAN_HPP
#define TINYLAMB_PLAN_HPP
#include <memory>
#include <string_view>

#include "plan/plan_base.hpp"

namespace tinylamb {
class TransactionContext;
class ExecutorBase;
class Expression;

class Plan {
  explicit Plan(PlanBase* plan) : plan_(plan) {}

 public:
  static Plan FullScan(std::string_view table_name);
  static Plan Product(Plan left_src, std::vector<size_t> left_cols,
                      Plan right_src, std::vector<size_t> right_cols);
  static Plan Product(Plan left_src, Plan right_src);
  static Plan Projection(Plan src, const std::vector<size_t>& cols);
  static Plan Selection(Plan src, Expression exp);

  std::unique_ptr<ExecutorBase> EmitExecutor(TransactionContext& ctx) const {
    return plan_->EmitExecutor(ctx);
  }
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const {
    return plan_->GetSchema(ctx);
  }
  void Dump(std::ostream& o, int indent) const { plan_->Dump(o, indent); }

 private:
  std::shared_ptr<PlanBase> plan_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_HPP
