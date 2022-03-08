//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_SELECTION_PLAN_HPP
#define TINYLAMB_SELECTION_PLAN_HPP

#include <utility>

#include "expression/expression.hpp"
#include "plan.hpp"
#include "plan/plan_base.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Plan;
class Expression;

class SelectionPlan : public PlanBase {
 public:
  ~SelectionPlan() override = default;
  SelectionPlan(Plan src, Expression exp, TableStatistics stat)
      : src_(std::move(src)), exp_(std::move(exp)), stats_(std::move(stat)) {}
  std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  [[nodiscard]] size_t AccessRowCount(TransactionContext& txn) const override;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& txn) const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  mutable Plan src_;
  mutable Expression exp_;
  TableStatistics stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_SELECTION_PLAN_HPP
