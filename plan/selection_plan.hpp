//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_SELECTION_PLAN_HPP
#define TINYLAMB_SELECTION_PLAN_HPP

#include <utility>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SelectionPlan : public PlanBase {
 public:
  SelectionPlan(Plan src, Expression exp, const TableStatistics& stat)
      : src_(std::move(src)), exp_(exp), stats_(stat) {}

  ~SelectionPlan() override = default;
  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Schema& GetSchema() const override;

  [[nodiscard]] size_t AccessRowCount(TransactionContext& txn) const override;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& txn) const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan src_;
  Expression exp_;
  const TableStatistics& stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_SELECTION_PLAN_HPP
