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
  SelectionPlan(Plan src, Expression exp, TableStatistics stat)
      : src_(std::move(src)), exp_(std::move(exp)), stats_(std::move(stat)) {}
  SelectionPlan(const SelectionPlan&) = delete;
  SelectionPlan(SelectionPlan&&) = delete;
  SelectionPlan& operator=(const SelectionPlan&) = delete;
  SelectionPlan& operator=(SelectionPlan&&) = delete;
  ~SelectionPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return src_->ScanSource();
  };
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan src_;
  Expression exp_;
  TableStatistics stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_SELECTION_PLAN_HPP
