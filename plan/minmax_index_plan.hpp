/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MINMAX_INDEX_PLAN_HPP
#define TINYLAMB_MINMAX_INDEX_PLAN_HPP

#include <cstddef>
#include <string>

#include "expression/aggregate_expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class MinMaxIndexPlan final : public PlanBase {
 public:
  MinMaxIndexPlan(Plan child, NamedExpression aggregate, size_t value_slot,
                  bool reverse);

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override { return 1; }
  [[nodiscard]] size_t EmitRowCount() const override { return 1; }
  [[nodiscard]] size_t ValueSlot() const { return value_slot_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  NamedExpression aggregate_;
  size_t value_slot_;
  bool reverse_;
  Schema schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_MINMAX_INDEX_PLAN_HPP
