/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_DISTINCT_PLAN_HPP
#define TINYLAMB_PLAN_DISTINCT_PLAN_HPP

#include <utility>

#include "plan/plan.hpp"

namespace tinylamb {

class DistinctPlan final : public PlanBase {
 public:
  explicit DistinctPlan(Plan child, std::vector<Expression> distinct_on = {})
      : child_(std::move(child)), distinct_on_(std::move(distinct_on)) {}

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return child_->GetSchema();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return child_->EmitRowCount();
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override {
    return child_->IsOrderedBy(expressions, ascending);
  }
  [[nodiscard]] const Plan& Child() const { return child_; }
  [[nodiscard]] const std::vector<Expression>& DistinctOn() const {
    return distinct_on_;
  }
  [[nodiscard]] bool HasDistinctOn() const { return !distinct_on_.empty(); }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<Expression> distinct_on_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_DISTINCT_PLAN_HPP
