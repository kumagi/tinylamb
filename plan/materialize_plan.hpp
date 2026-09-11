/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_MATERIALIZE_PLAN_HPP
#define TINYLAMB_PLAN_MATERIALIZE_PLAN_HPP

#include <utility>

#include "plan/plan.hpp"

namespace tinylamb {

// Caches the child output for multi-pass consumers (CTE references scanned
// twice, recurring work-table reads). Row-preserving and order-preserving:
// the executor replays cached rows in insertion order.
class MaterializePlan final : public PlanBase {
 public:
  explicit MaterializePlan(Plan child) : child_(std::move(child)) {}

  Executor EmitExecutor(TransactionContext& ctx) const override;
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
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override {
    return child_->IsOrderedBy(expressions, ascending, nulls_first);
  }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override { return "Materialize"; }

 private:
  Plan child_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_MATERIALIZE_PLAN_HPP
