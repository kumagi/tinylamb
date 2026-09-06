/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_INDEX_SKIP_SCAN_PLAN_HPP
#define TINYLAMB_PLAN_INDEX_SKIP_SCAN_PLAN_HPP

#include <memory>
#include <ostream>

#include "plan/plan.hpp"

namespace tinylamb {

// Skip-scan access path over a secondary index.  The scan navigates the
// index by seeking to range boundaries instead of reading the table; for a
// constrained leading column it degenerates to a positioned range scan, and
// the node marks the access path family that can also skip unconstrained
// leading prefixes.  Delegates execution to the wrapped index scan plan.
class IndexSkipScanPlan final : public PlanBase {
 public:
  explicit IndexSkipScanPlan(Plan inner) : inner_(std::move(inner)) {}
  IndexSkipScanPlan(const IndexSkipScanPlan&) = delete;
  IndexSkipScanPlan(IndexSkipScanPlan&&) = delete;
  IndexSkipScanPlan& operator=(const IndexSkipScanPlan&) = delete;
  IndexSkipScanPlan& operator=(IndexSkipScanPlan&&) = delete;
  ~IndexSkipScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;
  [[nodiscard]] const Table* ScanSource() const override {
    return inner_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return inner_->GetSchema();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return inner_->GetStats();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return inner_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return inner_->EmitRowCount();
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override {
    return inner_->IsOrderedBy(expressions, ascending);
  }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override {
    return inner_->IsOrderedBy(expressions, ascending, nulls_first);
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan inner_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_INDEX_SKIP_SCAN_PLAN_HPP
