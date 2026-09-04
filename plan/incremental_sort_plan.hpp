/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_INCREMENTAL_SORT_PLAN_HPP
#define TINYLAMB_INCREMENTAL_SORT_PLAN_HPP

#include <optional>
#include <string>
#include <vector>

#include "plan/plan.hpp"
#include "plan/sort_plan.hpp"

namespace tinylamb {

class IncrementalSortPlan final : public PlanBase {
 public:
  IncrementalSortPlan(Plan child, std::vector<SortKey> prefix_keys,
                      std::vector<SortKey> suffix_keys);

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
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<SortKey> prefix_keys_;
  std::vector<SortKey> suffix_keys_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INCREMENTAL_SORT_PLAN_HPP
