/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_PLAN_SORT_PLAN_HPP
#define TINYLAMB_PLAN_SORT_PLAN_HPP

#include <optional>
#include <utility>
#include <vector>

#include "plan/plan.hpp"

namespace tinylamb {

struct SortKey {
  Expression expression;
  bool ascending{true};
  std::optional<bool> nulls_first;
};

class SortPlan final : public PlanBase {
 public:
  SortPlan(Plan child, std::vector<SortKey> keys)
      : child_(std::move(child)), keys_(std::move(keys)) {}
  SortPlan(const SortPlan&) = delete;
  SortPlan(SortPlan&&) = delete;
  SortPlan& operator=(const SortPlan&) = delete;
  SortPlan& operator=(SortPlan&&) = delete;
  ~SortPlan() override = default;

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
  // Sorting reorders but never adds or removes rows, so a limit below a
  // Sort still shapes the final output exactly.
  [[nodiscard]] bool EnforcesLimit(size_t limit_count,
                                   size_t limit_offset) const override {
    return child_->EnforcesLimit(limit_count, limit_offset);
  }

  [[nodiscard]] const std::vector<SortKey>& Keys() const { return keys_; }
  [[nodiscard]] const Plan& Child() const { return child_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<SortKey> keys_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_SORT_PLAN_HPP
