/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_PLAN_EMPTY_PLAN_HPP
#define TINYLAMB_PLAN_EMPTY_PLAN_HPP

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// A zero-row alternative that preserves the child's output schema. The child
// is retained for metadata but is never opened by EmitExecutor.
class EmptyPlan final : public PlanBase {
 public:
  explicit EmptyPlan(Plan child);
  EmptyPlan(const EmptyPlan&) = delete;
  EmptyPlan(EmptyPlan&&) = delete;
  EmptyPlan& operator=(const EmptyPlan&) = delete;
  EmptyPlan& operator=(EmptyPlan&&) = delete;
  ~EmptyPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return child_->GetSchema();
  }
  [[nodiscard]] size_t AccessRowCount() const override { return 0; }
  [[nodiscard]] size_t EmitRowCount() const override { return 0; }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& /*expressions*/,
      const std::vector<bool>& /*ascending*/) const override {
    // An empty relation satisfies every ordering requirement.
    return true;
  }
  [[nodiscard]] bool EnforcesLimit(size_t /*limit_count*/,
                                   size_t /*limit_offset*/) const override {
    return true;
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_EMPTY_PLAN_HPP
