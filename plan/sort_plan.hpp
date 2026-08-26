/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_SORT_PLAN_HPP
#define TINYLAMB_SORT_PLAN_HPP

#include <cstddef>
#include <vector>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Physical Sort: wraps a child plan and guarantees that its output is sorted
// by the specified keys.  The engine safety net also adds a SortExecutor when
// the child does not satisfy the required ordering; this plan node makes the
// sort explicit so the optimizer can cost it and avoid double-applying.
//
// IsOrderedBy returns true when the requested ordering is a prefix of the
// plan's own sort keys, enabling downstream Top-N and limit pushdown.
class SortPlan final : public PlanBase {
 public:
  SortPlan(Plan src, std::vector<Expression> keys,
           std::vector<bool> ascending)
      : src_(std::move(src)),
        keys_(std::move(keys)),
        ascending_(std::move(ascending)) {}
  SortPlan(const SortPlan&) = delete;
  SortPlan(SortPlan&&) = delete;
  SortPlan& operator=(const SortPlan&) = delete;
  SortPlan& operator=(SortPlan&&) = delete;
  ~SortPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return src_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return src_->GetStats();
  }
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  // True when the requested ordering is already satisfied: either the child
  // delivers it or our own sort keys cover it.
  [[nodiscard]] bool IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan src_;
  std::vector<Expression> keys_;
  std::vector<bool> ascending_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SORT_PLAN_HPP
