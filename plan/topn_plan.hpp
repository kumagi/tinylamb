/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TOPN_PLAN_HPP
#define TINYLAMB_TOPN_PLAN_HPP

#include <cstddef>
#include <vector>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Physical Top-N: efficient k-row output for ORDER BY LIMIT queries where k
// is small.  Internally uses a heap to avoid materializing the full sorted
// result.  Falls back to a full sort when the child delivers more than a
// threshold number of rows.
//
// Enforces limit semantics: at most limit_count rows are emitted after
// skipping limit_offset rows.
class TopNPlan final : public PlanBase {
 public:
  TopNPlan(Plan src, std::vector<Expression> keys,
           std::vector<bool> ascending, size_t limit_count,
           size_t limit_offset)
      : src_(std::move(src)),
        keys_(std::move(keys)),
        ascending_(std::move(ascending)),
        limit_count_(limit_count),
        limit_offset_(limit_offset) {}
  TopNPlan(const TopNPlan&) = delete;
  TopNPlan(TopNPlan&&) = delete;
  TopNPlan& operator=(const TopNPlan&) = delete;
  TopNPlan& operator=(TopNPlan&&) = delete;
  ~TopNPlan() override = default;

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
  // The Top-N output is always ordered by our sort keys.
  [[nodiscard]] bool IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const override;
  // TopN enforces its own limit.
  [[nodiscard]] bool EnforcesLimit(size_t limit_count,
                                   size_t limit_offset) const override {
    return limit_count_ == limit_count && limit_offset_ == limit_offset;
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan src_;
  std::vector<Expression> keys_;
  std::vector<bool> ascending_;
  size_t limit_count_;
  size_t limit_offset_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TOPN_PLAN_HPP
