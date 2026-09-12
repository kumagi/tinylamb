/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_SKIP_SCAN_DISTINCT_PLAN_HPP
#define TINYLAMB_PLAN_SKIP_SCAN_DISTINCT_PLAN_HPP

#include <memory>
#include <ostream>
#include <vector>

#include "index/index_schema.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Table;

// DISTINCT evaluated by skip-scanning a covering single-column index: the
// executor advances the index iterator past all entries sharing the current
// key, so each distinct value costs one seek instead of one row.  The output
// arrives in index order, which satisfies an ORDER BY on the distinct column
// without a Sort.
class SkipScanDistinctPlan final : public PlanBase {
 public:
  SkipScanDistinctPlan(const Table& table, const Index& index,
                       TableStatistics stats, bool ascending,
                       std::vector<NamedExpression> select_items,
                       Schema output_schema);
  SkipScanDistinctPlan(const SkipScanDistinctPlan&) = delete;
  SkipScanDistinctPlan(SkipScanDistinctPlan&&) = delete;
  SkipScanDistinctPlan& operator=(const SkipScanDistinctPlan&) = delete;
  SkipScanDistinctPlan& operator=(SkipScanDistinctPlan&&) = delete;
  ~SkipScanDistinctPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;
  [[nodiscard]] const Table* ScanSource() const override { return &table_; }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool EnforcesDistinct() const override { return true; }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  const Table& table_;
  const Index& index_;
  TableStatistics stats_;
  bool ascending_;
  std::vector<NamedExpression> select_items_;
  Schema output_schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_SKIP_SCAN_DISTINCT_PLAN_HPP
