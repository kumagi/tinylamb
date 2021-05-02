//
// Created by kumagi on 22/06/19.
//

#ifndef TINYLAMB_INDEX_SCAN_PLAN_HPP
#define TINYLAMB_INDEX_SCAN_PLAN_HPP

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Index;
class Table;

class IndexScanPlan : public PlanBase {
 public:
  IndexScanPlan(const Table& table, const Index& index,
                const TableStatistics& ts, const Value& begin, const Value& end,
                bool ascending, Expression where);
  IndexScanPlan(const IndexScanPlan&) = delete;
  IndexScanPlan(IndexScanPlan&&) = delete;
  IndexScanPlan& operator=(const IndexScanPlan&) = delete;
  IndexScanPlan& operator=(IndexScanPlan&&) = delete;
  ~IndexScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] const Table* ScanSource() const override { return &table_; };
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  const Table& table_;
  const Index& index_;
  TableStatistics stats_;
  Value begin_;
  Value end_;
  bool ascending_;
  Expression where_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_PLAN_HPP
