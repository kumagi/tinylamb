//
// Created by kumagi on 22/06/19.
//

#ifndef TINYLAMB_INDEX_ONLY_SCAN_PLAN_HPP
#define TINYLAMB_INDEX_ONLY_SCAN_PLAN_HPP

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Index;
class Table;

class IndexOnlyScanPlan : public PlanBase {
 public:
  IndexOnlyScanPlan(const Table& table, const Index& index,
                    const TableStatistics& ts, const Value& begin,
                    const Value& end, bool ascending, Expression where);
  IndexOnlyScanPlan(const IndexOnlyScanPlan&) = delete;
  IndexOnlyScanPlan(IndexOnlyScanPlan&&) = delete;
  IndexOnlyScanPlan& operator=(const IndexOnlyScanPlan&) = delete;
  IndexOnlyScanPlan& operator=(IndexOnlyScanPlan&&) = delete;
  ~IndexOnlyScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] const Table* ScanSource() const override { return &table_; };
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  [[nodiscard]] Schema OutputSchema() const;
  const Table& table_;
  const Index& index_;
  TableStatistics stats_;
  Value begin_;
  Value end_;
  bool ascending_;
  Expression where_;
  std::vector<NamedExpression> select_;
  Schema output_schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_PLAN_HPP
