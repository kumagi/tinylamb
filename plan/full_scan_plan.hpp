//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_FULL_SCAN_PLAN_HPP
#define TINYLAMB_FULL_SCAN_PLAN_HPP

#include "plan/plan.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class FullScanPlan : public PlanBase {
 public:
  explicit FullScanPlan(const Table& table, const TableStatistics& ts);
  FullScanPlan(const FullScanPlan&) = delete;
  FullScanPlan(FullScanPlan&&) = delete;
  FullScanPlan& operator=(const FullScanPlan&) = delete;
  FullScanPlan& operator=(FullScanPlan&&) = delete;
  ~FullScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  const Table& table_;
  const TableStatistics& stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_PLAN_HPP
