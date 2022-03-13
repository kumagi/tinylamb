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
  explicit FullScanPlan(std::string_view table_name, TableStatistics ts);
  ~FullScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] Schema GetSchema(TransactionContext& txn) const override;

  [[nodiscard]] size_t AccessRowCount(TransactionContext& txn) const override;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& txn) const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  std::string table_name_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_PLAN_HPP
