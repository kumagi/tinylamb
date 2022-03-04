//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_FULL_SCAN_PLAN_HPP
#define TINYLAMB_FULL_SCAN_PLAN_HPP

#include "plan/plan_base.hpp"
#include "table/table.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class FullScanPlan : public PlanBase {
 public:
  explicit FullScanPlan(std::string_view table_name);
  ~FullScanPlan() override = default;

  std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& txn) const override;

  [[nodiscard]] Schema GetSchema(TransactionContext& txn) const override;

  [[nodiscard]] int AccessRowCount(TransactionContext& txn) const override;
  [[nodiscard]] int EmitRowCount(TransactionContext& txn) const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  std::string table_name_;
  mutable std::unique_ptr<Table> tbl_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_PLAN_HPP
