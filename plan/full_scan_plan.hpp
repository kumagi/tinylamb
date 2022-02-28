//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_FULL_SCAN_PLAN_HPP
#define TINYLAMB_FULL_SCAN_PLAN_HPP

#include "page/page_manager.hpp"
#include "plan/plan_base.hpp"
#include "table/table.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Catalog;
class Transaction;
class Table;

class FullScanPlan : public PlanBase {
 public:
  FullScanPlan(Transaction& txn, std::string_view table_name, Catalog* catalog,
               PageManager* pm);
  ~FullScanPlan() override = default;

  std::unique_ptr<ExecutorBase> EmitExecutor(Transaction& txn) const override;

  [[nodiscard]] Schema GetSchema() const override { return schema_; }

 private:
  std::string table_name_;

  Schema schema_;
  Table tbl_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_PLAN_HPP
