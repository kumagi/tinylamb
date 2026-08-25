/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_RELATIONAL_PLAN_HPP
#define TINYLAMB_PLAN_RELATIONAL_PLAN_HPP

#include <memory>
#include <string>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

class SelectStatement;

// Physical implementation for relational IR shapes that are not yet lowered
// into the scalar scan/join operators. It is nevertheless selected through
// the Cascades memo, so every SELECT now has one optimizer entry point.
class RelationalPlan final : public PlanBase {
 public:
  RelationalPlan(std::shared_ptr<const SelectStatement> statement,
                 Schema output_schema)
      : statement_(std::move(statement)),
        output_schema_(std::move(output_schema)),
        statistics_(output_schema_) {}

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return statistics_;
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] size_t AccessRowCount() const override { return 0; }
  [[nodiscard]] size_t EmitRowCount() const override { return 0; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  std::shared_ptr<const SelectStatement> statement_;
  Schema output_schema_;
  TableStatistics statistics_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_RELATIONAL_PLAN_HPP
