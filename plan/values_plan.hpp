/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#ifndef TINYLAMB_PLAN_VALUES_PLAN_HPP
#define TINYLAMB_PLAN_VALUES_PLAN_HPP

#include <utility>
#include <vector>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/row.hpp"

namespace tinylamb {

class ValuesPlan final : public PlanBase {
 public:
  ValuesPlan(Schema schema, std::vector<Row> rows);

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override { return rows_.size(); }
  [[nodiscard]] size_t EmitRowCount() const override { return rows_.size(); }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] const std::vector<Row>& Rows() const { return rows_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Schema schema_;
  std::vector<Row> rows_;
  TableStatistics stats_;
};

class DummyScanPlan final : public PlanBase {
 public:
  DummyScanPlan();

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override { return 1; }
  [[nodiscard]] size_t EmitRowCount() const override { return 1; }
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& /*expressions*/,
      const std::vector<bool>& /*ascending*/) const override {
    return true;
  }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_VALUES_PLAN_HPP
