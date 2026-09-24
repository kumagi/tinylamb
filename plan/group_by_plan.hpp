/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GROUP_BY_PLAN_HPP
#define TINYLAMB_GROUP_BY_PLAN_HPP

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SelectStatement;

// Bridge plan node for GROUP BY / HAVING / aggregate SELECT statements: the
// child plan is the Cascades-optimized FROM + WHERE core (join ordering,
// access paths, filter pushdown), and the grouping/aggregation finish
// pipeline runs through the proven relational finish path.  EmitExecutor
// lives in the relational factory (executor/relational_factory.cpp).
class GroupByPlan final : public PlanBase {
 public:
  GroupByPlan(Plan child, std::shared_ptr<const SelectStatement> statement,
              Schema output_schema, bool grouped = false,
              std::optional<size_t> finite_limit = std::nullopt)
      : child_(std::move(child)),
        statement_(std::move(statement)),
        output_schema_(std::move(output_schema)),
        grouped_(grouped),
        stats_(output_schema_) {
    std::vector<ColumnStats> columns;
    columns.reserve(output_schema_.ColumnCount());
    for (size_t i = 0; i < output_schema_.ColumnCount(); ++i) {
      columns.emplace_back(output_schema_.GetColumn(i).Type());
    }
    const size_t base_rows = grouped_ ? child_->EmitRowCount() : 1;
    const size_t estimated_rows =
        finite_limit ? std::min(base_rows, *finite_limit) : base_rows;
    stats_.Assign(estimated_rows, std::move(columns));
  }
  GroupByPlan(const GroupByPlan&) = delete;
  GroupByPlan(GroupByPlan&&) = delete;
  GroupByPlan& operator=(const GroupByPlan&) = delete;
  GroupByPlan& operator=(const GroupByPlan&&) = delete;
  ~GroupByPlan() override = default;

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override {
    return child_->ContainsParameterSlots();
  }
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override {
    return child_->InstallParameterSlots(parameters, cursor, slots);
  }

  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  [[nodiscard]] size_t EmitRowCount() const override { return stats_.Rows(); }
  [[nodiscard]] Plan Child() const { return child_; }
  [[nodiscard]] const std::shared_ptr<const SelectStatement>& Statement()
      const {
    return statement_;
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override {
    return "GroupByFinish";
  }

 private:
  Plan child_;
  std::shared_ptr<const SelectStatement> statement_{};
  Schema output_schema_;
  bool grouped_{false};
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_GROUP_BY_PLAN_HPP
