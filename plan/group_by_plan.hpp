/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_GROUP_BY_PLAN_HPP
#define TINYLAMB_GROUP_BY_PLAN_HPP

#include <memory>
#include <string>

#include "plan/plan.hpp"
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
  GroupByPlan(const Plan& child,
              std::shared_ptr<const SelectStatement> statement,
              Schema output_schema)
      : child_(std::move(child)),
        statement_(std::move(statement)),
        output_schema_(std::move(output_schema)) {}
  GroupByPlan(const GroupByPlan&) = delete;
  GroupByPlan(GroupByPlan&&) = delete;
  GroupByPlan& operator=(const GroupByPlan&) = delete;
  GroupByPlan& operator=(const GroupByPlan&&) = delete;
  ~GroupByPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return child_->ScanSource();
  }
  [[nodiscard]] const Schema& GetSchema() const override {
    return output_schema_;
  }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return child_->GetStats();
  }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_->AccessRowCount();
  }
  // Scalar aggregation yields one row; GROUP BY is capped by the input.
  [[nodiscard]] size_t EmitRowCount() const override {
    return std::min<size_t>(child_->EmitRowCount(), 1);
  }
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
};

}  // namespace tinylamb

#endif  // TINYLAMB_GROUP_BY_PLAN_HPP
