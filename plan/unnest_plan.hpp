/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_UNNEST_PLAN_HPP
#define TINYLAMB_PLAN_UNNEST_PLAN_HPP

#include <memory>
#include <string>
#include <utility>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class UnnestPlan final : public PlanBase {
 public:
  UnnestPlan(Plan child, Expression unnest_expr, std::string alias,
             std::string offset_alias, Schema schema);

  UnnestPlan(const UnnestPlan&) = delete;
  UnnestPlan(UnnestPlan&&) = delete;
  UnnestPlan& operator=(const UnnestPlan&) = delete;
  UnnestPlan& operator=(UnnestPlan&&) = delete;
  ~UnnestPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_ ? child_->AccessRowCount() : 1;
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return (child_ ? child_->EmitRowCount() : 1) * 10;
  }
  [[nodiscard]] const Plan& Child() const { return child_; }
  [[nodiscard]] const Expression& UnnestExpr() const { return unnest_expr_; }
  [[nodiscard]] const std::string& Alias() const { return alias_; }
  [[nodiscard]] const std::string& OffsetAlias() const { return offset_alias_; }

  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  Expression unnest_expr_;
  std::string alias_;
  std::string offset_alias_;
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_UNNEST_PLAN_HPP
