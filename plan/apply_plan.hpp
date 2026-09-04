/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_APPLY_PLAN_HPP
#define TINYLAMB_PLAN_APPLY_PLAN_HPP

#include <memory>
#include <string>
#include <utility>

#include "common/join_kind.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ApplyPlan final : public PlanBase {
 public:
  ApplyPlan(Plan child, std::shared_ptr<const SelectStatement> inner_query,
            std::string alias, Expression predicate,
            JoinKind kind = JoinKind::kInner, Schema schema = Schema{});

  ApplyPlan(Plan child, Plan inner_child, Expression predicate,
            JoinKind kind = JoinKind::kInner, Schema schema = Schema{});

  ApplyPlan(Plan child, Plan inner_child,
            std::shared_ptr<const SelectStatement> inner_query,
            std::string alias, Expression predicate,
            JoinKind kind = JoinKind::kInner, Schema schema = Schema{});

  ApplyPlan(const ApplyPlan&) = delete;
  ApplyPlan(ApplyPlan&&) = delete;
  ApplyPlan& operator=(const ApplyPlan&) = delete;
  ApplyPlan& operator=(ApplyPlan&&) = delete;
  ~ApplyPlan() override = default;

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
  [[nodiscard]] const Plan& InnerChild() const { return inner_child_; }
  [[nodiscard]] const std::shared_ptr<const SelectStatement>& InnerQuery()
      const {
    return inner_query_;
  }
  [[nodiscard]] const std::string& Alias() const { return alias_; }
  [[nodiscard]] const Expression& Predicate() const { return predicate_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }

  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  Plan inner_child_{nullptr};
  std::shared_ptr<const SelectStatement> inner_query_{nullptr};
  std::string alias_;
  Expression predicate_;
  JoinKind kind_{JoinKind::kInner};
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_APPLY_PLAN_HPP
