/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BAND_JOIN_PLAN_HPP
#define TINYLAMB_BAND_JOIN_PLAN_HPP

#include <optional>

#include "common/join_kind.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class BandJoinPlan final : public PlanBase {
 public:
  BandJoinPlan(Plan left, std::vector<ColumnName> left_keys, Plan right,
               std::vector<ColumnName> right_keys, JoinKind kind,
               BinaryOperation comparison, Expression predicate,
               std::optional<size_t> estimated_rows = std::nullopt);

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override;
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

  [[nodiscard]] const Plan& Left() const { return left_; }
  [[nodiscard]] const Plan& Right() const { return right_; }
  [[nodiscard]] const std::vector<ColumnName>& LeftKeys() const {
    return left_keys_;
  }
  [[nodiscard]] const std::vector<ColumnName>& RightKeys() const {
    return right_keys_;
  }
  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] BinaryOperation Comparison() const { return comparison_; }
  [[nodiscard]] const Expression& Predicate() const { return predicate_; }
  [[nodiscard]] const Expression& Residual() const { return predicate_; }

 private:
  Plan left_;
  Plan right_;
  std::vector<ColumnName> left_keys_;
  std::vector<ColumnName> right_keys_;
  JoinKind kind_{JoinKind::kInner};
  BinaryOperation comparison_{BinaryOperation::kLessThan};
  Expression predicate_;
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_BAND_JOIN_PLAN_HPP
