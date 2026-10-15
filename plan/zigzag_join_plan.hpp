#ifndef TINYLAMB_ZIGZAG_JOIN_PLAN_HPP
#define TINYLAMB_ZIGZAG_JOIN_PLAN_HPP

#include <optional>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ZigZagJoinPlan final : public PlanBase {
 public:
  ZigZagJoinPlan(Plan left, ColumnName left_key, Plan right,
                 ColumnName right_key, BinaryOperation operation,
                 Expression predicate,
                 std::optional<size_t> estimated_rows = std::nullopt);
  ZigZagJoinPlan(Plan left, std::vector<ColumnName> left_keys, Plan right,
                 std::vector<ColumnName> right_keys, BinaryOperation operation,
                 Expression predicate,
                 std::optional<size_t> estimated_rows = std::nullopt);
  ZigZagJoinPlan(const ZigZagJoinPlan&) = delete;
  ZigZagJoinPlan(ZigZagJoinPlan&&) = delete;
  ZigZagJoinPlan& operator=(const ZigZagJoinPlan&) = delete;
  ZigZagJoinPlan& operator=(ZigZagJoinPlan&&) = delete;
  ~ZigZagJoinPlan() override = default;

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
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

  [[nodiscard]] const Plan& Left() const { return left_; }
  [[nodiscard]] const Plan& Right() const { return right_; }
  [[nodiscard]] const ColumnName& LeftKey() const { return left_key_; }
  [[nodiscard]] const ColumnName& RightKey() const { return right_key_; }
  [[nodiscard]] std::vector<ColumnName> LeftKeys() const { return {left_key_}; }
  [[nodiscard]] std::vector<ColumnName> RightKeys() const {
    return {right_key_};
  }
  [[nodiscard]] BinaryOperation Operation() const { return operation_; }
  [[nodiscard]] BinaryOperation Comparison() const { return operation_; }
  [[nodiscard]] const Expression& Predicate() const { return predicate_; }
  [[nodiscard]] const Expression& Residual() const { return predicate_; }

 private:
  Plan left_;
  Plan right_;
  ColumnName left_key_;
  ColumnName right_key_;
  BinaryOperation operation_{BinaryOperation::kEquals};
  Expression predicate_;
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif
