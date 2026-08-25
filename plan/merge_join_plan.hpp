/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MERGE_JOIN_PLAN_HPP
#define TINYLAMB_MERGE_JOIN_PLAN_HPP

#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class MergeJoinPlan final : public PlanBase {
 public:
  MergeJoinPlan(Plan left, std::vector<ColumnName> left_keys, Plan right,
                std::vector<ColumnName> right_keys,
                JoinKind kind = JoinKind{});

  Executor EmitExecutor(TransactionContext& ctx) const override;
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

 private:
  Plan left_;
  Plan right_;
  std::vector<ColumnName> left_keys_;
  std::vector<ColumnName> right_keys_;
  JoinKind kind_{};
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_MERGE_JOIN_PLAN_HPP
