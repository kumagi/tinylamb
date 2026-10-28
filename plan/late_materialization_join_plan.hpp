/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_LATE_MATERIALIZATION_JOIN_PLAN_HPP
#define TINYLAMB_PLAN_LATE_MATERIALIZATION_JOIN_PLAN_HPP

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "common/late_materialization.hpp"
#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Index;
class Table;

class LateMaterializationJoinPlan final : public PlanBase {
 public:
  LateMaterializationJoinPlan(
      Plan left, std::vector<ColumnName> left_keys, const Table& right_table,
      const Index& right_index, std::vector<ColumnName> right_keys,
      const TableStatistics& right_statistics, Expression predicate,
      Schema declared_output = Schema(),
      std::vector<slot_t> right_projection = {}, bool unique_winner = false,
      std::optional<size_t> estimated_rows = std::nullopt);
  LateMaterializationJoinPlan(
      Plan left, const Table& right_table, Schema declared_output,
      std::vector<slot_t> right_projection,
      LateMaterializationRowIdBinding rowid, Expression predicate = nullptr,
      std::optional<size_t> estimated_rows = std::nullopt);
  LateMaterializationJoinPlan(const LateMaterializationJoinPlan&) = delete;
  LateMaterializationJoinPlan(LateMaterializationJoinPlan&&) = delete;
  LateMaterializationJoinPlan& operator=(const LateMaterializationJoinPlan&) =
      delete;
  LateMaterializationJoinPlan& operator=(LateMaterializationJoinPlan&&) =
      delete;
  ~LateMaterializationJoinPlan() override = default;

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
  [[nodiscard]] size_t EmitRowCount() const override { return stats_.Rows(); }
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
  [[nodiscard]] const Table* RightTable() const { return right_table_; }
  [[nodiscard]] const Index* RightIndex() const { return right_index_; }
  [[nodiscard]] const std::vector<ColumnName>& LeftKeys() const {
    return left_keys_;
  }
  [[nodiscard]] const std::vector<ColumnName>& RightKeys() const {
    return right_keys_;
  }
  [[nodiscard]] const std::vector<slot_t>& RightProjection() const {
    return right_projection_;
  }
  [[nodiscard]] const Expression& Predicate() const { return predicate_; }
  [[nodiscard]] const Expression& Residual() const { return predicate_; }
  [[nodiscard]] LateMaterializationIdentity Identity() const {
    return identity_;
  }
  [[nodiscard]] bool UniqueWinner() const { return unique_winner_; }
  [[nodiscard]] const LateMaterializationRowIdBinding& RowId() const {
    return rowid_;
  }

 private:
  void InitializeProjection(const Schema& declared_output);
  // Appends the payload columns' statistics so stats_ has one column per
  // output column (stats_ started as the left input's statistics only, and a
  // Selection over this join then indexed past the end of the column stats).
  void AppendPayloadStats(const TableStatistics* right_statistics);

  Plan left_;
  const Table* right_table_{nullptr};
  const Index* right_index_{nullptr};
  std::vector<ColumnName> left_keys_;
  std::vector<ColumnName> right_keys_;
  std::vector<slot_t> right_projection_;
  Expression predicate_;
  LateMaterializationIdentity identity_{LateMaterializationIdentity::kEquiKey};
  LateMaterializationRowIdBinding rowid_{};
  bool unique_winner_{false};
  size_t right_row_count_{0};
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_LATE_MATERIALIZATION_JOIN_PLAN_HPP
