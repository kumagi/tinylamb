/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

// Physical join configuration types owned by the executor layer
// (executor/hash_join_mode.hpp, executor/join_kind.hpp). The plan layer only
// carries their values opaquely; the relational factory translates them when
// emitting executors.
enum class HashJoinMode : uint8_t;
enum class JoinKind : uint8_t;

class ExecutorBase;

class ProductPlan final : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols);
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols,
              HashJoinMode hash_mode);
  // Explicit hash join kind. Semi/anti kinds emit only the left child's
  // columns; outer kinds retain both schemas and add NULL padding.
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols, HashJoinMode hash_mode,
              JoinKind kind);
  // Same, always executing in memory (the decorrelation rewrite in the
  // optimizer emits this shape).
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols, JoinKind kind);
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
              const Table& right_tbl, const Index& idx,
              std::vector<ColumnName> right_cols,
              const TableStatistics& right_ts);
  // Index Join whose right side is exposed under a relation-renamed schema:
  // `right_cols` resolve against the physical table while `declared_output`
  // names the emitted columns (Phase 8 self-joins).
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
              const Table& right_tbl, const Index& idx,
              std::vector<ColumnName> right_cols,
              const TableStatistics& right_ts, Schema declared_output);
  ProductPlan(Plan left_src, Plan right_src);
  ProductPlan(const ProductPlan&) = delete;
  ProductPlan(ProductPlan&&) = delete;
  ProductPlan& operator=(const ProductPlan&) = delete;
  ProductPlan& operator=(ProductPlan&&) = delete;
  ~ProductPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] HashJoinMode GetHashJoinMode() const { return hash_mode_; }
  [[nodiscard]] JoinKind Kind() const { return kind_; }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan left_src_;
  Plan right_src_;
  std::vector<ColumnName> left_cols_;
  std::vector<ColumnName> right_cols_;
  const Table* right_tbl_;
  const Index* right_idx_;
  const TableStatistics* right_ts_;
  HashJoinMode hash_mode_{};
  JoinKind kind_{};
  Schema output_schema_;
  TableStatistics stats_;
};

// Opaque accessors for the executor-layer enum values: the plan layer sees
// only the forward-declared JoinKind, so callers pick a side by name instead
// of naming enumerators.
[[nodiscard]] bool IsSemiJoinKind(JoinKind kind);
[[nodiscard]] bool IsAntiJoinKind(JoinKind kind);
[[nodiscard]] bool IsNullAwareAntiJoinKind(JoinKind kind);
[[nodiscard]] bool IsLeftOuterJoinKind(JoinKind kind);
[[nodiscard]] bool IsRightOuterJoinKind(JoinKind kind);
[[nodiscard]] bool IsFullOuterJoinKind(JoinKind kind);
[[nodiscard]] JoinKind SemiJoinKind();
[[nodiscard]] JoinKind AntiJoinKind();
[[nodiscard]] JoinKind NullAwareAntiJoinKind();
[[nodiscard]] JoinKind LeftOuterJoinKind();
[[nodiscard]] JoinKind RightOuterJoinKind();
[[nodiscard]] JoinKind FullOuterJoinKind();

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
