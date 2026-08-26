/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MERGE_JOIN_HPP
#define TINYLAMB_MERGE_JOIN_HPP

#include <cstddef>
#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/join_kind.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Merge join for inputs already sorted by the supplied key columns.
// NULL keys never match. Equal-key runs are cross-multiplied, preserving
// many-to-many SQL join semantics.
//
// An optional residual predicate (an inequality such as `l.v < r.w`) is
// evaluated on each concatenated candidate pair: only pairs that satisfy it
// count as matches. Outer kinds NULL-pad sides whose key-equal partners all
// failed the residual; semi/anti treat the residual as part of the match
// test.
class MergeJoin final : public ExecutorBase {
 public:
  MergeJoin(Executor left, std::vector<slot_t> left_columns, Executor right,
            std::vector<slot_t> right_columns, JoinKind kind = JoinKind::kInner,
            size_t left_width = 0, size_t right_width = 0,
            Expression residual = Expression(),
            Schema residual_schema = Schema());

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  void Materialize();
  bool KeyIsNull(const Row& row, const std::vector<slot_t>& columns) const;
  int CompareKeys(const Row& left, const Row& right) const;
  // Evaluates the optional residual over `left_rows_[i] + right_rows_[j]`;
  // always true when no residual was supplied.
  bool PairPasses(size_t i, size_t j) const;
  Row Concatenate(size_t i, size_t j) const;

  Executor left_;
  Executor right_;
  std::vector<slot_t> left_columns_;
  std::vector<slot_t> right_columns_;
  std::vector<Row> left_rows_;
  std::vector<Row> right_rows_;
  std::vector<Row> output_;
  std::vector<RowPosition> left_positions_;
  std::vector<RowPosition> output_positions_;
  JoinKind kind_{JoinKind::kInner};
  size_t left_width_{0};
  size_t right_width_{0};
  Expression residual_{};
  Schema residual_schema_{};
  size_t output_index_{0};
  bool materialized_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MERGE_JOIN_HPP
