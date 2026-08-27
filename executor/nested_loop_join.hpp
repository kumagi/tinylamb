/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_NESTED_LOOP_JOIN_HPP
#define TINYLAMB_NESTED_LOOP_JOIN_HPP

#include <cstddef>
#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/join_kind.hpp"
#include "expression/expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class NestedLoopJoin : public ExecutorBase {
 public:
  NestedLoopJoin(Executor left, Schema left_schema, Executor right,
                 Schema right_schema, Expression predicate,
                 JoinKind kind = JoinKind::kInner, size_t block_size = 1024,
                 bool assert_unique = false);
  NestedLoopJoin(const NestedLoopJoin&) = delete;
  NestedLoopJoin(NestedLoopJoin&&) = delete;
  NestedLoopJoin& operator=(const NestedLoopJoin&) = delete;
  NestedLoopJoin& operator=(NestedLoopJoin&&) = delete;
  ~NestedLoopJoin() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] JoinKind Kind() const { return kind_; }
  [[nodiscard]] bool AssertUnique() const { return assert_unique_; }

 private:
  void Materialize();
  [[nodiscard]] bool EvaluatePredicate(const Row& left, const Row& right) const;

  Executor left_;
  Schema left_schema_;
  Executor right_;
  Schema right_schema_;
  Expression predicate_;
  JoinKind kind_;
  size_t block_size_;
  bool assert_unique_{false};
  Schema combined_schema_;

  bool materialized_{false};
  std::vector<std::pair<Row, RowPosition>> output_;
  size_t output_offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_NESTED_LOOP_JOIN_HPP
