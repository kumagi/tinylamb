/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/parallel_merge_join.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Left-outer NULL padding must use the FULL right row width: padding only
// the key columns mixes two widths in one output and trips the downstream
// "data chunk row width mismatch" check.
TEST(ParallelMergeJoinExtraTest, LeftOuterPadsFullRightWidth) {
  const Schema left_schema(
      "l", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{10})}),
                       Row({Value(int64_t{2}), Value(int64_t{20})})});
  auto right = std::make_shared<ConstantExecutor>(std::vector<Row>{
      Row({Value(int64_t{1}), Value(int64_t{100}), Value("x")}),
      Row({Value(int64_t{3}), Value(int64_t{300}), Value("z")})});

  ParallelMergeJoin join(left, {0}, right, {0}, 2, JoinKind::kLeftOuter,
                         Expression(), Schema());
  Row row;
  RowPosition pos;
  std::vector<Row> rows;
  while (join.Next(&row, &pos)) {
    ASSERT_EQ(row.values_.size(), 5U)
        << "padded row must carry left(2) + right(3) values";
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 2U);
  // key 1 matched: left(1,10) + right(1,100,"x")
  EXPECT_EQ(rows[0], Row({Value(int64_t{1}), Value(int64_t{10}),
                          Value(int64_t{1}), Value(int64_t{100}), Value("x")}));
  // key 2 unmatched: left(2,20) + three NULLs (full right width)
  EXPECT_EQ(rows[1], Row({Value(int64_t{2}), Value(int64_t{20}), Value(),
                          Value(), Value()}));
}

// The residual predicate must filter matched pairs instead of being ignored.
TEST(ParallelMergeJoinExtraTest, ResidualFiltersMatchedPairs) {
  auto left = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{10})}),
                       Row({Value(int64_t{1}), Value(int64_t{140})}),
                       Row({Value(int64_t{2}), Value(int64_t{20})})});
  auto right = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(int64_t{1}), Value(int64_t{100})}),
                       Row({Value(int64_t{2}), Value(int64_t{200})})});

  // residual: l.v < r.a
  ParallelMergeJoin join(
      left, {0}, right, {0}, 2, JoinKind::kInner,
      BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kLessThan,
                          ColumnValueExp("a")),
      Schema("residual",
             {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64),
              Column("k", ValueType::kInt64), Column("a", ValueType::kInt64)}));
  Row row;
  RowPosition pos;
  std::vector<Row> rows;
  while (join.Next(&row, &pos)) {
    ASSERT_EQ(row.values_.size(), 4U);
    rows.push_back(row);
  }
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[0], Row({Value(int64_t{1}), Value(int64_t{10}),
                          Value(int64_t{1}), Value(int64_t{100})}));
  EXPECT_EQ(rows[1], Row({Value(int64_t{2}), Value(int64_t{20}),
                          Value(int64_t{2}), Value(int64_t{200})}));
}

}  // namespace

}  // namespace tinylamb
