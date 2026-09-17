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
    ASSERT_EQ(row.values_.size(), 5U) << true;
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

TEST(ParallelMergeJoinExtraTest, DuplicateKeysAtDifferentOffsets) {
  for (const size_t workers : {size_t{1}, size_t{4}}) {
    SCOPED_TRACE(workers);
    std::vector<Row> left_rows;
    std::vector<Row> right_rows;
    std::vector<Row> expected;
    for (int64_t key = 0; key < 25; ++key) {
      for (int64_t duplicate = 0; duplicate < 3; ++duplicate) {
        left_rows.emplace_back(Row({Value(key), Value(100 + duplicate)}));
      }
      for (int64_t duplicate = 0; duplicate < 2; ++duplicate) {
        right_rows.emplace_back(Row({Value(200 + duplicate), Value(key)}));
      }
      for (int64_t left_duplicate = 0; left_duplicate < 3; ++left_duplicate) {
        for (int64_t right_duplicate = 0; right_duplicate < 2;
             ++right_duplicate) {
          expected.emplace_back(Row({Value(key), Value(100 + left_duplicate),
                                     Value(200 + right_duplicate), Value(key)}));
        }
      }
    }
    ParallelMergeJoin join(
        std::make_shared<ConstantExecutor>(left_rows), {0},
        std::make_shared<ConstantExecutor>(right_rows), {1}, workers);
    std::vector<Row> actual;
    Row row;
    while (join.Next(&row, nullptr)) {
      actual.push_back(row);
    }
    EXPECT_EQ(join.GetStatus(), Status::kSuccess);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(join.Partitions().size(), workers);
  }
}

}  // namespace

}  // namespace tinylamb
