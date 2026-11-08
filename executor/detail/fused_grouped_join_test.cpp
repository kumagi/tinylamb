/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_grouped_join.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::AddJoinKeyBias;
using relational_detail::ClassifyProgram;
using relational_detail::CollectLoadCells;
using relational_detail::CompositeIntJoinKey;
using relational_detail::CompositeIntKey;
using relational_detail::CompositeIntKeyHash;
using relational_detail::EncodeCompositeIntKey;
using relational_detail::EvaluateProgramValue;
using relational_detail::FlatJoinBuckets;
using relational_detail::ProgramShape;

TEST(FusedGroupedJoinTest, AddJoinKeyBiasHandlesSignedExtremesWithoutOverflow) {
  const int64_t min_val = std::numeric_limits<int64_t>::min();
  EXPECT_EQ(AddJoinKeyBias(min_val, min_val), 0);
  EXPECT_EQ(AddJoinKeyBias(42, -10), 32);
}

TEST(FusedGroupedJoinTest, CompositeIntJoinKeyValidationAndEncoding) {
  const Schema left("l", {
                             Column(ColumnName("k1"), ValueType::kInt64),
                             Column(ColumnName("k2"), ValueType::kInt64),
                             Column(ColumnName("s"), ValueType::kVarChar),
                         });
  const Schema right("r", {
                              Column(ColumnName("k1"), ValueType::kInt64),
                              Column(ColumnName("k2"), ValueType::kInt64),
                              Column(ColumnName("s"), ValueType::kVarChar),
                          });

  EXPECT_TRUE(CompositeIntJoinKey(left, {0, 1}, right, {0, 1}));
  // String column is not eligible for CompositeIntJoinKey.
  EXPECT_FALSE(CompositeIntJoinKey(left, {0, 2}, right, {0, 2}));
  // Mismatched column counts or >4 columns rejected.
  EXPECT_FALSE(CompositeIntJoinKey(left, {0}, right, {0, 1}));
  EXPECT_FALSE(
      CompositeIntJoinKey(left, {0, 1, 0, 1, 0}, right, {0, 1, 0, 1, 0}));

  const Row r1({Value(100), Value(200), Value("x")});
  const Row r2({Value(100), Value(200), Value("y")});
  const Row r3({Value(100), Value(201), Value("x")});
  const std::vector<int64_t> offsets = {-50, -100};

  const CompositeIntKey key1 = EncodeCompositeIntKey(r1, {0, 1}, &offsets);
  const CompositeIntKey key2 = EncodeCompositeIntKey(r2, {0, 1}, &offsets);
  const CompositeIntKey key3 = EncodeCompositeIntKey(r3, {0, 1}, &offsets);
  EXPECT_EQ(key1.count, 2U);
  EXPECT_EQ(key1.parts[0], 50);
  EXPECT_EQ(key1.parts[1], 100);
  EXPECT_EQ(key1, key2);
  EXPECT_NE(key1, key3);

  CompositeIntKeyHash hasher;
  EXPECT_EQ(hasher(key1), hasher(key2));
}

TEST(FusedGroupedJoinTest, FlatJoinBucketsChainsDuplicatesAndCollisions) {
  FlatJoinBuckets<int64_t> buckets;
  buckets.PrefetchBucket(10);  // Safe on empty buckets
  buckets.reserve(16);

  const Row row_a({Value(1), Value("a1")});
  const Row row_b({Value(1), Value("a2")});
  const Row row_c({Value(2), Value("b1")});

  EXPECT_TRUE(buckets.emplace(10, &row_a));   // First occurrence returns true
  EXPECT_FALSE(buckets.emplace(10, &row_b));  // Duplicate returns false
  EXPECT_TRUE(buckets.emplace(20, &row_c));

  buckets.PrefetchBucket(10);
  auto [it10, end10] = buckets.equal_range(10);
  std::vector<const Row*> matches10;
  for (; it10 != end10; ++it10) {
    matches10.push_back(it10->second);
  }
  ASSERT_EQ(matches10.size(), 2U);
  EXPECT_EQ(matches10[0], &row_b);
  EXPECT_EQ(matches10[1], &row_a);

  auto [it99, end99] = buckets.equal_range(99);
  EXPECT_EQ(it99, end99);
}

TEST(FusedGroupedJoinTest, ClassifyProgramAndEvaluateProgramValueShapes) {
  const Schema schema("s", {
                               Column(ColumnName("c0"), ValueType::kDouble),
                               Column(ColumnName("c1"), ValueType::kDouble),
                               Column(ColumnName("c2"), ValueType::kDouble),
                               Column(ColumnName("c3"), ValueType::kDouble),
                           });
  const Row row({Value(100.0), Value(0.1), Value(25.0), Value(2.0)});

  // 1. Direct column load: c0
  const Expression direct_expr = ColumnValueExp(ColumnName("c0"));
  auto direct_prog = BytecodeCompiler::Compile(direct_expr, schema);
  ASSERT_TRUE(direct_prog.has_value());
  if (!direct_prog.has_value()) {
    return;
  }
  const ProgramShape direct_shape =
      ClassifyProgram(*direct_prog, direct_expr, &schema);
  EXPECT_EQ(direct_shape.direct_column, 0);
  EXPECT_TRUE(direct_shape.HasPairDirect());
  auto direct_val = EvaluateProgramValue(*direct_prog, direct_shape, row);
  ASSERT_TRUE(direct_val.HasValue());
  EXPECT_DOUBLE_EQ(direct_val.Value().value.double_value, 100.0);

  // 2. Two-column multiply: c0 * c1
  const Expression mul_expr = BinaryExpressionExp(
      ColumnValueExp(ColumnName("c0")), BinaryOperation::kMultiply,
      ColumnValueExp(ColumnName("c1")));
  auto mul_prog = BytecodeCompiler::Compile(mul_expr, schema);
  ASSERT_TRUE(mul_prog.has_value());
  if (!mul_prog.has_value()) {
    return;
  }
  const ProgramShape mul_shape = ClassifyProgram(*mul_prog, mul_expr, &schema);
  EXPECT_EQ(mul_shape.mul_col0, 0);
  EXPECT_EQ(mul_shape.mul_col1, 1);
  auto mul_val = EvaluateProgramValue(*mul_prog, mul_shape, row);
  ASSERT_TRUE(mul_val.HasValue());
  EXPECT_DOUBLE_EQ(mul_val.Value().value.double_value, 10.0);

  // 3. Mul-one-minus: c0 * (1.0 - c1)
  const Expression one_minus = BinaryExpressionExp(
      ConstantValueExp(Value(1.0)), BinaryOperation::kSubtract,
      ColumnValueExp(ColumnName("c1")));
  const Expression disc_expr = BinaryExpressionExp(
      ColumnValueExp(ColumnName("c0")), BinaryOperation::kMultiply, one_minus);
  auto disc_prog = BytecodeCompiler::Compile(disc_expr, schema);
  ASSERT_TRUE(disc_prog.has_value());
  if (!disc_prog.has_value()) {
    return;
  }
  const ProgramShape disc_shape =
      ClassifyProgram(*disc_prog, disc_expr, &schema);
  EXPECT_EQ(disc_shape.mul_one_minus_col0, 0);
  EXPECT_EQ(disc_shape.mul_one_minus_col1, 1);
  auto disc_val = EvaluateProgramValue(*disc_prog, disc_shape, row);
  ASSERT_TRUE(disc_val.HasValue());
  EXPECT_DOUBLE_EQ(disc_val.Value().value.double_value, 90.0);

  // 4. Profit expression: c0 * (1.0 - c1) - c2 * c3
  const Expression cost_expr = BinaryExpressionExp(
      ColumnValueExp(ColumnName("c2")), BinaryOperation::kMultiply,
      ColumnValueExp(ColumnName("c3")));
  const Expression profit_expr =
      BinaryExpressionExp(disc_expr, BinaryOperation::kSubtract, cost_expr);
  auto profit_prog = BytecodeCompiler::Compile(profit_expr, schema);
  ASSERT_TRUE(profit_prog.has_value());
  if (!profit_prog.has_value()) {
    return;
  }
  const ProgramShape profit_shape =
      ClassifyProgram(*profit_prog, profit_expr, &schema);
  EXPECT_EQ(profit_shape.profit_col0, 0);
  EXPECT_EQ(profit_shape.profit_col1, 1);
  EXPECT_EQ(profit_shape.profit_col2, 2);
  EXPECT_EQ(profit_shape.profit_col3, 3);
  auto profit_val = EvaluateProgramValue(*profit_prog, profit_shape, row);
  ASSERT_TRUE(profit_val.HasValue());
  EXPECT_DOUBLE_EQ(profit_val.Value().value.double_value, 40.0);

  std::vector<slot_t> touched;
  CollectLoadCells(*profit_prog, &touched);
  EXPECT_EQ(touched.size(), 4U);
}

}  // namespace
}  // namespace tinylamb
