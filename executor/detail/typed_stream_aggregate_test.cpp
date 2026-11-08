/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "executor/detail/expression_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
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

using relational_detail::AggregateAccumulator;
using relational_detail::DecideTypedFilterCompare;
using relational_detail::EvaluateTypedConditionalOnRow;
using relational_detail::EvaluateTypedDoubleExprOnRow;
using relational_detail::MatchTypedStringInList;
using relational_detail::PositionalHaving;
using relational_detail::TryCompilePositionalHaving;
using relational_detail::TryCompilePositionalHavingConjuncts;
using relational_detail::TryCompileTypedConditionalInput;
using relational_detail::TryCompileTypedDoubleExpr;
using relational_detail::TryCompileTypedFilterCompare;
using relational_detail::TypedAggregateInput;
using relational_detail::TypedDoubleExprInput;
using relational_detail::TypedInListCompare;

Schema MakeScanSchema() {
  return Schema("lineitem",
                {
                    Column(ColumnName("l_orderkey"), ValueType::kInt64),
                    Column(ColumnName("l_quantity"), ValueType::kInt64),
                    Column(ColumnName("l_extendedprice"), ValueType::kDouble),
                    Column(ColumnName("l_discount"), ValueType::kDouble),
                    Column(ColumnName("l_tax"), ValueType::kDouble),
                    Column(ColumnName("l_shipmode"), ValueType::kVarChar),
                });
}

TEST(TypedStreamAggregateTest, MatchTypedStringInListExactAndPrefix) {
  TypedInListCompare exact{
      .column = 5,
      .is_double = false,
      .is_string = true,
      .prefix_len = 0,
      .int_values = {},
      .double_values = {},
      .strings = {"MAIL", "SHIP"},
  };
  EXPECT_TRUE(MatchTypedStringInList("MAIL", exact));
  EXPECT_TRUE(MatchTypedStringInList("SHIP", exact));
  EXPECT_FALSE(MatchTypedStringInList("AIR", exact));

  TypedInListCompare prefix{
      .column = 5,
      .is_double = false,
      .is_string = true,
      .prefix_len = 2,
      .int_values = {},
      .double_values = {},
      .strings = {"13", "31"},
  };
  EXPECT_TRUE(MatchTypedStringInList("13-456-7890", prefix));
  EXPECT_TRUE(MatchTypedStringInList("31-000-0000", prefix));
  EXPECT_FALSE(MatchTypedStringInList("22-000-0000", prefix));
  EXPECT_FALSE(MatchTypedStringInList("1", prefix));
}

TEST(TypedStreamAggregateTest, TryCompileAndDecideTypedFilterCompare) {
  const Schema schema = MakeScanSchema();

  const Expression pred = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l_quantity")), BinaryOperation::kLessThan,
      ConstantValueExp(Value(24)));
  auto compiled = TryCompileTypedFilterCompare(pred, schema, true);
  ASSERT_TRUE(compiled.has_value());
  if (!compiled.has_value()) {
    return;
  }
  EXPECT_EQ(compiled->left, 1U);
  EXPECT_TRUE(compiled->constant_payload);
  EXPECT_EQ(compiled->int_constant, 24);
  EXPECT_TRUE(DecideTypedFilterCompare(Value(10), Value(24), *compiled));
  EXPECT_FALSE(DecideTypedFilterCompare(Value(30), Value(24), *compiled));
}

TEST(TypedStreamAggregateTest, TryCompileAndEvaluateTypedConditionalInput) {
  const Schema schema = MakeScanSchema();

  // CASE WHEN l_shipmode = 'MAIL' THEN l_quantity ELSE NULL END
  const Expression cond = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l_shipmode")), BinaryOperation::kEquals,
      ConstantValueExp(Value("MAIL")));
  const Expression case_expr = std::make_shared<CaseExpression>(
      std::vector<std::pair<Expression, Expression>>{
          {cond, ColumnValueExp(ColumnName("l_quantity"))}},
      ConstantValueExp(Value()));

  auto compiled = TryCompileTypedConditionalInput(case_expr, schema);
  ASSERT_TRUE(compiled.has_value());
  if (!compiled.has_value()) {
    return;
  }
  EXPECT_EQ(compiled->result_type, ValueType::kInt64);

  const Row matching_row({Value(1), Value(15), Value(100.0), Value(0.05),
                          Value(0.02), Value("MAIL")});
  const Row non_matching_row({Value(1), Value(15), Value(100.0), Value(0.05),
                              Value(0.02), Value("TRUCK")});
  EXPECT_EQ(EvaluateTypedConditionalOnRow(*compiled, matching_row), Value(15));
  EXPECT_TRUE(
      EvaluateTypedConditionalOnRow(*compiled, non_matching_row).IsNull());
}

TEST(TypedStreamAggregateTest, TryCompileAndEvaluateTypedDoubleExpressions) {
  const Schema schema = MakeScanSchema();
  const Row row({Value(1), Value(10), Value(200.0), Value(0.1), Value(0.05),
                 Value("AIR")});

  // 1. kSingleDoubleCol: l_extendedprice
  const Expression single_col = ColumnValueExp(ColumnName("l_extendedprice"));
  auto compiled_single = TryCompileTypedDoubleExpr(single_col, schema);
  ASSERT_TRUE(compiled_single.has_value());
  if (!compiled_single.has_value()) {
    return;
  }
  EXPECT_EQ(compiled_single->shape,
            TypedDoubleExprInput::Shape::kSingleDoubleCol);
  EXPECT_DOUBLE_EQ(
      EvaluateTypedDoubleExprOnRow(*compiled_single, row).value.double_value,
      200.0);

  // 2. kMulTwoDoubleCols: l_extendedprice * l_discount
  const Expression mul_two = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l_extendedprice")), BinaryOperation::kMultiply,
      ColumnValueExp(ColumnName("l_discount")));
  auto compiled_mul2 = TryCompileTypedDoubleExpr(mul_two, schema);
  ASSERT_TRUE(compiled_mul2.has_value());
  if (!compiled_mul2.has_value()) {
    return;
  }
  EXPECT_EQ(compiled_mul2->shape,
            TypedDoubleExprInput::Shape::kMulTwoDoubleCols);
  EXPECT_DOUBLE_EQ(
      EvaluateTypedDoubleExprOnRow(*compiled_mul2, row).value.double_value,
      20.0);

  // 3. kMulConstMinusCol: l_extendedprice * (1.0 - l_discount)
  const Expression one_minus_disc = BinaryExpressionExp(
      ConstantValueExp(Value(1.0)), BinaryOperation::kSubtract,
      ColumnValueExp(ColumnName("l_discount")));
  const Expression disc_price =
      BinaryExpressionExp(ColumnValueExp(ColumnName("l_extendedprice")),
                          BinaryOperation::kMultiply, one_minus_disc);
  auto compiled_disc = TryCompileTypedDoubleExpr(disc_price, schema);
  ASSERT_TRUE(compiled_disc.has_value());
  if (!compiled_disc.has_value()) {
    return;
  }
  EXPECT_EQ(compiled_disc->shape,
            TypedDoubleExprInput::Shape::kMulConstMinusCol);
  EXPECT_DOUBLE_EQ(
      EvaluateTypedDoubleExprOnRow(*compiled_disc, row).value.double_value,
      180.0);

  // 4. kMulConstMinusColMulConstPlusCol:
  //    l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)
  const Expression one_plus_tax =
      BinaryExpressionExp(ConstantValueExp(Value(1.0)), BinaryOperation::kAdd,
                          ColumnValueExp(ColumnName("l_tax")));
  const Expression charge =
      BinaryExpressionExp(disc_price, BinaryOperation::kMultiply, one_plus_tax);
  auto compiled_charge = TryCompileTypedDoubleExpr(charge, schema);
  ASSERT_TRUE(compiled_charge.has_value());
  if (!compiled_charge.has_value()) {
    return;
  }
  EXPECT_EQ(compiled_charge->shape,
            TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol);
  EXPECT_NEAR(
      EvaluateTypedDoubleExprOnRow(*compiled_charge, row).value.double_value,
      189.0, 1e-9);
}

TEST(TypedStreamAggregateTest, TryCompilePositionalHavingAndConjuncts) {
  const Expression sum_qty = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp(ColumnName("l_quantity")));
  const Expression cnt_star = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ConstantValueExp(Value(1)));

  std::vector<TypedAggregateInput> aggs(2);
  aggs[0].aggregate = &sum_qty->AsAggregateExpression();
  aggs[1].aggregate = &cnt_star->AsAggregateExpression();

  const Expression having1 = BinaryExpressionExp(
      sum_qty, BinaryOperation::kGreaterThan, ConstantValueExp(Value(300)));
  auto single = TryCompilePositionalHaving(having1, aggs);
  ASSERT_TRUE(single.has_value());
  if (!single.has_value()) {
    return;
  }
  EXPECT_EQ(single->agg_index, 0U);
  EXPECT_EQ(single->op, BinaryOperation::kGreaterThan);
  EXPECT_EQ(single->constant, Value(300));

  const Expression having2 =
      BinaryExpressionExp(cnt_star, BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(5)));
  const Expression conj =
      BinaryExpressionExp(having1, BinaryOperation::kAnd, having2);
  std::vector<PositionalHaving> conjuncts;
  ASSERT_TRUE(TryCompilePositionalHavingConjuncts(conj, aggs, &conjuncts));
  ASSERT_EQ(conjuncts.size(), 2U);
  EXPECT_EQ(conjuncts[0].agg_index, 0U);
  EXPECT_EQ(conjuncts[1].agg_index, 1U);
}

TEST(TypedStreamAggregateTest, AggregateAccumulatorStreamingAndMerge) {
  const Expression sum_expr = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp(ColumnName("x")));
  AggregateAccumulator acc1(&sum_expr->AsAggregateExpression());
  AggregateAccumulator acc2(&sum_expr->AsAggregateExpression());
  EXPECT_TRUE(acc1.CanMerge());
  ASSERT_EQ(acc1.Add(Value(10)), Status::kSuccess);
  ASSERT_EQ(acc1.Add(Value(20)), Status::kSuccess);
  ASSERT_EQ(acc1.Add(Value()), Status::kSuccess);  // NULL ignored
  ASSERT_EQ(acc2.Add(Value(35)), Status::kSuccess);
  ASSERT_EQ(acc1.Merge(acc2), Status::kSuccess);
  auto finished = acc1.TryFinish();
  ASSERT_TRUE(finished.HasValue());
  EXPECT_EQ(finished.Value(), Value(65));

  // Inline distinct int64 tracking and promotion past
  // kInlineDistinctIntCapacity
  const Expression cnt_distinct = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp(ColumnName("x")), true);
  AggregateAccumulator distinct_acc(&cnt_distinct->AsAggregateExpression());
  EXPECT_TRUE(distinct_acc.DistinctMode());
  for (int64_t i = 0; i < 12; ++i) {
    EXPECT_TRUE(distinct_acc.InsertDistinctInt(i));
    EXPECT_FALSE(distinct_acc.InsertDistinctInt(i));
  }
  EXPECT_EQ(distinct_acc.DistinctIntCount(), 12U);
}

TEST(TypedStreamAggregateTest, HavingReferencesOnlyKeysAndAggregatesAndCells) {
  const Schema schema = MakeScanSchema();
  const std::vector<slot_t> key_offsets = {0};  // l_orderkey

  const Expression sum_qty = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp(ColumnName("l_quantity")));
  const Expression having_ok = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("l_orderkey")),
                          BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(0))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(sum_qty, BinaryOperation::kGreaterThan,
                          ConstantValueExp(Value(100))));
  EXPECT_TRUE(relational_detail::HavingReferencesOnlyKeysAndAggregates(
      having_ok, schema, key_offsets));

  const Expression having_bad = BinaryExpressionExp(
      ColumnValueExp(ColumnName("l_shipmode")), BinaryOperation::kEquals,
      ConstantValueExp(Value("RAIL")));
  EXPECT_FALSE(relational_detail::HavingReferencesOnlyKeysAndAggregates(
      having_bad, schema, key_offsets));

  uint32_t packed = 0;
  EXPECT_TRUE(relational_detail::TryPackShortVarcharSpan("AB", &packed));
  EXPECT_NE(packed, 0U);
  EXPECT_FALSE(relational_detail::TryPackShortVarcharSpan("ABCD", &packed));
}

}  // namespace
}  // namespace tinylamb
