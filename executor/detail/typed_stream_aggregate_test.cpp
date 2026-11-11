/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
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

TEST(TypedStreamAggregateTest, DependentIntegerKernelsAndFallbackMatchAst) {
  const Schema schema("input", {Column("k", ValueType::kInt64)});
  const auto key = ColumnValueExp("k");
  const auto one = ConstantValueExp(Value(1));
  const auto two = ConstantValueExp(Value(2));
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  const auto doubled =
      BinaryExpressionExp(key, BinaryOperation::kMultiply, two);
  for (const auto& expression :
       {BinaryExpressionExp(key, BinaryOperation::kAdd, one),
        BinaryExpressionExp(key, BinaryOperation::kSubtract, one), doubled,
        BinaryExpressionExp(key, BinaryOperation::kMultiply, key),
        BinaryExpressionExp(doubled, BinaryOperation::kSubtract, key)}) {
    SelectStatement statement(
        {NamedExpression("k", key), NamedExpression("d", expression),
         NamedExpression("n", count)},
        {}, nullptr);
    statement.SetGroupBy({key, expression});
    const auto plan = relational_detail::PlanTypedAggregation(
        statement, nullptr, schema, {count.get()}, {});
    ASSERT_TRUE(plan.eligible);
    ASSERT_EQ(plan.dependent_key_inputs.size(), 1U);
    ASSERT_EQ(plan.dependent_key_programs.size(), 1U);
    for (const int64_t raw :
         {std::numeric_limits<int64_t>::min(),
          std::numeric_limits<int64_t>::min() + 1, int64_t{-3037000500},
          int64_t{-3}, int64_t{-1}, int64_t{0}, int64_t{1}, int64_t{3},
          int64_t{3037000500}, std::numeric_limits<int64_t>::max() - 1,
          std::numeric_limits<int64_t>::max()}) {
      SCOPED_TRACE(raw);
      const Row row({Value(raw)});
      const auto expected = expression->TryEvaluate(row, schema);
      int64_t actual = 0;
      Status error = Status::kSuccess;
      const bool success = relational_detail::EvaluateTypedIntExprCells(
          plan.dependent_key_inputs[0], &raw, [](slot_t) { return false; },
          &actual, &error);
      EXPECT_EQ(success, expected.HasValue());
      if (expected.HasValue()) {
        EXPECT_EQ(Value(actual), expected.Value());
      } else {
        // The primitive reports failure internally; Bytecode below supplies
        // the public error classification, which must match the AST exactly.
        EXPECT_NE(error, Status::kSuccess);
      }
      const auto fallback =
          TryEvaluateRowAuto(plan.dependent_key_programs[0], row);
      EXPECT_EQ(fallback.GetStatus(), expected.GetStatus());
      if (fallback.HasValue() && expected.HasValue()) {
        EXPECT_EQ(fallback.Value(), expected.Value());
      }
    }
    const Row null_row({Value()});
    const auto expected = expression->TryEvaluate(null_row, schema);
    const auto fallback =
        TryEvaluateRowAuto(plan.dependent_key_programs[0], null_row);
    ASSERT_TRUE(expected.HasValue());
    ASSERT_TRUE(fallback.HasValue());
    EXPECT_EQ(fallback.Value(), expected.Value());
    EXPECT_TRUE(fallback.Value().IsNull());
  }
}

TEST(TypedStreamAggregateTest, ConstantInListsFoldSignsButDeferUnsafeInputs) {
  const Schema schema("input", {Column("k", ValueType::kInt64)});
  const auto key = ColumnValueExp("k");
  const auto count = std::make_shared<AggregateExpression>(
      AggregationType::kCount, ColumnValueExp("*"));
  SelectStatement statement(
      {NamedExpression("k", key), NamedExpression("n", count)}, {}, nullptr);
  statement.SetGroupBy({key});
  const auto negative =
      UnaryExpressionExp(ConstantValueExp(Value(1)), UnaryOperation::kMinus);
  const auto six =
      BinaryExpressionExp(ConstantValueExp(Value(4)), BinaryOperation::kAdd,
                          ConstantValueExp(Value(2)));
  const auto plan_for = [&](std::vector<Expression> list) {
    return relational_detail::PlanTypedAggregation(
        statement, nullptr, schema, {count.get()},
        {InExpressionExp(key, std::move(list))});
  };
  const auto plan = plan_for({negative, six});
  ASSERT_TRUE(plan.eligible);
  ASSERT_EQ(plan.inlist_compares.size(), 1U);
  EXPECT_EQ(plan.inlist_compares.front().int_values,
            (std::vector<int64_t>{-1, 6}));
  const auto divide_by_zero =
      BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kDivide,
                          ConstantValueExp(Value(0)));
  for (const auto& unsafe :
       std::vector<Expression>{divide_by_zero, key, ConstantValueExp(Value()),
                               ConstantValueExp(Value(-1).WithUnsigned()),
                               FunctionCallExp("rand", {})}) {
    EXPECT_TRUE(plan_for({negative, unsafe}).inlist_compares.empty());
  }
  // The first matching alternative must still hide a later arithmetic error.
  ASSIGN_OR_ASSERT_FAIL(Value, result,
                        InExpressionExp(key, {negative, divide_by_zero})
                            ->TryEvaluate(Row({Value(-1)}), schema));
  EXPECT_TRUE(result.Truthy());
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

TEST(TypedStreamAggregateTest, LiteralLikeMatchesAstAndRejectsOtherDomains) {
  const Schema schema("input", {Column("s", ValueType::kVarChar)});
  const std::vector<std::string> patterns{"",
                                          "%",
                                          "%%",
                                          "alpha",
                                          "alpha%",
                                          "%alpha",
                                          "%alpha%",
                                          "尾%",
                                          "%尾",
                                          "%a\\b%",
                                          std::string("%a\0b%", 5)};
  const std::vector<Value> values{Value(),
                                  Value(""),
                                  Value("alpha"),
                                  Value("alphaalpha"),
                                  Value("xalpha"),
                                  Value("alphax"),
                                  Value("xalphax"),
                                  Value("ALPHA"),
                                  Value("尾alpha"),
                                  Value("alpha尾"),
                                  Value("a\\b"),
                                  Value(std::string("a\0b", 3)),
                                  Value(std::string("a\0bx", 4))};
  for (const auto op : {BinaryOperation::kLike, BinaryOperation::kNotLike}) {
    for (const auto& pattern : patterns) {
      const auto predicate =
          BinaryExpressionExp(ColumnValueExp("s"), op,
                              ConstantValueExp(Value(std::string(pattern))));
      const auto compiled =
          TryCompileTypedFilterCompare(predicate, schema, true);
      if (!compiled.has_value()) {
        ADD_FAILURE() << "literal LIKE did not compile: " << pattern;
        continue;
      }
      EXPECT_FALSE(TryCompileTypedFilterCompare(predicate, schema, false));
      for (const auto& value : values) {
        const auto expected = predicate->TryEvaluate(Row({value}), schema);
        ASSERT_TRUE(expected.HasValue()) << expected.GetStatus();
        const bool actual = relational_detail::DecideTypedFilterCompareCells(
            *compiled, nullptr, [&](slot_t) { return value.IsNull(); },
            [&](slot_t) { return value; });
        EXPECT_EQ(actual, !expected.Value().IsNull() &&
                              expected.Value().value.int_value != 0)
            << pattern << " / " << value;
      }
    }
  }
  for (const auto& pattern :
       {Value("a_b"), Value("a%b"), Value("%a%b%"), Value("%%%"), Value(),
        Value(1), Value("alpha%").WithCollation(2)}) {
    EXPECT_FALSE(TryCompileTypedFilterCompare(
        BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLike,
                            ConstantValueExp(pattern)),
        schema, true));
  }
  Column interval("s", ValueType::kVarChar);
  interval.SetInterval(true);
  EXPECT_FALSE(TryCompileTypedFilterCompare(
      BinaryExpressionExp(ColumnValueExp("s"), BinaryOperation::kLike,
                          ConstantValueExp(Value("%"))),
      Schema("input", {interval}), true));
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
