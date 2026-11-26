/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/typed_stream_aggregate.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
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
using relational_detail::EvaluateTypedIntExprOnRow;
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
using relational_detail::TypedIntExprInput;
using relational_detail::TypedIntExprOp;

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

TEST(TypedStreamAggregateTest, VarcharRunEndOffsetsCrossFourGiB) {
  if (sizeof(size_t) <= sizeof(uint32_t)) {
    GTEST_SKIP() << "The backing vector cannot exceed 4 GiB on this platform";
  }
  relational_detail::BaseRelationArena::VarcharRun run;
  const auto four_gib = static_cast<size_t>(uint64_t{1} << 32);
  // Exercise the rebase used by both append paths without reserving 4 GiB.
  run.AppendEnd(four_gib - 3, 1);
  run.AppendEnd(four_gib - 3, 5);
  run.AppendEnd(four_gib + 2, 7);
  EXPECT_EQ(run.ends,
            (std::vector<size_t>{four_gib - 2, four_gib + 2, four_gib + 9}));
}

TEST(TypedStreamAggregateTest, VarcharRunsKeepSlicedBatchBoundariesAfterMerge) {
  const auto make_arena = [] {
    relational_detail::BaseRelationArena arena;
    arena.stride = 1;
    arena.slot_types = {ValueType::kVarChar};
    arena.null_bits.resize(1);
    arena.varchars.resize(1);
    return arena;
  };
  TypedCellBatch batch;
  batch.Configure(1, {ValueType::kVarChar}, {false});
  batch.cells = {0, 0, 0};
  batch.nulls = {0, 0, 0};
  batch.varchar_bytes[0] = {'a', 'b', 'c'};
  batch.varchar_ends[0] = {1, 1, 3};
  batch.rows = 3;
  auto combined = make_arena();
  relational_detail::AppendBatchToArena(batch, 0, 1, &combined);
  auto second = make_arena();
  relational_detail::AppendBatchToArena(batch, 1, 2, &second);
  combined.AppendTyped(second);
  EXPECT_EQ(combined.Varchar(0, 0), "a");
  EXPECT_EQ(combined.Varchar(1, 0), "");
  EXPECT_EQ(combined.Varchar(2, 0), "bc");
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
        EXPECT_EQ(error, expected.GetStatus());
        EXPECT_EQ(ToString(error), ToString(expected.GetStatus()));
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
  if (!compiled.has_value()) {
    ADD_FAILURE() << "expected optional value";
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
  const std::vector<std::string> patterns{
      "",        "%",   "%%",  "alpha", "alpha%", "%alpha",
      "%alpha%", "尾%", "%尾", "%a%b%", "%a%%b%", std::string("%a\0b%", 5)};
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
                                  Value("a-middle-b"),
                                  Value("b-middle-a"),
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
       {Value("a_b"), Value("a%b"), Value("%a\\b%"), Value("%%%"), Value(),
        Value(1), Value("alpha%").WithCollation(2)}) {
    SCOPED_TRACE(pattern.AsString());
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
  if (!compiled.has_value()) {
    ADD_FAILURE() << "expected optional value";
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
  if (!compiled_single.has_value()) {
    ADD_FAILURE() << "expected optional value";
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
  if (!compiled_mul2.has_value()) {
    ADD_FAILURE() << "expected optional value";
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
  if (!compiled_disc.has_value()) {
    ADD_FAILURE() << "expected optional value";
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
  if (!compiled_charge.has_value()) {
    ADD_FAILURE() << "expected optional value";
    return;
  }
  EXPECT_EQ(compiled_charge->shape,
            TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol);
  EXPECT_NEAR(
      EvaluateTypedDoubleExprOnRow(*compiled_charge, row).value.double_value,
      189.0, 1e-9);

  // A NULL factor leaves the typed double expression NULL; COALESCE(expr, 0)
  // is intentionally not folded here, because the general COALESCE returns
  // the fallback's own type (an INT64 0), which the dynamic SUM typing keeps
  // observable. Compiling it as a double 0 would change that result type.
  const Row null_price(
      {Value(1), Value(10), Value(), Value(0.1), Value(0.05), Value("AIR")});
  EXPECT_TRUE(
      EvaluateTypedDoubleExprOnRow(*compiled_mul2, null_price).IsNull());
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
  if (!single.has_value()) {
    ADD_FAILURE() << "expected optional value";
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

// The precomputed-Shape merge overload used by OOC sinks must be equivalent
// to the one that analyzes the expression on every call.
TEST(TypedStreamAggregateTest,
     MergeSerializedPartialStateWithPrecomputedShape) {
  const std::vector<AggregationType> kinds = {
      AggregationType::kSum, AggregationType::kAvg, AggregationType::kMin,
      AggregationType::kMax};
  for (const AggregationType kind : kinds) {
    const Expression expr = std::make_shared<AggregateExpression>(
        kind, ColumnValueExp(ColumnName("x")));
    const auto* agg = &expr->AsAggregateExpression();
    const AggregateAccumulator::Shape shape =
        AggregateAccumulator::AnalyzeShape(agg, nullptr);

    AggregateAccumulator lhs_with_shape(agg, shape);
    AggregateAccumulator lhs_without_shape(agg);
    AggregateAccumulator partial(agg);
    ASSERT_EQ(lhs_with_shape.Add(Value(int64_t{11})), Status::kSuccess);
    ASSERT_EQ(lhs_without_shape.Add(Value(int64_t{11})), Status::kSuccess);
    ASSERT_EQ(partial.Add(Value(int64_t{27})), Status::kSuccess);
    ASSERT_EQ(partial.Add(Value(int64_t{3})), Status::kSuccess);

    std::string blob;
    ASSERT_EQ(partial.SerializePartialState(&blob), Status::kSuccess);
    std::string_view with_shape_view = blob;
    std::string_view without_shape_view = blob;
    ASSERT_EQ(
        lhs_with_shape.MergeSerializedPartialState(&with_shape_view, shape),
        Status::kSuccess);
    ASSERT_EQ(
        lhs_without_shape.MergeSerializedPartialState(&without_shape_view),
        Status::kSuccess);
    EXPECT_TRUE(with_shape_view.empty());
    EXPECT_TRUE(without_shape_view.empty());

    auto a = lhs_with_shape.TryFinish();
    auto b = lhs_without_shape.TryFinish();
    ASSERT_TRUE(a.HasValue());
    ASSERT_TRUE(b.HasValue());
    EXPECT_EQ(a.Value(), b.Value());
  }
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

// AccumulatorChunks commits backing store in kSegmentSlots-sized segments,
// so a small group count must not commit the full 512-slot (~440KB) slab.
TEST(TypedStreamAggregateTest, AccumulatorChunksCommitsStorageInSegments) {
  const Expression sum_expr = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp(ColumnName("x")));
  const AggregateExpression* aggregate = &sum_expr->AsAggregateExpression();
  const std::vector<const AggregateExpression*> aggregates = {aggregate};

  relational_detail::AccumulatorChunks chunks;
  chunks.Configure(1);

  // Ten groups x one aggregate = ten slots: only the first segment of the
  // first chunk may be committed, not the whole 512-slot slab.
  for (size_t i = 0; i < 10; ++i) {
    chunks.AppendGroup(aggregates, nullptr);
  }
  EXPECT_EQ(chunks.CommittedBytes(),
            relational_detail::AccumulatorChunks::kSegmentBytes);
  EXPECT_EQ(chunks.CommittedBytes(),
            relational_detail::AccumulatorChunks::kSegmentSlots *
                sizeof(AggregateAccumulator));

  // The accumulators are still live and writable through the same indexing.
  EXPECT_EQ(chunks.State(0, 0).Add(Value(int64_t{7})), Status::kSuccess);
  EXPECT_EQ(chunks.State(9, 0).Add(Value(int64_t{9})), Status::kSuccess);

  // Fill past the first segment and past the first chunk: commitment must
  // track usage, then cap at the whole slab per chunk.
  for (size_t i = 10; i < 70; ++i) {
    chunks.AppendGroup(aggregates, nullptr);
  }
  EXPECT_EQ(chunks.CommittedBytes(),
            2 * relational_detail::AccumulatorChunks::kSegmentBytes);
  for (size_t i = 70; i < 512 + 10; ++i) {
    chunks.AppendGroup(aggregates, nullptr);
  }
  EXPECT_EQ(chunks.CommittedBytes(),
            // NOLINTNEXTLINE(readability-math-missing-parentheses) precedence
            // is unambiguous
            (relational_detail::AccumulatorChunks::kSlotsPerChunk *
             sizeof(AggregateAccumulator)) +
                relational_detail::AccumulatorChunks::kSegmentBytes);
  EXPECT_EQ(chunks.State(511, 0).Add(Value(int64_t{5})), Status::kSuccess);
  EXPECT_EQ(chunks.State(520, 0).Add(Value(int64_t{6})), Status::kSuccess);
  EXPECT_EQ(chunks.State(0, 0).TryFinish().Value(), Value(int64_t{7}));
  EXPECT_EQ(chunks.State(9, 0).TryFinish().Value(), Value(int64_t{9}));
  EXPECT_EQ(chunks.State(511, 0).TryFinish().Value(), Value(int64_t{5}));
  EXPECT_EQ(chunks.State(520, 0).TryFinish().Value(), Value(int64_t{6}));
}

// Builds a TLCS-compacted k/v table so the GROUP BY over it runs the
// columnar typed-stream compact aggregation kernel.
struct TypedStreamColumnarKvFixture {
  static void Build(Database* db, const std::string& name,
                    const std::vector<int64_t>& ks,
                    const std::vector<int64_t>& vs);
};

// static
void TypedStreamColumnarKvFixture::Build(Database* db, const std::string& name,
                                         const std::vector<int64_t>& ks,
                                         const std::vector<int64_t>& vs) {
  TransactionContext ctx = db->BeginContext();
  Schema schema(
      name, {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
  Table tbl = db->GetTable(ctx, name).MoveValue();
  for (size_t i = 0; i < vs.size(); ++i) {
    ASSERT_TRUE(
        tbl.Insert(ctx.txn_, Row({Value(ks[i]), Value(vs[i])})).HasValue());
  }
  ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  TransactionContext compact_ctx = db->BeginContext();
  ASSERT_EQ(db->CompactTableToTlcs(compact_ctx, name), Status::kSuccess);
  ASSERT_EQ(compact_ctx.txn_.PreCommit(), Status::kSuccess);
}

// Runs `sql` and reports executor completion status (kSuccess on a clean
// drain). Prepare failures surface through *status too.
std::vector<Row> RunGroupedQuery(SqlEngine* engine, TransactionContext* ctx,
                                 const std::string& sql, Status* status) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    *status = prepared.GetStatus();
    return {};
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(std::move(row));
    row = Row();
  }
  *status = prepared.Value()->GetStatus();
  return rows;
}

// Bug #543: the partitioned columnar aggregation folded a group's rows in
// worker-major order over per-shard buffers, so whether SUM(INT64) overflowed
// (and with what value) depended on how blocks were handed to workers. These
// patterns put the overflow (a) exactly across a TLCS block boundary the fold
// must still report, and (b) inside a group where the canonical row-order sum
// is legal even though M + M wraps locally. The checked SUM keeps the kernel
// on a single worker, so every run must reproduce the canonical answer; the
// query repeats below catch a regression that only some block hand-offs
// expose.
TEST(TypedStreamAggregateTest, SkewedGroupedSumInt64KeepsCanonicalOverflow) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  // 3 TLCS blocks (40,000 rows each in the fixture's compaction); markers sit
  // across the 40,000-row block boundary so a worker-boundary recombination
  // cannot hide or fabricate the overflow.
  constexpr int64_t kRows = 120003;
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  const int64_t kMin = std::numeric_limits<int64_t>::min();

  const std::string prefix = "tsagg_skew_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();

  // Case A: rows 0 and 40000 are INT64_MAX, row 40001 is INT64_MIN. The
  // canonical row-order sum overflows at row 40000 (M + M), and the
  // block-boundary fold must still report it.
  std::vector<int64_t> ks_a(kRows, 1);
  std::vector<int64_t> vs_a(kRows, 0);
  vs_a[0] = kMax;
  vs_a[40000] = kMax;
  vs_a[40001] = kMin;
  TypedStreamColumnarKvFixture::Build(db.get(), "ta", ks_a, vs_a);

  // Case B: rows 0 and 40002 are INT64_MIN, rows 40000 and 40001 are
  // INT64_MAX. The canonical running sums (−2^63, ..., M, M−1, −2) never
  // leave int64 — even though a fold that sees rows 40000/40001 without the
  // leading INT64_MIN wraps — and the final group-1 sum is exactly −2. The
  // last row joins group 7 (sum 5) so a second group is exercised too.
  std::vector<int64_t> ks_b(kRows, 1);
  std::vector<int64_t> vs_b(kRows, 0);
  vs_b[0] = kMin;
  vs_b[40000] = kMax;
  vs_b[40001] = kMax;
  vs_b[40002] = kMin;
  ks_b[kRows - 1] = 7;
  vs_b[kRows - 1] = 5;
  TypedStreamColumnarKvFixture::Build(db.get(), "tb", ks_b, vs_b);

  TransactionContext ctx = db->BeginReadOnlyContext();
  SqlEngine engine(*db);
  Status status = Status::kSuccess;
  const std::string sql_b = "SELECT k, SUM(v) FROM tb GROUP BY k";

  for (int attempt = 0; attempt < 4; ++attempt) {
    auto rows_a = RunGroupedQuery(
        &engine, &ctx, "SELECT k, SUM(v) FROM ta GROUP BY k", &status);
    EXPECT_EQ(status, Status::kIsInfinity)
        << "attempt " << attempt << ": block-boundary fold must report the "
        << "canonical SUM overflow";

    auto rows_b = RunGroupedQuery(&engine, &ctx, sql_b, &status);
    EXPECT_EQ(status, Status::kSuccess) << "attempt " << attempt;
    EXPECT_EQ(rows_b.size(), 2U);
    std::sort(rows_b.begin(), rows_b.end(), [](const Row& x, const Row& y) {
      return x[0].AsInt64() < y[0].AsInt64();
    });
    ASSERT_EQ(rows_b.size(), 2U);
    EXPECT_EQ(rows_b[0][0].AsInt64(), 1);
    EXPECT_EQ(rows_b[0][1].AsInt64(), -2);
    EXPECT_EQ(rows_b[1][0].AsInt64(), 7);
    EXPECT_EQ(rows_b[1][1].AsInt64(), 5);
  }

  // The canonical (typed-aggregation disabled) path must agree exactly.
  ScopedEnvVar disable_typed("TINYLAMB_DISABLE_TYPED_AGG", "1");
  auto canonical = RunGroupedQuery(&engine, &ctx, sql_b, &status);
  disable_typed.Set(nullptr);
  EXPECT_EQ(status, Status::kSuccess);
  ASSERT_EQ(canonical.size(), 2U);
  std::sort(canonical.begin(), canonical.end(), [](const Row& x, const Row& y) {
    return x[0].AsInt64() < y[0].AsInt64();
  });
  EXPECT_EQ(canonical[0][0].AsInt64(), 1);
  EXPECT_EQ(canonical[0][1].AsInt64(), -2);
  EXPECT_EQ(canonical[1][0].AsInt64(), 7);
  EXPECT_EQ(canonical[1][1].AsInt64(), 5);

  db->DeleteAll();
}

// P596: the scalar narrow-dictionary shortcut folded AVG(INT64) as
// `double(entry_value) * count` per dict entry, re-associating the canonical
// row-ordered double fold (aggregate_accumulator's `total += double(v)`), so
// the same SQL could differ in ULPs with plan shape.  The shortcut now
// defers once the exact Σ|v| leaves the always-exact 2^53 integer band;
// typed and canonical folds must agree bit-for-bit.
TEST(TypedStreamAggregateTest, ScalarDictAvgMatchesRowOrderedFold) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  constexpr int64_t kRows = 120003;
  constexpr int64_t kPow2_53 = int64_t{1} << 53;
  const std::string prefix = "tsagg_avg596_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  std::vector<int64_t> ks(kRows, 1);
  std::vector<int64_t> vs(kRows, 1);
  vs[0] = kPow2_53;
  vs[kRows / 2] = kPow2_53;
  TypedStreamColumnarKvFixture::Build(db.get(), "tc", ks, vs);

  TransactionContext ctx = db->BeginReadOnlyContext();
  SqlEngine engine(*db);
  Status status = Status::kSuccess;
  const std::vector<Row> typed =
      RunGroupedQuery(&engine, &ctx, "SELECT AVG(v) FROM tc", &status);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(typed.size(), 1U);

  ScopedEnvVar disable_typed("TINYLAMB_DISABLE_TYPED_AGG", "1");
  const std::vector<Row> canonical =
      RunGroupedQuery(&engine, &ctx, "SELECT AVG(v) FROM tc", &status);
  disable_typed.Set(nullptr);
  ASSERT_EQ(status, Status::kSuccess);
  ASSERT_EQ(canonical.size(), 1U);

  EXPECT_EQ(typed[0][0].value.double_value, canonical[0][0].value.double_value)
      << "AVG must not depend on fold order (P596 re-association)";
  db->DeleteAll();
}
// the scalar domain's canonical kInvalidArgument "integer overflow on
// '<op>'" (type/value.cpp / expression/bytecode.cpp).  It previously raised
// kIsInfinity, a code reserved for SUM-accumulation overflow
// (executor/aggregation.cpp family).
TEST(TypedStreamAggregateTest, TypedColumnEvalOverflowUsesCanonicalStatus) {
  const int64_t kMax = std::numeric_limits<int64_t>::max();
  const int64_t kMin = std::numeric_limits<int64_t>::min();
  const Row row({Value(kMax), Value(kMin)});

  TypedIntExprOp load0;
  load0.kind = TypedIntExprOp::Kind::kLoadIntCol;
  load0.column = 0;
  TypedIntExprOp load1;
  load1.kind = TypedIntExprOp::Kind::kLoadIntCol;
  load1.column = 1;
  TypedIntExprOp two;
  two.kind = TypedIntExprOp::Kind::kConstant;
  two.constant = 2;

  const auto run = [](std::vector<TypedIntExprOp> ops, const Row& r) {
    TypedIntExprInput input;
    input.shape = TypedIntExprInput::Shape::kGeneric;
    input.ops = std::move(ops);
    return EvaluateTypedIntExprOnRow(input, r);
  };

  const StatusOr<Value> mul =
      run({load0, two, {TypedIntExprOp::Kind::kMul}}, row);
  ASSERT_FALSE(mul.HasValue());
  EXPECT_EQ(mul.GetStatus(), Status::kInvalidArgument);
  EXPECT_EQ(ToString(mul.GetStatus()),
            "InvalidArgument: integer overflow on '*'");

  const StatusOr<Value> add =
      run({load0, load0, {TypedIntExprOp::Kind::kAdd}}, row);
  ASSERT_FALSE(add.HasValue());
  EXPECT_EQ(add.GetStatus(), Status::kInvalidArgument);

  const StatusOr<Value> sub =
      run({load0, load1, {TypedIntExprOp::Kind::kSub}}, row);
  ASSERT_FALSE(sub.HasValue());
  EXPECT_EQ(sub.GetStatus(), Status::kInvalidArgument);

  // Sanity: in-domain programs still evaluate.
  const StatusOr<Value> ok = run({load0, load1, {TypedIntExprOp::Kind::kSub}},
                                 Row({Value(kMax), Value(1)}));
  ASSERT_TRUE(ok.HasValue()) << ToString(ok.GetStatus());
  EXPECT_EQ(ok.Value().AsInt64(), kMax - 1);
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean
// (InRangeSynopsis_sound).
// Verifies scalar IN-list + integer range folds (both 64-bit bitmask and
// wide-span list, plus empty-match zone-map/bloom skipping and NULL handling)
// match the canonical row-ordered fold bit-for-bit.
TEST(TypedStreamAggregateTest, ScalarInRangeSynopsisMatchesCanonicalFold) {
  if (!GoogleSqlFrontend::Available()) {
    GTEST_SKIP() << "execute_query unavailable";
  }
  const std::string prefix = "tsagg_inrange_" + RandomString();
  std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
  Schema schema(
      "x", {Column("id1", ValueType::kInt64), Column("id2", ValueType::kInt64),
            Column("v1", ValueType::kInt64)});
  {
    TransactionContext ctx = db->BeginContext();
    ASSERT_EQ(db->CreateTable(ctx, schema).GetStatus(), Status::kSuccess);
    Table table = db->GetTable(ctx, "x").MoveValue();
    for (int64_t i = 0; i < 20000; ++i) {
      Value id1 = (i % 37 == 0) ? Value() : Value((i % 100) + 1);
      Value id2 = (i % 53 == 0) ? Value() : Value(i % 100);
      Value v1 = (i % 29 == 0) ? Value() : Value(((i * 17) % 1000) - 250);
      if (i % 1000 == 0) {
        id1 = Value(int64_t{50000});
      }
      ASSERT_TRUE(table.Insert(ctx.txn_, Row(std::vector<Value>{id1, id2, v1}))
                      .HasValue());
    }
    ASSERT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
  }

  SqlEngine engine(*db);
  const std::vector<std::string> queries = {
      "SELECT COUNT(*) AS n, COALESCE(SUM(v1), 0) AS s "
      "FROM x WHERE id1 IN (1, 2, 3, 7, 11) AND id2 BETWEEN 10 AND 40",
      "SELECT COUNT(*) AS n, COALESCE(SUM(v1), 0) AS s "
      "FROM x WHERE id1 IN (1, 50000) AND id2 BETWEEN 10 AND 40",
      "SELECT COUNT(*) AS n, COALESCE(SUM(v1), 0) AS s "
      "FROM x WHERE id1 IN (999999, 1000000) AND id2 BETWEEN 10 AND 40",
  };

  const auto verify_all = [&]() {
    TransactionContext ctx = db->BeginReadOnlyContext();
    for (const std::string& sql : queries) {
      Status status = Status::kSuccess;
      const std::vector<Row> fast =
          RunGroupedQuery(&engine, &ctx, sql, &status);
      ASSERT_EQ(status, Status::kSuccess) << sql;
      ASSERT_EQ(fast.size(), 1U) << sql;

      ScopedEnvVar disable_typed("TINYLAMB_DISABLE_TYPED_AGG", "1");
      const std::vector<Row> canonical =
          RunGroupedQuery(&engine, &ctx, sql, &status);
      disable_typed.Set(nullptr);
      ASSERT_EQ(status, Status::kSuccess) << sql;
      ASSERT_EQ(canonical.size(), 1U) << sql;

      EXPECT_EQ(fast[0], canonical[0]) << sql;
    }
  };

  // 1. Direct LSTableStore path (CompiledLSTableBlockFilter::IntInList).
  verify_all();

  // 2. Compacted TLCS path (FastNumericPred::Kind::kIntInMask64 / kIntInList).
  {
    TransactionContext compact_ctx = db->BeginContext();
    ASSERT_EQ(db->CompactTableToTlcs(compact_ctx, "x"), Status::kSuccess);
    ASSERT_EQ(compact_ctx.txn_.PreCommit(), Status::kSuccess);
  }
  verify_all();
  db->DeleteAll();
}

}  // namespace
}  // namespace tinylamb
