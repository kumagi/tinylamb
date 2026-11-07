/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/scan_filter.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::BuildIntegerPeeks;
using relational_detail::CompiledScanFilter;
using relational_detail::CompileScanFilter;
using relational_detail::EvaluateConjunctsTolerant;
using relational_detail::FastLikeMatch;
using relational_detail::FlatIntSet;
using relational_detail::IsNonThrowingScanComparison;
using relational_detail::MatchPrebuiltMembership;
using relational_detail::MatchPrebuiltScalarCompare;
using relational_detail::MatchPrefixIn;
using relational_detail::MatchSimpleCompare;
using relational_detail::PrebuiltMembershipPredicate;
using relational_detail::PrebuiltScalarComparePredicate;
using relational_detail::PrefixInPredicate;
using relational_detail::SimpleComparePredicate;
using relational_detail::TryCompilePrefixIn;
using relational_detail::TryCompileSimpleCompare;
using relational_detail::TryDecideConstantConjunct;

Schema MakeTestSchema() {
  return Schema("test", {
                            Column(ColumnName("id"), ValueType::kInt64),
                            Column(ColumnName("price"), ValueType::kDouble),
                            Column(ColumnName("code"), ValueType::kVarChar),
                            Column(ColumnName("alt_id"), ValueType::kInt64),
                        });
}

TEST(ScanFilterTest, FastLikeMatchAsciiAndWildcards) {
  EXPECT_TRUE(FastLikeMatch("", ""));
  EXPECT_TRUE(FastLikeMatch("abc", "%"));
  EXPECT_TRUE(FastLikeMatch("", "%%%"));
  EXPECT_FALSE(FastLikeMatch("a", ""));
  EXPECT_TRUE(FastLikeMatch("PROMO_123", "PROMO%"));
  EXPECT_FALSE(FastLikeMatch("STANDARD_123", "PROMO%"));
  EXPECT_TRUE(FastLikeMatch("hello_world", "%world"));
  EXPECT_TRUE(FastLikeMatch("green_almond_ metallic", "%green%almond%"));
  EXPECT_FALSE(FastLikeMatch("almond_green", "%green%almond%"));
  EXPECT_TRUE(FastLikeMatch("a1c", "a_c"));
  EXPECT_FALSE(FastLikeMatch("ab1c", "a_c"));
  // Multi-byte UTF-8 is 2 bytes, so byte-wise '_' rejects a 2-byte char.
  EXPECT_FALSE(FastLikeMatch("a\xC3\xA9z", "a_z"));
}

TEST(ScanFilterTest, TryDecideConstantConjunctFoldsCompileTimeConstants) {
  const Expression true_const = ConstantValueExp(Value(true));
  auto decided_true = TryDecideConstantConjunct(true_const);
  ASSERT_TRUE(decided_true.has_value());
  EXPECT_TRUE(decided_true->Truthy());

  const Expression false_math =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kEquals,
                          ConstantValueExp(Value(20)));
  auto decided_false = TryDecideConstantConjunct(false_math);
  ASSERT_TRUE(decided_false.has_value());
  EXPECT_FALSE(decided_false->Truthy());

  // Column reference cannot be decided at compile time.
  const Expression col_pred =
      BinaryExpressionExp(ColumnValueExp(ColumnName("id")),
                          BinaryOperation::kEquals, ConstantValueExp(Value(1)));
  EXPECT_FALSE(TryDecideConstantConjunct(col_pred).has_value());
}

TEST(ScanFilterTest, TryCompileAndMatchSimpleCompare) {
  const Schema schema = MakeTestSchema();

  // INT64 column = constant
  const Expression eq_int = BinaryExpressionExp(
      ColumnValueExp(ColumnName("id")), BinaryOperation::kEquals,
      ConstantValueExp(Value(42)));
  EXPECT_TRUE(IsNonThrowingScanComparison(eq_int, schema));
  auto compiled_int = TryCompileSimpleCompare(eq_int, schema);
  ASSERT_TRUE(compiled_int.has_value());
  EXPECT_EQ(compiled_int->column, 0U);
  EXPECT_TRUE(compiled_int->int_payload);
  EXPECT_EQ(compiled_int->int_constant, 42);

  EXPECT_TRUE(MatchSimpleCompare(
      Row({Value(42), Value(1.5), Value("abc"), Value(42)}), *compiled_int));
  EXPECT_FALSE(MatchSimpleCompare(
      Row({Value(41), Value(1.5), Value("abc"), Value(42)}), *compiled_int));
  EXPECT_FALSE(MatchSimpleCompare(
      Row({Value(), Value(1.5), Value("abc"), Value(42)}), *compiled_int));

  // Flipped constant < DOUBLE column (`10.0 < price` -> `price > 10.0`)
  const Expression lt_double = BinaryExpressionExp(
      ConstantValueExp(Value(10.0)), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("price")));
  auto compiled_dbl = TryCompileSimpleCompare(lt_double, schema);
  ASSERT_TRUE(compiled_dbl.has_value());
  EXPECT_EQ(compiled_dbl->column, 1U);
  EXPECT_EQ(compiled_dbl->op, BinaryOperation::kGreaterThan);
  EXPECT_TRUE(compiled_dbl->double_payload);
  EXPECT_TRUE(MatchSimpleCompare(
      Row({Value(1), Value(12.5), Value("x"), Value(1)}), *compiled_dbl));
  EXPECT_FALSE(MatchSimpleCompare(
      Row({Value(1), Value(9.5), Value("x"), Value(1)}), *compiled_dbl));

  // Column-vs-column compare (`id < alt_id`)
  const Expression col_vs_col = BinaryExpressionExp(
      ColumnValueExp(ColumnName("id")), BinaryOperation::kLessThan,
      ColumnValueExp(ColumnName("alt_id")));
  auto compiled_col = TryCompileSimpleCompare(col_vs_col, schema);
  ASSERT_TRUE(compiled_col.has_value());
  EXPECT_TRUE(compiled_col->column_payload);
  EXPECT_EQ(compiled_col->column, 0U);
  EXPECT_EQ(compiled_col->right_column, 3U);
  EXPECT_TRUE(MatchSimpleCompare(
      Row({Value(5), Value(0.0), Value("x"), Value(10)}), *compiled_col));
  EXPECT_FALSE(MatchSimpleCompare(
      Row({Value(10), Value(0.0), Value("x"), Value(5)}), *compiled_col));
}

TEST(ScanFilterTest, TryCompileAndMatchPrefixIn) {
  const Schema schema = MakeTestSchema();
  const Expression substr_call = FunctionCallExp(
      "SUBSTR", {ColumnValueExp(ColumnName("code")), ConstantValueExp(Value(1)),
                 ConstantValueExp(Value(2))});
  const Expression in_expr = std::make_shared<InExpression>(
      substr_call, std::vector<Expression>{ConstantValueExp(Value("13")),
                                           ConstantValueExp(Value("31")),
                                           ConstantValueExp(Value("23"))});

  auto prefix_in = TryCompilePrefixIn(in_expr, schema);
  ASSERT_TRUE(prefix_in.has_value());
  EXPECT_EQ(prefix_in->column, 2U);
  ASSERT_EQ(prefix_in->prefixes.size(), 3U);

  EXPECT_TRUE(MatchPrefixIn(
      Row({Value(1), Value(1.0), Value("31-555-1234"), Value(1)}), *prefix_in));
  EXPECT_FALSE(MatchPrefixIn(
      Row({Value(1), Value(1.0), Value("99-555-1234"), Value(1)}), *prefix_in));
  EXPECT_FALSE(MatchPrefixIn(Row({Value(1), Value(1.0), Value("3"), Value(1)}),
                             *prefix_in));  // shorter than prefix
  EXPECT_FALSE(MatchPrefixIn(Row({Value(1), Value(1.0), Value(), Value(1)}),
                             *prefix_in));
}

TEST(ScanFilterTest, MatchPrebuiltMembershipThreeValuedAndExistence) {
  auto set = std::make_shared<FlatIntSet>();
  set->Reserve(4);
  set->Insert(10);
  set->Insert(20);

  PrebuiltMembershipPredicate in_pred{
      .column = 0,
      .negated = false,
      .domain_type = ValueType::kInt64,
      .values = set,
      .runtime_filter = nullptr,
      .contains_null = false,
      .existence = false,
  };
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value(10)}), in_pred));
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value(30)}), in_pred));
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value()}), in_pred));

  // NOT IN without NULL in build set: clean miss returns TRUE, NULL probe
  // returns FALSE (UNKNOWN).
  PrebuiltMembershipPredicate not_in_pred = in_pred;
  not_in_pred.negated = true;
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value(10)}), not_in_pred));
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value(30)}), not_in_pred));
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value()}), not_in_pred));

  // NOT IN with NULL in build set: every miss is UNKNOWN -> FALSE.
  PrebuiltMembershipPredicate not_in_with_null = not_in_pred;
  not_in_with_null.contains_null = true;
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value(30)}), not_in_with_null));

  // NOT EXISTS (existence = true, negated = true, contains_null = false):
  // two-valued, so a NULL probe still keeps the outer row!
  PrebuiltMembershipPredicate not_exists_pred = not_in_pred;
  not_exists_pred.existence = true;
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value(30)}), not_exists_pred));
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value()}), not_exists_pred));
}

TEST(ScanFilterTest, MatchPrebuiltScalarCompare) {
  auto map = std::make_shared<std::unordered_map<int64_t, Value>>();
  (*map)[1] = Value(100.0);
  (*map)[2] = Value();  // NULL group aggregate result

  PrebuiltScalarComparePredicate pred{
      .column = 1,
      .key_column = 0,
      .op = BinaryOperation::kLessThan,
      .key_type = ValueType::kInt64,
      .values = map,
  };
  EXPECT_TRUE(MatchPrebuiltScalarCompare(Row({Value(1), Value(50.0)}), pred));
  EXPECT_FALSE(MatchPrebuiltScalarCompare(Row({Value(1), Value(150.0)}), pred));
  // Missing key -> false
  EXPECT_FALSE(MatchPrebuiltScalarCompare(Row({Value(99), Value(50.0)}), pred));
  // NULL group value -> false
  EXPECT_FALSE(MatchPrebuiltScalarCompare(Row({Value(2), Value(50.0)}), pred));
  // NULL probe value -> false
  EXPECT_FALSE(MatchPrebuiltScalarCompare(Row({Value(1), Value()}), pred));
}

TEST(ScanFilterTest, EvaluateConjunctsTolerantSuppressesSiblingErrorsOnReject) {
  const Expression pass_expr = ConstantValueExp(Value(true));
  const Expression reject_expr = ConstantValueExp(Value(false));
  const Expression error_expr = ConstantValueExp(Value(42));  // sentinel

  auto eval = [&](const Expression& e) -> StatusOr<Value> {
    if (e == error_expr) {
      return Status(StatusCode::kRuntimeError, "division by zero");
    }
    return e->TryEvaluate(Row{}, Schema{});
  };

  // Error + FALSE -> FALSE suppresses the error.
  StatusOr<bool> suppressed =
      EvaluateConjunctsTolerant({error_expr, reject_expr}, eval);
  ASSERT_TRUE(suppressed.HasValue());
  EXPECT_FALSE(suppressed.Value());

  // Error + TRUE -> error surfaces.
  StatusOr<bool> surfaced =
      EvaluateConjunctsTolerant({pass_expr, error_expr}, eval);
  EXPECT_FALSE(surfaced.HasValue());
}

TEST(ScanFilterTest, BuildIntegerPeeksRemapsProjectedColumns) {
  const Schema full_schema = MakeTestSchema();
  const Schema projected_schema(
      "test", {Column(ColumnName("code"), ValueType::kVarChar),
               Column(ColumnName("alt_id"), ValueType::kInt64)});
  const std::vector<slot_t> projection = {2, 3};

  const Expression pred = BinaryExpressionExp(
      ColumnValueExp(ColumnName("alt_id")), BinaryOperation::kGreaterThanEquals,
      ConstantValueExp(Value(500)));
  const CompiledScanFilter filter =
      CompileScanFilter({pred}, projected_schema, nullptr);
  const auto peeks = BuildIntegerPeeks(filter, &projection, full_schema);
  ASSERT_EQ(peeks.size(), 1U);
  EXPECT_EQ(peeks[0].column, 3U);
  EXPECT_EQ(peeks[0].constant, 500);
}

}  // namespace
}  // namespace tinylamb
