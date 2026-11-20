/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/scan_filter.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
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

TEST(FlatIntSetAuditTest, RejectsOutOfRangeDenseWrites) {
  FlatIntSet set;
  ASSERT_TRUE(set.TryReserveDense(-2, 2));
  set.Insert(-2);
  set.Insert(2);
  set.Insert(INT64_MIN);
  set.Insert(INT64_MAX);
  EXPECT_TRUE(set.DenseOverflowed());
  EXPECT_EQ(set.Size(), 2U);
  EXPECT_FALSE(set.Contains(INT64_MIN));
  EXPECT_FALSE(set.Contains(INT64_MAX));
  ASSERT_TRUE(set.TryReserveDense(INT64_MAX - 2, INT64_MAX));
  set.InsertDenseAtomic(INT64_MAX);
  set.InsertDenseAtomic(INT64_MIN);
  set.FinalizeDenseBounds();
  EXPECT_TRUE(set.DenseOverflowed());
  EXPECT_EQ(set.Size(), 1U);
  EXPECT_TRUE(set.Contains(INT64_MAX));
  EXPECT_FALSE(set.Contains(INT64_MIN));
  set.Reserve(4);
  EXPECT_FALSE(set.DenseOverflowed());
}

// A dense set keeps no hash slots: a block range wider than eight bitmap
// words must still be answered from the bitmap (it used to return false,
// which made an IN-subquery probe scan skip whole blocks).
TEST(FlatIntSetAuditTest, DenseContainsAnyInRangeSpansManyWords) {
  FlatIntSet set;
  ASSERT_TRUE(set.TryReserveDense(3, 5076));
  for (int64_t k = 3; k <= 5076; k += 3) {
    set.Insert(k);
  }
  EXPECT_TRUE(set.ContainsAnyInRange(1, 746));
  EXPECT_TRUE(set.ContainsAnyInRange(747, 897));
  EXPECT_TRUE(set.ContainsAnyInRange(-100, 10000));
  EXPECT_FALSE(set.ContainsAnyInRange(4, 5));
  EXPECT_FALSE(set.ContainsAnyInRange(5077, 9000));
  FlatIntSet sparse;
  ASSERT_TRUE(sparse.TryReserveDense(0, 4000));
  sparse.Insert(0);
  sparse.Insert(4000);
  EXPECT_FALSE(sparse.ContainsAnyInRange(1, 3999));
  EXPECT_TRUE(sparse.ContainsAnyInRange(1, 4000));
}

TEST(FlatIntSetAuditTest, BufferedDenseWritersKeepOverlapsAndPartialWords) {
  constexpr int64_t span = 300;
  for (int64_t base : {INT64_MIN, int64_t{-130}, INT64_MAX - span}) {
    FlatIntSet set;
    ASSERT_TRUE(set.TryReserveDense(base, base + span));
    {
      std::vector<std::jthread> workers;
      workers.reserve(3);
      for (int64_t worker = 0; worker < 3; ++worker) {
        workers.emplace_back([&, worker] {
          FlatIntSet::DenseWriter writer(set);
          for (int64_t n = 0; n <= span; ++n) {
            const int64_t i = worker == 1 ? span - n : n;
            if (i % 5 >= 3) {
              continue;
            }
            if (worker == 2) {
              set.InsertDenseAtomic(base + i);
            } else {
              writer.Insert(base + i);
              writer.Insert(base + i);
            }
          }
        });
      }
    }
    set.FinalizeDenseBounds();
    EXPECT_FALSE(set.DenseOverflowed());
    size_t expected = 0;
    for (int64_t i = 0; i <= span; ++i) {
      EXPECT_EQ(set.Contains(base + i), i % 5 < 3);
      expected += static_cast<size_t>(i % 5 < 3);
    }
    EXPECT_EQ(set.Size(), expected);
    {
      FlatIntSet::DenseWriter writer(set);
      writer.Insert(base == INT64_MIN ? INT64_MAX : INT64_MIN);
    }
    EXPECT_TRUE(set.DenseOverflowed());
  }
}

TEST(FlatIntSetAuditTest, SortedBufferedInsertMatchesPerKeyAtomic) {
  // T1231: the unordered runtime-filter build path now buffers the worker's
  // keys and streams them sorted through DenseWriter; that must be
  // bit-identical to per-key InsertDenseAtomic on the shuffled order.
  constexpr int64_t base = -5000;
  constexpr int64_t span = 10000;
  std::vector<int64_t> keys;
  keys.reserve(4096);
  uint64_t rng = 0x243F6A8885A308D3ULL;
  for (int i = 0; i < 4096; ++i) {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    keys.push_back(base + static_cast<int64_t>(rng % (span + 1)));
  }
  FlatIntSet atomic_set;
  ASSERT_TRUE(atomic_set.TryReserveDense(base, base + span));
  for (const int64_t key : keys) {
    atomic_set.InsertDenseAtomic(key);
  }
  atomic_set.FinalizeDenseBounds();

  FlatIntSet sorted_set;
  ASSERT_TRUE(sorted_set.TryReserveDense(base, base + span));
  std::vector<int64_t> buf = keys;
  std::ranges::sort(buf);
  buf.erase(std::ranges::unique(buf).begin(), buf.end());
  {
    FlatIntSet::DenseWriter writer(sorted_set);
    for (const int64_t key : buf) {
      writer.Insert(key);
    }
  }
  sorted_set.FinalizeDenseBounds();
  EXPECT_FALSE(sorted_set.DenseOverflowed());
  EXPECT_EQ(sorted_set.Size(), atomic_set.Size());
  for (int64_t i = 0; i <= span; ++i) {
    ASSERT_EQ(sorted_set.Contains(base + i), atomic_set.Contains(base + i))
        << "i=" << i;
  }
}

TEST(FlatIntSetAuditTest, WideDenseMembershipKeepsHolesAndSignedEndpoints) {
  constexpr int64_t span = 60000000;
  for (const int64_t base : {INT64_MIN, int64_t{-30000000}, INT64_MAX - span}) {
    FlatIntSet set;
    ASSERT_TRUE(set.TryReserveDense(base, base + span));
    ASSERT_TRUE(set.IsDenseMode());
    {
      std::vector<std::jthread> workers;
      workers.reserve(3);
      for (int64_t w = 0; w < 3; ++w) {
        workers.emplace_back([&, w] {
          for (int64_t i = w; i <= span / 1000; i += 3) {
            set.InsertDenseAtomic(base + (i * 1000));
            set.InsertDenseAtomic(base + (i * 1000));
          }
        });
      }
    }
    set.FinalizeDenseBounds();
    EXPECT_FALSE(set.DenseOverflowed());
    EXPECT_EQ(set.Size(), span / 1000 + 1);  // NOLINT(readability-math-missing-parentheses)
    for (int64_t i = 0; i <= span / 1000; ++i) {
      EXPECT_TRUE(set.Contains(base + i * 1000));  // NOLINT(readability-math-missing-parentheses)
      if (i != span / 1000) {
        EXPECT_FALSE(set.Contains(base + i * 1000 + 1));  // NOLINT(readability-math-missing-parentheses)
      }
    }
    if (base > INT64_MIN) {
      EXPECT_FALSE(set.Contains(base - 1));
    }
    if (base + span < INT64_MAX) {
      EXPECT_FALSE(set.Contains(base + span + 1));
    }
  }
  FlatIntSet too_wide;
  EXPECT_FALSE(too_wide.TryReserveDense(INT64_MIN, INT64_MAX));
  EXPECT_FALSE(too_wide.TryReserveDense(0, FlatIntSet::kMaxDenseSpan + 1));
}

TEST(ScanFilterTest, DateStringConstantsParseOnceWithoutChangingOtherDomains) {
  const Schema schema("", {Column("d", ValueType::kDate)});
  const Value literal("2000-06-30");
  const Value day = Value::DateFromDays(11138);
  for (const auto op :
       {BinaryOperation::kEquals, BinaryOperation::kNotEquals,
        BinaryOperation::kLessThan, BinaryOperation::kLessThanEquals,
        BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals}) {
    const auto expression =
        BinaryExpressionExp(ColumnValueExp("d"), op, ConstantValueExp(literal));
    const auto compiled = TryCompileSimpleCompare(expression, schema);
    if (!compiled) {
      FAIL() << "date comparison did not compile";
    }
    EXPECT_TRUE(compiled->date_string_payload);
    EXPECT_EQ(compiled->constant, literal);
    for (const Value& value :
         {Value(), day, Value::DateFromDays(11137), Value::DateFromDays(11139),
          Value("2000-06-30"), Value("2001-01-01")}) {
      const Row row({value});
      auto expected = expression->TryEvaluate(row, schema);
      ASSERT_TRUE(expected.HasValue());
      EXPECT_EQ(MatchSimpleCompare(row, *compiled), expected.Value().Truthy());
    }
  }
  // An invalid date literal must not be folded into the fast path: the value
  // matchers would treat the failed parse as "no match" and silently reject
  // every row, whereas the checked evaluator reports the error.
  const auto invalid = TryCompileSimpleCompare(
      BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("invalid date"))),
      schema);
  EXPECT_FALSE(invalid.has_value())
      << "an invalid date literal took the scan fast path";
}

TEST(ScanFilterTest, AuditDateNumericComparisonUsesCheckedEvaluator) {
  const Schema schema(
      "", {Column("d", ValueType::kDate), Column("i", ValueType::kInt64)});
  const Row row({Value::DateFromDays(5), Value(5)});
  for (const auto op : {BinaryOperation::kEquals, BinaryOperation::kNotEquals,
                        BinaryOperation::kLessThan}) {
    for (const auto& rhs :
         {ConstantValueExp(Value(5)), ConstantValueExp(Value(5.0)),
          ColumnValueExp("i")}) {
      for (bool reverse : {false, true}) {
        const auto expression =
            reverse ? BinaryExpressionExp(rhs, op, ColumnValueExp("d"))
                    : BinaryExpressionExp(ColumnValueExp("d"), op, rhs);
        EXPECT_FALSE(TryCompileSimpleCompare(expression, schema));
        EXPECT_FALSE(expression->TryEvaluate(row, schema).HasValue());
      }
    }
  }
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
  // '_' matches one whole UTF-8 character: 'é' is one character.
  EXPECT_TRUE(FastLikeMatch("a\xC3\xA9z", "a_z"));
  EXPECT_FALSE(FastLikeMatch("a\xC3\xA9\xC3\xA9z", "a_z"));
  EXPECT_TRUE(FastLikeMatch("a\xC3\xA9z", "a%z"));
  EXPECT_TRUE(FastLikeMatch("a\xC3\xA9z", "%_z"));
  // A truncated trailing multi-byte sequence still counts as one character.
  EXPECT_TRUE(FastLikeMatch("a\xC3", "a_"));
}

TEST(ScanFilterTest, TryDecideConstantConjunctFoldsCompileTimeConstants) {
  const Expression true_const = ConstantValueExp(Value(true));
  auto decided_true = TryDecideConstantConjunct(true_const);
  ASSERT_TRUE(decided_true.has_value());
  if (!decided_true.has_value()) {
    return;
  }
  EXPECT_TRUE(decided_true->Truthy());

  const Expression false_math =
      BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kEquals,
                          ConstantValueExp(Value(20)));
  auto decided_false = TryDecideConstantConjunct(false_math);
  ASSERT_TRUE(decided_false.has_value());
  if (!decided_false.has_value()) {
    return;
  }
  EXPECT_FALSE(decided_false->Truthy());

  // Column reference cannot be decided at compile time.
  const Expression col_pred =
      BinaryExpressionExp(ColumnValueExp(ColumnName("id")),
                          BinaryOperation::kEquals, ConstantValueExp(Value(1)));
  EXPECT_FALSE(TryDecideConstantConjunct(col_pred).has_value());
}

TEST(ScanFilterTest, ConstantArithmeticInListCompilesWithoutRowEvaluation) {
  const Schema schema = MakeTestSchema();
  const Expression predicate = InExpressionExp(
      ColumnValueExp("id"),
      {ConstantValueExp(Value(10)),
       BinaryExpressionExp(ConstantValueExp(Value(10)), BinaryOperation::kAdd,
                           ConstantValueExp(Value(1)))});
  const auto filter = CompileScanFilter({predicate}, schema);
  ASSERT_TRUE(filter.residual.empty());
  ASSERT_EQ(filter.disjunctive_branches.size(), 2U);
  const auto peeks = BuildIntegerPeeks(filter, nullptr, schema);
  ASSERT_EQ(peeks.size(), 2U);
  EXPECT_EQ(peeks[0].op, BinaryOperation::kGreaterThanEquals);
  EXPECT_EQ(peeks[0].constant, 10);
  EXPECT_EQ(peeks[1].op, BinaryOperation::kLessThanEquals);
  EXPECT_EQ(peeks[1].constant, 11);
  for (int64_t value = 8; value < 14; ++value) {
    const Row row({Value(value), Value(), Value(), Value()});
    bool matches = false;
    for (const auto& branch : filter.disjunctive_branches) {
      ASSERT_EQ(branch.simple.size(), 1U);
      matches |= MatchSimpleCompare(row, branch.simple[0]);
    }
    ASSERT_TRUE(predicate->TryEvaluate(row, schema).HasValue());
    EXPECT_EQ(matches, predicate->TryEvaluate(row, schema).Value().Truthy());
  }
  // An error is left on the row evaluator, including its short-circuit rules.
  const auto raising = InExpressionExp(
      ColumnValueExp("id"),
      {BinaryExpressionExp(ConstantValueExp(Value(1)), BinaryOperation::kDivide,
                           ConstantValueExp(Value(0)))});
  EXPECT_FALSE(CompileScanFilter({raising}, schema).residual.empty());
  // A sparse list's enclosure admits holes; the real filter must reject them.
  const auto sparse = CompileScanFilter(
      {InExpressionExp(ColumnValueExp("id"), {ConstantValueExp(Value(10)),
                                              ConstantValueExp(Value(12))})},
      schema);
  EXPECT_EQ(BuildIntegerPeeks(sparse, nullptr, schema).size(), 2U);
  const Row hole({Value(11), Value(), Value(), Value()});
  for (const auto& branch : sparse.disjunctive_branches) {
    EXPECT_FALSE(MatchSimpleCompare(hole, branch.simple[0]));
  }
}

TEST(ScanFilterTest, DisjunctEnclosureCombinesBoundsAcrossColumns) {
  const auto eq = [](const char* column, int value) {
    return BinaryExpressionExp(ColumnValueExp(column), BinaryOperation::kEquals,
                               ConstantValueExp(Value(value)));
  };
  const Expression predicate = BinaryExpressionExp(
      BinaryExpressionExp(eq("id", 10), BinaryOperation::kAnd, eq("alt_id", 1)),
      BinaryOperation::kOr,
      BinaryExpressionExp(eq("id", 12), BinaryOperation::kAnd,
                          eq("alt_id", 2)));
  const auto filter = CompileScanFilter({predicate}, MakeTestSchema());
  const auto peeks = BuildIntegerPeeks(filter, nullptr, MakeTestSchema());
  ASSERT_EQ(peeks.size(), 4U);
  EXPECT_EQ(peeks[0].column, 0U);
  EXPECT_EQ(peeks[0].constant, 10);
  EXPECT_EQ(peeks[1].constant, 12);
  EXPECT_EQ(peeks[2].column, 3U);
  const auto unbounded =
      CompileScanFilter({BinaryExpressionExp(eq("id", 10), BinaryOperation::kOr,
                                             eq("alt_id", 1))},
                        MakeTestSchema());
  EXPECT_TRUE(BuildIntegerPeeks(unbounded, nullptr, MakeTestSchema()).empty());
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
  if (!compiled_int.has_value()) {
    return;
  }
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
  if (!compiled_dbl.has_value()) {
    return;
  }
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
  if (!compiled_col.has_value()) {
    return;
  }
  EXPECT_TRUE(compiled_col->column_payload);
  EXPECT_EQ(compiled_col->column, 0U);
  EXPECT_EQ(compiled_col->right_column, 3U);
  EXPECT_TRUE(MatchSimpleCompare(
      Row({Value(5), Value(0.0), Value("x"), Value(10)}), *compiled_col));
  EXPECT_FALSE(MatchSimpleCompare(
      Row({Value(10), Value(0.0), Value("x"), Value(5)}), *compiled_col));
}

TEST(ScanFilterTest, CoalesceComparisonDropsOnlyRejectingFallbacks) {
  const Schema schema("t", {Column("x", ValueType::kInt64)});
  for (const auto op :
       {BinaryOperation::kEquals, BinaryOperation::kNotEquals,
        BinaryOperation::kLessThan, BinaryOperation::kLessThanEquals,
        BinaryOperation::kGreaterThan, BinaryOperation::kGreaterThanEquals}) {
    for (const auto& fallback : {Value(), Value(-1), Value(0), Value(1)}) {
      for (const bool reversed : {false, true}) {
        const auto coalesce = FunctionCallExp(
            "coalesce", {ColumnValueExp("x"), ConstantValueExp(fallback)});
        const auto zero = ConstantValueExp(Value(0));
        const auto expression = reversed
                                    ? BinaryExpressionExp(zero, op, coalesce)
                                    : BinaryExpressionExp(coalesce, op, zero);
        auto missing = expression->TryEvaluate(Row({Value()}), schema);
        ASSERT_TRUE(missing.HasValue());
        const auto compiled = TryCompileSimpleCompare(expression, schema);
        if (!missing.Value().IsNull() && missing.Value().Truthy()) {
          EXPECT_FALSE(compiled.has_value());
          continue;
        }
        ASSERT_TRUE(compiled.has_value()) << expression->ToString();
        if (!compiled.has_value()) {
          continue;
        }
        for (const auto& value : {Value(), Value(-2), Value(0), Value(2)}) {
          const Row row({value});
          auto expected = expression->TryEvaluate(row, schema);
          ASSERT_TRUE(expected.HasValue());
          EXPECT_EQ(MatchSimpleCompare(row, *compiled),
                    !expected.Value().IsNull() && expected.Value().Truthy());
        }
      }
    }
  }
  const auto a = BinaryExpressionExp(
      FunctionCallExp("ifnull",
                      {ColumnValueExp("x"), ConstantValueExp(Value(0))}),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(0)));
  EXPECT_TRUE(TryCompileSimpleCompare(a, schema));
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
  if (!prefix_in.has_value()) {
    return;
  }
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
      .string_values = nullptr,
      .distinct_witness_minmax = nullptr,
      .packed_pair_col2 = std::nullopt,
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

TEST(ScanFilterTest, StringMembershipPreservesNullEmptyAndCollationSemantics) {
  auto strings = std::make_shared<relational_detail::MembershipStringSet>();
  strings->insert("ABC");
  PrebuiltMembershipPredicate pred;
  pred.column = 0;
  pred.domain_type = ValueType::kVarChar;
  pred.string_values = strings;
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value("ABC")}), pred));
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value("abc")}), pred));
  EXPECT_TRUE(
      MatchPrebuiltMembership(Row({Value("abc").WithCollation(2)}), pred));
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value()}), pred));
  pred.negated = true;
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value("missing")}), pred));
  pred.contains_null = true;
  EXPECT_FALSE(MatchPrebuiltMembership(Row({Value("missing")}), pred));
  strings->clear();
  pred.contains_null = false;
  EXPECT_TRUE(MatchPrebuiltMembership(Row({Value()}), pred));
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

TEST(ScanFilterTest, ConstantInMembershipCompilesTenElementIntAndStringLists) {
  const Schema schema = MakeTestSchema();

  // >8 constant arithmetic INT64 items: compiles to PrebuiltMembershipPredicate
  // via TryCompileConstantInMembership (and preserves NULL UNKNOWN semantics).
  std::vector<Expression> int_items;
  int_items.reserve(10);
  for (int64_t i = 0; i < 10; ++i) {
    int_items.push_back(BinaryExpressionExp(ConstantValueExp(Value(100)),
                                            BinaryOperation::kAdd,
                                            ConstantValueExp(Value(i))));
  }
  const Expression int_in =
      InExpressionExp(ColumnValueExp(ColumnName("id")), std::move(int_items));
  const CompiledScanFilter int_filter = CompileScanFilter({int_in}, schema);
  ASSERT_TRUE(int_filter.residual.empty());
  ASSERT_EQ(int_filter.prebuilt_membership.size(), 1U);
  for (int64_t v = 98; v <= 112; ++v) {
    EXPECT_EQ(
        MatchPrebuiltMembership(Row({Value(v), Value(), Value(), Value()}),
                                int_filter.prebuilt_membership[0]),
        v >= 100 && v < 110);
  }
  EXPECT_FALSE(
      MatchPrebuiltMembership(Row({Value(), Value(), Value(), Value()}),
                              int_filter.prebuilt_membership[0]));

  // >8 constant VarChar literals: compiles to PrebuiltMembershipPredicate
  // string set.
  std::vector<Expression> str_items;
  str_items.reserve(10);
  for (int i = 0; i < 10; ++i) {
    str_items.push_back(ConstantValueExp(Value("k" + std::to_string(i))));
  }
  const Expression str_in =
      InExpressionExp(ColumnValueExp(ColumnName("code")), std::move(str_items));
  const CompiledScanFilter str_filter = CompileScanFilter({str_in}, schema);
  ASSERT_TRUE(str_filter.residual.empty());
  ASSERT_EQ(str_filter.prebuilt_membership.size(), 1U);
  EXPECT_TRUE(
      MatchPrebuiltMembership(Row({Value(), Value(), Value("k7"), Value()}),
                              str_filter.prebuilt_membership[0]));
  EXPECT_FALSE(
      MatchPrebuiltMembership(Row({Value(), Value(), Value("k99"), Value()}),
                              str_filter.prebuilt_membership[0]));
  EXPECT_FALSE(
      MatchPrebuiltMembership(Row({Value(), Value(), Value(), Value()}),
                              str_filter.prebuilt_membership[0]));
}

TEST(ScanFilterTest, SingletonInUsesComparisonWithCanonicalNullSemantics) {
  for (const Value& member : {Value(42), Value(42.5), Value("Electronics")}) {
    const Schema schema("single", {Column("v", member.type)});
    for (const bool negated : {false, true}) {
      Expression predicate = InExpressionExp(ColumnValueExp(ColumnName("v")),
                                             {ConstantValueExp(member)});
      if (negated) {
        predicate = UnaryExpressionExp(predicate, UnaryOperation::kNot);
      }
      const auto filter = CompileScanFilter({predicate}, schema);
      ASSERT_TRUE(filter.residual.empty());
      ASSERT_EQ(filter.simple.size(), 1U);
      const Value other = member.type == ValueType::kVarChar  ? Value("Books")
                          : member.type == ValueType::kDouble ? Value(7.5)
                                                              : Value(7);
      for (const Value& probe : {Value(), member, other}) {
        const Row row({probe});
        auto canonical = predicate->TryEvaluate(row, schema);
        ASSERT_TRUE(canonical.HasValue());
        EXPECT_EQ(MatchSimpleCompare(row, filter.simple.front()),
                  !canonical.Value().IsNull() && canonical.Value().Truthy());
      }
    }
    // A NULL member must retain UNKNOWN, including under NOT IN.
    const auto null_filter =
        CompileScanFilter({InExpressionExp(ColumnValueExp(ColumnName("v")),
                                           {ConstantValueExp(Value())})},
                          schema);
    EXPECT_TRUE(null_filter.simple.empty());
  }
}

TEST(ScanFilterTest,
     InvalidDateRuntimeConstantPropagatesInsteadOfSilentReject) {
  const Schema schema("", {Column("d", ValueType::kDate)});
  const auto expression =
      BinaryExpressionExp(ColumnValueExp("d"), BinaryOperation::kEquals,
                          ConstantValueExp(Value("2000-01-01")));
  const auto compiled = TryCompileSimpleCompare(expression, schema);
  ASSERT_TRUE(compiled.has_value());
  ASSERT_TRUE(compiled->date_string_payload);  // NOLINT(bugprone-unchecked-optional-access)
  // A parameter rebind can replace the parsed literal with an unparseable
  // string after compilation; the failed coercion must surface as an error,
  // not a clean per-row reject.
  SimpleComparePredicate rebound = *compiled;  // NOLINT(bugprone-unchecked-optional-access)
  rebound.date_string_payload = false;
  rebound.constant = Value("invalid date");
  const Row row({Value::DateFromDays(11138)});
  Status error{Status::kSuccess};
  EXPECT_FALSE(MatchSimpleCompare(row, rebound, &error));
  EXPECT_FALSE(error.ok())
      << "a failed DATE coercion was folded into an ordinary reject";
}

TEST(ScanFilterTest, UnsignedTaggingLimitedToReferencedColumns) {
  Column unsigned_column(ColumnName("u"), ValueType::kInt64);
  unsigned_column.SetUnsigned(true);
  const Schema schema("test", {Column(ColumnName("id"), ValueType::kInt64),
                               unsigned_column});
  // A predicate over only the signed column must not disable the fast paths
  // because of an untouched unsigned column.
  const auto signed_filter = CompileScanFilter(
      {BinaryExpressionExp(ColumnValueExp("id"), BinaryOperation::kEquals,
                           ConstantValueExp(Value(5)))},
      schema);
  EXPECT_FALSE(signed_filter.needs_unsigned_tagging);
  EXPECT_TRUE(signed_filter.unsigned_columns.empty());
  // The unsigned column itself still requires tagging.
  const auto unsigned_filter = CompileScanFilter(
      {BinaryExpressionExp(ColumnValueExp("u"), BinaryOperation::kEquals,
                           ConstantValueExp(Value(5)))},
      schema);
  EXPECT_TRUE(unsigned_filter.needs_unsigned_tagging);
  ASSERT_EQ(unsigned_filter.unsigned_columns.size(), 1U);
  EXPECT_EQ(unsigned_filter.unsigned_columns[0], 1);
}

TEST(ScanFilterTest, NullPeeksFoldTestsCommonToEveryBranch) {
  const Schema schema("test", {Column(ColumnName("a"), ValueType::kInt64),
                               Column(ColumnName("b"), ValueType::kInt64)});
  CompiledScanFilter filter;
  const Expression is_null_a =
      UnaryExpressionExp(ColumnValueExp("a"), UnaryOperation::kIsNull);
  const Expression is_not_null_a =
      UnaryExpressionExp(ColumnValueExp("a"), UnaryOperation::kIsNotNull);
  filter.disjunctive_branches.push_back({{}, {}, {}, {is_null_a}});
  filter.disjunctive_branches.push_back({{}, {}, {}, {is_null_a}});
  const auto common = BuildNullPeeks(filter, schema, nullptr, schema);
  ASSERT_EQ(common.size(), 1U);
  EXPECT_EQ(common[0].column, 0);
  EXPECT_TRUE(common[0].expect_null);
  // A test kept by only one branch must not reject rows surviving via the
  // other branch.
  filter.disjunctive_branches[1].residual = {is_not_null_a};
  EXPECT_TRUE(BuildNullPeeks(filter, schema, nullptr, schema).empty());
  // An uncovered top-level residual keeps the previous size-equality
  // contract: nothing is folded from the branches.
  CompiledScanFilter partial;
  partial.residual = {
      is_not_null_a,
      BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kAdd,
                          ConstantValueExp(Value(1)))};
  const Expression like =
      BinaryExpressionExp(ColumnValueExp("b"), BinaryOperation::kLike,
                          ConstantValueExp(Value("%x%")));
  partial.disjunctive_branches.push_back({{}, {}, {}, {is_null_a}});
  partial.disjunctive_branches.push_back({{}, {}, {}, {is_null_a}});
  const auto folded = BuildNullPeeks(partial, schema, nullptr, schema);
  EXPECT_EQ(folded.size(), 1U);
}

TEST(ScanFilterTest, ManualNullBinaryChildrenStayOnResidualPath) {
  const Schema schema("test", {Column(ColumnName("id"), ValueType::kInt64)});
  // Hand-built AST nodes may carry null operands; compiling must not crash.
  const Expression broken_equals = BinaryExpressionExp(
      Expression(), BinaryOperation::kEquals, ConstantValueExp(Value(5)));
  const Expression broken_child = BinaryExpressionExp(
      ColumnValueExp("id"), BinaryOperation::kEquals, Expression());
  const CompiledScanFilter filter =
      CompileScanFilter({broken_equals, broken_child}, schema);
  EXPECT_GE(filter.residual.size(), 1U);
}

}  // namespace
}  // namespace tinylamb
