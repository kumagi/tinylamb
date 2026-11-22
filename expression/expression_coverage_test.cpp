/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Cast-matrix coverage for expression/cast_expression.cpp: the SAFE_CAST
// contract (safe failures collapse to NULL, successful conversions agree
// with the strict cast) is asserted over the full source x target matrix,
// with exact expectations pinned for the conversion edges themselves.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "expression/cast_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

// Runs the strict and SAFE forms of one conversion.  Both results stay
// StatusOr so a SAFE_CAST contract violation (a Status error instead of a
// NULL value) surfaces as a test failure instead of a MoveValue abort.
std::pair<StatusOr<Value>, StatusOr<Value>> CastBoth(const Value& from,
                                                     const std::string& type) {
  CastExpression strict(ConstantValueExp(from), type, false);
  CastExpression safe(ConstantValueExp(from), type, true);
  StatusOr<Value> result = strict.TryCast(from);
  StatusOr<Value> safe_result = safe.TryCast(from);
  return {std::move(result), std::move(safe_result)};
}

}  // namespace

TEST(ExpressionCoverage, SafeCastContractHoldsOverMatrix) {
  std::vector<Value> sources = {
      Value(),
      Value(int64_t{42}),
      Value(int64_t{-1}),
      Value(2.5),
      Value(std::numeric_limits<double>::infinity()),
      Value("123"),
      Value("abc"),
      Value(""),
      Value(" 42 "),
      Value("true"),
      Value("TRUE"),
      Value("2026-01-02"),
      Value("2026-01-02 03:04:05"),
      Value("[1,2]"),
      Value("{a: 1}"),
      Value::Date("2026-01-02"),
      Value::Array({Value(1)}, "INT64"),
      // Unsigned (UINT64-tagged) values and the signed boundary: the tag is
      // carried in Value's collation slot, so a SAFE_CAST that drops it or a
      // width check that misreads 2^63 would otherwise slip past this matrix.
      Value(-1).WithUnsigned(),
      Value(static_cast<int64_t>(uint64_t{1} << 63)).WithUnsigned(),
      Value(std::numeric_limits<int64_t>::min()),
      Value(std::numeric_limits<int64_t>::max()),
      // Non-finite and signed-zero doubles: JSON/STRING targets have to
      // spell them out instead of quietly emitting a NULL or a bare token.
      Value(std::numeric_limits<double>::quiet_NaN()),
      Value(std::numeric_limits<double>::infinity()),
      Value(-std::numeric_limits<double>::infinity()),
      Value(-0.0),
  };
  std::vector<const char*> targets = {
      "INT64",
      "INT32",
      "UINT64",
      "FLOAT64",
      "DOUBLE",
      "STRING",
      "VARCHAR",
      "BYTES",
      "BOOL",
      "BOOLEAN",
      "DATE",
      "TIMESTAMP",
      "DATETIME",
      "TIME",
      "INTERVAL",
      "JSON",
      "NUMERIC",
      "UUID",
      "ARRAY<INT64>",
      "ARRAY<STRING>",
      "STRUCT<a INT64>",
      "googlesql_test.KitchenSinkPB",
  };
  for (const Value& from : sources) {
    for (const char* type : targets) {
      SCOPED_TRACE(std::string(type));
      auto [strict, safe_or] = CastBoth(from, type);
      ASSERT_TRUE(safe_or.HasValue())
          << "SAFE_CAST returned a Status error for " << type << ": "
          << safe_or.GetStatus().GetMessage();
      const Value& safe = safe_or.Value();
      if (strict.HasValue()) {
        EXPECT_TRUE(safe == strict.Value());
      } else {
        EXPECT_TRUE(safe.IsNull())
            << "safe cast succeeded where strict failed: "
            << strict.GetStatus();
      }
    }
  }
}

TEST(ExpressionCoverage, PinnedConversionEdges) {
  // Numeric strings tolerate surrounding whitespace; non-numerics fail.
  EXPECT_EQ(CastBoth(Value(" 42 "), "INT64").first.MoveValue(), Value(42));
  EXPECT_TRUE(CastBoth(Value("abc"), "INT64").first.HasValue() == false);
  // BOOL accepts case-insensitive true/false spellings.
  EXPECT_EQ(CastBoth(Value("TRUE"), "BOOL").first.MoveValue(), Value(1));
  EXPECT_EQ(CastBoth(Value("false"), "BOOL").first.MoveValue(), Value(0));
  // Narrowed integer widths reject out-of-range values.
  EXPECT_TRUE(CastBoth(Value(300), "INT8").first.HasValue() == false);
  EXPECT_TRUE(CastBoth(Value(-1), "UINT64").first.HasValue() == false);
  EXPECT_TRUE(
      CastBoth(Value("18446744073709551616"), "UINT64").first.HasValue() ==
      false);
  // A tagged UINT64 Value must keep its SQL signedness through SAFE_CAST and
  // render as an unsigned decimal, not as INT64 -1.
  const Value uint64_max = Value(-1).WithUnsigned();
  EXPECT_EQ(CastBoth(uint64_max, "UINT64").first.MoveValue(), uint64_max);
  EXPECT_EQ(CastBoth(uint64_max, "STRING").first.MoveValue(),
            Value("18446744073709551615"));
  // Non-finite doubles have no JSON numeric literal; SAFE_CAST must keep the
  // same spelling (or NULL) on both the strict and safe paths.
  auto nan_to_string =
      CastBoth(Value(std::numeric_limits<double>::quiet_NaN()), "STRING");
  ASSERT_TRUE(nan_to_string.first.HasValue());
  ASSERT_TRUE(nan_to_string.second.HasValue());
  EXPECT_EQ(nan_to_string.second.MoveValue(), nan_to_string.first.Value());
  // DATE accepts its canonical form and rejects nonsense.
  EXPECT_EQ(CastBoth(Value("2026-01-02"), "DATE").first.MoveValue(),
            Value::Date("2026-01-02"));
  EXPECT_TRUE(CastBoth(Value("not-a-date"), "DATE").first.HasValue() == false);
  // STRING round-trips every scalar.
  EXPECT_EQ(CastBoth(Value::Date("2026-01-02"), "STRING").first.MoveValue(),
            Value("2026-01-02"));
  EXPECT_EQ(CastBoth(Value(42), "STRING").first.MoveValue(), Value("42"));
  EXPECT_EQ(CastBoth(Value(2.5), "STRING").first.MoveValue(), Value("2.5"));
  // NULL casts to NULL for every target without error. Keep this pinned over
  // the same full type set as the matrix above: a SAFE_CAST(NULL, TIMESTAMP)
  // or proto returning a Status would otherwise go unnoticed.
  for (const char* type : {"INT64",
                           "INT32",
                           "UINT64",
                           "FLOAT64",
                           "DOUBLE",
                           "STRING",
                           "VARCHAR",
                           "BYTES",
                           "BOOL",
                           "BOOLEAN",
                           "DATE",
                           "TIMESTAMP",
                           "DATETIME",
                           "TIME",
                           "INTERVAL",
                           "JSON",
                           "NUMERIC",
                           "UUID",
                           "ARRAY<INT64>",
                           "ARRAY<STRING>",
                           "STRUCT<a INT64>",
                           "googlesql_test.KitchenSinkPB"}) {
    auto [strict, safe_or] = CastBoth(Value(), type);
    ASSERT_TRUE(strict.HasValue());
    ASSERT_TRUE(safe_or.HasValue());
    EXPECT_TRUE(strict.Value().IsNull());
    EXPECT_TRUE(safe_or.Value().IsNull());
  }
  // Text-to-array conversion is not offered at the cast level; both forms
  // reject it (SAFE_CAST collapses the rejection to NULL).
  EXPECT_TRUE(CastBoth(Value("[1,2,3]"), "ARRAY<INT64>").first.HasValue() ==
              false);
  auto arrayed_safe_or = CastBoth(Value("[1,2,3]"), "ARRAY<INT64>").second;
  ASSERT_TRUE(arrayed_safe_or.HasValue());
  EXPECT_TRUE(arrayed_safe_or.Value().IsNull());
  // Array sources convert to their element text form.
  auto array_to_string =
      CastBoth(Value::Array({Value(1), Value(2)}, "INT64"), "STRING").first;
  ASSERT_TRUE(array_to_string.HasValue());
  EXPECT_EQ(array_to_string.Value(), Value(std::string("[1,2]")));
  // Type parameters are stripped for base types but not array types.
  EXPECT_EQ(CastBoth(Value("42"), "STRING(5)").first.MoveValue(), Value("42"));
}

}  // namespace tinylamb
