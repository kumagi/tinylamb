/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Function-level sweep over the relational expression evaluator
// (executor/detail/expression_eval.cpp TryEvaluateFunction).  Every function
// name dispatched there is called with realistic constant arguments through
// relational_detail::TryEvaluate, the same entry point scan filters, sort
// keys, join conditions, and grouped projections use.
//
// Expectation codes:
//   1: the call must succeed
//   0: the call must fail (argument-contract rejection)
//  -1: either outcome is accepted; the case exists to execute the branch
//      bodies.  Behavioral truth for the covered families is pinned by the
//      SQL-level suites (sql_function_coverage_test and friends); this file
//      exists so the relational implementations execute, and so any
//      evaluator that crashes or leaks exceptions through the EXC-SHIM
//      boundary fails loudly.

#include <gtest/gtest.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/interval_expression.hpp"
#include "type/date.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

struct Case {
  const char* name;
  std::vector<Value> args;
  int expect;  // 1: succeed, 0: fail, -1: either.
};

Value Arr(std::vector<Value> elements, const char* elem_type = "INT64") {
  return Value::Array(std::move(elements), elem_type);
}

std::vector<Case> SweepCases() {
  const Value kInt(3);
  const Value kIntZero(0);
  const Value kNeg(-4);
  const Value kDouble(2.5);
  const Value kStr(std::string("Hello World"));
  const Value kEmptyStr(std::string(""));
  const Value kJson(std::string(R"({"a": 1, "b": {"c": "x"}})"));
  const Value kPath(std::string("$.b.c"));
  const Value kDate(Value::Date("2026-09-22"));
  const Value kArrInt = Arr({Value(1), Value(2), Value(3)});
  const Value kArrStr =
      Arr({Value(std::string("a")), Value(std::string("b"))}, "STRING");
  const Value kArrEmpty = Arr({});

  return {
      // ---- math / numeric -------------------------------------------------
      {"abs", {kNeg}, 1},
      {"abs", {kDouble}, 1},
      {"acos", {kDouble}, -1},
      {"asin", {kDouble}, -1},
      {"atan", {kDouble}, 1},
      {"atan2", {kDouble, kDouble}, 1},
      {"cbrt", {kDouble}, 1},
      {"ceil", {kDouble}, 1},
      {"ceil", {kInt}, 1},
      {"cos", {kDouble}, 1},
      {"cosh", {kDouble}, 1},
      {"degrees", {kDouble}, 1},
      {"div", {Value(7), Value(2)}, 1},
      {"div", {Value(7), kIntZero}, 0},
      {"exp", {kDouble}, 1},
      {"floor", {kDouble}, 1},
      {"greatest", {Value(1), Value(2), Value(3)}, 1},
      {"greatest", {Value(), Value(2)}, -1},
      {"ieee_divide", {Value(1), kIntZero}, 1},
      {"is_inf", {kDouble}, 1},
      {"is_nan", {kDouble}, 1},
      {"ln", {kDouble}, -1},
      {"log", {Value(8), Value(2)}, 1},
      {"log", {kDouble}, -1},
      {"log10", {kDouble}, -1},
      {"mod", {Value(7), Value(2)}, 1},
      {"mod", {Value(7), kIntZero}, 0},
      {"pi", {}, 1},
      {"pow", {kDouble, kInt}, 1},
      {"radians", {kDouble}, 1},
      {"rand", {}, 1},
      {"rand", {kInt}, -1},
      {"round", {kDouble}, 1},
      {"round", {kDouble, kInt}, 1},
      {"safe_add", {Value(1), Value(2)}, 1},
      {"safe_add", {Value(std::numeric_limits<int64_t>::max()), Value(1)}, 1},
      {"safe_divide", {Value(1), kIntZero}, 1},
      {"safe_multiply", {Value(3), Value(4)}, 1},
      {"safe_negate", {kNeg}, 1},
      {"safe_subtract", {Value(5), Value(2)}, 1},
      {"sign", {kNeg}, 1},
      {"sin", {kDouble}, 1},
      {"sinh", {kDouble}, 1},
      {"sqrt", {kDouble}, 1},
      {"tan", {kDouble}, 1},
      {"tanh", {kDouble}, 1},
      {"trunc", {kDouble}, 1},
      {"trunc", {kDouble, kInt}, 1},
      {"euclidean_distance", {kArrInt, kArrInt}, -1},

      // ---- string ---------------------------------------------------------
      {"ascii", {kStr}, 1},
      {"byte_length", {kStr}, 1},
      {"chr", {Value(65)}, 1},
      {"code_points_to_bytes", {kArrInt}, -1},
      {"code_points_to_string", {kArrInt}, 1},
      {"collate", {kStr, Value(std::string("und:ci"))}, -1},
      {"concat", {kStr, kStr}, 1},
      {"concat", {kStr, Value()}, -1},
      {"ends_with", {kStr, Value(std::string("World"))}, 1},
      {"format", {Value(std::string("%d|%s")), kInt, kStr}, 1},
      {"from_base64", {Value(std::string("aGVsbG8="))}, -1},
      {"from_hex", {Value(std::string("4142"))}, -1},
      {"initcap", {kStr}, 1},
      {"instr", {kStr, Value(std::string("World"))}, 1},
      {"left", {kStr, Value(5)}, 1},
      {"length", {kStr}, 1},
      {"lower", {kStr}, 1},
      {"lpad", {kStr, Value(15), Value(std::string("*"))}, 1},
      {"ltrim", {kStr}, 1},
      {"md5", {kStr}, 1},
      {"repeat", {kStr, Value(2)}, 1},
      {"replace",
       {kStr, Value(std::string("World")), Value(std::string("There"))},
       1},
      {"reverse", {kStr}, 1},
      {"right", {kStr, Value(5)}, 1},
      {"rpad", {kStr, Value(15), Value(std::string("*"))}, 1},
      {"rtrim", {kStr}, 1},
      {"sha1", {kStr}, 1},
      {"sha256", {kStr}, 1},
      {"soundex", {kStr}, -1},
      {"split", {kStr, Value(std::string(" "))}, 1},
      {"split_substr", {kStr, Value(std::string(" ")), Value(1)}, -1},
      {"starts_with", {kStr, Value(std::string("Hello"))}, 1},
      {"strpos", {kStr, Value(std::string("World"))}, 1},
      {"substr", {kStr, Value(1), Value(5)}, 1},
      {"to_base64", {kStr}, -1},
      {"to_hex", {kStr}, -1},
      {"to_json_string", {kInt}, -1},
      {"translate",
       {kStr, Value(std::string("lo")), Value(std::string("10"))},
       1},
      {"trim", {kStr}, 1},
      {"unicode", {kStr}, 1},
      {"upper", {kStr}, 1},
      {"safe_convert_bytes_to_string", {kStr}, -1},
      {"byte_cast_to_string", {kStr}, -1},

      // ---- regexp ---------------------------------------------------------
      {"regexp_contains", {kStr, Value(std::string("W[a-z]+"))}, 1},
      {"regexp_extract", {kStr, Value(std::string("W[a-z]+"))}, 1},
      {"regexp_extract_all", {kStr, Value(std::string("[a-z]+"))}, 1},
      {"regexp_instr", {kStr, Value(std::string("W[a-z]+"))}, -1},
      {"regexp_match", {kStr, Value(std::string("^Hello"))}, -1},
      {"regexp_replace",
       {kStr, Value(std::string("World")), Value(std::string("There"))},
       1},
      {"byte_regexp_extract_all", {kStr, Value(std::string("[a-z]+"))}, -1},
      {"byte_regexp_instr", {kStr, Value(std::string("W"))}, -1},

      // ---- byte-string overloads ------------------------------------------
      {"byte_left", {kStr, Value(2)}, -1},
      {"byte_reverse", {kStr}, -1},
      {"byte_right", {kStr, Value(2)}, -1},
      {"byte_substr", {kStr, Value(1), Value(2)}, -1},

      // ---- json / struct --------------------------------------------------
      {"json_extract", {kJson, kPath}, 1},
      {"json_extract_scalar", {kJson, kPath}, 1},
      {"json_extract_string_array", {kJson, Value(std::string("$"))}, -1},
      {"json_query_array", {kJson, Value(std::string("$.b"))}, -1},
      {"json_value", {kJson, kPath}, 1},
      {"json_value_array", {kJson, Value(std::string("$"))}, -1},
      // JSON document family (shared json_doc implementation).
      {"json_type", {kJson}, 1},
      {"json_type", {kJson, Value(std::string("$.b"))}, 1},
      {"json_type", {kJson, Value(std::string("$.zz"))}, 1},
      {"json_type", {kStr}, 0},
      {"json_keys", {kJson}, 1},
      {"json_keys", {Value(std::string("[1]"))}, 1},
      {"json_array_length", {Value(std::string("[1, 2, 3]"))}, 1},
      {"json_array_length", {kJson}, 1},
      {"json_set", {kJson, Value(std::string("$.z")), Value(9)}, 1},
      {"json_set",
       {Value(std::string("[1]")), Value(std::string("$[3]")), Value(9)},
       0},
      {"json_insert", {kJson, Value(std::string("$.q")), Value(1)}, 1},
      {"json_insert", {kJson, Value(std::string("$.a")), Value(1)}, 1},
      {"json_replace", {kJson, Value(std::string("$.a")), Value(2)}, 1},
      {"json_replace", {kJson, Value(std::string("$.zz")), Value(2)}, 1},
      {"json_remove", {kJson, Value(std::string("$.a"))}, 1},
      {"json_array", {Value(1), kStr}, 1},
      {"json_object", {Value(std::string("k")), Value(1)}, 1},
      {"json_object", {Value(1), Value(2)}, 0},
      {"lax_int64", {Value(std::string("5.9"))}, 1},
      {"lax_float64", {kJson}, 1},
      {"lax_bool", {Value(std::string("true"))}, 1},
      {"lax_string", {Value(42)}, 1},
      {"lax_int64", {Value(std::string("[1]"))}, 1},
      {"__struct_json__", {kInt, kStr}, -1},
      {"__struct_set", {kJson, Value(std::string("a")), Value(9)}, -1},
      {"__get_field_safe", {kJson, Value(std::string("a"))}, 1},
      {"__get_field_safe", {Value(), Value(std::string("a"))}, 1},
      {"get_field", {kJson, Value(std::string("a"))}, 1},
      // T1647: the relational/batch evaluator must match the AST's strict
      // contract -- a missing member or a non-struct payload fails instead
      // of returning NULL.
      {"get_field", {kJson, Value(std::string("nope"))}, 0},
      {"get_field", {Value(std::string("27")), Value(std::string("a"))}, 0},
      {"__pipe_concat", {kArrStr, Value(std::string("-"))}, -1},

      // ---- arrays ---------------------------------------------------------
      {"array_concat", {kArrInt, kArrInt}, 1},
      {"array_element_offset", {kArrInt, Value(2)}, -1},
      {"array_element_safe_offset", {kArrInt, Value(2)}, -1},
      {"array_element_safe_ordinal", {kArrInt, Value(2)}, -1},
      {"array_first", {kArrInt}, 1},
      {"array_includes", {kArrInt, Value(2)}, 1},
      {"array_includes_all", {kArrInt, kArrInt}, -1},
      {"array_includes_any", {kArrInt, kArrInt}, -1},
      {"array_is_distinct", {kArrInt}, 1},
      {"array_length", {kArrInt}, 1},
      {"array_length", {kArrEmpty}, 1},
      {"array_reverse", {kArrInt}, 1},
      {"array_slice", {kArrInt, Value(1), Value(2)}, 1},
      {"array_to_string", {kArrStr, Value(std::string(","))}, 1},
      {"element", {kArrInt}, -1},
      {"generate_array", {Value(1), Value(5), Value(1)}, 1},
      {"generate_series", {Value(1), Value(5), Value(1)}, 1},
      {"offset", {kArrInt, Value(1)}, -1},
      {"safe_offset", {kArrInt, Value(1)}, -1},
      {"ordinal", {kArrInt, Value(1)}, -1},
      {"safe_ordinal", {kArrInt, Value(1)}, -1},

      // ---- dates / times --------------------------------------------------
      {"current_date", {}, 1},
      {"current_datetime", {}, 1},
      {"current_time", {}, -1},
      {"current_timestamp", {}, 1},
      {"date", {kStr}, -1},
      {"date", {kStr, Value(std::string("UTC"))}, -1},
      {"date_add", {kDate, Value(1)}, -1},
      {"date_bucket", {kDate, Value(7)}, -1},
      {"date_diff", {kDate, kDate, Value(std::string("DAY"))}, -1},
      {"date_from_unix_date", {Value(19000)}, 1},
      {"date_trunc", {kDate, Value(std::string("MONTH"))}, -1},
      {"datetime", {kStr}, -1},
      {"datetime_bucket", {kStr, Value(7)}, -1},
      {"datetime_diff", {kStr, kStr, Value(std::string("DAY"))}, -1},
      {"datetime_sub", {kStr, Value(1)}, -1},
      {"extract_date", {kStr}, -1},
      {"extract_datetime", {kStr}, -1},
      {"extract_day", {kDate}, 1},
      {"extract_dayofweek", {kDate}, 1},
      {"extract_dayofyear", {kDate}, 1},
      {"extract_hour", {kStr}, -1},
      {"extract_isoyear", {kDate}, 1},
      {"extract_microsecond", {kStr}, -1},
      {"extract_millisecond", {kStr}, -1},
      {"extract_minute", {kStr}, -1},
      {"extract_month", {kDate}, 1},
      {"extract_nanosecond", {kStr}, -1},
      {"extract_quarter", {kDate}, 1},
      {"extract_second", {kStr}, -1},
      {"extract_time", {kStr}, -1},
      {"extract_week", {kDate}, 1},
      {"extract_year", {kDate}, 1},
      {"format_date", {Value(std::string("%Y-%m-%d")), kDate}, -1},
      {"format_datetime", {Value(std::string("%Y")), kStr}, -1},
      {"format_timestamp", {Value(std::string("%Y")), kStr}, -1},
      {"generate_date_array", {kDate, kDate}, -1},
      {"justify_days", {Value(45)}, -1},
      {"justify_hours", {Value(45)}, -1},
      {"justify_interval", {Value(45)}, -1},
      {"last_day", {kDate}, -1},
      {"make_interval", {Value(1), Value(0), Value(0), Value(1)}, -1},
      {"months_between", {kDate, kDate}, -1},
      {"next_day", {kDate, Value(std::string("MONDAY"))}, -1},
      {"add_months", {kDate, Value(3)}, -1},
      {"parse_date",
       {Value(std::string("%Y-%m-%d")), Value(std::string("2026-09-22"))},
       -1},
      {"parse_timestamp",
       {Value(std::string("%Y-%m-%dT%H:%M:%E*S%Ez")),
        Value(std::string("2026-09-22T00:00:00Z"))},
       -1},
      {"time", {kStr}, -1},
      {"timestamp", {kStr}, -1},
      {"timestamp_bucket", {kStr, Value(600)}, -1},
      {"timestamp_diff", {kStr, kStr, Value(std::string("MINUTE"))}, -1},
      {"timestamp_sub", {kStr, Value(60)}, -1},
      {"timestamp_trunc", {kStr, Value(std::string("DAY"))}, -1},
      {"unix_date", {kDate}, 1},
      {"unix_micros", {kStr}, -1},
      {"unix_millis", {kStr}, -1},
      {"unix_seconds", {kStr}, -1},
      {"week", {kDate}, -1},

      // ---- misc / conditional ---------------------------------------------
      {"if", {Value(true), Value(1), Value(2)}, 1},
      {"iferror", {kInt, kIntZero}, 1},
      {"ifnull", {Value(), Value(5)}, 1},
      {"ifnull", {Value(1), Value(5)}, 1},
      {"nullif", {Value(1), Value(1)}, 1},
      {"nullif", {Value(1), Value(2)}, 1},
      {"nulliferror", {kInt}, -1},
      {"coalesce", {Value(), Value(2)}, 1},
      {"coalesce", {Value(), Value()}, 1},
      {"error", {Value(std::string("boom"))}, 0},
      {"bit_cast_to_int64", {Value(true)}, -1},
      {"bit_cast_to_uint64", {Value(true)}, -1},
      {"__bit_and", {kArrInt}, -1},
      {"__bit_or", {kArrInt}, -1},
      {"__bit_xor", {kArrInt}, -1},
      {"__shift_left", {Value(1), Value(2)}, -1},
      {"generate_uuid", {}, 1},
      {"session_user", {}, 1},
      {"current_user", {}, 1},
      {"string", {kInt}, -1},
      {"count", {kInt}, -1},
      {"amin", {kArrInt}, -1},
      {"amax", {kArrInt}, -1},
      {"hll_count.extract", {Value(std::string("sketch"))}, -1},
      {"value", {kInt}, -1},
      {"__quantified__", {kInt}, -1},

      // ---- guards and internal plumbing ------------------------------------
      {"$proto_repeated_guard", {Arr({Value(1)}), Value(9)}, 1},
      {"$proto_repeated_guard", {kInt, Value(9)}, 0},
      {"$proto_repeated_guard", {Value(), Value(9)}, 1},
      {"$proto_field_guard", {kInt, Value(std::string("INT64")), Value(7)}, 1},
      {"$proto_field_guard", {kInt}, 0},
      {"$proto_enum_guard", {kInt, Value(std::string("MEMBER"))}, 1},
      {"$proto_enum_guard", {kInt}, 0},
      {"__proto_new", {kStr}, -1},
      {"__proto_set", {kStr, kStr, kInt}, -1},
      {"__get_extension", {kStr}, -1},
      {"__value_table_proto", {kStr}, -1},
      {"__value_table_proto_existing", {kStr}, -1},
      {"__value_table_value", {kStr}, -1},

      // ---- unknown / contract rejections ------------------------------------
      {"unsupported_fn", {kInt}, 0},
      {"abs", {}, 0},
      {"if", {Value(true)}, 0},
  };
}

}  // namespace

class RelationalFunctionSweepTest : public ::testing::Test {
 protected:
  void SetUp() override {
    database_ =
        Database::Create("expression_eval_sweep-" + RandomString()).MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
  }
  void TearDown() override {
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }

  // Evaluates one function call through the relational evaluator.  Returns
  // nullopt when the call failed (Status or boundary exception); `leaked`
  // reports the exception case separately so expect<=0 cases cannot mask an
  // EXC-SHIM boundary leak as a legitimate failure.
  std::optional<Value> Evaluate(const std::string& name,
                                const std::vector<Value>& args,
                                bool* leaked = nullptr) {
    std::vector<Expression> children;
    children.reserve(args.size());
    for (const Value& value : args) {
      children.push_back(ConstantValueExp(value));
    }
    const Expression call = FunctionCallExp(name, std::move(children));
    static const Schema schema("t", {Column("c0", ValueType::kInt64)});
    static const Row row({Value(1)});
    const relational_detail::Scope scope{.row = &row, .schema = &schema};
    relational_detail::CteMap ctes;
    try {
      StatusOr<Value> result =
          relational_detail::TryEvaluate(call, scope, nullptr, *context_, ctes);
      if (!result.HasValue()) {
        return std::nullopt;
      }
      return result.MoveValue();
    } catch (const std::exception&) {
      if (leaked != nullptr) {
        *leaked = true;
      }
      return std::nullopt;  // EXC-SHIM boundary leak counts as failure.
    }
  }

  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
};

TEST_F(RelationalFunctionSweepTest, EveryDispatchedFunctionReturnsCleanly) {
  const std::vector<Case> cases = SweepCases();
  SCOPED_TRACE(testing::Message() << "cases=" << cases.size());
  for (const Case& c : cases) {
    SCOPED_TRACE(testing::Message()
                 << c.name << "/" << c.args.size() << " args");
    bool leaked = false;
    const std::optional<Value> result = Evaluate(c.name, c.args, &leaked);
    EXPECT_FALSE(leaked) << "exception crossed the Status boundary: " << c.name;
    if (c.expect == 1) {
      ASSERT_TRUE(result.has_value()) << "expected success: " << c.name
                                      << " with " << c.args.size() << " args";
    } else if (c.expect == 0) {
      EXPECT_FALSE(result.has_value()) << "expected failure: " << c.name
                                       << " with " << c.args.size() << " args";
    }
    // Either outcome must produce a dumpable result value.
    if (result.has_value()) {
      std::ostringstream image;
      image << result->AsString();
      EXPECT_FALSE(image.str().empty());
    }
  }
}

TEST_F(RelationalFunctionSweepTest, RegexpReplacementViewsKeepConversions) {
  for (int pass = 0; pass < 2; ++pass) {
    EXPECT_EQ(Evaluate("regexp_replace", {Value(std::string("a\0b", 3)),
                                          Value("(a)"), Value(R"(\1$)")}),
              std::make_optional(Value(std::string("a$\0b", 4))));
    EXPECT_EQ(Evaluate("regexp_replace", {Value(123), Value(2), Value(9)}),
              std::make_optional(Value("193")));
    EXPECT_EQ(Evaluate("regexp_replace",
                       {Value("b"), Value("(a)?b"), Value(R"(\1X)")}),
              std::make_optional(Value("X")));
    EXPECT_EQ(Evaluate("regexp_replace", {Value(), Value("["), Value(R"(\9)")}),
              std::make_optional(Value()));
    EXPECT_EQ(Evaluate("regexp_replace", {Value("a"), Value("["), Value()}),
              std::make_optional(Value()));
    EXPECT_FALSE(
        Evaluate("regexp_replace", {Value("a"), Value("(a)"), Value(R"(\2)")})
            .has_value());
  }
}

// SAFE_CONVERT_BYTES_TO_STRING used to re-decode `\xHH`-shaped byte runs as
// if they were string-literal escapes, silently shrinking valid UTF-8 data
// that merely contained that text. The payload is raw bytes — string-literal
// escapes were already expanded by the visitor — so valid UTF-8 maps to
// itself and only invalid sequences become U+FFFD.
TEST_F(RelationalFunctionSweepTest, SafeConvertBytesToStringKeepsRawBytes) {
  // "\x41\x41\x42" as data is eight ASCII characters, not "AAB".
  const std::string escaped = R"(\x41\x41\x42)";
  EXPECT_EQ(
      Evaluate("safe_convert_bytes_to_string", {Value(std::string(escaped))}),
      std::make_optional(Value(std::string(escaped))));
  // Valid multibyte UTF-8 round-trips untouched (all seven bytes: the
  // four-byte astral sequence plus the complete three-byte one).
  const std::string valid = std::string("\xF0\x90\x8C\xBC\xE2\x82\xA1");
  EXPECT_EQ(
      Evaluate("safe_convert_bytes_to_string", {Value(std::string(valid))}),
      std::make_optional(Value(std::string(valid))));
  // Invalid sequences (compliance golden b'\xe2\x28\xa1') replace per byte.
  const std::string invalid = std::string("\xE2\x28\xA1", 3);
  const std::optional<Value> replaced =
      Evaluate("safe_convert_bytes_to_string", {Value(std::string(invalid))});
  ASSERT_TRUE(replaced.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  EXPECT_EQ(replaced->AsStringView(),
            std::string("\xEF\xBF\xBD(\xEF\xBF\xBD", 7));
}

// The byte-run validator above dropped the two UPPER bounds of the canonical
// validator (query/googlesql_ast_visitor_expr.cpp IsValidUtf8Text): a lead of
// 0xED kept no ceiling on the second byte, so surrogate half-sequences
// (ED A0-BF .., U+D800-DFFF) were accepted as text, and a lead of 0xF4 had no
// ceiling either, so anything above U+10FFFF (F4 90-BF ..) passed. Both shapes
// are what CHR() refuses; the conversion must replace them with U+FFFD.
TEST_F(RelationalFunctionSweepTest,
       SafeConvertBytesToStringRejectsSurrogatesAndOverrange) {
  // Surrogate half pairs: three invalid bytes -> three replacements, one per
  // byte (the same granularity the b'\xe2\x28\xa1' case above pins).
  const std::string surrogate =
      std::string("\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD", 9);
  EXPECT_EQ(Evaluate("safe_convert_bytes_to_string",
                     {Value(std::string("\xED\xA0\x80", 3))}),
            std::make_optional(Value(std::string(surrogate))));
  EXPECT_EQ(Evaluate("safe_convert_bytes_to_string",
                     {Value(std::string("\xED\xBF\xBF", 3))}),
            std::make_optional(Value(std::string(surrogate))));
  // Above U+10FFFF: four bytes -> four replacements.
  EXPECT_EQ(Evaluate("safe_convert_bytes_to_string",
                     {Value(std::string("\xF4\x90\x80\x80", 4))}),
            std::make_optional(Value(std::string(
                "\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD\xEF\xBF\xBD", 12))));
  // The new ceilings must not over-reject: U+D7FF is the last code point
  // below the surrogate range and U+10FFFF the last there is, so both still
  // round-trip untouched.
  const std::string last_bmp = std::string("\xED\x9F\xBF", 3);
  EXPECT_EQ(
      Evaluate("safe_convert_bytes_to_string", {Value(std::string(last_bmp))}),
      std::make_optional(Value(std::string(last_bmp))));
  const std::string last_code_point = std::string("\xF4\x8F\xBF\xBF", 4);
  EXPECT_EQ(Evaluate("safe_convert_bytes_to_string",
                     {Value(std::string(last_code_point))}),
            std::make_optional(Value(std::string(last_code_point))));
  // A truncated four-byte sequence still degrades per byte.
  EXPECT_EQ(
      Evaluate("safe_convert_bytes_to_string", {Value(std::string("\xF4", 1))}),
      std::make_optional(Value(std::string("\xEF\xBF\xBD", 3))));
}

TEST_F(RelationalFunctionSweepTest, CanonicalPins) {
  struct Pin {
    const char* name;
    std::vector<Value> args;
    Value expected;
  };
  const std::vector<Pin> pins = {
      {"abs", {Value(-4)}, Value(4)},
      {"concat",
       {Value(std::string("a")), Value(std::string("b"))},
       Value(std::string("ab"))},
      {"length", {Value(std::string("abcd"))}, Value(4)},
      // LENGTH/CHAR_LENGTH/CHARACTER_LENGTH count Unicode code points;
      // like the AST evaluator they skip orphan continuation bytes (0x80-0xBF).
      {"length", {Value(std::string("\xC3\xA1"))}, Value(1)},
      {"char_length", {Value(std::string("\xC3\xA1"))}, Value(1)},
      {"character_length", {Value(std::string("\xC3\xA1"))}, Value(1)},
      {"char_length",
       {Value(std::string("a\x80"
                          "b"))},
       Value(2)},
      // STRPOS counts code points before the byte match with the same
      // continuation-byte-skip rule the AST evaluator uses.
      {"strpos",
       {Value(std::string("a\x80"
                          "z")),
        Value(std::string("z"))},
       Value(2)},
      {"lower", {Value(std::string("AbC"))}, Value(std::string("abc"))},
      {"upper", {Value(std::string("AbC"))}, Value(std::string("ABC"))},
      {"reverse", {Value(std::string("abc"))}, Value(std::string("cba"))},
      {"trim", {Value(std::string("  a  "))}, Value(std::string("a"))},
      {"substr",
       {Value(std::string("abcde")), Value(2), Value(3)},
       Value(std::string("bcd"))},
      {"replace",
       {Value(std::string("aaa")), Value(std::string("a")),
        Value(std::string("bb"))},
       Value(std::string("bbbbbb"))},
      {"starts_with",
       {Value(std::string("abc")), Value(std::string("ab"))},
       Value(true)},
      {"ends_with",
       {Value(std::string("abc")), Value(std::string("bc"))},
       Value(true)},
      {"instr", {Value(std::string("abc")), Value(std::string("c"))}, Value(3)},
      {"strpos",
       {Value(std::string("abc")), Value(std::string("c"))},
       Value(3)},
      {"repeat",
       {Value(std::string("ab")), Value(2)},
       Value(std::string("abab"))},
      {"ascii", {Value(std::string("A"))}, Value(65)},
      {"unicode", {Value(std::string("A"))}, Value(65)},
      {"chr", {Value(65)}, Value(std::string("A"))},
      {"array_first", {Arr({Value(9), Value(8)})}, Value(9)},
      {"array_length", {Arr({Value(9), Value(8)})}, Value(2)},
      {"array_reverse", {Arr({Value(9), Value(8)})}, Arr({Value(8), Value(9)})},
      {"array_is_distinct", {Arr({Value(1), Value(2)})}, Value(true)},
      {"array_is_distinct", {Arr({Value(1), Value(1)})}, Value(false)},
      {"array_includes", {Arr({Value(1), Value(2)}), Value(2)}, Value(true)},
      {"array_to_string",
       {Arr({Value(std::string("a")), Value(std::string("b"))}, "STRING"),
        Value(std::string(","))},
       Value(std::string("a,b"))},
      {"safe_divide", {Value(7), Value(2)}, Value(3.5)},
      {"safe_add", {Value(1), Value(2)}, Value(3)},
      {"safe_subtract", {Value(5), Value(2)}, Value(3)},
      {"safe_multiply", {Value(3), Value(4)}, Value(12)},
      {"safe_negate", {Value(4)}, Value(-4)},
      {"div", {Value(7), Value(2)}, Value(3)},
      {"mod", {Value(7), Value(2)}, Value(1)},
      {"greatest", {Value(1), Value(3), Value(2)}, Value(3)},
      {"coalesce", {Value(), Value(2), Value(3)}, Value(2)},
      {"ifnull", {Value(), Value(5)}, Value(5)},
      {"nullif", {Value(1), Value(1)}, Value()},
      {"nullif", {Value(1), Value(2)}, Value(1)},
      {"if", {Value(true), Value(1), Value(2)}, Value(1)},
      {"if", {Value(false), Value(1), Value(2)}, Value(2)},
      {"iferror", {Value(1), Value(0)}, Value(1)},
      {"generate_array",
       {Value(1), Value(4), Value(1)},
       Arr({Value(1), Value(2), Value(3), Value(4)})},
      {"date_from_unix_date", {Value(0)}, Value::Date("1970-01-01")},
      {"unix_date", {Value::Date("1970-01-01")}, Value(0)},
      {"extract_year", {Value::Date("2026-09-22")}, Value(2026)},
      {"extract_month", {Value::Date("2026-09-22")}, Value(9)},
      {"extract_day", {Value::Date("2026-09-22")}, Value(22)},
      {"byte_length", {Value(std::string("abcd"))}, Value(4)},
      {"to_json_string", {Value(7)}, Value(std::string("7"))},
      {"json_type",
       {Value(std::string(R"({"a": [1]})"))},
       Value(std::string("object"))},
      {"json_array_length", {Value(std::string("[1, 2, 3]"))}, Value(3)},
      {"json_keys",
       {Value(std::string(R"({"b": 1, "a": 2})"))},
       Arr({Value(std::string("b")), Value(std::string("a"))}, "STRING")},
      {"json_set",
       {Value(std::string(R"({"a": 1})")), Value(std::string("$.b")), Value(2)},
       Value(std::string(R"({"a":1,"b":2})"))},
      {"json_object",
       {Value(std::string("k")), Value(1)},
       Value(std::string(R"({"k":1})"))},
      {"lax_int64", {Value(std::string("5.9"))}, Value(5)},
      {"lax_float64", {Value(std::string("2.5"))}, Value(2.5)},
      {"lax_bool",
       {Value(std::string("false"))},
       Value(static_cast<int64_t>(0))},
      {"lax_string", {Value(42)}, Value(std::string("42"))},
  };
  for (const Pin& pin : pins) {
    SCOPED_TRACE(testing::Message() << pin.name << "/" << pin.args.size());
    const std::optional<Value> result = Evaluate(pin.name, pin.args);
    ASSERT_TRUE(result.has_value()) << "pin must succeed: " << pin.name;
    assert(result.has_value());
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(*result, pin.expected) << pin.name;
  }
}

TEST_F(RelationalFunctionSweepTest, DeterminismAndNullPropagation) {
  // Pure functions of constant arguments return the same value on re-eval.
  const std::vector<Value> args = {Value(std::string("Hello"))};
  const std::optional<Value> first = Evaluate("upper", args);
  const std::optional<Value> second = Evaluate("upper", args);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  assert(first.has_value());
  assert(second.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(first, *second);

  // NULL arguments: the evaluator must either return NULL or fail cleanly,
  // never crash, for a representative slice of the function surface.
  const std::vector<std::string> null_tolerant_probe = {
      "abs",          "concat",    "length",   "upper",       "lower",
      "reverse",      "trim",      "substr",   "array_first", "array_length",
      "extract_year", "unix_date", "coalesce", "ifnull"};
  for (const std::string& name : null_tolerant_probe) {
    SCOPED_TRACE(name);
    std::optional<Value> result;
    EXPECT_NO_THROW({ result = Evaluate(name, {Value()}); });
    // Either NULL or a clean error is allowed; a fabricated non-NULL value
    // (empty string, 0, "NULL") is the regression this pin must catch.
    if (result.has_value()) {
      EXPECT_TRUE(result->IsNull());
    }
  }
}

TEST_F(RelationalFunctionSweepTest,
       ExtractSeparatesTimestampAndIntervalDomains) {
  const std::vector<std::pair<std::string, int64_t>> parts{
      {"year", 2024},
      {"month", 2},
      {"day", 29},
      {"hour", 12},
      {"minute", 34},
      {"second", 56},
      {"millisecond", 123},
      {"microsecond", 123456},
      {"nanosecond", 123456789}};
  for (const auto& [part, expected] : parts) {
    for (const char* text : {"2024-02-29 12:34:56.123456789+00",
                             "2024-02-29T12:34:56.123456789Z"}) {
      EXPECT_EQ(Evaluate("extract_" + part, {Value(text)}),
                std::make_optional(Value(expected)))
          << part << " / " << text;
    }
    EXPECT_EQ(Evaluate("extract_" + part, {Value()}),
              std::make_optional(Value()));
  }
  EXPECT_EQ(Evaluate("extract_hour",
                     {Value("2024-02-29 12:34:56+00"), Value("+05:30")}),
            std::make_optional(Value(18)));
  EXPECT_EQ(Evaluate("extract_minute",
                     {Value("2024-02-29 12:34:56+00"), Value("+05:30")}),
            std::make_optional(Value(4)));
  EXPECT_EQ(
      Evaluate("extract_minute", {Value("2024-02-29 12:34:56+00"), Value()}),
      std::make_optional(Value()));
  for (const char* text :
       {"2024-02-30 12:34:56+00", "2024-02-29 25:00:00+00", "not-a-date"}) {
    EXPECT_FALSE(Evaluate("extract_minute", {Value(text)}).has_value());
  }
  const std::vector<std::pair<std::string, int64_t>> interval_parts{
      {"year", 1},
      {"month", 2},
      {"day", 3},
      {"hour", 4},
      {"minute", 5},
      {"second", 6},
      {"millisecond", 123},
      {"microsecond", 123456},
      {"nanosecond", 123456789}};
  for (const auto& [part, expected] : interval_parts) {
    const Value interval = Value("1-2 3 4:5:6.123456789").WithInterval();
    EXPECT_EQ(Evaluate("extract_" + part, {interval}),
              std::make_optional(Value(expected)))
        << part;
    const auto ast = ExecuteFunction("extract_" + part, {interval});
    EXPECT_TRUE(ast.HasValue()) << part;
    if (ast.HasValue()) {
      EXPECT_EQ(ast.Value(), Value(expected)) << part;
    }
  }
}

TEST_F(RelationalFunctionSweepTest, FixedCivilTimesMatchGeneralParser) {
  // +0000 retains the same UTC meaning but uses the general input parser.
  // Include century leap-year rules, both ends of the supported year range,
  // and day/week/month boundaries in positive and negative epochs.
  for (const char* date :
       {"0001-01-01", "1582-10-15", "1900-02-28", "1969-12-31", "1970-01-01",
        "2000-02-29", "2024-02-29", "2024-12-31", "9999-12-31"}) {
    for (const char* time : {"00:00:00", "12:34:56", "23:59:59"}) {
      const std::string canonical =
          std::string(date).append(" ").append(time).append("+00");
      for (const char* unit : {"year", "quarter", "month", "week", "day",
                               "hour", "minute", "second"}) {
        for (const char* zone : {"UTC", "+05:30", "-03:30"}) {
          SCOPED_TRACE(testing::Message()
                       << canonical << " " << unit << " " << zone);
          const auto fixed =
              Evaluate("timestamp_trunc",
                       {Value(std::string(canonical)), Value(std::string(unit)),
                        Value(std::string(zone))});
          const auto general =
              Evaluate("timestamp_trunc",
                       {Value(canonical + "00"), Value(std::string(unit)),
                        Value(std::string(zone))});
          ASSERT_TRUE(fixed.has_value());
          ASSERT_TRUE(general.has_value());
          EXPECT_EQ(fixed, general);
        }
      }
      const auto minute = Evaluate(
          "timestamp_trunc",
          {Value(std::string(canonical)), Value("minute"), Value("UTC")});
      ASSERT_TRUE(minute.has_value());
      const std::string expected =
          std::string(date).append(" ").append(time, 0, 6).append("00+00");
      EXPECT_EQ(minute, std::make_optional(Value(std::string(expected))));
      for (const char* unit : {"year", "month", "day"}) {
        // Bare DATE strings exercise the ten-byte form without a timestamp.
        const auto result = Evaluate(
            "datetime_trunc",
            {Value(std::string(date)), Value(std::string(unit)), Value("UTC")});
        const auto midnight = Evaluate(
            "datetime_trunc", {Value(std::string(date) + " 00:00:00"),
                               Value(std::string(unit)), Value("UTC")});
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(midnight.has_value());
        EXPECT_EQ(result, midnight);
      }
    }
  }
}

TEST_F(RelationalFunctionSweepTest, PackedUtcTruncationKeysMatchReference) {
  using relational_detail::CompileUtcTimestampTrunc;
  using relational_detail::FormatUtcTimestampTruncKey;
  using relational_detail::TryUtcTimestampTruncKey;
  const std::string previous = GetDefaultTimeZone();
  struct Restore {  // NOLINT(cppcoreguidelines-special-member-functions): local scope guard.
    explicit Restore(std::string value) : zone(std::move(value)) {}
    Restore(const Restore&) = delete;
    Restore& operator=(const Restore&) = delete;
    Restore(Restore&&) = delete;
    Restore& operator=(Restore&&) = delete;
    ~Restore() { SetDefaultTimeZone(zone); }
    std::string zone;
  } restore(previous);
  SetDefaultTimeZone("UTC");
  for (const char* unit : {"SECOND", "minutes", "HOUR", "day"}) {
    const auto expression = FunctionCallExp(
        "timestamp_trunc", {ColumnValueExp("s"), ColumnValueExp(unit),
                            ConstantValueExp(Value("UTC"))});
    const auto compiled =
        CompileUtcTimestampTrunc(expression->AsFunctionCallExpression());
    ASSERT_TRUE(compiled);
    const auto literal = FunctionCallExp(
        "timestamp_trunc",
        {ColumnValueExp("s"), ConstantValueExp(Value(std::string(unit)))});
    EXPECT_EQ(CompileUtcTimestampTrunc(literal->AsFunctionCallExpression()),
              compiled);
    int64_t last_key = 0;
    std::string last_text;
    for (const char* date :
         {"0001-01-01", "1582-10-15", "1900-02-28", "1969-12-31", "1970-01-01",
          "2000-02-29", "2024-02-29", "9999-12-31"}) {
      for (const char* time : {"00:00:00", "12:34:56", "23:59:59"}) {
        for (const char* separator : {" ", "T"}) {
          for (const char* suffix : {"", "+00"}) {
            std::string text = date;
            text += separator;
            text += time;
            text += suffix;
            SCOPED_TRACE(testing::Message() << text << " " << unit);
            const auto key = TryUtcTimestampTruncKey(text, *compiled);
            ASSERT_TRUE(key);
            ASSIGN_OR_ASSERT_FAIL(Value, formatted,
                                  FormatUtcTimestampTruncKey(*key));
            const auto reference = Evaluate(
                "timestamp_trunc", {Value(std::string(text)),
                                    Value(std::string(unit)), Value("UTC")});
            ASSERT_TRUE(reference);
            EXPECT_EQ(formatted, *reference);
            EXPECT_GE(*key, last_key);
            EXPECT_GE(formatted.AsStringView(), std::string_view(last_text));
            last_key = *key;
            last_text = formatted.AsStringView();
          }
        }
      }
    }
    for (const char* text :
         {"", "2024-01-01", "2024-01-01 00:00:00+0000",
          "2024-01-01 00:00:00-01", "2024-01-01 00:00:00.1+00",
          "2024-01-01 00:00:60+00", " 2024-01-01 00:00:00+00 ",
          "0000-01-01 00:00:00+00", "1900-02-29 00:00:00+00",
          "2024-13-01 00:00:00+00", "2024-01-01 24:00:00+00",
          "2024-01-01 00:60:00+00", "2024-01-01 00:00:61+00"}) {
      EXPECT_FALSE(TryUtcTimestampTruncKey(text, *compiled)) << text;
    }
  }
  for (const int64_t key :
       {int64_t{0}, int64_t{20240230000000LL}, int64_t{20240101006000LL},
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max()}) {
    EXPECT_EQ(FormatUtcTimestampTruncKey(key).GetStatus(),
              Status::kInvalidArgument);
  }
  for (const Value& unit : {Value(), Value("month"), Value(1)}) {
    const auto call = FunctionCallExp(
        "timestamp_trunc", {ColumnValueExp("s"), ConstantValueExp(unit)});
    EXPECT_FALSE(CompileUtcTimestampTrunc(call->AsFunctionCallExpression()));
  }
  for (const Value& zone : {Value(), Value("+09:00"), Value("invalid_zone")}) {
    const auto call = FunctionCallExp(
        "timestamp_trunc", {ColumnValueExp("s"), ColumnValueExp("MINUTE"),
                            ConstantValueExp(zone)});
    EXPECT_FALSE(CompileUtcTimestampTrunc(call->AsFunctionCallExpression()));
  }
  SetDefaultTimeZone("+09:00");
  const auto call = FunctionCallExp(
      "timestamp_trunc", {ColumnValueExp("s"), ColumnValueExp("MINUTE"),
                          ConstantValueExp(Value("UTC"))});
  EXPECT_FALSE(CompileUtcTimestampTrunc(call->AsFunctionCallExpression()));
}

TEST_F(RelationalFunctionSweepTest, CivilTimeFractionsLeapSecondsAndErrors) {
  struct Pin {
    const char* input;
    const char* unit;
    const char* expected;
  };
  for (const Pin& pin : {Pin{.input = "2000-02-29 23:59:60+00",
                             .unit = "second",
                             .expected = "2000-03-01 00:00:00+00"},
                         Pin{.input = "1969-12-31 23:59:60+00",
                             .unit = "minute",
                             .expected = "1970-01-01 00:00:00+00"},
                         Pin{.input = "2024-01-02 03:04:05.000000001+00",
                             .unit = "nanosecond",
                             .expected = "2024-01-02 03:04:05.000000001+00"},
                         Pin{.input = "2024-01-02 03:04:05.123456789+00",
                             .unit = "millisecond",
                             .expected = "2024-01-02 03:04:05.123+00"},
                         Pin{.input = "2024-01-02 03:04:05.123456789+00",
                             .unit = "microsecond",
                             .expected = "2024-01-02 03:04:05.123456+00"},
                         Pin{.input = "2024-01-02 03:04:05.010000000+00",
                             .unit = "nanosecond",
                             .expected = "2024-01-02 03:04:05.010+00"},
                         Pin{.input = "2024-01-02 03:04:05.000010000+00",
                             .unit = "nanosecond",
                             .expected = "2024-01-02 03:04:05.000010+00"}}) {
    SCOPED_TRACE(pin.input);
    const auto result = Evaluate(
        "timestamp_trunc", {Value(pin.input), Value(pin.unit), Value("UTC")});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result, std::make_optional(Value(pin.expected)));
  }
  for (const char* text : {"0000-01-01 00:00:00+00", "1900-02-29 00:00:00+00",
                           "2024-13-01 00:00:00+00", "2024-01-32 00:00:00+00",
                           "2024-01-01 24:00:00+00", "2024-01-01 00:60:00+00",
                           "2024-01-01 00:00:61+00", "9999-12-31 23:59:60+00",
                           "2024-x1-01 00:00:00+00", ""}) {
    SCOPED_TRACE(text);
    EXPECT_FALSE(Evaluate("timestamp_trunc", {Value(std::string(text)),
                                              Value("minute"), Value("UTC")})
                     .has_value());
  }
  EXPECT_EQ(Evaluate("timestamp_trunc", {Value(" 2024-01-02T03:04:05+00 "),
                                         Value("minute"), Value("UTC")}),
            std::make_optional(Value("2024-01-02 03:04:00+00")));
  const auto null =
      Evaluate("timestamp_trunc", {Value(), Value("minute"), Value("UTC")});
  ASSERT_TRUE(null.has_value());
  EXPECT_EQ(null, std::make_optional(Value()));
  EXPECT_FALSE(Evaluate("timestamp_trunc", {Value("2024-01-01 00:00:00+00"),
                                            Value("fortnight"), Value("UTC")})
                   .has_value());
  EXPECT_FALSE(
      Evaluate("timestamp_trunc", {Value("2024-01-01 00:00:00+00"),
                                   Value("minute"), Value("invalid_time_zone")})
          .has_value());
}

TEST_F(RelationalFunctionSweepTest, TruncationDatePartIsNotARowReference) {
  const Schema schema("t", {Column("s", ValueType::kVarChar),
                            Column("MINUTE", ValueType::kVarChar),
                            Column("payload", ValueType::kVarChar)});
  const Row row({Value("2024-02-29 12:34:56+00"), Value("invalid-unit"),
                 Value(std::string(4096, 'x'))});
  const relational_detail::Scope scope{.row = &row, .schema = &schema};
  for (const char* function :
       {"date_trunc", "datetime_trunc", "timestamp_trunc"}) {
    for (const char* unit : {"MINUTE", "HOUR", "DAY"}) {
      SCOPED_TRACE(testing::Message() << function << " " << unit);
      const auto symbolic =
          FunctionCallExp(function, {ColumnValueExp("s"), ColumnValueExp(unit),
                                     ConstantValueExp(Value("UTC"))});
      const auto literal = FunctionCallExp(
          function, {ColumnValueExp("s"), ConstantValueExp(Value(unit)),
                     ConstantValueExp(Value("UTC"))});
      ASSIGN_OR_ASSERT_FAIL(Value, actual,
                            relational_detail::TryEvaluate(
                                symbolic, scope, nullptr, *context_, {}));
      ASSIGN_OR_ASSERT_FAIL(Value, expected,
                            relational_detail::TryEvaluate(
                                literal, scope, nullptr, *context_, {}));
      EXPECT_EQ(actual, expected);
    }
  }
}

TEST_F(RelationalFunctionSweepTest, AuditControlFlowMatchesAst) {
  const Schema schema("t", {Column("v", ValueType::kInt64)});
  const Row row({Value(1)});
  const relational_detail::Scope scope{.row = &row, .schema = &schema};
  const relational_detail::CteMap ctes;
  const std::vector<Expression> expressions{
      FunctionCallExp("if", {ConstantValueExp(Value(true)), ColumnValueExp("v"),
                             ConstantValueExp(Value(2.0))}),
      FunctionCallExp("if", {ConstantValueExp(Value(true)),
                             ConstantValueExp(Value(-1).WithUnsigned()),
                             ConstantValueExp(Value(2.0))}),
      FunctionCallExp(
          "ifnull",
          {ColumnValueExp("v"),
           FunctionCallExp("error", {ConstantValueExp(Value("untaken"))})})};
  for (const auto& expression : expressions) {
    const auto expected = expression->TryEvaluate(row, schema);
    const auto actual = relational_detail::TryEvaluate(
        expression, scope, nullptr, *context_, ctes);
    ASSERT_TRUE(expected.HasValue());
    ASSERT_TRUE(actual.HasValue()) << actual.GetStatus();
    EXPECT_EQ(actual.Value().type, expected.Value().type);
    EXPECT_EQ(actual.Value(), expected.Value());
  }
}

TEST_F(RelationalFunctionSweepTest, AuditStructEncodingMatchesAst) {
  for (const Value& text :
       {Value(std::string("quote\"\n\t")), Value("{\"nested\":1}"),
        Value("null"), Value("UPPER").WithCollation(2)}) {
    for (const Value& quoted : {Value(), Value(0), Value(1)}) {
      const std::vector<Value> args{Value("key"), text, quoted};
      const auto expected = ExecuteFunction("__struct_json__", args);
      const auto actual = Evaluate("__struct_json__", args);
      ASSERT_TRUE(expected.HasValue());
      if (!actual.has_value()) {
        ADD_FAILURE() << "STRUCT evaluation failed";
        continue;
      }
      EXPECT_EQ(actual->AsStringView(), expected.Value().AsStringView());
    }
  }
}

TEST_F(RelationalFunctionSweepTest, StringFunctionsUseLargeUnsignedArguments) {
  const Value huge = Value(-1).WithUnsigned();
  const Value text("abc");
  struct ExpectedCall {
    const char* name;
    std::vector<Value> args;
    Value expected;
  };
  for (const auto& call : std::vector<ExpectedCall>{
           {"left", {text, huge}, text},
           {"byte_right", {text, huge}, text},
           {"byte_substr", {text, huge}, Value("")},
           {"byte_substr", {text, Value(2), huge}, Value("bc")},
           {"substr", {text, Value(2), huge}, Value("bc")},
           {"instr", {text, Value("b"), huge}, Value(0)},
           {"split_substr",
            {Value("a,b,c"), Value(","), Value(2), huge},
            Value("b,c")},
           {"regexp_instr", {text, Value("b"), huge}, Value(0)},
           {"regexp_extract", {text, Value("b"), Value(1), huge}, Value()},
           {"repeat", {Value(""), huge}, Value("")}}) {
    SCOPED_TRACE(call.name);
    const auto ast = ExecuteFunction(call.name, call.args);
    ASSERT_TRUE(ast.HasValue()) << ast.GetStatus();
    EXPECT_EQ(ast.Value(), call.expected);
    EXPECT_EQ(Evaluate(call.name, call.args),
              std::make_optional(call.expected));
  }
  for (const char* name : {"repeat", "lpad", "rpad"}) {
    SCOPED_TRACE(name);
    const auto ast = ExecuteFunction(name, {text, huge});
    ASSERT_FALSE(ast.HasValue());
    EXPECT_NE(
        ast.GetStatus().GetMessage().find("exceeds max allowed output size"),
        std::string::npos);
    EXPECT_FALSE(Evaluate(name, {text, huge}).has_value());
  }
}

TEST_F(RelationalFunctionSweepTest, CivilFunctionsEvaluateOperandsOnce) {
  // Interval nodes in this evaluator delegate to their virtual TryEvaluate.
  // Use that hook to observe evaluation of arguments without clock or RNG
  // dependencies.
  struct CountedDate final : IntervalExpression {
    CountedDate(Value value, int& evaluations)
        : IntervalExpression(1, "DAY"),
          date(std::move(value)),
          calls(evaluations) {}
    [[nodiscard]] StatusOr<Value> TryEvaluate(const Row& /*unused*/, const Schema& /*unused*/) const override {
      ++calls;
      return date;
    }
    Value date;
    int& calls;
  };
  const Row row;
  const Schema schema;
  const relational_detail::Scope scope{.row = &row, .schema = &schema};
  int left_calls = 0;
  int right_calls = 0;
  auto left =
      std::make_shared<CountedDate>(Value::Date("2026-01-10"), left_calls);
  auto right =
      std::make_shared<CountedDate>(Value::Date("2026-01-01"), right_calls);
  const auto difference = relational_detail::TryEvaluate(
      FunctionCallExp("date_diff", {left, right, ColumnValueExp("DAY")}), scope,
      nullptr, *context_, {});
  ASSERT_TRUE(difference.HasValue()) << difference.GetStatus();
  EXPECT_EQ(difference.Value(), Value(9));
  EXPECT_EQ(left_calls, 1);
  EXPECT_EQ(right_calls, 1);

  const auto bucket = relational_detail::TryEvaluate(
      FunctionCallExp(
          "date_bucket",
          {left, std::make_shared<IntervalExpression>(7, "DAY"), right}),
      scope, nullptr, *context_, {});
  ASSERT_TRUE(bucket.HasValue()) << bucket.GetStatus();
  EXPECT_EQ(bucket.Value(), Value::Date("2026-01-08"));
  EXPECT_EQ(left_calls, 2);
  EXPECT_EQ(right_calls, 2);
}

TEST_F(RelationalFunctionSweepTest, AuditArrayIndexesRejectNonIntegers) {
  const Value array = Arr({Value(10), Value(20)});
  for (const Value& index : {Value(1.0), Value("1"), Value::DateFromDays(1)}) {
    for (const char* name :
         {"array_element_offset", "array_element_ordinal",
          "array_element_safe_offset", "array_element_safe_ordinal"}) {
      EXPECT_FALSE(Evaluate(name, {array, index}).has_value());
    }
    EXPECT_FALSE(Evaluate("array_slice", {array, index, Value(1)}).has_value());
    EXPECT_FALSE(Evaluate("array_slice", {array, Value(1), index}).has_value());
  }
}

TEST_F(RelationalFunctionSweepTest, BucketRejectsOverflowingWidth) {
  const relational_detail::Scope scope{};
  for (const auto& [amount, unit] :
       std::vector<std::pair<int64_t, std::string>>{
           {106752, "day"}, {15300, "week"}, {-106752, "day"}}) {
    const auto expression =
        FunctionCallExp("timestamp_bucket",
                        {ConstantValueExp(Value("2020-01-01 00:00:00+00")),
                         std::make_shared<IntervalExpression>(amount, unit)});
    const auto result = relational_detail::TryEvaluate(expression, scope,
                                                       nullptr, *context_, {});
    EXPECT_FALSE(result.HasValue()) << unit;
  }
}

TEST_F(RelationalFunctionSweepTest, ExplicitTemporalZonesAreAppliedOnce) {
  struct Restore {  // NOLINT(cppcoreguidelines-special-member-functions): local scope guard.
    std::string zone = GetDefaultTimeZone();
    ~Restore() { SetDefaultTimeZone(zone); }
  } restore{};
  SetDefaultTimeZone("-08:00");
  const auto evaluate = [&](const char* name, std::vector<Expression> args,
                            const char* expected) {
    SCOPED_TRACE(name);
    const relational_detail::Scope scope{};
    const auto expr = FunctionCallExp(name, std::move(args));
    ASSIGN_OR_ASSERT_FAIL(
        Value, result,
        relational_detail::TryEvaluate(expr, scope, nullptr, *context_, {}));
    EXPECT_EQ(result, Value(expected));
  };
  for (const std::string marker : {"Z", " UTC", " GMT", " gmt", "+00"}) {
    SCOPED_TRACE(marker);
    const auto input = ConstantValueExp(Value("2021-01-01 03:30:45" + marker));
    evaluate("timestamp", {input}, "2021-01-01 03:30:45+00");
    evaluate("datetime", {input}, "2020-12-31 19:30:45");
    evaluate("time", {input}, "19:30:45");
    evaluate("timestamp_trunc", {input, ColumnValueExp("DAY")},
             "2020-12-31 08:00:00+00");
    evaluate("timestamp_bucket",
             {input, std::make_shared<IntervalExpression>(1, "day"),
              ConstantValueExp(Value("2021-01-01 00:00:00" + marker))},
             "2021-01-01 00:00:00+00");
  }
  evaluate("time", {ConstantValueExp(Value("12:30:45+05:00"))}, "23:30:45");
  evaluate("time", {ConstantValueExp(Value("12:30:45-05:00"))}, "09:30:45");
  const auto offset = ConstantValueExp(Value("2021-01-01 12:30:45+05:00"));
  evaluate("timestamp", {offset}, "2021-01-01 07:30:45+00");
  evaluate("datetime", {offset}, "2020-12-31 23:30:45");
}

TEST_F(RelationalFunctionSweepTest, AuditTimestampArithmeticUsesAbsoluteTime) {
  const std::string previous = GetDefaultTimeZone();
  struct Restore {  // NOLINT(cppcoreguidelines-special-member-functions): local scope guard.
    explicit Restore(std::string saved) : zone(std::move(saved)) {}
    Restore(const Restore&) = delete;
    Restore& operator=(const Restore&) = delete;
    Restore(Restore&&) = delete;
    Restore& operator=(Restore&&) = delete;
    std::string zone;
    ~Restore() { SetDefaultTimeZone(zone); }
  } restore{previous};
  for (const char* zone : {"UTC", "America/Los_Angeles", "+05:30"}) {
    SetDefaultTimeZone(zone);
    struct Case {
      const char* input;
      const char* added;
      const char* subtracted;
    };
    for (const Case& pin : {Case{.input = "2020-07-01 12:00:00+00",
                                 .added = "2020-07-01 13:00:00+00",
                                 .subtracted = "2020-07-01 11:00:00+00"},
                            Case{.input = "2020-03-08 09:00:00+00",
                                 .added = "2020-03-08 10:00:00+00",
                                 .subtracted = "2020-03-08 08:00:00+00"},
                            Case{.input = "2020-11-01 08:00:00+00",
                                 .added = "2020-11-01 09:00:00+00",
                                 .subtracted = "2020-11-01 07:00:00+00"}}) {
      const char* input = pin.input;
      for (const bool subtract : {false, true}) {
        SCOPED_TRACE(testing::Message()
                     << zone << " " << input << " " << subtract);
        const auto expression =
            FunctionCallExp(subtract ? "timestamp_sub" : "timestamp_add",
                            {ConstantValueExp(Value(input)),
                             std::make_shared<IntervalExpression>(1, "hour")});
        const relational_detail::Scope scope{};
        ASSIGN_OR_ASSERT_FAIL(Value, result,
                              relational_detail::TryEvaluate(
                                  expression, scope, nullptr, *context_, {}));
        EXPECT_EQ(result, Value(subtract ? pin.subtracted : pin.added));
      }
    }
  }
}

TEST_F(RelationalFunctionSweepTest, NamedZoneOffsetUsesHistoricalYear) {
  // America/Los_Angeles observed Pacific War Time (UTC-7) in March 1945.
  // The relational evaluator must look the zone up in the timestamp's own
  // year; clamping it to 1970 would wrongly answer PST (UTC-8) and shift the
  // rendered local time an hour earlier.
  const std::optional<Value> as_string =
      Evaluate("string",
               {Value("1945-03-15 12:00:00+00"), Value("America/Los_Angeles")});
  ASSERT_TRUE(as_string.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  EXPECT_EQ(*as_string, Value("1945-03-15 05:00:00-07"));
  const std::optional<Value> formatted =
      Evaluate("format_timestamp",
               {Value("%Y-%m-%d %H:%M:%S"), Value("1945-03-15 12:00:00+00"),
                Value("America/Los_Angeles")});
  ASSERT_TRUE(formatted.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  EXPECT_EQ(*formatted, Value("1945-03-15 05:00:00"));
}

// The batch function evaluator used to run a private JSON parser that
// disagreed with the AST evaluator on malformed input, number formatting,
// surrogate pairs and NULL array elements. Both must now use the same
// canonical implementation.
TEST_F(RelationalFunctionSweepTest, AuditJsonExtractionMatchesAst) {
  const std::vector<std::vector<Value>> cases{
      {Value(std::string("abc")), Value(std::string("$"))},
      {Value(std::string(R"({"a": 1})")), Value(std::string("$.a"))},
      {Value(std::string(R"({"a": 1}})")), Value(std::string("$.a"))},
      {Value(std::string(R"({"a": 1.50})")), Value(std::string("$.a"))},
      {Value(std::string(R"({"e": "\ud83d\ude00"})")),
       Value(std::string("$.e"))},
  };
  for (const auto& args : cases) {
    for (const char* name :
         {"json_extract", "json_value", "json_extract_scalar"}) {
      const Expression call = FunctionCallExp(
          name, {ConstantValueExp(args[0]), ConstantValueExp(args[1])});
      const auto expected = call->TryEvaluate(Row(), Schema("t", {}));
      const auto actual = Evaluate(name, args);
      ASSERT_TRUE(expected.HasValue()) << name;
      if (!actual.has_value()) {
        ADD_FAILURE() << name << " returned no value";
        return;
      }
      const Value& actual_value = actual.value();
      EXPECT_EQ(actual_value, expected.Value()) << name;
    }
  }

  // NULL elements inside a JSON array must stay NULL in *_ARRAY results.
  const std::vector<Value> arr_args{Value(std::string(R"([null, 2])")),
                                    Value(std::string("$"))};
  const Expression arr_call = FunctionCallExp(
      "json_value_array",
      {ConstantValueExp(arr_args[0]), ConstantValueExp(arr_args[1])});
  const auto arr_expected = arr_call->TryEvaluate(Row(), Schema("t", {}));
  const auto arr_actual = Evaluate("json_value_array", arr_args);
  ASSERT_TRUE(arr_expected.HasValue());
  ASSERT_TRUE(arr_actual.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  EXPECT_EQ(*arr_actual, arr_expected.Value());
}

// P454: __pipe_concat's element contract -- non-`{...}` elements parse
// leniently as their whole body (proto-text shapes) and member keys match
// case-insensitively -- must be identical in the AST ground truth and the
// relational batch path, which used to reject the same inputs with
// "requires STRUCT elements" and exact-case keys.
TEST_F(RelationalFunctionSweepTest, PipeConcatProtoTextElementsMatchAst) {
  const std::vector<Value> args = {
      Arr({Value(std::string("a: 1 b: 2")), Value(std::string("B: 3, A: 4")),
           Value(std::string("bare"))},
          "STRING"),
      Value(std::string("|"))};
  const Expression call = FunctionCallExp(
      "__pipe_concat", {ConstantValueExp(args[0]), ConstantValueExp(args[1])});
  const auto ast = call->TryEvaluate(Row(), Schema("t", {}));
  const auto batch = Evaluate("__pipe_concat", args);
  ASSERT_TRUE(ast.HasValue()) << ast.GetStatus().GetMessage();
  ASSERT_TRUE(batch.has_value())
      << "batch must accept the AST's lenient element contract (P454)";
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  EXPECT_EQ(*batch, ast.Value());
}

}  // namespace tinylamb
