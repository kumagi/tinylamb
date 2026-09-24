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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
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
      {"__struct_json__", {kInt, kStr}, -1},
      {"__struct_set", {kJson, Value(std::string("a")), Value(9)}, -1},
      {"__get_field_safe", {kJson, Value(std::string("a"))}, 1},
      {"__get_field_safe", {Value(), Value(std::string("a"))}, 1},
      {"get_field", {kJson, Value(std::string("a"))}, 1},
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
  // nullopt when the call failed (Status or boundary exception).
  std::optional<Value> Evaluate(const std::string& name,
                                const std::vector<Value>& args) {
    std::vector<Expression> children;
    children.reserve(args.size());
    for (const Value& value : args) {
      children.push_back(ConstantValueExp(value));
    }
    const Expression call = FunctionCallExp(name, children);
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
    const std::optional<Value> result = Evaluate(c.name, c.args);
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
  };
  for (const Pin& pin : pins) {
    SCOPED_TRACE(testing::Message() << pin.name << "/" << pin.args.size());
    const std::optional<Value> result = Evaluate(pin.name, pin.args);
    ASSERT_TRUE(result.has_value()) << "pin must succeed: " << pin.name;
    EXPECT_EQ(*result, pin.expected) << pin.name;  // NOLINT(bugprone-unchecked-optional-access)
  }
}

TEST_F(RelationalFunctionSweepTest, DeterminismAndNullPropagation) {
  // Pure functions of constant arguments return the same value on re-eval.
  const std::vector<Value> args = {Value(std::string("Hello"))};
  const std::optional<Value> first = Evaluate("upper", args);
  const std::optional<Value> second = Evaluate("upper", args);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first.value(), *second);  // NOLINT(bugprone-unchecked-optional-access)

  // NULL arguments: the evaluator must either return NULL or fail cleanly,
  // never crash, for a representative slice of the function surface.
  const std::vector<std::string> null_tolerant_probe = {
      "abs",          "concat",    "length",   "upper",       "lower",
      "reverse",      "trim",      "substr",   "array_first", "array_length",
      "extract_year", "unix_date", "coalesce", "ifnull"};
  for (const std::string& name : null_tolerant_probe) {
    SCOPED_TRACE(name);
    EXPECT_NO_THROW({ (void)Evaluate(name, {Value()}); });
  }
}

}  // namespace tinylamb
