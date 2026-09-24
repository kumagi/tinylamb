/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Drives FunctionCallExpression's AST evaluation path (the ExecuteFunction
// dispatch in function_call_expression.cpp) directly for every dispatched
// function name: one valid argument shape plus error shapes where the
// function validates arguments.  The SQL-level sweeps cover the relational
// evaluator's parallel implementations; this file is the AST ground truth.

#include <gtest/gtest.h>

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "expression/constant_value.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

class NoSubqueryContext : public EvaluationContext {
 public:
  StatusOr<std::vector<Value>> RunSubquery(
      const SelectStatement& /*statement*/ /*statement*/,
      /*outer_row*/ const Row* /*outer_row*/) override {
    return StatusError(StatusCode::kRuntimeError, "no subqueries in this test");
  }
  [[nodiscard]] const AggregateResultMap* CurrentAggregates() const override {
    return nullptr;
  }
  Status GetOrAddFunction(std::string_view /*function_name*/,
                          int /*argument_count*/) override {
    return Status::kSuccess;
  }
};

struct Case {
  const char* name;
  std::vector<Value> args;
  int expect;  // 1: must succeed, 0: must fail, -1: either way.
};

}  // namespace

TEST(FunctionCallAstSweepTest, EvaluateEveryDispatchedFunction) {
  Schema schema("t", {Column("c0", ValueType::kInt64),
                      Column("c1", ValueType::kVarChar)});
  Row row({Value(1), Value("x")});
  Row empty_row({});

  const std::vector<Case> cases = {
      {"pi", {}, 1},
      {"rand", {}, 1},
      {"current_date", {}, 1},
      {"current_datetime", {}, 1},
      {"current_timestamp", {}, 1},
      {"pi", {Value(int64_t{1})}, 0},
      {"abs", {Value(2.5)}, 1},
      {"abs", {Value(int64_t{4})}, 1},
      {"atan", {Value(2.5)}, 1},
      {"atan", {Value(int64_t{4})}, 1},
      {"cbrt", {Value(2.5)}, 1},
      {"cbrt", {Value(int64_t{4})}, 1},
      {"ceil", {Value(2.5)}, 1},
      {"ceil", {Value(int64_t{4})}, 1},
      {"ceiling", {Value(2.5)}, 1},
      {"ceiling", {Value(int64_t{4})}, 1},
      {"degrees", {Value(2.5)}, 1},
      {"degrees", {Value(int64_t{4})}, 1},
      {"exp", {Value(2.5)}, 1},
      {"exp", {Value(int64_t{4})}, 1},
      {"floor", {Value(2.5)}, 1},
      {"floor", {Value(int64_t{4})}, 1},
      {"ln", {Value(2.5)}, 1},
      {"ln", {Value(int64_t{4})}, 1},
      {"log10", {Value(2.5)}, 1},
      {"log10", {Value(int64_t{4})}, 1},
      {"radians", {Value(2.5)}, 1},
      {"radians", {Value(int64_t{4})}, 1},
      {"round", {Value(2.5)}, 1},
      {"round", {Value(int64_t{4})}, 1},
      {"sign", {Value(2.5)}, 1},
      {"sign", {Value(int64_t{4})}, 1},
      {"sin", {Value(2.5)}, 1},
      {"sin", {Value(int64_t{4})}, 1},
      {"cos", {Value(2.5)}, 1},
      {"cos", {Value(int64_t{4})}, 1},
      {"tan", {Value(2.5)}, 1},
      {"tan", {Value(int64_t{4})}, 1},
      {"tanh", {Value(2.5)}, 1},
      {"tanh", {Value(int64_t{4})}, 1},
      {"sinh", {Value(2.5)}, 1},
      {"sinh", {Value(int64_t{4})}, 1},
      {"cosh", {Value(2.5)}, 1},
      {"cosh", {Value(int64_t{4})}, 1},
      {"sqrt", {Value(2.5)}, 1},
      {"sqrt", {Value(int64_t{4})}, 1},
      {"trunc", {Value(2.5)}, 1},
      {"trunc", {Value(int64_t{4})}, 1},
      {"truncate", {Value(2.5)}, 1},
      {"truncate", {Value(int64_t{4})}, 1},
      {"is_inf", {Value(2.5)}, 1},
      {"is_inf", {Value(int64_t{4})}, 1},
      {"is_nan", {Value(2.5)}, 1},
      {"is_nan", {Value(int64_t{4})}, 1},
      {"ascii", {Value(2.5)}, 1},
      {"ascii", {Value(int64_t{4})}, 1},
      {"unicode", {Value(2.5)}, 1},
      {"unicode", {Value(int64_t{4})}, 1},
      {"chr", {Value(2.5)}, 1},
      {"chr", {Value(int64_t{4})}, 1},
      {"to_hex", {Value(2.5)}, 0},
      {"to_hex", {Value(int64_t{4})}, 0},
      {"acos", {Value(0.5)}, 1},
      {"acos", {Value(2.5)}, 0},
      {"acos", {Value(int64_t{4})}, 0},
      {"asin", {Value(0.5)}, 1},
      {"asin", {Value(2.5)}, 0},
      {"asin", {Value(int64_t{4})}, 0},
      {"round", {Value(2.567), Value(int64_t{1})}, 1},
      {"round", {Value(std::string("x"))}, 0},
      {"trunc", {Value(23.56), Value(int64_t{-2})}, 1},
      {"log", {Value(100.0)}, 1},
      {"log", {Value(8.0), Value(2.0)}, 1},
      {"atan2", {Value(1.0), Value(20)}, 1},
      {"pow", {Value(2.0), Value(11)}, 1},
      {"power", {Value(int64_t{2}), Value(int64_t{-2})}, 1},
      {"mod", {Value(int64_t{10}), Value(int64_t{3})}, 1},
      {"mod", {Value(10.5), Value(int64_t{3})}, 1},
      {"mod", {Value(10.5), Value(3.0)}, 0},
      {"mod", {Value(int64_t{10}), Value(int64_t{0})}, 0},
      {"safe_add", {Value(int64_t{1}), Value(int64_t{2})}, 1},
      {"safe_subtract", {Value(int64_t{5}), Value(int64_t{2})}, 1},
      {"safe_multiply", {Value(int64_t{3}), Value(int64_t{4})}, 1},
      {"safe_divide", {Value(1.0), Value(0.0)}, 1},
      {"safe_negate", {Value(int64_t{5})}, 1},
      {"ieee_divide", {Value(7.0), Value(2.0)}, 1},
      {"greatest",
       {Value(int64_t{1}), Value(int64_t{5}), Value(int64_t{3})},
       1},
      {"greatest", {Value(int64_t{1}), Value(), Value(int64_t{3})}, 1},
      {"greatest", {Value(std::string("a")), Value(std::string("b"))}, 1},
      {"least", {Value(int64_t{1}), Value(int64_t{5}), Value(int64_t{3})}, 1},
      {"least", {Value(int64_t{1}), Value(), Value(int64_t{3})}, 1},
      {"least", {Value(std::string("a")), Value(std::string("b"))}, 1},
      {"coalesce", {Value(), Value(), Value(int64_t{3})}, 1},
      {"coalesce", {}, 1},
      {"ifnull", {Value(), Value(std::string("d"))}, 1},
      {"ifnull", {Value(std::string("x")), Value(std::string("d"))}, 1},
      {"nullif", {Value(std::string("a")), Value(std::string("a"))}, 1},
      {"nullif", {Value(std::string("a")), Value(std::string("b"))}, 1},
      {"concat", {Value(std::string("a")), Value(std::string("b"))}, 1},
      {"concat", {Value(std::string("a")), Value(int64_t{1})}, 0},
      {"concat", {}, 1},
      {"length", {Value(std::string("hello"))}, -1},
      {"length", {Value(int64_t{1})}, -1},
      {"char_length", {Value(std::string("hello"))}, 1},
      {"character_length", {Value(std::string("hello"))}, 1},
      {"octet_length", {Value(std::string("hello"))}, 1},
      {"byte_length", {Value(std::string("hello"))}, 1},
      {"ascii", {Value(std::string("A"))}, -1},
      {"ascii", {Value(std::string(""))}, -1},
      {"unicode", {Value(std::string("A"))}, -1},
      {"chr", {Value(int64_t{8364})}, -1},
      {"chr", {Value(int64_t{-1})}, -1},
      {"upper", {Value(std::string("hello world"))}, 1},
      {"lower", {Value(std::string("hello world"))}, 1},
      {"initcap", {Value(std::string("hello world"))}, 1},
      {"reverse", {Value(std::string("hello world"))}, 1},
      {"soundex", {Value(std::string("hello world"))}, 1},
      {"trim", {Value(std::string("  ab  "))}, 1},
      {"trim", {Value(std::string("xxabxx")), Value(std::string("x"))}, 1},
      {"ltrim", {Value(std::string("  ab  "))}, 1},
      {"ltrim", {Value(std::string("xxabxx")), Value(std::string("x"))}, 1},
      {"rtrim", {Value(std::string("  ab  "))}, 1},
      {"rtrim", {Value(std::string("xxabxx")), Value(std::string("x"))}, 1},
      {"left", {Value(std::string("abcdef")), Value(int64_t{3})}, -1},
      {"left", {Value(std::string("abc")), Value(int64_t{-1})}, -1},
      {"right", {Value(std::string("abcdef")), Value(int64_t{3})}, -1},
      {"right", {Value(std::string("abc")), Value(int64_t{-1})}, -1},
      {"lpad", {Value(std::string("ab")), Value(int64_t{5})}, -1},
      {"lpad",
       {Value(std::string("ab")), Value(int64_t{5}), Value(std::string("*"))},
       -1},
      {"lpad",
       {Value(std::string("abcde")), Value(int64_t{3}),
        Value(std::string("*"))},
       -1},
      {"rpad",
       {Value(std::string("ab")), Value(int64_t{5}), Value(std::string("*"))},
       -1},
      {"substr", {Value(std::string("abcdef")), Value(int64_t{2})}, -1},
      {"substr",
       {Value(std::string("abcdef")), Value(int64_t{2}), Value(int64_t{3})},
       -1},
      {"substr",
       {Value(std::string("abcdef")), Value(int64_t{-3}), Value(int64_t{2})},
       -1},
      {"substr",
       {Value(std::string("abcdef")), Value(int64_t{2}), Value(int64_t{-1})},
       -1},
      {"substring",
       {Value(std::string("abcdef")), Value(int64_t{2}), Value(int64_t{2})},
       -1},
      {"instr", {Value(std::string("abcabc")), Value(std::string("b"))}, -1},
      {"instr",
       {Value(std::string("abcabc")), Value(std::string("b")),
        Value(int64_t{3})},
       -1},
      {"instr",
       {Value(std::string("abcabc")), Value(std::string("b")),
        Value(int64_t{-1})},
       -1},
      {"strpos", {Value(std::string("hello")), Value(std::string("ll"))}, -1},
      {"translate",
       {Value(std::string("hello")), Value(std::string("el")),
        Value(std::string("ip"))},
       -1},
      {"replace",
       {Value(std::string("aaa")), Value(std::string("a")),
        Value(std::string("bb"))},
       -1},
      {"repeat", {Value(std::string("ab")), Value(int64_t{3})}, -1},
      {"repeat", {Value(std::string("ab")), Value(int64_t{-1})}, -1},
      {"starts_with",
       {Value(std::string("hello")), Value(std::string("he"))},
       -1},
      {"ends_with",
       {Value(std::string("hello")), Value(std::string("lo"))},
       -1},
      {"split", {Value(std::string("a,b,c"))}, -1},
      {"split", {Value(std::string("a1b1c")), Value(std::string("1"))}, -1},
      {"split_substr",
       {Value(std::string("a,b,c")), Value(std::string(",")), Value(int64_t{2}),
        Value(int64_t{1})},
       -1},
      {"md5", {Value(std::string("abc"))}, 1},
      {"sha1", {Value(std::string("abc"))}, 1},
      {"sha256", {Value(std::string("abc"))}, 1},
      {"sha512", {Value(std::string("abc"))}, 1},
      {"to_hex", {Value(std::string("abc"))}, -1},
      {"bool_val", {Value(true)}, -1},
      {"str_value", {Value(int64_t{1})}, -1},
      {"regexp_contains",
       {Value(std::string("hello")), Value(std::string("l+"))},
       -1},
      {"regexp_contains",
       {Value(std::string("x")), Value(std::string("("))},
       -1},
      {"regexp_extract",
       {Value(std::string("a1b2")), Value(std::string("[0-9]"))},
       -1},
      {"regexp_extract_all",
       {Value(std::string("a1b2")), Value(std::string("[0-9]"))},
       -1},
      {"regexp_match",
       {Value(std::string("hello")), Value(std::string("h.*o"))},
       -1},
      {"regexp_instr",
       {Value(std::string("abcabc")), Value(std::string("b"))},
       -1},
      {"regexp_replace",
       {Value(std::string("a1b2")), Value(std::string("[0-9]")),
        Value(std::string("_"))},
       -1},
      {"byte_regexp_extract_all",
       {Value(std::string("a1b2")), Value(std::string("[0-9]"))},
       -1},
      {"byte_regexp_instr",
       {Value(std::string("abcabc")), Value(std::string("b"))},
       -1},
      {"__bit_and", {Value(int64_t{6}), Value(int64_t{3})}, 1},
      {"__bit_or", {Value(int64_t{6}), Value(int64_t{3})}, 1},
      {"__bit_xor", {Value(int64_t{6}), Value(int64_t{3})}, 1},
      {"__shift_left", {Value(int64_t{1}), Value(int64_t{4})}, 1},
      {"__shift_right", {Value(int64_t{16}), Value(int64_t{2})}, 1},
      {"__shift_right", {Value(int64_t{1}), Value(int64_t{-1})}, 0},
      {"code_points_to_bytes",
       {Value::Array({Value(int64_t{104}), Value(int64_t{105})}, "INT64")},
       1},
      {"code_points_to_string",
       {Value::Array({Value(int64_t{104}), Value(int64_t{8364})}, "INT64")},
       1},
      {"code_points_to_string",
       {Value::Array({Value(int64_t{-1})}, "INT64")},
       0},
      {"code_points_to_bytes",
       {Value::Array({Value(int64_t{256})}, "INT64")},
       0},
      {"byte_left", {Value(std::string("abc")), Value(int64_t{2})}, 1},
      {"byte_right", {Value(std::string("abc")), Value(int64_t{2})}, 1},
      {"byte_reverse", {Value(std::string("abc"))}, 1},
      {"byte_substr", {Value(std::string("abcde")), Value(int64_t{2})}, 1},
      {"byte_substr",
       {Value(std::string("abcde")), Value(int64_t{2}), Value(int64_t{2})},
       1},
      {"extract_year", {Value::Date("2026-01-02")}, -1},
      {"extract_month", {Value::Date("2026-01-02")}, -1},
      {"extract_day", {Value::Date("2026-01-02")}, -1},
      {"unix_date", {Value::Date("1970-01-02")}, -1},
      {"unix_seconds", {Value(std::string("1970-01-01 00:00:01+00"))}, 1},
      {"unix_millis", {Value(std::string("1970-01-01 00:00:01+00"))}, 1},
      {"unix_micros", {Value(std::string("1970-01-01 00:00:01+00"))}, 1},
      {"make_interval",
       {Value(int64_t{1}), Value(int64_t{2}), Value(int64_t{3})},
       1},
      {"date_sub", {Value::Date("2026-01-02"), Value(std::string("5 day"))}, 0},
      {"generate_array", {Value(int64_t{1}), Value(int64_t{5})}, -1},
      {"generate_array",
       {Value(int64_t{1}), Value(int64_t{10}), Value(int64_t{3})},
       -1},
      {"generate_array",
       {Value(int64_t{1}), Value(int64_t{5}), Value(int64_t{0})},
       -1},
      {"generate_date_array",
       {Value::Date("2026-01-01"), Value::Date("2026-01-05")},
       -1},
      {"format_date",
       {Value(std::string("%Y-%m-%d")), Value::Date("2026-01-02")},
       -1},
      {"format_datetime",
       {Value(std::string("%Y-%m-%d")),
        Value(std::string("2026-01-02T03:04:05"))},
       1},
      {"format_timestamp",
       {Value(std::string("%Y")), Value(std::string("2026-01-02 03:04:05+00"))},
       1},
      {"parse_timestamp",
       {Value(std::string("%Y-%m-%d %H:%M:%S")),
        Value(std::string("2026-01-02 03:04:05"))},
       -1},
      {"parse_timestamp",
       {Value(std::string("%Y")), Value(std::string("bad"))},
       -1},
      {"timestamp",
       {Value(std::string("2026-01-02 03:04:05")),
        Value(std::string("+05:30"))},
       0},
      {"format",
       {Value(std::string("%d %s %f %e %g %x %X %o %t %%")), Value(int64_t{42}),
        Value(std::string("a")), Value(3.5), Value(1234.5), Value(0.0001),
        Value(int64_t{255}), Value(int64_t{48879}), Value(int64_t{8}),
        Value(std::string("x"))},
       -1},
      {"format", {Value(std::string("%d"))}, -1},
      {"format",
       {Value(std::string("%*d")), Value(std::string("x")), Value(int64_t{1})},
       -1},
      {"format", {Value(std::string()), Value(int64_t{2})}, -1},
      {"format", {Value(std::string("%d")), Value(std::string("x"))}, -1},
      {"json_extract",
       {Value(std::string("{\"a\": 1}")), Value(std::string("$.a"))},
       -1},
      {"json_extract_array",
       {Value(std::string("[1,2]")), Value(std::string("$"))},
       -1},
      {"json_extract_scalar",
       {Value(std::string(R"({"a": "x"})")), Value(std::string("$.a"))},
       -1},
      {"json_extract_string_array",
       {Value(std::string("[\"a\"]")), Value(std::string("$"))},
       -1},
      {"json_query",
       {Value(std::string("{\"a\": 1}")), Value(std::string("$.a"))},
       -1},
      {"json_query_array",
       {Value(std::string(R"({"a": "x"})")), Value(std::string("$"))},
       -1},
      {"json_value",
       {Value(std::string("{\"a\": 1}")), Value(std::string("$.a"))},
       -1},
      {"json_value_array",
       {Value(std::string("[\"a\"]")), Value(std::string("$"))},
       -1},
      {"__struct_json__", {Value(int64_t{1}), Value(std::string("x"))}, -1},
      {"__struct_set",
       {Value(std::string("{\"a\": 1}")), Value(std::string("b")),
        Value(int64_t{2})},
       -1},
      {"__get_field_safe",
       {Value(std::string("{\"a\": 1}")), Value(std::string("a"))},
       -1},
      {"__get_field_safe", {Value(), Value(std::string("a"))}, -1},
      {"__pipe_concat",
       {Value::Array({Value(std::string("a")), Value(std::string("b"))},
                     "STRING"),
        Value(std::string("-"))},
       -1},
      {"__pipe_concat", {Value(int64_t{1}), Value(std::string("-"))}, -1},
      {"$proto_repeated_guard",
       {Value::Array({Value(int64_t{1})}, "INT64"), Value(int64_t{9})},
       1},
      {"$proto_repeated_guard", {Value(int64_t{1}), Value(int64_t{9})}, 0},
      {"$proto_repeated_guard", {Value(), Value(int64_t{9})}, 1},
      {"$proto_repeated_guard",
       {Value::Array({Value()}, "INT64"), Value(int64_t{9})},
       0},
      {"$proto_repeated_guard", {Value(int64_t{1})}, 0},
      {"$proto_field_guard",
       {Value(int64_t{1}), Value(std::string("INT64")), Value(int64_t{7})},
       1},
      {"$proto_field_guard", {Value(int64_t{1})}, 0},
      {"$proto_enum_guard",
       {Value(int64_t{1}), Value(std::string("MEMBER"))},
       1},
      {"$proto_enum_guard", {Value(int64_t{1})}, 0},
      {"__get_extension", {Value(std::string("x"))}, -1},
      {"__proto_new", {Value(std::string("x"))}, -1},
      {"__proto_set", {Value(std::string("x"))}, -1},
      {"__value_table_proto", {Value(std::string("x"))}, -1},
      {"__value_table_value", {Value(std::string("x"))}, -1},
      {"__value_table_proto_existing", {Value(std::string("x"))}, -1},
      {"get_field",
       {Value(std::string("{\"a\": 1}")), Value(std::string("a"))},
       -1},
      {"unsupported_fn", {Value(int64_t{1})}, 0},
  };
  NoSubqueryContext context;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    std::vector<Expression> args;
    args.reserve(c.args.size());
    for (const auto& v : c.args) {
      args.push_back(ConstantValueExp(v));
    }
    Expression expr = FunctionCallExp(c.name, args);

    // Metadata paths execute for every shape.
    EXPECT_FALSE(expr->ToString().empty());
    std::ostringstream dump;
    expr->Dump(dump);
    EXPECT_FALSE(dump.str().empty());
    EXPECT_EQ(expr->Type(), TypeTag::kFunctionCallExp);
    EXPECT_TRUE(expr->TouchedColumns().empty());
    EXPECT_NO_THROW((void)expr->ResultType(schema));
    EXPECT_NO_THROW((void)expr->ResultType(schema, schema));
    EXPECT_EQ(expr->Validate(context, schema), Status::kSuccess);

    StatusOr<Value> result =
        StatusError(StatusCode::kRuntimeError, "uncaught exception");
    bool threw = false;
    try {
      result = expr->TryEvaluate(row, schema);
    } catch (const std::exception& e) {
      // A residual EXC-SHIM boundary leak still counts as a failed call.
      threw = true;
      if (c.expect == 1) {
        ADD_FAILURE() << e.what();
      }
    }
    if (!threw) {
      if (c.expect == 1) {
        EXPECT_TRUE(result.HasValue()) << result.GetStatus().GetMessage();
        EXPECT_NO_THROW((void)expr->Evaluate(row, schema));
        EXPECT_NO_THROW((void)expr->Evaluate(row, schema, context));
        // Two-sided overloads accept constant-only expressions.
        auto two_sided = expr->TryEvaluate(&row, schema, &empty_row, schema);
        EXPECT_TRUE(two_sided.HasValue());
      } else if (c.expect == 0) {
        EXPECT_FALSE(result.HasValue());
      }
    }
  }
}

TEST(FunctionCallAstSweepTest, MetadataOnEmptySchema) {
  Schema empty_schema("e", {});
  NoSubqueryContext context;
  const std::vector<std::string> names = {
      "concat",         "abs",          "length",
      "format",         "json_extract", "get_field",
      "generate_array", "coalesce",     "regexp_extract"};
  for (const auto& name : names) {
    SCOPED_TRACE(name);
    Expression expr =
        FunctionCallExp(name, {ConstantValueExp(Value(int64_t{1}))});
    EXPECT_NO_THROW((void)expr->ResultType(empty_schema));
    EXPECT_NO_THROW((void)expr->ResultType(empty_schema, empty_schema));
    EXPECT_NO_THROW((void)expr->ToString());
  }
}

}  // namespace tinylamb
