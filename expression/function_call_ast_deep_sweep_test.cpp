/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Deep sweep over the AST function dispatcher (expression/
// function_call_expression.cpp ExecuteFunction): per-family calls with
// realistic constant arguments exercise the overload/type branches that a
// success-only sweep cannot reach.  Complements the SQL-level sweeps (which
// route through whichever evaluator the plan picks) by calling the AST
// evaluator directly for every case.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

struct DeepCase {
  const char* name;
  std::vector<Value> args;
  int expect;  // 1: must succeed, 0: must fail, -1: either.
};

Value Arr(std::vector<Value> elements, const char* elem_type = "INT64") {
  return Value::Array(std::move(elements), elem_type);
}

std::vector<DeepCase> DeepCases() {
  const Value kStr(std::string("TargetString"));
  const Value kBytes(std::string("\x01\x02\xff"));
  const Value kJson(std::string(R"({"a": 1, "nested": {"b": "x"}})"));
  const Value kTs(std::string("2026-09-22T10:11:12.123456789Z"));
  const Value kDate("2026-09-22");
  const Value kArrInt = Arr({Value(5), Value(1), Value(9)});
  const Value kArrStr =
      Arr({Value(std::string("b")), Value(std::string("a"))}, "STRING");
  const Value kArrEmpty = Arr({});
  return {
      // String overloads: byte vs character variants, format strings.
      {"substr", {kStr, Value(2)}, 1},
      {"substr", {kStr, Value(-3)}, 1},
      {"substr", {kStr, Value(2), Value(100)}, 1},
      {"left", {kStr, Value(100)}, 1},
      {"left", {kStr, Value(-1)}, 0},
      {"right", {kStr, Value(-1)}, 0},
      {"lpad", {kStr, Value(3)}, 1},
      {"rpad", {kStr, Value(3)}, 1},
      {"lpad", {kStr, Value(3), Value(std::string("ab"))}, 1},
      {"rpad", {kStr, Value(3), Value(std::string("ab"))}, 1},
      {"repeat", {kStr, Value(0)}, 1},
      {"translate",
       {kStr, Value(std::string("aeiou")), Value(std::string("12345"))},
       1},
      {"translate",
       {kStr, Value(std::string("aei")), Value(std::string("xy"))},
       1},
      {"instr", {kStr, Value(std::string("Target")), Value(2)}, -1},
      {"strpos", {kStr, Value(std::string("zzz"))}, 1},
      {"initcap", {kStr}, 1},
      {"soundex", {kStr}, -1},
      {"format",
       {Value(std::string("%s|%d|%f")), kStr, Value(7), Value(2.5)},
       1},
      {"format", {Value(std::string("%5d")), Value(42)}, 1},
      {"format", {Value(std::string("%<10s")), kStr}, -1},

      // Byte-oriented overloads.
      {"byte_length", {kBytes}, 1},
      {"byte_substr", {kBytes, Value(1), Value(2)}, -1},
      {"byte_reverse", {kBytes}, -1},
      {"to_hex", {kBytes}, -1},
      {"from_hex", {Value(std::string("0x41"))}, -1},
      {"from_hex", {Value(std::string("zz"))}, 0},
      {"from_base64", {Value(std::string("!!!not base64"))}, 0},
      {"to_base64", {kStr}, -1},

      // JSON family: paths, quoting, arrays, nested lookup.
      {"json_extract", {kJson, Value(std::string("$.nested.b"))}, 1},
      {"json_extract", {kJson, Value(std::string("$.missing"))}, 1},
      {"json_extract", {kJson, Value(std::string("$"))}, 1},
      {"json_extract_scalar", {kJson, Value(std::string("$.a"))}, 1},
      {"json_extract_scalar", {kJson, Value(std::string("$.nested"))}, 1},
      {"json_extract_string_array", {kJson, Value(std::string("$"))}, -1},
      {"json_value", {kJson, Value(std::string("$.a"))}, 1},
      {"json_value_array", {kJson, Value(std::string("$.a"))}, -1},
      {"json_query_array", {kJson, Value(std::string("$.nested"))}, -1},
      {"to_json_string", {kStr}, -1},
      {"to_json_string", {Value(true)}, -1},
      {"__struct_json__", {Value(std::string("k")), Value(int64_t{1})}, -1},
      {"__struct_set", {kJson, Value(std::string("c")), Value(true)}, -1},
      {"get_field", {kJson, Value(std::string("nested"))}, 1},
      {"get_field", {kJson, Value(std::string("nope"))}, 0},
      {"__get_field_safe", {kJson, Value(std::string("a"))}, 1},

      // Timestamp / date coercion through strings.
      {"extract_year", {kTs}, -1},
      {"extract_nanosecond", {kTs}, -1},
      {"parse_date",
       {Value(std::string("%Y-%m-%d")), Value(std::string("2026-09-22"))},
       -1},
      {"parse_date",
       {Value(std::string("%Y-%m-%d")), Value(std::string("22/09/2026"))},
       0},
      {"parse_timestamp",
       {Value(std::string("%Y-%m-%dT%H:%M:%E*S%Ez")), kTs},
       -1},
      {"format_date", {Value(std::string("%d/%m/%Y")), kDate}, -1},
      {"date", {kTs}, -1},
      {"datetime", {kStr}, -1},
      {"timestamp", {kStr}, -1},
      {"last_day", {kDate}, -1},
      {"date_diff",
       {kDate, Value("2026-01-01"), Value(std::string("MONTH"))},
       -1},

      // Arrays: slices, safe accessors, dedup, conversions.
      {"array_slice", {kArrInt, Value(2)}, -1},
      {"array_slice", {kArrInt, Value(2), Value(3), Value(1)}, -1},
      {"array_slice", {kArrInt, Value(-2)}, 0},
      {"array_first", {kArrEmpty}, -1},
      {"array_last", {kArrInt}, -1},  // AST side lacks it; relational has it.
      {"array_last", {kArrEmpty}, -1},
      {"array_includes", {kArrStr, Value(std::string("a"))}, -1},
      {"array_includes_all", {kArrInt, Arr({Value(5)})}, -1},
      {"array_includes_any", {kArrInt, Arr({Value(99)})}, -1},
      {"array_concat", {kArrEmpty, kArrInt}, -1},
      {"array_to_string", {kArrEmpty, Value(std::string(","))}, -1},
      {"array_to_string",
       {kArrInt, Value(std::string(",")), Value(std::string("N"))},
       -1},
      {"array_is_distinct", {kArrEmpty}, -1},
      {"element", {kArrStr}, -1},
      {"array_element_offset", {kArrStr, Value(std::string("a"))}, -1},
      {"array_element_safe_offset", {kArrStr, Value(std::string("z"))}, -1},
      {"array_element_ordinal", {kArrStr, Value(std::string("a"))}, -1},
      {"array_element_safe_ordinal", {kArrStr, Value(std::string("a"))}, -1},
      {"generate_array", {Value(5), Value(1), Value(-2)}, 1},
      {"generate_date_array", {Value("2026-01-01"), Value("2026-01-03")}, -1},

      // Proto-text shim surface (has_xxx, repeated, message fields).
      {"__proto_get", {kStr, Value(std::string("f"))}, -1},
      {"$proto_field_guard",
       {Value(1), Value(std::string("INT64")), Value(3)},
       1},
      {"$proto_enum_guard", {Value(1), Value(std::string("MEMBER"))}, 1},
      {"__value_table_proto", {kStr}, -1},

      // Crypto / encoding round trips.
      {"md5", {kStr}, 1},
      {"sha1", {kStr}, 1},
      {"sha256", {kStr}, 1},
      {"to_base64", {kBytes}, -1},
      {"code_points_to_string", {Arr({Value(72), Value(105)})}, 1},
      {"code_points_to_bytes", {Arr({Value(72), Value(105)})}, -1},
      {"chr", {Value(0)}, -1},

      // Safe arithmetic contracts (NULL on overflow / division).
      {"safe_add", {Value(std::numeric_limits<int64_t>::max()), Value(1)}, 1},
      {"safe_multiply",
       {Value(std::numeric_limits<int64_t>::max()), Value(2)},
       1},
      {"safe_divide", {Value(1.5), Value(0)}, 1},
      {"safe_negate", {Value(std::numeric_limits<int64_t>::min())}, 1},
      {"ieee_divide", {Value(-1.0), Value(0)}, 1},

      // Error contract: unsupported functions and arity mismatches.
      {"definitely_not_a_function", {Value(1)}, 0},
      {"upper", {}, 0},
      {"upper", {Value(1), Value(2)}, 0},
      {"substr", {kStr}, 0},
  };
}

}  // namespace

TEST(FunctionCallAstDeepSweepTest, EvaluateAllFamiliesDirectly) {
  const Schema schema("t", {Column("c0", ValueType::kInt64),
                            Column("c1", ValueType::kVarChar)});
  const Row row({Value(1), Value("x")});
  const std::vector<DeepCase> cases = DeepCases();
  for (const DeepCase& c : cases) {
    SCOPED_TRACE(testing::Message() << c.name << "/" << c.args.size());
    std::vector<Expression> children;
    children.reserve(c.args.size());
    for (const Value& value : c.args) {
      children.push_back(ConstantValueExp(value));
    }
    const Expression call = FunctionCallExp(c.name, children);
    StatusOr<Value> result = StatusError(StatusCode::kUnknown, "unset");
    bool threw = false;
    try {
      result = call->TryEvaluate(row, schema);
    } catch (const std::exception&) {
      threw = true;
      EXPECT_NE(c.expect, 1) << c.name << " leaked an exception";
    }
    if (!threw) {
      if (c.expect == 1) {
        EXPECT_TRUE(result.HasValue())
            << c.name << ": " << result.GetStatus().GetMessage();
      } else if (c.expect == 0) {
        EXPECT_FALSE(result.HasValue()) << c.name;
      }
    }
    // Result type resolution must not throw regardless of evaluation.
    EXPECT_NO_THROW((void)call->ResultType(schema));
  }
}

}  // namespace tinylamb
