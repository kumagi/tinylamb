/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// Byte-driven scalar FUNCTION fuzzing.  Executes fuzzer-chosen function
// names with fuzzer-built constant arguments through the AST evaluator and
// pins three invariants that hold for the whole surface:
//   1. TryEvaluate never lets an exception escape (Status contract;
//      fuzz-found class of bugs: IntervalValue::Parse / Value::Date throws
//      leaking through ExecuteFunction),
//   2. evaluation is deterministic: two runs give the same outcome,
//   3. NULL-propagating functions return NULL for any NULL argument
//      (curated list of strict builtins; aggregates/volatile/variadic
//      exception paths are out of scope for this oracle).

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/interval_expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

struct Cursor {
  const uint8_t* data;
  size_t size;
  size_t pos{0};
  uint8_t Next() { return pos < size ? data[pos++] : 0; }
};

// Functions that must propagate NULL (each verified against the
// `if (values[i].IsNull()) return Value();` guard in
// function_call_expression.cpp).
const char* const kStrictFunctions[] = {
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "length",
    "char_length",
    "byte_length",
    "abs",
    "sign",
    "mod",
    "sqrt",
    "ln",
    "exp",
    "floor",
    "ceil",
    "round",
    "trunc",
    "cos",
    "sin",
    "tan",
    "strpos",
    "instr",
    "reverse",
    "starts_with",
    "ends_with",
    "repeat",
    "replace",
    "substr",
    "substring",
    "left",
    "right",
    "lpad",
    "rpad",
    "chr",
    "code_points_to_bytes",
    "initcap",
    // Verified-strict additions from the campaign's function audit:
    "translate",
    "soundex",
    "split",
    "ascii",
    "unicode",
    "byte_left",
    "byte_right",
    "byte_reverse",
    "byte_substr",
    "div",
    "power",
    "cbrt",
    "log10",
    "acos",
    "asin",
    "atan",
    "atan2",
    "radians",
    "degrees",
    "cosh",
    "sinh",
    "tanh",
    "safe_divide",
    "safe_subtract",
    "safe_negate",
    "safe_add",
    "safe_multiply",
    "log",
    "pow",
    "pi",
    "ieee_divide",
    "is_inf",
    "is_nan",
    "ceiling",
    "truncate",
    "octet_length",
    "character_length",
    "split_substr",
    "code_points_to_string",
    "concat",
    "regexp_contains",
    "regexp_match",
    "regexp_instr",
    "byte_regexp_instr",
    "regexp_extract_all",
    "byte_regexp_extract_all",
    "regexp_replace",
    "md5",
    "sha1",
    "sha256",
    "sha512",
    "format",
    "format_timestamp",
    "format_datetime",
    "format_date",
    "parse_timestamp",
    "string",
    "extract_year",
    "extract_month",
    "extract_day",
    "unix_seconds",
    "unix_millis",
    "unix_micros",
    "unix_date",
    // DATE arg is strict; the INTERVAL operand is generated separately
    // below (it is an expression type, not a constant Value).
    "date_add",
    "date_sub",
    "generate_array",
    "generate_date_array",
    "json_extract",
    "json_query",
    "json_value",
    "json_extract_scalar",
    "json_extract_array",
    "json_query_array",
    "json_value_array",
    "json_extract_string_array",
    "__bit_and",
    "__bit_or",
    "__bit_xor",
    "__shift_left",
    "__shift_right",
};

// Deterministic, non-throwing builtins that intentionally do NOT propagate
// NULL (null-handling and conditional-evaluation functions).  Only
// invariants 1 (no escape) and 2 (determinism) are checked for these.
const char* const kNonStrictFunctions[] = {
    // make_interval ignores arguments past the first two (lenient arity,
    // pinned by SqlIntervalCoverageTest), so NULLs there stay observable.
    "make_interval",
    // regexp_extract accepts 2-4 args but reads only the first two.
    "regexp_extract",
    "iserror",
    "nullif",
    "ifnull",
    "coalesce",
    "greatest",
    "least",
    "if",
    "iferror",
    "nulliferror",
};

Value MakeArg(Cursor& c) {
  switch (c.Next() % 8) {
    case 0:
      return Value();  // NULL
    case 1:
      return Value(static_cast<int64_t>(c.Next()) - 127);
    case 2:
      return Value(int64_t{1} << (c.Next() % 64));
    case 3:
      return Value(static_cast<double>(static_cast<int32_t>(c.Next() << 16)) /
                   3.0);
    case 4: {
      std::string s;
      const size_t len = c.Next() % 6;
      for (size_t i = 0; i < len; ++i) {
        s.push_back(static_cast<char>(c.Next() % 128));
      }
      return Value(std::move(s));
    }
    case 5:
      return Value::DateFromDays(static_cast<int64_t>(c.Next()));
    case 6: {
      // Small array (occasional NULL / mixed-type / nested elements) so
      // array-taking builtins (code_points_to_*, greatest/least) reach their
      // real paths -- including cross-type element comparisons.
      std::vector<Value> elems;
      const size_t n = c.Next() % 5;
      const uint8_t flavor = c.Next() % 4;
      for (size_t i = 0; i < n; ++i) {
        const uint8_t b = c.Next();
        if (b == 0) {
          elems.push_back(Value());
          continue;
        }
        switch (flavor) {
          case 0:
            elems.push_back(Value(static_cast<int64_t>(b) - 64));
            break;
          case 1:
            elems.push_back(Value(std::string(1, static_cast<char>(b % 128))));
            break;
          case 2:
            elems.push_back(
                Value(static_cast<double>(static_cast<int8_t>(b)) / 7.0));
            break;
          default:
            elems.push_back(
                Value::Array({Value(static_cast<int64_t>(b))}, "INT64"));
            break;
        }
      }
      static const char* kElemTypes[] = {"INT64", "STRING", "DOUBLE",
                                         "ARRAY<INT64>"};
      return Value::Array(std::move(elems), kElemTypes[flavor]);
    }
    default:
      return Value(std::string("abc"));
  }
}

}  // namespace

inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 2) {
    return;
  }
  Cursor c{data, size};
  constexpr size_t kStrictCount =
      sizeof(kStrictFunctions) / sizeof(kStrictFunctions[0]);
  constexpr size_t kNonStrictCount =
      sizeof(kNonStrictFunctions) / sizeof(kNonStrictFunctions[0]);
  const size_t pick = c.Next() % (kStrictCount + kNonStrictCount);
  const bool strict = pick < kStrictCount;
  const std::string name = strict ? kStrictFunctions[pick]
                                  : kNonStrictFunctions[pick - kStrictCount];
  std::vector<Expression> args;
  bool has_null = false;
  if (name == "date_add" || name == "date_sub") {
    // DATE_ADD/DATE_SUB require (date, INTERVAL) — the interval operand is
    // an IntervalExpression node, so it cannot be NULL itself; strictness
    // is judged on the date argument only.
    static const char* const kUnits[] = {"year", "month",  "week",  "day",
                                         "hour", "minute", "second"};
    Value v = MakeArg(c);
    has_null |= v.IsNull();
    args.push_back(ConstantValueExp(std::move(v)));
    args.push_back(IntervalExpressionExp(static_cast<int64_t>(c.Next()) - 127,
                                         kUnits[c.Next() % 7]));
  } else {
    const size_t argc = 1 + c.Next() % 3;
    for (size_t i = 0; i < argc; ++i) {
      Value v = MakeArg(c);
      has_null |= v.IsNull();
      args.push_back(ConstantValueExp(std::move(v)));
    }
  }
  const size_t argc = args.size();
  if (getenv("TINYLAMB_FUNC_FUZZ_TRACE")) {
    fprintf(stderr, "trace %s argc=%zu", name.c_str(), argc);
    for (const auto& a : args) {
      if (a->Type() == TypeTag::kConstantValue) {
        std::string dbg = a->AsConstantValue().GetValue().AsString();
        fprintf(stderr, " arg=%s", dbg.c_str());
      }
    }
    fprintf(stderr, "\n");
  }
  const Expression call = FunctionCallExp(name, std::move(args));

  StatusOr<Value> first = Value();
  try {
    first = call->TryEvaluate(Row(), Schema());
  } catch (...) {
    fprintf(stderr, "function threw: %s(argc=%zu)\n", name.c_str(), argc);
    abort();
  }
  const StatusOr<Value> second = call->TryEvaluate(Row(), Schema());
  assert(first.HasValue() == second.HasValue());
  // The join-context overload shares ExecuteFunction but walks its own
  // argument loop; with constant arguments it must agree exactly.
  const Row empty_row;
  const Schema empty_schema;
  const StatusOr<Value> join_ctx =
      call->TryEvaluate(&empty_row, empty_schema, &empty_row, empty_schema);
  assert(first.HasValue() == join_ctx.HasValue());
  if (first.HasValue() && join_ctx.HasValue()) {
    assert(first.Value().IsNull() == join_ctx.Value().IsNull() &&
           first.Value().AsString() == join_ctx.Value().AsString());
  }
  if (first.HasValue() && second.HasValue()) {
    assert(first.Value().IsNull() == second.Value().IsNull() &&
           first.Value().AsString() == second.Value().AsString());
  }
  if (strict && has_null && first.HasValue()) {
    // A strict builtin that evaluated at all must fold to NULL, never to a
    // concrete value derived from the missing operand.
    assert(first.Value().IsNull());
  }
  (void)verbose;
}

}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
