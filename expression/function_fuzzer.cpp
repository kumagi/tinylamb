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
};

Value MakeArg(Cursor& c) {
  switch (c.Next() % 7) {
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
  const std::string name =
      kStrictFunctions[c.Next() % (sizeof(kStrictFunctions) /
                                   sizeof(kStrictFunctions[0]))];
  const size_t argc = 1 + c.Next() % 3;
  std::vector<Expression> args;
  bool has_null = false;
  for (size_t i = 0; i < argc; ++i) {
    Value v = MakeArg(c);
    has_null |= v.IsNull();
    args.push_back(ConstantValueExp(std::move(v)));
  }
  const Expression call = FunctionCallExp(name, std::move(args));
  if (getenv("TINYLAMB_FUNC_FUZZ_TRACE")) {
    fprintf(stderr, "trace %s argc=%zu\n", name.c_str(), argc);
  }

  StatusOr<Value> first = Value();
  try {
    first = call->TryEvaluate(Row(), Schema());
  } catch (...) {
    fprintf(stderr, "function threw: %s(argc=%zu)\n", name.c_str(), argc);
    abort();
  }
  const StatusOr<Value> second = call->TryEvaluate(Row(), Schema());
  assert(first.HasValue() == second.HasValue());
  if (first.HasValue() && second.HasValue()) {
    assert(first.Value().IsNull() == second.Value().IsNull() &&
           first.Value().AsString() == second.Value().AsString());
  }
  if (has_null && first.HasValue()) {
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
