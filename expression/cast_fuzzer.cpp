/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0. */

// Byte-driven CAST fuzzing: TryCastValue must honor its Try contract for
// every (value, target-type) pair:
//   * never let an exception escape (Status-based error handling),
//   * safe casts never report failure (they yield NULL instead),
//   * NULL in => NULL out,
//   * deterministic: the same call twice gives the same outcome,
//   * INT64 -> STRING -> INT64 round-trips exactly.

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>

#include "expression/cast_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

struct Cursor {
  const uint8_t* data;
  size_t size;
  size_t pos{0};
  uint8_t Next() { return pos < size ? data[pos++] : 0; }
};

std::string WeirdText(Cursor& c) {
  static const char* kPieces[] = {
      "",      "-",          "+",          "9223372036854775808",
      "1e999", "-1e999",     "nan",        "NaN",
      "inf",   "Infinity",   "-inf",       "0x10",
      " 7",    "7 ",         "1.5",        ".",
      "e5",    "2020-01-01", "2020-13-45", "1970-00-00",
      "0",     "-0",         "1_000"};
  std::string out;
  const size_t parts = c.Next() % 3;
  for (size_t i = 0; i <= parts; ++i) {
    out += kPieces[c.Next() % 22];
  }
  return out;
}

Value MakeValue(Cursor& c) {
  switch (c.Next() % 5) {
    case 0:
      return Value();
    case 1:
      return Value(static_cast<int64_t>(c.Next()) - 128);
    case 2:
      return Value(static_cast<double>(static_cast<int64_t>(c.Next())));
    case 3:
      return Value(WeirdText(c));
    default:
      return Value::DateFromDays(static_cast<int64_t>(c.Next()));
  }
}

}  // namespace

inline void Try(const uint8_t* data, size_t size, bool verbose) {
  if (size < 2) {
    return;
  }
  Cursor c{data, size};
  static const char* kTypes[] = {"INT64",    "DOUBLE",       "STRING",  "DATE",
                                 "BOOL",     "TIMESTAMP",    "NUMERIC", "BYTES",
                                 "INTERVAL", "ARRAY<INT64>", ""};
  const Value source = MakeValue(c);
  const std::string type = kTypes[c.Next() % 11];
  const bool safe = c.Next() % 2 == 0;

  // Exercise the public AST path; CastExpression::TryEvaluate must itself
  // honor the Try contract (no escaping exceptions).
  const Expression node =
      CastExpressionExp(ConstantValueExp(source), type, safe);
  StatusOr<Value> first = Value();
  try {
    first = node->TryEvaluate(Row(), Schema());
  } catch (...) {
    fprintf(stderr, "cast threw: (%d)%s -> %s safe=%d\n", (int)source.type,
            source.AsString().c_str(), type.c_str(), safe);
    abort();
  }
  const StatusOr<Value> second = node->TryEvaluate(Row(), Schema());
  assert(first.HasValue() == second.HasValue());
  if (first.HasValue() && second.HasValue()) {
    assert(first.Value().IsNull() == second.Value().IsNull() &&
           (first.Value().IsNull() ||
            first.Value().AsString() == second.Value().AsString()));
  }
  if (source.IsNull()) {
    assert(first.HasValue() && first.Value().IsNull());
  }
  if (safe) {
    assert(first.HasValue());  // SAFE casts never fail.
  }
  if (type == "STRING" && source.type == ValueType::kInt64 &&
      first.HasValue() && !first.Value().IsNull()) {
    const Expression back_node =
        CastExpressionExp(ConstantValueExp(first.Value()), "INT64", false);
    const StatusOr<Value> back = back_node->TryEvaluate(Row(), Schema());
    assert(back.HasValue());
    assert(back.Value().value.int_value == source.value.int_value);
  }
  (void)verbose;
}

}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  tinylamb::Try(data, size, false);
  return 0;
}
