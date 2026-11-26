/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
#include "expression/bytecode.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/encoder.hpp"
#include "common/exc_shim.hpp"
#include "common/math_utils.hpp"
#include "common/regex_guard.hpp"
#include "executor/data_chunk.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/unary_expression.hpp"
#include "type/collation.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Counts Unicode code points the same way the AST evaluator does: continuation
// bytes (0x80..0xBF) are skipped, so every well-formed sequence counts once.
int64_t Utf8CodePointCount(std::string_view s) {
  int64_t count = 0;
  for (size_t i = 0; i < s.size();) {
    const unsigned char c =                // NOLINT(modernize-use-auto)
        static_cast<unsigned char>(s[i]);  // NOLINT(modernize-use-auto)
    size_t width = 1;
    if ((c & 0xE0) == 0xC0)  // NOLINT(readability-braces-around-statements)
      width = 2;             // NOLINT(readability-braces-around-statements)
    else if ((c & 0xF0) ==
             0xE0)  // NOLINT(readability-braces-around-statements)
      width = 3;    // NOLINT(readability-braces-around-statements)
    else if ((c & 0xF8) ==
             0xF0)  // NOLINT(readability-braces-around-statements)
      width = 4;    // NOLINT(readability-braces-around-statements)
    bool valid = (i + width <= s.size());
    for (size_t k = 1; valid && k < width; ++k) {
      if ((static_cast<unsigned char>(s[i + k]) & 0xC0) !=
          0x80)         // NOLINT(readability-braces-around-statements)
        valid = false;  // NOLINT(readability-braces-around-statements)
    }
    i += valid ? width : 1;
    ++count;
  }
  return count;
}

ValueType ValueTypeFor(const Type& type) {
  switch (type.GetType()) {
    case TypeTag::kInteger:
    case TypeTag::kBigInt:
      return ValueType::kInt64;
    case TypeTag::kDouble:
      return ValueType::kDouble;
    case TypeTag::kVarChar:
      return ValueType::kVarChar;
    case TypeTag::kDate:
      return ValueType::kDate;
    default:
      return ValueType::kNull;
  }
}

ExpressionRuleSet MakeValueRules(const ExpressionRuleSet& base_rules) {
  ExpressionRuleSet rules = base_rules;
  rules.Remove("inner_join_not_null_inference");
  // Truthiness-preserving collapses (x AND TRUE -> x, x OR x -> x,
  // NOT NOT x -> x, ...) are only sound where the result is consumed as a
  // predicate.  As a *value* the AST always normalizes these to
  // BOOLEAN (or NULL), while the collapsed operand may carry -1, "abc",
  // etc. (oracle-found: `(-1 OR -1)` -> AST TRUE, VM -1).
  for (const char* name :
       {"boolean_identity", "double_negation", "xor_boolean_identity",
        "and_idempotent", "or_idempotent", "absorption_and", "absorption_or",
        "absorption_and_reversed", "absorption_or_reversed",
        "factor_or_common_and", "boolean_filter_pullup",
        // Neutral-constant identities drop the 0/1.0 operand, which
        // erases the INT64<->DOUBLE promotion the AST applies for
        // mixed-type arithmetic (`CASE..THEN 1.0..END * i` stays IEEE
        // double in the reference but becomes a checked INT64 kernel
        // after the fold).  Where both sides are statically INT64 the
        // kernel is identical anyway, so dropping the fold only gives
        // up work the VM can do at full speed.
        "identity_add_zero", "identity_subtract_zero", "identity_multiply_one",
        "identity_divide_one"}) {
    rules.Remove(name);
  }
  return rules;
}

const ExpressionRuleSet& DefaultValueRules() {
  static const ExpressionRuleSet rules =
      MakeValueRules(ExpressionRuleSet::Default());
  return rules;
}

const ExpressionRuleSet& NotComparisonValueRules() {
  static const ExpressionRuleSet rules =
      MakeValueRules(NotComparisonFreeRules());
  return rules;
}

std::optional<BytecodeOp> BinaryOpcode(ValueType type) {
  switch (type) {
    case ValueType::kInt64:
      return BytecodeOp::kBinaryInt64;
    case ValueType::kDouble:
      return BytecodeOp::kBinaryDouble;
    case ValueType::kVarChar:
      return BytecodeOp::kBinaryVarchar;
    case ValueType::kDate:
      return BytecodeOp::kBinaryDate;
    case ValueType::kArray:
    case ValueType::kNull:
      return std::nullopt;
  }
  return std::nullopt;
}

// DATE parts come directly from the calendar, including signed/wide years.
// VARCHAR mirrors the AST exactly: validate with TryParseDateDays (after
// truncating DATETIME/TIMESTAMP text at its date part) instead of slicing
// fixed columns.
StatusOr<Value> TryExtractDatePart(const Value& value, uint16_t part) {
  if (value.IsNull()) {
    return Value();
  }
  if (value.IsInterval()) {
    return ExecuteFunction(part == 0   ? "extract_year"
                           : part == 1 ? "extract_month"
                                       : "extract_day",
                           {value});
  }
  if (value.type == ValueType::kDate) {
    if (value.value.int_value < kDateMinDays ||
        kDateMaxDays < value.value.int_value) {
      return StatusError(StatusCode::kInvalidArgument,
                         "DATE value out of range");
    }
    const std::chrono::year_month_day ymd{std::chrono::sys_days{
        std::chrono::days{static_cast<int>(value.value.int_value)}}};
    if (part == 0) {
      return Value(static_cast<int64_t>(static_cast<int>(ymd.year())));
    }
    if (part == 1) {
      return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
    }
    return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
  }
  if (value.type != ValueType::kVarChar) {
    return StatusError(StatusCode::kInvalidArgument,
                       "EXTRACT requires DATE or STRING");
  }
  // STRING input: validate the calendar date exactly like CAST(... AS DATE)
  // does instead of slicing fixed columns (which mis-read 5-digit and
  // negative years and accepted month 13 / day 45). A DATETIME / TIMESTAMP
  // text contributes its date part.
  const std::string_view text(value.value.varchar_value);
  const size_t date_end = text.find_first_of(" Tt");
  const std::string_view date_part =
      date_end == std::string_view::npos ? text : text.substr(0, date_end);
  StatusOr<int64_t> days_or = TryParseDateDays(date_part);
  if (!days_or.HasValue()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "invalid DATE value: " + std::string(text));
  }
  const std::chrono::year_month_day ymd{std::chrono::sys_days{
      std::chrono::days{static_cast<int>(days_or.Value())}}};
  if (part == 0) {
    return Value(static_cast<int64_t>(static_cast<int>(ymd.year())));
  }
  if (part == 1) {
    return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
  }
  return Value(static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
}

inline bool EnvDisabled(const char* name) noexcept {
  const char* env = ::tinylamb::platform::GetEnv(name);
  return env != nullptr && env[0] == '1';
}

bool ContainsCollateCall(const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kFunctionCallExp &&
      expr->AsFunctionCallExpression().FuncName() == "collate") {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expr),
      [](const Expression& ch) { return ContainsCollateCall(ch); });
}

bool ExprMayBeUnsigned(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema) {
  if (!expr) {
    return true;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue: {
      const int offset = schema.Offset(expr->AsColumnValue().GetColumnName());
      if (offset < 0) {
        return true;
      }
      const Column& col = schema.GetColumn(offset);
      return col.Type() != ValueType::kInt64 || col.IsUnsigned();
    }
    case TypeTag::kConstantValue: {
      const Value& v = expr->AsConstantValue().GetValue();
      if (v.IsNull()) {
        return false;
      }
      return v.type != ValueType::kInt64 || v.IsUnsigned();
    }
    case TypeTag::kCastExp: {
      const CastExpression& cast = expr->AsCastExpression();
      const std::string& target_name = cast.TargetTypeName();
      constexpr std::string_view kUint = "uint";
      const auto uint_name = std::search(
          target_name.begin(), target_name.end(), kUint.begin(), kUint.end(),
          [](char actual, char expected) {
            return std::tolower(static_cast<unsigned char>(actual)) == expected;
          });
      return cast.TargetValueType() != ValueType::kInt64 ||
             uint_name != target_name.end();
    }
    case TypeTag::kUnaryExp:
    case TypeTag::kBinaryExp:
    case TypeTag::kFunctionCallExp: {
      const auto children = ExpressionChildren(expr);
      return std::ranges::any_of(children, [&](const Expression& ch) {
        return ExprMayBeUnsigned(ch, schema);
      });
    }
    default:
      return true;
  }
}

// Static collation of `expr`. CollationOfExpression only sees an explicit
// COLLATE() call; a constant Value can also carry a collation tag directly,
// which is what HaveConflictingCollations checks at evaluation time.
Collation StaticCollationOf(const Expression& expr) {
  const Collation explicit_col = CollationOfExpression(expr);
  if (explicit_col != Collation::kNone || !expr ||
      expr->Type() != TypeTag::kConstantValue) {
    return explicit_col;
  }
  return CollationOf(expr->AsConstantValue().GetValue().Collation());
}

}  // namespace

namespace {

// Lean proof: lean/TinylambOpt/FilterEvalOrder.lean (filter_eval_order_sound)
// Conservative error-free check: returns true only when evaluating `expr` on
// any row conforming to `schema` cannot raise an error.
bool IsErrorFreePredicateForOrdering(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue:
      return schema.Offset(expr->AsColumnValue().GetColumnName()) >= 0;
    case TypeTag::kConstantValue:
      return true;
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expr->AsUnaryExpression();
      if (unary.Op() != UnaryOperation::kNot &&
          unary.Op() != UnaryOperation::kIsNull &&
          unary.Op() != UnaryOperation::kIsNotNull) {
        return false;
      }
      return IsErrorFreePredicateForOrdering(unary.Child(), schema);
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& bin = expr->AsBinaryExpression();
      switch (bin.Op()) {
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom: {
          if (!IsErrorFreePredicateForOrdering(bin.Left(), schema) ||
              !IsErrorFreePredicateForOrdering(bin.Right(), schema)) {
            return false;
          }
          const ValueType lt = ValueTypeFor(bin.Left()->ResultType(schema));
          const ValueType rt = ValueTypeFor(bin.Right()->ResultType(schema));
          // A VARCHAR pair with conflicting explicit collations errors at
          // evaluation (HaveConflictingCollations in binary_expression.cpp).
          // It is NOT error-free: reordering it ahead of a FALSE conjunct
          // would surface the error the AST's left-to-right AND suppresses.
          if (lt == ValueType::kVarChar && rt == ValueType::kVarChar) {
            // Row values may carry runtime collation tags that are not
            // represented in Schema. Keep such comparisons in source order.
            if (!bin.Left()->TouchedColumns().empty() ||
                !bin.Right()->TouchedColumns().empty()) {
              return false;
            }
            const Collation lcol = StaticCollationOf(bin.Left());
            const Collation rcol = StaticCollationOf(bin.Right());
            if (lcol != Collation::kNone && rcol != Collation::kNone &&
                lcol != rcol) {
              return false;
            }
          }
          if (lt == rt && lt != ValueType::kArray) {
            return true;
          }
          const bool both_num =
              (lt == ValueType::kInt64 || lt == ValueType::kDouble) &&
              (rt == ValueType::kInt64 || rt == ValueType::kDouble);
          return both_num || lt == ValueType::kNull || rt == ValueType::kNull;
        }
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return IsErrorFreePredicateForOrdering(bin.Left(), schema) &&
                 IsErrorFreePredicateForOrdering(bin.Right(), schema);
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike: {
          if (ContainsCollateCall(bin.Left()) ||
              ContainsCollateCall(bin.Right())) {
            return false;
          }
          const ValueType lt = ValueTypeFor(bin.Left()->ResultType(schema));
          const ValueType rt = ValueTypeFor(bin.Right()->ResultType(schema));
          if (lt != ValueType::kVarChar || rt != ValueType::kVarChar) {
            return false;
          }
          return IsErrorFreePredicateForOrdering(bin.Left(), schema) &&
                 IsErrorFreePredicateForOrdering(bin.Right(), schema);
        }
        default:
          return false;
      }
    }
    case TypeTag::kInExp: {
      const InExpression& in = expr->AsInExpression();
      if (!IsErrorFreePredicateForOrdering(in.child_, schema)) {
        return false;
      }
      const ValueType ct = ValueTypeFor(in.child_->ResultType(schema));
      for (const Expression& cand : in.list_) {
        if (!cand || cand->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& cv = cand->AsConstantValue().GetValue();
        if (!cv.IsNull() && ValueTypeFor(cand->ResultType(schema)) != ct) {
          return false;
        }
      }
      // IN evaluates HaveConflictingCollations(child, candidate) per candidate
      // (in_expression.cpp). Static ValueType does not carry collation, so a
      // mismatched pair is NOT error-free: reordering it ahead of a FALSE
      // conjunct would surface an error the AST's short circuit suppresses.
      if (ct == ValueType::kVarChar) {
        // Column values can carry collation tags that are absent from Schema.
        if (!in.child_->TouchedColumns().empty()) {
          return false;
        }
        const Collation child_col = StaticCollationOf(in.child_);
        if (child_col != Collation::kNone) {
          for (const Expression& cand : in.list_) {
            const Collation cand_col = StaticCollationOf(cand);
            if (cand_col != Collation::kNone && cand_col != child_col) {
              return false;
            }
          }
        }
      }
      return true;
    }
    case TypeTag::kFunctionCallExp: {
      const FunctionCallExpression& call = expr->AsFunctionCallExpression();
      if (call.FuncName() != "regexp_contains" || call.Args().size() != 2) {
        return false;
      }
      if (!IsErrorFreePredicateForOrdering(call.Args()[0], schema) ||
          !call.Args()[1] ||
          call.Args()[1]->Type() != TypeTag::kConstantValue) {
        return false;
      }
      const Value& pat = call.Args()[1]->AsConstantValue().GetValue();
      if (pat.IsNull()) {
        return true;
      }
      if (pat.type != ValueType::kVarChar) {
        return false;
      }
      // Compilation alone does not prove evaluation is error-free: a
      // compilable pattern can still exhaust the runtime regex step budget
      // for particular input strings. Preserve source-order short-circuiting
      // whenever a non-NULL pattern may execute.
      return false;
    }
    default:
      return false;
  }
}

}  // namespace

namespace {

int PredicateOrderCost(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema, int* tie_col_out) {
  *tie_col_out = std::numeric_limits<int>::max();
  if (!expr) {
    return 100;
  }
  switch (expr->Type()) {
    case TypeTag::kUnaryExp: {
      const UnaryExpression& u = expr->AsUnaryExpression();
      if (u.Op() == UnaryOperation::kIsNull ||
          u.Op() == UnaryOperation::kIsNotNull) {
        if (u.Child() && u.Child()->Type() == TypeTag::kColumnValue) {
          *tie_col_out =
              schema.Offset(u.Child()->AsColumnValue().GetColumnName());
        }
        return 1;
      }
      return PredicateOrderCost(u.Child(), schema, tie_col_out) + 1;
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& bin = expr->AsBinaryExpression();
      if (bin.Op() == BinaryOperation::kLike ||
          bin.Op() == BinaryOperation::kNotLike) {
        return 25;
      }
      if (bin.Op() == BinaryOperation::kOr) {
        int dummy_l = 0;
        int dummy_r = 0;
        return std::min(55,
                        PredicateOrderCost(bin.Left(), schema, &dummy_l) +
                            PredicateOrderCost(bin.Right(), schema, &dummy_r));
      }
      const ValueType lt = ValueTypeFor(bin.Left()->ResultType(schema));
      const ValueType rt = ValueTypeFor(bin.Right()->ResultType(schema));
      if (lt == ValueType::kInt64 || lt == ValueType::kDate ||
          rt == ValueType::kInt64 || rt == ValueType::kDate) {
        if (bin.Left()->Type() == TypeTag::kColumnValue) {
          *tie_col_out =
              schema.Offset(bin.Left()->AsColumnValue().GetColumnName());
        } else if (bin.Right()->Type() == TypeTag::kColumnValue) {
          *tie_col_out =
              schema.Offset(bin.Right()->AsColumnValue().GetColumnName());
        }
        return 2;
      }
      if (lt == ValueType::kDouble || rt == ValueType::kDouble) {
        return 3;
      }
      return 6;
    }
    case TypeTag::kInExp:
      return 8;
    case TypeTag::kFunctionCallExp:
      if (expr->AsFunctionCallExpression().FuncName() == "regexp_contains") {
        return 60;
      }
      return 30;
    default:
      return 10;
  }
}

void FlattenAndConjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::vector<Expression>* out) {
  if (expr && expr->Type() == TypeTag::kBinaryExp &&
      expr->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
    FlattenAndConjuncts(expr->AsBinaryExpression().Left(), out);
    FlattenAndConjuncts(expr->AsBinaryExpression().Right(), out);
    return;
  }
  out->push_back(expr);
}

// Lean proof: lean/TinylambOpt/SimdInRange.lean
//   (simd_in_range_sound, int64_range_lowering)
bool TryExtractInt64Bound(const Expression& expr, const Schema& schema,
                          uint16_t* col_out, ValueType* col_type_out,
                          int64_t* bound_out, bool* is_lower_out) {
  if (!expr || expr->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& bin = expr->AsBinaryExpression();
  Expression col_expr;
  Expression const_expr;
  BinaryOperation op = bin.Op();
  if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    col_expr = bin.Left();
    const_expr = bin.Right();
  } else if (bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue &&
             bin.Right() && bin.Right()->Type() == TypeTag::kColumnValue) {
    col_expr = bin.Right();
    const_expr = bin.Left();
    switch (op) {
      case BinaryOperation::kLessThan:
        op = BinaryOperation::kGreaterThan;
        break;
      case BinaryOperation::kLessThanEquals:
        op = BinaryOperation::kGreaterThanEquals;
        break;
      case BinaryOperation::kGreaterThan:
        op = BinaryOperation::kLessThan;
        break;
      case BinaryOperation::kGreaterThanEquals:
        op = BinaryOperation::kLessThanEquals;
        break;
      default:
        return false;
    }
  } else {
    return false;
  }

  const int offset = schema.Offset(col_expr->AsColumnValue().GetColumnName());
  if (offset < 0 ||
      std::cmp_greater(offset, std::numeric_limits<uint16_t>::max())) {
    return false;
  }
  const Column& col = schema.GetColumn(offset);
  if ((col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate) ||
      col.IsUnsigned()) {
    return false;
  }
  const Value& cv = const_expr->AsConstantValue().GetValue();
  if (cv.IsNull() || cv.IsUnsigned() || cv.type != col.Type()) {
    return false;
  }

  const int64_t val = cv.value.int_value;
  int64_t closed_bound = val;
  bool is_lower = false;
  switch (op) {
    case BinaryOperation::kGreaterThanEquals:
      is_lower = true;
      break;
    case BinaryOperation::kGreaterThan:
      if (val == std::numeric_limits<int64_t>::max()) {
        return false;
      }
      closed_bound = val + 1;
      is_lower = true;
      break;
    case BinaryOperation::kLessThanEquals:
      is_lower = false;
      break;
    case BinaryOperation::kLessThan:
      if (val == std::numeric_limits<int64_t>::min()) {
        return false;
      }
      closed_bound = val - 1;
      is_lower = false;
      break;
    default:
      return false;
  }

  *col_out = static_cast<uint16_t>(offset);
  *col_type_out = col.Type();
  *bound_out = closed_bound;
  *is_lower_out = is_lower;
  return true;
}

bool ParseStrictSignedInt64(std::string_view sv, int64_t* out) noexcept {
  if (sv.empty()) {
    return false;
  }
  size_t start = 0;
  if (sv[0] == '-') {
    if (sv.size() == 1 || sv[1] == '0') {
      return false;
    }
    start = 1;
  } else if (sv[0] < '0' || sv[0] > '9' || (sv[0] == '0' && sv.size() > 1)) {
    return false;
  }
  for (size_t i = start; i < sv.size(); ++i) {
    if (sv[i] < '0' || sv[i] > '9') {
      return false;
    }
  }
  int64_t val = 0;
  const auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
  if (ec != std::errc{} || ptr != sv.data() + sv.size()) {
    return false;
  }
  *out = val;
  return true;
}

bool ParseStructJsonIntRow(std::string_view json, size_t expected_fields,
                           std::vector<std::optional<int64_t>>* out_fields) {
  out_fields->clear();
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    return false;
  }
  size_t i = 1;
  const size_t end = json.size() - 1;
  const auto skip_ws = [&]() {
    while (i < end && (json[i] == ' ' || json[i] == '\t' || json[i] == '\n' ||
                       json[i] == '\r')) {
      ++i;
    }
  };
  skip_ws();
  if (i == end) {
    return false;
  }
  while (i < end) {
    skip_ws();
    if (i >= end || json[i] != '"') {
      return false;
    }
    ++i;
    bool closed = false;
    while (i < end) {
      if (json[i] == '\\') {
        if (i + 1 >= end) {
          return false;
        }
        i += 2;
        continue;
      }
      if (json[i] == '"') {
        ++i;
        closed = true;
        break;
      }
      ++i;
    }
    if (!closed) {
      return false;
    }
    skip_ws();
    if (i >= end || json[i] != ':') {
      return false;
    }
    ++i;
    skip_ws();
    if (i >= end || json[i] == '"' || json[i] == '{' || json[i] == '[') {
      return false;
    }
    const size_t val_start = i;
    while (i < end && json[i] != ',' && json[i] != ' ' && json[i] != '\t' &&
           json[i] != '\n' && json[i] != '\r') {
      ++i;
    }
    const std::string_view tok = json.substr(val_start, i - val_start);
    if (tok == "null") {
      out_fields->emplace_back(std::nullopt);
    } else {
      int64_t parsed = 0;
      if (!ParseStrictSignedInt64(tok, &parsed)) {
        return false;
      }
      out_fields->emplace_back(parsed);
    }
    skip_ws();
    if (i < end) {
      if (json[i] != ',') {
        return false;
      }
      ++i;
      skip_ws();
      if (i == end) {
        return false;
      }
    }
  }
  return out_fields->size() == expected_fields;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/RowInMask.lean (sound)
bool TryExtractRowInMask(const InExpression& in, const Schema& schema,
                         std::vector<Expression>* field_exprs,
                         RowInMaskSpec* spec_out) {
  if (!in.child_ || in.list_.empty() || in.list_.size() > 256 ||
      in.child_->Type() != TypeTag::kFunctionCallExp) {
    return false;
  }
  const auto& child_call = in.child_->AsFunctionCallExpression();
  const size_t stride = StructJsonFieldWidth(child_call);
  if (stride == 0 || child_call.Args().size() < stride ||
      child_call.Args().size() % stride != 0) {
    return false;
  }
  const size_t k = child_call.Args().size() / stride;
  if (k == 0 || k > 64) {
    return false;
  }
  field_exprs->clear();
  field_exprs->reserve(k);
  for (size_t c = 0; c < k; ++c) {
    const Expression& name_arg = child_call.Args()[c * stride];
    if (!name_arg || name_arg->Type() != TypeTag::kConstantValue) {
      return false;
    }
    if (stride == 3) {
      const Expression& flag_arg = child_call.Args()[(c * stride) + 2];
      if (!flag_arg || flag_arg->Type() != TypeTag::kConstantValue) {
        return false;
      }
      const Value& fv = flag_arg->AsConstantValue().GetValue();
      if (fv.IsNull() || fv.Truthy()) {
        return false;
      }
    }
    Expression field_expr = child_call.Args()[(c * stride) + 1];
    if (!field_expr) {
      return false;
    }
    if (field_expr->Type() == TypeTag::kConstantValue) {
      const Value& cv = field_expr->AsConstantValue().GetValue();
      if (cv.IsNull()) {
        field_exprs->push_back(ConstantValueExp(Value()));
        continue;
      }
      if (stride == 3 && cv.type == ValueType::kVarChar &&
          cv.Collation() == 0 && !cv.IsInterval() &&
          cv.value.varchar_value == "null") {
        field_exprs->push_back(ConstantValueExp(Value()));
        continue;
      }
      if (cv.type != ValueType::kInt64 || cv.IsUnsigned()) {
        return false;
      }
      field_exprs->push_back(field_expr);
      continue;
    }
    if (ValueTypeFor(field_expr->ResultType(schema)) != ValueType::kInt64 ||
        ExprMayBeUnsigned(field_expr, schema) ||
        ContainsCollateCall(field_expr)) {
      return false;
    }
    field_exprs->push_back(std::move(field_expr));
  }

  std::vector<std::vector<std::optional<int64_t>>> candidates;
  candidates.reserve(in.list_.size());
  std::vector<std::optional<int64_t>> parsed_row;
  for (const Expression& item : in.list_) {
    if (!item) {
      return false;
    }
    if (item->Type() == TypeTag::kConstantValue) {
      const Value& cv = item->AsConstantValue().GetValue();
      if (cv.IsNull()) {
        candidates.emplace_back(k, std::nullopt);
        continue;
      }
      if (cv.type != ValueType::kVarChar || cv.Collation() != 0 ||
          cv.IsInterval() ||
          !ParseStructJsonIntRow(cv.value.varchar_value, k, &parsed_row)) {
        return false;
      }
      candidates.push_back(parsed_row);
      continue;
    }
    if (item->Type() == TypeTag::kFunctionCallExp &&
        item->AsFunctionCallExpression().FuncName() == "__struct_json__") {
      const auto& cand_call = item->AsFunctionCallExpression();
      const size_t c_stride = StructJsonFieldWidth(cand_call);
      if (c_stride == 0 || cand_call.Args().size() != k * c_stride) {
        return false;
      }
      parsed_row.assign(k, std::nullopt);
      for (size_t c = 0; c < k; ++c) {
        const Expression& name_arg = cand_call.Args()[c * c_stride];
        const Expression& val_arg = cand_call.Args()[(c * c_stride) + 1];
        if (!name_arg || name_arg->Type() != TypeTag::kConstantValue ||
            !val_arg || val_arg->Type() != TypeTag::kConstantValue) {
          return false;
        }
        bool is_quoted_text = false;
        if (c_stride == 3) {
          const Expression& flag_arg = cand_call.Args()[(c * c_stride) + 2];
          if (!flag_arg || flag_arg->Type() != TypeTag::kConstantValue) {
            return false;
          }
          const Value& fv = flag_arg->AsConstantValue().GetValue();
          if (fv.IsNull()) {
            return false;
          }
          is_quoted_text = fv.Truthy();
        }
        const Value& vv = val_arg->AsConstantValue().GetValue();
        if (vv.IsNull() ||
            (c_stride == 3 && !is_quoted_text &&
             vv.type == ValueType::kVarChar && vv.Collation() == 0 &&
             !vv.IsInterval() && vv.value.varchar_value == "null")) {
          parsed_row[c] = std::nullopt;
        } else if (vv.type == ValueType::kInt64 && !vv.IsUnsigned()) {
          parsed_row[c] = vv.value.int_value;
        } else {
          return false;
        }
      }
      candidates.push_back(parsed_row);
      continue;
    }
    return false;
  }

  spec_out->num_fields = static_cast<uint32_t>(k);
  spec_out->chunks.clear();
  for (size_t offset = 0; offset < candidates.size(); offset += 64) {
    const size_t cnt = std::min<size_t>(64, candidates.size() - offset);
    RowInMaskSpec::Chunk chunk;
    chunk.count = static_cast<uint32_t>(cnt);
    chunk.enabled =
        (cnt == 64) ? ~uint64_t{0} : ((uint64_t{1} << cnt) - uint64_t{1});
    chunk.columns.resize(k);
    for (size_t slot = 0; slot < cnt; ++slot) {
      const uint64_t bit = uint64_t{1} << slot;
      for (size_t c = 0; c < k; ++c) {
        const auto& candidate = candidates[offset + slot][c];
        if (candidate.has_value()) {
          chunk.columns[c].non_null_mask |= bit;
          chunk.columns[c].values[slot] = candidate.value();
        } else {
          chunk.columns[c].null_mask |= bit;
        }
      }
    }
    spec_out->chunks.push_back(std::move(chunk));
  }
  return true;
}

bool CompileNode(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    BytecodeProgram* program) {
  if (!expression) {
    // Null operands only exist on hand-built or damaged trees; report the
    // node as unsupported so callers keep the generic evaluation path.
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const int offset =
          schema.Offset(expression->AsColumnValue().GetColumnName());
      if (offset < 0 ||
          std::cmp_greater(offset, std::numeric_limits<uint16_t>::max())) {
        return false;
      }
      program->AddInstruction({.opcode = BytecodeOp::kLoadColumn,
                               .operand = static_cast<uint16_t>(offset)});
      return true;
    }
    case TypeTag::kConstantValue: {
      uint16_t offset = 0;
      if (!program->AddConstant(expression->AsConstantValue().GetValue(),
                                &offset)) {
        return false;
      }
      program->AddInstruction(
          {.opcode = BytecodeOp::kLoadConstant, .operand = offset});
      return true;
    }
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      // D7 (docs/design.md): AND/OR compile to short-circuit control flow so
      // the VM skips the right operand exactly when the AST does, keeping the
      // three-valued truth table AND the error-occurrence conditions
      // identical. AND is flattened to N conjuncts first; when every
      // conjunct is provably error-free they may be reordered by
      // PredicateOrderCost (TINYLAMB_DISABLE_FILTER_ORDER=1 keeps source
      // order). Adjacent bounds on the same INT64 column may be fused into
      // one range item. Emission puts a JumpIfFalse after each item except
      // the last, so any FALSE skips the remaining items and the AND merges
      // TRUE/NULL values as they are evaluated.
      // OR uses JumpIfTrue symmetrically.  The jump peeks the stack (does not
      // pop), so every path reaches `end` with exactly one value.
      if (binary.Op() == BinaryOperation::kAnd) {
        std::vector<Expression> conjuncts;
        FlattenAndConjuncts(expression, &conjuncts);
        if (!EnvDisabled("TINYLAMB_DISABLE_FILTER_ORDER") &&
            conjuncts.size() >= 2 &&
            std::ranges::all_of(conjuncts, [&](const Expression& c) {
              return IsErrorFreePredicateForOrdering(c, schema);
            })) {
          struct RankedConjunct {
            int cost{0};
            int tie_col{0};
            size_t orig_idx{0};
            Expression expr;
          };
          std::vector<RankedConjunct> ranked;
          ranked.reserve(conjuncts.size());
          for (size_t i = 0; i < conjuncts.size(); ++i) {
            int tie_col = std::numeric_limits<int>::max();
            const int cost = PredicateOrderCost(conjuncts[i], schema, &tie_col);
            ranked.push_back({.cost = cost,
                              .tie_col = tie_col,
                              .orig_idx = i,
                              .expr = conjuncts[i]});
          }
          std::stable_sort(
              ranked.begin(), ranked.end(),
              [](const RankedConjunct& a, const RankedConjunct& b) {
                return std::tie(a.cost, a.tie_col, a.orig_idx) <
                       std::tie(b.cost, b.tie_col, b.orig_idx);
              });
          for (size_t i = 0; i < conjuncts.size(); ++i) {
            conjuncts[i] = std::move(ranked[i].expr);
          }
        }

        struct AndItem {
          bool is_range{false};
          uint16_t col_idx{0};
          uint16_t range_spec_idx{0};
          Expression expr;
        };
        std::vector<AndItem> items;
        items.reserve(conjuncts.size());
        const bool enable_range =
            !EnvDisabled("TINYLAMB_DISABLE_SIMD_IN_RANGE");
        for (size_t i = 0; i < conjuncts.size();) {
          if (enable_range && i + 1 < conjuncts.size()) {
            uint16_t c1 = 0, c2 = 0;
            ValueType t1 = ValueType::kNull, t2 = ValueType::kNull;
            int64_t b1 = 0, b2 = 0;
            bool lower1 = false, lower2 = false;
            if (TryExtractInt64Bound(conjuncts[i], schema, &c1, &t1, &b1,
                                     &lower1) &&
                TryExtractInt64Bound(conjuncts[i + 1], schema, &c2, &t2, &b2,
                                     &lower2) &&
                c1 == c2 && t1 == t2 && lower1 != lower2) {
              const int64_t lo = lower1 ? b1 : b2;
              const int64_t hi = lower1 ? b2 : b1;
              if (lo <= hi) {
                uint16_t spec_idx = 0;
                if (!program->AddInt64Range(
                        {.lo = lo, .hi = hi, .expected_type = t1}, &spec_idx)) {
                  return false;
                }
                items.push_back({.is_range = true,
                                 .col_idx = c1,
                                 .range_spec_idx = spec_idx,
                                 .expr = {}});
                i += 2;
                continue;
              }
            }
          }
          items.push_back({.is_range = false,
                           .col_idx = 0,
                           .range_spec_idx = 0,
                           .expr = conjuncts[i]});
          ++i;
        }

        const auto emit_item = [&](const AndItem& item) {
          if (item.is_range) {
            program->AddInstruction(
                {.opcode = BytecodeOp::kLoadColumn, .operand = item.col_idx});
            program->AddInstruction({.opcode = BytecodeOp::kInt64InRange,
                                     .operand = item.range_spec_idx});
            return true;
          }
          return CompileNode(item.expr, schema, program);
        };
        if (items.size() == 1) {
          return emit_item(items[0]);
        }
        if (!emit_item(items[0])) {
          return false;
        }
        std::vector<size_t> jump_indices;
        jump_indices.reserve(items.size());
        jump_indices.push_back(program->Size());
        program->AddInstruction({.opcode = BytecodeOp::kJumpIfFalse});
        for (size_t k = 1; k < items.size(); ++k) {
          if (!emit_item(items[k])) {
            return false;
          }
          program->AddInstruction({.opcode = BytecodeOp::kBinaryInt64,
                                   .operand = 0,
                                   .binary = BinaryOperation::kAnd});
          if (k + 1 < items.size()) {
            jump_indices.push_back(program->Size());
            program->AddInstruction({.opcode = BytecodeOp::kJumpIfFalse});
          }
        }
        for (const size_t jump_idx : jump_indices) {
          program->SetJumpTarget(jump_idx,
                                 static_cast<int32_t>(program->Size()) -
                                     static_cast<int32_t>(jump_idx));
        }
        return true;
      }
      if (binary.Op() == BinaryOperation::kOr) {
        if (!CompileNode(binary.Left(), schema, program)) {
          return false;
        }
        const size_t jump_index = program->Size();
        program->AddInstruction({.opcode = BytecodeOp::kJumpIfTrue});
        if (!CompileNode(binary.Right(), schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kBinaryInt64,
                                 .operand = 0,
                                 .binary = binary.Op()});
        // Patch: jump lands just past the merge instruction.
        program->SetJumpTarget(jump_index,
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(jump_index));
        return true;
      }
      if (binary.Op() == BinaryOperation::kModulo &&
          !EnvDisabled("TINYLAMB_DISABLE_RECIPROCAL_DIVMOD") && binary.Left() &&
          binary.Right() && binary.Right()->Type() == TypeTag::kConstantValue &&
          ValueTypeFor(binary.Left()->ResultType(schema)) ==
              ValueType::kInt64 &&
          !ExprMayBeUnsigned(binary.Left(), schema)) {
        const Value& dv = binary.Right()->AsConstantValue().GetValue();
        if (!dv.IsNull() && !dv.IsUnsigned() && dv.type == ValueType::kInt64 &&
            dv.value.int_value != 0 && dv.value.int_value != 1 &&
            dv.value.int_value != -1) {
          if (!CompileNode(binary.Left(), schema, program)) {
            return false;
          }
          uint16_t spec_idx = 0;
          if (!program->AddReciprocal(
                  ReciprocalDivModSpec::FromDivisor(dv.value.int_value),
                  &spec_idx)) {
            return false;
          }
          program->AddInstruction(
              {.opcode = BytecodeOp::kModInt64Const, .operand = spec_idx});
          return true;
        }
      }
      if (!CompileNode(binary.Left(), schema, program) ||
          !CompileNode(binary.Right(), schema, program)) {
        return false;
      }
      const ValueType left_type =
          ValueTypeFor(binary.Left()->ResultType(schema));
      const ValueType right_type =
          ValueTypeFor(binary.Right()->ResultType(schema));
      if (binary.Op() == BinaryOperation::kLike ||
          binary.Op() == BinaryOperation::kNotLike) {
        if (left_type != ValueType::kVarChar ||
            right_type != ValueType::kVarChar) {
          return false;
        }
      }
      ValueType operand_type = left_type;
      if (operand_type == ValueType::kDouble ||
          right_type == ValueType::kDouble) {
        operand_type = ValueType::kDouble;
      }
      // Interval arithmetic (any VARCHAR operand under +,-,*,/,%) can return
      // a VARCHAR interval string from EvaluateBinary, while BinaryResultType
      // declares other results for those shapes (kVarChar for VARCHAR+VARCHAR
      // add, kDate for DATE mixing, kDouble for division, kBigInt otherwise).
      // A typed bytecode kernel cannot represent that shape-dependent result
      // without mismatching the appended Value ("column vector type
      // mismatch"). Reject the shape so callers fall back to the AST
      // evaluator, the semantic ground truth.
      const bool arithmetic = binary.Op() == BinaryOperation::kAdd ||
                              binary.Op() == BinaryOperation::kSubtract ||
                              binary.Op() == BinaryOperation::kMultiply ||
                              binary.Op() == BinaryOperation::kDivide ||
                              binary.Op() == BinaryOperation::kModulo;
      if (arithmetic && (left_type == ValueType::kVarChar ||
                         right_type == ValueType::kVarChar)) {
        return false;
      }
      // A kNull static type means BOOLEAN (or genuinely untyped NULL) on
      // one side.  The AST treats numeric-with-bool arithmetic as a mixed
      // numeric promotion and evaluates it in IEEE DOUBLE (never raising
      // integer overflow), while a typed kernel here would add in the
      // operand's own domain and raise at a different boundary.
      if (arithmetic &&
          (left_type == ValueType::kNull || right_type == ValueType::kNull)) {
        return false;
      }
      const std::optional<BytecodeOp> opcode = BinaryOpcode(operand_type);
      if (!opcode.has_value()) {
        return false;
      }
      program->AddInstruction(
          {.opcode = *opcode, .operand = 0, .binary = binary.Op()});
      return true;
    }
    case TypeTag::kInExp: {
      // `x IN (a, b, ...)` evaluates the child exactly once, matching the
      // AST's TryMembership: the child value stays under a running
      // three-valued OR accumulator and every candidate compares against a
      // Dup of that one instance.  Layout (the conditional jump PEEKS the
      // accumulator and normalizes a taken top to TRUE):
      //     <child>
      //     Dup(0); <item_0>; Eq_0                    acc = eq_0
      //     [JumpIfTrue -> TAIL; Dup(1); <item_i>; Eq_i; Or]*
      //   TAIL: Pop(1)                                drop the child
      // A truthy equality skips the remaining candidates (and their errors)
      // like the AST's early return; every path reaches TAIL with
      // [child, result] and Pop(1) leaves one value.  The VM's binary
      // instruction dispatches through the same canonical evaluator the AST
      // uses, so NULL and NaN semantics are identical; any operand the
      // compiler cannot lower (subqueries, unsupported functions) fails
      // compilation and the caller falls back to the AST evaluator.
      const InExpression& in = expression->AsInExpression();
      if (!in.child_) {
        return false;
      }
      if (in.list_.empty()) {
        // The result is a constant FALSE, but the child is deliberately still
        // compiled and evaluated before being popped. The AST's TryMembership
        // evaluates the child unconditionally (and propagates its errors /
        // volatile effects) even for an empty list; skipping it here would
        // make bytecode diverge from that semantic reference. Folding the
        // child away is only sound when the rewriter has already proved it
        // side-effect-free and non-throwing.
        if (!CompileNode(in.child_, schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kPop});
        return CompileNode(ConstantValueExp(Value(false)), schema, program);
      }
      if (!EnvDisabled("TINYLAMB_DISABLE_ROW_IN_MASK")) {
        std::vector<Expression> field_exprs;
        RowInMaskSpec row_spec;
        if (TryExtractRowInMask(in, schema, &field_exprs, &row_spec)) {
          for (const Expression& field_expr : field_exprs) {
            if (!CompileNode(field_expr, schema, program)) {
              return false;
            }
          }
          uint16_t mask_offset = 0;
          if (!program->AddRowInMask(std::move(row_spec), &mask_offset)) {
            return false;
          }
          program->AddInstruction(
              {.opcode = BytecodeOp::kRowInMask, .operand = mask_offset});
          return true;
        }
      }
      // String IN with uncollated, non-interval string constants lowers to an
      // O(1) flat string hash-set lookup (Lean proof:
      // lean/TinylambOpt/TpcdsExecCodegen.lean: in_const_set_3vl_sound).
      const Type child_decl_type = in.child_->ResultType(schema);
      // Each constant below is checked for collation/interval explicitly.
      if (ValueTypeFor(child_decl_type) == ValueType::kVarChar) {
        auto safe_plain_const_string = [](std::string_view text) noexcept {
          if (text.size() >= 2 && text.front() == '{' && text.back() == '}') {
            return false;
          }
          if (text.size() >= 19 && text[4] == '-' && text[7] == '-' &&
              (text[10] == ' ' || text[10] == 'T') && text[13] == ':' &&
              text[16] == ':') {
            return false;
          }
          return true;
        };
        bool all_const_str = true;
        StringSetSpec spec;
        spec.non_empty = true;
        std::vector<std::string_view> uniq_views;
        uniq_views.reserve(in.list_.size());
        size_t total_bytes = 0;
        for (const Expression& item : in.list_) {
          if (!item || item->Type() != TypeTag::kConstantValue) {
            all_const_str = false;
            break;
          }
          const Value& v = item->AsConstantValue().GetValue();
          if (v.IsNull()) {
            spec.has_null = true;
            continue;
          }
          if (v.type != ValueType::kVarChar || v.Collation() != 0 ||
              v.IsInterval() ||
              !safe_plain_const_string(v.value.varchar_value)) {
            all_const_str = false;
            break;
          }
          uniq_views.push_back(v.value.varchar_value);
          total_bytes += v.value.varchar_value.size();
        }
        if (all_const_str && total_bytes <= (uint32_t{1} << 28U)) {
          if (!CompileNode(in.child_, schema, program)) {
            return false;
          }
          if (!uniq_views.empty()) {
            uint32_t cap = 4;
            while (cap < uniq_views.size() * 2U) {
              cap <<= 1U;
            }
            spec.table.resize(cap);
            spec.mask = cap - 1U;
            spec.arena.reserve(total_bytes);
            for (const std::string_view sv : uniq_views) {
              const uint64_t h = StringSetSpec::HashView(sv);
              uint32_t pos = static_cast<uint32_t>(h) & spec.mask;
              bool exists = false;
              while (spec.table[pos].offset !=
                     std::numeric_limits<uint32_t>::max()) {
                const StringSetSpec::Slot& s = spec.table[pos];
                if (s.hash == h && s.len == sv.size() &&
                    (sv.empty() || std::memcmp(spec.arena.data() + s.offset,
                                               sv.data(), sv.size()) == 0)) {
                  exists = true;
                  break;
                }
                pos = (pos + 1U) & spec.mask;
              }
              if (!exists) {
                const auto off = static_cast<uint32_t>(spec.arena.size());
                spec.arena.append(sv.data(), sv.size());
                spec.table[pos] = StringSetSpec::Slot{
                    .hash = h,
                    .offset = off,
                    .len = static_cast<uint32_t>(sv.size()),
                };
              }
            }
          }
          uint16_t set_offset = 0;
          if (!program->AddStringSet(std::move(spec), &set_offset)) {
            return false;
          }
          program->AddInstruction(
              {.opcode = BytecodeOp::kInStringSet, .operand = set_offset});
          return true;
        }
      }
      const auto numeric_or_date = [&](const Expression& operand) {
        if (!operand) {
          return false;
        }
        const auto type = ValueTypeFor(operand->ResultType(schema));
        return type == ValueType::kInt64 || type == ValueType::kDouble ||
               type == ValueType::kDate;
      };
      if (!numeric_or_date(in.child_) ||
          !std::ranges::all_of(in.list_, numeric_or_date)) {
        return false;
      }
      const ValueType child_type = ValueTypeFor(in.child_->ResultType(schema));
      const auto equality_op = [&](const Expression& item) {
        ValueType operand_type = child_type;
        const ValueType item_type = ValueTypeFor(item->ResultType(schema));
        if (operand_type == ValueType::kDouble ||
            item_type == ValueType::kDouble) {
          operand_type = ValueType::kDouble;
        }
        return BinaryOpcode(operand_type);
      };
      if (!CompileNode(in.child_, schema, program)) {
        return false;
      }
      std::vector<size_t> truthy_jumps;
      truthy_jumps.reserve(in.list_.size());
      for (size_t i = 0; i < in.list_.size(); ++i) {
        const std::optional<BytecodeOp> opcode = equality_op(in.list_[i]);
        if (!opcode.has_value()) {
          return false;
        }
        if (i != 0) {
          truthy_jumps.push_back(program->Size());
          program->AddInstruction({.opcode = BytecodeOp::kJumpIfTrue});
        }
        program->AddInstruction({.opcode = BytecodeOp::kDup,
                                 .operand = static_cast<uint16_t>(i != 0)});
        if (!CompileNode(in.list_[i], schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = *opcode,
                                 .operand = 0,
                                 .binary = BinaryOperation::kEquals});
        if (i != 0) {
          program->AddInstruction({.opcode = BytecodeOp::kBinaryInt64,
                                   .operand = 0,
                                   .binary = BinaryOperation::kOr});
        }
      }
      const size_t tail = program->Size();
      for (const size_t jump : truthy_jumps) {
        program->SetJumpTarget(
            jump, static_cast<int32_t>(tail) - static_cast<int32_t>(jump));
      }
      program->AddInstruction({.opcode = BytecodeOp::kPop, .operand = 1});
      return true;
    }
    case TypeTag::kCaseExp: {
      // Searched CASE lowers to chained conditional jumps.  Layout (the
      // conditional jump PEEKS the top, so each WHEN condition is popped
      // explicitly on both of its exits):
      //     cond_0; JumpIfTrue -> T0; Pop
      //     cond_1; JumpIfTrue -> T1; Pop
      //     <else | NULL constant>;  Jump -> END
      // T0: Pop; then_0; Jump -> END
      // T1: Pop; then_1; Jump -> END
      // END:
      // TryPickBranch semantics hold branch-for-branch: a NULL condition is
      // not taken (kJumpIfTrue only fires on TRUE), a raising condition
      // aborts the whole CASE in evaluation order, and no match yields
      // ELSE-or-NULL.  Branch types must unify (ResultType kInvalid fails
      // the final type check exactly as before).
      const CaseExpression& case_expression = expression->AsCaseExpression();
      if (case_expression.when_clauses_.empty()) {
        return false;
      }
      std::vector<size_t> then_jumps;
      then_jumps.reserve(case_expression.when_clauses_.size());
      std::vector<size_t> branch_ends;
      branch_ends.reserve(case_expression.when_clauses_.size());
      for (const auto& when : case_expression.when_clauses_) {
        if (!when.first || !when.second) {
          return false;
        }
        if (!CompileNode(when.first, schema, program)) {
          return false;
        }
        then_jumps.push_back(program->Size());
        program->AddInstruction({.opcode = BytecodeOp::kJumpIfTrue});
        program->AddInstruction({.opcode = BytecodeOp::kPop});
      }
      if (case_expression.else_clause_ != nullptr) {
        if (!CompileNode(case_expression.else_clause_, schema, program)) {
          return false;
        }
      } else {
        uint16_t offset = 0;
        if (!program->AddConstant(Value(), &offset)) {
          return false;
        }
        program->AddInstruction(
            {.opcode = BytecodeOp::kLoadConstant, .operand = offset});
      }
      const size_t end_jump = program->Size();
      program->AddInstruction({.opcode = BytecodeOp::kJump});
      for (size_t i = 0; i < case_expression.when_clauses_.size(); ++i) {
        // Jump target: relative from the jump instruction itself.
        program->SetJumpTarget(then_jumps[i],
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(then_jumps[i]));
        program->AddInstruction({.opcode = BytecodeOp::kPop});
        if (!CompileNode(case_expression.when_clauses_[i].second, schema,
                         program)) {
          return false;
        }
        branch_ends.push_back(program->Size());
        program->AddInstruction({.opcode = BytecodeOp::kJump});
      }
      program->SetJumpTarget(end_jump, static_cast<int32_t>(program->Size()) -
                                           static_cast<int32_t>(end_jump));
      for (const size_t branch_end : branch_ends) {
        program->SetJumpTarget(branch_end,
                               static_cast<int32_t>(program->Size()) -
                                   static_cast<int32_t>(branch_end));
      }
      if (case_expression.ResultType(schema).GetType() == TypeTag::kDouble) {
        uint16_t offset = 0;
        if (!program->AddCast({.target_type_name = "FLOAT64",
                               .target_value_type = ValueType::kDouble,
                               .return_null_on_error = false},
                              &offset)) {
          return false;
        }
        program->AddInstruction(
            {.opcode = BytecodeOp::kCast, .operand = offset});
      }
      return true;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      if (!CompileNode(unary.Child(), schema, program)) {
        return false;
      }
      // The null-check / truthiness family is type-agnostic for IS [NOT] NULL
      // on both legs; IS [NOT] TRUE/FALSE additionally requires the INT64 BOOL
      // carrier (or NULL) -- non-INT64, non-NULL operands are rejected by
      // TryEvaluateUnary's T3698 guard rather than accepted by Truthy().
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
          program->AddInstruction({.opcode = BytecodeOp::kUnaryInt64,
                                   .operand = 0,
                                   .binary = BinaryOperation::kAdd,
                                   .unary = unary.Op()});
          return true;
        default:
          break;
      }
      const ValueType operand_type =
          ValueTypeFor(unary.Child()->ResultType(schema));
      if (operand_type != ValueType::kInt64 &&
          operand_type != ValueType::kDouble) {
        return false;
      }
      program->AddInstruction({.opcode = operand_type == ValueType::kDouble
                                             ? BytecodeOp::kUnaryDouble
                                             : BytecodeOp::kUnaryInt64,
                               .operand = 0,
                               .binary = BinaryOperation::kAdd,
                               .unary = unary.Op()});
      return true;
    }
    case TypeTag::kFunctionCallExp: {
      const FunctionCallExpression& call =
          expression->AsFunctionCallExpression();
      if (call.FuncName() == "byte_length" ||
          call.FuncName() == "octet_length") {
        if (call.Args().size() != 1 ||
            !CompileNode(call.Args()[0], schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kByteLength});
        return true;
      }
      if (call.FuncName() == "length" || call.FuncName() == "char_length" ||
          call.FuncName() == "character_length") {
        if (call.Args().size() != 1 ||
            !CompileNode(call.Args()[0], schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kCharLength});
        return true;
      }
      if ((call.FuncName() == "div" || call.FuncName() == "mod") &&
          call.Args().size() == 2 &&
          !EnvDisabled("TINYLAMB_DISABLE_RECIPROCAL_DIVMOD") &&
          call.Args()[0] && call.Args()[1] &&
          call.Args()[1]->Type() == TypeTag::kConstantValue &&
          ValueTypeFor(call.Args()[0]->ResultType(schema)) ==
              ValueType::kInt64 &&
          !ExprMayBeUnsigned(call.Args()[0], schema)) {
        const Value& dv = call.Args()[1]->AsConstantValue().GetValue();
        if (!dv.IsNull() && !dv.IsUnsigned() && dv.type == ValueType::kInt64 &&
            dv.value.int_value != 0 && dv.value.int_value != 1 &&
            dv.value.int_value != -1) {
          if (!CompileNode(call.Args()[0], schema, program)) {
            return false;
          }
          uint16_t spec_idx = 0;
          if (!program->AddReciprocal(
                  ReciprocalDivModSpec::FromDivisor(dv.value.int_value),
                  &spec_idx)) {
            return false;
          }
          program->AddInstruction({.opcode = call.FuncName() == "div"
                                                 ? BytecodeOp::kDivInt64Const
                                                 : BytecodeOp::kModInt64Const,
                                   .operand = spec_idx});
          return true;
        }
      }
      if (call.FuncName() == "abs") {
        if (call.Args().size() != 1 ||
            !CompileNode(call.Args()[0], schema, program)) {
          return false;
        }
        program->AddInstruction({.opcode = BytecodeOp::kAbs});
        return true;
      }
      if (call.FuncName() == "substr" || call.FuncName() == "substring") {
        // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
        //   (ascii_substr_1based_slice_sound)
        if (call.Args().size() != 2 && call.Args().size() != 3) {
          return false;
        }
        if (!call.Args()[0]) {
          return false;
        }
        const Type str_t = call.Args()[0]->ResultType(schema);
        if (ValueTypeFor(str_t) != ValueType::kVarChar) {
          return false;
        }
        for (size_t i = 1; i < call.Args().size(); ++i) {
          if (!call.Args()[i]) {
            return false;
          }
          const Type idx_t = call.Args()[i]->ResultType(schema);
          if (ValueTypeFor(idx_t) != ValueType::kInt64) {
            return false;
          }
        }
        for (const Expression& arg : call.Args()) {
          if (!CompileNode(arg, schema, program)) {
            return false;
          }
        }
        program->MarkHasSubstr();
        program->AddInstruction(
            {.opcode = BytecodeOp::kSubstr,
             .operand = static_cast<uint16_t>(call.Args().size())});
        return true;
      }
      if (call.FuncName() == "coalesce" || call.FuncName() == "ifnull") {
        if (call.Args().empty() ||
            (call.FuncName() == "ifnull" && call.Args().size() != 2)) {
          return false;
        }
        std::vector<size_t> exits;
        exits.reserve(call.Args().size());
        for (size_t i = 0; i < call.Args().size(); ++i) {
          if (!CompileNode(call.Args()[i], schema, program)) {
            return false;
          }
          if (i + 1 != call.Args().size()) {
            exits.push_back(program->Size());
            program->AddInstruction({.opcode = BytecodeOp::kJumpIfNotNull});
            program->AddInstruction({.opcode = BytecodeOp::kPop});
          }
        }
        for (const auto jump : exits) {
          program->SetJumpTarget(jump, static_cast<int32_t>(program->Size()) -
                                           static_cast<int32_t>(jump));
        }
        {
          const auto common = call.CoalesceType(schema);
          if (!common.HasValue()) {
            return false;
          }
          if (common.Value() == TypeTag::kDouble) {
            uint16_t offset = 0;
            if (!program->AddCast({.target_type_name = "FLOAT64",
                                   .target_value_type = ValueType::kDouble,
                                   .return_null_on_error = false},
                                  &offset)) {
              return false;
            }
            program->AddInstruction(
                {.opcode = BytecodeOp::kCast, .operand = offset});
          }
        }
        return true;
      }
      // EXTRACT(year|month|day FROM x) lowers to one dedicated op whose
      // kernel reproduces the AST evaluator; every other function (and any
      // odd arity) fails compilation and the caller falls back to the AST
      // evaluator, the semantic ground truth.
      uint16_t part = 0;
      if (call.FuncName() == "extract_year") {
        part = 0;
      } else if (call.FuncName() == "extract_month") {
        part = 1;
      } else if (call.FuncName() == "extract_day") {
        part = 2;
      } else {
        return false;
      }
      if (call.Args().size() != 1) {
        return false;
      }
      if (!CompileNode(call.Args()[0], schema, program)) {
        return false;
      }
      program->AddInstruction(
          {.opcode = BytecodeOp::kExtractDatePart, .operand = part});
      return true;
    }
    case TypeTag::kCastExp: {
      const CastExpression& cast = expression->AsCastExpression();
      auto to_upper = [](std::string_view text) {
        std::string out;
        out.reserve(text.size());
        for (char c : text) {
          out.push_back(
              static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
        }
        return out;
      };
      const std::string upper_target = to_upper(cast.TargetTypeName());
      const bool timezone_sensitive =
          upper_target == "TIMESTAMP" || upper_target == "DATETIME" ||
          upper_target == "TIME" ||
          (upper_target == "DATE" &&
           (!cast.Child() || cast.Child()->Type() != TypeTag::kConstantValue ||
            (!cast.Child()->AsConstantValue().GetValue().IsNull() &&
             cast.Child()->AsConstantValue().GetValue().type !=
                 ValueType::kDate &&
             cast.Child()->AsConstantValue().GetValue().type !=
                 ValueType::kInt64 &&
             (cast.Child()->AsConstantValue().GetValue().type !=
                  ValueType::kVarChar ||
              cast.Child()
                      ->AsConstantValue()
                      .GetValue()
                      .value.varchar_value.size() > 10))));
      if (!timezone_sensitive && cast.Child() &&
          cast.Child()->Type() == TypeTag::kConstantValue) {
        StatusOr<Value> folded =
            cast.TryCast(cast.Child()->AsConstantValue().GetValue());
        if (folded.HasValue()) {
          uint16_t offset = 0;
          if (!program->AddConstant(folded.MoveValue(), &offset)) {
            return false;
          }
          program->AddInstruction(
              {.opcode = BytecodeOp::kLoadConstant, .operand = offset});
          return true;
        }
      }
      if (!cast.Child() || !CompileNode(cast.Child(), schema, program)) {
        return false;
      }
      uint16_t cast_offset = 0;
      if (!program->AddCast({.target_type_name = cast.TargetTypeName(),
                             .target_value_type = cast.TargetValueType(),
                             .return_null_on_error = cast.ReturnNullOnError()},
                            &cast_offset)) {
        return false;
      }
      program->AddInstruction(
          {.opcode = BytecodeOp::kCast, .operand = cast_offset});
      return true;
    }
    default:
      return false;
  }
}

}  // namespace

bool AuditIsErrorFreePredicateForOrdering(const Expression& expr,
                                          const Schema& schema) {
  return IsErrorFreePredicateForOrdering(expr, schema);
}

namespace {

StatusOr<Value> TryAbsValue(const Value& value) {
  if (value.IsNull()) {
    return Value();
  }
  if (value.type == ValueType::kInt64) {
    if (value.IsUnsigned()) {
      return value;
    }
    if (value.value.int_value == std::numeric_limits<int64_t>::min()) {
      return StatusError(StatusCode::kIsInfinity, "integer overflow in ABS");
    }
    return Value(std::abs(value.value.int_value));
  }
  if (value.type == ValueType::kDouble) {
    return Value(std::fabs(value.value.double_value));
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "ABS requires numeric argument");
}

bool IsAllAsciiFast(std::string_view s) noexcept {
  const auto* p = reinterpret_cast<const unsigned char*>(s.data());
  size_t i = 0;
  const size_t n = s.size();
  while (i + 8 <= n) {
    uint64_t word = 0;
    std::memcpy(&word, p + i, sizeof(word));
    if ((word & 0x8080808080808080ULL) != 0) {
      return false;
    }
    i += 8;
  }
  while (i < n) {
    if ((p[i] & 0x80U) != 0) {
      return false;
    }
    ++i;
  }
  return true;
}

// Lean proofs: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (ascii_substr_1based_slice_sound),
//   lean/TinylambOpt/ResearchProofs/SubstrAsciiByteSliceEquiv.lean (sound)
StatusOr<std::string_view> TryEvalSubstrView(
    std::string_view input, int64_t start,
    std::optional<int64_t> opt_len) {
  if (opt_len.has_value()) {
    if (*opt_len < 0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "SUBSTR length cannot be negative");
    }
    if (*opt_len == 0) {
      return std::string_view{};
    }
  }
  if (IsAllAsciiFast(input)) [[likely]] {
    const size_t total_cps = input.size();
    int64_t actual_start = 0;
    if (start > 0) {
      actual_start = start - 1;
    } else if (start < 0) {
      actual_start = static_cast<int64_t>(total_cps) + start;
    }
    actual_start = std::max<int64_t>(actual_start, 0);
    if (std::cmp_greater_equal(actual_start, total_cps)) {
      return std::string_view{};
    }
    const auto ustart = static_cast<size_t>(actual_start);
    const size_t rem = total_cps - ustart;
    const size_t take = opt_len.has_value()
                            ? std::min(rem, static_cast<size_t>(*opt_len))
                            : rem;
    return input.substr(ustart, take);
  }
  // General UTF-8 code point slice: still a contiguous byte view of `input`.
  size_t total_cps = 0;
  for (const char c : input) {
    if ((static_cast<unsigned char>(c) & 0xC0U) != 0x80U) {
      ++total_cps;
    }
  }
  int64_t actual_start = 0;
  if (start > 0) {
    actual_start = start - 1;
  } else if (start < 0) {
    actual_start = static_cast<int64_t>(total_cps) + start;
  }
  actual_start = std::max<int64_t>(actual_start, 0);
  if (std::cmp_greater_equal(actual_start, total_cps)) {
    return std::string_view{};
  }
  const auto cp_begin = static_cast<size_t>(actual_start);
  const size_t max_take =
      opt_len.has_value() ? static_cast<size_t>(*opt_len) : total_cps;
  const size_t cp_end =
      max_take >= total_cps - cp_begin ? total_cps : cp_begin + max_take;
  size_t cur_cp = 0;
  size_t byte_begin = input.size();
  size_t byte_end = input.size();
  for (size_t i = 0; i < input.size(); ++i) {
    if ((static_cast<unsigned char>(input[i]) & 0xC0U) != 0x80U) {
      if (cur_cp == cp_begin) {
        byte_begin = i;
      }
      if (cur_cp == cp_end) {
        byte_end = i;
        break;
      }
      ++cur_cp;
    }
  }
  if (byte_begin > byte_end) {
    return std::string_view{};
  }
  return input.substr(byte_begin, byte_end - byte_begin);
}

StatusOr<Value> TryEvalSubstrCanonical(const Value& str_v, const Value& start_v,
                                       const Value* len_v) {
  if (str_v.IsNull() || start_v.IsNull() ||
      (len_v != nullptr && len_v->IsNull())) {
    return Value();
  }
  if (str_v.type != ValueType::kVarChar || str_v.IsInterval() ||
      str_v.Collation() != 0 || start_v.type != ValueType::kInt64 ||
      (len_v != nullptr && len_v->type != ValueType::kInt64)) {
    return StatusError(StatusCode::kInvalidArgument,
                       "SUBSTR argument type mismatch");
  }
  // AST Evaluate is the semantic reference: SUBSTR accepts UINT64 start/length
  // (they share the kInt64 tag). The start keeps its raw bit pattern so a
  // UINT64 near 2^64 still acts as an end-relative negative offset, while the
  // length uses the same saturating conversion as the AST evaluator.
  const std::optional<int64_t> opt_len =
      len_v != nullptr
          ? std::optional<int64_t>(
                len_v->IsUnsigned()
                    ? SaturatingUnsignedToInt64(
                          static_cast<uint64_t>(len_v->value.int_value))
                    : len_v->value.int_value)
          : std::nullopt;
  ASSIGN_OR_RETURN(std::string_view, sliced,
                   TryEvalSubstrView(str_v.value.varchar_value,
                                     start_v.value.int_value, opt_len));
  return Value(std::string(sliced));
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean (in_const_set_3vl_sound)
StatusOr<Value> TryEvalInStringSetCanonical(const Value& child,
                                            const StringSetSpec& spec) {
  if (!spec.non_empty) {
    return Value(false);
  }
  if (child.IsNull()) {
    return Value();
  }
  if (child.type != ValueType::kVarChar || child.IsInterval()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "IN string-set operand mismatch");
  }
  if (child.Collation() != 0) {
    for (const auto& slot : spec.table) {
      if (slot.offset == std::numeric_limits<uint32_t>::max()) {
        continue;
      }
      const Value candidate(
          std::string(spec.arena.data() + slot.offset, slot.len));
      ASSIGN_OR_RETURN(
          Value, match,
          TryEvaluateBinary(BinaryOperation::kEquals, child, candidate));
      if (!match.IsNull() && match.Truthy()) {
        return Value(true);
      }
    }
    return spec.has_null ? Value() : Value(false);
  }
  if (spec.Contains(child.value.varchar_value)) {
    return Value(true);
  }
  return spec.has_null ? Value() : Value(false);
}

// Lean proof: lean/TinylambOpt/SimdInRange.lean
//   (simd_in_range_sound, int64_range_lowering)
StatusOr<Value> TryEvalInt64InRangeCanonical(const Value& child,
                                             const Int64RangeSpec& spec) {
  if (child.IsNull()) {
    return Value();
  }
  if (child.type != spec.expected_type || child.IsUnsigned()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "int64 range operand type mismatch");
  }
  return Value(spec.Contains(child.value.int_value));
}

// Lean proof: lean/TinylambOpt/ResearchProofs/ReciprocalDivMod.lean
//   (est_bounds, divModConst_sound)
StatusOr<Value> TryEvalReciprocalDivModCanonical(
    const Value& child, const ReciprocalDivModSpec& spec, bool is_div) {
  if (child.IsNull()) {
    return Value();
  }
  if (child.type != ValueType::kInt64 || child.IsUnsigned()) {
    if (child.type == ValueType::kInt64 && child.IsUnsigned()) {
      return is_div ? child.TryArithmetic(Value(spec.divisor),
                                          BinaryOperation::kDivide)
                    : child.TryArithmetic(Value(spec.divisor),
                                          BinaryOperation::kModulo);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       "reciprocal div/mod operand type mismatch");
  }
  return Value(is_div ? spec.Div(child.value.int_value)
                      : spec.Mod(child.value.int_value));
}

// Lean proof: lean/TinylambOpt/ResearchProofs/RowInMask.lean (sound)
StatusOr<Value> TryEvalRowInMaskCanonical(std::span<const Value> fields,
                                          const RowInMaskSpec& spec) {
  const uint32_t k = spec.num_fields;
  if (k == 0 || fields.size() != k) {
    return StatusError(StatusCode::kCorrupt, "invalid RowInMask arity");
  }
  bool all_signed_int_or_null = true;
  for (uint32_t c = 0; c < k; ++c) {
    const Value& v = fields[c];
    if (!v.IsNull() && (v.type != ValueType::kInt64 || v.IsUnsigned())) {
      all_signed_int_or_null = false;
      break;
    }
  }
  if (all_signed_int_or_null) {
    uint64_t any_sure = 0;
    uint64_t any_possible = 0;
    for (const RowInMaskSpec::Chunk& chunk : spec.chunks) {
      uint64_t sure = chunk.enabled;
      uint64_t possible = chunk.enabled;
      for (uint32_t c = 0; c < k; ++c) {
        const Value& xv_val = fields[c];
        const RowInMaskSpec::Column& col = chunk.columns[c];
        if (xv_val.IsNull()) {
          sure = 0;
        } else {
          const int64_t xv = xv_val.value.int_value;
          uint64_t eq = 0;
          const uint32_t cnt = chunk.count;
          for (uint32_t i = 0; i < cnt; ++i) {
            eq |= static_cast<uint64_t>(col.values[i] == xv) << i;
          }
          eq &= col.non_null_mask;
          sure &= eq;
          possible &= (col.null_mask | eq);
          if (possible == 0) {
            break;
          }
        }
      }
      any_sure |= sure;
      any_possible |= possible;
      if (any_sure != 0) {
        break;
      }
    }
    if (any_sure != 0) {
      return Value(true);
    }
    return any_possible != 0 ? Value() : Value(false);
  }
  // Off-schema runtime fallback: match AST StructJsonCompare / InExpression.
  std::vector<std::pair<std::string, Value>> lhs_fields;
  lhs_fields.reserve(k);
  for (uint32_t c = 0; c < k; ++c) {
    lhs_fields.emplace_back("f" + std::to_string(c + 1), fields[c]);
  }
  const Value lhs(EncodeStructJson(lhs_fields));
  bool saw_null = false;
  std::vector<std::pair<std::string, Value>> rhs_fields(k);
  for (uint32_t c = 0; c < k; ++c) {
    rhs_fields[c].first = "f" + std::to_string(c + 1);
  }
  for (const RowInMaskSpec::Chunk& chunk : spec.chunks) {
    for (uint32_t i = 0; i < chunk.count; ++i) {
      const uint64_t bit = uint64_t{1} << i;
      for (uint32_t c = 0; c < k; ++c) {
        if ((chunk.columns[c].non_null_mask & bit) != 0) {
          rhs_fields[c].second = Value(chunk.columns[c].values[i]);
        } else {
          rhs_fields[c].second = Value();
        }
      }
      const Value rhs(EncodeStructJson(rhs_fields));
      ASSIGN_OR_RETURN(Value, eq,
                       TryEvaluateBinary(BinaryOperation::kEquals, lhs, rhs));
      if (eq.IsNull()) {
        saw_null = true;
      } else if (eq.Truthy()) {
        return Value(true);
      }
    }
  }
  return saw_null ? Value() : Value(false);
}

// The fast interpreter supports these opcodes, including control-flow jumps,
// native DATE EXTRACT and CAST. Remaining VARCHAR/DATE operations delegate to
// the canonical scalar evaluator.
bool FastOpcode(BytecodeOp opcode) {
  switch (opcode) {
    case BytecodeOp::kLoadColumn:
    case BytecodeOp::kLoadConstant:
    case BytecodeOp::kBinaryInt64:
    case BytecodeOp::kBinaryDouble:
    case BytecodeOp::kBinaryVarchar:
    case BytecodeOp::kBinaryDate:
    case BytecodeOp::kUnaryInt64:
    case BytecodeOp::kUnaryDouble:
    case BytecodeOp::kExtractDatePart:
    case BytecodeOp::kAbs:
    case BytecodeOp::kJump:
    case BytecodeOp::kJumpIfFalse:
    case BytecodeOp::kJumpIfTrue:
    case BytecodeOp::kJumpIfNotNull:
    case BytecodeOp::kPop:
    case BytecodeOp::kDup:
    case BytecodeOp::kCast:
    case BytecodeOp::kInStringSet:
    case BytecodeOp::kSubstr:
    case BytecodeOp::kInt64InRange:
    case BytecodeOp::kDivInt64Const:
    case BytecodeOp::kModInt64Const:
    case BytecodeOp::kRowInMask:
      return true;
    default:
      return false;
  }
}

// Compile-time under-approximation: every instruction is fast-shaped and
// every constant loaded is numeric, untagged VARCHAR, or NULL.  Programs
// built by hand
// (BytecodeProgram{} placeholders) never get the flag.
bool ProgramIsFastInterpretable(const BytecodeProgram& program) {
  if (program.Instructions().empty()) {
    return false;
  }
  for (const BytecodeInstruction& instruction : program.Instructions()) {
    if (!FastOpcode(instruction.opcode)) {
      return false;
    }
    if (instruction.opcode == BytecodeOp::kCast) {
      const CastSpec& spec = program.Casts()[instruction.operand];
      if (spec.target_value_type != ValueType::kInt64 &&
          spec.target_value_type != ValueType::kDouble &&
          spec.target_value_type != ValueType::kDate) {
        return false;
      }
    }
    if (instruction.opcode == BytecodeOp::kLoadConstant) {
      const Value& constant = program.Constants()[instruction.operand];
      switch (constant.type) {
        case ValueType::kNull:
        case ValueType::kInt64:
        case ValueType::kDouble:
        case ValueType::kDate:
          if (constant.IsUnsigned()) {
            return false;
          }
          break;
        case ValueType::kVarChar:
          // A collated string constant compares under folding rules the
          // fast slot cannot reproduce; keep those programs canonical.
          if (constant.Collation() != 0) {
            return false;
          }
          break;
        default:
          return false;
      }
    }
  }
  return true;
}

}  // namespace

namespace {
bool EncodeCompileTree(const Expression& expression, Encoder* encoder,
                       size_t* nodes, size_t depth = 0) {
  // Bounded key encoding: the cap keeps cache-key construction O(1) per node
  // and the key under the caller's 16 KiB budget. An oversized/deep tree is
  // not an error -- the caller simply declines the cache key and compiles the
  // expression normally (CompileRowCached falls back to Compile,
  // ScalarExpressionCacheKey returns nullopt). The cliff near 2048 nodes is
  // therefore a one-time per-execution compile, not a wrong answer.
  if (++*nodes > 2048 || depth > 128) {
    return false;
  }
  *encoder << static_cast<bool>(expression);
  if (!expression) {
    return true;
  }
  *encoder << static_cast<uint32_t>(expression->Type());
  const auto child = [&](const Expression& value) {
    return EncodeCompileTree(value, encoder, nodes, depth + 1);
  };
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      *encoder << expression->AsColumnValue().GetColumnName();
      return true;
    case TypeTag::kConstantValue: {
      const Value& value = expression->AsConstantValue().GetValue();
      if (value.type == ValueType::kArray ||
          (value.type == ValueType::kVarChar &&
           value.value.varchar_value.size() > 16384)) {
        return false;
      }
      const ExpressionBase& expr_base = *expression;
      if (typeid(expr_base) != typeid(ConstantValue)) {
        const auto evaluated = expression->TryEvaluate(Row(), Schema());
        if (!evaluated.HasValue() || evaluated.Value().type != value.type ||
            evaluated.Value().Collation() != value.Collation()) {
          return false;
        }
        const auto encoded = TryEncode(evaluated.Value());
        if (!encoded.HasValue() || encoded.Value() != Encode(value)) {
          return false;
        }
      }
      // STRING and TIMESTAMP share the VARCHAR representation, so the Value
      // alone cannot distinguish two constants that the evaluator treats
      // differently. Fold the logical text domain into the cache key or a
      // compiled program for one domain would be reused for the other.
      *encoder << value << value.Collation()
               << static_cast<uint8_t>(
                      expression->AsConstantValue().GetTextDomain());
      return true;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      *encoder << static_cast<uint32_t>(binary.Op());
      return child(binary.Left()) && child(binary.Right());
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      *encoder << static_cast<uint32_t>(unary.Op());
      return child(unary.Child());
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      *encoder << static_cast<uint64_t>(in.list_.size());
      if (!child(in.child_)) {
        return false;
      }
      return std::ranges::all_of(
          in.list_, [&](const auto& value) { return child(value); });
    }
    case TypeTag::kCaseExp: {
      const auto& conditional = expression->AsCaseExpression();
      *encoder << conditional.IsFromIf()
               << static_cast<uint64_t>(conditional.when_clauses_.size());
      for (const auto& clause : conditional.when_clauses_) {
        if (!child(clause.first) || !child(clause.second)) {
          return false;
        }
      }
      return child(conditional.else_clause_);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      if (call.FuncName() != "coalesce" && call.FuncName() != "ifnull" &&
          call.FuncName() != "byte_length" &&
          call.FuncName() != "octet_length" && call.FuncName() != "length" &&
          call.FuncName() != "char_length" &&
          call.FuncName() != "character_length" && call.FuncName() != "abs" &&
          call.FuncName() != "substr" && call.FuncName() != "substring" &&
          call.FuncName() != "__struct_json__") {
        return false;
      }
      *encoder << call.FuncName() << static_cast<uint64_t>(call.Args().size());
      return std::ranges::all_of(call.Args(), child);
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      std::string upper_target = cast.TargetTypeName();
      for (char& c : upper_target) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
      }
      if (upper_target == "TIMESTAMP" || upper_target == "DATETIME" ||
          upper_target == "TIME") {
        return false;
      }
      if (upper_target == "DATE" && cast.Child() &&
          cast.Child()->Type() == TypeTag::kConstantValue &&
          cast.Child()->AsConstantValue().GetValue().type ==
              ValueType::kVarChar &&
          cast.Child()
                  ->AsConstantValue()
                  .GetValue()
                  .value.varchar_value.size() > 10) {
        return false;
      }
      *encoder << cast.TargetTypeName() << cast.ReturnNullOnError();
      return child(cast.Child());
    }
    default:
      // Functions, subqueries, aggregates and other context-bearing nodes
      // keep the ordinary compiler; none enter the retained cache.
      return false;
  }
}
}  // namespace

bool StringSetSpec::Contains(std::string_view sv) const noexcept {
  if (table.empty()) {
    return false;
  }
  const uint64_t h = HashView(sv);
  uint32_t pos = static_cast<uint32_t>(h) & mask;
  while (table[pos].offset != std::numeric_limits<uint32_t>::max()) {
    const Slot& s = table[pos];
    if (s.hash == h && s.len == sv.size() &&
        (sv.empty() ||
         std::memcmp(arena.data() + s.offset, sv.data(), sv.size()) == 0)) {
      return true;
    }
    pos = (pos + 1U) & mask;
  }
  return false;
}

std::optional<std::string> ScalarExpressionCacheKey(
    const std::vector<Expression>& expressions, const Schema& schema) {
  std::string key;
  key.reserve(512);
  Encoder encoder(key);
  encoder << schema << static_cast<uint64_t>(expressions.size());
  size_t nodes = 0;
  // EncodeCompileTree may evaluate constant-shaped nodes to build the cache
  // key; TryEvaluate is the no-throw contract, but Encoder allocations
  // (bad_alloc) and any contract-violating override can still throw. Mirror
  // Compile(): an unsupported shape simply opts out of the cache.
  try {
    for (const auto& expression : expressions) {
      if (!EncodeCompileTree(expression, &encoder, &nodes)) {
        return std::nullopt;
      }
    }
  } catch (...) {
    return std::nullopt;
  }
  if (encoder.GetStatus() != Status::kSuccess || key.size() > 16384) {
    return std::nullopt;
  }
  return key;
}

Expression FreezeScalarBindings(const Expression& expression) {
  if (!expression) {
    return nullptr;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    return ConstantValueExp(expression->AsConstantValue().GetValue());
  }
  auto children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  for (auto& child : children) {
    child = FreezeScalarBindings(child);
  }
  return WithExpressionChildren(expression, std::move(children));
}

std::optional<BytecodeProgram> BytecodeCompiler::CompileRowCached(
    const Expression& expression, const Schema& schema, Context context) {
  if (!expression) {
    return std::nullopt;
  }
  std::string key;
  key.reserve(512);
  Encoder encoder(key);
  encoder << static_cast<uint8_t>(context) << BytecodeOptToggleBits() << schema;
  size_t nodes = 0;
  // EncodeCompileTree may evaluate constant-shaped nodes to build the key
  // (see ScalarExpressionCacheKey above). TryEvaluate is the no-throw
  // contract -- an un-rebound ParameterSlot surfaces as a kRuntimeError
  // Status, not an exception -- but Encoder allocations (bad_alloc) and any
  // contract-violating override can still throw. Opt out of the cache and
  // compile normally rather than letting an exception escape this
  // optional-returning path; Compile is the semantic ground truth, so the
  // fallback can never change results, only skip the cache.
  try {
    if (!EncodeCompileTree(expression, &encoder, &nodes)) {
      return Compile(expression, schema, context);
    }
  } catch (...) {
    return Compile(expression, schema, context);
  }
  if (encoder.GetStatus() != Status::kSuccess || key.size() > 16384) {
    return Compile(expression, schema, context);
  }
  struct CacheEntry {
    std::optional<BytecodeProgram> program;
    size_t bytes;
  };
  struct Cache {
    size_t bytes{0};
    std::unordered_map<std::string, CacheEntry> entries;
  };
  static thread_local Cache cache;
  if (auto found = cache.entries.find(key); found != cache.entries.end()) {
    return found->second.program;
  }
  auto program = Compile(expression, schema, context);
  if (!program && expression &&
      (expression->Type() == TypeTag::kCaseExp ||
       expression->Type() == TypeTag::kFunctionCallExp)) {
    // CASE (and any function call, which may hide a dynamic result type)
    // needs row-at-a-time interpretation: keep each branch's Value intact
    // and mark the program as having no single batch output type.
    BytecodeProgram dynamic;
    if (CompileNode(expression, schema, &dynamic)) {
      dynamic.SetResultType(ValueType::kNull);
      dynamic.SetFastInterpretable(ProgramIsFastInterpretable(dynamic));
      program = std::move(dynamic);
    }
  }
  const auto entry_bytes = [](const std::string& entry_key,
                              const std::optional<BytecodeProgram>& value) {
    size_t result = entry_key.capacity() + 128;
    if (value) {
      result +=
          (value->Instructions().capacity() * sizeof(BytecodeInstruction)) +
          (value->Constants().capacity() * sizeof(Value));
      for (const Value& constant : value->Constants()) {
        if (constant.type == ValueType::kVarChar) {
          result += (2 * constant.value.varchar_value.size()) + 32;
        }
      }
    }
    return result;
  };
  const size_t bytes = entry_bytes(key, program);
  constexpr size_t kLimit = size_t{2} * 1024 * 1024;
  if (bytes > kLimit) {
    return program;
  }
  // Evict only as many entries as are needed to fit the new one instead of
  // dropping the whole cache. A full clear makes a just-compiled expression
  // recompile on every call once the cache is warm, collapsing the
  // steady-state hit rate to zero. The map keeps no LRU order, so the victim
  // is arbitrary, but the rest of the cache survives.
  while (!cache.entries.empty() &&
         (cache.entries.size() >= 256 || cache.bytes > kLimit - bytes)) {
    const auto victim = cache.entries.begin();
    cache.bytes -= victim->second.bytes;
    cache.entries.erase(victim);
  }
  cache.entries.emplace(std::move(key), CacheEntry{.program=program, .bytes=bytes});
  cache.bytes += bytes;
  return program;
}

std::optional<BytecodeProgram> BytecodeCompiler::Compile(
    const Expression& expression, const Schema& schema, Context context) {
  try {
    // Mirror the optimizer's split: NOT() over an ordered double comparison
    // is NaN-unsound, so such trees keep the comparison-free rule set instead
    // of Default() (see plan/optimizer.cpp).  Value contexts additionally
    // drop the truthiness-preserving collapses in MakeValueRules, which are
    // only sound where the result is consumed as a predicate.
    const ExpressionRuleSet& base_rules =
        ContainsNotOfOrderedDoubleComparison(expression, schema)
            ? NotComparisonFreeRules()
            : ExpressionRuleSet::Default();
    const ExpressionRuleSet* rules = &base_rules;
    if (context == Context::kValue) {
      rules = (&base_rules) == &ExpressionRuleSet::Default()
                  ? &DefaultValueRules()
                  : &NotComparisonValueRules();
    }
    ExpressionRewriter rewriter(*rules);

    rewriter.set_schema(&schema);
    StatusOr<Expression> folded_or = rewriter.TryRewrite(expression);
    if (!folded_or.HasValue()) {
      return std::nullopt;
    }
    const Expression folded = folded_or.MoveValue();
    BytecodeProgram program;
    if (!folded || !CompileNode(folded, schema, &program)) {
      return std::nullopt;
    }
    const auto uniform_coalesce = [&](const auto& self,
                                      const Expression& node) -> bool {
      if (!node) {
        return true;
      }
      if (node->Type() == TypeTag::kFunctionCallExp) {
        const auto& call = node->AsFunctionCallExpression();
        if (call.FuncName() == "coalesce" || call.FuncName() == "ifnull") {
          ValueType common = ValueType::kNull;
          for (const auto& arg : call.Args()) {
            if (!arg) {
              return false;
            }
            const auto type = ValueTypeFor(arg->ResultType(schema));
            if (type == ValueType::kNull) {
              if (arg->Type() != TypeTag::kConstantValue ||
                  !arg->AsConstantValue().GetValue().IsNull()) {
                return false;
              }
              continue;
            }
            if (common != ValueType::kNull && common != type) {
              return false;
            }
            common = type;
          }
        }
      }
      return std::ranges::all_of(
          ExpressionChildren(node),
          [&](const auto& child) { return self(self, child); });
    };
    if (!uniform_coalesce(uniform_coalesce, folded)) {
      return std::nullopt;
    }
    const ValueType result_type = ValueTypeFor(folded->ResultType(schema));
    if (result_type == ValueType::kNull) {
      return std::nullopt;
    }
    program.SetResultType(result_type);
    program.SetFastInterpretable(ProgramIsFastInterpretable(program));
    return program;
  } catch (const std::exception&) {
    // Plans with partially qualified or deferred schemas remain valid.  An
    // unsupported bytecode shape must fall back to the expression tree.
    return std::nullopt;
  }
}

StatusOr<ColumnVector> BytecodeProgram::TryEvaluateBatch(
    const DataChunk& input) const {
  // Infer the value domain as well as the physical type: the static result
  // type alone cannot distinguish UINT64 from INT64.
  ColumnVector result(ValueType::kNull, input.Size());
  std::vector<Value> stack;
  stack.reserve(instructions_.size());
  for (size_t row = 0; row < input.Size(); ++row) {
    stack.clear();
    // D7: a program counter (not a range-for) so the short-circuit jumps can
    // skip the right operand; jump targets are relative to the jump itself.
    for (size_t pc = 0; pc < instructions_.size(); ++pc) {
      const BytecodeInstruction& instruction = instructions_[pc];
      switch (instruction.opcode) {
        case BytecodeOp::kLoadColumn:
          stack.push_back(input.ColumnAt(instruction.operand).ValueAt(row));
          break;
        case BytecodeOp::kLoadConstant:
          stack.push_back(constants_[instruction.operand]);
          break;
        case BytecodeOp::kJumpIfFalse:
        case BytecodeOp::kJumpIfTrue: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          const Value& top = stack.back();
          const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                   ? (!top.IsNull() && !top.Truthy())
                                   : (!top.IsNull() && top.Truthy());
          if (decides) {
            // The AST normalizes a short-circuit result to BOOLEAN
            // (Value(op == kOr)); replace the raw truthy operand so e.g.
            // `(-1 OR x)` yields TRUE, not -1.
            stack.back() = Value(instruction.opcode == BytecodeOp::kJumpIfTrue);
            pc += static_cast<size_t>(instruction.jump_target);
            --pc;  // the for-loop ++pc lands on the target.
          }
          break;
        }
        case BytecodeOp::kJumpIfNotNull:
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          if (!stack.back().IsNull()) {
            pc += static_cast<size_t>(instruction.jump_target);
            --pc;
          }
          break;
        case BytecodeOp::kJump:
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
          break;
        case BytecodeOp::kBinaryInt64:
        case BytecodeOp::kBinaryDouble:
        case BytecodeOp::kBinaryVarchar:
        case BytecodeOp::kBinaryDate: {
          // Only compiler-generated programs reach here; an empty stack means
          // a malformed program.
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value right = std::move(stack.back());
          stack.pop_back();
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value left = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(
              Value, combined,
              (TryEvaluateBinary(instruction.binary, left, right)));
          stack.push_back(std::move(combined));
          break;
        }
        case BytecodeOp::kUnaryInt64:
        case BytecodeOp::kUnaryDouble: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value child = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, result_value,
                           (TryEvaluateUnary(instruction.unary, child)));
          stack.push_back(std::move(result_value));
          break;
        }
        case BytecodeOp::kExtractDatePart: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value child = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, part_value,
                           TryExtractDatePart(child, instruction.operand));
          stack.push_back(std::move(part_value));
          break;
        }
        case BytecodeOp::kByteLength: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          const auto& child = stack.back();
          const Value result_value =
              child.IsNull() ? Value()
                             : Value(static_cast<int64_t>(
                                   child.type == ValueType::kVarChar
                                       ? child.value.varchar_value.size()
                                       : child.AsString().size()));
          stack.back() = result_value;
          break;
        }
        case BytecodeOp::kCharLength: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          const auto& child = stack.back();
          const Value result_value =
              child.IsNull() ? Value()
                             : Value(Utf8CodePointCount(
                                   child.type == ValueType::kVarChar
                                       ? child.value.varchar_value
                                       : std::string_view(child.AsString())));
          stack.back() = result_value;
          break;
        }
        case BytecodeOp::kAbs: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          ASSIGN_OR_RETURN(Value, abs_value, TryAbsValue(stack.back()));
          stack.back() = std::move(abs_value);
          break;
        }
        case BytecodeOp::kPop: {
          if (stack.size() <= instruction.operand) {
            return StatusError(StatusCode::kInvalidArgument,
                               "bytecode stack index out of range");
          }
          stack.erase(stack.end() - 1 - instruction.operand);
          break;
        }
        case BytecodeOp::kDup: {
          if (stack.size() <= instruction.operand) {
            return StatusError(StatusCode::kInvalidArgument,
                               "bytecode stack index out of range");
          }
          stack.push_back(stack[stack.size() - 1 - instruction.operand]);
          break;
        }
        case BytecodeOp::kCast: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value child = std::move(stack.back());
          stack.pop_back();
          const CastSpec& spec = casts_[instruction.operand];
          ASSIGN_OR_RETURN(
              Value, casted,
              TryCastValue(child, spec.target_type_name, spec.target_value_type,
                           spec.return_null_on_error));
          stack.push_back(std::move(casted));
          break;
        }
        case BytecodeOp::kInStringSet: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value child = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, matched,
                           TryEvalInStringSetCanonical(
                               child, string_sets_[instruction.operand]));
          stack.push_back(std::move(matched));
          break;
        }
        case BytecodeOp::kSubstr: {
          Value len_v;
          const bool has_len = instruction.operand == 3;
          if (has_len) {
            if (stack.empty()) {
              return StatusError(StatusCode::kCorrupt,
                                 "bytecode stack underflow");
            }
            len_v = std::move(stack.back());
            stack.pop_back();
          }
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value start_v = std::move(stack.back());
          stack.pop_back();
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          Value str_v = std::move(stack.back());
          stack.pop_back();
          ASSIGN_OR_RETURN(Value, sub_v,
                           TryEvalSubstrCanonical(str_v, start_v,
                                                  has_len ? &len_v : nullptr));
          stack.push_back(std::move(sub_v));
          break;
        }
        case BytecodeOp::kInt64InRange: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          if (instruction.operand >= int64_ranges_.size()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode int64-range operand out of bounds");
          }
          ASSIGN_OR_RETURN(
              Value, in_range,
              TryEvalInt64InRangeCanonical(stack.back(),
                                           int64_ranges_[instruction.operand]));
          stack.back() = std::move(in_range);
          break;
        }
        case BytecodeOp::kDivInt64Const:
        case BytecodeOp::kModInt64Const: {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          if (instruction.operand >= reciprocals_.size()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode reciprocal operand out of bounds");
          }
          ASSIGN_OR_RETURN(
              Value, res,
              TryEvalReciprocalDivModCanonical(
                  stack.back(), reciprocals_[instruction.operand],
                  instruction.opcode == BytecodeOp::kDivInt64Const));
          stack.back() = std::move(res);
          break;
        }
        case BytecodeOp::kRowInMask: {
          const RowInMaskSpec& spec = row_in_masks_[instruction.operand];
          const uint32_t k = spec.num_fields;
          if (k == 0 || stack.size() < k) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          const size_t base = stack.size() - k;
          ASSIGN_OR_RETURN(
              Value, res,
              TryEvalRowInMaskCanonical(
                  std::span<const Value>(stack.data() + base, k), spec));
          stack.resize(base);
          stack.push_back(std::move(res));
          break;
        }
      }
    }
    if (stack.size() != 1) {
      return StatusError(StatusCode::kRuntimeError, "invalid bytecode stack");
    }
    result.Append(stack.back());
  }
  return result;
}

ColumnVector BytecodeProgram::EvaluateBatch(const DataChunk& input) const {
  return ExcShimUnwrap(TryEvaluateBatch(input),
                       "BytecodeProgram::EvaluateBatch");
}

StatusOr<Value> BytecodeProgram::TryEvaluateRow(const Row& row) const {
  // The interpreter has no call instruction, so evaluation cannot re-enter
  // this function; reusing one stack per thread removes a heap allocation
  // per row on the hot aggregation/filter paths.
  thread_local std::vector<Value> stack;
  stack.clear();
  stack.reserve(instructions_.size());
  for (size_t pc = 0; pc < instructions_.size(); ++pc) {
    const BytecodeInstruction& instruction = instructions_[pc];
    switch (instruction.opcode) {
      case BytecodeOp::kLoadColumn:
        // T1950: the fast evaluator's load_col bounds-checks the operand on
        // every use; the canonical row interpreter indexed unchecked, so a
        // program compiled for a wider schema (projection, residual filter)
        // read a raw out-of-range Value from Row::operator[].  Reject like
        // the AST ground truth would reject a missing column.
        if (instruction.operand >= row.Count()) {
          return StatusError(StatusCode::kInvalidArgument,
                             "bytecode kLoadColumn operand out of row range");
        }
        stack.push_back(row[instruction.operand]);
        break;
      case BytecodeOp::kLoadConstant:
        stack.push_back(constants_[instruction.operand]);
        break;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const Value& top = stack.back();
        const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                 ? (!top.IsNull() && !top.Truthy())
                                 : (!top.IsNull() && top.Truthy());
        if (decides) {
          // Match the AST: a short-circuit AND/OR evaluates to a normalized
          // BOOLEAN, never the raw truthy operand value.
          stack.back() = Value(instruction.opcode == BytecodeOp::kJumpIfTrue);
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        break;
      }
      case BytecodeOp::kJumpIfNotNull:
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (!stack.back().IsNull()) {
          pc += static_cast<size_t>(instruction.jump_target);
          --pc;
        }
        break;
      case BytecodeOp::kJump:
        pc += static_cast<size_t>(instruction.jump_target);
        --pc;
        break;
      case BytecodeOp::kBinaryInt64:
      case BytecodeOp::kBinaryDouble:
      case BytecodeOp::kBinaryVarchar:
      case BytecodeOp::kBinaryDate: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value right = std::move(stack.back());
        stack.pop_back();
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value left = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, combined,
                         (TryEvaluateBinary(instruction.binary, left, right)));
        stack.push_back(std::move(combined));
        break;
      }
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kUnaryDouble: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value child = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, result_value,
                         (TryEvaluateUnary(instruction.unary, child)));
        stack.push_back(std::move(result_value));
        break;
      }
      case BytecodeOp::kExtractDatePart: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value child = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, part_value,
                         TryExtractDatePart(child, instruction.operand));
        stack.push_back(std::move(part_value));
        break;
      }
      case BytecodeOp::kByteLength: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const auto& child = stack.back();
        const Value result_value =
            child.IsNull() ? Value()
                           : Value(static_cast<int64_t>(
                                 child.type == ValueType::kVarChar
                                     ? child.value.varchar_value.size()
                                     : child.AsString().size()));
        stack.back() = result_value;
        break;
      }
      case BytecodeOp::kCharLength: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const auto& child = stack.back();
        const Value result_value =
            child.IsNull() ? Value()
                           : Value(Utf8CodePointCount(
                                 child.type == ValueType::kVarChar
                                     ? child.value.varchar_value
                                     : std::string_view(child.AsString())));
        stack.back() = result_value;
        break;
      }
      case BytecodeOp::kAbs: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        ASSIGN_OR_RETURN(Value, abs_value, TryAbsValue(stack.back()));
        stack.back() = std::move(abs_value);
        break;
      }
      case BytecodeOp::kPop: {
        if (stack.size() <= instruction.operand) {
          return StatusError(StatusCode::kInvalidArgument,
                             "bytecode stack index out of range");
        }
        stack.erase(stack.end() - 1 - instruction.operand);
        break;
      }
      case BytecodeOp::kDup: {
        if (stack.size() <= instruction.operand) {
          return StatusError(StatusCode::kInvalidArgument,
                             "bytecode stack index out of range");
        }
        stack.push_back(stack[stack.size() - 1 - instruction.operand]);
        break;
      }
      case BytecodeOp::kCast: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value child = std::move(stack.back());
        stack.pop_back();
        const CastSpec& spec = casts_[instruction.operand];
        ASSIGN_OR_RETURN(
            Value, casted,
            TryCastValue(child, spec.target_type_name, spec.target_value_type,
                         spec.return_null_on_error));
        stack.push_back(std::move(casted));
        break;
      }
      case BytecodeOp::kInStringSet: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value child = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(Value, matched,
                         TryEvalInStringSetCanonical(
                             child, string_sets_[instruction.operand]));
        stack.push_back(std::move(matched));
        break;
      }
      case BytecodeOp::kSubstr: {
        Value len_v;
        const bool has_len = instruction.operand == 3;
        if (has_len) {
          if (stack.empty()) {
            return StatusError(StatusCode::kCorrupt,
                               "bytecode stack underflow");
          }
          len_v = std::move(stack.back());
          stack.pop_back();
        }
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value start_v = std::move(stack.back());
        stack.pop_back();
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        Value str_v = std::move(stack.back());
        stack.pop_back();
        ASSIGN_OR_RETURN(
            Value, sub_v,
            TryEvalSubstrCanonical(str_v, start_v, has_len ? &len_v : nullptr));
        stack.push_back(std::move(sub_v));
        break;
      }
      case BytecodeOp::kInt64InRange: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (instruction.operand >= int64_ranges_.size()) {
          return StatusError(StatusCode::kCorrupt,
                             "bytecode int64-range operand out of bounds");
        }
        ASSIGN_OR_RETURN(Value, in_range,
                         TryEvalInt64InRangeCanonical(
                             stack.back(), int64_ranges_[instruction.operand]));
        stack.back() = std::move(in_range);
        break;
      }
      case BytecodeOp::kDivInt64Const:
      case BytecodeOp::kModInt64Const: {
        if (stack.empty()) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (instruction.operand >= reciprocals_.size()) {
          return StatusError(StatusCode::kCorrupt,
                             "bytecode reciprocal operand out of bounds");
        }
        ASSIGN_OR_RETURN(Value, res,
                         TryEvalReciprocalDivModCanonical(
                             stack.back(), reciprocals_[instruction.operand],
                             instruction.opcode == BytecodeOp::kDivInt64Const));
        stack.back() = std::move(res);
        break;
      }
      case BytecodeOp::kRowInMask: {
        const RowInMaskSpec& spec = row_in_masks_[instruction.operand];
        const uint32_t k = spec.num_fields;
        if (k == 0 || stack.size() < k) {
          return StatusError(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const size_t base = stack.size() - k;
        ASSIGN_OR_RETURN(
            Value, res,
            TryEvalRowInMaskCanonical(
                std::span<const Value>(stack.data() + base, k), spec));
        stack.resize(base);
        stack.push_back(std::move(res));
        break;
      }
    }
  }
  if (stack.size() != 1) {
    return StatusError(StatusCode::kRuntimeError, "invalid bytecode stack");
  }
  return std::move(stack.back());
}

Value BytecodeProgram::EvaluateRow(const Row& row) const {
  return ExcShimUnwrap(TryEvaluateRow(row), "BytecodeProgram::EvaluateRow");
}

namespace {

// One boxing-free stack slot: the raw payload plus the runtime type.  Doubles
// ride in the same int64 payload through memcpy on push/pop.  VARCHAR
// operands carry a view into the row cell (or program constant) so string
// compares avoid Value construction; collated strings are rejected by
// FastSlotFromValue and fall back to the AST path.
struct FastSlot {
  int64_t bits{};
  ValueType type{ValueType::kNull};
  bool is_null{};
  std::string_view text;
  bool is_substring{false};
};

bool FastTruthy(const FastSlot& slot) {
  if (slot.is_null) {
    return false;
  }
  if (slot.type == ValueType::kInt64) {
    return slot.bits != 0;
  }
  if (slot.type == ValueType::kDouble) {
    double d = 0.0;
    std::memcpy(&d, &slot.bits, sizeof(d));
    return d != 0.0;
  }
  return true;
}

// Int64 operands promote to double on the double opcode (the compiler emits
// kBinaryDouble for mixed-operand shapes).
void FastSlotToDouble(const FastSlot& slot, double* out) {
  if (slot.type == ValueType::kDouble) {
    std::memcpy(out, &slot.bits, sizeof(double));
  } else {
    *out = static_cast<double>(slot.bits);
  }
}

FastSlot FastSlotFromValue(const Value& value, bool* unsupported) {
  FastSlot slot{
      .bits = 0, .type = value.type, .is_null = value.IsNull(), .text = {}};
  switch (value.type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      if (value.IsUnsigned()) {
        *unsupported = true;
        break;
      }
      slot.bits = value.value.int_value;
      break;
    case ValueType::kDouble:
      std::memcpy(&slot.bits, &value.value.double_value, sizeof(double));
      break;
    case ValueType::kVarChar:
      // A collated (case-insensitive) or interval-tagged string compares under
      // rules the plain view cannot reproduce; keep those on the canonical VM.
      if (value.Collation() != 0 || value.IsInterval()) {
        *unsupported = true;
        break;
      }
      slot.text = value.value.varchar_value;
      break;
    case ValueType::kNull:
      break;
    default:
      *unsupported = true;
      break;
  }
  return slot;
}

Value FastValueFromSlot(const FastSlot& slot) {
  if (slot.is_null) {
    return {};
  }
  switch (slot.type) {
    case ValueType::kInt64:
      // Value's numeric constructors are explicit, so the braced form the
      // check prefers does not compile here.
      return Value(slot.bits);  // NOLINT(modernize-return-braced-init-list)
    case ValueType::kDouble: {
      double d = 0.0;
      std::memcpy(&d, &slot.bits, sizeof(d));
      return Value(d);  // NOLINT(modernize-return-braced-init-list)
    }
    case ValueType::kDate:
      return Value::DateFromDays(slot.bits);
    case ValueType::kVarChar: {
      // Plain view-carrying value (collation 0): no owned copy.
      Value string_value;
      string_value.type = ValueType::kVarChar;
      string_value.value.varchar_value = slot.text;
      return string_value;
    }
    default:
      return {};
  }
}

using FastEvalStatus = BytecodeProgram::FastEval;

template <typename LoadColFn>
FastEvalStatus ExecuteFastProgram(
    const std::vector<BytecodeInstruction>& instructions,
    const std::vector<Value>& constants, const std::vector<CastSpec>& casts,
    const std::vector<StringSetSpec>& string_sets,
    const std::vector<Int64RangeSpec>& int64_ranges,
    const std::vector<ReciprocalDivModSpec>& reciprocals,
    const std::vector<RowInMaskSpec>& row_in_masks, LoadColFn&& load_col,
    FastSlot* out_slot, Status* error) {
  thread_local std::vector<FastSlot> stack;
  stack.clear();
  stack.reserve(instructions.size());
  const auto push_null = [](std::vector<FastSlot>& s) {
    s.push_back(FastSlot{
        .bits = 0, .type = ValueType::kNull, .is_null = true, .text = {}});
  };
  const auto push_int = [](std::vector<FastSlot>& s, int64_t v) {
    s.push_back(FastSlot{
        .bits = v, .type = ValueType::kInt64, .is_null = false, .text = {}});
  };
  const auto push_double = [](std::vector<FastSlot>& s, double v) {
    FastSlot slot{
        .bits = 0, .type = ValueType::kDouble, .is_null = false, .text = {}};
    std::memcpy(&slot.bits, &v, sizeof(v));
    s.push_back(slot);
  };
  const auto push_error = [error](StatusCode code, const char* message) {
    if (error != nullptr) {
      *error = StatusError(code, message);
    }
    return FastEvalStatus::kError;
  };
  const auto jump = [&](int32_t offset, size_t* pc) {
    if (offset <= 0 ||
        static_cast<size_t>(offset) > instructions.size() - *pc) {
      return false;
    }
    *pc += static_cast<size_t>(offset) - 1;
    return true;
  };
  for (size_t pc = 0; pc < instructions.size(); ++pc) {
    const BytecodeInstruction& instruction = instructions[pc];
    switch (instruction.opcode) {
      case BytecodeOp::kJumpIfNotNull:
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (!stack.back().is_null && !jump(instruction.jump_target, &pc)) {
          return push_error(StatusCode::kCorrupt, "invalid bytecode jump");
        }
        continue;
      case BytecodeOp::kJump:
        if (!jump(instruction.jump_target, &pc)) {
          return push_error(StatusCode::kCorrupt, "invalid bytecode jump");
        }
        continue;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const FastSlot& top = stack.back();
        const bool decides = instruction.opcode == BytecodeOp::kJumpIfFalse
                                 ? (!top.is_null && !FastTruthy(top))
                                 : (!top.is_null && FastTruthy(top));
        if (decides) {
          stack.back() = FastSlot{
              .bits = instruction.opcode == BytecodeOp::kJumpIfTrue ? 1 : 0,
              .type = ValueType::kInt64,
              .is_null = false,
              .text = {}};
          if (!jump(instruction.jump_target, &pc)) {
            return push_error(StatusCode::kCorrupt, "invalid bytecode jump");
          }
        }
        continue;
      }
      case BytecodeOp::kPop: {
        if (stack.size() <= instruction.operand) {
          return push_error(StatusCode::kInvalidArgument,
                            "bytecode stack index out of range");
        }
        stack.erase(stack.end() - 1 - instruction.operand);
        continue;
      }
      case BytecodeOp::kDup: {
        if (stack.size() <= instruction.operand) {
          return push_error(StatusCode::kInvalidArgument,
                            "bytecode stack index out of range");
        }
        stack.push_back(stack[stack.size() - 1 - instruction.operand]);
        continue;
      }
      case BytecodeOp::kLoadColumn: {
        FastSlot loaded{};
        if (!load_col(instruction.operand, &loaded)) {
          return FastEvalStatus::kUnsupported;
        }
        stack.push_back(loaded);
        break;
      }
      case BytecodeOp::kLoadConstant: {
        bool unsupported = false;
        stack.push_back(
            FastSlotFromValue(constants[instruction.operand], &unsupported));
        if (unsupported) {
          return FastEvalStatus::kUnsupported;
        }
        break;
      }
      case BytecodeOp::kBinaryInt64:
      case BytecodeOp::kBinaryDouble:
      case BytecodeOp::kBinaryVarchar:
      case BytecodeOp::kBinaryDate: {
        if (stack.size() < 2) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const FastSlot right = stack.back();
        stack.pop_back();
        const FastSlot left = stack.back();
        stack.pop_back();
        switch (instruction.binary) {
          case BinaryOperation::kAnd: {
            const bool lt = FastTruthy(left);
            const bool rt = FastTruthy(right);
            if ((!left.is_null && !lt) || (!right.is_null && !rt)) {
              push_int(stack, 0);
            } else if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, 1);
            }
            continue;
          }
          case BinaryOperation::kOr: {
            const bool lt = FastTruthy(left);
            const bool rt = FastTruthy(right);
            if ((!left.is_null && lt) || (!right.is_null && rt)) {
              push_int(stack, 1);
            } else if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, 0);
            }
            continue;
          }
          case BinaryOperation::kXor: {
            if (left.is_null || right.is_null) {
              push_null(stack);
            } else {
              push_int(stack, FastTruthy(left) != FastTruthy(right) ? 1 : 0);
            }
            continue;
          }
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom: {
            bool equal = false;
            if (left.is_null || right.is_null) {
              equal = left.is_null && right.is_null;
            } else if ((left.type != ValueType::kInt64 &&
                        left.type != ValueType::kDouble) ||
                       (right.type != ValueType::kInt64 &&
                        right.type != ValueType::kDouble)) {
              return FastEvalStatus::kUnsupported;
            } else if (left.type == ValueType::kDouble ||
                       right.type == ValueType::kDouble) {
              double l = 0.0;
              double r = 0.0;
              FastSlotToDouble(left, &l);
              FastSlotToDouble(right, &r);
              equal = l == r || (std::isnan(l) && std::isnan(r));
            } else {
              equal = left.bits == right.bits;
            }
            push_int(stack, static_cast<int64_t>(
                                instruction.binary ==
                                        BinaryOperation::kIsNotDistinctFrom
                                    ? equal
                                    : !equal));
            continue;
          }
          default:
            break;
        }
        if (left.is_null || right.is_null) {
          push_null(stack);
          continue;
        }
        if (instruction.opcode == BytecodeOp::kBinaryInt64) {
          if (left.type != ValueType::kInt64 ||
              right.type != ValueType::kInt64) {
            return FastEvalStatus::kUnsupported;
          }
          switch (instruction.binary) {
            case BinaryOperation::kEquals:
              push_int(stack, static_cast<int64_t>(left.bits == right.bits));
              continue;
            case BinaryOperation::kNotEquals:
              push_int(stack, static_cast<int64_t>(left.bits != right.bits));
              continue;
            case BinaryOperation::kLessThan:
              push_int(stack, static_cast<int64_t>(left.bits < right.bits));
              continue;
            case BinaryOperation::kLessThanEquals:
              push_int(stack, static_cast<int64_t>(left.bits <= right.bits));
              continue;
            case BinaryOperation::kGreaterThan:
              push_int(stack, static_cast<int64_t>(left.bits > right.bits));
              continue;
            case BinaryOperation::kGreaterThanEquals:
              push_int(stack, static_cast<int64_t>(left.bits >= right.bits));
              continue;
            case BinaryOperation::kAdd:
            case BinaryOperation::kSubtract:
            case BinaryOperation::kMultiply: {
              int64_t result = 0;
              bool overflow = false;
              if (instruction.binary == BinaryOperation::kAdd) {
                overflow =
                    __builtin_add_overflow(left.bits, right.bits, &result);
              } else if (instruction.binary == BinaryOperation::kSubtract) {
                overflow =
                    __builtin_sub_overflow(left.bits, right.bits, &result);
              } else {
                overflow =
                    __builtin_mul_overflow(left.bits, right.bits, &result);
              }
              if (overflow) {
                return push_error(
                    StatusCode::kInvalidArgument,
                    instruction.binary == BinaryOperation::kAdd
                        ? "integer overflow on '+'"
                    : instruction.binary == BinaryOperation::kSubtract
                        ? "integer overflow on '-'"
                        : "integer overflow on '*'");
              }
              push_int(stack, result);
              continue;
            }
            case BinaryOperation::kDivide: {
              const auto ld = static_cast<double>(left.bits);
              const auto rd = static_cast<double>(right.bits);
              if (rd == 0.0) {
                return push_error(StatusCode::kIsInfinity, "division by zero");
              }
              const double result = ld / rd;
              if (std::isfinite(ld) && std::isfinite(rd) &&
                  std::isinf(result)) {
                return push_error(StatusCode::kIsInfinity, "double overflow");
              }
              push_double(stack, result);
              continue;
            }
            case BinaryOperation::kModulo:
              if (right.bits == 0) {
                return push_error(StatusCode::kIsInfinity, "modulo by zero");
              }
              if (left.bits == std::numeric_limits<int64_t>::min() &&
                  right.bits == -1) {
                return push_error(StatusCode::kInvalidArgument,
                                  "integer overflow on '%'");
              }
              push_int(stack, left.bits % right.bits);
              continue;
            default:
              return FastEvalStatus::kUnsupported;
          }
        }
        if (instruction.opcode == BytecodeOp::kBinaryVarchar ||
            instruction.opcode == BytecodeOp::kBinaryDate) {
          StatusOr<Value> combined =
              TryEvaluateBinary(instruction.binary, FastValueFromSlot(left),
                                FastValueFromSlot(right));
          if (!combined.HasValue()) {
            *error = combined.GetStatus();
            return FastEvalStatus::kError;
          }
          bool result_unsupported = false;
          stack.push_back(
              FastSlotFromValue(combined.Value(), &result_unsupported));
          if (result_unsupported) {
            return FastEvalStatus::kUnsupported;
          }
          continue;
        }
        if ((left.type != ValueType::kInt64 &&
             left.type != ValueType::kDouble) ||
            (right.type != ValueType::kInt64 &&
             right.type != ValueType::kDouble)) {
          return FastEvalStatus::kUnsupported;
        }
        double l = 0.0;
        double r = 0.0;
        FastSlotToDouble(left, &l);
        FastSlotToDouble(right, &r);
        const bool l_nan = std::isnan(l);
        const bool r_nan = std::isnan(r);
        switch (instruction.binary) {
          case BinaryOperation::kEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l == r));
            continue;
          case BinaryOperation::kNotEquals:
            push_int(stack, l_nan || r_nan ? 1 : static_cast<int64_t>(l != r));
            continue;
          case BinaryOperation::kLessThan:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l < r));
            continue;
          case BinaryOperation::kLessThanEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l <= r));
            continue;
          case BinaryOperation::kGreaterThan:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l > r));
            continue;
          case BinaryOperation::kGreaterThanEquals:
            push_int(stack, l_nan || r_nan ? 0 : static_cast<int64_t>(l >= r));
            continue;
          case BinaryOperation::kAdd:
            push_double(stack, l + r);
            continue;
          case BinaryOperation::kSubtract:
            push_double(stack, l - r);
            continue;
          case BinaryOperation::kMultiply:
            push_double(stack, l * r);
            continue;
          case BinaryOperation::kDivide:
            if (r == 0.0) {
              return push_error(StatusCode::kIsInfinity, "division by zero");
            }
            {
              const double result = l / r;
              if (std::isfinite(l) && std::isfinite(r) && std::isinf(result)) {
                return push_error(StatusCode::kIsInfinity, "double overflow");
              }
              push_double(stack, result);
            }
            continue;
          case BinaryOperation::kModulo:
            if (left.type == ValueType::kDouble &&
                right.type == ValueType::kDouble) {
              return push_error(StatusCode::kInvalidArgument,
                                "unsupported binary operation");
            }
            if (r == 0.0) {
              return push_error(StatusCode::kIsInfinity, "division by zero");
            }
            push_double(stack, std::fmod(l, r));
            continue;
          default:
            return FastEvalStatus::kUnsupported;
        }
      }
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kUnaryDouble: {
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const FastSlot child = stack.back();
        stack.pop_back();
        switch (instruction.unary) {
          case UnaryOperation::kIsNull:
            push_int(stack, static_cast<int64_t>(child.is_null));
            continue;
          case UnaryOperation::kIsNotNull:
            push_int(stack, static_cast<int64_t>(!child.is_null));
            continue;
          case UnaryOperation::kIsTrue:
          case UnaryOperation::kIsNotTrue:
          case UnaryOperation::kIsFalse:
          case UnaryOperation::kIsNotFalse: {
            // Match TryEvaluateUnary's BOOL domain: NULL and INT64 0 or 1.
            if (!child.is_null && (child.type != ValueType::kInt64 ||
                                   child.bits < 0 || child.bits > 1)) {
              return push_error(
                  StatusCode::kInvalidArgument,
                  instruction.unary == UnaryOperation::kIsTrue ||
                          instruction.unary == UnaryOperation::kIsNotTrue
                      ? "IS TRUE requires a BOOL operand"
                      : "IS FALSE requires a BOOL operand");
            }
            const bool truthy = !child.is_null && FastTruthy(child);
            switch (instruction.unary) {
              case UnaryOperation::kIsTrue:
                push_int(stack, static_cast<int64_t>(truthy));
                break;
              case UnaryOperation::kIsNotTrue:
                push_int(stack, static_cast<int64_t>(!truthy));
                break;
              case UnaryOperation::kIsFalse:
                push_int(stack,
                         static_cast<int64_t>(child.is_null ? false : !truthy));
                break;
              default:  // kIsNotFalse
                push_int(stack, static_cast<int64_t>(child.is_null || truthy));
                break;
            }
            continue;
          }
          case UnaryOperation::kNot:
            if (child.is_null) {
              push_null(stack);
            } else {
              push_int(stack, static_cast<int64_t>(!FastTruthy(child)));
            }
            continue;
          case UnaryOperation::kMinus:
            if (child.is_null) {
              push_null(stack);
              continue;
            }
            if (child.type == ValueType::kDouble) {
              double d = 0.0;
              std::memcpy(&d, &child.bits, sizeof(d));
              push_double(stack, -d);
              continue;
            }
            if (child.type == ValueType::kInt64) {
              if (child.bits == std::numeric_limits<int64_t>::min()) {
                return push_error(StatusCode::kIsInfinity,
                                  "integer overflow in unary minus");
              }
              push_int(stack, -child.bits);
              continue;
            }
            return push_error(StatusCode::kInvalidArgument,
                              "unary minus requires a number");
          case UnaryOperation::kBitwiseNot:
            if (child.is_null) {
              push_null(stack);
              continue;
            }
            if (child.type != ValueType::kInt64) {
              return push_error(StatusCode::kInvalidArgument,
                                "bitwise not requires an integer");
            }
            push_int(stack,
                     static_cast<int64_t>(~static_cast<uint64_t>(child.bits)));
            continue;
          default:
            return FastEvalStatus::kUnsupported;
        }
      }
      case BytecodeOp::kExtractDatePart: {
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const FastSlot child = stack.back();
        stack.pop_back();
        if (child.is_null) {
          push_null(stack);
          continue;
        }
        if (child.type != ValueType::kDate) {
          return FastEvalStatus::kUnsupported;
        }
        if (child.bits < kDateMinDays || kDateMaxDays < child.bits) {
          return push_error(StatusCode::kInvalidArgument,
                            "DATE value out of range");
        }
        const std::chrono::year_month_day ymd{std::chrono::sys_days{
            std::chrono::days{static_cast<int>(child.bits)}}};
        if (instruction.operand == 0) {
          push_int(stack, static_cast<int64_t>(static_cast<int>(ymd.year())));
        } else if (instruction.operand == 1) {
          push_int(stack,
                   static_cast<int64_t>(static_cast<unsigned>(ymd.month())));
        } else {
          push_int(stack,
                   static_cast<int64_t>(static_cast<unsigned>(ymd.day())));
        }
        continue;
      }
      case BytecodeOp::kCast: {
        const CastSpec& spec = casts[instruction.operand];
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        FastSlot& top = stack.back();
        if (top.is_null) {
          continue;
        }
        if (spec.target_value_type == ValueType::kDate &&
            top.type == ValueType::kDate) {
          continue;
        }
        StatusOr<Value> casted =
            TryCastValue(FastValueFromSlot(top), spec.target_type_name,
                         spec.target_value_type, spec.return_null_on_error);
        if (!casted.HasValue()) {
          *error = casted.GetStatus();
          return FastEvalStatus::kError;
        }
        if (casted.Value().type == ValueType::kVarChar) {
          return FastEvalStatus::kUnsupported;
        }
        bool unsupported = false;
        top = FastSlotFromValue(casted.Value(), &unsupported);
        if (unsupported) {
          return FastEvalStatus::kUnsupported;
        }
        continue;
      }
      case BytecodeOp::kAbs: {
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        FastSlot& top = stack.back();
        if (top.is_null) {
          continue;
        }
        if (top.type == ValueType::kInt64) {
          if (top.bits == std::numeric_limits<int64_t>::min()) {
            return push_error(StatusCode::kIsInfinity,
                              "integer overflow in ABS");
          }
          top.bits = std::abs(top.bits);
          continue;
        }
        if (top.type == ValueType::kDouble) {
          double d = 0.0;
          std::memcpy(&d, &top.bits, sizeof(d));
          d = std::fabs(d);
          std::memcpy(&top.bits, &d, sizeof(d));
          continue;
        }
        return push_error(StatusCode::kInvalidArgument,
                          "ABS requires numeric argument");
      }
      case BytecodeOp::kInStringSet: {
        // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
        //   (in_const_set_3vl_sound)
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const FastSlot child = stack.back();
        stack.pop_back();
        const StringSetSpec& spec = string_sets[instruction.operand];
        if (!spec.non_empty) {
          push_int(stack, 0);
          continue;
        }
        if (child.is_null) {
          push_null(stack);
          continue;
        }
        if (child.type != ValueType::kVarChar) {
          return FastEvalStatus::kUnsupported;
        }
        if (spec.Contains(child.text)) {
          push_int(stack, 1);
        } else if (spec.has_null) {
          push_null(stack);
        } else {
          push_int(stack, 0);
        }
        continue;
      }
      case BytecodeOp::kSubstr: {
        // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
        //   (ascii_substr_1based_slice_sound)
        const bool has_len = instruction.operand == 3;
        if (stack.size() < (has_len ? 3 : 2)) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        FastSlot len_s{};
        if (has_len) {
          len_s = stack.back();
          stack.pop_back();
        }
        const FastSlot start_s = stack.back();
        stack.pop_back();
        const FastSlot str_s = stack.back();
        stack.pop_back();
        if (str_s.is_null || start_s.is_null || (has_len && len_s.is_null)) {
          push_null(stack);
          continue;
        }
        if (str_s.type != ValueType::kVarChar ||
            start_s.type != ValueType::kInt64 ||
            (has_len && len_s.type != ValueType::kInt64)) {
          return FastEvalStatus::kUnsupported;
        }
        const std::optional<int64_t> opt_len =
            has_len ? std::optional<int64_t>(len_s.bits) : std::nullopt;
        StatusOr<std::string_view> sub_or =
            TryEvalSubstrView(str_s.text, start_s.bits, opt_len);
        if (!sub_or.HasValue()) {
          *error = sub_or.GetStatus();
          return FastEvalStatus::kError;
        }
        stack.push_back(FastSlot{
            .bits = 0,
            .type = ValueType::kVarChar,
            .is_null = false,
            .text = sub_or.Value(),
            .is_substring = true,
        });
        continue;
      }
      case BytecodeOp::kInt64InRange: {
        // Lean proof: lean/TinylambOpt/SimdInRange.lean
        //   (simd_in_range_sound, int64_range_lowering)
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (instruction.operand >= int64_ranges.size()) {
          return push_error(StatusCode::kCorrupt,
                            "bytecode int64-range operand out of bounds");
        }
        FastSlot& top = stack.back();
        if (top.is_null) {
          continue;
        }
        const Int64RangeSpec& spec = int64_ranges[instruction.operand];
        if (top.type != spec.expected_type) {
          return FastEvalStatus::kUnsupported;
        }
        top.bits = static_cast<int64_t>(spec.Contains(top.bits));
        top.type = ValueType::kInt64;
        continue;
      }
      case BytecodeOp::kDivInt64Const:
      case BytecodeOp::kModInt64Const: {
        // Lean proof: lean/TinylambOpt/ResearchProofs/ReciprocalDivMod.lean
        //   (est_bounds, divModConst_sound)
        if (stack.empty()) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        if (instruction.operand >= reciprocals.size()) {
          return push_error(StatusCode::kCorrupt,
                            "bytecode reciprocal operand out of bounds");
        }
        FastSlot& top = stack.back();
        if (top.is_null) {
          continue;
        }
        if (top.type != ValueType::kInt64) {
          return FastEvalStatus::kUnsupported;
        }
        const ReciprocalDivModSpec& spec = reciprocals[instruction.operand];
        top.bits = instruction.opcode == BytecodeOp::kDivInt64Const
                       ? spec.Div(top.bits)
                       : spec.Mod(top.bits);
        continue;
      }
      case BytecodeOp::kRowInMask: {
        // Lean proof: lean/TinylambOpt/ResearchProofs/RowInMask.lean (sound)
        const RowInMaskSpec& spec = row_in_masks[instruction.operand];
        const uint32_t k = spec.num_fields;
        if (k == 0 || stack.size() < k) {
          return push_error(StatusCode::kCorrupt, "bytecode stack underflow");
        }
        const size_t base = stack.size() - k;
        for (uint32_t c = 0; c < k; ++c) {
          const FastSlot& s = stack[base + c];
          if (!s.is_null && s.type != ValueType::kInt64) {
            return FastEvalStatus::kUnsupported;
          }
        }
        uint64_t any_sure = 0;
        uint64_t any_possible = 0;
        for (const RowInMaskSpec::Chunk& chunk : spec.chunks) {
          uint64_t sure = chunk.enabled;
          uint64_t possible = chunk.enabled;
          for (uint32_t c = 0; c < k; ++c) {
            const FastSlot& s = stack[base + c];
            const RowInMaskSpec::Column& col = chunk.columns[c];
            if (s.is_null) {
              sure = 0;
            } else {
              const int64_t xv = s.bits;
              uint64_t eq = 0;
              const uint32_t cnt = chunk.count;
              for (uint32_t i = 0; i < cnt; ++i) {
                eq |= static_cast<uint64_t>(col.values[i] == xv) << i;
              }
              eq &= col.non_null_mask;
              sure &= eq;
              possible &= (col.null_mask | eq);
              if (possible == 0) {
                break;
              }
            }
          }
          any_sure |= sure;
          any_possible |= possible;
          if (any_sure != 0) {
            break;
          }
        }
        stack.resize(base);
        if (any_sure != 0) {
          push_int(stack, 1);
        } else if (any_possible != 0) {
          push_null(stack);
        } else {
          push_int(stack, 0);
        }
        continue;
      }
      default:
        return FastEvalStatus::kUnsupported;
    }
  }
  if (stack.size() != 1) {
    return push_error(StatusCode::kRuntimeError, "invalid bytecode stack");
  }
  *out_slot = stack.back();
  return FastEvalStatus::kDone;
}

}  // namespace

BytecodeProgram::FastEval BytecodeProgram::TryEvaluateRowFast(
    const Row& row, Value* out, Status* error) const {
  if (!fast_interpretable_) {
    return FastEval::kUnsupported;
  }
  // Column projections and NULL tests need no VM stack or payload decoding.
  // Preserve the actual Value domain, including unsigned integers and arrays.
  if (!instructions_.empty() && instructions_.size() <= 3 &&
      instructions_[0].opcode == BytecodeOp::kLoadColumn &&
      instructions_[0].operand < row.Count()) {
    const Value& value = row[instructions_[0].operand];
    if (instructions_.size() == 1) {
      *out = value;
      return FastEval::kDone;
    }
    const auto& test = instructions_[1];
    const bool negate = instructions_.size() == 3 &&
                        instructions_[2].opcode == BytecodeOp::kUnaryInt64 &&
                        instructions_[2].unary == UnaryOperation::kNot;
    if ((instructions_.size() == 2 || negate) &&
        test.opcode == BytecodeOp::kUnaryInt64 &&
        (test.unary == UnaryOperation::kIsNull ||
         test.unary == UnaryOperation::kIsNotNull)) {
      const bool result = test.unary == UnaryOperation::kIsNull
                              ? value.IsNull()
                              : !value.IsNull();
      *out = Value(static_cast<int64_t>(result != negate));
      return FastEval::kDone;
    }
  }
  FastSlot slot{};
  const auto load_col = [&](uint16_t operand, FastSlot* dst) {
    if (operand >= row.Count()) {
      return false;
    }
    bool unsupported = false;
    *dst = FastSlotFromValue(row[operand], &unsupported);
    return !unsupported;
  };
  const FastEval status = ExecuteFastProgram(
      instructions_, constants_, casts_, string_sets_, int64_ranges_,
      reciprocals_, row_in_masks_, load_col, &slot, error);
  if (status == FastEval::kDone) {
    if (slot.is_substring && !slot.is_null &&
        slot.type == ValueType::kVarChar) {
      *out = Value(std::string(slot.text));
    } else {
      *out = FastValueFromSlot(slot);
    }
  }
  return status;
}

BytecodeProgram::FastEval BytecodeProgram::TryEvaluateColumnarBatch(
    std::span<const BytecodeColumnView> cols, const uint32_t* sel,
    size_t row_count, BytecodeColumnSink* sink, Status* error) const {
  if (row_count == 0) {
    return FastEval::kDone;
  }
  if (!fast_interpretable_ || sink == nullptr) {
    return FastEval::kUnsupported;
  }
  const size_t orig_nulls = sink->nulls != nullptr ? sink->nulls->size() : 0;
  const size_t orig_ints = sink->ints != nullptr ? sink->ints->size() : 0;
  const size_t orig_doubles =
      sink->doubles != nullptr ? sink->doubles->size() : 0;
  const size_t orig_strings =
      sink->strings != nullptr ? sink->strings->size() : 0;
  const size_t orig_fallback =
      sink->fallback != nullptr ? sink->fallback->size() : 0;
  const ValueType orig_type = sink->type;
  const bool orig_uniform = sink->is_uniform;
  const bool orig_has_null = sink->has_null;
  const bool orig_borrowed = sink->may_borrow_constant_strings;
  size_t sink_rows = sink->initial_rows;

  const auto rollback = [&]() {
    sink->type = orig_type;
    sink->is_uniform = orig_uniform;
    sink->has_null = orig_has_null;
    sink->may_borrow_constant_strings = orig_borrowed;
    if (sink->nulls) {
      sink->nulls->resize(orig_nulls);
    }
    if (sink->ints) {
      sink->ints->resize(orig_ints);
    }
    if (sink->doubles) {
      sink->doubles->resize(orig_doubles);
    }
    if (sink->strings) {
      sink->strings->resize(orig_strings);
    }
    if (sink->fallback) {
      sink->fallback->resize(orig_fallback);
    }
  };

  // SIMD-friendly tight loop for 3-instruction scalar DOUBLE binary op
  // (`col op col`, `col op const`, `const op col`) without NULLs or errors.
  if (instructions_.size() == 3 &&
      instructions_[2].opcode == BytecodeOp::kBinaryDouble &&
      (instructions_[2].binary == BinaryOperation::kAdd ||
       instructions_[2].binary == BinaryOperation::kSubtract ||
       instructions_[2].binary == BinaryOperation::kMultiply ||
       instructions_[2].binary == BinaryOperation::kDivide) &&
      sink->is_uniform && sink->doubles != nullptr &&
      (sink->type == ValueType::kDouble ||
       (sink->type == ValueType::kNull && sink_rows == 0))) {
    const auto is_fast_num_col = [&](uint16_t c) noexcept {
      if (c >= cols.size()) {
        return false;
      }
      const BytecodeColumnView& v = cols[c];
      return v.is_uniform && !v.is_unsigned && !v.has_null &&
             ((v.type == ValueType::kInt64 && v.ints != nullptr) ||
              (v.type == ValueType::kDouble &&
               (v.doubles != nullptr || v.raw_double_bits != nullptr)));
    };
    const auto is_fast_num_const = [&](uint16_t k, double* out_d) noexcept {
      const Value& cv = constants_[k];
      if (cv.IsNull() || cv.IsUnsigned()) {
        return false;
      }
      if (cv.type == ValueType::kDouble) {
        *out_d = cv.value.double_value;
        return true;
      }
      if (cv.type == ValueType::kInt64) {
        *out_d = static_cast<double>(cv.value.int_value);
        return true;
      }
      return false;
    };
    const auto read_num = [](const BytecodeColumnView& v,
                             size_t r) noexcept -> double {
      if (v.type == ValueType::kInt64) {
        return static_cast<double>(v.ints[r]);
      }
      return v.doubles != nullptr ? v.doubles[r]
                                  : std::bit_cast<double>(v.raw_double_bits[r]);
    };
    const BinaryOperation bop = instructions_[2].binary;
    const auto run_double_binop = [&](auto get_l, auto get_r) -> FastEval {
      sink->type = ValueType::kDouble;
      if (sink->has_null && sink->nulls) {
        sink->nulls->resize(orig_nulls + row_count, 0);
      }
      const size_t base = sink->doubles->size();
      sink->doubles->resize(base + row_count);
      double* __restrict dst = sink->doubles->data() + base;
      const auto run_simple = [&](auto op) {
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            const uint32_t idx = sel[i];
            dst[i] = op(get_l(idx), get_r(idx));
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = op(get_l(i), get_r(i));
          }
        }
      };
      switch (bop) {
        case BinaryOperation::kMultiply:
          run_simple([](double l, double r) noexcept { return l * r; });
          return FastEval::kDone;
        case BinaryOperation::kAdd:
          run_simple([](double l, double r) noexcept { return l + r; });
          return FastEval::kDone;
        case BinaryOperation::kSubtract:
          run_simple([](double l, double r) noexcept { return l - r; });
          return FastEval::kDone;
        case BinaryOperation::kDivide: {
          uint8_t any_bad = 0;
          if (sel != nullptr) {
            for (size_t i = 0; i < row_count; ++i) {
              const uint32_t idx = sel[i];
              const double l = get_l(idx);
              const double r = get_r(idx);
              const double res = l / r;
              dst[i] = res;
              any_bad |= static_cast<uint8_t>(
                  r == 0.0 ||
                  (std::isfinite(l) && std::isfinite(r) && std::isinf(res)));
            }
          } else {
            for (size_t i = 0; i < row_count; ++i) {
              const double l = get_l(i);
              const double r = get_r(i);
              const double res = l / r;
              dst[i] = res;
              any_bad |= static_cast<uint8_t>(
                  r == 0.0 ||
                  (std::isfinite(l) && std::isfinite(r) && std::isinf(res)));
            }
          }
          if (any_bad != 0) [[unlikely]] {
            for (size_t i = 0; i < row_count; ++i) {
              const size_t idx = sel != nullptr ? sel[i] : i;
              const double l = get_l(idx);
              const double r = get_r(idx);
              if (r == 0.0) {
                rollback();
                if (error != nullptr) {
                  *error =
                      StatusError(StatusCode::kIsInfinity, "division by zero");
                }
                return FastEval::kError;
              }
              if (std::isfinite(l) && std::isfinite(r) && std::isinf(dst[i])) {
                rollback();
                if (error != nullptr) {
                  *error =
                      StatusError(StatusCode::kIsInfinity, "double overflow");
                }
                return FastEval::kError;
              }
            }
          }
          return FastEval::kDone;
        }
        default:
          rollback();
          return FastEval::kUnsupported;
      }
    };
    const BytecodeInstruction& i0 = instructions_[0];
    const BytecodeInstruction& i1 = instructions_[1];
    double c0 = 0.0;
    double c1 = 0.0;
    if (i0.opcode == BytecodeOp::kLoadColumn &&
        i1.opcode == BytecodeOp::kLoadColumn && is_fast_num_col(i0.operand) &&
        is_fast_num_col(i1.operand)) {
      const BytecodeColumnView lv = cols[i0.operand];
      const BytecodeColumnView rv = cols[i1.operand];
      return run_double_binop(
          [&](size_t idx) noexcept { return read_num(lv, idx); },
          [&](size_t idx) noexcept { return read_num(rv, idx); });
    }
    if (i0.opcode == BytecodeOp::kLoadColumn &&
        i1.opcode == BytecodeOp::kLoadConstant && is_fast_num_col(i0.operand) &&
        is_fast_num_const(i1.operand, &c1)) {
      const BytecodeColumnView lv = cols[i0.operand];
      return run_double_binop(
          [&](size_t idx) noexcept { return read_num(lv, idx); },
          [&](size_t) noexcept { return c1; });
    }
    if (i0.opcode == BytecodeOp::kLoadConstant &&
        i1.opcode == BytecodeOp::kLoadColumn &&
        is_fast_num_const(i0.operand, &c0) && is_fast_num_col(i1.operand)) {
      const BytecodeColumnView rv = cols[i1.operand];
      return run_double_binop(
          [&](size_t) noexcept { return c0; },
          [&](size_t idx) noexcept { return read_num(rv, idx); });
    }
  }

  // Tight loop for the 3-instruction scalar INT64 binary op (`col op col`,
  // `col op const`, `const op col`) over NULL-free signed columns: + - * with
  // overflow detection, and the six comparisons (0/1 INT64 result).  Anything
  // else (division, modulo, NULLs, unsigned operands) keeps the generic path.
  if (instructions_.size() == 3 &&
      instructions_[2].opcode == BytecodeOp::kBinaryInt64 && sink->is_uniform &&
      sink->ints != nullptr && !sink->is_unsigned && !sink->is_interval &&
      (sink->type == ValueType::kInt64 ||
       (sink->type == ValueType::kNull && sink_rows == 0))) {
    const BinaryOperation bop = instructions_[2].binary;
    const bool is_arith = bop == BinaryOperation::kAdd ||
                          bop == BinaryOperation::kSubtract ||
                          bop == BinaryOperation::kMultiply;
    const bool is_cmp = bop == BinaryOperation::kEquals ||
                        bop == BinaryOperation::kNotEquals ||
                        bop == BinaryOperation::kLessThan ||
                        bop == BinaryOperation::kLessThanEquals ||
                        bop == BinaryOperation::kGreaterThan ||
                        bop == BinaryOperation::kGreaterThanEquals;
    const auto int_col = [&](uint16_t c) noexcept -> const int64_t* {
      if (c >= cols.size()) {
        return nullptr;
      }
      const BytecodeColumnView& v = cols[c];
      if (!v.is_uniform || v.is_unsigned || v.has_null ||
          v.type != ValueType::kInt64) {
        return nullptr;
      }
      return v.ints;
    };
    const auto int_const = [&](uint16_t k, int64_t* out_v) noexcept {
      const Value& cv = constants_[k];
      if (cv.IsNull() || cv.IsUnsigned() || cv.type != ValueType::kInt64) {
        return false;
      }
      *out_v = cv.value.int_value;
      return true;
    };
    const auto run_int_binop = [&](auto get_l, auto get_r) -> FastEval {
      sink->type = ValueType::kInt64;
      if (sink->has_null && sink->nulls) {
        sink->nulls->resize(orig_nulls + row_count, 0);
      }
      const size_t base = sink->ints->size();
      sink->ints->resize(base + row_count);
      int64_t* __restrict dst = sink->ints->data() + base;
      const auto run_cmp = [&](auto op) {
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            const uint32_t idx = sel[i];
            dst[i] = static_cast<int64_t>(op(get_l(idx), get_r(idx)));
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = static_cast<int64_t>(op(get_l(i), get_r(i)));
          }
        }
      };
      const auto run_arith = [&](auto op, const char* message) -> FastEval {
        uint8_t overflow = 0;
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            const uint32_t idx = sel[i];
            int64_t res = 0;
            overflow |= static_cast<uint8_t>(op(get_l(idx), get_r(idx), &res));
            dst[i] = res;
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            int64_t res = 0;
            overflow |= static_cast<uint8_t>(op(get_l(i), get_r(i), &res));
            dst[i] = res;
          }
        }
        if (overflow != 0) [[unlikely]] {
          rollback();
          if (error != nullptr) {
            *error = StatusError(StatusCode::kInvalidArgument, message);
          }
          return FastEval::kError;
        }
        return FastEval::kDone;
      };
      switch (bop) {
        case BinaryOperation::kAdd:
          return run_arith(
              [](int64_t l, int64_t r, int64_t* o) noexcept {
                return __builtin_add_overflow(l, r, o);
              },
              "integer overflow on '+'");
        case BinaryOperation::kSubtract:
          return run_arith(
              [](int64_t l, int64_t r, int64_t* o) noexcept {
                return __builtin_sub_overflow(l, r, o);
              },
              "integer overflow on '-'");
        case BinaryOperation::kMultiply:
          return run_arith(
              [](int64_t l, int64_t r, int64_t* o) noexcept {
                return __builtin_mul_overflow(l, r, o);
              },
              "integer overflow on '*'");
        case BinaryOperation::kEquals:
          run_cmp([](int64_t l, int64_t r) noexcept { return l == r; });
          return FastEval::kDone;
        case BinaryOperation::kNotEquals:
          run_cmp([](int64_t l, int64_t r) noexcept { return l != r; });
          return FastEval::kDone;
        case BinaryOperation::kLessThan:
          run_cmp([](int64_t l, int64_t r) noexcept { return l < r; });
          return FastEval::kDone;
        case BinaryOperation::kLessThanEquals:
          run_cmp([](int64_t l, int64_t r) noexcept { return l <= r; });
          return FastEval::kDone;
        case BinaryOperation::kGreaterThan:
          run_cmp([](int64_t l, int64_t r) noexcept { return l > r; });
          return FastEval::kDone;
        default:
          run_cmp([](int64_t l, int64_t r) noexcept { return l >= r; });
          return FastEval::kDone;
      }
    };
    if (is_arith || is_cmp) {
      const BytecodeInstruction& i0 = instructions_[0];
      const BytecodeInstruction& i1 = instructions_[1];
      int64_t c0 = 0;
      int64_t c1 = 0;
      if (i0.opcode == BytecodeOp::kLoadColumn &&
          i1.opcode == BytecodeOp::kLoadColumn) {
        const int64_t* lp = int_col(i0.operand);
        const int64_t* rp = int_col(i1.operand);
        if (lp != nullptr && rp != nullptr) {
          return run_int_binop([lp](size_t idx) noexcept { return lp[idx]; },
                               [rp](size_t idx) noexcept { return rp[idx]; });
        }
      } else if (i0.opcode == BytecodeOp::kLoadColumn &&
                 i1.opcode == BytecodeOp::kLoadConstant) {
        const int64_t* lp = int_col(i0.operand);
        if (lp != nullptr && int_const(i1.operand, &c1)) {
          return run_int_binop([lp](size_t idx) noexcept { return lp[idx]; },
                               [c1](size_t) noexcept { return c1; });
        }
      } else if (i0.opcode == BytecodeOp::kLoadConstant &&
                 i1.opcode == BytecodeOp::kLoadColumn) {
        const int64_t* rp = int_col(i1.operand);
        if (rp != nullptr && int_const(i0.operand, &c0)) {
          return run_int_binop([c0](size_t) noexcept { return c0; },
                               [rp](size_t idx) noexcept { return rp[idx]; });
        }
      }
    }
  }

  // Tight SIMD-friendly loops for fused 2-instruction unary/specialized ops
  // (`kInt64InRange`, `kDivInt64Const`, `kModInt64Const`) over NULL-free
  // uniform columns.
  if (instructions_.size() == 2 &&
      instructions_[0].opcode == BytecodeOp::kLoadColumn &&
      instructions_[0].operand < cols.size() && sink->is_uniform &&
      sink->ints != nullptr && !sink->is_unsigned && !sink->is_interval &&
      (sink->type == ValueType::kInt64 ||
       (sink->type == ValueType::kNull && sink_rows == 0))) {
    const BytecodeColumnView& v = cols[instructions_[0].operand];
    if (instructions_[1].opcode == BytecodeOp::kInt64InRange &&
        instructions_[1].operand < int64_ranges_.size() && v.is_uniform &&
        !v.is_unsigned && !v.has_null && v.ints != nullptr) {
      const Int64RangeSpec& spec = int64_ranges_[instructions_[1].operand];
      if (v.type == spec.expected_type) {
        sink->type = ValueType::kInt64;
        if (sink->has_null && (sink->nulls != nullptr)) {
          sink->nulls->resize(orig_nulls + row_count, 0);
        }
        const size_t base = sink->ints->size();
        sink->ints->resize(base + row_count);
        int64_t* __restrict dst = sink->ints->data() + base;
        const int64_t* __restrict src = v.ints;
        const auto ulo = static_cast<uint64_t>(spec.lo);
        const uint64_t uwidth = static_cast<uint64_t>(spec.hi) - ulo;
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            const uint32_t idx = sel[i];
            dst[i] = static_cast<int64_t>(
                (static_cast<uint64_t>(src[idx]) - ulo) <= uwidth);
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = static_cast<int64_t>(
                (static_cast<uint64_t>(src[i]) - ulo) <= uwidth);
          }
        }
        return FastEval::kDone;
      }
    } else if ((instructions_[1].opcode == BytecodeOp::kDivInt64Const ||
                instructions_[1].opcode == BytecodeOp::kModInt64Const) &&
               instructions_[1].operand < reciprocals_.size() &&
               v.is_uniform && !v.is_unsigned && !v.has_null &&
               v.type == ValueType::kInt64 && v.ints != nullptr) {
      const ReciprocalDivModSpec& spec = reciprocals_[instructions_[1].operand];
      const bool is_div = instructions_[1].opcode == BytecodeOp::kDivInt64Const;
      sink->type = ValueType::kInt64;
      if (sink->has_null && (sink->nulls != nullptr)) {
        sink->nulls->resize(orig_nulls + row_count, 0);
      }
      const size_t base = sink->ints->size();
      sink->ints->resize(base + row_count);
      int64_t* __restrict dst = sink->ints->data() + base;
      const int64_t* __restrict src = v.ints;
      if (is_div) {
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = spec.Div(src[sel[i]]);
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = spec.Div(src[i]);
          }
        }
      } else {
        if (sel != nullptr) {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = spec.Mod(src[sel[i]]);
          }
        } else {
          for (size_t i = 0; i < row_count; ++i) {
            dst[i] = spec.Mod(src[i]);
          }
        }
      }
      return FastEval::kDone;
    }
  }

  const auto demote_sink_to_fallback = [&]() {
    if (!sink->is_uniform || sink->fallback == nullptr) {
      return false;
    }
    const size_t n = sink_rows;
    std::vector<Value> out;
    out.reserve(n + row_count);
    for (size_t k = 0; k < n; ++k) {
      if (sink->type == ValueType::kNull ||
          (sink->has_null && sink->nulls && (*sink->nulls)[k] != 0)) {
        out.emplace_back();
        continue;
      }
      switch (sink->type) {
        case ValueType::kInt64:
          if (sink->ints == nullptr) {
            return false;
          }
          out.emplace_back((*sink->ints)[k]);
          if (sink->is_unsigned) {
            out.back().SetUnsigned();
          }
          break;
        case ValueType::kDate:
          if (sink->ints == nullptr) {
            return false;
          }
          out.push_back(Value::DateFromDays((*sink->ints)[k]));
          break;
        case ValueType::kDouble:
          if (sink->doubles == nullptr) {
            return false;
          }
          out.emplace_back((*sink->doubles)[k]);
          break;
        case ValueType::kVarChar: {
          if (sink->strings == nullptr) {
            return false;
          }
          Value sv;
          sv.CopyFromStringView((*sink->strings)[k]);
          if (sink->is_interval) {
            sv.SetInterval();
          }
          out.push_back(std::move(sv));
          break;
        }
        default:
          out.emplace_back();
          break;
      }
    }
    sink->is_uniform = false;
    sink->has_null = false;
    if (sink->nulls) {
      std::vector<uint8_t>().swap(*sink->nulls);
    }
    if (sink->ints) {
      std::vector<int64_t>().swap(*sink->ints);
    }
    if (sink->doubles) {
      std::vector<double>().swap(*sink->doubles);
    }
    if (sink->strings) {
      std::vector<std::string_view>().swap(*sink->strings);
    }
    *sink->fallback = std::move(out);
    return true;
  };

  size_t current_row = 0;
  const auto load_col = [&](uint16_t operand, FastSlot* dst) {
    if (operand >= cols.size()) {
      return false;
    }
    const BytecodeColumnView& view = cols[operand];
    const size_t r = current_row;
    if (!view.is_uniform) {
      if (view.fallback == nullptr) {
        return false;
      }
      bool unsupported = false;
      *dst = FastSlotFromValue(view.fallback[r], &unsupported);
      return !unsupported;
    }
    if (view.type == ValueType::kNull ||
        (view.has_null && view.nulls != nullptr && view.nulls[r] != 0)) {
      *dst = FastSlot{
          .bits = 0, .type = ValueType::kNull, .is_null = true, .text = {}};
      return true;
    }
    switch (view.type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        if (view.is_unsigned || view.ints == nullptr) {
          return false;
        }
        *dst = FastSlot{.bits = view.ints[r],
                        .type = view.type,
                        .is_null = false,
                        .text = {}};
        return true;
      case ValueType::kDouble:
        if (view.raw_double_bits != nullptr) {
          *dst = FastSlot{.bits = view.raw_double_bits[r],
                          .type = ValueType::kDouble,
                          .is_null = false,
                          .text = {}};
          return true;
        }
        if (view.doubles == nullptr) {
          return false;
        }
        *dst = FastSlot{.bits = std::bit_cast<int64_t>(view.doubles[r]),
                        .type = ValueType::kDouble,
                        .is_null = false,
                        .text = {}};
        return true;
      case ValueType::kVarChar:
        if (view.is_interval || view.strings == nullptr) {
          return false;
        }
        *dst = FastSlot{.bits = 0,
                        .type = ValueType::kVarChar,
                        .is_null = false,
                        .text = view.strings[r]};
        return true;
      default:
        return false;
    }
  };

  for (size_t i = 0; i < row_count; ++i) {
    current_row = sel != nullptr ? sel[i] : i;
    FastSlot slot{};
    const FastEval st = ExecuteFastProgram(
        instructions_, constants_, casts_, string_sets_, int64_ranges_,
        reciprocals_, row_in_masks_, load_col, &slot, error);
    if (st != FastEval::kDone) {
      rollback();
      return st;
    }
    if (!sink->is_uniform) {
      if (sink->fallback == nullptr) {
        rollback();
        return FastEval::kUnsupported;
      }
      sink->fallback->push_back(FastValueFromSlot(slot));
      ++sink_rows;
      continue;
    }
    if (slot.is_null) {
      if (sink->type != ValueType::kNull) {
        if (!sink->has_null) {
          if (sink->nulls == nullptr) {
            rollback();
            return FastEval::kUnsupported;
          }
          sink->has_null = true;
          sink->nulls->assign(sink_rows, 0);
        }
        if (sink->nulls == nullptr) {
          rollback();
          return FastEval::kUnsupported;
        }
        sink->nulls->push_back(1);
        switch (sink->type) {
          case ValueType::kInt64:
          case ValueType::kDate:
            if (sink->ints == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->ints->push_back(0);
            break;
          case ValueType::kDouble:
            if (sink->doubles == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->doubles->push_back(0.0);
            break;
          case ValueType::kVarChar:
            if (sink->strings == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->strings->emplace_back();
            break;
          default:
            break;
        }
      }
      ++sink_rows;
      continue;
    }
    if (sink->type == ValueType::kNull) {
      if (slot.type == ValueType::kArray) {
        rollback();
        return FastEval::kUnsupported;
      }
      sink->type = slot.type;
      if (sink_rows > 0) {
        if (sink->nulls == nullptr) {
          rollback();
          return FastEval::kUnsupported;
        }
        sink->has_null = true;
        sink->nulls->assign(sink_rows, 1);
        switch (sink->type) {
          case ValueType::kInt64:
          case ValueType::kDate:
            if (sink->ints == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->ints->assign(sink_rows, 0);
            break;
          case ValueType::kDouble:
            if (sink->doubles == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->doubles->assign(sink_rows, 0.0);
            break;
          case ValueType::kVarChar:
            if (sink->strings == nullptr) {
              rollback();
              return FastEval::kUnsupported;
            }
            sink->strings->assign(sink_rows, std::string_view{});
            break;
          default:
            break;
        }
      }
    } else if (slot.type != sink->type ||
               (sink->is_unsigned && slot.type == ValueType::kInt64) ||
               (sink->is_interval && slot.type == ValueType::kVarChar)) {
      if (!demote_sink_to_fallback()) {
        rollback();
        return FastEval::kUnsupported;
      }
      sink->fallback->push_back(FastValueFromSlot(slot));
      ++sink_rows;
      continue;
    }
    if (sink->has_null) {
      if (sink->nulls == nullptr) {
        rollback();
        return FastEval::kUnsupported;
      }
      sink->nulls->push_back(0);
    }
    switch (slot.type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        if (sink->ints == nullptr) {
          rollback();
          return FastEval::kUnsupported;
        }
        sink->ints->push_back(slot.bits);
        break;
      case ValueType::kDouble: {
        if (sink->doubles == nullptr) {
          rollback();
          return FastEval::kUnsupported;
        }
        double d = 0.0;
        std::memcpy(&d, &slot.bits, sizeof(d));
        sink->doubles->push_back(d);
        break;
      }
      case ValueType::kVarChar:
        if (sink->strings == nullptr) {
          rollback();
          return FastEval::kUnsupported;
        }
        // A VARCHAR result may be a view into a program constant that lives
        // only as long as this BytecodeProgram. The bool is the whole ownership
        // contract: sinks that observe it copy every output string into their
        // own arena (BytecodeColumnSink consumers call EnsureArena) before the
        // program can be retired, so no string_view can outlive its program.
        // No per-instruction provenance is needed because copying is all-or-
        // nothing for a program that contains any borrowed string source.
        if (has_varchar_constant_ || has_substr_) {
          sink->may_borrow_constant_strings = true;
        }
        sink->strings->push_back(slot.text);
        break;
      default:
        rollback();
        return FastEval::kUnsupported;
    }
    ++sink_rows;
  }
  return FastEval::kDone;
}

// In-process differential harnesses must be able to flip the bytecode switch
// between two executions of one statement. Mutating the environment with
// setenv/unsetenv while morsel workers call getenv is a data race (UB); this
// atomic override gives them a thread-safe channel. The environment variable
// remains the public, process-external switch.
namespace {
std::atomic<bool> g_bytecode_disabled_override{false};
}  // namespace

// Lean proof: lean/TinylambOpt/ResearchProofs/PlanCacheToggleInvalidation.lean
//   (sound)
uint8_t BytecodeOptToggleBits() noexcept {
  uint8_t bits = 0;
  if (EnvDisabled("TINYLAMB_DISABLE_SIMD_IN_RANGE")) { bits |= 1U << 0U;
}
  if (EnvDisabled("TINYLAMB_DISABLE_RECIPROCAL_DIVMOD")) { bits |= 1U << 1U;
}
  if (EnvDisabled("TINYLAMB_DISABLE_FILTER_ORDER")) { bits |= 1U << 2U;
}
  if (EnvDisabled("TINYLAMB_DISABLE_ROW_IN_MASK")) { bits |= 1U << 3U;
}
  return bits;
}

bool BytecodeEnabled() {
  if (g_bytecode_disabled_override.load(std::memory_order_acquire)) {
    return false;
  }
  // Not cached: an embedder may set the environment variable at runtime.
  return !EnvDisabled("TINYLAMB_DISABLE_BYTECODE");
}

void SetBytecodeDisabledForProcess(bool disabled) {
  g_bytecode_disabled_override.store(disabled, std::memory_order_release);
}

StatusOr<Value> TryEvaluateRowAuto(const BytecodeProgram& program,
                                   const Row& row) {
  if (program.FastInterpretable()) {
    Value out;
    Status error{Status::kSuccess};
    switch (program.TryEvaluateRowFast(row, &out, &error)) {
      case BytecodeProgram::FastEval::kDone:
        return out;
      case BytecodeProgram::FastEval::kError:
        return error;
      case BytecodeProgram::FastEval::kUnsupported:
        break;
    }
  }
  return program.TryEvaluateRow(row);
}

}  // namespace tinylamb
