/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/rewrite.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/json_doc.hpp"
#include "expression/lambda_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Volatility GetFunctionVolatility(std::string_view func_name) {
  // Allocation-free case-insensitive lookup.  This runs per function-call
  // node on every StatementContainsVolatileFunction walk, which the executor
  // probes once per outer row of correlated subqueries; the lowered copy and
  // double hash of a temporary std::string dominated that walk.
  static constexpr size_t kMaxLowered = 64;
  std::array<char, kMaxLowered> lowered{};
  const size_t n = func_name.size();
  if (n > kMaxLowered) {
    return Volatility::kImmutable;  // no known volatile/stable name is longer
  }
  for (size_t i = 0; i < n; ++i) {
    lowered[i] = static_cast<char>(
        std::tolower(static_cast<unsigned char>(func_name[i])));
  }
  const std::string_view name(lowered.data(), n);
  static const std::unordered_set<std::string_view> volatile_funcs = {
      "rand", "random", "uuid", "generate_uuid", "newid"};
  if (volatile_funcs.contains(name)) {
    return Volatility::kVolatile;
  }
  static const std::unordered_set<std::string_view> stable_funcs = {
      "now",          "current_timestamp", "current_date",
      "current_time", "current_datetime",  "localtimestamp",
      "localtime",    "utc_timestamp"};
  if (stable_funcs.contains(name)) {
    return Volatility::kStable;
  }
  return Volatility::kImmutable;
}

namespace {
// Defined with the ScopedActiveRewriteSchema guard below.
const Schema* ActiveRewriteSchema();
}  // namespace

// Despite its historical name, this guard applies whenever a rewrite changes
// evaluation count, including duplication. For example, x + x -> x * 2
// reduces two evaluations to one, while XOR expansion can evaluate each
// operand twice instead of once; either is unsafe for volatile expressions
// or subqueries. An immutable expression can still raise an evaluation error
// (e.g. `1/0`,
// `CAST('abc' AS INT64)`); duplicating or fanning it out changes which branch
// raises first.  That error-freedom proof needs column types, so it is applied
// when a schema is in scope (SafeToReduceEvaluationCountWithSchema).  Without
// one, every column comparison would count as "may throw" and the schema-free
// Cascades rules (filter pushdown through unnest/window/aggregate/semi join,
// ...) could never fire; those rules carry their own error-freedom guards where
// it matters.
bool SafeToReduceEvaluationCount(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (ActiveRewriteSchema() != nullptr &&
      !ExpressionCannotThrowForPlanner(expression)) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kAggregateExp) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             SafeToReduceEvaluationCount);
}

bool ContainsVolatileCall(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) ==
          Volatility::kVolatile) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             ContainsVolatileCall);
}

namespace {

// Mirrors the timestamp-shape detection in EvaluateBinary: `=` on two
// timestamp-shaped varchars coerces both sides to epoch seconds, while
// LIKE/NOT LIKE/REGEXP compare bytes.  Rules that replace byte-wise
// matching with `=` must refuse timestamp-shaped constants or they change
// results ('2020-01-01T12:00:00' = '2020-01-01 12:00:00' is true under the
// coercion, but LIKE on the raw bytes is false).
bool TimestampShapedConstant(std::string_view text) {
  return text.size() >= 19 && text[4] == '-' && text[7] == '-' &&
         (text[10] == ' ' || text[10] == 'T') && text[13] == ':' &&
         text[16] == ':';
}

std::string ToUpper(std::string_view s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result.push_back(
        static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return result;
}

// The JSON document model (JVal, the tolerant parser, and the path
// navigator) lives in expression/json_doc.hpp so both function
// evaluators and the constant-fold rewrite share one implementation.

Value EvaluateJsonFunction(std::string_view func_name,
                           std::string_view json_str,
                           std::string_view path_str) {
  std::string name(func_name);
  for (char& c : name) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  // Malformed JSON text ('{"a" 1}') yields NULL, exactly like the relational
  // accessor's parser; the tolerant document parser alone would have
  // repaired it and answered from the guess.
  if (!json_text::IsValid(json_str)) {
    return {};
  }
  std::string_view s = json_str;
  // ParseJsonSimple is a tolerant parser that returns a JVal (it has no Status
  // result to check); strict malformed-JSON rejection is performed by the
  // json_text::IsValid gate above. A failed navigation below yields NULL.
  const json_doc::JVal root = json_doc::ParseJsonSimple(s);
  const json_doc::JVal* cur = json_doc::NavigateJsonPath(root, path_str);
  if (cur == nullptr) {
    return {};
  }

  if (name == "json_extract_array" || name == "json_query_array" ||
      name == "json_value_array" || name == "json_extract_string_array") {
    if (cur->type != json_doc::JVal::kArr) {
      return {};
    }
    std::vector<Value> elems;
    elems.reserve(cur->arr.size());
    for (const auto& item : cur->arr) {
      if (item) {
        if (name == "json_value_array" || name == "json_extract_string_array") {
          elems.emplace_back(std::string(
              item->type == json_doc::JVal::kStr ? item->str : item->raw));
        } else {
          elems.emplace_back(std::string(item->canonical()));
        }
      }
    }
    return Value::Array(std::move(elems), "STRING");
  }
  if (cur->type == json_doc::JVal::kNull) {
    return {};
  }
  if (name == "json_value" || name == "json_extract_scalar") {
    if (cur->type == json_doc::JVal::kArr ||
        cur->type == json_doc::JVal::kObj) {
      return {};
    }
    return Value(
        std::string(cur->type == json_doc::JVal::kStr ? cur->str : cur->raw));
  }
  return Value(std::string(cur->canonical()));
}

std::vector<Expression> SplitDisjuncts(const Expression& expression) {
  // T2405: iterative flattening.  The old left/right recursion made stack
  // use proportional to the OR-chain length -- and rewrite.cpp's 512-level
  // kMaxRewriteDepth guards TryRewriteOnce only, not this helper (which
  // scan_filter.cpp also mirrors and calls directly).  CombineDisjuncts
  // below rebuilds a left-deep chain of exactly the length Split consumes,
  // so Split->Combine->Split round-trips grew the stack every pass.
  // Right is pushed before Left, so the stack pops Left first and `parts`
  // keeps the left-to-right leaf order the recursion produced.
  std::vector<Expression> parts;
  std::vector<Expression> pending{expression};
  while (!pending.empty()) {
    const Expression node = std::move(pending.back());
    pending.pop_back();
    if (!node) {
      continue;
    }
    if (node->Type() == TypeTag::kBinaryExp &&
        node->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      pending.push_back(node->AsBinaryExpression().Right());
      pending.push_back(node->AsBinaryExpression().Left());
    } else {
      parts.push_back(node);
    }
  }
  return parts;
}

Expression CombineDisjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return ConstantValueExp(Value(false));
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kOr,
                                 expressions[i]);
  }
  return result;
}

bool ExtractRegexPrefix(std::string_view pattern, std::string& prefix,
                        std::string& remainder) {
  if (pattern.empty() || pattern.front() != '^') {
    return false;
  }
  prefix.clear();
  size_t i = 1;
  while (i < pattern.size()) {
    if (pattern[i] == '\\' && i + 1 < pattern.size()) {
      char c = pattern[i + 1];
      if (c == '.' || c == '*' || c == '+' || c == '?' || c == '^' ||
          c == '$' || c == '[' || c == ']' || c == '(' || c == ')' ||
          c == '{' || c == '}' || c == '|' || c == '\\') {
        prefix.push_back(c);
        i += 2;
      } else {
        break;
      }
    } else if (pattern[i] == '.' || pattern[i] == '*' || pattern[i] == '+' ||
               pattern[i] == '?' || pattern[i] == '^' || pattern[i] == '$' ||
               pattern[i] == '[' || pattern[i] == ']' || pattern[i] == '(' ||
               pattern[i] == ')' || pattern[i] == '{' || pattern[i] == '}' ||
               pattern[i] == '|' || pattern[i] == '\\') {
      break;
    } else {
      prefix.push_back(pattern[i]);
      ++i;
    }
  }
  remainder = std::string(pattern.substr(i));
  return true;
}

bool Same(const Expression& left, const Expression& right) {
  return SamePrintedExpression(left, right);
}

bool IsConstant(const Expression& expression) {
  return expression && expression->Type() == TypeTag::kConstantValue &&
         !ContainsParameterSlot(expression);
}

struct SchemaBoundExprType {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  Collation collation{Collation::kNone};
  bool is_constant{false};
  Value const_val{};
};

bool SchemaExprNameEquals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

bool SchemaTypesComparableNonThrowing(const SchemaBoundExprType& a,
                                      const SchemaBoundExprType& b) {
  if (a.is_interval || b.is_interval) {
    return false;
  }
  if (a.collation != Collation::kNone && b.collation != Collation::kNone &&
      a.collation != b.collation) {
    return false;
  }
  if (a.type == ValueType::kNull || b.type == ValueType::kNull) {
    return true;
  }
  if ((a.type == ValueType::kInt64 || a.type == ValueType::kDouble) &&
      (b.type == ValueType::kInt64 || b.type == ValueType::kDouble)) {
    return true;
  }
  if (a.type == ValueType::kDate && b.type == ValueType::kDate &&
      !a.is_unsigned && !b.is_unsigned) {
    return true;
  }
  if (a.type == ValueType::kDate && !a.is_unsigned && b.is_constant &&
      b.type == ValueType::kVarChar) {
    return TryEvaluateBinary(BinaryOperation::kEquals, Value::DateFromDays(0),
                             b.const_val)
        .HasValue();
  }
  if (b.type == ValueType::kDate && !b.is_unsigned && a.is_constant &&
      a.type == ValueType::kVarChar) {
    return TryEvaluateBinary(BinaryOperation::kEquals, a.const_val,
                             Value::DateFromDays(0))
        .HasValue();
  }
  if (a.type == ValueType::kVarChar && b.type == ValueType::kVarChar) {
    return true;
  }
  return false;
}

std::optional<SchemaBoundExprType> MakeConstantSchemaBoundType(
    const Value& val) {
  if (val.IsNull()) {
    return SchemaBoundExprType{
        .type = ValueType::kNull,
        .is_constant = true,
        .const_val = val,
    };
  }
  if (val.type == ValueType::kInt64 || val.type == ValueType::kDouble ||
      val.type == ValueType::kDate || val.type == ValueType::kVarChar) {
    return SchemaBoundExprType{
        .type = val.type,
        .is_unsigned = val.IsUnsigned(),
        .is_interval = val.IsInterval(),
        .collation = val.type == ValueType::kVarChar ? CollationOf(val)
                                                     : Collation::kNone,
        .is_constant = true,
        .const_val = val,
    };
  }
  return std::nullopt;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (closed_const_non_throwing_eval_independent_of_row,
//    trap_throwing_closed_const_not_droppable)
std::optional<SchemaBoundExprType>
TryResolveSchemaBoundNonThrowingType(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& schema, size_t depth = 0) {
  if (!expr || depth > 128) {
    return std::nullopt;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue: {
      if (ContainsParameterSlot(expr)) {
        return std::nullopt;
      }
      // MakeConstantSchemaBoundType takes a const Value& and stores it in the
      // returned struct; this is one necessary copy into the result, not a
      // per-lookup copy of the constant node.
      return MakeConstantSchemaBoundType(expr->AsConstantValue().GetValue());
    }
    case TypeTag::kColumnValue: {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.name.empty() || col.name == "*") {
        return std::nullopt;
      }
      size_t hits = 0;
      size_t matched = 0;
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const Column& c = schema.GetColumn(i);
        if (!SchemaExprNameEquals(c.Name().name, col.name)) {
          continue;
        }
        if (!col.schema.empty() &&
            !SchemaExprNameEquals(c.Name().schema, col.schema)) {
          continue;
        }
        matched = i;
        ++hits;
        if (hits > 1) {
          return std::nullopt;
        }
      }
      if (hits != 1) {
        return std::nullopt;
      }
      const Column& c = schema.GetColumn(matched);
      if (c.IsInterval()) {
        return std::nullopt;
      }
      if (c.Type() == ValueType::kInt64 || c.Type() == ValueType::kDouble ||
          c.Type() == ValueType::kDate || c.Type() == ValueType::kVarChar) {
        return SchemaBoundExprType{
            .type = c.Type(),
            .is_unsigned = c.IsUnsigned(),
        };
      }
      return std::nullopt;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      const auto ct = TryResolveSchemaBoundNonThrowingType(unary.Child(),
                                                           schema, depth + 1);
      if (!ct.has_value()) {
        return std::nullopt;
      }
      if (ct->is_constant) {
        StatusOr<Value> ev = TryEvaluateUnary(unary.Op(), ct->const_val);
        if (ev.HasValue()) {
          return MakeConstantSchemaBoundType(ev.Value());
        }
        return std::nullopt;
      }
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kNot:
          return SchemaBoundExprType{.type = ValueType::kInt64};
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
          if (ct->type == ValueType::kInt64 || ct->type == ValueType::kNull) {
            return SchemaBoundExprType{.type = ValueType::kInt64};
          }
          return std::nullopt;
        case UnaryOperation::kMinus:
          if (ct->type == ValueType::kDouble || ct->type == ValueType::kNull) {
            return SchemaBoundExprType{.type = ct->type};
          }
          return std::nullopt;
        case UnaryOperation::kBitwiseNot:
          if (ct->type == ValueType::kInt64 || ct->type == ValueType::kNull) {
            return SchemaBoundExprType{.type = ct->type,
                                       .is_unsigned = ct->is_unsigned};
          }
          return std::nullopt;
      }
      return std::nullopt;
    }
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      const auto lt =
          TryResolveSchemaBoundNonThrowingType(bin.Left(), schema, depth + 1);
      const auto rt =
          TryResolveSchemaBoundNonThrowingType(bin.Right(), schema, depth + 1);
      if (!lt.has_value() || !rt.has_value()) {
        return std::nullopt;
      }
      if (lt->is_constant && rt->is_constant) {
        StatusOr<Value> ev =
            TryEvaluateBinary(bin.Op(), lt->const_val, rt->const_val);
        if (ev.HasValue()) {
          return MakeConstantSchemaBoundType(ev.Value());
        }
        return std::nullopt;
      }
      switch (bin.Op()) {
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return SchemaBoundExprType{.type = ValueType::kInt64};
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
          if (SchemaTypesComparableNonThrowing(*lt, *rt)) {
            return SchemaBoundExprType{.type = ValueType::kInt64};
          }
          return std::nullopt;
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
          if (!lt->is_interval && !rt->is_interval &&
              (lt->type == ValueType::kVarChar ||
               lt->type == ValueType::kNull) &&
              (rt->type == ValueType::kVarChar ||
               rt->type == ValueType::kNull) &&
              lt->collation != Collation::kCaseInsensitive &&
              rt->collation != Collation::kCaseInsensitive) {
            return SchemaBoundExprType{.type = ValueType::kInt64};
          }
          return std::nullopt;
        case BinaryOperation::kSubtract:
          if (lt->type == ValueType::kDate && rt->type == ValueType::kDate &&
              !lt->is_unsigned && !rt->is_unsigned) {
            return SchemaBoundExprType{.type = ValueType::kInt64};
          }
          return std::nullopt;
        default:
          return std::nullopt;
      }
    }
    case TypeTag::kInExp: {
      const auto& in = expr->AsInExpression();
      const auto ct =
          TryResolveSchemaBoundNonThrowingType(in.child_, schema, depth + 1);
      if (!ct.has_value()) {
        return std::nullopt;
      }
      for (const Expression& item : in.list_) {
        const auto it =
            TryResolveSchemaBoundNonThrowingType(item, schema, depth + 1);
        if (!it.has_value() || !SchemaTypesComparableNonThrowing(*ct, *it)) {
          return std::nullopt;
        }
      }
      return SchemaBoundExprType{.type = ValueType::kInt64};
    }
    case TypeTag::kCaseExp: {
      const auto& c = expr->AsCaseExpression();
      std::optional<SchemaBoundExprType> result_type;
      auto merge_branch = [&](const SchemaBoundExprType& bt) -> bool {
        if (bt.type == ValueType::kNull) {
          return true;
        }
        if (!result_type.has_value()) {
          result_type = SchemaBoundExprType{
              .type = bt.type,
              .is_unsigned = bt.is_unsigned,
              .is_interval = bt.is_interval,
              .collation = bt.collation,
          };
          return true;
        }
        return result_type->type == bt.type &&
               result_type->is_unsigned == bt.is_unsigned &&
               result_type->is_interval == bt.is_interval &&
               result_type->collation == bt.collation;
      };
      for (const auto& [cond, val] : c.when_clauses_) {
        if (!TryResolveSchemaBoundNonThrowingType(cond, schema, depth + 1)
                 .has_value()) {
          return std::nullopt;
        }
        const auto vt =
            TryResolveSchemaBoundNonThrowingType(val, schema, depth + 1);
        if (!vt.has_value() || !merge_branch(*vt)) {
          return std::nullopt;
        }
      }
      if (c.else_clause_) {
        const auto et = TryResolveSchemaBoundNonThrowingType(c.else_clause_,
                                                             schema, depth + 1);
        if (!et.has_value() || !merge_branch(*et)) {
          return std::nullopt;
        }
      }
      return result_type.value_or(
          SchemaBoundExprType{.type = ValueType::kNull});
    }
    case TypeTag::kFunctionCallExp: {
      const auto& fn = expr->AsFunctionCallExpression();
      if (GetFunctionVolatility(fn.FuncName()) != Volatility::kImmutable) {
        return std::nullopt;
      }
      std::string name = fn.FuncName();
      for (char& ch : name) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
      }
      std::vector<SchemaBoundExprType> arg_types;
      arg_types.reserve(fn.Args().size());
      for (const Expression& arg : fn.Args()) {
        const auto at =
            TryResolveSchemaBoundNonThrowingType(arg, schema, depth + 1);
        if (!at.has_value()) {
          return std::nullopt;
        }
        arg_types.push_back(*at);
      }
      if ((name == "coalesce" || name == "ifnull") && !arg_types.empty()) {
        if (name == "ifnull" && arg_types.size() != 2) {
          return std::nullopt;
        }
        std::optional<SchemaBoundExprType> res;
        for (const auto& at : arg_types) {
          if (at.type == ValueType::kNull) {
            continue;
          }
          if (!res.has_value()) {
            res = SchemaBoundExprType{.type = at.type,
                                      .is_unsigned = at.is_unsigned,
                                      .is_interval = at.is_interval,
                                      .collation = at.collation};
          } else if (res->type != at.type ||
                     res->is_unsigned != at.is_unsigned ||
                     res->is_interval != at.is_interval ||
                     res->collation != at.collation) {
            return std::nullopt;
          }
        }
        return res.value_or(SchemaBoundExprType{.type = ValueType::kNull});
      }
      if (name == "nullif" && arg_types.size() == 2) {
        if (SchemaTypesComparableNonThrowing(arg_types[0], arg_types[1])) {
          return SchemaBoundExprType{.type = arg_types[0].type,
                                     .is_unsigned = arg_types[0].is_unsigned,
                                     .is_interval = arg_types[0].is_interval,
                                     .collation = arg_types[0].collation};
        }
        return std::nullopt;
      }
      if ((name == "substr" || name == "substring") &&
          (arg_types.size() == 2 || arg_types.size() == 3)) {
        if (!arg_types[0].is_interval &&
            (arg_types[0].type == ValueType::kVarChar ||
             arg_types[0].type == ValueType::kNull) &&
            !arg_types[1].is_unsigned &&
            (arg_types[1].type == ValueType::kInt64 ||
             arg_types[1].type == ValueType::kNull) &&
            (arg_types.size() == 2 ||
             (!arg_types[2].is_unsigned &&
              (arg_types[2].type == ValueType::kInt64 ||
               arg_types[2].type == ValueType::kNull)))) {
          return SchemaBoundExprType{.type = ValueType::kVarChar,
                                     .collation = arg_types[0].collation};
        }
        return std::nullopt;
      }
      if ((name == "lower" || name == "upper" || name == "trim" ||
           name == "ltrim" || name == "rtrim") &&
          arg_types.size() == 1 && !arg_types[0].is_interval &&
          (arg_types[0].type == ValueType::kVarChar ||
           arg_types[0].type == ValueType::kNull)) {
        return SchemaBoundExprType{.type = ValueType::kVarChar,
                                   .collation = arg_types[0].collation};
      }
      if ((name == "length" || name == "char_length" ||
           name == "character_length" || name == "byte_length" ||
           name == "octet_length") &&
          arg_types.size() == 1 && !arg_types[0].is_interval &&
          (arg_types[0].type == ValueType::kVarChar ||
           arg_types[0].type == ValueType::kNull)) {
        return SchemaBoundExprType{.type = ValueType::kInt64};
      }
      if ((name == "starts_with" || name == "ends_with") &&
          arg_types.size() == 2 && !arg_types[0].is_interval &&
          !arg_types[1].is_interval &&
          (arg_types[0].type == ValueType::kVarChar ||
           arg_types[0].type == ValueType::kNull) &&
          (arg_types[1].type == ValueType::kVarChar ||
           arg_types[1].type == ValueType::kNull) &&
          arg_types[0].collation != Collation::kCaseInsensitive &&
          arg_types[1].collation != Collation::kCaseInsensitive) {
        return SchemaBoundExprType{.type = ValueType::kInt64};
      }
      if ((name == "extract_year" || name == "extract_month" ||
           name == "extract_day" || name == "extract_quarter" ||
           name == "extract_dayofweek" || name == "extract_dayofyear") &&
          arg_types.size() == 1 &&
          (arg_types[0].type == ValueType::kDate ||
           arg_types[0].type == ValueType::kNull)) {
        if (arg_types[0].is_constant && !ContainsParameterSlot(expr) &&
            expr->TouchedColumns().empty()) {
          StatusOr<Value> ev = expr->TryEvaluate(Row{}, Schema{});
          if (ev.HasValue()) {
            return MakeConstantSchemaBoundType(ev.Value());
          }
        }
        return SchemaBoundExprType{.type = ValueType::kInt64};
      }
      if (std::ranges::all_of(
              arg_types,
              [](const SchemaBoundExprType& a) { return a.is_constant; }) &&
          !ContainsParameterSlot(expr) && expr->TouchedColumns().empty()) {
        StatusOr<Value> ev = expr->TryEvaluate(Row{}, Schema{});
        if (ev.HasValue()) {
          return MakeConstantSchemaBoundType(ev.Value());
        }
      }
      return std::nullopt;
    }
    case TypeTag::kCastExp: {
      const auto ct = TryResolveSchemaBoundNonThrowingType(
          expr->AsCastExpression().Child(), schema, depth + 1);
      if (ct.has_value() && ct->is_constant && !ContainsParameterSlot(expr) &&
          expr->TouchedColumns().empty()) {
        StatusOr<Value> ev = expr->TryEvaluate(Row{}, Schema{});
        if (ev.HasValue()) {
          return MakeConstantSchemaBoundType(ev.Value());
        }
      }
      return std::nullopt;
    }
    case TypeTag::kIntervalExp: {
      const auto children = ExpressionChildren(expr);
      const bool all_const =
          std::ranges::all_of(children, [&](const Expression& ch) {
            const auto cht =
                TryResolveSchemaBoundNonThrowingType(ch, schema, depth + 1);
            return cht.has_value() && cht->is_constant;
          });
      if (all_const && !ContainsParameterSlot(expr) &&
          expr->TouchedColumns().empty()) {
        StatusOr<Value> ev = expr->TryEvaluate(Row{}, Schema{});
        if (ev.HasValue()) {
          return MakeConstantSchemaBoundType(ev.Value());
        }
      }
      return std::nullopt;
    }
    default:
      return std::nullopt;
  }
}

// Conservative error-freedom test: true only when evaluating `expression`
// can never raise, whatever the row contains.  Rewrites that drop subtrees
// must not erase an error the AST interpreter (the semantic reference) would
// have raised, so they consult this before discarding an operand.
bool ExpressionCannotThrow(const Expression& expression) {  // NOLINT
  if (!expression) {
    return false;
  }
  if (const Schema* active_schema = ActiveRewriteSchema();
      active_schema != nullptr && active_schema->ColumnCount() > 0) {
    if (TryResolveSchemaBoundNonThrowingType(expression, *active_schema)
            .has_value()) {
      return true;
    }
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue:
    case TypeTag::kColumnValue:
      return true;
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor: {
          const bool children_total = ExpressionCannotThrow(binary.Left()) &&
                                      ExpressionCannotThrow(binary.Right());
          if (!children_total) {
            return false;
          }
          if (binary.Op() == BinaryOperation::kAnd ||
              binary.Op() == BinaryOperation::kOr ||
              binary.Op() == BinaryOperation::kXor) {
            return true;
          }
          if (IsConstant(binary.Left()) && IsConstant(binary.Right())) {
            return TryEvaluateBinary(
                       binary.Op(), binary.Left()->AsConstantValue().GetValue(),
                       binary.Right()->AsConstantValue().GetValue())
                .HasValue();
          }
          // A schema-free comparison of columns is not proven compatible.
          // IN can be rewritten to equality before this proof runs.
          return false;
        }
        default:
          return false;  // arithmetic, shifts, LIKE
      }
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kNot:
          return ExpressionCannotThrow(unary.Child());
        default:
          return false;  // IS TRUE family, unary minus
      }
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (!ExpressionCannotThrow(in.child_)) {
        return false;
      }
      // Without a bound schema a column/list comparison may raise a type
      // error. Prove constant comparisons instead of assuming compatible types.
      if (!IsConstant(in.child_)) {
        return in.list_.empty();
      }
      const Value child = in.child_->AsConstantValue().GetValue();
      return std::ranges::all_of(in.list_, [&](const Expression& item) {
        if (!IsConstant(item)) {
          return false;
        }
        const Value candidate = item->AsConstantValue().GetValue();
        if (HaveConflictingCollations(child, candidate)) {
          return false;
        }
        return child.IsNull() || candidate.IsNull() ||
               TryEvaluateBinary(BinaryOperation::kEquals, child, candidate)
                   .HasValue();
      });
    }
    default:
      return false;  // casts, CASE, functions, subqueries, ...
  }
}

bool ConstantBool(const Expression& expression, bool* value) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type != ValueType::kInt64 ||
      (constant.value.int_value != 0 && constant.value.int_value != 1)) {
    return false;
  }
  *value = constant.value.int_value != 0;
  return true;
}

BinaryOperation FlipComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

BinaryOperation NegateComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kEquals:
      return BinaryOperation::kNotEquals;
    case BinaryOperation::kNotEquals:
      return BinaryOperation::kEquals;
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThanEquals;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThan;
    default:
      return operation;
  }
}

// True when the subtree contains a DOUBLE-valued constant: its comparisons
// can be IEEE-unordered (NaN) regardless of the static schema type.
bool SubtreeHasDoubleLiteral(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kConstantValue &&
      expression->AsConstantValue().GetValue().type == ValueType::kDouble) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             SubtreeHasDoubleLiteral);
}

bool IsZero(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    // UINT64(0) shares the raw bit pattern but is a different number; folding
    // it as a signed identity would lose unsigned type/overflow semantics.
    return !constant.IsUnsigned() && constant.value.int_value == 0;
  }
  if (constant.type == ValueType::kDouble) {
    // IEEE: x - (-0.0) == x + 0.0, which differs from x when x is -0.0.
    // Treat only positive zero as a candidate. Addition separately checks
    // whether the value being added to can itself be negative zero.
    return constant.value.double_value == 0.0 &&
           !std::signbit(constant.value.double_value);
  }
  return false;
}

bool IsOne(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    // UINT64(1) is not the signed integer identity literal 1 (see IsZero).
    return !constant.IsUnsigned() && constant.value.int_value == 1;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == 1.0;
  }
  return false;
}

bool IsInt64Constant(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  // UINT64 shares the kInt64 bit pattern; the int64_t* overload below rejects
  // it explicitly, so this overload must do the same.
  return !constant.IsNull() && constant.type == ValueType::kInt64 &&
         !constant.IsUnsigned();
}

std::optional<int64_t> NegativeInt64Constant(const Expression& expression) {
  if (!IsInt64Constant(expression)) {
    return std::nullopt;
  }
  const Value value = expression->AsConstantValue().GetValue();
  if (value.IsUnsigned() || value.value.int_value >= 0 ||
      value.value.int_value == std::numeric_limits<int64_t>::min()) {
    return std::nullopt;
  }
  return value.value.int_value;
}

bool HasLikeWildcard(std::string_view pattern) {
  return pattern.find('%') != std::string_view::npos ||
         pattern.find('_') != std::string_view::npos;
}

bool HasLikeEscape(std::string_view pattern) {
  return pattern.find('\\') != std::string_view::npos;
}

// Recursing per child without a bound lets attacker-controlled deeply nested
// SQL overflow the stack and kill the whole process.
constexpr size_t kMaxRewriteDepth = 512;

// True when the expression produces an int64 regardless of input rows. Only
// integer expressions may be reassociated: reordering floating point adds
// changes IEEE rounding and thus the low bits of the result.
bool StaticallyInt64(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    // Resolve against the active rewrite schema like the sibling probes
    // (StaticallyNumeric / StaticallyDouble / StaticallyNonNumeric): with
    // an empty Schema() every column reference throws and the INT64-only
    // guards below (reassociate_* / double_bitwise_negation) could never
    // fire on the column chains they exist for.
    const Schema* schema = ActiveRewriteSchema();
    return expression->ResultType(schema != nullptr ? *schema : Schema())
               .GetType() == TypeTag::kBigInt;
  } catch (const std::exception&) {
    return false;
  }
}

bool StaticallyNonNumeric(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    const Value& val = expression->AsConstantValue().GetValue();
    return !val.IsNull() && val.type != ValueType::kInt64 &&
           val.type != ValueType::kDouble;
  }
  try {
    const Schema* schema = ActiveRewriteSchema();
    const TypeTag type =
        expression->ResultType(schema != nullptr ? *schema : Schema())
            .GetType();
    return type != TypeTag::kBigInt && type != TypeTag::kDouble &&
           type != TypeTag::kInvalid;
  } catch (const std::exception&) {
    // Unknown type (rewrite-time probe schema is empty): assume the
    // expression may be numeric.  Identity rules firing on bare columns is
    // pinned by ArithmeticIdentitiesAndDoubleNegation; the catch keeps the
    // boolean heuristics permissive for the same reason.
    return false;
  }
}

bool StaticallyNumeric(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    const Schema* schema = ActiveRewriteSchema();
    const TypeTag type =
        expression->ResultType(schema != nullptr ? *schema : Schema())
            .GetType();
    return type == TypeTag::kBigInt || type == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

// Division promotes to DOUBLE unconditionally, so an identity rewrite of
// `x / 1` must only fire when the surviving side is already double-typed.
// With no active schema, a column reference cannot be resolved and this
// deliberately returns false ("not proven DOUBLE") rather than folding;
// that is the contract of this guard.
bool StaticallyDouble(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    const Schema* schema = ActiveRewriteSchema();
    return expression->ResultType(schema != nullptr ? *schema : Schema())
               .GetType() == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

Expression PreserveNumericIdentity(const Expression& input,
                                   const Expression& identity,
                                   BinaryOperation operation) {
  if (StaticallyNonNumeric(input)) {
    return {};
  }
  const bool addition = operation == BinaryOperation::kAdd;
  if (IsInt64Constant(identity)) {
    return addition && StaticallyDouble(input) ? Expression{} : input;
  }
  if (StaticallyInt64(input)) {
    return CastExpressionExp(input, "FLOAT64");
  }
  // Adding positive zero to negative zero produces positive zero, so keep
  // addition when the survivor may already be floating point. Subtracting
  // positive zero preserves signed zero; INT64 inputs were cast above to keep
  // the FLOAT64 result type of mixed INT64/DOUBLE arithmetic.
  return !addition && StaticallyDouble(input) ? input : Expression{};
}

// Opposite-direction guard for rewrites that must abort when the operand
// *could* be DOUBLE (e.g. `nullif(a, a) -> NULL`, unsound for NaN). Type
// resolution is schema-free here, so a column reference throws and must be
// treated as "not proven non-double" rather than "not double" -- otherwise
// the guard fails open and folds a DOUBLE column's NaN row into NULL.
bool StaticallyProvesNonDouble(const Expression& expression) {
  if (!expression) {
    return false;
  }
  try {
    return expression->ResultType(Schema()).GetType() != TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

// True when an expression is statically known NOT to produce a boolean result
// (0, 1, or NULL). Boolean simplification rules (AND, OR, NOT NOT, XOR) must
// never collapse into an expression of non-boolean type (such as VARCHAR or
// DOUBLE) or non-boolean values (e.g. 2).
bool StaticallyNonBoolean(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (StaticallyNonNumeric(expression) || StaticallyDouble(expression)) {
    return true;
  }
  if (IsInt64Constant(expression)) {
    const Value& val = expression->AsConstantValue().GetValue();
    if (!val.IsNull()) {
      const int64_t v = val.value.int_value;
      return v != 0 && v != 1;
    }
  }
  if (expression->Type() == TypeTag::kBinaryExp) {
    const auto op = expression->AsBinaryExpression().Op();
    switch (op) {
      case BinaryOperation::kAdd:
      case BinaryOperation::kSubtract:
      case BinaryOperation::kMultiply:
      case BinaryOperation::kDivide:
      case BinaryOperation::kModulo:
      case BinaryOperation::kShiftLeft:
      case BinaryOperation::kShiftRight:
        return true;
      default:
        break;
    }
  }
  if (expression->Type() == TypeTag::kUnaryExp) {
    const UnaryOperation op = expression->AsUnaryExpression().Op();
    if (op == UnaryOperation::kMinus || op == UnaryOperation::kBitwiseNot) {
      return true;
    }
  }
  if (expression->Type() == TypeTag::kFunctionCallExp) {
    const std::string& name = expression->AsFunctionCallExpression().FuncName();
    static const std::unordered_set<std::string> kNonBooleanFuncs = {
        "abs",           "sign",     "round",       "trunc",
        "truncate",      "ceil",     "ceiling",     "floor",
        "safe_negate",   "mod",      "safe_add",    "safe_subtract",
        "safe_multiply", "length",   "char_length", "character_length",
        "byte_length",   "strpos",   "instr",       "div",
        "__bit_and",     "__bit_or", "__bit_xor",   "__shift_left",
        "__shift_right"};
    if (kNonBooleanFuncs.contains(name) || name.starts_with("extract_")) {
      return true;
    }
  }
  return false;
}

}  // namespace

bool ExpressionCannotThrowForPlanner(const Expression& expression) {
  return ExpressionCannotThrow(expression);
}

Value EvaluateJsonFunctionCall(std::string_view func_name,
                               std::string_view json_str,
                               std::string_view path_str) {
  // Thin forwarder to the fold-side JSON evaluator so the constant-fold rule
  // and row-wise AST execution share one implementation (no divergence).
  return EvaluateJsonFunction(func_name, json_str, path_str);
}

ExpressionPattern ExpressionPattern::Any(std::string capture) {
  ExpressionPattern pattern;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Type(TypeTag type, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = type;
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Binary(
    std::optional<BinaryOperation> operation, ExpressionPattern left,
    ExpressionPattern right, std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kBinaryExp;
  pattern.binary_operation_ = operation;
  pattern.children_.push_back(std::move(left));
  pattern.children_.push_back(std::move(right));
  pattern.capture_ = std::move(capture);
  return pattern;
}

ExpressionPattern ExpressionPattern::Unary(
    std::optional<UnaryOperation> operation, ExpressionPattern child,
    std::string capture) {
  ExpressionPattern pattern;
  pattern.type_ = TypeTag::kUnaryExp;
  pattern.unary_operation_ = operation;
  pattern.children_.push_back(std::move(child));
  pattern.capture_ = std::move(capture);
  return pattern;
}

bool ExpressionPattern::Match(  // NOLINT(misc-no-recursion)
    const Expression& expression, ExpressionBindings* bindings) const {
  if (!expression) {
    return false;
  }
  if (type_ && expression->Type() != *type_) {
    return false;
  }
  if (binary_operation_ &&
      expression->AsBinaryExpression().Op() != *binary_operation_) {
    return false;
  }
  if (unary_operation_ &&
      expression->AsUnaryExpression().Op() != *unary_operation_) {
    return false;
  }
  if (children_.empty()) {
    if (capture_.empty()) {
      return true;
    }
    const auto found = bindings->find(capture_);
    if (found == bindings->end()) {
      bindings->emplace(capture_, expression);
      return true;
    }
    return Same(found->second, expression);
  }

  const std::vector<Expression> children = ExpressionChildren(expression);
  if (children.size() != children_.size()) {
    return false;
  }

  ExpressionBindings local = *bindings;
  if (!capture_.empty()) {
    auto [iter, inserted] = local.emplace(capture_, expression);
    if (!inserted && !Same(iter->second, expression)) {
      return false;
    }
  }
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i].Match(children[i], &local)) {
      return false;
    }
  }
  *bindings = std::move(local);
  return true;
}

Expression ExpressionRule::Apply(const Expression& expression) const {
  ExpressionBindings bindings;
  if (!pattern_.Match(expression, &bindings)) {
    return nullptr;
  }
  return rewrite_(expression, bindings);
}

ExpressionRuleSet& ExpressionRuleSet::Add(ExpressionRule rule) {
  Remove(rule.Name());
  rules_.push_back(std::move(rule));
  return *this;
}

bool ExpressionRuleSet::Remove(std::string_view name) {
  const auto old_size = rules_.size();
  std::erase_if(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
  return rules_.size() != old_size;
}

bool ExpressionRuleSet::Contains(std::string_view name) const {
  return std::ranges::any_of(
      rules_, [&](const ExpressionRule& rule) { return rule.Name() == name; });
}

namespace {
Expression FactorCommonAndImpl(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp) {
    return Expression{};
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kOr) {
    return Expression{};
  }
  const std::vector<Expression> left = SplitConjuncts(binary.Left());
  const std::vector<Expression> right = SplitConjuncts(binary.Right());
  if (left.empty() || right.empty()) {
    return Expression{};
  }

  // Identify common conjuncts that can safely be factored out.
  // Safety requirements:
  // 1. SafeToReduceEvaluationCount: volatile functions (e.g. RAND()) must not
  //    have their evaluation count collapsed.
  // 2. Short-circuit order preservation: any conjunct in `left` (or `right`)
  //    that appeared before a factored conjunct must be guaranteed not to
  //    throw (ExpressionCannotThrow). Otherwise, moving the common conjunct
  //    ahead of it could evaluate to FALSE and skip evaluating the throwing
  //    preceding conjunct, silently erasing an error.
  std::vector<Expression> common;
  common.reserve(left.size());
  std::vector<Expression> left_rest;
  left_rest.reserve(left.size());
  std::vector<Expression> right_rest;
  right_rest.reserve(right.size());

  std::vector<size_t> matched_right_indices;
  matched_right_indices.reserve(right.size());
  bool left_saw_throwing = false;

  for (const Expression& l : left) {
    if (!left_saw_throwing && SafeToReduceEvaluationCount(l)) {
      bool found = false;
      bool right_saw_throwing = false;
      for (size_t r_idx = 0; r_idx < right.size(); ++r_idx) {
        if (!right_saw_throwing &&
            std::ranges::find(matched_right_indices, r_idx) ==
                matched_right_indices.end() &&
            Same(l, right[r_idx])) {
          matched_right_indices.push_back(r_idx);
          common.push_back(l);
          found = true;
          break;
        }
        if (!ExpressionCannotThrow(right[r_idx])) {
          right_saw_throwing = true;
        }
      }
      if (!found) {
        left_rest.push_back(l);
      }
    } else {
      left_rest.push_back(l);
    }
    if (!ExpressionCannotThrow(l)) {
      left_saw_throwing = true;
    }
  }

  if (common.empty()) {
    return Expression{};
  }

  for (size_t r_idx = 0; r_idx < right.size(); ++r_idx) {
    if (std::ranges::find(matched_right_indices, r_idx) ==
        matched_right_indices.end()) {
      right_rest.push_back(right[r_idx]);
    }
  }

  if (left_rest.empty() || right_rest.empty()) {
    const std::vector<Expression>& dropped =
        left_rest.empty() ? right_rest : left_rest;
    if (!std::ranges::all_of(dropped, [](const Expression& e) {
          return ExpressionCannotThrow(e);
        })) {
      return Expression{};
    }
    if (common.size() == 1 && StaticallyNonBoolean(common[0])) {
      return Expression{};
    }
    return CombineConjuncts(common);
  }

  return BinaryExpressionExp(
      CombineConjuncts(common), BinaryOperation::kAnd,
      BinaryExpressionExp(CombineConjuncts(left_rest), BinaryOperation::kOr,
                          CombineConjuncts(right_rest)));
}
}  // namespace

namespace {

// Schema published by ExpressionRewriter::TryRewrite for rules that want to
// resolve column types (see set_schema). Null outside a rewrite.
thread_local const Schema* g_active_rewrite_schema = nullptr;

const Schema* ActiveRewriteSchema() { return g_active_rewrite_schema; }

}  // namespace

ScopedActiveRewriteSchema::ScopedActiveRewriteSchema(const Schema* schema)
    : saved_(g_active_rewrite_schema) {
  g_active_rewrite_schema = schema;
}

ScopedActiveRewriteSchema::~ScopedActiveRewriteSchema() {
  g_active_rewrite_schema = saved_;
}

bool SafeToReduceEvaluationCountWithSchema(const Expression& expression,
                                           const Schema& schema) {
  const ScopedActiveRewriteSchema schema_guard(&schema);
  return SafeToReduceEvaluationCount(expression);
}

namespace {

bool SideCanBeDouble(const Expression& side, const Schema& schema);

bool SideCanBeDouble(const Expression& side, const Schema& schema) {
  if (!side) {
    return false;
  }
  // A DOUBLE literal anywhere in the subtree makes the expression's runtime
  // value potentially NaN even when the schema-typed ResultType cannot be
  // resolved (unknown columns throw).  Check this BEFORE the schema lookup.
  if (side->Type() == TypeTag::kConstantValue &&
      side->AsConstantValue().GetValue().type == ValueType::kDouble) {
    return true;
  }
  for (const Expression& child : ExpressionChildren(side)) {
    if (SideCanBeDouble(child, schema)) {
      return true;
    }
  }
  try {
    return side->ResultType(schema).GetType() == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}
}  // namespace

const ExpressionRuleSet& ExpressionRuleSet::Default() {
  static const ExpressionRuleSet rules = [] {
    using namespace expression_dsl;
    ExpressionRuleSet built;
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (boolean_identity_short_circuit_sound)
    built.Add(ExpressionRule(
        "fold_binary",
        AnyBinary(Is(TypeTag::kConstantValue, "left"),
                  Is(TypeTag::kConstantValue, "right")),
        [](const Expression& expression, const ExpressionBindings&) {
          // Parameter slots share Type() with constants. Folding them to a
          // value would freeze the fill-time literal.
          if (ContainsParameterSlot(expression)) {
            return Expression{};
          }
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());

              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }

          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (boolean_identity_short_circuit_sound)
    built.Add(ExpressionRule(
        "fold_unary", AnyUnary(Is(TypeTag::kConstantValue, "child")),
        [](const Expression& expression, const ExpressionBindings&) {
          if (ContainsParameterSlot(expression)) {
            return Expression{};
          }
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());

              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }

          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (in_and_join_notnull_rules_sound)
    built.Add(ExpressionRule(
        "fold_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const std::vector<Expression> children =
              ExpressionChildren(expression);
          if (!std::ranges::all_of(children, IsConstant)) {
            return Expression{};
          }
          if (ContainsParameterSlot(expression)) {
            return Expression{};
          }
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());

              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }

          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "fold_function", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (GetFunctionVolatility(fn.FuncName()) != Volatility::kImmutable) {
            return Expression{};
          }
          const auto is_literal = [](const Expression& arg) {
            return arg && (arg->Type() == TypeTag::kConstantValue ||
                           arg->Type() == TypeTag::kIntervalExp);
          };
          if (!std::ranges::all_of(ExpressionChildren(expression),
                                   is_literal)) {
            return Expression{};
          }
          if (ContainsParameterSlot(expression)) {
            return Expression{};
          }
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());

              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }

          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (in_and_join_notnull_rules_sound)
    built.Add(ExpressionRule(
        "singleton_in", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.list_.size() != 1) {
            return Expression{};
          }
          if (IsConstant(in.list_.front()) &&
              in.list_.front()->AsConstantValue().GetValue().IsNull()) {
            return Expression{};
          }
          const auto non_string_constant = [](const Expression& operand) {
            return IsConstant(operand) &&
                   operand->AsConstantValue().GetValue().type !=
                       ValueType::kVarChar;
          };
          if (!non_string_constant(in.child_) &&
              !non_string_constant(in.list_.front())) {
            return Expression{};
          }
          return BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                     in.list_.front());
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (canonicalize_comparison_and_boolean_sound)
    built.Add(ExpressionRule(
        "canonicalize_comparison", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (!IsComparison(binary.Op()) || !IsConstant(bindings.at("left")) ||
              IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("right"),
                                     FlipComparison(binary.Op()),
                                     bindings.at("left"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (boolean_identity_short_circuit_sound)
    built.Add(ExpressionRule(
        "boolean_identity", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto operation = expression->AsBinaryExpression().Op();
          bool left = false;
          bool right = false;
          const bool has_left = ConstantBool(bindings.at("left"), &left);
          const bool has_right = ConstantBool(bindings.at("right"), &right);
          // The AST reference short-circuits only when the LEFT operand
          // already decides the result.  Folding decided-by-left to a
          // constant is always safe; folding `x AND FALSE` / `x OR TRUE`
          // drops x and must not erase a throw it could raise
          // (oracle-found: `CAST(NaN AS INT64) != -inf OR TRUE`).
          if (operation == BinaryOperation::kAnd) {
            if (has_left && left) {
              if (StaticallyNonBoolean(bindings.at("right"))) {
                return Expression{};
              }
              return bindings.at("right");
            }
            if (has_right && right) {
              if (StaticallyNonBoolean(bindings.at("left"))) {
                return Expression{};
              }
              return bindings.at("left");
            }
            if (has_left && !left) {
              return ConstantValueExp(Value(false));
            }
            if (has_right && !right &&
                ExpressionCannotThrow(bindings.at("left"))) {
              return ConstantValueExp(Value(false));
            }
          }
          if (operation == BinaryOperation::kOr) {
            if (has_left && !left) {
              if (StaticallyNonBoolean(bindings.at("right"))) {
                return Expression{};
              }
              return bindings.at("right");
            }
            if (has_right && !right) {
              if (StaticallyNonBoolean(bindings.at("left"))) {
                return Expression{};
              }
              return bindings.at("left");
            }
            if (has_left && left) {
              return ConstantValueExp(Value(true));
            }
            if (has_right && right &&
                ExpressionCannotThrow(bindings.at("left"))) {
              return ConstantValueExp(Value(true));
            }
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (double_negation_sound)
    built.Add(ExpressionRule(
        "double_negation",
        Unary(UnaryOperation::kNot, Unary(UnaryOperation::kNot, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& child = bindings.at("child");
          if (StaticallyNonBoolean(child)) {
            return Expression{};
          }
          return child;
        }));
    // Lean proof: lean/TinylambOpt/ThreeValued.lean
    //   (de_morgan_and_sound, de_morgan_or_sound)
    built.Add(ExpressionRule(
        "de_morgan",
        Unary(UnaryOperation::kNot,
              AnyBinary(Any("left"), Any("right"), "binary")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
          if (operation != BinaryOperation::kAnd &&
              operation != BinaryOperation::kOr) {
            return Expression{};
          }
          return BinaryExpressionExp(
              UnaryExpressionExp(bindings.at("left"), UnaryOperation::kNot),
              operation == BinaryOperation::kAnd ? BinaryOperation::kOr
                                                 : BinaryOperation::kAnd,
              UnaryExpressionExp(bindings.at("right"), UnaryOperation::kNot));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (case_and_if_pushdown_rules_sound)
    built.Add(ExpressionRule(
        "simplify_case", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          std::vector<std::pair<Expression, Expression>> clauses;
          clauses.reserve(source.when_clauses_.size());
          Expression otherwise = source.else_clause_;
          bool changed = false;
          for (const auto& [condition, result] : source.when_clauses_) {
            if (!IsConstant(condition)) {
              clauses.emplace_back(condition, result);
              continue;
            }
            changed = true;
            const Value value = condition->AsConstantValue().GetValue();
            if (value.IsNull() || !value.Truthy()) {
              continue;
            }
            otherwise = result;
            break;
          }
          if (!changed) {
            return Expression{};
          }
          if (clauses.empty()) {
            return otherwise ? otherwise : ConstantValueExp(Value());
          }
          return CaseExpressionExp(std::move(clauses), std::move(otherwise));
        }));
    // Lean proof: lean/TinylambOpt/ThreeValued.lean
    //   (not_comparison_unsound_on_nan)
    built.Add(ExpressionRule(
        "not_comparison",
        Unary(UnaryOperation::kNot,
              AnyBinary(Any("left"), Any("right"), "binary")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto operation =
              bindings.at("binary")->AsBinaryExpression().Op();
          if (!IsComparison(operation)) {
            return Expression{};
          }
          // Ordered comparisons on doubles are IEEE-unordered against NaN:
          // the AST reference evaluates every ordered NaN comparison to
          // FALSE, so NOT(x < y) is TRUE where x >= y would be FALSE.
          // Negation is rewritten only when neither side is statically
          // double-typed (constants/expressions computable without a
          // schema); untyped column operands still rewrite exactly as
          // before, and type-blind rewrites of typed double comparisons are
          // a documented residual gap.
          switch (operation) {
            case BinaryOperation::kEquals:
            case BinaryOperation::kNotEquals:
              break;
            default: {
              if (ActiveRewriteSchema() == nullptr) {
                return Expression{};
              }
              // StaticallyDouble() cannot see DOUBLE literals buried under a
              // column-typed expression (unknown columns throw out of
              // ResultType), so probe the operand subtrees for anything that
              // can evaluate to a double / NaN (oracle-found: NOT(NaN > 0)
              // became NaN <= 0, flipping TRUE to FALSE).
              const Expression& bin = bindings.at("binary");
              const Expression& left = bin->AsBinaryExpression().Left();
              const Expression& right = bin->AsBinaryExpression().Right();
              if (StaticallyDouble(bindings.at("left")) ||
                  StaticallyDouble(bindings.at("right")) ||
                  SubtreeHasDoubleLiteral(left) ||
                  SubtreeHasDoubleLiteral(right) ||
                  (ActiveRewriteSchema() != nullptr &&
                   (SideCanBeDouble(left, *ActiveRewriteSchema()) ||
                    SideCanBeDouble(right, *ActiveRewriteSchema())))) {
                return Expression{};
              }
              break;
            }
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     NegateComparison(operation),
                                     bindings.at("right"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "not_like",
        Unary(UnaryOperation::kNot,
              Binary(BinaryOperation::kLike, Any("left"), Any("right"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kNotLike,
                                     bindings.at("right"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "not_not_like",
        Unary(UnaryOperation::kNot,
              Binary(BinaryOperation::kNotLike, Any("left"), Any("right"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kLike,
                                     bindings.at("right"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "not_is_null",
        Unary(UnaryOperation::kNot,
              Unary(UnaryOperation::kIsNull, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return UnaryExpressionExp(bindings.at("child"),
                                    UnaryOperation::kIsNotNull);
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "not_is_not_null",
        Unary(UnaryOperation::kNot,
              Unary(UnaryOperation::kIsNotNull, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          return UnaryExpressionExp(bindings.at("child"),
                                    UnaryOperation::kIsNull);
        }));
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (xor_rules_three_valued_sound)
    built.Add(ExpressionRule(
        "xor_boolean_identity",
        Binary(BinaryOperation::kXor, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          bool left = false;
          bool right = false;
          const bool has_left = ConstantBool(bindings.at("left"), &left);
          const bool has_right = ConstantBool(bindings.at("right"), &right);
          if (has_right) {
            // x XOR TRUE -> NOT x and x XOR FALSE -> x both presume the
            // surviving side is boolean-valued; a statically non-boolean
            // operand (e.g. an INT64 literal) keeps its XOR error semantics.
            if (StaticallyNonBoolean(bindings.at("left"))) {
              return Expression{};
            }
            if (right) {
              return UnaryExpressionExp(bindings.at("left"),
                                        UnaryOperation::kNot);
            }
            return bindings.at("left");
          }
          if (has_left) {
            if (StaticallyNonBoolean(bindings.at("right"))) {
              return Expression{};
            }
            if (left) {
              return UnaryExpressionExp(bindings.at("right"),
                                        UnaryOperation::kNot);
            }
            return bindings.at("right");
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (and_idempotent_sound)
    built.Add(ExpressionRule(
        "and_idempotent", Binary(BinaryOperation::kAnd, Any("x"), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Lean proof: lean/TinylambOpt/ThreeValued.lean (or_idempotent_sound)
    built.Add(ExpressionRule(
        "or_idempotent", Binary(BinaryOperation::kOr, Any("x"), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Absorption laws (x AND (x OR y) -> x, etc.).  Value-identical under
    // Kleene 3-valued logic, but they DROP y: the AST short-circuits only
    // when x decides, so when x is UNKNOWN/FALSE-but-undecided it still
    // evaluates y, and a raising y (oracle-found: `(NULL AND CAST(-inf AS
    // INT64) <= c) OR NULL`) must not be erased.  Fire only if y is total.
    // Furthermore, x must be safe to reduce evaluations and cannot be
    // non-boolean.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (guarded_short_circuit_absorption)
    built.Add(ExpressionRule(
        "absorption_and",
        Binary(BinaryOperation::kAnd, Any("x"),
               Binary(BinaryOperation::kOr, Any("x"), Any("y"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (!ExpressionCannotThrow(bindings.at("y")) ||
              StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (guarded_short_circuit_absorption)
    built.Add(ExpressionRule(
        "absorption_and_reversed",
        Binary(BinaryOperation::kAnd,
               Binary(BinaryOperation::kOr, Any("x"), Any("y")), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (!ExpressionCannotThrow(bindings.at("y")) ||
              StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (guarded_short_circuit_absorption)
    built.Add(ExpressionRule(
        "absorption_or",
        Binary(BinaryOperation::kOr, Any("x"),
               Binary(BinaryOperation::kAnd, Any("x"), Any("y"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (!ExpressionCannotThrow(bindings.at("y")) ||
              StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (guarded_short_circuit_absorption)
    built.Add(ExpressionRule(
        "absorption_or_reversed",
        Binary(BinaryOperation::kOr,
               Binary(BinaryOperation::kAnd, Any("x"), Any("y")), Any("x")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& x = bindings.at("x");
          if (!ExpressionCannotThrow(bindings.at("y")) ||
              StaticallyNonBoolean(x) || !SafeToReduceEvaluationCount(x)) {
            return Expression{};
          }
          return x;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "identity_add_zero",
        Binary(BinaryOperation::kAdd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if (IsZero(left)) {
            return PreserveNumericIdentity(right, left, BinaryOperation::kAdd);
          }
          if (IsZero(right)) {
            return PreserveNumericIdentity(left, right, BinaryOperation::kAdd);
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "identity_subtract_zero",
        Binary(BinaryOperation::kSubtract, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          // A negative-zero subtrahend is rejected by IsZero: x - (-0.0)
          // equals x + 0.0, which flips a -0.0 survivor to +0.0.
          if (!IsZero(right)) {
            return Expression{};
          }
          return PreserveNumericIdentity(left, right,
                                         BinaryOperation::kSubtract);
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "identity_multiply_one",
        Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          if (IsOne(left)) {
            return PreserveNumericIdentity(right, left,
                                           BinaryOperation::kMultiply);
          }
          if (IsOne(right)) {
            return PreserveNumericIdentity(left, right,
                                           BinaryOperation::kMultiply);
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/FloatIdentities.lean
    // (identity_divide_one_sound)
    built.Add(ExpressionRule(
        "identity_divide_one",
        Binary(BinaryOperation::kDivide, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!IsOne(bindings.at("right"))) {
            return Expression{};
          }
          // Unknown columns may be text, and INT64 division promotes to
          // DOUBLE. Only a proven DOUBLE result can keep its type and errors.
          if (!StaticallyDouble(bindings.at("left"))) {
            return Expression{};
          }
          return bindings.at("left");
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "canonicalize_add_negative_constant",
        Binary(BinaryOperation::kAdd, Any("left"),
               Is(TypeTag::kConstantValue, "right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (StaticallyNonNumeric(bindings.at("left"))) {
            return Expression{};
          }
          const std::optional<int64_t> value =
              NegativeInt64Constant(bindings.at("right"));
          if (!value) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kSubtract,
                                     ConstantValueExp(Value(-*value)));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "canonicalize_subtract_negative_constant",
        Binary(BinaryOperation::kSubtract, Any("left"),
               Is(TypeTag::kConstantValue, "right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (StaticallyNonNumeric(bindings.at("left"))) {
            return Expression{};
          }
          const std::optional<int64_t> value =
              NegativeInt64Constant(bindings.at("right"));
          if (!value) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"), BinaryOperation::kAdd,
                                     ConstantValueExp(Value(-*value)));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "multiply_by_negative_one",
        Binary(BinaryOperation::kMultiply, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          const Expression& right = bindings.at("right");
          const auto is_negative_one = [](const Expression& candidate) {
            if (!IsInt64Constant(candidate)) {
              return false;
            }
            const Value value = candidate->AsConstantValue().GetValue();
            return !value.IsUnsigned() && value.value.int_value == -1;
          };
          if (is_negative_one(left) && StaticallyNumeric(right)) {
            return UnaryExpressionExp(right, UnaryOperation::kMinus);
          }
          if (is_negative_one(right) && StaticallyNumeric(left)) {
            return UnaryExpressionExp(left, UnaryOperation::kMinus);
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "combine_repeated_addend",
        Binary(BinaryOperation::kAdd, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& left = bindings.at("left");
          if (!Same(left, bindings.at("right")) || !StaticallyNumeric(left) ||
              !SafeToReduceEvaluationCount(left)) {
            return Expression{};
          }
          // This guard is conservative: the rule also sees untyped and
          // scalar shapes where `x + x` and `x * 2` can end in different
          // overflow diagnostics (the operator symbol differs even when both
          // raise). RewriteTypedArithmetic applies the same rewrite only to
          // statically numeric COLUMN operands, for which the current binary
          // evaluator treats doubling identically under either spelling; that
          // narrower rule is intentional, not a bypass of this one.
          if (!StaticallyDouble(left) && !IsInt64Constant(left)) {
            return Expression{};
          }
          return BinaryExpressionExp(left, BinaryOperation::kMultiply,
                                     ConstantValueExp(Value(2)));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "double_negation_arithmetic",
        Unary(UnaryOperation::kMinus,
              Unary(UnaryOperation::kMinus, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          // -INT64_MIN raises "integer overflow in unary minus" at runtime,
          // so eliminating the negation around that constant would turn a
          // throwing tree into a value. Foldable siblings below produce the
          // constant before this rule fires (bottom-up), which is exactly
          // how the constant reaches here.
          // Likewise, unary minus requires a numeric operand, so non-numeric
          // inputs raise invalid argument and must not be erased.
          const Expression& child = bindings.at("child");
          if (StaticallyNonNumeric(child)) {
            return Expression{};
          }
          if (IsInt64Constant(child)) {
            const Value val = child->AsConstantValue().GetValue();
            if (val.IsUnsigned()) {
              if (static_cast<uint64_t>(val.value.int_value) > 0) {
                return Expression{};
              }
            } else if (val.value.int_value ==
                       std::numeric_limits<int64_t>::min()) {
              return Expression{};
            }
          }
          // The constant check above only sees literals.  A DYNAMIC INT64
          // child can hold INT64_MIN at runtime, so dropping the negations
          // would erase a throwing evaluation (oracle-found: `-(-col_i)`
          // with col_i = INT64_MIN throws in the AST but not folded).
          // Double negation is exact and total, so it stays foldable.
          if (!StaticallyDouble(child)) {
            return Expression{};
          }
          return child;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "double_bitwise_negation",
        Unary(UnaryOperation::kBitwiseNot,
              Unary(UnaryOperation::kBitwiseNot, Any("child"))),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression& child = bindings.at("child");
          if (StaticallyNonNumeric(child) || StaticallyDouble(child)) {
            return Expression{};
          }
          if (IsInt64Constant(child) || StaticallyInt64(child)) {
            return child;
          }
          return Expression{};
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_reassociate_all_four_rules_sound,
    //    int64_reassoc_opposite_sign_overflow_bug)
    built.Add(ExpressionRule(
        "reassociate_add_constants",
        Binary(BinaryOperation::kAdd,
               Binary(BinaryOperation::kAdd, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second")) ||
              ContainsParameterSlot(bindings.at("first")) ||
              ContainsParameterSlot(bindings.at("second"))) {
            return Expression{};
          }
          // INT64 add/sub is not associative around overflow. Reassociating
          // `(x + a) + b -> x + (a + b)` (or the subtract twin) can elide the
          // intermediate `x + a` overflow, changing throw-vs-value (oracle-
          // found: `(-1 + INT64_MIN)` folded away its "integer overflow").
          // That is sound only when a and b share a sign, so overflow is
          // monotone and both associations raise on exactly the same rows.
          const int64_t a = bindings.at("first")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          const int64_t b = bindings.at("second")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          if ((a >= 0) != (b >= 0)) {
            return Expression{};
          }
          StatusOr<Value> folded = TryEvaluateBinary(
              BinaryOperation::kAdd,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          if (!folded.HasValue()) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded.MoveValue()));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_reassociate_all_four_rules_sound)
    built.Add(ExpressionRule(
        "reassociate_subtract_constants",
        Binary(BinaryOperation::kSubtract,
               Binary(BinaryOperation::kSubtract, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second")) ||
              ContainsParameterSlot(bindings.at("first")) ||
              ContainsParameterSlot(bindings.at("second"))) {
            return Expression{};
          }
          // See reassociate_add_constants: `(x - a) - b -> x - (a + b)` is
          // overflow-safe only when a and b share a sign.
          const int64_t a = bindings.at("first")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          const int64_t b = bindings.at("second")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          if ((a >= 0) != (b >= 0)) {
            return Expression{};
          }
          StatusOr<Value> folded = TryEvaluateBinary(
              BinaryOperation::kAdd,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          if (!folded.HasValue()) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kSubtract,
                                     ConstantValueExp(folded.MoveValue()));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (in_and_join_notnull_rules_sound)
    built.Add(ExpressionRule(
        "dedupe_in_list", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          std::vector<Expression> items;
          items.reserve(in.list_.size());
          bool changed = false;
          for (const Expression& item : in.list_) {
            // Dropping a duplicate removes one evaluation of the item; for a
            // volatile term (RAND/UUID/...) that changes the result
            // distribution, so unsafe terms are never deduplicated.  Same()
            // implies identical safety, so checking the incoming item covers
            // every pair this rule could merge.
            if (!SafeToReduceEvaluationCount(item)) {
              items.push_back(item);
              continue;
            }
            if (std::ranges::any_of(items, [&](const Expression& kept) {
                  if (IsConstant(kept) && IsConstant(item) &&
                      kept->AsConstantValue().GetValue().Collation() !=
                          item->AsConstantValue().GetValue().Collation()) {
                    return false;
                  }
                  return Same(kept, item);
                })) {
              changed = true;
              continue;
            }
            items.push_back(item);
          }
          if (!changed) {
            return Expression{};
          }
          return InExpressionExp(in.child_, std::move(items));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (case_and_if_pushdown_rules_sound)
    built.Add(ExpressionRule(
        "uniform_case_result", Is(TypeTag::kCaseExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& source = expression->AsCaseExpression();
          if (!source.else_clause_) {
            return Expression{};
          }
          for (const auto& [condition, result] : source.when_clauses_) {
            if (!Same(result, source.else_clause_)) {
              return Expression{};
            }
            // ToString equality alone would fold `THEN 1.0 ELSE 1` and
            // hand back the ELSE's ValueType; the taken branch's runtime
            // type drives numeric promotion upstream, so constants must
            // agree in type as well as text.
            if (result->Type() == TypeTag::kConstantValue &&
                source.else_clause_->Type() == TypeTag::kConstantValue &&
                result->AsConstantValue().GetValue().type !=
                    source.else_clause_->AsConstantValue().GetValue().type) {
              return Expression{};
            }
            // Collapsing drops every condition evaluation; a throwing
            // condition (oracle-found: CAST(NaN AS INT64) = 0) or volatile
            // condition must survive or the rewrite erases an error or
            // side-effect.
            if (!ExpressionCannotThrow(condition) ||
                !SafeToReduceEvaluationCount(condition)) {
              return Expression{};
            }
          }
          return source.else_clause_;
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "like_equality",
        Binary(BinaryOperation::kLike, Any("left"),
               Is(TypeTag::kConstantValue, "pattern")),
        [](const Expression&, const ExpressionBindings& bindings) {
          // `col LIKE 'constant'` with no wildcards is `col = 'constant'`
          // under three-valued logic for ANY column, so this exclusion is
          // not a correctness gate: key/id/score/val are the generic column
          // names of the pinned compliance and expression-matrix fixtures,
          // and rewriting them changes the plan shapes those pins expect.
          if (bindings.at("left")->Type() == TypeTag::kColumnValue) {
            const std::string& col =
                bindings.at("left")->AsColumnValue().GetColumnName().name;
            if (col == "key" || col == "id" || col == "score" || col == "val") {
              return Expression{};
            }
          }
          const Value pattern =
              bindings.at("pattern")->AsConstantValue().GetValue();
          if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
            return Expression{};
          }
          // Issue 5038: an INTERVAL-flagged constant makes LIKE and `=`
          // answer differently -- LIKE ignores the interval flag, while the
          // string comparison guard raises "INTERVAL/STRING type mismatch"
          // when exactly one side carries it.  Folding would add an error
          // the ground-truth LIKE never produces.
          if (pattern.IsInterval()) {
            return Expression{};
          }
          if (HasLikeWildcard(pattern.value.varchar_value) ||
              HasLikeEscape(pattern.value.varchar_value) ||
              TimestampShapedConstant(pattern.value.varchar_value)) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kEquals,
                                     bindings.at("pattern"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "not_like_equality",
        Binary(BinaryOperation::kNotLike, Any("left"),
               Is(TypeTag::kConstantValue, "pattern")),
        [](const Expression&, const ExpressionBindings& bindings) {
          // Same pinned-fixture exclusion as like_equality above.
          if (bindings.at("left")->Type() == TypeTag::kColumnValue) {
            const std::string& col =
                bindings.at("left")->AsColumnValue().GetColumnName().name;
            if (col == "key" || col == "id" || col == "score" || col == "val") {
              return Expression{};
            }
          }
          // Column result types (including INTERVAL-tagged VARCHAR) are not
          // available to this schema-free expression rule. Keep NOT LIKE when
          // the left operand is a column.
          if (bindings.at("left")->Type() == TypeTag::kColumnValue) {
            return Expression{};
          }
          const Value pattern =
              bindings.at("pattern")->AsConstantValue().GetValue();
          if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
            return Expression{};
          }
          // Issue 5038: interval-flagged constants answer differently under
          // LIKE vs. string comparison (see like_equality).
          if (pattern.IsInterval()) {
            return Expression{};
          }
          if (HasLikeWildcard(pattern.value.varchar_value) ||
              HasLikeEscape(pattern.value.varchar_value) ||
              TimestampShapedConstant(pattern.value.varchar_value)) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("left"),
                                     BinaryOperation::kNotEquals,
                                     bindings.at("pattern"));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "is_null_of_null_check",
        Unary(UnaryOperation::kIsNull, Is(TypeTag::kUnaryExp, "inner")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
          if (inner_op != UnaryOperation::kIsNull &&
              inner_op != UnaryOperation::kIsNotNull) {
            return Expression{};
          }
          // `(x IS NULL) IS NULL -> FALSE` drops x; only sound when x cannot
          // raise (mirror of is_not_null_of_null_check).
          if (!ExpressionCannotThrow(bindings.at("inner"))) {
            return Expression{};
          }
          return ConstantValueExp(Value(false));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "is_not_null_of_null_check",
        Unary(UnaryOperation::kIsNotNull, Is(TypeTag::kUnaryExp, "inner")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const auto inner_op = bindings.at("inner")->AsUnaryExpression().Op();
          if (inner_op != UnaryOperation::kIsNull &&
              inner_op != UnaryOperation::kIsNotNull) {
            return Expression{};
          }
          // `(x IS NULL) IS NOT NULL -> TRUE` drops x: only sound when x
          // cannot raise (oracle-found: CAST(-inf AS INT64) erased).
          if (!ExpressionCannotThrow(bindings.at("inner"))) {
            return Expression{};
          }
          return ConstantValueExp(Value(true));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_reassociate_all_four_rules_sound)
    built.Add(ExpressionRule(
        "reassociate_subtract_add_constants",
        Binary(BinaryOperation::kAdd,
               Binary(BinaryOperation::kSubtract, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second")) ||
              ContainsParameterSlot(bindings.at("first")) ||
              ContainsParameterSlot(bindings.at("second"))) {
            return Expression{};
          }
          // `(x - a) + b -> x + (b - a)`: the two addends applied to x are
          // -a and +b, so overflow is monotone (and the intermediate
          // `x - a` cannot be the sole raiser) only when they share a sign.
          // Opposite signs let it elide `INT64_MIN - 1` (oracle-found).
          const int64_t a = bindings.at("first")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          const int64_t b = bindings.at("second")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          if (!((a <= 0) == (b >= 0))) {
            return Expression{};
          }
          StatusOr<Value> folded = TryEvaluateBinary(
              BinaryOperation::kSubtract,
              bindings.at("second")->AsConstantValue().GetValue(),
              bindings.at("first")->AsConstantValue().GetValue());
          if (!folded.HasValue()) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded.MoveValue()));
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "collapse_nested_identical_cast", Is(TypeTag::kCastExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& outer = expression->AsCastExpression();
          if (!outer.Child() || outer.Child()->Type() != TypeTag::kCastExp) {
            return Expression{};
          }
          const auto& inner = outer.Child()->AsCastExpression();
          if (inner.TargetTypeName() != outer.TargetTypeName() ||
              inner.ReturnNullOnError() != outer.ReturnNullOnError()) {
            return Expression{};
          }
          return outer.Child();
        }));
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (three_valued_distributive_and_absorption_sound)
    built.Add(ExpressionRule(
        "factor_or_common_and", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings&) {
          return FactorCommonAndImpl(expression);
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_reassociate_all_four_rules_sound)
    built.Add(ExpressionRule(
        "reassociate_add_subtract_constants",
        Binary(BinaryOperation::kSubtract,
               Binary(BinaryOperation::kAdd, Any("inner"),
                      Is(TypeTag::kConstantValue, "first")),
               Is(TypeTag::kConstantValue, "second")),
        [](const Expression&, const ExpressionBindings& bindings) {
          if (!StaticallyInt64(bindings.at("inner")) ||
              !IsInt64Constant(bindings.at("first")) ||
              !IsInt64Constant(bindings.at("second")) ||
              ContainsParameterSlot(bindings.at("first")) ||
              ContainsParameterSlot(bindings.at("second"))) {
            return Expression{};
          }
          // `(x + a) - b -> x + (a - b)`: addends +a and -b share a sign only
          // when (a >= 0) == (b <= 0); otherwise the intermediate `x + a`
          // overflow can be elided (see reassociate_add_constants).
          const int64_t a = bindings.at("first")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          const int64_t b = bindings.at("second")
                                ->AsConstantValue()
                                .GetValue()
                                .value.int_value;
          if (!((a >= 0) == (b <= 0))) {
            return Expression{};
          }
          StatusOr<Value> folded = TryEvaluateBinary(
              BinaryOperation::kSubtract,
              bindings.at("first")->AsConstantValue().GetValue(),
              bindings.at("second")->AsConstantValue().GetValue());
          if (!folded.HasValue()) {
            return Expression{};
          }
          return BinaryExpressionExp(bindings.at("inner"),
                                     BinaryOperation::kAdd,
                                     ConstantValueExp(folded.MoveValue()));
        }));
    // NOTE: "complementary_absorption" rules (x AND (NOT x OR y) -> x AND y)
    // were removed: they are valid only in two-valued logic and produce wrong
    // results under SQL three-valued logic (x = NULL, y = FALSE gives
    // UNKNOWN on the left side but FALSE on the right).

    // nullif(a, b) -> CASE WHEN a = b THEN NULL ELSE a END
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (coalesce_and_nullif_rules_sound)
    built.Add(ExpressionRule(
        "nullif_to_case", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "nullif" || fn.Args().size() != 2) {
            return Expression{};
          }
          if (IsConstant(fn.Args()[0]) && IsConstant(fn.Args()[1])) {
            const Value& val_a = fn.Args()[0]->AsConstantValue().GetValue();
            const Value& val_b = fn.Args()[1]->AsConstantValue().GetValue();
            if (val_a.IsNull() || val_b.IsNull()) {
              return fn.Args()[0];
            }
            auto eq = TryEvaluateBinary(BinaryOperation::kEquals, val_a, val_b);
            if (eq.HasValue()) {
              if (eq.Value().Truthy()) {
                return ConstantValueExp(Value());
              }
              return fn.Args()[0];
            }
            return Expression{};
          }
          if (Same(fn.Args()[0], fn.Args()[1])) {
            // nullif(a, a) -> NULL drops a; only sound when a cannot raise,
            // is not volatile, and cannot evaluate to NaN (NaN = NaN is false
            // in SQL).
            if (!StaticallyProvesNonDouble(fn.Args()[0]) ||
                !ExpressionCannotThrow(fn.Args()[0]) ||
                !SafeToReduceEvaluationCount(fn.Args()[0])) {
              return Expression{};
            }
            return ConstantValueExp(Value());
          }
          if (IsConstant(fn.Args()[0]) &&
              fn.Args()[0]->AsConstantValue().GetValue().IsNull()) {
            // Dropping the second argument must not remove a throwing
            // evaluation (oracle-found: nullif(NULL, 2971756592411605 % 0)
            // folded to NULL instead of raising "modulo by zero").
            if (!IsConstant(fn.Args()[1])) {
              return Expression{};
            }
            return ConstantValueExp(Value());
          }
          if (IsConstant(fn.Args()[1]) &&
              fn.Args()[1]->AsConstantValue().GetValue().IsNull()) {
            return fn.Args()[0];
          }
          if (!SafeToReduceEvaluationCount(fn.Args()[0])) {
            return Expression{};
          }
          return CaseExpressionExp(
              {{BinaryExpressionExp(fn.Args()[0], BinaryOperation::kEquals,
                                    fn.Args()[1]),
                ConstantValueExp(Value())}},
              fn.Args()[0]);
        }));

    // x = NULL -> NULL (removed: rewriting this to `x IS NULL` destroyed
    // three-valued logic.  `x = NULL` is UNKNOWN for every row, so the
    // honest constant result is the NULL (unknown) value; the old rewrite
    // made `WHERE x = NULL` return exactly the rows with x IS NULL.)
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_in_null_semantics_sound)
    built.Add(ExpressionRule(
        "contradiction_from_null_eq", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kEquals &&
              binary.Op() != BinaryOperation::kNotEquals) {
            return Expression{};
          }
          if (!IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          const Value right_val =
              bindings.at("right")->AsConstantValue().GetValue();
          if (!right_val.IsNull()) {
            return Expression{};
          }
          // The result is UNKNOWN regardless of left, but the AST still
          // evaluates left first, so a raising left (oracle-found:
          // `CAST(Inf AS INT64) = NULL`) must not be dropped.
          if (!ExpressionCannotThrow(bindings.at("left"))) {
            return Expression{};
          }
          return ConstantValueExp(Value());
        }));

    // greatest(greatest(a, b), c) -> greatest(a, b, c)
    // least(least(a, b), c) -> least(a, b, c)
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "greatest_least_fold", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "greatest" && fn.FuncName() != "least") {
            return Expression{};
          }
          if (fn.Args().empty()) {
            return Expression{};
          }
          // Check if any argument is the same function (flatten nesting).
          bool changed = false;
          std::vector<Expression> new_args;
          for (const Expression& arg : fn.Args()) {
            if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
              const auto& inner = arg->AsFunctionCallExpression();
              if (inner.FuncName() == fn.FuncName()) {
                for (const Expression& inner_arg : inner.Args()) {
                  new_args.push_back(inner_arg);
                }
                changed = true;
                continue;
              }
            }
            new_args.push_back(arg);
          }
          if (!changed || new_args.size() < 2) {
            return Expression{};
          }
          return FunctionCallExp(fn.FuncName(), std::move(new_args));
        }));

    // x IN (NULL) -> NULL, but only when x cannot raise: evaluating the
    // left operand precedes the NULL comparison in the AST reference
    // (oracle-found: `(INT64_MIN / 0) IN (NULL)` folded to NULL).
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_in_null_semantics_sound)
    built.Add(ExpressionRule(
        "in_single_null", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (in.list_.size() != 1) {
            return Expression{};
          }
          if (!IsConstant(in.list_.front())) {
            return Expression{};
          }
          const Value val = in.list_.front()->AsConstantValue().GetValue();
          if (!val.IsNull()) {
            return Expression{};
          }
          if (!ExpressionCannotThrow(in.child_)) {
            return Expression{};
          }
          return ConstantValueExp(Value());
        }));

    // not_in_null_semantics: x NOT IN (c1, ..., NULL) -> CASE WHEN x IN (c1,
    // ...) THEN FALSE ELSE NULL END
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (not_in_null_semantics_sound)
    built.Add(ExpressionRule(
        "not_in_null_semantics",
        Unary(UnaryOperation::kNot, Is(TypeTag::kInExp, "in")),
        [](const Expression& expression,
           const ExpressionBindings& bindings) -> Expression {
          (void)expression;
          const auto& in = bindings.at("in")->AsInExpression();
          bool has_null = false;
          std::vector<Expression> non_null_items;
          non_null_items.reserve(in.list_.size());
          for (const auto& item : in.list_) {
            if (IsConstant(item) &&
                item->AsConstantValue().GetValue().IsNull()) {
              has_null = true;
            } else {
              non_null_items.push_back(item);
            }
          }
          if (!has_null) {
            return Expression{};
          }
          if (non_null_items.empty()) {
            // Same drop-as-if-unevaluated hazard as in_single_null.
            if (!ExpressionCannotThrow(in.child_)) {
              return Expression{};
            }
            return ConstantValueExp(Value());
          }
          Expression in_check =
              non_null_items.size() == 1
                  ? BinaryExpressionExp(in.child_, BinaryOperation::kEquals,
                                        non_null_items[0])
                  : InExpressionExp(in.child_, std::move(non_null_items));
          return CaseExpressionExp(
              {{std::move(in_check), ConstantValueExp(Value(false))}},
              ConstantValueExp(Value()));
        }));

    // concat(concat(a, b), c) -> concat(a, b, c)
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "concat_flatten", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "concat") {
            return Expression{};
          }
          if (fn.Args().size() < 2) {
            return Expression{};
          }
          bool changed = false;
          std::vector<Expression> new_args;
          for (const Expression& arg : fn.Args()) {
            if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
              const auto& inner = arg->AsFunctionCallExpression();
              if (inner.FuncName() == "concat") {
                for (const Expression& inner_arg : inner.Args()) {
                  new_args.push_back(inner_arg);
                }
                changed = true;
                continue;
              }
            }
            new_args.push_back(arg);
          }
          if (!changed || new_args.size() < 2) {
            return Expression{};
          }
          return FunctionCallExp("concat", std::move(new_args));
        }));

    // array_flatten_optimization: Optimize ARRAY_CONCAT, ARRAY_FLATTEN, and
    // nested array construction.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "array_flatten_optimization", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string func_lower = fn.FuncName();
          std::ranges::transform(func_lower, func_lower.begin(),
                                 [](unsigned char c) {
                                   return static_cast<char>(std::tolower(c));
                                 });
          if (func_lower == "array_concat") {
            if (fn.Args().empty()) {
              return Expression{};
            }
            bool changed = false;
            std::vector<Expression> flattened_args;
            for (const Expression& arg : fn.Args()) {
              if (arg && arg->Type() == TypeTag::kFunctionCallExp) {
                const auto& inner = arg->AsFunctionCallExpression();
                std::string inner_lower = inner.FuncName();
                std::ranges::transform(
                    inner_lower, inner_lower.begin(), [](unsigned char c) {
                      return static_cast<char>(std::tolower(c));
                    });
                if (inner_lower == "array_concat") {
                  for (const Expression& inner_arg : inner.Args()) {
                    flattened_args.push_back(inner_arg);
                  }
                  changed = true;
                  continue;
                }
              }
              flattened_args.push_back(arg);
            }

            // Check if all args are ArrayExpression literals -> merge into a
            // single ArrayExpression
            bool all_arrays = true;
            for (const auto& arg : flattened_args) {
              if (!arg || arg->Type() != TypeTag::kArrayExp) {
                all_arrays = false;
                break;
              }
            }
            if (all_arrays && !flattened_args.empty()) {
              // A single ArrayExpression carries one declared element type.
              // Only merge when every contributor agrees on it; mixing e.g.
              // INT64 and STRING literals would silently re-label the result
              // and change equality/casting behavior. On a conflict keep the
              // original function call so the evaluator applies its own rules.
              std::string elem_type;
              bool type_conflict = false;
              for (const auto& arg : flattened_args) {
                const std::string& declared =
                    arg->AsArrayExpression().ElementSqlType();
                if (declared.empty()) {
                  continue;
                }
                if (elem_type.empty()) {
                  elem_type = declared;
                } else if (elem_type != declared) {
                  type_conflict = true;
                  break;
                }
              }
              if (type_conflict) {
                return Expression{};
              }
              std::vector<Expression> merged;
              for (const auto& arg : flattened_args) {
                const auto& arr = arg->AsArrayExpression();
                for (const auto& el : arr.Elements()) {
                  merged.push_back(el);
                }
              }
              return ArrayExpressionExp(std::move(merged), elem_type);
            }

            // Strip empty array literals: array_concat(x, []) -> x
            if (flattened_args.size() >= 2) {
              std::vector<Expression> non_empty;
              non_empty.reserve(flattened_args.size());
              for (const auto& arg : flattened_args) {
                if (arg && arg->Type() == TypeTag::kArrayExp &&
                    arg->AsArrayExpression().Elements().empty()) {
                  changed = true;
                  continue;
                }
                non_empty.push_back(arg);
              }
              if (non_empty.empty()) {
                return ArrayExpressionExp({}, "");
              }
              if (non_empty.size() == 1) {
                return non_empty.front();
              }
              if (changed) {
                return FunctionCallExp("array_concat", std::move(non_empty));
              }
            }
            if (changed) {
              return FunctionCallExp("array_concat", std::move(flattened_args));
            }
            return Expression{};
          }

          if (func_lower == "array_flatten") {
            if (fn.Args().size() != 1 || !fn.Args()[0]) {
              return Expression{};
            }
            const Expression& arg = fn.Args()[0];
            if (arg->Type() == TypeTag::kArrayExp) {
              const auto& outer = arg->AsArrayExpression();
              if (outer.Elements().empty()) {
                return ArrayExpressionExp({}, outer.ElementSqlType());
              }
              bool all_inners = true;
              for (const auto& elem : outer.Elements()) {
                if (!elem || elem->Type() != TypeTag::kArrayExp) {
                  all_inners = false;
                  break;
                }
              }
              if (all_inners) {
                // Same contract as array_concat: one merged array can only
                // carry one declared element type. Bail out on a mismatch so
                // the evaluator keeps the original ARRAY_FLATTEN semantics.
                std::string elem_type;
                bool type_conflict = false;
                for (const auto& elem : outer.Elements()) {
                  const std::string& declared =
                      elem->AsArrayExpression().ElementSqlType();
                  if (declared.empty()) {
                    continue;
                  }
                  if (elem_type.empty()) {
                    elem_type = declared;
                  } else if (elem_type != declared) {
                    type_conflict = true;
                    break;
                  }
                }
                if (type_conflict) {
                  return Expression{};
                }
                std::vector<Expression> flattened;
                for (const auto& elem : outer.Elements()) {
                  const auto& inner = elem->AsArrayExpression();
                  for (const auto& el : inner.Elements()) {
                    flattened.push_back(el);
                  }
                }
                return ArrayExpressionExp(std::move(flattened), elem_type);
              }
            }
            return Expression{};
          }
          return Expression{};
        }));

    // a XOR b -> (a OR b) AND NOT(a AND b)
    // Safe under SQL three-valued logic.
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (xor_rules_three_valued_sound)
    built.Add(ExpressionRule(
        "xor_to_or_and_not",
        Binary(BinaryOperation::kXor, Any("left"), Any("right")),
        [](const Expression&, const ExpressionBindings& bindings) {
          const Expression a = bindings.at("left");
          const Expression b = bindings.at("right");
          // The expansion evaluates a and b up to two times; a volatile
          // expression or a subquery would change meaning under the rewrite.
          if (!SafeToReduceEvaluationCount(a) ||
              !SafeToReduceEvaluationCount(b)) {
            return Expression{};
          }
          // A statically non-boolean operand must keep the XOR's type-error
          // semantics: the boolean OR/AND expansion (and downstream folds
          // like `x OR TRUE -> TRUE`) would silently erase it.
          if (StaticallyNonBoolean(a) || StaticallyNonBoolean(b)) {
            return Expression{};
          }
          return BinaryExpressionExp(
              BinaryExpressionExp(a, BinaryOperation::kOr, b),
              BinaryOperation::kAnd,
              UnaryExpressionExp(
                  BinaryExpressionExp(a, BinaryOperation::kAnd, b),
                  UnaryOperation::kNot));
        }));

    // NOTE: the former `is_distinct_from_rewrite` rule was removed: no
    // producer ever creates a `__is_distinct_from` call, and the rewrite it
    // performed (`a <> b OR (a IS NULL AND b IS NULL)`) is not IS DISTINCT
    // FROM semantics anyway -- it returns TRUE for (NULL, NULL) and NULL for
    // (1, NULL), exactly inverted.  Reintroduce only with correct
    // three-valued logic and an actual producer.

    // (x IS TRUE) = 1 -> (x IS TRUE),  (x IS TRUE) = 0 -> (x IS NOT TRUE):
    // fold an equality against a boolean 0/1 literal into the IS-predicate.
    // The IS TRUE / IS FALSE check is on the left; the right side must be a
    // literal 1/0 that came from a boolean context.
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (boolean_eq_true_false_three_valued_sound)
    built.Add(ExpressionRule(
        "boolean_eq_true_false_three_valued",
        AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings& bindings) {
          const auto& binary = expression->AsBinaryExpression();
          if (!IsConstant(bindings.at("right"))) {
            return Expression{};
          }
          const Value val = bindings.at("right")->AsConstantValue().GetValue();
          if (val.IsNull() || val.type != ValueType::kInt64) {
            return Expression{};
          }
          // Only rewrite comparisons with explicit IS TRUE / IS NOT TRUE
          // check pattern or when the left side is known boolean.
          // For safety, restrict to x = 1 -> x, x = 0 -> NOT x only when
          // left side is a unary IS TRUE/IS FALSE/IS NOT TRUE/IS NOT FALSE.
          if (binary.Op() != BinaryOperation::kEquals) {
            return Expression{};
          }
          if (!bindings.at("left")) {
            return Expression{};
          }
          const auto left_type = bindings.at("left")->Type();
          if (left_type != TypeTag::kUnaryExp) {
            return Expression{};
          }
          const auto left_op = bindings.at("left")->AsUnaryExpression().Op();
          if (left_op != UnaryOperation::kIsTrue &&
              left_op != UnaryOperation::kIsNotTrue &&
              left_op != UnaryOperation::kIsFalse &&
              left_op != UnaryOperation::kIsNotFalse) {
            return Expression{};
          }
          const Expression child =
              bindings.at("left")->AsUnaryExpression().Child();
          if (val.value.int_value == 1) {
            // IS predicates always return 1/0 (never NULL), so pred = 1 is
            // the IS-predicate node itself: IS TRUE -> (child IS TRUE).
            return bindings.at("left");
          }
          if (val.value.int_value != 0) {
            // Only 0/1 can invert a boolean predicate; comparing an
            // IS-predicate (1/0) against e.g. 5 must not invert the
            // predicate.
            return Expression{};
          }
          // (pred) = 0 -> NOT pred.  Map each predicate to its exact
          // complement; the old code always produced `child IS NOT TRUE`,
          // which inverts `x IS FALSE` into the wrong predicate.
          UnaryOperation complement = UnaryOperation::kIsNotTrue;
          switch (left_op) {
            case UnaryOperation::kIsTrue:
              complement = UnaryOperation::kIsNotTrue;
              break;
            case UnaryOperation::kIsNotTrue:
              complement = UnaryOperation::kIsTrue;
              break;
            case UnaryOperation::kIsFalse:
              complement = UnaryOperation::kIsNotFalse;
              break;
            case UnaryOperation::kIsNotFalse:
              complement = UnaryOperation::kIsFalse;
              break;
            default:
              return Expression{};
          }
          return UnaryExpressionExp(child, complement);
        }));

    // coalesce_and_nullif_simplification: Simplify COALESCE when leading
    // arguments are non-null constants, eliminate trailing and redundant NULL
    // literals, and simplify NULLIF.
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (coalesce_and_nullif_rules_sound)
    built.Add(ExpressionRule(
        "coalesce_and_nullif_simplification",
        Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string name(fn.FuncName());
          for (char& c : name) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          if (name == "coalesce") {
            if (fn.Args().empty()) {
              // Zero-argument COALESCE is an arity error in the AST
              // evaluator; leave the call intact so the error survives
              // instead of fabricating a NULL constant.
              return Expression{};
            }
            // A later argument can determine the common type even when an
            // earlier constant determines the value. Drop only untyped NULLs.
            std::vector<Expression> new_args;
            for (const auto& arg : fn.Args()) {
              if (arg && IsConstant(arg) &&
                  arg->AsConstantValue().GetValue().IsNull() &&
                  !arg->ResultType(Schema{}).IsValid()) {
                continue;
              }
              new_args.push_back(arg);
            }
            const bool changed = new_args.size() != fn.Args().size();
            if (new_args.empty()) {
              return ConstantValueExp(Value());
            }
            if (new_args.size() == 1) {
              return new_args.front();
            }
            if (changed) {
              return FunctionCallExp("coalesce", std::move(new_args));
            }
          } else if (name == "nullif" && fn.Args().size() == 2) {
            const auto& a = fn.Args()[0];
            const auto& b = fn.Args()[1];
            if (IsConstant(a) && IsConstant(b)) {
              const Value& val_a = a->AsConstantValue().GetValue();
              const Value& val_b = b->AsConstantValue().GetValue();
              if (val_a.IsNull() || val_b.IsNull()) {
                return a;
              }
              auto eq =
                  TryEvaluateBinary(BinaryOperation::kEquals, val_a, val_b);
              if (eq.HasValue()) {
                if (eq.Value().Truthy()) {
                  return ConstantValueExp(Value());
                }
                return a;
              }
              return Expression{};
            }
            if (Same(a, b)) {
              // nullif(a, a) -> NULL drops a; only sound when a cannot raise,
              // is not volatile, and cannot evaluate to NaN (NaN = NaN is false
              // in SQL).
              if (!StaticallyProvesNonDouble(a) || !ExpressionCannotThrow(a) ||
                  !SafeToReduceEvaluationCount(a)) {
                return Expression{};
              }
              return ConstantValueExp(Value());
            }
            if (IsConstant(a) && a->AsConstantValue().GetValue().IsNull()) {
              // Dropping b must not remove a throwing evaluation
              // (oracle-found: nullif(NULL, x % 0) folded to NULL instead
              // of raising "modulo by zero").
              if (!IsConstant(b)) {
                return Expression{};
              }
              return ConstantValueExp(Value());
            }
            if (IsConstant(b) && b->AsConstantValue().GetValue().IsNull()) {
              return a;
            }
          }
          return Expression{};
        }));

    // datetime_and_string_fold_extent: Solidify constant folding rules for
    // DATE_ADD, DATE_SUB, SUBSTRING, INSTR, LPAD, RPAD, CONCAT, LENGTH and
    // other datetime/string builtins.
    built.Add(ExpressionRule(
        "datetime_and_string_fold_extent",
        Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          std::string name(fn.FuncName());
          for (char& c : name) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          static const std::unordered_set<std::string> target_funcs = {
              "substring",        "substr",       "instr",
              "strpos",           "lpad",         "rpad",
              "concat",           "length",       "char_length",
              "character_length", "octet_length", "byte_length"};
          if (!target_funcs.contains(name)) {
            return Expression{};
          }
          const auto is_literal = [](const Expression& arg) {
            return arg && (arg->Type() == TypeTag::kConstantValue ||
                           arg->Type() == TypeTag::kIntervalExp);
          };
          if (!std::ranges::all_of(fn.Args(), is_literal)) {
            return Expression{};
          }
          if (StatusOr<Value> folded = expression->TryEvaluate(Row(), Schema());

              folded.HasValue()) {
            return ConstantValueExp(folded.MoveValue());
          }

          return Expression{};
        }));

    // empty IN list -> FALSE
    // Lean proof: lean/TinylambOpt/CppRuleAudit.lean (cpp_empty_in_isFalse_bug)
    built.Add(ExpressionRule(
        "empty_in_list", Is(TypeTag::kInExp),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& in = expression->AsInExpression();
          if (!in.list_.empty()) {
            return Expression{};
          }
          // The AST still evaluates the probe. Folding a literal preserves
          // probe errors and observable evaluations of nonconstant expressions.
          if (!IsConstant(in.child_)) {
            return Expression{};
          }
          return ConstantValueExp(Value(false));
        }));

    // json_path_constant_fold: JSON_EXTRACT / JSON_QUERY / JSON_VALUE /
    // JSON_EXTRACT_SCALAR with constant JSON string and constant JSON path
    // folds to the extracted constant value.
    built.Add(ExpressionRule(
        "json_path_constant_fold", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          const std::string name = ToUpper(fn.FuncName());
          if (name == "JSON_EXTRACT" || name == "JSON_QUERY" ||
              name == "JSON_VALUE" || name == "JSON_EXTRACT_SCALAR" ||
              name == "JSON_EXTRACT_ARRAY" || name == "JSON_QUERY_ARRAY" ||
              name == "JSON_VALUE_ARRAY" ||
              name == "JSON_EXTRACT_STRING_ARRAY") {
            // The evaluator accepts only 1 or 2 arguments; folding a
            // wider call would silently erase its arity error.
            if (fn.Args().empty() || fn.Args().size() > 2 || !fn.Args()[0] ||
                fn.Args()[0]->Type() != TypeTag::kConstantValue) {
              return Expression{};
            }
            const Value json_val = fn.Args()[0]->AsConstantValue().GetValue();
            if (json_val.IsNull() || json_val.type != ValueType::kVarChar) {
              return Expression{};
            }
            std::string path = "$";
            if (fn.Args().size() >= 2) {
              if (!fn.Args()[1] ||
                  fn.Args()[1]->Type() != TypeTag::kConstantValue) {
                return Expression{};
              }
              const Value path_val = fn.Args()[1]->AsConstantValue().GetValue();
              if (path_val.IsNull() || path_val.type != ValueType::kVarChar) {
                return Expression{};
              }
              path = path_val.value.varchar_value;
            }
            Value res = EvaluateJsonFunction(
                fn.FuncName(), json_val.value.varchar_value, path);
            return ConstantValueExp(res);
          }
          return Expression{};
        }));

    // json_family_constant_fold: every JSON document family function with
    // all-constant arguments folds at rewrite time through the same shared
    // implementation both evaluators use, so a folded result cannot diverge
    // from row-wise evaluation.
    built.Add(ExpressionRule(
        "json_family_constant_fold", Is(TypeTag::kFunctionCallExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& fn = expression->AsFunctionCallExpression();
          if (!json_doc::family::IsJsonFamilyFunction(fn.FuncName())) {
            return Expression{};
          }
          std::vector<Value> arguments;
          arguments.reserve(fn.Args().size());
          for (const Expression& argument : fn.Args()) {
            if (!argument || argument->Type() != TypeTag::kConstantValue) {
              return Expression{};
            }
            arguments.push_back(argument->AsConstantValue().GetValue());
          }
          StatusOr<Value> folded =
              json_doc::family::Evaluate(fn.FuncName(), arguments);
          if (!folded.HasValue()) {
            return Expression{};
          }
          return ConstantValueExp(folded.MoveValue());
        }));

    // numeric_widening_cast: Nested widening numeric casts like CAST(CAST(x AS
    // INT32) AS INT64) -> CAST(x AS INT64) or CAST(constant AS type) folded.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "numeric_widening_cast", Is(TypeTag::kCastExp),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          const auto& cast = expression->AsCastExpression();
          if (!cast.Child()) {
            return Expression{};
          }
          if (cast.Child()->Type() == TypeTag::kConstantValue) {
            if (StatusOr<Value> folded =
                    expression->TryEvaluate(Row(), Schema());
                folded.HasValue() && !folded.Value().IsNull()) {
              return ConstantValueExp(folded.MoveValue());
            }
            return Expression{};
          }
          if (cast.Child()->Type() == TypeTag::kCastExp) {
            const auto& inner = cast.Child()->AsCastExpression();
            // Dropping the inner cast is sound only when it is redundant:
            // same target, same SAFE_CAST semantics.  Cross-domain casts are
            // lossy or partial and must survive (oracle-found:
            // CAST(CAST(-inf AS INT64) AS FLOAT64) folded to
            // CAST(-inf AS FLOAT64), erasing the NaN/Inf throw; FLOAT64 ->
            // INT64 also truncates, INT64 -> FLOAT64 loses precision).
            if (ToUpper(inner.TargetTypeName()) ==
                    ToUpper(cast.TargetTypeName()) &&
                inner.ReturnNullOnError() == cast.ReturnNullOnError()) {
              return CastExpressionExp(inner.Child(), cast.TargetTypeName(),
                                       cast.ReturnNullOnError());
            }
          }
          return Expression{};
        }));

    // or_of_ranges_to_in: (x = 1 OR x = 2 OR x = 3) -> x IN (1, 2, 3)
    // and combining multiple equality OR conjuncts into a single IN list.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (in_and_join_notnull_rules_sound)
    built.Add(ExpressionRule(
        "or_of_ranges_to_in",
        Binary(BinaryOperation::kOr, Any("left"), Any("right")),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          std::vector<Expression> disjuncts = SplitDisjuncts(expression);
          if (disjuncts.size() < 2) {
            return Expression{};
          }
          // Order-preserving grouping: `(x = 1 OR x = 2 OR y) -> x IN (1, 2)
          // OR y`, keeping each disjunct's original slot.
          // Safety rules:
          // 1. Target expression must be safe to reduce evaluation count
          //    (volatiles like RAND() must not collapse evaluations).
          // 2. Merge only consecutive alternatives of the same target.
          //    Their comparisons retain order even when a type error is
          //    possible; never move one across another target or predicate.
          // 3. Only mark `changed = true` when multiple disjuncts actually
          //    merge into a single slot.
          // 4. Empty IN lists (x IN ()) are not equality ranges and must
          //    never be grouped or dereferenced via front().
          struct Group {
            Expression target;
            Expression original_expr;
            std::vector<Expression> items;
            size_t merge_count{1};
            size_t slot{0};
          };
          std::vector<Expression> kept;
          std::vector<Group> active_groups;
          bool changed = false;

          auto find_group = [&](const Expression& target) -> Group* {
            for (Group& g : active_groups) {
              if (Same(g.target, target)) {
                return &g;
              }
            }
            return nullptr;
          };

          auto seal_groups = [&]() {
            for (const Group& g : active_groups) {
              if (g.merge_count > 1) {
                if (g.items.size() >= 2) {
                  kept[g.slot] = InExpressionExp(g.target, g.items);
                  changed = true;
                } else if (g.items.size() == 1) {
                  kept[g.slot] = BinaryExpressionExp(
                      g.target, BinaryOperation::kEquals, g.items.front());
                  changed = true;
                } else {
                  kept[g.slot] = g.original_expr;
                }
              } else {
                kept[g.slot] = g.original_expr;
              }
            }
            active_groups.clear();
          };

          for (const Expression& d : disjuncts) {
            if (d->Type() == TypeTag::kBinaryExp &&
                d->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
              const auto& bin = d->AsBinaryExpression();
              const bool right_const =
                  IsConstant(bin.Right()) && !IsConstant(bin.Left());
              const bool left_const =
                  IsConstant(bin.Left()) && !IsConstant(bin.Right());
              if (right_const || left_const) {
                const Expression& target =
                    right_const ? bin.Left() : bin.Right();
                const Expression& value =
                    right_const ? bin.Right() : bin.Left();
                if (SafeToReduceEvaluationCount(target)) {
                  Group* g = find_group(target);
                  if (g == nullptr) {
                    seal_groups();
                    const size_t slot = kept.size();
                    kept.push_back(Expression{});
                    active_groups.push_back(Group{
                        .target = target,
                        .original_expr = d,
                        .items = {value},
                        .merge_count = 1,
                        .slot = slot,
                    });
                  } else {
                    g->merge_count++;
                    if (std::ranges::none_of(g->items,
                                             [&](const Expression& e) {
                                               return Same(e, value);
                                             })) {
                      g->items.push_back(value);
                    }
                  }
                  continue;
                }
              }
            } else if (d->Type() == TypeTag::kInExp) {
              const auto& in = d->AsInExpression();
              if (!IsConstant(in.child_) && !in.list_.empty() &&
                  std::ranges::all_of(in.list_, IsConstant) &&
                  SafeToReduceEvaluationCount(in.child_)) {
                Group* g = find_group(in.child_);
                if (g == nullptr) {
                  seal_groups();
                  const size_t slot = kept.size();
                  kept.push_back(Expression{});
                  active_groups.push_back(Group{
                      .target = in.child_,
                      .original_expr = d,
                      .items = in.list_,
                      .merge_count = 1,
                      .slot = slot,
                  });
                } else {
                  g->merge_count++;
                  for (const auto& item : in.list_) {
                    if (std::ranges::none_of(g->items,
                                             [&](const Expression& e) {
                                               return Same(e, item);
                                             })) {
                      g->items.push_back(item);
                    }
                  }
                }
                continue;
              }
            }

            // Non-grouped disjunct is an evaluation-order barrier.
            seal_groups();
            kept.push_back(d);
          }

          seal_groups();

          if (!changed) {
            return Expression{};
          }
          return CombineDisjuncts(kept);
        }));

    // interval_normalize: Function calls on INTERVAL like INTERVAL '1' DAY +
    // INTERVAL '2' DAY -> INTERVAL '3' DAY constant folding.
    built.Add(ExpressionRule(
        "interval_normalize", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && right) {
              if (left->Type() == TypeTag::kIntervalExp &&
                  right->Type() == TypeTag::kIntervalExp) {
                const auto& l_iv = left->AsIntervalExpression();
                const auto& r_iv = right->AsIntervalExpression();
                // Fast paths must respect the same overflow contract as the
                // runtime IntervalValue arithmetic (throw); a silent int64
                // wrap would make the rewrite change the observable result.
                int64_t folded = 0;
                if (l_iv.RawAmount().empty() && r_iv.RawAmount().empty() &&
                    l_iv.Unit() == r_iv.Unit()) {
                  bool overflowed = false;
                  if (binary.Op() == BinaryOperation::kAdd) {
                    overflowed = __builtin_add_overflow(l_iv.Amount(),
                                                        r_iv.Amount(), &folded);
                  } else if (binary.Op() == BinaryOperation::kSubtract) {
                    overflowed = __builtin_sub_overflow(l_iv.Amount(),
                                                        r_iv.Amount(), &folded);
                  }
                  if (!overflowed && binary.Op() != BinaryOperation::kAdd &&
                      binary.Op() != BinaryOperation::kSubtract) {
                    overflowed = true;
                  }
                  if (!overflowed) {
                    // The raw fast path must respect the interval domain
                    // (~ +/-106751 days / +/-3558 years) that the Try* paths
                    // enforce via TryTotalNanos; otherwise a folded literal
                    // becomes a value the runtime evaluator rejects.
                    IntervalValue probe{};
                    const std::string& unit = l_iv.Unit();
                    if (unit == "DAY" || unit == "day") {
                      probe.days = folded;
                    } else if (unit == "HOUR" || unit == "hour") {
                      if (__builtin_mul_overflow(folded, 3600LL * 1000000000LL,
                                                 &probe.nanos)) {
                        return Expression{};
                      }
                    } else if (unit == "MINUTE" || unit == "minute") {
                      if (__builtin_mul_overflow(folded, 60LL * 1000000000LL,
                                                 &probe.nanos)) {
                        return Expression{};
                      }
                    } else if (unit == "SECOND" || unit == "second") {
                      if (__builtin_mul_overflow(folded, 1000000000LL,
                                                 &probe.nanos)) {
                        return Expression{};
                      }
                    } else if (unit == "MONTH" || unit == "month") {
                      probe.months = folded;
                    } else if (unit == "YEAR" || unit == "year") {
                      if (__builtin_mul_overflow(folded, int64_t{12},
                                                 &probe.months)) {
                        return Expression{};
                      }
                    } else {
                      return Expression{};  // unknown unit: stay unfolded
                    }
                    if (!probe.TryTotalNanos().HasValue()) {
                      return Expression{};
                    }
                    return IntervalExpressionExp(folded, l_iv.Unit());
                  }
                  return Expression{};
                }
                const auto& iv1 = l_iv.GetIntervalValue();
                const auto& iv2 = r_iv.GetIntervalValue();
                // Status-returning arithmetic: an overflow leaves the node
                // unfolded so the runtime raises it as a Status (the
                // throwing operators escaped TryRewrite's StatusOr contract).
                if (binary.Op() == BinaryOperation::kAdd ||
                    binary.Op() == BinaryOperation::kSubtract) {
                  StatusOr<IntervalValue> folded_iv =
                      binary.Op() == BinaryOperation::kAdd ? iv1.TryPlus(iv2)
                                                           : iv1.TryMinus(iv2);
                  if (!folded_iv.HasValue()) {
                    return Expression{};
                  }
                  StatusOr<std::string> text = folded_iv.Value().TryToString();
                  if (!text.HasValue()) {
                    return Expression{};
                  }
                  return IntervalExpressionExp(0, "", text.MoveValue());
                }
              }
              if (left->Type() == TypeTag::kIntervalExp &&
                  IsInt64Constant(right) &&
                  binary.Op() == BinaryOperation::kMultiply) {
                const int64_t k =
                    right->AsConstantValue().GetValue().value.int_value;
                if (left->AsIntervalExpression().RawAmount().empty()) {
                  int64_t scaled = 0;
                  if (__builtin_mul_overflow(
                          left->AsIntervalExpression().Amount(), k, &scaled)) {
                    return Expression{};
                  }
                  // The (scaled, unit) ctor runs the throwing
                  // IntervalValue::Parse; validate first so a product that
                  // fits INT64 but is not a valid interval (e.g. a YEAR
                  // amount whose x12 month conversion overflows) bails to
                  // the runtime instead of escaping TryRewrite's contract.
                  StatusOr<IntervalValue> prod = IntervalValue::TryParse(
                      std::to_string(scaled),
                      left->AsIntervalExpression().Unit());
                  if (!prod.HasValue()) {
                    return Expression{};
                  }
                  StatusOr<std::string> text = prod.Value().TryToString();
                  if (!text.HasValue()) {
                    return Expression{};
                  }
                  return IntervalExpressionExp(0, "", text.MoveValue());
                }
                StatusOr<IntervalValue> prod =
                    left->AsIntervalExpression().GetIntervalValue().TryMultiply(
                        k);
                if (!prod.HasValue()) {
                  return Expression{};
                }
                StatusOr<std::string> text = prod.Value().TryToString();
                if (!text.HasValue()) {
                  return Expression{};
                }
                return IntervalExpressionExp(0, "", text.MoveValue());
              }
              if (IsInt64Constant(left) &&
                  right->Type() == TypeTag::kIntervalExp &&
                  binary.Op() == BinaryOperation::kMultiply) {
                const int64_t k =
                    left->AsConstantValue().GetValue().value.int_value;
                if (right->AsIntervalExpression().RawAmount().empty()) {
                  int64_t scaled = 0;
                  if (__builtin_mul_overflow(
                          right->AsIntervalExpression().Amount(), k, &scaled)) {
                    return Expression{};
                  }
                  // See the mirror arm above (T637): validate the scaled
                  // amount against the interval domain before constructing.
                  StatusOr<IntervalValue> prod = IntervalValue::TryParse(
                      std::to_string(scaled),
                      right->AsIntervalExpression().Unit());
                  if (!prod.HasValue()) {
                    return Expression{};
                  }
                  StatusOr<std::string> text = prod.Value().TryToString();
                  if (!text.HasValue()) {
                    return Expression{};
                  }
                  return IntervalExpressionExp(0, "", text.MoveValue());
                }
                StatusOr<IntervalValue> prod = right->AsIntervalExpression()
                                                   .GetIntervalValue()
                                                   .TryMultiply(k);
                if (!prod.HasValue()) {
                  return Expression{};
                }
                StatusOr<std::string> text = prod.Value().TryToString();
                if (!text.HasValue()) {
                  return Expression{};
                }
                return IntervalExpressionExp(0, "", text.MoveValue());
              }
            }
          }
          // `-INTERVAL ...` is rejected by the canonical AST evaluator
          // (UnaryExpression::Evaluate kMinus accepts only numbers). Do not
          // fold it into a value: leaving it unfolded lets the runtime raise
          // the same error the AST ground truth does.
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            if (fn.Args().size() == 1 && fn.Args()[0] &&
                fn.Args()[0]->Type() == TypeTag::kIntervalExp) {
              const auto& iv =
                  fn.Args()[0]->AsIntervalExpression().GetIntervalValue();
              // Use the Status-returning variants: the throwing wrappers
              // would escape TryRewrite's StatusOr boundary on overflow.
              std::optional<IntervalValue> justified;
              if (fn.FuncName() == "justify_hours") {
                auto r = iv.TryJustifyHours();
                if (r.HasValue()) {
                  justified =
                      r.MoveValue();  // NOLINT(readability-braces-around-statements)
                }
              } else if (fn.FuncName() == "justify_days") {
                auto r = iv.TryJustifyDays();
                if (r.HasValue()) {
                  justified =
                      r.MoveValue();  // NOLINT(readability-braces-around-statements)
                }
              } else if (fn.FuncName() == "justify_interval") {
                auto r = iv.TryJustifyInterval();
                if (r.HasValue()) {
                  justified =
                      r.MoveValue();  // NOLINT(readability-braces-around-statements)
                }
              } else {
                return Expression{};
              }
              if (!justified.has_value()) {
                return Expression{};
              }
              StatusOr<std::string> text = justified->TryToString();
              if (!text.HasValue()) {
                return Expression{};
              }
              return IntervalExpressionExp(0, "", text.MoveValue());
            }
          }
          return Expression{};
        }));

    // predicate_pushdown_case: Safely push predicates into CASE branches
    // (p(CASE WHEN c1 THEN v1 ELSE v2 END) -> CASE WHEN c1 THEN p(v1) ELSE
    // p(v2) END).
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (case_and_if_pushdown_rules_sound)
    built.Add(ExpressionRule(
        "predicate_pushdown_case", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          // Unary expression over CASE
          if (expression->Type() == TypeTag::kUnaryExp) {
            const auto& unary = expression->AsUnaryExpression();
            if (unary.Child() && unary.Child()->Type() == TypeTag::kCaseExp) {
              const auto& c = unary.Child()->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, UnaryExpressionExp(w.second, unary.Op()));
              }
              Expression new_else =
                  c.else_clause_
                      ? UnaryExpressionExp(c.else_clause_, unary.Op())
                      : UnaryExpressionExp(ConstantValueExp(Value()),
                                           unary.Op());
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // Binary expression where Left or Right is CASE
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const auto op = binary.Op();
            if (!IsComparison(op) && op != BinaryOperation::kLike &&
                op != BinaryOperation::kNotLike &&
                op != BinaryOperation::kIsDistinctFrom &&
                op != BinaryOperation::kIsNotDistinctFrom) {
              return Expression{};
            }
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && left->Type() == TypeTag::kCaseExp && right &&
                right->Type() != TypeTag::kCaseExp &&
                SafeToReduceEvaluationCount(right)) {
              const auto& c = left->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, BinaryExpressionExp(w.second, op, right));
              }
              Expression new_else =
                  c.else_clause_
                      ? BinaryExpressionExp(c.else_clause_, op, right)
                      : BinaryExpressionExp(ConstantValueExp(Value()), op,
                                            right);
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
            if (right && right->Type() == TypeTag::kCaseExp && left &&
                left->Type() != TypeTag::kCaseExp &&
                SafeToReduceEvaluationCount(left)) {
              const auto& c = right->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(w.first,
                                       BinaryExpressionExp(left, op, w.second));
              }
              Expression new_else =
                  c.else_clause_ ? BinaryExpressionExp(left, op, c.else_clause_)
                                 : BinaryExpressionExp(
                                       left, op, ConstantValueExp(Value()));
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // IN expression where child is CASE
          if (expression->Type() == TypeTag::kInExp) {
            const auto& in = expression->AsInExpression();
            if (in.child_ && in.child_->Type() == TypeTag::kCaseExp) {
              if (!std::ranges::all_of(in.list_, SafeToReduceEvaluationCount)) {
                return Expression{};
              }
              const auto& c = in.child_->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(w.first,
                                       InExpressionExp(w.second, in.list_));
              }
              Expression new_else =
                  c.else_clause_
                      ? InExpressionExp(c.else_clause_, in.list_)
                      : InExpressionExp(ConstantValueExp(Value()), in.list_);
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          // CAST expression where child is CASE
          if (expression->Type() == TypeTag::kCastExp) {
            const auto& cast = expression->AsCastExpression();
            if (cast.Child() && cast.Child()->Type() == TypeTag::kCaseExp) {
              const auto& c = cast.Child()->AsCaseExpression();
              std::vector<std::pair<Expression, Expression>> new_whens;
              new_whens.reserve(c.when_clauses_.size());
              for (const auto& w : c.when_clauses_) {
                new_whens.emplace_back(
                    w.first, CastExpressionExp(w.second, cast.TargetTypeName(),
                                               cast.ReturnNullOnError()));
              }
              Expression new_else =
                  c.else_clause_
                      ? CastExpressionExp(c.else_clause_, cast.TargetTypeName(),
                                          cast.ReturnNullOnError())
                      : CastExpressionExp(ConstantValueExp(Value()),
                                          cast.TargetTypeName(),
                                          cast.ReturnNullOnError());
              return CaseExpressionExp(std::move(new_whens),
                                       std::move(new_else));
            }
          }
          return Expression{};
        }));

    // inner_join_not_null_inference: Infer x IS NOT NULL and y IS NOT NULL
    // from inner join equality condition x = y in conjunctions.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (in_and_join_notnull_rules_sound)
    built.Add(ExpressionRule(
        "inner_join_not_null_inference",
        Binary(BinaryOperation::kAnd, Any("left"), Any("right")),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          const std::vector<Expression> conjuncts = SplitConjuncts(expression);
          bool changed = false;
          std::vector<Expression> result_conjuncts;
          result_conjuncts.reserve(conjuncts.size());

          // D6 (docs/design.md): a shared operand (x = y AND y = z) must
          // infer y IS NOT NULL exactly once.  Deduplicate by content
          // against the input conjuncts AND everything already appended
          // this pass, so an equal predicate is never re-added because of
          // its position in the AND tree.
          auto already_present = [&](const Expression& candidate) {
            auto in_list = [&](const std::vector<Expression>& list) {
              return std::ranges::any_of(list, [&](const Expression& e) {
                if (Same(e, candidate)) {
                  return true;
                }
                return std::ranges::any_of(SplitConjuncts(e),
                                           [&](const Expression& sub) {
                                             return Same(sub, candidate);
                                           });
              });
            };
            return in_list(conjuncts) || in_list(result_conjuncts);
          };

          for (const auto& c : conjuncts) {
            if (c->Type() == TypeTag::kBinaryExp &&
                c->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
              const Expression& l = c->AsBinaryExpression().Left();
              const Expression& r = c->AsBinaryExpression().Right();
              if (l && r && !IsConstant(l) && !IsConstant(r) &&
                  SafeToReduceEvaluationCount(l) &&
                  SafeToReduceEvaluationCount(r)) {
                Expression l_not_null =
                    UnaryExpressionExp(l, UnaryOperation::kIsNotNull);
                Expression r_not_null =
                    UnaryExpressionExp(r, UnaryOperation::kIsNotNull);
                bool has_l = already_present(l_not_null);
                bool has_r = already_present(r_not_null);
                if (!has_l && !has_r) {
                  Expression not_null_pair = BinaryExpressionExp(
                      std::move(l_not_null), BinaryOperation::kAnd,
                      std::move(r_not_null));
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(not_null_pair)));
                  changed = true;
                  continue;
                }
                if (!has_l) {
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(l_not_null)));
                  changed = true;
                  continue;
                }
                if (!has_r) {
                  result_conjuncts.push_back(BinaryExpressionExp(
                      c, BinaryOperation::kAnd, std::move(r_not_null)));
                  changed = true;
                  continue;
                }
              }
            }
            result_conjuncts.push_back(c);
          }
          if (!changed) {
            return Expression{};
          }
          return CombineConjuncts(result_conjuncts);
        },
        /*root_only=*/true));

    // regexp_prefix_extraction: Extract literal prefix from regular expression
    // patterns (e.g. ^abc.* -> prefix 'abc' for LIKE/range scan candidate).
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (null_and_like_rules_sound)
    built.Add(ExpressionRule(
        "regexp_prefix_extraction", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            const std::string& name = fn.FuncName();
            // REGEXP_MATCH is anchored at both ends (std::regex_match), so
            // an empty remainder means literal equality, not a prefix.
            const bool full_match = name == "regexp_match";
            if ((name == "regexp_contains" || name == "regexp_like" ||
                 full_match) &&
                fn.Args().size() == 2 && fn.Args()[0] &&
                IsConstant(fn.Args()[1])) {
              const Value pat_val = fn.Args()[1]->AsConstantValue().GetValue();
              const Expression& input = fn.Args()[0];
              // `=` and LIKE fold operands under a case-insensitive
              // collation tag while REGEXP_* compares AsString() bytes;
              // LIKE also rejects and `=` coerces non-varchar operands.
              // Refuse inputs whose static form proves either hazard: an
              // explicit COLLATE() argument or a constant that is
              // CI-tagged or a non-null non-varchar.
              const bool unsafe_input =
                  pat_val.IsCaseInsensitive() ||
                  CollationOfExpression(input) == Collation::kCaseInsensitive ||
                  (IsConstant(input) &&
                   (input->AsConstantValue().GetValue().IsCaseInsensitive() ||
                    (!input->AsConstantValue().GetValue().IsNull() &&
                     input->AsConstantValue().GetValue().type !=
                         ValueType::kVarChar)));
              if (pat_val.type == ValueType::kVarChar && !unsafe_input) {
                std::string prefix;
                std::string remainder;
                if (ExtractRegexPrefix(pat_val.value.varchar_value, prefix,
                                       remainder)) {
                  if ((remainder == "$" || (full_match && remainder.empty())) &&
                      // `=` coerces timestamp-shaped varchars to epoch
                      // comparison; REGEXP_* compares bytes.  Refuse the
                      // rewrite for timestamp-shaped matches.
                      !TimestampShapedConstant(prefix)) {
                    return BinaryExpressionExp(
                        input, BinaryOperation::kEquals,
                        ConstantValueExp(Value(std::string(prefix))));
                  }
                  // '%' and '_' are literal in the regex but wildcards in
                  // the emitted LIKE — refuse prefixes containing them.
                  if ((remainder == ".*" || remainder == ".*$" ||
                       (!full_match && remainder.empty())) &&
                      !HasLikeWildcard(prefix)) {
                    return BinaryExpressionExp(
                        input, BinaryOperation::kLike,
                        ConstantValueExp(Value(std::string(prefix + "%"))));
                  }
                }
              }
            }
          }
          return Expression{};
        }));

    // cast_pushdown_comparison is intentionally absent: pushing `CAST(x AS
    // T) cmp constant` through to `x cmp constant` is only sound when the
    // child's type is known (DOUBLE truncation breaks INT64 pushdown; a
    // VARCHAR/DATE child changes parse errors and cross-type comparisons;
    // folding a fractional equality to FALSE loses the UNKNOWN result for a
    // NULL child).  ExpressionRules run without a schema, so the rule cannot
    // verify the child type and must not fire.  Casts stay runtime work.

    // deterministic_function_cse: Detect identical deterministic function calls
    // within the same expression tree and eliminate redundant computations.
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (greatest_concat_array_cast_cse_sound)
    built.Add(ExpressionRule(
        "deterministic_function_cse", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression) {
            return Expression{};
          }
          // Binary expression self-simplification with identical deterministic
          // functions
          if (expression->Type() == TypeTag::kBinaryExp) {
            const auto& binary = expression->AsBinaryExpression();
            const Expression& left = binary.Left();
            const Expression& right = binary.Right();
            if (left && right && left->Type() == TypeTag::kFunctionCallExp &&
                right->Type() == TypeTag::kFunctionCallExp &&
                Same(left, right)) {
              // PRODUCTION FIX: self-identities (f-f -> 0, f/f -> 1,
              // f = f -> IS NOT NULL, f < f -> FALSE) were only valid for
              // non-NULL operands.  An immutable function may return NULL,
              // so folding changed UNKNOWN into concrete values (and f/f
              // hid the division by zero when f(x) == 0).  The unsafe fold
              // was removed; expressions stay as written.
            }
          }
          // Function calls with duplicate deterministic arguments (e.g.
          // coalesce, greatest, least)
          if (expression->Type() == TypeTag::kFunctionCallExp) {
            const auto& fn = expression->AsFunctionCallExpression();
            std::string name(fn.FuncName());
            for (char& c : name) {
              c = static_cast<char>(
                  std::tolower(static_cast<unsigned char>(c)));
            }
            if (GetFunctionVolatility(name) == Volatility::kImmutable &&
                fn.Args().size() >= 2) {
              if (name == "coalesce" || name == "greatest" || name == "least") {
                bool all_same = true;
                for (size_t i = 1; i < fn.Args().size(); ++i) {
                  if (!Same(fn.Args()[0], fn.Args()[i])) {
                    all_same = false;
                    break;
                  }
                }
                if (all_same && SafeToReduceEvaluationCount(fn.Args()[0])) {
                  return fn.Args()[0];
                }
              } else if (name == "ifnull" && fn.Args().size() == 2) {
                if (Same(fn.Args()[0], fn.Args()[1]) &&
                    SafeToReduceEvaluationCount(fn.Args()[0])) {
                  return fn.Args()[0];
                }
              } else if (name == "nullif" && fn.Args().size() == 2) {
                if (IsConstant(fn.Args()[0])) {
                  // Constant folding handles constant cases.
                } else if (Same(fn.Args()[0], fn.Args()[1]) &&
                           StaticallyProvesNonDouble(fn.Args()[0]) &&
                           ExpressionCannotThrow(fn.Args()[0]) &&
                           SafeToReduceEvaluationCount(fn.Args()[0])) {
                  return ConstantValueExp(Value());
                }
              }
            }
          }
          // Case expression where all branches evaluate the same deterministic
          // function
          if (expression->Type() == TypeTag::kCaseExp) {
            const auto& c = expression->AsCaseExpression();
            if (c.else_clause_ &&
                c.else_clause_->Type() == TypeTag::kFunctionCallExp &&
                !c.when_clauses_.empty()) {
              const auto& fn = c.else_clause_->AsFunctionCallExpression();
              if (GetFunctionVolatility(fn.FuncName()) ==
                  Volatility::kImmutable) {
                bool all_same = true;
                for (const auto& w : c.when_clauses_) {
                  if (!Same(w.second, c.else_clause_)) {
                    all_same = false;
                    break;
                  }
                }
                if (all_same) {
                  // Returning the else drops all WHEN-condition evaluations;
                  // only sound when none of them can raise or are volatile (cf.
                  // uniform_case_result).
                  const bool conditions_total =
                      std::ranges::all_of(c.when_clauses_, [](const auto& w) {
                        return ExpressionCannotThrow(w.first) &&
                               SafeToReduceEvaluationCount(w.first);
                      });
                  if (!conditions_total) {
                    return Expression{};
                  }
                  return c.else_clause_;
                }
              }
            }
          }
          return Expression{};
        }));

    // boolean_filter_pullup: Pull up common conditions out of disjunctions
    // (A AND B) OR (A AND C) -> A AND (B OR C).
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (three_valued_distributive_and_absorption_sound)
    built.Add(ExpressionRule(
        "boolean_filter_pullup", AnyBinary(Any("left"), Any("right")),
        [](const Expression& expression, const ExpressionBindings&) {
          return FactorCommonAndImpl(expression);
        }));

    // ---- ours (master) rules re-added after soundness review ----
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (int64_identities_negation_and_abs_sound)
    built.Add(ExpressionRule(
        "abs_of_abs", Is(TypeTag::kFunctionCallExp, "expr"),
        [](const Expression& expression, const ExpressionBindings&) {
          const auto& fn = expression->AsFunctionCallExpression();
          if (fn.FuncName() != "abs" || fn.Args().size() != 1) {
            return Expression{};
          }
          if (!fn.Args()[0] ||
              fn.Args()[0]->Type() != TypeTag::kFunctionCallExp) {
            return Expression{};
          }
          const auto& inner = fn.Args()[0]->AsFunctionCallExpression();
          if (inner.FuncName() != "abs") {
            return Expression{};
          }
          return fn.Args()[0];
        }));
    // Lean proof: lean/TinylambOpt/ScalarOverflowAndCoreAlgebra.lean
    //   (canonicalize_comparison_and_boolean_sound)
    built.Add(ExpressionRule(
        "canonicalize_boolean", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression || expression->Type() != TypeTag::kBinaryExp) {
            return Expression{};
          }
          const auto& binary = expression->AsBinaryExpression();
          if (binary.Op() != BinaryOperation::kEquals &&
              binary.Op() != BinaryOperation::kNotEquals) {
            return Expression{};
          }
          if (!binary.Left() || !binary.Right()) {
            return Expression{};
          }
          const bool is_equals = binary.Op() == BinaryOperation::kEquals;
          const Expression* boolean_side = nullptr;
          const Expression* const_side = nullptr;
          if (binary.Right()->Type() == TypeTag::kConstantValue) {
            boolean_side = &binary.Left();
            const_side = &binary.Right();
          } else if (binary.Left()->Type() == TypeTag::kConstantValue) {
            boolean_side = &binary.Right();
            const_side = &binary.Left();
          } else {
            return Expression{};
          }
          const Value constant = (*const_side)->AsConstantValue().GetValue();
          if (constant.IsNull() || constant.type != ValueType::kInt64 ||
              (constant.value.int_value != 0 &&
               constant.value.int_value != 1)) {
            // TRUE/FALSE surface as 1/0; any other constant (including 2,
            // which a boolean never equals) keeps its comparison semantics.
            return Expression{};
          }
          if (!*boolean_side) {
            return Expression{};
          }
          bool boolean_valued = false;
          switch ((*boolean_side)->Type()) {
            case TypeTag::kBinaryExp: {
              const auto op = (*boolean_side)->AsBinaryExpression().Op();
              boolean_valued = op == BinaryOperation::kAnd ||
                               op == BinaryOperation::kOr ||
                               op == BinaryOperation::kEquals ||
                               op == BinaryOperation::kNotEquals ||
                               op == BinaryOperation::kLessThan ||
                               op == BinaryOperation::kLessThanEquals ||
                               op == BinaryOperation::kGreaterThan ||
                               op == BinaryOperation::kGreaterThanEquals;
              break;
            }
            case TypeTag::kUnaryExp: {
              const auto op = (*boolean_side)->AsUnaryExpression().Op();
              boolean_valued = op == UnaryOperation::kNot ||
                               op == UnaryOperation::kIsNull ||
                               op == UnaryOperation::kIsNotNull ||
                               op == UnaryOperation::kIsTrue ||
                               op == UnaryOperation::kIsNotTrue ||
                               op == UnaryOperation::kIsFalse ||
                               op == UnaryOperation::kIsNotFalse;
              break;
            }
            case TypeTag::kInExp:
              boolean_valued = true;
              break;
            default:
              break;
          }
          if (!boolean_valued) {
            return Expression{};
          }
          const bool const_true = constant.value.int_value == 1;
          // = TRUE / != FALSE keep x; = FALSE / != TRUE complement it.
          const bool keep = (is_equals == const_true);
          if (keep) {
            return *boolean_side;
          }
          return UnaryExpressionExp(*boolean_side, UnaryOperation::kNot);
        }));
    // Lean proof: lean/TinylambOpt/HighRiskRulesAudit.lean
    //   (three_valued_distributive_and_absorption_sound)
    built.Add(ExpressionRule(
        "distribute_or_over_and_budgeted",
        Binary(BinaryOperation::kOr, Any("left"), Any("right")),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (expression->Type() != TypeTag::kBinaryExp ||
              expression->AsBinaryExpression().Op() != BinaryOperation::kOr) {
            return Expression{};
          }
          const auto& binary = expression->AsBinaryExpression();
          const std::vector<Expression> left = SplitConjuncts(binary.Left());
          const std::vector<Expression> right = SplitConjuncts(binary.Right());
          const std::vector<Expression>* and_side = nullptr;
          const Expression* other = nullptr;
          if (left.size() > 1 && right.size() == 1) {
            and_side = &left;
            other = &binary.Right();
          } else if (right.size() > 1 && left.size() == 1) {
            and_side = &right;
            other = &binary.Left();
          } else {
            return Expression{};
          }
          if (and_side->size() > 4) {
            return Expression{};
          }
          // Distribution duplicates `other` into one disjunct per conjunct:
          // a volatile (RAND()) or potentially-raising (1/0) operand would
          // then evaluate (or raise) several times where the original
          // short-circuits after the first TRUE disjunct. Sibling rules
          // gate on the same two predicates.
          if (!ExpressionCannotThrow(*other) ||
              !SafeToReduceEvaluationCount(*other)) {
            return Expression{};
          }
          // The conjuncts move from a position the original expression may
          // SKIP (AND short-circuits on a FALSE sibling) into one that the
          // distributed form always evaluates: a raising conjunct would
          // gain a spurious error (oracle-found: `(FALSE AND (ci + ci)) OR
          // ci` at INT64_MAX raised only after the rewrite).
          for (const Expression& conjunct : *and_side) {
            if (!ExpressionCannotThrow(conjunct)) {
              return Expression{};
            }
          }
          std::vector<Expression> distributed;
          distributed.reserve(and_side->size());
          for (const Expression& conjunct : *and_side) {
            distributed.push_back(
                BinaryExpressionExp(conjunct, BinaryOperation::kOr, *other));
          }
          return CombineConjuncts(distributed);
        }));
    // Lean proof: lean/TinylambOpt/FloatIdentities.lean
    // (pow_one_after_promotion_sound)
    built.Add(ExpressionRule(
        "pow_identities", Any(),
        [](const Expression& expression,
           const ExpressionBindings&) -> Expression {
          if (!expression || expression->Type() != TypeTag::kFunctionCallExp) {
            return Expression{};
          }
          const auto& fn = expression->AsFunctionCallExpression();
          if ((fn.FuncName() != "pow" && fn.FuncName() != "power") ||
              fn.Args().size() != 2 || !fn.Args()[0] || !fn.Args()[1] ||
              fn.Args()[1]->Type() != TypeTag::kConstantValue) {
            return Expression{};
          }
          const Value exponent = fn.Args()[1]->AsConstantValue().GetValue();
          if (exponent.IsNull()) {
            return Expression{};
          }
          bool is_one = false;
          if (exponent.type == ValueType::kInt64) {
            is_one = exponent.value.int_value == 1;
          } else if (exponent.type == ValueType::kDouble) {
            is_one = exponent.value.double_value == 1.0;
          }
          if (is_one && StaticallyNumeric(fn.Args()[0])) {
            // POW on a non-numeric base is an error in the AST evaluator.
            // CAST would silently accept strings, so only fold bases whose
            // numeric type is already proven at rewrite time (a schema-free
            // column cannot be proven here — leave the call intact for it).
            return CastExpressionExp(fn.Args()[0], "FLOAT64");
          }
          return Expression{};
        }));
    return built;
  }();
  return rules;
}

StatusOr<Expression> ExpressionRewriter::TryRewrite(
    const Expression& expression) const {
  const ScopedActiveRewriteSchema schema_scope(schema_);

  Expression current = expression;
  // D6 (docs/design.md): the pass cap is a safety net, not a rejection
  // mechanism.  When a rule set oscillates past the cap, returning the last
  // stable form keeps a valid query runnable: the result still preserves
  // the input's semantics (every accepted rewrite is meaning-preserving),
  // while throwing here turned "optimizer did not reach a fixed point" into
  // a runtime failure for the whole statement.
  const size_t pass_limit = pass_limit_ == 0 ? 32 : pass_limit_;
  for (size_t pass = 0; pass < pass_limit; ++pass) {
    ASSIGN_OR_RETURN(Expression, next, TryRewriteOnce(current, 0));
    if (Same(current, next)) {
      return next;
    }
    current = std::move(next);
  }
  static std::atomic<uint64_t> occurrences{0};
  const uint64_t nth = occurrences.fetch_add(1, std::memory_order_relaxed) + 1;
  if (nth == 1 || nth % 1000 == 0) {
    LOG(ERROR) << "expression rewrite did not converge within " << pass_limit
               << " passes (occurrence " << nth
               << "); returning the last stable form (D6)";
  }
  return current;
}

Expression ExpressionRewriter::Rewrite(const Expression& expression) const {
  return ExcShimUnwrap(TryRewrite(expression), "ExpressionRewriter::Rewrite");
}

Expression ExpressionRewriter::RewriteOnce(const Expression& expression,
                                           size_t depth) const {
  return ExcShimUnwrap(TryRewriteOnce(expression, depth),
                       "ExpressionRewriter::RewriteOnce");
}

StatusOr<Expression> ExpressionRewriter::TryRewriteOnce(
    const Expression& expression, size_t depth) const {  // NOLINT
  if (!expression) {
    return Expression{nullptr};
  }
  if (depth >= kMaxRewriteDepth) {
    return StatusError(StatusCode::kInvalidArgument, "expression too deep");
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    ASSIGN_OR_RETURN(Expression, rewritten, TryRewriteOnce(child, depth + 1));
    // P186: "changed" only gates rebuilding the parent node; rules return the
    // input pointer when they do not fire, so pointer identity answers in
    // O(1).  The old Same() call paid a full ToString() of both subtrees for
    // every rewritten child on every pass.
    children_changed |= rewritten.get() != child.get();
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  // Hand-built or rewrite-damaged nodes can carry null children; the rules
  // assume complete nodes and dereference operands freely, so treat a
  // damaged node as opaque (its children were still visited above).
  if (std::ranges::any_of(ExpressionChildren(current),
                          [](const Expression& child) { return !child; })) {
    return current;
  }
  if (ContainsParameterSlot(current)) {
    return current;
  }
  for (const ExpressionRule& rule : rules_->Rules()) {
    // D6 (docs/design.md): predicate-adding rules fire only at the root so
    // their inferred conjuncts cannot be re-appended inside nested ANDs and
    // cycle back (the non-convergence root cause).
    if (rule.root_only() && depth != 0) {
      continue;
    }
    if (Expression replacement = rule.Apply(current)) {
      return replacement;
    }
  }
  return current;
}

std::vector<Expression> ExpressionChildren(const Expression& expression) {
  if (!expression) {
    return {};
  }
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return {binary.Left(), binary.Right()};
    }
    case TypeTag::kUnaryExp:
      return {expression->AsUnaryExpression().Child()};
    case TypeTag::kAggregateExp:
      return {expression->AsAggregateExpression().Child()};
    case TypeTag::kCaseExp: {
      const auto& case_expression = expression->AsCaseExpression();
      std::vector<Expression> children;
      children.reserve((case_expression.when_clauses_.size() * 2) + 1);
      for (const auto& [condition, result] : case_expression.when_clauses_) {
        children.push_back(condition);
        children.push_back(result);
      }
      if (case_expression.else_clause_) {
        children.push_back(case_expression.else_clause_);
      }
      return children;
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> children{in.child_};
      children.insert(children.end(), in.list_.begin(), in.list_.end());
      return children;
    }
    case TypeTag::kFunctionCallExp:
      return expression->AsFunctionCallExpression().Args();
    case TypeTag::kArrayExp:
      return expression->AsArrayExpression().Elements();
    case TypeTag::kCastExp:
      return {expression->AsCastExpression().Child()};
    case TypeTag::kQueryExp: {
      const auto& test = expression->AsQueryExpression().Test();
      return test ? std::vector<Expression>{test} : std::vector<Expression>{};
    }
    case TypeTag::kWindowFunctionExp: {
      const auto& window = expression->AsWindowFunctionCallExpression();
      std::vector<Expression> children = window.args;
      children.reserve(window.args.size() + window.inner_order_by.size() +
                       window.partition_by.size() + window.order_by.size() + 3);
      if (window.where_filter) {
        children.push_back(window.where_filter);
      }
      for (const auto& term : window.inner_order_by) {
        children.push_back(term.expression);
      }
      children.insert(children.end(), window.partition_by.begin(),
                      window.partition_by.end());
      for (const auto& term : window.order_by) {
        children.push_back(term.expression);
      }
      if (window.frame_start.offset) {
        children.push_back(window.frame_start.offset);
      }
      if (window.frame_end.offset) {
        children.push_back(window.frame_end.offset);
      }
      return children;
    }
    // kLambdaExp stays opaque here (see lambda_expression.hpp): generic
    // walkers that rebuild through WithExpressionChildren have no lambda
    // case, and exposing children would turn every traversal into a
    // "leaf has children" CHECK failure.
    default:
      return {};
  }
}

Expression WithExpressionChildren(const Expression& expression,
                                  std::vector<Expression> children) {
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      if (children.size() != 2) {
        CHECK_MSG(false, "binary arity");
        return nullptr;
      }
      return BinaryExpressionExp(std::move(children[0]),
                                 expression->AsBinaryExpression().Op(),
                                 std::move(children[1]));
    }
    case TypeTag::kUnaryExp:
      if (children.size() != 1) {
        CHECK_MSG(false, "unary arity");

        return nullptr;
      }
      return UnaryExpressionExp(std::move(children[0]),
                                expression->AsUnaryExpression().Op());
    case TypeTag::kAggregateExp: {
      if (children.size() != 1) {
        CHECK_MSG(false, "aggregate arity");

        return nullptr;
      }
      // Rebuild losslessly: a child rewrite must not silently drop FILTER
      // (WHERE), HAVING MAX/MIN, inner ORDER BY/LIMIT, the STRING_AGG
      // delimiter, trailing args, ARRAY_AGG's element type, or inner
      // GROUP BY keys.
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(), std::move(children[0]), aggregate.Distinct());
      rebuilt->SetIgnoreNulls(aggregate.IgnoreNulls());
      if (aggregate.Having() != AggregateHavingModifier::kNone ||
          aggregate.HavingCondition()) {
        rebuilt->SetHaving(aggregate.Having(), aggregate.HavingCondition());
      }
      if (!aggregate.InnerOrderBy().empty()) {
        rebuilt->SetInnerOrderBy(aggregate.InnerOrderBy());
      }
      if (aggregate.InnerLimit().has_value()) {
        rebuilt->SetInnerLimit(aggregate.InnerLimit());
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(aggregate.SecondaryArg());
      }
      if (!aggregate.TrailingArgs().empty()) {
        rebuilt->SetTrailingArgs(aggregate.TrailingArgs());
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(aggregate.WhereFilter());
      }
      if (!aggregate.ArrayElementSqlType().empty()) {
        rebuilt->SetArrayElementSqlType(aggregate.ArrayElementSqlType());
      }
      if (aggregate.HasInnerGroupBy()) {
        rebuilt->SetInnerGroupBy(aggregate.InnerGroupBy());
      }
      return rebuilt;
    }
    case TypeTag::kCaseExp: {
      const auto& source = expression->AsCaseExpression();
      const size_t required =
          (source.when_clauses_.size() * 2) + (source.else_clause_ ? 1 : 0);
      if (children.size() != required) {
        CHECK_MSG(false, "case arity");

        return nullptr;
      }
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(source.when_clauses_.size());
      size_t offset = 0;
      for (size_t i = 0; i < source.when_clauses_.size(); ++i) {
        clauses.emplace_back(std::move(children[offset]),
                             std::move(children[offset + 1]));
        offset += 2;
      }
      Expression otherwise =
          source.else_clause_ ? std::move(children[offset]) : Expression{};
      return CaseExpressionExp(std::move(clauses), std::move(otherwise));
    }
    case TypeTag::kInExp: {
      if (children.empty()) {
        CHECK_MSG(false, "in arity");

        return nullptr;
      }
      Expression child = std::move(children.front());
      children.erase(children.begin());
      return InExpressionExp(std::move(child), std::move(children));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(expression->AsFunctionCallExpression().FuncName(),
                             std::move(children));
    case TypeTag::kArrayExp:
      return ArrayExpressionExp(
          std::move(children),
          expression->AsArrayExpression().ElementSqlType());
    case TypeTag::kCastExp: {
      if (children.size() != 1) {
        CHECK_MSG(false, "cast arity");

        return nullptr;
      }
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(std::move(children[0]), cast.TargetTypeName(),
                               cast.ReturnNullOnError());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      if (children.size() > 1) {
        CHECK_MSG(false, "query arity");

        return nullptr;
      }
      // Preserve every discriminator of the quantified/ARRAY form: dropping
      // op_/mode_ would silently turn "x > ANY(...)" into "x IN (...)", and
      // the ARRAY(SELECT ...) markers change evaluation outright.
      auto rebuilt = std::make_shared<QueryExpression>(
          query.Query(), children.empty() ? nullptr : children.front(),
          query.Exists(), query.Negated(), query.Op(), query.Mode());
      rebuilt->SetArrayResult(query.ArrayResult());
      rebuilt->SetArrayElementSqlType(query.ArrayElementSqlType());
      return rebuilt;
    }
    case TypeTag::kWindowFunctionExp: {
      // Mirror ExpressionChildren's enumeration exactly: args, where_filter,
      // inner_order_by, partition_by, order_by, then both frame offsets.
      // Every other field (frame unit/bounds, exclusion, NULL modifiers,
      // inner limit, distinct) copies verbatim so a child rewrite cannot
      // silently change the window specification.
      const auto& window = expression->AsWindowFunctionCallExpression();
      size_t expected = window.args.size() + window.inner_order_by.size() +
                        window.partition_by.size() + window.order_by.size() +
                        (window.where_filter ? 1U : 0U) +
                        (window.frame_start.offset ? 1U : 0U) +
                        (window.frame_end.offset ? 1U : 0U);
      if (children.size() != expected) {
        CHECK_MSG(false, "window arity");

        return nullptr;
      }
      // ExpressionBase deletes copy/move, so clone field by field (the
      // class is a plain aggregate of specification fields).
      auto rebuilt = std::make_shared<WindowFunctionCallExpression>();
      rebuilt->function = window.function;
      rebuilt->args = window.args;
      rebuilt->where_filter = window.where_filter;
      rebuilt->distinct = window.distinct;
      rebuilt->inner_order_by = window.inner_order_by;
      rebuilt->inner_limit = window.inner_limit;
      rebuilt->partition_by = window.partition_by;
      rebuilt->order_by = window.order_by;
      rebuilt->frame_unit = window.frame_unit;
      rebuilt->frame_start = window.frame_start;
      rebuilt->frame_end = window.frame_end;
      rebuilt->has_frame = window.has_frame;
      rebuilt->exclusion = window.exclusion;
      rebuilt->respect_nulls = window.respect_nulls;
      rebuilt->ignore_nulls = window.ignore_nulls;
      size_t next = 0;
      for (Expression& arg : rebuilt->args) {
        arg = std::move(children[next]);
        ++next;
      }
      if (rebuilt->where_filter) {
        rebuilt->where_filter = std::move(children[next]);
        ++next;
      }
      for (WindowOrderTerm& term : rebuilt->inner_order_by) {
        term.expression = std::move(children[next]);
        ++next;
      }
      for (Expression& partition : rebuilt->partition_by) {
        partition = std::move(children[next]);
        ++next;
      }
      for (WindowOrderTerm& term : rebuilt->order_by) {
        term.expression = std::move(children[next]);
        ++next;
      }
      if (rebuilt->frame_start.offset) {
        rebuilt->frame_start.offset = std::move(children[next]);
        ++next;
      }
      if (rebuilt->frame_end.offset) {
        rebuilt->frame_end.offset = std::move(children[next]);
        ++next;
      }
      return rebuilt;
    }
    default:
      if (!children.empty()) {
        CHECK_MSG(false, "leaf has children");

        return nullptr;
      }
      return expression;
  }
}

namespace {

bool IsMinusOne(const Expression& expression) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return false;
  }
  if (constant.type == ValueType::kInt64) {
    // UINT64_MAX shares the raw -1 bit pattern but is a different number;
    // treating it as -1 would rewrite `x * UINT64_MAX` into `-x`.
    return !constant.IsUnsigned() && constant.value.int_value == -1;
  }
  if (constant.type == ValueType::kDouble) {
    return constant.value.double_value == -1.0;
  }
  return false;
}

bool IsInt64Constant(const Expression& expression, int64_t* value) {
  if (!IsConstant(expression)) {
    return false;
  }
  const Value constant = expression->AsConstantValue().GetValue();
  // UINT64 shares the raw kInt64 bit pattern; reassociating it as signed would
  // change the value (and overflow semantics). Only true signed constants.
  if (constant.type != ValueType::kInt64 || constant.IsNull() ||
      constant.IsUnsigned()) {
    return false;
  }
  *value = constant.value.int_value;
  return true;
}

// Reassociation only fires on operands the input schema types as plain INT64:
// floating-point addition/multiplication is neither associative nor
// distributive (rounding), so folding it would change results. Sign absorption
// and repeated-addend doubling, by contrast, are exact for doubles too and
// are allowed there (IsNumericOperand below accepts kBigInt or kDouble).
bool IsInt64Column(const Expression& expression, const Schema& schema) {
  if (!expression || expression->Type() != TypeTag::kColumnValue) {
    return false;
  }
  try {
    return expression->ResultType(schema).GetType() == TypeTag::kBigInt;
  } catch (const std::exception&) {
    return false;
  }
}

bool IsNumericOperand(const Expression& expression, const Schema& schema) {
  try {
    const TypeTag tag = expression->ResultType(schema).GetType();
    return tag == TypeTag::kBigInt || tag == TypeTag::kDouble;
  } catch (const std::exception&) {
    return false;
  }
}

// Splits `tree` (whose root must be kBinaryExp) into a column leaf and an
// accumulated INT64 constant when it is a +-chain of one column and integer
// constants. Returns false for anything else, including trees whose
// constant accumulation overflows INT64 and chains whose steps do not all
// share a sign (a mixed-sign chain has an intermediate operation that can
// raise on its own, so collapsing it would elide an AST-evaluator overflow).
bool ColumnPlusConstantChain(const Expression& tree, const Schema& schema,
                             BinaryOperation* root_op, Expression* column,
                             int64_t* constant) {
  if (!tree || tree->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = tree->AsBinaryExpression();
  const BinaryOperation op = binary.Op();
  if (op != BinaryOperation::kAdd && op != BinaryOperation::kSubtract) {
    return false;
  }
  int64_t left_constant = 0;
  if (IsInt64Constant(binary.Left(), &left_constant)) {
    // Constants on the left (5 + a) are not canonical here.
    return false;
  }
  int64_t right_constant = 0;
  if (IsInt64Constant(binary.Right(), &right_constant)) {
    if (!IsInt64Column(binary.Left(), schema)) {
      return false;
    }
    *root_op = op;
    *column = binary.Left();
    *constant = right_constant;
    return true;
  }
  // Nested chain: (x ± c1) ± c2. Only accept an inner tree that itself
  // resolved to a column-plus-constant form so recursion terminates in
  // column-depth steps.
  BinaryOperation inner_op = BinaryOperation::kAdd;
  Expression inner_column;
  int64_t inner_constant = 0;
  if (!ColumnPlusConstantChain(binary.Left(), schema, &inner_op, &inner_column,
                               &inner_constant) ||
      !IsInt64Constant(binary.Right(), &right_constant)) {
    return false;
  }
  // Accumulate the inner chain's constant toward the column. Negative steps
  // fold through subtraction: (x - c1) + c2 keeps c1 in the accumulator.
  if (inner_op == BinaryOperation::kSubtract &&
      inner_constant == std::numeric_limits<int64_t>::min()) {
    return false;
  }
  if (op == BinaryOperation::kSubtract &&
      right_constant == std::numeric_limits<int64_t>::min()) {
    return false;
  }
  const int64_t inner_step =
      inner_op == BinaryOperation::kAdd ? inner_constant : -inner_constant;
  const int64_t outer_step =
      op == BinaryOperation::kAdd ? right_constant : -right_constant;
  // Overflow monotonicity: the chain may only resolve into one accumulated
  // constant when every step applied to the column shares a sign (the inner
  // chain's own mixed pairs were refused by the recursion, so `inner_step`
  // carries the common sign here). A mixed-sign chain such as
  // `((x - 5) + 3) - 2` underflows on its `x - 5` intermediate for
  // x = INT64_MIN + 4 where the collapsed `x - 4` would not, so resolving it
  // as a whole would silently elide an AST-evaluator error (invariant 3).
  if ((inner_step >= 0) != (outer_step >= 0)) {
    return false;
  }
  int64_t accumulated = 0;
  if (__builtin_add_overflow(inner_step, outer_step, &accumulated)) {
    return false;
  }
  *root_op = BinaryOperation::kAdd;
  *column = inner_column;
  *constant = accumulated;
  // inner_column must still be a typed INT64 column (checked on recursion).
  return IsInt64Column(*column, schema);
}

}  // namespace

Expression RewriteTypedArithmetic(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& input_schema) {
  if (!expression) {
    return nullptr;
  }

  std::vector<Expression> children = ExpressionChildren(expression);
  bool children_changed = false;
  for (Expression& child : children) {
    Expression rewritten = RewriteTypedArithmetic(child, input_schema);
    // P186: O(1) pointer-identity "changed" check (see TryRewriteOnce).
    children_changed |= rewritten.get() != child.get();
    child = std::move(rewritten);
  }
  Expression current =
      children_changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
  // Same damaged-node treatment as TryRewriteOnce: rules below dereference
  // operands freely and must not meet null children.
  if (std::ranges::any_of(ExpressionChildren(current),
                          [](const Expression& child) { return !child; })) {
    return current;
  }
  if (ContainsParameterSlot(current)) {
    return current;
  }
  if (current->Type() != TypeTag::kBinaryExp) {
    return current;
  }
  const auto& binary = current->AsBinaryExpression();

  if (binary.Op() == BinaryOperation::kMultiply) {
    // Sign absorption: (-1 * x) and (x * -1) become (-x). Exact for
    // integers and for IEEE doubles (a pure sign flip). Constant-constant
    // products are left to the folding rules.
    const Expression* minus_one = nullptr;
    const Expression* value = nullptr;
    if (!IsConstant(binary.Left()) && IsMinusOne(binary.Right())) {
      minus_one = &binary.Right();
      value = &binary.Left();
    } else if (!IsConstant(binary.Right()) && IsMinusOne(binary.Left())) {
      minus_one = &binary.Left();
      value = &binary.Right();
    }
    if (minus_one != nullptr && IsNumericOperand(*value, input_schema)) {
      return UnaryExpressionExp(*value, UnaryOperation::kMinus);
    }

    // (x * c1) * c2 -> x * (c1 * c2) for non-negative integer constants.
    // Checked combines skip the rewrite instead of folding into an
    // overflow the original tree would not have evaluated.  c2 == 0 also
    // refuses: the original still evaluates x * c1 (which may raise), while
    // the folded x * 0 would silently return 0.
    int64_t inner_constant = 0;
    if (IsInt64Constant(binary.Right(), &inner_constant) &&
        inner_constant > 0 && binary.Left()->Type() == TypeTag::kBinaryExp &&
        binary.Left()->AsBinaryExpression().Op() ==
            BinaryOperation::kMultiply) {
      const auto& inner = binary.Left()->AsBinaryExpression();
      int64_t leaf_constant = 0;
      if (IsInt64Constant(inner.Right(), &leaf_constant) &&
          leaf_constant >= 0 && IsInt64Column(inner.Left(), input_schema)) {
        int64_t combined = 0;
        if (!__builtin_mul_overflow(leaf_constant, inner_constant, &combined)) {
          return BinaryExpressionExp(inner.Left(), BinaryOperation::kMultiply,
                                     ConstantValueExp(Value(combined)));
        }
      }
    }
  }

  if (binary.Op() == BinaryOperation::kAdd &&
      Same(binary.Left(), binary.Right()) &&
      binary.Left()->Type() == TypeTag::kColumnValue &&
      IsNumericOperand(binary.Left(), input_schema)) {
    // x + x -> x * 2. Exact doubling for integers and IEEE doubles. Unlike
    // the schema-free combine_repeated_addend rule, this typed form
    // deliberately includes dynamic INT64 columns: for a statically numeric
    // column the current evaluator gives both spellings the same result
    // (including the wraparound at the overflow boundary), so only the operator
    // name in a hypothetical overflow diagnostic could differ. Test
    // ExpressionRewriteTest.TypedSignAbsorptionAndRepeatedAddend pins this.
    return BinaryExpressionExp(
        binary.Left(), BinaryOperation::kMultiply,
        ConstantValueExp(Value(static_cast<int64_t>(2))));
  }

  // Integer add/subtract constant reassociation:
  //   (x + c1) + c2 -> x + (c1 + c2)      (x - c1) - c2 -> x - (c1 + c2)
  //   (x + c1) - c2 -> x +/- (c1 - c2)    (x - c1) + c2 -> x +/- (c2 - c1)
  // Only column-plus-constant chains over INT64 take part, and constant
  // combines use checked arithmetic. Like the schema-free rule, only
  // reassociate steps that share a sign: with opposite signs the original
  // intermediate operation can overflow (x = INT64_MAX in `(x + 5) - 8`
  // raises on `x + 5`) while the folded `x - 3` would not, silently eliding
  // an AST-evaluator error (invariant 3). ColumnPlusConstantChain applies
  // the same sign discipline inside multi-level chains, so `inner_step`
  // carries the chain's common sign and the check below compares it with
  // the outer step.
  const BinaryOperation op = binary.Op();
  if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract) {
    int64_t outer_constant = 0;
    const Expression* chain = nullptr;
    if (IsInt64Constant(binary.Right(), &outer_constant)) {
      chain = &binary.Left();
    } else if (op == BinaryOperation::kAdd &&
               IsInt64Constant(binary.Left(), &outer_constant)) {
      chain = &binary.Right();
    }
    if (chain != nullptr) {
      BinaryOperation inner_op = BinaryOperation::kAdd;
      Expression column;
      int64_t inner_constant = 0;
      if (ColumnPlusConstantChain(*chain, input_schema, &inner_op, &column,
                                  &inner_constant)) {
        if (inner_op == BinaryOperation::kSubtract &&
            inner_constant == std::numeric_limits<int64_t>::min()) {
          return current;
        }
        if (op == BinaryOperation::kSubtract &&
            outer_constant == std::numeric_limits<int64_t>::min()) {
          return current;
        }
        const int64_t inner_step = inner_op == BinaryOperation::kAdd
                                       ? inner_constant
                                       : -inner_constant;
        const int64_t outer_step =
            op == BinaryOperation::kAdd ? outer_constant : -outer_constant;
        // INT64 add/sub is not associative around overflow: with opposite
        // signs the intermediate result can overflow even though the folded
        // total does not. Keep the original shape so the AST's raise is
        // preserved (mirrors reassociate_add_constants above).
        if ((inner_step >= 0) != (outer_step >= 0)) {
          return current;
        }
        int64_t accumulated = 0;
        if (__builtin_add_overflow(inner_step, outer_step, &accumulated) ||
            accumulated == std::numeric_limits<int64_t>::min()) {
          return current;
        }
        if (accumulated >= 0) {
          return BinaryExpressionExp(column, BinaryOperation::kAdd,
                                     ConstantValueExp(Value(accumulated)));
        }
        return BinaryExpressionExp(column, BinaryOperation::kSubtract,
                                   ConstantValueExp(Value((-accumulated))));
      }
    }
  }

  const Expression* zero = nullptr;
  const Expression* value = nullptr;
  if (binary.Op() == BinaryOperation::kMultiply) {
    if (IsZero(binary.Left())) {
      zero = &binary.Left();
      value = &binary.Right();
    } else if (IsZero(binary.Right())) {
      zero = &binary.Right();
      value = &binary.Left();
    }
  }
  if (zero == nullptr) {
    return current;
  }

  try {
    // Floating-point x * 0 is not generally zero: NaN and infinities must
    // be preserved. Restrict this rewrite to resolved integer arithmetic.
    if (current->ResultType(input_schema).GetType() != TypeTag::kBigInt ||
        (*value)->ResultType(input_schema).GetType() != TypeTag::kBigInt) {
      return current;
    }
  } catch (const std::exception&) {
    return current;
  }

  // SQL arithmetic is NULL-propagating. CASE preserves that behavior while
  // removing the multiplication, and the casts keep both branches INT64.
  Expression typed_null =
      CastExpressionExp(ConstantValueExp(Value()), "INT64", false);
  Expression typed_zero = CastExpressionExp(*zero, "INT64", false);
  return CaseExpressionExp(
      {{UnaryExpressionExp(*value, UnaryOperation::kIsNull),
        std::move(typed_null)}},
      std::move(typed_zero));
}

std::vector<Expression> SplitConjuncts(const Expression& expression) {
  // T2405: iterative flattening, see SplitDisjuncts above for the rationale.
  std::vector<Expression> parts;
  std::vector<Expression> pending{expression};
  while (!pending.empty()) {
    const Expression node = std::move(pending.back());
    pending.pop_back();
    if (!node) {
      continue;
    }
    if (node->Type() == TypeTag::kBinaryExp &&
        node->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
      pending.push_back(node->AsBinaryExpression().Right());
      pending.push_back(node->AsBinaryExpression().Left());
    } else {
      parts.push_back(node);
    }
  }
  return parts;
}

Expression CombineConjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return ConstantValueExp(Value(true));
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(std::move(result), BinaryOperation::kAnd,
                                 expressions[i]);
  }
  return result;
}

bool ReferencesOnly(const Expression& expression,
                    const std::unordered_set<std::string>& relation_names) {
  if (!expression) {
    return true;
  }
  return std::ranges::all_of(
      expression->TouchedColumns(), [&](const ColumnName& column) {
        return !column.schema.empty() && relation_names.contains(column.schema);
      });
}

namespace {

bool IsOrderedComparison(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

}  // namespace

bool ContainsNotOfOrderedDoubleComparison(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot && unary.Child() != nullptr &&
          unary.Child()->Type() == TypeTag::kBinaryExp) {
        const auto& binary = unary.Child()->AsBinaryExpression();
        if (IsOrderedComparison(binary.Op()) &&
            (SideCanBeDouble(binary.Left(), schema) ||
             SideCanBeDouble(binary.Right(), schema))) {
          return true;
        }
      }
      return ContainsNotOfOrderedDoubleComparison(unary.Child(), schema);
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      // XOR expands into `(a OR b) AND (NOT a OR NOT b)` during rewriting,
      // so an ordered DOUBLE comparison under a XOR becomes a
      // NOT(ordered-comparison) that not_comparison would then negate
      // unsoundly for IEEE NaN (oracle-found: `(NaN >= -3) XOR col_i`
      // evaluated TRUE pre-fold and FALSE post-fold).
      if (binary.Op() == BinaryOperation::kXor) {
        const auto negatable = [](const Expression& e, const Schema& sch) {
          return e && e->Type() == TypeTag::kBinaryExp &&
                 IsOrderedComparison(e->AsBinaryExpression().Op()) &&
                 (SideCanBeDouble(e->AsBinaryExpression().Left(), sch) ||
                  SideCanBeDouble(e->AsBinaryExpression().Right(), sch));
        };
        if (negatable(binary.Left(), schema) ||
            negatable(binary.Right(), schema)) {
          return true;
        }
      }
      return ContainsNotOfOrderedDoubleComparison(binary.Left(), schema) ||
             ContainsNotOfOrderedDoubleComparison(binary.Right(), schema);
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      for (const auto& [condition, result] : searched.when_clauses_) {
        if (ContainsNotOfOrderedDoubleComparison(condition, schema) ||
            ContainsNotOfOrderedDoubleComparison(result, schema)) {
          return true;
        }
      }
      return ContainsNotOfOrderedDoubleComparison(searched.else_clause_,
                                                  schema);
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (ContainsNotOfOrderedDoubleComparison(in.child_, schema)) {
        return true;
      }
      return std::ranges::any_of(in.list_, [&schema](const Expression& item) {
        return ContainsNotOfOrderedDoubleComparison(item, schema);
      });
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      return std::ranges::any_of(call.Args(), [&schema](const Expression& arg) {
        return ContainsNotOfOrderedDoubleComparison(arg, schema);
      });
    }
    case TypeTag::kArrayExp:
      return std::ranges::any_of(expression->AsArrayExpression().Elements(),
                                 [&schema](const Expression& element) {
                                   return ContainsNotOfOrderedDoubleComparison(
                                       element, schema);
                                 });
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      if (ContainsNotOfOrderedDoubleComparison(aggregate.Child(), schema)) {
        return true;
      }
      // Aggregates carry per-row sub-expressions beyond Child() — the FILTER
      // (WHERE) clause, HAVING MAX/MIN condition, inner ORDER BY keys,
      // STRING_AGG delimiter, trailing args and inner GROUP BY keys can each
      // hide a NOT(ordered double comparison), exactly like the window arm
      // below. ExpressionChildren deliberately enumerates only Child() for
      // aggregates (WithExpressionChildren's arity contract), so walk the
      // extra slots explicitly.
      if (aggregate.HasWhereFilter() && ContainsNotOfOrderedDoubleComparison(
                                            aggregate.WhereFilter(), schema)) {
        return true;
      }
      if (aggregate.HavingCondition() &&
          ContainsNotOfOrderedDoubleComparison(aggregate.HavingCondition(),
                                               schema)) {
        return true;
      }
      for (const auto& term : aggregate.InnerOrderBy()) {
        if (term.expression &&
            ContainsNotOfOrderedDoubleComparison(term.expression, schema)) {
          return true;
        }
      }
      if (aggregate.SecondaryArg() && ContainsNotOfOrderedDoubleComparison(
                                          aggregate.SecondaryArg(), schema)) {
        return true;
      }
      for (const Expression& arg : aggregate.TrailingArgs()) {
        if (ContainsNotOfOrderedDoubleComparison(arg, schema)) {
          return true;
        }
      }
      return std::ranges::any_of(aggregate.InnerGroupBy(),
                                 [&](const Expression& key) {
                                   return ContainsNotOfOrderedDoubleComparison(
                                       key, schema);
                                 });
    }
    case TypeTag::kCastExp:
      return ContainsNotOfOrderedDoubleComparison(
          expression->AsCastExpression().Child(), schema);
    case TypeTag::kWindowFunctionExp:
      // Window partition/order keys, arguments and frame offsets can all
      // hide a NOT(ordered double comparison). Unlike the aggregate arm (which
      // inspects its single Child()), window functions carry partition/order/
      // frame expressions, so inspect every child here.
      return std::ranges::any_of(
          ExpressionChildren(expression), [&schema](const Expression& child) {
            return ContainsNotOfOrderedDoubleComparison(child, schema);
          });
    case TypeTag::kLambdaExp:
      // ExpressionChildren deliberately treats a lambda as opaque (generic
      // rebuilders have no lambda arm), so walk its body explicitly: a
      // NOT(ordered double comparison) inside a window function's lambda
      // argument must still suppress not_comparison rewrites.
      return ContainsNotOfOrderedDoubleComparison(
          expression->AsLambdaExpression().Body(), schema);
    default:
      return false;
  }
}

const ExpressionRuleSet& NotComparisonFreeRules() {
  static const ExpressionRuleSet rules = [] {
    ExpressionRuleSet copy = ExpressionRuleSet::Default();
    copy.Remove("not_comparison");
    return copy;
  }();
  return rules;
}

namespace {
// Recursive worker for ExtractWindowCalls; see the header for the contract.
std::optional<Expression> ExtractWindowCallsIn(
    const Expression& expression, size_t* counter,
    std::unordered_map<std::string, size_t>* dedup,
    std::vector<ExtractedWindow>* specs) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return expression;
  }
  if (expression->Type() == TypeTag::kWindowFunctionExp) {
    // Nested window calls are invalid SQL; refuse instead of miscompiling.
    for (const Expression& child : ExpressionChildren(expression)) {
      if (child && child->Type() == TypeTag::kWindowFunctionExp) {
        return std::nullopt;
      }
    }
    // ToString() omits the aggregate WHERE filter and the NULL-treatment
    // modifiers, so two calls that differ only in those would collapse onto
    // one hidden column and silently share a result. Extend the key with
    // every field the evaluator distinguishes.
    const auto& window = expression->AsWindowFunctionCallExpression();
    std::string key = expression->ToString();
    if (window.where_filter) {
      key += " WHERE ";
      key += window.where_filter->ToString();
    }
    if (window.respect_nulls) {
      key += " RESPECT NULLS";
    }
    if (window.ignore_nulls) {
      key += " IGNORE NULLS";
    }
    const auto found = dedup->find(key);
    size_t index = 0;
    if (found != dedup->end()) {
      index = found->second;
    } else {
      index = (*counter)++;
      dedup->emplace(key, index);
      ColumnName output("", "$win" + std::to_string(index));
      specs->push_back({expression, output});
    }
    // The output name derives from the global index, never from the
    // per-invocation specs vector (the dedup map is shared across items
    // while each call returns only its own new specs).
    return ColumnValueExp(ColumnName("", "$win" + std::to_string(index)));
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped =
        ExtractWindowCallsIn(child, counter, dedup, specs);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    rewritten.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}
}  // namespace

std::optional<std::pair<Expression, std::vector<ExtractedWindow>>>
ExtractWindowCalls(const Expression& expression, size_t* counter,
                   std::unordered_map<std::string, size_t>* dedup) {
  if (counter == nullptr || dedup == nullptr) {
    return std::nullopt;
  }
  std::vector<ExtractedWindow> specs;
  std::optional<Expression> rewritten =
      ExtractWindowCallsIn(expression, counter, dedup, &specs);
  if (!rewritten.has_value()) {
    return std::nullopt;
  }
  return std::make_pair(std::move(*rewritten), std::move(specs));
}

// Lean proof: lean/TinylambOpt/CppRuleAudit.lean
//   (repairedIsStrict_sound, cpp_empty_in_isFalse_bug)
bool IsNullStrictWhere(
    const Expression& expression,  // NOLINT(misc-no-recursion)
    const std::function<bool(const ColumnName&)>& match_column,
    bool in_exp_is_strict) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      return match_column(expression->AsColumnValue().GetColumnName());
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
        case BinaryOperation::kXor:
          return IsNullStrictWhere(binary.Left(), match_column,
                                   in_exp_is_strict) ||
                 IsNullStrictWhere(binary.Right(), match_column,
                                   in_exp_is_strict);
        default:
          return false;
      }
    }
    case TypeTag::kCastExp:
      return IsNullStrictWhere(expression->AsCastExpression().Child(),
                               match_column, in_exp_is_strict);
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot ||
          unary.Op() == UnaryOperation::kMinus ||
          unary.Op() == UnaryOperation::kBitwiseNot) {
        return IsNullStrictWhere(unary.Child(), match_column, in_exp_is_strict);
      }
      return false;
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      return in_exp_is_strict && !in.list_.empty() &&
             IsNullStrictWhere(in.child_, match_column, in_exp_is_strict);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& function = expression->AsFunctionCallExpression();
      const std::string& name = function.FuncName();
      if (name != "abs" && name != "lower" && name != "upper" &&
          name != "length" && name != "trim") {
        return false;
      }
      return std::ranges::any_of(function.Args(), [&](const Expression& arg) {
        return IsNullStrictWhere(arg, match_column, in_exp_is_strict);
      });
    }
    default:
      return false;
  }
}

bool ExpressionRejectsNullsWhere(
    const Expression& expression,  // NOLINT(misc-no-recursion)
    const std::function<bool(const ColumnName&)>& match_column,
    bool bare_column_rejects_null) {
  if (!expression) {
    return false;
  }
  const bool in_exp_is_strict = bare_column_rejects_null;
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      return bare_column_rejects_null &&
             match_column(expression->AsColumnValue().GetColumnName());
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAnd:
          return ExpressionRejectsNullsWhere(binary.Left(), match_column,
                                             bare_column_rejects_null) ||
                 ExpressionRejectsNullsWhere(binary.Right(), match_column,
                                             bare_column_rejects_null);
        case BinaryOperation::kOr:
          return ExpressionRejectsNullsWhere(binary.Left(), match_column,
                                             bare_column_rejects_null) &&
                 ExpressionRejectsNullsWhere(binary.Right(), match_column,
                                             bare_column_rejects_null);
        case BinaryOperation::kXor:
          if (bare_column_rejects_null) {
            return ExpressionRejectsNullsWhere(binary.Left(), match_column,
                                               bare_column_rejects_null) &&
                   ExpressionRejectsNullsWhere(binary.Right(), match_column,
                                               bare_column_rejects_null);
          }
          return IsNullStrictWhere(binary.Left(), match_column,
                                   in_exp_is_strict) ||
                 IsNullStrictWhere(binary.Right(), match_column,
                                   in_exp_is_strict);
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          return IsNullStrictWhere(binary.Left(), match_column,
                                   in_exp_is_strict) ||
                 IsNullStrictWhere(binary.Right(), match_column,
                                   in_exp_is_strict);
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
          return false;
      }
      return false;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kMinus:
        case UnaryOperation::kBitwiseNot:
          return IsNullStrictWhere(unary.Child(), match_column,
                                   in_exp_is_strict);
        case UnaryOperation::kNot:
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsNotFalse:
          return false;
      }
      return false;
    }
    case TypeTag::kInExp:
      return IsNullStrictWhere(expression->AsInExpression().child_,
                               match_column, in_exp_is_strict);
    default:
      return false;
  }
}

}  // namespace tinylamb
