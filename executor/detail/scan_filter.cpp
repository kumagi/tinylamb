/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/scan_filter.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

StatusOr<bool> EvaluateConjunctsTolerant(
    const std::vector<Expression>& conjuncts,
    const std::function<StatusOr<Value>(const Expression&)>& eval) {
  Status first_error{Status::kSuccess};
  for (const Expression& conjunct : conjuncts) {
    if (!conjunct) {
      continue;
    }
    StatusOr<Value> res = eval(conjunct);
    if (!res.HasValue()) {
      if (first_error.ok()) {
        first_error = res.GetStatus();
      }
      continue;
    }
    const Value& value = res.Value();
    if (value.IsNull() || !value.Truthy()) {
      // In SQL, conjunction is commutative: if any conjunct cleanly
      // evaluates to FALSE or NULL, the row is discarded by the WHERE
      // filter. Any error in another conjunct on this row must not abort
      // the query.
      return false;
    }
  }
  if (!first_error.ok()) {
    return first_error;
  }
  return true;
}

namespace {

BinaryOperation FlipCompare(BinaryOperation operation) {
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

}  // namespace

namespace {

// Raw comparison dispatch shared by the column-vs-column fast path.  Callers
// pre-verify the type pair, so every case below is total.
template <typename Left, typename Right>
bool ComparePositional(BinaryOperation op, const Left& left,
                       const Right& right) {
  switch (op) {
    case BinaryOperation::kEquals:
      return left == right;
    case BinaryOperation::kNotEquals:
      return left != right;
    case BinaryOperation::kLessThan:
      return left < right;
    case BinaryOperation::kLessThanEquals:
      return left <= right;
    case BinaryOperation::kGreaterThan:
      return left > right;
    case BinaryOperation::kGreaterThanEquals:
      return left >= right;
    default:
      return false;
  }
}

bool IsIntLike(const Value& value) {
  return value.type == ValueType::kInt64 || value.type == ValueType::kDate;
}

}  // namespace

bool FastLikeMatch(std::string_view value, std::string_view pattern) {
  if (pattern.find('_') == std::string_view::npos) {
    const size_t first_wild = pattern.find('%');
    if (first_wild == std::string_view::npos) {
      return value == pattern;
    }
    size_t pos = 0;
    if (first_wild > 0) {
      if (value.size() < first_wild ||
          std::memcmp(value.data(), pattern.data(), first_wild) != 0) {
        return false;
      }
      pos = first_wild;
    }
    const size_t last_wild = pattern.rfind('%');
    const size_t tail_len = pattern.size() - last_wild - 1;
    size_t p = first_wild;
    while (p < last_wild) {
      size_t sep = pattern.find('%', p + 1);
      if (sep == std::string_view::npos || sep > last_wild) {
        sep = last_wild;
      }
      const size_t seg_len = sep - (p + 1);
      if (seg_len > 0) {
        const size_t avail =
            value.size() >= pos + tail_len ? value.size() - tail_len - pos : 0;
        if (seg_len > avail) {
          return false;
        }
        const size_t found =
            value.substr(pos, avail).find(pattern.substr(p + 1, seg_len));
        if (found == std::string_view::npos) {
          return false;
        }
        pos += found + seg_len;
      }
      p = sep;
    }
    if (tail_len > 0) {
      if (value.size() < pos + tail_len ||
          std::memcmp(value.data() + value.size() - tail_len,
                      pattern.data() + last_wild + 1, tail_len) != 0) {
        return false;
      }
    }
    return true;
  }
  size_t value_pos = 0;
  size_t pattern_pos = 0;
  size_t wildcard = std::string_view::npos;
  size_t retry = 0;
  while (value_pos < value.size()) {
    if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
      wildcard = pattern_pos++;
      retry = value_pos;
    } else if (pattern_pos < pattern.size() &&
               (pattern[pattern_pos] == '_' ||
                pattern[pattern_pos] == value[value_pos])) {
      ++value_pos;
      ++pattern_pos;
    } else if (wildcard != std::string_view::npos) {
      pattern_pos = wildcard + 1;
      value_pos = ++retry;
    } else {
      return false;
    }
  }
  while (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
    ++pattern_pos;
  }
  return pattern_pos == pattern.size();
}

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred) {
  if (pred.column_payload) {
    const Value& left = row[pred.column];
    const Value& right = row[pred.right_column];
    // NULL on either side compares to UNKNOWN: the row is rejected, exactly
    // like the constant path below treats a NULL column value.
    if (left.IsNull() || right.IsNull()) {
      return false;
    }
    if (IsIntLike(left) && IsIntLike(right)) {
      if (left.IsUnsigned() || right.IsUnsigned()) {
        StatusOr<Value> cmp = TryBinary(pred.op, left, right);
        return cmp.HasValue() && cmp.Value().Truthy();
      }
      return ComparePositional(pred.op, left.value.int_value,
                               right.value.int_value);
    }
    if (left.type == ValueType::kDouble && right.type == ValueType::kDouble) {
      return ComparePositional(pred.op, left.value.double_value,
                               right.value.double_value);
    }
    if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
      if (left.IsCaseInsensitive() || right.IsCaseInsensitive()) {
        StatusOr<Value> cmp = TryBinary(pred.op, left, right);
        return cmp.HasValue() && cmp.Value().Truthy();
      }
      return ComparePositional(
          pred.op, static_cast<std::string_view>(left.value.varchar_value),
          static_cast<std::string_view>(right.value.varchar_value));
    }
    // Compile time only admits like-kind pairs, but a stored value can still
    // surprise (e.g. mixed-layout rows shared with a differently-typed
    // cache); route the rest through the exact evaluator rather than
    // guessing a verdict.
    StatusOr<Value> cmp = TryBinary(pred.op, left, right);
    return cmp.HasValue() && cmp.Value().Truthy();
  }
  const Value& value = row[pred.column];
  if (value.IsNull() || pred.constant.IsNull()) {
    return false;
  }
  // Plain string constants pre-qualified at compile time compare byte-exact;
  // routing them through TryEvaluateBinary cost two extra scans per row.
  // A cell carrying a case-insensitive collation tag (e.g. a CTE column
  // produced by COLLATE(s, 'und:ci')) must fold both sides like the checked
  // evaluator does; only untagged cells may take the byte-exact route.
  if (pred.plain_string_payload && value.type == ValueType::kVarChar &&
      value.IsCaseInsensitive()) {
    StatusOr<Value> cmp = TryBinary(pred.op, value, pred.constant);
    return cmp.HasValue() && cmp.Value().Truthy();
  }
  if (pred.plain_string_payload && value.type == ValueType::kVarChar) {
    const std::string_view l = value.value.varchar_value;
    const std::string_view r = pred.constant.value.varchar_value;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return l == r;
      case BinaryOperation::kNotEquals:
        return l != r;
      case BinaryOperation::kLessThan:
        return l < r;
      case BinaryOperation::kLessThanEquals:
        return l <= r;
      case BinaryOperation::kGreaterThan:
        return l > r;
      case BinaryOperation::kGreaterThanEquals:
        return l >= r;
      default:
        break;
    }
  }
  // Unsigned integers share the INT64 storage but compare under unsigned
  // semantics in the ground-truth evaluator. The integer fast paths below
  // are all signed, so any unsigned involvement must go through
  // EvaluateBinary (e.g. UINT64_MAX, stored as bit pattern -1, is NOT < 0).
  if ((value.type == ValueType::kInt64 || value.type == ValueType::kDate) &&
      (value.IsUnsigned() || pred.constant.IsUnsigned())) {
    StatusOr<Value> cmp = TryBinary(pred.op, value, pred.constant);
    return cmp.HasValue() && cmp.Value().Truthy();
  }

  // The scan fast path must use the same coercions as the full expression
  // evaluator.  In particular, DATE/TIMESTAMP columns commonly receive bare
  // string literals from an IN list; rejecting the type mismatch here would
  // silently filter the matching row before the residual evaluator sees it.
  if (value.type == ValueType::kDate &&
      pred.constant.type == ValueType::kVarChar) {
    try {
      const int64_t rhs =
          pred.date_string_payload
              ? pred.int_constant
              : ParseDateDays(pred.constant.value.varchar_value);
      const int64_t lhs = value.value.int_value;
      switch (pred.op) {
        case BinaryOperation::kEquals:
          return lhs == rhs;
        case BinaryOperation::kNotEquals:
          return lhs != rhs;
        case BinaryOperation::kLessThan:
          return lhs < rhs;
        case BinaryOperation::kLessThanEquals:
          return lhs <= rhs;
        case BinaryOperation::kGreaterThan:
          return lhs > rhs;
        case BinaryOperation::kGreaterThanEquals:
          return lhs >= rhs;
        default:
          break;
      }
    } catch (...) {
      return false;
    }
  }
  if (value.type == ValueType::kVarChar &&
      pred.constant.type == ValueType::kVarChar) {
    if ((pred.op == BinaryOperation::kLike ||
         pred.op == BinaryOperation::kNotLike) &&
        !value.IsCaseInsensitive() && !pred.constant.IsCaseInsensitive()) {
      const bool matched = FastLikeMatch(value.value.varchar_value,
                                         pred.constant.value.varchar_value);
      return pred.op == BinaryOperation::kLike ? matched : !matched;
    }
    StatusOr<Value> cmp = TryBinary(pred.op, value, pred.constant);
    return cmp.HasValue() && cmp.Value().Truthy();
  }

  if (pred.int_payload &&
      (value.type == ValueType::kInt64 || value.type == ValueType::kDate)) {
    const int64_t left = value.value.int_value;
    const int64_t right = pred.int_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }
  if (pred.double_payload && value.type == ValueType::kDouble) {
    const double left = value.value.double_value;
    const double right = pred.double_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }

  const auto as_double = [](const Value& v) -> std::optional<double> {
    if (v.type == ValueType::kDouble) {
      return v.value.double_value;
    }
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return static_cast<double>(v.value.int_value);
    }
    return std::nullopt;
  };
  const auto as_int = [](const Value& v) -> std::optional<int64_t> {
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return v.value.int_value;
    }
    return std::nullopt;
  };

  switch (pred.op) {
    case BinaryOperation::kEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value == pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li == *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld == *rd;
      }
      return false;
    }
    case BinaryOperation::kNotEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value != pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li != *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld != *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value < pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li < *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld < *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value <= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li <= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld <= *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value > pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li > *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld > *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value >= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li >= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld >= *rd;
      }
      return false;
    }
    default:
      return false;
  }
}

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  // The type-blind rewrite may turn NOT(x < y) into NaN-unsound x >= y (see
  // BytecodeCompiler::Compile); a pre-filter built from that would silently
  // drop rows, so use the same suppression when doubles can reach a negated
  // ordered comparison.
  ExpressionRewriter rewriter(
      ContainsNotOfOrderedDoubleComparison(predicate, schema)
          ? NotComparisonFreeRules()
          : ExpressionRuleSet::Default());
  rewriter.set_schema(&schema);
  StatusOr<Expression> folded_or = rewriter.TryRewrite(predicate);
  if (!folded_or.HasValue()) {
    return std::nullopt;
  }
  Expression folded = folded_or.MoveValue();
  if (!folded || folded->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const BinaryExpression& binary = folded->AsBinaryExpression();
  const bool is_like = binary.Op() == BinaryOperation::kLike ||
                       binary.Op() == BinaryOperation::kNotLike;
  if (!IsComparison(binary.Op()) && !is_like) {
    return std::nullopt;
  }
  Expression column = binary.Left();
  Expression constant = binary.Right();
  BinaryOperation op = binary.Op();
  auto try_fold_closed_constant = [](Expression* expr_ptr) {
    const Expression& expr = *expr_ptr;
    if (!expr || expr->Type() == TypeTag::kConstantValue ||
        expr->Type() == TypeTag::kColumnValue) {
      return;
    }
    if (!ContainsVolatileCall(expr) && !ContainsParameterSlot(expr) &&
        !ContainsQuery(expr) && expr->TouchedColumns().empty()) {
      StatusOr<Value> val = expr->TryEvaluate(Row(), Schema());
      if (val.HasValue()) {
        *expr_ptr = ConstantValueExp(val.MoveValue());
      }
    }
  };
  try_fold_closed_constant(&column);
  try_fold_closed_constant(&constant);
  if (!is_like && column->Type() == TypeTag::kConstantValue &&
      (constant->Type() == TypeTag::kColumnValue ||
       constant->Type() == TypeTag::kFunctionCallExp)) {
    std::swap(column, constant);
    op = FlipCompare(op);
  }
  // In a filter, COALESCE(column, literal) can use the ordinary comparison
  // when the fallback cannot pass. Both FALSE and NULL reject that row; the
  // non-NULL case still compares the original column with the same constant.
  if (!is_like && column->Type() == TypeTag::kFunctionCallExp &&
      constant->Type() == TypeTag::kConstantValue &&
      !ContainsParameterSlot(column) && !ContainsParameterSlot(constant)) {
    const auto& function = column->AsFunctionCallExpression();
    const auto& args = function.Args();
    if ((function.FuncName() == "coalesce" ||
         function.FuncName() == "ifnull") &&
        args.size() == 2 && args[0]->Type() == TypeTag::kColumnValue &&
        args[1]->Type() == TypeTag::kConstantValue) {
      const auto slot =
          LocalColumnOffset(schema, args[0]->AsColumnValue().GetColumnName());
      const auto& fallback = args[1]->AsConstantValue().GetValue();
      const auto& threshold = constant->AsConstantValue().GetValue();
      if (slot) {
        const auto& physical = schema.GetColumn(*slot);
        if (!physical.IsUnsigned() &&
            (physical.Type() == ValueType::kInt64 ||
             physical.Type() == ValueType::kDouble) &&
            (fallback.IsNull() ||
             (fallback.type == physical.Type() && !fallback.IsUnsigned())) &&
            !threshold.IsUnsigned() &&
            (threshold.type == ValueType::kInt64 ||
             threshold.type == ValueType::kDouble)) {
          auto verdict = TryBinary(op, fallback, threshold);
          if (verdict.HasValue() && !Truthy(verdict.Value())) {
            column = args[0];
          }
        }
      }
    }
  }
  // A/B hatch: TINYLAMB_DISABLE_COLCOL_COMPARE=1 keeps column-vs-column
  // predicates in the residual (bytecode/AST) path.
  static const bool colcol_disabled =
      std::getenv("TINYLAMB_DISABLE_COLCOL_COMPARE") != nullptr;
  if (!is_like && !colcol_disabled && column->Type() == TypeTag::kColumnValue &&
      constant->Type() == TypeTag::kColumnValue) {
    // Like-kind column pairs only: the positional match below cannot raise
    // for these, while anything exotic (arrays, intervals, mixed numeric
    // classes with distinct coercion rules) stays in the residual where the
    // full evaluator reproduces its exact semantics.
    const auto left_offset =
        LocalColumnOffset(schema, column->AsColumnValue().GetColumnName());
    const auto right_offset =
        LocalColumnOffset(schema, constant->AsColumnValue().GetColumnName());
    if (!left_offset || !right_offset) {
      return std::nullopt;
    }
    const ValueType left_type =
        schema.GetColumn(static_cast<size_t>(*left_offset)).Type();
    const ValueType right_type =
        schema.GetColumn(static_cast<size_t>(*right_offset)).Type();
    const auto int_like = [](ValueType type) {
      return type == ValueType::kInt64 || type == ValueType::kDate;
    };
    const bool supported =
        (int_like(left_type) && left_type == right_type) ||
        (left_type == ValueType::kDouble && right_type == ValueType::kDouble) ||
        (left_type == ValueType::kVarChar && right_type == ValueType::kVarChar);
    if (!supported) {
      return std::nullopt;
    }
    SimpleComparePredicate compiled;
    compiled.column = static_cast<slot_t>(*left_offset);
    compiled.op = op;
    compiled.column_payload = true;
    compiled.right_column = static_cast<slot_t>(*right_offset);
    return compiled;
  }
  if (column->Type() != TypeTag::kColumnValue ||
      constant->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const auto offset =
      LocalColumnOffset(schema, column->AsColumnValue().GetColumnName());
  if (!offset) {
    return std::nullopt;
  }
  SimpleComparePredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.op = op;
  compiled.constant = constant->AsConstantValue().GetValue();
  const Column& target_col = schema.GetColumn(static_cast<size_t>(*offset));
  // DATE has integer storage, but DATE-vs-number is a type error, not
  // a comparison of day counts. Leave it to the checked evaluator.
  if ((target_col.Type() == ValueType::kDate &&
       (compiled.constant.type == ValueType::kInt64 ||
        compiled.constant.type == ValueType::kDouble)) ||
      (compiled.constant.type == ValueType::kDate &&
       (target_col.Type() == ValueType::kInt64 ||
        target_col.Type() == ValueType::kDouble))) {
    return std::nullopt;
  }
  if (target_col.Type() == ValueType::kDate &&
      compiled.constant.type == ValueType::kVarChar) {
    try {
      compiled.int_constant =
          ParseDateDays(compiled.constant.value.varchar_value);
      compiled.date_string_payload = true;
    } catch (const std::exception&) {
      // An invalid date literal is a query error in the checked evaluator;
      // a fast-path compare would silently reject every row instead.
      return std::nullopt;
    }
  }
  // Numeric-vs-string and string-vs-numeric comparisons raise "type
  // mismatch" in the checked evaluator. The payload matchers below would
  // fall through to "no match" and turn that error into an empty result.
  {
    const ValueType col_type = target_col.Type();
    const ValueType const_type = compiled.constant.type;
    const bool numeric_col =
        col_type == ValueType::kInt64 || col_type == ValueType::kDouble;
    const bool numeric_const =
        const_type == ValueType::kInt64 || const_type == ValueType::kDouble;
    if ((numeric_col && const_type == ValueType::kVarChar) ||
        (col_type == ValueType::kVarChar &&
         (numeric_const || const_type == ValueType::kDate))) {
      return std::nullopt;
    }
  }
  if (is_like) {
    if (target_col.Type() != ValueType::kVarChar ||
        compiled.constant.type != ValueType::kVarChar ||
        compiled.constant.IsCaseInsensitive()) {
      return std::nullopt;
    }
    return compiled;
  }
  if ((compiled.constant.type == ValueType::kInt64 ||
       compiled.constant.type == ValueType::kDate) &&
      target_col.Type() == ValueType::kDouble && !target_col.IsUnsigned() &&
      compiled.constant.type == ValueType::kInt64 &&
      !compiled.constant.IsUnsigned() &&
      compiled.constant.value.int_value >= -(int64_t{1} << 53) &&
      compiled.constant.value.int_value <= (int64_t{1} << 53)) {
    compiled.double_payload = true;
    compiled.double_constant =
        static_cast<double>(compiled.constant.value.int_value);
  } else if (compiled.constant.type == ValueType::kInt64 ||
             compiled.constant.type == ValueType::kDate) {
    compiled.int_payload = true;
    compiled.int_constant = compiled.constant.value.int_value;
  } else if (compiled.constant.type == ValueType::kDouble) {
    const double d = compiled.constant.value.double_value;
    bool translated_to_int = false;
    if (target_col.Type() == ValueType::kInt64 && !target_col.IsUnsigned() &&
        std::isfinite(d) && d >= -static_cast<double>(int64_t{1} << 52) &&
        d <= static_cast<double>(int64_t{1} << 52)) {
      const auto fl = static_cast<int64_t>(std::floor(d));
      const auto cl = static_cast<int64_t>(std::ceil(d));
      if (fl == cl) {
        compiled.int_payload = true;
        compiled.int_constant = fl;
        compiled.constant = Value(fl);
        translated_to_int = true;
      } else if (op == BinaryOperation::kGreaterThan ||
                 op == BinaryOperation::kGreaterThanEquals) {
        compiled.op = BinaryOperation::kGreaterThan;
        compiled.int_payload = true;
        compiled.int_constant = fl;
        compiled.constant = Value(fl);
        translated_to_int = true;
      } else if (op == BinaryOperation::kLessThan ||
                 op == BinaryOperation::kLessThanEquals) {
        compiled.op = BinaryOperation::kLessThanEquals;
        compiled.int_payload = true;
        compiled.int_constant = fl;
        compiled.constant = Value(fl);
        translated_to_int = true;
      }
    }
    if (!translated_to_int) {
      compiled.double_payload = true;
      compiled.double_constant = d;
    }
  } else if (compiled.constant.type == ValueType::kVarChar &&
             !compiled.constant.IsCaseInsensitive() &&
             PlainStringComparable(compiled.constant.value.varchar_value)) {
    compiled.plain_string_payload = true;
  }
  return compiled;
}

// Restrict early evaluation to bare, like-typed comparisons. Arbitrary
// expressions can throw or be volatile even for rows a join would reject.
bool IsNonThrowingScanComparison(const Expression& predicate,
                                 const Schema& schema) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  const auto operand_type =
      [&](const Expression& operand) -> std::optional<ValueType> {
    if (!operand) {
      return std::nullopt;
    }
    if (operand->Type() == TypeTag::kConstantValue) {
      return operand->AsConstantValue().GetValue().type;
    }
    if (operand->Type() == TypeTag::kColumnValue) {
      const auto offset =
          LocalColumnOffset(schema, operand->AsColumnValue().GetColumnName());
      if (offset) {
        return schema.GetColumn(*offset).Type();
      }
      return std::nullopt;
    }
    if (!ContainsVolatileCall(operand) && !ContainsParameterSlot(operand) &&
        !ContainsQuery(operand) && !ContainsAggregate(operand) &&
        operand->TouchedColumns().empty()) {
      StatusOr<Value> val = operand->TryEvaluate(Row(), Schema());
      if (val.HasValue()) {
        return val.Value().type;
      }
    }
    return std::nullopt;
  };
  const auto left = operand_type(binary.Left());
  const auto right = operand_type(binary.Right());
  const bool numeric_cross =
      left && right &&
      ((*left == ValueType::kInt64 && *right == ValueType::kDouble) ||
       (*left == ValueType::kDouble && *right == ValueType::kInt64));
  if (!left || !right ||
      (*left != *right && *left != ValueType::kNull &&
       *right != ValueType::kNull && !numeric_cross)) {
    return false;
  }
  const auto scalar = [](ValueType type) {
    return type == ValueType::kNull || type == ValueType::kInt64 ||
           type == ValueType::kDate || type == ValueType::kDouble ||
           type == ValueType::kVarChar;
  };
  return scalar(*left) && scalar(*right) &&
         TryCompileSimpleCompare(predicate, schema).has_value();
}

// Compile a single AND expression into a DisjunctiveBranch.
// Splits conjuncts and tries to compile each as SimpleCompare.
namespace {

// Ceiling for the AND-of-OR cross product; beyond this the OR conjunct is
// evaluated as an ordinary residual instead of multiplying branch count.
constexpr size_t kMaxDisjunctiveBranches = 32;

}  // namespace

bool MatchPrefixIn(const Row& row, const PrefixInPredicate& pred) {
  if (pred.column >= row.Count()) {
    return false;
  }
  const Value& cell = row[pred.column];
  if (cell.type != ValueType::kVarChar || cell.IsNull()) {
    return false;
  }
  const std::string_view text = cell.value.varchar_value;
  return std::ranges::any_of(pred.prefixes, [text](const std::string& prefix) {
    return text.starts_with(prefix);
  });
}

std::optional<PrefixInPredicate> TryCompilePrefixIn(const Expression& predicate,
                                                    const Schema& schema) {
  static const bool prefix_in_disabled =
      std::getenv("TINYLAMB_DISABLE_PREFIX_IN") != nullptr;
  if (prefix_in_disabled || !predicate ||
      predicate->Type() != TypeTag::kInExp) {
    return std::nullopt;
  }
  const InExpression& in = predicate->AsInExpression();
  if (in.list_.empty() || !in.child_ ||
      in.child_->Type() != TypeTag::kFunctionCallExp) {
    return std::nullopt;
  }
  const FunctionCallExpression& call = in.child_->AsFunctionCallExpression();
  if (call.FuncName() != "substr" && call.FuncName() != "substring") {
    return std::nullopt;
  }
  const std::vector<Expression>& args = call.Args();
  if (args.size() != 3 || !args[0] || !args[1] || !args[2] ||
      args[0]->Type() != TypeTag::kColumnValue ||
      args[1]->Type() != TypeTag::kConstantValue ||
      args[2]->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const Value& start = args[1]->AsConstantValue().GetValue();
  const Value& length = args[2]->AsConstantValue().GetValue();
  // substr semantics: 1-based start, non-negative length.  Anything else
  // (negative start, non-int positional args) stays on the AST path.
  if (start.type != ValueType::kInt64 || start.value.int_value != 1 ||
      length.type != ValueType::kInt64 || length.value.int_value <= 0) {
    return std::nullopt;
  }
  const auto offset =
      LocalColumnOffset(schema, args[0]->AsColumnValue().GetColumnName());
  if (!offset || schema.GetColumn(static_cast<size_t>(*offset)).Type() !=
                     ValueType::kVarChar) {
    return std::nullopt;
  }
  const auto k = static_cast<size_t>(length.value.int_value);
  PrefixInPredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.prefixes.reserve(in.list_.size());
  for (const Expression& element : in.list_) {
    if (!element || element->Type() != TypeTag::kConstantValue) {
      return std::nullopt;
    }
    const Value& value = element->AsConstantValue().GetValue();
    if (value.type != ValueType::kVarChar || value.IsNull() ||
        value.value.varchar_value.size() != k) {
      return std::nullopt;
    }
    const std::string_view prefix = value.value.varchar_value;
    for (const char byte : prefix) {
      if ((static_cast<unsigned char>(byte) & 0x80) != 0) {
        // A multi-byte code point in the prefix would make the byte-prefix
        // compare diverge from the code-point substr for strings whose
        // later code points are multi-byte; keep those on the AST path.
        return std::nullopt;
      }
    }
    compiled.prefixes.emplace_back(prefix);
  }
  return compiled;
}

namespace {

Expression ConjoinAll(const std::vector<Expression>& predicates);

// Compiles `col [NOT] IN (uncorrelated subquery)` into one immutable value
// set built by the runtime's own uncorrelated-execution cache.  Soundness
// mirrors the per-row membership build in the expression evaluator: the
// statement executes once against this statement's snapshot, so the member
// set is row-invariant.  The peel is restricted to probe columns whose type
// tag and unsigned tag statically equal a uniform INT64/DATE build domain,
// which makes the int-value set verdict exact (the same trust the runtime
// hash grants a same-domain probe) and lets the matcher answer every input
// without the contextful evaluator.  Anything else keeps the residual path.
std::optional<PrebuiltMembershipPredicate> TryCompilePrebuiltMembership(
    const Expression& predicate, const Schema& schema,
    TransactionContext& context, const CteMap& ctes) {
  static const bool membership_peel_disabled =
      std::getenv("TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP") != nullptr;
  static const bool membership_peel_debug =
      std::getenv("TINYLAMB_PREBUILT_MEMBERSHIP_DEBUG") != nullptr;
  const auto mbail = [&](const char* why) {
    if (membership_peel_debug) {
      std::cerr << "[prebuilt-mem] not peeled: " << why << "\n";
    }
    return std::nullopt;
  };
  if (membership_peel_disabled || !predicate ||
      predicate->Type() != TypeTag::kQueryExp) {
    return mbail("head");
  }
  const QueryExpression& query = predicate->AsQueryExpression();
  // EXISTS/ARRAY/scalar probes are row-independent and fold to constants
  // earlier; only a quantified membership probe with a row-side test lands
  // here.
  if (query.Exists() || query.ArrayResult() || !query.Test() ||
      query.Mode() != QuantifierMode::kIn) {
    return mbail("mode");
  }
  // Plain column probe only: an arbitrary test expression keeps the AST path
  // (its evaluation semantics are richer than a value lookup).
  if (query.Test()->Type() != TypeTag::kColumnValue) {
    return mbail("test");
  }
  const auto offset =
      LocalColumnOffset(schema, query.Test()->AsColumnValue().GetColumnName());
  if (!offset) {
    return mbail("col");
  }
  const Column& probe_column = schema.GetColumn(static_cast<size_t>(*offset));
  const ValueType probe_type = probe_column.Type();
  if (probe_type != ValueType::kInt64 && probe_type != ValueType::kDate &&
      probe_type != ValueType::kVarChar) {
    return mbail("type");
  }
  const SelectStatement& statement = *query.Query();
  const bool grouped_subquery =
      !statement.GroupBy().empty() || statement.Having() != nullptr;
  const bool allow_grouped_subquery =
      context.execution_runtime() != nullptr &&
      context.execution_runtime()->root_statement != nullptr &&
      context.execution_runtime()->root_statement->Sources().size() > 1;
  // SELECT AS STRUCT members are composite; LIMIT/OFFSET change the member
  // set; GROUP BY / HAVING produce aggregate rows.  None of those shapes
  // reduce to a plain value set.
  if (statement.AsStruct() || statement.HasLimit() || statement.Offset() != 0 ||
      (grouped_subquery && !allow_grouped_subquery) ||
      statement.SelectList().size() != 1) {
    return mbail("sub shape");
  }
  ExecutionRuntime* runtime = context.execution_runtime();
  if (runtime == nullptr ||
      !ContainsOnlyUncorrelatedQueries(context, predicate, ctes)) {
    return mbail("correlated");
  }
  if (probe_type == ValueType::kVarChar) {
    runtime->retained_statements.push_back(query.Query());
    const Relation* members =
        ExecuteCachedUncorrelated(context, statement, ctes, false);
    if ((members == nullptr) || members->schema.ColumnCount() != 1) {
      return mbail("string exec");
    }
    auto strings = std::make_shared<MembershipStringSet>();
    bool compatible = true;
    bool contains_null = false;
    const Status status = members->ForEachRow([&](const Row& row) {
      const Value& value = row[0];
      if (value.IsNull()) {
        contains_null = true;
        return;
      }
      if (value.type != ValueType::kVarChar || value.Collation() != 0 ||
          !PlainStringComparable(value.AsStringView())) {
        compatible = false;
        return;
      }
      strings->emplace(value.AsStringView());
    });
    if (!compatible || status != Status::kSuccess) {
      return mbail("string domain");
    }
    PrebuiltMembershipPredicate compiled;
    compiled.column = static_cast<slot_t>(*offset);
    compiled.negated = query.Negated();
    compiled.domain_type = probe_type;
    compiled.contains_null = contains_null;
    compiled.string_values = std::move(strings);
    return compiled;
  }
  auto values = std::make_shared<FlatIntSet>();
  bool contains_null = false;
  bool bail = false;
  bool has_domain = false;
  ValueType domain_type = ValueType::kNull;
  bool domain_unsigned = false;
  bool direct_filled = false;
  // Direct-scan fill: a trivial single-column scan of one base table reads
  // its column straight into the set through the full-scan iterator -- Q22's
  // 1.5M-row orders membership build otherwise executes the whole relational
  // pipeline (Row boxing per member) inside the filter-compile phase.  The
  // FlatIntSet cannot grow, so the reserve upper-bounds the distinct keys:
  // the table's ANALYZE row count with a running safety check that falls
  // back to the generic execution below the moment the estimate looks stale.
  static const bool direct_fill_disabled =
      std::getenv("TINYLAMB_DISABLE_ARENA_MEMBERSHIP") != nullptr;
  if (!direct_fill_disabled && !grouped_subquery &&
      statement.Sources().size() == 1 && statement.SelectList().size() == 1 &&
      statement.SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
      !statement.SelectList()[0]
           .expression->AsColumnValue()
           .GetColumnName()
           .Empty()) {
    const SelectSource& source = statement.Sources()[0];
    if (!source.query && !source.unnest && !source.sample &&
        !ctes.contains(source.table)) {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      StatusOr<std::shared_ptr<TableStatistics>> stats =
          context.GetStats(source.table);
      if (table.HasValue() && stats.HasValue() && stats.Value()->Rows() > 0) {
        const ColumnName& column_name = statement.SelectList()[0]
                                            .expression->AsColumnValue()
                                            .GetColumnName();
        const std::string qualifier =
            source.alias.empty() ? source.table : source.alias;
        const Schema table_schema =
            qualifier.empty()
                ? table.Value()->GetSchema()
                : QualifySchema(table.Value()->GetSchema(), qualifier);
        const auto slot = LocalColumnOffset(table_schema, column_name);
        const ValueType column_type =
            slot.has_value()
                ? table_schema.GetColumn(static_cast<size_t>(*slot)).Type()
                : ValueType::kNull;
        if (slot.has_value() && (column_type == ValueType::kInt64 ||
                                 column_type == ValueType::kDate)) {
          const size_t reserve_members = stats.Value()->Rows();
          const auto col_idx = static_cast<slot_t>(*slot);
          {
            std::vector<SimpleComparePredicate> local_simple;
            bool local_filter_ok = true;
            if (statement.WhereClause() != nullptr) {
              for (const Expression& conj :
                   SplitConjuncts(statement.WhereClause())) {
                auto sc = TryCompileSimpleCompare(conj, table_schema);
                if (!sc.has_value()) {
                  local_filter_ok = false;
                  break;
                }
                const Column& lcol = table_schema.GetColumn(sc->column);
                if ((lcol.Type() != ValueType::kInt64 &&
                     lcol.Type() != ValueType::kDate) ||
                    lcol.IsUnsigned()) {
                  local_filter_ok = false;
                  break;
                }
                if (sc->column_payload) {
                  const Column& rcol = table_schema.GetColumn(sc->right_column);
                  if (rcol.Type() != lcol.Type() || rcol.IsUnsigned()) {
                    local_filter_ok = false;
                    break;
                  }
                } else if (!sc->int_payload || sc->constant.IsNull() ||
                           sc->constant.IsUnsigned()) {
                  local_filter_ok = false;
                  break;
                }
                local_simple.push_back(std::move(*sc));
              }
            }
            std::vector<slot_t> needed{col_idx};
            for (const auto& compare : local_simple) {
              needed.push_back(compare.column);
              if (compare.column_payload) {
                needed.push_back(compare.right_column);
              }
            }
            std::shared_ptr<const LSTableStore> store =
                local_filter_ok
                    ? GetOrBuildLSTableStore(context.txn_, *table.Value(), true,
                                             &needed)
                    : nullptr;
            if (store != nullptr &&
                store->layout_generation == table.Value()->LayoutGeneration() &&
                store->first_pid == table.Value()->FirstPageId() &&
                col_idx < store->column_min_int.size()) {
              bool valid = true;
              if (store->uncommitted_owner_txn != 0) {
                valid = (store->uncommitted_owner_txn == context.txn_.ID());
              } else if (!context.txn_.IsReadOnly() &&
                         !context.txn_.WriteSetEmpty()) {
                valid = false;
              } else if (context.txn_.HasAnyPageVersionChains()) {
                for (const page_id_t pid : store->page_ids) {
                  if (context.txn_.HasPageVersionChains(pid)) {
                    valid = false;
                    break;
                  }
                }
              }
              if (valid) {
                if (!values->TryReserveDense(store->column_min_int[col_idx],
                                             store->column_max_int[col_idx])) {
                  values->Reserve(
                      std::max<size_t>(reserve_members, store->total_rows));
                }
                contains_null = false;
                const bool col_unsigned =
                    table_schema.GetColumn(col_idx).IsUnsigned();
                const auto eval_local_simple =
                    [&](const LSTablePageBlock& blk,
                        uint32_t r) noexcept -> bool {
                  return std::ranges::all_of(
                      local_simple, [&](const SimpleComparePredicate& sc) {
                        const LSTableColumnBlock& lc = blk.columns[sc.column];
                        if (lc.IsNull(r)) {
                          return false;
                        }
                        if (sc.column_payload) {
                          const LSTableColumnBlock& rc =
                              blk.columns[sc.right_column];
                          if (rc.IsNull(r)) {
                            return false;
                          }
                          return ComparePositional(sc.op, lc.int_cells[r],
                                                   rc.int_cells[r]);
                        }
                        return ComparePositional(sc.op, lc.int_cells[r],
                                                 sc.int_constant);
                      });
                };
                if (values->IsDenseMode() && store->total_rows >= 32768 &&
                    store->blocks.size() >= 2) {
                  const auto nworkers = std::min<size_t>(
                      {store->blocks.size(),
                       QueryParallelWorkerLimit(
                           std::thread::hardware_concurrency()),
                       size_t{16}});
                  if (nworkers >= 2) {
                    std::vector<char> worker_saw_null(nworkers, 0);
                    {
                      std::vector<std::jthread> threads;
                      threads.reserve(nworkers);
                      for (size_t w = 0; w < nworkers; ++w) {
                        threads.emplace_back([&, w] {
                          const size_t b_begin =
                              (store->blocks.size() * w) / nworkers;
                          const size_t b_end =
                              (store->blocks.size() * (w + 1)) / nworkers;
                          bool have_last = false;
                          int64_t last_key = 0;
                          for (size_t bi = b_begin; bi < b_end; ++bi) {
                            const LSTablePageBlock& blk = store->blocks[bi];
                            if (blk.row_count == 0) {
                              continue;
                            }
                            const LSTableColumnBlock& col =
                                blk.columns[col_idx];
                            const int64_t* __restrict cells =
                                col.int_cells.data();
                            if (!col.has_null && local_simple.empty()) {
                              for (uint32_t r = 0; r < blk.row_count; ++r) {
                                const int64_t k = cells[r];
                                if (have_last && k == last_key) {
                                  continue;
                                }
                                last_key = k;
                                have_last = true;
                                values->InsertDenseAtomic(k);
                              }
                            } else {
                              for (uint32_t r = 0; r < blk.row_count; ++r) {
                                const int64_t k = cells[r];
                                if (!col.has_null && have_last &&
                                    k == last_key) {
                                  continue;
                                }
                                if (!eval_local_simple(blk, r)) {
                                  continue;
                                }
                                if (col.IsNull(r)) {
                                  worker_saw_null[w] = 1;
                                } else {
                                  last_key = k;
                                  have_last = true;
                                  values->InsertDenseAtomic(k);
                                }
                              }
                            }
                          }
                        });
                      }
                    }
                    values->FinalizeDenseBounds();
                    for (const char flag : worker_saw_null) {
                      contains_null = contains_null || (flag != 0);
                    }
                    if (!values->Empty()) {
                      has_domain = true;
                      domain_type = column_type;
                      domain_unsigned = col_unsigned;
                    }
                    direct_filled = true;
                  }
                }
                if (!direct_filled) {
                  for (const LSTablePageBlock& blk : store->blocks) {
                    if (blk.row_count == 0) {
                      continue;
                    }
                    const LSTableColumnBlock& col = blk.columns[col_idx];
                    const int64_t* __restrict cells = col.int_cells.data();
                    if (!col.has_null && local_simple.empty()) {
                      if (!has_domain) {
                        has_domain = true;
                        domain_type = column_type;
                        domain_unsigned = col_unsigned;
                      }
                      for (uint32_t r = 0; r < blk.row_count; ++r) {
                        values->Insert(cells[r]);
                      }
                    } else {
                      for (uint32_t r = 0; r < blk.row_count; ++r) {
                        if (!eval_local_simple(blk, r)) {
                          continue;
                        }
                        if (col.IsNull(r)) {
                          contains_null = true;
                        } else {
                          if (!has_domain) {
                            has_domain = true;
                            domain_type = column_type;
                            domain_unsigned = col_unsigned;
                          }
                          values->Insert(cells[r]);
                        }
                      }
                    }
                  }
                  direct_filled = true;
                }
              }
            }
          }
          if (!direct_filled && statement.WhereClause() == nullptr) {
            values->Reserve(reserve_members);
            const std::vector<slot_t> projection{col_idx};
            bool overflow = false;
            static const bool parallel_membership_disabled =
                std::getenv("TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP") != nullptr;
            // Morsel-parallel fill: the serial walk boxes one Row per member
            // on one thread (Q22: 1.5M orders rows ≈ 90 ms of the filter
            // phase).  Workers collect raw int payloads per morsel chunk; the
            // merge reuses the serial path's distinct-bound guard so the
            // stale-stats fallback decision stays identical.
            std::vector<Table::ScanMorsel> morsels;
            size_t worker_count = 0;
            if (!parallel_membership_disabled && reserve_members >= 65536) {
              morsels = table.Value()->BuildScanMorsels(context.txn_, 8);
              if (morsels.size() >= 4) {
                worker_count =
                    std::min<size_t>({morsels.size(),
                                      QueryParallelWorkerLimit(
                                          std::thread::hardware_concurrency()),
                                      size_t{16}});
              }
            }
            if (worker_count >= 2) {
              std::vector<std::vector<int64_t>> cells(worker_count);
              std::vector<char> saw_null(worker_count, 0);
              std::vector<Status> scan_status(worker_count, Status::kSuccess);
              {
                std::vector<std::jthread> threads;
                threads.reserve(worker_count);
                for (size_t w = 0; w < worker_count; ++w) {
                  threads.emplace_back([&, w] {
                    size_t mi = w;
                    while (mi < morsels.size()) {
                      Iterator iterator = table.Value()->BeginMorselScan(
                          context.txn_, morsels[mi], projection);
                      while (iterator.IsValid()) {
                        const Row& row = *iterator;
                        if (row.values_.size() == 1) {
                          const Value& member = row.values_[0];
                          if (member.IsNull()) {
                            saw_null[w] = 1;
                          } else {
                            cells[w].push_back(member.value.int_value);
                          }
                        }
                        ++iterator;
                      }
                      if (iterator.GetStatus() != Status::kSuccess) {
                        scan_status[w] = iterator.GetStatus();
                        return;
                      }
                      mi += worker_count;
                    }
                  });
                }
              }
              if (std::ranges::any_of(scan_status, [](const Status& status) {
                    return status != Status::kSuccess;
                  })) {
                overflow = true;
              }
              size_t total = 0;
              for (const std::vector<int64_t>& chunk : cells) {
                total += chunk.size();
              }
              if (overflow || total > reserve_members) {
                // More members than the distinct bound the reserve absorbed:
                // the stale-stats case, identical verdict to the serial guard.
                overflow = true;
              } else {
                for (const std::vector<int64_t>& chunk : cells) {
                  for (const int64_t member : chunk) {
                    if (values->Size() >= reserve_members) {
                      overflow = true;
                      break;
                    }
                    values->Insert(member);
                  }
                  if (overflow) {
                    break;
                  }
                }
                if (!overflow) {
                  contains_null = false;
                  for (const char flag : saw_null) {
                    contains_null = contains_null || flag != 0;
                  }
                  if (total > 0) {
                    has_domain = true;
                    domain_type = column_type;
                    domain_unsigned =
                        table_schema.GetColumn(*slot).IsUnsigned();
                  }
                }
              }
            } else {
              Iterator iterator =
                  table.Value()->BeginFullScan(context.txn_, projection);
              while (iterator.IsValid()) {
                const Row& row = *iterator;
                if (row.values_.size() == 1) {
                  const Value& member = row.values_[0];
                  if (member.IsNull()) {
                    contains_null = true;
                  } else {
                    if (!has_domain) {
                      has_domain = true;
                      domain_type = column_type;
                      domain_unsigned =
                          table_schema.GetColumn(*slot).IsUnsigned();
                    }
                    if (values->Size() >= reserve_members) {
                      // Distinct keys reached the reserve bound: the
                      // stale-stats case the reserve cannot absorb.  Rebuild
                      // generically.
                      overflow = true;
                      break;
                    }
                    values->Insert(member.value.int_value);
                  }
                }
                ++iterator;
              }
              overflow = overflow || iterator.GetStatus() != Status::kSuccess;
            }
            if (!overflow) {
              direct_filled = true;
            } else {
              contains_null = false;
              bail = false;
              has_domain = false;
              domain_type = ValueType::kNull;
              domain_unsigned = false;
              values = std::make_shared<FlatIntSet>();
            }
          }
        }
      }
    }
  }
  const Relation* members = nullptr;
  if (!direct_filled) {
    auto& retained = runtime->retained_statements;
    bool already_retained = false;
    for (size_t k = retained.size() > 8 ? retained.size() - 8 : 0;
         k < retained.size(); ++k) {
      if (retained[k] == query.Query()) {
        already_retained = true;
        break;
      }
    }
    if (!already_retained) {
      retained.push_back(query.Query());
    }
    members = ExecuteCachedUncorrelated(context, statement, ctes, false);
    if (members == nullptr || members->schema.ColumnCount() != 1) {
      return mbail("exec");
    }
    bool reserved_dense = false;
    if (!members->HasSpill() && !members->rows.empty()) {
      int64_t min_k = std::numeric_limits<int64_t>::max();
      int64_t max_k = std::numeric_limits<int64_t>::min();
      bool any_k = false;
      for (const Row& row : members->rows) {
        if (!row.values_.empty() && !row.values_[0].IsNull() &&
            (row.values_[0].type == ValueType::kInt64 ||
             row.values_[0].type == ValueType::kDate)) {
          const int64_t v = row.values_[0].value.int_value;
          min_k = std::min(min_k, v);
          max_k = std::max(max_k, v);
          any_k = true;
        }
      }
      if (any_k && values->TryReserveDense(min_k, max_k)) {
        reserved_dense = true;
      }
    }
    if (!reserved_dense) {
      values->Reserve(members->TotalRows());
    }
  }
  const Status iteration =
      direct_filled ? Status::kSuccess
                    : members->ForEachRow([&](const Row& row) {
                        if (bail) {
                          return;
                        }
                        const Value member = ProjectSubqueryRow(row, false);
                        if (member.IsNull()) {
                          contains_null = true;
                          return;
                        }
                        if (!has_domain) {
                          has_domain = true;
                          domain_type = member.type;
                          domain_unsigned = member.IsUnsigned();
                        } else if (member.type != domain_type ||
                                   member.IsUnsigned() != domain_unsigned) {
                          // Mixed numeric domains promote under `=`; the
                          // type-strict set would wrongly decide those probes.
                          // Keep the AST path.
                          bail = true;
                          return;
                        }
                        if (member.Collation() != 0) {
                          // A collated domain compares under folding rules the
                          // set cannot reproduce; keep the AST path.
                          bail = true;
                          return;
                        }
                        values->Insert(member.value.int_value);
                      });
  if (bail || iteration != Status::kSuccess || values->DenseOverflowed()) {
    return mbail("build");
  }
  if (has_domain && (domain_type != probe_type ||
                     domain_unsigned != probe_column.IsUnsigned())) {
    return mbail("domain");
  }
  PrebuiltMembershipPredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.negated = query.Negated();
  compiled.domain_type = probe_type;
  compiled.values = std::move(values);
  compiled.contains_null = contains_null;
  if (membership_peel_debug) {
    std::cerr << "[prebuilt-mem] peeled: members=" << compiled.values->Size()
              << " contains_null=" << contains_null << "\n";
  }
  return compiled;
}

// A selective equality-prefix index can supply just the matching heap rows.
// Recheck every local predicate against the MVCC-visible image: a prefix seek
// is an enclosure, not a proof of the WHERE clause. The read gate and stale-key
// test are the same contract as the physical index executors.
std::shared_ptr<const FlatIntSet> TryBuildIndexedExistsKeys(
    Table& table, const Schema& local_schema, slot_t key_column,
    const std::vector<Expression>& predicates,
    const TableStatistics& statistics, double build_rows,
    const FilterCompileEnv& env) {
  if (table.IndexCount() == 0 || !(build_rows >= 0) ||
      build_rows > static_cast<double>(env.input_rows) * 2.0) {
    return nullptr;
  }
  std::vector<SimpleComparePredicate> comparisons;
  for (const auto& predicate : predicates) {
    auto comparison = TryCompileSimpleCompare(predicate, local_schema);
    if (!comparison ||
        local_schema.GetColumn(comparison->column).IsUnsigned() ||
        (comparison->column_payload &&
         local_schema.GetColumn(comparison->right_column).IsUnsigned())) {
      return nullptr;
    }
    comparisons.push_back(std::move(*comparison));
  }
  const Index* selected = nullptr;
  const Value* equality = nullptr;
  for (size_t i = 0; i < table.IndexCount() && selected == nullptr; ++i) {
    const Index& index = table.GetIndex(i);
    if (index.sc_.key_.empty()) {
      continue;
    }
    for (size_t p = 0; p < comparisons.size(); ++p) {
      const auto& comparison = comparisons[p];
      const auto type = local_schema.GetColumn(comparison.column).Type();
      if (!comparison.column_payload &&
          comparison.op == BinaryOperation::kEquals &&
          comparison.column == index.sc_.key_.front() &&
          (type == ValueType::kInt64 || type == ValueType::kDate) &&
          comparison.constant.type == type &&
          !comparison.constant.IsUnsigned()) {
        // Cost the index prefix itself, not the more selective conjunction:
        // a different residual must not disguise an almost-full heap walk.
        const double prefix_rows =
            static_cast<double>(statistics.Rows()) *
            statistics.EstimateSelectivity(local_schema, predicates[p]);
        if (prefix_rows < 0 ||
            prefix_rows > static_cast<double>(env.input_rows) * 2.0 ||
            prefix_rows > static_cast<double>(statistics.Rows()) * 0.1) {
          continue;
        }
        selected = &index;
        equality = &comparison.constant;
        break;
      }
    }
  }
  if (selected == nullptr ||
      env.context->txn_.IndexKeysMayBeStale(selected->Root())) {
    return nullptr;
  }
  // Estimates choose capacity only. Actual distinct-key overflow abandons the
  // optimization, rather than truncating the answer or filling a full table.
  constexpr size_t kMaxExistsKeys = 65536;
  // ANALYZE's NDV is approximate. A small underestimate must not abandon a
  // 100-key build after almost finishing it, then pay for a full table scan.
  // Keep slack within the same charged bound; real overflow still falls back.
  const size_t reserve_keys = static_cast<size_t>(std::clamp(
      std::ceil(build_rows * 2.0), 128.0, static_cast<double>(kMaxExistsKeys)));
  auto charge =
      TryReserveQueryMemory(FlatIntSet::ReservationBytes(reserve_keys));
  if (charge == nullptr) {
    return nullptr;
  }
  struct ChargedSet {
    std::shared_ptr<QueryMemoryCharge> charge;
    FlatIntSet values;
  };
  auto owner = std::make_shared<ChargedSet>();
  owner->charge = std::move(charge);
  owner->values.Reserve(reserve_keys);
  auto& values = owner->values;
  auto guard = env.context->txn_.AcquireIndexReadGuard(selected->Root());
  if (env.context->txn_.IndexKeysMayBeStale(selected->Root())) {
    return nullptr;
  }
  size_t visited = 0;
  {
    IndexScanIterator iterator(
        table, *selected, env.context->txn_, std::vector<Value>{*equality},
        std::vector<Value>{*equality}, true, false, &guard);
    while (iterator.IsValid()) {
      ++visited;
      const Row& row = *iterator;
      if (std::ranges::all_of(comparisons, [&](const auto& comparison) {
            return MatchSimpleCompare(row, comparison);
          })) {
        const Value& key = row[key_column];
        if (!key.IsNull()) {
          if (values.Size() >= reserve_keys &&
              !values.Contains(key.value.int_value)) {
            return nullptr;
          }
          values.Insert(key.value.int_value);
        }
      }
      ++iterator;
    }
    if (!iterator.GetStatus().ok()) {
      return nullptr;
    }
  }
  if (auto* runtime = env.context->execution_runtime()) {
    ++runtime->prebuilt_exists_index_builds;
    runtime->prebuilt_exists_index_rows += visited;
    runtime->prebuilt_exists_keys += values.Size();
    runtime->scan_rows += visited;
    runtime->scan_values_available += visited * local_schema.ColumnCount();
    runtime->scan_values_decoded += visited * local_schema.ColumnCount();
  }
  return {owner, &owner->values};
}

// A selective, single-key EXISTS can build one row-independent membership
// set instead of re-entering the correlated runtime for every outer row.
// Keep the domain-pruned correlated build for large/nonselective inner inputs.
std::optional<PrebuiltMembershipPredicate> TryCompilePrebuiltExists(
    const Expression& predicate, const Schema& schema,
    const FilterCompileEnv& env) {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP") != nullptr;
  if (disabled || !predicate || predicate->Type() != TypeTag::kQueryExp ||
      env.input_rows == 0) {
    return std::nullopt;
  }
  const auto& query = predicate->AsQueryExpression();
  if (!query.Exists() || query.Test() || query.ArrayResult()) {
    return std::nullopt;
  }
  const SelectStatement& sub = *query.Query();
  if (sub.Sources().size() != 1 || !sub.WithQueries().empty() ||
      !sub.UnionAll().empty() || sub.GetSetOperationTree() || sub.Qualify() ||
      sub.HasLimit() || sub.Offset() != 0 || !sub.GroupBy().empty() ||
      sub.Having() || sub.Distinct() || !sub.OrderBy().empty() ||
      sub.AsStruct() || sub.ForUpdate() || sub.HasDistinctOn()) {
    return std::nullopt;
  }
  const auto& source = sub.Sources()[0];
  if (source.query || source.unnest || source.sample || source.join_condition ||
      env.ctes->contains(source.table)) {
    return std::nullopt;
  }
  auto table = env.context->GetTable(source.table);
  auto stats = env.context->GetStats(source.table);
  if (!table.HasValue() || !stats.HasValue() || stats.Value()->Rows() == 0) {
    return std::nullopt;
  }
  const Schema local_schema =
      QualifySchema(table.Value()->GetSchema(),
                    source.alias.empty() ? source.table : source.alias);
  for (const auto& item : sub.SelectList()) {
    if (!item.expression ||
        (item.expression->Type() != TypeTag::kConstantValue &&
         (item.expression->Type() != TypeTag::kColumnValue ||
          (item.expression->AsColumnValue().GetColumnName().name != "*" &&
           !LocalColumnOffset(
               local_schema,
               item.expression->AsColumnValue().GetColumnName()))))) {
      return std::nullopt;
    }
  }
  Expression local_key;
  ColumnName outer_key;
  std::vector<Expression> rest;
  for (const auto& conjunct : SplitConjuncts(sub.WhereClause())) {
    if (!conjunct) {
      return std::nullopt;
    }
    if (conjunct->Type() == TypeTag::kBinaryExp) {
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() == BinaryOperation::kEquals &&
          binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kColumnValue) {
        const auto& left = binary.Left()->AsColumnValue().GetColumnName();
        const auto& right = binary.Right()->AsColumnValue().GetColumnName();
        const bool left_local =
            LocalColumnOffset(local_schema, left).has_value();
        const bool right_local =
            LocalColumnOffset(local_schema, right).has_value();
        if (left_local != right_local) {
          if (local_key) {
            return std::nullopt;
          }
          local_key = left_local ? binary.Left() : binary.Right();
          outer_key = left_local ? right : left;
          continue;
        }
      }
    }
    if (!IsNonThrowingScanComparison(conjunct, local_schema)) {
      return std::nullopt;
    }
    rest.push_back(conjunct);
  }
  const auto probe = LocalColumnOffset(schema, outer_key);
  if (!local_key || !probe) {
    return std::nullopt;
  }
  const auto local = LocalColumnOffset(
      local_schema, local_key->AsColumnValue().GetColumnName());
  if (!local) {
    return std::nullopt;
  }
  const Column& inner_column =
      local_schema.GetColumn(static_cast<size_t>(*local));
  const Column& outer_column = schema.GetColumn(static_cast<size_t>(*probe));
  if ((inner_column.Type() != ValueType::kInt64 &&
       inner_column.Type() != ValueType::kDate) ||
      inner_column.Type() != outer_column.Type() ||
      inner_column.IsUnsigned() != outer_column.IsUnsigned()) {
    return std::nullopt;
  }
  const Expression local_filter = rest.empty() ? nullptr : ConjoinAll(rest);
  const double build_rows = static_cast<double>(stats.Value()->Rows()) *
                            (local_filter ? stats.Value()->EstimateSelectivity(
                                                local_schema, local_filter)
                                          : 1.0);
  if (!rest.empty()) {
    if (auto values = TryBuildIndexedExistsKeys(
            *table.Value(), local_schema, static_cast<slot_t>(*local), rest,
            *stats.Value(), build_rows, env)) {
      PrebuiltMembershipPredicate compiled;
      compiled.column = static_cast<slot_t>(*probe);
      compiled.negated = query.Negated();
      compiled.domain_type = outer_column.Type();
      compiled.values = std::move(values);
      compiled.existence = true;
      return compiled;
    }
  }
  auto rewritten = std::make_shared<SelectStatement>(sub);
  rewritten->SetWhereClause(local_filter);
  rewritten->SetSelectList({NamedExpression("", local_key)});
  Expression membership = QueryExpressionExp(
      std::move(rewritten), ColumnValueExp(outer_key), false, query.Negated());
  auto compiled =
      TryCompilePrebuiltMembership(membership, schema, *env.context, *env.ctes);
  if (compiled) {
    compiled->existence = true;
    compiled->contains_null = false;
  }
  return compiled;
}

// Compiles `col op (correlated scalar aggregate subquery)` into one
// key->value map by executing the subquery rewritten as GROUP BY
// correlation column (the pre-aggregated form of the per-key correlated
// apply).  Soundness: the rewrite runs once against this statement's
// snapshot and StatementUsesOnlyScopes verifies the rewritten statement
// references no outer scope, so every per-key value equals what a per-row
// apply would recompute; min/max/count/sum/avg are exactly the aggregates
// whose per-group value is independent of evaluation order.
std::optional<PrebuiltScalarComparePredicate> TryCompilePrebuiltScalarCompare(
    const Expression& predicate, const Schema& schema,
    TransactionContext& context, const CteMap& ctes) {
  // Opt-in: the pre-aggregated map build executes the rewritten GROUP BY
  // join at compile time, which only pays for itself when per-key applies
  // dominate (measured: Q2 -100 ms at SF1 with the build reused, +170 ms
  // when the grouped execution re-pays the join).  Off until the build can
  // share the statement's own join.
  static const bool scalar_peel_enabled =
      std::getenv("TINYLAMB_ENABLE_PREBUILT_SCALAR") != nullptr;
  static const bool scalar_peel_debug =
      std::getenv("TINYLAMB_PREBUILT_SCALAR_DEBUG") != nullptr;
  const auto bail = [&](const char* why) {
    if (scalar_peel_debug) {
      std::cerr << "[prebuilt-scalar] not peeled: " << why
                << " :: " << predicate->ToString().substr(0, 120) << "\n";
    }
    return std::nullopt;
  };
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return bail("head");
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  if (!binary.Left() || !binary.Right()) {
    return bail("null operand");
  }
  switch (binary.Op()) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      break;
    default:
      return bail("op");
  }
  const Expression* column_side = nullptr;
  const Expression* query_side = nullptr;
  if (binary.Left()->Type() == TypeTag::kColumnValue &&
      binary.Right()->Type() == TypeTag::kQueryExp) {
    column_side = &binary.Left();
    query_side = &binary.Right();
  } else if (binary.Left()->Type() == TypeTag::kQueryExp &&
             binary.Right()->Type() == TypeTag::kColumnValue) {
    column_side = &binary.Right();
    query_side = &binary.Left();
  } else {
    return bail("shape");
  }
  const QueryExpression& query = (*query_side)->AsQueryExpression();
  // Scalar probes only: EXISTS/quantified/ARRAY shapes have their own
  // semantics (and the uncorrelated ones already fold to constants).
  if (query.Exists() || query.ArrayResult() || query.Test()) {
    return bail("not scalar");
  }
  const SelectStatement& sub = *query.Query();
  // Materialized CTEs already paid for their join: grouping those rows once
  // avoids the allocation/name lookup of a correlated apply on every probe.
  const bool materialized_cte = sub.Sources().size() == 1 &&
                                sub.WithQueries().empty() &&
                                ctes.contains(sub.Sources()[0].table);
  if (!scalar_peel_enabled && !materialized_cte) {
    return bail("cost");
  }
  if (!sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr ||
      sub.Qualify() || HasWindowFunctions(sub)) {
    return bail("complex subquery");
  }
  // Anything beyond a plain aggregate-over-join block keeps the per-row path.
  if (sub.AsStruct() || sub.HasLimit() || sub.Offset() != 0 || sub.Distinct() ||
      sub.HasDistinctOn() || !sub.GroupBy().empty() || sub.Having() ||
      !sub.OrderBy().empty() || sub.Sources().empty() ||
      sub.SelectList().size() != 1) {
    return bail("sub shape");
  }
  const Expression& item = sub.SelectList()[0].expression;
  Expression aggregate_item = item;
  if (materialized_cte && item && item->Type() == TypeTag::kBinaryExp &&
      item->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
    const auto& product = item->AsBinaryExpression();
    Expression constant;
    if (product.Left()->Type() == TypeTag::kAggregateExp) {
      aggregate_item = product.Left();
      constant = product.Right();
    } else if (product.Right()->Type() == TypeTag::kAggregateExp) {
      aggregate_item = product.Right();
      constant = product.Left();
    }
    if (!constant || constant->Type() != TypeTag::kConstantValue) {
      return bail("aggregate product");
    }
    const Value& value = constant->AsConstantValue().GetValue();
    if (value.type != ValueType::kDouble ||
        !std::isfinite(value.value.double_value)) {
      return bail("aggregate multiplier");
    }
  }
  if (!aggregate_item || aggregate_item->Type() != TypeTag::kAggregateExp) {
    return bail("not aggregate");
  }
  const AggregateExpression& aggregate =
      aggregate_item->AsAggregateExpression();
  // Missing groups are NULL in this map. COUNT has a different empty value.
  if (aggregate.GetType() == AggregationType::kCount) {
    return bail("count empty semantics");
  }
  if (aggregate.Distinct() ||
      aggregate.Having() != AggregateHavingModifier::kNone) {
    return bail("distinct/having");
  }
  switch (aggregate.GetType()) {
    case AggregationType::kCount:
    case AggregationType::kSum:
    case AggregationType::kAvg:
    case AggregationType::kMin:
    case AggregationType::kMax:
      break;
    default:
      return bail("agg kind");
  }
  // The subquery's local schema: plain cross-joined base tables only (the
  // shapes the alias-qualified name resolution below can bind).
  Schema local_schema;
  for (const SelectSource& source : sub.Sources()) {
    if (source.query || source.unnest || source.from_nested_join ||
        source.join_type != JoinType::kCross) {
      return bail("source shape");
    }
    Schema table_schema;
    if (materialized_cte) {
      table_schema = ctes.at(source.table)->schema;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        return bail("table missing");
      }
      table_schema = table.Value()->GetSchema();
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty()) {
      table_schema = QualifySchema(table_schema, qualifier);
    }
    local_schema = local_schema + table_schema;
  }
  // Exactly one column = column correlation equality; every other conjunct
  // must be local on both sides (an unresolvable side is an outer reference
  // outside the correlation shape).
  std::optional<Expression> correlation;
  Expression correlation_inner;
  ColumnName correlation_outer;
  std::vector<Expression> rest;
  for (const Expression& conjunct : SplitConjuncts(sub.WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
        conjunct->AsBinaryExpression().Left()->Type() !=
            TypeTag::kColumnValue ||
        conjunct->AsBinaryExpression().Right()->Type() !=
            TypeTag::kColumnValue) {
      rest.push_back(conjunct);
      continue;
    }
    const ColumnName& left_name =
        conjunct->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
    const ColumnName& right_name =
        conjunct->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
    const auto left_local = LocalColumnOffset(local_schema, left_name);
    const auto right_local = LocalColumnOffset(local_schema, right_name);
    if (left_local.has_value() == right_local.has_value()) {
      if (!left_local.has_value()) {
        return bail("outer ref");  // Neither side local: an outer reference.
      }
      rest.push_back(conjunct);  // Pure inner predicate.
      continue;
    }
    if (correlation.has_value()) {
      return bail("second corr");  // Second correlation column: not an int key.
    }
    correlation = conjunct;
    if (left_local.has_value()) {
      correlation_inner = conjunct->AsBinaryExpression().Left();
      correlation_outer = right_name;
    } else {
      correlation_inner = conjunct->AsBinaryExpression().Right();
      correlation_outer = left_name;
    }
  }
  if (!correlation.has_value()) {
    return bail("no corr");
  }
  // The probe column must resolve in the scan schema with a type and
  // unsigned tag the int-key map can probe exactly.
  const auto probe_offset = LocalColumnOffset(schema, correlation_outer);
  if (!probe_offset) {
    return bail("probe col");
  }
  const Column& probe_column =
      schema.GetColumn(static_cast<size_t>(*probe_offset));
  const ValueType probe_type = probe_column.Type();
  if (probe_type != ValueType::kInt64 && probe_type != ValueType::kDate) {
    return bail("probe type");
  }
  const auto inner_offset = LocalColumnOffset(
      local_schema, correlation_inner->AsColumnValue().GetColumnName());
  if (!inner_offset ||
      local_schema.GetColumn(static_cast<size_t>(*inner_offset)).Type() !=
          probe_type ||
      local_schema.GetColumn(static_cast<size_t>(*inner_offset)).IsUnsigned() !=
          probe_column.IsUnsigned()) {
    return bail("inner type");
  }
  // Rewrite: same FROM, correlation conjunct dropped, GROUP BY the inner
  // column, SELECT [inner column, aggregate].  Shared expression nodes are
  // never mutated, so the member-wise statement copy is safe.
  auto rewritten = std::make_shared<SelectStatement>(sub);
  if (!rest.empty()) {
    rewritten->SetWhereClause(ConjoinAll(rest));
  } else {
    rewritten->SetWhereClause(Expression());
  }
  std::vector<NamedExpression> select_list;
  select_list.emplace_back(std::string_view(""),
                           correlation_inner->AsColumnValue().GetColumnName());
  select_list.emplace_back(std::string_view(""), item);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetGroupBy(
      {ColumnValueExp(correlation_inner->AsColumnValue().GetColumnName())});
  ExecutionRuntime* runtime = context.execution_runtime();
  if (runtime == nullptr ||
      !StatementUsesOnlyScopes(context, *rewritten, {}, ctes) ||
      StatementContainsVolatileFunctionCached(runtime, sub)) {
    return bail("scopes/volatile");
  }
  StatusOr<Relation> grouped = ExecuteQuery(context, *rewritten, nullptr, ctes);
  if (!grouped.HasValue() || grouped.Value().schema.ColumnCount() < 2) {
    return bail("grouped exec");
  }
  // The comparison column resolves in the filter schema; its type must be
  // one TryBinary compares exactly against the map's scalar values.
  const auto compare_offset = LocalColumnOffset(
      schema, (*column_side)->AsColumnValue().GetColumnName());
  if (!compare_offset) {
    return bail("compare col");
  }
  const ValueType compare_type =
      schema.GetColumn(static_cast<size_t>(*compare_offset)).Type();
  if (compare_type != ValueType::kInt64 && compare_type != ValueType::kDate &&
      compare_type != ValueType::kDouble) {
    return bail("compare type");
  }
  auto values = std::make_shared<std::unordered_map<int64_t, Value>>();
  bool mixed = false;
  bool has_scalar_type = false;
  ValueType scalar_type = ValueType::kNull;
  bool scalar_unsigned = false;
  const Status iteration = grouped.Value().ForEachRow([&](const Row& row) {
    const Value& key = row[0];
    if (key.IsNull()) {
      return;  // NULL correlation keys never match an outer probe.
    }
    const Value& scalar = row[1];
    if (!scalar.IsNull()) {
      if (!has_scalar_type) {
        has_scalar_type = true;
        scalar_type = scalar.type;
        scalar_unsigned = scalar.IsUnsigned();
      } else if (scalar.type != scalar_type ||
                 scalar.IsUnsigned() != scalar_unsigned) {
        mixed = true;  // Mixed domains promote under `=`; keep the AST path.
      }
    }
    values->emplace(key.value.int_value, scalar);
  });
  if (mixed || iteration != Status::kSuccess) {
    return bail("map build");
  }
  if (has_scalar_type && scalar_type != ValueType::kInt64 &&
      scalar_type != ValueType::kDate && scalar_type != ValueType::kDouble) {
    return bail("scalar type");
  }
  // DATE compares only with DATE (and numbers only with numbers); a cross
  // pair is a "type mismatch" error for the per-row evaluator, which the
  // matcher would otherwise turn into a silent row rejection.
  if (has_scalar_type &&
      (compare_type == ValueType::kDate) != (scalar_type == ValueType::kDate)) {
    return bail("compare/scalar type cross");
  }
  PrebuiltScalarComparePredicate compiled;
  compiled.column = static_cast<slot_t>(*compare_offset);
  compiled.key_column = static_cast<slot_t>(*probe_offset);
  compiled.op =
      column_side == &binary.Left() ? binary.Op() : FlipCompare(binary.Op());
  compiled.key_type = probe_type;
  compiled.values = std::move(values);
  return compiled;
}

Expression TryExpandConstantInList(const Expression& expr) {
  if (!expr) {
    return expr;
  }
  bool negated = false;
  const InExpression* in_ptr = nullptr;
  if (expr->Type() == TypeTag::kInExp) {
    in_ptr = &expr->AsInExpression();
  } else if (expr->Type() == TypeTag::kUnaryExp &&
             expr->AsUnaryExpression().Op() == UnaryOperation::kNot &&
             expr->AsUnaryExpression().Child() &&
             expr->AsUnaryExpression().Child()->Type() == TypeTag::kInExp) {
    negated = true;
    in_ptr = &expr->AsUnaryExpression().Child()->AsInExpression();
  } else {
    return expr;
  }
  const InExpression& in = *in_ptr;
  if (!in.child_ || in.child_->Type() != TypeTag::kColumnValue ||
      in.list_.empty() || in.list_.size() > kMaxDisjunctiveBranches) {
    return expr;
  }
  std::vector<Expression> constants;
  constants.reserve(in.list_.size());
  for (const Expression& item : in.list_) {
    // Arithmetic/date expressions in an IN list are just as invariant as
    // literals. Fold only successful, immutable, column-free evaluations;
    // errors and parameter-dependent expressions retain runtime semantics.
    auto value = TryDecideConstantConjunct(item);
    if (!value || value->IsNull()) {
      return expr;
    }
    constants.push_back(ConstantValueExp(*value));
  }
  const BinaryOperation eq_op =
      negated ? BinaryOperation::kNotEquals : BinaryOperation::kEquals;
  const BinaryOperation join_op =
      negated ? BinaryOperation::kAnd : BinaryOperation::kOr;
  Expression acc = BinaryExpressionExp(in.child_, eq_op, constants[0]);
  for (size_t i = 1; i < constants.size(); ++i) {
    acc = BinaryExpressionExp(
        std::move(acc), join_op,
        BinaryExpressionExp(in.child_, eq_op, constants[i]));
  }
  return acc;
}

CompiledScanFilter::DisjunctiveBranch CompileAndBranch(
    const Expression& and_expr, const Schema& schema) {
  CompiledScanFilter::DisjunctiveBranch branch;
  std::vector<Expression> conjuncts = SplitConjuncts(and_expr);
  for (const Expression& raw_conjunct : conjuncts) {
    const Expression expanded =
        (raw_conjunct && raw_conjunct->Type() == TypeTag::kUnaryExp &&
         raw_conjunct->AsUnaryExpression().Op() == UnaryOperation::kNot)
            ? TryExpandConstantInList(raw_conjunct)
            : raw_conjunct;
    for (const Expression& conjunct : SplitConjuncts(expanded)) {
      if (auto simple = TryCompileSimpleCompare(conjunct, schema)) {
        branch.simple.push_back(std::move(*simple));
      } else if (auto prefix_in = TryCompilePrefixIn(conjunct, schema)) {
        branch.prefix_in.push_back(std::move(*prefix_in));
      } else {
        branch.residual.push_back(conjunct);
      }
    }
  }
  return branch;
}

// Constant-residual boundary (see CompileScanFilter): decides whether a
// conjunct carries a compile-time value and evaluates it once if so.
// Returns nullopt when the conjunct must stay a runtime predicate.  Volatile
// calls (rand/...) must not change in evaluation count, stable calls keep
// per-row timing meaning inside one statement (conservatively -- kImmutable
// only), parameter slots bind later than this compile, and subqueries /
// aggregates need the runtime context.  Anything referencing a column
// fails the empty-schema evaluation and stays runtime, which makes this
// self-gating: only genuinely constant trees pass.
}  // namespace

std::optional<Value> TryDecideConstantConjunct(const Expression& predicate) {
  if (!predicate) {
    return std::nullopt;
  }
  if (ContainsVolatileCall(predicate) || ContainsParameterSlot(predicate) ||
      ContainsQuery(predicate)) {
    return std::nullopt;
  }
  // Hard column gate: the empty-schema TryEvaluate below is NOT a reliable
  // column gate by itself.  An unbound column value does not necessarily
  // fail there, so a conjunct that references a column could otherwise be
  // "decided" at compile time against an accident of the empty-row
  // evaluation (observed: ISERROR/NULLIFERROR over `A + 1/A` decided to
  // constant TRUE/NULL and silently filtered whole groups).  Only genuinely
  // column-free trees may be decided as compile-time facts.
  if (!predicate->TouchedColumns().empty()) {
    return std::nullopt;
  }
  StatusOr<Value> decided = predicate->TryEvaluate(Row(), Schema());
  if (!decided.HasValue()) {
    return std::nullopt;
  }
  return decided.MoveValue();
}

namespace {

CompiledScanFilter CompileScanFilterUncached(
    const std::vector<Expression>& predicates, const Schema& schema,
    const FilterCompileEnv* env) {
  CompiledScanFilter compiled;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).IsInterval()) {
      // The typed VARCHAR kernels compare bytes and have no interval tag.
      // Preserve the canonical evaluator for this schema.
      compiled.residual = predicates;
      return compiled;
    }
  }
  // Accumulated AND-of-OR state: every OR predicate contributes its own
  // branch list, and the conjunction across predicates is the cross
  // product (AND) of the lists -- NOT a concatenation (OR), which would
  // silently accept rows matching only one of several OR conjuncts.
  std::vector<CompiledScanFilter::DisjunctiveBranch> branches;
  for (const Expression& predicate : predicates) {
    if (!predicate) {
      continue;
    }
    // Constant-residual boundary: a conjunct with a compile-time value is a
    // fact, not a per-row predicate.  Deterministic gate: no volatile call
    // (rand/... change per evaluation), no parameter slot (the slot table
    // arrives with the statement, after this compile), no subquery.  The
    // empty-schema TryEvaluate is itself the column gate: a conjunct that
    // references a column cannot be resolved and stays a runtime predicate.
    if (std::optional<Value> decided = TryDecideConstantConjunct(predicate);
        decided.has_value()) {
      // Mirror MatchRow's conjunct rule exactly once instead of per row:
      // non-NULL truthy decides TRUE (discharge), FALSE or NULL rejects the
      // conjunction cleanly (sibling errors never surface with a
      // rejecting conjunct present; EvaluateConjunctsTolerant).
      const Value& verdict = *decided;
      if (verdict.IsNull() || !verdict.Truthy()) {
        compiled.compile_decided_reject = true;
        break;
      }
      continue;
    }
    Expression eff_predicate = TryExpandConstantInList(predicate);
    if (eff_predicate != predicate &&
        eff_predicate->Type() == TypeTag::kBinaryExp &&
        eff_predicate->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
      std::vector<SimpleComparePredicate> expanded_simples;
      bool all_simple = true;
      for (const Expression& sub : SplitConjuncts(eff_predicate)) {
        if (auto sc = TryCompileSimpleCompare(sub, schema)) {
          expanded_simples.push_back(std::move(*sc));
        } else {
          all_simple = false;
          break;
        }
      }
      if (all_simple && !expanded_simples.empty()) {
        for (auto& sc : expanded_simples) {
          compiled.simple.push_back(std::move(sc));
        }
        continue;
      }
      eff_predicate = predicate;
    }
    // Decompose OR expressions into disjunctive branches.
    // Each OR branch is compiled independently into simple + residual.
    // Only create disjunctive branches when the branch has at least one
    // SimpleCompare predicate AND no query subexpressions.  Otherwise
    // the branch evaluation overhead exceeds the original tree traversal.
    if (eff_predicate->Type() == TypeTag::kBinaryExp &&
        eff_predicate->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      std::vector<Expression> disjuncts = SplitDisjuncts(eff_predicate);
      if (disjuncts.size() >= 2 && !ContainsQuery(eff_predicate)) {
        bool all_branches_usable = true;
        std::vector<CompiledScanFilter::DisjunctiveBranch> new_branches;
        for (const Expression& disjunct : disjuncts) {
          auto branch = CompileAndBranch(disjunct, schema);
          if (branch.residual.size() == 1 &&
              branch.residual[0]->Type() == TypeTag::kInExp) {
            const Expression expanded_in =
                TryExpandConstantInList(branch.residual[0]);
            if (expanded_in != branch.residual[0] &&
                expanded_in->Type() == TypeTag::kBinaryExp &&
                expanded_in->AsBinaryExpression().Op() ==
                    BinaryOperation::kOr) {
              const std::vector<Expression> in_disjuncts =
                  SplitDisjuncts(expanded_in);
              if (new_branches.size() + in_disjuncts.size() <=
                  kMaxDisjunctiveBranches) {
                std::vector<SimpleComparePredicate> in_simples;
                bool all_in_simple = true;
                for (const Expression& id : in_disjuncts) {
                  if (auto sc = TryCompileSimpleCompare(id, schema)) {
                    in_simples.push_back(std::move(*sc));
                  } else {
                    all_in_simple = false;
                    break;
                  }
                }
                if (all_in_simple && !in_simples.empty()) {
                  for (SimpleComparePredicate& sc : in_simples) {
                    CompiledScanFilter::DisjunctiveBranch sub_b;
                    sub_b.simple = branch.simple;
                    sub_b.simple.push_back(std::move(sc));
                    sub_b.prefix_in = branch.prefix_in;
                    new_branches.push_back(std::move(sub_b));
                  }
                  continue;
                }
              }
            }
          }
          // A prefix predicate (or an IN list expanded above) is already a
          // complete branch. Requiring an additional comparison left mixed
          // prefix/IN/range disjunctions on the recursive AST path.
          if (branch.simple.empty() && branch.prefix_in.empty()) {
            all_branches_usable = false;
            break;
          }
          new_branches.push_back(std::move(branch));
        }
        // Cross-product size guard: keep the branch list bounded and push
        // the predicate into the ordinary residual when it would explode.
        const bool too_wide =
            !branches.empty() &&
            branches.size() * new_branches.size() > kMaxDisjunctiveBranches;
        if (all_branches_usable && !too_wide) {
          if (branches.empty()) {
            branches = std::move(new_branches);
          } else {
            std::vector<CompiledScanFilter::DisjunctiveBranch> crossed;
            for (const auto& left : branches) {
              for (const auto& right : new_branches) {
                CompiledScanFilter::DisjunctiveBranch merged;
                merged.simple = left.simple;
                merged.simple.insert(merged.simple.end(), right.simple.begin(),
                                     right.simple.end());
                merged.prefix_in = left.prefix_in;
                merged.prefix_in.insert(merged.prefix_in.end(),
                                        right.prefix_in.begin(),
                                        right.prefix_in.end());
                merged.residual = left.residual;
                merged.residual.insert(merged.residual.end(),
                                       right.residual.begin(),
                                       right.residual.end());
                crossed.push_back(std::move(merged));
              }
            }
            branches = std::move(crossed);
          }
          continue;
        }
      }
    }
    if (auto simple = TryCompileSimpleCompare(predicate, schema)) {
      compiled.simple.push_back(std::move(*simple));
    } else if (auto prefix_in = TryCompilePrefixIn(predicate, schema)) {
      compiled.prefix_in.push_back(std::move(*prefix_in));
    } else if (env != nullptr) {
      // With a compile-time environment, an uncorrelated membership probe
      // over an INT64/DATE column compiles to one immutable value set; the
      // conjunct then leaves the residual entirely, which both removes the
      // per-row contextful dispatch and lets the worker programs complete
      // (TryParallelFilterRows).
      if (auto membership =
              env->prebuild_only_exists
                  ? std::optional<PrebuiltMembershipPredicate>()
                  : TryCompilePrebuiltMembership(predicate, schema,
                                                 *env->context, *env->ctes)) {
        compiled.prebuilt_membership.push_back(std::move(*membership));
      } else if (auto existence =
                     TryCompilePrebuiltExists(predicate, schema, *env)) {
        compiled.prebuilt_membership.push_back(std::move(*existence));
      } else if (auto scalar =
                     env->prebuild_only_exists
                         ? std::optional<PrebuiltScalarComparePredicate>()
                         : TryCompilePrebuiltScalarCompare(
                               predicate, schema, *env->context, *env->ctes)) {
        compiled.prebuilt_scalar_compare.push_back(std::move(*scalar));
      } else {
        compiled.residual.push_back(predicate);
      }
    } else {
      compiled.residual.push_back(predicate);
    }
  }
  compiled.disjunctive_branches = std::move(branches);
  // Pre-compute unsigned column info to avoid per-row schema iteration.
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).IsUnsigned()) {
      compiled.needs_unsigned_tagging = true;
      compiled.unsigned_columns.push_back(i);
    }
  }
  // Compile residual expressions into bytecode for fast row-by-row evaluation.
  // Bytecode avoids recursive expression tree traversal and virtual dispatch,
  // which is the dominant cost for non-selective filters like TPC-H Q20.
  // Combine all residuals into a single AND expression for compilation.
  if (!compiled.residual.empty() && !compiled.residual_bytecode) {
    // Skip bytecode compilation when the schema has duplicate column names.
    // The bytecode evaluator resolves columns by name, and duplicate names
    // cause ambiguity errors that the generic Evaluate() path catches at
    // runtime via FindColumn(). Bytecode bypasses that check, so we must
    // stay on the generic path to preserve the ambiguous-column error.
    bool has_duplicate_cols = false;
    {
      std::unordered_set<std::string> seen_names;
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const std::string& col_name = schema.GetColumn(i).Name().name;
        if (!seen_names.insert(col_name).second) {
          has_duplicate_cols = true;
          break;
        }
      }
    }
    if (BytecodeEnabled() && !has_duplicate_cols) {
      Expression combined = compiled.residual[0];
      for (size_t i = 1; i < compiled.residual.size(); ++i) {
        combined = BinaryExpressionExp(combined, BinaryOperation::kAnd,
                                       compiled.residual[i]);
      }
      if (auto bytecode = BytecodeCompiler::Compile(
              combined, schema, BytecodeCompiler::Context::kFilter)) {
        compiled.residual_bytecode = std::move(*bytecode);
      }
    }
  }
  return compiled;
}

}  // namespace

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema,
                                     const FilterCompileEnv* env) {
  if (predicates.empty()) {
    return CompileScanFilterUncached(predicates, schema, env);
  }
  auto key = ScalarExpressionCacheKey(predicates, schema);
  if (!key) {
    return CompileScanFilterUncached(predicates, schema, env);
  }
  key->push_back(BytecodeEnabled() ? char{1} : char{0});
  struct Cache {
    size_t bytes{0};
    std::unordered_map<std::string, CompiledScanFilter> entries;
  };
  static thread_local Cache cache;
  if (auto found = cache.entries.find(*key); found != cache.entries.end()) {
    return found->second;
  }
  std::vector<Expression> frozen;
  frozen.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    frozen.push_back(FreezeScalarBindings(predicate));
  }
  auto filter = CompileScanFilterUncached(frozen, schema, env);
  // Overcharge AST ownership by the key size, then account the expanded OR
  // branches separately: their cross-product can exceed the input AST size.
  size_t bytes = (key->capacity() * 64) + sizeof(CompiledScanFilter);
  const auto comparisons_bytes = [](const auto& comparisons) {
    size_t total = comparisons.capacity() * sizeof(SimpleComparePredicate);
    for (const auto& comparison : comparisons) {
      if (comparison.constant.type == ValueType::kVarChar) {
        total += (2 * comparison.constant.value.varchar_value.size()) + 32;
      }
    }
    return total;
  };
  bytes += comparisons_bytes(filter.simple);
  for (const auto& branch : filter.disjunctive_branches) {
    bytes += sizeof(branch) + comparisons_bytes(branch.simple);
  }
  constexpr size_t kLimit = size_t{2} * 1024 * 1024;
  if (bytes > kLimit) {
    return filter;
  }
  if (cache.entries.size() >= 256 || cache.bytes > kLimit - bytes) {
    cache.entries.clear();
    cache.bytes = 0;
  }
  cache.bytes += bytes;
  cache.entries.emplace(std::move(*key), filter);
  return filter;
}

bool MatchPrebuiltMembership(const Row& row,
                             const PrebuiltMembershipPredicate& pred) {
  if (pred.runtime_filter != nullptr) {
    const Value& probe = row[pred.column];
    if (probe.IsNull() || probe.type != pred.domain_type) {
      return false;
    }
    return !pred.runtime_filter->Reject(probe.value.int_value);
  }
  if (pred.string_values) {
    if (pred.string_values->empty() && !pred.contains_null) {
      return pred.negated;
    }
    const Value& probe = row[pred.column];
    if (probe.IsNull() || probe.type != ValueType::kVarChar) {
      return false;
    }
    bool found = false;
    if (!probe.IsCaseInsensitive() &&
        PlainStringComparable(probe.AsStringView())) {
      found = pred.string_values->contains(probe.AsStringView());
    } else {
      for (const auto& member : *pred.string_values) {
        auto equal = TryBinary(BinaryOperation::kEquals, probe,
                               Value(std::string(member)));
        if (equal.HasValue() && equal.Value().Truthy()) {
          found = true;
          break;
        }
      }
    }
    return found ? !pred.negated : !pred.contains_null && pred.negated;
  }
  const FlatIntSet& values = *pred.values;
  // An empty candidate set is decisive without consulting the probe: no
  // comparison exists that could produce UNKNOWN, so `x IN ()` is FALSE and
  // `x NOT IN ()` is TRUE even when x itself is NULL.  (Same contract the
  // per-row runtime membership build applies.)
  if (values.Empty() && !pred.contains_null) {
    return pred.negated;
  }
  const Value& probe = row[pred.column];
  if (probe.IsNull() && pred.existence) {
    return pred.negated;
  }
  if (probe.IsNull()) {
    // `x IN (s)` with NULL x is UNKNOWN; `x NOT IN (s)` likewise.  A WHERE
    // conjunct keeps the row only on TRUE, so both polarities reject.
    return false;
  }
  // Peel-time static checks pinned the probe's type tag and unsigned tag to
  // the build domain, so the int-set verdict (type-strict equality) is the
  // SQL equality here -- the same trust the runtime build grants a
  // same-domain probe.  DATE members and probes share the day-count bit
  // domain, so the raw int payload is the key on both sides.
  if (probe.type != pred.domain_type) {
    return false;  // Unreachable by construction; reject conservatively.
  }
  const bool found = values.Contains(probe.value.int_value);
  if (found) {
    return !pred.negated;
  }
  // Miss: a NULL member turns both polarities UNKNOWN; a clean miss answers
  // FALSE for IN and TRUE for NOT IN.
  return !pred.contains_null && pred.negated;
}

bool MatchPrebuiltScalarCompare(const Row& row,
                                const PrebuiltScalarComparePredicate& pred) {
  const Value& key = row[pred.key_column];
  if (key.IsNull() || key.type != pred.key_type) {
    return false;  // NULL key never matches a group.
  }
  const auto it = pred.values->find(key.value.int_value);
  if (it == pred.values->end() || it->second.IsNull()) {
    // Missing group or an all-NULL group: the scalar subquery returns NULL
    // and the comparison is UNKNOWN.
    return false;
  }
  const Value& probe = row[pred.column];
  if (probe.IsNull()) {
    return false;  // Comparison against a NULL scalar is UNKNOWN.
  }
  StatusOr<Value> cmp = TryBinary(pred.op, probe, it->second);
  return cmp.HasValue() && cmp.Value().Truthy();
}

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes,
                     Status* error, ColumnLookupCache* lookup_cache) {
  // A compile-time FALSE / NULL conjunct decides the whole conjunction; see
  // CompiledScanFilter::compile_decided_reject.
  if (filter.compile_decided_reject) {
    return false;
  }
  // Stored rows carry INT64 bit patterns for UINT64 columns; the catalog
  // schema retains the unsigned declaration. Reattach it before matching so
  // both the simple fast path and residual evaluation observe the same
  // unsigned-tagged values as post-scan normalization produces.
  const Row* match_row = &row;
  Row tagged;
  if (filter.needs_unsigned_tagging) {
    tagged = row;
    for (slot_t i : filter.unsigned_columns) {
      if (i < static_cast<slot_t>(tagged.Count()) &&
          tagged[i].type == ValueType::kInt64 && !tagged[i].IsUnsigned()) {
        tagged[i] = tagged[i].WithUnsigned();
      }
    }
    match_row = &tagged;
  }
  // Evaluate top-level simple predicates (conjunction).
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!MatchSimpleCompare(*match_row, pred)) {
      return false;
    }
  }
  for (const PrefixInPredicate& pred : filter.prefix_in) {
    if (!MatchPrefixIn(*match_row, pred)) {
      return false;
    }
  }
  for (const PrebuiltMembershipPredicate& pred : filter.prebuilt_membership) {
    if (!MatchPrebuiltMembership(*match_row, pred)) {
      return false;
    }
  }
  for (const PrebuiltScalarComparePredicate& pred :
       filter.prebuilt_scalar_compare) {
    if (!MatchPrebuiltScalarCompare(*match_row, pred)) {
      return false;
    }
  }
  // Evaluate disjunctive branches: at least one branch must fully pass.
  // Each branch has its own SimpleCompare predicates and residual expressions.
  // This avoids recursive expression tree evaluation for OR-of-ANDs patterns
  // (e.g. TPC-H Q20's 3-way brand filter).
  if (!filter.disjunctive_branches.empty()) {
    bool any_branch_passed = false;
    Status first_branch_error{Status::kSuccess};
    for (const auto& branch : filter.disjunctive_branches) {
      bool branch_ok = true;
      for (const SimpleComparePredicate& pred : branch.simple) {
        if (!MatchSimpleCompare(*match_row, pred)) {
          branch_ok = false;
          break;
        }
      }
      if (branch_ok) {
        for (const PrefixInPredicate& pred : branch.prefix_in) {
          if (!MatchPrefixIn(*match_row, pred)) {
            branch_ok = false;
            break;
          }
        }
      }
      if (branch_ok && !branch.residual.empty()) {
        Scope scope{.row = match_row,
                    .schema = &schema,
                    .outer = outer,
                    .lookup_cache = lookup_cache};
        Status branch_residual_error{Status::kSuccess};
        for (const Expression& predicate : branch.residual) {
          StatusOr<Value> res =
              TryEvaluate(predicate, scope, nullptr, context, ctes);
          if (!res.HasValue()) {
            if (branch_residual_error.ok()) {
              branch_residual_error = res.GetStatus();
            }
            continue;
          }
          if (!Truthy(res.Value())) {
            branch_ok = false;
            branch_residual_error = Status{Status::kSuccess};
            break;
          }
        }
        if (!branch_residual_error.ok()) {
          branch_ok = false;
          if (first_branch_error.ok()) {
            first_branch_error = std::move(branch_residual_error);
          }
        }
      }
      if (branch_ok) {
        any_branch_passed = true;
        break;
      }
    }
    if (!any_branch_passed) {
      if (!first_branch_error.ok() && error != nullptr && error->ok()) {
        *error = std::move(first_branch_error);
      }
      return false;
    }
  }
  if (filter.residual.empty()) {
    return true;
  }
  // Use bytecode evaluator when available: avoids recursive expression tree
  // traversal and virtual dispatch, which is the dominant cost for complex
  // residual predicates like TPC-H Q20's 3-way OR filter.  The compiled
  // program is one left-to-right AND tree, so a raise inside it aborts the
  // whole batch eval; a batch-level error does not decide the row because a
  // sibling conjunct may still cleanly reject it (commutative WHERE
  // semantics), so on error fall through to the per-conjunct evaluation.
  Status residual_error{Status::kSuccess};
  if (filter.residual_bytecode) {
    StatusOr<Value> result =
        TryEvaluateRowAuto(*filter.residual_bytecode, *match_row);
    if (result.HasValue()) {
      return !result.Value().IsNull() && result.Value().Truthy();
    }
    residual_error = result.GetStatus();
  }
  Scope scope{.row = match_row,
              .schema = &schema,
              .outer = outer,
              .lookup_cache = lookup_cache};
  for (const Expression& predicate : filter.residual) {
    StatusOr<Value> res = TryEvaluate(predicate, scope, nullptr, context, ctes);
    if (!res.HasValue()) {
      if (residual_error.ok()) {
        residual_error = res.GetStatus();
      }
      continue;
    }
    if (!Truthy(res.Value())) {
      return false;
    }
  }
  if (!residual_error.ok()) {
    if (error != nullptr && error->ok()) {
      *error = std::move(residual_error);
    }
    return false;
  }
  return true;
}

std::vector<NullPeekCompare> BuildNullPeeks(
    const CompiledScanFilter& filter, const Schema& projected_schema,
    const std::vector<slot_t>* projection, const Schema& full_schema) {
  std::vector<NullPeekCompare> null_tests;
  null_tests.reserve(filter.residual.size());
  for (const Expression& residual : filter.residual) {
    Expression expression = residual;
    bool negated = false;
    while (expression && expression->Type() == TypeTag::kUnaryExp &&
           expression->AsUnaryExpression().Op() == UnaryOperation::kNot) {
      negated = !negated;
      expression = expression->AsUnaryExpression().Child();
    }
    if (!expression || expression->Type() != TypeTag::kUnaryExp) {
      continue;
    }
    const auto& unary = expression->AsUnaryExpression();
    if ((unary.Op() != UnaryOperation::kIsNull &&
         unary.Op() != UnaryOperation::kIsNotNull) ||
        !unary.Child() || unary.Child()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const auto column = LocalColumnOffset(
        projected_schema, unary.Child()->AsColumnValue().GetColumnName());
    if (!column || *column >= projected_schema.ColumnCount() ||
        (projection != nullptr && *column >= projection->size())) {
      continue;
    }
    const slot_t full_column = projection != nullptr
                                   ? (*projection)[*column]
                                   : static_cast<slot_t>(*column);
    if (full_column >= full_schema.ColumnCount()) {
      continue;
    }
    null_tests.push_back(
        {full_column, (unary.Op() == UnaryOperation::kIsNull) != negated});
  }
  return null_tests;
}

std::vector<IntegerPeekCompare> BuildIntegerPeeks(
    const CompiledScanFilter& filter, const std::vector<slot_t>* projection,
    const Schema& full_schema) {
  // A parsed string is an integer payload only in the DATE domain. Keep
  // invalid strings and other column types on their ordinary evaluator path.
  const auto signed_payload = [&](const SimpleComparePredicate& pred) {
    if (pred.constant.IsUnsigned()) {
      return false;
    }
    if (pred.int_payload) {
      return true;
    }
    if (!pred.date_string_payload ||
        (projection != nullptr && pred.column >= projection->size())) {
      return false;
    }
    const slot_t full_column =
        projection != nullptr ? (*projection)[pred.column] : pred.column;
    return full_column < full_schema.ColumnCount() &&
           full_schema.GetColumn(full_column).Type() == ValueType::kDate &&
           !full_schema.GetColumn(full_column).IsUnsigned();
  };
  std::vector<IntegerPeekCompare> peeks;
  peeks.reserve(filter.simple.size());
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!signed_payload(pred)) {
      continue;
    }
    IntegerPeekCompare peek;
    if (projection != nullptr) {
      if (pred.column >= projection->size()) {
        continue;
      }
      peek.column = (*projection)[pred.column];
    } else {
      peek.column = pred.column;
    }
    if (peek.column >= full_schema.ColumnCount()) {
      continue;
    }
    const Column& schema_column = full_schema.GetColumn(peek.column);
    const ValueType type = schema_column.Type();
    if (type != ValueType::kInt64 && type != ValueType::kDate) {
      continue;
    }
    // Peek pre-filters compare signed integers without rechecking; an
    // unsigned column would mis-filter boundary values (see
    // MatchSimpleCompare), so leave unsigned columns to the exact filter.
    if (schema_column.IsUnsigned()) {
      continue;
    }
    peek.op = pred.op;
    peek.constant = pred.int_constant;
    peeks.push_back(peek);
  }
  // An OR/IN domain on one integer column has a safe min/max enclosure.
  // Publish only a pre-decode filter: the original branches still decide
  // every survivor, including holes, NULLs and mixed numeric values.
  if (!filter.disjunctive_branches.empty()) {
    std::unordered_set<slot_t> visited;
    for (const auto& candidate : filter.disjunctive_branches.front().simple) {
      if (!signed_payload(candidate) ||
          !visited.insert(candidate.column).second) {
        continue;
      }
      int64_t low = std::numeric_limits<int64_t>::max();
      int64_t high = std::numeric_limits<int64_t>::min();
      for (const auto& branch : filter.disjunctive_branches) {
        int64_t branch_low = std::numeric_limits<int64_t>::min();
        int64_t branch_high = std::numeric_limits<int64_t>::max();
        for (const auto& pred : branch.simple) {
          if (pred.column != candidate.column || !signed_payload(pred)) {
            continue;
          }
          switch (pred.op) {
            case BinaryOperation::kEquals:
              branch_low = std::max(branch_low, pred.int_constant);
              branch_high = std::min(branch_high, pred.int_constant);
              break;
            case BinaryOperation::kGreaterThan:
            case BinaryOperation::kGreaterThanEquals:
              branch_low = std::max(branch_low, pred.int_constant);
              break;
            case BinaryOperation::kLessThan:
            case BinaryOperation::kLessThanEquals:
              branch_high = std::min(branch_high, pred.int_constant);
              break;
            default:
              break;
          }
        }
        low = std::min(low, branch_low);
        high = std::max(high, branch_high);
      }
      CompiledScanFilter enclosure;
      for (const auto& [op, value] :
           {std::pair{BinaryOperation::kGreaterThanEquals, low},
            std::pair{BinaryOperation::kLessThanEquals, high}}) {
        if ((op == BinaryOperation::kGreaterThanEquals &&
             value == std::numeric_limits<int64_t>::min()) ||
            (op == BinaryOperation::kLessThanEquals &&
             value == std::numeric_limits<int64_t>::max())) {
          continue;
        }
        SimpleComparePredicate predicate;
        predicate.column = candidate.column;
        predicate.op = op;
        predicate.constant = Value(value);
        predicate.int_payload = true;
        predicate.int_constant = value;
        enclosure.simple.push_back(std::move(predicate));
      }
      auto bounds = BuildIntegerPeeks(enclosure, projection, full_schema);
      peeks.insert(peeks.end(), bounds.begin(), bounds.end());
    }
  }
  return peeks;
}

namespace {

bool SchemaHasDuplicateNames(const Schema& schema) {
  std::unordered_set<std::string> seen_names;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (!seen_names.insert(schema.GetColumn(i).Name().name).second) {
      return true;
    }
  }
  return false;
}

Expression ConjoinAll(const std::vector<Expression>& predicates) {
  Expression combined = predicates[0];
  for (size_t i = 1; i < predicates.size(); ++i) {
    combined =
        BinaryExpressionExp(combined, BinaryOperation::kAnd, predicates[i]);
  }
  return combined;
}

// Shared first step of MatchScanFilter / MatchScanFilterWorker: reattach the
// catalog's unsigned declaration to stored INT64 bit patterns.  Returns the
// input row untouched, or the tagged scratch copy.
const Row& TagUnsignedRow(const Row& row, const CompiledScanFilter& filter,
                          Row& scratch) {
  if (!filter.needs_unsigned_tagging) {
    // Callers pass lvalue scan rows; returning the parameter keeps the
    // no-tagging hot path copy-free.
    return row;  // NOLINT(bugprone-return-const-ref-from-parameter)
  }
  scratch = row;
  for (slot_t i : filter.unsigned_columns) {
    if (i < static_cast<slot_t>(scratch.Count()) &&
        scratch[i].type == ValueType::kInt64 && !scratch[i].IsUnsigned()) {
      scratch[i] = scratch[i].WithUnsigned();
    }
  }
  return scratch;
}

}  // namespace

WorkerScanFilterPrograms CompileWorkerFilter(const CompiledScanFilter& filter,
                                             const Schema& schema) {
  WorkerScanFilterPrograms programs;
  if (!BytecodeEnabled() || SchemaHasDuplicateNames(schema)) {
    return programs;
  }
  if (!filter.residual.empty()) {
    if (filter.residual_bytecode) {
      programs.residual = *filter.residual_bytecode;
    } else if (auto compiled = BytecodeCompiler::CompileRowCached(
                   ConjoinAll(filter.residual), schema,
                   BytecodeCompiler::Context::kFilter)) {
      programs.residual = std::move(*compiled);
    } else {
      return programs;
    }
  }
  if (!filter.disjunctive_branches.empty()) {
    programs.branch_residuals.resize(filter.disjunctive_branches.size());
    for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
      const auto& branch = filter.disjunctive_branches[b];
      if (branch.residual.empty()) {
        continue;
      }
      if (auto compiled = BytecodeCompiler::CompileRowCached(
              ConjoinAll(branch.residual), schema,
              BytecodeCompiler::Context::kFilter)) {
        programs.branch_residuals[b] = std::move(*compiled);
      } else {
        return programs;
      }
    }
  }
  programs.complete = true;
  return programs;
}

int MatchScanFilterWorker(const Row& row, const Schema& schema,
                          const CompiledScanFilter& filter,
                          const WorkerScanFilterPrograms& programs) {
  // A compile-time FALSE / NULL conjunct decides the whole conjunction; the
  // programs are vacuously complete for the empty residual and would
  // otherwise accept every row.
  if (filter.compile_decided_reject) {
    return 0;
  }
  // Column binding happens at compile time (name-bound bytecode), so the
  // schema is only part of the interface symmetry with MatchScanFilter.
  (void)schema;
  Row tagged;
  const Row& match_row = TagUnsignedRow(row, filter, tagged);
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!MatchSimpleCompare(match_row, pred)) {
      return 0;
    }
  }
  for (const PrefixInPredicate& pred : filter.prefix_in) {
    if (!MatchPrefixIn(match_row, pred)) {
      return 0;
    }
  }
  for (const PrebuiltMembershipPredicate& pred : filter.prebuilt_membership) {
    if (!MatchPrebuiltMembership(match_row, pred)) {
      return 0;
    }
  }
  for (const PrebuiltScalarComparePredicate& pred :
       filter.prebuilt_scalar_compare) {
    if (!MatchPrebuiltScalarCompare(match_row, pred)) {
      return 0;
    }
  }
  if (!filter.disjunctive_branches.empty()) {
    bool any_branch_passed = false;
    for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
      const auto& branch = filter.disjunctive_branches[b];
      bool branch_ok = true;
      for (const SimpleComparePredicate& pred : branch.simple) {
        if (!MatchSimpleCompare(match_row, pred)) {
          branch_ok = false;
          break;
        }
      }
      if (branch_ok) {
        for (const PrefixInPredicate& pred : branch.prefix_in) {
          if (!MatchPrefixIn(match_row, pred)) {
            branch_ok = false;
            break;
          }
        }
      }
      if (branch_ok && !branch.residual.empty()) {
        StatusOr<Value> decided = TryEvaluateRowAuto(
            // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
            *programs.branch_residuals[b], match_row);
        // A raised program must not decide the row: sibling conjuncts or a
        // later branch may still cleanly reject/accept it, so defer.
        if (!decided.HasValue()) {
          return 2;
        }
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        if (!Truthy(decided.Value())) {
          branch_ok = false;
        }
      }
      if (branch_ok) {
        any_branch_passed = true;
        break;
      }
    }
    if (!any_branch_passed) {
      return 0;
    }
  }
  if (filter.residual.empty()) {
    return 1;
  }
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  StatusOr<Value> decided = TryEvaluateRowAuto(*programs.residual, match_row);
  if (!decided.HasValue()) {
    return 2;
  }
  return Truthy(decided.Value()) ? 1 : 0;
}

bool TryParallelFilterRows(TransactionContext& context, const Relation& source,
                           const CompiledScanFilter& filter,
                           const Schema& match_schema, const Scope* outer,
                           const CteMap& ctes,
                           const ScanRuntimeFilter* runtime_filter,
                           Relation* result, Status* error,
                           Relation* movable_source) {
  constexpr size_t kMinParallelFilterRows = 32768;
  constexpr size_t kMaxParallelFilterWorkers = 16;
  static const bool parallel_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER") != nullptr;
  if (parallel_filter_disabled || source.HasSpill() ||
      (movable_source != nullptr && movable_source != &source) ||
      source.rows.size() < kMinParallelFilterRows) {
    return false;
  }
  const WorkerScanFilterPrograms programs =
      CompileWorkerFilter(filter, match_schema);
  if (!programs.complete) {
    return false;
  }
  const size_t workers =
      std::min({kMaxParallelFilterWorkers,
                QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                source.rows.size() / 8192});
  if (workers <= 1) {
    return false;
  }
  std::vector<Relation> shards(workers);
  struct DeferredRow {
    size_t preceding_rows;
    Row row;
  };
  std::vector<std::vector<DeferredRow>> deferred(workers);
  std::atomic<size_t> key_rejected{0};
  std::atomic<size_t> key_null_rejected{0};
  std::mutex error_mu;
  std::exception_ptr error_ex;
  const size_t chunk_rows = (source.rows.size() + workers - 1) / workers;
  {
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
      try {
        threads.emplace_back([&, w] {
          try {
            auto& local = shards[w];
            // Each worker owns one contiguous range. Merge ranges in source
            // order, including rows that need contextful evaluation.
            size_t accepted_rows = 0;
            const size_t begin = w * chunk_rows;
            const size_t end = std::min(source.rows.size(), begin + chunk_rows);
            for (size_t i = begin; i < end; ++i) {
              const Row& row = source.rows[i];
              if (runtime_filter != nullptr && runtime_filter->enabled()) {
                const Value& key = row[runtime_filter->column];
                if (runtime_filter->Reject(static_cast<int8_t>(key.type),
                                           key.IsNull(), key.value.int_value)) {
                  ++key_rejected;
                  if (key.IsNull()) {
                    ++key_null_rejected;
                  }
                  continue;
                }
              }
              switch (
                  MatchScanFilterWorker(row, match_schema, filter, programs)) {
                case 0:
                  break;
                case 1:
                  ++accepted_rows;
                  if (movable_source != nullptr) {
                    local.AddRow(std::move(movable_source->rows[i]));
                  } else {
                    local.AddRow(row);
                  }
                  break;
                default:
                  if (movable_source != nullptr) {
                    deferred[w].push_back(
                        {accepted_rows, std::move(movable_source->rows[i])});
                  } else {
                    deferred[w].push_back({accepted_rows, row});
                  }
                  break;
              }
            }
          } catch (...) {
            std::scoped_lock lock(error_mu);
            if (!error_ex) {
              error_ex = std::current_exception();
            }
          }
        });
      } catch (...) {
        // Thread *creation* failed, not the worker body. Record it, or the
        // merge below drains empty shards and reports a clean result that is
        // silently missing every row the missing worker would have produced.
        std::scoped_lock lock(error_mu);
        if (!error_ex) {
          error_ex = std::current_exception();
        }
        break;
      }
    }
  }
  if (error_ex) {
    if (error != nullptr && error->ok()) {
      try {
        std::rethrow_exception(error_ex);
      } catch (const std::exception& ex) {
        *error = StatusError(StatusCode::kRuntimeError, ex.what());
      } catch (...) {
        *error = StatusError(StatusCode::kRuntimeError,
                             "parallel filter worker failed");
      }
    }
    return true;
  }
  // Main-thread merge in worker order; deferred rows take the contextful
  // MatchScanFilter path so error semantics are bit-identical to serial.
  const auto record_error = [&](Status status) {
    if (error != nullptr && error->ok() && status != Status::kSuccess) {
      *error = std::move(status);
    }
  };
  size_t total_in_memory = 0;
  for (size_t w = 0; w < workers; ++w) {
    if (!shards[w].HasSpill()) {
      total_in_memory += shards[w].rows.size();
    }
  }
  if (total_in_memory > 0) {
    result->rows.reserve(result->rows.size() + total_in_memory);
  }
  for (size_t w = 0; w < workers; ++w) {
    record_error(shards[w].FinishSpill());
    if (deferred[w].empty() && !shards[w].HasSpill()) {
      result->MergeInMemoryShard(std::move(shards[w]));
    } else {
      size_t accepted_rows = 0;
      size_t deferred_index = 0;
      const auto emit_deferred = [&] {
        while (deferred_index < deferred[w].size() &&
               deferred[w][deferred_index].preceding_rows == accepted_rows) {
          Row& row = deferred[w][deferred_index++].row;
          Status deferred_error{Status::kSuccess};
          if (MatchScanFilter(row, match_schema, filter, outer, context, ctes,
                              &deferred_error)) {
            result->AddRow(std::move(row));
          }
          record_error(std::move(deferred_error));
        }
      };
      if (shards[w].HasSpill()) {
        // ForEachRow includes the append-only tail as well as resident rows.
        record_error(shards[w].ForEachRow([&](const Row& row) {
          emit_deferred();
          result->AddRow(row);
          ++accepted_rows;
        }));
      } else {
        for (Row& row : shards[w].rows) {
          emit_deferred();
          result->AddRow(std::move(row));
          ++accepted_rows;
        }
      }
      emit_deferred();
    }
    deferred[w].clear();
    record_error(shards[w].GetStatus());
    shards[w].ResetContents();
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rejected.load();
    context.execution_runtime()->key_filter_null_rejected +=
        key_null_rejected.load();
  }
  return true;
}

Relation UnnestValueToRelation(const SelectSource& source,
                               const Value& array_val) {
  auto trim_part = [](std::string_view text) {
    size_t begin = text.find_first_not_of(" \t");
    if (begin == std::string_view::npos) {
      return std::string();
    }
    size_t end = text.find_last_not_of(" \t");
    return std::string(text.substr(begin, end - begin + 1));
  };
  Relation result;
  std::string col_name = source.alias.empty() ? "unnest" : source.alias;
  ValueType elem_type = ValueType::kNull;
  std::vector<Value> elements;
  if (array_val.IsArray()) {
    elements = array_val.ArrayElements();
    // Infer from the first NON-NULL element: a leading NULL would pin the
    // column to kNull while later rows carry real types — the per-row type
    // flip vectorized consumers reject.
    for (const Value& element : elements) {
      if (!element.IsNull()) {
        elem_type = element.type;
        break;
      }
    }
  } else if (!array_val.IsNull()) {
    elements.push_back(array_val);
    elem_type = array_val.type;
  }
  const std::string elem_sql_type =
      array_val.IsArray() ? array_val.ArrayElementSqlType() : "";
  if (elem_sql_type == "PROTO") {
    std::vector<Column> cols;
    cols.emplace_back(col_name, ValueType::kVarChar);
    std::vector<std::string> field_names;
    for (const Value& element : elements) {
      if (element.IsNull() || element.type != ValueType::kVarChar) {
        continue;
      }
      std::vector<ProtoTextEntry> parsed;
      const std::string text(element.value.varchar_value);
      if (!ParseProtoTextEntries(text, &parsed)) {
        continue;
      }
      for (const ProtoTextEntry& entry : parsed) {
        if (std::ranges::none_of(field_names, [&](const std::string& name) {
              return name == entry.name;
            })) {
          field_names.push_back(entry.name);
        }
      }
    }
    std::vector<std::vector<Value>> field_values(
        elements.size(), std::vector<Value>(field_names.size()));
    for (size_t field_idx = 0; field_idx < field_names.size(); ++field_idx) {
      std::string qualified_name = col_name + "." + field_names[field_idx];
      ValueType type = ValueType::kNull;
      for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
        if (elements[row_idx].IsNull() ||
            elements[row_idx].type != ValueType::kVarChar) {
          continue;
        }
        Value field;
        if (ProtoTextExtractField(elements[row_idx].value.varchar_value,
                                  field_names[field_idx], &field)) {
          field_values[row_idx][field_idx] = field;
          if (!field.IsNull()) {
            type = field.type;
          }
        }
      }
      cols.emplace_back(std::move(qualified_name), type);
    }
    if (!source.offset_alias.empty()) {
      cols.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    result.schema = Schema("", std::move(cols));
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      std::vector<Value> row_vals;
      row_vals.push_back(std::move(elements[row_idx]));
      for (auto& fv : field_values[row_idx]) {
        row_vals.push_back(std::move(fv));
      }
      if (!source.offset_alias.empty()) {
        row_vals.emplace_back(static_cast<int64_t>(row_idx));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  // GoogleSQL UNNEST over an ARRAY<STRUCT> flattens one structural level:
  // the struct's fields become top-level columns (`FROM UNNEST([STRUCT(1 AS
  // y, 2 AS x)])` exposes y and x).  Field names come from the first
  // non-NULL element's encoded JSON keys; NULL elements and later rows with
  // fewer members NULL-fill to the initialized width.
  const bool declared_struct = elem_sql_type.starts_with("STRUCT");
  bool object_shaped = false;
  if (!declared_struct && (elem_sql_type.empty() || elem_sql_type == "INT64")) {
    for (const Value& element : elements) {
      if (element.IsNull()) {
        continue;
      }
      if (element.type == ValueType::kVarChar) {
        const std::string_view text(element.value.varchar_value);
        object_shaped =
            text.size() >= 2 && text.front() == '{' && text.back() == '}';
      }
      break;
    }
  }
  if (declared_struct || object_shaped) {
    // An explicitly aliased UNNEST (`t.struct_arrcol elem`) keeps the whole
    // element reachable under its alias (a NULL element stays NULL); the
    // flattened member columns sit beside it.  An unaliased UNNEST exposes
    // only the member columns.
    const bool keep_element_column =
        !source.alias.empty() && source.alias != "unnest";
    std::vector<std::pair<std::string, ValueType>> fields;
    // Declared element types (`ARRAY<STRUCT<start_day DATE, ...>>`) name
    // fields even when the encoded members use anonymous fN keys, and let
    // textual members coerce back to their declared runtime type.
    std::vector<std::pair<std::string, std::string>> declared_fields;
    if (declared_struct && elem_sql_type.back() == '>' &&
        elem_sql_type.find('<') != std::string::npos) {
      const size_t open = elem_sql_type.find('<');
      std::vector<std::string> parts;
      int bracket = 0;
      std::string current;
      for (size_t i = open + 1; i < elem_sql_type.size(); ++i) {
        const char c = elem_sql_type[i];
        if (c == '<' || c == '(') {
          ++bracket;
        }
        if (c == '>' || c == ')') {
          --bracket;
        }
        if (c == ',' && bracket == 0) {
          parts.push_back(current);
          current.clear();
          continue;
        }
        current.push_back(c);
      }
      if (!current.empty()) {
        parts.push_back(current);
      }
      for (std::string& part : parts) {
        const std::string trimmed = trim_part(part);
        if (trimmed.empty()) {
          continue;
        }
        const size_t space = trimmed.find_first_of(" \t");
        if (space == std::string::npos) {
          declared_fields.emplace_back(std::string(), trimmed);
        } else {
          declared_fields.emplace_back(trimmed.substr(0, space),
                                       trim_part(trimmed.substr(space + 1)));
        }
      }
    }
    struct ParsedElement {
      std::vector<Value> values;
      bool null_row{false};
    };
    std::vector<ParsedElement> parsed_rows(elements.size());
    for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
      const Value& element = elements[row_idx];
      if (element.IsNull() || element.type != ValueType::kVarChar) {
        parsed_rows[row_idx].null_row = true;
        continue;
      }
      const std::string text(element.value.varchar_value);
      if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
        parsed_rows[row_idx].null_row = true;
        continue;
      }
      const auto members =
          SplitJsonObjectMembers(text.substr(1, text.size() - 2));
      for (const auto& [key, member_text] : members) {
        Value parsed;
        if (!JsonTextToValue(member_text, &parsed)) {
          // Non-JSON scalars (e.g. `inf`, `nan` doubles) fall back to
          // numeric then textual interpretation.
          char* parse_end = nullptr;
          const double number = std::strtod(member_text.c_str(), &parse_end);
          if (parse_end != member_text.c_str() && *parse_end == '\0') {
            parsed = Value(number);
          } else {
            parsed = Value(std::string(member_text));
          }
        }
        // Coerce textual members back to declared runtime types: struct
        // serialization stores DATE/TIMESTAMP cells as plain text.
        if (parsed.type == ValueType::kVarChar &&
            declared_fields.size() == members.size()) {
          const size_t member_index = parsed_rows[row_idx].values.size();
          const std::string& declared_type =
              declared_fields[member_index].second;
          std::string declared_text(parsed.value.varchar_value);
          if ((declared_type == "DATE" || declared_type == "date")) {
            int y = 0, m = 0, d = 0;
            if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                    declared_text.c_str(), "%d-%d-%d", &y, &m, &d) == 3) {
              parsed = Value::Date(declared_text);
            }
          } else if (declared_type == "TIMESTAMP" ||
                     declared_type == "timestamp" ||
                     declared_type == "DATETIME") {
            int y = 0, mo = 0, d = 0;
            if (sscanf(  // NOLINT(cert-err34-c) - conversion count is checked.
                    declared_text.c_str(), "%d-%d-%d", &y, &mo, &d) == 3) {
              parsed =
                  Value(std::move(declared_text));  // keep canonical text form
            }
          }
        }
        parsed_rows[row_idx].values.push_back(std::move(parsed));
      }
      if (fields.empty()) {
        for (const auto& [key, member_text] : members) {
          fields.emplace_back(key, ValueType::kNull);
        }
        // Prefer declared field names when the arity matches: anonymous
        // tuple literals encode members as f1..fN under a declared type.
        if (declared_fields.size() == fields.size()) {
          for (size_t i = 0; i < declared_fields.size(); ++i) {
            if (!declared_fields[i].first.empty()) {
              fields[i].first = declared_fields[i].first;
            }
          }
        }
        // Fix up column types from the representative row's values.
        for (size_t i = 0;
             i < parsed_rows[row_idx].values.size() && i < fields.size(); ++i) {
          fields[i].second = parsed_rows[row_idx].values[i].IsNull()
                                 ? ValueType::kNull
                                 : parsed_rows[row_idx].values[i].type;
        }
      }
    }
    std::vector<Column> unnest_cols;
    unnest_cols.reserve(fields.size() + (keep_element_column ? 1 : 0) +
                        (!source.offset_alias.empty() ? 1 : 0));
    for (const auto& [field_name, field_type] : fields) {
      unnest_cols.emplace_back(field_name, field_type);
    }
    if (keep_element_column) {
      unnest_cols.emplace_back(col_name, ValueType::kVarChar);
    }
    if (!source.offset_alias.empty()) {
      unnest_cols.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    result.schema = Schema("", std::move(unnest_cols));
    for (size_t row_idx = 0; row_idx < parsed_rows.size(); ++row_idx) {
      std::vector<Value> row_vals(fields.size(), Value());
      if (!parsed_rows[row_idx].null_row) {
        for (size_t i = 0;
             i < fields.size() && i < parsed_rows[row_idx].values.size(); ++i) {
          row_vals[i] = parsed_rows[row_idx].values[i];
        }
      }
      if (keep_element_column) {
        row_vals.push_back(elements[row_idx]);
      }
      if (!source.offset_alias.empty()) {
        row_vals.emplace_back(static_cast<int64_t>(row_idx));
      }
      result.AddRow(Row(std::move(row_vals)));
    }
    return result;
  }
  // Scalar / untyped elements stay in their single encoded column instead:
  // a NULL element surfaces as a NULL value, and field access resolves via
  // Lookup's dotted-path traversal of the encoding.
  std::vector<Column> unnest_cols;
  unnest_cols.emplace_back(col_name, elem_type);
  if (!source.offset_alias.empty()) {
    unnest_cols.emplace_back(source.offset_alias, ValueType::kInt64);
  }
  result.schema = Schema("", std::move(unnest_cols));
  for (size_t row_idx = 0; row_idx < elements.size(); ++row_idx) {
    if (!source.offset_alias.empty()) {
      result.AddRow(Row({std::move(elements[row_idx]),
                         Value(static_cast<int64_t>(row_idx))}));
    } else {
      result.AddRow(Row({std::move(elements[row_idx])}));
    }
  }
  return result;
}

namespace {
// Forward declaration: defined with the subquery-fold helpers below.
bool ContainsReentrantQueryIn(const Expression& expression,
                              std::string_view table);
}  // namespace

namespace {
// A selective numeric range supplies an enclosure of the visible rows. Keep
// physical table order so floating-point aggregates and stable ties retain
// the full-scan contract. Estimates choose this path, never bound its answer.
StatusOr<bool> TrySelectiveIndexSource(
    TransactionContext& context, const Table& table,
    const TableStatistics& statistics, const Schema& filter_schema,
    const std::vector<slot_t>* projection, const CompiledScanFilter& filter,
    const ScanRuntimeFilter* runtime_filter, const Scope* outer,
    const CteMap& ctes, Relation* result) {
  if (table.IndexCount() == 0 || statistics.Rows() < 1024) {
    return false;
  }
  const bool pure_range =
      !filter.simple.empty() && !filter.compile_decided_reject &&
      filter.prefix_in.empty() && filter.prebuilt_membership.empty() &&
      filter.prebuilt_scalar_compare.empty() &&
      filter.disjunctive_branches.empty() && !filter.needs_unsigned_tagging;
  const auto* exact = (runtime_filter != nullptr) && runtime_filter->enabled()
                          ? runtime_filter->group->ExactKeys()
                          : nullptr;
  const auto local_expression = [&](const auto& self,
                                    const Expression& expression) -> bool {
    if (!expression) {
      return true;
    }
    switch (expression->Type()) {
      case TypeTag::kColumnValue:
      case TypeTag::kConstantValue:
      case TypeTag::kBinaryExp:
      case TypeTag::kUnaryExp:
      case TypeTag::kCaseExp:
      case TypeTag::kInExp:
        return std::ranges::all_of(
            ExpressionChildren(expression),
            [&](const Expression& child) { return self(self, child); });
      default:
        return false;
    }
  };
  const auto local_predicates = [&](const auto& predicates) {
    return std::ranges::all_of(predicates, [&](const Expression& predicate) {
      return local_expression(local_expression, predicate);
    });
  };
  // A literal LIKE over a local string column does not widen the key
  // enclosure. Evaluate it only after restoring heap order, just as on the
  // full scan. Other residuals keep the previous conservative range gate.
  const bool safe_range =
      pure_range &&
      std::ranges::all_of(filter.residual, [&](const Expression& expression) {
        if (!expression || expression->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& binary = expression->AsBinaryExpression();
        if ((binary.Op() != BinaryOperation::kLike &&
             binary.Op() != BinaryOperation::kNotLike) ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const auto offset = LocalColumnOffset(
            filter_schema, binary.Left()->AsColumnValue().GetColumnName());
        return offset &&
               filter_schema.GetColumn(*offset).Type() == ValueType::kVarChar &&
               binary.Right()->AsConstantValue().GetValue().type ==
                   ValueType::kVarChar;
      });
  if ((exact != nullptr) &&
      (exact->size() > 1024 || !local_predicates(filter.residual) ||
       !std::ranges::all_of(filter.disjunctive_branches,
                            [&](const auto& branch) {
                              return local_predicates(branch.residual);
                            }))) {
    exact = nullptr;
  }
  if (!safe_range && (exact == nullptr)) {
    return false;
  }
  const Index* selected = nullptr;
  std::vector<Value> begin, end;
  const std::vector<int64_t>* selected_keys = nullptr;
  double best = 0.05;
  const Schema& schema = table.GetSchema();
  for (size_t i = 0; i < table.IndexCount(); ++i) {
    const Index& index = table.GetIndex(i);
    if (index.sc_.key_.empty() || index.RetainsDeletedEntries()) {
      continue;
    }
    const slot_t key = index.sc_.key_.front();
    const Column& column = schema.GetColumn(key);
    if (column.IsUnsigned() || (column.Type() != ValueType::kInt64 &&
                                column.Type() != ValueType::kDate)) {
      continue;
    }
    if (key >= statistics.Columns()) {
      continue;
    }
    if ((exact != nullptr) &&
        runtime_filter->column < filter_schema.ColumnCount()) {
      const slot_t runtime_column = (projection != nullptr)
                                        ? (*projection)[runtime_filter->column]
                                        : runtime_filter->column;
      if (runtime_column == key && runtime_filter->type == column.Type()) {
        double matches = 0;
        for (int64_t raw : *exact) {
          if (runtime_filter->group->Reject(raw)) {
            continue;
          }
          Value value(raw);
          value.type = column.Type();
          matches += statistics.Column(key).EstimateEqual(value);
        }
        const double fraction =
            matches / static_cast<double>(statistics.Rows());
        if (fraction >= 0 && fraction < best) {
          selected = &index;
          selected_keys = exact;
          best = fraction;
        }
      }
    }
    if (!safe_range) {
      continue;
    }
    std::optional<Value> lower, upper;
    for (const auto& predicate : filter.simple) {
      if (predicate.column_payload ||
          predicate.column >= filter_schema.ColumnCount()) {
        continue;
      }
      const slot_t full_column = (projection != nullptr)
                                     ? (*projection)[predicate.column]
                                     : predicate.column;
      if (full_column != key || predicate.constant.IsNull() ||
          predicate.constant.type != column.Type() ||
          predicate.constant.IsUnsigned()) {
        continue;
      }
      const auto op = predicate.op;
      if (op == BinaryOperation::kEquals ||
          op == BinaryOperation::kGreaterThan ||
          op == BinaryOperation::kGreaterThanEquals) {
        if (!lower ||
            predicate.constant.value.int_value > lower->value.int_value) {
          lower = predicate.constant;
        }
      }
      if (op == BinaryOperation::kEquals || op == BinaryOperation::kLessThan ||
          op == BinaryOperation::kLessThanEquals) {
        if (!upper ||
            predicate.constant.value.int_value < upper->value.int_value) {
          upper = predicate.constant;
        }
      }
    }
    if (!lower && !upper) {
      continue;
    }
    // Cost the enclosure itself; an unrelated predicate must not disguise
    // a broad index range as a selective lookup.
    if (key >= statistics.Columns()) {
      continue;
    }
    const double fraction =
        statistics.Column(key).EstimateRange(lower, true, upper, true) /
        static_cast<double>(statistics.Rows());
    if (fraction >= 0 && fraction < best) {
      selected = &index;
      selected_keys = nullptr;
      begin = lower ? std::vector<Value>{*lower} : std::vector<Value>{};
      end = upper ? std::vector<Value>{*upper} : std::vector<Value>{};
      best = fraction;
    }
  }
  if ((selected == nullptr) ||
      context.txn_.IndexKeysMayBeStale(selected->Root())) {
    return false;
  }
  constexpr size_t kMaxRows = 8192;
  constexpr size_t kBytes = size_t{4} * 1024 * 1024;
  auto charge = TryReserveQueryMemory(kBytes);
  if (!charge) {
    return false;
  }
  auto guard = context.txn_.AcquireIndexReadGuard(selected->Root());
  if (context.txn_.IndexKeysMayBeStale(selected->Root())) {
    return false;
  }
  std::vector<Expression> early_memberships;
  if (!filter.needs_unsigned_tagging) {
    for (const auto& expression : filter.residual) {
      if (!expression || expression->Type() != TypeTag::kInExp) {
        continue;
      }
      const auto& membership = expression->AsInExpression();
      if (membership.child_ &&
          membership.child_->Type() == TypeTag::kColumnValue &&
          std::ranges::all_of(membership.list_, [](const Expression& value) {
            return value && value->Type() == TypeTag::kConstantValue;
          })) {
        early_memberships.push_back(expression);
      }
    }
  }
  struct SourceRow {
    RowPosition position;
    Row row;
    size_t page_ordinal{0};
  };
  std::vector<SourceRow> rows;
  size_t bytes = 0, visited = 0;
  const size_t scans = (selected_keys != nullptr) ? selected_keys->size() : 1;
  for (size_t seek = 0; seek < scans; ++seek) {
    if (selected_keys != nullptr) {
      const int64_t raw = (*selected_keys)[seek];
      if (runtime_filter->group->Reject(raw)) {
        continue;
      }
      Value value(raw);
      value.type = schema.GetColumn(selected->sc_.key_.front()).Type();
      begin = end = std::vector<Value>{value};
    }
    IndexScanIterator iterator(table, *selected, context.txn_, begin, end, true,
                               false, &guard);
    if (projection != nullptr)
      RETURN_IF_FAIL(iterator.SetProjection(*projection));
    while (iterator.IsValid()) {
      if (++visited > kMaxRows) {
        return false;
      }
      const Row& full_row = *iterator;
      RETURN_IF_FAIL(iterator.GetStatus());
      if (!full_row.IsValid()) {
        ++iterator;
        continue;
      }
      if (full_row.Count() != filter_schema.ColumnCount()) {
        return StatusError(StatusCode::kCorrupt,
                           "indexed source row width mismatch");
      }
      const Row& row = full_row;
      bool matches = true;
      if (!filter.needs_unsigned_tagging) {
        matches =
            std::ranges::all_of(filter.simple, [&](const auto& predicate) {
              return MatchSimpleCompare(row, predicate);
            });
        if (matches && !filter.disjunctive_branches.empty()) {
          matches = std::ranges::any_of(
              filter.disjunctive_branches, [&](const auto& branch) {
                return std::ranges::all_of(
                    branch.simple, [&](const auto& predicate) {
                      return MatchSimpleCompare(row, predicate);
                    });
              });
        }
      }
      for (const auto& membership : early_memberships) {
        if (!matches) {
          break;
        }
        const auto value = membership->TryEvaluate(row, filter_schema);
        // Errors stay on the ordered canonical path. Only a clean rejection
        // can discard a row before restoring physical order.
        if (value.HasValue() &&
            (value.Value().IsNull() || !value.Value().Truthy())) {
          matches = false;
        }
      }
      if (matches && (runtime_filter != nullptr) && runtime_filter->enabled()) {
        const Value& key = row[runtime_filter->column];
        matches = !runtime_filter->Reject(static_cast<int8_t>(key.type),
                                          key.IsNull(), key.value.int_value);
      }
      if (matches) {
        bytes += (row.values_.capacity() * sizeof(Value)) +
                 (sizeof(RowPosition) * 4) + (sizeof(Row) * 2);
        for (const Value& value : row.values_) {
          if (value.type == ValueType::kVarChar) {
            bytes += (2 * value.value.varchar_value.size()) + 32;
          }
        }
        if (bytes > kBytes / 2) {
          return false;
        }
        rows.push_back({iterator.Position(), std::move(*iterator), 0});
      }
      ++iterator;
    }
    RETURN_IF_FAIL(iterator.GetStatus());
  }
  if (rows.size() > 1) {
    std::unordered_map<page_id_t, size_t> page_order;
    const auto morsels = table.BuildScanMorsels(context.txn_);
    size_t ordinal = 0;
    for (const auto& morsel : morsels) {
      for (page_id_t page : morsel) {
        if ((page_order.size() * 64) + bytes > kBytes) {
          return false;
        }
        page_order.emplace(page, ordinal++);
      }
    }
    for (auto& row : rows) {
      const auto found = page_order.find(row.position.page_id);
      if (found == page_order.end()) {
        return false;
      }
      row.page_ordinal = found->second;
    }
    std::stable_sort(rows.begin(), rows.end(),
                     [](const auto& a, const auto& b) {
                       return a.page_ordinal != b.page_ordinal
                                  ? a.page_ordinal < b.page_ordinal
                                  : a.position.slot < b.position.slot;
                     });
  }
  // The auxiliary reservation remains held until its buffers are destroyed;
  // Relation independently charges (and can spill) the transferred rows.
  Status eval_error{Status::kSuccess};
  size_t emitted = 0;
  for (auto& row : rows) {
    if (MatchScanFilter(row.row, filter_schema, filter, outer, context, ctes,
                        &eval_error)) {
      result->AddRow(std::move(row.row));
      ++emitted;
    }
    RETURN_IF_FAIL(eval_error);
  }
  RETURN_IF_FAIL(result->GetStatus());
  if (auto* runtime = context.execution_runtime()) {
    ++runtime->selective_index_scans;
    runtime->selective_index_rows += visited;
    runtime->scan_rows += visited;
    runtime->scan_values_available += visited * schema.ColumnCount();
    runtime->scan_values_decoded += visited * filter_schema.ColumnCount();
    runtime->scan_output_rows += emitted;
  }
  return true;
}

void MaterializeSingleArenaRow(const BaseRelationArena& arena,
                               const Schema& schema, size_t index, Row* row) {
  const size_t stride = arena.stride;
  const int64_t* cells = arena.cells.data() + (index * stride);
  row->values_.resize(stride);
  for (size_t c = 0; c < stride; ++c) {
    Value& dst = row->values_[c];
    if (arena.IsNull(index, c)) [[unlikely]] {
      if (dst.type != ValueType::kNull) {
        dst = Value();
      }
      continue;
    }
    const ValueType type = arena.slot_types.size() == stride
                               ? arena.slot_types[c]
                               : schema.GetColumn(c).Type();
    if (type == ValueType::kVarChar) {
      const std::string_view sv = arena.Varchar(index, c);
      dst.type = ValueType::kVarChar;
      dst.owned_data.assign(sv);
      dst.value.varchar_value = dst.owned_data;
    } else if (type == ValueType::kDouble) {
      dst.type = ValueType::kDouble;
      dst.value.double_value = std::bit_cast<double>(cells[c]);
    } else {
      dst.type = type;
      dst.value.int_value = cells[c];
      if (type == ValueType::kInt64 && schema.ColumnCount() > c &&
          schema.GetColumn(c).IsUnsigned()) [[unlikely]] {
        dst.SetUnsigned();
      }
    }
  }
}

std::optional<bool> TryMatchSimpleCompareArena(
    const BaseRelationArena& arena, size_t row,
    const SimpleComparePredicate& pred) {
  const size_t stride = arena.stride;
  if (pred.column >= stride || arena.slot_types.size() != stride) {
    return std::nullopt;
  }
  if (pred.column_payload) {
    if (pred.right_column >= stride) {
      return std::nullopt;
    }
    if (arena.IsNull(row, pred.column) ||
        arena.IsNull(row, pred.right_column)) {
      return false;
    }
    const ValueType lt = arena.slot_types[pred.column];
    const ValueType rt = arena.slot_types[pred.right_column];
    const int64_t* row_cells = arena.cells.data() + (row * stride);
    if ((lt == ValueType::kInt64 || lt == ValueType::kDate) &&
        (rt == ValueType::kInt64 || rt == ValueType::kDate)) {
      return ComparePositional(pred.op, row_cells[pred.column],
                               row_cells[pred.right_column]);
    }
    if (lt == ValueType::kDouble && rt == ValueType::kDouble) {
      return ComparePositional(
          pred.op, std::bit_cast<double>(row_cells[pred.column]),
          std::bit_cast<double>(row_cells[pred.right_column]));
    }
    if (lt == ValueType::kVarChar && rt == ValueType::kVarChar) {
      return ComparePositional(pred.op, arena.Varchar(row, pred.column),
                               arena.Varchar(row, pred.right_column));
    }
    return std::nullopt;
  }
  if (arena.IsNull(row, pred.column) || pred.constant.IsNull()) {
    return false;
  }
  const ValueType vt = arena.slot_types[pred.column];
  const int64_t* row_cells = arena.cells.data() + (row * stride);
  if (pred.int_payload && !pred.constant.IsUnsigned() &&
      (vt == ValueType::kInt64 || vt == ValueType::kDate)) {
    return ComparePositional(pred.op, row_cells[pred.column],
                             pred.int_constant);
  }
  if (pred.double_payload && vt == ValueType::kDouble) {
    return ComparePositional(pred.op,
                             std::bit_cast<double>(row_cells[pred.column]),
                             pred.double_constant);
  }
  if (vt == ValueType::kVarChar && pred.constant.type == ValueType::kVarChar &&
      !pred.constant.IsCaseInsensitive()) {
    const std::string_view l = arena.Varchar(row, pred.column);
    const std::string_view r = pred.constant.value.varchar_value;
    if (pred.op == BinaryOperation::kLike ||
        pred.op == BinaryOperation::kNotLike) {
      const bool matched = FastLikeMatch(l, r);
      return pred.op == BinaryOperation::kLike ? matched : !matched;
    }
    if (pred.plain_string_payload) {
      return ComparePositional(pred.op, l, r);
    }
  }
  return std::nullopt;
}

bool TryFilterArenaCache(
    TransactionContext& context, const BaseRelationArena& arena,
    const Schema& cached_schema, const CompiledScanFilter* filter,
    const Schema* filter_view, const Scope* outer, const CteMap& ctes,
    const ScanRuntimeFilter* runtime_filter, Relation* result, Status* error) {
  if (arena.rows == 0 || arena.slot_types.size() != arena.stride) {
    return false;
  }
  for (size_t c = 0; c < cached_schema.ColumnCount(); ++c) {
    if (cached_schema.GetColumn(c).IsUnsigned()) {
      return false;
    }
  }
  if (filter != nullptr && filter->compile_decided_reject) {
    return true;
  }
  const bool has_non_simple =
      filter != nullptr &&
      (filter->needs_unsigned_tagging || !filter->prefix_in.empty() ||
       !filter->prebuilt_membership.empty() ||
       !filter->prebuilt_scalar_compare.empty() || !filter->residual.empty() ||
       !filter->disjunctive_branches.empty());
  const WorkerScanFilterPrograms programs =
      (filter != nullptr && has_non_simple && filter_view != nullptr)
          ? CompileWorkerFilter(*filter, *filter_view)
          : WorkerScanFilterPrograms{};
  constexpr size_t kMinParallelFilterRows = 32768;
  constexpr size_t kMaxParallelFilterWorkers = 16;
  static const bool parallel_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER") != nullptr;
  const size_t workers =
      (!parallel_filter_disabled && arena.rows >= kMinParallelFilterRows &&
       (!has_non_simple || programs.complete))
          ? std::min(
                {kMaxParallelFilterWorkers,
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 arena.rows / 8192})
          : 1;

  auto row_passes_arena_precheck = [&](size_t r, size_t* key_rej,
                                       size_t* key_null_rej,
                                       bool* all_simple_decided) -> bool {
    if (runtime_filter != nullptr && runtime_filter->enabled()) {
      const slot_t col = runtime_filter->column;
      if (col < arena.stride) {
        const bool is_null = arena.IsNull(r, col);
        const ValueType col_type = arena.slot_types[col];
        const int64_t val = is_null ? 0 : arena.cells[(r * arena.stride) + col];
        if (runtime_filter->Reject(static_cast<int8_t>(col_type), is_null,
                                   val)) {
          ++(*key_rej);
          if (is_null) {
            ++(*key_null_rej);
          }
          return false;
        }
      }
    }
    *all_simple_decided = true;
    if (filter != nullptr) {
      for (const SimpleComparePredicate& pred : filter->simple) {
        const std::optional<bool> verdict =
            TryMatchSimpleCompareArena(arena, r, pred);
        if (!verdict.has_value()) {
          *all_simple_decided = false;
        } else if (!*verdict) {
          return false;
        }
      }
    }
    return true;
  };

  if (workers > 1) {
    std::vector<Relation> shards(workers);
    std::vector<std::vector<Row>> deferred(workers);
    std::atomic<size_t> key_rejected{0};
    std::atomic<size_t> key_null_rejected{0};
    std::mutex error_mu;
    std::exception_ptr error_ex;
    const size_t chunk_rows =
        std::max<size_t>(1, (arena.rows + (workers * 4) - 1) / (workers * 4));
    {
      std::vector<std::jthread> threads;
      threads.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        try {
          threads.emplace_back([&, w] {
            try {
              auto& local = shards[w];
              size_t local_key_rej = 0;
              size_t local_key_null_rej = 0;
              for (size_t chunk = w; chunk < workers * 4; chunk += workers) {
                const size_t begin = chunk * chunk_rows;
                if (begin >= arena.rows) {
                  break;
                }
                const size_t end = std::min(arena.rows, begin + chunk_rows);
                for (size_t r = begin; r < end; ++r) {
                  bool all_simple_decided = true;
                  if (!row_passes_arena_precheck(r, &local_key_rej,
                                                 &local_key_null_rej,
                                                 &all_simple_decided)) {
                    continue;
                  }
                  Row row;
                  MaterializeSingleArenaRow(arena, cached_schema, r, &row);
                  if (!has_non_simple && all_simple_decided) {
                    local.AddRow(std::move(row));
                    continue;
                  }
                  if (filter != nullptr && filter_view != nullptr &&
                      programs.complete) {
                    switch (MatchScanFilterWorker(row, *filter_view, *filter,
                                                  programs)) {
                      case 0:
                        break;
                      case 1:
                        local.AddRow(std::move(row));
                        break;
                      default:
                        deferred[w].push_back(std::move(row));
                        break;
                    }
                  } else {
                    deferred[w].push_back(std::move(row));
                  }
                }
              }
              key_rejected.fetch_add(local_key_rej, std::memory_order_relaxed);
              key_null_rejected.fetch_add(local_key_null_rej,
                                          std::memory_order_relaxed);
            } catch (...) {
              std::scoped_lock lock(error_mu);
              if (!error_ex) {
                error_ex = std::current_exception();
              }
            }
          });
        } catch (...) {
          std::scoped_lock lock(error_mu);
          if (!error_ex) {
            error_ex = std::current_exception();
          }
          break;
        }
      }
    }
    if (error_ex) {
      if (error != nullptr && error->ok()) {
        try {
          std::rethrow_exception(error_ex);
        } catch (const std::exception& ex) {
          *error = StatusError(StatusCode::kRuntimeError, ex.what());
        } catch (...) {
          *error = StatusError(StatusCode::kRuntimeError,
                               "parallel arena filter worker failed");
        }
      }
      return true;
    }
    const auto record_error = [&](Status status) {
      if (error != nullptr && error->ok() && status != Status::kSuccess) {
        *error = std::move(status);
      }
    };
    for (size_t w = 0; w < workers; ++w) {
      for (Row& row : deferred[w]) {
        Status deferred_error{Status::kSuccess};
        if (filter == nullptr ||
            MatchScanFilter(row, *filter_view, *filter, outer, context, ctes,
                            &deferred_error)) {
          result->AddRow(std::move(row));
        }
        record_error(std::move(deferred_error));
      }
      deferred[w].clear();
      record_error(shards[w].FinishSpill());
      if (shards[w].HasSpill()) {
        record_error(
            shards[w].ForEachRow([&](const Row& row) { result->AddRow(row); }));
      } else {
        for (Row& row : shards[w].rows) {
          result->AddRow(std::move(row));
        }
      }
      record_error(shards[w].GetStatus());
      shards[w].ResetContents();
    }
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->key_filter_rejected += key_rejected.load();
      context.execution_runtime()->key_filter_null_rejected +=
          key_null_rejected.load();
    }
    return true;
  }

  size_t key_rej = 0;
  size_t key_null_rej = 0;
  for (size_t r = 0; r < arena.rows; ++r) {
    if (error != nullptr && !error->ok()) {
      break;
    }
    bool all_simple_decided = true;
    if (!row_passes_arena_precheck(r, &key_rej, &key_null_rej,
                                   &all_simple_decided)) {
      continue;
    }
    Row row;
    MaterializeSingleArenaRow(arena, cached_schema, r, &row);
    if (!has_non_simple && all_simple_decided) {
      result->AddRow(std::move(row));
      continue;
    }
    if (filter == nullptr || MatchScanFilter(row, *filter_view, *filter, outer,
                                             context, ctes, error)) {
      result->AddRow(std::move(row));
    }
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rej;
    context.execution_runtime()->key_filter_null_rejected += key_null_rej;
  }
  return true;
}
}  // namespace

StatusOr<Relation> LoadSource(TransactionContext& context,
                              const SelectSource& source, const Scope* outer,
                              const CteMap& ctes,
                              const std::vector<slot_t>* projection,
                              const std::vector<Expression>* scan_predicates,
                              const ScanRuntimeFilter* int_runtime_filter) {
  const std::vector<Expression>* saved_scan_predicates = scan_predicates;
  const ScanRuntimeFilter* saved_int_runtime_filter = int_runtime_filter;
  if (source.sample) {
    scan_predicates = nullptr;
    int_runtime_filter = nullptr;
  }
  // Fold row-independent subqueries to constants before filter compilation:
  // the folded compares join the typed SimpleCompare path, and query-free
  // predicates skip the contextful per-row filter and the cached-relation
  // snapshot a residual query would otherwise force.
  std::vector<Expression> folded_predicates;
  if (scan_predicates != nullptr && !scan_predicates->empty()) {
    folded_predicates =
        FoldRowIndependentSubqueries(context, *scan_predicates, outer, ctes);
    if (!folded_predicates.empty()) {
      scan_predicates = &folded_predicates;
    }
  }
  Relation result(context.execution_runtime());
  if (source.unnest) {
    ASSIGN_OR_RETURN(Value, array_val,
                     (TryEvaluate(source.unnest, Scope{.outer = outer}, nullptr,
                                  context, ctes)));
    result = UnnestValueToRelation(source, array_val);
  } else if (source.query) {
    ASSIGN_OR_RETURN(Relation, sub,
                     (ExecuteQuery(context, *source.query, outer, ctes)));
    result = std::move(sub);
    // Derived outputs carry bare (or inner-qualified) column names, while
    // outer predicates reference them through this source's alias
    // (QualifyExpression). Without alias qualification, LocalColumnOffset
    // cannot bind `alias.col`, join equalities go unresolved, and the join
    // degrades to a cross product plus filter (TPC-H Q20 at SF1: 800K x
    // derived rows instead of a hash join). Mirror the estimate path
    // (MakeScanNode), which already qualifies derived schemas.
    if (!source.alias.empty()) {
      result.schema = QualifySchema(result.schema, source.alias);
    }
  } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
    ASSIGN_OR_RETURN(Relation, materialized,
                     (MaterializeRelation(*cte->second)));
    result = std::move(materialized);
  } else {
    bool reusable =
        context.execution_runtime() != nullptr &&
        context.execution_runtime()->reusable_base_relations.contains(
            source.table);
    const bool reusable_table = reusable;
    const std::string cache_key =
        BaseRelationCacheKey(source.table, projection);
    // Predicates attached to this source are written against the source's
    // alias, but cached/raw table schemas carry the table name (or no
    // qualifier at all).  Evaluate every scan filter against an alias-qualified
    // view of the schema; row layouts are positional so only names change.
    const std::string load_qualifier =
        source.alias.empty() ? source.table : source.alias;
    auto filter_view_of = [&load_qualifier](const Schema& schema) {
      return load_qualifier.empty() ? schema
                                    : QualifySchema(schema, load_qualifier);
    };
    RelationPtr cached_entry;
    if (reusable) {
      const auto cached =
          context.execution_runtime()->base_relations.find(cache_key);
      if (cached != context.execution_runtime()->base_relations.end()) {
        cached_entry = cached->second;
      }
      StatusOr<std::shared_ptr<Table>> table_chk =
          context.GetTable(source.table);
      const bool large_lstable =
          table_chk.HasValue() &&
          table_chk.Value()->GetLSTableStore() != nullptr &&
          table_chk.Value()->GetLSTableStore()->total_rows > 16384;
      if (cached_entry && cached_entry->rows.empty() &&
          !cached_entry->HasSpill() && large_lstable) {
        const auto arena_it =
            context.execution_runtime()->base_relation_arenas.find(cache_key);
        const bool has_arena =
            arena_it !=
                context.execution_runtime()->base_relation_arenas.end() &&
            arena_it->second != nullptr && arena_it->second->rows > 0;
        if (!has_arena) {
          ++context.execution_runtime()->base_scan_cache_hits;
          cached_entry.reset();
          reusable = false;
        }
      } else if (!cached_entry && large_lstable) {
        auto marker = std::make_shared<Relation>(context.execution_runtime());
        context.execution_runtime()->base_relations.emplace(cache_key,
                                                            std::move(marker));
        reusable = false;
      } else if (!cached_entry && int_runtime_filter != nullptr) {
        reusable = false;
      }
    }
    const bool filter_during_scan =
        !reusable && scan_predicates != nullptr && !scan_predicates->empty();
    if (cached_entry) {
      // Materialize only rows that survive this alias's local predicates so
      // we never deep-copy a multi-million-row cache and filter afterwards.
      Relation& cached_relation = *cached_entry;
      result.schema = cached_relation.schema;
      RETURN_IF_FAIL(cached_relation.FinishSpill());
      RETURN_IF_FAIL(cached_relation.GetStatus());
      const BaseRelationArena* unboxed_arena = nullptr;
      if (cached_relation.rows.empty() && !cached_relation.HasSpill() &&
          context.execution_runtime() != nullptr) {
        const auto arena_it =
            context.execution_runtime()->base_relation_arenas.find(cache_key);
        if (arena_it !=
                context.execution_runtime()->base_relation_arenas.end() &&
            arena_it->second != nullptr && arena_it->second->rows > 0 &&
            arena_it->second->published_rowsless &&
            !arena_it->second->rows_boxed) {
          unboxed_arena = arena_it->second.get();
        }
      }
      if (scan_predicates == nullptr || scan_predicates->empty()) {
        Status key_error{Status::kSuccess};
        if (unboxed_arena != nullptr &&
            (int_runtime_filter != nullptr && int_runtime_filter->enabled()) &&
            TryFilterArenaCache(context, *unboxed_arena, cached_relation.schema,
                                nullptr, nullptr, outer, ctes,
                                int_runtime_filter, &result, &key_error)) {
          RETURN_IF_FAIL(key_error);
        } else {
          if (unboxed_arena != nullptr && unboxed_arena->rows >= 32768) {
            MaterializeCacheRowsParallel(cache_key, &cached_relation,
                                         context.execution_runtime());
          } else {
            MaterializeCacheRowsIfNeeded(cache_key, &cached_relation,
                                         context.execution_runtime());
          }
          const CompiledScanFilter pass_through;
          if (TryParallelFilterRows(context, cached_relation, pass_through,
                                    cached_relation.schema, outer, ctes,
                                    int_runtime_filter, &result, &key_error)) {
            RETURN_IF_FAIL(key_error);
          } else {
            Status iteration = cached_relation.ForEachRow([&](const Row& row) {
              if (int_runtime_filter != nullptr &&
                  int_runtime_filter->enabled()) {
                const Value& key = row[int_runtime_filter->column];
                if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                               key.IsNull(),
                                               key.value.int_value)) {
                  if (context.execution_runtime() != nullptr) {
                    ++context.execution_runtime()->key_filter_rejected;
                    if (key.IsNull()) {
                      ++context.execution_runtime()->key_filter_null_rejected;
                    }
                  }
                  return;
                }
                for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    continue;
                  }
                  const Value& ek = row[ext.column];
                  if (ext.Reject(static_cast<int8_t>(ek.type), ek.IsNull(),
                                 ek.value.int_value)) {
                    if (context.execution_runtime() != nullptr) {
                      ++context.execution_runtime()->key_filter_rejected;
                    }
                    return;
                  }
                }
              }
              result.AddRow(row);
            });
            RETURN_IF_FAIL(iteration);
          }
        }
      } else {
        const auto filter_begin = std::chrono::steady_clock::now();
        const std::vector<Expression> folded_scan_preds =
            FoldRowIndependentSubqueries(context, *scan_predicates, outer,
                                         ctes);
        if (unboxed_arena != nullptr && unboxed_arena->rows_boxed) {
          unboxed_arena = nullptr;
        }
        const std::vector<Expression>& effective_scan_preds =
            folded_scan_preds.empty() ? *scan_predicates : folded_scan_preds;
        const Schema filter_view = filter_view_of(cached_relation.schema);
        const FilterCompileEnv compile_env{
            .context = &context,
            .outer = outer,
            .ctes = &ctes,
            .input_rows = unboxed_arena != nullptr
                              ? unboxed_arena->rows
                              : cached_relation.TotalRows()};
        const CompiledScanFilter scan_filter =
            CompileScanFilter(effective_scan_preds, filter_view, &compile_env);
        Status eval_error{Status::kSuccess};
        const bool needs_snapshot = std::ranges::any_of(
            *scan_predicates, [&](const Expression& predicate) {
              return ContainsReentrantQueryIn(predicate, source.table);
            });
        if (!needs_snapshot && unboxed_arena != nullptr &&
            TryFilterArenaCache(context, *unboxed_arena, cached_relation.schema,
                                &scan_filter, &filter_view, outer, ctes,
                                int_runtime_filter, &result, &eval_error)) {
          RETURN_IF_FAIL(eval_error);
        } else {
          if (unboxed_arena != nullptr && unboxed_arena->rows >= 32768) {
            MaterializeCacheRowsParallel(cache_key, &cached_relation,
                                         context.execution_runtime());
          } else {
            MaterializeCacheRowsIfNeeded(cache_key, &cached_relation,
                                         context.execution_runtime());
          }
          auto emit_filtered = [&](const Relation& source_rel) -> Status {
            if (TryParallelFilterRows(
                    context, source_rel, scan_filter, filter_view, outer, ctes,
                    int_runtime_filter, &result, &eval_error)) {
              return eval_error;
            }
            Status iteration = source_rel.ForEachRow([&](const Row& row) {
              if (!eval_error.ok()) {
                return;
              }
              if (int_runtime_filter != nullptr &&
                  int_runtime_filter->enabled()) {
                const Value& key = row[int_runtime_filter->column];
                if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                               key.IsNull(),
                                               key.value.int_value)) {
                  if (context.execution_runtime() != nullptr) {
                    ++context.execution_runtime()->key_filter_rejected;
                    if (key.IsNull()) {
                      ++context.execution_runtime()->key_filter_null_rejected;
                    }
                  }
                  return;
                }
                for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    continue;
                  }
                  const Value& ek = row[ext.column];
                  if (ext.Reject(static_cast<int8_t>(ek.type), ek.IsNull(),
                                 ek.value.int_value)) {
                    if (context.execution_runtime() != nullptr) {
                      ++context.execution_runtime()->key_filter_rejected;
                    }
                    return;
                  }
                }
              }
              if (MatchScanFilter(row, filter_view, scan_filter, outer, context,
                                  ctes, &eval_error)) {
                result.AddRow(row);
              }
            });
            if (iteration != Status::kSuccess) {
              return iteration;
            }
            return source_rel.GetStatus();
          };
          if (needs_snapshot) {
            ASSIGN_OR_RETURN(Relation, snapshot,
                             MaterializeRelation(cached_relation));
            RETURN_IF_FAIL(snapshot.FinishSpill());
            RETURN_IF_FAIL(snapshot.GetStatus());
            RETURN_IF_FAIL(emit_filtered(snapshot));
          } else {
            RETURN_IF_FAIL(emit_filtered(cached_relation));
          }
          RETURN_IF_FAIL(eval_error);
        }
        context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
      }
      ++context.execution_runtime()->base_scan_cache_hits;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "table " + source.table + " not found");
      }
      const Schema& table_schema = table.Value()->GetSchema();
      result.schema = projection != nullptr
                          ? ProjectSchema(table_schema, *projection)
                          : table_schema;
      size_t table_input_rows = 0;
      if (!context.txn_.HasAnyPageVersionChains()) {
        std::shared_ptr<const LSTableStore> lst_store = GetOrBuildLSTableStore(
            context.txn_, *table.Value(), true, projection);
        if (lst_store != nullptr && lst_store->uncommitted_owner_txn == 0 &&
            lst_store->layout_generation == table.Value()->LayoutGeneration() &&
            lst_store->first_pid == table.Value()->FirstPageId()) {
          table_input_rows = lst_store->total_rows;
          bool any_promoted = false;
          std::vector<Column> cols;
          cols.reserve(result.schema.ColumnCount());
          for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
            Column col = result.schema.GetColumn(ci);
            const slot_t full_c = projection != nullptr
                                      ? (*projection)[ci]
                                      : static_cast<slot_t>(ci);
            if (col.GetConstraint().ctype == Constraint::kNothing &&
                full_c < lst_store->column_is_unique_nonnull.size() &&
                lst_store->column_is_unique_nonnull[full_c] != 0) {
              col = Column(col.Name(), col.Type(),
                           Constraint(Constraint::kPrimaryKey));
              any_promoted = true;
            }
            cols.push_back(std::move(col));
          }
          if (any_promoted) {
            result.schema = Schema(result.schema.Name(), std::move(cols));
          }
        }
      }
      if (table_input_rows == 0) {
        if (auto st = context.GetStats(source.table); st.HasValue()) {
          table_input_rows = st.Value()->Rows();
        }
      }
      const Schema filter_view = filter_view_of(result.schema);
      CompiledScanFilter scan_filter;
      if (filter_during_scan) {
        const std::vector<Expression> folded_scan_preds =
            FoldRowIndependentSubqueries(context, *scan_predicates, outer,
                                         ctes);
        const std::vector<Expression>& effective_scan_preds =
            folded_scan_preds.empty() ? *scan_predicates : folded_scan_preds;
        const FilterCompileEnv compile_env{.context = &context,
                                           .outer = outer,
                                           .ctes = &ctes,
                                           .input_rows = table_input_rows};
        scan_filter =
            CompileScanFilter(effective_scan_preds, filter_view, &compile_env);
      }
      const auto scan_begin = std::chrono::steady_clock::now();
      const auto filter_begin = scan_begin;
      // Prefer skipping full-row decode when a dynamic join filter is active.
      std::optional<slot_t> full_key_column;
      if (int_runtime_filter != nullptr && int_runtime_filter->enabled()) {
        if (projection != nullptr) {
          if (int_runtime_filter->column < projection->size()) {
            full_key_column = (*projection)[int_runtime_filter->column];
          }
        } else {
          full_key_column = int_runtime_filter->column;
        }
      }
      std::string keyed_cache_key;
      if (context.execution_runtime() != nullptr && reusable_table &&
          !context.txn_.HasAnyPageVersionChains() &&
          (scan_predicates == nullptr || scan_predicates->empty()) &&
          full_key_column.has_value() && int_runtime_filter != nullptr &&
          int_runtime_filter->extra.empty() &&
          int_runtime_filter->group != nullptr &&
          int_runtime_filter->group->ExactKeys() != nullptr &&
          int_runtime_filter->group->ExactKeys()->size() <= 8192) {
        const RuntimeFilterGroup& fg = *int_runtime_filter->group;
        const std::vector<int64_t>& exact_keys = *fg.ExactKeys();
        uint64_t h_sum = 0;
        uint64_t h_xor = 0;
        for (int64_t k : exact_keys) {
          auto x = static_cast<uint64_t>(k) + 0x9e3779b97f4a7c15ULL;
          x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
          x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
          const uint64_t m = x ^ (x >> 31);
          h_sum += m;
          h_xor ^= m;
        }
        keyed_cache_key =
            "__keyed__:" + source.table + ":" +
            std::to_string(*full_key_column) + ":" +
            std::to_string(static_cast<int>(int_runtime_filter->type)) + ":" +
            std::to_string(exact_keys.size()) + ":" + std::to_string(fg.Lo()) +
            ":" + std::to_string(fg.Hi()) + ":" + std::to_string(h_sum) + ":" +
            std::to_string(h_xor);
        const auto k_it =
            context.execution_runtime()->base_relations.find(keyed_cache_key);
        if (k_it != context.execution_runtime()->base_relations.end() &&
            k_it->second != nullptr && !k_it->second->HasSpill()) {
          const Relation& cached_k = *k_it->second;
          std::vector<slot_t> col_map;
          col_map.reserve(result.schema.ColumnCount());
          bool all_found = true;
          for (size_t ci = 0; ci < result.schema.ColumnCount(); ++ci) {
            const Column& wcol = result.schema.GetColumn(ci);
            std::optional<slot_t> found_ci;
            for (size_t cci = 0; cci < cached_k.schema.ColumnCount(); ++cci) {
              const Column& ccol = cached_k.schema.GetColumn(cci);
              if (ccol.Name().name == wcol.Name().name &&
                  ccol.Type() == wcol.Type()) {
                found_ci = static_cast<slot_t>(cci);
                break;
              }
            }
            if (!found_ci.has_value()) {
              all_found = false;
              break;
            }
            col_map.push_back(*found_ci);
          }
          if (all_found) {
            result.rows.reserve(cached_k.rows.size());
            const size_t ncols = col_map.size();
            for (const Row& src_row : cached_k.rows) {
              Row dst_row;
              dst_row.values_.resize(ncols);
              for (size_t ci = 0; ci < ncols; ++ci) {
                dst_row.values_[ci] = src_row[col_map[ci]];
                if (result.schema.GetColumn(ci).IsUnsigned() &&
                    dst_row.values_[ci].type == ValueType::kInt64) {
                  dst_row.values_[ci].SetUnsigned();
                }
              }
              result.AddRow(std::move(dst_row));
            }
            ++context.execution_runtime()->base_scan_cache_hits;
            if (!source.alias.empty()) {
              result.schema = QualifySchema(result.schema, source.alias);
            } else if (!source.table.empty()) {
              result.schema = QualifySchema(result.schema, source.table);
            }
            return result;
          }
        }
      }
      // The low-level iterator rejects key-filter misses before row decode;
      // the parallel path applies the same rejection inside workers (whose
      // shard counters keep scan accounting; the per-candidate
      // rejected/null-rejected split is recorded shard-side).
      const bool iterator_handles_key_filter = full_key_column.has_value();
      const RuntimeFilterGroup* filter_group =
          iterator_handles_key_filter ? int_runtime_filter->group.get()
                                      : nullptr;
      const ValueType filter_type = iterator_handles_key_filter
                                        ? int_runtime_filter->type
                                        : ValueType::kNull;
      Status eval_error{Status::kSuccess};
      static const bool keyed_parallel_disabled =
          std::getenv("TINYLAMB_DISABLE_PARALLEL_KEYED_SCAN") != nullptr;
      bool indexed_ok = false;
      if (!reusable && (filter_during_scan || int_runtime_filter != nullptr)) {
        auto statistics = context.GetStats(source.table);
        if (statistics.HasValue()) {
          ASSIGN_OR_RETURN(bool, used_index,
                           TrySelectiveIndexSource(
                               context, *table.Value(), *statistics.Value(),
                               filter_view, projection, scan_filter,
                               int_runtime_filter, outer, ctes, &result));
          indexed_ok = used_index;
        }
      }
      bool effective_filter_during_scan = filter_during_scan;
      if (int_runtime_filter != nullptr && !int_runtime_filter->extra.empty()) {
        if (indexed_ok) {
          const auto keep_row = [&](const Row& row) {
            return std::ranges::none_of(
                int_runtime_filter->extra, [&](const ScanRuntimeFilter& ext) {
                  if (!ext.enabled() || ext.column >= row.Count()) {
                    return false;
                  }
                  const Value& key = row[ext.column];
                  return ext.Reject(static_cast<int8_t>(key.type), key.IsNull(),
                                    key.value.int_value);
                });
          };
          if (result.HasSpill()) {
            RETURN_IF_FAIL(result.FinishSpill());
            Relation filtered(context.execution_runtime());
            filtered.schema = result.schema;
            RETURN_IF_FAIL(result.ForEachRow([&](const Row& row) {
              if (keep_row(row)) filtered.AddRow(row);
            }));
            RETURN_IF_FAIL(filtered.FinishSpill());
            CopyExecutionStats(&filtered, result);
            result = std::move(filtered);
          } else {
            size_t out = 0;
            for (size_t i = 0; i < result.rows.size(); ++i) {
              if (keep_row(result.rows[i])) {
                if (out != i) {
                  result.rows[out] = std::move(result.rows[i]);
                }
                ++out;
              }
            }
            result.rows.resize(out);
          }
        } else {
          for (const ScanRuntimeFilter& ext : int_runtime_filter->extra) {
            if (!ext.enabled()) {
              continue;
            }
            PrebuiltMembershipPredicate pm;
            pm.column = ext.column;
            pm.negated = false;
            pm.domain_type = ext.type;
            pm.runtime_filter = ext.group;
            pm.existence = true;
            scan_filter.prebuilt_membership.push_back(std::move(pm));
            effective_filter_during_scan = true;
          }
        }
      }
      const bool parallel_ok =
          indexed_ok ||
          ((filter_group == nullptr || !keyed_parallel_disabled) &&
           TryParallelTableScan(
               context, *table.Value(), projection, filter_group, filter_type,
               full_key_column, effective_filter_during_scan,
               effective_filter_during_scan ? &scan_filter : nullptr,
               filter_view, outer, ctes, &result, &eval_error));
      RETURN_IF_FAIL(eval_error);
      if (!parallel_ok) {
        // The canonical row predicate still decides every survivor. Signed
        // INT64/DATE peeks reject known misses before allocating and decoding
        // projected Rows, using the same schema/type guard as typed scans.
        const auto integer_peeks =
            effective_filter_during_scan
                ? BuildIntegerPeeks(scan_filter, projection, table_schema)
                : std::vector<IntegerPeekCompare>{};
        TableScanOptions options;
        if (projection != nullptr) {
          options.projection = *projection;
        }
        options.peek_compares =
            integer_peeks.empty() ? nullptr : &integer_peeks;
        if (iterator_handles_key_filter) {
          options.key_column = full_key_column;
          options.runtime_filter = filter_group;
          options.runtime_filter_type = filter_type;
        }
        Iterator iterator = table.Value()->BeginFullScan(context.txn_, options);
        while (iterator.IsValid()) {
          if (context.execution_runtime() != nullptr) {
            ++context.execution_runtime()->scan_rows;
            context.execution_runtime()->scan_values_available +=
                table_schema.ColumnCount();
            context.execution_runtime()->scan_values_decoded +=
                result.schema.ColumnCount();
          }
          bool matches = true;
          if (!iterator_handles_key_filter && int_runtime_filter != nullptr &&
              int_runtime_filter->enabled()) {
            const Value& key = (*iterator)[int_runtime_filter->column];
            if (int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                           key.IsNull(), key.value.int_value)) {
              matches = false;
              if (context.execution_runtime() != nullptr) {
                ++context.execution_runtime()->key_filter_rejected;
                if (key.IsNull()) {
                  ++context.execution_runtime()->key_filter_null_rejected;
                }
              }
            }
          }
          if (matches && effective_filter_during_scan) {
            matches = MatchScanFilter(*iterator, filter_view, scan_filter,
                                      outer, context, ctes, &eval_error);
            RETURN_IF_FAIL(eval_error);
          }
          if (matches) {
            // Move the iterator's row in: all reads of the row happened
            // above, and ++ re-deserializes into a fresh row anyway.  A
            // copy here materialized every scanned row twice.
            result.AddRow(std::move(*iterator));
            if (context.execution_runtime() != nullptr) {
              ++context.execution_runtime()->scan_output_rows;
            }
          }
          ++iterator;
        }
        RETURN_IF_FAIL(iterator.GetStatus());
        RETURN_IF_FAIL(eval_error);
      }
      RETURN_IF_FAIL(result.FinishSpill());
      RETURN_IF_FAIL(result.GetStatus());
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->scan_ms += ElapsedMs(scan_begin);
        if (filter_during_scan) {
          context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
        }
        if (reusable && int_runtime_filter == nullptr) {
          auto cached = std::make_shared<Relation>(std::move(result));
          context.execution_runtime()->base_relations.emplace(cache_key,
                                                              cached);
          // The contents moved into the cache above; start from a fresh
          // relation before refilling so we never touch a moved-from object.
          result = Relation{};
          result.schema = cached->schema;
          ASSIGN_OR_RETURN(Relation, cached_copy, MaterializeRelation(*cached));
          result = std::move(cached_copy);
        } else if (!keyed_cache_key.empty() && !result.HasSpill() &&
                   result.rows.size() <= 65536) {
          auto& slot =
              context.execution_runtime()->base_relations[keyed_cache_key];
          if (slot == nullptr ||
              slot->schema.ColumnCount() < result.schema.ColumnCount()) {
            auto cached =
                std::make_shared<Relation>(context.execution_runtime());
            cached->schema = result.schema;
            cached->rows = result.rows;
            slot = std::move(cached);
          }
        }
      }
      if (reusable && scan_predicates != nullptr && !scan_predicates->empty()) {
        // Evaluate this alias's predicates against the alias-qualified view;
        // the stored schema stays neutral for cache sharing across aliases.
        const Schema saved_schema = result.schema;
        const Schema view = filter_view_of(saved_schema);
        result.schema = view;
        const FilterCompileEnv compile_env{
            .context = &context, .outer = outer, .ctes = &ctes};
        const CompiledScanFilter fill_filter =
            CompileScanFilter(*scan_predicates, view, &compile_env);
        Relation filtered(context.execution_runtime());
        filtered.schema = saved_schema;
        Status filter_error{Status::kSuccess};
        if (TryParallelFilterRows(context, result, fill_filter, view, outer,
                                  ctes, nullptr, &filtered, &filter_error,
                                  &result)) {
          RETURN_IF_FAIL(filter_error);
          RETURN_IF_FAIL(filtered.FinishSpill());
          RETURN_IF_FAIL(filtered.GetStatus());
          result = std::move(filtered);
        } else {
          RETURN_IF_FAIL(
              FilterRelation(context, &result, *scan_predicates, outer, ctes));
        }
        result.schema = saved_schema;
      }
    }
  }
  // Values are stored as INT64 bit patterns, while the catalog retains the
  // SQL UINT64 declaration on the column.  Reattach that declaration to the
  // row values before expressions (notably `id < 0`) evaluate them.
  bool has_unsigned_column = false;
  for (size_t i = 0; i < result.schema.ColumnCount(); ++i) {
    has_unsigned_column =
        has_unsigned_column || result.schema.GetColumn(i).IsUnsigned();
  }
  if (has_unsigned_column) {
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.GetStatus());
    Relation normalized(context.execution_runtime());
    normalized.schema = result.schema;
    RETURN_IF_FAIL(result.ForEachRow([&](const Row& input) {
      Row row = input;
      for (size_t i = 0; i < row.Count() && i < normalized.schema.ColumnCount();
           ++i) {
        if (normalized.schema.GetColumn(i).IsUnsigned() &&
            row[i].type == ValueType::kInt64 && !row[i].IsUnsigned()) {
          row[i] = row[i].WithUnsigned();
        }
      }
      normalized.AddRow(std::move(row));
    }));
    RETURN_IF_FAIL(normalized.FinishSpill());
    RETURN_IF_FAIL(normalized.GetStatus());
    CopyExecutionStats(&normalized, result);
    result = std::move(normalized);
  }
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  // Unnest outputs carry the alias as qualifier too: bare element refs still
  // match by column name, while alias-qualified and alias-as-row references
  // (`s.field`, `SELECT s`) resolve uniformly with table sources.
  if (!qualifier.empty()) {
    result.schema = QualifySchema(result.schema, qualifier);
  }
  if (source.sample) {
    ASSIGN_OR_RETURN(Relation, sampled,
                     (ApplyTableSample(context, source, std::move(result))));
    result = std::move(sampled);
    if (saved_scan_predicates != nullptr && !saved_scan_predicates->empty()) {
      RETURN_IF_FAIL(FilterRelation(context, &result, *saved_scan_predicates,
                                    outer, ctes));
    }
    if (saved_int_runtime_filter != nullptr &&
        saved_int_runtime_filter->enabled()) {
      const slot_t filter_column = saved_int_runtime_filter->column;
      if (filter_column >= result.schema.ColumnCount()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "TABLESAMPLE key filter column is out of range");
      }
      Relation filtered(context.execution_runtime());
      filtered.schema = result.schema;
      CopyExecutionStats(&filtered, result);
      RETURN_IF_FAIL(result.FinishSpill());
      RETURN_IF_FAIL(result.GetStatus());
      RETURN_IF_FAIL(result.ForEachRow([&](const Row& row) {
        const Value& key = row[filter_column];
        if (!saved_int_runtime_filter->Reject(static_cast<int8_t>(key.type),
                                              key.IsNull(),
                                              key.value.int_value)) {
          filtered.AddRow(row);
        } else if (context.execution_runtime() != nullptr) {
          ++context.execution_runtime()->key_filter_rejected;
          if (key.IsNull()) {
            ++context.execution_runtime()->key_filter_null_rejected;
          }
        }
      }));
      RETURN_IF_FAIL(filtered.FinishSpill());
      RETURN_IF_FAIL(filtered.GetStatus());
      result = std::move(filtered);
    }
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  return result;
}

bool ContainsQuery(const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ContainsQuery(child);
      });
}

namespace {

// Replaces every row-independent subquery in `expression` with the constant
// value one probe of that subquery returns.  A kQueryExp node folds when all
// queries beneath it are uncorrelated (no outer-scope reference) and carry no
// volatile function: the value is then identical for every row of this
// statement's snapshot -- the same contract the per-row caches
// (uncorrelated_scalar_by_node / uncorrelated_membership_by_node) rely on.
// A node whose evaluation raises keeps its original form, so errors (scalar
// subquery with more than one row, collation conflicts, ...) stay with the
// per-row path, including conjunct-tolerant suppression of sibling errors.
Expression FoldRowIndependentSubqueriesIn(  // NOLINT(misc-no-recursion)
    TransactionContext& context, const Expression& expression,
    const Scope* outer, const CteMap& ctes, bool* changed) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    ExecutionRuntime* runtime = context.execution_runtime();
    // Scalar (Test() == nullptr) and EXISTS probes are row-independent when
    // uncorrelated; quantified probes (`x IN (SELECT ...)` with a row-side
    // Test) and ARRAY builds keep their per-row path here.
    if (runtime != nullptr && query.Test() == nullptr && !query.ArrayResult() &&
        ContainsOnlyUncorrelatedQueries(context, expression, ctes) &&
        !StatementContainsVolatileFunctionCached(runtime, *query.Query())) {
      Scope probe{.outer = outer};
      StatusOr<Value> value =
          TryEvaluate(expression, probe, nullptr, context, ctes);
      if (value.HasValue()) {
        *changed = true;
        return ConstantValueExp(value.MoveValue());
      }
    }
    return expression;
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  bool child_changed = false;
  for (Expression& child : children) {
    Expression folded =
        FoldRowIndependentSubqueriesIn(context, child, outer, ctes, changed);
    if (folded.get() != child.get()) {
      child = std::move(folded);
      child_changed = true;
    }
  }
  if (!child_changed) {
    return expression;
  }
  return WithExpressionChildren(expression, std::move(children));
}

// True when `expression` contains a kQueryExp whose statement scans `table`
// (directly or through nested sources): iterating this table's cached
// relation while such a query re-enters LoadSource for the same cache_key
// would break the spill-file read position, so the caller must snapshot
// first.  Subqueries over other tables (uncorrelated membership or scalar
// probes kept because they carry a row-side Test) iterate their own sources
// and need no copy.
bool ContainsReentrantQueryIn(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::string_view table) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    std::unordered_map<std::string, size_t> counts;
    CountStatementTables(*query.Query(), &counts);
    if (counts.contains(std::string(table))) {
      return true;
    }
    return ContainsReentrantQueryIn(query.Test(), table);
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [&](const Expression& child) {  // NOLINT(misc-no-recursion)
        return ContainsReentrantQueryIn(child, table);
      });
}

}  // namespace

std::vector<Expression> FoldRowIndependentSubqueries(
    TransactionContext& context, const std::vector<Expression>& predicates,
    const Scope* outer, const CteMap& ctes) {
  static const bool fold_disabled =
      std::getenv("TINYLAMB_DISABLE_SUBQUERY_FOLD") != nullptr;
  if (fold_disabled || context.execution_runtime() == nullptr ||
      std::ranges::none_of(predicates, [](const Expression& predicate) {
        return ContainsQuery(predicate);
      })) {
    return predicates;
  }
  std::vector<Expression> folded;
  folded.reserve(predicates.size());
  bool changed = false;
  for (const Expression& predicate : predicates) {
    folded.push_back(FoldRowIndependentSubqueriesIn(context, predicate, outer,
                                                    ctes, &changed));
  }
  if (!changed) {
    return predicates;
  }
  return folded;
}

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name) {
  // GoogleSQL identifiers are case-insensitive.
  auto equals = [](std::string_view left, std::string_view right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
      const auto lhs = static_cast<unsigned char>(left[i]);
      const auto rhs = static_cast<unsigned char>(right[i]);
      if (lhs == rhs) {
        continue;
      }
      if (lhs >= 0x80 || rhs >= 0x80) {
        if (std::tolower(lhs) != std::tolower(rhs)) {
          return false;
        }
        continue;
      }
      const unsigned char folded_left =
          lhs >= 'A' && lhs <= 'Z' ? lhs + ('a' - 'A') : lhs;
      const unsigned char folded_right =
          rhs >= 'A' && rhs <= 'Z' ? rhs + ('a' - 'A') : rhs;
      if (folded_left != folded_right) {
        return false;
      }
    }
    return true;
  };
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!equals(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() && !equals(candidate.schema, name.schema)) {
      continue;
    }
    if (match) {
      return std::nullopt;
    }
    match = i;
  }
  if (!match && name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const ColumnName& candidate = schema.GetColumn(i).Name();
      if (equals(candidate.schema, name.name)) {
        if (match) {
          return std::nullopt;
        }
        match = i;
      }
    }
  }
  return match;
}
std::vector<Expression> SplitDisjuncts(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    std::vector<Expression> result =
        SplitDisjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitDisjuncts(expression->AsBinaryExpression().Right());
    result.insert(result.end(), right.begin(), right.end());
    return result;
  }
  return expression ? std::vector<Expression>{expression}
                    : std::vector<Expression>{};
}

Expression CombineDisjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) {
    return nullptr;
  }
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(result, BinaryOperation::kOr, expressions[i]);
  }
  return result;
}
Status FilterRelation(TransactionContext& context, Relation* relation,
                      const std::vector<Expression>& predicates,
                      const Scope* outer, const CteMap& ctes) {
  if (predicates.empty()) {
    return Status::kSuccess;
  }
  const std::vector<Expression> folded_predicates =
      FoldRowIndependentSubqueries(context, predicates, outer, ctes);
  const std::vector<Expression>& effective_predicates =
      folded_predicates.empty() ? predicates : folded_predicates;
  const auto filter_begin = std::chrono::steady_clock::now();
  const FilterCompileEnv compile_env{.context = &context,
                                     .outer = outer,
                                     .ctes = &ctes,
                                     .input_rows = relation->TotalRows()};
  const CompiledScanFilter scan_filter =
      CompileScanFilter(effective_predicates, relation->schema, &compile_env);
  RETURN_IF_FAIL(relation->FinishSpill());
  RETURN_IF_FAIL(relation->GetStatus());
  if (scan_filter.residual.empty() || scan_filter.residual_bytecode) {
    Relation filtered(context.execution_runtime());
    filtered.schema = relation->schema;
    CopyExecutionStats(&filtered, *relation);
    Status par_error{Status::kSuccess};
    if (TryParallelFilterRows(context, *relation, scan_filter, relation->schema,
                              outer, ctes, nullptr, &filtered, &par_error,
                              relation)) {
      RETURN_IF_FAIL(par_error);
      RETURN_IF_FAIL(filtered.FinishSpill());
      RETURN_IF_FAIL(filtered.GetStatus());
      *relation = std::move(filtered);
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
      }
      return Status::kSuccess;
    }
  } else if (!scan_filter.simple.empty() || !scan_filter.prefix_in.empty() ||
             !scan_filter.prebuilt_membership.empty() ||
             !scan_filter.prebuilt_scalar_compare.empty() ||
             !scan_filter.disjunctive_branches.empty()) {
    CompiledScanFilter non_residual = scan_filter;
    non_residual.residual.clear();
    Relation prefiltered(context.execution_runtime());
    prefiltered.schema = relation->schema;
    CopyExecutionStats(&prefiltered, *relation);
    Status pre_error{Status::kSuccess};
    if (!TryParallelFilterRows(context, *relation, non_residual,
                               relation->schema, outer, ctes, nullptr,
                               &prefiltered, &pre_error, relation)) {
      ColumnLookupCache pre_lookup;
      Status pre_iter = relation->ForEachRow([&](const Row& row) {
        if (!pre_error.ok()) {
          return;
        }
        if (MatchScanFilter(row, relation->schema, non_residual, outer, context,
                            ctes, &pre_error, &pre_lookup)) {
          prefiltered.AddRow(row);
        }
      });
      RETURN_IF_FAIL(pre_iter);
    }
    RETURN_IF_FAIL(pre_error);
    RETURN_IF_FAIL(prefiltered.FinishSpill());
    RETURN_IF_FAIL(prefiltered.GetStatus());
    *relation = std::move(prefiltered);
  }
  Relation filtered(context.execution_runtime());
  filtered.schema = relation->schema;
  CopyExecutionStats(&filtered, *relation);
  ColumnLookupCache lookup_cache;
  Status eval_error{Status::kSuccess};
  Status iteration;
  {
    // Register the relation being filtered so a correlated-subquery build
    // triggered by one of the predicates can prune itself to this
    // pipeline's outer key domain (the loop below presents every outer
    // value the probes will ever see).
    ScopedCorrelatedOuter correlated_outer(context, *relation);
    iteration = relation->ForEachRow([&](const Row& row) {
      if (!eval_error.ok()) {
        return;
      }
      if (MatchScanFilter(row, relation->schema, scan_filter, outer, context,
                          ctes, &eval_error, &lookup_cache)) {
        filtered.AddRow(row);
      }
    });
  }
  if (iteration != Status::kSuccess) {
    return iteration;
  }
  RETURN_IF_FAIL(eval_error);
  RETURN_IF_FAIL(filtered.FinishSpill());
  RETURN_IF_FAIL(filtered.GetStatus());
  *relation = std::move(filtered);
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->filter_ms += ElapsedMs(filter_begin);
  }
  return Status::kSuccess;
}

}  // namespace tinylamb::relational_detail
