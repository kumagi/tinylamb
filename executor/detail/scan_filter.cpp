/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/scan_filter.hpp"

#include <algorithm>
#include <array>
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
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exception_status.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
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

// Raw comparison dispatch used by the column-vs-column fast path, constant
// comparisons and columnar VARCHAR/DOUBLE checks. Higher-level matchers verify
// the type pair before reaching this function; do not call it with arbitrary
// mixed types assuming it is total.
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
    } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == '_') {
      // '_' matches one whole UTF-8 character, not one byte (same rule as
      // the AST evaluator's Like); truncated trailing sequences count as
      // that one character.
      value_pos += std::min(
          Utf8SequenceLength(static_cast<unsigned char>(value[value_pos])),
          value.size() - value_pos);
      ++pattern_pos;
    } else if (pattern_pos < pattern.size() &&
               pattern[pattern_pos] == value[value_pos]) {
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

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred,
                        Status* error) {
  // A failed exact comparison is not a clean FALSE. Record it for the caller
  // (MatchScanFilter applies the commutative WHERE rule) instead of folding
  // it into an ordinary reject that would hide a genuine evaluation error.
  const auto exact = [&](StatusOr<Value> cmp) {
    if (!cmp.HasValue()) {
      if (error != nullptr && error->ok()) {
        *error = cmp.GetStatus();
      }
      return false;
    }
    return cmp.Value().Truthy();
  };
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
        return exact(std::move(cmp));
      }
      return ComparePositional(pred.op, left.value.int_value,
                               right.value.int_value);
    }
    if (left.type == ValueType::kDouble && right.type == ValueType::kDouble) {
      // The canonical Value comparison folds all NaNs equal (matching group
      // keys / hashing); a raw IEEE `==` would reject NaN=NaN rows when this
      // predicate is pushed into the scan.
      StatusOr<Value> cmp = TryBinary(pred.op, left, right);
      return exact(std::move(cmp));
    }
    if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
      if (left.IsCaseInsensitive() || right.IsCaseInsensitive()) {
        StatusOr<Value> cmp = TryBinary(pred.op, left, right);
        return exact(std::move(cmp));
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
    return exact(std::move(cmp));
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
    return exact(std::move(cmp));
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
    return exact(std::move(cmp));
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
      // An unparseable date string is an evaluation error, not a clean
      // FALSE: route it through the exact evaluator so the status reaches
      // the caller like every other coerced comparison.
      return exact(TryBinary(pred.op, value, pred.constant));
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
    return exact(std::move(cmp));
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
  {
    const auto& head = predicate->AsBinaryExpression();
    if (!head.Left() || !head.Right()) {
      return std::nullopt;
    }
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
  if (!column || !constant) {
    return std::nullopt;
  }
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
  // An interval-tagged constant compared with a plain string column raises
  // "INTERVAL/STRING type mismatch" in the checked evaluator; a fast-path
  // match would swallow that error and silently reject every row. (LIKE
  // with an INTERVAL argument is FALSE everywhere in the canonical
  // evaluator, so the like arm needs no delegation.)
  if (!is_like && compiled.constant.IsInterval() &&
      target_col.Type() == ValueType::kVarChar) {
    return std::nullopt;
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
  // A type pair that reaches none of the payload branches above (arrays,
  // timestamps, intervals, BOOL, ...) has no comparison kernel. The matcher
  // would fall through to "no match" for every row, silently turning a TRUE
  // predicate into an empty result. Leave such expressions in the residual
  // evaluator, as the column-vs-column path already does.
  if (!compiled.int_payload && !compiled.double_payload &&
      !compiled.plain_string_payload && !compiled.date_string_payload &&
      !compiled.column_payload) {
    return std::nullopt;
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

// Keeps a subquery statement alive for the runtime's lifetime. Duplicate
// entries would extend the AST (and everything it caches) past the compile
// that needed it, so the whole retained list is the dedup window.
void RetainSubqueryStatement(
    ExecutionRuntime* runtime,
    const std::shared_ptr<const SelectStatement>& statement) {
  auto& retained = runtime->retained_statements;
  if (std::ranges::find(retained, statement) == retained.end()) {
    retained.push_back(statement);
  }
}

// Compiles `col [NOT] IN (uncorrelated subquery)` into one immutable value
// set built by the runtime's own uncorrelated-execution cache.  Soundness
// mirrors the per-row membership build in the expression evaluator: the
// statement executes once against this statement's snapshot, so the member
// set is row-invariant.  The peel is restricted to probe columns whose type
// tag and unsigned tag statically equal a uniform INT64/DATE build domain,
// which makes the int-value set verdict exact (the same trust the runtime
// hash grants a same-domain probe) and lets the matcher answer every input
// without the contextual evaluator.  Anything else keeps the residual path.
std::optional<PrebuiltMembershipPredicate> TryCompilePrebuiltMembership(
    const Expression& predicate, const Schema& schema,
    TransactionContext& context, const CteMap& ctes,
    bool allow_grouped_override = false) {
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
      (grouped_subquery && !allow_grouped_subquery &&
       !allow_grouped_override) ||
      statement.SelectList().size() != 1) {
    return mbail("sub shape");
  }
  ExecutionRuntime* runtime = context.execution_runtime();
  if (runtime == nullptr ||
      !ContainsOnlyUncorrelatedQueries(context, predicate, ctes)) {
    return mbail("correlated");
  }
  if (probe_type == ValueType::kVarChar) {
    RetainSubqueryStatement(runtime, query.Query());
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
            needed.reserve(1 + local_simple.size() * 2);
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
                    const bool flat_ready =
                        std::ranges::all_of(needed, [&](slot_t col) {
                          return col < store->integer_columns.size() &&
                                 store->integer_columns[col].size() ==
                                     store->total_rows &&
                                 col < store->column_has_null.size() &&
                                 !store->column_has_null[col];
                        });
                    std::vector<char> worker_saw_null(nworkers, 0);
                    {
                      std::vector<std::jthread> threads;
                      threads.reserve(nworkers);
                      for (size_t w = 0; w < nworkers; ++w) {
                        threads.emplace_back([&, w] {
                          if (flat_ready) {
                            const auto* keys =
                                store->integer_columns[col_idx].data();
                            const size_t begin =
                                store->total_rows * w / nworkers;
                            const size_t end =
                                store->total_rows * (w + 1) / nworkers;
                            const bool ordered_keys =
                                col_idx < store->column_is_monotonic.size() &&
                                store->column_is_monotonic[col_idx];
                            const auto scan = [&](auto&& matches) {
                              bool have_last = false;
                              int64_t last = 0;
                              if (ordered_keys) {
                                FlatIntSet::DenseWriter writer(*values);
                                for (size_t r = begin; r < end; ++r) {
                                  const int64_t key = keys[r];
                                  if (have_last && key == last) {
                                    continue;
                                  }
                                  if (!matches(r)) {
                                    continue;
                                  }
                                  writer.Insert(key);
                                  have_last = true;
                                  last = key;
                                }
                                return;
                              }
                              // T1231: unordered keys used to pay one
                              // InsertDenseAtomic RMW per row — the
                              // DenseWriter's word buffer cannot amortize a
                              // random word sequence.  Buffer this worker's
                              // slice instead, sort+unique it, then stream it
                              // through the writer: atomic traffic drops from
                              // one per row to one per 64 distinct keys that
                              // share a word.
                              std::vector<int64_t> keys_buf;
                              keys_buf.reserve(end - begin > (1U << 16)
                                                   ? (1U << 16)
                                                   : end - begin);
                              for (size_t r = begin; r < end; ++r) {
                                const int64_t key = keys[r];
                                if (have_last && key == last) {
                                  continue;
                                }
                                if (!matches(r)) {
                                  continue;
                                }
                                keys_buf.push_back(key);
                                have_last = true;
                                last = key;
                              }
                              std::ranges::sort(keys_buf);
                              keys_buf.erase(
                                  std::ranges::unique(keys_buf).begin(),
                                  keys_buf.end());
                              FlatIntSet::DenseWriter writer(*values);
                              for (const int64_t key : keys_buf) {
                                writer.Insert(key);
                              }
                            };
                            // Hoist the comparison dispatch out of the hot
                            // loop for a single column-to-column predicate.
                            if (local_simple.size() == 1 &&
                                local_simple[0].column_payload) {
                              const auto& compare = local_simple[0];
                              const auto* a =
                                  store->integer_columns[compare.column].data();
                              const auto* b =
                                  store->integer_columns[compare.right_column]
                                      .data();
                              switch (compare.op) {
                                case BinaryOperation::kEquals:
                                  scan([&](size_t r) { return a[r] == b[r]; });
                                  return;
                                case BinaryOperation::kNotEquals:
                                  scan([&](size_t r) { return a[r] != b[r]; });
                                  return;
                                case BinaryOperation::kLessThan:
                                  scan([&](size_t r) { return a[r] < b[r]; });
                                  return;
                                case BinaryOperation::kLessThanEquals:
                                  scan([&](size_t r) { return a[r] <= b[r]; });
                                  return;
                                case BinaryOperation::kGreaterThan:
                                  scan([&](size_t r) { return a[r] > b[r]; });
                                  return;
                                case BinaryOperation::kGreaterThanEquals:
                                  scan([&](size_t r) { return a[r] >= b[r]; });
                                  return;
                                default:
                                  break;
                              }
                            }
                            scan([&](size_t r) {
                              return std::ranges::all_of(
                                  local_simple, [&](const auto& compare) {
                                    const int64_t a =
                                        store->integer_columns[compare.column]
                                                              [r];
                                    const int64_t b =
                                        compare.column_payload
                                            ? store->integer_columns
                                                  [compare.right_column][r]
                                            : compare.int_constant;
                                    return ComparePositional(compare.op, a, b);
                                  });
                            });
                            return;
                          }
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
            Status morsel_status = Status::kSuccess;
            if (!parallel_membership_disabled && reserve_members >= 65536) {
              morsels = table.Value()->BuildScanMorsels(context.txn_, 8,
                                                        &morsel_status);
              if (morsel_status != Status::kSuccess) {
                overflow = true;
              } else if (morsels.size() >= 4) {
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
    RetainSubqueryStatement(runtime, query.Query());
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
  comparisons.reserve(predicates.size());
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
bool IsNonThrowingLocalFilter(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Schema& local_schema, bool* simple_ok) {
  if (!expr) {
    return false;
  }
  if (IsNonThrowingScanComparison(expr, local_schema)) {
    return true;
  }
  if (simple_ok != nullptr) {
    *simple_ok = false;
  }
  if (expr->Type() == TypeTag::kUnaryExp) {
    const auto& unary = expr->AsUnaryExpression();
    if ((unary.Op() == UnaryOperation::kIsNull ||
         unary.Op() == UnaryOperation::kIsNotNull) &&
        unary.Child() && unary.Child()->Type() == TypeTag::kColumnValue) {
      return LocalColumnOffset(local_schema,
                               unary.Child()->AsColumnValue().GetColumnName())
          .has_value();
    }
    return false;
  }
  if (expr->Type() == TypeTag::kInExp) {
    const auto& in = expr->AsInExpression();
    if (!in.child_ || in.child_->Type() != TypeTag::kColumnValue ||
        in.list_.empty()) {
      return false;
    }
    const auto offset = LocalColumnOffset(
        local_schema, in.child_->AsColumnValue().GetColumnName());
    if (!offset) {
      return false;
    }
    const ValueType col_type = local_schema.GetColumn(*offset).Type();
    if (col_type != ValueType::kInt64 && col_type != ValueType::kDate &&
        col_type != ValueType::kDouble && col_type != ValueType::kVarChar) {
      return false;
    }
    return std::ranges::all_of(in.list_, [&](const Expression& item) {
      if (!item || item->Type() != TypeTag::kConstantValue) {
        return false;
      }
      const ValueType item_type = item->AsConstantValue().GetValue().type;
      return item_type == ValueType::kNull || item_type == col_type ||
             (col_type == ValueType::kDouble &&
              item_type == ValueType::kInt64) ||
             (col_type == ValueType::kInt64 && item_type == ValueType::kDouble);
    });
  }
  if (expr->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& binary = expr->AsBinaryExpression();
  if (binary.Op() == BinaryOperation::kAnd ||
      binary.Op() == BinaryOperation::kOr) {
    return IsNonThrowingLocalFilter(binary.Left(), local_schema, nullptr) &&
           IsNonThrowingLocalFilter(binary.Right(), local_schema, nullptr);
  }
  if (binary.Op() == BinaryOperation::kLike ||
      binary.Op() == BinaryOperation::kNotLike) {
    if (!binary.Left() || binary.Left()->Type() != TypeTag::kColumnValue ||
        !binary.Right() || binary.Right()->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const auto offset = LocalColumnOffset(
        local_schema, binary.Left()->AsColumnValue().GetColumnName());
    if (!offset) {
      return false;
    }
    const Column& col = local_schema.GetColumn(*offset);
    if (col.Type() != ValueType::kVarChar || col.IsInterval()) {
      return false;
    }
    const Value& pat = binary.Right()->AsConstantValue().GetValue();
    return pat.type == ValueType::kNull ||
           (pat.type == ValueType::kVarChar && !pat.IsCaseInsensitive());
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
      return false;
  }
  auto is_const_operand = [](const Expression& operand,
                             std::optional<ValueType>* out_type) -> bool {
    if (!operand) {
      return false;
    }
    if (operand->Type() == TypeTag::kConstantValue) {
      *out_type = operand->AsConstantValue().GetValue().type;
      return true;
    }
    if (!ContainsVolatileCall(operand) && !ContainsParameterSlot(operand) &&
        !ContainsQuery(operand) && !ContainsAggregate(operand) &&
        operand->TouchedColumns().empty()) {
      StatusOr<Value> evaluated = operand->TryEvaluate(Row(), Schema());
      if (evaluated.HasValue()) {
        *out_type = evaluated.Value().type;
        return true;
      }
    }
    return false;
  };
  const Expression* col_side = nullptr;
  const Expression* const_side = nullptr;
  if (binary.Left() && binary.Left()->Type() == TypeTag::kColumnValue) {
    col_side = &binary.Left();
    const_side = &binary.Right();
  } else if (binary.Right() &&
             binary.Right()->Type() == TypeTag::kColumnValue) {
    col_side = &binary.Right();
    const_side = &binary.Left();
  } else {
    return false;
  }
  const auto col_off = LocalColumnOffset(
      local_schema, (*col_side)->AsColumnValue().GetColumnName());
  std::optional<ValueType> const_type;
  if (!col_off.has_value() || !is_const_operand(*const_side, &const_type) ||
      !const_type.has_value()) {
    return false;
  }
  const ValueType col_type = local_schema.GetColumn(*col_off).Type();
  const ValueType rhs_type = *const_type;
  return rhs_type == col_type || rhs_type == ValueType::kNull ||
         (col_type == ValueType::kDouble && rhs_type == ValueType::kInt64) ||
         (col_type == ValueType::kInt64 && rhs_type == ValueType::kDouble);
}

std::optional<PrebuiltMembershipPredicate> TryCompilePrebuiltExists(
    const Expression& predicate, const Schema& schema,
    const FilterCompileEnv& env) {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP") != nullptr;
  if (disabled || !predicate || predicate->Type() != TypeTag::kQueryExp ||
      env.input_rows == 0 || env.context == nullptr || env.ctes == nullptr ||
      env.context->execution_runtime() == nullptr) {
    return std::nullopt;
  }
  const auto& query = predicate->AsQueryExpression();
  if (!query.Exists() || query.Test() || query.ArrayResult()) {
    return std::nullopt;
  }
  const SelectStatement& sub = *query.Query();
  if (sub.Sources().empty() || !sub.WithQueries().empty() ||
      !sub.UnionAll().empty() || sub.GetSetOperationTree() || sub.Qualify() ||
      sub.HasLimit() || sub.Offset() != 0 || !sub.GroupBy().empty() ||
      sub.Having() || sub.Distinct() || !sub.OrderBy().empty() ||
      sub.AsStruct() || sub.ForUpdate() || sub.HasDistinctOn()) {
    return std::nullopt;
  }
  Schema local_schema;
  std::shared_ptr<Table> single_table;
  std::shared_ptr<TableStatistics> single_stats;
  for (const auto& source : sub.Sources()) {
    if (source.query || source.unnest || source.sample ||
        source.join_condition ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner)) {
      return std::nullopt;
    }
    const std::string& qualifier =
        source.alias.empty() ? source.table : source.alias;
    Schema part;
    if (env.ctes->contains(source.table)) {
      const Relation* cte = env.ctes->at(source.table).get();
      if (cte == nullptr) {
        return std::nullopt;
      }
      part = qualifier.empty() ? cte->schema
                               : QualifySchema(cte->schema, qualifier);
    } else {
      auto table = env.context->GetTable(source.table);
      auto stats = env.context->GetStats(source.table);
      if (!table.HasValue() || !stats.HasValue() ||
          stats.Value()->Rows() == 0) {
        return std::nullopt;
      }
      if (sub.Sources().size() > 1 && env.input_rows < 64) {
        return std::nullopt;
      }
      part = qualifier.empty()
                 ? table.Value()->GetSchema()
                 : QualifySchema(table.Value()->GetSchema(), qualifier);
      if (sub.Sources().size() == 1) {
        single_table = table.Value();
        single_stats = stats.Value();
      }
    }
    local_schema = local_schema.ColumnCount() == 0 ? std::move(part)
                                                   : (local_schema + part);
  }
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
  Expression local_key2;
  ColumnName outer_key2;
  const bool allow_cte_pair_key =
      sub.Sources().size() == 1 && !sub.Sources()[0].query &&
      !sub.Sources()[0].unnest && !sub.Sources()[0].sample &&
      env.ctes->contains(sub.Sources()[0].table);
  Expression local_witness;
  ColumnName outer_witness;
  std::vector<Expression> rest;
  bool all_rest_simple = true;
  for (const auto& conjunct : SplitConjuncts(sub.WhereClause())) {
    if (!conjunct) {
      return std::nullopt;
    }
    if (conjunct->Type() == TypeTag::kBinaryExp) {
      const auto& binary = conjunct->AsBinaryExpression();
      if (binary.Op() == BinaryOperation::kEquals && binary.Left() &&
          binary.Right() && binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kColumnValue) {
        const auto& left = binary.Left()->AsColumnValue().GetColumnName();
        const auto& right = binary.Right()->AsColumnValue().GetColumnName();
        const bool left_local =
            LocalColumnOffset(local_schema, left).has_value();
        const bool right_local =
            LocalColumnOffset(local_schema, right).has_value();
        if (left_local != right_local) {
          if (!local_key) {
            local_key = left_local ? binary.Left() : binary.Right();
            outer_key = left_local ? right : left;
            continue;
          }
          if (allow_cte_pair_key && !local_key2) {
            local_key2 = left_local ? binary.Left() : binary.Right();
            outer_key2 = left_local ? right : left;
            continue;
          }
          return std::nullopt;
        }
      }
      if (binary.Op() == BinaryOperation::kNotEquals && binary.Left() &&
          binary.Right() && binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kColumnValue) {
        const auto& left = binary.Left()->AsColumnValue().GetColumnName();
        const auto& right = binary.Right()->AsColumnValue().GetColumnName();
        const bool left_local =
            LocalColumnOffset(local_schema, left).has_value();
        const bool right_local =
            LocalColumnOffset(local_schema, right).has_value();
        if (left_local != right_local) {
          if (local_witness) {
            return std::nullopt;
          }
          local_witness = left_local ? binary.Left() : binary.Right();
          outer_witness = left_local ? right : left;
          continue;
        }
      }
    }
    if (!IsNonThrowingLocalFilter(conjunct, local_schema, &all_rest_simple)) {
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
  if (local_key2) {
    if (local_witness || outer_column.IsUnsigned() ||
        StatementContainsVolatileFunctionCached(
            env.context->execution_runtime(), sub)) {
      return std::nullopt;
    }
    const auto probe2 = LocalColumnOffset(schema, outer_key2);
    const auto local2 = LocalColumnOffset(
        local_schema, local_key2->AsColumnValue().GetColumnName());
    if (!probe2 || !local2 || *local2 == *local) {
      return std::nullopt;
    }
    const Column& inner_col2 =
        local_schema.GetColumn(static_cast<size_t>(*local2));
    const Column& outer_col2 = schema.GetColumn(static_cast<size_t>(*probe2));
    if ((inner_col2.Type() != ValueType::kInt64 &&
         inner_col2.Type() != ValueType::kDate) ||
        inner_col2.Type() != outer_col2.Type() || inner_col2.IsUnsigned() ||
        outer_col2.IsUnsigned()) {
      return std::nullopt;
    }
    const Relation* cte_rel = env.ctes->at(sub.Sources()[0].table).get();
    if (cte_rel == nullptr || cte_rel->HasSpill()) {
      return std::nullopt;
    }
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
    //   (packed_int32_pair_bijection_sound)
    const CompiledScanFilter rest_filter =
        rest.empty() ? CompiledScanFilter{}
                     : CompileScanFilter(rest, local_schema);
    auto pair_set = std::make_shared<FlatIntSet>();
    pair_set->Reserve(std::min<size_t>(cte_rel->TotalRows(), 262144));
    const auto idx0 = static_cast<size_t>(*local);
    const auto idx1 = static_cast<size_t>(*local2);
    const ValueType exp_t0 = outer_column.Type();
    const ValueType exp_t1 = outer_col2.Type();
    bool pair_ok = true;
    Status eval_err = Status::kSuccess;
    auto record_pair_row = [&](const Row& r) {
      if (idx0 >= r.values_.size() || idx1 >= r.values_.size()) {
        pair_ok = false;
        return;
      }
      const Value& v0 = r.values_[idx0];
      const Value& v1 = r.values_[idx1];
      if (v0.IsNull() || v1.IsNull()) {
        return;
      }
      if (!rest.empty() &&
          !MatchScanFilter(r, local_schema, rest_filter, nullptr, *env.context,
                           *env.ctes, &eval_err)) {
        if (!eval_err.ok()) {
          pair_ok = false;
        }
        return;
      }
      if (v0.type != exp_t0 || v0.IsUnsigned() || v1.type != exp_t1 ||
          v1.IsUnsigned() || !std::in_range<int32_t>(v0.value.int_value) ||
          !std::in_range<int32_t>(v1.value.int_value)) {
        pair_ok = false;
        return;
      }
      const uint64_t packed =
          (static_cast<uint64_t>(
               static_cast<uint32_t>(static_cast<int32_t>(v0.value.int_value)))
           << 32) |
          static_cast<uint32_t>(static_cast<int32_t>(v1.value.int_value));
      pair_set->Insert(std::bit_cast<int64_t>(packed));
    };
    if (cte_rel->HasColumnarOnly()) {
      Row scratch_row;
      for (const ColumnarChunk& ch : cte_rel->columnar->chunks) {
        if (idx0 >= ch.columns.size() || idx1 >= ch.columns.size()) {
          pair_ok = false;
          break;
        }
        const ColumnarColumn& c0 = ch.columns[idx0];
        const ColumnarColumn& c1 = ch.columns[idx1];
        for (size_t r = 0; r < ch.rows; ++r) {
          if (c0.IsNull(r) || c1.IsNull(r)) {
            continue;
          }
          if (rest.empty() && c0.is_uniform && !c0.is_unsigned &&
              c0.type == exp_t0 && c1.is_uniform && !c1.is_unsigned &&
              c1.type == exp_t1) {
            const int64_t k0 = c0.IntAt(r);
            const int64_t k1 = c1.IntAt(r);
            if (!std::in_range<int32_t>(k0) || !std::in_range<int32_t>(k1)) {
              pair_ok = false;
              break;
            }
            const uint64_t packed =
                (static_cast<uint64_t>(
                     static_cast<uint32_t>(static_cast<int32_t>(k0)))
                 << 32) |
                static_cast<uint32_t>(static_cast<int32_t>(k1));
            pair_set->Insert(std::bit_cast<int64_t>(packed));
            continue;
          }
          ch.MaterializeRowInto(r, &scratch_row);
          record_pair_row(scratch_row);
          if (!pair_ok) {
            break;
          }
        }
        if (!pair_ok) {
          break;
        }
      }
    } else {
      for (const Row& r : cte_rel->rows) {
        record_pair_row(r);
        if (!pair_ok) {
          break;
        }
      }
    }
    if (!pair_ok || !eval_err.ok()) {
      return std::nullopt;
    }
    if (auto* runtime = env.context->execution_runtime()) {
      ++runtime->prebuilt_exists_index_builds;
      runtime->prebuilt_exists_keys += pair_set->Size();
    }
    PrebuiltMembershipPredicate compiled;
    compiled.column = static_cast<slot_t>(*probe);
    compiled.negated = query.Negated();
    compiled.domain_type = exp_t0;
    compiled.values = std::move(pair_set);
    compiled.existence = true;
    compiled.contains_null = false;
    compiled.packed_pair_col2 = static_cast<slot_t>(*probe2);
    compiled.packed_pair_type2 = exp_t1;
    return compiled;
  }
  std::optional<size_t> witness_probe;
  if (local_witness) {
    witness_probe = LocalColumnOffset(schema, outer_witness);
    const auto witness_local = LocalColumnOffset(
        local_schema, local_witness->AsColumnValue().GetColumnName());
    if (!witness_probe || !witness_local) {
      return std::nullopt;
    }
    const Column& iw_col =
        local_schema.GetColumn(static_cast<size_t>(*witness_local));
    const Column& ow_col =
        schema.GetColumn(static_cast<size_t>(*witness_probe));
    if ((iw_col.Type() != ValueType::kInt64 &&
         iw_col.Type() != ValueType::kDate) ||
        iw_col.Type() != ow_col.Type() || iw_col.IsUnsigned() ||
        ow_col.IsUnsigned() || outer_column.IsUnsigned()) {
      return std::nullopt;
    }
  }
  // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
  //   (prebuilt_exists_outer_domain_filter_sound,
  //    not_exists_on_null_outer_key_is_true)
  // Restrict the inner membership build to the non-NULL probe keys actually
  // present in `env.input_relation` when bounded in memory.
  std::vector<int64_t> outer_domain_keys;
  bool have_outer_domain = false;
  if (env.input_relation != nullptr && !env.input_relation->HasSpill() &&
      env.input_relation->TotalRows() <= 32768 &&
      static_cast<size_t>(*probe) < env.input_relation->schema.ColumnCount() &&
      !outer_column.IsUnsigned()) {
    bool domain_ok = true;
    const auto probe_idx = static_cast<size_t>(*probe);
    const ValueType expected_t = outer_column.Type();
    outer_domain_keys.reserve(env.input_relation->TotalRows());
    if (env.input_relation->HasColumnarOnly()) {
      for (const ColumnarChunk& ch : env.input_relation->columnar->chunks) {
        if (probe_idx >= ch.columns.size()) {
          domain_ok = false;
          break;
        }
        const ColumnarColumn& cc = ch.columns[probe_idx];
        for (size_t r = 0; r < ch.rows; ++r) {
          if (cc.IsNull(r)) {
            continue;
          }
          if (cc.is_uniform) {
            if (cc.type != expected_t || cc.is_unsigned) {
              domain_ok = false;
              break;
            }
            outer_domain_keys.push_back(cc.IntAt(r));
          } else {
            Value tmp;
            cc.WriteValueInto(r, &tmp);
            if (tmp.IsNull()) {
              continue;
            }
            if (tmp.type != expected_t || tmp.IsUnsigned()) {
              domain_ok = false;
              break;
            }
            outer_domain_keys.push_back(tmp.value.int_value);
          }
        }
        if (!domain_ok) {
          break;
        }
      }
    } else {
      for (const Row& r : env.input_relation->rows) {
        if (probe_idx >= r.values_.size()) {
          domain_ok = false;
          break;
        }
        const Value& v = r.values_[probe_idx];
        if (v.IsNull()) {
          continue;
        }
        if (v.type != expected_t || v.IsUnsigned()) {
          domain_ok = false;
          break;
        }
        outer_domain_keys.push_back(v.value.int_value);
      }
    }
    if (domain_ok) {
      std::ranges::sort(outer_domain_keys);
      const auto dup = std::ranges::unique(outer_domain_keys);
      outer_domain_keys.erase(dup.begin(), dup.end());
      have_outer_domain = true;
    }
  }
  if (have_outer_domain && outer_domain_keys.empty()) {
    PrebuiltMembershipPredicate empty_compiled;
    empty_compiled.column = static_cast<slot_t>(*probe);
    empty_compiled.negated = query.Negated();
    empty_compiled.domain_type = outer_column.Type();
    empty_compiled.values = std::make_shared<FlatIntSet>();
    empty_compiled.existence = true;
    empty_compiled.contains_null = false;
    return empty_compiled;
  }

  const Expression local_filter = rest.empty() ? nullptr : ConjoinAll(rest);
  if (!local_witness && single_table != nullptr && single_stats != nullptr &&
      !rest.empty() && all_rest_simple) {
    const double build_rows = static_cast<double>(single_stats->Rows()) *
                              (local_filter ? single_stats->EstimateSelectivity(
                                                  local_schema, local_filter)
                                            : 1.0);
    if (auto values = TryBuildIndexedExistsKeys(
            *single_table, local_schema, static_cast<slot_t>(*local), rest,
            *single_stats, build_rows, env)) {
      PrebuiltMembershipPredicate compiled;
      compiled.column = static_cast<slot_t>(*probe);
      compiled.negated = query.Negated();
      compiled.domain_type = outer_column.Type();
      compiled.values = std::move(values);
      compiled.existence = true;
      return compiled;
    }
  }
  if (StatementContainsVolatileFunctionCached(env.context->execution_runtime(),
                                              sub)) {
    return std::nullopt;
  }
  // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
  //   (distinct_witness_exists_to_minmax_sound,
  //    prebuilt_exists_outer_domain_filter_sound)
  if (local_witness) {
    if (!have_outer_domain || outer_domain_keys.size() > 8192) {
      return std::nullopt;
    }
    const ValueType expected_k = outer_column.Type();
    const ValueType expected_w =
        schema.GetColumn(static_cast<size_t>(*witness_probe)).Type();
    const auto witness_local_opt = LocalColumnOffset(
        local_schema, local_witness->AsColumnValue().GetColumnName());
    if (single_table != nullptr && sub.Sources().size() == 1 &&
        !sub.Sources()[0].sample && witness_local_opt.has_value()) {
      std::vector<slot_t> proj = {static_cast<slot_t>(*local),
                                  static_cast<slot_t>(*witness_local_opt)};
      for (const Expression& rpred : rest) {
        for (const ColumnName& cn : rpred->TouchedColumns()) {
          if (const auto off = LocalColumnOffset(local_schema, cn)) {
            proj.push_back(static_cast<slot_t>(*off));
          }
        }
      }
      std::ranges::sort(proj);
      const auto pdup = std::ranges::unique(proj);
      proj.erase(pdup.begin(), pdup.end());
      const auto key_it =
          std::ranges::lower_bound(proj, static_cast<slot_t>(*local));
      const auto wit_it = std::ranges::lower_bound(
          proj, static_cast<slot_t>(*witness_local_opt));
      if (key_it != proj.end() && wit_it != proj.end()) {
        const auto proj_key =
            static_cast<slot_t>(std::distance(proj.begin(), key_it));
        const auto proj_wit =
            static_cast<slot_t>(std::distance(proj.begin(), wit_it));
        RuntimeFilterBuilder rf_builder;
        for (const int64_t k : outer_domain_keys) {
          rf_builder.Observe(k);
        }
        ScanRuntimeFilter domain_rf;
        domain_rf.group = rf_builder.Build(false);
        domain_rf.column = proj_key;
        domain_rf.type = expected_k;
        StatusOr<Relation> loaded = LoadSource(
            *env.context, sub.Sources()[0], nullptr, *env.ctes, &proj,
            rest.empty() ? nullptr : &rest,
            domain_rf.group != nullptr ? &domain_rf : nullptr, nullptr,
            /*columnar_output=*/true);
        if (loaded.HasValue() && loaded.Value().GetStatus().ok() &&
            !loaded.Value().HasSpill()) {
          Relation& rel = loaded.Value();
          auto minmax_map =
              std::make_shared<FlatIntKeyMap<std::pair<int64_t, int64_t>>>();
          minmax_map->Reserve(outer_domain_keys.size());
          bool fast_ok = true;
          const auto observe_pair = [&](int64_t kv, int64_t wv) {
            if (!std::ranges::binary_search(outer_domain_keys, kv)) {
              return;
            }
            bool ins = false;
            auto* slot = minmax_map->TryEmplace(kv, &ins);
            if (ins) {
              *slot = {wv, wv};
            } else {
              slot->first = std::min(slot->first, wv);
              slot->second = std::max(slot->second, wv);
            }
          };
          if (rel.HasColumnarOnly() && rel.columnar != nullptr) {
            Value tk;
            Value tw;
            for (const ColumnarChunk& ch : rel.columnar->chunks) {
              if (proj_key >= ch.columns.size() ||
                  proj_wit >= ch.columns.size()) {
                fast_ok = false;
                break;
              }
              const ColumnarColumn& ck = ch.columns[proj_key];
              const ColumnarColumn& cw = ch.columns[proj_wit];
              if (ck.is_uniform && cw.is_uniform && ck.type == expected_k &&
                  !ck.is_unsigned && cw.type == expected_w && !cw.is_unsigned) {
                for (size_t r = 0; r < ch.rows; ++r) {
                  if (ck.IsNull(r) || cw.IsNull(r)) {
                    continue;
                  }
                  observe_pair(ck.IntAt(r), cw.IntAt(r));
                }
              } else {
                for (size_t r = 0; r < ch.rows; ++r) {
                  ck.WriteValueInto(r, &tk);
                  cw.WriteValueInto(r, &tw);
                  if (tk.IsNull() || tw.IsNull()) {
                    continue;
                  }
                  if (tk.type != expected_k || tk.IsUnsigned() ||
                      tw.type != expected_w || tw.IsUnsigned()) {
                    fast_ok = false;
                    break;
                  }
                  observe_pair(tk.value.int_value, tw.value.int_value);
                }
              }
              if (!fast_ok) {
                break;
              }
            }
          } else {
            for (const Row& row : rel.rows) {
              if (proj_key >= row.values_.size() ||
                  proj_wit >= row.values_.size()) {
                fast_ok = false;
                break;
              }
              const Value& vk = row.values_[proj_key];
              const Value& vw = row.values_[proj_wit];
              if (vk.IsNull() || vw.IsNull()) {
                continue;
              }
              if (vk.type != expected_k || vk.IsUnsigned() ||
                  vw.type != expected_w || vw.IsUnsigned()) {
                fast_ok = false;
                break;
              }
              observe_pair(vk.value.int_value, vw.value.int_value);
            }
          }
          if (fast_ok) {
            if (auto* runtime = env.context->execution_runtime()) {
              ++runtime->prebuilt_exists_index_builds;
              runtime->prebuilt_exists_keys += minmax_map->Size();
            }
            PrebuiltMembershipPredicate compiled;
            compiled.column = static_cast<slot_t>(*probe);
            compiled.negated = query.Negated();
            compiled.domain_type = expected_k;
            compiled.existence = true;
            compiled.contains_null = false;
            compiled.distinct_witness_minmax = std::move(minmax_map);
            compiled.witness_column = static_cast<slot_t>(*witness_probe);
            compiled.witness_type = expected_w;
            return compiled;
          }
        }
      }
    }
    const auto make_val = [&](int64_t k) {
      return outer_column.Type() == ValueType::kDate ? Value::DateFromDays(k)
                                                     : Value(k);
    };
    std::vector<Expression> restricted_preds = rest;
    restricted_preds.push_back(
        UnaryExpressionExp(local_key, UnaryOperation::kIsNotNull));
    restricted_preds.push_back(
        UnaryExpressionExp(local_witness, UnaryOperation::kIsNotNull));
    if (outer_domain_keys.size() == 1) {
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kEquals,
          ConstantValueExp(make_val(outer_domain_keys[0]))));
    } else {
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kGreaterThanEquals,
          ConstantValueExp(make_val(outer_domain_keys.front()))));
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kLessThanEquals,
          ConstantValueExp(make_val(outer_domain_keys.back()))));
      std::vector<Expression> in_items;
      in_items.reserve(outer_domain_keys.size());
      for (const int64_t k : outer_domain_keys) {
        in_items.push_back(ConstantValueExp(make_val(k)));
      }
      restricted_preds.push_back(
          InExpressionExp(local_key, std::move(in_items)));
    }
    auto restricted_stmt = std::make_shared<SelectStatement>(sub);
    restricted_stmt->SetWhereClause(ConjoinAll(restricted_preds));
    Expression min_w = std::make_shared<AggregateExpression>(
        AggregationType::kMin, local_witness, false);
    Expression max_w = std::make_shared<AggregateExpression>(
        AggregationType::kMax, local_witness, false);
    restricted_stmt->SetSelectList({NamedExpression("", local_key),
                                    NamedExpression("", std::move(min_w)),
                                    NamedExpression("", std::move(max_w))});
    restricted_stmt->SetGroupBy({local_key});
    if (!StatementUsesOnlyScopes(*env.context, *restricted_stmt, {},
                                 *env.ctes)) {
      return std::nullopt;
    }
    StatusOr<Relation> exec_res = ExecuteQuery(
        *env.context, *restricted_stmt, nullptr, *env.ctes, nullptr, false);
    if (!exec_res.HasValue() || !exec_res.Value().GetStatus().ok() ||
        exec_res.Value().schema.ColumnCount() != 3 ||
        exec_res.Value().HasSpill()) {
      return std::nullopt;
    }
    Relation& members = exec_res.Value();
    if (members.HasColumnarOnly()) {
      // T4539: no rows visible, no prebuilt EXISTS filter (fallback stays
      // correct, just slower) instead of compiling a membership over an
      // empty row set.
      if (!members.EnsureRowsMaterialized().ok()) {
        return std::nullopt;
      }
    }
    auto minmax_map =
        std::make_shared<FlatIntKeyMap<std::pair<int64_t, int64_t>>>();
    minmax_map->Reserve(members.rows.size());
    for (const Row& row : members.rows) {
      if (row.values_.size() < 3 || row.values_[0].IsNull() ||
          row.values_[1].IsNull() || row.values_[2].IsNull()) {
        continue;
      }
      if (row.values_[0].type != expected_k || row.values_[0].IsUnsigned() ||
          row.values_[1].type != expected_w || row.values_[1].IsUnsigned() ||
          row.values_[2].type != expected_w || row.values_[2].IsUnsigned()) {
        return std::nullopt;
      }
      bool ins = false;
      auto* slot = minmax_map->TryEmplace(row.values_[0].value.int_value, &ins);
      *slot = {row.values_[1].value.int_value, row.values_[2].value.int_value};
    }
    if (auto* runtime = env.context->execution_runtime()) {
      ++runtime->prebuilt_exists_index_builds;
      runtime->prebuilt_exists_keys += minmax_map->Size();
    }
    PrebuiltMembershipPredicate compiled;
    compiled.column = static_cast<slot_t>(*probe);
    compiled.negated = query.Negated();
    compiled.domain_type = expected_k;
    compiled.existence = true;
    compiled.contains_null = false;
    compiled.distinct_witness_minmax = std::move(minmax_map);
    compiled.witness_column = static_cast<slot_t>(*witness_probe);
    compiled.witness_type = expected_w;
    return compiled;
  }
  const bool group_keys = sub.Sources().size() > 1 || single_table == nullptr;
  if (have_outer_domain && outer_domain_keys.size() <= 8192) {
    const auto make_val = [&](int64_t k) {
      return outer_column.Type() == ValueType::kDate ? Value::DateFromDays(k)
                                                     : Value(k);
    };
    std::vector<Expression> restricted_preds = rest;
    restricted_preds.push_back(
        UnaryExpressionExp(local_key, UnaryOperation::kIsNotNull));
    if (outer_domain_keys.size() == 1) {
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kEquals,
          ConstantValueExp(make_val(outer_domain_keys[0]))));
    } else {
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kGreaterThanEquals,
          ConstantValueExp(make_val(outer_domain_keys.front()))));
      restricted_preds.push_back(BinaryExpressionExp(
          local_key, BinaryOperation::kLessThanEquals,
          ConstantValueExp(make_val(outer_domain_keys.back()))));
      std::vector<Expression> in_items;
      in_items.reserve(outer_domain_keys.size());
      for (const int64_t k : outer_domain_keys) {
        in_items.push_back(ConstantValueExp(make_val(k)));
      }
      restricted_preds.push_back(
          InExpressionExp(local_key, std::move(in_items)));
    }
    auto restricted_stmt = std::make_shared<SelectStatement>(sub);
    restricted_stmt->SetWhereClause(ConjoinAll(restricted_preds));
    restricted_stmt->SetSelectList({NamedExpression("", local_key)});
    if (group_keys) {
      restricted_stmt->SetGroupBy({local_key});
    }
    if (StatementUsesOnlyScopes(*env.context, *restricted_stmt, {},
                                *env.ctes)) {
      StatusOr<Relation> exec_res = ExecuteQuery(
          *env.context, *restricted_stmt, nullptr, *env.ctes, nullptr, true);
      if (exec_res.HasValue() && exec_res.Value().GetStatus().ok() &&
          exec_res.Value().schema.ColumnCount() == 1 &&
          !exec_res.Value().HasSpill()) {
        const Relation& members = exec_res.Value();
        auto values = std::make_shared<FlatIntSet>();
        if (!values->TryReserveDense(outer_domain_keys.front(),
                                     outer_domain_keys.back())) {
          values->Reserve(
              std::min(outer_domain_keys.size(), members.TotalRows()));
        }
        bool domain_clean = true;
        const ValueType expected_t = outer_column.Type();
        if (members.HasColumnarOnly()) {
          for (const ColumnarChunk& ch : members.columnar->chunks) {
            const ColumnarColumn& cc = ch.columns[0];
            for (size_t r = 0; r < ch.rows; ++r) {
              if (cc.IsNull(r)) {
                continue;
              }
              if (cc.is_uniform) {
                if (cc.type != expected_t || cc.is_unsigned) {
                  domain_clean = false;
                  break;
                }
                values->Insert(cc.IntAt(r));
              } else {
                Value tmp;
                cc.WriteValueInto(r, &tmp);
                if (tmp.IsNull()) {
                  continue;
                }
                if (tmp.type != expected_t || tmp.IsUnsigned()) {
                  domain_clean = false;
                  break;
                }
                values->Insert(tmp.value.int_value);
              }
            }
            if (!domain_clean) {
              break;
            }
          }
        } else {
          for (const Row& row : members.rows) {
            if (row.values_.empty() || row.values_[0].IsNull()) {
              continue;
            }
            if (row.values_[0].type != expected_t ||
                row.values_[0].IsUnsigned() ||
                row.values_[0].Collation() != 0) {
              domain_clean = false;
              break;
            }
            values->Insert(row.values_[0].value.int_value);
          }
        }
        if (domain_clean && !values->DenseOverflowed()) {
          if (auto* runtime = env.context->execution_runtime()) {
            ++runtime->prebuilt_exists_index_builds;
            runtime->prebuilt_exists_keys += values->Size();
          }
          PrebuiltMembershipPredicate compiled;
          compiled.column = static_cast<slot_t>(*probe);
          compiled.negated = query.Negated();
          compiled.domain_type = expected_t;
          compiled.values = std::move(values);
          compiled.existence = true;
          compiled.contains_null = false;
          return compiled;
        }
      }
    }
  }
  auto rewritten = std::make_shared<SelectStatement>(sub);
  if (group_keys) {
    std::vector<Expression> inner_preds = std::move(rest);
    inner_preds.push_back(
        UnaryExpressionExp(local_key, UnaryOperation::kIsNotNull));
    rewritten->SetWhereClause(ConjoinAll(inner_preds));
    rewritten->SetSelectList({NamedExpression("", local_key)});
    rewritten->SetGroupBy({local_key});
  } else {
    rewritten->SetWhereClause(local_filter);
    rewritten->SetSelectList({NamedExpression("", local_key)});
  }
  if (!StatementUsesOnlyScopes(*env.context, *rewritten, {}, *env.ctes) ||
      StatementContainsVolatileFunctionCached(env.context->execution_runtime(),
                                              sub)) {
    return std::nullopt;
  }
  Expression membership = QueryExpressionExp(
      std::move(rewritten), ColumnValueExp(outer_key), false, query.Negated());
  auto compiled = TryCompilePrebuiltMembership(membership, schema, *env.context,
                                               *env.ctes, group_keys);
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
    if (product.Left() && product.Right() &&
        product.Left()->Type() == TypeTag::kAggregateExp) {
      aggregate_item = product.Left();
      constant = product.Right();
    } else if (product.Left() && product.Right() &&
               product.Right()->Type() == TypeTag::kAggregateExp) {
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
        !conjunct->AsBinaryExpression().Left() ||
        !conjunct->AsBinaryExpression().Right() ||
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
  constexpr size_t kMaxInListDisjunctiveExpansion = 8;
  const InExpression& in = *in_ptr;
  if (!in.child_ || in.child_->Type() != TypeTag::kColumnValue ||
      in.list_.empty() || in.list_.size() > kMaxInListDisjunctiveExpansion) {
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

std::optional<PrebuiltMembershipPredicate> TryCompileConstantInMembership(
    const Expression& predicate, const Schema& schema) {
  if (!predicate) {
    return std::nullopt;
  }
  bool negated = false;
  const InExpression* in_ptr = nullptr;
  if (predicate->Type() == TypeTag::kInExp) {
    in_ptr = &predicate->AsInExpression();
  } else if (predicate->Type() == TypeTag::kUnaryExp &&
             predicate->AsUnaryExpression().Op() == UnaryOperation::kNot &&
             predicate->AsUnaryExpression().Child() &&
             predicate->AsUnaryExpression().Child()->Type() ==
                 TypeTag::kInExp) {
    negated = true;
    in_ptr = &predicate->AsUnaryExpression().Child()->AsInExpression();
  } else {
    return std::nullopt;
  }
  const InExpression& in = *in_ptr;
  if (!in.child_ || in.child_->Type() != TypeTag::kColumnValue ||
      in.list_.size() < 2) {
    return std::nullopt;
  }
  const auto offset =
      LocalColumnOffset(schema, in.child_->AsColumnValue().GetColumnName());
  if (!offset) {
    return std::nullopt;
  }
  const Column& probe_col = schema.GetColumn(static_cast<size_t>(*offset));
  const ValueType probe_type = probe_col.Type();
  if (probe_col.IsInterval() || probe_col.IsUnsigned() ||
      (probe_type != ValueType::kInt64 && probe_type != ValueType::kDate &&
       probe_type != ValueType::kVarChar)) {
    return std::nullopt;
  }
  PrebuiltMembershipPredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.negated = negated;
  compiled.domain_type = probe_type;
  compiled.existence = false;
  if (probe_type == ValueType::kVarChar) {
    auto strings = std::make_shared<MembershipStringSet>();
    strings->reserve(in.list_.size());
    for (const Expression& item : in.list_) {
      std::optional<Value> val = TryDecideConstantConjunct(item);
      if (!val.has_value()) {
        return std::nullopt;
      }
      if (val->IsNull()) {
        compiled.contains_null = true;
        continue;
      }
      if (val->type != ValueType::kVarChar || val->Collation() != 0 ||
          !PlainStringComparable(val->AsStringView())) {
        return std::nullopt;
      }
      strings->emplace(val->AsStringView());
    }
    compiled.string_values = std::move(strings);
    return compiled;
  }
  auto values = std::make_shared<FlatIntSet>();
  std::vector<int64_t> int_vals;
  int_vals.reserve(in.list_.size());
  for (const Expression& item : in.list_) {
    std::optional<Value> val = TryDecideConstantConjunct(item);
    if (!val.has_value()) {
      return std::nullopt;
    }
    if (val->IsNull()) {
      compiled.contains_null = true;
      continue;
    }
    if (val->type != probe_type || val->IsUnsigned()) {
      return std::nullopt;
    }
    int_vals.push_back(val->value.int_value);
  }
  if (!int_vals.empty()) {
    values->Reserve(int_vals.size());
    for (int64_t v : int_vals) {
      values->Insert(v);
    }
  }
  compiled.values = std::move(values);
  return compiled;
}

CompiledScanFilter::DisjunctiveBranch CompileAndBranch(
    const Expression& and_expr, const Schema& schema,
    const FilterCompileEnv* env) {
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
      } else if (auto const_mem =
                     TryCompileConstantInMembership(conjunct, schema)) {
        branch.prebuilt_membership.push_back(std::move(*const_mem));
      } else if (env != nullptr) {
        if (auto membership =
                env->prebuild_only_exists
                    ? std::optional<PrebuiltMembershipPredicate>()
                    : TryCompilePrebuiltMembership(conjunct, schema,
                                                   *env->context, *env->ctes)) {
          branch.prebuilt_membership.push_back(std::move(*membership));
        } else if (auto existence =
                       TryCompilePrebuiltExists(conjunct, schema, *env)) {
          branch.prebuilt_membership.push_back(std::move(*existence));
        } else {
          branch.residual.push_back(conjunct);
        }
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

// Collect the schema slots a residual expression can read.  Returns false
// when a column name does not resolve against `schema`, telling the caller
// to fall back to the conservative whole-schema treatment.
bool CollectResidualColumnSlots(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Schema& schema,
    std::unordered_set<slot_t>* slots) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const auto offset =
        LocalColumnOffset(schema, expression->AsColumnValue().GetColumnName());
    if (!offset) {
      return false;
    }
    slots->insert(*offset);
    return true;
  }
  return !std::ranges::any_of(ExpressionChildren(expression),
                              [&](const Expression& child) {  // NOLINT
                                return !CollectResidualColumnSlots(
                                    child, schema, slots);
                              });
}

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
      // Mirror the constant-conjunct rule exactly once instead of per row:
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
      const bool had_query = ContainsQuery(eff_predicate);
      if (disjuncts.size() >= 2 && (!had_query || env != nullptr)) {
        bool all_branches_usable = true;
        std::vector<CompiledScanFilter::DisjunctiveBranch> new_branches;
        new_branches.reserve(disjuncts.size());
        for (const Expression& disjunct : disjuncts) {
          auto branch = CompileAndBranch(disjunct, schema, env);
          if (had_query &&
              std::ranges::any_of(branch.residual, [](const Expression& expr) {
                return ContainsQuery(expr);
              })) {
            all_branches_usable = false;
            break;
          }
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
                in_simples.reserve(in_disjuncts.size());
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
                    sub_b.prebuilt_membership = branch.prebuilt_membership;
                    new_branches.push_back(std::move(sub_b));
                  }
                  continue;
                }
              }
            }
          }
          // A prefix, IN-list, or prebuilt-membership predicate is already a
          // complete branch.
          if (branch.simple.empty() && branch.prefix_in.empty() &&
              branch.prebuilt_membership.empty()) {
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
            crossed.reserve(branches.size() * new_branches.size());
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
                merged.prebuilt_membership = left.prebuilt_membership;
                merged.prebuilt_membership.insert(
                    merged.prebuilt_membership.end(),
                    right.prebuilt_membership.begin(),
                    right.prebuilt_membership.end());
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
    // A singleton IN expands to one comparison, not an OR/AND tree. Compile
    // that comparison too instead of sending it back to per-row evaluation.
    if (auto simple = TryCompileSimpleCompare(eff_predicate, schema)) {
      compiled.simple.push_back(std::move(*simple));
    } else if (auto prefix_in = TryCompilePrefixIn(predicate, schema)) {
      compiled.prefix_in.push_back(std::move(*prefix_in));
    } else if (auto const_mem =
                   TryCompileConstantInMembership(predicate, schema)) {
      compiled.prebuilt_membership.push_back(std::move(*const_mem));
    } else if (env != nullptr) {
      // With a compile-time environment, an uncorrelated membership probe
      // over an INT64/DATE column compiles to one immutable value set; the
      // conjunct then leaves the residual entirely, which both removes the
      // per-row contextual dispatch and lets the worker programs complete
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
  // Tagging matters only for columns the filter itself reads: an unsigned
  // column no predicate touches cannot change a verdict, so it must not
  // disable the vectorized and columnar fast paths. Duplicate names make
  // name-based resolution ambiguous inside residuals, and an unresolvable
  // residual name may resolve to a column this pass cannot see; both keep
  // the previous tag-everything treatment.
  {
    std::unordered_set<slot_t> referenced;
    referenced.reserve(schema.ColumnCount());
    bool conservative = false;
    {
      std::unordered_set<std::string> seen_names;
      seen_names.reserve(schema.ColumnCount());
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        if (!seen_names.insert(schema.GetColumn(i).Name().name).second) {
          conservative = true;
          break;
        }
      }
    }
    const auto note_simple = [&](const SimpleComparePredicate& p) {
      referenced.insert(p.column);
      if (p.column_payload) {
        referenced.insert(p.right_column);
      }
    };
    for (const SimpleComparePredicate& p : compiled.simple) {
      note_simple(p);
    }
    for (const PrefixInPredicate& p : compiled.prefix_in) {
      referenced.insert(p.column);
    }
    for (const PrebuiltMembershipPredicate& p : compiled.prebuilt_membership) {
      referenced.insert(p.column);
      if (p.distinct_witness_minmax != nullptr) {
        referenced.insert(p.witness_column);
      }
      if (p.packed_pair_col2.has_value()) {
        referenced.insert(*p.packed_pair_col2);
      }
    }
    for (const PrebuiltScalarComparePredicate& p :
         compiled.prebuilt_scalar_compare) {
      referenced.insert(p.column);
      referenced.insert(p.key_column);
    }
    for (const Expression& expression : compiled.residual) {
      conservative |=
          !CollectResidualColumnSlots(expression, schema, &referenced);
    }
    for (const CompiledScanFilter::DisjunctiveBranch& branch :
         compiled.disjunctive_branches) {
      for (const SimpleComparePredicate& p : branch.simple) {
        note_simple(p);
      }
      for (const PrefixInPredicate& p : branch.prefix_in) {
        referenced.insert(p.column);
      }
      for (const PrebuiltMembershipPredicate& p : branch.prebuilt_membership) {
        referenced.insert(p.column);
        if (p.distinct_witness_minmax != nullptr) {
          referenced.insert(p.witness_column);
        }
        if (p.packed_pair_col2.has_value()) {
          referenced.insert(*p.packed_pair_col2);
        }
      }
      for (const Expression& expression : branch.residual) {
        conservative |=
            !CollectResidualColumnSlots(expression, schema, &referenced);
      }
    }
    for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).IsUnsigned() &&
          (conservative || referenced.contains(i))) {
        compiled.needs_unsigned_tagging = true;
        compiled.unsigned_columns.push_back(i);
      }
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
      seen_names.reserve(schema.ColumnCount());
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

bool IsNonThrowingScanFilter(const Expression& predicate,
                             const Schema& schema) {
  return IsNonThrowingLocalFilter(predicate, schema, nullptr);
}

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
  if (pred.packed_pair_col2.has_value()) {
    if (pred.values == nullptr ||
        (pred.values->Empty() && !pred.contains_null)) {
      return pred.negated;
    }
    const slot_t c2 = *pred.packed_pair_col2;
    if (pred.column >= row.values_.size() || c2 >= row.values_.size()) {
      return pred.negated;
    }
    const Value& p0 = row[pred.column];
    const Value& p1 = row[c2];
    if (p0.IsNull() || p1.IsNull() || p0.type != pred.domain_type ||
        p1.type != pred.packed_pair_type2 || p0.IsUnsigned() ||
        p1.IsUnsigned()) {
      return pred.negated;
    }
    if (!std::in_range<int32_t>(p0.value.int_value) ||
        !std::in_range<int32_t>(p1.value.int_value)) {
      return pred.negated;
    }
    const uint64_t packed =
        (static_cast<uint64_t>(
             static_cast<uint32_t>(static_cast<int32_t>(p0.value.int_value)))
         << 32) |
        static_cast<uint32_t>(static_cast<int32_t>(p1.value.int_value));
    const bool found = pred.values->Contains(std::bit_cast<int64_t>(packed));
    return pred.negated ? !found : found;
  }
  if (pred.distinct_witness_minmax != nullptr) {
    if (pred.column >= row.values_.size() ||
        pred.witness_column >= row.values_.size()) {
      return pred.negated;
    }
    const Value& pk = row[pred.column];
    const Value& pw = row[pred.witness_column];
    if (pk.IsNull() || pw.IsNull() || pk.type != pred.domain_type ||
        pw.type != pred.witness_type || pk.IsUnsigned() || pw.IsUnsigned()) {
      return pred.negated;
    }
    const auto* mm = pred.distinct_witness_minmax->Find(pk.value.int_value);
    const int64_t ow = pw.value.int_value;
    const bool found = (mm != nullptr) && (mm->first != ow || mm->second != ow);
    return pred.negated ? !found : found;
  }
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
                                const PrebuiltScalarComparePredicate& pred,
                                Status* error) {
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
  if (!cmp.HasValue()) {
    if (error != nullptr && error->ok()) {
      *error = cmp.GetStatus();
    }
    return false;
  }
  return cmp.Value().Truthy();
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
  // An exact-evaluator raise inside a `simple` predicate is not a clean
  // reject. Keep it aside while the sibling conjuncts run: a clean FALSE
  // from any of them suppresses the error under the commutative WHERE rule,
  // otherwise the error must reach the caller instead of an empty result.
  Status simple_error{Status::kSuccess};
  // Evaluate top-level simple predicates (conjunction).
  for (const SimpleComparePredicate& pred : filter.simple) {
    Status pred_error{Status::kSuccess};
    if (!MatchSimpleCompare(*match_row, pred, &pred_error)) {
      if (!pred_error.ok()) {
        if (simple_error.ok()) {
          simple_error = std::move(pred_error);
        }
        continue;
      }
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
    Status pred_error{Status::kSuccess};
    if (!MatchPrebuiltScalarCompare(*match_row, pred, &pred_error)) {
      if (!pred_error.ok()) {
        if (simple_error.ok()) {
          simple_error = std::move(pred_error);
        }
        continue;
      }
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
      Status branch_error{Status::kSuccess};
      for (const SimpleComparePredicate& pred : branch.simple) {
        Status pred_error{Status::kSuccess};
        if (!MatchSimpleCompare(*match_row, pred, &pred_error)) {
          branch_ok = false;
          if (!pred_error.ok() && branch_error.ok()) {
            branch_error = std::move(pred_error);
          }
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
      if (branch_ok) {
        for (const PrebuiltMembershipPredicate& pred :
             branch.prebuilt_membership) {
          if (!MatchPrebuiltMembership(*match_row, pred)) {
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
      if (!branch_error.ok() && first_branch_error.ok()) {
        first_branch_error = std::move(branch_error);
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
    if (!simple_error.ok()) {
      if (error != nullptr && error->ok()) {
        *error = std::move(simple_error);
      }
      return false;
    }
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
      const bool matches = !result.Value().IsNull() && result.Value().Truthy();
      if (matches && !simple_error.ok()) {
        if (error != nullptr && error->ok()) {
          *error = std::move(simple_error);
        }
        return false;
      }
      return matches;
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
  if (!simple_error.ok()) {
    if (error != nullptr && error->ok()) {
      *error = std::move(simple_error);
    }
    return false;
  }
  return true;
}

std::vector<NullPeekCompare> BuildNullPeeks(
    const CompiledScanFilter& filter, const Schema& projected_schema,
    const std::vector<slot_t>* projection, const Schema& full_schema) {
  const auto collect = [&](const std::vector<Expression>& residuals) {
    std::vector<NullPeekCompare> out;
    out.reserve(residuals.size());
    for (const Expression& residual : residuals) {
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
      out.push_back(
          {full_column, (unary.Op() == UnaryOperation::kIsNull) != negated});
    }
    return out;
  };
  std::vector<NullPeekCompare> null_tests = collect(filter.residual);
  const bool residual_fully_covered =
      null_tests.size() == filter.residual.size();
  // An OR-decomposed conjunct lives in one branch's residual, and a row may
  // survive through any other branch, so only a test carried by EVERY branch
  // is implied by the disjunction. Fold that intersection in to reject such
  // rows before decode; callers equate the returned size with
  // residual.size(), so restrict the fold to the already-fully-covered case
  // and let it opt out of the residual-skip shortcut instead of risking a
  // coincidental size match.
  if (residual_fully_covered && !filter.disjunctive_branches.empty()) {
    const auto same_test = [](const NullPeekCompare& a,
                              const NullPeekCompare& b) {
      return a.column == b.column && a.expect_null == b.expect_null;
    };
    std::vector<NullPeekCompare> common =
        collect(filter.disjunctive_branches.front().residual);
    for (size_t b = 1;
         b < filter.disjunctive_branches.size() && !common.empty(); ++b) {
      const auto branch_tests =
          collect(filter.disjunctive_branches[b].residual);
      std::erase_if(common, [&](const NullPeekCompare& t) {
        return !std::ranges::any_of(
            branch_tests,
            [&](const NullPeekCompare& o) { return same_test(t, o); });
      });
    }
    for (const NullPeekCompare& t : common) {
      if (!std::ranges::any_of(null_tests, [&](const NullPeekCompare& o) {
            return same_test(t, o);
          })) {
        null_tests.push_back(t);
      }
    }
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
    visited.reserve(filter.disjunctive_branches.front().simple.size());
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
      if (branch_ok) {
        for (const PrebuiltMembershipPredicate& pred :
             branch.prebuilt_membership) {
          if (!MatchPrebuiltMembership(match_row, pred)) {
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

namespace {

bool BytecodeColumnTruthy(const ColumnarColumn& col, size_t r) {
  if (!col.is_uniform) {
    return col.ValueAt(r).Truthy();
  }
  if (col.IsNull(r)) {
    return false;
  }
  switch (col.type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      return col.IntAt(r) != 0;
    case ValueType::kDouble:
      return col.DoubleAt(r) != 0.0;
    case ValueType::kVarChar:
      return !col.StringAt(r).empty();
    default:
      return false;
  }
}

template <typename PredFn>
void NarrowSelection(size_t nrows, std::vector<uint32_t>* sel, bool* has_sel,
                     PredFn&& pred) {
  if (!*has_sel) {
    sel->clear();
    for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
      if (pred(r)) {
        sel->push_back(r);
      }
    }
    *has_sel = true;
    return;
  }
  size_t out = 0;
  for (const uint32_t r : *sel) {
    if (pred(r)) {
      (*sel)[out++] = r;
    }
  }
  sel->resize(out);
}

bool ComputeColumnarUniformNonNull(const ColumnarTable& table) {
  if (table.total_rows == 0 || table.chunks.empty()) {
    return false;
  }
  size_t ncols = 0;
  for (const ColumnarChunk& chunk : table.chunks) {
    if (chunk.rows > 0) {
      ncols = chunk.columns.size();
      break;
    }
  }
  if (ncols == 0) {
    return false;
  }
  for (size_t c = 0; c < ncols; ++c) {
    bool saw_non_null = false;
    ValueType vt = ValueType::kNull;
    bool is_unsigned = false;
    bool is_interval = false;
    for (const ColumnarChunk& chunk : table.chunks) {
      if (chunk.rows == 0) {
        continue;
      }
      if (c >= chunk.columns.size()) {
        return false;
      }
      const ColumnarColumn& col = chunk.columns[c];
      if (!col.is_uniform) {
        return false;
      }
      if (col.type == ValueType::kNull || !col.AnyNonNull(chunk.rows)) {
        continue;
      }
      if (!saw_non_null) {
        saw_non_null = true;
        vt = col.type;
        is_unsigned = col.is_unsigned;
        is_interval = col.is_interval;
      } else if (col.type != vt || col.is_unsigned != is_unsigned ||
                 col.is_interval != is_interval) {
        return false;
      }
    }
    if (!saw_non_null) {
      return false;
    }
  }
  return true;
}

bool TryFilterColumnarTable(TransactionContext& context,
                            std::shared_ptr<const ColumnarTable> input_table,
                            const CompiledScanFilter& filter,
                            const Schema& match_schema, const Scope* outer,
                            const CteMap& ctes,
                            const ScanRuntimeFilter* runtime_filter,
                            Relation* result, Status* error,
                            bool parallel_disabled) {
  const ColumnarTable& input = *input_table;
  if (filter.needs_unsigned_tagging) {
    return false;
  }
  if (filter.compile_decided_reject) {
    auto empty = std::make_shared<ColumnarTable>();
    empty->keepalives = input.keepalives;
    result->SetColumnarTable(std::move(empty), false);
    return true;
  }
  const bool has_rf = runtime_filter != nullptr && runtime_filter->enabled();
  const bool filter_empty =
      !has_rf && filter.simple.empty() && filter.prefix_in.empty() &&
      filter.prebuilt_membership.empty() &&
      filter.prebuilt_scalar_compare.empty() &&
      filter.disjunctive_branches.empty() && filter.residual.empty();
  if (filter_empty || input.total_rows == 0 || input.chunks.empty()) {
    // Pass-through: the columnar table is immutable, so share the source's
    // storage instead of deep-copying every chunk and recomputing from the
    // duplicate.
    const bool uniform = ComputeColumnarUniformNonNull(input);
    result->SetColumnarTable(std::move(input_table), uniform);
    return true;
  }
  const WorkerScanFilterPrograms programs =
      CompileWorkerFilter(filter, match_schema);
  if (!programs.complete) {
    return false;
  }

  const size_t nchunks = input.chunks.size();
  const size_t max_workers =
      (!parallel_disabled && input.total_rows >= 16384 && nchunks >= 2)
          ? std::min<size_t>(
                {size_t{16},
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 nchunks, std::max<size_t>(1, input.total_rows / 4096)})
          : 1;
  const size_t workers = std::max<size_t>(1, max_workers);

  struct DeferredChunkRow {
    size_t preceding_rows;
    Row row;
  };
  struct DictFilterCacheEntry {
    const void* pred_ptr{nullptr};
    const LSTableColumnDictionary* dict{nullptr};
    std::vector<uint8_t> pass;
    bool supported{true};
  };
  // Bound the per-worker cache: chunk-dict pairs only ever add entries, and
  // lookups scan the vector linearly.  Restart the cache past the cap to
  // keep both memory and probe cost bounded.
  constexpr size_t kMaxDictFilterCacheEntries = 32;
  std::vector<ColumnarTable> worker_tables(workers);
  std::vector<std::vector<DeferredChunkRow>> deferred(workers);
  std::vector<std::vector<DictFilterCacheEntry>> worker_dict_caches(workers);
  std::atomic<size_t> key_rejected{0};
  std::atomic<size_t> key_null_rejected{0};

  const auto eval_rf_col = [&](const ColumnarChunk& chunk,
                               const ScanRuntimeFilter& rf, bool count_nulls,
                               std::vector<uint32_t>* sel, bool* has_sel,
                               size_t* rej, size_t* null_rej) {
    if (rf.column >= chunk.columns.size()) {
      return;
    }
    const ColumnarColumn& col = chunk.columns[rf.column];
    const size_t nrows = chunk.rows;
    if (col.is_uniform && col.type == ValueType::kNull) {
      if (rf.Reject(static_cast<int8_t>(ValueType::kNull), true, 0)) {
        const size_t n = *has_sel ? sel->size() : nrows;
        *rej += n;
        if (count_nulls) {
          *null_rej += n;
        }
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      if (rf.Reject(static_cast<int8_t>(col.type), col.const_is_null,
                    col.const_is_null ? 0 : col.const_int)) {
        const size_t n = *has_sel ? sel->size() : nrows;
        *rej += n;
        if (count_nulls && col.const_is_null) {
          *null_rej += n;
        }
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.IsFlat() && col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      const auto vt = static_cast<int8_t>(col.type);
      const bool has_null = col.has_null;
      const uint8_t* nulls = col.nulls.data();
      const int64_t* ints = col.ints.data();
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        const bool is_null = has_null && nulls[r] != 0;
        if (rf.Reject(vt, is_null, is_null ? 0 : ints[r])) {
          ++(*rej);
          if (count_nulls && is_null) {
            ++(*null_rej);
          }
          return false;
        }
        return true;
      });
      return;
    }
    if (col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      const auto vt = static_cast<int8_t>(col.type);
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        const bool is_null = col.IsNull(r);
        if (rf.Reject(vt, is_null, is_null ? 0 : col.IntAt(r))) {
          ++(*rej);
          if (count_nulls && is_null) {
            ++(*null_rej);
          }
          return false;
        }
        return true;
      });
      return;
    }
    Value kv;
    NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &kv);
      if (rf.Reject(static_cast<int8_t>(kv.type), kv.IsNull(),
                    kv.value.int_value)) {
        ++(*rej);
        if (count_nulls && kv.IsNull()) {
          ++(*null_rej);
        }
        return false;
      }
      return true;
    });
  };

  const auto apply_simple_pred = [&](size_t worker_id,
                                     const ColumnarChunk& chunk,
                                     const SimpleComparePredicate& pred,
                                     Row* scratch_row,
                                     std::vector<uint32_t>* sel,
                                     bool* has_sel) {
    const size_t nrows = chunk.rows;
    if (pred.column_payload || pred.column >= chunk.columns.size()) {
      const size_t need = std::max<size_t>(pred.column, pred.right_column) + 1;
      if (scratch_row->values_.size() < need) {
        scratch_row->values_.resize(need);
      }
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        if (pred.column < chunk.columns.size()) {
          chunk.columns[pred.column].WriteValueInto(
              r, &scratch_row->values_[pred.column]);
        }
        if (pred.column_payload && pred.right_column < chunk.columns.size()) {
          chunk.columns[pred.right_column].WriteValueInto(
              r, &scratch_row->values_[pred.right_column]);
        }
        return MatchSimpleCompare(*scratch_row, pred);
      });
      return;
    }
    if (pred.constant.IsNull()) {
      sel->clear();
      *has_sel = true;
      return;
    }
    const ColumnarColumn& col = chunk.columns[pred.column];
    if (col.is_uniform && col.type == ValueType::kNull) {
      sel->clear();
      *has_sel = true;
      return;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform) {
      if (col.const_is_null) {
        sel->clear();
        *has_sel = true;
        return;
      }
      if (scratch_row->values_.size() <= pred.column) {
        scratch_row->values_.resize(pred.column + 1);
      }
      col.WriteValueInto(0, &scratch_row->values_[pred.column]);
      if (!MatchSimpleCompare(*scratch_row, pred)) {
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.is_uniform && !col.is_unsigned &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
        (pred.int_payload ||
         (col.type == ValueType::kDate && pred.date_string_payload))) {
      const int64_t rhs = pred.int_constant;
      const BinaryOperation op = pred.op;
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const int64_t* ints = col.ints.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          return ComparePositional(op, ints[r], rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          return ComparePositional(op, col.IntAt(r), rhs);
        });
      }
      return;
    }
    if (col.is_uniform && col.type == ValueType::kDouble &&
        pred.double_payload) {
      const double rhs = pred.double_constant;
      const BinaryOperation op = pred.op;
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const double* doubles = col.doubles.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          return ComparePositional(op, doubles[r], rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          return ComparePositional(op, col.DoubleAt(r), rhs);
        });
      }
      return;
    }
    if (col.is_uniform && !col.is_interval && col.type == ValueType::kVarChar &&
        pred.constant.type == ValueType::kVarChar &&
        !pred.constant.IsCaseInsensitive() &&
        (pred.plain_string_payload || pred.op == BinaryOperation::kLike ||
         pred.op == BinaryOperation::kNotLike)) {
      const std::string_view rhs = pred.constant.value.varchar_value;
      const BinaryOperation op = pred.op;
      if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
          col.dict != nullptr) {
        auto& cache = worker_dict_caches[worker_id];
        const DictFilterCacheEntry* cached = nullptr;
        for (const auto& entry : cache) {
          if (entry.pred_ptr == &pred && entry.dict == col.dict) {
            cached = &entry;
            break;
          }
        }
        if (cached == nullptr) {
          DictFilterCacheEntry fresh;
          fresh.pred_ptr = &pred;
          fresh.dict = col.dict;
          const size_t ec = col.dict->StringEntryCount();
          // Every slot is written by the scan below; reserve avoids a
          // zero-fill that the overwrite would make redundant.
          fresh.pass.reserve(ec);
          for (size_t code = 0; code < ec; ++code) {
            const std::string_view lhs = col.dict->StringEntryAt(code);
            bool ok = false;
            if (op == BinaryOperation::kLike ||
                op == BinaryOperation::kNotLike) {
              const bool m = FastLikeMatch(lhs, rhs);
              ok = op == BinaryOperation::kLike ? m : !m;
            } else {
              ok = ComparePositional(op, lhs, rhs);
            }
            fresh.pass.push_back(ok ? 1 : 0);
          }
          if (cache.size() >= kMaxDictFilterCacheEntries) {
            cache.clear();
          }
          cache.push_back(std::move(fresh));
          cached = &cache.back();
        }
        const uint8_t* __restrict pass_arr = cached->pass.data();
        const size_t ec = cached->pass.size();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          const uint32_t code = col.DictCodeAt(r);
          return code < ec && pass_arr[code] != 0;
        });
        return;
      }
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const std::string_view* strs = col.strings.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          const std::string_view lhs = strs[r];
          if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
            const bool m = FastLikeMatch(lhs, rhs);
            return op == BinaryOperation::kLike ? m : !m;
          }
          return ComparePositional(op, lhs, rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          const std::string_view lhs = col.StringAt(r);
          if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
            const bool m = FastLikeMatch(lhs, rhs);
            return op == BinaryOperation::kLike ? m : !m;
          }
          return ComparePositional(op, lhs, rhs);
        });
      }
      return;
    }
    if (scratch_row->values_.size() <= pred.column) {
      scratch_row->values_.resize(pred.column + 1);
    }
    NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &scratch_row->values_[pred.column]);
      return MatchSimpleCompare(*scratch_row, pred);
    });
  };

  const auto apply_prefix_in = [&](size_t worker_id, const ColumnarChunk& chunk,
                                   const PrefixInPredicate& pred,
                                   std::vector<uint32_t>* sel, bool* has_sel) {
    if (pred.column >= chunk.columns.size()) {
      sel->clear();
      *has_sel = true;
      return;
    }
    const ColumnarColumn& col = chunk.columns[pred.column];
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform) {
      if (col.const_is_null || col.type != ValueType::kVarChar ||
          !std::ranges::any_of(pred.prefixes, [&](const std::string& p) {
            return col.const_string.starts_with(p);
          })) {
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.is_uniform && col.type == ValueType::kVarChar &&
        col.encoding == ColumnarColumn::Encoding::kDictionary &&
        col.dict != nullptr) {
      auto& cache = worker_dict_caches[worker_id];
      const DictFilterCacheEntry* cached = nullptr;
      for (const auto& entry : cache) {
        if (entry.pred_ptr == &pred && entry.dict == col.dict) {
          cached = &entry;
          break;
        }
      }
      if (cached == nullptr) {
        DictFilterCacheEntry fresh;
        fresh.pred_ptr = &pred;
        fresh.dict = col.dict;
        const size_t ec = col.dict->StringEntryCount();
        // The loop writes every slot; skip the zero-fill.
        fresh.pass.reserve(ec);
        for (size_t code = 0; code < ec; ++code) {
          const std::string_view sv = col.dict->StringEntryAt(code);
          fresh.pass.push_back(std::ranges::any_of(pred.prefixes,
                                                   [&](const std::string& p) {
                                                     return sv.starts_with(p);
                                                   })
                                   ? 1
                                   : 0);
        }
        if (cache.size() >= kMaxDictFilterCacheEntries) {
          cache.clear();
        }
        cache.push_back(std::move(fresh));
        cached = &cache.back();
      }
      const uint8_t* __restrict pass_arr = cached->pass.data();
      const size_t ec = cached->pass.size();
      NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
        const uint32_t code = col.DictCodeAt(r);
        return code < ec && pass_arr[code] != 0;
      });
      return;
    }
    if (col.is_uniform && col.type == ValueType::kVarChar) {
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const std::string_view* strs = col.strings.data();
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          const std::string_view sv = strs[r];
          return std::ranges::any_of(pred.prefixes, [&](const std::string& p) {
            return sv.starts_with(p);
          });
        });
      } else {
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          const std::string_view sv = col.StringAt(r);
          return std::ranges::any_of(pred.prefixes, [&](const std::string& p) {
            return sv.starts_with(p);
          });
        });
      }
      return;
    }
    if (col.is_uniform && col.type != ValueType::kVarChar) {
      sel->clear();
      *has_sel = true;
      return;
    }
    Value v;
    NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &v);
      if (v.IsNull() || v.type != ValueType::kVarChar) {
        return false;
      }
      return std::ranges::any_of(pred.prefixes, [&](const std::string& p) {
        return v.value.varchar_value.starts_with(p);
      });
    });
  };

  const auto apply_prebuilt_membership =
      [&](size_t worker_id, const ColumnarChunk& chunk,
          const PrebuiltMembershipPredicate& pred, Row* scratch_row,
          std::vector<BytecodeColumnView>* views,
          std::vector<ColumnarColumn>* view_scratches,
          std::vector<uint8_t>* flat_view_ready, std::vector<uint32_t>* sel,
          bool* has_sel) {
        if (pred.column >= chunk.columns.size()) {
          sel->clear();
          *has_sel = true;
          return;
        }
        const ColumnarColumn& col = chunk.columns[pred.column];
        const auto resolve_flat_int = [&](bool* has_null_out,
                                          const uint8_t** nulls_out,
                                          const int64_t** ints_out) -> bool {
          if (col.IsFlat()) {
            *has_null_out = col.has_null && !col.nulls.empty();
            *nulls_out = col.nulls.data();
            *ints_out = col.ints.data();
            return true;
          }
          if (views != nullptr && view_scratches != nullptr &&
              flat_view_ready != nullptr &&
              pred.column < flat_view_ready->size()) {
            if ((*flat_view_ready)[pred.column] == 0) {
              (*views)[pred.column] = col.ToBytecodeView(
                  chunk.rows, &(*view_scratches)[pred.column], nullptr);
              (*flat_view_ready)[pred.column] = 1;
            }
            const BytecodeColumnView& v = (*views)[pred.column];
            if (v.is_uniform && v.ints != nullptr) {
              *has_null_out = v.has_null && v.nulls != nullptr;
              *nulls_out = v.nulls;
              *ints_out = v.ints;
              return true;
            }
          }
          return false;
        };
        if (col.is_uniform && pred.runtime_filter != nullptr &&
            col.type == pred.domain_type && !col.is_unsigned) {
          const RuntimeFilterGroup& rf = *pred.runtime_filter;
          if (rf.RejectsAll()) {
            sel->clear();
            *has_sel = true;
            return;
          }
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            if (col.const_is_null || rf.Reject(col.const_int)) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          bool has_null = false;
          const uint8_t* nulls = nullptr;
          const int64_t* ints = nullptr;
          if (resolve_flat_int(&has_null, &nulls, &ints)) {
            if (rf.has_dense_bits() && rf.has_bounds() && rf.Hi() >= rf.Lo()) {
              const auto rbase = static_cast<uint64_t>(rf.Lo());
              const uint64_t rspan_m1 = static_cast<uint64_t>(rf.Hi()) - rbase;
              if ((rspan_m1 >> 6) < rf.dense_bits_word_count()) {
                const uint64_t* __restrict dbits = rf.dense_bits_data();
                if (!has_null) {
                  NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
                    const uint64_t off = static_cast<uint64_t>(ints[r]) - rbase;
                    return off <= rspan_m1 &&
                           (dbits[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
                  });
                } else {
                  NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
                    if (nulls[r] != 0) {
                      return false;
                    }
                    const uint64_t off = static_cast<uint64_t>(ints[r]) - rbase;
                    return off <= rspan_m1 &&
                           (dbits[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
                  });
                }
                return;
              }
            }
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              return (!has_null || nulls[r] == 0) && !rf.Reject(ints[r]);
            });
          } else {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              return !col.IsNull(r) && !rf.Reject(col.IntAt(r));
            });
          }
          return;
        }
        if (col.is_uniform && !col.is_interval &&
            col.type == ValueType::kVarChar && pred.string_values &&
            !pred.string_values->empty() &&
            col.encoding == ColumnarColumn::Encoding::kDictionary &&
            col.dict != nullptr) {
          auto& cache = worker_dict_caches[worker_id];
          const DictFilterCacheEntry* cached = nullptr;
          for (const auto& entry : cache) {
            if (entry.pred_ptr == &pred && entry.dict == col.dict) {
              cached = &entry;
              break;
            }
          }
          if (cached == nullptr) {
            DictFilterCacheEntry fresh;
            fresh.pred_ptr = &pred;
            fresh.dict = col.dict;
            const size_t ec = col.dict->StringEntryCount();
            // Slots are appended as they are decided; an early bail leaves
            // `supported` false and the table is never consulted.
            fresh.pass.reserve(ec);
            for (size_t code = 0; code < ec; ++code) {
              const std::string_view sv = col.dict->StringEntryAt(code);
              if (!PlainStringComparable(sv)) {
                fresh.supported = false;
                break;
              }
              const bool found = pred.string_values->contains(sv);
              const bool ok =
                  found ? !pred.negated : (!pred.contains_null && pred.negated);
              fresh.pass.push_back(ok ? 1 : 0);
            }
            if (cache.size() >= kMaxDictFilterCacheEntries) {
              cache.clear();
            }
            cache.push_back(std::move(fresh));
            cached = &cache.back();
          }
          if (cached->supported) {
            const uint8_t* __restrict pass_arr = cached->pass.data();
            const size_t ec = cached->pass.size();
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              const uint32_t code = col.DictCodeAt(r);
              return code < ec && pass_arr[code] != 0;
            });
            return;
          }
        }
        if (pred.packed_pair_col2.has_value() && pred.values != nullptr) {
          const slot_t c2 = *pred.packed_pair_col2;
          if (c2 >= chunk.columns.size()) {
            if (!pred.negated) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          if (pred.values->Empty() && !pred.contains_null) {
            if (!pred.negated) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          const ColumnarColumn& col2 = chunk.columns[c2];
          if (col.is_uniform && !col.is_unsigned &&
              col.type == pred.domain_type && col2.is_uniform &&
              !col2.is_unsigned && col2.type == pred.packed_pair_type2) {
            const FlatIntSet& values = *pred.values;
            const bool neg = pred.negated;
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (col.IsNull(r) || col2.IsNull(r)) {
                return neg;
              }
              const int64_t k0 = col.IntAt(r);
              const int64_t k1 = col2.IntAt(r);
              if (!std::in_range<int32_t>(k0) || !std::in_range<int32_t>(k1)) {
                return neg;
              }
              const uint64_t packed =
                  (static_cast<uint64_t>(
                       static_cast<uint32_t>(static_cast<int32_t>(k0)))
                   << 32) |
                  static_cast<uint32_t>(static_cast<int32_t>(k1));
              const bool found =
                  values.Contains(std::bit_cast<int64_t>(packed));
              return neg ? !found : found;
            });
            return;
          }
        }
        if (col.is_uniform && pred.runtime_filter == nullptr &&
            !pred.packed_pair_col2.has_value() &&
            pred.distinct_witness_minmax == nullptr && !pred.string_values &&
            pred.values != nullptr && col.type == pred.domain_type &&
            !col.is_unsigned && (!pred.values->Empty() || pred.contains_null)) {
          const FlatIntSet& values = *pred.values;
          const bool neg = pred.negated;
          const bool ex = pred.existence;
          const bool cnull = pred.contains_null;
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            const bool ok =
                col.const_is_null
                    ? (ex && neg)
                    : (values.Contains(col.const_int) ? !neg : (!cnull && neg));
            if (!ok) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          bool has_null = false;
          const uint8_t* nulls = nullptr;
          const int64_t* ints = nullptr;
          if (resolve_flat_int(&has_null, &nulls, &ints)) {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (has_null && nulls[r] != 0) {
                return ex && neg;
              }
              const bool found = values.Contains(ints[r]);
              return found ? !neg : (!cnull && neg);
            });
          } else {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (col.IsNull(r)) {
                return ex && neg;
              }
              const bool found = values.Contains(col.IntAt(r));
              return found ? !neg : (!cnull && neg);
            });
          }
          return;
        }
        slot_t max_col = pred.distinct_witness_minmax != nullptr
                             ? std::max(pred.column, pred.witness_column)
                             : pred.column;
        if (pred.packed_pair_col2.has_value()) {
          max_col = std::max(max_col, *pred.packed_pair_col2);
        }
        if (scratch_row->values_.size() <= max_col) {
          scratch_row->values_.resize(max_col + 1);
        }
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          col.WriteValueInto(r, &scratch_row->values_[pred.column]);
          if (pred.distinct_witness_minmax != nullptr &&
              pred.witness_column < chunk.columns.size()) {
            chunk.columns[pred.witness_column].WriteValueInto(
                r, &scratch_row->values_[pred.witness_column]);
          }
          if (pred.packed_pair_col2.has_value() &&
              *pred.packed_pair_col2 < chunk.columns.size()) {
            chunk.columns[*pred.packed_pair_col2].WriteValueInto(
                r, &scratch_row->values_[*pred.packed_pair_col2]);
          }
          return MatchPrebuiltMembership(*scratch_row, pred);
        });
      };

  const auto eval_chunk_worker = [&](size_t w) {
    const size_t cbegin = (nchunks * w) / workers;
    const size_t cend = (nchunks * (w + 1)) / workers;
    ColumnarChunk cur_chunk;
    bool cur_chunk_init = false;
    std::vector<std::shared_ptr<ChunkGatherIndex>> cur_gathers;
    std::vector<uint32_t> sel;
    std::vector<uint32_t> disj_sel;
    std::vector<uint8_t> disj_pass;
    std::vector<BytecodeColumnView> views;
    std::vector<ColumnarColumn> view_scratches;
    std::vector<uint8_t> flat_view_ready;
    ColumnarColumn truthy_col;
    Row scratch_row;
    Row full_row;
    size_t local_accepted_rows = 0;
    size_t local_rej = 0;
    size_t local_null_rej = 0;

    const bool can_columnar_disjunction =
        !filter.disjunctive_branches.empty() &&
        std::ranges::all_of(filter.disjunctive_branches, [](const auto& b) {
          return b.residual.empty() &&
                 std::ranges::all_of(b.simple, [](const auto& sp) {
                   return !sp.column_payload;
                 });
        });

    std::vector<uint8_t> residual_needed_cols;
    if (!filter.residual.empty() && programs.residual.has_value()) {
      for (const BytecodeInstruction& inst :
           programs.residual->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn) {
          if (residual_needed_cols.size() <= inst.operand) {
            residual_needed_cols.resize(inst.operand + 1, 0);
          }
          residual_needed_cols[inst.operand] = 1;
        }
      }
    }

    for (size_t ci = cbegin; ci < cend; ++ci) {
      const ColumnarChunk& chunk = input.chunks[ci];
      if (chunk.rows == 0) {
        continue;
      }
      const size_t nrows = chunk.rows;
      views.resize(chunk.columns.size());
      view_scratches.resize(chunk.columns.size());
      flat_view_ready.assign(chunk.columns.size(), 0);
      bool has_sel = false;
      sel.clear();
      const auto empty_now = [&]() { return has_sel && sel.empty(); };

      if (has_rf) {
        eval_rf_col(chunk, *runtime_filter, true, &sel, &has_sel, &local_rej,
                    &local_null_rej);
        if (empty_now()) {
          continue;
        }
        for (const ScanRuntimeFilter& ext : runtime_filter->extra) {
          if (!ext.enabled()) {
            continue;
          }
          eval_rf_col(chunk, ext, false, &sel, &has_sel, &local_rej,
                      &local_null_rej);
          if (empty_now()) {
            break;
          }
        }
        if (empty_now()) {
          continue;
        }
      }
      for (const SimpleComparePredicate& pred : filter.simple) {
        apply_simple_pred(w, chunk, pred, &scratch_row, &sel, &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrefixInPredicate& pred : filter.prefix_in) {
        apply_prefix_in(w, chunk, pred, &sel, &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrebuiltMembershipPredicate& pred :
           filter.prebuilt_membership) {
        apply_prebuilt_membership(w, chunk, pred, &scratch_row, &views,
                                  &view_scratches, &flat_view_ready, &sel,
                                  &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrebuiltScalarComparePredicate& pred :
           filter.prebuilt_scalar_compare) {
        const size_t need = std::max<size_t>(pred.column, pred.key_column) + 1;
        if (scratch_row.values_.size() < need) {
          scratch_row.values_.resize(need);
        }
        NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
          if (pred.column < chunk.columns.size()) {
            chunk.columns[pred.column].WriteValueInto(
                r, &scratch_row.values_[pred.column]);
          }
          if (pred.key_column < chunk.columns.size()) {
            chunk.columns[pred.key_column].WriteValueInto(
                r, &scratch_row.values_[pred.key_column]);
          }
          return MatchPrebuiltScalarCompare(scratch_row, pred);
        });
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }

      std::vector<uint32_t> chunk_deferred_rows;
      if (!filter.disjunctive_branches.empty()) {
        if (can_columnar_disjunction) {
          disj_pass.assign(nrows, 0);
          bool any_full_pass = false;
          for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
            const auto& branch = filter.disjunctive_branches[b];
            bool branch_has_sel = true;
            if (b == 0) {
              branch_has_sel = has_sel;
              if (has_sel) {
                disj_sel = sel;
              } else {
                disj_sel.clear();
              }
            } else {
              disj_sel.clear();
              if (has_sel) {
                for (uint32_t r : sel) {
                  if (disj_pass[r] == 0) {
                    disj_sel.push_back(r);
                  }
                }
              } else {
                for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
                  if (disj_pass[r] == 0) {
                    disj_sel.push_back(r);
                  }
                }
              }
              if (disj_sel.empty()) {
                break;
              }
            }
            const auto branch_empty = [&]() {
              return branch_has_sel && disj_sel.empty();
            };
            for (const SimpleComparePredicate& sp : branch.simple) {
              apply_simple_pred(w, chunk, sp, &scratch_row, &disj_sel,
                                &branch_has_sel);
              if (branch_empty()) {
                break;
              }
            }
            if (!branch_empty()) {
              for (const PrefixInPredicate& pp : branch.prefix_in) {
                apply_prefix_in(w, chunk, pp, &disj_sel, &branch_has_sel);
                if (branch_empty()) {
                  break;
                }
              }
            }
            if (!branch_empty()) {
              for (const PrebuiltMembershipPredicate& pm :
                   branch.prebuilt_membership) {
                apply_prebuilt_membership(w, chunk, pm, &scratch_row, &views,
                                          &view_scratches, &flat_view_ready,
                                          &disj_sel, &branch_has_sel);
                if (branch_empty()) {
                  break;
                }
              }
            }
            if (!branch_has_sel) {
              any_full_pass = true;
              break;
            }
            for (uint32_t idx : disj_sel) {
              disj_pass[idx] = 1;
            }
          }
          if (!any_full_pass) {
            NarrowSelection(nrows, &sel, &has_sel,
                            [&](uint32_t r) { return disj_pass[r] != 0; });
          }
        } else {
          NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
            bool any_branch_passed = false;
            for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
              const auto& branch = filter.disjunctive_branches[b];
              bool branch_ok = true;
              for (const SimpleComparePredicate& sp : branch.simple) {
                const size_t need =
                    std::max<size_t>(sp.column, sp.right_column) + 1;
                if (scratch_row.values_.size() < need) {
                  scratch_row.values_.resize(need);
                }
                if (sp.column < chunk.columns.size()) {
                  chunk.columns[sp.column].WriteValueInto(
                      r, &scratch_row.values_[sp.column]);
                }
                if (sp.column_payload &&
                    sp.right_column < chunk.columns.size()) {
                  chunk.columns[sp.right_column].WriteValueInto(
                      r, &scratch_row.values_[sp.right_column]);
                }
                if (!MatchSimpleCompare(scratch_row, sp)) {
                  branch_ok = false;
                  break;
                }
              }
              if (branch_ok) {
                for (const PrefixInPredicate& pp : branch.prefix_in) {
                  if (scratch_row.values_.size() <= pp.column) {
                    scratch_row.values_.resize(pp.column + 1);
                  }
                  if (pp.column < chunk.columns.size()) {
                    chunk.columns[pp.column].WriteValueInto(
                        r, &scratch_row.values_[pp.column]);
                  }
                  if (!MatchPrefixIn(scratch_row, pp)) {
                    branch_ok = false;
                    break;
                  }
                }
              }
              if (branch_ok) {
                for (const PrebuiltMembershipPredicate& pm :
                     branch.prebuilt_membership) {
                  slot_t max_col = pm.distinct_witness_minmax != nullptr
                                       ? std::max(pm.column, pm.witness_column)
                                       : pm.column;
                  if (pm.packed_pair_col2.has_value()) {
                    max_col = std::max(max_col, *pm.packed_pair_col2);
                  }
                  if (scratch_row.values_.size() <= max_col) {
                    scratch_row.values_.resize(max_col + 1);
                  }
                  if (pm.column < chunk.columns.size()) {
                    chunk.columns[pm.column].WriteValueInto(
                        r, &scratch_row.values_[pm.column]);
                  }
                  if (pm.distinct_witness_minmax != nullptr &&
                      pm.witness_column < chunk.columns.size()) {
                    chunk.columns[pm.witness_column].WriteValueInto(
                        r, &scratch_row.values_[pm.witness_column]);
                  }
                  if (pm.packed_pair_col2.has_value() &&
                      *pm.packed_pair_col2 < chunk.columns.size()) {
                    chunk.columns[*pm.packed_pair_col2].WriteValueInto(
                        r, &scratch_row.values_[*pm.packed_pair_col2]);
                  }
                  if (!MatchPrebuiltMembership(scratch_row, pm)) {
                    branch_ok = false;
                    break;
                  }
                }
              }
              if (branch_ok && !branch.residual.empty()) {
                chunk.MaterializeRowInto(r, &full_row);
                StatusOr<Value> decided =
                    TryEvaluateRowAuto(*programs.branch_residuals[b], full_row);
                if (!decided.HasValue()) {
                  chunk_deferred_rows.push_back(r);
                  return false;
                }
                if (!Truthy(decided.Value())) {
                  branch_ok = false;
                }
              }
              if (branch_ok) {
                any_branch_passed = true;
                break;
              }
            }
            return any_branch_passed;
          });
        }
      }

      if (!filter.residual.empty() && !empty_now()) {
        const size_t active_count = has_sel ? sel.size() : nrows;
        for (size_t c = 0; c < chunk.columns.size(); ++c) {
          if (c < residual_needed_cols.size() && residual_needed_cols[c] != 0) {
            if (flat_view_ready[c] == 0) {
              views[c] = chunk.columns[c].ToBytecodeView(
                  chunk.rows, &view_scratches[c], nullptr);
              flat_view_ready[c] = 1;
            }
          } else {
            views[c] = BytecodeColumnView{};
          }
        }
        truthy_col.Init(ValueType::kNull, false, false, active_count);
        BytecodeColumnSink sink = truthy_col.AsBytecodeSink(0);
        Status eval_err{Status::kSuccess};
        const auto st = programs.residual->TryEvaluateColumnarBatch(
            views, has_sel ? sel.data() : nullptr, active_count, &sink,
            &eval_err);
        if (st == BytecodeProgram::FastEval::kDone) {
          truthy_col.SyncFromBytecodeSink(sink, nullptr, active_count);
          if (!has_sel) {
            sel.clear();
            for (size_t i = 0; i < active_count; ++i) {
              if (BytecodeColumnTruthy(truthy_col, i)) {
                sel.push_back(static_cast<uint32_t>(i));
              }
            }
            has_sel = true;
          } else {
            size_t out = 0;
            for (size_t i = 0; i < active_count; ++i) {
              if (BytecodeColumnTruthy(truthy_col, i)) {
                sel[out++] = sel[i];
              }
            }
            sel.resize(out);
          }
        } else {
          NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
            chunk.MaterializeRowInto(r, &full_row);
            StatusOr<Value> decided =
                TryEvaluateRowAuto(*programs.residual, full_row);
            if (!decided.HasValue()) {
              chunk_deferred_rows.push_back(r);
              return false;
            }
            return Truthy(decided.Value());
          });
        }
      }

      if (!chunk_deferred_rows.empty()) {
        std::sort(chunk_deferred_rows.begin(), chunk_deferred_rows.end());
        size_t def_idx = 0;
        const auto push_def = [&](Row&& r, size_t before_acc) {
          deferred[w].push_back(DeferredChunkRow{.preceding_rows = before_acc,
                                                 .row = std::move(r)});
        };
        const size_t acc_cnt = has_sel ? sel.size() : nrows;
        for (size_t i = 0; i < acc_cnt; ++i) {
          const uint32_t r = has_sel ? sel[i] : static_cast<uint32_t>(i);
          while (def_idx < chunk_deferred_rows.size() &&
                 chunk_deferred_rows[def_idx] < r) {
            chunk.MaterializeRowInto(chunk_deferred_rows[def_idx++], &full_row);
            push_def(std::move(full_row), local_accepted_rows + i);
            full_row = Row{};
          }
        }
        while (def_idx < chunk_deferred_rows.size()) {
          chunk.MaterializeRowInto(chunk_deferred_rows[def_idx++], &full_row);
          push_def(std::move(full_row), local_accepted_rows + acc_cnt);
          full_row = Row{};
        }
      }

      const size_t keep_count = has_sel ? sel.size() : nrows;
      if (keep_count == 0) {
        continue;
      }
      if (!has_sel && keep_count == nrows) {
        if (cur_chunk_init && cur_chunk.rows > 0) {
          worker_tables[w].AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_chunk_init = false;
          cur_gathers.clear();
        }
        worker_tables[w].AppendChunk(ColumnarChunk(chunk));
        local_accepted_rows += keep_count;
        continue;
      }
      const size_t ncols = chunk.columns.size();
      const uint32_t* sel_ptr = has_sel ? sel.data() : nullptr;
      size_t offset = 0;
      const auto append_keepalive_unique =
          [&](const std::shared_ptr<const void>& k) {
            if (k == nullptr) {
              return;
            }
            for (const auto& existing : cur_chunk.keepalives) {
              if (existing.get() == k.get()) {
                return;
              }
            }
            cur_chunk.keepalives.push_back(k);
          };
      while (offset < keep_count) {
        if (!cur_chunk_init) {
          cur_chunk.InitFromSchema(match_schema, 0);
          cur_chunk_init = true;
          cur_gathers.clear();
        }
        const size_t space = kColumnarChunkRows - cur_chunk.rows;
        const size_t n = std::min(space, keep_count - offset);
        const size_t prev_rows = cur_chunk.rows;
        if (sel_ptr == nullptr && offset > 0) {
          sel.resize(keep_count);
          for (size_t i = 0; i < keep_count; ++i) {
            sel[i] = static_cast<uint32_t>(i);
          }
          sel_ptr = sel.data();
        }
        const uint32_t* slice_sel =
            sel_ptr != nullptr ? (sel_ptr + offset) : nullptr;
        std::vector<std::pair<const ChunkGatherIndex*,
                              std::shared_ptr<ChunkGatherIndex>>>
            slice_gi_map;
        const auto get_filtered_gather =
            [&](const ColumnarColumn& src_col,
                size_t col_idx) -> std::shared_ptr<ChunkGatherIndex> {
          const ChunkGatherIndex* src_gi = src_col.gather_index.get();
          for (const auto& entry : slice_gi_map) {
            if (entry.first == src_gi) {
              return entry.second;
            }
          }
          std::shared_ptr<ChunkGatherIndex> dst_gi;
          if (prev_rows == 0) {
            dst_gi = std::make_shared<ChunkGatherIndex>();
            dst_gi->source_table = src_gi->source_table;
            dst_gi->src_chunks.reserve(n);
            dst_gi->src_rows.reserve(n);
            cur_gathers.push_back(dst_gi);
          } else if (cur_chunk.columns[col_idx].encoding ==
                         ColumnarColumn::Encoding::kGathered &&
                     cur_chunk.columns[col_idx].gather_index != nullptr) {
            const ChunkGatherIndex* prev_gi_ptr =
                cur_chunk.columns[col_idx].gather_index.get();
            for (const auto& cg : cur_gathers) {
              if (cg.get() == prev_gi_ptr &&
                  cg->source_table == src_gi->source_table &&
                  cg->src_rows.size() == prev_rows) {
                dst_gi = cg;
                break;
              }
            }
          }
          if (dst_gi == nullptr) {
            return nullptr;
          }
          const size_t before = dst_gi->src_rows.size();
          dst_gi->src_chunks.resize(before + n);
          dst_gi->src_rows.resize(before + n);
          bool found_null = dst_gi->has_null;
          for (size_t i = 0; i < n; ++i) {
            const uint32_t r =
                slice_sel != nullptr ? slice_sel[i] : static_cast<uint32_t>(i);
            const uint32_t sr = src_gi->src_rows[r];
            dst_gi->src_chunks[before + i] = src_gi->src_chunks[r];
            dst_gi->src_rows[before + i] = sr;
            if (sr == ChunkGatherIndex::kNullRow) {
              found_null = true;
            }
          }
          dst_gi->has_null = found_null;
          slice_gi_map.emplace_back(src_gi, dst_gi);
          return dst_gi;
        };
        for (size_t c = 0; c < ncols; ++c) {
          const ColumnarColumn& sc = chunk.columns[c];
          if (sc.encoding == ColumnarColumn::Encoding::kGathered &&
              sc.gather_index != nullptr &&
              (prev_rows == 0 || cur_chunk.columns[c].encoding ==
                                     ColumnarColumn::Encoding::kGathered)) {
            if (auto dst_gi = get_filtered_gather(sc, c)) {
              cur_chunk.columns[c].SetGathered(
                  std::move(dst_gi), sc.gather_col, sc.type, sc.is_unsigned,
                  sc.is_interval, sc.is_uniform, sc.has_null);
              continue;
            }
          }
          cur_chunk.columns[c].AppendSliceFrom(sc, slice_sel, n, nullptr,
                                               prev_rows);
        }
        append_keepalive_unique(chunk.string_arena);
        for (const auto& k : chunk.keepalives) {
          append_keepalive_unique(k);
        }
        cur_chunk.rows += n;
        offset += n;
        if (cur_chunk.rows == kColumnarChunkRows) {
          worker_tables[w].AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_chunk_init = false;
          cur_gathers.clear();
        }
      }
      local_accepted_rows += keep_count;
    }
    if (cur_chunk_init && cur_chunk.rows > 0) {
      worker_tables[w].AppendChunk(std::move(cur_chunk));
    }
    if (has_rf) {
      key_rejected.fetch_add(local_rej, std::memory_order_relaxed);
      key_null_rejected.fetch_add(local_null_rej, std::memory_order_relaxed);
    }
  };

  if (workers <= 1) {
    eval_chunk_worker(0);
  } else {
    const Status st = RunParallelTasks(workers, eval_chunk_worker);
    if (!st.ok()) {
      if (error != nullptr && error->ok()) {
        *error = st;
      }
      return true;
    }
  }

  auto out = std::make_shared<ColumnarTable>();
  out->keepalives = input.keepalives;
  const bool any_deferred =
      std::ranges::any_of(deferred, [](const auto& d) { return !d.empty(); });
  if (!any_deferred) {
    for (size_t w = 0; w < workers; ++w) {
      out->AppendTable(std::move(worker_tables[w]));
    }
  } else {
    const auto record_error = [&](Status status) {
      if (error != nullptr && error->ok() && status != Status::kSuccess) {
        *error = std::move(status);
      }
    };
    ColumnarChunk merged_chunk;
    bool merged_chunk_init = false;
    const auto emit_row = [&](const Row& r) {
      if (!merged_chunk_init) {
        merged_chunk.InitFromSchema(match_schema, kColumnarChunkRows);
        merged_chunk_init = true;
      }
      merged_chunk.AppendRow(r);
      if (merged_chunk.rows == kColumnarChunkRows) {
        out->AppendChunk(std::move(merged_chunk));
        merged_chunk = ColumnarChunk{};
        merged_chunk_init = false;
      }
    };
    for (size_t w = 0; w < workers; ++w) {
      if (deferred[w].empty()) {
        if (merged_chunk_init && merged_chunk.rows > 0) {
          out->AppendChunk(std::move(merged_chunk));
          merged_chunk = ColumnarChunk{};
          merged_chunk_init = false;
        }
        out->AppendTable(std::move(worker_tables[w]));
        continue;
      }
      size_t accepted = 0;
      size_t di = 0;
      const auto emit_def = [&] {
        while (di < deferred[w].size() &&
               deferred[w][di].preceding_rows == accepted) {
          Row& r = deferred[w][di++].row;
          Status def_err{Status::kSuccess};
          if (MatchScanFilter(r, match_schema, filter, outer, context, ctes,
                              &def_err)) {
            emit_row(r);
          }
          record_error(std::move(def_err));
        }
      };
      Row r_tmp;
      for (const ColumnarChunk& wc : worker_tables[w].chunks) {
        for (size_t r = 0; r < wc.rows; ++r) {
          emit_def();
          wc.MaterializeRowInto(r, &r_tmp);
          emit_row(r_tmp);
          ++accepted;
        }
      }
      emit_def();
      deferred[w].clear();
    }
    if (merged_chunk_init && merged_chunk.rows > 0) {
      out->AppendChunk(std::move(merged_chunk));
    }
  }
  const bool out_uniform = ComputeColumnarUniformNonNull(*out);
  result->SetColumnarTable(std::move(out), out_uniform);
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rejected.load();
    context.execution_runtime()->key_filter_null_rejected +=
        key_null_rejected.load();
  }
  return true;
}

}  // namespace

bool TryParallelFilterRows(TransactionContext& context, const Relation& source,
                           const CompiledScanFilter& filter,
                           const Schema& match_schema, const Scope* outer,
                           const CteMap& ctes,
                           const ScanRuntimeFilter* runtime_filter,
                           Relation* result, Status* error,
                           Relation* movable_source) {
  constexpr size_t kMinParallelFilterRows = 16384;
  constexpr size_t kMaxParallelFilterWorkers = 16;
  static const bool parallel_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER") != nullptr;
  if (source.HasSpill() ||
      (movable_source != nullptr && movable_source != &source)) {
    return false;
  }
  if (source.HasColumnarOnly()) {
    return TryFilterColumnarTable(context, source.columnar, filter,
                                  match_schema, outer, ctes, runtime_filter,
                                  result, error, parallel_filter_disabled);
  }
  if (parallel_filter_disabled || source.rows.size() < kMinParallelFilterRows) {
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
                source.rows.size() / 4096});
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
  Status error_ex{Status::kSuccess};
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
            // order, including rows that need contextual evaluation.
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
            if (error_ex == Status::kSuccess) {
              error_ex =
                  StatusFromCurrentException("parallel filter worker failed");
            }
          }
        });
      } catch (...) {
        // Thread *creation* failed, not the worker body. Record it, or the
        // merge below drains empty shards and reports a clean result that is
        // silently missing every row the missing worker would have produced.
        std::scoped_lock lock(error_mu);
        if (error_ex == Status::kSuccess) {
          error_ex =
              StatusFromCurrentException("parallel filter worker failed");
        }
        break;
      }
    }
  }
  if (error_ex != Status::kSuccess) {
    if (error != nullptr && error->ok()) {
      *error = error_ex;
    }
    return true;
  }
  // Main-thread merge in worker order; deferred rows take the contextual
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
      current.reserve(elem_sql_type.size() - open - 1);
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
      std::vector<std::pair<size_t, Value>> cells;
      bool null_row{false};
    };
    // Name-aligned flattening: when the declared struct names every field
    // AND the encoded members carry those names, each member is placed at
    // its declared position (a JSON document may serialize members in any
    // order). Anonymous fN encodings and positional members keep the
    // serialized-order behaviour.
    const bool declared_named =
        declared_struct && !declared_fields.empty() &&
        std::ranges::none_of(declared_fields,
                             [](const std::pair<std::string, std::string>& f) {
                               return f.first.empty();
                             });
    // Flattening mode, decided from the first parsed element: 1 = place
    // members by declared name, 0 = keep serialized order.
    int align_mode = -1;
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
      const bool row_named =
          declared_named &&
          std::ranges::all_of(
              members, [&](const std::pair<std::string, std::string>& member) {
                return std::ranges::any_of(
                    declared_fields,
                    [&](const std::pair<std::string, std::string>& field) {
                      return field.first == member.first;
                    });
              });
      if (align_mode < 0) {
        align_mode = row_named ? 1 : 0;
      }
      const bool aligned = align_mode == 1 && row_named;
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
        size_t cell_index = parsed_rows[row_idx].cells.size();
        std::string declared_type;
        if (aligned) {
          const auto fit = std::ranges::find(
              declared_fields, key,
              [](const std::pair<std::string, std::string>& f) {
                return f.first;
              });
          cell_index = static_cast<size_t>(fit - declared_fields.begin());
          declared_type = fit->second;
        } else if (declared_fields.size() == members.size()) {
          declared_type = declared_fields[cell_index].second;
        }
        // Coerce textual members back to declared runtime types: struct
        // serialization stores DATE/TIMESTAMP cells as plain text.
        if (!declared_type.empty() && parsed.type == ValueType::kVarChar) {
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
        parsed_rows[row_idx].cells.emplace_back(cell_index, std::move(parsed));
      }
      if (align_mode == 1) {
        // Declared names drive the field list; no per-row key discovery.
        continue;
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
      }
    }
    if (align_mode == 1) {
      for (const auto& declared : declared_fields) {
        fields.emplace_back(declared.first, ValueType::kNull);
      }
    }
    // Fix up column types from the flattened cells (first non-NULL value
    // wins, matching the previous representative-row treatment).
    for (const ParsedElement& parsed_row : parsed_rows) {
      for (const auto& [cell_index, cell_value] : parsed_row.cells) {
        if (cell_index < fields.size() && !cell_value.IsNull() &&
            fields[cell_index].second == ValueType::kNull) {
          fields[cell_index].second = cell_value.type;
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
        for (const auto& [cell_index, cell_value] :
             parsed_rows[row_idx].cells) {
          if (cell_index < fields.size()) {
            row_vals[cell_index] = cell_value;
          }
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
            !binary.Left() || !binary.Right() ||
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
  early_memberships.reserve(filter.residual.size());
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
  rows.reserve(kMaxRows);
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
                return std::ranges::all_of(branch.simple,
                                           [&](const auto& predicate) {
                                             return MatchSimpleCompare(
                                                 row, predicate);
                                           }) &&
                       std::ranges::all_of(branch.prefix_in,
                                           [&](const auto& predicate) {
                                             return MatchPrefixIn(row,
                                                                  predicate);
                                           }) &&
                       std::ranges::all_of(branch.prebuilt_membership,
                                           [&](const auto& predicate) {
                                             return MatchPrebuiltMembership(
                                                 row, predicate);
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
    Status morsel_status = Status::kSuccess;
    const auto morsels =
        table.BuildScanMorsels(context.txn_, 8, &morsel_status);
    RETURN_IF_FAIL(morsel_status);
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

}  // namespace

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
    // Deferred rows record how many accepted rows precede them so the merge
    // below can reinsert them at their source position -- the same protocol
    // TryParallelFilterRows uses.
    struct DeferredRow {
      size_t preceding_rows;
      Row row;
    };
    std::vector<std::vector<DeferredRow>> deferred(workers);
    std::atomic<size_t> key_rejected{0};
    std::atomic<size_t> key_null_rejected{0};
    std::mutex error_mu;
    Status error_ex{Status::kSuccess};
    const size_t chunk_rows = (arena.rows + workers - 1) / workers;
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
              size_t accepted_rows = 0;
              // Each worker owns one contiguous range; merging the ranges in
              // worker order then reproduces the serial scan's row order.
              const size_t begin = w * chunk_rows;
              const size_t end = std::min(arena.rows, begin + chunk_rows);
              // Reuse one scratch row: MaterializeSingleArenaRow resizes its
              // values vector, and keeping the allocation avoids one vector
              // (plus VARCHAR payload) free+alloc per accepted row.
              Row row;
              for (size_t r = begin; r < end; ++r) {
                bool all_simple_decided = true;
                if (!row_passes_arena_precheck(r, &local_key_rej,
                                               &local_key_null_rej,
                                               &all_simple_decided)) {
                  continue;
                }
                MaterializeSingleArenaRow(arena, cached_schema, r, &row);
                if (!has_non_simple && all_simple_decided) {
                  ++accepted_rows;
                  local.AddRow(std::move(row));
                  row = Row();
                  continue;
                }
                if (filter != nullptr && filter_view != nullptr &&
                    programs.complete) {
                  switch (MatchScanFilterWorker(row, *filter_view, *filter,
                                                programs)) {
                    case 0:
                      break;
                    case 1:
                      ++accepted_rows;
                      local.AddRow(std::move(row));
                      row = Row();
                      break;
                    default:
                      deferred[w].push_back({accepted_rows, std::move(row)});
                      row = Row();
                      break;
                  }
                } else {
                  deferred[w].push_back({accepted_rows, std::move(row)});
                  row = Row();
                }
              }
              key_rejected.fetch_add(local_key_rej, std::memory_order_relaxed);
              key_null_rejected.fetch_add(local_key_null_rej,
                                          std::memory_order_relaxed);
            } catch (...) {
              std::scoped_lock lock(error_mu);
              if (error_ex == Status::kSuccess) {
                error_ex =
                    StatusFromCurrentException("parallel filter worker failed");
              }
            }
          });
        } catch (...) {
          std::scoped_lock lock(error_mu);
          if (error_ex == Status::kSuccess) {
            error_ex =
                StatusFromCurrentException("parallel filter worker failed");
          }
          break;
        }
      }
    }
    if (error_ex != Status::kSuccess) {
      if (error != nullptr && error->ok()) {
        *error = error_ex;
      }
      return true;
    }
    const auto record_error = [&](Status status) {
      if (error != nullptr && error->ok() && status != Status::kSuccess) {
        *error = std::move(status);
      }
    };
    // Main-thread merge in worker order; deferred rows take the contextual
    // MatchScanFilter path so error semantics are bit-identical to serial,
    // and are reinserted at their source position via preceding_rows.
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
            if (filter == nullptr ||
                MatchScanFilter(row, *filter_view, *filter, outer, context,
                                ctes, &deferred_error)) {
              result->AddRow(std::move(row));
            }
            record_error(std::move(deferred_error));
          }
        };
        if (shards[w].HasSpill()) {
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

StatusOr<Relation> LoadSource(TransactionContext& context,
                              const SelectSource& source, const Scope* outer,
                              const CteMap& ctes,
                              const std::vector<slot_t>* projection,
                              const std::vector<Expression>* scan_predicates,
                              const ScanRuntimeFilter* int_runtime_filter,
                              const ScanTupleFilter* tuple_filter,
                              bool columnar_output,
                              const PushedScanProjection* pushed_projection,
                              bool* pushed_projection_applied) {
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = false;
  }
  bool pushed_proj_applied = false;
  const std::vector<Expression>* saved_scan_predicates = scan_predicates;
  if (int_runtime_filter != nullptr && !int_runtime_filter->enabled()) {
    int_runtime_filter = nullptr;
  }
  const ScanRuntimeFilter* saved_int_runtime_filter = int_runtime_filter;
  if (source.sample) {
    scan_predicates = nullptr;
    int_runtime_filter = nullptr;
    tuple_filter = nullptr;
    columnar_output = false;
    pushed_projection = nullptr;
  }
  // Fold row-independent subqueries to constants before filter compilation:
  // the folded compares join the typed SimpleCompare path, and query-free
  // predicates skip the contextual per-row filter and the cached-relation
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
    const Relation* cached = nullptr;
    if (outer == nullptr && !source.sample &&
        source.query->WithQueries().empty() &&
        source.query->UnionAll().empty() &&
        !source.query->GetSetOperationTree() && !source.query->Qualify() &&
        source.query->DistinctOn().empty()) {
      cached = ExecuteCachedUncorrelated(context, *source.query, ctes);
    }
    if (cached != nullptr) {
      ASSIGN_OR_RETURN(Relation, materialized, MaterializeRelation(*cached));
      result = std::move(materialized);
    } else {
      ASSIGN_OR_RETURN(Relation, sub,
                       (ExecuteQuery(context, *source.query, outer, ctes,
                                     nullptr, columnar_output)));
      result = std::move(sub);
    }
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
        tuple_filter == nullptr && context.execution_runtime() != nullptr &&
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
    SharedBaseRelationCache* const shared_cache =
        context.execution_runtime() != nullptr
            ? context.execution_runtime()->shared_base_cache.get()
            : nullptr;
    struct SharedBaseSlotGuard {
      SharedBaseRelationCache* cache{nullptr};
      std::string key;
      bool active{false};
      SharedBaseSlotGuard(SharedBaseRelationCache* c, std::string k)
          : cache(c), key(std::move(k)) {}
      SharedBaseSlotGuard(const SharedBaseSlotGuard&) = delete;
      SharedBaseSlotGuard& operator=(const SharedBaseSlotGuard&) = delete;
      SharedBaseSlotGuard(SharedBaseSlotGuard&&) = delete;
      SharedBaseSlotGuard& operator=(SharedBaseSlotGuard&&) = delete;
      ~SharedBaseSlotGuard() {
        if (active && cache != nullptr) {
          cache->AbandonBase(key);
        }
      }
    } shared_base_guard(shared_cache, cache_key);
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
          table_chk.Value()->BackingColumnarRowCount() > 131072;
      if (!cached_entry && !large_lstable && reusable &&
          int_runtime_filter == nullptr && shared_cache != nullptr &&
          !cache_key.empty()) {
        RelationPtr shared_hit;
        if (!shared_cache->AcquireBaseSlot(cache_key, &shared_hit)) {
          if (shared_hit) {
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                shared_hit);
            cached_entry = std::move(shared_hit);
          }
        } else {
          shared_base_guard.active = true;
        }
      }
      // Direct aggregate scans can publish an empty reuse marker for an
      // in-memory LSTable even when the table has no persisted columnar rows.
      // Without a row arena this is not a materialized empty relation: rescan
      // the table, independent of its backing format or current row count.
      if (cached_entry && cached_entry->rows.empty() &&
          !cached_entry->HasSpill() && !cached_entry->HasColumnarOnly()) {
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
        if (cached_relation.HasColumnarOnly() &&
            (int_runtime_filter == nullptr || !int_runtime_filter->enabled())) {
          result.SetColumnarTable(cached_relation.columnar,
                                  cached_relation.uniform_cell_types);
        } else if (unboxed_arena != nullptr &&
                   (int_runtime_filter != nullptr &&
                    int_runtime_filter->enabled()) &&
                   TryFilterArenaCache(context, *unboxed_arena,
                                       cached_relation.schema, nullptr, nullptr,
                                       outer, ctes, int_runtime_filter, &result,
                                       &key_error)) {
          RETURN_IF_FAIL(key_error);
        } else {
          if (unboxed_arena != nullptr && unboxed_arena->rows >= 32768) {
            RETURN_IF_FAIL(MaterializeCacheRowsParallel(
                cache_key, &cached_relation, context.execution_runtime()));
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
                  int_runtime_filter->enabled() &&
                  int_runtime_filter->column < row.Count()) {
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
            RETURN_IF_FAIL(MaterializeCacheRowsParallel(
                cache_key, &cached_relation, context.execution_runtime()));
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
                  int_runtime_filter->enabled() &&
                  int_runtime_filter->column < row.Count()) {
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
          !columnar_output && !context.txn_.HasAnyPageVersionChains() &&
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
        keyed_cache_key = "__keyed__:" + source.table + ":";
        AppendInteger(keyed_cache_key, *full_key_column);
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key,
                      static_cast<int>(int_runtime_filter->type));
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, exact_keys.size());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, fg.Lo());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, fg.Hi());
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, h_sum);
        keyed_cache_key.push_back(':');
        AppendInteger(keyed_cache_key, h_xor);
        // The 64-bit digest can collide across different key sets; embed the
        // key bytes so a cache hit can only mean an identical key set.  Sort
        // first so build order does not split equivalent probes.
        std::vector<int64_t> sorted_keys(exact_keys);
        std::sort(sorted_keys.begin(), sorted_keys.end());
        keyed_cache_key.reserve(
            keyed_cache_key.size() +
            sorted_keys
                    .size() *  // NOLINT(readability-math-missing-parentheses)
                sizeof(
                    int64_t));  // NOLINT(readability-math-missing-parentheses)
        for (int64_t k : sorted_keys) {
          keyed_cache_key.append(reinterpret_cast<const char*>(&k), sizeof(k));
        }
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
      bool has_unsigned_view = false;
      for (size_t i = 0; i < table_schema.ColumnCount(); ++i) {
        if (table_schema.GetColumn(i).IsUnsigned()) {
          has_unsigned_view = true;
          break;
        }
      }
      if (!has_unsigned_view) {
        for (size_t i = 0; i < filter_view.ColumnCount(); ++i) {
          if (filter_view.GetColumn(i).IsUnsigned()) {
            has_unsigned_view = true;
            break;
          }
        }
      }
      const bool allow_scan_columnar = columnar_output &&
                                       keyed_cache_key.empty() &&
                                       !source.sample && !has_unsigned_view;
      const PushedScanProjection* scan_pushed_proj =
          (allow_scan_columnar && !reusable) ? pushed_projection : nullptr;
      const bool parallel_ok =
          indexed_ok ||
          ((filter_group == nullptr || !keyed_parallel_disabled) &&
           TryParallelTableScan(
               context, *table.Value(), projection, filter_group, filter_type,
               full_key_column, effective_filter_during_scan,
               effective_filter_during_scan ? &scan_filter : nullptr,
               filter_view, outer, ctes, &result, &eval_error, tuple_filter,
               allow_scan_columnar, scan_pushed_proj, &pushed_proj_applied));
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
              int_runtime_filter->enabled() &&
              int_runtime_filter->column < iterator->Count()) {
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
        bool filtered_from_cached = false;
        if (reusable && int_runtime_filter == nullptr) {
          if (result.HasColumnarOnly()) {
            auto cached = std::make_shared<Relation>(nullptr);
            cached->schema = result.schema;
            cached->SetColumnarTable(result.columnar,
                                     result.uniform_cell_types);
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                cached);
            if (shared_base_guard.active && shared_cache != nullptr) {
              shared_cache->PublishBase(cache_key, cached);
              shared_base_guard.active = false;
            }
          } else {
            auto cached = std::make_shared<Relation>(std::move(result));
            context.execution_runtime()->base_relations.emplace(cache_key,
                                                                cached);
            if (shared_base_guard.active && shared_cache != nullptr) {
              shared_cache->PublishBase(cache_key, cached);
              shared_base_guard.active = false;
            }
            result = Relation(context.execution_runtime());
            result.schema = cached->schema;
            if (scan_predicates != nullptr && !scan_predicates->empty()) {
              const Schema saved_schema = cached->schema;
              const Schema view = filter_view_of(saved_schema);
              const FilterCompileEnv compile_env{
                  .context = &context, .outer = outer, .ctes = &ctes};
              const CompiledScanFilter fill_filter =
                  CompileScanFilter(*scan_predicates, view, &compile_env);
              Status filter_error{Status::kSuccess};
              if (TryParallelFilterRows(context, *cached, fill_filter, view,
                                        outer, ctes, nullptr, &result,
                                        &filter_error, nullptr)) {
                RETURN_IF_FAIL(filter_error);
                RETURN_IF_FAIL(result.FinishSpill());
                RETURN_IF_FAIL(result.GetStatus());
                result.schema = saved_schema;
                filtered_from_cached = true;
              }
            }
            if (!filtered_from_cached) {
              bool copied_parallel = false;
              if (!cached->HasSpill() && cached->rows.size() >= 16384) {
                const CompiledScanFilter pass_through;
                Status copy_error{Status::kSuccess};
                if (TryParallelFilterRows(context, *cached, pass_through,
                                          cached->schema, outer, ctes, nullptr,
                                          &result, &copy_error)) {
                  RETURN_IF_FAIL(copy_error);
                  copied_parallel = true;
                }
              }
              if (!copied_parallel) {
                ASSIGN_OR_RETURN(Relation, cached_copy,
                                 MaterializeRelation(*cached));
                result = std::move(cached_copy);
              }
            }
          }
        } else if (!keyed_cache_key.empty() && !result.HasSpill() &&
                   !result.HasColumnarOnly() && result.rows.size() <= 65536) {
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
        if (reusable && !filtered_from_cached && scan_predicates != nullptr &&
            !scan_predicates->empty()) {
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
            RETURN_IF_FAIL(FilterRelation(context, &result, *scan_predicates,
                                          outer, ctes));
          }
          result.schema = saved_schema;
        }
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
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
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
  if (!pushed_proj_applied) {
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    // Unnest outputs carry the alias as qualifier too: bare element refs still
    // match by column name, while alias-qualified and alias-as-row references
    // (`s.field`, `SELECT s`) resolve uniformly with table sources.
    if (!qualifier.empty()) {
      result.schema = QualifySchema(result.schema, qualifier);
    }
  }
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = pushed_proj_applied;
  }
  if (source.sample) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
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
  if (!columnar_output && result.HasColumnarOnly()) {
    RETURN_IF_FAIL(result.EnsureRowsMaterialized());
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
    // Compare via string_view: the map keys are few, so a scan avoids
    // materializing `table` into a std::string on every visited node.
    if (std::ranges::any_of(
            counts, [&](const auto& entry) { return entry.first == table; })) {
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

// LocalColumnOffset is defined in executor/detail/columnar_table.cpp.
std::vector<Expression> SplitDisjuncts(const Expression& expression) {
  // T2405: iterative flattening -- this mirror of rewrite.cpp's helper runs
  // on the scan-filter path where no rewrite-depth guard applies at all, so
  // a long OR chain hit the stack directly.  Right is pushed before Left
  // so the stack pops Left first and `parts` keeps the left-to-right leaf
  // order the recursion produced.
  std::vector<Expression> parts;
  std::vector<Expression> pending;
  if (expression) {
    pending.push_back(expression);
  }
  while (!pending.empty()) {
    const Expression node = std::move(pending.back());
    pending.pop_back();
    if (!node) {
      continue;  // a null OR side contributes nothing (old base case)
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
                                     .input_rows = relation->TotalRows(),
                                     .input_relation = relation};
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
      RETURN_IF_FAIL(relation->EnsureRowsMaterialized());
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
  RETURN_IF_FAIL(relation->EnsureRowsMaterialized());
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
