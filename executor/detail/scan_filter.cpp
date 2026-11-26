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
#include <cstring>
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
#include "common/literal_like_pattern.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/regex_guard.hpp"
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
#include "expression/cast_expression.hpp"
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

bool IsIntLike(const Value& value) {
  return value.type == ValueType::kInt64 || value.type == ValueType::kDate;
}

}  // namespace

bool FastLikeMatch(std::string_view value, std::string_view pattern) {
  // Keep scan filters on the shared escape-aware matcher used by expression
  // evaluation. Its compiled literal/%/_ paths fall back to the UTF-8 matcher,
  // so this adapter does not maintain a second LIKE escape implementation.
  // Lean proofs: lean/TinylambOpt/LikeSegmentChain.lean (anchored_chain_iff),
  //   lean/TinylambOpt/LikeUnderscore.lean (like_underscore_split_iff),
  //   lean/TinylambOpt/LikeShiftAnd.lean (shiftAnd_iff)
  return EscapedLikeMatch(value, pattern);
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
      // Delegate to the canonical evaluator: SQL comparisons treat NaN as IEEE
      // unordered (NaN = NaN is false), unlike group-key equality/hashing which
      // folds all NaNs into one peer. A raw IEEE `==` here would agree for
      // equality but disagree for the other comparison operators.
      StatusOr<Value> cmp = TryBinary(pred.op, left, right);
      return exact(std::move(cmp));
    }
    if (left.type == ValueType::kVarChar && right.type == ValueType::kVarChar) {
      const std::string_view lhs = left.value.varchar_value;
      const std::string_view rhs = right.value.varchar_value;
      if (left.IsCaseInsensitive() || right.IsCaseInsensitive() ||
          (LooksLikeTimestampText(lhs) && LooksLikeTimestampText(rhs))) {
        StatusOr<Value> cmp = TryBinary(pred.op, left, right);
        return exact(std::move(cmp));
      }
      return ComparePositional(pred.op, lhs, rhs);
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
      // An unparsable date string is an evaluation error, not a clean
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COLCOL_COMPARE") !=
      nullptr;
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
    const StatusOr<int64_t> parsed =
        TryParseDateDays(compiled.constant.value.varchar_value);
    if (!parsed.HasValue()) {
      // An invalid date literal is a query error in the checked evaluator;
      // a fast-path compare would silently reject every row instead.
      return std::nullopt;
    }
    compiled.int_constant = parsed.Value();
    compiled.date_string_payload = true;
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
    // A runtime column value can carry case-insensitive collation even when
    // its schema only says VARCHAR. LIKE with '_' can then raise an error.
    const std::string_view pattern = compiled.constant.value.varchar_value;
    if (pattern.find('_') != std::string_view::npos) {
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
  return pred.Matches(text);
}

std::optional<PrefixInPredicate> TryCompilePrefixIn(const Expression& predicate,
                                                    const Schema& schema) {
  static const bool prefix_in_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PREFIX_IN") != nullptr;
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
  compiled.Finalize();
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP") !=
      nullptr;
  static const bool membership_peel_debug =
      ::tinylamb::platform::GetEnv("TINYLAMB_PREBUILT_MEMBERSHIP_DEBUG") !=
      nullptr;
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_ARENA_MEMBERSHIP") !=
      nullptr;
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
            needed.reserve(1 + (local_simple.size() * 2));
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
                          return store->IntegerColumn(col).size() ==
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
                                store->IntegerColumn(col_idx).data();
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
                              // Lean proof:
                              // lean/TinylambOpt/TpchSf10VectorizedExec.lean
                              //   (parallel_bitset_or_eq_union)
                              // When the dense bitset fits comfortably per
                              // worker (<= 4 MiB), set bits in a thread-local
                              // bitset and OR words once at the end instead of
                              // sorting millions of random keys per worker.
                              if (values->DenseWordCount() <= 524288) {
                                const int64_t dmin = values->DenseMin();
                                const int64_t dmax = values->DenseMax();
                                std::vector<uint64_t> local_bits(
                                    values->DenseWordCount(), 0ULL);
                                uint64_t* lbits = local_bits.data();
                                bool overflowed = false;
                                for (size_t r = begin; r < end; ++r) {
                                  const int64_t key = keys[r];
                                  if (have_last && key == last) {
                                    continue;
                                  }
                                  if (!matches(r)) {
                                    continue;
                                  }
                                  if (key < dmin || key > dmax) {
                                    overflowed = true;
                                  } else {
                                    const uint64_t off =
                                        static_cast<uint64_t>(key) -
                                        static_cast<uint64_t>(dmin);
                                    lbits[off >> 6] |= uint64_t{1}
                                                       << (off & 63);
                                  }
                                  have_last = true;
                                  last = key;
                                }
                                if (overflowed) {
                                  values->InsertDenseAtomic(dmin - 1);
                                }
                                values->MergeDenseWordsAtomic(
                                    local_bits.data(), local_bits.size());
                                return;
                              }
                              // T1231: fallback for huge dense spans.
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
                                  store->IntegerColumn(compare.column).data();
                              const auto* b =
                                  store->IntegerColumn(compare.right_column)
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
                                        store->IntegerColumn(compare.column)[r];
                                    const int64_t b =
                                        compare.column_payload
                                            ? store->IntegerColumn(
                                                  compare.right_column)[r]
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
                ::tinylamb::platform::GetEnv(
                    "TINYLAMB_DISABLE_PARALLEL_MEMBERSHIP") != nullptr;
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
  bool columnar_filled = false;
  if (!direct_filled) {
    RetainSubqueryStatement(runtime, query.Query());
    members = ExecuteCachedUncorrelated(context, statement, ctes, false);
    if (members == nullptr || members->schema.ColumnCount() != 1) {
      return mbail("exec");
    }
    // Fast columnar-only path: scan the single int64/date column directly
    // without boxing every member into a Row, and reserve dense mode when the
    // observed [min, max] fits within kMaxDenseSpan.
    if (!members->HasSpill() && members->HasColumnarOnly() &&
        members->columnar != nullptr && !members->columnar->chunks.empty()) {
      bool all_flat_int = true;
      int64_t min_k = std::numeric_limits<int64_t>::max();
      int64_t max_k = std::numeric_limits<int64_t>::min();
      bool any_k = false;
      ValueType col_type = ValueType::kNull;
      bool col_unsigned = false;
      for (const ColumnarChunk& chunk : members->columnar->chunks) {
        if (chunk.rows == 0) {
          continue;
        }
        if (chunk.columns.empty()) {
          all_flat_int = false;
          break;
        }
        const ColumnarColumn& c0 = chunk.columns[0];
        if (c0.encoding != ColumnarColumn::Encoding::kFlat || !c0.is_uniform ||
            c0.is_interval ||
            (c0.type != ValueType::kInt64 && c0.type != ValueType::kDate) ||
            c0.ints.size() < chunk.rows) {
          all_flat_int = false;
          break;
        }
        if (!has_domain) {
          has_domain = true;
          col_type = c0.type;
          col_unsigned = c0.is_unsigned;
        } else if (c0.type != col_type || c0.is_unsigned != col_unsigned) {
          all_flat_int = false;
          break;
        }
        const int64_t* vals = c0.ints.data();
        const bool has_nulls = c0.has_null && c0.nulls.size() >= chunk.rows;
        if (!has_nulls) {
          for (size_t r = 0; r < chunk.rows; ++r) {
            const int64_t v = vals[r];
            min_k = std::min(min_k, v);
            max_k = std::max(max_k, v);
          }
          any_k = true;
        } else {
          const uint8_t* nulls = c0.nulls.data();
          for (size_t r = 0; r < chunk.rows; ++r) {
            if (nulls[r] != 0) {
              contains_null = true;
            } else {
              const int64_t v = vals[r];
              min_k = std::min(min_k, v);
              max_k = std::max(max_k, v);
              any_k = true;
            }
          }
        }
      }
      if (all_flat_int) {
        domain_type = col_type;
        domain_unsigned = col_unsigned;
        if (any_k) {
          if (!values->TryReserveDense(min_k, max_k)) {
            values->Reserve(members->TotalRows());
          }
          for (const ColumnarChunk& chunk : members->columnar->chunks) {
            if (chunk.rows == 0) {
              continue;
            }
            const ColumnarColumn& c0 = chunk.columns[0];
            const int64_t* vals = c0.ints.data();
            const bool has_nulls = c0.has_null && c0.nulls.size() >= chunk.rows;
            if (!has_nulls) {
              for (size_t r = 0; r < chunk.rows; ++r) {
                values->Insert(vals[r]);
              }
            } else {
              const uint8_t* nulls = c0.nulls.data();
              for (size_t r = 0; r < chunk.rows; ++r) {
                if (nulls[r] == 0) {
                  values->Insert(vals[r]);
                }
              }
            }
          }
        } else {
          values->Reserve(0);
        }
        columnar_filled = true;
      } else {
        has_domain = false;
        contains_null = false;
      }
    }
    if (!columnar_filled) {
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
  }
  const Status iteration =
      (direct_filled || columnar_filled)
          ? Status::kSuccess
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
  values->TryCompactToDense();
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
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PREBUILT_MEMBERSHIP") !=
      nullptr;
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
      ::tinylamb::platform::GetEnv("TINYLAMB_ENABLE_PREBUILT_SCALAR") !=
      nullptr;
  static const bool scalar_peel_debug =
      ::tinylamb::platform::GetEnv("TINYLAMB_PREBUILT_SCALAR_DEBUG") != nullptr;
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
  auto extract_const_item = [](const Expression& item) -> std::optional<Value> {
    if (item && item->Type() == TypeTag::kConstantValue) {
      return item->AsConstantValue().GetValue();
    }
    return TryDecideConstantConjunct(item);
  };
  if (probe_type == ValueType::kVarChar) {
    auto strings = std::make_shared<MembershipStringSet>();
    strings->reserve(in.list_.size());
    for (const Expression& item : in.list_) {
      std::optional<Value> val = extract_const_item(item);
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
  int64_t min_v = std::numeric_limits<int64_t>::max();
  int64_t max_v = std::numeric_limits<int64_t>::min();
  for (const Expression& item : in.list_) {
    std::optional<Value> val = extract_const_item(item);
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
    const int64_t iv = val->value.int_value;
    int_vals.push_back(iv);
    min_v = std::min(min_v, iv);
    max_v = std::max(max_v, iv);
  }
  if (!int_vals.empty()) {
    if (!values->TryReserveDense(min_v, max_v)) {
      values->Reserve(int_vals.size());
    }
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
  bool seen_may_throw = false;
  for (const Expression& predicate : predicates) {
    if (!predicate) {
      continue;
    }
    if (seen_may_throw) {
      compiled.residual.push_back(predicate);
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
    // Also require the OR tree to be error-free under reordering: branch
    // decomposition evaluates simple compares before residuals and
    // short-circuits on NULL, which would suppress runtime errors (e.g.
    // ABS(INT64_MIN)) that the AST reference raises.
    if (eff_predicate->Type() == TypeTag::kBinaryExp &&
        eff_predicate->AsBinaryExpression().Op() == BinaryOperation::kOr &&
        AuditIsErrorFreePredicateForOrdering(eff_predicate, schema)) {
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
        // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
        //   (contiguous_interval_union_eq)
        // Collapse a contiguous union of closed intervals on a single numeric
        // column `(col >= lo_1 AND col <= hi_1) OR ...` (e.g. TPC-DS Q48/Q13/
        // Q85 `ss_sales_price` / `ss_net_profit`) into two simple bounds
        // `col >= min(lo_i) AND col <= max(hi_i)`, avoiding disjunctive-branch
        // cross-product explosion and unlocking the columnar scan fast path.
        if (all_branches_usable && new_branches.size() >= 2) {
          bool can_collapse_intervals = true;
          slot_t common_col = 0;
          bool is_dbl = false;
          bool is_int = false;
          struct IntervalItem {
            double d_lo{0.0};
            double d_hi{0.0};
            int64_t i_lo{0};
            int64_t i_hi{0};
            const SimpleComparePredicate* lo_pred{nullptr};
            const SimpleComparePredicate* hi_pred{nullptr};
          };
          std::vector<IntervalItem> ivals;
          ivals.reserve(new_branches.size());
          for (size_t bi = 0; bi < new_branches.size(); ++bi) {
            const auto& br = new_branches[bi];
            if (!br.prefix_in.empty() || !br.prebuilt_membership.empty() ||
                !br.residual.empty() || br.simple.size() != 2) {
              can_collapse_intervals = false;
              break;
            }
            const SimpleComparePredicate& p0 = br.simple[0];
            const SimpleComparePredicate& p1 = br.simple[1];
            if (p0.column_payload || p1.column_payload ||
                p0.column != p1.column || p0.column >= schema.ColumnCount()) {
              can_collapse_intervals = false;
              break;
            }
            if (bi == 0) {
              common_col = p0.column;
              const Column& cmeta = schema.GetColumn(common_col);
              if (cmeta.IsUnsigned() || cmeta.IsInterval()) {
                can_collapse_intervals = false;
                break;
              }
              if (cmeta.Type() == ValueType::kDouble) {
                is_dbl = true;
              } else if (cmeta.Type() == ValueType::kInt64 ||
                         cmeta.Type() == ValueType::kDate) {
                is_int = true;
              } else {
                can_collapse_intervals = false;
                break;
              }
            } else if (p0.column != common_col) {
              can_collapse_intervals = false;
              break;
            }
            const SimpleComparePredicate* ge = nullptr;
            const SimpleComparePredicate* le = nullptr;
            if (p0.op == BinaryOperation::kGreaterThanEquals &&
                p1.op == BinaryOperation::kLessThanEquals) {
              ge = &p0;
              le = &p1;
            } else if (p1.op == BinaryOperation::kGreaterThanEquals &&
                       p0.op == BinaryOperation::kLessThanEquals) {
              ge = &p1;
              le = &p0;
            } else {
              can_collapse_intervals = false;
              break;
            }
            IntervalItem item;
            item.lo_pred = ge;
            item.hi_pred = le;
            if (is_dbl) {
              if (!ge->double_payload || !le->double_payload ||
                  std::isnan(ge->double_constant) ||
                  std::isnan(le->double_constant) ||
                  ge->double_constant > le->double_constant) {
                can_collapse_intervals = false;
                break;
              }
              item.d_lo = ge->double_constant;
              item.d_hi = le->double_constant;
            } else {
              if (!ge->int_payload || !le->int_payload ||
                  ge->int_constant > le->int_constant) {
                can_collapse_intervals = false;
                break;
              }
              item.i_lo = ge->int_constant;
              item.i_hi = le->int_constant;
            }
            ivals.push_back(item);
          }
          if (can_collapse_intervals && ivals.size() >= 2) {
            if (is_dbl) {
              std::ranges::sort(ivals, [](const IntervalItem& a,
                                          const IntervalItem& b) {
                return a.d_lo < b.d_lo || (a.d_lo == b.d_lo && a.d_hi < b.d_hi);
              });
              double cur_hi = ivals[0].d_hi;
              const SimpleComparePredicate* best_hi = ivals[0].hi_pred;
              bool contiguous = true;
              for (size_t k = 1; k < ivals.size(); ++k) {
                if (ivals[k].d_lo > cur_hi) {
                  contiguous = false;
                  break;
                }
                if (ivals[k].d_hi > cur_hi) {
                  cur_hi = ivals[k].d_hi;
                  best_hi = ivals[k].hi_pred;
                }
              }
              if (contiguous) {
                compiled.simple.push_back(*ivals[0].lo_pred);
                compiled.simple.push_back(*best_hi);
                continue;
              }
            } else if (is_int) {
              std::ranges::sort(ivals, [](const IntervalItem& a,
                                          const IntervalItem& b) {
                return a.i_lo < b.i_lo || (a.i_lo == b.i_lo && a.i_hi < b.i_hi);
              });
              int64_t cur_hi = ivals[0].i_hi;
              const SimpleComparePredicate* best_hi = ivals[0].hi_pred;
              bool contiguous = true;
              for (size_t k = 1; k < ivals.size(); ++k) {
                if (ivals[k].i_lo > cur_hi) {
                  contiguous = false;
                  break;
                }
                if (ivals[k].i_hi > cur_hi) {
                  cur_hi = ivals[k].i_hi;
                  best_hi = ivals[k].hi_pred;
                }
              }
              if (contiguous) {
                compiled.simple.push_back(*ivals[0].lo_pred);
                compiled.simple.push_back(*best_hi);
                continue;
              }
            }
          }
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
        if (!AuditIsErrorFreePredicateForOrdering(predicate, schema)) {
          seen_may_throw = true;
        }
      }
    } else {
      compiled.residual.push_back(predicate);
      if (!AuditIsErrorFreePredicateForOrdering(predicate, schema)) {
        seen_may_throw = true;
      }
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
  // Lean proof: lean/TinylambOpt/FilterEvalOrder.lean (filter_eval_order_sound)
  // Order error-free residual conjuncts by ascending evaluation cost before
  // sequential/bytecode evaluation so cheap predicates short-circuit expensive
  // LIKE / REGEXP_CONTAINS / OR checks.
  if (compiled.residual.size() >= 2) {
    const char* disable_order =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FILTER_ORDER");
    if (disable_order == nullptr || disable_order[0] != '1') {
      auto residual_error_free = [&](const Expression& expr) -> bool {
        if (IsNonThrowingLocalFilter(expr, schema, nullptr)) {
          return true;
        }
        if (expr && expr->Type() == TypeTag::kFunctionCallExp) {
          const auto& call = expr->AsFunctionCallExpression();
          if (call.FuncName() == "regexp_contains" && call.Args().size() == 2 &&
              call.Args()[0] &&
              call.Args()[0]->Type() == TypeTag::kColumnValue &&
              LocalColumnOffset(schema,
                                call.Args()[0]->AsColumnValue().GetColumnName())
                  .has_value() &&
              call.Args()[1] &&
              call.Args()[1]->Type() == TypeTag::kConstantValue) {
            const Value& pat = call.Args()[1]->AsConstantValue().GetValue();
            if (pat.IsNull()) {
              return true;
            }
            if (pat.type == ValueType::kVarChar) {
              return CompileCheckedRegex(std::string(pat.value.varchar_value))
                  .HasValue();
            }
          }
        }
        return false;
      };
      auto residual_cost = [&](const auto& self,
                               const Expression& expr) -> int {
        if (!expr) {
          return 100;
        }
        if (expr->Type() == TypeTag::kUnaryExp) {
          return 1;
        }
        if (expr->Type() == TypeTag::kFunctionCallExp &&
            expr->AsFunctionCallExpression().FuncName() == "regexp_contains") {
          return 60;
        }
        if (expr->Type() == TypeTag::kInExp) {
          return 8;
        }
        if (expr->Type() == TypeTag::kBinaryExp) {
          const auto& b = expr->AsBinaryExpression();
          if (!b.Left() || !b.Right()) {
            return 100;
          }
          if (b.Op() == BinaryOperation::kLike ||
              b.Op() == BinaryOperation::kNotLike) {
            return 25;
          }
          if (b.Op() == BinaryOperation::kOr) {
            return std::min(55, self(self, b.Left()) + self(self, b.Right()));
          }
          if (b.Op() == BinaryOperation::kAnd) {
            return std::min(55, self(self, b.Left()) + self(self, b.Right()));
          }
          const TypeTag lt = b.Left()->ResultType(schema).GetType();
          const TypeTag rt = b.Right()->ResultType(schema).GetType();
          if (lt == TypeTag::kInteger || lt == TypeTag::kBigInt ||
              lt == TypeTag::kDate || rt == TypeTag::kInteger ||
              rt == TypeTag::kBigInt || rt == TypeTag::kDate) {
            return 2;
          }
          if (lt == TypeTag::kDouble || rt == TypeTag::kDouble) {
            return 3;
          }
          return 6;
        }
        return 10;
      };
      if (std::ranges::all_of(compiled.residual, residual_error_free)) {
        std::stable_sort(compiled.residual.begin(), compiled.residual.end(),
                         [&](const Expression& a, const Expression& b) {
                           return residual_cost(residual_cost, a) <
                                  residual_cost(residual_cost, b);
                         });
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
  {
    const char* disable_order =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FILTER_ORDER");
    key->push_back(disable_order != nullptr && disable_order[0] == '1'
                       ? char{1}
                       : char{0});
  }
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
  const bool has_branch_residual =
      std::ranges::any_of(filter.disjunctive_branches,
                          [](const auto& b) { return !b.residual.empty(); });
  // Positional predicates (simple, prefix_in, prebuilt_membership,
  // prebuilt_scalar_compare) are already bound to exact slot_t indices and do
  // not invoke bytecode, so duplicate column names in multi-self-join schemas
  // (e.g. TPC-DS Q72 date_dim d1/d2/d3) do not require bytecode compilation.
  if (filter.residual.empty() && !has_branch_residual) {
    programs.complete = true;
    return programs;
  }
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
    Status pred_error = Status::kSuccess;
    if (!MatchSimpleCompare(match_row, pred, &pred_error)) {
      if (!pred_error.ok()) {
        return 2;
      }
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
    Status pred_error = Status::kSuccess;
    if (!MatchPrebuiltScalarCompare(match_row, pred, &pred_error)) {
      if (!pred_error.ok()) {
        return 2;
      }
      return 0;
    }
  }
  if (!filter.disjunctive_branches.empty()) {
    bool any_branch_passed = false;
    for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
      const auto& branch = filter.disjunctive_branches[b];
      bool branch_ok = true;
      for (const SimpleComparePredicate& pred : branch.simple) {
        Status pred_error = Status::kSuccess;
        if (!MatchSimpleCompare(match_row, pred, &pred_error)) {
          if (!pred_error.ok()) {
            return 2;
          }
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

}  // namespace tinylamb::relational_detail
