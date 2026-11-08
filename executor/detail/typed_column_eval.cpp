/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exc_shim.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

bool HavingReferencesOnlyKeysAndAggregates(
    const Expression& expression,  // NOLINT(misc-no-recursion)
    const Schema& schema, const std::vector<slot_t>& key_offsets) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const auto offset = LocalColumnOffset(
          schema, expression->AsColumnValue().GetColumnName());
      if (!offset) {
        return false;
      }
      return std::find(key_offsets.begin(), key_offsets.end(),
                       static_cast<slot_t>(*offset)) != key_offsets.end();
    }
    case TypeTag::kAggregateExp:
      return true;
    default:
      break;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [&](const Expression& child) {
                               return HavingReferencesOnlyKeysAndAggregates(
                                   child, schema, key_offsets);
                             });
}

bool MatchTypedStringInList(std::string_view v,
                            const TypedInListCompare& list) noexcept {
  if (list.prefix_len != 0) {
    if (v.size() < list.prefix_len) {
      return false;
    }
    const std::string_view sub = v.substr(0, list.prefix_len);
    return std::ranges::any_of(list.strings, [&](const std::string& item) {
      return sub == std::string_view(item);
    });
  }
  return std::ranges::any_of(list.strings, [&](const std::string& item) {
    return v == std::string_view(item);
  });
}

namespace {

BinaryOperation FlipTypedCompare(BinaryOperation operation) {
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

bool DecideTypedFilterCompare(const Value& left, const Value& right,
                              const TypedFilterCompare& compare) {
  if (compare.string_payload) {
    const std::string_view a = left.value.varchar_value;
    const bool equal = a == std::string_view(compare.string_constant);
    return compare.op == BinaryOperation::kEquals ? equal : !equal;
  }
  if (compare.constant_payload) {
    if (compare.is_double) {
      const double a = left.value.double_value;
      switch (compare.op) {
        case BinaryOperation::kEquals:
          return a == compare.double_constant;
        case BinaryOperation::kNotEquals:
          return a != compare.double_constant;
        case BinaryOperation::kLessThan:
          return a < compare.double_constant;
        case BinaryOperation::kLessThanEquals:
          return a <= compare.double_constant;
        case BinaryOperation::kGreaterThan:
          return a > compare.double_constant;
        default:
          return a >= compare.double_constant;
      }
    }
    const int64_t a = left.value.int_value;
    switch (compare.op) {
      case BinaryOperation::kEquals:
        return a == compare.int_constant;
      case BinaryOperation::kNotEquals:
        return a != compare.int_constant;
      case BinaryOperation::kLessThan:
        return a < compare.int_constant;
      case BinaryOperation::kLessThanEquals:
        return a <= compare.int_constant;
      case BinaryOperation::kGreaterThan:
        return a > compare.int_constant;
      default:
        return a >= compare.int_constant;
    }
  }
  if (compare.is_double) {
    const double a = left.value.double_value;
    const double b = right.value.double_value;
    switch (compare.op) {
      case BinaryOperation::kEquals:
        return a == b;
      case BinaryOperation::kNotEquals:
        return a != b;
      case BinaryOperation::kLessThan:
        return a < b;
      case BinaryOperation::kLessThanEquals:
        return a <= b;
      case BinaryOperation::kGreaterThan:
        return a > b;
      default:
        return a >= b;
    }
  }
  const int64_t a = left.value.int_value;
  const int64_t b = right.value.int_value;
  switch (compare.op) {
    case BinaryOperation::kEquals:
      return a == b;
    case BinaryOperation::kNotEquals:
      return a != b;
    case BinaryOperation::kLessThan:
      return a < b;
    case BinaryOperation::kLessThanEquals:
      return a <= b;
    case BinaryOperation::kGreaterThan:
      return a > b;
    default:
      return a >= b;
  }
}

std::optional<TypedFilterCompare> TryCompileTypedFilterCompare(
    const Expression& predicate, const Schema& scan_schema,
    bool allow_const_fold) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  if (IsComparison(binary.Op()) &&
      binary.Left()->Type() == TypeTag::kColumnValue &&
      binary.Right()->Type() == TypeTag::kColumnValue) {
    const auto left_offset = LocalColumnOffset(
        scan_schema, binary.Left()->AsColumnValue().GetColumnName());
    const auto right_offset = LocalColumnOffset(
        scan_schema, binary.Right()->AsColumnValue().GetColumnName());
    if (left_offset && right_offset) {
      const Column& left_column =
          scan_schema.GetColumn(static_cast<slot_t>(*left_offset));
      const Column& right_column =
          scan_schema.GetColumn(static_cast<slot_t>(*right_offset));
      const ValueType left_type = left_column.Type();
      if (!left_column.IsUnsigned() && !right_column.IsUnsigned() &&
          left_type == right_column.Type() &&
          (left_type == ValueType::kInt64 || left_type == ValueType::kDate ||
           left_type == ValueType::kDouble)) {
        return TypedFilterCompare{.left = static_cast<slot_t>(*left_offset),
                                  .right = static_cast<slot_t>(*right_offset),
                                  .op = binary.Op(),
                                  .is_double = left_type == ValueType::kDouble};
      }
    }
  } else if (IsComparison(binary.Op()) && allow_const_fold &&
             (binary.Left()->Type() == TypeTag::kColumnValue ||
              binary.Right()->Type() == TypeTag::kColumnValue)) {
    ExpressionRewriter rewriter(
        ContainsNotOfOrderedDoubleComparison(predicate, scan_schema)
            ? NotComparisonFreeRules()
            : ExpressionRuleSet::Default());
    rewriter.set_schema(&scan_schema);
    StatusOr<Expression> folded_or = rewriter.TryRewrite(predicate);
    if (folded_or.HasValue() && folded_or.Value() &&
        folded_or.Value()->Type() == TypeTag::kBinaryExp) {
      const BinaryExpression& folded = folded_or.Value()->AsBinaryExpression();
      if (IsComparison(folded.Op())) {
        Expression column = folded.Left();
        Expression constant = folded.Right();
        BinaryOperation op = folded.Op();
        if (column->Type() == TypeTag::kConstantValue &&
            constant->Type() == TypeTag::kColumnValue) {
          std::swap(column, constant);
          op = FlipTypedCompare(op);
        }
        if (column->Type() == TypeTag::kColumnValue &&
            constant->Type() == TypeTag::kConstantValue) {
          const auto offset = LocalColumnOffset(
              scan_schema, column->AsColumnValue().GetColumnName());
          const Value& constant_value = constant->AsConstantValue().GetValue();
          if (offset && !constant_value.IsNull()) {
            const Column& column_object =
                scan_schema.GetColumn(static_cast<slot_t>(*offset));
            const ValueType column_type = column_object.Type();
            TypedFilterCompare compiled{.left = static_cast<slot_t>(*offset),
                                        .op = op,
                                        .constant_payload = true};
            bool supported = false;
            if (!column_object.IsUnsigned() &&
                (column_type == ValueType::kInt64 ||
                 column_type == ValueType::kDate) &&
                constant_value.type == column_type) {
              compiled.is_double = false;
              compiled.int_constant = constant_value.value.int_value;
              supported = true;
            } else if (!column_object.IsUnsigned() &&
                       column_type == ValueType::kInt64 &&
                       constant_value.type == ValueType::kDouble &&
                       std::isfinite(constant_value.value.double_value) &&
                       constant_value.value.double_value >=
                           -static_cast<double>(int64_t{1} << 52) &&
                       constant_value.value.double_value <=
                           static_cast<double>(int64_t{1} << 52)) {
              const double d = constant_value.value.double_value;
              const auto fl = static_cast<int64_t>(std::floor(d));
              const auto cl = static_cast<int64_t>(std::ceil(d));
              if (fl == cl) {
                compiled.is_double = false;
                compiled.int_constant = fl;
                supported = true;
              } else if (op == BinaryOperation::kGreaterThan ||
                         op == BinaryOperation::kGreaterThanEquals) {
                compiled.op = BinaryOperation::kGreaterThan;
                compiled.is_double = false;
                compiled.int_constant = fl;
                supported = true;
              } else if (op == BinaryOperation::kLessThan ||
                         op == BinaryOperation::kLessThanEquals) {
                compiled.op = BinaryOperation::kLessThanEquals;
                compiled.is_double = false;
                compiled.int_constant = fl;
                supported = true;
              }
            } else if (!column_object.IsUnsigned() &&
                       column_type == ValueType::kDouble &&
                       constant_value.type == ValueType::kDouble) {
              compiled.is_double = true;
              compiled.double_constant = constant_value.value.double_value;
              supported = true;
            } else if (column_type == ValueType::kVarChar &&
                       constant_value.type == ValueType::kVarChar &&
                       !constant_value.IsCaseInsensitive() &&
                       PlainStringComparable(
                           constant_value.value.varchar_value) &&
                       (op == BinaryOperation::kEquals ||
                        op == BinaryOperation::kNotEquals)) {
              compiled.string_payload = true;
              compiled.string_constant =
                  std::string(constant_value.value.varchar_value);
              supported = true;
            }
            if (supported) {
              return compiled;
            }
          }
        }
      }
    }
  }
  return std::nullopt;
}

std::optional<TypedConditionalInput> TryCompileTypedConditionalInput(
    const Expression& expr, const Schema& scan_schema) {
  if (!expr || expr->Type() != TypeTag::kCaseExp) {
    return std::nullopt;
  }
  const CaseExpression& case_exp = expr->AsCaseExpression();
  if (case_exp.when_clauses_.size() != 1) {
    return std::nullopt;
  }
  if (case_exp.else_clause_) {
    if (case_exp.else_clause_->Type() != TypeTag::kConstantValue ||
        !case_exp.else_clause_->AsConstantValue().GetValue().IsNull()) {
      return std::nullopt;
    }
  }
  const auto& when = case_exp.when_clauses_[0];
  std::optional<TypedFilterCompare> cond =
      TryCompileTypedFilterCompare(when.first, scan_schema, true);
  if (!cond.has_value() || !when.second) {
    return std::nullopt;
  }
  TypedConditionalInput compiled;
  compiled.condition = std::move(*cond);
  if (when.second->Type() == TypeTag::kColumnValue) {
    const auto offset = LocalColumnOffset(
        scan_schema, when.second->AsColumnValue().GetColumnName());
    if (!offset) {
      return std::nullopt;
    }
    const Column& col = scan_schema.GetColumn(static_cast<slot_t>(*offset));
    if (col.IsUnsigned() ||
        (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate &&
         col.Type() != ValueType::kDouble)) {
      return std::nullopt;
    }
    compiled.then_column = static_cast<slot_t>(*offset);
    compiled.result_type = col.Type();
    return compiled;
  }
  if (when.second->Type() == TypeTag::kConstantValue) {
    const Value& val = when.second->AsConstantValue().GetValue();
    if (val.IsNull() || val.IsUnsigned() ||
        (val.type != ValueType::kInt64 && val.type != ValueType::kDate)) {
      return std::nullopt;
    }
    compiled.then_int_constant = val.value.int_value;
    compiled.result_type = val.type;
    return compiled;
  }
  return std::nullopt;
}

namespace {

bool TryBuildTypedDoubleExprOps(const Expression& expr,
                                const Schema& scan_schema,
                                std::vector<TypedDoubleExprOp>* ops,
                                bool* saw_double_col) {
  if (!expr || ops->size() >= 15) {
    return false;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const auto offset =
        LocalColumnOffset(scan_schema, expr->AsColumnValue().GetColumnName());
    if (!offset) {
      return false;
    }
    const Column& col = scan_schema.GetColumn(static_cast<slot_t>(*offset));
    if (col.IsUnsigned()) {
      return false;
    }
    if (col.Type() == ValueType::kDouble) {
      *saw_double_col = true;
      ops->push_back(
          TypedDoubleExprOp{.kind = TypedDoubleExprOp::Kind::kLoadDoubleCol,
                            .column = static_cast<slot_t>(*offset)});
      return true;
    }
    if (col.Type() == ValueType::kInt64) {
      ops->push_back(TypedDoubleExprOp{
          .kind = TypedDoubleExprOp::Kind::kLoadIntColAsDouble,
          .column = static_cast<slot_t>(*offset)});
      return true;
    }
    return false;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    const Value& val = expr->AsConstantValue().GetValue();
    if (val.IsNull() || val.IsUnsigned()) {
      return false;
    }
    if (val.type == ValueType::kDouble) {
      ops->push_back(
          TypedDoubleExprOp{.kind = TypedDoubleExprOp::Kind::kConstant,
                            .constant = val.value.double_value});
      return true;
    }
    if (val.type == ValueType::kInt64) {
      ops->push_back(TypedDoubleExprOp{
          .kind = TypedDoubleExprOp::Kind::kConstant,
          .constant = static_cast<double>(val.value.int_value)});
      return true;
    }
    return false;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& bin = expr->AsBinaryExpression();
    TypedDoubleExprOp::Kind kind = TypedDoubleExprOp::Kind::kConstant;
    if (bin.Op() == BinaryOperation::kAdd) {
      kind = TypedDoubleExprOp::Kind::kAdd;
    } else if (bin.Op() == BinaryOperation::kSubtract) {
      kind = TypedDoubleExprOp::Kind::kSub;
    } else if (bin.Op() == BinaryOperation::kMultiply) {
      kind = TypedDoubleExprOp::Kind::kMul;
    } else {
      return false;
    }
    if (!TryBuildTypedDoubleExprOps(bin.Left(), scan_schema, ops,
                                    saw_double_col) ||
        !TryBuildTypedDoubleExprOps(bin.Right(), scan_schema, ops,
                                    saw_double_col) ||
        ops->size() >= 16) {
      return false;
    }
    ops->push_back(TypedDoubleExprOp{.kind = kind});
    return true;
  }
  return false;
}

}  // namespace

std::optional<TypedDoubleExprInput> TryCompileTypedDoubleExpr(
    const Expression& expr, const Schema& scan_schema) {
  TypedDoubleExprInput compiled;
  bool saw_double_col = false;
  if (!TryBuildTypedDoubleExprOps(expr, scan_schema, &compiled.ops,
                                  &saw_double_col) ||
      !saw_double_col || compiled.ops.empty()) {
    return std::nullopt;
  }
  const auto& ops = compiled.ops;
  if (ops.size() == 1 &&
      ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol) {
    compiled.shape = TypedDoubleExprInput::Shape::kSingleDoubleCol;
    compiled.col0 = ops[0].column;
  } else if (ops.size() == 3 &&
             ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[1].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[2].kind == TypedDoubleExprOp::Kind::kMul) {
    compiled.shape = TypedDoubleExprInput::Shape::kMulTwoDoubleCols;
    compiled.col0 = ops[0].column;
    compiled.col1 = ops[1].column;
  } else if (ops.size() == 5 &&
             ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[1].kind == TypedDoubleExprOp::Kind::kConstant &&
             ops[2].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[3].kind == TypedDoubleExprOp::Kind::kSub &&
             ops[4].kind == TypedDoubleExprOp::Kind::kMul) {
    compiled.shape = TypedDoubleExprInput::Shape::kMulConstMinusCol;
    compiled.col0 = ops[0].column;
    compiled.k0 = ops[1].constant;
    compiled.col1 = ops[2].column;
  } else if (ops.size() == 9 &&
             ops[0].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[1].kind == TypedDoubleExprOp::Kind::kConstant &&
             ops[2].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[3].kind == TypedDoubleExprOp::Kind::kSub &&
             ops[4].kind == TypedDoubleExprOp::Kind::kMul &&
             ops[5].kind == TypedDoubleExprOp::Kind::kConstant &&
             ops[6].kind == TypedDoubleExprOp::Kind::kLoadDoubleCol &&
             ops[7].kind == TypedDoubleExprOp::Kind::kAdd &&
             ops[8].kind == TypedDoubleExprOp::Kind::kMul) {
    compiled.shape =
        TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol;
    compiled.col0 = ops[0].column;
    compiled.k0 = ops[1].constant;
    compiled.col1 = ops[2].column;
    compiled.k1 = ops[5].constant;
    compiled.col2 = ops[6].column;
  }
  return compiled;
}

namespace {

bool TryBuildTypedIntExprOps(const Expression& expr, const Schema& scan_schema,
                             std::vector<TypedIntExprOp>* ops) {
  if (!expr || ops->size() >= 15) {
    return false;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const auto offset =
        LocalColumnOffset(scan_schema, expr->AsColumnValue().GetColumnName());
    if (!offset) {
      return false;
    }
    const Column& col = scan_schema.GetColumn(static_cast<slot_t>(*offset));
    if (col.IsUnsigned() || col.Type() != ValueType::kInt64) {
      return false;
    }
    ops->push_back(TypedIntExprOp{.kind = TypedIntExprOp::Kind::kLoadIntCol,
                                  .column = static_cast<slot_t>(*offset)});
    return true;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    const Value& val = expr->AsConstantValue().GetValue();
    if (val.IsNull() || val.IsUnsigned() || val.type != ValueType::kInt64) {
      return false;
    }
    ops->push_back(TypedIntExprOp{.kind = TypedIntExprOp::Kind::kConstant,
                                  .constant = val.value.int_value});
    return true;
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const BinaryExpression& bin = expr->AsBinaryExpression();
    TypedIntExprOp::Kind kind = TypedIntExprOp::Kind::kConstant;
    if (bin.Op() == BinaryOperation::kAdd) {
      kind = TypedIntExprOp::Kind::kAdd;
    } else if (bin.Op() == BinaryOperation::kSubtract) {
      kind = TypedIntExprOp::Kind::kSub;
    } else if (bin.Op() == BinaryOperation::kMultiply) {
      kind = TypedIntExprOp::Kind::kMul;
    } else {
      return false;
    }
    if (!TryBuildTypedIntExprOps(bin.Left(), scan_schema, ops) ||
        !TryBuildTypedIntExprOps(bin.Right(), scan_schema, ops) ||
        ops->size() >= 16) {
      return false;
    }
    ops->push_back(TypedIntExprOp{.kind = kind});
    return true;
  }
  return false;
}

}  // namespace

std::optional<TypedIntExprInput> TryCompileTypedIntExpr(
    const Expression& expr, const Schema& scan_schema) {
  TypedIntExprInput compiled;
  if (!TryBuildTypedIntExprOps(expr, scan_schema, &compiled.ops) ||
      compiled.ops.empty()) {
    return std::nullopt;
  }
  const auto& ops = compiled.ops;
  if (ops.size() == 1 && ops[0].kind == TypedIntExprOp::Kind::kLoadIntCol) {
    compiled.shape = TypedIntExprInput::Shape::kSingleIntCol;
    compiled.col0 = ops[0].column;
  } else if (ops.size() == 3 &&
             ops[0].kind == TypedIntExprOp::Kind::kLoadIntCol &&
             ops[1].kind == TypedIntExprOp::Kind::kLoadIntCol &&
             ops[2].kind == TypedIntExprOp::Kind::kMul) {
    compiled.shape = TypedIntExprInput::Shape::kMulTwoIntCols;
    compiled.col0 = ops[0].column;
    compiled.col1 = ops[1].column;
  } else if (ops.size() == 5 &&
             ops[0].kind == TypedIntExprOp::Kind::kLoadIntCol &&
             ops[1].kind == TypedIntExprOp::Kind::kLoadIntCol &&
             ops[2].kind == TypedIntExprOp::Kind::kMul &&
             ops[3].kind == TypedIntExprOp::Kind::kLoadIntCol &&
             ops[4].kind == TypedIntExprOp::Kind::kAdd) {
    compiled.shape = TypedIntExprInput::Shape::kMulTwoIntColsPlusCol;
    compiled.col0 = ops[0].column;
    compiled.col1 = ops[1].column;
    compiled.col2 = ops[3].column;
  }
  return compiled;
}

StatusOr<Value> EvaluateTypedIntExprOnRow(const TypedIntExprInput& expr,
                                          const Row& row) {
  std::array<int64_t, 16> stack{};
  size_t sp = 0;
  for (const TypedIntExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedIntExprOp::Kind::kLoadIntCol: {
        const Value& v = row[op.column];
        if (v.IsNull()) {
          return Value();
        }
        stack[sp++] = v.value.int_value;
        break;
      }
      case TypedIntExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedIntExprOp::Kind::kAdd: {
        int64_t out = 0;
        if (__builtin_add_overflow(stack[sp - 2], stack[sp - 1], &out)) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow on '+'");
        }
        stack[sp - 2] = out;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kSub: {
        int64_t out = 0;
        if (__builtin_sub_overflow(stack[sp - 2], stack[sp - 1], &out)) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow on '-'");
        }
        stack[sp - 2] = out;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kMul: {
        int64_t out = 0;
        if (__builtin_mul_overflow(stack[sp - 2], stack[sp - 1], &out)) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow on '*'");
        }
        stack[sp - 2] = out;
        --sp;
        break;
      }
    }
  }
  return Value(stack[0]);
}

Value EvaluateTypedConditionalOnRow(const TypedConditionalInput& cond,
                                    const Row& row) {
  const Value& left = row[cond.condition.left];
  bool matched = false;
  if (cond.condition.constant_payload) {
    matched =
        !left.IsNull() && DecideTypedFilterCompare(left, left, cond.condition);
  } else {
    const Value& right = row[cond.condition.right];
    matched = !left.IsNull() && !right.IsNull() &&
              DecideTypedFilterCompare(left, right, cond.condition);
  }
  if (!matched) {
    return {};
  }
  if (cond.then_column.has_value()) {
    return row[*cond.then_column];
  }
  Value v(cond.then_int_constant);
  v.type = cond.result_type;
  return v;
}

Value EvaluateTypedDoubleExprOnRow(const TypedDoubleExprInput& expr,
                                   const Row& row) {
  std::array<double, 16> stack{};
  size_t sp = 0;
  for (const TypedDoubleExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedDoubleExprOp::Kind::kLoadDoubleCol: {
        const Value& v = row[op.column];
        if (v.IsNull()) {
          return {};
        }
        stack[sp++] = v.value.double_value;
        break;
      }
      case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
        const Value& v = row[op.column];
        if (v.IsNull()) {
          return {};
        }
        stack[sp++] = static_cast<double>(v.value.int_value);
        break;
      }
      case TypedDoubleExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedDoubleExprOp::Kind::kAdd:
        stack[sp - 2] += stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kSub:
        stack[sp - 2] -= stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kMul:
        stack[sp - 2] *= stack[sp - 1];
        --sp;
        break;
    }
  }
  return Value(stack[0]);
}

TypedAggregationPlan PlanTypedAggregation(
    const SelectStatement& stmt, const Expression& having_expr,
    const Schema& scan_schema,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates) {
  TypedAggregationPlan plan;
  if (!BytecodeEnabled()) {
    return plan;
  }
  // Operational escape hatch: TINYLAMB_DISABLE_TYPED_AGG=1 routes every
  // statement back through the generic stream-aggregation loop.
  static const bool typed_agg_disabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_AGG") != nullptr;
  if (typed_agg_disabled) {
    return plan;
  }
  if (stmt.Distinct()) {
    return plan;
  }
  // Group keys: plain columns only.
  for (const Expression& key : stmt.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return plan;
    }
    const auto offset =
        LocalColumnOffset(scan_schema, key->AsColumnValue().GetColumnName());
    if (!offset) {
      return plan;
    }
    plan.key_offsets.push_back(static_cast<slot_t>(*offset));
  }
  // WHERE: every conjunct must compile to context-free bytecode.
  static const bool filter_peek_disabled =
      std::getenv("TINYLAMB_DISABLE_AGG_FILTER_PEEK") != nullptr;
  // A/B hatch: TINYLAMB_DISABLE_TYPED_CONST_PEEK=1 keeps col-vs-constant
  // conjuncts in the bytecode programs (col-vs-col peeling stays on).
  static const bool const_peek_disabled =
      std::getenv("TINYLAMB_DISABLE_TYPED_CONST_PEEK") != nullptr;
  // Only a PREFIX of the conjunct list may peel to positional compares: the
  // typed path evaluates conjuncts in order and a raising program aborts the
  // aggregate, so reordering a peel past a raising conjunct could silently
  // swallow its error.
  bool peel_allowed = !filter_peek_disabled;
  for (const Expression& predicate : scan_predicates) {
    if (!predicate) {
      continue;
    }
    if (ContainsQuery(predicate)) {
      return plan;
    }
    bool peeled = false;
    if (peel_allowed && !const_peek_disabled &&
        predicate->Type() == TypeTag::kInExp) {
      // Constant IN-list peel (`col IN ('RAIL', 'SHIP', ...)`): plain
      // same-type constants only, so the per-row decision is a short linear
      // scan with no dispatch.  NULL cells and no-match both reject the row,
      // which is what the SQL semantics demand in a WHERE conjunct.
      const InExpression& membership = predicate->AsInExpression();
      if (!membership.list_.empty() && membership.list_.size() <= 64 &&
          membership.child_->Type() == TypeTag::kColumnValue) {
        const auto offset = LocalColumnOffset(
            scan_schema, membership.child_->AsColumnValue().GetColumnName());
        if (offset) {
          const Column& column_object =
              scan_schema.GetColumn(static_cast<slot_t>(*offset));
          const ValueType column_type = column_object.Type();
          TypedInListCompare compiled;
          compiled.column = static_cast<slot_t>(*offset);
          bool all_plain = !column_object.IsUnsigned();
          if (column_type == ValueType::kVarChar) {
            compiled.is_string = true;
          } else if (column_type == ValueType::kDouble) {
            compiled.is_double = true;
          } else if (column_type != ValueType::kInt64 &&
                     column_type != ValueType::kDate) {
            all_plain = false;
          }
          for (const Expression& item : membership.list_) {
            if (!all_plain || !item ||
                item->Type() != TypeTag::kConstantValue) {
              all_plain = false;
              break;
            }
            const Value& item_value = item->AsConstantValue().GetValue();
            if (item_value.IsNull() ||
                (!compiled.is_string && item_value.type != column_type)) {
              all_plain = false;
              break;
            }
            if (compiled.is_string) {
              if (item_value.type != ValueType::kVarChar ||
                  item_value.IsCaseInsensitive() ||
                  !PlainStringComparable(item_value.value.varchar_value)) {
                all_plain = false;
                break;
              }
              compiled.strings.emplace_back(item_value.value.varchar_value);
            } else if (compiled.is_double) {
              compiled.double_values.push_back(item_value.value.double_value);
            } else {
              compiled.int_values.push_back(item_value.value.int_value);
            }
          }
          if (all_plain) {
            plan.inlist_compares.push_back(std::move(compiled));
            peeled = true;
          }
        }
      } else if (!membership.list_.empty() && membership.list_.size() <= 64) {
        const CompiledScanFilter cf =
            CompileScanFilter({predicate}, scan_schema);
        if (cf.prefix_in.size() == 1 && cf.residual.empty() &&
            !cf.prefix_in[0].prefixes.empty()) {
          TypedInListCompare compiled;
          compiled.column = cf.prefix_in[0].column;
          compiled.is_string = true;
          compiled.prefix_len = cf.prefix_in[0].prefixes[0].size();
          compiled.strings = cf.prefix_in[0].prefixes;
          plan.inlist_compares.push_back(std::move(compiled));
          peeled = true;
        }
      }
    }
    if (peel_allowed && !peeled && predicate->Type() == TypeTag::kBinaryExp) {
      // Column-vs-column compares of one numeric type evaluate positionally:
      // a multi-million-row fold (Q21's l_receiptdate > l_commitdate) skips
      // the per-row bytecode dispatch.  Such a compare can never raise, so
      // the conjunct-order error semantics are unchanged.
      const BinaryExpression& binary = predicate->AsBinaryExpression();
      if (IsComparison(binary.Op()) &&
          binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kColumnValue) {
        const auto left_offset = LocalColumnOffset(
            scan_schema, binary.Left()->AsColumnValue().GetColumnName());
        const auto right_offset = LocalColumnOffset(
            scan_schema, binary.Right()->AsColumnValue().GetColumnName());
        if (left_offset && right_offset) {
          const Column& left_column =
              scan_schema.GetColumn(static_cast<slot_t>(*left_offset));
          const Column& right_column =
              scan_schema.GetColumn(static_cast<slot_t>(*right_offset));
          const ValueType left_type = left_column.Type();
          if (!left_column.IsUnsigned() && !right_column.IsUnsigned() &&
              left_type == right_column.Type() &&
              (left_type == ValueType::kInt64 ||
               left_type == ValueType::kDate ||
               left_type == ValueType::kDouble)) {
            plan.filter_compares.push_back(TypedFilterCompare{
                .left = static_cast<slot_t>(*left_offset),
                .right = static_cast<slot_t>(*right_offset),
                .op = binary.Op(),
                .is_double = left_type == ValueType::kDouble});
            peeled = true;
          }
        }
      } else if (IsComparison(binary.Op()) && !const_peek_disabled &&
                 (binary.Left()->Type() == TypeTag::kColumnValue ||
                  binary.Right()->Type() == TypeTag::kColumnValue)) {
        // Column-vs-constant compares peel the same way after constant
        // folding (Q1's `l_shipdate <= date_sub(date '1998-12-01',
        // INTERVAL 74 day)` folds to a DATE constant).  Folding reuses the
        // scan-filter path's rules, including the NOT-of-ordered-double
        // suppression, so a peel never changes a NaN verdict.  A folding
        // failure keeps the conjunct in bytecode exactly as before.
        ExpressionRewriter rewriter(
            ContainsNotOfOrderedDoubleComparison(predicate, scan_schema)
                ? NotComparisonFreeRules()
                : ExpressionRuleSet::Default());
        rewriter.set_schema(&scan_schema);
        StatusOr<Expression> folded_or = rewriter.TryRewrite(predicate);
        if (folded_or.HasValue() && folded_or.Value() &&
            folded_or.Value()->Type() == TypeTag::kBinaryExp) {
          const BinaryExpression& folded =
              folded_or.Value()->AsBinaryExpression();
          if (IsComparison(folded.Op())) {
            Expression column = folded.Left();
            Expression constant = folded.Right();
            BinaryOperation op = folded.Op();
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
            if (column->Type() == TypeTag::kConstantValue &&
                constant->Type() == TypeTag::kColumnValue) {
              std::swap(column, constant);
              op = FlipTypedCompare(op);
            }
            if (column->Type() == TypeTag::kColumnValue &&
                constant->Type() == TypeTag::kConstantValue) {
              const auto offset = LocalColumnOffset(
                  scan_schema, column->AsColumnValue().GetColumnName());
              const Value& constant_value =
                  constant->AsConstantValue().GetValue();
              if (offset && !constant_value.IsNull()) {
                const Column& column_object =
                    scan_schema.GetColumn(static_cast<slot_t>(*offset));
                const ValueType column_type = column_object.Type();
                TypedFilterCompare compiled{
                    .left = static_cast<slot_t>(*offset),
                    .op = op,
                    .constant_payload = true};
                bool supported = false;
                if (!column_object.IsUnsigned() &&
                    (column_type == ValueType::kInt64 ||
                     column_type == ValueType::kDate) &&
                    constant_value.type == column_type) {
                  compiled.is_double = false;
                  compiled.int_constant = constant_value.value.int_value;
                  supported = true;
                } else if (!column_object.IsUnsigned() &&
                           column_type == ValueType::kInt64 &&
                           constant_value.type == ValueType::kDouble &&
                           std::isfinite(constant_value.value.double_value) &&
                           constant_value.value.double_value >=
                               -static_cast<double>(int64_t{1} << 52) &&
                           constant_value.value.double_value <=
                               static_cast<double>(int64_t{1} << 52)) {
                  const double d = constant_value.value.double_value;
                  const auto fl = static_cast<int64_t>(std::floor(d));
                  const auto cl = static_cast<int64_t>(std::ceil(d));
                  if (fl == cl) {
                    compiled.is_double = false;
                    compiled.int_constant = fl;
                    supported = true;
                  } else if (op == BinaryOperation::kGreaterThan ||
                             op == BinaryOperation::kGreaterThanEquals) {
                    compiled.op = BinaryOperation::kGreaterThan;
                    compiled.is_double = false;
                    compiled.int_constant = fl;
                    supported = true;
                  } else if (op == BinaryOperation::kLessThan ||
                             op == BinaryOperation::kLessThanEquals) {
                    compiled.op = BinaryOperation::kLessThanEquals;
                    compiled.is_double = false;
                    compiled.int_constant = fl;
                    supported = true;
                  }
                } else if (!column_object.IsUnsigned() &&
                           column_type == ValueType::kDouble &&
                           (constant_value.type == ValueType::kDouble ||
                            (constant_value.type == ValueType::kInt64 &&
                             !constant_value.IsUnsigned() &&
                             constant_value.value.int_value >=
                                 -(int64_t{1} << 53) &&
                             constant_value.value.int_value <=
                                 (int64_t{1} << 53)))) {
                  compiled.is_double = true;
                  compiled.double_constant =
                      constant_value.type == ValueType::kDouble
                          ? constant_value.value.double_value
                          : static_cast<double>(constant_value.value.int_value);
                  supported = true;
                } else if (column_type == ValueType::kVarChar &&
                           constant_value.type == ValueType::kVarChar &&
                           !constant_value.IsCaseInsensitive() &&
                           PlainStringComparable(
                               constant_value.value.varchar_value) &&
                           (op == BinaryOperation::kEquals ||
                            op == BinaryOperation::kNotEquals)) {
                  compiled.string_payload = true;
                  compiled.string_constant =
                      std::string(constant_value.value.varchar_value);
                  supported = true;
                }
                if (supported) {
                  plan.filter_compares.push_back(std::move(compiled));
                  peeled = true;
                }
              }
            }
          }
        }
      }
    }
    if (!peeled) {
      peel_allowed = false;
      std::optional<BytecodeProgram> compiled =
          BytecodeCompiler::CompileRowCached(
              predicate, scan_schema, BytecodeCompiler::Context::kFilter);
      if (!compiled.has_value() || compiled->ResultType() == ValueType::kNull) {
        return plan;
      }
      plan.filter_programs.push_back(std::move(*compiled));
    }
  }
  // Aggregates: exactly the accumulator kinds whose partial states merge.
  for (const AggregateExpression* aggregate : aggregate_expressions) {
    const AggregationType type = aggregate->GetType();
    if (type != AggregationType::kCount && type != AggregationType::kSum &&
        type != AggregationType::kAvg && type != AggregationType::kMin &&
        type != AggregationType::kMax) {
      return plan;
    }
    if (aggregate->NeedsGroupContext() || aggregate->WhereFilter() ||
        aggregate->Having() != AggregateHavingModifier::kNone ||
        aggregate->HavingCondition() || !aggregate->InnerOrderBy().empty() ||
        aggregate->SecondaryArg() || !aggregate->TrailingArgs().empty()) {
      return plan;
    }
    if (aggregate->Distinct() && type != AggregationType::kCount) {
      return plan;
    }
    TypedAggregateInput input;
    input.aggregate = aggregate;
    if (!aggregate->Child() || IsCountStar(*aggregate)) {
      plan.aggregates.push_back(std::move(input));
      continue;
    }
    const Expression& child = aggregate->Child();
    if (child->Type() == TypeTag::kColumnValue) {
      const auto offset = LocalColumnOffset(
          scan_schema, child->AsColumnValue().GetColumnName());
      if (!offset) {
        return plan;
      }
      input.column = static_cast<slot_t>(*offset);
    } else if (std::optional<TypedConditionalInput> cond =
                   TryCompileTypedConditionalInput(child, scan_schema);
               cond.has_value()) {
      input.conditional = std::move(*cond);
    } else if (!aggregate->Distinct() &&
               (type == AggregationType::kSum ||
                type == AggregationType::kAvg) &&
               [&]() {
                 std::optional<TypedDoubleExprInput> dbl =
                     TryCompileTypedDoubleExpr(child, scan_schema);
                 if (dbl.has_value()) {
                   input.double_expr = std::move(*dbl);
                   return true;
                 }
                 std::optional<TypedIntExprInput> iexpr =
                     TryCompileTypedIntExpr(child, scan_schema);
                 if (iexpr.has_value()) {
                   input.int_expr = std::move(*iexpr);
                   return true;
                 }
                 return false;
               }()) {
      // Handled inline as TypedDoubleExprInput or TypedIntExprInput.
    } else {
      if (ContainsQuery(child)) {
        return plan;
      }
      std::optional<BytecodeProgram> compiled =
          BytecodeCompiler::CompileRowCached(child, scan_schema,
                                             BytecodeCompiler::Context::kValue);
      if (!compiled.has_value() || compiled->ResultType() == ValueType::kNull) {
        return plan;
      }
      // A SUM/AVG over a computed child must not land in the unsigned
      // inference path: only exact int64/double results qualify.
      if ((type == AggregationType::kSum || type == AggregationType::kAvg) &&
          compiled->ResultType() != ValueType::kInt64 &&
          compiled->ResultType() != ValueType::kDouble) {
        return plan;
      }
      input.child = std::move(*compiled);
    }
    plan.aggregates.push_back(std::move(input));
  }
  // Slim-state classification: COUNT/SUM/AVG over signed INT64 or DOUBLE
  // inputs keep a packed count+total pair per group instead of the generic
  // ~300-byte accumulator.  One kNone anywhere keeps the whole plan on the
  // chunked-accumulator layout (MIN/MAX need the extreme Value, DISTINCT
  // needs the set, UINT64 needs the unsigned domain — none are slim).
  static const bool slim_disabled =
      std::getenv("TINYLAMB_DISABLE_SLIM_AGG") != nullptr;
  plan.slim_kinds.reserve(plan.aggregates.size());
  bool all_slim = !plan.aggregates.empty() && !slim_disabled;
  for (const TypedAggregateInput& input : plan.aggregates) {
    const AggregateExpression* aggregate = input.aggregate;
    const AggregationType type = aggregate->GetType();
    SlimAggKind kind = SlimAggKind::kNone;
    const bool distinct_free = !aggregate->Distinct();
    if (distinct_free && type == AggregationType::kCount) {
      kind = SlimAggKind::kCount;
    } else if (distinct_free && (type == AggregationType::kSum ||
                                 type == AggregationType::kAvg)) {
      ValueType input_type = ValueType::kNull;
      if (input.column.has_value()) {
        const Column& column =
            scan_schema.GetColumn(static_cast<slot_t>(*input.column));
        if (!column.IsUnsigned()) {
          input_type = column.Type();
        }
      } else if (input.conditional.has_value()) {
        input_type = input.conditional->result_type;
      } else if (input.double_expr.has_value()) {
        input_type = ValueType::kDouble;
      } else if (input.int_expr.has_value()) {
        input_type = ValueType::kInt64;
      } else if (input.child.has_value()) {
        input_type = input.child->ResultType();
      }
      if (type == AggregationType::kAvg) {
        if (input_type == ValueType::kInt64 ||
            input_type == ValueType::kDouble) {
          kind = SlimAggKind::kAvgDouble;
        }
      } else if (input_type == ValueType::kInt64) {
        kind = SlimAggKind::kSumInt64;
      } else if (input_type == ValueType::kDouble) {
        kind = SlimAggKind::kSumDouble;
      }
    } else if (!distinct_free && type == AggregationType::kCount) {
      // COUNT(DISTINCT ...) over an INT64/DATE input keeps the inline-slot
      // slim layout (same 8-slot threshold as the generic accumulator).
      ValueType input_type = ValueType::kNull;
      if (input.column.has_value()) {
        input_type =
            scan_schema.GetColumn(static_cast<slot_t>(*input.column)).Type();
      } else if (input.conditional.has_value()) {
        input_type = input.conditional->result_type;
      } else if (input.child.has_value()) {
        input_type = input.child->ResultType();
      }
      if (input_type == ValueType::kInt64 || input_type == ValueType::kDate) {
        kind = SlimAggKind::kCountDistinctInt64;
      }
    }
    plan.slim_kinds.push_back(kind);
    all_slim = all_slim && kind != SlimAggKind::kNone;
  }
  plan.slim = all_slim;
  // Classifies one output expression as a group-key column or a top-level
  // aggregate; `alias` names the select item it came from for ORDER BY
  // alias resolution (empty for direct select items).
  const auto classify = [&](const Expression& expression,
                            std::string_view alias,
                            TypedEmitItem* item) -> bool {
    if (!expression) {
      return false;
    }
    if (!alias.empty() && expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = TypedEmitItem{.is_key = false,
                              .index = static_cast<size_t>(std::distance(
                                  aggregate_expressions.begin(), pos))};
        return true;
      }
      return false;
    }
    if (expression->Type() == TypeTag::kColumnValue &&
        expression->AsColumnValue().GetColumnName().name != "*") {
      const auto offset = LocalColumnOffset(
          scan_schema, expression->AsColumnValue().GetColumnName());
      if (offset) {
        const auto key_pos =
            std::find(plan.key_offsets.begin(), plan.key_offsets.end(),
                      static_cast<slot_t>(*offset));
        if (key_pos != plan.key_offsets.end()) {
          *item = TypedEmitItem{.is_key = true,
                                .index = static_cast<size_t>(std::distance(
                                    plan.key_offsets.begin(), key_pos))};
          return true;
        }
      }
    }
    if (expression->Type() == TypeTag::kAggregateExp) {
      const auto pos =
          std::find(aggregate_expressions.begin(), aggregate_expressions.end(),
                    &expression->AsAggregateExpression());
      if (pos != aggregate_expressions.end()) {
        *item = TypedEmitItem{.is_key = false,
                              .index = static_cast<size_t>(std::distance(
                                  aggregate_expressions.begin(), pos))};
        return true;
      }
    }
    return false;
  };
  for (const NamedExpression& item : stmt.SelectList()) {
    TypedEmitItem emit_item;
    if (!classify(item.expression, "", &emit_item)) {
      return plan;
    }
    plan.emit_items.push_back(emit_item);
  }
  for (const auto& term : stmt.OrderBy()) {
    TypedEmitItem order_item;
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty()) {
      // ORDER BY terms may reference select-list aliases.
      const ColumnName& name = term.expression->AsColumnValue().GetColumnName();
      bool matched = false;
      for (const auto& item : stmt.SelectList()) {
        if (item.name.size() != name.name.size() ||
            !std::equal(
                item.name.begin(), item.name.end(), name.name.begin(),
                [](char left, char right) {
                  return std::tolower(static_cast<unsigned char>(left)) ==
                         std::tolower(static_cast<unsigned char>(right));
                })) {
          continue;
        }
        if (!classify(item.expression, item.name, &order_item)) {
          return plan;
        }
        matched = true;
        break;
      }
      if (matched) {
        plan.order_items.push_back(order_item);
        plan.order_ascending.push_back(term.ascending);
        plan.order_nulls_first.push_back(
            term.nulls_first.value_or(term.ascending));
        continue;
      }
    }
    if (!classify(term.expression, "", &order_item)) {
      return plan;
    }
    plan.order_items.push_back(order_item);
    plan.order_ascending.push_back(term.ascending);
    plan.order_nulls_first.push_back(term.nulls_first.value_or(term.ascending));
  }
  if (having_expr) {
    if (ContainsQuery(having_expr) ||
        !HavingReferencesOnlyKeysAndAggregates(having_expr, scan_schema,
                                               plan.key_offsets)) {
      return plan;
    }
  }
  plan.eligible = true;
  return plan;
}

namespace {

BinaryOperation FlipHavingCompare(BinaryOperation operation) {
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

std::optional<PositionalHaving> TryCompilePositionalHaving(
    const Expression& having,
    const std::vector<TypedAggregateInput>& aggregates) {
  if (!having) {
    return std::nullopt;
  }
  const bool is_binary = having->Type() == TypeTag::kBinaryExp;
  if (!is_binary) {
    // Bare `HAVING AGG`: truthy test on the finished value.
    if (having->Type() != TypeTag::kAggregateExp) {
      return std::nullopt;
    }
    const AggregateExpression* aggregate = &having->AsAggregateExpression();
    for (size_t i = 0; i < aggregates.size(); ++i) {
      if (aggregates[i].aggregate == aggregate) {
        PositionalHaving compiled;
        compiled.agg_index = i;
        compiled.has_constant = false;
        return compiled;
      }
    }
    return std::nullopt;
  }
  const BinaryExpression& binary = having->AsBinaryExpression();
  if (!IsComparison(binary.Op())) {
    return std::nullopt;
  }
  const Expression lhs = binary.Left();
  const Expression rhs = binary.Right();
  const AggregateExpression* aggregate = nullptr;
  bool agg_on_right = false;
  if (lhs && lhs->Type() == TypeTag::kAggregateExp) {
    aggregate = &lhs->AsAggregateExpression();
  } else if (rhs && rhs->Type() == TypeTag::kAggregateExp) {
    aggregate = &rhs->AsAggregateExpression();
    agg_on_right = true;
  } else {
    return std::nullopt;
  }
  const Expression& other = agg_on_right ? lhs : rhs;
  if (!other || other->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  size_t index = aggregates.size();
  for (size_t i = 0; i < aggregates.size(); ++i) {
    if (aggregates[i].aggregate == aggregate) {
      index = i;
      break;
    }
  }
  if (index == aggregates.size()) {
    return std::nullopt;
  }
  PositionalHaving compiled;
  compiled.agg_index = index;
  compiled.op = agg_on_right ? FlipHavingCompare(binary.Op()) : binary.Op();
  compiled.constant = other->AsConstantValue().GetValue();
  compiled.has_constant = true;
  return compiled;
}

bool TryCompilePositionalHavingConjuncts(
    const Expression& having,
    const std::vector<TypedAggregateInput>& aggregates,
    std::vector<PositionalHaving>* out) {
  if (!having) {
    return false;
  }
  if (having->Type() == TypeTag::kBinaryExp &&
      having->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
    const BinaryExpression& bin = having->AsBinaryExpression();
    return TryCompilePositionalHavingConjuncts(bin.Left(), aggregates, out) &&
           TryCompilePositionalHavingConjuncts(bin.Right(), aggregates, out);
  }
  std::optional<PositionalHaving> single =
      TryCompilePositionalHaving(having, aggregates);
  if (!single.has_value()) {
    return false;
  }
  out->push_back(std::move(*single));
  return true;
}

// Statement-level classification for fusing the final join with this
// statement's grouping (see GroupedJoinFusionSpec).  Schema-dependent checks
// (bytecode compilability against the joined schema) run at the join step in
// BuildInput; this pass only decides whether the statement shape qualifies.
GroupedJoinFusionSpec BuildGroupedJoinFusionSpec(
    const SelectStatement& statement) {
  GroupedJoinFusionSpec spec;
  if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
    std::cerr << "[fusion] spec: sources=" << statement.Sources().size()
              << " group_by=" << statement.GroupBy().size()
              << " distinct=" << statement.Distinct()
              << " having=" << (statement.Having() != nullptr)
              << " order=" << statement.OrderBy().size() << "\n";
  }
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION") != nullptr;
  static const bool debug_fusion =
      std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr;
  const auto bail = [&spec](const char* reason) {
    if (debug_fusion) {
      std::cerr << "[fusion] spec bail: " << reason << "\n";
    }
    return spec;
  };
  if (disabled || statement.Sources().size() < 2 || statement.Distinct() ||
      statement.Having() || HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement)) {
    return bail("shape head");
  }
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::ranges::any_of(statement.SelectList(), [](const NamedExpression& e) {
        return ContainsAggregate(e.expression);
      });
  if (!grouped) {
    return bail("not grouped");
  }
  // GROUP BY must not lean on select-list alias resolution: the fused emit
  // evaluates keys against the joined schema before any projection exists.
  for (const Expression& key : statement.GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue ||
        ContainsAggregate(key)) {
      continue;
    }
    const ColumnName& column = key->AsColumnValue().GetColumnName();
    if (!column.schema.empty()) {
      continue;
    }
    for (const NamedExpression& item : statement.SelectList()) {
      // A select item whose expression IS this same column does not shadow
      // it; only an alias for a different expression does.
      if (!item.name.empty() && IdentifierEquals(item.name, column.name) &&
          (!item.expression ||
           item.expression->Type() != TypeTag::kColumnValue ||
           !IdentifierEquals(
               item.expression->AsColumnValue().GetColumnName().name,
               column.name))) {
        return bail("alias group by");  // keep the materializing path
      }
    }
  }
  std::vector<const AggregateExpression*> aggregates;
  std::unordered_set<const AggregateExpression*> seen;
  for (const NamedExpression& item : statement.SelectList()) {
    CollectAggregates(item.expression, &aggregates, &seen);
  }
  for (const AggregateExpression* aggregate : aggregates) {
    // Only mergeable streaming kinds fuse: the parallel build merges
    // thread-local accumulator states.  COUNT(DISTINCT plain column) merges
    // as an exact set union; DISTINCT on other kinds stays serial.  The
    // child may be an arbitrary expression (Q9's SUM of arithmetic): the
    // join-step gate compiles it against the joined schema and falls back to
    // the materializing join when compilation fails.
    const bool distinct_count = aggregate->Distinct() &&
                                aggregate->GetType() == AggregationType::kCount;
    const bool supported_kind =
        aggregate->GetType() == AggregationType::kCount ||
        aggregate->GetType() == AggregationType::kSum ||
        aggregate->GetType() == AggregationType::kMin ||
        aggregate->GetType() == AggregationType::kMax ||
        aggregate->GetType() == AggregationType::kAvg;
    if (!supported_kind || aggregate->WhereFilter() ||
        (!IsCountStar(*aggregate) &&
         (aggregate->Having() != AggregateHavingModifier::kNone ||
          !aggregate->InnerOrderBy().empty() || aggregate->SecondaryArg() ||
          !aggregate->ExtraArgs().empty() || aggregate->NeedsGroupContext() ||
          !aggregate->TrailingArgs().empty() ||
          (aggregate->Distinct() && !distinct_count)))) {
      return bail("aggregate kind");
    }
  }
  // Every ORDER BY term must resolve against the projected output (an
  // ordinal or a select-item alias): the fused path orders after emission.
  spec.output_order = statement.OrderBy();
  for (SelectStatement::OrderByTerm& term : spec.output_order) {
    if (!term.expression) {
      return spec;
    }
    if (term.expression->Type() == TypeTag::kConstantValue) {
      const Value& value = term.expression->AsConstantValue().GetValue();
      if (value.type == ValueType::kInt64 && !value.IsNull() &&
          value.value.int_value >= 1) {
        continue;
      }
      return spec;
    }
    if (term.expression->Type() != TypeTag::kColumnValue) {
      return spec;
    }
    const ColumnName& column = term.expression->AsColumnValue().GetColumnName();
    if (!column.schema.empty()) {
      // A qualified grouping column projected verbatim is reachable by its
      // output ordinal after fusion, even when its output alias differs.
      size_t position = 0;
      for (; position < statement.SelectList().size(); ++position) {
        const Expression& item = statement.SelectList()[position].expression;
        if (item && item->Type() == TypeTag::kColumnValue &&
            item->AsColumnValue().GetColumnName() == column) {
          break;
        }
      }
      if (position == statement.SelectList().size()) {
        return spec;
      }
      term.expression =
          ConstantValueExp(Value(static_cast<int64_t>(position + 1)));
      continue;
    }
    const bool matches_alias = std::ranges::any_of(
        statement.SelectList(), [&](const NamedExpression& item) {
          return !item.name.empty() && IdentifierEquals(item.name, column.name);
        });
    if (!matches_alias) {
      size_t position = 0;
      for (; position < statement.SelectList().size(); ++position) {
        const NamedExpression& item = statement.SelectList()[position];
        if (item.name.empty() && item.expression &&
            item.expression->Type() == TypeTag::kColumnValue &&
            IdentifierEquals(
                item.expression->AsColumnValue().GetColumnName().name,
                column.name)) {
          break;
        }
      }
      if (position == statement.SelectList().size()) {
        return spec;
      }
      term.expression =
          ConstantValueExp(Value(static_cast<int64_t>(position + 1)));
    }
  }
  spec.group_keys.assign(statement.GroupBy().begin(),
                         statement.GroupBy().end());
  spec.aggregates = std::move(aggregates);
  spec.count_star.reserve(spec.aggregates.size());
  for (const AggregateExpression* aggregate : spec.aggregates) {
    spec.count_star.push_back(IsCountStar(*aggregate));
  }
  spec.select_items.reserve(statement.SelectList().size());
  spec.select_names.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    spec.select_items.push_back(item.expression);
    spec.select_names.push_back(item.name);
  }
  if (!statement.Distinct() && statement.HasLimit() && !statement.WithTies() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    spec.topn_limit = statement.Offset() + statement.Limit();
  }
  spec.eligible = true;
  return spec;
}

}  // namespace tinylamb::relational_detail
