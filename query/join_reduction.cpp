/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/join_reduction.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool RejectsNulls(const Expression& expression,
                  const std::unordered_set<std::string>& qualifiers);
bool NeverFalse(const Expression& expression,
                const std::unordered_set<std::string>& qualifiers);

bool IsStrictOnQualifiers(const Expression& expression,
                          const std::unordered_set<std::string>& qualifiers) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      return !column.schema.empty() && qualifiers.contains(column.schema);
    }
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
          return IsStrictOnQualifiers(binary.Left(), qualifiers) ||
                 IsStrictOnQualifiers(binary.Right(), qualifiers);
        default:
          return false;
      }
    }
    case TypeTag::kCastExp:
      return IsStrictOnQualifiers(expression->AsCastExpression().Child(),
                                  qualifiers);
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot ||
          unary.Op() == UnaryOperation::kMinus ||
          unary.Op() == UnaryOperation::kBitwiseNot) {
        return IsStrictOnQualifiers(unary.Child(), qualifiers);
      }
      return false;
    }
    case TypeTag::kInExp:
      return IsStrictOnQualifiers(expression->AsInExpression().child_,
                                  qualifiers);
    case TypeTag::kFunctionCallExp: {
      const auto& function = expression->AsFunctionCallExpression();
      const std::string& name = function.FuncName();
      if (name != "abs" && name != "lower" && name != "upper" &&
          name != "length" && name != "trim") {
        return false;
      }
      return std::ranges::any_of(function.Args(), [&](const Expression& arg) {
        return IsStrictOnQualifiers(arg, qualifiers);
      });
    }
    default:
      return false;
  }
}

// True when evaluating `expression` with every column qualified by
// `qualifier` bound to NULL can only yield FALSE or NULL, never TRUE.
// NULL-padded rows of an outer join therefore never survive a WHERE conjunct
// with this property, which makes the outer join reducible to an inner join.
bool RejectsNulls(const Expression& expression,
                  const std::unordered_set<std::string>& qualifiers) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      return !column.schema.empty() && qualifiers.contains(column.schema);
    }
    case TypeTag::kConstantValue:
      // Constants do not depend on the padded columns, so a constant TRUE
      // keeps the padded rows alive.
      return false;
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAnd:
          // FALSE/NULL on either side rejects the row.
          return RejectsNulls(binary.Left(), qualifiers) ||
                 RejectsNulls(binary.Right(), qualifiers);
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          // Both sides must independently reject NULLs: `a IS NULL OR b > 1`
          // is satisfied by a padded row whose `b` is a real NULL, XOR of
          // NULL is NULL, and NULL propagates through arithmetic only when
          // every operand rejects; otherwise a non-NULL operand may keep the
          // result TRUE.
          return RejectsNulls(binary.Left(), qualifiers) &&
                 RejectsNulls(binary.Right(), qualifiers);
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
          // Any NULL operand makes the comparison NULL: one rejecting side
          // is enough.  Null-safe comparisons are two-valued and therefore
          // cannot prove that a NULL-padded outer row is rejected.
          if (binary.Op() == BinaryOperation::kIsDistinctFrom ||
              binary.Op() == BinaryOperation::kIsNotDistinctFrom) {
            return false;
          }
          return IsStrictOnQualifiers(binary.Left(), qualifiers) ||
                 IsStrictOnQualifiers(binary.Right(), qualifiers);
      }
      return false;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kNot:
          // NOT flips the property: NOT(e) is FALSE or NULL exactly when e
          // is TRUE or NULL ("never false").  RejectsNulls(child) alone is
          // unsound: it also allows FALSE, and NOT FALSE = TRUE keeps a
          // padded row alive (`NOT (padded.a > 5 AND other.x > 100)`).
          return NeverFalse(unary.Child(), qualifiers);
        case UnaryOperation::kMinus:
        case UnaryOperation::kBitwiseNot:
          return IsStrictOnQualifiers(unary.Child(), qualifiers);
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsNotFalse:
          return false;
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsFalse:
          return IsStrictOnQualifiers(unary.Child(), qualifiers);
      }
      return false;
    }
    case TypeTag::kInExp: {
      const InExpression& in = expression->AsInExpression();
      if (!in.list_.empty()) {
        // `padded_col IN (1, 2)` is NULL for padded rows when the operand
        // rejects; the value list cannot resurrect a NULL operand.
        return IsStrictOnQualifiers(in.child_, qualifiers);
      }
      return false;
    }
    default:
      // CASE, function calls, subqueries: no structural proof, stay
      // conservative and keep the outer join.
      return false;
  }
}

// True when evaluating `expression` with every column qualified by
// `qualifier` bound to NULL can only yield TRUE or NULL, never FALSE.  This
// is the dual property `NOT(...)` needs: NOT(e) is FALSE or NULL exactly
// when e never evaluates FALSE on a padded row.
bool NeverFalse(const Expression& expression,
                const std::unordered_set<std::string>& qualifiers) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      // A padded column evaluates to NULL in filter (boolean) context.
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      return !column.schema.empty() && qualifiers.contains(column.schema);
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue().Truthy();
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAnd:
          // FALSE survives AND unless every operand is TRUE/NULL.
          return NeverFalse(binary.Left(), qualifiers) &&
                 NeverFalse(binary.Right(), qualifiers);
        case BinaryOperation::kOr:
          // FALSE OR NULL is NULL: one never-false side is enough.
          return NeverFalse(binary.Left(), qualifiers) ||
                 NeverFalse(binary.Right(), qualifiers);
        case BinaryOperation::kXor:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          // NULL on any operand keeps the result NULL; FALSE stays FALSE,
          // so every operand must be TRUE/NULL.
          return NeverFalse(binary.Left(), qualifiers) &&
                 NeverFalse(binary.Right(), qualifiers);
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
          // Any NULL operand makes the comparison NULL: the rejecting-side
          // proof carries over verbatim.
          return IsStrictOnQualifiers(binary.Left(), qualifiers) ||
                 IsStrictOnQualifiers(binary.Right(), qualifiers);
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
          // Two-valued: a padded NULL side yields a concrete FALSE too.
          return false;
      }
      return false;
    }
    case TypeTag::kUnaryExp: {
      const UnaryExpression& unary = expression->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kNot:
          // NOT(e) is never false exactly when e never returns TRUE, which
          // is precisely RejectsNulls(e).
          return RejectsNulls(unary.Child(), qualifiers);
        case UnaryOperation::kMinus:
        case UnaryOperation::kBitwiseNot:
          return NeverFalse(unary.Child(), qualifiers);
        default:
          // IS-family predicates return concrete FALSE for many padded-row
          // shapes (`padded.a IS NOT NULL`); no structural proof.
          return false;
      }
    }
    default:
      return false;
  }
}

bool ContainsQueryNode(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expression), ContainsQueryNode);
}

}  // namespace

bool ReduceOuterJoins(SelectStatement* statement) {
  if (statement == nullptr || !statement->WhereClause() ||
      ContainsQueryNode(statement->WhereClause())) {
    return false;
  }
  std::vector<SelectSource> sources = statement->Sources();
  if (sources.empty()) {
    return false;
  }

  const auto source_name = [&](const SelectSource& source) {
    return source.alias.empty() ? source.table : source.alias;
  };
  bool changed = false;
  for (size_t i = 1; i < sources.size(); ++i) {
    SelectSource& source = sources[i];
    if (source.join_type != JoinType::kLeft &&
        source.join_type != JoinType::kRight &&
        source.join_type != JoinType::kFull) {
      continue;
    }
    if (source.query != nullptr || source.unnest != nullptr ||
        source.from_nested_join) {
      continue;
    }
    std::unordered_set<std::string> prefix;
    for (size_t j = 0; j < i; ++j) {
      const std::string name = source_name(sources[j]);
      if (!name.empty()) {
        prefix.insert(name);
      }
    }
    std::unordered_set<std::string> right;
    const std::string right_name = source_name(source);
    if (!right_name.empty()) {
      right.insert(right_name);
    }
    bool rejects = false;
    if (source.join_type == JoinType::kLeft) {
      rejects = RejectsNulls(statement->WhereClause(), right);
    } else if (source.join_type == JoinType::kRight) {
      rejects = RejectsNulls(statement->WhereClause(), prefix);
    } else {
      rejects = RejectsNulls(statement->WhereClause(), prefix) &&
                RejectsNulls(statement->WhereClause(), right);
    }
    if (rejects) {
      source.join_type = JoinType::kInner;
      changed = true;
    }
  }
  if (changed) {
    statement->SetSources(std::move(sources));
  }
  return changed;
}

}  // namespace tinylamb
