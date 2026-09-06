/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/join_reduction.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/binary_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool RejectsNulls(const Expression& expression, const std::string& qualifier);
bool NeverFalse(const Expression& expression, const std::string& qualifier);

// True when evaluating `expression` with every column qualified by
// `qualifier` bound to NULL can only yield FALSE or NULL, never TRUE.
// NULL-padded rows of a LEFT JOIN therefore never survive a WHERE conjunct
// with this property, which makes the outer join reducible to an inner
// join.
bool RejectsNulls(const Expression& expression, const std::string& qualifier) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      return !column.schema.empty() && column.schema == qualifier;
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
          return RejectsNulls(binary.Left(), qualifier) ||
                 RejectsNulls(binary.Right(), qualifier);
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
          return RejectsNulls(binary.Left(), qualifier) &&
                 RejectsNulls(binary.Right(), qualifier);
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
          return RejectsNulls(binary.Left(), qualifier) ||
                 RejectsNulls(binary.Right(), qualifier);
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
          return NeverFalse(unary.Child(), qualifier);
        case UnaryOperation::kMinus:
          // Arithmetic negation keeps NULL; a non-NULL child may stay TRUE
          // (e.g. IS TRUE wrappers), so require the child to reject.
          return RejectsNulls(unary.Child(), qualifier);
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
          // IS-family predicates never return NULL: `o.amount IS NULL` is
          // exactly the padded-row pattern and must keep the outer join.
          return false;
      }
      return false;
    }
    case TypeTag::kInExp: {
      const InExpression& in = expression->AsInExpression();
      if (!in.list_.empty()) {
        // `padded_col IN (1, 2)` is NULL for padded rows when the operand
        // rejects; the value list cannot resurrect a NULL operand.
        return RejectsNulls(in.child_, qualifier);
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
bool NeverFalse(const Expression& expression, const std::string& qualifier) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      // A padded column evaluates to NULL in filter (boolean) context.
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      return !column.schema.empty() && column.schema == qualifier;
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue().Truthy();
    case TypeTag::kBinaryExp: {
      const BinaryExpression& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAnd:
          // FALSE survives AND unless every operand is TRUE/NULL.
          return NeverFalse(binary.Left(), qualifier) &&
                 NeverFalse(binary.Right(), qualifier);
        case BinaryOperation::kOr:
          // FALSE OR NULL is NULL: one never-false side is enough.
          return NeverFalse(binary.Left(), qualifier) ||
                 NeverFalse(binary.Right(), qualifier);
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
          return NeverFalse(binary.Left(), qualifier) &&
                 NeverFalse(binary.Right(), qualifier);
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
          return RejectsNulls(binary.Left(), qualifier) ||
                 RejectsNulls(binary.Right(), qualifier);
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
          return RejectsNulls(unary.Child(), qualifier);
        case UnaryOperation::kMinus:
          return NeverFalse(unary.Child(), qualifier);
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

// Column identifiers (bare names) the conjunct references on the given
// source, using the same bare-name resolution the engine applies.
bool ConjunctTouches(const Expression& conjunct, const std::string& qualifier) {
  if (!conjunct) {
    return false;
  }
  return std::ranges::any_of(
      conjunct->TouchedColumns(), [&](const ColumnName& column) {
        return !column.schema.empty() && column.schema == qualifier;
      });
}

}  // namespace

bool ReduceOuterJoins(SelectStatement* statement) {
  if (statement == nullptr || !statement->WhereClause()) {
    return false;
  }
  std::vector<SelectSource> sources = statement->Sources();
  if (sources.empty()) {
    return false;
  }

  // Qualifiers visible at each depth: sources [0, i] feed the join whose
  // right side is source i.
  bool changed = false;
  for (size_t i = 1; i < sources.size(); ++i) {
    SelectSource& source = sources[i];
    if (source.join_type != JoinType::kLeft || source.query != nullptr) {
      continue;
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    bool all_reject = true;
    bool touches = false;
    for (const Expression& conjunct :
         SplitConjuncts(statement->WhereClause())) {
      if (!ConjunctTouches(conjunct, qualifier)) {
        continue;
      }
      touches = true;
      if (!RejectsNulls(conjunct, qualifier)) {
        all_reject = false;
        break;
      }
    }
    if (touches && all_reject) {
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
