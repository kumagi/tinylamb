/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/join_reduction.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

bool RejectsNulls(const Expression& expression,
                  const std::unordered_set<std::string>& qualifiers);
bool NeverFalse(const Expression& expression,
                const std::unordered_set<std::string>& qualifiers);

// Lean proof: lean/TinylambOpt/CppRuleAudit.lean
//   (repairedIsStrict_sound, cpp_empty_in_isFalse_bug)
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
    case TypeTag::kInExp: {
      const InExpression& in = expression->AsInExpression();
      // `NULL IN ()` evaluates to FALSE in SQL, not NULL, so an empty IN list
      // is NOT NULL-strict (`(r.a IN ()) IS FALSE` is TRUE on padded rows).
      return !in.list_.empty() && IsStrictOnQualifiers(in.child_, qualifiers);
    }
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
// Lean proof: lean/TinylambOpt/BagRel.lean (outer_to_inner_join_sound) &
//   lean/TinylambOpt/CppRuleAudit.lean (repairedRejectsAndNeverFalse_sound,
//   cpp_empty_in_isFalse_bug, cpp_neverFalse_sub_and_bitNot_bug)
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
          // Both sides must independently reject NULLs: `a IS NULL OR b > 1`
          // is satisfied by a padded row whose `b` is a real NULL.
          return RejectsNulls(binary.Left(), qualifiers) &&
                 RejectsNulls(binary.Right(), qualifiers);
        case BinaryOperation::kXor:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          // Non-zero integer operands can combine to zero or non-zero (`0 - 1`
          // is truthy while `1 - 1` is falsy), so only a guaranteed NULL
          // operand makes the arithmetic/bitwise result provably NULL.
          return IsStrictOnQualifiers(binary.Left(), qualifiers) ||
                 IsStrictOnQualifiers(binary.Right(), qualifiers);
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
    case TypeTag::kConstantValue: {
      // NULL is the third truth value, not FALSE, so a NULL constant never
      // evaluates FALSE. Only an explicit FALSE constant does.
      const Value& v = expression->AsConstantValue().GetValue();
      return v.IsNull() || v.Truthy();
    }
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
        // NOLINTNEXTLINE(bugprone-branch-clone) distinct intent in each branch
        case BinaryOperation::kXor:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          // Non-zero operands can cancel to 0 (`1 - 1 = 0`, `1 ^ 1 = 0`,
          // `1 / 2 = 0`), which is falsy under `NOT(...)`; only a guaranteed
          // NULL operand keeps the result NULL (never FALSE).
          return IsStrictOnQualifiers(binary.Left(), qualifiers) ||
                 IsStrictOnQualifiers(binary.Right(), qualifiers);
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
          // `~(-1) == 0` is falsy even though `-1` is truthy; require strict
          // NULL propagation rather than truthiness preservation.
          return IsStrictOnQualifiers(unary.Child(), qualifiers);
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

bool QualifyExpressionWithCatalog(  // NOLINT(misc-no-recursion)
    Expression* expr,
    const std::vector<std::pair<std::string, const Schema*>>& relations) {
  if (expr == nullptr || !*expr) {
    return true;
  }
  if ((*expr)->Type() == TypeTag::kWindowFunctionExp ||
      (*expr)->Type() == TypeTag::kQueryExp) {
    return false;
  }
  if ((*expr)->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = (*expr)->AsColumnValue().GetColumnName();
    if (col.name == "*" || col.name.empty()) {
      return false;
    }
    if (!col.schema.empty()) {
      return true;
    }
    std::string matched_rel;
    for (const auto& [rel_name, schema] : relations) {
      if (schema != nullptr && schema->Offset(ColumnName("", col.name)) >= 0) {
        if (!matched_rel.empty() && matched_rel != rel_name) {
          return false;
        }
        matched_rel = rel_name;
      }
    }
    if (matched_rel.empty()) {
      return false;
    }
    *expr = ColumnValueExp(ColumnName(matched_rel, col.name));
    return true;
  }
  if ((*expr)->Type() == TypeTag::kAggregateExp) {
    const auto& agg = (*expr)->AsAggregateExpression();
    if (!agg.Child()) {
      return true;
    }
    if (agg.NeedsGroupContext() || agg.HasWhereFilter() ||
        !agg.InnerGroupBy().empty()) {
      return false;
    }
    Expression child = agg.Child();
    if (!QualifyExpressionWithCatalog(&child, relations)) {
      return false;
    }
    if (child.get() != agg.Child().get()) {
      auto copy = std::make_shared<AggregateExpression>(
          agg.GetType(), std::move(child), agg.Distinct());
      copy->SetIgnoreNulls(agg.IgnoreNulls());
      copy->SetArrayElementSqlType(agg.ArrayElementSqlType());
      *expr = std::move(copy);
    }
    return true;
  }
  const std::vector<Expression> children = ExpressionChildren(*expr);
  if (children.empty()) {
    return true;
  }
  bool changed = false;
  std::vector<Expression> updated;
  updated.reserve(children.size());
  for (Expression child : children) {
    const ExpressionBase* orig_ptr = child.get();
    if (!QualifyExpressionWithCatalog(&child, relations)) {
      return false;
    }
    if (child.get() != orig_ptr) {
      changed = true;
    }
    updated.push_back(std::move(child));
  }
  if (changed) {
    *expr = WithExpressionChildren(*expr, std::move(updated));
  }
  return true;
}

Expression SubstituteUnmatchedAggregates(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kCount) {
      return ConstantValueExp(Value(int64_t{0}));
    }
    return ColumnValueExp(ColumnName("__unmatched_having__", "val"));
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  std::vector<Expression> mapped;
  mapped.reserve(children.size());
  for (const Expression& child : children) {
    mapped.push_back(SubstituteUnmatchedAggregates(child));
  }
  Expression rebuilt = WithExpressionChildren(expr, std::move(mapped));
  if (rebuilt && rebuilt->Type() == TypeTag::kBinaryExp) {
    const auto& bin = rebuilt->AsBinaryExpression();
    if (bin.Left() && bin.Right() &&
        bin.Left()->Type() == TypeTag::kConstantValue &&
        bin.Right()->Type() == TypeTag::kConstantValue) {
      const Value& lv = bin.Left()->AsConstantValue().GetValue();
      const Value& rv = bin.Right()->AsConstantValue().GetValue();
      if (!lv.IsNull() && !rv.IsNull() && lv.type == ValueType::kInt64 &&
          rv.type == ValueType::kInt64) {
        const int64_t l = lv.value.int_value;
        const int64_t r = rv.value.int_value;
        switch (bin.Op()) {
          case BinaryOperation::kEquals:
            return ConstantValueExp(Value(l == r));
          case BinaryOperation::kNotEquals:
            return ConstantValueExp(Value(l != r));
          case BinaryOperation::kLessThan:
            return ConstantValueExp(Value(l < r));
          case BinaryOperation::kLessThanEquals:
            return ConstantValueExp(Value(l <= r));
          case BinaryOperation::kGreaterThan:
            return ConstantValueExp(Value(l > r));
          case BinaryOperation::kGreaterThanEquals:
            return ConstantValueExp(Value(l >= r));
          default:
            break;
        }
      }
    }
  }
  return rebuilt;
}

// Lean proof: lean/TinylambOpt/AvgMinMaxHavingDistinct.lean
//   (having_null_rejecting_demotes_left_join_sound,
//    having_demote_breaks_count_star_on_coarse_group)
// Checks whether HAVING rejects groups whose padded-side (`qualifiers`) rows
// are all unmatched while ensuring unmatched rows contribute nothing to mixed
// groups (no COUNT(*), all aggregates are non-context
// SUM/AVG/MIN/MAX/COUNT(expr) strict on qualifiers, and GROUP BY / bare columns
// never touch qualifiers).
bool HavingRejectsNullsOnPaddedSide(
    const SelectStatement& statement,
    const std::vector<std::pair<std::string, const Schema*>>* catalog_rels,
    const std::unordered_set<std::string>& qualifiers) {
  if (!statement.Having() || qualifiers.empty() ||
      ContainsQueryNode(statement.Having()) || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Qualify() ||
      statement.HasDistinctOn()) {
    return false;
  }

  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& item : statement.SelectList()) {
    if (!item.name.empty() && item.expression) {
      aliases.emplace(item.name, item.expression);
    }
  }
  const std::function<Expression(const Expression&)> substitute_aliases =
      [&](const Expression& expr) -> Expression {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.schema.empty()) {
        const auto found = aliases.find(col.name);
        if (found != aliases.end()) {
          return found->second;
        }
      }
      return expr;
    }
    if (expr->Type() == TypeTag::kAggregateExp ||
        expr->Type() == TypeTag::kWindowFunctionExp ||
        expr->Type() == TypeTag::kQueryExp) {
      return expr;
    }
    const std::vector<Expression> children = ExpressionChildren(expr);
    if (children.empty()) {
      return expr;
    }
    std::vector<Expression> mapped;
    mapped.reserve(children.size());
    for (const Expression& child : children) {
      mapped.push_back(substitute_aliases(child));
    }
    return WithExpressionChildren(expr, std::move(mapped));
  };

  auto normalize = [&](Expression expr, bool apply_aliases) -> Expression {
    if (!expr) {
      return expr;
    }
    if (apply_aliases) {
      expr = substitute_aliases(expr);
    }
    if (catalog_rels != nullptr && !catalog_rels->empty()) {
      if (!QualifyExpressionWithCatalog(&expr, *catalog_rels)) {
        return {};
      }
    }
    return expr;
  };

  bool ok = true;
  std::vector<const AggregateExpression*> aggs;
  const std::function<void(const Expression&)> inspect_expr =
      [&](const Expression& expr) {
        if (!expr || !ok) {
          return;
        }
        if (expr->Type() == TypeTag::kWindowFunctionExp ||
            expr->Type() == TypeTag::kQueryExp) {
          ok = false;
          return;
        }
        if (expr->Type() == TypeTag::kAggregateExp) {
          aggs.push_back(&expr->AsAggregateExpression());
          return;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& col = expr->AsColumnValue().GetColumnName();
          if (col.schema.empty() || col.name == "*" ||
              qualifiers.contains(col.schema)) {
            ok = false;
          }
          return;
        }
        for (const Expression& child : ExpressionChildren(expr)) {
          inspect_expr(child);
        }
      };

  std::vector<Expression> normalized_selects;
  normalized_selects.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression norm = normalize(item.expression, false);
    if (!norm) {
      return false;
    }
    inspect_expr(norm);
    normalized_selects.push_back(std::move(norm));
  }
  std::vector<Expression> normalized_groups;
  normalized_groups.reserve(statement.GroupBy().size());
  if (!statement.IsGroupAll()) {
    for (const Expression& gk : statement.GroupBy()) {
      Expression norm = normalize(gk, true);
      if (!norm) {
        return false;
      }
      inspect_expr(norm);
      normalized_groups.push_back(std::move(norm));
    }
  }
  Expression normalized_having = normalize(statement.Having(), true);
  if (!normalized_having) {
    return false;
  }
  inspect_expr(normalized_having);

  std::vector<Expression> normalized_orders;
  normalized_orders.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression norm = normalize(term.expression, true);
    if (!norm) {
      return false;
    }
    inspect_expr(norm);
    normalized_orders.push_back(std::move(norm));
  }
  if (!ok || aggs.empty()) {
    return false;
  }

  for (const AggregateExpression* agg : aggs) {
    if (agg->NeedsGroupContext() || agg->HasWhereFilter() ||
        !agg->InnerGroupBy().empty() || !agg->Child()) {
      return false;
    }
    switch (agg->GetType()) {
      case AggregationType::kSum:
      case AggregationType::kAvg:
      case AggregationType::kMin:
      case AggregationType::kMax:
      case AggregationType::kCount:
        break;
      default:
        return false;
    }
    if (!ExpressionCannotThrowForPlanner(agg->Child()) ||
        !IsStrictOnQualifiers(agg->Child(), qualifiers)) {
      return false;
    }
  }

  const std::unordered_set<std::string> synth_qual{"__unmatched_having__"};
  // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer here
  for (const Expression& conj : SplitConjuncts(normalized_having)) {
    const Expression probe = SubstituteUnmatchedAggregates(conj);
    if (!probe) {
      continue;
    }
    if (probe->Type() == TypeTag::kConstantValue) {
      const Value& v = probe->AsConstantValue().GetValue();
      if (v.IsNull() || !v.Truthy()) {
        return true;
      }
      continue;
    }
    if (RejectsNulls(probe, synth_qual)) {
      return true;
    }
  }
  return false;
}

}  // namespace

bool ReduceOuterJoins(SelectStatement* statement, TransactionContext* ctx) {
  if (statement == nullptr) {
    return false;
  }
  const bool has_usable_where =
      statement->WhereClause() && !ContainsQueryNode(statement->WhereClause());
  const bool has_usable_having =
      statement->Having() && !ContainsQueryNode(statement->Having());
  if (!has_usable_where && !has_usable_having) {
    return false;
  }
  std::vector<SelectSource> sources = statement->Sources();
  if (sources.empty()) {
    return false;
  }

  const auto source_name = [&](const SelectSource& source) {
    return source.alias.empty() ? source.table : source.alias;
  };
  std::vector<std::shared_ptr<Table>> table_holders;
  table_holders.reserve(sources.size());
  std::vector<std::pair<std::string, const Schema*>> catalog_rels;
  catalog_rels.reserve(sources.size());
  bool catalog_complete = ctx != nullptr;
  if (ctx != nullptr) {
    for (const SelectSource& src : sources) {
      if (src.table.empty() || src.query != nullptr || src.unnest != nullptr ||
          src.from_nested_join) {
        catalog_complete = false;
        break;
      }
      StatusOr<std::shared_ptr<Table>> tbl = ctx->GetTable(src.table);
      if (!tbl.HasValue()) {
        catalog_complete = false;
        break;
      }
      table_holders.push_back(tbl.MoveValue());
      catalog_rels.emplace_back(source_name(src),
                                &table_holders.back()->GetSchema());
    }
  }
  const auto* rels_ptr = catalog_complete ? &catalog_rels : nullptr;
  Expression qualified_where = statement->WhereClause();
  if (has_usable_where && rels_ptr != nullptr) {
    Expression candidate = qualified_where;
    if (QualifyExpressionWithCatalog(&candidate, *rels_ptr)) {
      qualified_where = std::move(candidate);
    }
  }

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
    prefix.reserve(i);
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
    const auto side_rejected =
        [&](const std::unordered_set<std::string>& quals) {
          if (has_usable_where && RejectsNulls(qualified_where, quals)) {
            return true;
          }
          if (has_usable_having &&
              HavingRejectsNullsOnPaddedSide(*statement, rels_ptr, quals)) {
            return true;
          }
          return false;
        };
    bool rejects = false;
    if (source.join_type == JoinType::kLeft) {
      rejects = side_rejected(right);
    } else if (source.join_type == JoinType::kRight) {
      rejects = side_rejected(prefix);
    } else {
      rejects = side_rejected(prefix) && side_rejected(right);
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
