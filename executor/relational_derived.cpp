/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/relational.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {
bool IsIdentityDerived(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].query ||
      statement.Sources()[0].unnest || statement.Sources()[0].table.empty() ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.OrderBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty()) {
    return false;
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& column =
            item.expression->AsColumnValue().GetColumnName();
        return column.name != "*" &&
               (item.name.empty() || item.name == column.name);
      });
}

bool IsDeterministicProjectionExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [](const Expression& child) {
        return IsDeterministicProjectionExpression(child);
      });
}

bool IsFlattenableProjection(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || statement.Sources()[0].unnest ||
      (!statement.Sources()[0].query && statement.Sources()[0].table.empty()) ||
      statement.WhereClause() || statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || !statement.OrderBy().empty() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement)) {
    return false;
  }
  // Projection flattening is only valid for expressions that do not create a
  // second query scope or change row cardinality.
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

Expression SubstituteProjectionColumns(  // NOLINT(misc-no-recursion)
    const Expression& expression, const SelectStatement& projection,
    const std::string& projection_alias, bool* valid) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    *valid = false;
    return nullptr;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (!column.schema.empty() &&
        !IdentifierEquals(column.schema, projection_alias)) {
      *valid = false;
      return nullptr;
    }
    for (const NamedExpression& item : projection.SelectList()) {
      if (!item.expression) {
        continue;
      }
      std::string output_name;
      if (item.name.empty() &&
          item.expression->Type() == TypeTag::kColumnValue) {
        output_name = item.expression->AsColumnValue().GetColumnName().name;
      } else {
        output_name = item.name;
      }
      if (IdentifierEquals(output_name, column.name)) {
        return item.expression;
      }
    }
    *valid = false;
    return nullptr;
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  if (children.empty()) {
    return expression;
  }
  std::vector<Expression> rewritten;
  rewritten.reserve(children.size());
  if (expression->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression& agg = expression->AsAggregateExpression();
    if (IsCountStar(agg) && !agg.HasWhereFilter() &&
        agg.Having() == AggregateHavingModifier::kNone &&
        agg.InnerOrderBy().empty() && !agg.SecondaryArg() &&
        agg.TrailingArgs().empty()) {
      return expression;
    }
  }
  for (const Expression& child : children) {
    rewritten.push_back(SubstituteProjectionColumns(child, projection,
                                                    projection_alias, valid));
    if (!*valid) {
      return nullptr;
    }
  }
  return WithExpressionChildren(expression, std::move(rewritten));
}

std::shared_ptr<SelectStatement> FlattenProjectionBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (!IsFlattenableProjection(derived) || (statement.Sources().size() != 1) ||
      statement.Having() || statement.Qualify() ||
      !statement.GroupBy().empty() || !statement.UnionAll().empty() ||
      statement.Distinct() || HasWindowFunctions(statement)) {
    return nullptr;
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  Expression where =
      statement.WhereClause() ? rewrite(statement.WhereClause()) : Expression{};
  if (statement.WhereClause() && !where) {
    return nullptr;
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    Expression expression = rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetWhereClause(std::move(where));
  rewritten->SetOrderBy(std::move(order_by));
  SelectSource merged = derived.Sources()[0];
  if (source.sample && merged.sample) {
    return nullptr;
  }
  if (source.sample) {
    merged.sample = source.sample;
  }
  merged.alias = source.alias.empty() ? merged.alias : source.alias;
  merged.join_type = source.join_type;
  merged.join_condition = source.join_condition;
  rewritten->SetSources({std::move(merged)});
  return rewritten;
}

// Rebind a predicate written against a derived-table output to the base
// relation below it. Only the expression forms reconstructed here are used by
// the pushdown rule; a nullptr means that the predicate has a shape we cannot
// prove safe to move.
Expression RebindDerivedPredicate(const Expression& expression,
                                  const SelectStatement& derived,
                                  const std::string& derived_alias) {
  if (!expression) {
    return nullptr;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& column = expression->AsColumnValue().GetColumnName();
      if (!column.schema.empty() && column.schema != derived_alias) {
        return expression;
      }
      for (const NamedExpression& item : derived.SelectList()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& projected =
            item.expression->AsColumnValue().GetColumnName();
        const std::string output_name =
            item.name.empty() ? projected.name : item.name;
        if (output_name == column.name) {
          // This expression is evaluated inside the original derived scope.
          // Preserve its owner: with joins it need not name the first source.
          return item.expression;
        }
      }
      return nullptr;
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      Expression left =
          RebindDerivedPredicate(binary.Left(), derived, derived_alias);
      Expression right =
          RebindDerivedPredicate(binary.Right(), derived, derived_alias);
      return left && right ? BinaryExpressionExp(std::move(left), binary.Op(),
                                                 std::move(right))
                           : nullptr;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      Expression child =
          RebindDerivedPredicate(unary.Child(), derived, derived_alias);
      return child ? UnaryExpressionExp(std::move(child), unary.Op()) : nullptr;
    }
    default:
      // Constants are safe and leave their identity unchanged.  Query,
      // aggregate, and function nodes are retained only when no column
      // rebinding is needed; the caller still rejects predicates with query
      // expressions before invoking this helper.
      return expression->TouchedColumns().empty() ? expression : nullptr;
  }
}

bool PredicateCanEnterDerived(const Expression& predicate,
                              const SelectStatement& derived,
                              const std::string& derived_alias) {
  if (!predicate || ContainsQuery(predicate) ||
      !SafeToReduceEvaluationCount(predicate) || derived.Sources().empty() ||
      std::ranges::any_of(derived.Sources(),
                          [](const auto& source) {
                            return source.query || source.unnest ||
                                   source.table.empty() || source.is_lateral ||
                                   !source.using_columns.empty();
                          }) ||
      derived.Having() || derived.Qualify() || !derived.GroupBy().empty() ||
      !derived.OrderBy().empty() || derived.Distinct() || derived.HasLimit() ||
      derived.Offset() != 0 || derived.SelectList().empty() ||
      HasWindowFunctions(derived) ||
      StatementContainsVolatileFunction(derived)) {
    return false;
  }
  for (const ColumnName& column : predicate->TouchedColumns()) {
    size_t matches = 0;
    bool direct = false;
    for (size_t i = 0; i < derived.SelectList().size(); ++i) {
      const NamedExpression& item = derived.SelectList()[i];
      if (!item.expression) {
        return false;
      }
      if (item.expression->Type() == TypeTag::kColumnValue &&
          item.expression->AsColumnValue().GetColumnName().name == "*") {
        return false;
      }
      if (ProjectionName(item, i) == column.name) {
        ++matches;
      }
      if (item.expression->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& projected =
          item.expression->AsColumnValue().GetColumnName();
      const std::string output_name =
          item.name.empty() ? projected.name : item.name;
      if (output_name == column.name &&
          (column.schema.empty() || column.schema == derived_alias)) {
        direct = true;
      }
    }
    if (matches != 1 || !direct) {
      return false;
    }
  }
  // Do not classify an expression as pushable unless the rebinder can
  // actually preserve its shape.  In particular, function calls and other
  // expression kinds must stay above the derived-table boundary rather than
  // being silently removed from the outer predicate.
  return !predicate->TouchedColumns().empty() &&
         RebindDerivedPredicate(predicate, derived, derived_alias) != nullptr;
}

// Multi-source variant of IsFlattenableProjection: the derived side is a
// plain inner-join projection.  Unlike the single-source boundary above, its
// WHERE clause is allowed -- it composes into the merged statement's WHERE,
// which is equivalent because the merged sources are exactly the derived's
// own sources and every join inside them is inner.
bool IsFlattenableJoinedProjection(const SelectStatement& statement) {
  if (statement.Sources().empty() || statement.Having() ||
      statement.Qualify() || !statement.GroupBy().empty() ||
      !statement.UnionAll().empty() || statement.Distinct() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      statement.SelectList().empty() || HasWindowFunctions(statement) ||
      !statement.WithQueries().empty()) {
    return false;
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.unnest || (source.query && source.table.empty()) ||
        (source.join_type != JoinType::kInner &&
         source.join_type != JoinType::kCross) ||
        source.sample) {
      return false;
    }
  }
  return std::ranges::all_of(
      statement.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               (item.expression->Type() != TypeTag::kColumnValue ||
                item.expression->AsColumnValue().GetColumnName().name != "*") &&
               !ContainsQuery(item.expression) &&
               !ContainsAggregate(item.expression) &&
               IsDeterministicProjectionExpression(item.expression);
      });
}

// Flattens a grouping boundary: a statement that GROUPs over a single derived
// table absorbs the derived projection when the derived side is itself a
// plain multi-source inner-join projection.  Group keys, select items and
// ORDER BY terms re-resolve against the derived select list (aggregates
// rebuild losslessly), so the grouping runs directly over the joined base
// relations and the grouped-join fusion can fire at the join step instead of
// materializing the multi-million-row join relation.
std::shared_ptr<SelectStatement> FlattenGroupedDerivedBoundary(
    const SelectStatement& statement, const SelectSource& source,
    const SelectStatement& derived) {
  if (statement.Sources().size() != 1 || statement.Having() ||
      statement.Qualify() || !statement.UnionAll().empty() ||
      HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement) ||
      !IsFlattenableJoinedProjection(derived)) {
    return nullptr;
  }
  if (source.join_type != JoinType::kInner &&
      source.join_type != JoinType::kCross) {
    return nullptr;
  }
  if (source.sample || ContainsQuery(derived.WhereClause())) {
    return nullptr;
  }
  // Duplicate derived output names make the substitution ambiguous.
  for (auto a = derived.SelectList().begin(); a != derived.SelectList().end();
       ++a) {
    const std::string a_name =
        !a->name.empty()
            ? a->name
            : (a->expression->Type() == TypeTag::kColumnValue
                   ? a->expression->AsColumnValue().GetColumnName().name
                   : std::string());
    if (a_name.empty()) {
      continue;
    }
    for (auto b = std::next(a); b != derived.SelectList().end(); ++b) {
      const std::string b_name =
          !b->name.empty()
              ? b->name
              : (b->expression->Type() == TypeTag::kColumnValue
                     ? b->expression->AsColumnValue().GetColumnName().name
                     : std::string());
      if (!b_name.empty() && IdentifierEquals(a_name, b_name)) {
        return nullptr;
      }
    }
  }
  const std::string derived_alias = source.alias.empty() ? "d" : source.alias;
  auto rewrite = [&](const Expression& expression) -> Expression {
    bool valid = true;
    Expression result =
        SubstituteProjectionColumns(expression, derived, derived_alias, &valid);
    return valid ? result : Expression{};
  };
  std::vector<NamedExpression> select_list;
  select_list.reserve(statement.SelectList().size());
  for (const NamedExpression& item : statement.SelectList()) {
    Expression expression = rewrite(item.expression);
    if (!expression) {
      return nullptr;
    }
    select_list.emplace_back(item.name, std::move(expression));
  }
  std::vector<Expression> group_by;
  group_by.reserve(statement.GroupBy().size());
  for (const Expression& key : statement.GroupBy()) {
    Expression expression = rewrite(key);
    if (!expression) {
      return nullptr;
    }
    group_by.push_back(std::move(expression));
  }
  std::vector<SelectStatement::OrderByTerm> order_by;
  order_by.reserve(statement.OrderBy().size());
  for (const auto& term : statement.OrderBy()) {
    // A term that names a select alias stays a plain column reference: the
    // fused path orders the projected output through select aliases, and a
    // substituted expression there would defeat that resolution.
    const bool alias_ref =
        term.expression && term.expression->Type() == TypeTag::kColumnValue &&
        term.expression->AsColumnValue().GetColumnName().schema.empty() &&
        std::any_of(
            statement.SelectList().begin(), statement.SelectList().end(),
            [&](const NamedExpression& item) {
              return !item.name.empty() &&
                     IdentifierEquals(
                         item.name,
                         term.expression->AsColumnValue().GetColumnName().name);
            });
    Expression expression =
        alias_ref ? term.expression : rewrite(term.expression);
    if (!expression) {
      return nullptr;
    }
    order_by.push_back(
        SelectStatement::OrderByTerm{.expression = std::move(expression),
                                     .ascending = term.ascending,
                                     .nulls_first = term.nulls_first});
  }
  // The derived's own conjuncts move up unchanged (same base sources); the
  // parent's conjuncts are re-bound to the derived outputs first.
  std::vector<Expression> where = SplitConjuncts(derived.WhereClause());
  if (statement.WhereClause()) {
    Expression rebound = rewrite(statement.WhereClause());
    if (!rebound) {
      return nullptr;
    }
    for (Expression& conjunct : SplitConjuncts(rebound)) {
      where.push_back(std::move(conjunct));
    }
  }

  auto rewritten = std::make_shared<SelectStatement>(statement);
  rewritten->SetSelectList(std::move(select_list));
  rewritten->SetGroupBy(std::move(group_by));
  rewritten->SetWhereClause(where.empty() ? Expression{}
                                          : CombineConjuncts(where));
  rewritten->SetOrderBy(std::move(order_by));
  rewritten->SetSources(derived.Sources());
  return rewritten;
}

struct StaticExprType {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool is_interval{false};
  bool operator==(const StaticExprType&) const = default;
};

std::optional<size_t> FindStaticColumn(const Schema& schema,
                                       const ColumnName& name) {
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!IdentifierEquals(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() &&
        !IdentifierEquals(candidate.schema, name.schema)) {
      continue;
    }
    if (match.has_value()) {
      return std::nullopt;
    }
    match = i;
  }
  return match;
}

std::optional<StaticExprType> InferStaticExprType(const Expression& node,
                                                  const Schema& schema) {
  if (!node) {
    return std::nullopt;
  }
  switch (node->Type()) {
    case TypeTag::kConstantValue: {
      const Value& val = node->AsConstantValue().GetValue();
      if (val.type == ValueType::kNull) {
        return std::nullopt;
      }
      return StaticExprType{.type = val.type,
                            .is_unsigned = val.IsUnsigned(),
                            .is_interval = val.IsInterval()};
    }
    case TypeTag::kColumnValue: {
      const auto slot =
          FindStaticColumn(schema, node->AsColumnValue().GetColumnName());
      if (!slot.has_value()) {
        return std::nullopt;
      }
      const Column& col = schema.GetColumn(*slot);
      if (col.Type() == ValueType::kNull) {
        return std::nullopt;
      }
      return StaticExprType{.type = col.Type(),
                            .is_unsigned = col.IsUnsigned(),
                            .is_interval = col.IsInterval()};
    }
    case TypeTag::kCastExp: {
      const auto& cast = node->AsCastExpression();
      const ValueType vt = cast.TargetValueType();
      if (vt == ValueType::kNull || vt == ValueType::kArray) {
        return std::nullopt;
      }
      // Avoid two heap allocations per CAST node during planning: compare the
      // resolved type-name prefix case-insensitively in place.
      const std::string_view type_name = cast.TargetTypeName();
      const auto starts_ci = [type_name](std::string_view prefix) {
        return type_name.size() >= prefix.size() &&
               std::equal(
                   prefix.begin(), prefix.end(), type_name.begin(),
                   [](char a, char b) {
                     return std::toupper(static_cast<unsigned char>(a)) ==
                            std::toupper(static_cast<unsigned char>(b));
                   });
      };
      const bool is_unsigned = starts_ci("UINT");
      const bool is_interval = starts_ci("INTERVAL");
      return StaticExprType{
          .type = vt, .is_unsigned = is_unsigned, .is_interval = is_interval};
    }
    case TypeTag::kUnaryExp: {
      const auto& un = node->AsUnaryExpression();
      const auto ch = InferStaticExprType(un.Child(), schema);
      if (!ch.has_value()) {
        return std::nullopt;
      }
      switch (un.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
        case UnaryOperation::kNot:
          return StaticExprType{.type = ValueType::kInt64,
                                .is_unsigned = false,
                                .is_interval = false};
        case UnaryOperation::kMinus:
          if (ch->is_unsigned || ch->is_interval ||
              (ch->type != ValueType::kInt64 &&
               ch->type != ValueType::kDouble)) {
            return std::nullopt;
          }
          return ch;
        case UnaryOperation::kBitwiseNot:
          return std::nullopt;
      }
      return std::nullopt;
    }
    case TypeTag::kBinaryExp: {
      const auto& bin = node->AsBinaryExpression();
      const auto lt = InferStaticExprType(bin.Left(), schema);
      const auto rt = InferStaticExprType(bin.Right(), schema);
      if (!lt.has_value() || !rt.has_value()) {
        return std::nullopt;
      }
      const BinaryOperation op = bin.Op();
      if (IsComparison(op) || op == BinaryOperation::kAnd ||
          op == BinaryOperation::kOr || op == BinaryOperation::kXor ||
          op == BinaryOperation::kLike || op == BinaryOperation::kNotLike ||
          op == BinaryOperation::kIsDistinctFrom ||
          op == BinaryOperation::kIsNotDistinctFrom) {
        return StaticExprType{.type = ValueType::kInt64,
                              .is_unsigned = false,
                              .is_interval = false};
      }
      if (lt->is_interval || rt->is_interval || lt->is_unsigned ||
          rt->is_unsigned) {
        return std::nullopt;
      }
      if (op == BinaryOperation::kDivide) {
        if ((lt->type == ValueType::kInt64 || lt->type == ValueType::kDouble) &&
            (rt->type == ValueType::kInt64 || rt->type == ValueType::kDouble)) {
          return StaticExprType{.type = ValueType::kDouble,
                                .is_unsigned = false,
                                .is_interval = false};
        }
        return std::nullopt;
      }
      if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract) {
        if (lt->type == ValueType::kDate && rt->type == ValueType::kDate &&
            op == BinaryOperation::kSubtract) {
          return StaticExprType{.type = ValueType::kInt64,
                                .is_unsigned = false,
                                .is_interval = false};
        }
        if ((lt->type == ValueType::kDate) != (rt->type == ValueType::kDate)) {
          const auto& other = (lt->type == ValueType::kDate) ? *rt : *lt;
          if (other.type == ValueType::kInt64) {
            return StaticExprType{.type = ValueType::kDate,
                                  .is_unsigned = false,
                                  .is_interval = false};
          }
          return std::nullopt;
        }
      }
      if (op == BinaryOperation::kAdd || op == BinaryOperation::kSubtract ||
          op == BinaryOperation::kMultiply || op == BinaryOperation::kModulo) {
        if ((lt->type == ValueType::kInt64 || lt->type == ValueType::kDouble) &&
            (rt->type == ValueType::kInt64 || rt->type == ValueType::kDouble)) {
          const ValueType res =
              (lt->type == ValueType::kDouble || rt->type == ValueType::kDouble)
                  ? ValueType::kDouble
                  : ValueType::kInt64;
          return StaticExprType{
              .type = res, .is_unsigned = false, .is_interval = false};
        }
      }
      return std::nullopt;
    }
    default:
      return std::nullopt;
  }
}

std::optional<Schema> ResolveStaticBranchSchema(const SelectStatement& branch,
                                                const CteMap& inherited_ctes,
                                                TransactionContext* context,
                                                int depth = 0) {
  if (depth > 8 || !branch.WithQueries().empty() ||
      !branch.UnionAll().empty() || branch.UnionDistinct() ||
      branch.GetSetOperationTree()) {
    return std::nullopt;
  }
  std::vector<Column> columns;
  for (const SelectSource& src : branch.Sources()) {
    if (src.unnest || src.sample || src.is_lateral || src.from_nested_join ||
        !src.using_columns.empty()) {
      return std::nullopt;
    }
    const std::string qualifier = src.alias.empty() ? src.table : src.alias;
    if (src.query != nullptr) {
      auto sub_input = ResolveStaticBranchSchema(*src.query, inherited_ctes,
                                                 context, depth + 1);
      if (!sub_input.has_value()) {
        return std::nullopt;
      }
      for (size_t i = 0; i < src.query->SelectList().size(); ++i) {
        const NamedExpression& item = src.query->SelectList()[i];
        const auto t = InferStaticExprType(item.expression, *sub_input);
        if (!t.has_value()) {
          return std::nullopt;
        }
        std::string col_name = ProjectionName(item, i);
        Column col(ColumnName{qualifier, std::move(col_name)}, t->type);
        col.SetUnsigned(t->is_unsigned);
        col.SetInterval(t->is_interval);
        columns.push_back(std::move(col));
      }
      continue;
    }
    if (src.table.empty()) {
      return std::nullopt;
    }
    if (auto cte_it = inherited_ctes.find(src.table);
        cte_it != inherited_ctes.end()) {
      if (!cte_it->second) {
        return std::nullopt;
      }
      const Schema& cte_schema = cte_it->second->schema;
      for (size_t i = 0; i < cte_schema.ColumnCount(); ++i) {
        columns.push_back(cte_schema.GetColumn(i));
        columns.back().Name().schema = qualifier;
      }
      continue;
    }
    if (context == nullptr) {
      return std::nullopt;
    }
    auto tbl = context->GetTable(src.table);
    if (!tbl.HasValue()) {
      return std::nullopt;
    }
    const Schema& tbl_schema = tbl.Value()->GetSchema();
    for (size_t i = 0; i < tbl_schema.ColumnCount(); ++i) {
      columns.push_back(tbl_schema.GetColumn(i));
      columns.back().Name().schema = qualifier;
    }
  }
  return Schema("", std::move(columns));
}

bool AreCompatibleUnreferencedSetTypes(const StaticExprType& a,
                                       const StaticExprType& b) {
  if (a == b) {
    return true;
  }
  if (a.is_interval != b.is_interval) {
    return false;
  }
  const bool a_num =
      (a.type == ValueType::kInt64 || a.type == ValueType::kDouble);
  const bool b_num =
      (b.type == ValueType::kInt64 || b.type == ValueType::kDouble);
  if (a_num && b_num) {
    return true;
  }
  if (a.type == ValueType::kVarChar && (b_num || b.type == ValueType::kDate)) {
    return true;
  }
  if (b.type == ValueType::kVarChar && (a_num || a.type == ValueType::kDate)) {
    return true;
  }
  return false;
}

bool UnionBranchesAgreeForScalarPushdown(
    const std::vector<const SelectStatement*>& branches,
    const std::vector<bool>& referenced_cols, const CteMap& inherited_ctes,
    TransactionContext* context) {
  if (branches.empty()) {
    return false;
  }
  std::vector<Schema> branch_schemas;
  branch_schemas.reserve(branches.size());
  for (const SelectStatement* b : branches) {
    auto s = ResolveStaticBranchSchema(*b, inherited_ctes, context);
    if (!s.has_value()) {
      return false;
    }
    branch_schemas.push_back(std::move(*s));
  }
  const size_t width = branches[0]->SelectList().size();
  for (size_t col = 0; col < width; ++col) {
    std::optional<StaticExprType> head_type = InferStaticExprType(
        branches[0]->SelectList()[col].expression, branch_schemas[0]);
    if (referenced_cols[col]) {
      if (!head_type.has_value()) {
        return false;
      }
      for (size_t bi = 1; bi < branches.size(); ++bi) {
        const auto bt = InferStaticExprType(
            branches[bi]->SelectList()[col].expression, branch_schemas[bi]);
        if (!bt.has_value() || *bt != *head_type) {
          return false;
        }
      }
    } else if (head_type.has_value()) {
      for (size_t bi = 1; bi < branches.size(); ++bi) {
        const auto bt = InferStaticExprType(
            branches[bi]->SelectList()[col].expression, branch_schemas[bi]);
        if (bt.has_value() &&
            !AreCompatibleUnreferencedSetTypes(*head_type, *bt)) {
          return false;
        }
      }
    }
  }
  return true;
}

}  // namespace

size_t FlattenableProjectionDepth(const SelectStatement& statement) {
  if (statement.Sources().size() != 1 || !statement.Sources()[0].query ||
      !IsFlattenableProjection(*statement.Sources()[0].query)) {
    return 0;
  }
  return 1 + FlattenableProjectionDepth(*statement.Sources()[0].query);
}

std::shared_ptr<SelectStatement> OptimizeDerivedBoundaries(
    const SelectStatement& statement, const CteMap& inherited_ctes,
    TransactionContext* context) {
  if (statement.Sources().size() != 1 ||
      statement.Sources()[0].query == nullptr) {
    return nullptr;
  }
  const SelectSource& source = statement.Sources()[0];
  const SelectStatement& derived = *source.query;
  if (derived.Sources().empty()) {
    return nullptr;
  }
  const auto& inner_source = derived.Sources()[0];
  // A CTE name occupies the same syntactic slot as a base table but cannot be
  // flattened into a catalog scan. This also covers a derived query's own
  // WITH clause and an inherited outer CTE.
  if (!inner_source.table.empty() &&
      (derived.WithQueries().contains(inner_source.table) ||
       inherited_ctes.contains(inner_source.table))) {
    return nullptr;
  }
  // Note: scalar aggregate pushdown through UNION ALL (lines 6436+) only
  // fires when UnionBranchesAgreeForScalarPushdown verifies that every column
  // referenced by pushed aggregates resolves to the exact same static
  // (ValueType, is_unsigned, is_interval) across all branches. Mixed types
  // (e.g. INT64 vs DOUBLE promotion before arithmetic) remain unpushed so the
  // UNION widens types before outer arithmetic runs.
  auto rewritten = std::make_shared<SelectStatement>(statement);

  // Flatten SELECT column-list FROM one-table derived tables.  This is
  // limited to identity projections so aliases and row multiplicity remain
  // unchanged without expression substitution.
  // A star in the parent select list (`t.*`, `*`, or the AST visitor's
  // __value_table_value(t.*) wrapper) names the derived query's output
  // columns. Merging the base table in would expand it over every base
  // column instead, and the wrapped form hides the star from column pruning,
  // so keep the boundary.
  const auto references_star = [](const Expression& root) {
    std::vector<Expression> stack{root};
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue &&
          current->AsColumnValue().GetColumnName().name == "*") {
        return true;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        stack.push_back(child);
      }
    }
    return false;
  };
  const bool parent_has_star = std::ranges::any_of(
      statement.SelectList(), [&](const NamedExpression& item) {
        return references_star(item.expression);
      });
  if (!parent_has_star && IsIdentityDerived(derived)) {
    SelectSource merged = derived.Sources()[0];
    if (source.sample && merged.sample) {
      return nullptr;
    }
    if (source.sample) {
      merged.sample = source.sample;
    }
    merged.alias = source.alias.empty() ? merged.alias : source.alias;
    merged.join_type = source.join_type;
    merged.join_condition = source.join_condition;
    rewritten->SetSources({std::move(merged)});
    return rewritten;
  }

  // Compose projections through a cardinality-preserving derived query. This
  // removes nested projection stacks while substituting the derived output
  // expressions into the parent SELECT/WHERE/ORDER BY. The rewrite is
  // intentionally restricted to a single source with no grouping, ordering,
  // limit, window, or subquery expression.
  // WITH TIES compares ORDER BY keys after projection. Either flattener
  // can substitute a sort expression that needs hidden input columns.
  if (!statement.WithTies()) {
    if (std::shared_ptr<SelectStatement> flattened =
            FlattenProjectionBoundary(statement, source, derived)) {
      return flattened;
    }

    // A grouping parent over a plain joined projection can run the grouping
    // (and the grouped-join fusion) directly over the base sources.
    if (std::shared_ptr<SelectStatement> flattened =
            FlattenGroupedDerivedBoundary(statement, source, derived)) {
      return flattened;
    }
  }

  // An outer filter observes completed window values. Move it to QUALIFY,
  // never WHERE, so each window still sees every row in its partition while
  // rejected rows avoid the derived projection and parent materialization.
  if (statement.WhereClause() && !source.sample && !source.is_lateral &&
      !source.from_nested_join && !statement.ForUpdate() &&
      !derived.ForUpdate() && !derived.HasLimit() && derived.Offset() == 0 &&
      !derived.Distinct() && !derived.HasDistinctOn() &&
      derived.OrderBy().empty() && derived.GroupBy().empty() &&
      !derived.IsGroupAll() && !derived.Having() &&
      derived.UnionAll().empty() && !derived.GetSetOperationTree() &&
      HasWindowFunctions(derived) &&
      !StatementContainsVolatileFunction(derived)) {
    std::unordered_set<std::string> names;
    const bool plain_outputs = std::ranges::all_of(
        derived.SelectList(), [&](const NamedExpression& item) {
          if (!item.expression ||
              (item.expression->Type() != TypeTag::kColumnValue &&
               item.expression->Type() != TypeTag::kConstantValue &&
               item.expression->Type() != TypeTag::kWindowFunctionExp)) {
            return false;
          }
          if (item.expression->Type() == TypeTag::kColumnValue) {
            const auto& column =
                item.expression->AsColumnValue().GetColumnName();
            if (column.name.find('.') != std::string::npos ||
                column.schema.find('.') != std::string::npos) {
              return false;
            }
          }
          const auto name = ProjectionName(item, names.size());
          return name != "*" && names.insert(AsciiToLower(name)).second;
        });
    if (plain_outputs) {
      std::vector<Expression> qualify = SplitConjuncts(derived.Qualify());
      std::vector<Expression> above;
      bool moved = false;
      for (const auto& predicate : SplitConjuncts(statement.WhereClause())) {
        const auto columns = predicate->TouchedColumns();
        bool valid = !columns.empty() && !ContainsQuery(predicate) &&
                     SafeToReduceEvaluationCount(predicate) &&
                     std::ranges::none_of(columns, [](const ColumnName& name) {
                       return name.name.find('.') != std::string::npos ||
                              name.schema.find('.') != std::string::npos;
                     });
        Expression rebound;
        if (valid) {
          rebound = SubstituteProjectionColumns(predicate, derived,
                                                source.alias, &valid);
        }
        if (valid && rebound) {
          qualify.push_back(std::move(rebound));
          moved = true;
        } else {
          above.push_back(predicate);
        }
      }
      if (moved) {
        SelectSource updated = source;
        updated.query = std::make_shared<SelectStatement>(derived);
        updated.query->SetQualify(CombineConjuncts(qualify));
        rewritten->SetSources({std::move(updated)});
        rewritten->SetWhereClause(above.empty() ? Expression{}
                                                : CombineConjuncts(above));
        return rewritten;
      }
    }
  }

  // A predicate on an ordinary GROUP BY key selects complete groups. Move
  // it below aggregation, including joined inputs, but keep aggregate-output
  // predicates above it. LIMIT, grouping sets and windows are boundaries.
  if (statement.WhereClause() && !source.sample && !derived.GroupBy().empty() &&
      !derived.IsGroupAll() && !derived.HasLimit() && derived.Offset() == 0 &&
      !derived.Qualify() && derived.UnionAll().empty() &&
      !derived.GetSetOperationTree() && derived.WithQueries().empty() &&
      !HasWindowFunctions(derived) &&
      !StatementContainsVolatileFunction(derived)) {
    std::vector<Expression> inner = SplitConjuncts(derived.WhereClause());
    std::vector<Expression> above;
    bool moved = false;
    for (const auto& predicate : SplitConjuncts(statement.WhereClause())) {
      bool valid =
          !ContainsQuery(predicate) && SafeToReduceEvaluationCount(predicate);
      const auto columns = predicate->TouchedColumns();
      valid &= !columns.empty();
      for (const auto& column : columns) {
        size_t matches = 0;
        bool group_key = false;
        for (size_t i = 0; i < derived.SelectList().size(); ++i) {
          const auto& item = derived.SelectList()[i];
          if (!IdentifierEquals(ProjectionName(item, i), column.name)) {
            continue;
          }
          ++matches;
          group_key =
              item.expression &&
              item.expression->Type() == TypeTag::kColumnValue &&
              std::ranges::any_of(derived.GroupBy(), [&](const auto& key) {
                return key && key->Type() == TypeTag::kColumnValue &&
                       key->AsColumnValue().GetColumnName() ==
                           item.expression->AsColumnValue().GetColumnName();
              });
        }
        valid &= matches == 1 && group_key;
      }
      Expression rebound;
      if (valid) {
        rebound = SubstituteProjectionColumns(predicate, derived, source.alias,
                                              &valid);
      }
      if (valid && rebound) {
        inner.push_back(std::move(rebound));
        moved = true;
      } else {
        above.push_back(predicate);
      }
    }
    if (moved) {
      SelectSource updated = source;
      updated.query = std::make_shared<SelectStatement>(derived);
      updated.query->SetWhereClause(CombineConjuncts(inner));
      rewritten->SetSources({std::move(updated)});
      rewritten->SetWhereClause(CombineConjuncts(above));
      return rewritten;
    }
  }

  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    (PredicateCanEnterDerived(conjunct, derived,
                              source.alias.empty() ? "u" : source.alias)
         ? pushable
         : remaining)
        .push_back(conjunct);
  }
  // UNION ALL and UNION DISTINCT both distribute a local predicate over
  // their branches (filtering before or after the dedup is equivalent), so
  // the pushdown below is safe for either flavor.
  const bool is_union =
      !derived.UnionAll().empty() &&
      (derived.GetSetOperationTree() == nullptr ||
       !derived.GetSetOperationTree()->grouped) &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll ||
                                   kind == SetOperationKind::kUnion;
                          });
  if (is_union) {
    // UNION ALL / UNION DISTINCT have no row-changing boundary before the outer
    // filter. Push a local predicate into every branch, then retain any
    // non-local predicates above the set operation.
    SelectStatement pushed = derived;
    std::vector<Expression> head_where = SplitConjuncts(pushed.WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, pushed, source.alias.empty() ? "u" : source.alias)) {
        head_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    if (!pushable.empty()) {
      pushed.SetWhereClause(CombineConjuncts(head_where));
    }
    std::vector<std::shared_ptr<SelectStatement>> branches;
    branches.reserve(derived.UnionAll().size());
    for (const auto& branch : derived.UnionAll()) {
      if (!branch) {
        continue;
      }
      auto copied = std::make_shared<SelectStatement>(*branch);
      std::vector<Expression> branch_where =
          SplitConjuncts(copied->WhereClause());
      for (const Expression& predicate : pushable) {
        if (!PredicateCanEnterDerived(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          return nullptr;
        }
        if (Expression rebound = RebindDerivedPredicate(
                predicate, *copied,
                source.alias.empty() ? "u" : source.alias)) {
          branch_where.push_back(std::move(rebound));
        } else {
          return nullptr;
        }
      }
      if (!pushable.empty()) {
        copied->SetWhereClause(CombineConjuncts(branch_where));
      }
      branches.push_back(std::move(copied));
    }
    if (!pushable.empty()) {
      for (const auto& branch : branches) {
        if (branch == nullptr ||
            branch->SelectList().size() != pushed.SelectList().size()) {
          return nullptr;
        }
      }
      const std::vector<SetOperationKind> kinds = derived.SetOperationKinds();
      const std::vector<SetOperationMatch> matches = derived.Matches();
      pushed.ClearUnionAll();
      pushed.SetSetOperationTree(nullptr);
      for (size_t i = 0; i < branches.size(); ++i) {
        pushed.AddSetOperation(
            i < kinds.size() ? kinds[i] : SetOperationKind::kUnionAll,
            branches[i], i < matches.size() ? matches[i] : SetOperationMatch{});
      }
      SelectSource updated = source;
      updated.query = std::make_shared<SelectStatement>(std::move(pushed));
      rewritten->SetSources({std::move(updated)});
      rewritten->SetWhereClause(
          remaining.empty() ? Expression{} : CombineConjuncts(remaining));
      return rewritten;
    }
  }

  if (!pushable.empty() && derived.UnionAll().empty() &&
      !derived.GetSetOperationTree()) {
    auto copied = std::make_shared<SelectStatement>(derived);
    std::vector<Expression> inner_where = SplitConjuncts(copied->WhereClause());
    for (const Expression& predicate : pushable) {
      if (Expression rebound = RebindDerivedPredicate(
              predicate, *copied, source.alias.empty() ? "d" : source.alias)) {
        inner_where.push_back(std::move(rebound));
      } else {
        return nullptr;
      }
    }
    copied->SetWhereClause(CombineConjuncts(inner_where));
    SelectSource updated = source;
    updated.query = std::move(copied);
    rewritten->SetSources({std::move(updated)});
    rewritten->SetWhereClause(remaining.empty() ? Expression{}
                                                : CombineConjuncts(remaining));
    return rewritten;
  }

  // For UNION ALL without a pushed predicate, a finite outer LIMIT still
  // bounds how many rows each append branch can contribute.  The final outer
  // LIMIT remains authoritative, including OFFSET semantics.
  if (is_union && statement.OrderBy().empty() && statement.HasLimit() &&
      statement.Limit() > 0 && statement.Offset() == 0 &&
      !derived.UnionDistinct() &&
      std::ranges::all_of(derived.SetOperationKinds(),
                          [](SetOperationKind kind) {
                            return kind == SetOperationKind::kUnionAll;
                          })) {
    const size_t cap = statement.Limit();
    const bool head_needs_cap = !derived.HasLimit();
    const bool branch_needs_cap = std::ranges::any_of(
        derived.UnionAll(), [](const std::shared_ptr<SelectStatement>& branch) {
          return branch && !branch->HasLimit() && branch->Offset() == 0;
        });
    if (!head_needs_cap && !branch_needs_cap) {
      return nullptr;
    }
    SelectStatement pushed = derived;
    const std::vector<SetOperationMatch> matches = derived.Matches();
    pushed.ClearUnionAll();
    pushed.SetSetOperationTree(nullptr);
    auto cap_branch = [cap](const std::shared_ptr<SelectStatement>& branch) {
      auto copied = std::make_shared<SelectStatement>(*branch);
      if (!copied->HasLimit() && copied->Offset() == 0) {
        copied->SetLimit(cap);
      }
      return copied;
    };
    if (head_needs_cap) {
      pushed.SetLimit(cap);
    }
    for (size_t i = 0; i < derived.UnionAll().size(); ++i) {
      const auto& branch = derived.UnionAll()[i];
      if (branch) {
        pushed.AddSetOperation(
            SetOperationKind::kUnionAll, cap_branch(branch),
            i < matches.size() ? matches[i] : SetOperationMatch{});
      }
    }
    SelectSource updated = source;
    updated.query = std::make_shared<SelectStatement>(std::move(pushed));
    rewritten->SetSources({std::move(updated)});
    return rewritten;
  }

  // Scalar aggregate pushdown through UNION ALL: rewrite ungrouped scalar
  // COUNT/SUM/AVG/MIN/MAX over a pure positional UNION ALL so each branch
  // reduces rows in place (often hitting fused grouped/scalar columnar scans)
  // before combining partial results at the outer node.
  if (is_union && statement.WithQueries().empty() &&
      statement.UnionAll().empty() && !statement.UnionDistinct() &&
      !statement.GetSetOperationTree() && !statement.WhereClause() &&
      statement.GroupBy().empty() && !statement.IsGroupAll() &&
      !statement.Having() && !statement.Qualify() && !statement.Distinct() &&
      !statement.WithTies() && !statement.HasLimit() &&
      statement.Offset() == 0 && statement.OrderBy().empty() &&
      !statement.AsStruct() && !HasWindowFunctions(statement) &&
      !statement.SelectList().empty() && !source.is_lateral &&
      !source.from_nested_join && source.using_columns.empty() &&
      !source.join_condition && derived.WithQueries().empty() &&
      (!derived.GetSetOperationTree() ||
       !derived.GetSetOperationTree()->grouped) &&
      !derived.UnionDistinct() && !derived.UnionAll().empty() &&
      !derived.HasLimit() && derived.Offset() == 0 &&
      derived.OrderBy().empty() && !derived.WithTies()) {
    bool all_positional_union_all =
        derived.SetOperationKinds().size() <= derived.UnionAll().size() &&
        std::ranges::all_of(derived.SetOperationKinds(),
                            [](SetOperationKind kind) {
                              return kind == SetOperationKind::kUnionAll;
                            });
    for (size_t i = 0;
         all_positional_union_all && i < derived.UnionAll().size(); ++i) {
      const SetOperationMatch m = derived.MatchFor(i);
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        all_positional_union_all = false;
      }
    }
    if (all_positional_union_all) {
      SelectStatement head_branch(derived);
      head_branch.ClearUnionAll();
      head_branch.SetSetOperationTree(nullptr);
      std::vector<const SelectStatement*> branches;
      branches.reserve(1 + derived.UnionAll().size());
      branches.push_back(&head_branch);
      for (const auto& b : derived.UnionAll()) {
        branches.push_back(b.get());
      }
      const size_t branch_width = head_branch.SelectList().size();
      bool branches_ok = branch_width > 0;
      for (const SelectStatement* b : branches) {
        if (b == nullptr || !b->WithQueries().empty() ||
            !b->UnionAll().empty() || b->UnionDistinct() ||
            b->GetSetOperationTree() || b->HasLimit() || b->Offset() != 0 ||
            b->WithTies() || b->Distinct() || !b->GroupBy().empty() ||
            b->IsGroupAll() || b->Having() || b->Qualify() ||
            !b->OrderBy().empty() || b->AsStruct() || HasWindowFunctions(*b) ||
            b->SelectList().size() != branch_width) {
          branches_ok = false;
          break;
        }
        for (const NamedExpression& item : b->SelectList()) {
          if (!item.expression || ContainsAggregate(item.expression) ||
              ContainsQuery(item.expression) ||
              (item.expression->Type() == TypeTag::kColumnValue &&
               item.expression->AsColumnValue().GetColumnName().name == "*")) {
            branches_ok = false;
            break;
          }
        }
        if (!branches_ok) {
          break;
        }
      }
      if (branches_ok) {
        std::vector<std::string> head_names;
        head_names.reserve(branch_width);
        for (const NamedExpression& item : head_branch.SelectList()) {
          if (!item.name.empty()) {
            head_names.push_back(item.name);
          } else if (item.expression->Type() == TypeTag::kColumnValue) {
            head_names.push_back(
                item.expression->AsColumnValue().GetColumnName().name);
          } else {
            head_names.emplace_back();
          }
        }
        auto resolve_head_col =
            [&](const ColumnName& col) -> std::optional<size_t> {
          if (!col.schema.empty() &&
              (source.alias.empty() ||
               !IdentifierEquals(col.schema, source.alias))) {
            return std::nullopt;
          }
          std::optional<size_t> matched;
          for (size_t c = 0; c < head_names.size(); ++c) {
            if (!head_names[c].empty() &&
                IdentifierEquals(head_names[c], col.name)) {
              if (matched.has_value()) {
                return std::nullopt;  // ambiguous
              }
              matched = c;
            }
          }
          return matched;
        };
        struct PushedAgg {
          const AggregateExpression* agg{nullptr};
          AggregationType op{AggregationType::kCount};
          bool count_star{false};
          Expression child;
          std::string col_a;
          std::string col_c;
        };
        std::vector<PushedAgg> pushed_aggs;
        std::unordered_map<const AggregateExpression*, size_t> agg_to_idx;
        bool select_ok = true;
        auto validate_and_collect = [&](auto& self, const Expression& node,
                                        bool inside_agg) -> bool {
          if (!node || node->Type() == TypeTag::kQueryExp ||
              node->Type() == TypeTag::kWindowFunctionExp ||
              ContainsParameterSlot(node)) {
            return false;
          }
          if (node->Type() == TypeTag::kColumnValue) {
            if (!inside_agg) {
              return false;
            }
            return resolve_head_col(node->AsColumnValue().GetColumnName())
                .has_value();
          }
          if (node->Type() == TypeTag::kAggregateExp) {
            if (inside_agg) {
              return false;
            }
            const auto& agg = node->AsAggregateExpression();
            if (agg.Distinct() || agg.IgnoreNulls() ||
                agg.Having() != AggregateHavingModifier::kNone ||
                agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
                agg.InnerLimit().has_value() || agg.SecondaryArg() ||
                !agg.TrailingArgs().empty() || agg.WhereFilter() ||
                agg.NeedsGroupContext() || agg.HasInnerGroupBy()) {
              return false;
            }
            const AggregationType op = agg.GetType();
            if (op != AggregationType::kCount && op != AggregationType::kSum &&
                op != AggregationType::kAvg && op != AggregationType::kMin &&
                op != AggregationType::kMax) {
              return false;
            }
            const bool count_star =
                (op == AggregationType::kCount) && IsCountStar(agg);
            if (!agg_to_idx.contains(&agg)) {
              const size_t idx = pushed_aggs.size();
              agg_to_idx.emplace(&agg, idx);
              pushed_aggs.push_back(PushedAgg{
                  .agg = &agg,
                  .op = op,
                  .count_star = count_star,
                  .child = agg.Child(),
                  .col_a = "__tinylamb_uagg_a_" + std::to_string(idx),
                  .col_c = "__tinylamb_uagg_c_" + std::to_string(idx),
              });
            }
            return true;
          }
          for (const Expression& child : ExpressionChildren(node)) {
            if (!self(self, child, inside_agg)) {
              return false;
            }
          }
          return true;
        };
        for (const NamedExpression& item : statement.SelectList()) {
          if (!validate_and_collect(validate_and_collect, item.expression,
                                    false)) {
            select_ok = false;
            break;
          }
        }
        if (select_ok && !pushed_aggs.empty()) {
          std::vector<bool> referenced_cols(branch_width, false);
          auto collect_ref_cols = [&](auto& self,
                                      const Expression& node) -> void {
            if (!node) {
              return;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              if (const auto col_idx =
                      resolve_head_col(node->AsColumnValue().GetColumnName());
                  col_idx.has_value()) {
                referenced_cols[*col_idx] = true;
              }
              return;
            }
            for (const Expression& ch : ExpressionChildren(node)) {
              self(self, ch);
            }
          };
          for (const PushedAgg& pa : pushed_aggs) {
            if (!pa.count_star) {
              collect_ref_cols(collect_ref_cols, pa.child);
            }
          }
          select_ok = UnionBranchesAgreeForScalarPushdown(
              branches, referenced_cols, inherited_ctes, context);
        }
        if (select_ok && !pushed_aggs.empty()) {
          auto rebind_for_branch =
              [&](auto& self, const Expression& node,
                  const SelectStatement& branch) -> Expression {
            if (!node) {
              return nullptr;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              // Every referenced column was validated by select_ok above.
              const std::optional<size_t> col_idx =
                  resolve_head_col(node->AsColumnValue().GetColumnName());
              if (!col_idx.has_value()) {
                return nullptr;
              }
              return branch.SelectList()[*col_idx].expression;
            }
            const std::vector<Expression> children = ExpressionChildren(node);
            if (children.empty()) {
              return node;
            }
            std::vector<Expression> rewritten_children;
            rewritten_children.reserve(children.size());
            for (const Expression& ch : children) {
              rewritten_children.push_back(self(self, ch, branch));
            }
            return WithExpressionChildren(node, std::move(rewritten_children));
          };
          auto build_branch = [&](const SelectStatement& src_branch) {
            SelectStatement out(src_branch);
            out.ClearUnionAll();
            out.SetSetOperationTree(nullptr);
            std::vector<NamedExpression> b_select;
            b_select.reserve(pushed_aggs.size() * 2);
            for (const PushedAgg& pa : pushed_aggs) {
              if (pa.count_star) {
                b_select.emplace_back(
                    pa.col_a,
                    AggregateExpressionExp(AggregationType::kCount, pa.child));
              } else {
                Expression rebound =
                    rebind_for_branch(rebind_for_branch, pa.child, src_branch);
                if (pa.op == AggregationType::kAvg) {
                  b_select.emplace_back(
                      pa.col_a,
                      AggregateExpressionExp(AggregationType::kAvg, rebound));
                  b_select.emplace_back(
                      pa.col_c, AggregateExpressionExp(AggregationType::kCount,
                                                       std::move(rebound)));
                } else {
                  b_select.emplace_back(
                      pa.col_a,
                      AggregateExpressionExp(pa.op, std::move(rebound)));
                }
              }
            }
            out.SetSelectList(std::move(b_select));
            return out;
          };
          SelectStatement new_derived = build_branch(head_branch);
          for (size_t i = 1; i < branches.size(); ++i) {
            new_derived.AddUnionAll(
                std::make_shared<SelectStatement>(build_branch(*branches[i])));
          }
          auto rebuild_outer = [&](auto& self,
                                   const Expression& node) -> Expression {
            if (!node) {
              return nullptr;
            }
            if (node->Type() == TypeTag::kAggregateExp) {
              const PushedAgg& pa =
                  pushed_aggs[agg_to_idx.at(&node->AsAggregateExpression())];
              if (pa.op == AggregationType::kAvg) {
                Expression ref_a = ColumnValueExp(pa.col_a);
                Expression ref_c = ColumnValueExp(pa.col_c);
                Expression sum_c =
                    AggregateExpressionExp(AggregationType::kSum, ref_c);
                Expression sum_ac = AggregateExpressionExp(
                    AggregationType::kSum,
                    BinaryExpressionExp(ref_a, BinaryOperation::kMultiply,
                                        ref_c));
                Expression zero_cond =
                    BinaryExpressionExp(sum_c, BinaryOperation::kEquals,
                                        ConstantValueExp(Value(int64_t{0})));
                return CaseExpressionExp(
                    {{std::move(zero_cond), ConstantValueExp(Value())}},
                    BinaryExpressionExp(std::move(sum_ac),
                                        BinaryOperation::kDivide,
                                        std::move(sum_c)));
              }
              const AggregationType combine_op =
                  (pa.op == AggregationType::kCount) ? AggregationType::kSum
                                                     : pa.op;
              return AggregateExpressionExp(combine_op,
                                            ColumnValueExp(pa.col_a));
            }
            const std::vector<Expression> children = ExpressionChildren(node);
            if (children.empty()) {
              return node;
            }
            std::vector<Expression> rewritten_children;
            rewritten_children.reserve(children.size());
            for (const Expression& ch : children) {
              rewritten_children.push_back(self(self, ch));
            }
            return WithExpressionChildren(node, std::move(rewritten_children));
          };
          std::vector<NamedExpression> outer_select;
          outer_select.reserve(statement.SelectList().size());
          for (size_t si = 0; si < statement.SelectList().size(); ++si) {
            const NamedExpression& item = statement.SelectList()[si];
            const std::string out_name =
                item.name.empty() ? ProjectionName(item, si) : item.name;
            outer_select.emplace_back(
                out_name, rebuild_outer(rebuild_outer, item.expression));
          }
          rewritten->SetSelectList(std::move(outer_select));
          SelectSource updated = source;
          updated.query =
              std::make_shared<SelectStatement>(std::move(new_derived));
          rewritten->SetSources({std::move(updated)});
          return rewritten;
        }
      }
    }
  }
  return nullptr;
}

}  // namespace tinylamb::relational_detail
