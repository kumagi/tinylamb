/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

bool IsIdentChar(unsigned char c) { return std::isalnum(c) != 0 || c == '_'; }

// std::stod/std::stoll throw out_of_range on tokens like "1e999" or 20-digit
// integers; template extraction must never leak that exception through
// SqlEngine::Prepare's StatusOr contract. Parse defensively instead.
bool TryParseDouble(const std::string& token, double* out) {
  try {
    size_t consumed = 0;
    const double value = std::stod(token, &consumed);
    if (consumed != token.size()) {
      return false;
    }
    *out = value;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool TryParseInt64(const std::string& token, int64_t* out) {
  try {
    size_t consumed = 0;
    const int64_t value = std::stoll(token, &consumed);
    if (consumed != token.size()) {
      return false;
    }
    *out = value;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool KeywordAt(std::string_view sql, size_t pos, std::string_view keyword) {
  if (pos + keyword.size() > sql.size()) {
    return false;
  }
  for (size_t i = 0; i < keyword.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(sql[pos + i])) != keyword[i]) {
      return false;
    }
  }
  const bool start_ok =
      pos == 0 || !IsIdentChar(static_cast<unsigned char>(sql[pos - 1]));
  const bool end_ok =
      pos + keyword.size() == sql.size() ||
      !IsIdentChar(static_cast<unsigned char>(sql[pos + keyword.size()]));
  return start_ok && end_ok;
}

bool SkipBindConstant(const Value& value) {
  return value.IsNull() || value.type == ValueType::kNull ||
         (value.type != ValueType::kInt64 && value.type != ValueType::kDouble &&
          value.type != ValueType::kVarChar && value.type != ValueType::kDate);
}

// Visitor-synthesized function calls carry structural constant arguments
// (operators, quantifier modes, field names, unit names) that have no
// counterpart in the SQL text.  The text-driven extractor never emits them,
// so binding must pass them through verbatim; treating them as bindable
// literals shifts every later parameter and silently rewrites operator or
// field-name slots into user data.
bool IsStructuralCallArg(std::string_view func, size_t index) {
  if (func == "__quantified__") {
    return index >= 2;
  }
  if (func == "__struct_json__") {
    return index % 3 != 1;
  }
  if (func == "make_interval" || func == "get_field") {
    return index == 1;
  }
  return false;
}

StatusOr<Expression> BindExpression(const Expression& expression,
                                    const std::vector<Value>& parameters,
                                    size_t* index);

StatusOr<NamedExpression> BindNamed(
    const NamedExpression&
        item,  // NOLINT(misc-no-recursion) // Recursive expression-tree binding
               // by design; trees are parser-bounded in depth.
    const std::vector<Value>& parameters, size_t* index) {
  ASSIGN_OR_RETURN(Expression, bound_expr,
                   (BindExpression(item.expression, parameters, index)));
  return NamedExpression{item.name, std::move(bound_expr)};
}

StatusOr<std::shared_ptr<SelectStatement>> BindSelect(
    const SelectStatement& select, const std::vector<Value>& parameters,
    size_t* index);

StatusOr<Expression> BindExpression(
    const Expression&
        expression,  // NOLINT(misc-no-recursion) // Recursive expression-tree
                     // binding by design; trees are parser-bounded in depth.
    const std::vector<Value>& parameters, size_t* index) {
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      const Value current = expression->AsConstantValue().GetValue();
      if (SkipBindConstant(current)) {
        return expression;
      }
      if (*index >= parameters.size()) {
        return Status(Status::kInvalidArgument,
                      "SQL template parameter underflow");
      }
      Value parameter = parameters[(*index)++];
      // The extractor reads `date '...'` from the SQL text as a bare string and
      // would otherwise degrade the typed DATE constant into a VARCHAR.  Keep
      // the cached tree type-correct so later binds (and the interpreter) see a
      // DATE, not a string.
      if (current.type == ValueType::kDate &&
          parameter.type == ValueType::kVarChar) {
        parameter = Value::Date(parameter.value.varchar_value);
      }
      // The fingerprint collapses date literals and plain strings into the
      // same '?' slot. The forward direction is repaired above; the reverse
      // (cached string constant, new SQL carries a date literal) would
      // silently swap the comparison type, so reject it -- the engine falls
      // back to parsing the statement verbatim.
      if (current.type == ValueType::kVarChar &&
          parameter.type == ValueType::kDate) {
        return Status(Status::kInvalidArgument,
                      "SQL template literal type mismatch: date "
                      "literal bound into a string slot");
      }
      return ConstantValueExp(parameter);
    }
    case TypeTag::kColumnValue:
      return ColumnValueExp(expression->AsColumnValue().GetColumnName());
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      ASSIGN_OR_RETURN(Expression, left,
                       (BindExpression(binary.Left(), parameters, index)));
      ASSIGN_OR_RETURN(Expression, right,
                       (BindExpression(binary.Right(), parameters, index)));
      return BinaryExpressionExp(std::move(left), binary.Op(),
                                 std::move(right));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      ASSIGN_OR_RETURN(Expression, h_child,
                       (BindExpression(unary.Child(), parameters, index)));
      return UnaryExpressionExp(std::move(h_child), unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      ASSIGN_OR_RETURN(Expression, h_agg,
                       (BindExpression(aggregate.Child(), parameters, index)));
      return AggregateExpressionExp(aggregate.GetType(), std::move(h_agg),
                                    aggregate.Distinct());
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        ASSIGN_OR_RETURN(Expression, hv7531_0,
                         (BindExpression(clause.first, parameters, index)));
        Expression when = std::move(hv7531_0);
        ASSIGN_OR_RETURN(Expression, hv7606_0,
                         (BindExpression(clause.second, parameters, index)));
        Expression then = std::move(hv7606_0);
        clauses.emplace_back(std::move(when), std::move(then));
      }
      ASSIGN_OR_RETURN(
          Expression, h_else,
          (BindExpression(searched.else_clause_, parameters, index)));
      return CaseExpressionExp(std::move(clauses), std::move(h_else));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      ASSIGN_OR_RETURN(Expression, child,
                       (BindExpression(in.child_, parameters, index)));
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        ASSIGN_OR_RETURN(Expression, hv8164_0,
                         (BindExpression(item, parameters, index)));
        list.push_back(std::move(hv8164_0));
      }
      return InExpressionExp(std::move(child), std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      const std::string& func = call.FuncName();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (size_t i = 0; i < call.Args().size(); ++i) {
        if (IsStructuralCallArg(func, i)) {
          args.push_back(call.Args()[i]);
        } else {
          ASSIGN_OR_RETURN(Expression, h_arg,
                           (BindExpression(call.Args()[i], parameters, index)));
          args.push_back(std::move(h_arg));
        }
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        ASSIGN_OR_RETURN(Expression, hv9098_0,
                         (BindExpression(element, parameters, index)));
        elements.push_back(std::move(hv9098_0));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      // Text order inside "test IN (SELECT ...)": the tested expression comes
      // first, the subquery body afterwards.
      ASSIGN_OR_RETURN(Expression, test,
                       (BindExpression(query.Test(), parameters, index)));
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, subquery,
                       (BindSelect(*query.Query(), parameters, index)));
      auto bound = std::make_shared<QueryExpression>(
          std::move(subquery), std::move(test), query.Exists(),
          query.Negated());
      // Preserve ARRAY(SELECT ...) semantics across template rebinding.
      bound->SetArrayResult(query.ArrayResult());
      return Expression{bound};
    }
    case TypeTag::kIntervalExp: {
      const auto& interval = expression->AsIntervalExpression();
      return IntervalExpressionExp(interval.Amount(), interval.Unit());
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      ASSIGN_OR_RETURN(Expression, child,
                       (BindExpression(cast.Child(), parameters, index)));
      return std::make_shared<CastExpression>(
          std::move(child), cast.TargetTypeName(), cast.ReturnNullOnError());
    }
    default:
      // Unhandled expression kinds return the cached tree's subtree SHARED
      // (not cloned). Invariant: Statement/Expression trees are immutable
      // after construction -- every consumer must treat them as read-only or
      // this aliasing silently corrupts the template cache.
      return expression;
  }
}

// Binding must consume parameters in SQL text order -- the order
// ExtractSqlTemplate emits them: WITH -> DISTINCT ON keys -> SELECT list ->
// FROM/JOIN conditions -> WHERE -> GROUP BY -> HAVING -> QUALIFY -> ORDER BY
// -> UNION ALL branches.  Binding clauses in any other order silently swaps
// parameters between clauses of same-fingerprint statements
// (improvements2.md §7.1).
bool ContainsBindableConstant(const Expression& expression);

bool SelectHasBindableConstant(
    const SelectStatement&
        select) {  // NOLINT(misc-no-recursion) // Recursive statement-tree scan
                   // by design; trees are parser-bounded in depth.
  for (const Expression& key : select.DistinctOn()) {
    if (ContainsBindableConstant(key)) {
      return true;
    }
  }
  for (const NamedExpression& item : select.SelectList()) {
    if (ContainsBindableConstant(item.expression)) {
      return true;
    }
  }
  if (ContainsBindableConstant(select.WhereClause())) {
    return true;
  }
  for (const Expression& key : select.GroupBy()) {
    if (ContainsBindableConstant(key)) {
      return true;
    }
  }
  if (ContainsBindableConstant(select.Having())) {
    return true;
  }
  if (ContainsBindableConstant(select.Qualify())) {
    return true;
  }
  for (const auto& term : select.OrderBy()) {
    if (ContainsBindableConstant(term.expression)) {
      return true;
    }
  }
  for (const SelectSource& source : select.Sources()) {
    if (ContainsBindableConstant(source.join_condition)) {
      return true;
    }
    if (source.query && SelectHasBindableConstant(*source.query)) {
      return true;
    }
  }
  return std::ranges::any_of(
      select.WithQueries(),
      [](const auto&
             entry) {  // NOLINT(misc-no-recursion) // Recursive statement-tree
                       // scan by design; trees are parser-bounded in depth.
        return SelectHasBindableConstant(*entry.second);
      });
}

bool ContainsBindableConstant(
    const Expression&
        expression) {  // NOLINT(misc-no-recursion) // Recursive expression-tree
                       // scan by design; trees are parser-bounded in depth.
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue:
      return !SkipBindConstant(expression->AsConstantValue().GetValue());
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return ContainsBindableConstant(binary.Left()) ||
             ContainsBindableConstant(binary.Right());
    }
    case TypeTag::kUnaryExp:
      return ContainsBindableConstant(expression->AsUnaryExpression().Child());
    case TypeTag::kAggregateExp:
      return ContainsBindableConstant(
          expression->AsAggregateExpression().Child());
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      for (const auto& clause : searched.when_clauses_) {
        if (ContainsBindableConstant(clause.first) ||
            ContainsBindableConstant(clause.second)) {
          return true;
        }
      }
      return ContainsBindableConstant(searched.else_clause_);
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (ContainsBindableConstant(in.child_)) {
        return true;
      }
      return std::ranges::any_of(in.list_, ContainsBindableConstant);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      for (size_t i = 0; i < call.Args().size(); ++i) {
        if (IsStructuralCallArg(call.FuncName(), i)) {
          continue;
        }
        if (ContainsBindableConstant(call.Args()[i])) {
          return true;
        }
      }
      return false;
    }
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        if (ContainsBindableConstant(element)) {
          return true;
        }
      }
      return false;
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      return ContainsBindableConstant(query.Test()) ||
             SelectHasBindableConstant(*query.Query());
    }
    case TypeTag::kCastExp:
      return ContainsBindableConstant(expression->AsCastExpression().Child());
    default:
      return false;
  }
}

StatusOr<std::shared_ptr<SelectStatement>>
BindSelect(  // NOLINT(misc-no-recursion) // Recursive statement-tree binding by
             // design; trees are parser-bounded in depth.
    const SelectStatement& select, const std::vector<Value>& parameters,
    size_t* index) {
  std::vector<std::pair<std::string, std::shared_ptr<SelectStatement>>> withs;
  withs.reserve(select.WithQueries().size());
  if (select.WithQueries().size() > 1) {
    // CTE bodies precede the main SELECT in the text, but WithQueries() is an
    // unordered_map that lost their declaration order; binding by sorted name
    // would swap parameters whenever declarations are not alphabetical.
    // Refuse such statements instead -- the engine falls back to parsing the
    // SQL verbatim.
    for (const auto& [name, query] : select.WithQueries()) {
      (void)name;
      if (SelectHasBindableConstant(*query)) {
        return Status(Status::kInvalidArgument,
                      "SQL template cannot recover WITH declaration order");
      }
    }
  }
  {
    // CTE bodies precede the main SELECT in the text; unordered_map gives no
    // stable order, so bind them by sorted name.
    std::vector<std::string> names;
    names.reserve(select.WithQueries().size());
    for (const auto& [name, query] : select.WithQueries()) {
      names.push_back(name);
    }
    std::ranges::sort(names);
    for (const std::string& name : names) {
      ASSIGN_OR_RETURN(
          std::shared_ptr<SelectStatement>, hv16241_0,
          (BindSelect(*select.WithQueries().at(name), parameters, index)));
      auto bound = std::move(hv16241_0);
      withs.emplace_back(name, std::move(bound));
    }
  }

  std::vector<Expression> bound_distinct_on;
  if (select.HasDistinctOn()) {
    // DISTINCT ON keys sit immediately after SELECT in the SQL text -- before
    // the select list -- so bind them first to keep parameter consumption in
    // text order (ExtractSqlTemplate emits strictly left-to-right).
    bound_distinct_on.reserve(select.DistinctOn().size());
    for (const Expression& item : select.DistinctOn()) {
      ASSIGN_OR_RETURN(Expression, hv18863_0,
                       (BindExpression(item, parameters, index)));
      bound_distinct_on.push_back(std::move(hv18863_0));
    }
  }

  std::vector<NamedExpression> items;
  items.reserve(select.SelectList().size());
  for (const NamedExpression& item : select.SelectList()) {
    ASSIGN_OR_RETURN(NamedExpression, hv16537_0,
                     (BindNamed(item, parameters, index)));
    items.push_back(std::move(hv16537_0));
  }

  std::vector<SelectSource> sources;
  sources.reserve(select.Sources().size());
  for (const SelectSource& source : select.Sources()) {
    SelectSource copied = source;
    if (source.query) {
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv16794_0,
                       (BindSelect(*source.query, parameters, index)));
      copied.query = std::move(hv16794_0);
    }
    ASSIGN_OR_RETURN(
        Expression, hv16867_0,
        (BindExpression(source.join_condition, parameters, index)));
    copied.join_condition = std::move(hv16867_0);
    sources.push_back(std::move(copied));
  }
  ASSIGN_OR_RETURN(Expression, hv17007_0,
                   (BindExpression(select.WhereClause(), parameters, index)));
  Expression where = std::move(hv17007_0);

  std::vector<Expression> group;
  group.reserve(select.GroupBy().size());
  for (const Expression& item : select.GroupBy()) {
    ASSIGN_OR_RETURN(Expression, hv17214_0,
                     (BindExpression(item, parameters, index)));
    group.push_back(std::move(hv17214_0));
  }

  Expression having;
  if (select.Having()) {
    ASSIGN_OR_RETURN(Expression, hv17327_0,
                     (BindExpression(select.Having(), parameters, index)));
    having = std::move(hv17327_0);
  }

  // QUALIFY precedes ORDER BY in the SQL text (SELECT ... [QUALIFY ...]
  // [ORDER BY ...]), so bind it before the ORDER BY terms.
  Expression bound_qualify;
  if (select.Qualify()) {
    ASSIGN_OR_RETURN(Expression, hv18599_0,
                     (BindExpression(select.Qualify(), parameters, index)));
    bound_qualify = std::move(hv18599_0);
  }

  std::vector<SelectStatement::OrderByTerm> order;
  order.reserve(select.OrderBy().size());
  for (const auto& term : select.OrderBy()) {
    ASSIGN_OR_RETURN(Expression, hv17536_0,
                     (BindExpression(term.expression, parameters, index)));
    order.push_back({std::move(hv17536_0), term.ascending, term.nulls_first});
  }

  auto result = std::make_shared<SelectStatement>(
      std::move(items), select.FromClause(), std::move(where), std::move(order),
      select.Limit(), select.Offset(), select.Distinct());
  // The constructor sets limit_ but not has_limit_; re-apply every clause the
  // re-bound statement must preserve, otherwise a template-cache hit rebuilds
  // a statement that silently drops LIMIT / QUALIFY / DISTINCT ON / WITH TIES
  // / AS STRUCT (the visitor's SubstituteInSelect copies all of these).
  result->SetLimit(select.HasLimit() ? std::optional<size_t>(select.Limit())
                                     : std::nullopt);
  for (const auto& [alias, table] : select.Aliases()) {
    result->AddAlias(alias, table);
  }
  result->SetSources(std::move(sources));
  if (!group.empty()) {
    result->SetGroupBy(std::move(group));
  }
  if (having) {
    result->SetHaving(std::move(having));
  }
  if (bound_qualify) {
    result->SetQualify(std::move(bound_qualify));
  }
  if (!bound_distinct_on.empty()) {
    result->SetDistinctOn(std::move(bound_distinct_on));
  }
  result->SetWithTies(select.WithTies());
  result->SetAsStruct(select.AsStruct());
  // UNION ALL branches appear after the main SELECT in SQL text, so bind
  // them last (after every main-select parameter) in branch order. Dropping
  // them silently shrank re-bound statements to their first branch.
  for (size_t branch_index = 0; branch_index < select.UnionAll().size();
       ++branch_index) {
    const SetOperationKind kind =
        branch_index < select.SetOperationKinds().size()
            ? select.SetOperationKinds()[branch_index]
            : SetOperationKind::kUnionAll;
    const SetOperationMatch match = branch_index < select.Matches().size()
                                        ? select.Matches()[branch_index]
                                        : SetOperationMatch{};
    ASSIGN_OR_RETURN(
        std::shared_ptr<SelectStatement>, hv19796_0,
        (BindSelect(*select.UnionAll()[branch_index], parameters, index)));
    result->AddSetOperation(kind, std::move(hv19796_0), match);
  }
  if (select.GetSetOperationTree() != nullptr) {
    auto tree = std::make_shared<SetOperationTree>();
    ASSIGN_OR_RETURN(
        std::shared_ptr<SelectStatement>, hv20027_0,
        (BindSelect(*select.GetSetOperationTree()->first, parameters, index)));
    tree->first = std::move(hv20027_0);
    tree->kinds = select.GetSetOperationTree()->kinds;
    tree->grouped = select.GetSetOperationTree()->grouped;
    tree->branches.reserve(select.GetSetOperationTree()->branches.size());
    for (const auto& branch : select.GetSetOperationTree()->branches) {
      ASSIGN_OR_RETURN(std::shared_ptr<SelectStatement>, hv20383_0,
                       (BindSelect(*branch, parameters, index)));
      tree->branches.push_back(std::move(hv20383_0));
    }
    result->SetSetOperationTree(std::move(tree));
  }
  if (select.UnionDistinct()) {
    result->MarkUnionDistinct(select.UnionByName());
  }
  for (auto& [name, query] : withs) {
    // WITH RECURSIVE markers and depth specs must survive a re-bind, or a
    // template-cache hit replays a recursive CTE as a plain one (the
    // executor's work-table route and the depth column both disappear).
    if (select.IsRecursiveWith(name)) {
      result->AddRecursiveWithQuery(name, std::move(query));
    } else {
      result->AddWithQuery(name, std::move(query));
    }
    if (const RecursiveDepthSpec* depth = select.RecursiveDepthOf(name)) {
      result->SetRecursiveDepth(name, *depth);
    }
  }
  if (select.RequiresRelationalEvaluation()) {
    result->MarkComplex();
  }
  return result;
}

// Returns the end offset (exclusive) of the "$$"/"$tag$" opener at pos, or
// pos when the token is not a dollar-quote opener.
size_t DollarQuoteDelimiterEnd(std::string_view sql, size_t pos) {
  if (pos + 1 >= sql.size()) {
    return pos;
  }
  if (sql[pos + 1] == '$') {
    return pos + 2;
  }
  const auto first = static_cast<unsigned char>(sql[pos + 1]);
  if (std::isalpha(first) == 0 && first != '_') {
    return pos;
  }
  size_t j = pos + 1;
  while (j < sql.size() && IsIdentChar(static_cast<unsigned char>(sql[j]))) {
    ++j;
  }
  if (j < sql.size() && sql[j] == '$') {
    return j + 1;
  }
  return pos;
}

// Copies a "..." or `...` identifier verbatim into the fingerprint, honouring
// doubled quote escapes.  Contents never reach the literal scanner, so an
// apostrophe inside an identifier cannot start a string parameter.
void AppendQuotedIdentifier(std::string_view sql, size_t& i,
                            std::string& fingerprint, char quote) {
  fingerprint.push_back(quote);
  ++i;
  while (i < sql.size()) {
    fingerprint.push_back(sql[i]);
    if (sql[i] == quote) {
      ++i;
      if (i < sql.size() && sql[i] == quote) {
        fingerprint.push_back(quote);
        ++i;
        continue;
      }
      return;
    }
    ++i;
  }
}

}  // namespace

// The scanner is a quote/comment aware state machine mirroring
// SplitSqlStatements: string literals, quoted identifiers, dollar-quoted
// strings and comments are consumed atomically so that quote characters
// inside them are never mistaken for literal delimiters (improvements2.md
// §7.5).
SqlTemplate ExtractSqlTemplate(std::string_view sql) {
  SqlTemplate result;
  result.fingerprint.reserve(sql.size());
  result.templatable = true;
  size_t i = 0;
  while (i < sql.size()) {
    const auto c = static_cast<unsigned char>(sql[i]);
    const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';
    if (c == '-' && next == '-') {
      while (i < sql.size() && sql[i] != '\n') {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (c == '/' && next == '*') {
      result.fingerprint += "/*";
      i += 2;
      while (i < sql.size()) {
        if (sql[i] == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
          result.fingerprint += "*/";
          i += 2;
          break;
        }
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (c == '\'') {
      const size_t quote_pos = i;
      ++i;
      std::string raw_literal;
      while (i < sql.size()) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          // Backslash escapes the next character inside a single-quoted
          // string (the parser decodes \' and the splitter swallows the
          // pair); consume both so a \' does not terminate this scan.
          raw_literal.push_back(sql[i]);
          raw_literal.push_back(sql[i + 1]);
          i += 2;
          continue;
        }
        if (sql[i] == '\'') {
          if (i + 1 < sql.size() && sql[i + 1] == '\'') {
            raw_literal.push_back('\'');
            i += 2;
            continue;
          }
          ++i;
          break;
        }
        raw_literal.push_back(sql[i]);
        ++i;
      }
      // Decode through the visitor's single escape table so the extracted
      // parameter equals the constant the first parse produced.  A b/B
      // prefix (up to two r/B letters, like the visitor's own
      // DecodeSingleComponent) marks a bytes literal: decoding it as text
      // would UTF-8-encode its high bytes, so a cache replay bound a
      // different constant than the first parse evaluated.
      bool is_raw_prefix = false;
      bool is_bytes_prefix = false;
      size_t prefix_end = quote_pos;
      for (int taken = 0; taken < 2 && 0 < prefix_end; ++taken) {
        const char p = sql[prefix_end - 1];
        if (p == 'b' || p == 'B') {
          is_bytes_prefix = true;
          --prefix_end;
        } else if (p == 'r' || p == 'R') {
          is_raw_prefix = true;
          --prefix_end;
        } else {
          break;
        }
      }
      if (0 < prefix_end) {
        const char before = sql[prefix_end - 1];
        if (std::isalnum(static_cast<unsigned char>(before)) != 0 ||
            before == '_') {
          // The letters belong to a longer identifier, not a literal prefix.
          is_bytes_prefix = false;
          is_raw_prefix = false;
        }
      }
      std::string literal =
          is_raw_prefix
              ? raw_literal
              : DecodeStringEscapes(raw_literal, is_bytes_prefix, false, '\'');
      // PRODUCTION FIX (Q3): a TIMESTAMP '...' literal is UTC-normalized by
      // the visitor on the first parse, but the template/plan caches replayed
      // the RAW string parameter on later runs, so the same SQL returned a
      // different value the second time.  Normalize at extraction so every
      // consumer observes the same value.  The normalization is idempotent,
      // so re-binding a cached constant stays correct.
      // `quote_pos` is the opening quote; walk back over whitespace to the end
      // of the preceding token, then inspect the 9-char keyword window.
      size_t kw_end = quote_pos;
      while (0 < kw_end &&
             std::isspace(static_cast<unsigned char>(sql[kw_end - 1])) != 0) {
        --kw_end;
      }
      // Case-insensitive: the visitor normalizes typed literals regardless
      // of keyword case (`timestamp '...'` included).
      bool is_timestamp_kw = 9 <= kw_end;
      if (is_timestamp_kw) {
        const std::string_view kw(sql.data() + kw_end - 9, 9);
        for (size_t k = 0; k < 9; ++k) {
          if (std::toupper(static_cast<unsigned char>(kw[k])) !=
              "TIMESTAMP"[k]) {
            is_timestamp_kw = false;
            break;
          }
        }
      }
      if (is_timestamp_kw &&
          (kw_end == 9 ||
           !IsIdentChar(static_cast<unsigned char>(sql[kw_end - 10])))) {
        literal = NormalizeTimestampText(literal);
      }
      result.parameters.emplace_back(std::move(literal));
      result.fingerprint += "'?'";
      continue;
    }
    if (c == '"') {
      AppendQuotedIdentifier(sql, i, result.fingerprint, '"');
      continue;
    }
    if (c == '`') {
      AppendQuotedIdentifier(sql, i, result.fingerprint, '`');
      continue;
    }
    if (c == '$') {
      const size_t delimiter_end = DollarQuoteDelimiterEnd(sql, i);
      if (delimiter_end > i + 1) {
        const std::string_view delimiter = sql.substr(i, delimiter_end - i);
        const size_t close = sql.find(delimiter, delimiter_end);
        const size_t content_end =
            close == std::string_view::npos ? sql.size() : close;
        result.parameters.emplace_back(std::string(
            sql.substr(delimiter_end, content_end - delimiter_end)));
        result.fingerprint += "'?'";
        i = close == std::string_view::npos ? sql.size()
                                            : close + delimiter.size();
        continue;
      }
    }
    if (KeywordAt(sql, i, "LIMIT") || KeywordAt(sql, i, "OFFSET") ||
        KeywordAt(sql, i, "INTERVAL")) {
      size_t keyword_size = 5;
      if (KeywordAt(sql, i, "INTERVAL")) {
        keyword_size = 8;
      } else if (KeywordAt(sql, i, "OFFSET")) {
        keyword_size = 6;
      }
      result.fingerprint.append(sql.substr(i, keyword_size));
      i += keyword_size;
      while (i < sql.size() &&
             std::isspace(static_cast<unsigned char>(sql[i])) != 0) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      while (i < sql.size() &&
             (std::isdigit(static_cast<unsigned char>(sql[i])) != 0 ||
              sql[i] == '.' || sql[i] == '-' || sql[i] == '+')) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (KeywordAt(sql, i, "ASSERT_ROWS_MODIFIED")) {
      // The row-count assert is statement metadata carried outside the
      // expression tree; parameterizing its literal would let two asserts
      // that differ only in the count collide on one fingerprint (and one
      // cached shape) while binding substitutes the wrong value.
      constexpr std::string_view kAssert = "ASSERT_ROWS_MODIFIED";
      result.fingerprint.append(kAssert);
      i += kAssert.size();
      while (i < sql.size() &&
             std::isspace(static_cast<unsigned char>(sql[i])) != 0) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      while (i < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[i])) != 0) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (KeywordAt(sql, i, "CREATE") || KeywordAt(sql, i, "DROP")) {
      result.templatable = false;
    }
    const bool number_start =
        std::isdigit(c) != 0 ||
        (sql[i] == '.' && i + 1 < sql.size() &&
         std::isdigit(static_cast<unsigned char>(sql[i + 1])) != 0);
    if (number_start &&
        (i == 0 || !IsIdentChar(static_cast<unsigned char>(sql[i - 1])))) {
      const size_t begin = i;
      bool is_float = false;
      while (i < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[i])) != 0) {
        ++i;
      }
      if (i < sql.size() && sql[i] == '.') {
        is_float = true;
        ++i;
        while (i < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i])) != 0) {
          ++i;
        }
      }
      if (i < sql.size() && (sql[i] == 'e' || sql[i] == 'E')) {
        is_float = true;
        ++i;
        if (i < sql.size() && (sql[i] == '+' || sql[i] == '-')) {
          ++i;
        }
        while (i < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i])) != 0) {
          ++i;
        }
      }
      const std::string token(sql.substr(begin, i - begin));
      if (is_float) {
        double parsed = 0;
        if (TryParseDouble(token, &parsed)) {
          result.parameters.emplace_back(parsed);
          result.fingerprint += "0.0";
        } else {
          // Unrepresentable magnitude (e.g. 1e999): keep the raw token in the
          // fingerprint so the statement stays its own cache entry and is
          // always parsed verbatim instead of bound from a template.
          result.templatable = false;
          result.fingerprint.append(token);
        }
      } else {
        int64_t parsed = 0;
        if (TryParseInt64(token, &parsed)) {
          result.parameters.emplace_back(parsed);
          result.fingerprint += "0";
        } else {
          // Out-of-int64-range integer: same treatment as float overflow.
          result.templatable = false;
          result.fingerprint.append(token);
        }
      }
      continue;
    }
    result.fingerprint.push_back(sql[i]);
    ++i;
  }
  if (result.parameters.empty()) {
    result.templatable = false;
  }
  return result;
}

StatusOr<std::unique_ptr<Statement>> BindStatementLiterals(
    const Statement& statement, const std::vector<Value>& parameters) {
  size_t index = 0;
  std::unique_ptr<Statement> bound;
  switch (statement.Type()) {
    case StatementType::kSelect: {
      ASSIGN_OR_RETURN(
          std::shared_ptr<SelectStatement>, select,
          (BindSelect(dynamic_cast<const SelectStatement&>(statement),
                      parameters, &index)));
      bound = std::make_unique<SelectStatement>(std::move(*select));
      break;
    }
    case StatementType::kInsert: {
      const auto& insert = dynamic_cast<const InsertStatement&>(statement);
      std::vector<std::vector<Expression>> rows;
      rows.reserve(insert.Values().size());
      for (const auto& row : insert.Values()) {
        std::vector<Expression> values;
        values.reserve(row.size());
        for (const Expression& value : row) {
          ASSIGN_OR_RETURN(Expression, hv33047_0,
                           (BindExpression(value, parameters, &index)));
          values.push_back(std::move(hv33047_0));
        }
        rows.push_back(std::move(values));
      }
      auto bound_insert = std::make_unique<InsertStatement>(
          insert.TableName(), std::move(rows), insert.Columns());
      // Preserve conflict-handling attributes across template rebinding;
      // dropping them would silently turn INSERT IGNORE/UPDATE/REPLACE and
      // ASSERT_ROWS_MODIFIED into plain inserts.
      bound_insert->SetMode(insert.Mode());
      bound_insert->SetAssertRowsModified(insert.AssertRowsModified());
      bound_insert->SetQuery(insert.Query());
      bound = std::move(bound_insert);
      break;
    }
    case StatementType::kUpdate: {
      const auto& update = dynamic_cast<const UpdateStatement&>(statement);
      std::vector<std::pair<ColumnName, Expression>> assignments;
      assignments.reserve(update.SetClause().size());
      for (const auto& assignment : update.SetClause()) {
        ASSIGN_OR_RETURN(
            Expression, hv34015_0,
            (BindExpression(assignment.second, parameters, &index)));
        assignments.emplace_back(assignment.first, std::move(hv34015_0));
      }
      ASSIGN_OR_RETURN(
          Expression, h_where,
          (BindExpression(update.WhereClause(), parameters, &index)));
      auto bound_update = std::make_unique<UpdateStatement>(
          update.TableName(), std::move(assignments), std::move(h_where));
      bound_update->SetAssertRowsModified(update.AssertRowsModified());
      // "UPDATE tbl AS alias" makes bare alias references denote the whole
      // row; dropping the alias changed name resolution on a cache replay.
      bound_update->SetAlias(update.Alias());
      // Nested per-row array DML carries expressions that may hold bound
      // constants; rebind them like the SET/WHERE expressions above.
      if (update.HasNestedDml()) {
        std::vector<NestedDmlItem> items;
        items.reserve(update.NestedItems().size());
        for (const NestedDmlItem& item : update.NestedItems()) {
          NestedDmlItem bound_item = item;
          if (bound_item.predicate) {
            ASSIGN_OR_RETURN(
                Expression, hv35028_0,
                (BindExpression(bound_item.predicate, parameters, &index)));
            bound_item.predicate = std::move(hv35028_0);
          }
          if (bound_item.set_value) {
            ASSIGN_OR_RETURN(
                Expression, hv35187_0,
                (BindExpression(bound_item.set_value, parameters, &index)));
            bound_item.set_value = std::move(hv35187_0);
          }
          for (auto& row : bound_item.insert_values) {
            for (Expression& value : row) {
              ASSIGN_OR_RETURN(Expression, hv35407_0,
                               (BindExpression(value, parameters, &index)));
              value = std::move(hv35407_0);
            }
          }
          if (bound_item.insert_query) {
            ASSIGN_OR_RETURN(
                std::shared_ptr<SelectStatement>, hv35539_0,
                (BindSelect(*bound_item.insert_query, parameters, &index)));
            bound_item.insert_query = std::move(hv35539_0);
          }
          items.push_back(std::move(bound_item));
        }
        bound_update->SetNestedItems(std::move(items));
      }
      bound = std::move(bound_update);
      break;
    }
    case StatementType::kDelete: {
      const auto& remove = dynamic_cast<const DeleteStatement&>(statement);
      ASSIGN_OR_RETURN(
          Expression, h_where,
          (BindExpression(remove.WhereClause(), parameters, &index)));
      auto bound_delete = std::make_unique<DeleteStatement>(remove.TableName(),
                                                            std::move(h_where));
      bound_delete->SetAssertRowsModified(remove.AssertRowsModified());
      // See the UPDATE branch: the whole-row alias must survive a re-bind.
      bound_delete->SetAlias(remove.Alias());
      bound = std::move(bound_delete);
      break;
    }
    case StatementType::kCreateTable:
    case StatementType::kDropTable:
    case StatementType::kAnalyze:
      return Status(Status::kInvalidArgument, "SQL template does not bind DDL");
  }
  if (index != parameters.size()) {
    return Status(Status::kInvalidArgument,
                  "SQL template parameter count mismatch");
  }
  return bound;
}

}  // namespace tinylamb
