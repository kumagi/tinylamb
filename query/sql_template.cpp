/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <exception>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
#include "type/value.hpp"
#include "type/value_type.hpp"
#include "type/type.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"

namespace tinylamb {
namespace {

bool IsIdentChar(unsigned char c) {
  return std::isalnum(c) != 0 || c == '_';
}

// std::stod/std::stoll throw out_of_range on tokens like "1e999" or 20-digit
// integers; template extraction must never leak that exception through
// SqlEngine::Prepare's StatusOr contract. Parse defensively instead.
bool TryParseDouble(const std::string& token, double* out) {
  try {
    size_t consumed = 0;
    const double value = std::stod(token, &consumed);
    if (consumed != token.size()) { return false;
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
    if (consumed != token.size()) { return false;
}
    *out = value;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool KeywordAt(std::string_view sql, size_t pos, std::string_view keyword) {
  if (pos + keyword.size() > sql.size()) { return false;
}
  for (size_t i = 0; i < keyword.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(sql[pos + i])) !=
        keyword[i]) {
      return false;
    }
  }
  const bool start_ok = pos == 0 || !IsIdentChar(sql[pos - 1]);
  const bool end_ok = pos + keyword.size() == sql.size() ||
                      !IsIdentChar(sql[pos + keyword.size()]);
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
  if (func == "__quantified__") { return index >= 2; }
  if (func == "__struct_json__") { return index % 3 != 1; }
  if (func == "make_interval" || func == "get_field") { return index == 1; }
  return false;
}

Expression BindExpression(const Expression& expression,
                          const std::vector<Value>& parameters, size_t* index);

NamedExpression BindNamed(const NamedExpression& item,  // NOLINT(misc-no-recursion) // Recursive expression-tree binding by design; trees are parser-bounded in depth.
                          const std::vector<Value>& parameters, size_t* index) {
  return {item.name, BindExpression(item.expression, parameters, index)};
}

std::shared_ptr<SelectStatement> BindSelect(
    const SelectStatement& select, const std::vector<Value>& parameters,
    size_t* index);

Expression BindExpression(const Expression& expression,  // NOLINT(misc-no-recursion) // Recursive expression-tree binding by design; trees are parser-bounded in depth.
                          const std::vector<Value>& parameters, size_t* index) {
  if (!expression) { return expression;
}
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      const Value current = expression->AsConstantValue().GetValue();
      if (SkipBindConstant(current)) { return expression;
}
      if (*index >= parameters.size()) {
        throw std::runtime_error("SQL template parameter underflow");
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
        throw std::runtime_error("SQL template literal type mismatch: date "
                                 "literal bound into a string slot");
      }
      return ConstantValueExp(parameter);
    }
    case TypeTag::kColumnValue:
      return ColumnValueExp(expression->AsColumnValue().GetColumnName());
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      Expression left = BindExpression(binary.Left(), parameters, index);
      Expression right = BindExpression(binary.Right(), parameters, index);
      return BinaryExpressionExp(std::move(left), binary.Op(), std::move(right));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(BindExpression(unary.Child(), parameters, index),
                                unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      return AggregateExpressionExp(
          aggregate.GetType(),
          BindExpression(aggregate.Child(), parameters, index),
          aggregate.Distinct());
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        Expression when = BindExpression(clause.first, parameters, index);
        Expression then = BindExpression(clause.second, parameters, index);
        clauses.emplace_back(std::move(when), std::move(then));
      }
      return CaseExpressionExp(
          std::move(clauses),
          BindExpression(searched.else_clause_, parameters, index));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      Expression child = BindExpression(in.child_, parameters, index);
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(BindExpression(item, parameters, index));
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
          args.push_back(BindExpression(call.Args()[i], parameters, index));
        }
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(BindExpression(element, parameters, index));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      // Text order inside "test IN (SELECT ...)": the tested expression comes
      // first, the subquery body afterwards.
      Expression test = BindExpression(query.Test(), parameters, index);
      auto subquery = BindSelect(*query.Query(), parameters, index);
      auto bound = std::make_shared<QueryExpression>(
          std::move(subquery), std::move(test), query.Exists(),
          query.Negated());
      // Preserve ARRAY(SELECT ...) semantics across template rebinding.
      bound->SetArrayResult(query.ArrayResult());
      return Expression(bound);
    }
    case TypeTag::kIntervalExp: {
      const auto& interval = expression->AsIntervalExpression();
      return IntervalExpressionExp(interval.Amount(), interval.Unit());
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      Expression child = BindExpression(cast.Child(), parameters, index);
      return std::make_shared<CastExpression>(std::move(child), cast.TargetTypeName(), cast.ReturnNullOnError());
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
// ExtractSqlTemplate emits them: WITH -> SELECT list -> FROM/JOIN conditions
// -> WHERE -> GROUP BY -> HAVING -> ORDER BY.  Binding clauses in any other
// order silently swaps parameters between clauses of same-fingerprint
// statements (improvements2.md §7.1).
bool ContainsBindableConstant(const Expression& expression);

bool SelectHasBindableConstant(const SelectStatement& select) {  // NOLINT(misc-no-recursion) // Recursive statement-tree scan by design; trees are parser-bounded in depth.
  for (const NamedExpression& item : select.SelectList()) {
    if (ContainsBindableConstant(item.expression)) { return true;
}
  }
  if (ContainsBindableConstant(select.WhereClause())) { return true;
}
  for (const Expression& key : select.GroupBy()) {
    if (ContainsBindableConstant(key)) { return true;
}
  }
  if (ContainsBindableConstant(select.Having())) { return true;
}
  for (const auto& term : select.OrderBy()) {
    if (ContainsBindableConstant(term.expression)) { return true;
}
  }
  for (const SelectSource& source : select.Sources()) {
    if (ContainsBindableConstant(source.join_condition)) { return true;
}
    if (source.query && SelectHasBindableConstant(*source.query)) {
      return true;
    }
  }
  return std::ranges::any_of(
      select.WithQueries(),
      [](const auto& entry) {  // NOLINT(misc-no-recursion) // Recursive statement-tree scan by design; trees are parser-bounded in depth.
        return SelectHasBindableConstant(*entry.second);
      });
}

bool ContainsBindableConstant(const Expression& expression) {  // NOLINT(misc-no-recursion) // Recursive expression-tree scan by design; trees are parser-bounded in depth.
  if (!expression) { return false;
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
      return ContainsBindableConstant(
          expression->AsUnaryExpression().Child());
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
      if (ContainsBindableConstant(in.child_)) { return true;
}
      return std::ranges::any_of(in.list_, ContainsBindableConstant);
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      for (size_t i = 0; i < call.Args().size(); ++i) {
        if (IsStructuralCallArg(call.FuncName(), i)) { continue;
}
        if (ContainsBindableConstant(call.Args()[i])) { return true;
}
      }
      return false;
    }
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        if (ContainsBindableConstant(element)) { return true;
}
      }
      return false;
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      return ContainsBindableConstant(query.Test()) ||
             SelectHasBindableConstant(*query.Query());
    }
    case TypeTag::kCastExp:
      return ContainsBindableConstant(
          expression->AsCastExpression().Child());
    default:
      return false;
  }
}

std::shared_ptr<SelectStatement> BindSelect(  // NOLINT(misc-no-recursion) // Recursive statement-tree binding by design; trees are parser-bounded in depth.
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
        throw std::runtime_error(
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
      auto bound =
          BindSelect(*select.WithQueries().at(name), parameters, index);
      withs.emplace_back(name, std::move(bound));
    }
  }

  std::vector<NamedExpression> items;
  items.reserve(select.SelectList().size());
  for (const NamedExpression& item : select.SelectList()) {
    items.push_back(BindNamed(item, parameters, index));
  }

  std::vector<SelectSource> sources;
  sources.reserve(select.Sources().size());
  for (const SelectSource& source : select.Sources()) {
    SelectSource copied = source;
    if (source.query) {
      copied.query = BindSelect(*source.query, parameters, index);
    }
    copied.join_condition =
        BindExpression(source.join_condition, parameters, index);
    sources.push_back(std::move(copied));
  }

  Expression where = BindExpression(select.WhereClause(), parameters, index);

  std::vector<Expression> group;
  group.reserve(select.GroupBy().size());
  for (const Expression& item : select.GroupBy()) {
    group.push_back(BindExpression(item, parameters, index));
  }

  Expression having;
  if (select.Having()) {
    having = BindExpression(select.Having(), parameters, index);
  }

  std::vector<SelectStatement::OrderByTerm> order;
  order.reserve(select.OrderBy().size());
  for (const auto& term : select.OrderBy()) {
    order.push_back({BindExpression(term.expression, parameters, index),
                     term.ascending, term.nulls_first});
  }

  auto result = std::make_shared<SelectStatement>(
      std::move(items), select.FromClause(), std::move(where), std::move(order),
      select.Limit(), select.Offset(), select.Distinct());
  for (const auto& [alias, table] : select.Aliases()) {
    result->AddAlias(alias, table);
  }
  result->SetSources(std::move(sources));
  if (!group.empty()) { result->SetGroupBy(std::move(group));
}
  if (having) { result->SetHaving(std::move(having));
}
  // UNION ALL branches appear after the main SELECT in SQL text, so bind
  // them last (after every main-select parameter) in branch order. Dropping
  // them silently shrank re-bound statements to their first branch.
  for (size_t branch_index = 0; branch_index < select.UnionAll().size();
       ++branch_index) {
    const SetOperationKind kind =
        branch_index < select.SetOperationKinds().size()
            ? select.SetOperationKinds()[branch_index]
            : SetOperationKind::kUnionAll;
    const SetOperationMatch match =
        branch_index < select.Matches().size() ? select.Matches()[branch_index]
                                               : SetOperationMatch{};
    result->AddSetOperation(kind,
                            BindSelect(*select.UnionAll()[branch_index],
                                       parameters, index),
                            match);
  }
  if (select.UnionDistinct()) {
    result->MarkUnionDistinct(select.UnionByName());
  }
  for (auto& [name, query] : withs) {
    result->AddWithQuery(name, std::move(query));
  }
  if (select.RequiresRelationalEvaluation()) { result->MarkComplex();
}
  return result;
}

// Returns the end offset (exclusive) of the "$$"/"$tag$" opener at pos, or
// pos when the token is not a dollar-quote opener.
size_t DollarQuoteDelimiterEnd(std::string_view sql, size_t pos) {
  if (pos + 1 >= sql.size()) { return pos;
}
  if (sql[pos + 1] == '$') { return pos + 2;
}
  const auto first = static_cast<unsigned char>(sql[pos + 1]);
  if (std::isalpha(first) == 0 && first != '_') { return pos;
}
  size_t j = pos + 1;
  while (j < sql.size() && IsIdentChar(sql[j])) { ++j;
}
  if (j < sql.size() && sql[j] == '$') { return j + 1;
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
      ++i;
      std::string literal;
      while (i < sql.size()) {
        if (sql[i] == '\'') {
          if (i + 1 < sql.size() && sql[i + 1] == '\'') {
            literal.push_back('\'');
            i += 2;
            continue;
          }
          ++i;
          break;
        }
        literal.push_back(sql[i]);
        ++i;
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
      while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i])) != 0) {
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
    if (number_start && (i == 0 || !IsIdentChar(sql[i - 1]))) {
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
        if (i < sql.size() && (sql[i] == '+' || sql[i] == '-')) { ++i;
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
  if (result.parameters.empty()) { result.templatable = false;
}
  return result;
}

std::unique_ptr<Statement> BindStatementLiterals(
    const Statement& statement, const std::vector<Value>& parameters) {
  size_t index = 0;
  std::unique_ptr<Statement> bound;
  switch (statement.Type()) {
    case StatementType::kSelect: {
      auto select = BindSelect(
          dynamic_cast<const SelectStatement&>(statement), parameters, &index);
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
          values.push_back(BindExpression(value, parameters, &index));
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
        assignments.emplace_back(
            assignment.first,
            BindExpression(assignment.second, parameters, &index));
      }
      auto bound_update = std::make_unique<UpdateStatement>(
          update.TableName(), std::move(assignments),
          BindExpression(update.WhereClause(), parameters, &index));
      bound_update->SetAssertRowsModified(update.AssertRowsModified());
      bound = std::move(bound_update);
      break;
    }
    case StatementType::kDelete: {
      const auto& remove = dynamic_cast<const DeleteStatement&>(statement);
      auto bound_delete = std::make_unique<DeleteStatement>(
          remove.TableName(),
          BindExpression(remove.WhereClause(), parameters, &index));
      bound_delete->SetAssertRowsModified(remove.AssertRowsModified());
      bound = std::move(bound_delete);
      break;
    }
    case StatementType::kCreateTable:
    case StatementType::kDropTable:
    case StatementType::kAnalyze:
      throw std::runtime_error("SQL template does not bind DDL");
  }
  if (index != parameters.size()) {
    throw std::runtime_error("SQL template parameter count mismatch");
  }
  return bound;
}

}  // namespace tinylamb
