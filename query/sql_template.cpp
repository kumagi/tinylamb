/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_template.hpp"

#include <cctype>
#include <stdexcept>
#include <utility>

#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/unary_expression.hpp"
#include "parser/ast.hpp"

namespace tinylamb {
namespace {

bool IsIdentChar(unsigned char c) { return std::isalnum(c) || c == '_'; }

bool KeywordAt(std::string_view sql, size_t pos, std::string_view keyword) {
  if (pos + keyword.size() > sql.size()) return false;
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

Expression BindExpression(const Expression& expression,
                          const std::vector<Value>& parameters, size_t* index);

NamedExpression BindNamed(const NamedExpression& item,
                          const std::vector<Value>& parameters, size_t* index) {
  return NamedExpression(item.name, BindExpression(item.expression, parameters,
                                                   index));
}

std::shared_ptr<SelectStatement> BindSelect(
    const SelectStatement& select, const std::vector<Value>& parameters,
    size_t* index);

Expression BindExpression(const Expression& expression,
                          const std::vector<Value>& parameters, size_t* index) {
  if (!expression) return expression;
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      const Value current = expression->AsConstantValue().GetValue();
      if (SkipBindConstant(current)) return expression;
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
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (const Expression& arg : call.Args()) {
        args.push_back(BindExpression(arg, parameters, index));
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kQueryExp: {
      const auto& query = expression->AsQueryExpression();
      auto subquery = BindSelect(*query.Query(), parameters, index);
      Expression test = BindExpression(query.Test(), parameters, index);
      return QueryExpressionExp(std::move(subquery), std::move(test),
                                query.Exists(), query.Negated());
    }
    case TypeTag::kIntervalExp: {
      const auto& interval = expression->AsIntervalExpression();
      return IntervalExpressionExp(interval.Amount(), interval.Unit());
    }
    default:
      return expression;
  }
}

std::shared_ptr<SelectStatement> BindSelect(
    const SelectStatement& select, const std::vector<Value>& parameters,
    size_t* index) {
  std::vector<NamedExpression> items;
  items.reserve(select.SelectList().size());
  for (const NamedExpression& item : select.SelectList()) {
    items.push_back(BindNamed(item, parameters, index));
  }
  std::vector<SelectStatement::OrderByTerm> order;
  order.reserve(select.OrderBy().size());
  for (const auto& term : select.OrderBy()) {
    order.push_back({BindExpression(term.expression, parameters, index),
                     term.ascending});
  }
  auto result = std::make_shared<SelectStatement>(
      std::move(items), select.FromClause(),
      BindExpression(select.WhereClause(), parameters, index), std::move(order),
      select.Limit(), select.Offset(), select.Distinct());
  for (const auto& [alias, table] : select.Aliases()) {
    result->AddAlias(alias, table);
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
  result->SetSources(std::move(sources));
  if (!select.GroupBy().empty()) {
    std::vector<Expression> group;
    group.reserve(select.GroupBy().size());
    for (const Expression& item : select.GroupBy()) {
      group.push_back(BindExpression(item, parameters, index));
    }
    result->SetGroupBy(std::move(group));
  }
  if (select.Having()) {
    result->SetHaving(BindExpression(select.Having(), parameters, index));
  }
  for (const auto& [name, query] : select.WithQueries()) {
    result->AddWithQuery(name, BindSelect(*query, parameters, index));
  }
  if (select.RequiresRelationalEvaluation()) result->MarkComplex();
  return result;
}

}  // namespace

SqlTemplate ExtractSqlTemplate(std::string_view sql) {
  SqlTemplate result;
  result.fingerprint.reserve(sql.size());
  result.templatable = true;
  size_t i = 0;
  while (i < sql.size()) {
    const unsigned char c = static_cast<unsigned char>(sql[i]);
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
    if (sql[i] == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
      while (i < sql.size() && sql[i] != '\n') {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (sql[i] == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
      result.fingerprint += "/*";
      i += 2;
      while (i + 1 < sql.size() && !(sql[i] == '*' && sql[i + 1] == '/')) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (KeywordAt(sql, i, "LIMIT") || KeywordAt(sql, i, "OFFSET") ||
        KeywordAt(sql, i, "INTERVAL")) {
      const size_t keyword_size = KeywordAt(sql, i, "INTERVAL") ? 8
                                  : KeywordAt(sql, i, "OFFSET")  ? 6
                                                                : 5;
      result.fingerprint.append(sql.substr(i, keyword_size));
      i += keyword_size;
      while (i < sql.size() &&
             std::isspace(static_cast<unsigned char>(sql[i]))) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      while (i < sql.size() &&
             (std::isdigit(static_cast<unsigned char>(sql[i])) ||
              sql[i] == '.' || sql[i] == '-' || sql[i] == '+')) {
        result.fingerprint.push_back(sql[i]);
        ++i;
      }
      continue;
    }
    if (KeywordAt(sql, i, "CREATE") || KeywordAt(sql, i, "DROP")) {
      result.templatable = false;
    }
    const bool number_start =
        std::isdigit(c) ||
        (sql[i] == '.' && i + 1 < sql.size() &&
         std::isdigit(static_cast<unsigned char>(sql[i + 1])));
    if (number_start && (i == 0 || !IsIdentChar(sql[i - 1]))) {
      const size_t begin = i;
      bool is_float = false;
      while (i < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[i]))) {
        ++i;
      }
      if (i < sql.size() && sql[i] == '.') {
        is_float = true;
        ++i;
        while (i < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i]))) {
          ++i;
        }
      }
      if (i < sql.size() && (sql[i] == 'e' || sql[i] == 'E')) {
        is_float = true;
        ++i;
        if (i < sql.size() && (sql[i] == '+' || sql[i] == '-')) ++i;
        while (i < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[i]))) {
          ++i;
        }
      }
      const std::string token(sql.substr(begin, i - begin));
      if (is_float) {
        result.parameters.emplace_back(std::stod(token));
        result.fingerprint += "0.0";
      } else {
        result.parameters.emplace_back(static_cast<int64_t>(std::stoll(token)));
        result.fingerprint += "0";
      }
      continue;
    }
    result.fingerprint.push_back(sql[i]);
    ++i;
  }
  if (result.parameters.empty()) result.templatable = false;
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
      bound = std::make_unique<InsertStatement>(insert.TableName(),
                                                std::move(rows),
                                                insert.Columns());
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
      bound = std::make_unique<UpdateStatement>(
          update.TableName(), std::move(assignments),
          BindExpression(update.WhereClause(), parameters, &index));
      break;
    }
    case StatementType::kDelete: {
      const auto& remove = dynamic_cast<const DeleteStatement&>(statement);
      bound = std::make_unique<DeleteStatement>(
          remove.TableName(),
          BindExpression(remove.WhereClause(), parameters, &index));
      break;
    }
    case StatementType::kCreateTable:
    case StatementType::kDropTable:
      throw std::runtime_error("SQL template does not bind DDL");
  }
  if (index != parameters.size()) {
    throw std::runtime_error("SQL template parameter count mismatch");
  }
  return bound;
}

}  // namespace tinylamb
