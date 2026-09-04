/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "parser/parser.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "parser/pratt_parser.hpp"
#include "parser/token.hpp"
#include "query/statement.hpp"
#include "type/column_name.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Parses a non-negative integer literal for LIMIT/OFFSET, keeping the
// runtime_error-only API contract of the parser.
size_t ParseCountLiteral(const std::string& text) {
  if (text.find('.') != std::string::npos) {
    throw std::runtime_error("expected integer literal: " + text);
  }
  try {
    const unsigned long long value = std::stoull(text);
    if (value > std::numeric_limits<size_t>::max()) {
      throw std::out_of_range(text);
    }
    return static_cast<size_t>(value);
  } catch (const std::invalid_argument&) {
    throw std::runtime_error("invalid numeric literal: " + text);
  } catch (const std::out_of_range&) {
    throw std::runtime_error("numeric literal out of range: " + text);
  }
}

}  // namespace

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}
Parser::Parser(std::vector<Token>&& tokens) : tokens_(std::move(tokens)) {}

std::unique_ptr<Statement> Parser::Parse() {  // NOLINT(misc-no-recursion) // IN (SELECT ...) subqueries recurse into Parser::Parse by design.
  Token token = Peek();
  if (token.type == TokenType::kKeyword) {
    if (token.value == "CREATE") {
      return ParseCreateTable();
    }
    if (token.value == "DROP") {
      return ParseDropTable();
    }
    if (token.value == "INSERT") {
      return ParseInsert();
    }
    if (token.value == "SELECT") {
      return ParseSelect();
    }
    if (token.value == "UPDATE") {
      return ParseUpdate();
    }
    if (token.value == "DELETE") {
      return ParseDelete();
    }
  }
  throw std::runtime_error("Unsupported statement: " +
                           (token.type == TokenType::kEof
                                ? std::string("<eof>")
                                : token.ToString()));
}

std::unique_ptr<Statement> Parser::ParseInsert() {
  Advance();  // INSERT
  Advance();  // INTO
  std::string table_name = Advance().value;
  std::vector<std::string> columns;
  if (Peek().type == TokenType::kLParen) {
    Advance();
    for (;;) {
      const Token column = Advance();
      if (column.type != TokenType::kIdentifier) {
        throw std::runtime_error("expected column name in INSERT column list");
      }
      columns.push_back(column.value);
      if (Peek().type == TokenType::kComma) {
        Advance();
        continue;
      }
      break;
    }
    Expect(TokenType::kRParen);
  }
  ExpectKeyword("VALUES");
  Expect(TokenType::kLParen);
  std::vector<std::vector<Expression>> values;
  while (Peek().type != TokenType::kSemicolon) {
    std::vector<Expression> row;
    while (Peek().type != TokenType::kRParen) {
      row.push_back(ParseExpression());
      if (Peek().type == TokenType::kComma) {
        Advance();
      }
    }
    values.push_back(row);
    Expect(TokenType::kRParen);
    if (Peek().type == TokenType::kComma) {
      Advance();
      Expect(TokenType::kLParen);
    }
  }
  Expect(TokenType::kSemicolon);
  return std::make_unique<InsertStatement>(table_name, values, columns);
}

std::unique_ptr<Statement> Parser::ParseSelect() {  // NOLINT(misc-no-recursion) // Recursion only via IN (SELECT ...) subquery parsing; SQL grammar is recursive by design.
  Advance();  // SELECT
  bool distinct = false;
  if (Peek().type == TokenType::kKeyword && Peek().value == "DISTINCT") {
    Advance();
    distinct = true;
  }
  std::vector<NamedExpression> select_list;
  if (Peek().type == TokenType::kOperator && Peek().value == "*") {
    Advance();
    select_list.emplace_back("", ColumnValueExp("*"));
  } else {
    while (Peek().type != TokenType::kKeyword || Peek().value != "FROM") {
      Expression expression = ParseExpression();
      std::string alias;
      if (Peek().type == TokenType::kKeyword && Peek().value == "AS") {
        Advance();
        alias = Advance().value;
      }
      select_list.emplace_back(alias, std::move(expression));
      if (Peek().type == TokenType::kComma) {
        Advance();
      } else if (Peek().type != TokenType::kKeyword || Peek().value != "FROM") {
        throw std::runtime_error("expected ',' or FROM in SELECT list");
      }
    }
  }
  ExpectKeyword("FROM");
  std::vector<std::string> from_clause;
  std::unordered_map<std::string, std::string> aliases;
  const std::string first_table = Advance().value;
  from_clause.push_back(first_table);
  if (Peek().type == TokenType::kKeyword && Peek().value == "AS") {
    Advance();
    aliases.emplace(Advance().value, first_table);
  } else if (Peek().type == TokenType::kIdentifier) {
    aliases.emplace(Advance().value, first_table);
  }

  while (Peek().type == TokenType::kComma) {
    Advance();
    const Token table_token = Advance();
    if (table_token.type != TokenType::kIdentifier) {
      throw std::runtime_error("expected table name after ','");
    }
    const std::string table = table_token.value;
    from_clause.push_back(table);
    if (Peek().type == TokenType::kKeyword && Peek().value == "AS") {
      Advance();
      aliases.emplace(Advance().value, table);
    } else if (Peek().type == TokenType::kIdentifier) {
      aliases.emplace(Advance().value, table);
    }
  }

  Expression where_clause = nullptr;
  while (Peek().type == TokenType::kKeyword &&
         (Peek().value == "JOIN" || Peek().value == "INNER")) {
    if (Peek().value == "INNER") {
      Advance();
      ExpectKeyword("JOIN");
    } else {
      Advance();
    }
    const std::string table = Advance().value;
    from_clause.push_back(table);
    if (Peek().type == TokenType::kKeyword && Peek().value == "AS") {
      Advance();
      aliases.emplace(Advance().value, table);
    } else if (Peek().type == TokenType::kIdentifier) {
      aliases.emplace(Advance().value, table);
    }
    if (Advance().value != "ON") {
      throw std::runtime_error("expected ON after JOIN target");
    }
    Expression join_condition = ParseExpression();
    where_clause =
        where_clause ? BinaryExpressionExp(where_clause, BinaryOperation::kAnd,
                                           join_condition)
                     : std::move(join_condition);
  }

  if (Peek().type == TokenType::kKeyword && Peek().value == "WHERE") {
    ExpectKeyword("WHERE");
    Expression predicate = ParseWhereClause(&from_clause);
    where_clause = where_clause
                       ? BinaryExpressionExp(where_clause,
                                             BinaryOperation::kAnd, predicate)
                       : std::move(predicate);
  }

  if (Peek().type == TokenType::kKeyword &&
      (Peek().value == "GROUP" || Peek().value == "HAVING")) {
    // ParseWhereClause stops at GROUP/HAVING but nothing consumes them;
    // fail early with a clear message instead of a cryptic token error.
    throw std::runtime_error("GROUP BY/HAVING clauses are not supported");
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (Peek().type == TokenType::kKeyword && Peek().value == "ORDER") {
    Advance();
    ExpectKeyword("BY");
    for (;;) {
      Expression expression = ParseExpression();
      bool ascending = true;
      if (Peek().type == TokenType::kKeyword &&
          (Peek().value == "ASC" || Peek().value == "DESC")) {
        ascending = Advance().value == "ASC";
      }
      order_by.push_back({std::move(expression), ascending, std::nullopt});
      if (Peek().type != TokenType::kComma) {
        break;
      }
      Advance();
    }
  }

  std::optional<size_t> limit;
  size_t offset = 0;
  if (Peek().type == TokenType::kKeyword && Peek().value == "LIMIT") {
    Advance();
    limit = ParseCountLiteral(Advance().value);
  }
  if (Peek().type == TokenType::kKeyword && Peek().value == "OFFSET") {
    Advance();
    offset = ParseCountLiteral(Advance().value);
  }
  Expect(TokenType::kSemicolon);
  auto statement =
      std::make_unique<SelectStatement>(select_list, from_clause, where_clause,
                                        order_by, limit.value_or(0), offset,
                                        distinct);
  statement->SetLimit(limit);
  for (auto& [alias, table] : aliases) {
    statement->AddAlias(alias, table);
  }
  return statement;
}

std::unique_ptr<Statement> Parser::ParseUpdate() {
  Advance();  // UPDATE
  std::string table_name = Advance().value;
  ExpectKeyword("SET");
  std::vector<std::pair<ColumnName, Expression>> assignments;
  for (;;) {
    std::string column = Advance().value;
    Token equals = Advance();
    if (equals.type != TokenType::kOperator || equals.value != "=") {
      throw std::runtime_error("expected '=' in UPDATE assignment");
    }
    assignments.emplace_back(ColumnName(column), ParseExpression());
    if (Peek().type != TokenType::kComma) {
      break;
    }
    Advance();
  }
  Expression predicate;
  if (Peek().type == TokenType::kKeyword && Peek().value == "WHERE") {
    Advance();
    predicate = ParseExpression();
  }
  Expect(TokenType::kSemicolon);
  return std::make_unique<UpdateStatement>(table_name, std::move(assignments),
                                           std::move(predicate));
}

std::unique_ptr<Statement> Parser::ParseDelete() {
  Advance();  // DELETE
  if (Peek().type == TokenType::kKeyword && Peek().value == "FROM") {
    Advance();
  }
  std::string table_name = Advance().value;
  Expression predicate;
  if (Peek().type == TokenType::kKeyword && Peek().value == "WHERE") {
    Advance();
    predicate = ParseExpression();
  }
  Expect(TokenType::kSemicolon);
  return std::make_unique<DeleteStatement>(table_name, std::move(predicate));
}

std::unique_ptr<Statement> Parser::ParseDropTable() {
  Advance();  // DROP
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kSemicolon);
  return std::make_unique<DropTableStatement>(table_name);
}

std::unique_ptr<Statement> Parser::ParseCreateTable() {
  Advance();  // CREATE
  Advance();  // TABLE
  std::string table_name = Advance().value;
  Expect(TokenType::kLParen);
  if (Peek().type == TokenType::kRParen) {
    throw std::runtime_error("CREATE TABLE requires at least one column");
  }
  std::vector<Column> columns;
  while (Peek().type != TokenType::kRParen) {
    if (Peek().type == TokenType::kKeyword &&
        (Peek().value == "PRIMARY" || Peek().value == "UNIQUE")) {
      int constraint_depth = 0;
      while (constraint_depth != 0 ||
             (Peek().type != TokenType::kComma &&
              Peek().type != TokenType::kRParen &&
              Peek().type != TokenType::kEof)) {
        Token token = Advance();
        if (token.type == TokenType::kLParen) { ++constraint_depth;
}
        if (token.type == TokenType::kRParen) { --constraint_depth;
}
      }
      if (Peek().type == TokenType::kComma) { Advance();
}
      continue;
    }
    std::string column_name = Advance().value;
    std::string type_name = Advance().value;
    ValueType type = ValueType::kNull;
    std::string upper_type_name;
    std::ranges::transform(type_name, std::back_inserter(upper_type_name),
                           [](unsigned char c) {
                             return static_cast<char>(std::toupper(c));
                           });
    if (upper_type_name == "INT" || upper_type_name == "INT64" ||
        upper_type_name == "INTEGER" || upper_type_name == "BIGINT" ||
        upper_type_name == "BOOL" || upper_type_name == "BOOLEAN") {
      type = ValueType::kInt64;
    } else if (upper_type_name == "DATE") {
      type = ValueType::kDate;
    } else if (upper_type_name == "VARCHAR" || upper_type_name == "CHAR" ||
               upper_type_name == "STRING" || upper_type_name == "TIMESTAMP" ||
               upper_type_name == "DATETIME") {
      type = ValueType::kVarChar;
      if (Peek().type == TokenType::kLParen) {
        Advance();
        const Token length = Advance();
        if (length.type != TokenType::kNumeric) {
          throw std::runtime_error("expected numeric length in VARCHAR(n)");
        }
        Expect(TokenType::kRParen);
      }
    } else if (upper_type_name == "DOUBLE" || upper_type_name == "FLOAT" ||
               upper_type_name == "FLOAT64" || upper_type_name == "NUMERIC" ||
               upper_type_name == "DECIMAL") {
      type = ValueType::kDouble;
      if (Peek().type == TokenType::kLParen) {
        Advance();
        const Token precision = Advance();
        if (precision.type != TokenType::kNumeric) {
          throw std::runtime_error("expected numeric precision in NUMERIC(p)");
        }
        if (Peek().type == TokenType::kComma) {
          Advance();
          const Token scale = Advance();
          if (scale.type != TokenType::kNumeric) {
            throw std::runtime_error("expected numeric scale in NUMERIC(p, s)");
          }
        }
        Expect(TokenType::kRParen);
      }
    } else {
      throw std::runtime_error("Unsupported type");
    }
    columns.emplace_back(column_name, type);
    int constraint_depth = 0;
    while (constraint_depth != 0 ||
           (Peek().type != TokenType::kComma &&
            Peek().type != TokenType::kRParen &&
            Peek().type != TokenType::kEof)) {
      Token token = Advance();
      if (token.type == TokenType::kLParen) { ++constraint_depth;
}
      if (token.type == TokenType::kRParen) { --constraint_depth;
}
    }
    if (Peek().type == TokenType::kComma) {
      Advance();
    }
  }
  Expect(TokenType::kRParen);
  Expect(TokenType::kSemicolon);
  return std::make_unique<CreateTableStatement>(table_name, columns);
}

Token Parser::Peek() {
  if (pos_ >= tokens_.size()) {
    return {.type=TokenType::kEof, .value=""};
  }
  return tokens_[pos_];
}

Token Parser::Advance() {
  if (pos_ >= tokens_.size()) {
    throw std::runtime_error("Unexpected end of input");
  }
  return tokens_[pos_++];
}

void Parser::Expect(TokenType type) {
  Token token = Advance();
  if (token.type != type) {
    throw std::runtime_error("Unexpected token");
  }
}

void Parser::ExpectKeyword(const std::string& keyword) {
  const Token token = Advance();
  if (token.type != TokenType::kKeyword || token.value != keyword) {
    throw std::runtime_error("expected keyword " + keyword + " but got " +
                             token.ToString());
  }
}

Expression Parser::ParseExpression() {
  PrattParser pratt_parser(tokens_.begin() + pos_, tokens_.end());
  Expression expr = pratt_parser.ParseExpression(0);
  pos_ += pratt_parser.GetPos();  // Advance Parser's position
  return expr;
}

Expression Parser::ParseWhereClause(std::vector<std::string>* from_clause) {  // NOLINT(misc-no-recursion) // Recursion only via nested IN (SELECT ...) subquery; bounded by SQL nesting.
  size_t clause_end = pos_;
  int depth = 0;
  for (; clause_end < tokens_.size(); ++clause_end) {
    const Token& token = tokens_[clause_end];
    if (token.type == TokenType::kLParen) { ++depth;
}
    if (token.type == TokenType::kRParen) { --depth;
}
    if (depth == 0 && (token.type == TokenType::kSemicolon ||
                       (token.type == TokenType::kKeyword &&
                        (token.value == "ORDER" || token.value == "LIMIT" ||
                         token.value == "OFFSET" || token.value == "GROUP" ||
                         token.value == "HAVING")))) {
      break;
    }
  }

  std::vector<std::pair<size_t, size_t>> terms;
  size_t term_begin = pos_;
  depth = 0;
  for (size_t i = pos_; i < clause_end; ++i) {
    if (tokens_[i].type == TokenType::kLParen) { ++depth;
}
    if (tokens_[i].type == TokenType::kRParen) { --depth;
}
    if (depth == 0 && tokens_[i].type == TokenType::kKeyword &&
        tokens_[i].value == "AND") {
      terms.emplace_back(term_begin, i);
      term_begin = i + 1;
    }
  }
  terms.emplace_back(term_begin, clause_end);

  Expression result;
  for (const auto& [begin, end] : terms) {
    size_t in_position = end;
    depth = 0;
    for (size_t i = begin; i < end; ++i) {
      if (tokens_[i].type == TokenType::kLParen) { ++depth;
}
      if (tokens_[i].type == TokenType::kRParen) { --depth;
}
      if (depth == 0 && tokens_[i].type == TokenType::kKeyword &&
          tokens_[i].value == "IN" && i + 2 < end &&
          tokens_[i + 1].type == TokenType::kLParen &&
          tokens_[i + 2].type == TokenType::kKeyword &&
          tokens_[i + 2].value == "SELECT") {
        in_position = i;
        break;
      }
    }

    Expression term;
    if (in_position == end) {
      PrattParser parser(tokens_.begin() + static_cast<ptrdiff_t>(begin),
                         tokens_.begin() + static_cast<ptrdiff_t>(end));
      term = parser.ParseExpression(0);
      if (parser.GetPos() != end - begin) {
        throw std::runtime_error("unsupported WHERE expression");
      }
    } else {
      PrattParser left_parser(
          tokens_.begin() + static_cast<ptrdiff_t>(begin),
          tokens_.begin() + static_cast<ptrdiff_t>(in_position));
      Expression left = left_parser.ParseExpression(0);
      if (tokens_[end - 1].type != TokenType::kRParen) {
        throw std::runtime_error("unterminated IN subquery");
      }
      std::vector<Token> nested(
          tokens_.begin() + static_cast<ptrdiff_t>(in_position + 2),
          tokens_.begin() + static_cast<ptrdiff_t>(end - 1));
      nested.push_back({TokenType::kSemicolon, ";"});
      nested.push_back({TokenType::kEof, ""});
      Parser nested_parser(nested);
      std::unique_ptr<Statement> nested_statement = nested_parser.Parse();
      if (nested_statement->Type() != StatementType::kSelect) {
        throw std::runtime_error("IN requires a SELECT subquery");
      }
      auto& nested_select = dynamic_cast<SelectStatement&>(*nested_statement);
      if (nested_select.SelectList().size() != 1 || nested_select.Distinct() ||
          !nested_select.OrderBy().empty() || nested_select.HasLimit()) {
        throw std::runtime_error("unsupported IN subquery shape");
      }
      from_clause->insert(from_clause->end(),
                          nested_select.FromClause().begin(),
                          nested_select.FromClause().end());
      term = BinaryExpressionExp(left, BinaryOperation::kEquals,
                                 nested_select.SelectList()[0].expression);
      if (nested_select.WhereClause()) {
        term = BinaryExpressionExp(term, BinaryOperation::kAnd,
                                   nested_select.WhereClause());
      }
    }
    result = result ? BinaryExpressionExp(result, BinaryOperation::kAnd, term)
                    : std::move(term);
  }
  pos_ = clause_end;
  return result;
}

}  // namespace tinylamb
