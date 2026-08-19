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
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <vector>

#include "common/log_message.hpp"
#include "expression/constant_value.hpp"
#include "parser/ast.hpp"
#include "parser/pratt_parser.hpp"
#include "parser/token.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

Parser::Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

std::unique_ptr<Statement> Parser::Parse() {
  Token token = Peek();
  if (token.type == TokenType::kKeyword) {
    if (token.value == "CREATE") {
      return ParseCreateTable();
    } else if (token.value == "DROP") {
      return ParseDropTable();
    } else if (token.value == "INSERT") {
      return ParseInsert();
    } else if (token.value == "SELECT") {
      return ParseSelect();
    } else if (token.value == "UPDATE") {
      return ParseUpdate();
    } else if (token.value == "DELETE") {
      return ParseDelete();
    }
  }
  throw std::runtime_error("Unsupported statement");
}

std::unique_ptr<Statement> Parser::ParseInsert() {
  Advance();  // INSERT
  Advance();  // INTO
  std::string table_name = Advance().value;
  std::vector<std::string> columns;
  if (Peek().type == TokenType::kLParen) {
    Advance();
    while (Peek().type != TokenType::kRParen) {
      columns.push_back(Advance().value);
      if (Peek().type == TokenType::kComma) {
        Advance();
      }
    }
    Expect(TokenType::kRParen);
  }
  if (Advance().value != "VALUES") {
    throw std::runtime_error("expected VALUES");
  }
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

std::unique_ptr<Statement> Parser::ParseSelect() {
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
  Expect(TokenType::kKeyword);  // FROM
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
    const std::string table = Advance().value;
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
      if (Advance().value != "JOIN") {
        throw std::runtime_error("expected JOIN after INNER");
      }
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
    Expect(TokenType::kKeyword);  // WHERE
    Expression predicate = ParseWhereClause(&from_clause);
    where_clause = where_clause
                       ? BinaryExpressionExp(where_clause,
                                             BinaryOperation::kAnd, predicate)
                       : std::move(predicate);
  }

  std::vector<SelectStatement::OrderByTerm> order_by;
  if (Peek().type == TokenType::kKeyword && Peek().value == "ORDER") {
    Advance();
    if (Advance().value != "BY") {
      throw std::runtime_error("expected BY after ORDER");
    }
    for (;;) {
      Expression expression = ParseExpression();
      bool ascending = true;
      if (Peek().type == TokenType::kKeyword &&
          (Peek().value == "ASC" || Peek().value == "DESC")) {
        ascending = Advance().value == "ASC";
      }
      order_by.push_back({std::move(expression), ascending});
      if (Peek().type != TokenType::kComma) {
        break;
      }
      Advance();
    }
  }

  size_t limit = 0;
  size_t offset = 0;
  if (Peek().type == TokenType::kKeyword && Peek().value == "LIMIT") {
    Advance();
    limit = static_cast<size_t>(std::stoull(Advance().value));
  }
  if (Peek().type == TokenType::kKeyword && Peek().value == "OFFSET") {
    Advance();
    offset = static_cast<size_t>(std::stoull(Advance().value));
  }
  Expect(TokenType::kSemicolon);
  auto statement =
      std::make_unique<SelectStatement>(select_list, from_clause, where_clause,
                                        order_by, limit, offset, distinct);
  for (auto& [alias, table] : aliases) {
    statement->AddAlias(alias, table);
  }
  return statement;
}

std::unique_ptr<Statement> Parser::ParseUpdate() {
  Advance();  // UPDATE
  std::string table_name = Advance().value;
  if (Advance().value != "SET") {
    throw std::runtime_error("expected SET in UPDATE");
  }
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
  std::vector<Column> columns;
  while (Peek().type != TokenType::kRParen) {
    if (Peek().type == TokenType::kKeyword &&
        (Peek().value == "PRIMARY" || Peek().value == "UNIQUE")) {
      int constraint_depth = 0;
      while (!(constraint_depth == 0 && (Peek().type == TokenType::kComma ||
                                         Peek().type == TokenType::kRParen))) {
        Token token = Advance();
        if (token.type == TokenType::kLParen) ++constraint_depth;
        if (token.type == TokenType::kRParen) --constraint_depth;
      }
      if (Peek().type == TokenType::kComma) Advance();
      continue;
    }
    std::string column_name = Advance().value;
    std::string type_name = Advance().value;
    ValueType type;
    std::string upper_type_name;
    std::transform(type_name.begin(), type_name.end(),
                   std::back_inserter(upper_type_name), ::toupper);
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
        Advance();
        Expect(TokenType::kRParen);
      }
    } else if (upper_type_name == "DOUBLE" || upper_type_name == "FLOAT" ||
               upper_type_name == "FLOAT64" || upper_type_name == "NUMERIC" ||
               upper_type_name == "DECIMAL") {
      type = ValueType::kDouble;
      if (Peek().type == TokenType::kLParen) {
        while (Peek().type != TokenType::kRParen) Advance();
        Advance();
      }
    } else {
      throw std::runtime_error("Unsupported type");
    }
    columns.emplace_back(column_name, type);
    int constraint_depth = 0;
    while (!(constraint_depth == 0 && (Peek().type == TokenType::kComma ||
                                       Peek().type == TokenType::kRParen))) {
      Token token = Advance();
      if (token.type == TokenType::kLParen) ++constraint_depth;
      if (token.type == TokenType::kRParen) --constraint_depth;
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
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_];
}

Token Parser::Advance() {
  if (pos_ >= tokens_.size()) {
    return {TokenType::kEof, ""};
  }
  return tokens_[pos_++];
}

void Parser::Expect(TokenType type) {
  Token token = Advance();
  if (token.type != type) {
    throw std::runtime_error("Unexpected token");
  }
}

Expression Parser::ParseExpression() {
  PrattParser pratt_parser(tokens_.begin() + pos_, tokens_.end());
  Expression expr = pratt_parser.ParseExpression(0);
  pos_ += pratt_parser.GetPos();  // Advance Parser's position
  return expr;
}

Expression Parser::ParseWhereClause(std::vector<std::string>* from_clause) {
  size_t clause_end = pos_;
  int depth = 0;
  for (; clause_end < tokens_.size(); ++clause_end) {
    const Token& token = tokens_[clause_end];
    if (token.type == TokenType::kLParen) ++depth;
    if (token.type == TokenType::kRParen) --depth;
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
    if (tokens_[i].type == TokenType::kLParen) ++depth;
    if (tokens_[i].type == TokenType::kRParen) --depth;
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
      if (tokens_[i].type == TokenType::kLParen) ++depth;
      if (tokens_[i].type == TokenType::kRParen) --depth;
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
          !nested_select.OrderBy().empty() || nested_select.Limit() != 0) {
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
