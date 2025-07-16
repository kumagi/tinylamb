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

#ifndef TINYLAMB_AST_HPP
#define TINYLAMB_AST_HPP

#include <string>
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/column.hpp"

namespace tinylamb {

enum class StatementType {
  kCreateTable,
  kDropTable,
  kSelect,
  kInsert,
  kUpdate,
  kDelete,
};

class Statement {
 public:
  explicit Statement(StatementType type) : type_(type) {}
  virtual ~Statement() = default;
  StatementType Type() const { return type_; }

 private:
  StatementType type_;
};

class CreateTableStatement : public Statement {
 public:
  CreateTableStatement(std::string table_name, std::vector<Column> columns)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<Column>& Columns() const { return columns_; }

 private:
  std::string table_name_;
  std::vector<Column> columns_;
};

class DropTableStatement : public Statement {
 public:
  explicit DropTableStatement(std::string table_name)
      : Statement(StatementType::kDropTable),
        table_name_(std::move(table_name)) {}

  const std::string& TableName() const { return table_name_; }

 private:
  std::string table_name_;
};

class SelectStatement : public Statement {
 public:
  SelectStatement(std::vector<NamedExpression> select_list,
                  std::vector<std::string> from_clause, Expression where_clause)
      : Statement(StatementType::kSelect),
        select_list_(std::move(select_list)),
        from_clause_(std::move(from_clause)),
        where_clause_(std::move(where_clause)) {}

  const std::vector<NamedExpression>& SelectList() const {
    return select_list_;
  }
  const std::vector<std::string>& FromClause() const { return from_clause_; }
  const Expression& WhereClause() const { return where_clause_; }

 private:
  std::vector<NamedExpression> select_list_;
  std::vector<std::string> from_clause_;
  Expression where_clause_;
};

class InsertStatement : public Statement {
 public:
  InsertStatement(std::string table_name,
                  std::vector<std::vector<Expression>> values)
      : Statement(StatementType::kInsert),
        table_name_(std::move(table_name)),
        values_(std::move(values)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<std::vector<Expression>>& Values() const { return values_; }

 private:
  std::string table_name_;
  std::vector<std::vector<Expression>> values_;
};

class UpdateStatement : public Statement {
 public:
  UpdateStatement(std::string table_name,
                  std::vector<std::pair<ColumnName, Expression>> set_clause,
                  Expression where_clause)
      : Statement(StatementType::kUpdate),
        table_name_(std::move(table_name)),
        set_clause_(std::move(set_clause)),
        where_clause_(std::move(where_clause)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<std::pair<ColumnName, Expression>>& SetClause() const {
    return set_clause_;
  }
  const Expression& WhereClause() const { return where_clause_; }

 private:
  std::string table_name_;
  std::vector<std::pair<ColumnName, Expression>> set_clause_;
  Expression where_clause_;
};

class DeleteStatement : public Statement {
 public:
  DeleteStatement(std::string table_name, Expression where_clause)
      : Statement(StatementType::kDelete),
        table_name_(std::move(table_name)),
        where_clause_(std::move(where_clause)) {}

  const std::string& TableName() const { return table_name_; }
  const Expression& WhereClause() const { return where_clause_; }

 private:
  std::string table_name_;
  Expression where_clause_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_AST_HPP
