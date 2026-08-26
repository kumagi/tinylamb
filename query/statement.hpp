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

#ifndef TINYLAMB_QUERY_STATEMENT_HPP
#define TINYLAMB_QUERY_STATEMENT_HPP

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/column.hpp"

namespace tinylamb {

class SelectStatement;

enum class JoinType { kCross, kInner, kLeft };

struct SelectSource {
  SelectSource() = default;
  SelectSource(std::string in_table, std::string in_alias,
               std::shared_ptr<SelectStatement> in_query = nullptr,
               JoinType in_join_type = JoinType::kCross,
               Expression in_join_condition = nullptr,
               Expression in_unnest = nullptr)
      : table(std::move(in_table)),
        alias(std::move(in_alias)),
        query(std::move(in_query)),
        join_type(in_join_type),
        join_condition(std::move(in_join_condition)),
        unnest(std::move(in_unnest)) {}

  std::string table;

  std::string alias;
  std::shared_ptr<SelectStatement> query;
  JoinType join_type{JoinType::kCross};
  Expression join_condition;
  Expression unnest;
  std::string offset_alias;
};




enum class StatementType {
  kCreateTable,
  kDropTable,
  kSelect,
  kInsert,
  kUpdate,
  kDelete,
  kAnalyze,
};

inline std::string StatementTypeName(StatementType t) {
  switch (t) {
    case StatementType::kCreateTable:
      return "CreateTable";
    case StatementType::kDropTable:
      return "DropTable";
    case StatementType::kSelect:
      return "Select";
    case StatementType::kInsert:
      return "Insert";
    case StatementType::kUpdate:
      return "Update";
    case StatementType::kDelete:
      return "Delete";
    case StatementType::kAnalyze:
      return "Analyze";
  }
  return "Unknown";
}

class Statement {
 public:
  explicit Statement(StatementType type) : type_(type) {}
  virtual ~Statement() = default;
  StatementType Type() const { return type_; }
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const Statement& s) {
    o << StatementTypeName(s.Type()) << " ";
    s.Dump(o);
    return o;
  }

 private:
  StatementType type_;
};

class CreateTableStatement : public Statement {
 public:
  CreateTableStatement(std::string table_name, std::vector<Column> columns)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)) {}

  CreateTableStatement(std::string table_name,
                       std::shared_ptr<SelectStatement> as_query)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        as_query_(std::move(as_query)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<Column>& Columns() const { return columns_; }
  const std::shared_ptr<SelectStatement>& AsQuery() const { return as_query_; }
  bool IsAsSelect() const { return as_query_ != nullptr; }

  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_;
    if (as_query_) {
      o << " AS SELECT";
    } else {
      o << " columns=[";
      for (size_t i = 0; i < columns_.size(); i++) {
        if (i) {
          o << ", ";
        }
        o << columns_[i];
      }
      o << "]";
    }
  }

 private:
  std::string table_name_;
  std::vector<Column> columns_;
  std::shared_ptr<SelectStatement> as_query_;
};

class DropTableStatement : public Statement {
 public:
  explicit DropTableStatement(std::string table_name)
      : Statement(StatementType::kDropTable),
        table_name_(std::move(table_name)) {}

  const std::string& TableName() const { return table_name_; }
  void Dump(std::ostream& o) const override { o << "table=" << table_name_; }

 private:
  std::string table_name_;
};

class SelectStatement : public Statement {
 public:
  struct OrderByTerm {
    Expression expression;
    bool ascending{true};
    // Present when the SQL spelled out NULLS FIRST / NULLS LAST; absent keeps
    // the engine default (NULLS FIRST on ASC, NULLS LAST on DESC).
    std::optional<bool> nulls_first;
  };

  SelectStatement(std::vector<NamedExpression> select_list,
                  std::vector<std::string> from_clause, Expression where_clause,
                  std::vector<OrderByTerm> order_by = {}, size_t limit = 0,
                  size_t offset = 0, bool distinct = false)
      : Statement(StatementType::kSelect),
        select_list_(std::move(select_list)),
        from_clause_(std::move(from_clause)),
        where_clause_(std::move(where_clause)),
        order_by_(std::move(order_by)),
        limit_(limit),
        offset_(offset),
        distinct_(distinct) {
    for (const std::string& table : from_clause_) {
      sources_.push_back(
          SelectSource{table, table, nullptr, JoinType::kCross, nullptr});
    }
  }

  const std::vector<NamedExpression>& SelectList() const {
    return select_list_;
  }
  const std::vector<std::string>& FromClause() const { return from_clause_; }
  const Expression& WhereClause() const { return where_clause_; }
  const std::vector<OrderByTerm>& OrderBy() const { return order_by_; }
  size_t Limit() const { return limit_; }
  // True only when the SQL carried an explicit LIMIT clause; LIMIT 0 must be
  // distinguishable from an absent LIMIT (§6.3).
  bool HasLimit() const { return has_limit_; }
  void SetLimit(std::optional<size_t> limit) {
    has_limit_ = limit.has_value();
    limit_ = limit.value_or(0);
  }
  size_t Offset() const { return offset_; }
  bool Distinct() const { return distinct_; }
  const std::vector<SelectSource>& Sources() const { return sources_; }
  const std::vector<Expression>& GroupBy() const { return group_by_; }
  const Expression& Having() const { return having_; }
  const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
  WithQueries() const {
    return with_queries_;
  }
  bool RequiresRelationalEvaluation() const { return complex_; }
  // SELECT AS STRUCT: subquery consumers compare whole rows as structs.
  bool AsStruct() const { return as_struct_; }
  void SetAsStruct(bool as_struct) { as_struct_ = as_struct; }
  const std::unordered_map<std::string, std::string>& Aliases() const {
    return aliases_;
  }
  void AddAlias(std::string alias, std::string table) {
    aliases_.emplace(std::move(alias), std::move(table));
  }
  void SetSources(std::vector<SelectSource> sources) {
    sources_ = std::move(sources);
  }
  void SetGroupBy(std::vector<Expression> group_by) {
    group_by_ = std::move(group_by);
    complex_ = true;
  }
  void SetHaving(Expression having) {
    having_ = std::move(having);
    complex_ = true;
  }
  void AddWithQuery(std::string name, std::shared_ptr<SelectStatement> query) {
    with_queries_.emplace(std::move(name), std::move(query));
    complex_ = true;
  }
  const Expression& Qualify() const { return qualify_; }
  void SetQualify(Expression qualify) {
    qualify_ = std::move(qualify);
    complex_ = true;
  }
  // Rewriting hooks: the relational executor replaces window-function nodes
  // with references to pre-computed hidden columns on a shallow copy.
  void SetSelectList(std::vector<NamedExpression> select_list) {
    select_list_ = std::move(select_list);
  }
  void SetOrderBy(std::vector<OrderByTerm> order_by) {
    order_by_ = std::move(order_by);
  }
  const std::vector<std::shared_ptr<SelectStatement>>& UnionAll() const {
    return union_all_;
  }
  void AddUnionAll(std::shared_ptr<SelectStatement> query) {
    union_all_.push_back(std::move(query));
    complex_ = true;
  }
  void MarkComplex() { complex_ = true; }
  void Dump(std::ostream& o) const override {
    o << "select=[";
    for (size_t i = 0; i < select_list_.size(); i++) {
      if (i) {
        o << ", ";
      }
      o << select_list_[i];
    }
    o << "] from=[";
    for (size_t i = 0; i < from_clause_.size(); i++) {
      if (i) {
        o << ", ";
      }
      o << from_clause_[i];
    }
    o << "] where=";
    if (where_clause_) {
      o << *where_clause_;
    } else {
      o << "(null)";
    }
  }

 private:
  std::vector<NamedExpression> select_list_;
  std::vector<std::string> from_clause_;
  Expression where_clause_;
  std::vector<OrderByTerm> order_by_;
  size_t limit_{0};
  bool has_limit_{false};
  size_t offset_{0};
  bool distinct_{false};
  std::unordered_map<std::string, std::string> aliases_;
  std::vector<SelectSource> sources_;
  std::vector<Expression> group_by_;
  Expression having_;
  Expression qualify_;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      with_queries_;
  std::vector<std::shared_ptr<SelectStatement>> union_all_;
  bool as_struct_{false};
  bool complex_{false};
};

enum class InsertMode { kDefault, kIgnore, kUpdate, kReplace };

class InsertStatement : public Statement {
 public:
  InsertStatement(std::string table_name,
                  std::vector<std::vector<Expression>> values,
                  std::vector<std::string> columns = {})
      : Statement(StatementType::kInsert),
        table_name_(std::move(table_name)),
        values_(std::move(values)),
        columns_(std::move(columns)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<std::vector<Expression>>& Values() const { return values_; }
  const std::vector<std::string>& Columns() const { return columns_; }
  // INSERT ... SELECT: rows come from the query instead of VALUES tuples.
  const std::shared_ptr<SelectStatement>& Query() const { return query_; }
  void SetQuery(std::shared_ptr<SelectStatement> query) {
    query_ = std::move(query);
  }
  InsertMode Mode() const { return mode_; }
  void SetMode(InsertMode mode) { mode_ = mode; }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  int64_t AssertRowsModified() const { return assert_rows_modified_; }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  bool HasAssert() const { return assert_rows_modified_ >= 0; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " values=[";
    for (size_t i = 0; i < values_.size(); i++) {
      if (i) {
        o << "; ";
      }
      o << "(";
      for (size_t j = 0; j < values_[i].size(); j++) {
        if (j) {
          o << ", ";
        }
        o << *values_[i][j];
      }
      o << ")";
    }
    if (query_) {
      o << "query={" << *query_ << "}";
    }
    o << "]";
  }

 private:
  std::string table_name_;
  std::vector<std::vector<Expression>> values_;
  std::vector<std::string> columns_;
  std::shared_ptr<SelectStatement> query_;
  InsertMode mode_{InsertMode::kDefault};
  int64_t assert_rows_modified_{-1};
};

// Nested DML inside UPDATE SET (...): GoogleSQL applies per-row array
// mutations (DELETE / UPDATE / INSERT of array elements) while rewriting a
// target row. The element variable visible to `predicate` / `set_value` is
// the last component of `target_path`.
struct NestedDmlItem {
  enum class Kind { kDelete, kUpdate, kInsert };

  Kind kind{Kind::kDelete};
  std::string target_path;
  // DELETE elem WHERE predicate / UPDATE ... SET x = set_value WHERE predicate
  Expression predicate;
  Expression set_value;
  // INSERT VALUES ((a), (b)) rows or an INSERT ... (SELECT ...) source query.
  std::vector<std::vector<Expression>> insert_values;
  std::shared_ptr<SelectStatement> insert_query;
  // -1 = no ASSERT_ROWS_MODIFIED clause on this nested item.
  int64_t assert_rows_modified{-1};
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
  // Nested per-row array DML items (SET (DELETE/UPDATE/INSERT ...)).
  const std::vector<NestedDmlItem>& NestedItems() const {
    return nested_items_;
  }
  void SetNestedItems(std::vector<NestedDmlItem> items) {
    nested_items_ = std::move(items);
  }
  bool HasNestedDml() const { return !nested_items_.empty(); }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  int64_t AssertRowsModified() const { return assert_rows_modified_; }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  bool HasAssert() const { return assert_rows_modified_ >= 0; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " set=[";
    for (size_t i = 0; i < set_clause_.size(); i++) {
      if (i) {
        o << ", ";
      }
      o << set_clause_[i].first << " = " << *set_clause_[i].second;
    }
    o << "] where=";
    if (where_clause_) {
      o << *where_clause_;
    } else {
      o << "(null)";
    }
  }

 private:
  std::string table_name_;
  std::vector<std::pair<ColumnName, Expression>> set_clause_;
  Expression where_clause_;
  std::vector<NestedDmlItem> nested_items_;
  int64_t assert_rows_modified_{-1};
};

class DeleteStatement : public Statement {
 public:
  DeleteStatement(std::string table_name, Expression where_clause)
      : Statement(StatementType::kDelete),
        table_name_(std::move(table_name)),
        where_clause_(std::move(where_clause)) {}

  const std::string& TableName() const { return table_name_; }
  const Expression& WhereClause() const { return where_clause_; }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  int64_t AssertRowsModified() const { return assert_rows_modified_; }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  bool HasAssert() const { return assert_rows_modified_ >= 0; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " where=";
    if (where_clause_) {
      o << *where_clause_;
    } else {
      o << "(null)";
    }
  }

 private:
  std::string table_name_;
  Expression where_clause_;
  int64_t assert_rows_modified_{-1};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_STATEMENT_HPP
