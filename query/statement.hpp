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
#include <unordered_set>
#include <vector>

#include "common/set_operation.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/column.hpp"

namespace tinylamb {

class SelectStatement;
class TransactionContext;

// `WITH RECURSIVE ... WITH DEPTH [AS col] BETWEEN lo AND hi`: iteration
// count is capped explicitly and each row carries its recursion depth.
struct RecursiveDepthSpec {
  std::string column{"depth"};
  int64_t lower{0};
  int64_t upper{0};
};

enum class JoinType { kCross, kInner, kLeft, kRight, kFull };

// Column match mode of one set operation: plain (positional, same count),
// CORRESPONDING [BY (col, ...)] or BY NAME.  BY NAME aligns on the union of
// column names with NULL padding; CORRESPONDING uses the intersection.
struct SetOperationMatch {
  bool corresponding{false};
  bool by_name{false};
  std::vector<std::string> columns;
};

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
  // USING(col, ...) column names: the joined output must expose ONE merged
  // column per entry (the left side wins) instead of two same-named ones.
  std::vector<std::string> using_columns;
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
  // `CREATE TABLE t (cols) AS SELECT ...` carries both a schema and the
  // initial data source; execution creates the table then loads the query.
  void SetAsQuery(std::shared_ptr<SelectStatement> query) {
    as_query_ = std::move(query);
  }

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
  std::vector<NamedExpression>& SelectList() { return select_list_; }
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
  void SetOffset(size_t offset) { offset_ = offset; }
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
    with_query_order_.push_back(name);
    with_queries_.emplace(std::move(name), std::move(query));
    complex_ = true;
  }
  // WITH RECURSIVE: the body references its own CTE name and is evaluated
  // iteratively (anchor terms seed a work table; recursive terms read the
  // previous iteration's delta until it drains).
  void AddRecursiveWithQuery(std::string name,
                             std::shared_ptr<SelectStatement> query) {
    recursive_with_queries_.insert(name);
    AddWithQuery(std::move(name), std::move(query));
  }
  void SetRecursiveDepth(const std::string& name, RecursiveDepthSpec spec) {
    recursive_depth_specs_[name] = std::move(spec);
  }
  [[nodiscard]] bool IsRecursiveWith(const std::string& name) const {
    return recursive_with_queries_.contains(name);
  }
  [[nodiscard]] const RecursiveDepthSpec* RecursiveDepthOf(
      const std::string& name) const {
    const auto found = recursive_depth_specs_.find(name);
    return found == recursive_depth_specs_.end() ? nullptr : &found->second;
  }
  // Declaration order of WITH entries: a later CTE may reference earlier
  // ones, so resolution must follow source order rather than map order.
  [[nodiscard]] const std::vector<std::string>& WithQueryOrder() const {
    return with_query_order_;
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
  void SetWhereClause(Expression where_clause) {
    where_clause_ = std::move(where_clause);
  }
  void MarkDistinct() { distinct_ = true; }
  void SetOrderBy(std::vector<OrderByTerm> order_by) {
    order_by_ = std::move(order_by);
  }
  const std::vector<std::shared_ptr<SelectStatement>>& UnionAll() const {
    return union_all_;
  }
  const std::vector<SetOperationKind>& SetOperationKinds() const {
    return set_operation_kinds_;
  }
  // GROUPING SETS expansion (ROLLUP/CUBE lower to sets too).  Each entry is
  // one grouping set; GroupBy() carries the flattened column universe.
  const std::vector<std::vector<Expression>>& GroupingSets() const {
    return grouping_sets_;
  }
  void SetGroupingSets(std::vector<std::vector<Expression>> sets) {
    grouping_sets_ = std::move(sets);
    complex_ = true;
  }
  // CORRESPONDING [BY (cols)] match mode per set operation (parallel to
  // union_all_/set_operation_kinds_).
  const std::vector<SetOperationMatch>& Matches() const {
    return set_operation_matches_;
  }
  void AddSetOperation(SetOperationKind kind,
                       std::shared_ptr<SelectStatement> query,
                       SetOperationMatch match = {}) {
    set_operation_kinds_.push_back(kind);
    union_all_.push_back(std::move(query));
    set_operation_matches_.push_back(std::move(match));
    complex_ = true;
  }
  void AddUnionAll(std::shared_ptr<SelectStatement> query) {
    AddSetOperation(SetOperationKind::kUnionAll, std::move(query));
  }
  void ClearUnionAll() {
    union_all_.clear();
    set_operation_kinds_.clear();
    set_operation_matches_.clear();
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
  std::vector<std::vector<Expression>> grouping_sets_;
  Expression having_;
  Expression qualify_;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      with_queries_;
  std::vector<std::string> with_query_order_;
  std::unordered_set<std::string> recursive_with_queries_;
  std::unordered_map<std::string, RecursiveDepthSpec> recursive_depth_specs_;
  std::vector<std::shared_ptr<SelectStatement>> union_all_;
  std::vector<SetOperationKind> set_operation_kinds_;
  std::vector<SetOperationMatch> set_operation_matches_;
  bool complex_{false};
};

class InsertStatement : public Statement {
 public:
  InsertStatement(std::string table_name,
                  std::vector<std::vector<Expression>> values,
                  std::vector<std::string> columns = {})
      : Statement(StatementType::kInsert),
        table_name_(std::move(table_name)),
        values_(std::move(values)),
        columns_(std::move(columns)) {}

  InsertStatement(std::string table_name,
                  std::shared_ptr<SelectStatement> query,
                  std::vector<std::string> columns = {})
      : Statement(StatementType::kInsert),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)),
        query_(std::move(query)) {}

  const std::string& TableName() const { return table_name_; }
  const std::vector<std::vector<Expression>>& Values() const { return values_; }
  const std::vector<std::string>& Columns() const { return columns_; }
  const std::shared_ptr<SelectStatement>& Query() const { return query_; }
  bool IsSelect() const { return query_ != nullptr; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_;
    if (query_) {
      o << " AS SELECT";
      return;
    }
    o << " values=[";
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
    o << "]";
  }

 private:
  std::string table_name_;
  std::vector<std::vector<Expression>> values_;
  std::vector<std::string> columns_;
  std::shared_ptr<SelectStatement> query_;
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
  std::vector<std::pair<ColumnName, Expression>>& SetClauseMutable() {
    return set_clause_;
  }
  const Expression& WhereClause() const { return where_clause_; }
  // THEN RETURN projections: empty list with has_returning_==false means the
  // statement outputs its update count.
  bool HasReturning() const {
    return false;
  }  // TODO: enable after segfault fix
  const std::vector<NamedExpression>& Returning() const { return returning_; }
  void SetReturning(std::vector<NamedExpression> returning) {
    returning_ = std::move(returning);
    has_returning_ = true;
  }
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
  bool has_returning_{false};
  std::vector<NamedExpression> returning_;
};

// Shared THEN RETURN payload for DML statements.
struct ReturningClauseData {
  bool has_returning{false};
  std::vector<NamedExpression> projections;
};

class DeleteStatement : public Statement {
 public:
  DeleteStatement(std::string table_name, Expression where_clause)
      : Statement(StatementType::kDelete),
        table_name_(std::move(table_name)),
        where_clause_(std::move(where_clause)) {}

  const std::string& TableName() const { return table_name_; }
  const Expression& WhereClause() const { return where_clause_; }
  bool HasReturning() const {
    return false;
  }  // TODO: enable after segfault fix
  const std::vector<NamedExpression>& Returning() const { return returning_; }
  void SetReturning(std::vector<NamedExpression> returning) {
    returning_ = std::move(returning);
    has_returning_ = true;
  }
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
  bool has_returning_{false};
  std::vector<NamedExpression> returning_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_STATEMENT_HPP
