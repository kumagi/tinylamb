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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/set_operation.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/table_sample_spec.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SelectStatement;
struct SetOperationTree;

// Metadata needed by the relational executor for WITH RECURSIVE.  The
// recursive body remains a normal SelectStatement; these fields identify the
// CTEs whose self-reference must be bound to a work table.
struct RecursiveDepthSpec {
  std::string column{"depth"};
  int64_t lower{0};
  int64_t upper{0};
  // Whether each bound was written as an integer literal.  The template binder
  // needs this: literals in the text are extracted as parameters, but the
  // bound lives in this struct instead of an expression tree, so it has to be
  // consumed from the parameter list by hand (an UNBOUNDED side contributes
  // no parameter).
  bool lower_is_literal{false};
  bool upper_is_literal{false};
};

// Column matching for CORRESPONDING/BY NAME set operations.  Plain set
// operations remain positional; the optional names describe the output order
// for name-based matching.
struct SetOperationMatch {
  bool corresponding{false};
  bool by_name{false};
  std::vector<std::string> columns;
};

enum class JoinType : uint8_t { kCross, kInner, kLeft, kRight, kFull };

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
  bool is_lateral{false};
  bool from_nested_join{false};
  // USING (col, ...) names declared on the join whose right operand is this
  // source; empty for every other source.  The merged columns stay
  // physically duplicated in the joined schema, but bare references and
  // star expansion coalesce them (see Lookup / relational projection).
  std::vector<std::string> using_columns;
  std::optional<TableSampleSpec> sample;
};

enum class StatementType : uint8_t {
  kCreateTable,
  kDropTable,
  kSelect,
  kInsert,
  kUpdate,
  kDelete,
  kMerge,
  kAnalyze,
  kAlterTable,
  kCreateMaterializedView,
  kDropMaterializedView,
  kCreateView,
  kDropView,
  kCreateIndex,
};

// ALTER TABLE action.  v1 carries the catalog-level actions (table rename,
// column rename); row-layout actions (ADD/DROP COLUMN) stay explicit errors
// at the visitor until the rebuild path lands.
struct AlterTableAction {
  enum class Kind : uint8_t {
    kRenameTo,
    kRenameColumn,
  };
  Kind kind{Kind::kRenameTo};
  // kRenameTo: the new table name.  kRenameColumn: [0] = old column name,
  // [1] = new column name.
  std::string new_name;
  std::string old_column;
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
    case StatementType::kMerge:
      return "Merge";
    case StatementType::kAnalyze:
      return "Analyze";
    case StatementType::kAlterTable:
      return "AlterTable";
    case StatementType::kCreateMaterializedView:
      return "CreateMaterializedView";
    case StatementType::kDropMaterializedView:
      return "DropMaterializedView";
    case StatementType::kCreateView:
      return "CreateView";
    case StatementType::kDropView:
      return "DropView";
    case StatementType::kCreateIndex:
      return "CreateIndex";
  }
  return "Unknown";
}

// Statement nodes are value-copied by the view/UDF expansion clones.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class Statement {
 public:
  explicit Statement(StatementType type) : type_(type) {}
  virtual ~Statement() = default;
  [[nodiscard]] StatementType Type() const { return type_; }
  virtual void Dump(std::ostream& o) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const Statement& s) {
    o << StatementTypeName(s.Type()) << " ";
    s.Dump(o);
    return o;
  }

 private:
  StatementType type_;
};

// Table-level FOREIGN KEY definition: child_columns[i] references
// parent_table.parent_columns[i].  Only NO ACTION referential actions are
// supported; anything else is rejected at parse time.
struct ForeignKeyDef {
  enum class Action : uint8_t { kNoAction = 0 };

  std::vector<std::string> child_columns;
  std::string parent_table;
  std::vector<std::string> parent_columns;
  Action on_delete{Action::kNoAction};
  Action on_update{Action::kNoAction};
};

class AlterTableStatement : public Statement {
 public:
  AlterTableStatement(std::string table_name, AlterTableAction action)
      : Statement(StatementType::kAlterTable),
        table_name_(std::move(table_name)),
        action_(std::move(action)) {}

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
  [[nodiscard]] const AlterTableAction& Action() const { return action_; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " action="
      << (action_.kind == AlterTableAction::Kind::kRenameTo ? "rename-to "
                                                            : "rename-column ")
      << action_.new_name;
  }

 private:
  std::string table_name_;
  AlterTableAction action_;
};

class CreateIndexStatement : public Statement {
 public:
  CreateIndexStatement(std::string name, std::string table,
                       std::vector<std::string> keys,
                       std::vector<std::string> storing, bool unique,
                       bool if_not_exists)
      : Statement(StatementType::kCreateIndex),
        name_(std::move(name)),
        table_(std::move(table)),
        keys_(std::move(keys)),
        storing_(std::move(storing)),
        unique_(unique),
        if_not_exists_(if_not_exists) {}
  const std::string& Name() const { return name_; }
  const std::string& TableName() const { return table_; }
  const std::vector<std::string>& Keys() const { return keys_; }
  const std::vector<std::string>& Storing() const { return storing_; }
  bool Unique() const { return unique_; }
  bool IfNotExists() const { return if_not_exists_; }
  void Dump(std::ostream& o) const override {
    o << "name=" << name_ << " table=" << table_ << " unique=" << unique_;
  }

 private:
  std::string name_, table_;
  std::vector<std::string> keys_, storing_;
  bool unique_, if_not_exists_;
};

class CreateTableStatement : public Statement {
 public:
  CreateTableStatement(std::string table_name, std::vector<Column> columns)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)) {}

  CreateTableStatement(std::string table_name, std::vector<Column> columns,
                       std::vector<ForeignKeyDef> foreign_keys)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        columns_(std::move(columns)),
        foreign_keys_(std::move(foreign_keys)) {}

  CreateTableStatement(std::string table_name,
                       std::shared_ptr<SelectStatement> as_query)
      : Statement(StatementType::kCreateTable),
        table_name_(std::move(table_name)),
        as_query_(std::move(as_query)) {}

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
  [[nodiscard]] const std::vector<Column>& Columns() const { return columns_; }
  [[nodiscard]] const std::vector<ForeignKeyDef>& ForeignKeys() const {
    return foreign_keys_;
  }
  [[nodiscard]] const std::shared_ptr<SelectStatement>& AsQuery() const {
    return as_query_;
  }
  [[nodiscard]] bool IsAsSelect() const { return as_query_ != nullptr; }

  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_;
    if (as_query_) {
      o << " AS SELECT";
    } else {
      o << " columns=[";
      for (size_t i = 0; i < columns_.size(); i++) {
        if (i != 0U) {
          o << ", ";
        }
        o << columns_[i];
      }
      o << "]";
      for (const ForeignKeyDef& fk : foreign_keys_) {
        o << " FOREIGN KEY (";
        for (size_t i = 0; i < fk.child_columns.size(); i++) {
          if (i != 0U) {
            o << ", ";
          }
          o << fk.child_columns[i];
        }
        o << ") REFERENCES " << fk.parent_table << "(";
        for (size_t i = 0; i < fk.parent_columns.size(); i++) {
          if (i != 0U) {
            o << ", ";
          }
          o << fk.parent_columns[i];
        }
        o << ")";
      }
    }
  }

 private:
  std::string table_name_;
  std::vector<Column> columns_;
  std::vector<ForeignKeyDef> foreign_keys_;
  std::shared_ptr<SelectStatement> as_query_;
};

// CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS <query>.  The result is
// materialized into a catalog table of the same name; the defining query
// text rides along in the catalog so the view can be refreshed by re-running
// it.  Staleness is manual (BigQuery-style automatic incremental refresh is
// out of scope): queries read the stored rows until an explicit refresh.
class CreateMaterializedViewStatement : public Statement {
 public:
  CreateMaterializedViewStatement(std::string view_name, bool if_not_exists,
                                  std::shared_ptr<SelectStatement> query,
                                  std::string query_text)
      : Statement(StatementType::kCreateMaterializedView),
        view_name_(std::move(view_name)),
        if_not_exists_(if_not_exists),
        query_(std::move(query)),
        query_text_(std::move(query_text)) {}

  [[nodiscard]] const std::string& ViewName() const { return view_name_; }
  [[nodiscard]] bool IfNotExists() const { return if_not_exists_; }
  [[nodiscard]] const std::shared_ptr<SelectStatement>& Query() const {
    return query_;
  }
  // The defining query, sliced verbatim from the statement source; refresh
  // and catalog introspection re-run this text.
  [[nodiscard]] const std::string& QueryText() const { return query_text_; }
  void Dump(std::ostream& o) const override {
    o << "view=" << view_name_ << (if_not_exists_ ? " if_not_exists" : "")
      << " AS " << query_text_;
  }

 private:
  std::string view_name_;
  bool if_not_exists_;
  std::shared_ptr<SelectStatement> query_;
  std::string query_text_;
};

// CREATE VIEW: the defining query persists as text (Database view records)
// and expands at FROM-reference time through the session ViewRegistry.  The
// visitor registers the parsed query immediately so same-transaction
// references resolve; execution persists the definition for later sessions.
class CreateViewStatement : public Statement {
 public:
  CreateViewStatement(std::string view_name, bool or_replace,
                      std::string query_text)
      : Statement(StatementType::kCreateView),
        view_name_(std::move(view_name)),
        or_replace_(or_replace),
        query_text_(std::move(query_text)) {}

  [[nodiscard]] const std::string& ViewName() const { return view_name_; }
  [[nodiscard]] bool OrReplace() const { return or_replace_; }
  [[nodiscard]] const std::string& QueryText() const { return query_text_; }
  void Dump(std::ostream& o) const override {
    o << "view=" << view_name_ << (or_replace_ ? " or_replace" : "") << " AS "
      << query_text_;
  }

 private:
  std::string view_name_;
  bool or_replace_;
  std::string query_text_;
};

class DropViewStatement : public Statement {
 public:
  explicit DropViewStatement(std::string view_name)
      : Statement(StatementType::kDropView), view_name_(std::move(view_name)) {}

  [[nodiscard]] const std::string& ViewName() const { return view_name_; }
  void Dump(std::ostream& o) const override { o << "view=" << view_name_; }

 private:
  std::string view_name_;
};

class DropMaterializedViewStatement : public Statement {
 public:
  explicit DropMaterializedViewStatement(std::string view_name)
      : Statement(StatementType::kDropMaterializedView),
        view_name_(std::move(view_name)) {}

  [[nodiscard]] const std::string& ViewName() const { return view_name_; }
  void Dump(std::ostream& o) const override { o << "view=" << view_name_; }

 private:
  std::string view_name_;
};

class DropTableStatement : public Statement {
 public:
  explicit DropTableStatement(std::string table_name)
      : Statement(StatementType::kDropTable),
        table_name_(std::move(table_name)) {}

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
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

  SelectStatement() : Statement(StatementType::kSelect) {}
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
      sources_.emplace_back(table, table, nullptr, JoinType::kCross, nullptr);
    }
  }

  const std::vector<NamedExpression>& SelectList() const {
    return select_list_;
  }
  void SetSelectList(std::vector<NamedExpression> select_list) {
    select_list_ = std::move(select_list);
    runtime_id_.Bump();
  }
  const std::vector<std::string>& FromClause() const { return from_clause_; }
  const Expression& WhereClause() const { return where_clause_; }
  void SetWhereClause(Expression where_clause) {
    where_clause_ = std::move(where_clause);
    runtime_id_.Bump();
  }
  const std::vector<OrderByTerm>& OrderBy() const { return order_by_; }
  size_t Limit() const { return limit_; }
  // True only when the SQL carried an explicit LIMIT clause; LIMIT 0 must be
  // distinguishable from an absent LIMIT (§6.3).
  bool HasLimit() const { return has_limit_; }
  void SetLimit(std::optional<size_t> limit) {
    has_limit_ = limit.has_value();
    limit_ = limit.value_or(0);
    runtime_id_.Bump();
  }
  void SetOffset(size_t offset) {
    offset_ = offset;
    runtime_id_.Bump();
  }
  size_t Offset() const { return offset_; }
  bool Distinct() const { return distinct_; }
  void SetDistinct(bool distinct) {
    distinct_ = distinct;
    runtime_id_.Bump();
  }
  bool HasDistinctOn() const { return !distinct_on_.empty(); }
  const std::vector<Expression>& DistinctOn() const { return distinct_on_; }
  void SetDistinctOn(std::vector<Expression> distinct_on) {
    distinct_on_ = std::move(distinct_on);
    distinct_ = true;
    complex_ = true;
    runtime_id_.Bump();
  }
  // SELECT ... FOR UPDATE (GoogleSQL's only locking clause): the rows the
  // scan visits are write-intent locked and stay locked until the transaction
  // ends.  GoogleSQL has no FOR SHARE / NOWAIT / SKIP LOCKED, so a single flag
  // is the whole grammar.
  bool ForUpdate() const { return for_update_; }
  void SetForUpdate(bool for_update) {
    for_update_ = for_update;
    runtime_id_.Bump();
  }
  bool WithTies() const { return with_ties_; }
  void SetWithTies(bool with_ties) {
    with_ties_ = with_ties;
    if (with_ties) {
      complex_ = true;
    }
    runtime_id_.Bump();
  }
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
  void SetAsStruct(bool as_struct) {
    as_struct_ = as_struct;
    runtime_id_.Bump();
  }
  const std::unordered_map<std::string, std::string>& Aliases() const {
    return aliases_;
  }
  void AddAlias(std::string alias, std::string table) {
    aliases_.emplace(std::move(alias), std::move(table));
    runtime_id_.Bump();
  }
  void SetSources(std::vector<SelectSource> sources) {
    sources_ = std::move(sources);
    runtime_id_.Bump();
  }
  void SetGroupBy(std::vector<Expression> group_by) {
    group_by_ = std::move(group_by);
    complex_ = true;
    runtime_id_.Bump();
  }
  // True when the statement carries the syntactically empty grouping set
  // (`GROUP BY ()`, or the () set of GROUPING SETS / ROLLUP). The grouping
  // key list holds a placeholder constant then, but the standard semantics
  // is exactly one group over all input rows -- emitted even when the input
  // is empty (COUNT(*) = 0, other aggregates NULL).
  void SetGroupAll(bool group_all) {
    group_all_ = group_all;
    runtime_id_.Bump();
  }
  bool IsGroupAll() const { return group_all_; }
  void SetHaving(Expression having) {
    having_ = std::move(having);
    complex_ = true;
    runtime_id_.Bump();
  }
  void AddWithQuery(std::string name, std::shared_ptr<SelectStatement> query) {
    with_query_order_.push_back(name);
    with_queries_.emplace(std::move(name), std::move(query));
    complex_ = true;
    runtime_id_.Bump();
  }
  void AddRecursiveWithQuery(std::string name,
                             std::shared_ptr<SelectStatement> query) {
    recursive_with_queries_.insert(name);
    AddWithQuery(std::move(name), std::move(query));
  }
  // Erases a CTE definition after its single use was inlined (M4). Recursive
  // entries are never inlined and therefore never removed here.
  void RemoveWithQuery(const std::string& name) {
    with_queries_.erase(name);
    std::erase(with_query_order_, name);
    runtime_id_.Bump();
  }
  // Drops every CTE declaration from this statement. Grouping-set expansion
  // copies the WITH clause onto each UNION ALL branch; the owning statement
  // keeps the declarations and branch references resolve through the
  // execution's inherited CTE map, so a branch carrying its own copy would
  // re-execute every CTE body once per grouping set.
  void ClearWithQueries() {
    with_queries_.clear();
    with_query_order_.clear();
    recursive_with_queries_.clear();
    runtime_id_.Bump();
  }
  [[nodiscard]] bool IsRecursiveWith(const std::string& name) const {
    return recursive_with_queries_.contains(name);
  }
  [[nodiscard]] const RecursiveDepthSpec* RecursiveDepthOf(
      const std::string& name) const {
    const auto found = recursive_depth_specs_.find(name);
    return found == recursive_depth_specs_.end() ? nullptr : &found->second;
  }
  void SetRecursiveDepth(const std::string& name, RecursiveDepthSpec spec) {
    recursive_depth_specs_[name] = std::move(spec);
    runtime_id_.Bump();
  }
  [[nodiscard]] const std::vector<std::string>& WithQueryOrder() const {
    return with_query_order_;
  }
  const Expression& Qualify() const { return qualify_; }
  void SetQualify(Expression qualify) {
    qualify_ = std::move(qualify);
    complex_ = true;
    runtime_id_.Bump();
  }
  void SetOrderBy(std::vector<OrderByTerm> order_by) {
    order_by_ = std::move(order_by);
    runtime_id_.Bump();
  }
  const std::vector<std::shared_ptr<SelectStatement>>& UnionAll() const {
    return union_all_;
  }
  const std::vector<SetOperationKind>& SetOperationKinds() const {
    return set_operation_kinds_;
  }
  const std::vector<SetOperationMatch>& Matches() const {
    return set_operation_matches_;
  }
  // Column-match mode declared for the pair that introduces the branch at
  // `pair_index` (`BY NAME`, `CORRESPONDING`, or an ON/BY column list).
  // Statements assembled without per-pair metadata (hand-built plans, legacy
  // callers) fall back to the head-level BY NAME flag that
  // MarkUnionDistinct records.
  [[nodiscard]] SetOperationMatch MatchFor(size_t pair_index) const {
    if (pair_index < set_operation_matches_.size()) {
      const SetOperationMatch& match = set_operation_matches_[pair_index];
      if (match.by_name || match.corresponding || !match.columns.empty()) {
        return match;
      }
    }
    SetOperationMatch fallback;
    fallback.by_name = union_by_name_;
    return fallback;
  }
  void AddSetOperation(SetOperationKind kind,
                       std::shared_ptr<SelectStatement> query,
                       SetOperationMatch match = {}) {
    set_operation_kinds_.push_back(kind);
    union_all_.push_back(std::move(query));
    set_operation_matches_.push_back(std::move(match));
    complex_ = true;
    runtime_id_.Bump();
  }
  void AddUnionAll(std::shared_ptr<SelectStatement> query) {
    AddSetOperation(SetOperationKind::kUnionAll, std::move(query));
  }
  void MarkUnionDistinct(bool by_name = false) {
    union_distinct_ = true;
    union_by_name_ = by_name;
    runtime_id_.Bump();
  }
  [[nodiscard]] bool UnionDistinct() const { return union_distinct_; }
  [[nodiscard]] bool UnionByName() const { return union_by_name_; }
  [[nodiscard]] const std::shared_ptr<SetOperationTree>& GetSetOperationTree()
      const {
    return set_operation_tree_;
  }
  void SetSetOperationTree(std::shared_ptr<SetOperationTree> tree) {
    set_operation_tree_ = std::move(tree);
    complex_ = true;
    runtime_id_.Bump();
  }
  void ClearUnionAll() {
    union_all_.clear();
    set_operation_kinds_.clear();
    set_operation_matches_.clear();
    runtime_id_.Bump();
  }
  void MarkComplex() {
    complex_ = true;
    runtime_id_.Bump();
  }
  // Monotonic per-instance / per-mutation identity for runtime subquery
  // caches.  Bumped on construction, copy, move, and every mutating setter so
  // a recycled SelectStatement* address or in-place mutation never hits a
  // stale pointer-keyed memo entry.
  [[nodiscard]] uint64_t RuntimeId() const noexcept {
    return runtime_id_.value;
  }
  const std::string& Fingerprint() const {
    std::ostringstream stream;
    auto append_string = [&](std::string_view text) {
      stream << text.size() << ':' << text << ';';
    };
    auto append_expression = [&](auto& self,
                                 const Expression& expression) -> void {
      if (!expression) {
        stream << "<null>;";
        return;
      }
      auto walk_nested_special = [&](auto& walk_self,
                                     const Expression& node) -> void {
        if (!node) {
          return;
        }
        if (node->Type() == TypeTag::kQueryExp ||
            node->Type() == TypeTag::kAggregateExp ||
            node->Type() == TypeTag::kWindowFunctionExp) {
          self(self, node);
          return;
        }
        for (const Expression& child : ExpressionChildren(node)) {
          if (child) {
            walk_self(walk_self, child);
          }
        }
      };
      stream << "e" << static_cast<int>(expression->Type()) << ':';
      if (expression->Type() == TypeTag::kQueryExp) {
        const auto& query = expression->AsQueryExpression();
        stream << "q=";
        if (query.Query()) {
          stream << query.Query()->Fingerprint();
        } else {
          stream << "<null>";
        }
        stream << ':' << query.Exists() << ':' << query.Negated() << ':'
               << static_cast<int>(query.Op()) << ':'
               << static_cast<int>(query.Mode()) << ':' << query.ArrayResult()
               << ':';
        append_string(query.ArrayElementSqlType());
        if (query.Test()) {
          self(self, query.Test());
        } else {
          stream << "<null>;";
        }
      } else if (expression->Type() == TypeTag::kAggregateExp ||
                 expression->Type() == TypeTag::kWindowFunctionExp) {
        stream << static_cast<const void*>(expression.get());
      } else {
        stream << expression->ToString();
        for (const Expression& child : ExpressionChildren(expression)) {
          if (child) {
            walk_nested_special(walk_nested_special, child);
          }
        }
      }
      stream << ';';
    };
    stream << "select;";
    for (const NamedExpression& item : select_list_) {
      append_string(item.name);
      append_expression(append_expression, item.expression);
    }
    stream << "from;";
    for (const std::string& table : from_clause_) {
      append_string(table);
    }
    for (const SelectSource& source : sources_) {
      append_string(source.table);
      append_string(source.alias);
      stream << "q=";
      if (source.query) {
        stream << source.query->Fingerprint();
      } else {
        stream << "<null>";
      }
      stream << ";j=" << static_cast<int>(source.join_type) << ';';
      append_expression(append_expression, source.join_condition);
      append_expression(append_expression, source.unnest);
      append_string(source.offset_alias);
      stream << source.is_lateral << source.from_nested_join << ';';
      for (const std::string& column : source.using_columns) {
        append_string(column);
      }
      if (const std::optional<TableSampleSpec>& sample = source.sample) {
        sample->Dump(stream);
      }
      stream << ';';
    }
    stream << "where;";
    append_expression(append_expression, where_clause_);
    stream << "order;";
    for (const OrderByTerm& term : order_by_) {
      append_expression(append_expression, term.expression);
      stream << term.ascending << ':' << term.nulls_first.value_or(false)
             << ';';
    }
    stream << "limit;" << has_limit_ << ':' << limit_ << ':' << offset_
           << ";distinct;" << distinct_ << ';';
    for (const Expression& expression : distinct_on_) {
      append_expression(append_expression, expression);
    }
    stream << "ties;" << with_ties_ << ";aliases;";
    std::vector<std::pair<std::string, std::string>> aliases(aliases_.begin(),
                                                             aliases_.end());
    std::sort(aliases.begin(), aliases.end());
    for (const auto& [alias, table] : aliases) {
      append_string(alias);
      append_string(table);
    }
    stream << "group;" << group_all_ << ';';
    for (const Expression& expression : group_by_) {
      append_expression(append_expression, expression);
    }
    stream << "having;";
    append_expression(append_expression, having_);
    stream << "qualify;";
    append_expression(append_expression, qualify_);
    stream << "with;";
    for (const std::string& name : with_query_order_) {
      append_string(name);
      stream << "recursive=" << IsRecursiveWith(name) << ';';
      const auto found = with_queries_.find(name);
      if (found != with_queries_.end() && found->second) {
        stream << found->second->Fingerprint();
      } else {
        stream << "<null>";
      }
      const auto depth = recursive_depth_specs_.find(name);
      if (depth != recursive_depth_specs_.end()) {
        stream << ':' << depth->second.column << ':' << depth->second.lower
               << ':' << depth->second.upper;
      }
      stream << ';';
    }
    stream << "union;";
    for (size_t i = 0; i < union_all_.size(); ++i) {
      stream << (i < set_operation_kinds_.size()
                     ? static_cast<int>(set_operation_kinds_[i])
                     : static_cast<int>(SetOperationKind::kUnionAll))
             << ';';
      if (i < set_operation_matches_.size()) {
        const SetOperationMatch& match = set_operation_matches_[i];
        stream << match.corresponding << match.by_name << ':';
        for (const std::string& column : match.columns) {
          append_string(column);
        }
      }
      if (i < union_all_.size() && union_all_[i]) {
        stream << union_all_[i]->Fingerprint();
      } else {
        stream << "<null>";
      }
      stream << ';';
    }
    stream << "union_flags;" << union_distinct_ << ':' << union_by_name_
           << ";struct;" << as_struct_ << ";complex;" << complex_ << ";tree;"
           << static_cast<const void*>(set_operation_tree_.get()) << ';';
    cached_fingerprint_ = stream.str();
    return cached_fingerprint_;
  }
  void Dump(std::ostream& o) const override {
    o << "select=[";
    for (size_t i = 0; i < select_list_.size(); i++) {
      if (i != 0U) {
        o << ", ";
      }
      o << select_list_[i];
    }
    o << "] from=[";
    for (size_t i = 0; i < from_clause_.size(); i++) {
      if (i != 0U) {
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
    if (for_update_) {
      o << " for_update=true";
    }
    for (const SelectSource& source : sources_) {
      if (!source.sample) {
        continue;
      }
      o << " sample=";
      source.sample->Dump(o);
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
  std::vector<Expression> distinct_on_;
  bool with_ties_{false};
  bool for_update_{false};
  std::unordered_map<std::string, std::string> aliases_;
  std::vector<SelectSource> sources_;
  std::vector<Expression> group_by_;
  bool group_all_{false};
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
  std::shared_ptr<SetOperationTree> set_operation_tree_;
  bool union_distinct_{false};
  bool union_by_name_{false};
  bool as_struct_{false};
  bool complex_{false};
  struct RuntimeInstanceId {
    uint64_t value{Next()};
    RuntimeInstanceId() = default;
    RuntimeInstanceId(const RuntimeInstanceId&) noexcept : value(Next()) {}
    RuntimeInstanceId(RuntimeInstanceId&&) noexcept : value(Next()) {}
    RuntimeInstanceId& operator=(const RuntimeInstanceId&) noexcept {
      value = Next();
      return *this;
    }
    RuntimeInstanceId& operator=(RuntimeInstanceId&&) noexcept {
      value = Next();
      return *this;
    }
    ~RuntimeInstanceId() = default;
    void Bump() noexcept { value = Next(); }
    static uint64_t Next() noexcept {
      static std::atomic<uint64_t> next{1};
      return next.fetch_add(1, std::memory_order_relaxed);
    }
  };
  RuntimeInstanceId runtime_id_;
  mutable std::string cached_fingerprint_;
};

// Parenthesized set-operation groups cannot be represented by the legacy
// flat UnionAll()/SetOperationKinds() vectors.  Keep the original flat form
// for optimizer compatibility, and attach this tree for execution whenever a
// query contains an explicitly grouped operand.
struct SetOperationTree {
  std::shared_ptr<SelectStatement> first;
  std::vector<std::shared_ptr<SelectStatement>> branches;
  std::vector<SetOperationKind> kinds;
  bool grouped{false};
};

enum class InsertMode : uint8_t { kDefault, kIgnore, kUpdate, kReplace };

class InsertStatement : public Statement {
 public:
  InsertStatement(std::string table_name,
                  std::vector<std::vector<Expression>> values,
                  std::vector<std::string> columns = {})
      : Statement(StatementType::kInsert),
        table_name_(std::move(table_name)),
        values_(std::move(values)),
        columns_(std::move(columns)) {}

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
  std::vector<std::vector<Expression>>& MutableValues() { return values_; }
  [[nodiscard]] const std::vector<std::vector<Expression>>& Values() const {
    return values_;
  }
  [[nodiscard]] const std::vector<std::string>& Columns() const {
    return columns_;
  }
  // INSERT ... SELECT: rows come from the query instead of VALUES tuples.
  [[nodiscard]] const std::shared_ptr<SelectStatement>& Query() const {
    return query_;
  }
  void SetQuery(std::shared_ptr<SelectStatement> query) {
    query_ = std::move(query);
  }
  [[nodiscard]] InsertMode Mode() const { return mode_; }
  void SetMode(InsertMode mode) { mode_ = mode; }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  [[nodiscard]] int64_t AssertRowsModified() const {
    return assert_rows_modified_;
  }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  [[nodiscard]] bool HasAssert() const { return assert_rows_modified_ >= 0; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " values=[";
    for (size_t i = 0; i < values_.size(); i++) {
      if (i != 0U) {
        o << "; ";
      }
      o << "(";
      for (size_t j = 0; j < values_[i].size(); j++) {
        if (j != 0U) {
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
  enum class Kind : uint8_t { kDelete, kUpdate, kInsert };

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

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
  // UPDATE tbl AS alias: bare references to the alias denote the whole row
  // (value tables), so SET/WHERE may bind to the single physical column.
  [[nodiscard]] const std::string& Alias() const { return alias_; }
  void SetAlias(std::string alias) { alias_ = std::move(alias); }
  [[nodiscard]] const std::vector<std::pair<ColumnName, Expression>>&
  SetClause() const {
    return set_clause_;
  }
  // Prepare-time subquery folding rewrites the SET value expressions.
  std::vector<std::pair<ColumnName, Expression>>& MutableSetClause() {
    return set_clause_;
  }
  [[nodiscard]] const Expression& WhereClause() const { return where_clause_; }
  // Nested per-row array DML items (SET (DELETE/UPDATE/INSERT ...)).
  [[nodiscard]] const std::vector<NestedDmlItem>& NestedItems() const {
    return nested_items_;
  }
  void SetNestedItems(std::vector<NestedDmlItem> items) {
    nested_items_ = std::move(items);
  }
  [[nodiscard]] bool HasNestedDml() const { return !nested_items_.empty(); }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  [[nodiscard]] int64_t AssertRowsModified() const {
    return assert_rows_modified_;
  }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  [[nodiscard]] bool HasAssert() const { return assert_rows_modified_ >= 0; }
  void Dump(std::ostream& o) const override {
    o << "table=" << table_name_ << " set=[";
    for (size_t i = 0; i < set_clause_.size(); i++) {
      if (i != 0U) {
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
  std::string alias_;
  std::vector<std::pair<ColumnName, Expression>> set_clause_;
  Expression where_clause_;
  std::vector<NestedDmlItem> nested_items_;
  int64_t assert_rows_modified_{-1};
};

// MERGE INTO target USING source ON condition WHEN [NOT] MATCHED THEN action.
// Clones of the source expression / set expressions keep the source alias
// qualified so evaluation resolves source columns through an outer Scope.

enum class MergeActionKind : uint8_t { kSkip, kUpdate, kDelete, kInsert };

struct MergeAction {
  MergeActionKind kind{MergeActionKind::kSkip};
  // kUpdate: assignments in WHEN MATCHED THEN UPDATE SET order.
  std::vector<std::pair<ColumnName, Expression>> set_clause;
  // kInsert: target column list (empty = all columns in schema order) and the
  // single row's value expressions evaluated against the unmatched source row.
  std::vector<std::string> insert_columns;
  std::vector<Expression> insert_values;
};

struct MergeWhenClause {
  enum class MatchType : uint8_t {
    kMatched,
    kNotMatchedByTarget,
    kNotMatchedBySource,
  };

  MatchType match_type{MatchType::kMatched};
  // Search condition; empty = unconditional.
  Expression condition;
  MergeAction action;
};

class MergeStatement : public Statement {
 public:
  MergeStatement(std::string target_table,
                 std::shared_ptr<SelectStatement> source,
                 std::string source_alias, Expression on_condition)
      : Statement(StatementType::kMerge),
        target_table_(std::move(target_table)),
        source_(std::move(source)),
        source_alias_(std::move(source_alias)),
        on_condition_(std::move(on_condition)) {}

  [[nodiscard]] const std::string& TargetTable() const { return target_table_; }
  [[nodiscard]] const std::shared_ptr<SelectStatement>& Source() const {
    return source_;
  }
  [[nodiscard]] const std::string& SourceAlias() const { return source_alias_; }
  [[nodiscard]] const Expression& OnCondition() const { return on_condition_; }
  // True when this MERGE was produced by desugaring `UPDATE ... FROM` (the
  // visitor lowers that statement shape onto the MERGE machinery).  Diagnostics
  // and server command tags report UPDATE for such statements so scripts see
  // the statement the user wrote, not the implementation vehicle.
  [[nodiscard]] bool DesugaredFromUpdate() const {
    return desugared_from_update_;
  }
  void SetDesugaredFromUpdate(bool value) { desugared_from_update_ = value; }
  [[nodiscard]] const std::vector<MergeWhenClause>& Clauses() const {
    return clauses_;
  }
  void SetClauses(std::vector<MergeWhenClause> clauses) {
    clauses_ = std::move(clauses);
  }
  void Dump(std::ostream& o) const override {
    o << "target=" << target_table_ << " using=" << source_alias_
      << " on=" << (on_condition_ ? on_condition_->ToString() : "(null)")
      << " clauses=" << clauses_.size();
  }

 private:
  std::string target_table_;
  std::shared_ptr<SelectStatement> source_;
  std::string source_alias_;
  Expression on_condition_;
  bool desugared_from_update_{false};
  std::vector<MergeWhenClause> clauses_;
};
class DeleteStatement : public Statement {
 public:
  DeleteStatement(std::string table_name, Expression where_clause)
      : Statement(StatementType::kDelete),
        table_name_(std::move(table_name)),
        where_clause_(std::move(where_clause)) {}

  [[nodiscard]] const std::string& TableName() const { return table_name_; }
  [[nodiscard]] const std::string& Alias() const { return alias_; }
  void SetAlias(std::string alias) { alias_ = std::move(alias); }
  [[nodiscard]] const Expression& WhereClause() const { return where_clause_; }
  // -1 = no ASSERT_ROWS_MODIFIED clause.
  [[nodiscard]] int64_t AssertRowsModified() const {
    return assert_rows_modified_;
  }
  void SetAssertRowsModified(int64_t expected) {
    assert_rows_modified_ = expected;
  }
  [[nodiscard]] bool HasAssert() const { return assert_rows_modified_ >= 0; }
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
  std::string alias_;
  Expression where_clause_;
  int64_t assert_rows_modified_{-1};
};

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_STATEMENT_HPP
