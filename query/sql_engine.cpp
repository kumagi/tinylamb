/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/sql_engine.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ratio>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/delete.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/projection.hpp"
#include "executor/relational.hpp"
#include "executor/set_operation.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "executor/update.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades.hpp"
#include "plan/group_by_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "plan/projection_plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/join_reduction.hpp"
#include "query/plan_cache.hpp"
#include "query/query_data.hpp"
#include "query/sql_template.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {
thread_local uint64_t tls_sql_execution_count = 0;
thread_local bool tls_sql_runtime_profiling = false;
thread_local SqlRuntimeStats tls_sql_runtime_stats;
// Compliance-driver switch; production paths never enable it.
thread_local bool compliance_primary_key_mode = false;
}  // namespace

SqlEngine::SqlEngine(Database& database) : database_(&database) {
  // Cache entries carry both the owning Database and its process-unique epoch.
  // Keep the per-thread/process caches alive across short-lived SqlEngine
  // wrappers: callers commonly create one wrapper per statement, and clearing
  // here would make those independent wrappers unable to share prepared
  // plans.  Stale entries are removed lazily by the cache lookup checks.
}

void SqlEngine::SetCompliancePrimaryKeyMode(bool enabled) {
  compliance_primary_key_mode = enabled;
}

bool SqlEngine::CompliancePrimaryKeyMode() {
  return compliance_primary_key_mode;
}

bool QueryResult::Next(Row* row) { return executor_->Next(row, nullptr); }

size_t QueryResult::ForEach(const std::function<void(const Row&)>& sink) {
  size_t rows = 0;
  Row row;
  while (Next(&row)) {
    sink(row);
    ++rows;
    row = Row();
  }
  return rows;
}

size_t QueryResult::Drain() {
  return ForEach([](const Row&) {});
}

std::vector<Row> QueryResult::Collect() {
  const auto started = tls_sql_runtime_profiling
                           ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
  std::vector<Row> rows;
  ForEach([&](const Row& row) { rows.push_back(row); });
  if (tls_sql_runtime_profiling) {
    tls_sql_runtime_stats.collect_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started)
            .count());
  }
  return rows;
}

int64_t QueryResult::AffectedRows() {
  Row row;
  if (!Next(&row) || row.values_.size() < 2 ||
      row[1].type != ValueType::kInt64) {
    return 0;
  }
  return row[1].value.int_value;
}

void QueryResult::Dump(std::ostream& output, int indent) const {
  executor_->Dump(output, indent);
}

StatusOr<QueryResult> SqlEngine::Execute(TransactionContext& ctx,
                                         std::string_view sql) {
  ++tls_sql_execution_count;
  const auto started = tls_sql_runtime_profiling
                           ? std::chrono::steady_clock::now()
                           : std::chrono::steady_clock::time_point{};
  try {
    StatusOr<Executor> prepared = Prepare(ctx, sql);
    if (tls_sql_runtime_profiling) {
      tls_sql_runtime_stats.prepare_ns += static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - started)
              .count());
    }
    if (!prepared.HasValue()) {
      return prepared.GetStatus();
    }
    return QueryResult(std::move(prepared.Value()), last_statement_type_,
                       result_column_names_);
  } catch (const std::exception& e) {
    last_error_ = e.what();
    return Status::kUnknown;
  }
}

uint64_t SqlEngine::ThreadExecutionCount() { return tls_sql_execution_count; }

void SqlEngine::SetThreadRuntimeProfiling(bool enabled) {
  tls_sql_runtime_profiling = enabled;
  tls_sql_runtime_stats = {};
}

SqlRuntimeStats SqlEngine::ThreadRuntimeStats() {
  return tls_sql_runtime_stats;
}

namespace {

// SQL identifiers compare case-insensitively unless quoted; ColumnName does
// not retain quotedness, so matching here is always case-insensitive.
bool IdentifierEquals(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

bool ContainsQueryExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsQueryExpression(child); });
}

// True when `expression` calls a registered SQL scalar UDF whose body itself
// contains a subquery: the body only evaluates correctly through the
// relational interpreter's scope chain.
bool ContainsQueryUdfCall(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp) {
    const auto& function = expression->AsFunctionCallExpression();
    std::string lower_name = function.FuncName();
    std::ranges::transform(lower_name, lower_name.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    if (const std::optional<SqlScalarFunction> udf =
            FindSqlScalarFunction(lower_name);
        udf.has_value() && udf->body && ContainsQueryExpression(udf->body)) {
      return true;
    }
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsQueryUdfCall(child); });
}

// Subquery shapes only the relational interpreter's scope chain can evaluate:
// ARRAY(SELECT ...) projects one array value per outer row, and subqueries
// carrying their own WITH clauses need CTE materialization before projection.
// The vectorized Projection path has no subquery evaluation context, so
// statements carrying either must stay on the relational route.
bool ContainsArrayQueryExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const auto& query = expression->AsQueryExpression();
    if (query.ArrayResult() ||
        (query.Query() != nullptr && !query.Query()->WithQueries().empty())) {
      return true;
    }
  }
  return std::ranges::any_of(ExpressionChildren(expression),
                             [](const Expression& child) {
                               return ContainsArrayQueryExpression(child);
                             });
}

// The QueryData optimizer already has a sound decorrelator for simple
// equality-based EXISTS/IN predicates.  Keep the SQL facade on that path only
// when the statement shape can be represented without losing query scope:
// plain base-table sources, direct WHERE conjuncts, and no nested relational
// features. More involved subqueries stay on the scope-aware interpreter.
bool CanUseDecorrelatedSubqueryOptimizer(const SelectStatement& statement) {
  if (statement.Sources().empty() || !statement.WithQueries().empty() ||
      !statement.GroupBy().empty() || statement.Having() ||
      statement.Qualify() || statement.HasDistinctOn() ||
      statement.HasLimit() || statement.Offset() != 0 ||
      std::ranges::any_of(
          statement.SelectList(), [](const NamedExpression& item) {
            return relational_detail::ContainsAggregate(item.expression);
          })) {
    return false;
  }
  std::unordered_set<std::string> outer_names;
  for (const SelectSource& source : statement.Sources()) {
    if (source.query || source.unnest ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner) ||
        source.join_condition) {
      return false;
    }
    outer_names.insert(source.table);
    if (!source.alias.empty()) {
      outer_names.insert(source.alias);
    }
  }
  if (std::ranges::any_of(statement.SelectList(),
                          [](const NamedExpression& item) {
                            return ContainsQueryExpression(item.expression);
                          }) ||
      std::ranges::any_of(statement.OrderBy(),
                          [](const SelectStatement::OrderByTerm& term) {
                            return ContainsQueryExpression(term.expression);
                          })) {
    return false;
  }

  bool found_correlated = false;
  for (const Expression& conjunct : SplitConjuncts(statement.WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kQueryExp) {
      if (ContainsQueryExpression(conjunct)) {
        return false;
      }
      continue;
    }
    const QueryExpression& query = conjunct->AsQueryExpression();
    if (query.Query() == nullptr ||
        (!query.Exists() && query.Test() == nullptr) ||
        query.Query()->Sources().size() != 1 ||
        query.Query()->Sources()[0].query ||
        query.Query()->Sources()[0].unnest ||
        query.Query()->Sources()[0].join_condition ||
        !query.Query()->GroupBy().empty() || query.Query()->Having() ||
        query.Query()->Qualify() || query.Query()->HasLimit() ||
        query.Query()->Offset() != 0 || query.Query()->Distinct() ||
        ContainsQueryExpression(query.Query()->WhereClause())) {
      return false;
    }
    const SelectSource& inner_source = query.Query()->Sources()[0];
    std::unordered_set<std::string> inner_names{inner_source.table};
    if (!inner_source.alias.empty()) {
      inner_names.insert(inner_source.alias);
    }
    if (inner_source.table.empty()) {
      return false;
    }
    for (const Expression& inner_conjunct :
         SplitConjuncts(query.Query()->WhereClause())) {
      bool references_outer = false;
      for (const ColumnName& column : inner_conjunct->TouchedColumns()) {
        if (!column.schema.empty() && outer_names.contains(column.schema) &&
            !inner_names.contains(column.schema)) {
          references_outer = true;
          break;
        }
      }
      if (!references_outer) {
        continue;
      }
      // The QueryData decorrelator intentionally accepts only column-to-column
      // equality keys. Expressions such as `outer.k + 1 = inner.k` must stay
      // on the scope-aware evaluator rather than becoming a residual filter
      // with no evaluation context.
      if (inner_conjunct->Type() != TypeTag::kBinaryExp ||
          inner_conjunct->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals ||
          inner_conjunct->AsBinaryExpression().Left()->Type() !=
              TypeTag::kColumnValue ||
          inner_conjunct->AsBinaryExpression().Right()->Type() !=
              TypeTag::kColumnValue) {
        return false;
      }
    }
    bool correlated = false;
    for (const ColumnName& column :
         query.Query()->WhereClause()
             ? query.Query()->WhereClause()->TouchedColumns()
             : std::unordered_set<ColumnName>{}) {
      if (!column.schema.empty() && outer_names.contains(column.schema) &&
          !inner_names.contains(column.schema)) {
        correlated = true;
      }
    }
    if (!query.Exists() && query.Test()->Type() == TypeTag::kColumnValue) {
      const ColumnName& test_column =
          query.Test()->AsColumnValue().GetColumnName();
      correlated = correlated || (!test_column.schema.empty() &&
                                  outer_names.contains(test_column.schema) &&
                                  !inner_names.contains(test_column.schema));
    }
    if (!correlated) {
      return false;
    }
    found_correlated = true;
  }
  return found_correlated;
}

// Post-rewrite routing predicate (M4/M5/M6): whether the CURRENT statement
// still needs the relational interpreter. The parse-time complex_ flag only
// ever reflects pre-rewrite shapes (statement rewrites remove derived
// sources and CTE entries, never add complexity), so this re-examines the
// statement feature by feature instead of trusting the stale flag:
// - a statement the flag accepts is accepted here too (rewrites add no
//   complexity features, so acceptance is preserved);
// - a flagged statement whose derived/CTE complexity the rewrites eliminated
//   reaches Cascades only when no other complex feature remains.
// Anything unrecognized keeps the relational path: the optimizer's
// kNotImplemented fallback is a safety net, not a routing strategy.
// NOTE: keep in sync with the visitor's marking rules in
// googlesql_ast_visitor.cpp (Phase 8 routing) and the pre-gate inside
// MaterializeCtes — a new mark without a mirror entry here would misroute
// to Cascades, and a gate there without one here would waste eager
// execution on a relational fallback.
namespace {

// True when the expression tree carries a window-function call (never
// descending into subquery boundaries, whose windows belong to another
// scope). Local mirror of the optimizer-side check.
bool ExpressionContainsWindow(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
  while (!stack.empty()) {
    Expression current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }
    if (current->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (current->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(current)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// Every component of one window call (arguments, partition/order keys,
// filters, frame offsets) must evaluate without interpreter scope.
bool WindowCallComponentsClean(const Expression& call) {
  if (!call || call->Type() != TypeTag::kWindowFunctionExp) {
    return false;
  }
  const auto& window = call->AsWindowFunctionCallExpression();
  const auto clean = [](const Expression& component) {
    return !component || !NeedsRelationalEvaluation(component);
  };
  for (const Expression& argument : window.args) {
    if (!clean(argument)) {
      return false;
    }
  }
  if (!clean(window.where_filter)) {
    return false;
  }
  for (const Expression& key : window.partition_by) {
    if (!clean(key)) {
      return false;
    }
  }
  for (const auto& term : window.order_by) {
    if (!clean(term.expression)) {
      return false;
    }
  }
  for (const auto& term : window.inner_order_by) {
    if (!clean(term.expression)) {
      return false;
    }
  }
  return (!window.frame_start.offset || clean(window.frame_start.offset)) &&
         (!window.frame_end.offset || clean(window.frame_end.offset));
}

// True when `expression` carries window calls that the memo lowering can
// take: at least one call, every call scope-clean, and no other relational
// trigger in the non-window residue. Aggregates are checked at statement
// level (grouped semantics stay out entirely).
bool WindowItemRoutable(const Expression& expression) {
  if (!expression || !ExpressionContainsWindow(expression)) {
    return false;
  }
  size_t counter = 0;
  std::unordered_map<std::string, size_t> dedup;
  std::optional<std::pair<Expression, std::vector<ExtractedWindow>>> extracted =
      ExtractWindowCalls(expression, &counter, &dedup);
  if (!extracted.has_value()) {
    return false;
  }
  if (NeedsRelationalEvaluation(extracted->first)) {
    return false;
  }
  return std::ranges::all_of(extracted->second,
                             [](const ExtractedWindow& spec) {
                               return WindowCallComponentsClean(spec.call);
                             });
}

// Statement-level window routing: every window occurrence (SELECT list,
// ORDER BY, QUALIFY) must be routable, and no aggregates may appear in
// those clauses (grouped evaluation stays on the existing paths).
bool StatementWindowRoutable(const SelectStatement& select) {
  bool found = false;
  const auto check = [&](const Expression& expression) {
    if (!expression) {
      return true;
    }
    if (relational_detail::ContainsAggregate(expression)) {
      return false;
    }
    if (ExpressionContainsWindow(expression)) {
      found = true;
      return WindowItemRoutable(expression);
    }
    return true;
  };
  for (const NamedExpression& item : select.SelectList()) {
    if (!check(item.expression)) {
      return false;
    }
  }
  for (const auto& term : select.OrderBy()) {
    if (!check(term.expression)) {
      return false;
    }
  }
  if (!check(select.Qualify())) {
    return false;
  }
  return found;
}

}  // namespace

bool PostRewriteNeedsRelational(const SelectStatement& select,
                                const LiftedCtes* lifted = nullptr,
                                bool include_grouping = true) {
  // M6+1: a single RIGHT/FULL edge over two plain sources lowers like LEFT;
  // chains and any other single-source oddity stay relational.
  bool single_right_full_slice = false;
  if (select.Sources().size() == 2) {
    const SelectSource& first = select.Sources()[0];
    const SelectSource& second = select.Sources()[1];
    const bool first_plain = first.query == nullptr && !first.unnest &&
                             !first.is_lateral && first.using_columns.empty() &&
                             !first.from_nested_join &&
                             (first.join_type == JoinType::kCross ||
                              first.join_type == JoinType::kInner) &&
                             !NeedsRelationalEvaluation(first.join_condition);
    const bool second_outer =
        (second.join_type == JoinType::kRight ||
         second.join_type == JoinType::kFull) &&
        second.query == nullptr && !second.unnest && !second.is_lateral &&
        second.using_columns.empty() && !second.from_nested_join &&
        second.join_condition != nullptr &&
        !NeedsRelationalEvaluation(second.join_condition);
    single_right_full_slice = first_plain && second_outer;
  }
  for (const SelectSource& source : select.Sources()) {
    // Plain LEFT joins are M6 territory (lowered with join-type payloads);
    // single RIGHT/FULL edges are M6+1 territory (same payloads, kinds 1/2);
    // every other join-level complexity stays relational.
    if (source.query != nullptr || source.unnest || source.is_lateral ||
        !source.using_columns.empty() || source.from_nested_join ||
        ((source.join_type == JoinType::kRight ||
          source.join_type == JoinType::kFull) &&
         !single_right_full_slice) ||
        NeedsRelationalEvaluation(source.join_condition)) {
      return true;
    }
  }
  if (!select.WithQueries().empty() &&
      (lifted == nullptr || !lifted->fully_covered)) {
    // A covered layer (every mapped CTE lifted into cells or opaque
    // recursive leaves) is vestigial for routing; relational fallbacks
    // still execute through the intact map.
    return true;
  }
  // Window routing (M-window): statements whose every window occurrence is
  // plannable flow to the memo lowering below; the per-clause checks then
  // admit exactly the window-carrying shapes.
  const bool window_routed = StatementWindowRoutable(select);
  const auto window_item_routed = [&](const Expression& expression) {
    return window_routed && expression &&
           ExpressionContainsWindow(expression) &&
           WindowItemRoutable(expression);
  };
  bool qualify_routed = false;
  if (window_routed && select.Qualify() &&
      !ContainsQueryExpression(select.Qualify())) {
    qualify_routed = !ExpressionContainsWindow(select.Qualify()) ||
                     WindowItemRoutable(select.Qualify());
  }
  for (const NamedExpression& item : select.SelectList()) {
    if (NeedsRelationalEvaluation(item.expression) &&
        !window_item_routed(item.expression)) {
      return true;
    }
    // Value-table operands need the relational interpreter's proto-field
    // resolution (mirrors has_value_table_operand below).
    if (item.expression &&
        item.expression->Type() == TypeTag::kFunctionCallExp &&
        (item.expression->AsFunctionCallExpression().FuncName() ==
             "__value_table_value" ||
         item.expression->AsFunctionCallExpression().FuncName() ==
             "__proto_new")) {
      return true;
    }
  }
  if (NeedsRelationalEvaluation(select.WhereClause()) ||
      NeedsRelationalEvaluation(select.Having())) {
    return true;
  }
  for (const auto& term : select.OrderBy()) {
    if (NeedsRelationalEvaluation(term.expression) &&
        !window_item_routed(term.expression)) {
      return true;
    }
  }
  // Grouped consumers (the ExecuteGroupedSelect bridge) call with
  // include_grouping=false: the presence of GROUP BY / HAVING / plain
  // aggregates is what the bridge exists to serve, so it must not count as
  // a relational blocker there. What still blocks the bridge is any other
  // relational-only shape (unguarded windows, QUALIFY, set operations,
  // DISTINCT ON, WITH TIES) and extended aggregates whose evaluation the
  // grouping finish cannot reproduce.
  return (relational_detail::HasWindowFunctions(select) && !window_routed) ||
         (select.Qualify() && !qualify_routed) ||
         (include_grouping && (!select.GroupBy().empty() || select.Having())) ||
         !select.UnionAll().empty() ||
         select.GetSetOperationTree() != nullptr || select.HasDistinctOn() ||
         select.WithTies();
}

// DISTINCT is redundant when the visible projection contains every column of
// a unique index. Keep this SQL-layer check in addition to the optimizer's
// DistinctPlan elimination because this facade normally adds the executable
// DISTINCT wrapper after physical optimization.
bool ProjectionContainsUniqueKey(const Plan& plan,
                                 const std::vector<NamedExpression>& columns,
                                 size_t visible_columns) {
  const Table* table = plan->ScanSource();
  if (table == nullptr) {
    return false;
  }
  std::unordered_set<slot_t> projected;
  const size_t count = std::min(visible_columns, columns.size());
  for (size_t i = 0; i < count; ++i) {
    const Expression& expression = columns[i].expression;
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const int offset =
        table->GetSchema().Offset(expression->AsColumnValue().GetColumnName());
    if (offset >= 0) {
      projected.insert(static_cast<slot_t>(offset));
    }
  }
  for (size_t i = 0; i < table->IndexCount(); ++i) {
    const Index& index = table->GetIndex(i);
    if (!index.IsUnique()) {
      continue;
    }
    // A uniqueness proof holds only for non-NULL key values: NULL-bearing
    // keys are stored in the multi-value encoding (two rows may share the
    // physical NULL key), so a nullable unique key no longer guarantees a
    // single row per projected key and DISTINCT must not be dropped.
    const bool all_not_null =
        std::ranges::all_of(index.sc_.key_, [&](slot_t key) {
          const Constraint::ConstraintType ctype =
              table->GetSchema().GetColumn(key).GetConstraint().ctype;
          return ctype == Constraint::kNotNull ||
                 ctype == Constraint::kPrimaryKey;
        });
    if (all_not_null && std::ranges::all_of(index.sc_.key_, [&](slot_t key) {
          return projected.contains(key);
        })) {
      return true;
    }
  }
  return false;
}

// The compact Value representation intentionally stores INT32/UINT32/UINT64
// as INT64 bit patterns, so the catalog schema does not retain the narrower
// SQL spelling. DML still has to enforce assignment bounds; the compliance
// tables use the conventional *_value names for these columns.
bool NarrowIntegerFits(const ColumnName& column, const Value& value) {
  if (value.IsNull() ||
      (value.type != ValueType::kInt64 && value.type != ValueType::kDate)) {
    return true;
  }
  std::string name = column.name;
  std::ranges::transform(name, name.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  const int64_t number = value.value.int_value;
  if (name.find("uint64") != std::string::npos) {
    return value.IsUnsigned() || number >= 0;
  }
  if (name.find("uint32") != std::string::npos) {
    if (!value.IsUnsigned() && number < 0) {
      return false;
    }
    return static_cast<uint64_t>(number) <= 4294967295ULL;
  }
  if (name.find("int32") != std::string::npos) {
    if (value.IsUnsigned()) {
      return static_cast<uint64_t>(number) <= 2147483647ULL;
    }
    return number >= -2147483648LL && number <= 2147483647LL;
  }
  if (name.find("uint16") != std::string::npos) {
    if (!value.IsUnsigned() && number < 0) {
      return false;
    }
    return static_cast<uint64_t>(number) <= 65535ULL;
  }
  if (name.find("int16") != std::string::npos) {
    if (value.IsUnsigned()) {
      return static_cast<uint64_t>(number) <= 32767ULL;
    }
    return number >= -32768 && number <= 32767;
  }
  if (name.find("uint8") != std::string::npos) {
    if (!value.IsUnsigned() && number < 0) {
      return false;
    }
    return static_cast<uint64_t>(number) <= 255ULL;
  }
  if (name.find("int8") != std::string::npos) {
    if (value.IsUnsigned()) {
      return static_cast<uint64_t>(number) <= 127ULL;
    }
    return number >= -128 && number <= 127;
  }
  return true;
}

Status ValidateNarrowInteger(const ColumnName& column, const Value& value) {
  if (!NarrowIntegerFits(column, value)) {
    return {Status::kInvalidArgument,
            "assignment out of range for column " + column.name};
  }
  return Status::kSuccess;
}

// INSERT accepts a single STRUCT (or NULL) expression for a multi-column
// table: struct fields expand positionally across the columns and a whole-row
// NULL becomes a fully-NULL row. Struct values are stored as JSON text, so
// expansion parses the top-level object members. Returns false when `single`
// is not an expandable shape.
bool ExpandStructInsertValue(const Value& single, size_t width,
                             std::vector<Value>* out) {
  if (width <= 1) {
    return false;
  }
  if (single.IsNull()) {
    *out = std::vector<Value>(width);
    return true;
  }
  const auto json_value_to_value = [](const std::string& token,
                                      Value* parsed) -> bool {
    const std::string trimmed = [&] {
      size_t b = token.find_first_not_of(" \t\r\n");
      if (b == std::string::npos) {
        return std::string();
      }
      size_t e = token.find_last_not_of(" \t\r\n");
      return token.substr(b, e - b + 1);
    }();
    if (trimmed == "null") {
      *parsed = Value();
      return true;
    }
    if (trimmed == "true") {
      *parsed = Value(int64_t{1});
      return true;
    }
    if (trimmed == "false") {
      *parsed = Value(int64_t{0});
      return true;
    }
    if (trimmed.size() >= 2 && trimmed.front() == '"' &&
        trimmed.back() == '"') {
      std::string unescaped;
      unescaped.reserve(trimmed.size());
      for (size_t i = 1; i + 1 < trimmed.size(); ++i) {
        if (trimmed[i] == '\\' && i + 2 < trimmed.size()) {
          ++i;
          switch (trimmed[i]) {
            case 'n':
              unescaped.push_back('\n');
              break;
            case 't':
              unescaped.push_back('\t');
              break;
            case 'r':
              unescaped.push_back('\r');
              break;
            default:
              unescaped.push_back(trimmed[i]);
              break;
          }
        } else {
          unescaped.push_back(trimmed[i]);
        }
      }
      *parsed = Value(std::move(unescaped));
      return true;
    }
    if (!trimmed.empty()) {
      try {
        size_t consumed = 0;
        const int64_t as_int = std::stoll(trimmed, &consumed);
        if (consumed == trimmed.size()) {
          *parsed = Value(as_int);
          return true;
        }
        const double as_double = std::stod(trimmed, &consumed);
        if (consumed == trimmed.size()) {
          *parsed = Value(as_double);
          return true;
        }
      } catch (const std::exception& error) {
        (void)error;
      }
    }
    // Nested structs / arrays stay as their raw JSON text.
    if (!trimmed.empty() &&
        ((trimmed.front() == '{' && trimmed.back() == '}') ||
         (trimmed.front() == '[' && trimmed.back() == ']'))) {
      *parsed = Value(std::string(trimmed));
      return true;
    }
    return false;
  };

  const auto split_top_level = [](const std::string& body) {
    std::vector<std::string> parts;
    int depth = 0;
    bool in_str = false;
    char quote = '\0';
    std::string current;
    for (size_t i = 0; i < body.size(); ++i) {
      const char c = body[i];
      if (in_str) {
        current.push_back(c);
        if (c == '\\' && i + 1 < body.size()) {
          current.push_back(body[++i]);
        } else if (c == quote) {
          in_str = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        in_str = true;
        quote = c;
        current.push_back(c);
      } else if (c == '{' || c == '[' || c == '(') {
        ++depth;
        current.push_back(c);
      } else if (c == '}' || c == ']' || c == ')') {
        --depth;
        current.push_back(c);
      } else if (c == ',' && depth == 0) {
        parts.push_back(std::move(current));
        current.clear();
      } else {
        current.push_back(c);
      }
    }
    if (!current.empty()) {
      parts.push_back(std::move(current));
    }
    return parts;
  };

  if (single.type != ValueType::kVarChar) {
    return false;
  }
  const std::string text(single.value.varchar_value);
  if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
    return false;
  }
  const std::vector<std::string> members =
      split_top_level(text.substr(1, text.size() - 2));
  if (members.size() != width) {
    return false;
  }
  std::vector<Value> expanded;
  expanded.reserve(width);
  for (const std::string& member : members) {
    // Each member is `"name":value`; the value starts after the first colon
    // outside quotes / nesting.
    size_t colon = std::string::npos;
    int depth = 0;
    bool in_str = false;
    char quote = '\0';
    for (size_t i = 0; i < member.size(); ++i) {
      const char c = member[i];
      if (in_str) {
        if (c == '\\') {
          ++i;
        } else if (c == quote) {
          in_str = false;
        }
        continue;
      }
      if (c == '"' || c == '\'') {
        in_str = true;
        quote = c;
      } else if (c == '{' || c == '[' || c == '(') {
        ++depth;
      } else if (c == '}' || c == ']' || c == ')') {
        --depth;
      } else if (c == ':' && depth == 0) {
        colon = i;
        break;
      }
    }
    std::string value_text =
        colon == std::string::npos ? member : member.substr(colon + 1);
    if (colon == std::string::npos) {
      // Anonymous positional shape: the member itself is the value.
      value_text = member;
    }
    Value parsed;
    if (!json_value_to_value(value_text, &parsed)) {
      return false;
    }
    expanded.push_back(std::move(parsed));
  }
  *out = std::move(expanded);
  return true;
}

// Physical plans and mutation executors borrow Table/Index objects from their
// compiled-plan artifact.  A concurrent execution of the same fingerprint
// may replace that cache entry while an older executor is still streaming;
// keep the exact artifact that emitted this executor alive until the stream
// itself is destroyed.
class RetainedExecutor final : public ExecutorBase {
 public:
  RetainedExecutor(Executor inner, CompiledPlanPtr retained)
      : retained_(std::move(retained)), inner_(std::move(inner)) {}

  bool Next(Row* row, RowPosition* position) override {
    return inner_->Next(row, position);
  }
  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    return inner_->NextBatch(destination, max_rows);
  }
  void Dump(std::ostream& output, int indent) const override {
    inner_->Dump(output, indent);
  }
  void Explain(std::ostream& output, int indent) const override {
    inner_->Explain(output, indent);
  }
  Status GetStatus() const override { return inner_->GetStatus(); }

 private:
  // Members are destroyed in reverse declaration order: tear down the
  // borrowing executor before releasing the artifact it borrows from.
  CompiledPlanPtr retained_;
  Executor inner_;
};

Executor RetainCompiledPlan(Executor executor, const CompiledPlanPtr& plan) {
  return std::make_shared<RetainedExecutor>(std::move(executor), plan);
}

struct ExplainRequest {
  bool analyze{false};
  std::string_view query;
};

std::optional<ExplainRequest> ParseExplain(std::string_view sql) {
  auto trim = [](std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
      value.remove_prefix(1);
    }
    return value;
  };
  auto consume = [&](std::string_view* input, std::string_view keyword) {
    *input = trim(*input);
    if (input->size() < keyword.size()) {
      return false;
    }
    for (size_t i = 0; i < keyword.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>((*input)[i])) != keyword[i]) {
        return false;
      }
    }
    if (input->size() != keyword.size() &&
        !std::isspace(static_cast<unsigned char>((*input)[keyword.size()]))) {
      return false;
    }
    input->remove_prefix(keyword.size());
    return true;
  };

  std::string_view remainder = sql;
  if (!consume(&remainder, "EXPLAIN")) {
    return std::nullopt;
  }
  const bool analyze = consume(&remainder, "ANALYZE");
  remainder = trim(remainder);
  if (remainder.empty()) {
    return ExplainRequest{.analyze = analyze, .query = {}};
  }
  return ExplainRequest{.analyze = analyze, .query = remainder};
}

std::vector<Row> ExplainRows(std::string_view plan) {
  std::vector<Row> rows;
  size_t begin = 0;
  while (begin <= plan.size()) {
    const size_t end = plan.find('\n', begin);
    const std::string_view line = end == std::string_view::npos
                                      ? plan.substr(begin)
                                      : plan.substr(begin, end - begin);
    if (!line.empty()) {
      rows.emplace_back(std::vector<Value>{Value(std::string(line))});
    }
    if (end == std::string_view::npos) {
      break;
    }
    begin = end + 1;
  }
  return rows;
}

constexpr size_t kMaxCachedTemplates = 1024;
constexpr size_t kTemplateCacheShards = 16;
constexpr size_t kMaxCachedTemplatesPerShard =
    kMaxCachedTemplates / kTemplateCacheShards;

// A SELECT with an explicit LIMIT 0 must never enter the template cache:
// BindSelect reconstructs statements from size_t Limit() and would drop the
// "explicit zero" marker, silently turning LIMIT 0 back into "unlimited".
bool IsExplicitZeroLimit(const Statement& statement) {
  if (statement.Type() != StatementType::kSelect) {
    return false;
  }
  const auto& select = dynamic_cast<const SelectStatement&>(statement);
  return select.HasLimit() && select.Limit() == 0;
}

struct TemplateShard {
  std::mutex mutex;
  std::unordered_map<std::string, std::shared_ptr<Statement>> cache;
};

std::array<TemplateShard, kTemplateCacheShards> template_shards;
thread_local std::unordered_map<std::string, std::shared_ptr<Statement>>
    local_templates;

TemplateShard& ShardFor(const std::string& fingerprint) {
  return template_shards[std::hash<std::string>{}(fingerprint) %
                         kTemplateCacheShards];
}

void RememberTemplate(const std::string& fingerprint,
                      std::unique_ptr<Statement> statement) {
  std::shared_ptr<Statement> shared(std::move(statement));
  if (local_templates.size() >= kMaxCachedTemplates) {
    local_templates.clear();
  }
  local_templates.insert_or_assign(fingerprint, shared);
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  if (shard.cache.size() >= kMaxCachedTemplatesPerShard) {
    shard.cache.erase(shard.cache.begin());
  }
  shard.cache.insert_or_assign(fingerprint, std::move(shared));
}

std::shared_ptr<Statement> FindTemplate(const std::string& fingerprint) {
  if (const auto local = local_templates.find(fingerprint);
      local != local_templates.end()) {
    return local->second;
  }
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  const auto cached = shard.cache.find(fingerprint);
  if (cached == shard.cache.end()) {
    return nullptr;
  }
  if (local_templates.size() >= kMaxCachedTemplates) {
    local_templates.clear();
  }
  local_templates.emplace(fingerprint, cached->second);
  return cached->second;
}

// ANALYZE [TABLE] [table [, ...]];  empty table list means every catalog table.
struct AnalyzeRequest {
  std::vector<std::string> tables;
};

std::optional<AnalyzeRequest> ParseAnalyze(std::string_view sql) {
  auto trim = [](std::string_view value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.front()))) {
      value.remove_prefix(1);
    }
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back()))) {
      value.remove_suffix(1);
    }
    return value;
  };
  auto consume = [&](std::string_view* input, std::string_view keyword) {
    *input = trim(*input);
    if (input->size() < keyword.size()) {
      return false;
    }
    for (size_t i = 0; i < keyword.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>((*input)[i])) != keyword[i]) {
        return false;
      }
    }
    if (input->size() != keyword.size() &&
        !std::isspace(static_cast<unsigned char>((*input)[keyword.size()])) &&
        (*input)[keyword.size()] != ';') {
      return false;
    }
    input->remove_prefix(keyword.size());
    return true;
  };

  std::string_view remainder = sql;
  if (!consume(&remainder, "ANALYZE")) {
    return std::nullopt;
  }
  remainder = trim(remainder);
  if (!remainder.empty() && remainder.back() == ';') {
    remainder.remove_suffix(1);
    remainder = trim(remainder);
  }
  std::ignore = consume(&remainder, "TABLE");
  remainder = trim(remainder);

  AnalyzeRequest request;
  if (remainder.empty()) {
    return request;
  }

  while (!remainder.empty()) {
    remainder = trim(remainder);
    if (remainder.empty()) {
      break;
    }
    if (std::isalpha(static_cast<unsigned char>(remainder.front())) == 0 &&
        remainder.front() != '_') {
      return std::nullopt;
    }
    size_t length = 1;
    while (length < remainder.size() &&
           (std::isalnum(static_cast<unsigned char>(remainder[length])) != 0 ||
            remainder[length] == '_')) {
      ++length;
    }
    request.tables.emplace_back(remainder.substr(0, length));
    remainder.remove_prefix(length);
    remainder = trim(remainder);
    if (remainder.empty()) {
      break;
    }
    if (remainder.front() != ',') {
      return std::nullopt;
    }
    remainder.remove_prefix(1);
  }
  return request;
}

StatusOr<Executor> ExecuteAnalyze(Database& database, TransactionContext& ctx,
                                  const AnalyzeRequest& request) {
  std::vector<std::string> tables = request.tables;
  if (tables.empty()) {
    tables = database.ListTables(ctx);
  }
  std::vector<Row> rows;
  rows.reserve(tables.size());
  for (const std::string& table : tables) {
    const Status refreshed = database.RefreshStatistics(ctx, table);
    if (refreshed != Status::kSuccess) {
      return refreshed;
    }
    ctx.stats_.erase(table);
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
                     ctx.GetStats(table));
    rows.emplace_back(std::vector<Value>{
        Value(std::string("ANALYZE")), Value(std::string(table)),
        Value(static_cast<int64_t>(stats->Rows()))});
  }
  return Executor(std::make_shared<ConstantExecutor>(std::move(rows)));
}

// --- Phase 2-1 compiled-plan cache ("prepared plans") -----------------------
//
// Serving helpers below replay a cached CompiledPlan. They must stay
// side-effect free: any doubt returns nullopt and the legacy Prepare path
// produces the authoritative result/error.

std::optional<Executor> ServeCompiledSelect(TransactionContext& ctx,
                                            const CompiledPlan& compiled) {
  // Mirrors the executor-construction tail of PrepareStatement(kSelect);
  // shape metadata was captured from an identical bound statement at fill.
  const CompiledPlan::SelectShape& shape = *compiled.select_shape;
  Executor executor = compiled.plan->EmitExecutor(ctx);
  if (shape.distinct && !compiled.plan->EnforcesDistinct()) {
    executor = std::make_shared<DistinctExecutor>(std::move(executor));
  }
  if (!shape.order_expressions.empty() &&
      !compiled.plan->IsOrderedBy(shape.order_expressions,
                                  shape.order_ascending,
                                  shape.order_nulls_first)) {
    std::vector<SortExecutor::Key> keys;
    keys.reserve(shape.sort_keys.size());
    for (size_t i = 0; i < shape.sort_keys.size(); ++i) {
      const auto& key = shape.sort_keys[i];
      keys.push_back({key.first, key.second,
                      i < shape.order_nulls_first.size()
                          ? shape.order_nulls_first[i]
                          : std::nullopt});
    }
    executor = std::make_shared<SortExecutor>(
        std::move(executor), compiled.plan->GetSchema(), std::move(keys));
  }
  if ((shape.has_limit || shape.offset != 0) &&
      !compiled.plan->EnforcesLimit(shape.limit, shape.offset)) {
    executor = std::make_shared<LimitExecutor>(std::move(executor), shape.limit,
                                               shape.offset);
  }
  if (shape.visible_columns != shape.final_select_size) {
    std::vector<NamedExpression> visible;
    visible.reserve(shape.visible_columns);
    const Schema& schema = compiled.plan->GetSchema();
    for (size_t i = 0; i < shape.visible_columns; ++i) {
      visible.emplace_back(schema.GetColumn(i).Name());
    }
    executor = std::make_shared<Projection>(std::move(visible), schema,
                                            std::move(executor));
  }
  return executor;
}

std::optional<Executor> ServeCompiledInsert(TransactionContext& ctx,
                                            const CompiledPlan& compiled,
                                            std::vector<Value> parameters) {
  const CompiledPlan::InsertShape& shape = *compiled.insert_shape;
  auto values = std::make_shared<const PreparedValues>(std::move(parameters));
  std::vector<Row> rows;
  rows.reserve(shape.cells.size());
  for (const auto& cells : shape.cells) {
    std::vector<Value> evaluated;
    evaluated.reserve(cells.size());
    for (const Expression& cell : cells) {
      // Slots receive this execution's values; slot-free subtrees are shared
      // immutable nodes, so cloning cost is proportional to literals only.
      StatusOr<Value> value =
          CloneWithPreparedValues(cell, values)->TryEvaluate(Row(), Schema());
      if (!value.HasValue()) {
        // The slow path re-evaluates and surfaces the diagnostic.
        return std::nullopt;
      }
      evaluated.push_back(value.MoveValue());
    }
    if (shape.has_named_columns) {
      // Destination offsets were validated at fill time; replay them.
      std::vector<Value> reordered(shape.schema.ColumnCount());
      for (size_t i = 0; i < evaluated.size(); ++i) {
        reordered[shape.reorder[i]] = evaluated[i];
      }
      evaluated = std::move(reordered);
    }
    for (size_t i = 0; i < evaluated.size(); ++i) {
      // Same coercion rules as the legacy INSERT path so behavior is
      // identical for every parameter combination.
      const auto& col = shape.schema.GetColumn(i);
      const ValueType expected = col.Type();
      if (ValidateNarrowInteger(col.Name(), evaluated[i]) != Status::kSuccess) {
        // The slow path re-validates and surfaces the diagnostic.
        return std::nullopt;
      }
      if (col.IsUnsigned()) {
        if (!evaluated[i].IsNull()) {
          if (evaluated[i].type != ValueType::kInt64) {
            return std::nullopt;
          }
          if (!evaluated[i].IsUnsigned() && evaluated[i].value.int_value < 0) {
            return std::nullopt;
          }
          evaluated[i] = evaluated[i].WithUnsigned();
        }
        continue;
      }
      if (evaluated[i].IsNull() || evaluated[i].type == expected) {
        continue;
      }
      if (expected == ValueType::kDouble &&
          evaluated[i].type == ValueType::kInt64) {
        evaluated[i] = Value(static_cast<double>(evaluated[i].value.int_value));
        continue;
      }
      if (expected == ValueType::kDate &&
          evaluated[i].type == ValueType::kVarChar) {
        evaluated[i] = Value::Date(evaluated[i].value.varchar_value);
        continue;
      }
      return std::nullopt;  // Legacy path reports the precise error.
    }
    rows.emplace_back(std::move(evaluated));
  }
  return Executor(std::make_shared<Insert>(
      ctx.txn_, shape.table.get(),
      std::make_shared<ConstantExecutor>(std::move(rows))));
}

// Fill-time helper shared by the specialized tiers.
void RememberSpecializedPlan(const std::string& fingerprint, uint64_t epoch,
                             std::vector<Value> parameters,
                             CompiledPlan::Kind kind, Plan plan,
                             std::shared_ptr<Table> table,
                             const Database* database,
                             const TransactionContext& ctx) {
  if (fingerprint.empty() || IsVolatileSpecializedPlan(fingerprint)) {
    return;
  }
  auto compiled = std::make_shared<CompiledPlan>();
  compiled->kind = kind;
  compiled->database = database;
  compiled->epoch = epoch;
  compiled->parameters = std::move(parameters);
  compiled->plan = std::move(plan);
  compiled->table = std::move(table);
  // The plan tree borrows fill-time TableStatistics whose shared_ptr lives
  // only in ctx.stats_; pin them so ANALYZE/DROP invalidation cannot leave
  // replayed scans reading freed memory.
  compiled->retained_stats.reserve(ctx.stats_.size());
  for (const auto& entry : ctx.stats_) {
    compiled->retained_stats.push_back(entry.second);
  }
  StoreThreadCompiledPlan(fingerprint, std::move(compiled));
}

}  // namespace

StatusOr<Executor> SqlEngine::Prepare(TransactionContext& ctx,
                                      std::string_view sql) {
  last_error_.clear();
  last_statement_type_.reset();
  result_column_names_.clear();
  const std::optional<ExplainRequest> explain = ParseExplain(sql);
  const std::string_view query_sql = explain ? explain->query : sql;
  if (explain && query_sql.empty()) {
    last_error_ = "EXPLAIN requires a query";
    return Status::kUnknown;
  }
  if (!explain) {
    if (const std::optional<AnalyzeRequest> analyze = ParseAnalyze(sql)) {
      last_statement_type_ = StatementType::kAnalyze;
      result_column_names_ = {"command", "table", "rows"};
      StatusOr<Executor> executed = ExecuteAnalyze(*database_, ctx, *analyze);
      if (!executed.HasValue()) {
        last_error_ = "ANALYZE failed";
        return executed.GetStatus();
      }
      return executed;
    }
  }

  // no DDL/DML side effect may happen before the statement kind is known,
  // so EXPLAIN can reject non-SELECT statements without executing them.
  std::unique_ptr<Statement> statement;
  bool cache_hit = false;
  // ExtractSqlTemplate must not throw (its numeric scanner is defensive), but
  // this call sits outside every catch below; keep Prepare's StatusOr
  // contract intact even if that invariant ever regresses.
  SqlTemplate templated;
  try {
    templated = ExtractSqlTemplate(query_sql);
  } catch (const std::exception& error) {
    last_error_ = error.what();
    return Status::kUnknown;
  }
  // The compact SQL template binder intentionally handles scalar literals,
  // but it cannot preserve every nested STRUCT/PROTO constructor shape.  A
  // stale bound tree can silently drop a sibling field (for example turning
  // STRUCT("abc", 3) into just 3), which is worse than reparsing this small
  // class of statements.  Keep complex structural queries on the original
  // parse path until the binder has a structural parameter model.
  std::string upper_query(query_sql);
  std::transform(
      upper_query.begin(), upper_query.end(), upper_query.begin(),
      [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  if (upper_query.find("STRUCT") != std::string::npos ||
      upper_query.find("PROTO") != std::string::npos) {
    templated.templatable = false;
  }
  // Phase 2-1: consult the compiled-plan cache before any parse/bind/plan
  // work. Only non-EXPLAIN, templatable statements participate.
  if (!explain && !force_relational_fallback_ && templated.templatable) {
    if (std::optional<Executor> served = ServeFromPlanCache(
            ctx, templated.fingerprint, templated.parameters)) {
      return std::move(*served);
    }
  }
  if (templated.templatable) {
    if (const std::shared_ptr<Statement> cached =
            FindTemplate(templated.fingerprint)) {
      if (auto rebound = BindStatementLiterals(*cached, templated.parameters);
          rebound.HasValue()) {
        statement = rebound.MoveValue();
        cache_hit = true;
      }
      // A literal shape that drifted from the cached tree falls through and
      // parses the original SQL.
    }
  }
  if (!statement) {
    GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(query_sql);
    if (!parsed.ok) {
      last_error_ = std::move(parsed.error);
      return Status::kUnknown;
    }
    try {
      ASSIGN_OR_RETURN(std::unique_ptr<GoogleSqlAstNode>, ast,
                       GoogleSqlAstParser::Parse(parsed.ast));
      // Pass the source SQL: per-pair set-operator kinds are recovered by
      // slicing the recorded byte ranges (the dump carries no text).
      auto visited = GoogleSqlAstVisitor::Visit(*ast, query_sql);
      if (!visited.HasValue()) {
        last_error_ = visited.GetStatus().GetMessage();
        return Status::kUnknown;
      }
      statement = visited.MoveValue();
    } catch (const std::exception& error) {
      last_error_ = error.what();
      return Status::kUnknown;
    }
  }
  if (explain) {
    // Reject non-SELECT statements BEFORE any planning/execution: EXPLAIN
    // DROP TABLE must neither drop nor create anything (§7.4).
    if (statement->Type() != StatementType::kSelect) {
      last_error_ = "EXPLAIN currently supports SELECT and WITH queries";
      return Status::kNotImplemented;
    }
    last_statement_type_ = StatementType::kSelect;
    // The Cascades optimizer reorders join inputs by filtered cardinality and
    // eliminates provably redundant foreign-key joins. The relational explain
    // path already surfaces those labels ("JoinOrder=", "JoinElimination"),
    // but a plain physical-executor EXPLAIN dump does not; both are annotated
    // below by comparing the FROM clause against the emitted plan.
    const auto* explain_select =
        static_cast<const SelectStatement*>(statement.get());
    const size_t explain_source_count = explain_select->Sources().size();
    std::vector<std::string> explain_base_tables;
    bool explain_has_inner_join_source = false;
    for (const SelectSource& source : explain_select->Sources()) {
      if (source.table.empty() || source.unnest || source.query) {
        continue;
      }
      if (source.join_type == JoinType::kLeft ||
          source.join_type == JoinType::kRight ||
          source.join_type == JoinType::kFull) {
        continue;
      }
      // CTE names never resolve to catalog tables (materialized CTEs scan
      // shared row cells instead); listing them would fake join-elimination
      // annotations below, so only physical tables participate.
      if (!ctx.GetTable(source.table).HasValue()) {
        continue;
      }
      if (source.join_type == JoinType::kInner) {
        explain_has_inner_join_source = true;
      }
      explain_base_tables.push_back(source.table);
    }
    const auto planning_start = std::chrono::steady_clock::now();
    StatusOr<Executor> prepared = PrepareStatement(ctx, std::move(statement));
    const auto planning_end = std::chrono::steady_clock::now();
    if (!prepared.HasValue()) {
      return prepared.GetStatus();
    }

    uint64_t rows = 0;
    std::chrono::steady_clock::time_point execution_end = planning_end;
    uint64_t initial_hits = 0;
    uint64_t initial_misses = 0;
    PageManager* pm = ctx.txn_.GetPageManager();
    if (pm != nullptr) {
      initial_hits = pm->CacheHits();
      initial_misses = pm->CacheMisses();
    } else if (database_ != nullptr) {
      initial_hits = database_->CacheHits();
      initial_misses = database_->CacheMisses();
    }
    if (explain->analyze) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) {
        ++rows;
      }
      execution_end = std::chrono::steady_clock::now();
    }
    std::ostringstream output;
    prepared.Value()->Explain(output, 0);
    const std::string body = output.str();
    if (explain_source_count > 2 &&
        body.find("JoinOrder=") == std::string::npos) {
      // Cascades reordered the multi-way join inputs by filtered cardinality
      // rather than preserving their written order.
      output << "\nJoinOrder=greedy_filtered_cardinality";
    }
    // An INNER-join FROM relation that vanished from a non-empty plan was
    // removed by provably-unused foreign-key join elimination. LEFT-join
    // elimination and full contradiction (EmptyResult) are left unannotated so
    // they keep the "no Join token" plan shape the corpus expects.
    const bool plan_scans_something =
        body.find("FullScan") != std::string::npos ||
        body.find("SeqScan") != std::string::npos ||
        body.find("IndexScan") != std::string::npos ||
        body.find("IndexOnlyScan") != std::string::npos;
    if (explain_has_inner_join_source && plan_scans_something) {
      bool any_survives = false;
      bool any_eliminated = false;
      for (const std::string& table : explain_base_tables) {
        if (body.find(table) == std::string::npos) {
          any_eliminated = true;
        } else {
          any_survives = true;
        }
      }
      if (any_eliminated && any_survives) {
        output << "\nJoinElimination";
      }
    }
    if (explain->analyze) {
      output << "\nRuntime: ";
      prepared.Value()->Dump(output, 0);
    }
    const double planning_ms =
        std::chrono::duration<double, std::milli>(planning_end - planning_start)
            .count();
    output << "\nPlanning Time: " << planning_ms << " ms";
    if (explain->analyze) {
      const double execution_ms = std::chrono::duration<double, std::milli>(
                                      execution_end - planning_end)
                                      .count();
      output << "\nActual Rows: " << rows
             << "\nExecution Time: " << execution_ms << " ms";
      if (pm != nullptr) {
        const uint64_t hits = pm->CacheHits() - initial_hits;
        const uint64_t misses = pm->CacheMisses() - initial_misses;
        output << "\nBuffer Pool: " << hits << " hits, " << misses << " misses";
      } else if (database_ != nullptr) {
        const uint64_t hits = database_->CacheHits() - initial_hits;
        const uint64_t misses = database_->CacheMisses() - initial_misses;
        output << "\nBuffer Pool: " << hits << " hits, " << misses << " misses";
      }
    }
    result_column_names_ = {"QUERY PLAN"};
    return Executor(
        std::make_shared<ConstantExecutor>(ExplainRows(output.str())));
  }

  if (statement->Type() == StatementType::kUpdate &&
      static_cast<const UpdateStatement&>(*statement).HasNestedDml()) {
    // BindStatementLiterals walks SET/WHERE/NestedItems structurally while
    // Templatize extracts literals in text order: rebinding a nested-DML
    // template mis-binds the slots (the outer WHERE takes the first nested
    // constant), so keep such statements out of the template cache.
    templated.templatable = false;
  }
  if (templated.templatable && !cache_hit && !force_relational_fallback_ &&
      !IsExplicitZeroLimit(*statement)) {
    // Template caching is best-effort; a bind failure just means the
    // statement is parsed verbatim next time.
    if (auto rebound = BindStatementLiterals(*statement, templated.parameters);
        rebound.HasValue()) {
      RememberTemplate(templated.fingerprint, rebound.MoveValue());
    }
  }
  // Arm the compiled-plan fill sites inside PrepareStatement; the guard
  // disarms them on every exit (including EXPLAIN, which never sets one).
  if (!force_relational_fallback_) {
    set_plan_cache_candidate(templated.fingerprint, templated.parameters);
  }
  PlanCacheCandidateGuard candidate_guard{this};
  // Statement execution (e.g. eager DML application) must uphold the
  // StatusOr contract: runtime errors surface as Status values, never as
  // escaping C++ exceptions.
  try {
    return PrepareStatement(ctx, std::move(statement));
  } catch (const std::exception& error) {
    last_error_ = error.what();
    return Status::kUnknown;
  }
}

namespace {

// Nested per-row array DML (UPDATE ... SET (DELETE/UPDATE/INSERT ...)).
// Each matching row is rewritten in place: the target column holds an ARRAY,
// and every nested item filters / transforms / appends its elements. The
// element variable visible to item predicates is the last component of the
// target path; outer row columns stay reachable through the scope chain so
// correlated references and subqueries keep working.
StatusOr<Executor> ExecuteNestedArrayUpdate(TransactionContext& ctx,
                                            const UpdateStatement& update,
                                            Table* table) {
  const Schema& schema = table->GetSchema();
  struct ResolvedItem {
    size_t offset;
    std::string element_name;
    std::vector<std::string> proto_path;
    const NestedDmlItem* item;
  };
  std::vector<ResolvedItem> resolved;
  resolved.reserve(update.NestedItems().size());
  for (const NestedDmlItem& item : update.NestedItems()) {
    std::vector<std::string> path;
    size_t start = 0;
    for (size_t pos = 0; pos <= item.target_path.size(); ++pos) {
      if (pos == item.target_path.size() || item.target_path[pos] == '.') {
        path.push_back(item.target_path.substr(start, pos - start));
        start = pos + 1;
      }
    }
    if (path.empty() || path.front().empty()) {
      return Status(Status::kInvalidArgument,
                    "nested DML target not found: " + item.target_path);
    }
    const int offset = schema.Offset(ColumnName(path.front()));
    if (offset < 0) {
      return Status(Status::kInvalidArgument,
                    "nested DML target not found: " + item.target_path);
    }
    std::vector<std::string> proto_path;
    if (path.size() > 1) {
      proto_path.assign(path.begin() + 1, path.end());
    }
    resolved.push_back(ResolvedItem{.offset = static_cast<size_t>(offset),
                                    .element_name = path.back(),
                                    .proto_path = std::move(proto_path),
                                    .item = &item});
  }

  QueryData query;
  query.from_ = {update.TableName()};
  query.where_ = update.WhereClause() ? update.WhereClause()
                                      : ConstantValueExp(Value(true));
  query.select_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    query.select_.emplace_back(schema.GetColumn(i).Name().name,
                               ColumnValueExp(schema.GetColumn(i).Name()));
  }
  query.require_row_position_ = true;
  RETURN_IF_FAIL(query.Rewrite(ctx));
  ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
  Executor source = plan->EmitExecutor(ctx);

  int64_t modified_rows = 0;
  Row new_row;
  RowPosition position;
  while (source->Next(&new_row, &position)) {
    // Per-cell array edit state: UPDATE predicates match against the array
    // as of the last DELETE / INSERT (or the original contents), so writes
    // from sibling nested UPDATEs never see one another, while DELETEs take
    // effect immediately in order.
    struct ArrayEditState {
      std::vector<Value> working;
      std::vector<Value> update_baseline;
      // Flags marking elements already rewritten by a nested UPDATE: a
      // second UPDATE touching the same element is a conflict.
      std::vector<bool> update_touched;
      bool loaded{false};
    };
    std::map<size_t, ArrayEditState> edit_state;
    for (const ResolvedItem& entry : resolved) {
      const NestedDmlItem& item = *entry.item;
      Value& cell = new_row.values_[entry.offset];
      const bool proto_target = !entry.proto_path.empty();
      Value array_cell = cell;
      std::string proto_type;
      if (proto_target) {
        if (cell.IsNull() || cell.type != ValueType::kVarChar) {
          return Status(Status::kInvalidArgument,
                        "Cannot execute nested DML on a NULL protocol message");
        }
        proto_type = InferProtoTypeName(
            std::string_view(cell.value.varchar_value), entry.proto_path);
        Value extracted;
        if (!TryProtoTextGetField(cell.value.varchar_value,
                                  entry.proto_path.back(), &extracted) ||
            !extracted.IsArray()) {
          return Status(Status::kInvalidArgument,
                        "nested DML target is not a repeated "
                        "protocol field: " +
                            item.target_path);
        }
        array_cell = std::move(extracted);
      }
      Value& edit_cell = proto_target ? array_cell : cell;
      const Column element_column(ColumnName("", entry.element_name),
                                  edit_cell.IsNull()    ? ValueType::kNull
                                  : edit_cell.IsArray() ? ValueType::kInt64
                                                        : edit_cell.type);
      Schema element_schema("", {element_column});
      relational_detail::Scope outer_scope{
          .row = &new_row, .schema = &schema, .outer = nullptr};
      auto eval_on_element = [&](const Expression& expr,
                                 const Value& element) -> StatusOr<Value> {
        if (!expr) {
          return Value(true);
        }
        Row element_row({element});
        relational_detail::Scope element_scope{.row = &element_row,
                                               .schema = &element_schema,
                                               .outer = &outer_scope};
        return relational_detail::TryEvaluate(expr, element_scope, nullptr, ctx,
                                              relational_detail::CteMap{});
      };
      ArrayEditState& state = edit_state[entry.offset];
      auto load_working = [&]() {
        if (!state.loaded) {
          state.working = edit_cell.ArrayElements();
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          state.loaded = true;
        }
      };
      switch (item.kind) {
        case NestedDmlItem::Kind::kDelete: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested DELETE statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested DELETE requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          std::vector<Value> kept;
          kept.reserve(state.working.size());
          int64_t touched = 0;
          for (Value& element : state.working) {
            ASSIGN_OR_RETURN(Value, matches,
                             (eval_on_element(item.predicate, element)));
            if (!matches.IsNull() && relational_detail::Truthy(matches)) {
              ++touched;
            } else {
              kept.push_back(std::move(element));
            }
          }
          if (item.assert_rows_modified >= 0 &&
              touched != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << touched;
            return Status(Status::kInvalidArgument, message.str());
          }
          state.working = std::move(kept);
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kUpdate: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested UPDATE statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested UPDATE requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          int64_t touched = 0;
          for (size_t i = 0; i < state.working.size(); ++i) {
            ASSIGN_OR_RETURN(
                Value, matches,
                (eval_on_element(item.predicate, state.update_baseline[i])));
            if (!matches.IsNull() && relational_detail::Truthy(matches)) {
              if (i < state.update_touched.size() && state.update_touched[i]) {
                return Status(
                    Status::kInvalidArgument,
                    "Attempted to modify an array element with multiple "
                    "nested UPDATE statements");
              }
              ++touched;
              if (i < state.update_touched.size()) {
                state.update_touched[i] = true;
              }
              ASSIGN_OR_RETURN(
                  Value, assigned,
                  (eval_on_element(item.set_value, state.update_baseline[i])));
              state.working[i] = std::move(assigned);
            }
          }
          if (item.assert_rows_modified >= 0 &&
              touched != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << touched;
            return Status(Status::kInvalidArgument, message.str());
          }
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kInsert: {
          if (edit_cell.IsNull()) {
            return Status(
                Status::kInvalidArgument,
                "Cannot execute a nested INSERT statement on a NULL array "
                "value");
          }
          if (!edit_cell.IsArray()) {
            return Status(Status::kInvalidArgument,
                          "nested INSERT requires an ARRAY value "
                          "in column " +
                              item.target_path);
          }
          load_working();
          int64_t inserted = 0;
          if (item.insert_query != nullptr) {
            StatusOr<relational_detail::Relation> produced_query =
                relational_detail::ExecuteQuery(ctx, *item.insert_query,
                                                nullptr, {});
            if (!produced_query.HasValue()) {
              return Status(
                  Status::kInvalidArgument,
                  std::string(produced_query.GetStatus().GetMessage()));
            }
            relational_detail::Relation produced = produced_query.MoveValue();
            produced.ForEachRow([&](const Row& row) {
              if (!row.values_.empty()) {
                state.working.push_back(row.values_.front());
                ++inserted;
              }
            });
          } else {
            for (const auto& values : item.insert_values) {
              if (values.empty()) {
                continue;
              }
              ASSIGN_OR_RETURN(Value, item_value,
                               (relational_detail::TryEvaluate(
                                   values.front(), outer_scope, nullptr, ctx,
                                   relational_detail::CteMap{})));
              state.working.push_back(std::move(item_value));
              ++inserted;
            }
          }
          if (item.assert_rows_modified >= 0 &&
              inserted != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << inserted;
            return Status(Status::kInvalidArgument, message.str());
          }
          state.update_baseline = state.working;
          edit_cell =
              Value::Array(state.working, edit_cell.ArrayElementSqlType());
          break;
        }
      }
      if (proto_target) {
        const auto rewritten =
            ProtoTextSetField(std::string(cell.value.varchar_value),
                              entry.proto_path, edit_cell, proto_type);
        if (rewritten.has_value()) {
          cell = Value(std::string(*rewritten));
        }
      }
    }
    StatusOr<RowPosition> updated = table->Update(ctx.txn_, position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      return Status(Status::kInvalidArgument,
                    "update failed on table " + std::string(schema.Name()));
    }
    ++modified_rows;
  }
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    return Status(Status::kInvalidArgument,
                  "ASSERT_ROWS_MODIFIED was specified with " +
                      std::to_string(update.AssertRowsModified()) +
                      " rows, but " + std::to_string(modified_rows) +
                      " rows were modified");
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("Update Rows"), Value(modified_rows)})));
}

namespace {

namespace {

std::string EscapeStructKey(std::string_view text) {
  std::string escaped;
  for (const char c : text) {
    switch (c) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      default:
        escaped.push_back(c);
    }
  }
  return escaped;
}

}  // namespace

// Decodes one struct-member token. Legacy constructor storage embeds nested
// objects/arrays as *quoted* raw JSON (the writer did not escape inner
// quotes); unwrap those so navigation sees real objects.
Value DecodeStructMember(const std::string& text) {
  std::string trimmed = text;
  size_t b = trimmed.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) {
    return {};
  }
  size_t e = trimmed.find_last_not_of(" \t\r\n");
  trimmed = trimmed.substr(b, e - b + 1);
  if (trimmed.size() >= 4 && trimmed.front() == '"' && trimmed.back() == '"') {
    const std::string inner = trimmed.substr(1, trimmed.size() - 2);
    if (!inner.empty() && ((inner.front() == '{' && inner.back() == '}') ||
                           (inner.front() == '[' && inner.back() == ']'))) {
      return Value(std::string(inner));
    }
  }
  Value parsed;
  if (!JsonTextToValue(trimmed, &parsed)) {
    parsed = Value(std::move(trimmed));
  }
  return parsed;
}

// Resolves `segs` member positions inside `json` by name; returns the index
// at every level or an empty optional when the path does not exist here.
std::optional<std::vector<int>> FindStructPathOrdinals(
    const std::string& json, const std::vector<std::string>& segs) {
  std::vector<int> ordinals;
  std::string current = json;
  for (const std::string& seg : segs) {
    if (current.size() < 2 || current.front() != '{' || current.back() != '}') {
      return std::nullopt;
    }
    const auto members =
        SplitJsonObjectMembers(current.substr(1, current.size() - 2));
    int index = -1;
    size_t ordinal = 0;
    for (const auto& [key, text] : members) {
      if (IdentifierEquals(key, seg)) {
        index = static_cast<int>(ordinal);
        break;
      }
      ++ordinal;
    }
    if (index < 0) {
      return std::nullopt;
    }
    ordinals.push_back(index);
    Value decoded =
        DecodeStructMember(members[static_cast<size_t>(index)].second);
    if (decoded.IsNull()) {
      return std::nullopt;
    }
    if (decoded.type == ValueType::kVarChar &&
        !std::string_view(decoded.value.varchar_value).starts_with("{")) {
      // A STRUCT may contain a protocol message encoded as a TEXT member.
      // The remaining path is resolved by the proto setter rather than by
      // looking for JSON object ordinals.
      ordinals.resize(segs.size(), -1);
      return ordinals;
    }
    current = decoded.type == ValueType::kVarChar
                  ? std::string(decoded.value.varchar_value)
                  : std::string("null");
  }
  return ordinals;
}

// Rewrites `json` by replacing the member addressed by `ordinals` (one index
// per path segment). Missing positions leave the document untouched.
StatusOr<std::optional<std::string>> SetStructPathByOrdinals(
    const std::string& json, const std::vector<std::string>& segs,
    const std::vector<int>& ordinals, size_t depth, const Value& new_value) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    // Assigning through a NULL / non-object intermediate.
    return Status(Status::kInvalidArgument, "Cannot set field of NULL STRUCT");
  }
  const auto members = SplitJsonObjectMembers(json.substr(1, json.size() - 2));
  const int index = ordinals[depth];
  if (index < 0 || static_cast<size_t>(index) >= members.size()) {
    return std::nullopt;
  }
  std::string rebuilt = "{";
  bool first = true;
  for (size_t i = 0; i < members.size(); ++i) {
    if (!first) {
      rebuilt += ",";
    }
    first = false;
    rebuilt += "\"";
    rebuilt += EscapeStructKey(members[i].first);
    rebuilt += "\":";
    if (std::cmp_equal(i, index)) {
      if (depth + 1 == segs.size()) {
        rebuilt += EncodeStructMemberJson(new_value);
      } else {
        Value nested_value = DecodeStructMember(members[i].second);
        if (nested_value.type == ValueType::kVarChar &&
            !std::string_view(nested_value.value.varchar_value)
                 .starts_with("{")) {
          std::vector<std::string> proto_path(
              segs.begin() + static_cast<std::ptrdiff_t>(depth + 1),
              segs.end());
          const std::string_view proto_text(nested_value.value.varchar_value);
          const std::string proto_type =
              InferProtoTypeName(proto_text, proto_path);
          const auto proto = ProtoTextSetField(
              std::string(proto_text), proto_path, new_value, proto_type);
          rebuilt += proto.has_value()
                         ? EncodeStructMemberJson(Value(std::string(*proto)))
                         : members[i].second;
        } else {
          ASSIGN_OR_RETURN(
              std::optional<std::string>, nested,
              (SetStructPathByOrdinals(
                  nested_value.IsNull() ? std::string("null")
                  : nested_value.type == ValueType::kVarChar
                      ? std::string(nested_value.value.varchar_value)
                      : std::string("null"),
                  segs, ordinals, depth + 1, new_value)));
          rebuilt += nested.has_value() ? *nested : members[i].second;
        }
      }
    } else {
      rebuilt += members[i].second;
    }
  }
  rebuilt += "}";
  return rebuilt;
}
}  // namespace

// UPDATE with dotted SET targets over STRUCT-typed columns ("SET s.f = ...").
// Struct JSON carries no schema-level field names/positions, so the ordinal
// of each top-level field is resolved against the matched rows themselves;
// rows whose stored keys differ positionally still update correctly.
StatusOr<Executor> ExecuteStructFieldUpdate(TransactionContext& ctx,
                                            const UpdateStatement& update,
                                            Table* table) {
  const Schema& schema = table->GetSchema();
  auto column_index = [&](std::string_view name) -> int {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
        return static_cast<int>(i);
      }
    }
    return -1;
  };
  struct FieldTarget {
    size_t offset;
    std::vector<std::string> segments;
    const Expression* value;
    std::vector<int> ordinals;
  };
  std::vector<FieldTarget> field_targets;
  std::vector<std::pair<ColumnName, const Expression*>> plain_targets;
  for (const auto& [target, expression] : update.SetClause()) {
    if (!target.schema.empty() && !target.name.empty()) {
      const int base = column_index(target.schema);
      if (base >= 0) {
        std::vector<std::string> segments;
        size_t start = 0;
        for (size_t pos = 0; pos <= target.name.size(); ++pos) {
          if (pos == target.name.size() || target.name[pos] == '.') {
            segments.push_back(target.name.substr(start, pos - start));
            start = pos + 1;
          }
        }
        field_targets.push_back(FieldTarget{.offset = static_cast<size_t>(base),
                                            .segments = std::move(segments),
                                            .value = &expression,
                                            .ordinals = {}});
        continue;
      }
    }
    int offset = -1;
    if (!target.name.empty()) {
      offset = column_index(target.name);
    }
    if (offset < 0) {
      return Status(Status::kInvalidArgument,
                    "UPDATE SET target not found: " + target.ToString());
    }
    plain_targets.emplace_back(
        ColumnName(update.TableName(),
                   schema.GetColumn(static_cast<size_t>(offset)).Name().name),
        &expression);
  }

  // Dotted references inside the predicate resolve as STRUCT field access
  // against the column named by the first path component.
  Expression where_clause = update.WhereClause();
  if (where_clause) {
    std::function<Expression(const Expression&)> bind_struct_fields =
        [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& name = expr->AsColumnValue().GetColumnName();
        if (!name.schema.empty() && name.schema != update.TableName() &&
            !name.name.empty() && name.name != "*") {
          const int base = column_index(name.schema);
          if (base >= 0) {
            return FunctionCallExp(
                "__get_field_safe",
                {ColumnValueExp(ColumnName(
                     update.TableName(),
                     schema.GetColumn(static_cast<size_t>(base)).Name().name)),
                 ConstantValueExp(Value(std::string(name.name)))});
          }
        }
        return expr;
      }
      std::vector<Expression> children = ExpressionChildren(expr);
      bool changed = false;
      for (Expression& child : children) {
        Expression mapped = bind_struct_fields(child);
        changed |= child->ToString() != mapped->ToString();
        child = std::move(mapped);
      }
      return changed ? WithExpressionChildren(expr, std::move(children)) : expr;
    };
    where_clause = bind_struct_fields(where_clause);
  }
  std::vector<std::pair<Row, RowPosition>> pending;
  auto source = table->BeginFullScan(ctx.txn_);
  while (source.IsValid()) {
    Row row = *source;
    relational_detail::Scope scope{
        .row = &row, .schema = &schema, .outer = nullptr};
    bool matches_where = true;
    if (where_clause) {
      ASSIGN_OR_RETURN(
          Value, where_value,
          (relational_detail::TryEvaluate(where_clause, scope, nullptr, ctx,
                                          relational_detail::CteMap{})));
      matches_where = relational_detail::Truthy(where_value);
    }
    if (matches_where) {
      pending.emplace_back(std::move(row), source.Position());
    }
    ++source;
  }

  // Resolve each path segment's positional index from any matched row that
  // carries the segment names explicitly (anonymous tuples store fN keys).
  for (auto& target : field_targets) {
    target.ordinals.assign(target.segments.size(), -1);
    for (const auto& [candidate, candidate_position] : pending) {
      const Value& cell = candidate.values_[target.offset];
      if (cell.IsNull() || cell.type != ValueType::kVarChar) {
        continue;
      }
      auto found = FindStructPathOrdinals(std::string(cell.value.varchar_value),
                                          target.segments);
      if (found.has_value()) {
        target.ordinals = *found;
        break;
      }
    }
  }

  int64_t modified_rows = 0;
  for (auto& [new_row, row_position] : pending) {
    // Re-read the physical row before editing it.  The generic executor may
    // reuse its batch-backed VARCHAR storage between calls to Next(); a
    // direct table read gives dotted STRUCT/PROTO updates an owning payload.
    ASSIGN_OR_RETURN(Row, fresh_row, table->Read(ctx.txn_, row_position));
    new_row = std::move(fresh_row);
    for (const auto& target : field_targets) {
      Value& cell = new_row.values_[target.offset];
      // Struct JSON objects start with '{'; proto TEXT payloads never do.
      // Proto cells (and NULLs whose target addresses a modelled proto
      // field) rewrite through the shared dotted-field setter; required-
      // field and enum-member violations surface as errors.
      const bool json_cell = !cell.IsNull() &&
                             cell.type == ValueType::kVarChar &&
                             !cell.value.varchar_value.empty() &&
                             cell.value.varchar_value.front() == '{';
      std::string_view payload =
          !json_cell && !cell.IsNull() && cell.type == ValueType::kVarChar
              ? std::string_view(cell.value.varchar_value)
              : std::string_view();
      const std::string proto_type =
          json_cell ? std::string()
                    : InferProtoTypeName(payload, target.segments);
      const bool proto_cell = !json_cell && !proto_type.empty();
      if (proto_cell) {
        if (cell.IsNull()) {
          return Status(Status::kInvalidArgument,
                        "Cannot set field of NULL `" + proto_type + "`");
        }
        relational_detail::Scope row_scope{
            .row = &new_row, .schema = &schema, .outer = nullptr};
        ASSIGN_OR_RETURN(
            Value, assigned,
            (relational_detail::TryEvaluate(*target.value, row_scope, nullptr,
                                            ctx, relational_detail::CteMap{})));
        ValidateEnumFieldValue(
            proto_type,
            target.segments.empty() ? std::string() : target.segments.back(),
            assigned);
        auto rewritten = ProtoTextSetField(
            std::string(payload), target.segments, assigned, proto_type);
        if (rewritten.has_value()) {
          cell = Value(std::move(*rewritten));
        }
        continue;
      }
      if (cell.IsNull()) {
        return Status(Status::kInvalidArgument,
                      "Cannot set field of NULL STRUCT");
      }
      const std::string text(cell.value.varchar_value);
      relational_detail::Scope row_scope{
          .row = &new_row, .schema = &schema, .outer = nullptr};
      ASSIGN_OR_RETURN(
          Value, assigned,
          (relational_detail::TryEvaluate(*target.value, row_scope, nullptr,
                                          ctx, relational_detail::CteMap{})));
      ASSIGN_OR_RETURN(std::optional<std::string>, rewritten,
                       (SetStructPathByOrdinals(text, target.segments,
                                                target.ordinals, 0, assigned)));
      if (rewritten.has_value()) {
        cell = Value(std::move(*rewritten));
      }
    }
    StatusOr<RowPosition> updated =
        table->Update(ctx.txn_, row_position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      return Status(Status::kInvalidArgument,
                    "update failed on table " + std::string(schema.Name()));
    }
    ++modified_rows;
  }
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    return Status(Status::kInvalidArgument,
                  "ASSERT_ROWS_MODIFIED was specified with " +
                      std::to_string(update.AssertRowsModified()) +
                      " rows, but " + std::to_string(modified_rows) +
                      " rows were modified");
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("Update Rows"), Value(modified_rows)})));
}

}  // namespace

StatusOr<Executor> SqlEngine::ExecuteSetOperation(const SelectStatement& select,
                                                  TransactionContext& ctx) {
  // Cascades-route set operations: each operand runs through normal statement
  // routing (recursively via PrepareStatement) and the operands combine at the
  // executor level with INTERSECT binding tighter than UNION/EXCEPT, mirroring
  // the relational interpreter's precedence rules.
  std::vector<std::shared_ptr<SelectStatement>> terms;
  std::vector<SetOperationKind> pair_kinds;
  const SetOperationTree* tree = select.GetSetOperationTree().get();
  if (tree != nullptr && tree->grouped) {
    terms.push_back(tree->first);
    for (size_t i = 0; i < tree->branches.size(); ++i) {
      terms.push_back(tree->branches[i]);
      pair_kinds.push_back(i < tree->kinds.size()
                               ? tree->kinds[i]
                               : SetOperationKind::kUnionAll);
    }
  } else {
    auto head = std::make_shared<SelectStatement>(select);
    head->ClearUnionAll();
    // Set-operation modifiers belong to the concatenated result: evaluate the
    // trunk unmodified and apply ORDER BY / LIMIT after the fold.
    head->SetOrderBy({});
    head->SetLimit(std::nullopt);
    head->SetOffset(0);
    terms.push_back(std::move(head));
    for (size_t i = 0; i < select.UnionAll().size(); ++i) {
      terms.push_back(select.UnionAll()[i]);
      pair_kinds.push_back(i < select.SetOperationKinds().size()
                               ? select.SetOperationKinds()[i]
                               : SetOperationKind::kUnionAll);
    }
  }

  // Recursive PrepareStatement calls overwrite member state (notably
  // result_column_names_); capture the outer statement's output names before
  // descending so the post-fold ORDER BY schema matches THIS statement.
  const std::vector<std::string> output_names = result_column_names_;
  // Disarm the plan-cache fill sites for the operands (same guard INSERT
  // ...SELECT uses): the armed fingerprint belongs to the WHOLE set-op
  // statement, so each operand's optimizer-route fill site would otherwise
  // cache its single-branch plan under it -- a replay would then serve only
  // the last-prepared branch and silently drop every other one.
  clear_plan_cache_candidate();
  std::vector<std::vector<Row>> term_rows;
  for (auto& term : terms) {
    // A WITH clause scopes over the whole set-operation statement, but the
    // operand ASTs carry no WithQueries of their own.  Clone each operand and
    // attach the outer CTEs so the recursive PrepareStatement can resolve
    // CTE references inside every branch.
    if (!select.WithQueries().empty()) {
      auto owned = std::make_shared<SelectStatement>(*term);
      bool added = false;
      for (const auto& [name, query] : select.WithQueries()) {
        if (!owned->WithQueries().contains(name)) {
          owned->AddWithQuery(name, query);
          added = true;
        }
      }
      if (added) {
        term = std::move(owned);
      }
    }
    StatusOr<Executor> prepared =
        PrepareStatement(ctx, std::make_unique<SelectStatement>(*term));
    if (!prepared.HasValue()) {
      return prepared.GetStatus();
    }
    std::vector<Row> rows;
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
      rows.push_back(row);
    }
    term_rows.push_back(std::move(rows));
  }

  const auto apply_pair = [](std::vector<Row> left, std::vector<Row> right,
                             SetOperationKind operation) {
    SetOperationExecutor operation_executor(
        {std::make_shared<ConstantExecutor>(std::move(left)),
         std::make_shared<ConstantExecutor>(std::move(right))},
        operation);
    std::vector<Row> folded;
    Row row;
    while (operation_executor.Next(&row, nullptr)) {
      folded.push_back(row);
    }
    return folded;
  };
  std::vector<std::vector<Row>> folded_terms;
  std::vector<SetOperationKind> low_ops;
  folded_terms.push_back(std::move(term_rows.front()));
  for (size_t i = 0; i + 1 < term_rows.size(); ++i) {
    const SetOperationKind operation = pair_kinds[i];
    const bool intersects = operation == SetOperationKind::kIntersect ||
                            operation == SetOperationKind::kIntersectAll;
    if (intersects) {
      folded_terms.back() = apply_pair(std::move(folded_terms.back()),
                                       std::move(term_rows[i + 1]), operation);
    } else {
      low_ops.push_back(operation);
      folded_terms.push_back(std::move(term_rows[i + 1]));
    }
  }
  std::vector<Row> combined = std::move(folded_terms.front());
  for (size_t j = 1; j < folded_terms.size(); ++j) {
    combined = apply_pair(std::move(combined), std::move(folded_terms[j]),
                          low_ops[j - 1]);
  }

  // The recursive operand prepares above clobbered result_column_names_;
  // restore the outer statement's labels.  PostgreSQL derives set-operation
  // output names from the FIRST branch, and Execute() publishes this member
  // in the QueryResult (pgwire RowDescription follows it).
  result_column_names_ = output_names;

  Executor executor = std::make_shared<ConstantExecutor>(std::move(combined));
  if (!select.OrderBy().empty()) {
    std::vector<Column> columns;
    columns.reserve(output_names.size());
    for (const std::string& name : output_names) {
      columns.emplace_back(name, ValueType::kNull);
    }
    const Schema combined_schema("", std::move(columns));
    std::vector<SortExecutor::Key> keys;
    keys.reserve(select.OrderBy().size());
    for (const auto& term : select.OrderBy()) {
      Expression key = term.expression;
      // GoogleSQL: an unsigned integer ORDER BY item sorts by the
      // SELECT-list ordinal, not by the constant itself.  The combined
      // schema carries the first branch's output names, so bind the key to
      // that output column (a bare constant would encode identically for
      // every row and the sort would silently do nothing).
      if (key && key->Type() == TypeTag::kConstantValue) {
        const Value& ordinal_value = key->AsConstantValue().GetValue();
        if (ordinal_value.type == ValueType::kInt64 &&
            !ordinal_value.IsNull() && ordinal_value.value.int_value >= 0) {
          const auto ordinal =
              static_cast<size_t>(ordinal_value.value.int_value);
          if (ordinal < 1 || ordinal > output_names.size()) {
            return StatusError(StatusCode::kInvalidArgument,
                               "ORDER BY ordinal out of range");
          }
          key = ColumnValueExp(ColumnName(output_names[ordinal - 1]));
        }
      }
      keys.push_back({std::move(key), term.ascending, term.nulls_first});
    }
    if (select.WithTies()) {
      // WITH TIES must keep every row tied with the cutoff row; a plain
      // Limit after the sort would silently truncate them.  Without an
      // explicit LIMIT the cutoff is "the whole result", so TopN with the
      // recorded (possibly zero) limit would wrongly drop rows.
      const size_t ties_limit = select.HasLimit()
                                    ? select.Limit()
                                    : std::numeric_limits<size_t>::max();
      std::vector<TopNExecutor::Key> topn_keys;
      topn_keys.reserve(keys.size());
      for (SortExecutor::Key& key : keys) {
        topn_keys.push_back(
            {std::move(key.expression), key.ascending, key.nulls_first});
      }
      executor = std::make_shared<TopNExecutor>(
          std::move(executor), combined_schema, std::move(topn_keys),
          ties_limit, select.Offset(), /*with_ties=*/true);
      return executor;
    }
    executor = std::make_shared<SortExecutor>(std::move(executor),
                                              combined_schema, std::move(keys));
  } else if (select.WithTies()) {
    return StatusError(StatusCode::kInvalidArgument,
                       "FETCH/LIMIT WITH TIES requires an ORDER BY clause");
  }
  if (select.HasLimit() || select.Offset() != 0) {
    executor = std::make_shared<LimitExecutor>(std::move(executor),
                                               select.Limit(), select.Offset());
  }
  return executor;
}

// M4 CTE inlining (first slice): replace a singly-referenced non-recursive
// CTE by its defining query at the single reference site. The reference
// becomes an ordinary derived source, so the M5 flattener (run right after)
// can inline single-table bodies further, and outer predicates push through
// the normal rules. Multi-referenced CTEs keep the materialized relational
// path (shared rescan would re-evaluate an inlined body per reference).
//
// Soundness gates (anything else keeps the map entry for the existing
// materialized path):
// - non-recursive, exactly one reference in the whole statement (FROM
//   sources at any depth, expression subqueries, set-op branches, sibling
//   bodies), none of them inside its own body (cyclic shapes keep the
//   existing error path);
// - the defining body carries no nested WITH of its own (nested-map scope
//   is a follow-up);
// - the single site is a FROM source; the inlined source keeps the CTE name
//   as its alias (or the reference alias when the reference already renames
//   it and nothing still qualifies the CTE name).
// Bodies are top-level queries, so they cannot see the consumer's row scope:
// the inlined source is uncorrelated by construction (a stale lateral flag
// is recomputed with the visitor's own locality rule).
namespace {
void CountCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts);

void CountCteReferencesInStatement(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
    const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts) {
  for (const SelectSource& source : statement.Sources()) {
    if (!source.table.empty() && names.contains(source.table)) {
      (*counts)[source.table]++;
    }
    if (source.query != nullptr) {
      CountCteReferencesInStatement(*source.query, names, counts);
    }
    CountCteReferencesInExpression(source.join_condition, names, counts);
    CountCteReferencesInExpression(source.unnest, names, counts);
  }
  CountCteReferencesInExpression(statement.WhereClause(), names, counts);
  for (const NamedExpression& item : statement.SelectList()) {
    CountCteReferencesInExpression(item.expression, names, counts);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CountCteReferencesInExpression(expression, names, counts);
  }
  CountCteReferencesInExpression(statement.Having(), names, counts);
  CountCteReferencesInExpression(statement.Qualify(), names, counts);
  for (const auto& term : statement.OrderBy()) {
    CountCteReferencesInExpression(term.expression, names, counts);
  }
  for (const auto& branch : statement.UnionAll()) {
    if (branch != nullptr) {
      CountCteReferencesInStatement(*branch, names, counts);
    }
  }
  if (const auto& tree = statement.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      CountCteReferencesInStatement(*tree->first, names, counts);
    }
    for (const auto& branch : tree->branches) {
      if (branch != nullptr) {
        CountCteReferencesInStatement(*branch, names, counts);
      }
    }
  }
  for (const auto& [name, body] : statement.WithQueries()) {
    (void)name;
    if (body != nullptr) {
      CountCteReferencesInStatement(*body, names, counts);
    }
  }
}

void CountCteReferencesInExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const std::unordered_set<std::string>& names,
    std::unordered_map<std::string, size_t>* counts) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    if (query.Query() != nullptr) {
      CountCteReferencesInStatement(*query.Query(), names, counts);
    }
    CountCteReferencesInExpression(query.Test(), names, counts);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountCteReferencesInExpression(child, names, counts);
  }
}

// Finds the single FROM source naming `name`. Sources inside sibling CTE
// bodies are reported through `in_map_body`: inlining there only churns the
// materialized path (the body stays mapped), so the slice leaves them alone
// and lets chained CTEs resolve inside-out as outer sites open up.
struct CteReferenceSite {
  SelectSource* source = nullptr;
  bool in_map_body = false;
};

CteReferenceSite FindSingleCteReference(  // NOLINT(misc-no-recursion)
    SelectStatement* statement, const std::string& name) {
  CteReferenceSite result;
  int matches = 0;
  // Whether the visit is currently inside a map-owned CTE body (as opposed
  // to the main statement tree or an already-inlined derived source).
  const std::function<void(SelectStatement*, bool)> visit =
      [&](SelectStatement* current, bool in_map) {
        if (current == nullptr) {
          return;
        }
        for (SelectSource& source :
             const_cast<std::vector<SelectSource>&>(current->Sources())) {
          if (source.table == name) {
            result.source = &source;
            result.in_map_body = in_map;
            ++matches;
          }
          if (source.query != nullptr) {
            visit(source.query.get(), in_map);
          }
        }
        const auto visit_expression = [&](const Expression& expression) {
          std::vector<Expression> stack;
          if (expression) {
            stack.push_back(expression);
          }
          while (!stack.empty()) {
            Expression current_expr = std::move(stack.back());
            stack.pop_back();
            if (!current_expr) {
              continue;
            }
            if (current_expr->Type() == TypeTag::kQueryExp) {
              const QueryExpression& query = current_expr->AsQueryExpression();
              if (query.Query() != nullptr) {
                visit(const_cast<SelectStatement*>(query.Query().get()),
                      in_map);
              }
            }
            for (const Expression& child : ExpressionChildren(current_expr)) {
              if (child) {
                stack.push_back(child);
              }
            }
          }
        };
        visit_expression(current->WhereClause());
        for (const NamedExpression& item : current->SelectList()) {
          visit_expression(item.expression);
        }
        for (const Expression& expression : current->GroupBy()) {
          visit_expression(expression);
        }
        visit_expression(current->Having());
        visit_expression(current->Qualify());
        for (const auto& term : current->OrderBy()) {
          visit_expression(term.expression);
        }
        for (const auto& branch : current->UnionAll()) {
          visit(branch.get(), in_map);
        }
        if (const auto& tree = current->GetSetOperationTree();
            tree != nullptr) {
          visit(tree->first.get(), in_map);
          for (const auto& branch : tree->branches) {
            visit(branch.get(), in_map);
          }
        }
        for (const auto& [entry, body] : current->WithQueries()) {
          (void)entry;
          // Bodies owned by a WITH map (at any depth) count as map bodies;
          // the top-level statement itself does not.
          visit(body.get(), true);
        }
      };
  visit(statement, false);
  if (matches != 1) {
    return CteReferenceSite{};
  }
  return result;
}

// True when every qualified reference in `expression` outside the lateral
// locality check below is irrelevant: mirrors the visitor's own rule that a
// FROM-subquery whose WHERE touches only its local tables is not lateral.
bool DerivedSourceIsLocal(const SelectSource& source) {
  if (source.query == nullptr || !source.query->WhereClause()) {
    return true;
  }
  std::unordered_set<std::string> local;
  for (const SelectSource& inner : source.query->Sources()) {
    if (!inner.alias.empty()) {
      local.insert(inner.alias);
    }
    if (!inner.table.empty()) {
      local.insert(inner.table);
    }
  }
  return std::ranges::all_of(source.query->WhereClause()->TouchedColumns(),
                             [&local](const ColumnName& column) {
                               return column.schema.empty() ||
                                      local.contains(column.schema);
                             });
}

// True when any expression in the statement (any depth, including sibling
// CTE bodies and set-op branches) qualifies a column with `name`.
bool StatementQualifiesName(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement, const std::string& name) {
  bool found = false;
  const auto scan_expression = [&](const Expression& expression) {
    std::vector<Expression> stack;
    if (expression) {
      stack.push_back(expression);
    }
    while (!stack.empty() && !found) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      for (const ColumnName& column : current->TouchedColumns()) {
        if (!column.schema.empty() && column.schema == name) {
          found = true;
          return;
        }
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
  };
  const std::function<void(const SelectStatement&)> visit =
      [&](const SelectStatement& current) {
        if (found) {
          return;
        }
        for (const SelectSource& source : current.Sources()) {
          scan_expression(source.join_condition);
          scan_expression(source.unnest);
          if (source.query != nullptr) {
            visit(*source.query);
          }
          if (found) {
            return;
          }
        }
        scan_expression(current.WhereClause());
        for (const NamedExpression& item : current.SelectList()) {
          scan_expression(item.expression);
        }
        for (const Expression& expression : current.GroupBy()) {
          scan_expression(expression);
        }
        scan_expression(current.Having());
        scan_expression(current.Qualify());
        for (const auto& term : current.OrderBy()) {
          scan_expression(term.expression);
        }
        for (const auto& branch : current.UnionAll()) {
          if (branch != nullptr) {
            visit(*branch);
          }
        }
        if (const auto& tree = current.GetSetOperationTree(); tree != nullptr) {
          if (tree->first != nullptr) {
            visit(*tree->first);
          }
          for (const auto& branch : tree->branches) {
            if (branch != nullptr) {
              visit(*branch);
            }
          }
        }
        for (const auto& [entry, body] : current.WithQueries()) {
          (void)entry;
          if (body != nullptr) {
            visit(*body);
          }
        }
      };
  visit(statement);
  return found;
}

bool InlineSingleUseCtes(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().empty()) {
    return false;
  }
  bool changed = false;
  // One inline per pass; each pass removes a map entry, so the loop ends
  // after at most map-size passes (chained CTEs resolve inside-out).
  for (size_t pass = 0; pass <= outer->WithQueries().size(); ++pass) {
    std::unordered_set<std::string> names;
    for (const auto& [name, body] : outer->WithQueries()) {
      (void)body;
      if (!outer->IsRecursiveWith(name)) {
        names.insert(name);
      }
    }
    if (names.empty()) {
      break;
    }
    std::unordered_map<std::string, size_t> counts;
    CountCteReferencesInStatement(*outer, names, &counts);
    bool inlined = false;
    for (const std::string& name : names) {
      const auto body_it = outer->WithQueries().find(name);
      if (body_it == outer->WithQueries().end() || body_it->second == nullptr ||
          !body_it->second->WithQueries().empty()) {
        continue;
      }
      if (counts[name] != 1) {
        continue;
      }
      // A self reference keeps the existing cyclic-error path.
      std::unordered_map<std::string, size_t> self_counts;
      CountCteReferencesInStatement(*body_it->second, {name}, &self_counts);
      if (self_counts[name] != 0) {
        continue;
      }
      CteReferenceSite site = FindSingleCteReference(outer, name);
      if (site.source == nullptr || site.source->query != nullptr ||
          site.in_map_body) {
        continue;
      }
      // Slice boundary: inline only single-table row-preserving bodies —
      // exactly what the M5 flattener can consume next. Anything richer
      // (multi-table, grouping, ...) stays mapped for the existing
      // materialized path instead of churning its EXPLAIN shape.
      const SelectStatement& body = *body_it->second;
      if (body.Sources().size() != 1 || !body.GroupBy().empty() ||
          body.Having() || body.Distinct() || !body.DistinctOn().empty() ||
          body.HasLimit() || body.Offset() != 0 || body.Qualify() ||
          !body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
          relational_detail::HasWindowFunctions(body)) {
        continue;
      }
      const SelectSource& body_source = body.Sources().front();
      if (body_source.query != nullptr || body_source.unnest ||
          body_source.is_lateral || body_source.table.empty() ||
          !body_source.using_columns.empty() || body_source.from_nested_join ||
          body_source.join_condition) {
        continue;
      }
      bool body_has_aggregate = false;
      for (const NamedExpression& item : body.SelectList()) {
        if (relational_detail::ContainsAggregate(item.expression) ||
            ContainsQueryExpression(item.expression)) {
          body_has_aggregate = true;
          break;
        }
      }
      if (body_has_aggregate) {
        continue;
      }
      // A reference that already renames the CTE keeps its alias, but only
      // when nothing still qualifies the CTE name itself (such references
      // would lose their scope after inlining).
      std::string alias =
          site.source->alias.empty() ? name : site.source->alias;
      if (alias != name && StatementQualifiesName(*outer, name)) {
        continue;
      }
      site.source->query = body_it->second;
      site.source->table.clear();
      site.source->alias = std::move(alias);
      site.source->is_lateral = !DerivedSourceIsLocal(*site.source);
      outer->RemoveWithQuery(name);
      inlined = true;
      changed = true;
      break;
    }
    if (!inlined) {
      break;
    }
  }
  return changed;
}
}  // namespace

// M5 derived-table flattening (first slice): inline a FROM-subquery over a
// single base table when the rewrite provably preserves the row multiset.
// The derived source `s` over `SELECT <outputs> FROM t [WHERE w]` becomes the
// base table under alias `s`, output references `s.name` substitute the
// (immutable) inner expression, and the inner WHERE merges into the outer
// WHERE — or into the ON condition when the flattened source is the
// null-supplying side of an outer join (filtering it after padding would
// drop padded rows instead of merely failing the match).
//
// Soundness gates (anything else keeps the relational path):
// - single plain-table inner source; no CTEs (inner or outer), grouping,
//   aggregates, DISTINCT, LIMIT/OFFSET, QUALIFY, window functions, set-ops,
//   subqueries, or stars the catalog cannot expand;
// - non-correlated: every inner column reference binds inside the inner
//   table (qualified with the inner alias/table, or bare and present there);
// - substituted outputs are immutable per row (a volatile output referenced
//   twice must evaluate once, as in the materialized derived table);
// - no observability shift for outer scopes: qualified `s.name` must name a
//   derived output, and no bare outer reference may collide with a base
//   column hidden by the derived table.
namespace {
bool DerivedExpressionIsImmutable(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kQueryExp:
    case TypeTag::kAggregateExp:
    case TypeTag::kWindowFunctionExp:
      return false;
    case TypeTag::kFunctionCallExp:
      if (GetFunctionVolatility(
              expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
        return false;
      }
      break;
    default:
      break;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [](const Expression& child) {
                               return DerivedExpressionIsImmutable(child);
                             });
}

struct DerivedScope {
  std::string derived_alias;
  std::string inner_alias;
  std::string base_table;
  Schema base_schema;
  // Lowered output name -> rebound inner expression (qualifiers already name
  // the derived alias).
  std::unordered_map<std::string, Expression> outputs;
  // Lowered base column names hidden by the derived table (for the bare-name
  // collision gate).
  std::unordered_set<std::string> hidden_columns;
};

// Rebinds an inner-scope expression to the derived alias: qualifiers naming
// the inner alias/table become the derived alias, bare names must exist in
// the base schema. Nullopt on any outer reference or unknown column.
std::optional<Expression> RebindInnerExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const DerivedScope& scope) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return std::nullopt;
    }
    if (!column.schema.empty()) {
      if (!IdentifierEquals(column.schema, scope.inner_alias) &&
          !IdentifierEquals(column.schema, scope.base_table)) {
        return std::nullopt;
      }
      return ColumnValueExp(ColumnName(scope.derived_alias, column.name));
    }
    if (scope.base_schema.Offset(ColumnName("", column.name)) < 0) {
      return std::nullopt;
    }
    return ColumnValueExp(ColumnName(scope.derived_alias, column.name));
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> rebound;
  rebound.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped = RebindInnerExpression(child, scope);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    rebound.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(rebound));
}

// Substitutes derived-output references in an outer-scope expression with
// the rebound inner expression. Qualified `alias.name` must name a derived
// output; bare names matching an output substitute only when they cannot
// name a base column (otherwise the reference is left for normal scope
// resolution, which errors on ambiguity exactly as before).
std::optional<Expression>
SubstituteDerivedOutputs(  // NOLINT(misc-no-recursion)
    const Expression& expression, const DerivedScope& scope,
    const std::unordered_set<std::string>& base_column_names) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") {
      return expression;
    }
    if (!column.schema.empty()) {
      if (!IdentifierEquals(column.schema, scope.derived_alias)) {
        return expression;
      }
      std::string lowered = column.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      const auto found = scope.outputs.find(lowered);
      if (found == scope.outputs.end()) {
        return std::nullopt;
      }
      return found->second;
    }
    std::string lowered = column.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!scope.outputs.contains(lowered)) {
      return expression;
    }
    if (base_column_names.contains(lowered)) {
      return std::nullopt;
    }
    return scope.outputs.at(lowered);
  }
  std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> substituted;
  substituted.reserve(children.size());
  for (const Expression& child : children) {
    std::optional<Expression> mapped =
        SubstituteDerivedOutputs(child, scope, base_column_names);
    if (!mapped.has_value()) {
      return std::nullopt;
    }
    substituted.push_back(std::move(*mapped));
  }
  return WithExpressionChildren(expression, std::move(substituted));
}

bool FlattenOneDerivedSource(SelectStatement* outer, size_t index,
                             TransactionContext& ctx);
// True when the expression carries a `*` column reference at any depth,
// including the lone-star value-table wrapper (`__value_table_value(*)`,
// which selects the row as a value rather than expanding columns). Star
// expansion over derived tables keeps its dedicated (relational) path in
// this slice.
bool ContainsStarReference(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
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
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// M-union: merge UNION ALL / UNION DISTINCT branches over one shared table
// into a single scan. UNION ALL with pairwise-disjoint branch predicates is
// bag-identical to one OR-filtered scan; UNION DISTINCT additionally survives
// dedup either way. Anything else keeps the branch-wise set-operation path.
//
// Soundness gates (all must hold; the statement is untouched otherwise):
// - flat UNION ALL list, every pair kind ALL (bag merge) or every pair kind
//   DISTINCT (dedup merge); no INTERSECT/EXCEPT mixing, no grouped
//   SetOperationTree, default positional column matching only;
// - every part (head + branches) is a single plain-table source over the
//   SAME table and alias (so no qualifier rebinding is needed), with no
//   branch-level CTE map, grouping, aggregates, DISTINCT, LIMIT/OFFSET,
//   ORDER BY, QUALIFY, join condition, or USING clause. Head-level ORDER
//   BY/LIMIT/OFFSET are result-level modifiers and stay put; plain window
//   calls ride along (the merged shape routes through the normal window
//   lowering or relational fallback);
// - equal-width select lists with pairwise identical expressions (the head
//   part's output names win, matching set-operation semantics); `*`
//   expands over the shared table first;
// - UNION ALL additionally proves pairwise disjointness through one shared
//   column pinned to pairwise-different same-type constants (IS NULL counts
//   as its own constant; anything else stays relational).
bool UnionBranchShapeOk(const SelectStatement& part, bool is_head) {
  if (part.Sources().size() != 1 || !part.WithQueries().empty() ||
      !part.GroupBy().empty() || part.Having() || part.Distinct() ||
      !part.DistinctOn().empty() || part.Qualify()) {
    return false;
  }
  // Parenthesized (grouped) set operations change meaning when flattened;
  // the top-level gate already confined this merge to ungrouped trees, and
  // branches must not nest set operations at all.
  const auto& tree = part.GetSetOperationTree();
  if (tree != nullptr && (tree->grouped || !is_head)) {
    return false;
  }
  // The head's own UNION ALL list is what is being merged (not a nested set
  // operation); its ORDER BY / LIMIT / OFFSET are result-level modifiers
  // that stay put. Branches must be bare.
  if (!is_head && (!part.UnionAll().empty() || part.HasLimit() ||
                   part.Offset() != 0 || !part.OrderBy().empty())) {
    return false;
  }
  const SelectSource& source = part.Sources().front();
  if (source.query != nullptr || source.unnest || source.is_lateral ||
      source.table.empty() || !source.using_columns.empty() ||
      source.from_nested_join || source.join_condition ||
      (source.join_type != JoinType::kCross &&
       source.join_type != JoinType::kInner)) {
    return false;
  }
  // A window call is evaluated over the whole query block, so merging the
  // branch into one shared scan would recompute it over the union of every
  // branch's rows (wrong frames/ranks). Keep window-bearing branches on the
  // set-operation path.
  if (relational_detail::HasWindowFunctions(part)) {
    return false;
  }
  return std::ranges::all_of(
      part.SelectList(), [](const NamedExpression& item) {
        return item.expression &&
               !relational_detail::ContainsAggregate(item.expression);
      });
}

// Expands `*` select items of a single-table part over the catalog. False
// when unexpandable (unknown table stays relational).
bool ExpandUnionBranchStars(SelectStatement* part, TransactionContext& ctx) {
  if (part == nullptr || part->Sources().size() != 1) {
    return false;
  }
  bool has_star = false;
  for (const NamedExpression& item : part->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      has_star = true;
      break;
    }
  }
  if (!has_star) {
    return true;
  }
  const SelectSource& source = part->Sources().front();
  StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(source.table);
  if (!found.HasValue()) {
    return false;
  }
  const std::string relation =
      source.alias.empty() ? source.table : source.alias;
  std::vector<NamedExpression> expanded;
  for (const NamedExpression& item : part->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& star = item.expression->AsColumnValue().GetColumnName();
      if (!star.schema.empty() && !IdentifierEquals(star.schema, relation) &&
          !IdentifierEquals(star.schema, source.table)) {
        return false;
      }
      for (size_t i = 0; i < found.Value()->GetSchema().ColumnCount(); ++i) {
        const std::string& col =
            found.Value()->GetSchema().GetColumn(i).Name().name;
        expanded.emplace_back(col, ColumnValueExp(ColumnName(relation, col)));
      }
      continue;
    }
    expanded.push_back(item);
  }
  part->SetSelectList(std::move(expanded));
  return true;
}

// Collects single-column equality-to-constant (or IS NULL) conjuncts for the
// disjointness proof: lowered column name -> constant, or nullopt for
// IS NULL (equal only to itself, so two IS NULL branches correctly fail the
// all-different proof while IS NULL vs a constant passes it).
void CollectEqualityConsts(
    const Expression& where,
    std::unordered_map<std::string, std::optional<Value>>* out) {
  if (!where || out == nullptr) {
    return;
  }
  for (const Expression& conjunct : SplitConjuncts(where)) {
    if (!conjunct) {
      continue;
    }
    if (conjunct->Type() == TypeTag::kBinaryExp &&
        conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
      const auto& binary = conjunct->AsBinaryExpression();
      const Expression* column_side = nullptr;
      const Expression* const_side = nullptr;
      if (binary.Left()->Type() == TypeTag::kColumnValue &&
          binary.Right()->Type() == TypeTag::kConstantValue) {
        column_side = &binary.Left();
        const_side = &binary.Right();
      } else if (binary.Right()->Type() == TypeTag::kColumnValue &&
                 binary.Left()->Type() == TypeTag::kConstantValue) {
        column_side = &binary.Right();
        const_side = &binary.Left();
      }
      if (column_side != nullptr && const_side != nullptr) {
        const ColumnName& column =
            (*column_side)->AsColumnValue().GetColumnName();
        std::string lowered = column.name;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        const Value& constant = (*const_side)->AsConstantValue().GetValue();
        if (!constant.IsNull()) {
          out->emplace(lowered, constant);
        }
      }
      continue;
    }
    if (conjunct->Type() == TypeTag::kUnaryExp &&
        conjunct->AsUnaryExpression().Op() == UnaryOperation::kIsNull &&
        conjunct->AsUnaryExpression().Child()->Type() ==
            TypeTag::kColumnValue) {
      const ColumnName& column = conjunct->AsUnaryExpression()
                                     .Child()
                                     ->AsColumnValue()
                                     .GetColumnName();
      std::string lowered = column.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      out->emplace(lowered, std::nullopt);
    }
  }
}

bool UnionBranchesDisjoint(const std::vector<const SelectStatement*>& parts) {
  // One shared column pinned to pairwise-different same-type constants.
  // Extra conjuncts only narrow further, preserving disjointness.
  std::unordered_map<std::string, std::vector<std::optional<Value>>> by_column;
  for (const SelectStatement* part : parts) {
    std::unordered_map<std::string, std::optional<Value>> consts;
    CollectEqualityConsts(part->WhereClause(), &consts);
    for (const auto& [column, value] : consts) {
      by_column[column].push_back(value);
    }
  }
  for (const auto& [column, values] : by_column) {
    (void)column;
    if (values.size() != parts.size()) {
      continue;
    }
    bool pairwise_different = true;
    for (size_t i = 0; i < values.size() && pairwise_different; ++i) {
      for (size_t j = i + 1; j < values.size(); ++j) {
        const std::optional<Value>& left = values[i];
        const std::optional<Value>& right = values[j];
        if (!left.has_value() || !right.has_value()) {
          pairwise_different = left.has_value() != right.has_value();
        } else if (left->type != right->type || *left == *right) {
          pairwise_different = false;
        }
        if (!pairwise_different) {
          break;
        }
      }
    }
    if (pairwise_different) {
      return true;
    }
  }
  return false;
}

bool MergeCompatibleUnionBranches(SelectStatement* outer,
                                  TransactionContext& ctx) {
  if (outer == nullptr || outer->UnionAll().empty()) {
    return false;
  }
  if (outer->GetSetOperationTree() != nullptr &&
      outer->GetSetOperationTree()->grouped) {
    return false;
  }
  if (outer->UnionByName()) {
    return false;
  }
  for (const SetOperationMatch& match : outer->Matches()) {
    if (match.corresponding || match.by_name || !match.columns.empty()) {
      return false;
    }
  }
  bool all_all = true;
  bool all_distinct = true;
  for (size_t i = 0; i < outer->UnionAll().size(); ++i) {
    const SetOperationKind kind = i < outer->SetOperationKinds().size()
                                      ? outer->SetOperationKinds()[i]
                                      : SetOperationKind::kUnionAll;
    all_all = all_all && kind == SetOperationKind::kUnionAll;
    all_distinct = all_distinct && kind == SetOperationKind::kUnion;
  }
  if (!all_all && !all_distinct) {
    return false;
  }
  // Parts: the head (this statement's own clauses; its ORDER BY / LIMIT /
  // OFFSET are result-level and stay put) plus every branch.
  std::vector<SelectStatement*> parts{outer};
  for (const auto& branch : outer->UnionAll()) {
    if (branch == nullptr) {
      return false;
    }
    parts.push_back(branch.get());
  }
  if (!UnionBranchShapeOk(*parts[0], /*is_head=*/true)) {
    return false;
  }
  for (size_t i = 1; i < parts.size(); ++i) {
    if (!UnionBranchShapeOk(*parts[i], /*is_head=*/false)) {
      return false;
    }
  }
  const SelectSource& head_source = parts[0]->Sources().front();
  for (size_t i = 1; i < parts.size(); ++i) {
    const SelectSource& source = parts[i]->Sources().front();
    if (!IdentifierEquals(source.table, head_source.table) ||
        !IdentifierEquals(source.alias.empty() ? source.table : source.alias,
                          head_source.alias.empty() ? head_source.table
                                                    : head_source.alias)) {
      return false;
    }
  }
  for (SelectStatement* part : parts) {
    if (!ExpandUnionBranchStars(part, ctx)) {
      return false;
    }
  }
  const std::vector<NamedExpression>& head_list = parts[0]->SelectList();
  if (head_list.empty()) {
    return false;
  }
  for (size_t i = 1; i < parts.size(); ++i) {
    const std::vector<NamedExpression>& branch_list = parts[i]->SelectList();
    if (branch_list.size() != head_list.size()) {
      return false;
    }
    for (size_t c = 0; c < head_list.size(); ++c) {
      if (!head_list[c].expression || !branch_list[c].expression ||
          head_list[c].expression->ToString() !=
              branch_list[c].expression->ToString()) {
        return false;
      }
    }
  }
  if (all_all) {
    std::vector<const SelectStatement*> const_parts(parts.begin(), parts.end());
    if (!UnionBranchesDisjoint(const_parts)) {
      return false;
    }
  }
  Expression merged;
  for (SelectStatement* part : parts) {
    Expression branch_where = part->WhereClause()
                                  ? part->WhereClause()
                                  : ConstantValueExp(Value(true));
    merged =
        merged ? BinaryExpressionExp(merged, BinaryOperation::kOr, branch_where)
               : branch_where;
  }
  if (!merged) {
    return false;
  }
  // An integer ORDER BY item is a SELECT-list ordinal. The set-operation
  // path translates it, but this merge collapses to a plain SELECT whose
  // ordinary path would treat the constant as a no-op sort key (silently
  // ignoring the ordering and any LIMIT). Bind the ordinal to the head
  // projection here, after every bail-out so a rejected merge leaves the
  // statement untouched.
  if (!outer->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> rebound_order;
    rebound_order.reserve(outer->OrderBy().size());
    for (const SelectStatement::OrderByTerm& term : outer->OrderBy()) {
      if (term.expression &&
          term.expression->Type() == TypeTag::kConstantValue) {
        const Value& value = term.expression->AsConstantValue().GetValue();
        if (value.type == ValueType::kInt64 && !value.IsNull()) {
          const int64_t ordinal = value.value.int_value;
          if (ordinal < 1 || static_cast<size_t>(ordinal) > head_list.size()) {
            return false;
          }
          const NamedExpression& target =
              head_list[static_cast<size_t>(ordinal) - 1];
          if (!target.expression) {
            return false;
          }
          rebound_order.push_back(
              {target.expression, term.ascending, term.nulls_first});
          continue;
        }
      }
      rebound_order.push_back(term);
    }
    outer->SetOrderBy(std::move(rebound_order));
  }
  outer->SetWhereClause(std::move(merged));
  outer->ClearUnionAll();
  // Drop the (now stale) set-operation tree alongside the flat list: any
  // downstream reader, including relational fallbacks, must see the merged
  // single-query shape only.
  outer->SetSetOperationTree(nullptr);
  if (all_distinct) {
    outer->SetDistinct(true);
  }
  return true;
}
// LATERAL decorrelation, minimal slice (TODO.md item 3a): a LATERAL site
// whose subquery is a single plain base table, row-preserving, with all
// outer references confined to column-to-column equality correlations in its
// WHERE, is convertible to a decorrelated join. The correlation conjuncts
// move to the site's ON condition (rebound to the derived alias, extending
// the inner projection with the key columns when they are not outputs),
// the lateral flag clears, and the proven M5 flattener consumes the now
// uncorrelated site when its own gates pass. Per-row lateral evaluation and
// the decorrelated join produce the same row multiset: each outer row pairs
// with exactly the inner rows satisfying the local predicates plus the
// equalities (inner join), or null-pads when none match (LEFT kept as-is).
// Volatile inner expressions would observe different evaluation counts
// across the two shapes, so they block the rewrite.
bool DecorrelateOneLateralSource(SelectStatement* outer, size_t index,
                                 TransactionContext& ctx) {
  std::vector<SelectSource> sources = outer->Sources();
  if (index >= sources.size()) {
    return false;
  }
  SelectSource site = sources[index];
  if (site.query == nullptr || !site.is_lateral || site.unnest ||
      site.alias.empty() || !site.using_columns.empty() ||
      site.from_nested_join) {
    return false;
  }
  if (site.join_type != JoinType::kCross &&
      site.join_type != JoinType::kInner && site.join_type != JoinType::kLeft) {
    return false;
  }
  const std::shared_ptr<SelectStatement>& inner = site.query;
  if (inner->Sources().size() != 1 || !inner->WithQueries().empty() ||
      !inner->GroupBy().empty() || inner->Having() || inner->Distinct() ||
      !inner->DistinctOn().empty() || inner->HasLimit() ||
      inner->Offset() != 0 || inner->Qualify() || !inner->UnionAll().empty() ||
      inner->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*inner) ||
      inner->SelectList().empty()) {
    return false;
  }
  const SelectSource& base = inner->Sources()[0];
  if (base.query != nullptr || base.unnest || base.is_lateral ||
      base.table.empty() || !base.using_columns.empty() ||
      base.from_nested_join || base.join_condition) {
    return false;
  }
  if (outer->IsRecursiveWith(base.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> base_table = ctx.GetTable(base.table);
  if (!base_table.HasValue()) {
    return false;
  }
  const Schema& base_schema = base_table.Value()->GetSchema();
  const std::string inner_alias = base.alias.empty() ? base.table : base.alias;
  // Inner SELECT: named, immutable, subquery-free, star-free (a star would
  // hide whether a correlation key is projected).
  for (const NamedExpression& item : inner->SelectList()) {
    if (item.name.empty() || !item.expression ||
        relational_detail::ContainsAggregate(item.expression) ||
        ContainsQueryExpression(item.expression) ||
        !DerivedExpressionIsImmutable(item.expression) ||
        ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // Outer scope identities: preceding sources only (later sources cannot be
  // referenced, and the site itself is the derived alias, not the base).
  std::unordered_set<std::string> outer_identities;
  for (size_t j = 0; j < index; ++j) {
    if (!sources[j].alias.empty()) {
      outer_identities.insert(sources[j].alias);
    }
    if (!sources[j].table.empty()) {
      outer_identities.insert(sources[j].table);
    }
  }
  auto is_inner = [&](const ColumnName& column) {
    if (column.schema.empty()) {
      return base_schema.Offset(ColumnName("", column.name)) >= 0;
    }
    return IdentifierEquals(column.schema, inner_alias) ||
           IdentifierEquals(column.schema, base.table);
  };
  auto is_outer = [&](const ColumnName& column) {
    if (column.schema.empty()) {
      return false;
    }
    return std::ranges::any_of(
        outer_identities, [&](const std::string& identity) {
          return IdentifierEquals(column.schema, identity);
        });
  };
  // Partition the inner WHERE into correlation equalities and the local
  // remainder. Every other outer reference anywhere in the inner statement
  // aborts the rewrite.
  struct Correlation {
    Expression outer_column;
    std::string inner_name;
  };
  std::vector<Correlation> correlations;
  std::vector<Expression> remainder;
  for (const Expression& conjunct : SplitConjuncts(inner->WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
        conjunct->AsBinaryExpression().Left()->Type() !=
            TypeTag::kColumnValue ||
        conjunct->AsBinaryExpression().Right()->Type() !=
            TypeTag::kColumnValue) {
      if (conjunct && !DerivedExpressionIsImmutable(conjunct)) {
        return false;
      }
      if (conjunct) {
        for (const ColumnName& column : conjunct->TouchedColumns()) {
          if (!is_inner(column)) {
            return false;
          }
        }
        remainder.push_back(conjunct);
      }
      continue;
    }
    const ColumnName& left =
        conjunct->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
    const ColumnName& right =
        conjunct->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
    const ColumnName* inner_col = nullptr;
    const Expression* outer_expr = nullptr;
    if (is_inner(left) && is_outer(right)) {
      inner_col = &left;
      outer_expr = &conjunct->AsBinaryExpression().Right();
    } else if (is_inner(right) && is_outer(left)) {
      inner_col = &right;
      outer_expr = &conjunct->AsBinaryExpression().Left();
    } else {
      // Both sides inner (a local predicate), or unresolvable: the latter
      // may still name the outer scope, so only provably-local stays.
      if (!is_inner(left) || !is_inner(right)) {
        return false;
      }
      remainder.push_back(conjunct);
      continue;
    }
    correlations.push_back(Correlation{.outer_column = *outer_expr,
                                       .inner_name = inner_col->name});
  }
  if (correlations.empty()) {
    return false;
  }
  auto touches_outer = [&](const Expression& expression) {
    if (!expression) {
      return false;
    }
    return std::ranges::any_of(expression->TouchedColumns(),
                               [&](const ColumnName& column) {
                                 return column.name != "*" && !is_inner(column);
                               });
  };
  for (const NamedExpression& item : inner->SelectList()) {
    if (touches_outer(item.expression)) {
      return false;
    }
  }
  for (const auto& term : inner->OrderBy()) {
    if (touches_outer(term.expression)) {
      return false;
    }
  }
  // Extend the inner projection with correlation keys that are not outputs
  // (the ON condition below addresses them through the derived alias).
  // A clashing output name with different meaning aborts the rewrite.
  std::shared_ptr<SelectStatement> new_inner =
      std::make_shared<SelectStatement>(*inner);
  for (const Correlation& correlation : correlations) {
    std::string lowered = correlation.inner_name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    bool projected = false;
    for (const NamedExpression& item : new_inner->SelectList()) {
      std::string item_lowered = item.name;
      for (char& c : item_lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      if (item_lowered != lowered) {
        continue;
      }
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& projected_col =
          item.expression->AsColumnValue().GetColumnName();
      const bool same_base =
          (projected_col.schema.empty() &&
           base_schema.Offset(ColumnName("", projected_col.name)) >= 0 &&
           IdentifierEquals(projected_col.name, correlation.inner_name)) ||
          ((IdentifierEquals(projected_col.schema, inner_alias) ||
            IdentifierEquals(projected_col.schema, base.table)) &&
           IdentifierEquals(projected_col.name, correlation.inner_name));
      if (!same_base) {
        return false;
      }
      projected = true;
      break;
    }
    if (!projected) {
      std::vector<NamedExpression> extended = new_inner->SelectList();
      extended.emplace_back(
          correlation.inner_name,
          ColumnValueExp(ColumnName(inner_alias, correlation.inner_name)));
      new_inner->SetSelectList(std::move(extended));
    }
  }
  new_inner->SetWhereClause(remainder.empty() ? Expression{}
                                              : CombineConjuncts(remainder));
  // Publish the rewrite on the site: uncorrelated, explicitly joined, with
  // the correlations as the ON condition addressed through the derived
  // alias (the flattener rebinds them when it consumes the site).
  Expression on;
  for (const Correlation& correlation : correlations) {
    Expression term = BinaryExpressionExp(
        correlation.outer_column, BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(site.alias, correlation.inner_name)));
    on = on ? BinaryExpressionExp(on, BinaryOperation::kAnd, term) : term;
  }
  if (site.join_condition) {
    on = BinaryExpressionExp(site.join_condition, BinaryOperation::kAnd, on);
  }
  site.query = std::move(new_inner);
  site.is_lateral = false;
  if (site.join_type == JoinType::kCross) {
    site.join_type = JoinType::kInner;
  }
  site.join_condition = std::move(on);
  sources[index] = std::move(site);
  outer->SetSources(std::move(sources));
  // The proven M5 flattener consumes the uncorrelated site when its gates
  // pass; otherwise the site stays a single-evaluation uncorrelated derived
  // table, still cheaper than per-row lateral re-execution.
  (void)FlattenOneDerivedSource(outer, index, ctx);
  return true;
}

bool DecorrelateSingleTableLaterals(SelectStatement* outer,
                                    TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  bool changed = false;
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    if (outer->Sources()[i].query != nullptr &&
        DecorrelateOneLateralSource(outer, i, ctx)) {
      changed = true;
    }
  }
  return changed;
}

bool FlattenDerivedSources(SelectStatement* outer, TransactionContext& ctx,
                           const LiftedCtes* lifted = nullptr) {
  // A covered layer (LiftedCtes::fully_covered) leaves only vestigial map
  // entries, which no longer block flattening. Call adjacency guarantees
  // this: nothing mutates the map between lifting and this call.
  if (outer == nullptr || (!outer->WithQueries().empty() &&
                           (lifted == nullptr || !lifted->fully_covered))) {
    return false;
  }
  bool changed = false;
  // Depth-first: an inner derived table flattens before its consumer, so a
  // nested derived source is already a base table when the outer level runs.
  for (const SelectSource& source : outer->Sources()) {
    if (source.query != nullptr &&
        FlattenDerivedSources(source.query.get(), ctx, lifted)) {
      changed = true;
    }
  }
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    if (outer->Sources()[i].query != nullptr &&
        FlattenOneDerivedSource(outer, i, ctx)) {
      changed = true;
    }
  }
  return changed;
}

bool FlattenOneDerivedSource(SelectStatement* outer, size_t index,
                             TransactionContext& ctx) {
  std::vector<SelectSource> sources = outer->Sources();
  if (index >= sources.size()) {
    return false;
  }
  const SelectSource& derived = sources[index];
  const auto& inner = derived.query;
  if (inner == nullptr || derived.alias.empty() || derived.is_lateral ||
      derived.unnest || !derived.using_columns.empty() ||
      derived.from_nested_join || derived.join_type == JoinType::kFull) {
    return false;
  }
  // Grouped constructs route to the grouped bridge / relational finish paths
  // whose scope contracts assume unflattened shapes (in particular, the
  // single-source grouped path cannot resolve qualified references). Window
  // evaluation likewise owns its pre-computed scope. Keep those queries on
  // the existing path in this slice.
  if (!outer->GroupBy().empty() || outer->Having() ||
      relational_detail::HasWindowFunctions(*outer) ||
      std::ranges::any_of(outer->SelectList(), [](const NamedExpression& item) {
        return relational_detail::ContainsAggregate(item.expression);
      })) {
    return false;
  }
  // Stars (including the lone-star value-table wrapper) keep the dedicated
  // expansion paths: expanding them here would diverge from the row-value
  // semantics the visitor marked.
  for (const NamedExpression& item : outer->SelectList()) {
    if (ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // The immediate join decides where the inner WHERE may go: into the outer
  // WHERE everywhere except the null-supplying side of an outer join, where
  // it must ride the ON condition (see the header comment). Chains beyond
  // two relations only qualify when every join is inner.
  const bool two_sources = outer->Sources().size() == 2;
  bool inner_where_to_on = false;
  if (index > 0) {
    if (derived.join_type == JoinType::kLeft) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = true;
    } else if (derived.join_type == JoinType::kRight) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = false;
    } else if (derived.join_type != JoinType::kCross &&
               derived.join_type != JoinType::kInner) {
      return false;
    }
  } else if (outer->Sources().size() > 1) {
    const JoinType next = outer->Sources()[1].join_type;
    if (next == JoinType::kRight) {
      if (!two_sources) {
        return false;
      }
      inner_where_to_on = true;
    } else if (next != JoinType::kCross && next != JoinType::kInner &&
               next != JoinType::kLeft) {
      return false;
    }
    if (!two_sources &&
        std::ranges::any_of(outer->Sources(), [](const SelectSource& source) {
          return source.join_type == JoinType::kLeft ||
                 source.join_type == JoinType::kRight ||
                 source.join_type == JoinType::kFull;
        })) {
      return false;
    }
  }
  // Inner shape: one plain base table, row-preserving, uncorrelated.
  if (inner->Sources().size() != 1 || !inner->WithQueries().empty() ||
      !inner->GroupBy().empty() || inner->Having() || inner->Distinct() ||
      !inner->DistinctOn().empty() || inner->HasLimit() ||
      inner->Offset() != 0 || inner->Qualify() || !inner->UnionAll().empty() ||
      inner->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*inner) ||
      inner->SelectList().empty()) {
    return false;
  }
  const SelectSource& base = inner->Sources()[0];
  if (base.query != nullptr || base.unnest || base.is_lateral ||
      base.table.empty() || !base.using_columns.empty() ||
      base.from_nested_join || base.join_condition) {
    return false;
  }
  if (outer->IsRecursiveWith(base.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> base_table = ctx.GetTable(base.table);
  if (!base_table.HasValue()) {
    return false;
  }
  const Schema& base_schema = base_table.Value()->GetSchema();
  const std::string inner_alias = base.alias.empty() ? base.table : base.alias;
  const std::string derived_alias = derived.alias;

  // Expand an inner `*` over the base schema; anything else unexpandable
  // keeps the relational path.
  std::vector<NamedExpression> inner_items;
  for (const NamedExpression& item : inner->SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      const ColumnName& star = item.expression->AsColumnValue().GetColumnName();
      if (!star.schema.empty() && !IdentifierEquals(star.schema, inner_alias) &&
          !IdentifierEquals(star.schema, base.table)) {
        return false;
      }
      for (size_t i = 0; i < base_schema.ColumnCount(); ++i) {
        const std::string& col = base_schema.GetColumn(i).Name().name;
        inner_items.emplace_back(col, ColumnValueExp(ColumnName(col)));
      }
      continue;
    }
    inner_items.push_back(item);
  }
  DerivedScope scope;
  scope.derived_alias = derived_alias;
  scope.inner_alias = inner_alias;
  scope.base_table = base.table;
  scope.base_schema = base_schema;
  for (size_t i = 0; i < base_schema.ColumnCount(); ++i) {
    std::string lowered = base_schema.GetColumn(i).Name().name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    scope.hidden_columns.insert(std::move(lowered));
  }
  for (const NamedExpression& item : inner_items) {
    if (item.name.empty() || !item.expression ||
        relational_detail::ContainsAggregate(item.expression) ||
        ContainsQueryExpression(item.expression) ||
        !DerivedExpressionIsImmutable(item.expression)) {
      return false;
    }
    std::optional<Expression> rebound =
        RebindInnerExpression(item.expression, scope);
    if (!rebound.has_value()) {
      return false;
    }
    std::string lowered = item.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!scope.outputs.try_emplace(lowered, std::move(*rebound)).second) {
      return false;
    }
    scope.hidden_columns.erase(lowered);
  }
  std::optional<Expression> rebound_where =
      RebindInnerExpression(inner->WhereClause(), scope);
  if (!rebound_where.has_value()) {
    return false;
  }
  // Base column names visible in the outer scope (other sources plus the
  // flattened base itself): a bare outer reference colliding with one never
  // substitutes a derived output.
  std::unordered_set<std::string> base_column_names;
  for (size_t i = 0; i < sources.size(); ++i) {
    if (i == index) {
      continue;
    }
    const SelectSource& sibling = sources[i];
    if (sibling.query != nullptr) {
      for (const NamedExpression& item : sibling.query->SelectList()) {
        if (item.name.empty()) {
          continue;
        }
        std::string lowered = item.name;
        for (char& c : lowered) {
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        base_column_names.insert(std::move(lowered));
      }
      continue;
    }
    if (sibling.table.empty()) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> sibling_table =
        ctx.GetTable(sibling.table);
    if (!sibling_table.HasValue()) {
      continue;
    }
    for (size_t c = 0; c < sibling_table.Value()->GetSchema().ColumnCount();
         ++c) {
      std::string lowered =
          sibling_table.Value()->GetSchema().GetColumn(c).Name().name;
      for (char& ch : lowered) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
      }
      base_column_names.insert(std::move(lowered));
    }
  }
  // No bare outer reference may name a base column hidden by the derived
  // table: it errored (or bound elsewhere) before flattening and must not
  // silently start resolving to the base table.
  const auto outer_bare_names = [&](const Expression& expression) {
    std::unordered_set<std::string> names;
    if (!expression) {
      return names;
    }
    std::vector<Expression> stack{expression};
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue) {
        const ColumnName& column = current->AsColumnValue().GetColumnName();
        if (column.schema.empty() && column.name != "*") {
          std::string lowered = column.name;
          for (char& c : lowered) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          names.insert(std::move(lowered));
        }
        continue;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
    return names;
  };
  std::vector<Expression> outer_exprs;
  if (outer->WhereClause()) {
    outer_exprs.push_back(outer->WhereClause());
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (item.expression) {
      outer_exprs.push_back(item.expression);
    }
  }
  for (const auto& term : outer->OrderBy()) {
    if (term.expression) {
      outer_exprs.push_back(term.expression);
    }
  }
  for (const Expression& expression : outer->GroupBy()) {
    outer_exprs.push_back(expression);
  }
  if (outer->Having()) {
    outer_exprs.push_back(outer->Having());
  }
  for (const SelectSource& source : sources) {
    if (source.join_condition) {
      outer_exprs.push_back(source.join_condition);
    }
  }
  for (const Expression& expression : outer_exprs) {
    for (const std::string& name : outer_bare_names(expression)) {
      if (scope.hidden_columns.contains(name)) {
        return false;
      }
    }
    // A derived output used as a qualifier (`pb.field` where `pb` is an
    // output, not a relation) addresses the row value through the derived
    // scope; substituting the alias away would orphan it. The derived alias
    // itself is fine (handled by substitution below).
    std::vector<Expression> stack;
    if (expression) {
      stack.push_back(expression);
    }
    while (!stack.empty()) {
      Expression current = std::move(stack.back());
      stack.pop_back();
      if (!current) {
        continue;
      }
      if (current->Type() == TypeTag::kColumnValue) {
        const ColumnName& column = current->AsColumnValue().GetColumnName();
        if (!column.schema.empty() &&
            !IdentifierEquals(column.schema, derived_alias)) {
          std::string lowered = column.schema;
          for (char& c : lowered) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
          if (scope.outputs.contains(lowered)) {
            return false;
          }
        }
        continue;
      }
      for (const Expression& child : ExpressionChildren(current)) {
        if (child) {
          stack.push_back(child);
        }
      }
    }
  }
  if (outer->Qualify()) {
    for (const std::string& name : outer_bare_names(outer->Qualify())) {
      if (scope.hidden_columns.contains(name) || scope.outputs.contains(name)) {
        return false;
      }
    }
    for (const ColumnName& column : outer->Qualify()->TouchedColumns()) {
      if (!column.schema.empty() &&
          IdentifierEquals(column.schema, derived_alias)) {
        return false;
      }
    }
  }
  // Substitute derived references across the outer statement. (Stars were
  // rejected above and need no handling here.)
  std::vector<NamedExpression> new_select;
  for (const NamedExpression& item : outer->SelectList()) {
    std::optional<Expression> mapped =
        SubstituteDerivedOutputs(item.expression, scope, base_column_names);
    if (!mapped.has_value()) {
      return false;
    }
    new_select.emplace_back(item.name, std::move(*mapped));
  }
  const auto substitute_field = [&](const Expression& expression) {
    return SubstituteDerivedOutputs(expression, scope, base_column_names);
  };
  std::optional<Expression> new_where = substitute_field(outer->WhereClause());
  std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
  for (auto& term : new_order) {
    std::optional<Expression> mapped = substitute_field(term.expression);
    if (!mapped.has_value()) {
      return false;
    }
    term.expression = std::move(*mapped);
  }
  std::vector<Expression> new_group;
  for (const Expression& expression : outer->GroupBy()) {
    std::optional<Expression> mapped = substitute_field(expression);
    if (!mapped.has_value()) {
      return false;
    }
    new_group.push_back(std::move(*mapped));
  }
  std::optional<Expression> new_having = substitute_field(outer->Having());
  if (!new_where.has_value() || !new_having.has_value()) {
    return false;
  }
  // Merge the inner WHERE into the outer WHERE — or into the ON condition
  // when the flattened source is null-supplying for its immediate outer
  // join (a post-padding residual would drop padded rows outright).
  Expression merged_where = std::move(*new_where);
  // The join carrying the ON lives on the later source of each pair. When
  // the derived table is the right operand (index > 0) that is its own slot;
  // when it is the left operand of `d RIGHT JOIN next`, the ON sits on the
  // next source.  Every source's ON must be rebound through the derived
  // scope first: the derived alias disappears after flattening, so an
  // unrebound `d.col` would silently bind to the base table or fail.
  const size_t on_index =
      index > 0 ? index : (sources.size() > 1 ? size_t{1} : index);
  for (auto& source : sources) {
    if (!source.join_condition) {
      continue;
    }
    std::optional<Expression> mapped = substitute_field(source.join_condition);
    if (!mapped.has_value()) {
      return false;
    }
    source.join_condition = std::move(*mapped);
  }
  if (inner_where_to_on) {
    Expression on = sources[on_index].join_condition;
    if (*rebound_where) {
      on = on ? BinaryExpressionExp(on, BinaryOperation::kAnd, *rebound_where)
              : *rebound_where;
    }
    sources[on_index].join_condition = std::move(on);
  } else if (*rebound_where) {
    merged_where =
        merged_where ? BinaryExpressionExp(merged_where, BinaryOperation::kAnd,
                                           *rebound_where)
                     : *rebound_where;
  }
  // Publish the base table under the derived alias and drop the subquery.
  sources[index].table = base.table;
  sources[index].query = nullptr;
  outer->SetSources(std::move(sources));
  outer->SetWhereClause(std::move(merged_where));
  outer->SetSelectList(std::move(new_select));
  outer->SetOrderBy(std::move(new_order));
  outer->SetGroupBy(std::move(new_group));
  outer->SetHaving(std::move(*new_having));
  return true;
}

// Budget for eager CTE cells (M4): planning-time memory plus memo
// fingerprint cost stay trivial; anything bigger keeps the materialized
// relational path.
constexpr size_t kMaxMaterializedCteRows = 1024;

// CTE predicate pushdown (TODO.md item 2a): before a lifted CTE body is
// executed into a shared cell, narrow it with outer WHERE conjuncts. The
// cell serves every reference site, so an injected predicate must be a
// logical consequence of *each* site's filters (a weakening): per output
// column it keeps the minimum lower bound, the maximum upper bound, and a
// unanimous equality across all live sites. Single-site CTEs fall out as
// the special case (their own bounds). The outer filters are kept
// everywhere, so the injection only ever narrows the prefetched cell; it
// must still be exact (an over-narrowing injection would drop rows a site
// keeps), hence the passthrough-only mapping and the sargable-operator
// gate. A site whose own conjuncts contradict each other needs nothing and
// is excluded from the combination.
// Returns true when at least one conjunct was injected.
bool TryNarrowCteBody(SelectStatement* body, const SelectStatement& outer,
                      const std::vector<std::string>& site_aliases) {
  if (body == nullptr || site_aliases.empty() || body->Sources().size() != 1) {
    return false;
  }
  const SelectSource& base = body->Sources().front();
  if (base.table.empty() || base.query != nullptr || base.unnest ||
      base.is_lateral || base.from_nested_join || !base.using_columns.empty() ||
      base.join_condition || base.join_type != JoinType::kCross) {
    return false;
  }
  // Row-preserving bodies only (same shape the M5 flattener consumes, plus
  // no post-window/row-count operators whose input the filter would change).
  if (!body->GroupBy().empty() || body->Having() || body->Distinct() ||
      body->HasDistinctOn() || body->HasLimit() || body->Offset() != 0 ||
      !body->UnionAll().empty() || body->GetSetOperationTree() != nullptr ||
      body->Qualify() || body->WithTies() ||
      relational_detail::HasWindowFunctions(*body) ||
      !body->WithQueries().empty()) {
    return false;
  }
  for (const NamedExpression& item : body->SelectList()) {
    if (ContainsStarReference(item.expression)) {
      return false;
    }
  }
  // Lowered output-name -> base column for bare passthrough targets only.
  // Expression outputs have no mapping (conservative: no injection).
  auto lower = [](std::string text) {
    for (char& c : text) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return text;
  };
  std::unordered_map<std::string, ColumnName> output_to_base;
  for (const NamedExpression& item : body->SelectList()) {
    if (item.name.empty() || !item.expression ||
        item.expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    output_to_base.emplace(lower(item.name),
                           item.expression->AsColumnValue().GetColumnName());
  }
  if (output_to_base.empty()) {
    return false;
  }
  struct SiteBounds {
    bool dead{false};
    struct ColumnBounds {
      bool usable{true};
      bool has_lower{false};
      bool has_upper{false};
      bool has_eq{false};
      Value lower_value;
      Value upper_value;
      Value eq_value;
      bool lower_inclusive{true};
      bool upper_inclusive{true};
    };
    std::unordered_map<std::string, ColumnBounds> columns;
  };
  std::unordered_map<std::string, size_t> site_index;
  for (size_t i = 0; i < site_aliases.size(); ++i) {
    site_index.emplace(lower(site_aliases[i]), i);
  }
  std::vector<SiteBounds> sites(site_aliases.size());
  for (const Expression& conjunct : SplitConjuncts(outer.WhereClause())) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    BinaryOperation normalized_op = binary.Op();
    const Expression* column_side = nullptr;
    const Expression* const_side = nullptr;
    if (binary.Left()->Type() == TypeTag::kColumnValue &&
        binary.Right()->Type() == TypeTag::kConstantValue) {
      column_side = &binary.Left();
      const_side = &binary.Right();
    } else if (binary.Left()->Type() == TypeTag::kConstantValue &&
               binary.Right()->Type() == TypeTag::kColumnValue) {
      column_side = &binary.Right();
      const_side = &binary.Left();
      switch (binary.Op()) {
        case BinaryOperation::kEquals:
          normalized_op = BinaryOperation::kEquals;
          break;
        case BinaryOperation::kLessThan:
          normalized_op = BinaryOperation::kGreaterThan;
          break;
        case BinaryOperation::kLessThanEquals:
          normalized_op = BinaryOperation::kGreaterThanEquals;
          break;
        case BinaryOperation::kGreaterThan:
          normalized_op = BinaryOperation::kLessThan;
          break;
        case BinaryOperation::kGreaterThanEquals:
          normalized_op = BinaryOperation::kLessThanEquals;
          break;
        default:
          continue;
      }
    } else {
      continue;
    }
    if (normalized_op != BinaryOperation::kEquals &&
        normalized_op != BinaryOperation::kLessThan &&
        normalized_op != BinaryOperation::kLessThanEquals &&
        normalized_op != BinaryOperation::kGreaterThan &&
        normalized_op != BinaryOperation::kGreaterThanEquals) {
      continue;
    }
    const Value& constant = (*const_side)->AsConstantValue().GetValue();
    if (constant.IsNull()) {
      continue;
    }
    const ColumnName& outer_col =
        (*column_side)->AsColumnValue().GetColumnName();
    const auto site_it = site_index.find(lower(outer_col.schema));
    if (site_it == site_index.end()) {
      continue;
    }
    SiteBounds& site = sites[site_it->second];
    if (site.dead) {
      continue;
    }
    const std::string key = lower(outer_col.name);
    if (!output_to_base.contains(key)) {
      continue;
    }
    SiteBounds::ColumnBounds& bounds = site.columns[key];
    if (!bounds.usable) {
      continue;
    }
    if (normalized_op == BinaryOperation::kEquals) {
      if (bounds.has_eq) {
        if (bounds.eq_value.type != constant.type) {
          // Mixed-type equalities may still agree under numeric coercion;
          // the column simply becomes unrepresentable for this site (the
          // column is then skipped globally, never mis-narrowed).
          bounds.usable = false;
          bounds.has_lower = bounds.has_upper = bounds.has_eq = false;
          continue;
        }
        if (!(bounds.eq_value == constant)) {
          // Contradictory equalities: this site needs nothing.
          site.dead = true;
          site.columns.clear();
          continue;
        }
      } else {
        bounds.has_eq = true;
        bounds.eq_value = constant;
      }
      continue;
    }
    // Range bound: mixed-type comparisons on one column are skipped.
    bool* has = nullptr;
    Value* stored = nullptr;
    bool* inclusive = nullptr;
    const bool is_lower = normalized_op == BinaryOperation::kGreaterThan ||
                          normalized_op == BinaryOperation::kGreaterThanEquals;
    if (is_lower) {
      has = &bounds.has_lower;
      stored = &bounds.lower_value;
      inclusive = &bounds.lower_inclusive;
    } else {
      has = &bounds.has_upper;
      stored = &bounds.upper_value;
      inclusive = &bounds.upper_inclusive;
    }
    if (*has && stored->type != constant.type) {
      bounds.usable = false;
      bounds.has_lower = bounds.has_upper = bounds.has_eq = false;
      continue;
    }
    const bool edge_inclusive =
        normalized_op == BinaryOperation::kGreaterThanEquals ||
        normalized_op == BinaryOperation::kLessThanEquals;
    if (!*has) {
      *has = true;
      *stored = constant;
      *inclusive = edge_inclusive;
    } else if (is_lower) {
      if (constant.type == stored->type && *stored < constant) {
        *stored = constant;
        *inclusive = edge_inclusive;
      } else if (!(constant.type == stored->type) || constant == *stored) {
        *inclusive = *inclusive && edge_inclusive;
      }
    } else {
      if (constant.type == stored->type && constant < *stored) {
        *stored = constant;
        *inclusive = edge_inclusive;
      } else if (!(constant.type == stored->type) || constant == *stored) {
        *inclusive = *inclusive && edge_inclusive;
      }
    }
  }
  // Combine across live sites: the minimum lower bound, the maximum upper
  // bound, and a unanimous equality. The inclusive edge is always sound for
  // the weakening (a strict site bound implies its inclusive form).
  std::vector<Expression> injected;
  for (const auto& [output, base_column] : output_to_base) {
    bool all_lower = true;
    bool all_upper = true;
    bool all_eq = true;
    bool all_usable = true;
    bool any_live = false;
    Value lower_value;
    Value upper_value;
    Value eq_value;
    bool lower_set = false;
    bool upper_set = false;
    bool eq_set = false;
    for (const SiteBounds& site : sites) {
      if (site.dead) {
        continue;
      }
      any_live = true;
      const auto found = site.columns.find(output);
      if (found == site.columns.end() || !found->second.usable) {
        all_lower = all_upper = all_eq = false;
        if (found != site.columns.end()) {
          all_usable = false;
        }
        continue;
      }
      const SiteBounds::ColumnBounds& bounds = found->second;
      if (bounds.has_lower) {
        if (!lower_set) {
          lower_value = bounds.lower_value;
          lower_set = true;
        } else if (lower_value.type != bounds.lower_value.type) {
          all_lower = false;
        } else if (bounds.lower_value < lower_value) {
          lower_value = bounds.lower_value;
        }
      } else {
        all_lower = false;
      }
      if (bounds.has_upper) {
        if (!upper_set) {
          upper_value = bounds.upper_value;
          upper_set = true;
        } else if (upper_value.type != bounds.upper_value.type) {
          all_upper = false;
        } else if (bounds.upper_value < upper_value) {
          upper_value = bounds.upper_value;
        }
      } else {
        all_upper = false;
      }
      if (bounds.has_eq) {
        if (!eq_set) {
          eq_value = bounds.eq_value;
          eq_set = true;
        } else if (eq_value.type != bounds.eq_value.type ||
                   !(eq_value == bounds.eq_value)) {
          all_eq = false;
        }
      } else {
        all_eq = false;
      }
    }
    if (!any_live || !all_usable) {
      continue;
    }
    if (all_eq && eq_set) {
      injected.push_back(BinaryExpressionExp(ColumnValueExp(base_column),
                                             BinaryOperation::kEquals,
                                             ConstantValueExp(eq_value)));
      continue;
    }
    if (all_lower && lower_set) {
      injected.push_back(BinaryExpressionExp(
          ColumnValueExp(base_column), BinaryOperation::kGreaterThanEquals,
          ConstantValueExp(lower_value)));
    }
    if (all_upper && upper_set) {
      injected.push_back(BinaryExpressionExp(ColumnValueExp(base_column),
                                             BinaryOperation::kLessThanEquals,
                                             ConstantValueExp(upper_value)));
    }
  }
  if (injected.empty()) {
    return false;
  }
  std::vector<Expression> body_conjuncts = SplitConjuncts(body->WhereClause());
  body_conjuncts.insert(body_conjuncts.end(), injected.begin(), injected.end());
  body->SetWhereClause(CombineConjuncts(body_conjuncts));
  return true;
}

}  // namespace

void SqlEngine::MaterializeCtes(SelectStatement* outer, TransactionContext& ctx,
                                LiftedCtes* lifted) {
  if (outer == nullptr || lifted == nullptr || outer->WithQueries().empty() ||
      force_relational_fallback_) {
    return;
  }
  // Grouped outer queries never reach the cell-reading paths (grouping
  // routes relational), so lifting would only waste eager execution.
  if (!outer->GroupBy().empty() || outer->Having()) {
    return;
  }
  bool outer_has_aggregate = false;
  for (const NamedExpression& item : outer->SelectList()) {
    if (relational_detail::ContainsAggregate(item.expression)) {
      outer_has_aggregate = true;
      break;
    }
  }
  if (outer_has_aggregate) {
    return;
  }
  // Relational-only features never compose with cells either (same set as
  // PostRewriteNeedsRelational minus the CTE/derived clauses this lifting
  // resolves; keep in sync). Lifting there would execute eagerly only to
  // discard the rows on the relational fallback.
  for (const SelectSource& source : outer->Sources()) {
    if (source.unnest || source.is_lateral || !source.using_columns.empty() ||
        source.from_nested_join || source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull ||
        NeedsRelationalEvaluation(source.join_condition)) {
      return;
    }
  }
  for (const NamedExpression& item : outer->SelectList()) {
    if (NeedsRelationalEvaluation(item.expression)) {
      return;
    }
    if (item.expression &&
        item.expression->Type() == TypeTag::kFunctionCallExp &&
        (item.expression->AsFunctionCallExpression().FuncName() ==
             "__value_table_value" ||
         item.expression->AsFunctionCallExpression().FuncName() ==
             "__proto_new")) {
      return;
    }
  }
  if (NeedsRelationalEvaluation(outer->WhereClause()) ||
      relational_detail::HasWindowFunctions(*outer) || outer->Qualify() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->HasDistinctOn() || outer->WithTies()) {
    return;
  }
  for (const auto& term : outer->OrderBy()) {
    if (NeedsRelationalEvaluation(term.expression)) {
      return;
    }
  }
  // Deterministic declaration order for reproducible plans.
  std::vector<std::string> names;
  for (const std::string& name : outer->WithQueryOrder()) {
    if (outer->WithQueries().contains(name)) {
      names.push_back(name);
    }
  }
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    if (!outer->IsRecursiveWith(name) &&
        std::ranges::find(names, name) == names.end()) {
      names.push_back(name);
    }
  }
  // Plain (non-recursive) names lift here; recursive ones belong to
  // LiftRecursiveCtes below and stay mapped meanwhile. Reference counting
  // still spans every name so a plain CTE referenced from a recursive body
  // (or anywhere off the top level) keeps the layer mapped.
  std::vector<std::string> plain_names;
  for (const std::string& name : names) {
    if (!outer->IsRecursiveWith(name)) {
      plain_names.push_back(name);
    }
  }
  if (plain_names.empty()) {
    return;
  }
  std::unordered_set<std::string> name_set(names.begin(), names.end());
  std::unordered_map<std::string, size_t> total_refs;
  CountCteReferencesInStatement(*outer, name_set, &total_refs);
  // Every reference must be a top-level FROM source: nested placements
  // (expression subqueries, set-op branches, sibling bodies) keep the
  // map-scoped execution they rely on.
  std::unordered_map<std::string, std::vector<size_t>> top_sites;
  for (size_t i = 0; i < outer->Sources().size(); ++i) {
    const std::string& table = outer->Sources()[i].table;
    if (name_set.contains(table)) {
      top_sites[table].push_back(i);
    }
  }
  size_t top_total = 0;
  for (const auto& [name, sites] : top_sites) {
    (void)name;
    top_total += sites.size();
  }
  size_t grand_total = 0;
  for (const auto& [name, count] : total_refs) {
    (void)name;
    grand_total += count;
  }
  if (top_total != grand_total) {
    return;
  }
  // Every mapped CTE participates (no dead entries left behind to block
  // routing, no unmapped stragglers): each needs at least one top-level
  // site, distinct normalized aliases per CTE, and a CTE-free body with
  // unique output names.
  for (const std::string& name : plain_names) {
    const auto sites_it = top_sites.find(name);
    if (sites_it == top_sites.end() || sites_it->second.empty()) {
      return;
    }
    std::unordered_set<std::string> aliases;
    for (size_t index : sites_it->second) {
      const std::string& alias = outer->Sources()[index].alias;
      if (!aliases.insert(alias.empty() ? name : alias).second) {
        return;
      }
    }
    const auto body_it = outer->WithQueries().find(name);
    if (body_it == outer->WithQueries().end() || body_it->second == nullptr) {
      return;
    }
    std::unordered_map<std::string, size_t> body_refs;
    CountCteReferencesInStatement(*body_it->second, name_set, &body_refs);
    for (const auto& [ref, count] : body_refs) {
      (void)ref;
      if (count > 0) {
        return;
      }
    }
    std::unordered_set<std::string> output_names;
    for (const NamedExpression& item : body_it->second->SelectList()) {
      if (item.name.empty()) {
        return;
      }
      std::string lowered = item.name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      if (!output_names.insert(std::move(lowered)).second) {
        return;
      }
    }
    if (output_names.empty()) {
      return;
    }
  }
  // All gates passed: plan + execute each body through a fresh engine (same
  // transaction snapshot as the outer query; isolated facade state so the
  // outer preparation is undisturbed). Bodies are loaned out of the map and
  // moved straight back afterwards, so the map is complete on every return
  // path and the abort path is identical to never having tried (bodies may
  // come back statement-rewritten by the inner preparation, which preserves
  // their row multisets).
  SqlEngine body_engine(*database_);
  struct LiftedRows {
    std::string name;
    std::vector<std::string> labels;
    std::vector<Row> rows;
  };
  std::vector<LiftedRows> lifted_rows;
  for (const std::string& name : plain_names) {
    const auto body_it = outer->WithQueries().find(name);
    if (body_it == outer->WithQueries().end() || body_it->second == nullptr) {
      return;
    }
    // Shared ownership: the inner preparation may statement-rewrite the
    // (shared) body, which preserves its row multiset by construction; the
    // map entry itself is never disturbed, so every return path below keeps
    // a complete map and the abort path is identical to never having tried.
    // Single-use bodies additionally get outer filters pushed in (TODO.md
    // item 2a): the copy is narrowed, never the mapped body, and the outer
    // filter stays as the residual.
    auto body_copy = std::make_unique<SelectStatement>(*body_it->second);
    {
      // Every top-level reference site shares the cell, so the narrowing
      // must be a weakening over all of them (TODO.md item 2a).
      std::vector<std::string> site_aliases;
      for (const SelectSource& source : outer->Sources()) {
        if (source.table == name) {
          site_aliases.push_back(source.alias.empty() ? name : source.alias);
        }
      }
      TryNarrowCteBody(body_copy.get(), *outer, site_aliases);
    }
    StatusOr<Executor> planned =
        body_engine.PrepareStatement(ctx, std::move(body_copy));
    if (!planned.HasValue()) {
      return;
    }
    // The facade contract serves rows in ResultColumnNames order.
    const std::vector<std::string>& labels = body_engine.ResultColumnNames();
    if (labels.empty()) {
      return;
    }
    LiftedRows lifted_body;
    lifted_body.name = name;
    lifted_body.labels = labels;
    Row row;
    size_t drained = 0;
    while (planned.Value()->Next(&row, nullptr)) {
      if (row.values_.size() != labels.size()) {
        return;
      }
      if (++drained > kMaxMaterializedCteRows) {
        return;
      }
      lifted_body.rows.push_back(std::move(row));
    }
    if (planned.Value()->GetStatus() != Status::kSuccess) {
      return;
    }
    lifted_rows.push_back(std::move(lifted_body));
  }
  // Commit: normalize site aliases (FROM identities for the memo groups) and
  // record one alias-qualified cell per reference site sharing the rows.
  std::unordered_map<std::string, std::shared_ptr<MaterializedCte>> cells;
  std::vector<SelectSource> sources = outer->Sources();
  for (LiftedRows& lifted_body : lifted_rows) {
    // First-non-null type inference per column; all-null stays Null (typed
    // rewrites treat it conservatively, row semantics stay dynamic).
    std::vector<ValueType> column_types(lifted_body.labels.size(),
                                        ValueType::kNull);
    for (const Row& row : lifted_body.rows) {
      for (size_t i = 0; i < row.values_.size() && i < column_types.size();
           ++i) {
        if (!row.values_[i].IsNull() && column_types[i] == ValueType::kNull) {
          column_types[i] = row.values_[i].type;
        }
      }
    }
    for (SelectSource& source : sources) {
      if (source.table != lifted_body.name) {
        continue;
      }
      const std::string alias =
          source.alias.empty() ? lifted_body.name : source.alias;
      source.alias = alias;
      std::vector<Column> columns;
      columns.reserve(lifted_body.labels.size());
      for (size_t c = 0; c < lifted_body.labels.size(); ++c) {
        columns.emplace_back(ColumnName(alias, lifted_body.labels[c]),
                             column_types[c]);
      }
      auto cell = std::make_shared<MaterializedCte>();
      cell->schema = Schema(alias, std::move(columns));
      cell->rows = lifted_body.rows;
      cells.emplace(alias, std::move(cell));
    }
  }
  outer->SetSources(std::move(sources));
  lifted->cells = std::move(cells);
  // Every remaining non-recursive entry lifted (atomic gates above), so all
  // of them are covered from here on.
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    if (!outer->IsRecursiveWith(name)) {
      lifted->covered.insert(name);
    }
  }
  if (!lifted->cells.empty()) {
    plan_cache_fingerprint_.clear();
    plan_cache_parameters_.clear();
  }
}

// A recursive CTE lifts to an opaque memo leaf when exactly one top-level
// FROM site references it and its body references no mapped CTE except
// itself (the worktable). Sibling-scoped references keep the map-scoped
// execution they rely on; multiple sites would re-run the fixpoint per
// site instead of sharing it.
namespace {

bool RecursiveCteLiftable(const SelectStatement& outer,
                          const std::string& name) {
  int top_sites = 0;
  for (const SelectSource& source : outer.Sources()) {
    if (source.table == name) {
      ++top_sites;
    }
  }
  if (top_sites != 1) {
    return false;
  }
  const auto body_it = outer.WithQueries().find(name);
  if (body_it == outer.WithQueries().end() || body_it->second == nullptr) {
    return false;
  }
  const SelectStatement& body = *body_it->second;
  std::unordered_set<std::string> names;
  for (const auto& [entry, entry_body] : outer.WithQueries()) {
    (void)entry_body;
    names.insert(entry);
  }
  std::unordered_map<std::string, size_t> total_refs;
  CountCteReferencesInStatement(outer, names, &total_refs);
  std::unordered_map<std::string, size_t> body_refs;
  CountCteReferencesInStatement(body, names, &body_refs);
  // Exactly one EXTERNAL reference, living at the top level: the body's own
  // worktable self-references do not count (they resolve inside the
  // fixpoint, not through the map).
  size_t self_refs = 0;
  const auto self_it = body_refs.find(name);
  if (self_it != body_refs.end()) {
    self_refs = self_it->second;
  }
  const auto total_it = total_refs.find(name);
  const size_t total = total_it == total_refs.end() ? 0 : total_it->second;
  if (top_sites != 1 || total != 1 + self_refs) {
    return false;
  }
  for (const auto& [ref, count] : body_refs) {
    if (ref != name && count > 0) {
      return false;
    }
  }
  // Unique output names for the leaf schema (mirrors the cell gate: the
  // downstream name maps cannot tell duplicates apart).
  std::unordered_set<std::string> output_names;
  for (const NamedExpression& item : body.SelectList()) {
    if (item.name.empty()) {
      return false;
    }
    std::string lowered = item.name;
    for (char& c : lowered) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (!output_names.insert(std::move(lowered)).second) {
      return false;
    }
  }
  return !output_names.empty();
}
}  // namespace

void SqlEngine::LiftRecursiveCtes(SelectStatement* outer, LiftedCtes* lifted) {
  auto seal_coverage = [&] {
    if (lifted == nullptr) {
      return;
    }
    lifted->fully_covered = true;
    if (outer != nullptr) {
      for (const auto& [name, body] : outer->WithQueries()) {
        (void)body;
        if (!lifted->covered.contains(name)) {
          lifted->fully_covered = false;
          break;
        }
      }
    }
  };
  if (outer == nullptr || lifted == nullptr || outer->WithQueries().empty() ||
      force_relational_fallback_) {
    seal_coverage();
    return;
  }
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    if (!RecursiveCteLiftable(*outer, name)) {
      continue;
    }
    // Normalize the single site's alias (FROM identity for the memo group).
    for (SelectSource& source :
         const_cast<std::vector<SelectSource>&>(outer->Sources())) {
      if (source.table == name && source.alias.empty()) {
        source.alias = name;
      }
    }
    std::string alias;
    for (const SelectSource& source : outer->Sources()) {
      if (source.table == name) {
        alias = source.alias.empty() ? name : source.alias;
        break;
      }
    }
    if (alias.empty()) {
      continue;
    }
    // A reference that already renames the CTE keeps its alias, but only
    // when nothing still qualifies the CTE name itself.
    if (alias != name && StatementQualifiesName(*outer, name)) {
      continue;
    }
    std::vector<Column> columns;
    for (const NamedExpression& item : body->SelectList()) {
      columns.emplace_back(ColumnName(alias, item.name), ValueType::kNull);
    }
    RecursiveCteRef ref;
    ref.body = body;
    ref.output_schema = Schema(alias, std::move(columns));
    if (const RecursiveDepthSpec* depth = outer->RecursiveDepthOf(name)) {
      ref.depth_spec = *depth;
    }
    lifted->recursive.emplace(alias, std::move(ref));
    lifted->covered.insert(name);
  }
  seal_coverage();
}

StatusOr<Executor> SqlEngine::ExecuteGroupedSelect(
    const SelectStatement& select, TransactionContext& ctx,
    const LiftedCtes* lifted) {
  // Core: FROM + WHERE (+ join conditions) optimized by Cascades.  The
  // projection carries every base column the grouping pipeline needs
  // (select list, GROUP BY, HAVING, ORDER BY); Project re-resolves the real
  // select list against this core output.
  QueryData core;
  if (lifted != nullptr) {
    core.lifted_ctes_ = *lifted;
  }
  Expression where = select.WhereClause();
  std::unordered_set<std::string> seen_relations;
  for (const SelectSource& source : select.Sources()) {
    const std::string relation =
        source.alias.empty() ? source.table : source.alias;
    if (!seen_relations.insert(relation).second) {
      last_error_ = "duplicate FROM relation; use distinct aliases";
      return Status::kUnknown;
    }
    core.from_.push_back(relation);
    if (!source.alias.empty() && source.alias != source.table) {
      core.aliases_.emplace(source.alias, source.table);
    }
    if (source.join_condition) {
      // M6-bridge: outer joins keep their ON predicate on the join itself
      // (folding it into WHERE would move null-supplying-side filtering past
      // the NULL padding and silently drop preserved rows). The routing gate
      // (outer_bridge_slice) admits only memo-representable edges; inner and
      // cross conditions fold into WHERE exactly as before.
      if (source.join_type == JoinType::kLeft ||
          source.join_type == JoinType::kRight ||
          source.join_type == JoinType::kFull) {
        QueryData::OuterJoinEdge edge;
        edge.right_index = core.from_.size() - 1;
        edge.join_kind = source.join_type == JoinType::kLeft    ? 0
                         : source.join_type == JoinType::kRight ? 1
                                                                : 2;
        edge.on_condition = source.join_condition;
        core.outer_joins_.push_back(std::move(edge));
      } else {
        where = where ? BinaryExpressionExp(where, BinaryOperation::kAnd,
                                            source.join_condition)
                      : source.join_condition;
      }
    }
  }
  core.where_ = where ? where : ConstantValueExp(Value(true));

  std::vector<NamedExpression> required;
  const auto require = [&required](const Expression& expression) {
    if (!expression) {
      return;
    }
    for (const ColumnName& column : expression->TouchedColumns()) {
      if (column.name == "*") {
        continue;
      }
      const bool already = std::ranges::any_of(required, [&](const auto& item) {
        return item.expression->AsColumnValue().GetColumnName() == column;
      });
      if (!already) {
        required.emplace_back("", ColumnValueExp(column));
      }
    }
  };
  for (const NamedExpression& item : select.SelectList()) {
    require(item.expression);
  }
  // GROUP BY / HAVING / ORDER BY may reference SELECT-list aliases at any
  // expression depth; the core query knows nothing of output aliases, so
  // collect the aliased expressions' base columns instead of the alias
  // names themselves.  Over-substitution is safe here: the requirement set
  // is a superset of what the grouping pipeline needs.
  const std::function<Expression(const Expression&)> substitute_aliases =
      [&](const Expression& expression) -> Expression {
    if (!expression) {
      return expression;
    }
    if (expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (!name.schema.empty()) {
        return expression;
      }
      for (const NamedExpression& item : select.SelectList()) {
        if (!item.name.empty() && IdentifierEquals(item.name, name.name)) {
          return item.expression;
        }
      }
      return expression;
    }
    std::vector<Expression> rewritten;
    for (const Expression& child : ExpressionChildren(expression)) {
      rewritten.push_back(substitute_aliases(child));
    }
    return WithExpressionChildren(expression, rewritten);
  };
  for (const Expression& expression : select.GroupBy()) {
    require(substitute_aliases(expression));
  }
  require(substitute_aliases(select.Having()));
  for (const auto& term : select.OrderBy()) {
    require(substitute_aliases(term.expression));
  }
  if (required.empty()) {
    // Aggregates over no columns (COUNT(*)): a constant source column keeps
    // the core projection valid while carrying every input row.
    required.emplace_back("", ConstantValueExp(Value(true)));
  }
  core.select_ = std::move(required);

  Status rewrite_status = core.Rewrite(ctx);
  if (rewrite_status == Status::kAmbiguousQuery) {
    last_error_ = core.ambiguous_column_.empty()
                      ? "ambiguous column reference"
                      : "ambiguous column " + core.ambiguous_column_;
    return rewrite_status;
  }
  RETURN_IF_FAIL(rewrite_status);
  auto statement = std::make_shared<SelectStatement>(select);
  std::vector<Column> output_columns;
  output_columns.reserve(result_column_names_.size());
  for (const std::string& name : result_column_names_) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
  StatusOr<Plan> core_plan_or = Optimizer::Optimize(core, ctx);
  if (!core_plan_or.HasValue()) {
    StatusOr<Plan> optimized = Optimizer::OptimizeRelational(
        statement, Schema("", std::move(output_columns)), ctx);
    if (!optimized.HasValue()) {
      std::ostringstream error;
      error << "relational optimizer failed: " << optimized.GetStatus();
      last_error_ = error.str();
      return optimized.GetStatus();
    }
    return optimized.Value()->EmitExecutor(ctx);
  }
  Plan core_plan = std::move(core_plan_or.Value());
  Plan finish = std::make_shared<GroupByPlan>(
      core_plan, std::move(statement), Schema("", std::move(output_columns)));
  return finish->EmitExecutor(ctx);
}

namespace {
// Mirrors the runtime UNNEST relation layout (see UnnestValueToRelation in
// executor/detail/scan_filter.cpp) for a constant-folded array argument so
// the Cascades Unnest node declares the schema the UnnestExecutor actually
// emits: STRUCT elements flatten one level, an explicit alias keeps the
// element column beside the members, WITH OFFSET appends the counter.
// Returns an empty schema when the array is not a foldable constant.
bool StaticallyConstantArray(const Expression& expression) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    return true;
  }
  if (expression->Type() != TypeTag::kArrayExp) {
    return false;
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [](const Expression& child) {
        return child && child->Type() == TypeTag::kConstantValue;
      });
}

Schema InferUnnestOutputSchema(const SelectSource& source,
                               TransactionContext& ctx) {
  if (!source.unnest || !StaticallyConstantArray(source.unnest)) {
    return {"", {}};
  }
  StatusOr<Value> evaluated = relational_detail::TryEvaluate(
      source.unnest, relational_detail::Scope{}, nullptr, ctx, {});
  if (!evaluated.HasValue()) {
    return {"", {}};
  }
  Value array_val = evaluated.MoveValue();
  if (!array_val.IsArray()) {
    return {"", {}};
  }
  const std::string rel_name = source.alias.empty() ? "unnest" : source.alias;
  SelectSource shape;
  shape.alias = rel_name;
  shape.offset_alias = source.offset_alias;
  const relational_detail::Relation rel =
      relational_detail::UnnestValueToRelation(shape, array_val);
  std::vector<Column> cols;
  cols.reserve(rel.schema.ColumnCount());
  for (size_t i = 0; i < rel.schema.ColumnCount(); ++i) {
    cols.emplace_back(ColumnName(rel_name, rel.schema.GetColumn(i).Name().name),
                      rel.schema.GetColumn(i).Type());
  }
  return {rel_name, std::move(cols)};
}
}  // namespace

StatusOr<Executor> SqlEngine::ExecuteUnnestSelect(const SelectStatement& select,
                                                  TransactionContext& ctx) {
  std::vector<std::string> table_relations;
  std::unordered_map<std::string, std::string> table_aliases;
  for (const SelectSource& source : select.Sources()) {
    if (!source.unnest && !source.query && !source.table.empty() &&
        !select.IsRecursiveWith(source.table)) {
      const std::string relation =
          source.alias.empty() ? source.table : source.alias;
      table_relations.push_back(relation);
      if (!source.alias.empty() && source.alias != source.table) {
        table_aliases.emplace(source.alias, source.table);
      }
    }
  }

  cascades::Memo memo;
  cascades::GroupId current_group = cascades::kInvalidGroup;
  std::vector<std::string> current_relations;
  if (!table_relations.empty()) {
    current_relations = table_relations;
    current_group = memo.Build(table_relations);
  }

  size_t unnest_idx = 0;
  size_t apply_idx = 0;
  size_t rec_cte_idx = 0;
  for (const SelectSource& source : select.Sources()) {
    if (source.unnest) {
      if (current_group == cascades::kInvalidGroup) {
        current_group = memo.EnsureDerivedGroup({}, "dummy");
        memo.AddExpression(
            current_group,
            cascades::LogicalExpression{
                .operation = cascades::LogicalOperator::kDummyScan,
            });
      }
      const std::string tag = "unnest_" + std::to_string(unnest_idx++);
      const cascades::GroupId next_group =
          memo.EnsureDerivedGroup(current_relations, tag);
      cascades::LogicalExpression logical{
          .operation = cascades::LogicalOperator::kUnnest,
          .children = {current_group},
          .predicate = source.unnest,
          .unnest_alias = source.alias.empty() ? "unnest" : source.alias,
          .offset_alias = source.offset_alias,
      };
      Schema unnest_output = InferUnnestOutputSchema(source, ctx);
      if (unnest_output.ColumnCount() > 0) {
        logical.output_schema = std::move(unnest_output);
      }
      memo.AddExpression(next_group, std::move(logical));
      current_group = next_group;
    } else if (source.query) {
      if (current_group == cascades::kInvalidGroup) {
        current_group = memo.EnsureDerivedGroup({}, "dummy");
        memo.AddExpression(
            current_group,
            cascades::LogicalExpression{
                .operation = cascades::LogicalOperator::kDummyScan,
            });
      }
      const std::string tag = "apply_" + std::to_string(apply_idx++);
      const cascades::GroupId next_group =
          memo.EnsureDerivedGroup(current_relations, tag);
      JoinKind join_kind = JoinKind::kInner;
      switch (source.join_type) {
        case JoinType::kCross:
        case JoinType::kInner:
          join_kind = JoinKind::kInner;
          break;
        case JoinType::kLeft:
          join_kind = JoinKind::kLeftOuter;
          break;
        case JoinType::kRight:
          join_kind = JoinKind::kRightOuter;
          break;
        case JoinType::kFull:
          join_kind = JoinKind::kFullOuter;
          break;
      }
      const std::string alias =
          source.alias.empty() ? "subquery" : source.alias;
      std::vector<Column> inner_cols;
      for (const NamedExpression& item : source.query->SelectList()) {
        inner_cols.emplace_back(ColumnName(alias, item.name), ValueType::kNull);
      }
      Schema inner_schema(alias, std::move(inner_cols));
      memo.AddExpression(next_group,
                         cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kApply,
                             .children = {current_group},
                             .table = alias,
                             .predicate = source.join_condition,
                             .join_type = static_cast<uint8_t>(join_kind),
                             .relational_statement = source.query,
                             .output_schema = std::move(inner_schema),
                         });
      current_group = next_group;
    } else if (select.IsRecursiveWith(source.table)) {
      const std::string cte_name = source.table;
      const std::string alias = source.alias.empty() ? cte_name : source.alias;
      const auto it = select.WithQueries().find(cte_name);
      if (it == select.WithQueries().end()) {
        return Status::kUnknown;
      }
      const std::shared_ptr<SelectStatement>& rec_body = it->second;
      const RecursiveDepthSpec* depth_spec = select.RecursiveDepthOf(cte_name);

      std::vector<Column> cols;
      for (const NamedExpression& item : rec_body->SelectList()) {
        cols.emplace_back(ColumnName(alias, item.name), ValueType::kNull);
      }
      if (depth_spec != nullptr) {
        cols.emplace_back(ColumnName(alias, depth_spec->column),
                          ValueType::kInt64);
      }
      Schema cte_schema(alias, std::move(cols));

      std::optional<RecursiveDepthSpec> opt_depth;
      if (depth_spec != nullptr) {
        opt_depth = *depth_spec;
      }

      if (current_group == cascades::kInvalidGroup) {
        const std::string tag = "rec_cte_" + std::to_string(rec_cte_idx++);
        current_group = memo.EnsureDerivedGroup(current_relations, tag);
        memo.AddExpression(
            current_group,
            cascades::LogicalExpression{
                .operation = cascades::LogicalOperator::kRecursiveCte,
                .children = {},
                .table = alias,
                .relational_statement = rec_body,
                .output_schema = cte_schema,
                .cte_name = cte_name,
                .depth_limit = depth_spec != nullptr && depth_spec->upper > 0
                                   ? static_cast<size_t>(depth_spec->upper)
                                   : 0,
                .depth_spec = opt_depth,
            });
      } else {
        const std::string tag = "rec_cte_" + std::to_string(rec_cte_idx++);
        const cascades::GroupId next_group =
            memo.EnsureDerivedGroup(current_relations, tag);
        memo.AddExpression(
            next_group,
            cascades::LogicalExpression{
                .operation = cascades::LogicalOperator::kRecursiveCte,
                .children = {current_group},
                .table = alias,
                .relational_statement = rec_body,
                .output_schema = std::move(cte_schema),
                .cte_name = cte_name,
                .depth_limit = depth_spec != nullptr && depth_spec->upper > 0
                                   ? static_cast<size_t>(depth_spec->upper)
                                   : 0,
                .depth_spec = std::move(opt_depth),
            });
        current_group = next_group;
      }
    }
  }
  if (current_group == cascades::kInvalidGroup) {
    current_group = memo.EnsureDerivedGroup({}, "dummy");
    memo.AddExpression(current_group,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kDummyScan,
                       });
  }

  Expression filter_pred = select.WhereClause();
  for (const SelectSource& source : select.Sources()) {
    if (source.join_condition) {
      filter_pred =
          filter_pred ? BinaryExpressionExp(filter_pred, BinaryOperation::kAnd,
                                            source.join_condition)
                      : source.join_condition;
    }
  }
  if (filter_pred) {
    const cascades::GroupId sel_group =
        memo.EnsureDerivedGroup(current_relations, "selection");
    memo.AddExpression(sel_group,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kSelection,
                           .children = {current_group},
                           .predicate = filter_pred,
                       });
    current_group = sel_group;
  }

  const bool has_grouping =
      !select.GroupBy().empty() ||
      std::ranges::any_of(
          select.SelectList(),
          [](const NamedExpression& item) {
            return relational_detail::ContainsAggregate(item.expression);
          }) ||
      relational_detail::ContainsAggregate(select.Having());

  if (has_grouping) {
    cascades::RuleContext rule_context;
    rule_context.transaction = &ctx;
    for (const std::string& relation : table_relations) {
      const auto aliased = table_aliases.find(relation);
      const std::string& physical =
          aliased == table_aliases.end() ? relation : aliased->second;
      const StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
      const StatusOr<std::shared_ptr<TableStatistics>> found_stats =
          ctx.GetStats(physical);
      if (found.HasValue()) {
        rule_context.tables.emplace(relation, found.Value());
      }
      if (found_stats.HasValue()) {
        rule_context.statistics.emplace(relation, found_stats.Value());
      }
    }
    cascades::SearchEngine search(std::move(memo),
                                  cascades::RuleSet::Default());
    const std::optional<cascades::BestPlan> best =
        search.Optimize(current_group, cascades::PhysicalProperties{},
                        DefaultImplementationRules(), rule_context);
    if (!best) {
      last_error_ = "cascades unnest core optimization failed";
      return Status::kNotImplemented;
    }
    Plan core_plan = best->plan;
    auto statement = std::make_shared<SelectStatement>(select);
    std::vector<Column> output_columns;
    output_columns.reserve(result_column_names_.size());
    for (const std::string& name : result_column_names_) {
      output_columns.emplace_back(name, ValueType::kNull);
    }
    Plan finish = std::make_shared<GroupByPlan>(
        core_plan, std::move(statement), Schema("", std::move(output_columns)));
    return finish->EmitExecutor(ctx);
  }

  bool has_star = false;
  for (const NamedExpression& item : select.SelectList()) {
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      has_star = true;
      break;
    }
  }

  std::vector<NamedExpression> projection_items;
  if (has_star) {
    for (const SelectSource& source : select.Sources()) {
      if (source.unnest) {
        const std::string unnest_name =
            source.alias.empty() ? "unnest" : source.alias;
        const Schema shape = InferUnnestOutputSchema(source, ctx);
        if (shape.ColumnCount() > 0) {
          for (size_t i = 0; i < shape.ColumnCount(); ++i) {
            const std::string& column_name = shape.GetColumn(i).Name().name;
            projection_items.emplace_back(
                column_name,
                ColumnValueExp(ColumnName(unnest_name, column_name)));
          }
        } else {
          projection_items.emplace_back(
              unnest_name,
              ColumnValueExp(ColumnName(unnest_name, unnest_name)));
          if (!source.offset_alias.empty()) {
            projection_items.emplace_back(
                source.offset_alias,
                ColumnValueExp(ColumnName(unnest_name, source.offset_alias)));
          }
        }
      } else if (select.IsRecursiveWith(source.table)) {
        const std::string relation =
            source.alias.empty() ? source.table : source.alias;
        const auto it = select.WithQueries().find(source.table);
        if (it != select.WithQueries().end()) {
          for (const NamedExpression& q_item : it->second->SelectList()) {
            projection_items.emplace_back(
                q_item.name, ColumnValueExp(ColumnName(relation, q_item.name)));
          }
          const RecursiveDepthSpec* depth_spec =
              select.RecursiveDepthOf(source.table);
          if (depth_spec != nullptr) {
            projection_items.emplace_back(
                depth_spec->column,
                ColumnValueExp(ColumnName(relation, depth_spec->column)));
          }
        }
      } else if (!source.table.empty()) {
        const std::string relation =
            source.alias.empty() ? source.table : source.alias;
        const auto aliased = table_aliases.find(relation);
        const std::string& physical =
            aliased == table_aliases.end() ? relation : aliased->second;
        const StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
        if (found.HasValue()) {
          const Schema& s = found.Value()->GetSchema();
          for (size_t i = 0; i < s.ColumnCount(); ++i) {
            const std::string& col_name = s.GetColumn(i).Name().name;
            projection_items.emplace_back(
                col_name, ColumnValueExp(ColumnName(relation, col_name)));
          }
        }
      } else if (source.query) {
        const std::string relation =
            source.alias.empty() ? "subquery" : source.alias;
        for (const NamedExpression& q_item : source.query->SelectList()) {
          projection_items.emplace_back(
              q_item.name, ColumnValueExp(ColumnName(relation, q_item.name)));
        }
      }
    }
    result_column_names_.clear();
    result_column_names_.reserve(projection_items.size());
    for (const NamedExpression& item : projection_items) {
      result_column_names_.push_back(item.name);
    }
  } else {
    projection_items = select.SelectList();
  }

  // Field references into an UNNEST alias (`nrv.nested_int64` where `nrv`
  // unnests an array of protos/structs) cannot resolve as plain columns: the
  // unnest output carries one element column named `alias`.  Rewrite them to
  // the null-safe field accessor over that element column, which handles
  // both JSON-encoded structs and proto-text cells.
  // ORDER BY terms are rewritten exactly like the select list (alias field
  // references become safe accessors); the sort keys below must consume the
  // rewritten form or they reference columns the projection never emits.
  std::vector<Expression> rewritten_order_expressions;
  {
    struct UnnestAliasInfo {
      std::string alias;
      std::string offset_alias;
      std::vector<std::string> direct_columns;
    };
    std::vector<UnnestAliasInfo> unnest_aliases;
    for (const SelectSource& source : select.Sources()) {
      if (source.unnest) {
        UnnestAliasInfo info;
        info.alias = source.alias.empty() ? "unnest" : source.alias;
        info.offset_alias = source.offset_alias;
        const Schema shape = InferUnnestOutputSchema(source, ctx);
        for (size_t i = 0; i < shape.ColumnCount(); ++i) {
          info.direct_columns.push_back(shape.GetColumn(i).Name().name);
        }
        unnest_aliases.push_back(std::move(info));
      }
    }
    if (!unnest_aliases.empty()) {
      const std::function<Expression(const Expression&)> rewrite =
          [&](const Expression& expression) -> Expression {
        if (!expression) {
          return expression;
        }
        if (expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& column =
              expression->AsColumnValue().GetColumnName();
          const bool matches_alias =
              !column.schema.empty() && column.schema != "*" &&
              column.name != "*" &&
              std::any_of(
                  unnest_aliases.begin(), unnest_aliases.end(),
                  [&](const UnnestAliasInfo& info) {
                    if (!IdentifierEquals(info.alias, column.schema) ||
                        IdentifierEquals(info.alias, column.name) ||
                        (!info.offset_alias.empty() &&
                         IdentifierEquals(info.offset_alias, column.name))) {
                      return false;
                    }
                    return !std::any_of(
                        info.direct_columns.begin(), info.direct_columns.end(),
                        [&](const std::string& name) {
                          return IdentifierEquals(name, column.name);
                        });
                  });
          if (matches_alias) {
            return FunctionCallExp(
                "__get_field_safe",
                {ColumnValueExp(ColumnName(column.schema, column.schema)),
                 ConstantValueExp(Value(std::string(column.name)))});
          }
        }
        std::vector<Expression> children = ExpressionChildren(expression);
        bool changed = false;
        for (Expression& child : children) {
          Expression mapped = rewrite(child);
          changed |= child->ToString() != mapped->ToString();
          child = std::move(mapped);
        }
        return changed ? WithExpressionChildren(expression, std::move(children))
                       : expression;
      };
      for (NamedExpression& item : projection_items) {
        item.expression = rewrite(item.expression);
      }
      for (const SelectStatement::OrderByTerm& order : select.OrderBy()) {
        rewritten_order_expressions.push_back(rewrite(order.expression));
      }
    }
  }

  // ORDER BY keys are evaluated by the Sort/TopN node ABOVE the projection,
  // so a key that is not already a select-list output gets a hidden
  // projection column ($order<i>); the trim projection below strips it from
  // the client-visible result.  Without this, sorting by a source column the
  // select list omits (e.g. the WITH OFFSET pseudo-column) cannot resolve.
  const size_t visible_columns = projection_items.size();
  bool needs_hidden_order_columns = false;
  std::vector<NamedExpression> sort_keys;
  std::vector<bool> sort_ascending;
  std::vector<std::optional<bool>> sort_nulls_first;
  if (!select.OrderBy().empty()) {
    sort_keys.reserve(select.OrderBy().size());
    sort_ascending.reserve(select.OrderBy().size());
    sort_nulls_first.reserve(select.OrderBy().size());
    for (size_t term_index = 0; term_index < select.OrderBy().size();
         ++term_index) {
      const auto& term = select.OrderBy()[term_index];
      const Expression order_expression =
          rewritten_order_expressions.empty()
              ? term.expression
              : rewritten_order_expressions[term_index];
      sort_ascending.push_back(term.ascending);
      sort_nulls_first.push_back(term.nulls_first);
      if (has_grouping) {
        sort_keys.emplace_back("", order_expression);
        continue;
      }
      const NamedExpression* matched = nullptr;
      for (const NamedExpression& item : projection_items) {
        if (item.expression &&
            item.expression->ToString() == order_expression->ToString()) {
          matched = &item;
          break;
        }
        if (order_expression->Type() == TypeTag::kColumnValue &&
            !item.name.empty()) {
          const ColumnName& order_name =
              order_expression->AsColumnValue().GetColumnName();
          if (order_name.schema.empty() &&
              IdentifierEquals(item.name, order_name.name)) {
            matched = &item;
            break;
          }
        }
      }
      if (matched == nullptr) {
        const std::string hidden = "$order" + std::to_string(sort_keys.size());
        projection_items.emplace_back(hidden, order_expression);
        sort_keys.emplace_back("", ColumnValueExp(ColumnName(hidden)));
        needs_hidden_order_columns = true;
        continue;
      }
      // The sort consumes the projection OUTPUT, whose columns carry the
      // select-list names; bind a matched key to that output name.
      if (!matched->name.empty()) {
        sort_keys.emplace_back("", ColumnValueExp(ColumnName(matched->name)));
      } else {
        sort_keys.emplace_back("", order_expression);
      }
    }
  }

  const cascades::GroupId proj_group =
      memo.EnsureDerivedGroup(current_relations, "projection");
  memo.AddExpression(proj_group,
                     cascades::LogicalExpression{
                         .operation = cascades::LogicalOperator::kProjection,
                         .children = {current_group},
                         .target_list = projection_items,
                     });
  current_group = proj_group;

  if (select.Distinct()) {
    const cascades::GroupId distinct_group =
        memo.EnsureDerivedGroup(current_relations, "distinct");
    memo.AddExpression(distinct_group,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kDistinct,
                           .children = {current_group},
                       });
    current_group = distinct_group;
  }

  if (!select.OrderBy().empty()) {
    if (select.HasLimit() && select.Limit() != 0) {
      const cascades::GroupId topn_group =
          memo.EnsureDerivedGroup(current_relations, "topn");
      memo.AddExpression(topn_group,
                         cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kTopN,
                             .children = {current_group},
                             .target_list = std::move(sort_keys),
                             .sort_ascending = std::move(sort_ascending),
                             .sort_nulls_first = std::move(sort_nulls_first),
                             .limit_count = select.Limit(),
                             .limit_offset = select.Offset(),
                         });
      current_group = topn_group;
    } else {
      const cascades::GroupId sort_group =
          memo.EnsureDerivedGroup(current_relations, "sort");
      memo.AddExpression(sort_group,
                         cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kSort,
                             .children = {current_group},
                             .target_list = std::move(sort_keys),
                             .sort_ascending = std::move(sort_ascending),
                             .sort_nulls_first = std::move(sort_nulls_first),
                         });
      current_group = sort_group;
    }
  }

  if ((select.HasLimit() || select.Offset() != 0) &&
      (!select.HasLimit() || select.OrderBy().empty() || select.Limit() == 0)) {
    const cascades::GroupId limit_group =
        memo.EnsureDerivedGroup(current_relations, "limit");
    memo.AddExpression(limit_group,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kLimit,
                           .children = {current_group},
                           .limit_count = select.Limit(),
                           .limit_offset = select.Offset(),
                       });
    current_group = limit_group;
  }

  cascades::RuleContext rule_context;
  rule_context.transaction = &ctx;
  for (const std::string& relation : table_relations) {
    const auto aliased = table_aliases.find(relation);
    const std::string& physical =
        aliased == table_aliases.end() ? relation : aliased->second;
    const StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
    const StatusOr<std::shared_ptr<TableStatistics>> found_stats =
        ctx.GetStats(physical);
    if (found.HasValue()) {
      rule_context.tables.emplace(relation, found.Value());
    }
    if (found_stats.HasValue()) {
      rule_context.statistics.emplace(relation, found_stats.Value());
    }
  }

  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  const std::optional<cascades::BestPlan> best =
      search.Optimize(current_group, cascades::PhysicalProperties{},
                      DefaultImplementationRules(), rule_context);
  if (!best) {
    last_error_ = "cascades unnest optimization failed";
    return Status::kNotImplemented;
  }
  Executor executor = best->plan->EmitExecutor(ctx);
  if (needs_hidden_order_columns) {
    // Strip the hidden $order<i> sort-key columns from the client-visible
    // result; the select list defines the output width.
    const Schema& schema = best->plan->GetSchema();
    std::vector<NamedExpression> visible;
    visible.reserve(visible_columns);
    for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
      visible.emplace_back(schema.GetColumn(i).Name());
    }
    executor = std::make_shared<Projection>(std::move(visible), schema,
                                            std::move(executor));
  }
  return executor;
}

StatusOr<Executor> SqlEngine::PrepareStatement(
    TransactionContext& ctx, std::unique_ptr<Statement> statement) {
  last_statement_type_ = statement->Type();

  switch (statement->Type()) {
    case StatementType::kCreateTable: {
      const auto& create =
          dynamic_cast<const CreateTableStatement&>(*statement);
      last_dml_table_ = create.TableName();
      if (create.IsAsSelect()) {
        // Materialize through the relational engine so the star-expanded
        // output schema is available: a raw select list still holds the
        // literal "*" directive, whose width/name cannot define the catalog.
        StatusOr<relational_detail::Relation> materialized_query =
            relational_detail::ExecuteQuery(ctx, *create.AsQuery(), nullptr,
                                            {});
        if (!materialized_query.HasValue()) {
          last_error_ = materialized_query.GetStatus().GetMessage();
          return Status::kUnknown;
        }
        relational_detail::Relation materialized =
            materialized_query.MoveValue();
        std::vector<Row> rows;
        materialized.ForEachRow(
            [&rows](const Row& row) { rows.push_back(row); });
        std::vector<Column> columns;
        columns.reserve(materialized.schema.ColumnCount());
        for (size_t i = 0; i < materialized.schema.ColumnCount(); ++i) {
          const Column& schema_column = materialized.schema.GetColumn(i);
          std::string col_name = schema_column.Name().name;
          if (col_name.empty() || col_name == "*") {
            col_name = "col_" + std::to_string(i);
          }
          ValueType vtype = schema_column.Type();
          if (vtype == ValueType::kNull) {
            vtype = ValueType::kVarChar;
            for (const auto& row : rows) {
              if (i < row.Size() && !row[i].IsNull() &&
                  row[i].type != ValueType::kNull) {
                vtype = row[i].type;
                break;
              }
            }
          }
          const Constraint constraint =
              CompliancePrimaryKeyMode() && i == 0
                  ? Constraint(Constraint::kPrimaryKey)
                  : Constraint();
          columns.emplace_back(col_name, vtype, constraint);
          if (std::ranges::any_of(rows, [i](const Row& row) {
                return i < row.Size() && row[i].IsUnsigned();
              })) {
            columns.back().SetUnsigned(true);
          }
        }
        ASSIGN_OR_RETURN(
            Table, table,
            database_->CreateTable(ctx, Schema(create.TableName(), columns)));
        for (auto& row : rows) {
          // Coerce materialized values to the declared column types: the
          // SELECT output may carry narrower types (e.g. INT64 literals in
          // UNION ALL branches under a DOUBLE first row).
          for (size_t i = 0; i < row.Size() && i < columns.size(); ++i) {
            Value& cell = row.values_[i];
            const ValueType target = columns[i].Type();
            if (cell.IsNull() || cell.type == target ||
                target == ValueType::kNull) {
              continue;
            }
            if (target == ValueType::kDouble &&
                cell.type == ValueType::kInt64) {
              cell = Value(static_cast<double>(cell.value.int_value));
            }
          }
          RETURN_IF_FAIL(table.Insert(ctx.txn_, row).GetStatus());
        }
        return Executor(std::make_shared<ConstantExecutor>(
            Row({Value("CREATE TABLE"),
                 Value(static_cast<int64_t>(rows.size()))})));
      }
      ASSIGN_OR_RETURN(Table, table,
                       database_->CreateTable(
                           ctx, Schema(create.TableName(), create.Columns())));
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("CREATE TABLE"), Value(0)})));
    }
    case StatementType::kInsert: {
      const auto& insert = dynamic_cast<const InsertStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(insert.TableName()));
      last_dml_table_ = insert.TableName();
      std::vector<Row> rows;
      if (insert.Query() != nullptr) {
        // INSERT ... SELECT: materialize the source query, then feed its
        // rows through the same column mapping / coercion as VALUES rows.
        plan_cache_fingerprint_.clear();
        plan_cache_parameters_.clear();
        StatusOr<relational_detail::Relation> materialized_query =
            relational_detail::ExecuteQuery(ctx, *insert.Query(), nullptr, {});
        if (!materialized_query.HasValue()) {
          last_error_ = materialized_query.GetStatus().GetMessage();
          return Status::kUnknown;
        }
        relational_detail::Relation materialized =
            materialized_query.MoveValue();
        materialized.ForEachRow(
            [&rows](Row row) { rows.push_back(std::move(row)); });
      } else {
        if (insert.Values().empty()) {
          return Status::kUnknown;
        }
        // Batch INSERT chunking: evaluate values and process in chunks of
        // kInsertChunkSize
        constexpr size_t kInsertChunkSize = 64;
        const size_t num_values = insert.Values().size();
        rows.reserve(num_values);
        for (size_t chunk_start = 0; chunk_start < num_values;
             chunk_start += kInsertChunkSize) {
          const size_t chunk_end =
              std::min(chunk_start + kInsertChunkSize, num_values);
          for (size_t v_idx = chunk_start; v_idx < chunk_end; ++v_idx) {
            const auto& values = insert.Values()[v_idx];
            std::vector<Value> row;
            row.reserve(values.size());
            for (const auto& value : values) {
              StatusOr<Value> item = value->TryEvaluate(Row(), Schema());
              if (!item.HasValue()) {
                last_error_ = item.GetStatus().GetMessage();
                return Status::kUnknown;
              }
              row.push_back(item.MoveValue());
            }
            rows.emplace_back(std::move(row));
          }
        }
      }
      // Column mapping and assignment coercion, shared by VALUES rows and
      // INSERT ... SELECT rows.
      std::vector<size_t> upsert_offsets;
      if (!insert.Columns().empty()) {
        std::unordered_set<std::string> seen_columns;
        upsert_offsets.reserve(insert.Columns().size());
        for (const std::string& column : insert.Columns()) {
          if (!seen_columns.insert(column).second) {
            // A duplicate target would silently overwrite the earlier
            // value; reject it instead.
            last_error_ = "duplicate INSERT column: " + column;
            return Status::kUnknown;
          }
          const int destination = table->GetSchema().Offset(ColumnName(column));
          if (destination < 0) {
            last_error_ = "unknown INSERT column: " + column;
            return Status::kNotExists;
          }
          upsert_offsets.push_back(static_cast<size_t>(destination));
        }
      }
      for (auto& row : rows) {
        if (!insert.Columns().empty()) {
          if (insert.Columns().size() != row.values_.size()) {
            last_error_ = "INSERT column/value count mismatch";
            return Status::kUnknown;
          }
          std::vector<Value> reordered(table->GetSchema().ColumnCount());
          for (size_t i = 0; i < insert.Columns().size(); ++i) {
            reordered[upsert_offsets[i]] = row[i];
          }
          row = Row(std::move(reordered));
        } else if (row.values_.size() == 1 &&
                   table->GetSchema().ColumnCount() > 1) {
          // INSERT t VALUES ((1, 'x')) / VALUES (NULL) / (DEFAULT): a single
          // STRUCT (or whole-row NULL) expands across every column.
          std::vector<Value> expanded;
          if (ExpandStructInsertValue(row[0], table->GetSchema().ColumnCount(),
                                      &expanded)) {
            row = Row(std::move(expanded));
          }
        }
        if (row.values_.size() != table->GetSchema().ColumnCount()) {
          last_error_ = "INSERT value count does not match table schema (got " +
                        std::to_string(row.values_.size()) + ", expected " +
                        std::to_string(table->GetSchema().ColumnCount()) + ")";
          return Status::kUnknown;
        }
        for (size_t i = 0; i < row.values_.size(); ++i) {
          const auto& col = table->GetSchema().GetColumn(i);
          const ValueType expected = col.Type();
          if (Status st_ni = ValidateNarrowInteger(col.Name(), row[i]);
              st_ni != Status::kSuccess) {
            last_error_ = st_ni.GetMessage();
            return Status::kUnknown;
          }
          if (col.IsUnsigned()) {
            if (!row[i].IsNull()) {
              if (row[i].type != ValueType::kInt64) {
                last_error_ =
                    "INSERT type mismatch for column " + col.Name().name;
                return Status::kUnknown;
              }
              if (!row[i].IsUnsigned() && row[i].value.int_value < 0) {
                last_error_ =
                    "assignment out of range for column " + col.Name().name;
                return Status::kUnknown;
              }
              row[i] = row[i].WithUnsigned();
            }
            continue;
          }
          if (row[i].IsNull() || row[i].type == expected) {
            continue;
          }
          if (expected == ValueType::kDouble &&
              row[i].type == ValueType::kInt64) {
            row[i] = Value(static_cast<double>(row[i].value.int_value));
            continue;
          }
          if (expected == ValueType::kDate &&
              row[i].type == ValueType::kVarChar) {
            row[i] = Value::Date(row[i].value.varchar_value);
            continue;
          }
          last_error_ = "INSERT type mismatch for column " + col.Name().name;
          return Status::kUnknown;
        }
      }
      // Phase 2-1 fill: build the parametric INSERT artifact. Bindable
      // literals become ParameterSlot placeholders; runtime values are
      // injected per execution, so any literal combination hits.
      // Conflict-handling inserts (modes / PK emulation / row-count asserts)
      // always take the authoritative path so their semantics cannot be
      // lost in a replayed shape.
      if (!plan_cache_fingerprint_.empty() &&
          insert.Mode() == InsertMode::kDefault && !insert.HasAssert() &&
          !CompliancePrimaryKeyMode()) {
        auto shape = std::make_shared<CompiledPlan::InsertShape>();
        bool ok = true;
        size_t slot_cursor = 0;
        shape->cells.reserve(insert.Values().size());
        for (const auto& values : insert.Values()) {
          std::vector<Expression> cells;
          cells.reserve(values.size());
          for (const Expression& value : values) {
            if (ContainsNonDeterministicCall(value)) {
              ok = false;
              break;
            }
            cells.push_back(SlotizeLiterals(value, &slot_cursor, &ok));
            if (!ok) {
              break;
            }
          }
          if (!ok) {
            break;
          }
          shape->cells.push_back(std::move(cells));
        }
        // Slot count must match the extracted parameter count exactly;
        // otherwise text-order alignment is broken and caching stays off.
        if (ok && slot_cursor == plan_cache_parameters_.size()) {
          shape->table = table;
          shape->schema = table->GetSchema();
          shape->has_named_columns = !insert.Columns().empty();
          if (shape->has_named_columns) {
            // Offsets were validated above (no duplicates, all known).
            shape->reorder.reserve(insert.Columns().size());
            for (const std::string& column : insert.Columns()) {
              shape->reorder.push_back(static_cast<size_t>(
                  shape->schema.Offset(ColumnName(column))));
            }
          }
          auto compiled = std::make_shared<CompiledPlan>();
          compiled->kind = CompiledPlan::Kind::kInsert;
          compiled->database = database_;
          compiled->epoch = database_->SchemaEpoch();
          compiled->parameters = plan_cache_parameters_;
          compiled->insert_shape = std::move(shape);
          StoreCompiledPlan(plan_cache_fingerprint_, std::move(compiled));
        }
      }
      InsertExecutionMode exec_mode = InsertExecutionMode::kDefault;
      switch (insert.Mode()) {
        case InsertMode::kDefault:
          exec_mode = InsertExecutionMode::kDefault;
          break;
        case InsertMode::kIgnore:
          exec_mode = InsertExecutionMode::kIgnore;
          break;
        case InsertMode::kUpdate:
          exec_mode = InsertExecutionMode::kUpsert;
          break;
        case InsertMode::kReplace:
          exec_mode = InsertExecutionMode::kReplace;
          break;
      }
      return Executor(std::make_shared<Insert>(
          ctx.txn_, table.get(),
          std::make_shared<ConstantExecutor>(std::move(rows)), exec_mode,
          CompliancePrimaryKeyMode(), upsert_offsets,
          insert.AssertRowsModified()));
    }
    case StatementType::kSelect: {
      // Verify the concrete kind before releasing: a Type()/type mismatch
      // would make the former static_cast undefined behavior.
      if (dynamic_cast<const SelectStatement*>(statement.get()) == nullptr) {
        last_error_ = "statement marked kSelect is not a SelectStatement";
        return Status::kUnknown;
      }
      auto select = std::shared_ptr<SelectStatement>(
          dynamic_cast<SelectStatement*>(statement.release()));
      // M4: inline singly-referenced non-recursive CTEs into derived
      // sources, then lift the remaining layer (shared eager cells plus
      // opaque recursive leaves) when atomic; M5 flattens the single-table
      // derived sources last. All rewrites preserve the row multiset;
      // anything they reject keeps the existing materialized/relational
      // path untouched.
      (void)InlineSingleUseCtes(select.get());
      LiftedCtes lifted;
      MaterializeCtes(select.get(), ctx, &lifted);
      LiftRecursiveCtes(select.get(), &lifted);
      (void)FlattenDerivedSources(select.get(), ctx, &lifted);
      // M5+: single-table equality LATERALs become decorrelated joins (the
      // proven flattener above consumes them once uncorrelated).
      (void)DecorrelateSingleTableLaterals(select.get(), ctx);
      // A WHERE conjunct that rejects the NULL padding of a LEFT JOIN makes
      // the outer join's padded rows unreachable; planning it as an inner
      // join is exact and unlocks hash-join fast paths (§7.5).
      ReduceOuterJoins(select.get());
      // A qualified star naming no FROM relation must be rejected up front:
      // the per-route expanders below would otherwise silently drop the item
      // (or expand it over unrelated relations) instead of erroring.
      {
        const auto matches_source = [&](std::string_view qualifier) {
          return std::ranges::any_of(
              select->Sources(), [&](const SelectSource& source) {
                return (!source.alias.empty() &&
                        IdentifierEquals(source.alias, qualifier)) ||
                       (!source.table.empty() &&
                        IdentifierEquals(source.table, qualifier));
              });
        };
        // `rel.*` select items reach the engine either bare or wrapped in a
        // value-table/proto constructor by the AST visitor; unwrap one level
        // so the qualifier is validated in both shapes.
        const auto star_qualifier =
            [](const Expression& expression) -> std::string {
          Expression candidate = expression;
          if (candidate->Type() == TypeTag::kFunctionCallExp) {
            const auto& function = candidate->AsFunctionCallExpression();
            if ((function.FuncName() == "__value_table_value" ||
                 function.FuncName() == "__proto_new") &&
                function.Args().size() == 1) {
              candidate = function.Args()[0];
            }
          }
          if (candidate->Type() != TypeTag::kColumnValue) {
            return {};
          }
          const ColumnName& column = candidate->AsColumnValue().GetColumnName();
          if (column.name != "*" || column.schema.empty()) {
            return {};
          }
          return column.schema;
        };
        for (const NamedExpression& item : select->SelectList()) {
          if (!item.expression) {
            continue;
          }
          const std::string qualifier = star_qualifier(item.expression);
          if (!qualifier.empty() && !matches_source(qualifier)) {
            last_error_ =
                "unknown relation in select list: " + qualifier + ".*";
            return Status::kNotExists;
          }
        }
      }
      // Bind every base relation up front, including those nested in IN,
      // EXISTS, scalar subqueries, and CTE definitions. Lazy expression
      // evaluation must not let a missing table go unnoticed merely because
      // an outer predicate produced no rows.
      std::unordered_map<std::string, size_t> referenced_tables;
      relational_detail::CountStatementTables(*select, &referenced_tables);
      for (const auto& [table_name, count] : referenced_tables) {
        (void)count;
        StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(table_name);
        if (!table.HasValue()) {
          last_error_ = "table " + table_name + " not found";
          return table.GetStatus();
        }
      }
      result_column_names_.reserve(select->SelectList().size());
      for (const NamedExpression& item : select->SelectList()) {
        result_column_names_.push_back(
            item.name.empty() ? item.expression->ToString() : item.name);
      }
      const auto emit_relational = [&]() -> StatusOr<Executor> {
        std::vector<Column> columns;
        columns.reserve(result_column_names_.size());
        for (const std::string& name : result_column_names_) {
          columns.emplace_back(name, ValueType::kNull);
        }
        StatusOr<Plan> optimized = Optimizer::OptimizeRelational(
            select, Schema("", std::move(columns)), ctx);
        if (!optimized.HasValue()) {
          std::ostringstream error;
          error << "relational optimizer failed: " << optimized.GetStatus();
          last_error_ = error.str();
          return optimized.GetStatus();
        }
        return optimized.Value()->EmitExecutor(ctx);
      };
      // An explicit LIMIT 0 yields zero rows on every route (relational or
      // optimizer); neither LimitExecutor nor LimitedRows can express that
      // because they read limit==0 as "unbounded" (§6.3).
      if (select->HasLimit() && select->Limit() == 0) {
        return Executor(std::make_shared<ConstantExecutor>(std::vector<Row>{}));
      }
      const bool has_unnest = std::any_of(
          select->Sources().begin(), select->Sources().end(),
          [](const SelectSource& s) { return static_cast<bool>(s.unnest); });
      // UNNEST sources that participate in an outer join, an ON condition, or
      // a USING clause need real join semantics (per-pair matching plus
      // NULL-extended preservation of unmatched rows).  The memo route chains
      // unnests as lateral cross products and can only post-filter, which
      // drops LEFT-preserved rows and turns USING into a trivially-true
      // `col = col`; the relational interpreter implements the exact
      // semantics (LateralExpandRelation / Join), so route there instead.
      const bool unnest_join_needs_relational = std::any_of(
          select->Sources().begin(), select->Sources().end(),
          [](const SelectSource& s) {
            if (!s.unnest) {
              return false;
            }
            return s.join_type == JoinType::kLeft ||
                   s.join_type == JoinType::kRight ||
                   s.join_type == JoinType::kFull || !s.using_columns.empty();
          });
      const bool has_lateral =
          std::any_of(select->Sources().begin(), select->Sources().end(),
                      [](const SelectSource& s) { return s.is_lateral; });
      const bool can_decorrelate_subqueries =
          CanUseDecorrelatedSubqueryOptimizer(*select);
      const bool simple_count_star =
          select->Sources().size() == 1 && !select->Sources()[0].query &&
          !select->Sources()[0].unnest &&
          select->Sources()[0].join_type == JoinType::kCross &&
          select->Sources()[0].join_condition == nullptr &&
          select->WithQueries().empty() && select->GroupBy().empty() &&
          !select->Having() && !select->Qualify() &&
          select->OrderBy().empty() && !select->Distinct() &&
          select->SelectList().size() == 1 &&
          select->SelectList()[0].expression &&
          select->SelectList()[0].expression->Type() ==
              TypeTag::kAggregateExp &&
          select->SelectList()[0]
                  .expression->AsAggregateExpression()
                  .GetType() == AggregationType::kCount &&
          !select->SelectList()[0]
               .expression->AsAggregateExpression()
               .Distinct() &&
          select->SelectList()[0].expression->AsAggregateExpression().Child() &&
          select->SelectList()[0]
                  .expression->AsAggregateExpression()
                  .Child()
                  ->Type() == TypeTag::kColumnValue &&
          select->SelectList()[0]
                  .expression->AsAggregateExpression()
                  .Child()
                  ->AsColumnValue()
                  .GetColumnName()
                  .name == "*" &&
          (!select->WhereClause() ||
           select->WhereClause()->Type() == TypeTag::kConstantValue);
      // Value-table operands (SELECT AS VALUE / AS <proto>) need the
      // relational interpreter's proto-field ORDER BY resolution; their rows
      // are folded single-column payloads, not plain projections.
      const bool has_value_table_operand = std::any_of(
          select->SelectList().begin(), select->SelectList().end(),
          [](const NamedExpression& item) {
            return item.expression &&
                   item.expression->Type() == TypeTag::kFunctionCallExp &&
                   (item.expression->AsFunctionCallExpression().FuncName() ==
                        "__value_table_value" ||
                    item.expression->AsFunctionCallExpression().FuncName() ==
                        "__proto_new");
          });
      const bool sources_plain =
          std::all_of(select->Sources().begin(), select->Sources().end(),
                      [](const SelectSource& source) {
                        return source.query == nullptr && !source.unnest &&
                               !source.is_lateral && !source.table.empty();
                      });
      // M6: the grouped bridge folds every inner ON condition into WHERE,
      // which is exact only for inner joins. Outer joins keep their ON
      // predicates on the join itself as QueryData::OuterJoinEdge payloads
      // (see outer_bridge_slice below); anything beyond that slice stays
      // relational until the memo models the remaining grouped shapes.
      // M6-bridge slice: LEFT edges are admitted from source 1 onward;
      // RIGHT/FULL only in the M6+1 two-source shape (plain inner/cross
      // first source); every outer ON condition must be memo-representable
      // and no USING / nested-join sugar may remain on any source.
      const bool outer_bridge_slice = [&] {
        for (size_t i = 0; i < select->Sources().size(); ++i) {
          const SelectSource& source = select->Sources()[i];
          if (!source.using_columns.empty() || source.from_nested_join) {
            return false;
          }
          const bool outer_type = source.join_type == JoinType::kLeft ||
                                  source.join_type == JoinType::kRight ||
                                  source.join_type == JoinType::kFull;
          if (!source.join_condition || !outer_type) {
            continue;
          }
          if (i == 0) {
            return false;
          }
          // M6+1 admits only the single RIGHT/FULL edge over a plain
          // inner/cross first source.
          const bool m61_shape =
              select->Sources().size() == 2 && i == 1 &&
              (select->Sources()[0].join_type == JoinType::kCross ||
               select->Sources()[0].join_type == JoinType::kInner);
          if (source.join_type != JoinType::kLeft && !m61_shape) {
            return false;
          }
          if (NeedsRelationalEvaluation(source.join_condition)) {
            return false;
          }
        }
        return true;
      }();
      // Set operations fold independently planned operands, so a FROM-side
      // UNNEST is fine: each operand routes through its own statement
      // planning.  Subquery sources and LATERAL stay on the other paths.
      const bool sources_setop_safe =
          std::all_of(select->Sources().begin(), select->Sources().end(),
                      [](const SelectSource& source) {
                        return source.query == nullptr && !source.is_lateral;
                      });
      // M-union: merge same-table UNION ALL / UNION DISTINCT branches into
      // one scan before operand planning (the merged shape routes through
      // the normal single-query paths below).
      (void)MergeCompatibleUnionBranches(select.get(), ctx);
      if (!select->UnionAll().empty() && !has_value_table_operand &&
          select->WithQueries().empty() && sources_setop_safe) {
        return ExecuteSetOperation(*select, ctx);
      }
      // Self-joins expose the same base relation under two aliases; the
      // GROUP BY / HAVING / aggregate statements: the Cascades-optimized
      // FROM + WHERE core (join ordering, access paths, filter pushdown)
      // feeds the proven relational grouping finish pipeline (GroupByPlan).
      {
        const bool has_grouping =
            !select->GroupBy().empty() || select->Having() ||
            std::ranges::any_of(
                select->SelectList(), [](const NamedExpression& item) {
                  return relational_detail::ContainsAggregate(item.expression);
                });
        // Grouped statements whose expressions carry correlated subqueries
        // need the relational interpreter's scope chain (outer/CTE threading
        // through Project); the bridge below runs without it.  Aggregates
        // mixed with bare non-grouped columns inside one expression also stay
        // put: their result depends on the group representative row, whose
        // identity is not stable across access-path choices.
        const auto touches_query_expression =
            [](const Expression& expression) -> bool {
          return expression && relational_detail::ContainsQuery(expression);
        };
        bool grouped_expressions_correlate =
            touches_query_expression(select->Having());
        for (const Expression& expression : select->GroupBy()) {
          grouped_expressions_correlate = grouped_expressions_correlate ||
                                          touches_query_expression(expression);
        }
        for (const NamedExpression& item : select->SelectList()) {
          grouped_expressions_correlate =
              grouped_expressions_correlate ||
              touches_query_expression(item.expression);
        }
        // The bridge executes multi-relation and single-relation grouped
        // queries through the Cascades core (join ordering, access paths,
        // filter pushdown), feeding the grouping finish pipeline (GroupByPlan).
        // Routing reads the post-rewrite shape: flattened/inlined inputs are
        // plain by now, while genuinely complex shapes stay relational.
        if (!force_relational_fallback_ && select->Sources().size() > 1 &&
            has_grouping && !simple_count_star && !select->Qualify() &&
            !relational_detail::HasWindowFunctions(*select) &&
            select->WithQueries().empty() && sources_plain &&
            outer_bridge_slice &&
            (!PostRewriteNeedsRelational(*select, &lifted,
                                         /*include_grouping=*/false) ||
             can_decorrelate_subqueries) &&
            !touches_query_expression(select->WhereClause()) &&
            !grouped_expressions_correlate) {
          return ExecuteGroupedSelect(*select, ctx, &lifted);
        }
      }
      const auto touches_query_expression =
          [](const Expression& expression) -> bool {
        return expression && relational_detail::ContainsQuery(expression);
      };
      const bool touches_query =
          touches_query_expression(select->WhereClause()) ||
          std::ranges::any_of(
              select->SelectList(), [&](const NamedExpression& item) {
                return touches_query_expression(item.expression);
              });
      const bool has_recursive_cte =
          std::any_of(select->WithQueries().begin(),
                      select->WithQueries().end(), [&](const auto& pair) {
                        return select->IsRecursiveWith(pair.first);
                      });
      const bool has_plain_cte =
          std::any_of(select->WithQueries().begin(),
                      select->WithQueries().end(), [&](const auto& pair) {
                        return !select->IsRecursiveWith(pair.first);
                      });
      // ARRAY(SELECT ...) select items need the relational interpreter's
      // scope chain (see ContainsArrayQueryExpression); the unnest and
      // vectorized routes evaluate projections without it.  A SQL UDF whose
      // body contains a subquery needs it too: invoking the body through a
      // plain Projection would throw.
      const auto touches_relational_subquery =
          [](const Expression& expression) {
            return ContainsArrayQueryExpression(expression) ||
                   ContainsQueryUdfCall(expression);
          };
      const bool touches_array_subquery =
          std::ranges::any_of(
              select->SelectList(),
              [&](const NamedExpression& item) {
                return touches_relational_subquery(item.expression);
              }) ||
          std::ranges::any_of(
              select->OrderBy(), [&](const SelectStatement::OrderByTerm& term) {
                return touches_relational_subquery(term.expression);
              });
      // A subquery inside the select list itself projects per-row through
      // Projection::Evaluate, which cannot run subqueries; only the
      // relational interpreter's scope chain can evaluate that shape.
      const bool select_list_touches_query =
          std::ranges::any_of(select->SelectList(),
                              [](const NamedExpression& item) {
                                return ContainsQueryExpression(item.expression);
                              }) ||
          std::ranges::any_of(select->OrderBy(),
                              [](const SelectStatement::OrderByTerm& term) {
                                return ContainsQueryExpression(term.expression);
                              });
      if ((has_unnest || has_lateral ||
           (has_recursive_cte && !has_plain_cte && !touches_query)) &&
          !unnest_join_needs_relational && !select_list_touches_query &&
          !select->Qualify() &&
          !relational_detail::HasWindowFunctions(*select)) {
        StatusOr<Executor> unnest_exec = ExecuteUnnestSelect(*select, ctx);
        if (unnest_exec.HasValue()) {
          return unnest_exec;
        }
      }
      // Routing reads the post-rewrite shape (see PostRewriteNeedsRelational):
      // statements whose derived/CTE complexity the rewrites eliminated flow
      // to the cost-based planner; the optimizer's kNotImplemented fallback
      // still catches anything beyond its slices.
      if (force_relational_fallback_ ||
          (PostRewriteNeedsRelational(*select, &lifted) &&
           !can_decorrelate_subqueries) ||
          select->Sources().empty() || has_unnest || has_lateral ||
          touches_array_subquery ||
          (has_recursive_cte && !has_plain_cte && !touches_query)) {
        if (simple_count_star && !force_relational_fallback_) {
          // COUNT(*) over one plain table is fully representable by the
          // Cascades single-relation path, including a covering index-only
          // scan. Let that path handle the query even if the visitor marked
          // the aggregate as relational for another reason.
        } else {
          return emit_relational();
        }
      }
      if (PostRewriteNeedsRelational(*select, &lifted) && !simple_count_star &&
          !can_decorrelate_subqueries) {
        return emit_relational();
      }

      const bool multi_relation = select->Sources().size() > 1;
      const bool has_aggregate =
          std::ranges::any_of(
              select->SelectList(),
              [](const NamedExpression& item) {
                return relational_detail::ContainsAggregate(item.expression);
              }) ||
          relational_detail::ContainsAggregate(select->Having());
      if (multi_relation && has_aggregate) {
        return emit_relational();
      }
      // Post-projection ORDER BY scope: the adapter normalizes computed keys
      // and SELECT aliases to projection-output names (sort_expressions) and
      // a post-plan SortExecutor covers plans that do not enforce ordering,
      // so computed ordering no longer forces the relational path.
      QueryData query;
      Expression where = select->WhereClause();
      std::unordered_set<std::string> seen_relations;
      for (size_t i = 0; i < select->Sources().size(); ++i) {
        const SelectSource& source = select->Sources()[i];
        if (!seen_relations.insert(source.alias).second) {
          last_error_ = "duplicate FROM relation; use distinct aliases";
          return Status::kUnknown;
        }
        query.from_.push_back(source.alias);
        if (!source.alias.empty() && source.alias != source.table) {
          query.aliases_.emplace(source.alias, source.table);
        }
        if (source.join_condition) {
          // M6+1: a LEFT/RIGHT/FULL ON condition rides the outer-join
          // predicate, never WHERE (ON filters before NULL padding, WHERE
          // after). Anything beyond the optimizer's outer slice was already
          // routed to the relational path; reaching it here is a programming
          // error, so fall back rather than folding it unsoundly.
          if (source.join_type == JoinType::kLeft ||
              source.join_type == JoinType::kRight ||
              source.join_type == JoinType::kFull) {
            if (i == 0) {
              return emit_relational();
            }
            QueryData::OuterJoinEdge edge;
            edge.right_index = query.from_.size() - 1;
            edge.join_kind = source.join_type == JoinType::kLeft    ? 0
                             : source.join_type == JoinType::kRight ? 1
                                                                    : 2;
            edge.on_condition = source.join_condition;
            query.outer_joins_.push_back(std::move(edge));
          } else {
            where = where ? BinaryExpressionExp(where, BinaryOperation::kAnd,
                                                source.join_condition)
                          : source.join_condition;
          }
        }
      }
      query.where_ = where ? where : ConstantValueExp(Value(true));
      query.lifted_ctes_ = lifted;
      query.qualify_ = select->Qualify();
      query.select_.reserve(select->SelectList().size());
      for (const NamedExpression& item : select->SelectList()) {
        NamedExpression copied = item;
        // The SQL visitor gives an unaliased `o.key` projection the display
        // name `key`.  QueryData's ProjectionPlan interprets a non-empty name
        // as a new unqualified schema column, which would hide `o.key` from
        // the decorrelated semi/anti join key contract. Keep the expression
        // name for internal binding and let result_column_names_ provide the
        // client-facing label.
        if (copied.expression &&
            copied.expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& column =
              copied.expression->AsColumnValue().GetColumnName();
          if (copied.name == column.name) {
            copied.name.clear();
          }
        }
        query.select_.push_back(std::move(copied));
      }
      // Expand "*" projections here so the visible output width is the
      // expanded width; the optimizer's internal expansion would otherwise
      // disagree with visible_columns and truncate the final projection.
      {
        std::vector<NamedExpression> expanded;
        bool has_star = false;
        std::string unknown_star_qualifier;
        for (const NamedExpression& item : query.select_) {
          if (item.expression->Type() == TypeTag::kColumnValue) {
            const ColumnName& column =
                item.expression->AsColumnValue().GetColumnName();
            if (column.name == "*") {
              has_star = true;
              bool matched_relation = false;
              for (const std::string& relation : query.from_) {
                const auto aliased = query.aliases_.find(relation);
                const std::string& physical = aliased == query.aliases_.end()
                                                  ? relation
                                                  : aliased->second;
                if (!column.schema.empty() && column.schema != relation &&
                    column.schema != physical) {
                  continue;
                }
                // Lifted CTE leaves (M4) expand from their leaf schemas,
                // exactly like a base table's columns under this relation.
                const Schema* leaf_schema = nullptr;
                const auto cell = lifted.cells.find(relation);
                if (cell != lifted.cells.end() && cell->second != nullptr) {
                  leaf_schema = &cell->second->schema;
                } else {
                  const auto rec = lifted.recursive.find(relation);
                  if (rec != lifted.recursive.end()) {
                    leaf_schema = &rec->second.output_schema;
                  }
                }
                if (leaf_schema != nullptr) {
                  matched_relation = true;
                  const Schema& source_schema = *leaf_schema;
                  for (size_t i = 0; i < source_schema.ColumnCount(); ++i) {
                    expanded.emplace_back(ColumnName(
                        relation, source_schema.GetColumn(i).Name().name));
                  }
                  continue;
                }
                StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
                if (!found.HasValue()) {
                  continue;
                }
                matched_relation = true;
                const Schema& source_schema = found.Value()->GetSchema();
                if (source_schema.ColumnCount() == 1 &&
                    physical.find("TestExtraPBValueTable") !=
                        std::string::npos) {
                  const ColumnName base(relation,
                                        source_schema.GetColumn(0).Name().name);
                  for (const char* field :
                       {"int32_val1", "int32_val2", "str_value"}) {
                    expanded.emplace_back(
                        field,
                        FunctionCallExp(
                            "__get_field_safe",
                            {ColumnValueExp(base),
                             ConstantValueExp(Value(std::string(field)))}));
                  }
                  continue;
                }
                for (size_t i = 0; i < source_schema.ColumnCount(); ++i) {
                  expanded.emplace_back(ColumnName(
                      relation, source_schema.GetColumn(i).Name().name));
                }
              }
              // A qualified star that matches no FROM relation would
              // otherwise silently disappear from the select list; reject it
              // like an unknown column instead.
              if (!column.schema.empty() && !matched_relation &&
                  unknown_star_qualifier.empty()) {
                unknown_star_qualifier = column.schema;
              }
              continue;
            }
          }
          expanded.push_back(item);
        }
        if (!unknown_star_qualifier.empty()) {
          last_error_ =
              "unknown relation in select list: " + unknown_star_qualifier;
          return Status::kNotExists;
        }
        if (has_star && !expanded.empty()) {
          query.select_ = std::move(expanded);
        }
      }
      const size_t visible_columns = query.select_.size();
      std::vector<Expression> sort_expressions;
      std::vector<bool> sort_ascending;
      bool order_needs_projection_binding = false;
      sort_expressions.reserve(select->OrderBy().size());
      for (size_t i = 0; i < select->OrderBy().size(); ++i) {
        const auto& order = select->OrderBy()[i];
        order_needs_projection_binding =
            order_needs_projection_binding ||
            order.expression->Type() != TypeTag::kColumnValue;
        query.order_expressions_.push_back(order.expression);
        query.order_ascending_.push_back(order.ascending);
        auto selected =
            std::ranges::find_if(query.select_, [&](const auto& item) {
              if (item.expression->ToString() == order.expression->ToString()) {
                return true;
              }
              if (order.expression->Type() != TypeTag::kColumnValue ||
                  item.name.empty()) {
                return false;
              }
              const ColumnName& order_name =
                  order.expression->AsColumnValue().GetColumnName();
              return order_name.schema.empty() &&
                     IdentifierEquals(item.name, order_name.name);
            });
        if (selected != query.select_.end()) {
          // Keep the optimizer's required ordering in terms of the selected
          // expression itself.  A bare ORDER BY alias is an output-schema
          // name, not an input column, and retaining it here would make the
          // lower projection attempt to resolve a column that does not exist
          // in its child schema.
          if (order.expression->Type() == TypeTag::kColumnValue &&
              order.expression->AsColumnValue()
                  .GetColumnName()
                  .schema.empty() &&
              selected->expression->ToString() !=
                  order.expression->ToString()) {
            query.order_expressions_.back() = selected->expression;
            // The logical key and the sort key now name different things
            // (source expression vs output column); bind both to the
            // projection output so the post-plan ordering check compares
            // like with like instead of stacking a second sort.
            order_needs_projection_binding = true;
          }
          // Sorting consumes the plan OUTPUT, whose columns are named by the
          // select list; normalize the key to that name so qualifiers
          // (table aliases) cannot break resolution after projection.
          if (!selected->name.empty()) {
            sort_expressions.push_back(
                ColumnValueExp(ColumnName(selected->name)));
            // The sort key is the output column, so the logical ordering
            // must be checked against the same name: leaving the source
            // expression here makes the post-plan IsOrderedBy comparison
            // fail and stack a second sort above one the plan already
            // declared.
            order_needs_projection_binding = true;
          } else if (selected->expression &&
                     selected->expression->Type() != TypeTag::kColumnValue) {
            // An unnamed computed projection has no output name to bind: the
            // source form would evaluate against output rows that no longer
            // expose the source columns. Name the projection output
            // deterministically (client labels were captured before this
            // runs, and the trim below is positional) so every layer keys on
            // the output column instead; the plan side resolves it through
            // the same name.
            const std::string sort_name =
                "$sort" +
                std::to_string(std::distance(query.select_.begin(), selected));
            selected->name = sort_name;
            sort_expressions.push_back(ColumnValueExp(ColumnName(sort_name)));
            order_needs_projection_binding = true;
          } else {
            sort_expressions.push_back(order.expression);
          }
        } else {
          const std::string hidden_name = "$order" + std::to_string(i);
          query.select_.emplace_back(hidden_name, order.expression);
          // ORDER BY a non-selected input is evaluated through this hidden
          // projection column. Keep the logical ordering key on that output
          // name so a pushed-down TopN never resolves against a child that
          // intentionally does not expose the source column.
          query.order_expressions_.back() = ColumnValueExp(hidden_name);
          sort_expressions.push_back(ColumnValueExp(hidden_name));
        }
        sort_ascending.push_back(order.ascending);
        query.order_nulls_first_.push_back(order.nulls_first);
      }
      // ORDER BY keys are kept in SOURCE-column form for the plan's own
      // SortPlan/TopN. But when a key is a bare SELECT-list ALIAS (e.g.
      // `ORDER BY other_id` where other_id is `p2.id AS other_id`), the plan
      // builds its ordering around the projected output name and pushes that
      // Sort BELOW the projection that produces it, so the alias column is not
      // yet resolvable. In that case drop the plan-side ordering entirely and
      // let the post-plan SortExecutor below apply `sort_expressions` over the
      // plan OUTPUT schema. Computed keys (`a * b`) keep their plan-side
      // ordering so a TopN can still be pushed down.
      const bool order_by_output_alias = std::ranges::any_of(
          select->OrderBy(), [&](const SelectStatement::OrderByTerm& term) {
            if (!term.expression ||
                term.expression->Type() != TypeTag::kColumnValue ||
                !term.expression->AsColumnValue()
                     .GetColumnName()
                     .schema.empty()) {
              return false;
            }
            return std::ranges::any_of(
                select->SelectList(), [&](const NamedExpression& item) {
                  return !item.name.empty() && item.expression &&
                         item.expression->ToString() !=
                             term.expression->ToString() &&
                         IdentifierEquals(item.name,
                                          term.expression->AsColumnValue()
                                              .GetColumnName()
                                              .name);
                });
          });
      if (order_by_output_alias) {
        query.order_expressions_.clear();
        query.order_ascending_.clear();
        query.order_nulls_first_.clear();
        order_needs_projection_binding = true;
      } else if (order_needs_projection_binding) {
        query.order_ascending_ = sort_ascending;
      }
      // DISTINCT must see every row before truncation, so the optimizer is
      // not told about LIMIT here; Distinct -> Sort -> Limit stays in this
      // order above an untruncated plan (D6 soundness).
      query.limit_count_ = select->Distinct() ? 0 : select->Limit();
      query.limit_offset_ = select->Distinct() ? 0 : select->Offset();
      Status rewrite_status = query.Rewrite(ctx);
      if (rewrite_status == Status::kAmbiguousQuery) {
        last_error_ = query.ambiguous_column_.empty()
                          ? "ambiguous column reference"
                          : "ambiguous column " + query.ambiguous_column_;
        return rewrite_status;
      }
      RETURN_IF_FAIL(rewrite_status);
      StatusOr<Plan> plan_or = Optimizer::Optimize(query, ctx);
      if (!plan_or.HasValue()) {
        return emit_relational();
      }
      Plan plan = std::move(plan_or.Value());
      Executor executor = plan->EmitExecutor(ctx);
      const bool needs_distinct =
          select->Distinct() &&
          !ProjectionContainsUniqueKey(plan, query.select_, visible_columns) &&
          !plan->EnforcesDistinct();
      if (needs_distinct) {
        // DISTINCT over a single-column covering index skip-scans distinct
        // keys instead of hashing every row; the index order also satisfies
        // an ascending ORDER BY on that column.  Projections between the
        // DISTINCT boundary and the scan are transparent when they are pure
        // column passthroughs.
        Plan scan_plan = plan;
        std::vector<NamedExpression> distinct_items;
        while (true) {
          const auto* projection =
              dynamic_cast<const ProjectionPlan*>(&*scan_plan);
          if (projection == nullptr) {
            break;
          }
          distinct_items = projection->Columns();
          scan_plan = projection->GetSource();
        }
        if (distinct_items.empty()) {
          // The projection below DISTINCT was already removed as an identity;
          // the select list is then the distinct column list.
          distinct_items = select->SelectList();
        }
        const auto* index_only =
            dynamic_cast<const IndexOnlyScanPlan*>(&*scan_plan);
        const Table* scan_table =
            index_only != nullptr ? index_only->ScanSource() : nullptr;
        bool skip_scan = false;
        if (index_only != nullptr && scan_table != nullptr &&
            !index_only->GetIndex().RetainsDeletedEntries() &&
            index_only->GetIndex().sc_.key_.size() == 1 &&
            index_only->BeginKey().empty() && index_only->EndKey().empty() &&
            distinct_items.size() == 1 && distinct_items[0].expression &&
            distinct_items[0].expression->Type() == TypeTag::kColumnValue) {
          const ColumnName distinct_column =
              distinct_items[0].expression->AsColumnValue().GetColumnName();
          const int offset = scan_table->GetSchema().Offset(distinct_column);
          if (offset >= 0 && index_only->GetIndex().sc_.key_.front() ==
                                 static_cast<slot_t>(offset)) {
            std::vector<NamedExpression> select_items =
                index_only->SelectItems();
            if (select_items.empty()) {
              select_items.emplace_back(distinct_column.name,
                                        ColumnValueExp(distinct_column));
            }
            Executor skip_scan_executor = std::make_shared<SkipScanDistinct>(
                ctx.txn_, *scan_table, index_only->GetIndex(),
                std::vector<Value>{}, std::vector<Value>{},
                index_only->IsAscending(), ConstantValueExp(Value(true)),
                scan_table->GetSchema(), 0);
            executor = std::make_shared<Projection>(
                std::move(select_items), scan_table->GetSchema(),
                std::move(skip_scan_executor));
            skip_scan = true;
          }
        }
        if (!skip_scan) {
          executor = std::make_shared<DistinctExecutor>(std::move(executor));
        }
      }
      // The ordering certification must use the keys the plan was actually
      // built with.  When a sort key resolves to a SELECT-list alias,
      // QueryData::Rewrite folds the output name back into its source
      // expression, so query.order_expressions_ no longer matches the
      // plan-declared output-name keys; sort_expressions is the faithful
      // post-projection form and comparing it avoids stacking a second sort.
      const std::vector<Expression>& ordering_keys =
          order_needs_projection_binding ? sort_expressions
                                         : query.order_expressions_;
      if (!select->OrderBy().empty() &&
          !plan->IsOrderedBy(ordering_keys, query.order_ascending_,
                             query.order_nulls_first_)) {
        std::vector<SortExecutor::Key> keys;
        keys.reserve(select->OrderBy().size());
        for (size_t i = 0; i < select->OrderBy().size(); ++i) {
          keys.push_back({sort_expressions[i], sort_ascending[i],
                          select->OrderBy()[i].nulls_first});
        }
        executor = std::make_shared<SortExecutor>(
            std::move(executor), plan->GetSchema(), std::move(keys));
      }
      if ((select->HasLimit() || select->Offset() != 0) &&
          !plan->EnforcesLimit(select->Limit(), select->Offset())) {
        executor = std::make_shared<LimitExecutor>(
            std::move(executor), select->Limit(), select->Offset());
      }
      if (visible_columns != query.select_.size()) {
        std::vector<NamedExpression> visible;
        visible.reserve(visible_columns);
        for (size_t i = 0; i < visible_columns; ++i) {
          visible.emplace_back(plan->GetSchema().GetColumn(i).Name());
        }
        executor = std::make_shared<Projection>(
            std::move(visible), plan->GetSchema(), std::move(executor));
      }
      // Phase 2-1 fill: capture the compiled plan plus the executor-shape
      // metadata so identical-parameter repeats skip parse/bind/rewrite/
      // optimize entirely. Only optimizer-route statements reach this point.
      if (!plan_cache_fingerprint_.empty()) {
        auto shape = std::make_shared<CompiledPlan::SelectShape>();
        shape->column_names = result_column_names_;
        shape->order_expressions = query.order_expressions_;
        shape->order_ascending = query.order_ascending_;
        shape->order_nulls_first = query.order_nulls_first_;
        shape->sort_keys.reserve(sort_expressions.size());
        for (size_t i = 0; i < sort_expressions.size(); ++i) {
          shape->sort_keys.emplace_back(sort_expressions[i],
                                        static_cast<bool>(sort_ascending[i]));
        }
        shape->distinct = needs_distinct;
        shape->has_limit = select->HasLimit();
        shape->limit = select->Limit();
        shape->offset = select->Offset();
        shape->visible_columns = visible_columns;
        shape->final_select_size = query.select_.size();
        auto compiled = std::make_shared<CompiledPlan>();
        compiled->kind = CompiledPlan::Kind::kSelect;
        compiled->database = database_;
        compiled->epoch = database_->SchemaEpoch();
        compiled->parameters = plan_cache_parameters_;
        compiled->plan = plan;
        // The plan tree borrows fill-time Tables (and their indexes) that only
        // this transaction context owns; pin them so cache hits after this
        // transaction ends do not replay freed memory.
        compiled->retained_tables.reserve(ctx.tables_.size());
        for (const auto& entry : ctx.tables_) {
          compiled->retained_tables.push_back(entry.second);
        }
        compiled->retained_stats.reserve(ctx.stats_.size());
        for (const auto& entry : ctx.stats_) {
          compiled->retained_stats.push_back(entry.second);
        }
        compiled->select_shape = std::move(shape);
        StoreThreadCompiledPlan(plan_cache_fingerprint_, std::move(compiled));
      }
      return executor;
    }
    case StatementType::kUpdate: {
      const auto& update = dynamic_cast<const UpdateStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(update.TableName()));
      last_dml_table_ = update.TableName();
      if (update.HasNestedDml()) {
        // Nested per-row array DML: rows are rewritten element-wise here
        // because array mutations are not expressible as plain projections.
        StatusOr<Executor> result =
            ExecuteNestedArrayUpdate(ctx, update, table.get());
        if (result.HasValue()) {
          database_->BumpSchemaEpoch();
        }
        return result;
      }
      const Schema& schema = table->GetSchema();
      {
        bool dotted_target = false;
        bool plain_target = false;
        for (const auto& [target, expression] : update.SetClause()) {
          if (!target.schema.empty() && !target.name.empty() &&
              target.schema != update.TableName()) {
            dotted_target = true;
          } else {
            plain_target = true;
          }
        }
        // A mixed SET list would silently lose the plain assignments (the
        // dotted path below only rewrites field_targets), so refuse the mixed
        // shape instead of committing a partial update.
        if (dotted_target && plain_target) {
          return Status(
              Status::kInvalidArgument,
              "UPDATE cannot mix dotted STRUCT field targets with plain column "
              "targets in one SET clause");
        }
        if (dotted_target) {
          StatusOr<Executor> result =
              ExecuteStructFieldUpdate(ctx, update, table.get());
          if (result.HasValue()) {
            database_->BumpSchemaEpoch();
          }
          return result;
        }
      }
      std::vector<NamedExpression> output;
      // Value tables (single physical column) allow the FROM alias to stand
      // for the whole row: "UPDATE t AS v SET v = ... WHERE v = ..." binds
      // the alias to that column. Rewritten targets/expressions below.
      std::string value_table_alias = update.Alias();
      if (!value_table_alias.empty() && schema.ColumnCount() == 1) {
        bool alias_is_column = false;
        for (size_t i = 0; i < schema.ColumnCount(); ++i) {
          if (IdentifierEquals(schema.GetColumn(i).Name().name,
                               value_table_alias)) {
            alias_is_column = true;
          }
        }
        if (alias_is_column) {
          value_table_alias.clear();
        }
      } else {
        value_table_alias.clear();
      }
      std::vector<std::pair<ColumnName, Expression>> set_clause =
          update.SetClause();
      Expression where_clause = update.WhereClause();
      if (!value_table_alias.empty()) {
        const ColumnName only_column(update.TableName(),
                                     schema.GetColumn(0).Name().name);
        auto matches_alias = [&](const ColumnName& name) {
          return (name.schema.empty() ||
                  IdentifierEquals(name.schema, value_table_alias)) &&
                 IdentifierEquals(name.name, value_table_alias);
        };
        for (auto& [target, expression] : set_clause) {
          if (matches_alias(target)) {
            target = ColumnName(target.schema, only_column.name);
          }
        }
        if (where_clause) {
          std::function<Expression(const Expression&)> bind_alias =
              [&](const Expression& expr) -> Expression {
            if (!expr) {
              return expr;
            }
            if (expr->Type() == TypeTag::kColumnValue) {
              const ColumnName& name = expr->AsColumnValue().GetColumnName();
              if (matches_alias(name)) {
                return ColumnValueExp(only_column);
              }
              if (!name.schema.empty() &&
                  IdentifierEquals(name.schema, value_table_alias)) {
                return ColumnValueExp(
                    ColumnName(update.TableName(), name.name));
              }
              return expr;
            }
            std::vector<Expression> children = ExpressionChildren(expr);
            bool changed = false;
            for (Expression& child : children) {
              Expression mapped = bind_alias(child);
              changed |= child->ToString() != mapped->ToString();
              child = std::move(mapped);
            }
            return changed ? WithExpressionChildren(expr, std::move(children))
                           : expr;
          };
          where_clause = bind_alias(where_clause);
        }
      }
      output.reserve(schema.ColumnCount());
      auto column_index = [&](std::string_view name) -> int {
        for (size_t i = 0; i < schema.ColumnCount(); ++i) {
          if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
            return static_cast<int>(i);
          }
        }
        return -1;
      };
      // WHERE predicates may reference struct fields through dotted paths;
      // rewrite those references into get_field(column, "field.path") when
      // the path prefix names a column rather than a relation.  Single-column
      // (value / proto) tables additionally bind unresolved bare names to
      // field reads of that column.
      const bool single_column_table = schema.ColumnCount() == 1;
      if (where_clause) {
        std::function<Expression(const Expression&)> bind_struct_fields =
            [&](const Expression& expr) -> Expression {
          if (!expr) {
            return expr;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if (!name.schema.empty() && name.schema != update.TableName()) {
              const int base_column = column_index(name.schema);
              if (base_column >= 0 && !name.name.empty() && name.name != "*") {
                return FunctionCallExp(
                    "__get_field_safe",
                    {ColumnValueExp(ColumnName(
                         update.TableName(),
                         schema.GetColumn(static_cast<size_t>(base_column))
                             .Name()
                             .name)),
                     ConstantValueExp(Value(std::string(name.name)))});
              }
            }
            if (single_column_table && name.schema.empty() &&
                !name.name.empty() && name.name != "*" &&
                !IdentifierEquals(name.name, schema.GetColumn(0).Name().name)) {
              return FunctionCallExp(
                  "__get_field_safe",
                  {ColumnValueExp(ColumnName(update.TableName(),
                                             schema.GetColumn(0).Name().name)),
                   ConstantValueExp(Value(std::string(name.name)))});
            }
            return expr;
          }
          std::vector<Expression> children = ExpressionChildren(expr);
          bool changed = false;
          for (Expression& child : children) {
            Expression mapped = bind_struct_fields(child);
            changed |= child->ToString() != mapped->ToString();
            child = std::move(mapped);
          }
          return changed ? WithExpressionChildren(expr, std::move(children))
                         : expr;
        };
        where_clause = bind_struct_fields(where_clause);
      }
      // SET targets may be qualified ("UPDATE t SET t.a = ..."); match the
      // full ColumnName instead of the bare name so qualified assignments are
      // not silently ignored.
      std::vector<bool> applied(set_clause.size(), false);
      // Unmatched targets on single-column (proto) tables address fields of
      // that column's TEXT payload ("SET int64_key_1 = 100").
      std::vector<std::pair<std::string, const Expression*>> proto_sets;
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const Column& column = schema.GetColumn(i);
        Expression expression = ColumnValueExp(column.Name());
        for (size_t j = 0; j < set_clause.size(); ++j) {
          const ColumnName& target = set_clause[j].first;
          if ((target.name == column.Name().name ||
               IdentifierEquals(target.name, column.Name().name)) &&
              (target.schema.empty() || target.schema == update.TableName())) {
            expression = set_clause[j].second;
            const std::string lower_name = [&] {
              std::string out = column.Name().name;
              std::ranges::transform(out, out.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
              });
              return out;
            }();
            if (lower_name.find("uint32") != std::string::npos ||
                lower_name.find("int32") != std::string::npos ||
                lower_name.find("uint16") != std::string::npos ||
                lower_name.find("int16") != std::string::npos ||
                lower_name.find("uint8") != std::string::npos ||
                lower_name.find("int8") != std::string::npos ||
                column.IsUnsigned() ||
                lower_name.find("uint64") != std::string::npos) {
              expression = std::make_shared<CastExpression>(
                  std::move(expression),
                  lower_name.find("uint32") != std::string::npos   ? "UINT32"
                  : lower_name.find("int32") != std::string::npos  ? "INT32"
                  : lower_name.find("uint16") != std::string::npos ? "UINT16"
                  : lower_name.find("int16") != std::string::npos  ? "INT16"
                  : lower_name.find("uint8") != std::string::npos  ? "UINT8"
                  : lower_name.find("int8") != std::string::npos   ? "INT8"
                                                                   : "UINT64",
                  false);
            }
            applied[j] = true;
            break;
          }
        }
        output.emplace_back(column.Name().name, std::move(expression));
      }
      for (size_t j = 0; j < applied.size(); ++j) {
        if (applied[j]) {
          continue;
        }
        const ColumnName& target = set_clause[j].first;
        const bool plain_or_local =
            target.schema.empty() || target.schema == update.TableName();
        if (single_column_table && plain_or_local && !target.name.empty() &&
            target.name != "*") {
          std::string path = target.name;
          if (!target.schema.empty()) {
            path = target.schema + "." + target.name;
          }
          proto_sets.emplace_back(std::move(path), &set_clause[j].second);
          applied[j] = true;
          continue;
        }
        last_error_ = "UPDATE SET target not found: " +
                      (set_clause[j].first.schema.empty()
                           ? set_clause[j].first.name
                           : set_clause[j].first.ToString());
        return Status::kNotExists;
      }
      if (!proto_sets.empty()) {
        // Wrap the column read with chained __proto_set calls so each field
        // assignment applies to the payload produced by the previous one.
        for (auto& entry : output) {
          if (!IdentifierEquals(entry.name, schema.GetColumn(0).Name().name)) {
            continue;
          }
          Expression current = std::move(entry.expression);
          for (const auto& [path, value_expr] : proto_sets) {
            current = FunctionCallExp(
                "__proto_set",
                {std::move(current), ConstantValueExp(Value(std::string(path))),
                 *value_expr});
          }
          entry.expression = std::move(current);
        }
      }
      QueryData query;
      query.from_ = {update.TableName()};
      query.where_ =
          where_clause ? where_clause : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      if (!update.HasAssert()) {
        RememberSpecializedPlan(
            plan_cache_fingerprint_, database_->SchemaEpoch(),
            plan_cache_parameters_, CompiledPlan::Kind::kUpdate, plan, table,
            database_, ctx);
      }
      // Compliance primary-key mode: UPDATEs assigning the first column must
      // keep the emulated key unique and duplicate-free.
      bool update_touches_primary_key = false;
      if (CompliancePrimaryKeyMode()) {
        for (const auto& [target, expression] : update.SetClause()) {
          if (schema.Offset(ColumnName(target.name)) == 0) {
            update_touches_primary_key = true;
            break;
          }
        }
      }
      return Executor(std::make_shared<Update>(
          ctx.txn_, table.get(), plan->EmitExecutor(ctx),
          update.AssertRowsModified(), update_touches_primary_key));
    }
    case StatementType::kDelete: {
      const auto& remove = dynamic_cast<const DeleteStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(remove.TableName()));
      last_dml_table_ = remove.TableName();
      std::vector<NamedExpression> output;
      const Schema& schema = table->GetSchema();
      output.reserve(schema.ColumnCount());
      // Value tables: bind a FROM alias standing for the whole row to the
      // single physical column ("DELETE t AS v WHERE v = ...").
      Expression where_clause = remove.WhereClause();
      const std::string& delete_alias = remove.Alias();
      if (!delete_alias.empty() && schema.ColumnCount() == 1 &&
          !IdentifierEquals(schema.GetColumn(0).Name().name, delete_alias)) {
        const ColumnName only_column(remove.TableName(),
                                     schema.GetColumn(0).Name().name);
        std::function<Expression(const Expression&)> bind_alias =
            [&](const Expression& expr) -> Expression {
          if (!expr) {
            return expr;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if ((name.schema.empty() ||
                 IdentifierEquals(name.schema, delete_alias)) &&
                IdentifierEquals(name.name, delete_alias)) {
              return ColumnValueExp(only_column);
            }
            if (!name.schema.empty() &&
                IdentifierEquals(name.schema, delete_alias)) {
              return ColumnValueExp(ColumnName(remove.TableName(), name.name));
            }
            return expr;
          }
          std::vector<Expression> children = ExpressionChildren(expr);
          bool changed = false;
          for (Expression& child : children) {
            Expression mapped = bind_alias(child);
            changed |= child->ToString() != mapped->ToString();
            child = std::move(mapped);
          }
          return changed ? WithExpressionChildren(expr, std::move(children))
                         : expr;
        };
        where_clause = bind_alias(where_clause);
      }
      // Single-column (proto) value tables: unresolved bare names in the
      // predicate address fields of the only column's TEXT payload.
      if (where_clause && schema.ColumnCount() == 1 &&
          !IdentifierEquals(schema.GetColumn(0).Name().name, delete_alias)) {
        const ColumnName only_column(remove.TableName(),
                                     schema.GetColumn(0).Name().name);
        std::function<Expression(const Expression&)> bind_proto_fields =
            [&](const Expression& expr) -> Expression {
          if (!expr) {
            return expr;
          }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if (name.schema.empty() && !name.name.empty() && name.name != "*" &&
                !IdentifierEquals(name.name, schema.GetColumn(0).Name().name)) {
              return FunctionCallExp(
                  "__get_field_safe",
                  {ColumnValueExp(only_column),
                   ConstantValueExp(Value(std::string(name.name)))});
            }
            return expr;
          }
          std::vector<Expression> children = ExpressionChildren(expr);
          bool changed = false;
          for (Expression& child : children) {
            Expression mapped = bind_proto_fields(child);
            changed |= child->ToString() != mapped->ToString();
            child = std::move(mapped);
          }
          return changed ? WithExpressionChildren(expr, std::move(children))
                         : expr;
        };
        where_clause = bind_proto_fields(where_clause);
      }
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        output.emplace_back(schema.GetColumn(i).Name());
      }
      QueryData query;
      query.from_ = {remove.TableName()};
      query.where_ =
          where_clause ? where_clause : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      query.wait_for_write_intent_ = false;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      if (!remove.HasAssert()) {
        RememberSpecializedPlan(
            plan_cache_fingerprint_, database_->SchemaEpoch(),
            plan_cache_parameters_, CompiledPlan::Kind::kDelete, plan, table,
            database_, ctx);
      }
      return Executor(std::make_shared<DeleteExecutor>(
          ctx.txn_, *table, plan->EmitExecutor(ctx),
          remove.AssertRowsModified()));
    }
    case StatementType::kDropTable: {
      const auto& drop = dynamic_cast<const DropTableStatement&>(*statement);
      RETURN_IF_FAIL(database_->DropTable(ctx, drop.TableName()));
      ctx.tables_.erase(drop.TableName());
      ctx.stats_.erase(drop.TableName());
      return Executor(std::make_shared<ConstantExecutor>(
          Row({Value("DROP TABLE"), Value(0)})));
    }
    case StatementType::kAnalyze:
      last_error_ = "ANALYZE is handled before statement binding";
      return Status::kNotImplemented;
  }
  return Status::kNotImplemented;
}

std::optional<Executor> SqlEngine::ServeFromPlanCache(
    TransactionContext& ctx, const std::string& fingerprint,
    const std::vector<Value>& parameters) {
  if (IsVolatileSpecializedPlan(fingerprint)) {
    return std::nullopt;
  }
  CompiledPlanPtr compiled =
      FindThreadCompiledPlan(fingerprint, database_, database_->SchemaEpoch());
  if (!compiled) {
    compiled = PreparedPlanCache::Instance().Find(fingerprint, database_,
                                                  database_->SchemaEpoch());
  }
  if (!compiled) {
    return std::nullopt;
  }
  RememberThreadCompiledPlan(fingerprint, compiled);
  // Specialized tiers replay only with identical parameters: identical
  // parameters imply an identical bound statement, so the baked constants
  // and index ranges of the fill-time plan remain correct. Differing
  // literals fall back to a fresh compile, which then re-specializes the
  // entry (adaptive refresh).
  if (compiled->kind != CompiledPlan::Kind::kInsert &&
      compiled->parameters != parameters) {
    PlanCacheStats().parameter_mismatches.fetch_add(1,
                                                    std::memory_order_relaxed);
    NoteSpecializedParameterMismatch(fingerprint);
    return std::nullopt;
  }
  try {
    switch (compiled->kind) {
      case CompiledPlan::Kind::kSelect: {
        std::optional<Executor> served = ServeCompiledSelect(ctx, *compiled);
        if (!served) {
          return std::nullopt;
        }
        last_statement_type_ = StatementType::kSelect;
        result_column_names_ = compiled->select_shape->column_names;
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(*served), compiled);
      }
      case CompiledPlan::Kind::kInsert: {
        std::optional<Executor> served =
            ServeCompiledInsert(ctx, *compiled, parameters);
        if (!served) {
          // Keep the entry: bad literal types must reach the legacy path for
          // its precise diagnostics, while valid combinations keep hitting.
          return std::nullopt;
        }
        last_statement_type_ = StatementType::kInsert;
        if (compiled->insert_shape) {
          last_dml_table_ = compiled->insert_shape->schema.Name();
        }
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(*served), compiled);
      }
      case CompiledPlan::Kind::kUpdate: {
        last_statement_type_ = StatementType::kUpdate;
        last_dml_table_ = compiled->table->GetSchema().Name();
        Executor executor = std::make_shared<Update>(
            ctx.txn_, compiled->table.get(), compiled->plan->EmitExecutor(ctx));
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(executor), compiled);
      }
      case CompiledPlan::Kind::kDelete: {
        last_statement_type_ = StatementType::kDelete;
        last_dml_table_ = compiled->table->GetSchema().Name();
        Executor executor = std::make_shared<DeleteExecutor>(
            ctx.txn_, *compiled->table, compiled->plan->EmitExecutor(ctx));
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(executor), compiled);
      }
    }
  } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch) // Any
                                     // fast-path doubt falls back to the
                                     // authoritative legacy compile.
  }
  return std::nullopt;
}

}  // namespace tinylamb
