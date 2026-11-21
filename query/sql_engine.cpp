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
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
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
#include "common/memory_budget.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/columnar_tlcs_scan_executor.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/delete.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/pipeline_lowering.hpp"
#include "executor/projection.hpp"
#include "executor/relational.hpp"
#include "executor/set_operation.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "executor/update.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/sql_udf.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades.hpp"
#include "plan/empty_plan.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/group_by_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/index_only_scan_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/values_plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/grouped_pipeline_lowering.hpp"
#include "query/join_reduction.hpp"
#include "query/plan_cache.hpp"
#include "query/query_data.hpp"
#include "query/select_rewrites.hpp"
#include "query/sql_engine_internal.hpp"
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
  DataChunk chunk;
  while (true) {
    // T4494: drain buffered Next() rows first so mixing Next with batch
    // consumption cannot skip rows.
    const size_t count = executor_->NextBatchDraining(&chunk);
    if (count == 0) {
      break;
    }
    for (size_t i = 0; i < count; ++i) {
      sink(chunk.RowAt(i));
      ++rows;
    }
  }
  return rows;
}

size_t QueryResult::Drain() {
  size_t rows = 0;
  DataChunk chunk;
  while (true) {
    const size_t count = executor_->NextBatchDraining(&chunk);
    if (count == 0) {
      break;
    }
    rows += count;
  }
  return rows;
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

void SqlEngine::EnsureViewsRegistered(TransactionContext& ctx) {
  const uint64_t epoch = database_->SchemaEpoch();
  // ViewRegistry() is thread_local, so "already registered at this epoch"
  // must be tracked per thread too: a DROP VIEW on another connection
  // thread only bumps the epoch, and this thread has to notice it and drop
  // its own stale entry. Registration state deliberately does NOT key on
  // the SqlEngine instance -- see the T485 note below.
  struct RegisteredState {
    const Database* database{nullptr};
    uint64_t epoch{0};
  };
  static thread_local RegisteredState registered;
  // T485: registration identity is (database, epoch). The old key also
  // required `engine == this` plus a per-instance epoch, so callers that
  // construct a fresh SqlEngine wrapper per statement missed the fast path
  // and re-discovered (catalog scan + parse) every view, every statement.
  if (registered.database == database_ && registered.epoch == epoch) {
    return;
  }
  StatusOr<std::vector<std::string>> names =
      database_->ListViewDefinitions(ctx);
  if (!names.HasValue()) {
    return;
  }
  {
    // Forget views whose durable definition is gone (DROP VIEW elsewhere);
    // FROM resolution consults the registry before the catalog.
    std::unordered_set<std::string> live;
    live.reserve(names.Value().size());
    for (const std::string& name : names.Value()) {
      std::string lowered = name;
      for (char& c : lowered) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
      }
      live.insert(std::move(lowered));
    }
    auto& registry = ViewRegistry();
    for (auto it = registry.begin(); it != registry.end();) {
      it = live.contains(it->first) ? std::next(it) : registry.erase(it);
    }
  }
  bool complete = true;
  for (const std::string& name : names.Value()) {
    StatusOr<std::string> text = database_->GetViewDefinition(ctx, name);
    if (!text.HasValue()) {
      complete = false;
      continue;
    }
    const GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(text.Value());
    if (!parsed.ok) {
      complete = false;
      continue;
    }
    StatusOr<std::unique_ptr<GoogleSqlAstNode>> ast =
        GoogleSqlAstParser::Parse(parsed.ast);
    if (!ast.HasValue() || ast.Value() == nullptr) {
      complete = false;
      continue;
    }
    const GoogleSqlAstNode* query = ast.Value()->Child("Query");
    if (query == nullptr) {
      complete = false;
      continue;
    }
    RegisterViewDefinition(name, *query);
  }
  // A transient catalog/parser failure must be retried even if no DDL changes
  // the epoch. Successful individual definitions may already be registered.
  // T485: a txn with buffered catalog writes sees a TRANSACTION-LOCAL view
  // that a later Abort can undo; stamping from it would freeze that
  // temporary state (shared per-thread stamp, T485) -- re-discover next
  // time instead.
  if (complete && !ctx.txn_.IsCatalogDirty()) {
    registered = RegisteredState{.database = database_, .epoch = epoch};
  }
}

StatusOr<QueryResult> SqlEngine::Execute(TransactionContext& ctx,
                                         std::string_view sql) {
  static const bool kTraceFuzzSql =
      std::getenv("TINYLAMB_FUZZ_TRACE_SQL") != nullptr;
  if (kTraceFuzzSql) {
    std::cerr << "[fuzz-sql] " << sql << "\n" << std::flush;
  }
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
                       result_column_names_, result_column_types_);
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

class StatementTimeoutExecutor final : public ExecutorBase {
 public:
  StatementTimeoutExecutor(Executor inner, Transaction* txn)
      : inner_(std::move(inner)), txn_(txn) {}

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override {
    if (txn_ != nullptr && txn_->HasStatementTimeout()) {
      const Status ts = txn_->CheckStatementTimeout();
      if (ts != Status::kSuccess) {
        FailWith(ts);
        return 0;
      }
    }
    Transaction::ScopedThreadStatementDeadline deadline_guard(
        txn_ != nullptr ? txn_->StatementDeadlineNs() : 0);
    const size_t n = inner_->NextBatch(destination, max_rows);
    if (FailWithChildOf(*inner_)) {
      return 0;
    }
    if (txn_ != nullptr && txn_->HasStatementTimeout()) {
      const Status ts = txn_->CheckStatementTimeout();
      if (ts != Status::kSuccess) {
        FailWith(ts);
        return 0;
      }
    }
    return n;
  }

  void Dump(std::ostream& o, int indent) const override {
    inner_->Dump(o, indent);
  }
  void Explain(std::ostream& o, int indent) const override {
    inner_->Explain(o, indent);
  }
  bool AttachRuntimeFilter(const std::shared_ptr<const RuntimeFilterGroup>& g,
                           slot_t col, ValueType type) override {
    return inner_->AttachRuntimeFilter(g, col, type);
  }
  [[nodiscard]] Status GetStatus() const override {
    if (ExecutorBase::GetStatus() != Status::kSuccess) {
      return ExecutorBase::GetStatus();
    }
    return inner_->GetStatus();
  }

 private:
  Executor inner_;
  Transaction* txn_{nullptr};
};

StatusOr<Executor> FinishPrepareWithTimeout(Transaction& txn,
                                            StatusOr<Executor> exec,
                                            std::string* last_error) {
  if (!exec.HasValue()) {
    return exec;
  }
  if (txn.HasStatementTimeout()) {
    const Status ts = txn.CheckStatementTimeout();
    if (ts != Status::kSuccess) {
      if (last_error != nullptr) {
        *last_error = ts.GetMessage();
      }
      return ts;
    }
    return Executor(
        std::make_shared<StatementTimeoutExecutor>(exec.MoveValue(), &txn));
  }
  return exec;
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
  outer_names.reserve(statement.Sources().size() * 2);
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
    // Non-equality quantifiers are not membership tests; the keyed
    // semi/anti rewrite cannot express them (see TryDecorrelate), so the
    // statement stays on the scope-aware relational evaluator.
    if (query.Mode() != QuantifierMode::kIn &&
        (query.Mode() != QuantifierMode::kAny ||
         query.Op() != BinaryOperation::kEquals)) {
      return false;
    }
    if (query.Query() == nullptr ||
        (!query.Exists() && query.Test() == nullptr) ||
        query.Query()->Sources().size() != 1 ||
        query.Query()->Sources()[0].query ||
        query.Query()->Sources()[0].unnest ||
        query.Query()->Sources()[0].sample.has_value() ||
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
      // Mirror TryDecorrelate: a NOT IN whose subquery carries its own
      // correlation equality has tuple-valued UNKNOWN semantics the
      // null-aware anti join does not model, so the optimizer keeps that
      // conjunct as a plain filter -- which has no subquery evaluation
      // context and fails at run time ("query expression requires
      // relational evaluation").  Keep the statement on the scope-aware
      // relational evaluator instead.
      if (!query.Exists() && query.Negated()) {
        return false;
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
      // ...and the key must cross the scopes: exactly one operand outer.
      // An outer-only equality (`x.a = x.b` inside the subquery) is rejected
      // by TryDecorrelate and would otherwise be stranded on a context-free
      // physical filter.
      const auto is_outer = [&](const Expression& operand) {
        const ColumnName& column = operand->AsColumnValue().GetColumnName();
        return !column.schema.empty() && outer_names.contains(column.schema) &&
               !inner_names.contains(column.schema);
      };
      if (is_outer(inner_conjunct->AsBinaryExpression().Left()) ==
          is_outer(inner_conjunct->AsBinaryExpression().Right())) {
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
    // The keyed semi/anti rewrite probes with a single column. A composite
    // test (the deferred struct a row-value `(a, b) IN (...)` lowers to)
    // cannot become a probe key; declaring such a conjunct correlated here
    // would strand it on a context-free physical filter, so the statement
    // stays on the scope-aware relational evaluator.
    if (!query.Exists() && query.Test()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (!query.Exists()) {
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

bool StatisticalAggregateOnlyRelational(const Expression& expression) {
  if (!expression) {
    return true;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    const auto& aggregate = expression->AsAggregateExpression();
    if (IsExtendedAggregate(aggregate.GetType()) &&
        !IsStatisticalAggregate(aggregate.GetType())) {
      return false;
    }
  }
  if (expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kIntervalExp ||
      expression->Type() == TypeTag::kFunctionCallExp ||
      expression->Type() == TypeTag::kWindowFunctionExp) {
    return false;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    return column.name != "*" && column.schema.find('.') == std::string::npos &&
           column.name.find('.') == std::string::npos;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             [](const Expression& child) {
                               return StatisticalAggregateOnlyRelational(child);
                             });
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
  static const bool trace_route =
      std::getenv("TINYLAMB_TRACE_ROUTE") != nullptr;
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
      if (trace_route) {
        LOG(INFO) << "route:relational source query="
                  << (source.query != nullptr) << " unnest=" << source.unnest
                  << " lateral=" << source.is_lateral
                  << " nested=" << source.from_nested_join;
      }
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
    const bool statistical_finish =
        !include_grouping &&
        StatisticalAggregateOnlyRelational(item.expression);
    if (NeedsRelationalEvaluation(item.expression) &&
        !window_item_routed(item.expression) && !statistical_finish) {
      return true;
    }
    // Value-table operands need the relational interpreter's proto-field
    // resolution (mirrors has_value_table_operand below).
    if (item.expression &&
        item.expression->Type() == TypeTag::kFunctionCallExp) {
      const auto& call = item.expression->AsFunctionCallExpression();
      const bool ordinary_star =
          call.FuncName() == "__value_table_value" && call.Args().size() == 1 &&
          call.Args()[0] && call.Args()[0]->Type() == TypeTag::kColumnValue &&
          call.Args()[0]->AsColumnValue().GetColumnName().name == "*";
      if (!ordinary_star && (call.FuncName() == "__value_table_value" ||
                             call.FuncName() == "__proto_new")) {
        return true;
      }
    }
  }
  if (NeedsRelationalEvaluation(select.WhereClause()) ||
      (NeedsRelationalEvaluation(select.Having()) &&
       (include_grouping ||
        !StatisticalAggregateOnlyRelational(select.Having())))) {
    return true;
  }
  for (const auto& term : select.OrderBy()) {
    const bool statistical_finish =
        !include_grouping &&
        StatisticalAggregateOnlyRelational(term.expression);
    if (NeedsRelationalEvaluation(term.expression) &&
        !window_item_routed(term.expression) && !statistical_finish) {
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
  projected.reserve(std::min(visible_columns, columns.size()));
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

class RetainedExecutor final : public ExecutorBase {
 public:
  RetainedExecutor(Executor inner, CompiledPlanPtr retained)
      : retained_(std::move(retained)), inner_(std::move(inner)) {}

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

// Times one SELECT replay. The first completion becomes the baseline. A later
// completion that is both slower than 50ms and at least 8x that baseline
// drops the thread-local plan so the next execution is planned again.
class TimedSelectReuse final : public ExecutorBase {
 public:
  TimedSelectReuse(const TimedSelectReuse&) = delete;
  TimedSelectReuse& operator=(const TimedSelectReuse&) = delete;
  TimedSelectReuse(TimedSelectReuse&&) = delete;
  TimedSelectReuse& operator=(TimedSelectReuse&&) = delete;

  TimedSelectReuse(Executor inner, std::string fingerprint,
                   CompiledPlanPtr plan)
      : inner_(std::move(inner)),
        fingerprint_(std::move(fingerprint)),
        plan_(std::move(plan)),
        started_(std::chrono::steady_clock::now()) {}
  ~TimedSelectReuse() override { Finish(); }

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    const size_t rows = inner_->NextBatch(destination, max_rows);
    if (rows == 0) {
      Finish();
    }
    return rows;
  }
  void Dump(std::ostream& output, int indent) const override {
    inner_->Dump(output, indent);
  }
  void Explain(std::ostream& output, int indent) const override {
    inner_->Explain(output, indent);
  }
  Status GetStatus() const override { return inner_->GetStatus(); }

 private:
  void Finish() {
    if (finished_) {
      return;
    }
    finished_ = true;
    const auto elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_)
            .count());
    uint64_t baseline =
        plan_->reuse_baseline_ns.load(std::memory_order_relaxed);
    if (baseline == 0 && plan_->reuse_baseline_ns.compare_exchange_strong(
                             baseline, elapsed, std::memory_order_relaxed)) {
      return;
    }
    if (SelectReuseExceededThreshold(elapsed, baseline)) {
      plan_->retired.store(true, std::memory_order_relaxed);
      ForgetThreadCompiledPlan(fingerprint_);
    }
  }

  Executor inner_;
  std::string fingerprint_;
  CompiledPlanPtr plan_;
  std::chrono::steady_clock::time_point started_;
  bool finished_{false};
};

constexpr size_t kMaxCachedTemplates = 1024;
constexpr size_t kTemplateCacheShards = 16;
constexpr size_t kMaxCachedTemplatesPerShard =
    kMaxCachedTemplates / kTemplateCacheShards;
// Also bound total key bytes: a few very long fingerprints (huge IN lists)
// can otherwise blow the count-based limit's memory budget.
constexpr size_t kMaxCachedTemplateKeyBytes = 64ULL << 20;

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

// QueryData can bind these expressions from the base schema alone.
bool CanBindZeroLimitExpression(const Expression& expression) {
  if (!expression) {
    return true;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
    case TypeTag::kConstantValue:
      return true;
    case TypeTag::kBinaryExp:
    case TypeTag::kUnaryExp:
    case TypeTag::kCastExp:
      return std::ranges::all_of(ExpressionChildren(expression),
                                 CanBindZeroLimitExpression);
    default:
      return false;
  }
}

// A template is the parsed AST, which has views expanded inline, so it is only
// valid for the schema generation it was parsed under.  Database epochs start
// from a per-database range (next_database_epoch_base), so the stamp also
// identifies the database; a view DDL bumps it and orphans stale entries.
struct CachedTemplate {
  std::shared_ptr<Statement> statement;
  uint64_t epoch{0};
  // Monotonic use stamp; shared-shard eviction picks the oldest entry rather
  // than an arbitrary unordered_map bucket (T1715 sibling fix).
  uint64_t last_used{0};
};

std::atomic<uint64_t> g_template_use_clock{0};

struct TemplateShard {
  std::mutex mutex;
  std::unordered_map<std::string, CachedTemplate> cache;
};

std::array<TemplateShard, kTemplateCacheShards> template_shards;
thread_local std::unordered_map<std::string, CachedTemplate> local_templates;

TemplateShard& ShardFor(const std::string& fingerprint) {
  return template_shards[std::hash<std::string>{}(fingerprint) %
                         kTemplateCacheShards];
}

void RememberTemplate(const std::string& fingerprint,
                      std::unique_ptr<Statement> statement, uint64_t epoch) {
  // NOLINTNEXTLINE(modernize-use-designated-initializers) positional init is
  // clear
  const CachedTemplate shared{  // NOLINT(modernize-use-designated-initializers)
      // NOLINT(modernize-use-designated-initializers)
      std::shared_ptr<Statement>(std::move(
          statement)),  // NOLINT(modernize-use-designated-initializers)
      epoch, g_template_use_clock.fetch_add(1, std::memory_order_relaxed)};
  if (local_templates.size() >= kMaxCachedTemplates &&
      !local_templates.contains(fingerprint)) {
    // Evict the least-recently-used entry, not an arbitrary bucket head.
    auto oldest = local_templates.begin();
    for (auto it = std::next(oldest); it != local_templates.end(); ++it) {
      if (it->second.last_used < oldest->second.last_used) {
        oldest = it;
      }
    }
    local_templates.erase(oldest);
  }
  local_templates.insert_or_assign(fingerprint, shared);
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  size_t key_bytes = 0;
  for (const auto& [key, value] : shard.cache) {
    key_bytes += key.size();
  }
  while (shard.cache.size() >= kMaxCachedTemplatesPerShard ||
         key_bytes + fingerprint.size() > kMaxCachedTemplateKeyBytes) {
    if (shard.cache.empty()) {
      break;
    }
    auto oldest = shard.cache.begin();
    for (auto it = std::next(oldest); it != shard.cache.end(); ++it) {
      if (it->second.last_used < oldest->second.last_used) {
        oldest = it;
      }
    }
    key_bytes -= std::min(key_bytes, oldest->first.size());
    shard.cache.erase(oldest);
  }
  shard.cache.insert_or_assign(fingerprint, shared);
}

std::shared_ptr<Statement> FindTemplate(const std::string& fingerprint,
                                        uint64_t epoch) {
  if (const auto local = local_templates.find(fingerprint);
      local != local_templates.end()) {
    if (local->second.epoch == epoch) {
      return local->second.statement;
    }
    local_templates.erase(local);
  }
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  const auto cached = shard.cache.find(fingerprint);
  if (cached == shard.cache.end() || cached->second.epoch != epoch) {
    return nullptr;
  }
  if (local_templates.size() >= kMaxCachedTemplates &&
      !local_templates.contains(fingerprint)) {
    local_templates.erase(local_templates.begin());
  }
  cached->second.last_used = g_template_use_clock.fetch_add(
      1, std::memory_order_relaxed);
  local_templates.insert_or_assign(fingerprint, cached->second);
  return cached->second.statement;
}

bool PipelineEngineEnabled() noexcept {
  const char* env = std::getenv("TINYLAMB_EXEC_ENGINE");
  if (env == nullptr || env[0] == '\0') {
    return true;
  }
  return std::string_view(env) != "legacy";
}

StatusOr<Executor> LowerSelectPlanExecutor(
    const Plan& plan, TransactionContext& ctx, bool distinct,
    const std::vector<Expression>& order_expressions,
    const std::vector<bool>& order_ascending,
    const std::vector<std::optional<bool>>& order_nulls_first,
    const std::vector<OocSortSink::KeySpec>& sort_keys, bool has_limit,
    size_t limit, size_t offset, bool with_ties, size_t visible_columns,
    size_t final_select_size, bool keep_plan_explain_text = false) {
  const bool need_distinct = distinct && !plan->EnforcesDistinct();
  const bool need_sort =
      !order_expressions.empty() &&
      !plan->IsOrderedBy(order_expressions, order_ascending, order_nulls_first);
  const bool need_limit =
      (has_limit || offset != 0) && !plan->EnforcesLimit(limit, offset);
  const bool need_proj = visible_columns != final_select_size;

  if (PipelineEngineEnabled() && !(with_ties && need_limit)) {
    ASSIGN_OR_RETURN(
        Executor, direct_columnar,
        TryCreateDirectColumnarScanOrTopNExecutor(
            plan, ctx, need_distinct, order_expressions, order_ascending,
            order_nulls_first, sort_keys, has_limit, limit, offset, with_ties,
            visible_columns, final_select_size, keep_plan_explain_text));
    if (direct_columnar != nullptr) {
      return direct_columnar;
    }
    PipelineLoweringOptions opts;
    opts.keep_plan_explain_text = keep_plan_explain_text;
    opts.apply_post_distinct = need_distinct;
    if (need_distinct && visible_columns != final_select_size) {
      // Hidden ORDER BY carriers ($order<i>) trail the visible select list;
      // DISTINCT keys on the visible prefix only, matching the grouped
      // lane's distinct_cols contract.
      opts.post_distinct_key_cols.reserve(visible_columns);
      for (size_t c = 0; c < visible_columns; ++c) {
        opts.post_distinct_key_cols.push_back(static_cast<slot_t>(c));
      }
    }
    if (need_sort) {
      opts.post_sort_keys = sort_keys;
    }
    if (need_limit) {
      if (has_limit) {
        opts.post_limit = limit;
      }
      opts.post_offset = offset;
    }
    if (need_proj) {
      const Schema& schema = plan->GetSchema();
      std::vector<Column> proj_cols;
      proj_cols.reserve(visible_columns);
      opts.post_projection.reserve(visible_columns);
      for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
        opts.post_projection.emplace_back(schema.GetColumn(i).Name());
        proj_cols.push_back(schema.GetColumn(i));
      }
      opts.post_projection_schema = Schema("", std::move(proj_cols));
    }
    return PipelineLowering::LowerToExecutor(plan, ctx, opts);
  }

  Executor executor = plan->EmitExecutor(ctx);
  if (need_distinct) {
    if (visible_columns != final_select_size) {
      // Same visible-prefix contract as the pipeline branch above: hidden
      // ORDER BY carriers must not widen the duplicate key.
      std::vector<slot_t> key_cols;
      key_cols.reserve(visible_columns);
      for (size_t c = 0; c < visible_columns; ++c) {
        key_cols.push_back(static_cast<slot_t>(c));
      }
      executor = std::make_shared<DistinctExecutor>(
          std::move(executor), Schema{}, std::vector<Expression>{},
          std::move(key_cols));
    } else {
      executor = std::make_shared<DistinctExecutor>(std::move(executor));
    }
  }
  if (need_sort) {
    std::vector<SortExecutor::Key> legacy_keys;
    legacy_keys.reserve(sort_keys.size());
    for (const OocSortSink::KeySpec& k : sort_keys) {
      legacy_keys.push_back(SortExecutor::Key{.expression = k.expression,
                                              .ascending = k.ascending,
                                              .nulls_first = k.nulls_first,
                                              .collation = k.collation});
    }
    executor = std::make_shared<SortExecutor>(
        std::move(executor), plan->GetSchema(), std::move(legacy_keys));
  }
  if (need_limit) {
    executor =
        std::make_shared<LimitExecutor>(std::move(executor), limit, offset);
  }
  if (need_proj) {
    std::vector<NamedExpression> visible;
    visible.reserve(visible_columns);
    const Schema& schema = plan->GetSchema();
    for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
      visible.emplace_back(schema.GetColumn(i).Name());
    }
    executor = std::make_shared<Projection>(std::move(visible), schema,
                                            std::move(executor));
  }
  return executor;
}

std::optional<Executor> ServeCompiledSelect(TransactionContext& ctx,
                                            const CompiledPlan& compiled) {
  // Mirrors the executor-construction tail of PrepareStatement(kSelect);
  // shape metadata was captured from an identical bound statement at fill.
  const CompiledPlan::SelectShape& shape = *compiled.select_shape;
  std::vector<OocSortSink::KeySpec> sort_keys;
  sort_keys.reserve(shape.sort_keys.size());
  for (size_t i = 0; i < shape.sort_keys.size(); ++i) {
    const auto& key = shape.sort_keys[i];
    sort_keys.push_back(OocSortSink::KeySpec{
        .expression = key.first,
        .ascending = key.second,
        .nulls_first = i < shape.order_nulls_first.size()
                           ? shape.order_nulls_first[i]
                           : std::nullopt,
    });
  }
  StatusOr<Executor> exec_or = LowerSelectPlanExecutor(
      compiled.plan, ctx, shape.distinct, shape.order_expressions,
      shape.order_ascending, shape.order_nulls_first, sort_keys,
      shape.has_limit, shape.limit, shape.offset, shape.with_ties,
      shape.visible_columns, shape.final_select_size,
      /*keep_plan_explain_text=*/false);
  if (!exec_or.HasValue()) {
    return std::nullopt;
  }
  return exec_or.MoveValue();
}

}  // namespace

namespace {

// T3834: a bare integer ORDER BY item is a SELECT-list ordinal -- it picks
// WHICH output column to sort by, so it is semantics, not a literal the
// plan-cache fingerprint may share.  Without this, "... ORDER BY 1" and
// "... ORDER BY 2" (or f(1) vs f(2) sort keys) land on one fingerprint, and
// because Prepare rewrites the statement during ordinal re-resolution, a
// resolution from a different ordinal replays silently as "the" plan.
// Mix the raw text of every ORDER BY clause that contains a bare integer
// item into the fingerprint.  The suffix is a pure function of the statement
// text (same text -> same fingerprint -> cache still hits), and statements
// without such clauses are untouched.
std::string_view SkipSqlQuoted(std::string_view sql, size_t& i) {
  const char quote = sql[i];
  ++i;
  while (i < sql.size()) {
    if (sql[i] == '\\' && quote != '`' && i + 1 < sql.size()) {
      i += 2;
      continue;
    }
    if (sql[i] == quote) {
      ++i;
      break;
    }
    ++i;
  }
  return {};
}

bool IsWordChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';  // NOLINT(readability-implicit-bool-conversion)
}

// Matches the keyword `word` at pos (case-insensitive, word-bounded);
// returns the position just past it, or npos.
size_t MatchKeyword(std::string_view sql, size_t pos, std::string_view word) {
  if (pos + word.size() > sql.size()) {
    return std::string_view::npos;
  }
  for (size_t k = 0; k < word.size(); ++k) {
    const char a = static_cast<char>(
        std::toupper(static_cast<unsigned char>(sql[pos + k])));
    if (a != word[k]) {
      return std::string_view::npos;
    }
  }
  if (pos > 0 && IsWordChar(sql[pos - 1])) {
    return std::string_view::npos;
  }
  if (pos + word.size() < sql.size() && IsWordChar(sql[pos + word.size()])) {
    return std::string_view::npos;
  }
  return pos + word.size();
}

// Skips whitespace and comments; returns the next significant position.
size_t SkipBlanks(std::string_view sql, size_t pos) {
  while (pos < sql.size()) {
    if (std::isspace(static_cast<unsigned char>(sql[pos]))) {  // NOLINT(readability-implicit-bool-conversion)
      ++pos;
    } else if (sql.compare(pos, 2, "--") == 0) {
      pos = sql.find_first_of("\r\n", pos);
      if (pos == std::string_view::npos) {
        return sql.size();
      }
    } else if (sql.compare(pos, 2, "/*") == 0) {
      pos = sql.find("*/", pos + 2);
      if (pos == std::string_view::npos) {
        return sql.size();
      }
      pos += 2;
    } else {
      break;
    }
  }
  return pos;
}

std::string OrderByOrdinalFingerprintSuffix(std::string_view sql) {
  std::string suffix;
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    if (c == '\'' || c == '"' || c == '`') {
      SkipSqlQuoted(sql, i);
      continue;
    }
    if ((c == '-' && sql.compare(i, 2, "--") == 0) ||
        (c == '/' && sql.compare(i, 2, "/*") == 0)) {
      i = SkipBlanks(sql, i);
      continue;
    }
    const size_t after_order = MatchKeyword(sql, i, "ORDER");
    if (after_order == std::string_view::npos) {
      ++i;
      continue;
    }
    const size_t by_pos = SkipBlanks(sql, after_order);
    const size_t after_by = MatchKeyword(sql, by_pos, "BY");
    if (after_by == std::string_view::npos) {
      i = after_order;
      continue;
    }
    // Clause extent: to the statement terminator.  Stopping at ';' (top
    // level; nested parens only delay the stop) over-captures trailing
    // clauses, which is harmless -- the suffix must merely be
    // ordinal-sensitive and text-determined.
    size_t end = i;
    bool has_bare_integer = false;
    while (end < sql.size() && sql[end] != ';') {
      const char e = sql[end];
      if (e == '\'' || e == '"' || e == '`') {
        SkipSqlQuoted(sql, end);
        continue;
      }
      if ((e == '-' && sql.compare(end, 2, "--") == 0) ||
          (e == '/' && sql.compare(end, 2, "/*") == 0)) {
        end = SkipBlanks(sql, end);
        continue;
      }
      if (std::isdigit(static_cast<unsigned char>(e)) &&  // NOLINT(readability-implicit-bool-conversion)
          (end == i || !IsWordChar(sql[end - 1])) &&
          !(end > 0 && sql[end - 1] == '.')) {  // NOLINT(readability-simplify-boolean-expr)
        size_t digit_end = end;
        while (digit_end < sql.size() &&
               std::isdigit(static_cast<unsigned char>(sql[digit_end]))) {  // NOLINT(readability-implicit-bool-conversion)
          ++digit_end;
        }
        if (digit_end == sql.size() || !IsWordChar(sql[digit_end])) {
          has_bare_integer = true;
        }
        end = digit_end;
        continue;
      }
      ++end;
    }
    if (has_bare_integer) {
      suffix.append(sql.substr(i, end - i));
      suffix.push_back('\n');
    }
    i = after_by;
  }
  return suffix;
}

// True when SQL (outside string/identifier quotes and comments) contains
// STRUCT or PROTO as a standalone keyword.  See the call site in Prepare
// for why those statements stay off the template cache (P363/432).
bool ContainsStructOrProtoKeyword(std::string_view sql) {
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    if (c == '\'' || c == '"' || c == '`') {
      SkipSqlQuoted(sql, i);
      continue;
    }
    if ((c == '-' && sql.compare(i, 2, "--") == 0) ||
        (c == '/' && sql.compare(i, 2, "/*") == 0)) {
      i = SkipBlanks(sql, i);
      continue;
    }
    if (MatchKeyword(sql, i, "STRUCT") != std::string_view::npos ||
        MatchKeyword(sql, i, "PROTO") != std::string_view::npos) {
      return true;
    }
    ++i;
  }
  return false;
}

}  // namespace

StatusOr<Executor> SqlEngine::Prepare(TransactionContext& ctx,
                                      std::string_view sql) {
  last_error_.clear();
  last_statement_type_.reset();
  last_query_is_literal_constant_ = false;
  result_column_names_.clear();
  result_column_types_.clear();
  shared_grouping_cores_.clear();
  ctx.txn_.BeginStatementTimer();
  // Persisted views expand at FROM-reference time in the visitor, so the
  // session registry must be current before the statement is visited.
  EnsureViewsRegistered(ctx);
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
      // P3555: publish the declared types alongside the names. Without
      // them BuildColumnDescriptions falls back to the first-row sample,
      // and an ANALYZE over zero tables (no sample row) answered every
      // column as text, leaving typed JDBC clients unable to read `rows`.
      result_column_types_ = {ValueType::kVarChar, ValueType::kVarChar,
                              ValueType::kInt64};
      StatusOr<Executor> executed = ExecuteAnalyze(*database_, ctx, *analyze);
      if (!executed.HasValue()) {
        // P709: LastError() is printed verbatim by the CLI and the server
        // error path; a fixed "ANALYZE failed" string hid which table or
        // reason tripped the statement.
        last_error_ = "ANALYZE failed: " + executed.GetStatus().GetMessage();
        return executed.GetStatus();
      }
      return FinishPrepareWithTimeout(ctx.txn_, std::move(executed),
                                      &last_error_);
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
  // T3834: ORDER BY ordinals change plan semantics; mix them into the
  // fingerprint before the compiled-plan serve, the template lookup and the
  // fill-site candidate are all derived from it.
  if (templated.templatable) {
    const std::string ordinal_suffix =
        OrderByOrdinalFingerprintSuffix(query_sql);
    if (!ordinal_suffix.empty()) {
      templated.fingerprint += "\n--order-by-ordinal:" + ordinal_suffix;
    }
  }
  // The compact SQL template binder intentionally handles scalar literals,
  // but it cannot preserve every nested STRUCT/PROTO constructor shape.  A
  // stale bound tree can silently drop a sibling field (for example turning
  // STRUCT("abc", 3) into just 3), which is worse than reparsing this small
  // class of statements.  Keep complex structural queries on the original
  // parse path until the binder has a structural parameter model.
  // P363/432: the old substring test (`upper.find("STRUCT")`) also dropped
  // statements where the token appears only inside a word, string or comment
  // (WHERE name = 'STRUCTURE', FROM protocols).  Gate on real keywords
  // outside quoted spans instead; PARSE_PROTO-style embedded tokens now fall
  // through to the binder, whose parameter-count audit rejects any shape it
  // cannot re-bind, so correctness is unchanged and the common non-STRUCT
  // queries get their template.
  if (ContainsStructOrProtoKeyword(query_sql)) {
    templated.templatable = false;
  }
  std::string upper_query(query_sql);
  std::transform(
      upper_query.begin(), upper_query.end(), upper_query.begin(),
      [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  // The FOR UPDATE wait policy is baked into the plan (it becomes the scan's
  // lock mode) and GoogleSQL has no syntax for it, so the policy joins the
  // fingerprint: a cached plan filled under kWait must never replay a blocking
  // acquisition for a kNoWait engine.
  if (for_update_wait_mode_ == ForUpdateWaitMode::kNoWait &&
      upper_query.find("FOR UPDATE") != std::string::npos) {
    templated.fingerprint.append("\n-- for-update-nowait");
  }
  // Consult the compiled-plan cache before any parse/bind/plan
  // work. Literal-free SQL bypasses the template binder and may replay only
  // an audited immutable artifact under its exact fingerprint.
  // A transaction with staged catalog changes can see an index that other
  // transactions cannot use and that may disappear on rollback.
  const bool use_compiled_cache =
      !force_relational_fallback_ && !ctx.txn_.IsCatalogDirty();
  if (!explain && use_compiled_cache &&
      (templated.templatable || templated.parameters.empty())) {
    if (std::optional<Executor> served =
            ServeFromPlanCache(ctx, templated.fingerprint, templated.parameters,
                               !templated.templatable)) {
      return FinishPrepareWithTimeout(ctx.txn_, std::move(*served),
                                      &last_error_);
    }
  }
  // Read before any parse so a template is never stamped newer than the schema
  // it was parsed against.
  const uint64_t template_epoch = database_->SchemaEpoch();
  if (templated.templatable) {
    if (const std::shared_ptr<Statement> cached =
            FindTemplate(templated.fingerprint, template_epoch)) {
      if (auto rebound = BindStatementLiterals(*cached, templated.parameters);
          rebound.HasValue()) {
        statement = rebound.MoveValue();
        cache_hit = true;
        PlanCacheStats().template_hits.fetch_add(1, std::memory_order_relaxed);
      }
      // A literal shape that drifted from the cached tree falls through and
      // parses the original SQL.
    }
  }
  if (!statement) {
    static const bool plan_profile =
        std::getenv("TINYLAMB_PLAN_PROFILE") != nullptr;
    const auto parse_begin = std::chrono::steady_clock::now();
    GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(query_sql);
    if (plan_profile) {
      LOG(INFO) << "plan-profile: parse-subprocess="
                << std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - parse_begin)
                       .count()
                << "ms";
    }
    if (!parsed.ok) {
      last_error_ = std::move(parsed.error);
      // Syntax rejections are client errors; kUnknown hid that from wire
      // status mapping (pgwire reported them like internal faults).
      return Status::kInvalidArgument;
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
  if (!explain && statement->Type() == StatementType::kSelect) {
    const auto& select = static_cast<const SelectStatement&>(*statement);  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto literal = [&](const auto& self,
                             const Expression& expression) -> bool {
      if (!expression) {
        return false;
      }
      if (expression->Type() == TypeTag::kConstantValue) {
        return true;
      }
      if (expression->Type() != TypeTag::kBinaryExp &&
          expression->Type() != TypeTag::kUnaryExp) {
        return false;
      }
      return std::ranges::all_of(
          ExpressionChildren(expression),
          [&](const Expression& child) { return self(self, child); });
    };
    last_query_is_literal_constant_ =
        select.Sources().empty() && select.WithQueries().empty() &&
        select.UnionAll().empty() && !select.GetSetOperationTree() &&
        !select.WhereClause() && select.GroupBy().empty() && !select.Having() &&
        !select.Qualify() && select.OrderBy().empty() && !select.Distinct() &&
        !select.HasDistinctOn() && !select.HasLimit() && select.Offset() == 0 &&
        !select.WithTies() && !select.ForUpdate() && !select.AsStruct() &&
        !select.SelectList().empty() &&
        std::ranges::all_of(select.SelectList(),
                            [&](const NamedExpression& item) {
                              return literal(literal, item.expression);
                            });
  }
  if (explain) {
    return ExecuteExplain(ctx, query_sql, *explain, std::move(statement));
  }

  if (statement->Type() == StatementType::kUpdate &&
      static_cast<const UpdateStatement&>(*statement).HasNestedDml()) {  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
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
      RememberTemplate(templated.fingerprint, rebound.MoveValue(),
                       template_epoch);
      PlanCacheStats().template_fills.fetch_add(1, std::memory_order_relaxed);
    }
  }
  // Arm the compiled-plan fill sites inside PrepareStatement; the guard
  // disarms them on every exit (including EXPLAIN, which never sets one).
  // A statement that folds subqueries into constants at prepare time must
  // neither consume a cached plan (it may hold an older fold) nor publish
  // one (Bug 57): mark its fingerprint volatile around the compile.
  const std::string fold_fp = templated.fingerprint;
  const bool fold_marked_fresh =
      !fold_fp.empty() && MarkFingerprintVolatile(fold_fp);
  dml_fold_applied_ = false;
  if (use_compiled_cache) {
    set_plan_cache_candidate(templated.fingerprint, templated.parameters);
  }
  PlanCacheCandidateGuard candidate_guard{this};
  static const bool plan_profile2 =
      std::getenv("TINYLAMB_PLAN_PROFILE") != nullptr;
  const auto prepare_stmt_begin = std::chrono::steady_clock::now();
  // Statement execution (e.g. eager DML application) must uphold the
  // StatusOr contract: runtime errors surface as Status values, never as
  // escaping C++ exceptions.
  try {
    StatusOr<Executor> result = PrepareStatement(ctx, std::move(statement));
    if (plan_profile2) {
      LOG(INFO) << "plan-profile: prepare-statement="
                << std::chrono::duration<double, std::milli>(
                       std::chrono::steady_clock::now() - prepare_stmt_begin)
                       .count()
                << "ms";
    }
    // The mark was precautionary: without an actual prepare-time fold the
    // fingerprint stays cache-eligible.
    DemoteVolatileFingerprint(fold_fp, fold_marked_fresh && !dml_fold_applied_);
    return FinishPrepareWithTimeout(ctx.txn_, std::move(result), &last_error_);
  } catch (const std::exception& error) {
    DemoteVolatileFingerprint(fold_fp, fold_marked_fresh);
    last_error_ = error.what();
    return Status::kUnknown;
  }
}

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
  const bool can_short_circuit_disjoint_limit =
      tree != nullptr && tree->grouped && tree->disjoint_ordered_by_head &&
      select.HasLimit() && !select.WithTies() && !select.UnionDistinct() &&
      std::ranges::all_of(pair_kinds, [](SetOperationKind k) {
        return k == SetOperationKind::kUnionAll;
      });
  const size_t short_circuit_target = select.Limit() + select.Offset();
  size_t accumulated_rows = 0;
  std::vector<std::vector<Row>> term_rows;
  term_rows.reserve(terms.size());
  std::vector<ValueType> output_types;
  for (auto& term : terms) {
    // This path is only reached for set-op statements without a WITH clause
    // (the caller gates on WithQueries().empty()); WITH-bearing set ops are
    // handled by the relational route, where CTEs are already in scope.
    result_column_names_.clear();
    result_column_types_.clear();
    StatusOr<Executor> prepared =
        PrepareStatement(ctx, std::make_unique<SelectStatement>(*term));
    if (!prepared.HasValue()) {
      return prepared.GetStatus();
    }
    // Operand preparation overwrites the engine metadata. Keep the first
    // known type at each output position, even when its rows are all NULL.
    if (output_types.size() < result_column_types_.size()) {
      output_types.resize(result_column_types_.size(), ValueType::kNull);
    }
    for (size_t column = 0; column < result_column_types_.size(); ++column) {
      if (output_types[column] == ValueType::kNull) {
        output_types[column] = result_column_types_[column];
      }
    }
    std::vector<Row> rows;
    DataChunk batch;
    while (prepared.Value()->NextBatch(&batch) != 0) {
      // T4751: batch sizes are known before the copy loop; reserving to
      // rows.size() + batch.Size() per batch drops the geometric regrowth
      // (and the Row-vector moves it entails) to zero.
      rows.reserve(rows.size() + batch.Size());
      for (size_t i = 0; i < batch.Size(); ++i) {
        rows.push_back(batch.RowAt(i));
      }
    }
    // A term that aborted mid-stream (e.g. an aggregate overflow) must fail
    // the whole set operation; swallowing it here would silently truncate the
    // concatenated result.
    const Status term_status = prepared.Value()->GetStatus();
    if (term_status != Status::kSuccess) {
      return term_status;
    }
    accumulated_rows +=
        rows.size();  // NOLINT(readability-container-size-empty)
                      // not an emptiness check
    term_rows.push_back(std::move(rows));
    if (can_short_circuit_disjoint_limit &&  // NOLINT(readability-container-size-empty)
        accumulated_rows >= short_circuit_target) {
      // NOLINTNEXTLINE(readability-container-size-empty) size comparison is
      // intentional
      pair_kinds.resize(!term_rows.empty() ? term_rows.size() - 1 : 0);
      break;
    }
  }

  const auto apply_pair =
      [](std::vector<Row> left, std::vector<Row> right,
         SetOperationKind operation) -> StatusOr<std::vector<Row>> {
    // T4751: no set-op fold can emit more rows than both inputs combined
    // (UNION ALL hits the bound exactly); reserve it up front instead of
    // regrowing per 1024-row batch.
    const size_t fold_row_bound = left.size() + right.size();
    SetOperationExecutor operation_executor(
        {std::make_shared<ConstantExecutor>(std::move(left)),
         std::make_shared<ConstantExecutor>(std::move(right))},
        operation);
    std::vector<Row> folded;
    folded.reserve(fold_row_bound);
    DataChunk batch;
    while (operation_executor.NextBatch(&batch) != 0) {
      for (size_t i = 0; i < batch.Size(); ++i) {
        folded.push_back(batch.RowAt(i));
      }
    }
    // A fold that aborted mid-materialization (incompatible branch types,
    // spill I/O failure, child executor error) must fail the whole set
    // operation: NextBatch returns 0 after FailWith, so without this check
    // the partial rows would masquerade as a successful result -- the same
    // contract the per-operand GetStatus check above enforces.
    const Status fold_status = operation_executor.GetStatus();
    if (fold_status != Status::kSuccess) {
      return fold_status;
    }
    return folded;
  };
  std::vector<std::vector<Row>> folded_terms;
  folded_terms.reserve(term_rows.size());
  std::vector<SetOperationKind> low_ops;
  low_ops.reserve(term_rows.size());
  folded_terms.push_back(std::move(term_rows.front()));
  for (size_t i = 0; i + 1 < term_rows.size(); ++i) {
    const SetOperationKind operation = pair_kinds[i];
    const bool intersects = operation == SetOperationKind::kIntersect ||
                            operation == SetOperationKind::kIntersectAll;
    if (intersects) {
      ASSIGN_OR_RETURN(std::vector<Row>, intersected,
                       apply_pair(std::move(folded_terms.back()),
                                  std::move(term_rows[i + 1]), operation));
      folded_terms.back() = std::move(intersected);
    } else {
      low_ops.push_back(operation);
      folded_terms.push_back(std::move(term_rows[i + 1]));
    }
  }
  std::vector<Row> combined = std::move(folded_terms.front());
  for (size_t j = 1; j < folded_terms.size(); ++j) {
    ASSIGN_OR_RETURN(std::vector<Row>, next_combined,
                     apply_pair(std::move(combined), std::move(folded_terms[j]),
                                low_ops[j - 1]));
    combined = std::move(next_combined);
  }

  // The recursive operand prepares above clobbered result_column_names_;
  // restore the outer statement's labels.  PostgreSQL derives set-operation
  // output names from the FIRST branch, and Execute() publishes this member
  // in the QueryResult (pgwire RowDescription follows it).
  result_column_names_ = output_names;
  result_column_types_ = std::move(output_types);

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
          // `ORDER BY 1 COLLATE 'und:ci'` collates the SELECT-list item, not
          // the ordinal constant, so the clause survives the substitution.
          key = ReapplyCollation(
              ColumnValueExp(ColumnName(output_names[ordinal - 1])), key);
        }
      }
      keys.push_back({std::move(key), term.ascending, term.nulls_first});
    }
    if (select.WithTies()) {
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
          ties_limit, select.Offset(), true);
      return executor;
    }
    if (select.HasLimit()) {
      std::vector<TopNExecutor::Key> topn_keys;
      topn_keys.reserve(keys.size());
      for (SortExecutor::Key& key : keys) {
        topn_keys.push_back(
            {std::move(key.expression), key.ascending, key.nulls_first});
      }
      return std::make_shared<TopNExecutor>(
          std::move(executor), combined_schema, std::move(topn_keys),
          select.Limit(), select.Offset());
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

namespace {

// Fingerprint of the grouped FROM+WHERE core, excluding the projection.
// Empty means "do not share" (lifted CTE rows are data, not a plan key).
std::string GroupingCoreFingerprint(const QueryData& core) {
  if (!core.lifted_ctes_.cells.empty() ||
      !core.lifted_ctes_.recursive.empty()) {
    return {};
  }
  std::ostringstream out;
  out << "pos=" << core.require_row_position_ << '\n';
  for (const std::string& relation : core.from_) {
    out << relation << '\n';
  }
  std::vector<std::pair<std::string, std::string>> aliases(
      core.aliases_.begin(), core.aliases_.end());
  std::sort(aliases.begin(), aliases.end());
  for (const auto& [alias, table] : aliases) {
    out << alias << '=' << table << '\n';
  }
  out << (core.where_ ? core.where_->ToString() : "") << '\n';
  if (core.qualify_) {
    out << core.qualify_->ToString() << '\n';
  }
  for (const QueryData::OuterJoinEdge& edge : core.outer_joins_) {
    out << edge.right_index << ',' << static_cast<int>(edge.join_kind) << ',';
    if (edge.on_condition) {
      out << edge.on_condition->ToString();
    }
    out << '\n';
  }
  return out.str();
}

bool SchemaHasColumn(const Schema& schema, const ColumnName& name) {
  if (name.name.empty() || name.name == "*") {
    return true;
  }
  int bare_matches = 0;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!name.schema.empty()) {
      if (IdentifierEquals(candidate.schema, name.schema) &&
          IdentifierEquals(candidate.name, name.name)) {
        return true;
      }
    } else if (IdentifierEquals(candidate.name, name.name)) {
      ++bare_matches;
    }
  }
  return name.schema.empty() && bare_matches == 1;
}

// A cached core plan is reusable when every column this branch's grouping
// finish reads is already in the plan output. Constant projections (the
// COUNT(*) placeholder, NULL-padded keys) do not need a matching column.
// Extra columns in the cached plan do not change the row set.
bool GroupingCoreCovers(const Schema& schema,
                        const std::vector<NamedExpression>& select) {
  if (schema.ColumnCount() == 0 || select.empty()) {
    return false;
  }
  return std::ranges::all_of(select, [&](const NamedExpression& item) {
    if (!item.expression ||
        item.expression->Type() == TypeTag::kConstantValue) {
      return true;
    }
    return item.expression->Type() == TypeTag::kColumnValue &&
           SchemaHasColumn(schema,
                           item.expression->AsColumnValue().GetColumnName());
  });
}

}  // namespace

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
  seen_relations.reserve(select.Sources().size());
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
  {
    // Whole-row table references (e.g. `SELECT r, rs FROM rs GROUP BY ...`)
    // rewrite inside `core.Rewrite(ctx)` to `__struct_json__(...)` function
    // calls. Flatten any composite expression in `core.select_` into its
    // constituent `ColumnValue`s so the core plan projects every underlying
    // table column with its qualified schema name for `GroupedFinishExecutor`.
    std::vector<NamedExpression> flat_core_select;
    flat_core_select.reserve(core.select_.size());
    auto push_unique_col = [&](const ColumnName& col) {
      if (col.name == "*" || col.name.empty()) {
        return;
      }
      const bool already =
          std::ranges::any_of(flat_core_select, [&](const NamedExpression& e) {
            return e.expression &&
                   e.expression->Type() == TypeTag::kColumnValue &&
                   e.expression->AsColumnValue().GetColumnName() == col;
          });
      if (!already) {
        flat_core_select.emplace_back("", ColumnValueExp(col));
      }
    };
    for (const NamedExpression& item : core.select_) {
      if (!item.expression) {
        continue;
      }
      if (item.expression->Type() == TypeTag::kColumnValue) {
        push_unique_col(item.expression->AsColumnValue().GetColumnName());
      } else if (item.expression->Type() == TypeTag::kConstantValue) {
        flat_core_select.push_back(item);
      } else {
        const std::function<void(const Expression&)> collect_in_order =
            [&](const Expression& expr) {
              if (!expr) {
                return;
              }
              if (expr->Type() == TypeTag::kColumnValue) {
                push_unique_col(expr->AsColumnValue().GetColumnName());
                return;
              }
              for (const Expression& child : ExpressionChildren(expr)) {
                collect_in_order(child);
              }
            };
        collect_in_order(item.expression);
      }
    }
    if (flat_core_select.empty()) {
      flat_core_select.emplace_back("", ConstantValueExp(Value(true)));
    }
    core.select_ = std::move(flat_core_select);
  }
  auto statement = std::make_shared<SelectStatement>(select);
  std::vector<Column> output_columns;
  output_columns.reserve(result_column_names_.size());
  for (const std::string& name : result_column_names_) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
  // GROUPING SETS / ROLLUP / CUBE expands to UNION ALL of the same
  // FROM+WHERE with a narrower projection on later branches. Plan that core
  // once and reuse it when the cached schema still has every column this
  // branch aggregates.
  const std::string core_fingerprint = GroupingCoreFingerprint(core);
  Plan shared_core;
  if (!core_fingerprint.empty()) {
    for (const SharedGroupingCore& cached : shared_grouping_cores_) {
      if (cached.fingerprint == core_fingerprint &&
          GroupingCoreCovers(cached.plan->GetSchema(), core.select_)) {
        shared_core = cached.plan;
        break;
      }
    }
  }
  // The hand-rolled relational route (scan projections, runtime filters,
  // typed folds, grouped-join fusion) is the default for grouped statements.
  // History: the Cascades-first order originally won because the relational
  // heuristic lacked OR-branch equijoin inference (TPC-DS q13 degraded to
  // nested loops); SynthesizeDisjunctEqualityPredicates
  // (planning_heuristics.cpp) resolved that 2026-10-06, and the 2026-10-07
  // route-agnostic audit ran the full ctest suite under the relational route
  // (3797/3799; the one route-attributable failure, a fused grouped join
  // LEFT-join row loss with residual-rejected pairs, was root-fixed in
  // TryFusedGroupedJoin).  TINYLAMB_GROUPED_ROUTE=cascades restores the
  // old Cascades-first order.
  const bool grouped_route_relational = [] {
    const char* route = std::getenv("TINYLAMB_GROUPED_ROUTE");
    if (route != nullptr && std::string_view(route) != "cascades") {
      LOG(LogLevel::kWarn) << "unknown TINYLAMB_GROUPED_ROUTE=" << route
                           << ", using relational default";
    }
    return route == nullptr || std::string_view(route) != "cascades";
  }();
  // Bare non-grouped columns outside aggregates evaluate against the group
  // representative row, whose identity is not stable across access-path
  // choices (e.g. IndexScan vs FullScan). Route such queries through the
  // relational grouping pipeline even under TINYLAMB_GROUPED_ROUTE=cascades.
  const bool depends_on_representative_row = [&]() {
    if (grouped_route_relational) {
      return false;
    }
    const auto resolve_col = [&](const ColumnName& col) -> ColumnName {
      if (!col.schema.empty()) {
        return {AsciiToLower(col.schema), AsciiToLower(col.name)};
      }
      for (const NamedExpression& item : core.select_) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& cand =
            item.expression->AsColumnValue().GetColumnName();
        if (IdentifierEquals(cand.name, col.name)) {
          return {AsciiToLower(cand.schema), AsciiToLower(cand.name)};
        }
      }
      return {"", AsciiToLower(col.name)};
    };
    std::unordered_set<ColumnName> grouped_columns;
    grouped_columns.reserve(select.GroupBy().size());
    std::unordered_set<std::string> grouped_expr_strings;
    grouped_expr_strings.reserve(select.GroupBy().size());
    if (!select.IsGroupAll()) {
      for (const Expression& g : select.GroupBy()) {
        const Expression resolved_g = substitute_aliases(g);
        if (!resolved_g) {
          continue;
        }
        grouped_expr_strings.insert(resolved_g->ToString());
        if (resolved_g->Type() == TypeTag::kColumnValue) {
          grouped_columns.insert(
              resolve_col(resolved_g->AsColumnValue().GetColumnName()));
        }
      }
    }
    const std::function<bool(const Expression&)> has_ungrouped_column =
        [&](const Expression& expr) -> bool {
      if (!expr || expr->Type() == TypeTag::kAggregateExp ||
          expr->Type() == TypeTag::kQueryExp) {
        return false;
      }
      if (grouped_expr_strings.contains(expr->ToString())) {
        return false;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (col.name == "*" || col.name.empty()) {
          return false;
        }
        return !grouped_columns.contains(resolve_col(col));
      }
      return std::ranges::any_of(ExpressionChildren(expr),
                                 has_ungrouped_column);
    };
    for (const NamedExpression& item : select.SelectList()) {
      if (has_ungrouped_column(item.expression)) {
        return true;
      }
    }
    if (has_ungrouped_column(substitute_aliases(select.Having()))) {
      return true;
    }
    return std::ranges::any_of(select.OrderBy(), [&](const auto& term) {
      return has_ungrouped_column(substitute_aliases(term.expression));
    });
  }();
  auto emit_relational = [&]() -> StatusOr<Executor> {
    StatusOr<Plan> optimized = Optimizer::OptimizeRelational(
        statement, Schema("", output_columns), ctx);
    if (!optimized.HasValue()) {
      return optimized.GetStatus();
    }
    Plan relational_plan = std::move(optimized.Value());
    if (relational_plan == nullptr) {
      last_error_ = "relational optimizer produced no plan";
      return StatusError(StatusCode::kUnknown, last_error_);
    }
    // Sequenced: emit while the plan is alive, then hand both to the
    // explain wrapper so the plan tree outlives the executor.
    if (PipelineEngineEnabled()) {
      ASSIGN_OR_RETURN(Executor, pipe_exec,
                       PipelineLowering::LowerToExecutor(relational_plan, ctx));
      return WithPlanExplain(std::move(pipe_exec), std::move(relational_plan),
                             &result_column_types_);
    }
    Executor executor = relational_plan->EmitExecutor(ctx);
    return WithPlanExplain(std::move(executor), std::move(relational_plan),
                           &result_column_types_);
  };
  const bool force_ooc_pipeline =
      !grouped_route_relational ||
      ProcessMemoryBudget::Global().OpMemoryOverrideBytes() > 0 ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled() ||
      ProcessMemoryBudget::Global().Limit() <
          ProcessMemoryBudget::kDefaultLimitBytes;
  if (PipelineEngineEnabled() && force_ooc_pipeline &&
      !depends_on_representative_row) {
    StatusOr<Plan> pipe_core = shared_core ? StatusOr<Plan>(shared_core)
                                           : Optimizer::Optimize(core, ctx);
    if (pipe_core.HasValue()) {
      Plan core_plan = std::move(pipe_core.Value());
      StatusOr<Plan> opt_rel = Optimizer::OptimizeRelational(
          statement, Schema("", output_columns), ctx);
      Plan explain_target =
          opt_rel.HasValue() && opt_rel.Value() ? opt_rel.Value() : core_plan;
      ASSIGN_OR_RETURN(std::optional<PipelineGraph>, grouped_graph,
                       TryLowerGroupedStatementToPipeline(
                           *statement, core_plan, ctx,
                           [explain_target]() {
                             std::ostringstream oss;
                             if (explain_target != nullptr) {
                               explain_target->Dump(oss, 0);
                             }
                             return oss.str();
                           },
                           substitute_aliases));
      if (grouped_graph.has_value()) {
        Executor exec =
            std::make_shared<PipelineExecutor>(&ctx, std::move(*grouped_graph));
        return WithPlanExplain(std::move(exec), std::move(explain_target),
                               &result_column_types_);
      }
    }
  }
  if (grouped_route_relational || depends_on_representative_row) {
    StatusOr<Executor> relational_executor = emit_relational();
    if (relational_executor.HasValue()) {
      return relational_executor.MoveValue();
    }
  }
  StatusOr<Plan> core_plan_or = shared_core ? StatusOr<Plan>(shared_core)
                                            : Optimizer::Optimize(core, ctx);
  if (!core_plan_or.HasValue()) {
    StatusOr<Executor> optimized = emit_relational();
    if (!optimized.HasValue()) {
      std::ostringstream error;
      error << "relational optimizer failed: " << optimized.GetStatus();
      last_error_ = error.str();
      return optimized.GetStatus();
    }
    return optimized.MoveValue();
  }
  Plan core_plan = std::move(core_plan_or.Value());
  if (!shared_core && !core_fingerprint.empty() &&
      shared_grouping_cores_.size() < 8 &&
      GroupingCoreCovers(core_plan->GetSchema(), core.select_)) {
    shared_grouping_cores_.push_back(
        SharedGroupingCore{.fingerprint = core_fingerprint, .plan = core_plan});
  }
  std::optional<size_t> finite_limit;
  if (select.HasLimit() && !select.WithTies() &&
      select.Offset() <= std::numeric_limits<size_t>::max() - select.Limit()) {
    finite_limit = select.Offset() + select.Limit();
  }
  // Simple grouped SQL uses the same costed hash/sort/stream alternatives
  // as the memo API. Rich finish semantics keep the established bridge.
  if (core.from_.size() == 1 && !select.GroupBy().empty() && !select.Having() &&
      !select.Qualify() && select.OrderBy().empty() && !select.HasLimit() &&
      !select.Distinct()) {
    auto grouped = Optimizer::OptimizeGrouping(
        core, core_plan, select.GroupBy(), select.SelectList(), ctx);
    if (grouped.HasValue()) {
      Plan plan = std::move(grouped.Value());
      if (PipelineEngineEnabled()) {
        ASSIGN_OR_RETURN(Executor, pipe_exec,
                         PipelineLowering::LowerToExecutor(plan, ctx));
        return WithPlanExplain(std::move(pipe_exec), std::move(plan),
                               &result_column_types_);
      }
      Executor executor = plan->EmitExecutor(ctx);
      return WithPlanExplain(std::move(executor), std::move(plan),
                             &result_column_types_);
    }
  }
  Plan finish = std::make_shared<GroupByPlan>(
      core_plan, std::move(statement), Schema("", std::move(output_columns)),
      !select.GroupBy().empty(), finite_limit);
  finish->SetExplainInfo(core_plan->ExplainInfo());
  if (PipelineEngineEnabled()) {
    const auto* gb = dynamic_cast<const GroupByPlan*>(finish.get());
    if (gb != nullptr && gb->Statement() != nullptr) {
      ASSIGN_OR_RETURN(std::optional<PipelineGraph>, grouped_graph,
                       TryLowerGroupedStatementToPipeline(
                           *gb->Statement(), core_plan, ctx,
                           [finish]() {
                             std::ostringstream oss;
                             finish->Dump(oss, 0);
                             return oss.str();
                           },
                           substitute_aliases));
      if (grouped_graph.has_value()) {
        Executor exec =
            std::make_shared<PipelineExecutor>(&ctx, std::move(*grouped_graph));
        return WithPlanExplain(std::move(exec), std::move(finish),
                               &result_column_types_);
      }
    }
    ASSIGN_OR_RETURN(Executor, pipe_exec,
                     PipelineLowering::LowerToExecutor(finish, ctx));
    return WithPlanExplain(std::move(pipe_exec), std::move(finish),
                           &result_column_types_);
  }
  PlanBase& finish_ref = *finish;
  return WithPlanExplain(finish_ref.EmitExecutor(ctx), std::move(finish),
                         &result_column_types_);
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
  table_relations.reserve(select.Sources().size());
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
      const std::string unnest_tag = "unnest_" + std::to_string(unnest_idx++);
      const cascades::GroupId next_group =
          memo.EnsureDerivedGroup(current_relations, unnest_tag);
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
      const std::string apply_tag = "apply_" + std::to_string(apply_idx++);
      const cascades::GroupId next_group =
          memo.EnsureDerivedGroup(current_relations, apply_tag);
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
      inner_cols.reserve(source.query->SelectList().size());
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
      // The leaf runs the body through the work-table driver with no sibling
      // CTE in scope and one site to serve, so a body that reads another CTE
      // of the same statement must keep the relational route -- only that one
      // resolves the whole WITH list in dependency order.  The memo lift
      // applies the same gate before minting an opaque leaf, and a rejected
      // statement falls through to the relational route below.
      if (!RecursiveCteLiftable(select, cte_name)) {
        return StatusError(StatusCode::kNotImplemented,
                           "recursive CTE " + cte_name +
                               " needs the dependency-ordered route");
      }
      const std::shared_ptr<SelectStatement>& rec_body = it->second;
      const RecursiveDepthSpec* depth_spec = select.RecursiveDepthOf(cte_name);

      std::vector<Column> cols;
      cols.reserve(rec_body->SelectList().size());
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
        const std::string rec_cte_tag = "rec_cte_" + std::to_string(rec_cte_idx++);
        current_group = memo.EnsureDerivedGroup(current_relations, rec_cte_tag);
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
        const std::string rec_cte_tag = "rec_cte_" + std::to_string(rec_cte_idx++);
        const cascades::GroupId next_group =
            memo.EnsureDerivedGroup(current_relations, rec_cte_tag);
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
    std::optional<size_t> finite_limit;
    if (select.HasLimit() && !select.WithTies() &&
        select.Offset() <=
            std::numeric_limits<size_t>::max() - select.Limit()) {
      finite_limit = select.Offset() + select.Limit();
    }
    Plan finish = std::make_shared<GroupByPlan>(
        core_plan, std::move(statement), Schema("", std::move(output_columns)),
        !select.GroupBy().empty(), finite_limit);
    finish->SetExplainInfo(core_plan->ExplainInfo());
    PlanBase& finish_ref = *finish;
    return WithPlanExplain(finish_ref.EmitExecutor(ctx), std::move(finish),
                           &result_column_types_);
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
  projection_items.reserve(select.Sources().size());
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
    result_column_types_.clear();
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
      NamedExpression* matched = nullptr;
      for (NamedExpression& item : projection_items) {
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
      if (matched != nullptr && !matched->name.empty() &&
          std::ranges::count_if(projection_items, [&](const auto& item) {
            return IdentifierEquals(item.name, matched->name);
          }) > 1) {
        matched->name = "$sort" + std::to_string(static_cast<size_t>(
                                      matched - projection_items.data()));
      }
      // An anonymous select item (`MOD(x, 2)` with no alias) has no output
      // name to bind to, so it takes the hidden-column path even though the
      // projection below already computes it: referencing the expression
      // itself would re-evaluate it against the projection OUTPUT, where the
      // source columns it reads (e.g. the UNNEST value column) no longer
      // exist.
      if (matched == nullptr || matched->name.empty()) {
        const std::string hidden = "$order" + std::to_string(sort_keys.size());
        projection_items.emplace_back(hidden, order_expression);
        sort_keys.emplace_back("", ColumnValueExp(ColumnName(hidden)));
        needs_hidden_order_columns = true;
        continue;
      }
      // The sort consumes the projection OUTPUT, whose columns carry the
      // select-list names; bind a matched key to that output name.
      sort_keys.emplace_back("", ColumnValueExp(ColumnName(matched->name)));
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
  Plan explain_plan = best->plan;
  Executor executor;
  if (PipelineEngineEnabled()) {
    PipelineLoweringOptions opts;
    if (needs_hidden_order_columns) {
      const Schema& schema = best->plan->GetSchema();
      std::vector<Column> vis_cols;
      vis_cols.reserve(visible_columns);
      opts.post_projection.reserve(visible_columns);
      for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
        opts.post_projection.emplace_back(schema.GetColumn(i).Name());
        vis_cols.push_back(schema.GetColumn(i));
      }
      opts.post_projection_schema = Schema("", std::move(vis_cols));
    }
    StatusOr<Executor> pipe_or =
        PipelineLowering::LowerToExecutor(explain_plan, ctx, opts);
    RETURN_IF_FAIL(pipe_or.GetStatus());
    executor = pipe_or.MoveValue();
  } else {
    executor = explain_plan->EmitExecutor(ctx);
    if (needs_hidden_order_columns) {
      const Schema& schema = best->plan->GetSchema();
      std::vector<NamedExpression> visible;
      visible.reserve(visible_columns);
      for (size_t i = 0; i < visible_columns && i < schema.ColumnCount(); ++i) {
        visible.emplace_back(schema.GetColumn(i).Name());
      }
      executor = std::make_shared<Projection>(std::move(visible), schema,
                                              std::move(executor));
    }
  }
  return WithPlanExplain(std::move(executor), std::move(explain_plan),
                         &result_column_types_);
}

StatusOr<Executor> SqlEngine::PrepareStatement(
    TransactionContext& ctx, std::unique_ptr<Statement> statement) {
  last_statement_type_ = statement->Type();
  // UPDATE ... FROM runs on the MERGE machinery after desugaring; the user
  // statement is an UPDATE, so diagnostics and server tags report UPDATE.
  if (statement->Type() == StatementType::kMerge &&
      dynamic_cast<const MergeStatement&>(*statement).DesugaredFromUpdate()) {
    last_statement_type_ = StatementType::kUpdate;
  }

  switch (statement->Type()) {
    case StatementType::kCreateTable:
      return ExecuteCreateTable(
          ctx, dynamic_cast<const CreateTableStatement&>(*statement));
    case StatementType::kInsert:
      return ExecuteInsert(ctx,
                           dynamic_cast<InsertStatement&>(*statement));
    case StatementType::kSelect: {
      auto* select_ptr = dynamic_cast<SelectStatement*>(statement.get());
      if (select_ptr == nullptr) {
        last_error_ = "statement marked kSelect is not a SelectStatement";
        return Status::kUnknown;
      }
      return ExecuteSelect(
          ctx, std::unique_ptr<SelectStatement>(
                   static_cast<SelectStatement*>(statement.release())));  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
    }
    case StatementType::kUpdate:
      return ExecuteUpdate(ctx,
                           dynamic_cast<UpdateStatement&>(*statement));
    case StatementType::kMerge:
      return ExecuteMerge(ctx, dynamic_cast<const MergeStatement&>(*statement));
    case StatementType::kDelete:
      return ExecuteDelete(ctx,
                           dynamic_cast<const DeleteStatement&>(*statement));
    case StatementType::kDropTable:
      return ExecuteDropTable(
          ctx, dynamic_cast<const DropTableStatement&>(*statement));
    case StatementType::kCreateMaterializedView:
      return ExecuteCreateMaterializedView(
          ctx,
          dynamic_cast<const CreateMaterializedViewStatement&>(*statement));
    case StatementType::kDropMaterializedView:
      return ExecuteDropMaterializedView(
          ctx, dynamic_cast<const DropMaterializedViewStatement&>(*statement));
    case StatementType::kCreateView:
      return ExecuteCreateView(
          ctx, dynamic_cast<const CreateViewStatement&>(*statement));
    case StatementType::kCreateFunction:
    case StatementType::kCreateConstant:
      // T2552: the visitor parsed the declaration but deferred the
      // thread_local registry write into the statement; executing it now
      // means EXPLAIN and failed prepares never register the object.
      return ExecuteCreateObject(
          ctx, dynamic_cast<const CreateObjectStatement&>(*statement));
    case StatementType::kDropView:
      return ExecuteDropView(
          ctx, dynamic_cast<const DropViewStatement&>(*statement));
    case StatementType::kCreateIndex:
      return ExecuteCreateIndex(
          ctx, dynamic_cast<const CreateIndexStatement&>(*statement));
    case StatementType::kAlterTable:
      return ExecuteAlterTable(
          ctx, dynamic_cast<const AlterTableStatement&>(*statement));
    case StatementType::kAnalyze:
      last_error_ = "ANALYZE is handled before statement binding";
      return Status::kNotImplemented;
  }
  return Status::kNotImplemented;
}

StatusOr<Executor> SqlEngine::ExecuteSelect(
    TransactionContext& ctx,
    std::unique_ptr<SelectStatement> select_statement) {
  fused_sibling_aggregates_ = 0;
  cte_restriction_pushdowns_ = 0;
  inner_prefix_pullups_ = 0;
  cte_materialization_attempts_ = 0;
  {
    const Status cycle =
        relational_detail::ValidateCteDependencies(*select_statement);
    if (cycle != Status::kSuccess) {
      last_error_ = cycle.GetMessage();
      return cycle.GetCode();
    }
  }
  auto select = std::shared_ptr<SelectStatement>(std::move(select_statement));
  // SELECT ... FOR UPDATE locks the rows its scan visits, which is the DML
  // source contract (`require_row_position` -> scan `lock_rows`): one base
  // table, no join, no aggregate/window/set-op/CTE/derived source.  A join
  // or an aggregate has no single row position to lock per output row, and
  // the relational route does not carry positions at all, so the shape is
  // refused up front instead of silently running an unlocked read.
  const bool for_update = select->ForUpdate();
  if (for_update) {
    const SelectSource* only_source =
        select->Sources().size() == 1 ? &select->Sources().front() : nullptr;
    const bool has_aggregate =
        std::ranges::any_of(
            select->SelectList(),
            [](const NamedExpression& item) {
              return relational_detail::ContainsAggregate(item.expression);
            }) ||
        relational_detail::ContainsAggregate(select->Having());
    if (only_source == nullptr || only_source->query != nullptr ||
        only_source->unnest || only_source->is_lateral ||
        only_source->table.empty() || only_source->sample.has_value() ||
        !select->WithQueries().empty() || !select->UnionAll().empty() ||
        select->GetSetOperationTree() != nullptr || select->Distinct() ||
        select->HasDistinctOn() || !select->GroupBy().empty() ||
        has_aggregate || select->Having() || select->Qualify() ||
        relational_detail::HasWindowFunctions(*select)) {
      last_error_ =
          "SELECT ... FOR UPDATE requires a single base table with no "
          "join, aggregate, DISTINCT, GROUP BY, HAVING, window function, "
          "set operation, sampled source, or CTE";
      return Status(Status::kInvalidArgument, last_error_);
    }
  }
  // M4: inline singly-referenced non-recursive CTEs into derived
  // sources, then lift the remaining layer (shared eager cells plus
  // opaque recursive leaves) when atomic; M5 flattens the single-table
  // derived sources last. All rewrites preserve the row multiset;
  // anything they reject keeps the existing materialized/relational
  // path untouched.
  // Lean-proven TPC-DS AST rewrites (lean/TinylambOpt/TpcdsDiscovery.lean):
  // skipped under `force_relational_fallback_` so differential oracle tests
  // cross-check the rewritten plans against the unrewritten interpreter.
  if (!force_relational_fallback_) {
    (void)CanonicalizeTwoValuedNegations(select.get());
    if (CanonicalizeSelectAliasGroupBy(select.get(), ctx)) {
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (PushSharedGroupedCteKeyRestrictions(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (PullInnerJoinsBeforeLeftJoins(select.get(), ctx)) {
      ++inner_prefix_pullups_;
    }
    if (DistributeZeroPaddedUnionGroupBy(select.get(), ctx)) {
      if (PullInnerJoinsBeforeLeftJoins(select.get(), ctx)) {
      ++inner_prefix_pullups_;
    }
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (SpecializeAndPivotUnionCtes(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (RewriteAdjacentRankSelfJoinToLagLead(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (FoldSingletonPartitionRankToOne(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (DistributeDistinctUnionDimJoins(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (EliminateUnreferencedUniquePkInnerJoins(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (EliminateSingletonScalarCrossJoins(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (DeferUniqueDimJoinPastGroupBy(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (RewriteFullOuterIndicatorSumsToSemiJoin(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (RewriteSelfJoinAggregateToWindow(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (HoistIdenticalDerivedSubqueriesToCte(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (RewriteCteArgmaxSubqueryToTopTies(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (PruneRedundantGroupByKeys(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (RewriteCorrelatedPositiveCountToInSubquery(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (PushDisjunctiveNullRejectingLeftJoinSemiFilter(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
  }
  (void)ReduceGroupByFunctionalDependencies(select.get(), ctx);
  (void)InlineSingleUseGroupedCtes(select.get(), ctx);
  (void)InlineSingleUseCtes(select.get());
  if (!force_relational_fallback_) {
    bool post_inline_rewritten = false;
    post_inline_rewritten |=
        PushSharedGroupedCteKeyRestrictions(select.get(), ctx);
    if (DistributeZeroPaddedUnionGroupBy(select.get(), ctx)) {
      if (PullInnerJoinsBeforeLeftJoins(select.get(), ctx)) {
      ++inner_prefix_pullups_;
    }
      post_inline_rewritten = true;
    }
    post_inline_rewritten |= DistributeDistinctUnionDimJoins(select.get(), ctx);
    post_inline_rewritten |=
        EliminateUnreferencedUniquePkInnerJoins(select.get(), ctx);
    post_inline_rewritten |=
        EliminateSingletonScalarCrossJoins(select.get(), ctx);
    post_inline_rewritten |= DeferUniqueDimJoinPastGroupBy(select.get(), ctx);
    post_inline_rewritten |=
        CollapseSharedUniqueDimAcrossJoinedSubqueries(select.get(), ctx);
    if (post_inline_rewritten) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
  }
  // A CTE that stays shared cannot take one consumer's filter, but it can
  // take the disjunction of every consumer's restriction: that keeps every
  // row any reference can emit and drops the rest before the cell is
  // built.  The estimate decides whether the narrower cell is worth it.
  cte_restriction_pushdowns_ += PushSharedCteRestrictions(select.get(), ctx);
  LiftedCtes lifted;
  MaterializeCtes(select.get(), ctx, &lifted);
  LiftRecursiveCtes(select.get(), &lifted);
  (void)FlattenDerivedSources(select.get(), ctx, &lifted);
  // M5+: single-table equality LATERALs become decorrelated joins (the
  // proven flattener above consumes them once uncorrelated).
  (void)DecorrelateSingleTableLaterals(select.get(), ctx);
  (void)StripIdempotentDistinctAggregates(select.get());
  // Sibling derived tables over one base table sharing a GROUP BY key
  // fold into a single aggregation (TPC-H Q21's `multi`/`only_late`).
  fused_sibling_aggregates_ += FuseSiblingDerivedAggregates(select.get());
  // Push HAVING MAX(col) > c / MIN(col) < c bounds into WHERE before
  // outer-join reduction so newly inferred null-rejecting WHERE bounds can
  // demote outer joins too (Lean Theorems 4-2 & 4-2').
  (void)PushHavingExtremumBounds(select.get(), ctx);
  // A WHERE (or HAVING) conjunct that rejects the NULL padding of an outer
  // join makes the outer join's padded rows unreachable; planning it as an
  // inner join is exact and unlocks hash-join fast paths (Lean 4-3).
  ReduceOuterJoins(select.get(), &ctx);
  // Unused LEFT JOINs under SELECT DISTINCT or multiplicity-insensitive
  // aggregates drop without requiring UNIQUE/PK (Lean Theorem 3-4).
  (void)EliminateUnusedOuterJoinsForIdempotentResult(select.get(), ctx);
  if (!force_relational_fallback_) {
    if (BypassDisjointUnionGroupBy(select.get())) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    if (PushDistinctSurrogateKeysBeforeDimJoins(select.get(), ctx)) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
    bool tpch_rewritten = false;
    tpch_rewritten |= FoldSingletonPartitionRankToOne(select.get());
    tpch_rewritten |=
        EliminateUnreferencedUniquePkInnerJoins(select.get(), ctx);
    tpch_rewritten |= DeferUniqueDimJoinPastGroupBy(select.get(), ctx);
    tpch_rewritten |=
        CollapseSharedUniqueDimAcrossJoinedSubqueries(select.get(), ctx);
    tpch_rewritten |= FactorAndEnvelopeDisjunctiveWhere(select.get(), ctx);
    tpch_rewritten |= PreaggregateLeftJoinRightCounts(select.get(), ctx);
    tpch_rewritten |= AbsorbSelfSemijoinHavingAggregate(select.get(), ctx);
    tpch_rewritten |= DecorrelateCorrelatedSumComparisons(select.get(), ctx);
    tpch_rewritten |=
        RewriteCorrelatedPositiveCountToInSubquery(select.get(), ctx);
    tpch_rewritten |= SynthesizeTransitiveEquijoins(select.get(), ctx);
    tpch_rewritten |=
        DeferUniqueDimensionAttributesAfterTopN(select.get(), ctx);
    tpch_rewritten |= RewriteAffineLinearSums(select.get(), ctx);
    if (tpch_rewritten) {
      dml_fold_applied_ = true;
      plan_cache_fingerprint_.clear();
      plan_cache_parameters_.clear();
    }
  }
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
        last_error_ = "unknown relation in select list: " + qualifier + ".*";
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
    // A literal `*` select item expands to every source column at execution;
    // pinning the literal `*` as the single output name makes the pgwire
    // RowDescription report `*` / `?column?` for the real N columns.
    if (item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      result_column_names_.clear();
      for (const SelectSource& source : select->Sources()) {
        const std::string name = source.table;
        if (name.empty()) {
          continue;
        }
        StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(name);
        if (!table.HasValue()) {
          continue;
        }
        const Schema& s = table.Value()->GetSchema();
        for (size_t i = 0; i < s.ColumnCount(); ++i) {
          result_column_names_.push_back(s.GetColumn(i).Name().name);
          // NOLINTNEXTLINE(modernize-use-emplace) explicit construction is
          // clear
        }
      }
      if (!result_column_names_.empty()) {
        break;
      }
      result_column_names_.push_back("*");  // NOLINT(modernize-use-emplace)
    } else {
      result_column_names_.push_back(
          !item.name.empty()
              ? item.name
              : (item.expression ? item.expression->ToString() : "?"));
    }
  }
  const auto declared_cast_type = [&](size_t column) {
    if (result_column_names_.size() != select->SelectList().size()) {
      return ValueType::kNull;
    }
    const Expression& expression = select->SelectList()[column].expression;
    return expression && expression->Type() == TypeTag::kCastExp
               ? expression->AsCastExpression().TargetValueType()
               : ValueType::kNull;
  };
  const bool has_table_sample = std::ranges::any_of(
      select->Sources(),
      [](const SelectSource& source) { return source.sample.has_value(); });
  const auto emit_relational = [&]() -> StatusOr<Executor> {
    if (for_update) {
      last_error_ =
          "SELECT ... FOR UPDATE cannot run on the relational route: this "
          "query shape has no row positions to lock (a plain single-table "
          "WHERE/ORDER BY/LIMIT shape is supported)";
      return Status(Status::kInvalidArgument, last_error_);
    }
    std::vector<Column> columns;
    columns.reserve(result_column_names_.size());
    for (size_t i = 0; i < result_column_names_.size(); ++i) {
      // Relational plans have no bound projection schema at this point,
      // but a CAST declares its output type even when the value is NULL.
      columns.emplace_back(result_column_names_[i], declared_cast_type(i));
    }
    StatusOr<Plan> optimized = Optimizer::OptimizeRelational(
        select, Schema("", std::move(columns)), ctx);
    if (!optimized.HasValue()) {
      std::ostringstream error;
      error << "relational optimizer failed: " << optimized.GetStatus();
      last_error_ = error.str();
      return optimized.GetStatus();
    }
    Plan relational_plan = std::move(optimized.Value());
    if (relational_plan == nullptr) {
      last_error_ = "relational optimizer produced no plan";
      return StatusError(StatusCode::kUnknown, last_error_);
    }
    if (PipelineEngineEnabled()) {
      ASSIGN_OR_RETURN(Executor, pipe_exec,
                       PipelineLowering::LowerToExecutor(relational_plan, ctx));
      return WithPlanExplain(std::move(pipe_exec), std::move(relational_plan),
                             &result_column_types_);
    }
    PlanBase& relational_ref = *relational_plan;
    Executor emitted = relational_ref.EmitExecutor(ctx);
    return WithPlanExplain(std::move(emitted), std::move(relational_plan),
                           &result_column_types_);
  };
  const bool sampled_relational_source =
      std::ranges::any_of(select->Sources(), [](const SelectSource& source) {
        return source.sample.has_value() &&
               (source.query != nullptr || source.unnest ||
                source.table.empty());
      });
  if (sampled_relational_source) {
    return emit_relational();
  }
  if (select->HasLimit() && select->Limit() == 0) {
    const SelectSource* source =
        select->Sources().size() == 1 ? &select->Sources().front() : nullptr;
    if (source != nullptr && !source->table.empty() && !source->sample &&
        !source->join_condition && !for_update && !select->IsGroupAll() &&
        !select->AsStruct() && !PostRewriteNeedsRelational(*select) &&
        CanBindZeroLimitExpression(select->WhereClause()) &&
        std::ranges::all_of(select->SelectList(),
                            [](const auto& item) {
                              return CanBindZeroLimitExpression(
                                  item.expression);
                            }) &&
        std::ranges::all_of(select->OrderBy(), [](const auto& term) {
          return CanBindZeroLimitExpression(term.expression);
        })) {
      // Bind names and infer projection types without opening the input. In
      // particular, a predicate may be valid to bind but fail on actual rows.
      const std::string& relation =
          source->alias.empty() ? source->table : source->alias;
      QueryData query({relation}, select->WhereClause(), select->SelectList());
      query.aliases_.emplace(relation, source->table);
      for (const auto& term : select->OrderBy()) {
        query.order_expressions_.push_back(term.expression);
      }
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(source->table));
      Plan input = std::make_shared<ValuesPlan>(
          table->GetSchema().WithName(relation), std::vector<Row>{});
      Plan plan = std::make_shared<EmptyPlan>(std::make_shared<ProjectionPlan>(
          std::move(input), std::move(query.select_)));
      const Schema& output = plan->GetSchema();
      if (result_column_names_.size() != output.ColumnCount()) {
        result_column_names_.clear();
        for (size_t i = 0; i < output.ColumnCount(); ++i) {
          result_column_names_.push_back(output.GetColumn(i).Name().name);
        }
      }
      Executor empty = plan->EmitExecutor(ctx);
      return WithPlanExplain(std::move(empty), std::move(plan),
                             &result_column_types_);
    }

    ASSIGN_OR_RETURN(Executor, validated, emit_relational());
    // The relational interpreter binds names when it opens, not when the
    // executor is built: pull once so an unknown column still fails here.
    if (validated) {
      Row probe;
      (void)validated->Next(&probe);
      if (Status status = validated->GetStatus(); !status.ok()) {
        if (last_error_.empty()) {
          last_error_ = ToString(status);
        }
        return status;
      }
    }
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
  const bool lateral_join_needs_relational =
      has_lateral &&
      std::any_of(select->Sources().begin(), select->Sources().end(),
                  [](const SelectSource& s) {
                    return !s.using_columns.empty() ||
                           s.join_type == JoinType::kLeft ||
                           s.join_type == JoinType::kRight ||
                           s.join_type == JoinType::kFull ||
                           s.join_condition != nullptr || s.from_nested_join;
                  });
  const bool can_decorrelate_subqueries =
      CanUseDecorrelatedSubqueryOptimizer(*select);
  const bool simple_count_star =
      select->Sources().size() == 1 && !select->Sources()[0].query &&
      !select->Sources()[0].unnest &&
      select->Sources()[0].join_type == JoinType::kCross &&
      select->Sources()[0].join_condition == nullptr &&
      select->WithQueries().empty() && select->GroupBy().empty() &&
      !select->Having() && !select->Qualify() && select->OrderBy().empty() &&
      !select->Distinct() && select->SelectList().size() == 1 &&
      select->SelectList()[0].expression &&
      select->SelectList()[0].expression->Type() == TypeTag::kAggregateExp &&
      select->SelectList()[0].expression->AsAggregateExpression().GetType() ==
          AggregationType::kCount &&
      !select->SelectList()[0].expression->AsAggregateExpression().Distinct() &&
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
    if (relational_detail::CanUseExactIntegerGroupingSets(ctx, *select)) {
      return emit_relational();
    }
    return ExecuteSetOperation(*select, ctx);
  }
  // Self-joins expose the same base relation under two aliases.
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
      grouped_expressions_correlate =
          grouped_expressions_correlate || touches_query_expression(expression);
    }
    for (const NamedExpression& item : select->SelectList()) {
      grouped_expressions_correlate = grouped_expressions_correlate ||
                                      touches_query_expression(item.expression);
    }
    // The bridge executes multi-relation and single-relation grouped
    // queries through the Cascades core (join ordering, access paths,
    // filter pushdown), feeding the grouping finish pipeline (GroupByPlan).
    // Routing reads the post-rewrite shape: flattened/inlined inputs are
    // plain by now, while genuinely complex shapes stay relational.
    const bool simple_grouped_memo =
        select->Sources().size() == 1 && !select->GroupBy().empty() &&
        !select->Having() && select->OrderBy().empty() && !select->HasLimit() &&
        !select->Distinct() &&
        Optimizer::SupportsGrouping(select->GroupBy(), select->SelectList());
    if (!force_relational_fallback_ && !has_table_sample &&
        (select->Sources().size() > 1 || simple_grouped_memo) && has_grouping &&
        !simple_count_star && !select->Qualify() &&
        !relational_detail::HasWindowFunctions(*select) &&
        select->WithQueries().empty() && sources_plain && outer_bridge_slice &&
        // The bridge's requirement collector skips star columns; a
        // modifier star must expand through the generic path instead.
        std::ranges::none_of(select->SelectList(),
                             [](const NamedExpression& item) {
                               return item.star_modifiers != nullptr;
                             }) &&
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
      std::ranges::any_of(select->SelectList(),
                          [&](const NamedExpression& item) {
                            return touches_query_expression(item.expression);
                          });
  const bool has_recursive_cte = std::any_of(
      select->WithQueries().begin(), select->WithQueries().end(),
      [&](const auto& pair) { return select->IsRecursiveWith(pair.first); });
  const bool has_plain_cte = std::any_of(
      select->WithQueries().begin(), select->WithQueries().end(),
      [&](const auto& pair) { return !select->IsRecursiveWith(pair.first); });
  // ARRAY(SELECT ...) select items need the relational interpreter's
  // scope chain (see ContainsArrayQueryExpression); the unnest and
  // vectorized routes evaluate projections without it.  A SQL UDF whose
  // body contains a subquery needs it too: invoking the body through a
  // plain Projection would throw.
  const auto touches_relational_subquery = [](const Expression& expression) {
    return ContainsArrayQueryExpression(expression) ||
           ContainsQueryUdfCall(expression);
  };
  const bool touches_array_subquery =
      std::ranges::any_of(select->SelectList(),
                          [&](const NamedExpression& item) {
                            return touches_relational_subquery(item.expression);
                          }) ||
      std::ranges::any_of(select->OrderBy(),
                          [&](const SelectStatement::OrderByTerm& term) {
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
      !unnest_join_needs_relational && !lateral_join_needs_relational &&
      !select_list_touches_query && !select->Qualify() &&
      !relational_detail::HasWindowFunctions(*select)) {
    StatusOr<Executor> unnest_exec = ExecuteUnnestSelect(*select, ctx);
    if (unnest_exec.HasValue()) {
      return unnest_exec;
    }
    if (unnest_exec.GetStatus() != Status::kNotImplemented) {
      std::ostringstream msg;
      msg << unnest_exec.GetStatus();
      last_error_ = msg.str();
      return unnest_exec.GetStatus();
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
  if (multi_relation && has_aggregate && !has_table_sample) {
    return emit_relational();
  }
  // Computed scalar aggregates otherwise decode Rows into one parallel
  // scan pool and hand them to a second pool evaluating the AST. The
  // relational typed scan already compiles these numeric expressions
  // once and folds while decoding, without either batch handoff.
  // Large filtered floating-point aggregates use that same path even when
  // the optional column cache is cold or evicted. Cache residency must not
  // choose a persistent row-decoding plan for later warm executions.
  const auto& budget = ProcessMemoryBudget::Global();
  const auto is_star_column = [](const Expression& expr) {
    return expr && expr->Type() == TypeTag::kColumnValue &&
           expr->AsColumnValue().GetColumnName().name == "*";
  };
  if (!multi_relation && has_aggregate && !has_table_sample &&
      select->Sources().size() == 1 && !select->Sources()[0].query &&
      !select->Sources()[0].unnest && select->GroupBy().empty() &&
      !select->Having() && !select->Qualify() && !select->Distinct() &&
      !select->ForUpdate() && BytecodeEnabled() &&
      budget.OpMemoryOverrideBytes() == 0 && !budget.AlwaysSpillEnabled()) {
    std::vector<const AggregateExpression*> scalar_aggs;
    auto check_scalar_agg_expr = [&](auto&& self,
                                     const Expression& expr) -> bool {
      if (!expr) {
        return false;
      }
      switch (expr->Type()) {
        case TypeTag::kConstantValue:
          return true;
        case TypeTag::kUnaryExp:
          return self(self, expr->AsUnaryExpression().Child());
        case TypeTag::kCastExp:
          return self(self, expr->AsCastExpression().Child());
        case TypeTag::kBinaryExp:
          return self(self, expr->AsBinaryExpression().Left()) &&
                 self(self, expr->AsBinaryExpression().Right());
        case TypeTag::kAggregateExp: {
          const auto& aggregate = expr->AsAggregateExpression();
          const auto type = aggregate.GetType();
          const bool supported_type =
              type == AggregationType::kSum || type == AggregationType::kAvg ||
              type == AggregationType::kMin || type == AggregationType::kMax ||
              type == AggregationType::kCount;
          const bool valid_child = (aggregate.Child() != nullptr &&
                                    (!is_star_column(aggregate.Child()) ||
                                     type == AggregationType::kCount)) ||
                                   type == AggregationType::kCount;
          const bool single_column_count_distinct =
              aggregate.Distinct() && select->SelectList().size() == 1 &&
              select->SelectList()[0].expression.get() == expr.get() &&
              type == AggregationType::kCount && aggregate.Child() != nullptr &&
              aggregate.Child()->Type() == TypeTag::kColumnValue &&
              !is_star_column(aggregate.Child());
          if (!((!aggregate.Distinct() || single_column_count_distinct) &&  // NOLINT(readability-simplify-boolean-expr)
                !aggregate.HasWhereFilter() && !aggregate.NeedsGroupContext() &&
                supported_type && valid_child)) {
            return false;
          }
          scalar_aggs.push_back(&aggregate);
          return true;
        }
        default:
          return false;
      }
    };
    const bool all_scalar_items_ok =
        std::ranges::all_of(select->SelectList(), [&](const auto& item) {
          return check_scalar_agg_expr(check_scalar_agg_expr, item.expression);
        });
    if (all_scalar_items_ok && !scalar_aggs.empty()) {
      const auto table = ctx.GetTable(select->Sources()[0].table);
      const auto stats = ctx.GetStats(select->Sources()[0].table);
      const bool clean_tlcs =
          table.HasValue() &&
          relational_detail::ResolveCleanBackingTlcs(*table.Value(), ctx) !=
              nullptr;
      const bool allow_simple_count_star =
          !simple_count_star ||
          (clean_tlcs && table.Value()->IndexCount() == 0 &&
           table.Value()->BackingColumnarRowCount() >= 65536);
      const bool large_table =
          clean_tlcs ||
          (table.HasValue() &&
           table.Value()->BackingColumnarRowCount() >= 65536) ||
          (stats.HasValue() && stats.Value()->Rows() >= 65536);
      const bool has_computed_child = std::ranges::any_of(
          scalar_aggs, [](const AggregateExpression* aggregate) {
            return aggregate->Child() != nullptr &&
                   aggregate->Child()->Type() != TypeTag::kColumnValue;
          });
      const bool has_post_agg_wrapper =
          std::ranges::any_of(select->SelectList(), [](const auto& item) {
            return item.expression &&
                   item.expression->Type() != TypeTag::kAggregateExp;
          });
      if (allow_simple_count_star && table.HasValue() &&
          (has_computed_child || has_post_agg_wrapper || large_table)) {
        const std::string& alias = select->Sources()[0].alias;
        const Schema schema = alias.empty()
                                  ? table.Value()->GetSchema()
                                  : relational_detail::QualifySchema(
                                        table.Value()->GetSchema(), alias);
        if ((!select->WhereClause() ||
             BytecodeCompiler::Compile(select->WhereClause(), schema,
                                       BytecodeCompiler::Context::kFilter)
                 .has_value()) &&
            std::ranges::all_of(
                scalar_aggs, [&](const AggregateExpression* aggregate) {
                  if (!aggregate->Child() ||
                      is_star_column(aggregate->Child())) {
                    return true;
                  }
                  const auto program =
                      BytecodeCompiler::Compile(aggregate->Child(), schema);
                  return program.has_value() &&
                         (!select->WhereClause() ||
                          program->ResultType() == ValueType::kInt64 ||
                          large_table);
                })) {
          if (simple_count_star && clean_tlcs) {
            plan_cache_fingerprint_.clear();
            plan_cache_parameters_.clear();
          }
          return emit_relational();
        }
      }
    }
  }

  // Post-projection ORDER BY scope: the adapter normalizes computed keys
  // and SELECT aliases to projection-output names (sort_expressions) and
  // a post-plan SortExecutor covers plans that do not enforce ordering,
  // so computed ordering no longer forces the relational path.
  QueryData query;
  // FOR UPDATE reuses the DML source contract: scans reserve a write
  // intent (and a row lock) on every row they visit, and the transaction
  // holds them until commit/abort.  Positions also have to survive the
  // projection for the plan to be accepted.
  query.require_row_position_ = for_update;
  if (for_update) {
    query.wait_for_write_intent_ =
        for_update_wait_mode_ != ForUpdateWaitMode::kNoWait;
  }
  // Stamp literals in SQL-text order before the optimizer sorts
  // conjuncts. Identical values (warehouse 1 AND district 1) would
  // otherwise be numbered by plan-tree position and bind swapped.
  SqlLiteralSlots literal_slots;
  if (!plan_cache_fingerprint_.empty() && !dml_fold_applied_ &&
      !plan_cache_parameters_.empty() && select->WithQueries().empty() &&
      !select->HasDistinctOn() && select->UnionAll().empty() &&
      select->GetSetOperationTree() == nullptr) {
    literal_slots.parameters = &plan_cache_parameters_;
  }
  std::vector<Expression> slotted_select;
  slotted_select.reserve(select->SelectList().size());
  for (const NamedExpression& item : select->SelectList()) {
    slotted_select.push_back(literal_slots.Apply(item.expression));
  }
  std::vector<Expression> slotted_joins;
  slotted_joins.reserve(select->Sources().size());
  std::vector<std::optional<TableSampleSpec>> slotted_samples;
  slotted_samples.reserve(select->Sources().size());
  for (const SelectSource& source : select->Sources()) {
    if (source.query != nullptr) {
      literal_slots.ok = false;
    }
    if (source.sample) {
      TableSampleSpec sample = *source.sample;
      sample.size = literal_slots.Apply(sample.size);
      for (Expression& partition : sample.partition_by) {
        partition = literal_slots.Apply(partition);
      }
      if (sample.repeatable_seed) {
        sample.repeatable_seed = literal_slots.Apply(*sample.repeatable_seed);
      }
      slotted_samples.emplace_back(std::move(sample));
    } else {
      slotted_samples.emplace_back(std::nullopt);
    }
    slotted_joins.push_back(literal_slots.Apply(source.join_condition));
  }
  const Expression slotted_where = literal_slots.Apply(select->WhereClause());
  for (const Expression& key : select->GroupBy()) {
    (void)literal_slots.Apply(key);
  }
  (void)literal_slots.Apply(select->Having());
  const Expression slotted_qualify = literal_slots.Apply(select->Qualify());
  std::vector<Expression> slotted_order;
  slotted_order.reserve(select->OrderBy().size());
  for (const auto& term : select->OrderBy()) {
    slotted_order.push_back(literal_slots.Apply(term.expression));
  }
  const bool use_sql_slots = literal_slots.Complete();
  Expression where = use_sql_slots ? slotted_where : select->WhereClause();
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
    if (use_sql_slots && slotted_samples[i]) {
      query.samples_.emplace(source.alias, std::move(*slotted_samples[i]));
    } else if (source.sample) {
      query.samples_.emplace(source.alias, *source.sample);
    }
    const Expression join_condition =
        use_sql_slots ? slotted_joins[i] : source.join_condition;
    if (join_condition) {
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
        edge.on_condition = join_condition;
        query.outer_joins_.push_back(std::move(edge));
      } else {
        where = where ? BinaryExpressionExp(where, BinaryOperation::kAnd,
                                            join_condition)
                      : join_condition;
      }
    }
  }
  query.where_ = where ? where : ConstantValueExp(Value(true));
  query.lifted_ctes_ = lifted;
  query.qualify_ = use_sql_slots ? slotted_qualify : select->Qualify();
  query.select_.reserve(select->SelectList().size());
  for (size_t item_index = 0; item_index < select->SelectList().size();
       ++item_index) {
    const NamedExpression& item = select->SelectList()[item_index];
    NamedExpression copied = item;
    if (use_sql_slots) {
      copied.expression = slotted_select[item_index];
    }
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
      Expression star_expression = item.expression;
      if (star_expression &&
          star_expression->Type() == TypeTag::kFunctionCallExp) {
        const auto& call = star_expression->AsFunctionCallExpression();
        if (call.FuncName() == "__value_table_value" &&
            call.Args().size() == 1) {
          star_expression = call.Args()[0];
        }
      }
      if (star_expression && star_expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& column =
            star_expression->AsColumnValue().GetColumnName();
        if (column.name == "*") {
          if (item.star_modifiers && star_expression != item.expression) {
            // `SELECT AS VALUE * EXCEPT/REPLACE` would need the modifier
            // columns folded into a single value; reject instead of
            // spilling several columns into the value context.
            last_error_ =
                "SELECT * EXCEPT/REPLACE cannot be combined with "
                "SELECT AS VALUE";
            return Status::kInvalidArgument;
          }
          has_star = true;
          bool matched_relation = false;
          std::unordered_set<std::string> resolved_except;
          std::unordered_set<std::string> resolved_replace;
          for (const std::string& relation : query.from_) {
            const auto aliased = query.aliases_.find(relation);
            const std::string& physical =
                aliased == query.aliases_.end() ? relation : aliased->second;
            const auto sample_source = std::ranges::find_if(
                select->Sources(), [&](const SelectSource& source) {
                  return source.alias == relation;
                });
            const TableSampleSpec* sample_spec =
                sample_source != select->Sources().end() &&
                        sample_source->sample
                    ? &*sample_source->sample
                    : nullptr;
            if (!column.schema.empty() && column.schema != relation &&
                column.schema != physical) {
              continue;
            }
            // A derived source (subquery / UNNEST) has no catalog table and
            // no lifted leaf schema here. Keep its `rel.*` wrapped so the
            // relational interpreter can expand it from the derived
            // relation's own schema; do not reject it as an unknown source.
            const SelectSource* derived_source = nullptr;
            for (const SelectSource& src : select->Sources()) {
              if (src.alias == relation) {
                derived_source = &src;
                break;
              }
            }
            if (derived_source != nullptr &&
                (derived_source->query != nullptr ||
                 derived_source->unnest != nullptr)) {
              matched_relation = true;
              expanded.push_back(item);
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
              if (item.star_modifiers) {
                ExpandStarWithModifiers(item, relation, source_schema,
                                        &expanded, &resolved_except,
                                        &resolved_replace);
                continue;
              }
              for (size_t i = 0; i < source_schema.ColumnCount(); ++i) {
                expanded.emplace_back(ColumnName(
                    relation, source_schema.GetColumn(i).Name().name));
              }
              if (column.schema.empty() && sample_spec != nullptr &&
                  sample_spec->with_weight) {
                expanded.emplace_back(
                    sample_spec->weight_alias,
                    ColumnValueExp(sample_spec->weight_alias));
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
                physical.find("TestExtraPBValueTable") != std::string::npos) {
              if (item.star_modifiers) {
                last_error_ =
                    "SELECT * EXCEPT/REPLACE over this table is not "
                    "supported";
                return Status::kInvalidArgument;
              }
              const ColumnName base(relation,
                                    source_schema.GetColumn(0).Name().name);
              for (const char* field :
                   {"int32_val1", "int32_val2", "str_value"}) {
                expanded.emplace_back(
                    field, FunctionCallExp(
                               "__get_field_safe",
                               {ColumnValueExp(base),
                                ConstantValueExp(Value(std::string(field)))}));
              }
              if (column.schema.empty() && sample_spec != nullptr &&
                  sample_spec->with_weight) {
                expanded.emplace_back(
                    sample_spec->weight_alias,
                    ColumnValueExp(sample_spec->weight_alias));
              }
              continue;
            }
            if (item.star_modifiers) {
              ExpandStarWithModifiers(item, relation, source_schema, &expanded,
                                      &resolved_except, &resolved_replace);
            } else {
              for (size_t i = 0; i < source_schema.ColumnCount(); ++i) {
                expanded.emplace_back(ColumnName(
                    relation, source_schema.GetColumn(i).Name().name));
              }
            }
            if (column.schema.empty() && sample_spec != nullptr &&
                sample_spec->with_weight) {
              expanded.emplace_back(sample_spec->weight_alias,
                                    ColumnValueExp(sample_spec->weight_alias));
            }
          }
          // Modifier names that matched no FROM relation would silently
          // keep (REPLACE) or drop their effect (EXCEPT); reject them
          // like unknown columns.
          if (item.star_modifiers &&
              (resolved_except.size() !=
                   item.star_modifiers->except_columns.size() ||
               resolved_replace.size() !=
                   item.star_modifiers->replace_items.size())) {
            last_error_ =
                "star modifier column not present in any FROM "
                "relation";
            return Status::kInvalidArgument;
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
      if (has_table_sample) {
        result_column_names_.clear();
        result_column_types_.clear();
        result_column_names_.reserve(query.select_.size());
        for (const NamedExpression& item : query.select_) {
          result_column_names_.push_back(
              !item.name.empty()
                  ? item.name
                  : (item.expression ? item.expression->ToString() : "?"));
        }
      }
    }
  }
  // GoogleSQL: a bare integer ORDER BY item is a SELECT-list ordinal.
  // The AST visitor defers it when the select list still contained an
  // unexpanded star; the list is expanded into query.select_ by now.
  // Rewrite the statement's terms in place so every downstream consumer
  // (required ordering, post-plan sort, safety-net sort) sees the
  // resolved output column instead of a constant that sorts identically
  // on every row.
  if (!select->OrderBy().empty()) {
    auto resolved_orders = select->OrderBy();
    bool orders_changed = false;
    for (SelectStatement::OrderByTerm& order : resolved_orders) {
      const Expression& expression = order.expression;
      if (!expression || expression->Type() != TypeTag::kConstantValue ||
          dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
        continue;
      }
      const Value& ordinal_value = expression->AsConstantValue().GetValue();
      if (ordinal_value.type != ValueType::kInt64 || ordinal_value.IsNull()) {
        continue;
      }
      if (ordinal_value.value.int_value < 1) {
        // A bare constant ordinal here would survive as a no-op sort key;
        // the visitor/relational paths reject it, so do the same here.
        last_error_ = "ORDER BY ordinal out of range";
        return Status::kInvalidArgument;
      }
      const auto ordinal = static_cast<size_t>(ordinal_value.value.int_value);
      const Expression& target = ordinal <= query.select_.size()
                                     ? query.select_[ordinal - 1].expression
                                     : nullptr;
      // A star cannot expand here (single-source shapes expand above;
      // anything else stays unsupported), so leave the constant for the
      // per-term check below to report.
      if (!target || (target->Type() == TypeTag::kColumnValue &&
                      target->AsColumnValue().GetColumnName().name == "*")) {
        continue;
      }
      order.expression = target;
      orders_changed = true;
    }
    if (orders_changed) {
      select->SetOrderBy(std::move(resolved_orders));
    }
  }
  const size_t visible_columns = query.select_.size();
  std::vector<Expression> sort_expressions;
  std::vector<bool> sort_ascending;
  bool order_needs_projection_binding = false;
  sort_expressions.reserve(select->OrderBy().size());
  for (size_t i = 0; i < select->OrderBy().size(); ++i) {
    auto order = select->OrderBy()[i];
    if (use_sql_slots) {
      order.expression = slotted_order[i];
    }
    // GoogleSQL: a bare integer ORDER BY item is a SELECT-list ordinal.
    // The AST visitor defers it when the select list still contained an
    // unexpanded star; the list is expanded here, so swap the constant
    // for the ordinal's source expression and let the normal binding
    // below handle aliases and output naming.
    if (order.expression &&
        order.expression->Type() == TypeTag::kConstantValue) {
      const Value& ordinal_value =
          order.expression->AsConstantValue().GetValue();
      if (ordinal_value.type == ValueType::kInt64 && !ordinal_value.IsNull() &&
          ordinal_value.value.int_value >= 1) {
        const auto ordinal = static_cast<size_t>(ordinal_value.value.int_value);
        if (ordinal > query.select_.size() ||
            !query.select_[ordinal - 1].expression) {
          return StatusError(StatusCode::kInvalidArgument,
                             "ORDER BY ordinal out of range");
        }
        // A COLLATE clause on the ordinal applies to the SELECT-list item
        // it names, so it must survive the substitution.
        order.expression = ReapplyCollation(
            query.select_[ordinal - 1].expression, order.expression);
      }
    } else if (const std::optional<CollatedOrdinal> collated =
                   AsCollatedOrdinal(order.expression);
               collated.has_value() && collated->ordinal >= 1 &&
               static_cast<size_t>(collated->ordinal) <= query.select_.size() &&
               query.select_[static_cast<size_t>(collated->ordinal) - 1]
                   .expression) {
      // `ORDER BY 1 COLLATE 'und:ci'`: the ordinal is a constant wrapped in
      // the collator, so it never looks like an ordinal above. Resolving
      // it here keeps the clause on the SELECT-list item it names —
      // leaving the constant in place would sort on a value every row
      // shares and silently order nothing.
      order.expression = ApplyCollationSpec(
          query.select_[static_cast<size_t>(collated->ordinal) - 1].expression,
          collated->spec);
    }
    order_needs_projection_binding =
        order_needs_projection_binding ||
        order.expression->Type() != TypeTag::kColumnValue;
    query.order_expressions_.push_back(order.expression);
    query.order_ascending_.push_back(order.ascending);
    auto selected = std::ranges::find_if(query.select_, [&](const auto& item) {
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
      // Client labels were captured before physical planning. Give a
      // duplicated output name a unique internal binding so ordering the
      // second projection cannot resolve to the first one.
      if (!selected->name.empty() &&
          std::ranges::count_if(query.select_, [&](const auto& item) {
            return IdentifierEquals(item.name, selected->name);
          }) > 1) {
        selected->name = "$sort" + std::to_string(std::distance(
                                       query.select_.begin(), selected));
      }
      // Keep the optimizer's required ordering in terms of the selected
      // expression itself.  A bare ORDER BY alias is an output-schema
      // name, not an input column, and retaining it here would make the
      // lower projection attempt to resolve a column that does not exist
      // in its child schema.
      if (order.expression->Type() == TypeTag::kColumnValue &&
          order.expression->AsColumnValue().GetColumnName().schema.empty() &&
          selected->expression->ToString() != order.expression->ToString()) {
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
        sort_expressions.push_back(ColumnValueExp(ColumnName(selected->name)));
        // An earlier ORDER BY term may have introduced this hidden output.
        // Repeated computed keys must use the same binding in the memo.
        if (std::distance(query.select_.begin(), selected) >=
            static_cast<ptrdiff_t>(visible_columns)) {
          query.order_expressions_.back() = sort_expressions.back();
        }
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
            !term.expression->AsColumnValue().GetColumnName().schema.empty()) {
          return false;
        }
        return std::ranges::any_of(
            select->SelectList(), [&](const NamedExpression& item) {
              return !item.name.empty() && item.expression &&
                     item.expression->ToString() !=
                         term.expression->ToString() &&
                     IdentifierEquals(
                         item.name,
                         term.expression->AsColumnValue().GetColumnName().name);
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
  // Let the memo compare hash, sort and index-skip DISTINCT over the
  // visible projection. Hidden ORDER BY columns still use the finishing
  // path below because they are not part of the duplicate key.
  query.distinct_ =
      select->Distinct() && visible_columns == query.select_.size();
  // DISTINCT must see every row before truncation, so the optimizer is
  // not told about LIMIT here; Distinct -> Sort -> Limit stays in this
  // order above an untruncated plan (D6 soundness).
  // The alias-ordered shape needs the identical treatment for the identical
  // reason: order_by_output_alias above drops the optimizer's ordering and
  // leaves the post-plan SortExecutor to order the projection output (see
  // LowerSelectPlanExecutor). A LIMIT the optimizer can still see then
  // folds a bare kLimit directly onto the scan (`limit` implementation
  // rule), truncating the input before the sort ever reads it: `SELECT
  // x+1 AS y, z FROM t ORDER BY y LIMIT 3` answered from the first three
  // rows of t instead of its smallest three (SortExecutor only ordered the
  // already-truncated rows). Hiding the LIMIT keeps the tail's Sort ->
  // Limit order sound.
  const bool hide_limit_from_optimizer =
      select->Distinct() || order_by_output_alias;
  query.limit_count_ = hide_limit_from_optimizer ? 0 : select->Limit();
  query.limit_offset_ = hide_limit_from_optimizer ? 0 : select->Offset();
  Status rewrite_status = query.Rewrite(ctx);
  if (rewrite_status == Status::kAmbiguousQuery) {
    last_error_ = query.ambiguous_column_.empty()
                      ? "ambiguous column reference"
                      : "ambiguous column " + query.ambiguous_column_;
    return rewrite_status;
  }
  RETURN_IF_FAIL(rewrite_status);
  // Sampled before optimizing as well as before storing: a clustered index
  // order is a revocable proof, so a plan that may have elided a sort (or
  // folded a LIMIT / an incremental sort) on it must not outlive the
  // statement in the compiled-plan cache.
  const bool clustered_order_before = AnyResolvedTableHasClusteredOrder(ctx);
  StatusOr<Plan> plan_or = Optimizer::Optimize(query, ctx);
  if (!plan_or.HasValue()) {
    return emit_relational();
  }
  Plan plan = std::move(plan_or.Value());
  const bool needs_distinct =
      select->Distinct() &&
      !ProjectionContainsUniqueKey(plan, query.select_, visible_columns) &&
      !plan->EnforcesDistinct();
  Executor executor;
  bool skip_scan = false;
  if (needs_distinct) {
    Plan scan_plan = plan;
    std::vector<NamedExpression> distinct_items;
    while (true) {
      const auto* projection = dynamic_cast<const ProjectionPlan*>(&*scan_plan);
      if (projection == nullptr) {
        break;
      }
      distinct_items = projection->Columns();
      scan_plan = projection->GetSource();
    }
    if (distinct_items.empty()) {
      distinct_items = select->SelectList();
    }
    const auto* index_only =
        dynamic_cast<const IndexOnlyScanPlan*>(&*scan_plan);
    const Table* scan_table =
        index_only != nullptr ? index_only->ScanSource() : nullptr;
    if (index_only != nullptr && scan_table != nullptr &&
        !index_only->GetIndex().RetainsDeletedEntries() &&
        !ctx.txn_.IndexKeysMayBeStale(index_only->GetIndex().Root()) &&
        index_only->GetIndex().sc_.key_.size() == 1 &&
        index_only->BeginKey().empty() && index_only->EndKey().empty() &&
        distinct_items.size() == 1 && distinct_items[0].expression &&
        distinct_items[0].expression->Type() == TypeTag::kColumnValue) {
      const ColumnName distinct_column =
          distinct_items[0].expression->AsColumnValue().GetColumnName();
      const int offset = scan_table->GetSchema().Offset(distinct_column);
      if (offset >= 0 && index_only->GetIndex().sc_.key_.front() ==
                             static_cast<slot_t>(offset)) {
        std::vector<NamedExpression> select_items = index_only->SelectItems();
        if (select_items.empty()) {
          select_items.emplace_back(distinct_column.name,
                                    ColumnValueExp(distinct_column));
        }
        Executor skip_scan_executor = std::make_shared<SkipScanDistinct>(
            ctx.txn_, *scan_table, index_only->GetIndex(), std::vector<Value>{},
            std::vector<Value>{}, index_only->IsAscending(),
            ConstantValueExp(Value(true)), scan_table->GetSchema(), 0);
        executor = std::make_shared<Projection>(std::move(select_items),
                                                scan_table->GetSchema(),
                                                std::move(skip_scan_executor));
        skip_scan = true;
      }
    }
  }
  const std::vector<Expression>& ordering_keys = order_needs_projection_binding
                                                     ? sort_expressions
                                                     : query.order_expressions_;
  std::vector<OocSortSink::KeySpec> post_sort_keys;
  post_sort_keys.reserve(select->OrderBy().size());
  for (size_t i = 0; i < select->OrderBy().size(); ++i) {
    post_sort_keys.push_back(OocSortSink::KeySpec{
        .expression = sort_expressions[i],
        .ascending = static_cast<bool>(sort_ascending[i]),
        .nulls_first = select->OrderBy()[i].nulls_first,
        .collation = CollationOfExpression(sort_expressions[i]),
    });
  }
  if (skip_scan) {
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
  } else {
    StatusOr<Executor> pipe_or = LowerSelectPlanExecutor(
        plan, ctx, needs_distinct, ordering_keys, query.order_ascending_,
        query.order_nulls_first_, post_sort_keys, select->HasLimit(),
        select->Limit(), select->Offset(), select->WithTies(), visible_columns,
        query.select_.size());
    RETURN_IF_FAIL(pipe_or.GetStatus());
    executor = pipe_or.MoveValue();
  }
  result_column_types_ = SchemaColumnTypes(plan->GetSchema());
  if (result_column_types_.size() > visible_columns) {
    result_column_types_.resize(visible_columns);
  }
  if (result_column_types_.size() == result_column_names_.size()) {
    for (size_t column = 0; column < result_column_types_.size(); ++column) {
      if (result_column_types_[column] == ValueType::kNull) {
        // Keep the declaration even if the optimizer folded CAST(NULL)
        // or a failing SAFE_CAST to an untyped NULL.
        result_column_types_[column] = declared_cast_type(column);
      }
    }
  }
  // Compiled-plan fill: capture the compiled plan plus the executor-shape
  // metadata so identical-parameter repeats skip parse/bind/rewrite/
  // optimize entirely. Only optimizer-route statements reach this point.
  if (!plan_cache_fingerprint_.empty() && !multi_relation &&
      !clustered_order_before && !AnyResolvedTableHasClusteredOrder(ctx)) {
    auto shape = std::make_shared<CompiledPlan::SelectShape>();
    shape->column_names = result_column_names_;
    shape->column_types = result_column_types_;
    // The ordering must be captured from the values the executor tail was
    // actually built from (`ordering_keys`, the same reference
    // LowerSelectPlanExecutor received above), not from the optimizer's
    // logical keys. For `ORDER BY <SELECT-list alias>` those were cleared
    // because the sort is delegated to the post-plan SortExecutor over the
    // projection output; an empty key vector makes the replay's
    // `need_sort = !order_expressions.empty() && ...` false, so the cached
    // plan silently answered in scan order. The projection-bound keys plus
    // their directions and NULLS placement reproduce the first execution.
    shape->order_expressions = ordering_keys;
    if (order_by_output_alias) {
      shape->order_ascending = sort_ascending;
      shape->order_nulls_first.clear();
      shape->order_nulls_first.reserve(select->OrderBy().size());
      for (const SelectStatement::OrderByTerm& term : select->OrderBy()) {
        shape->order_nulls_first.push_back(term.nulls_first);
      }
    } else {
      shape->order_ascending = query.order_ascending_;
      shape->order_nulls_first = query.order_nulls_first_;
    }
    shape->sort_keys.reserve(sort_expressions.size());
    for (size_t i = 0; i < sort_expressions.size(); ++i) {
      shape->sort_keys.emplace_back(sort_expressions[i],
                                    static_cast<bool>(sort_ascending[i]));
    }
    shape->distinct = needs_distinct;
    shape->has_limit = select->HasLimit();
    shape->with_ties = select->WithTies();
    shape->limit = select->Limit();
    shape->offset = select->Offset();
    shape->visible_columns = visible_columns;
    shape->final_select_size = query.select_.size();
    auto compiled = std::make_shared<CompiledPlan>();
    compiled->kind = CompiledPlan::Kind::kSelect;
    compiled->database = database_;
    compiled->epoch = database_->SchemaEpoch();
    compiled->parameters = plan_cache_parameters_;
    if (!dml_fold_applied_) {
      compiled->parameter_slots =
          InstallPlanSlots(plan, plan_cache_parameters_);
    }
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
    // SQL requests can land on different workers. Immutable bounded
    // column reads do not need eight independent cold optimizations.
    // Bound slots stay thread-local; LIMIT/OFFSET literals remain in the
    // fingerprint, and any expression parameters must match exactly.
    const auto bare_column = [](const Expression& expression) {
      return expression && expression->Type() == TypeTag::kColumnValue;
    };
    if (!dml_fold_applied_ && !query.require_row_position_ &&
        select->Sources().size() == 1 && plan->ScanSource() != nullptr &&
        plan->ScanSource()->GetSchema().Name() == select->Sources()[0].table &&
        !select->Sources()[0].query && !select->Sources()[0].unnest &&
        !select->Sources()[0].sample && !select->Sources()[0].join_condition &&
        !select->AsStruct() && !select->Distinct() &&
        !select->HasDistinctOn() && !select->WithTies() &&
        !select->ForUpdate() && select->WithQueries().empty() &&
        select->GroupBy().empty() && !select->Having() && !select->Qualify() &&
        std::ranges::all_of(select->SelectList(),
                            [&](const NamedExpression& item) {
                              return bare_column(item.expression);
                            }) &&
        compiled->parameter_slots.empty() && !plan->ContainsParameterSlots() &&
        select->HasLimit() && !query.order_expressions_.empty() &&
        query.grouping_keys_.empty() &&
        (!query.where_ ||
         (query.where_->Type() == TypeTag::kConstantValue &&
          query.where_->AsConstantValue().GetValue().Truthy())) &&
        std::ranges::all_of(query.order_expressions_, bare_column) &&
        std::ranges::all_of(query.select_, [&](const NamedExpression& item) {
          return bare_column(item.expression);
        })) {
      compiled->immutable_exact_read = true;
      StoreCompiledPlan(plan_cache_fingerprint_, compiled);
    }
    // Literal-free scalar aggregates have no binding state either. Each
    // replay still emits fresh executors and reads the current snapshot;
    // only the immutable plan is shared, never an aggregate result.
    const auto scalar_read_expression =
        [&](auto&& self, const Expression& expression) -> bool {
      if (!expression) {
        return true;
      }
      switch (expression->Type()) {
        case TypeTag::kColumnValue:
        case TypeTag::kConstantValue:
        case TypeTag::kBinaryExp:
        case TypeTag::kUnaryExp:
          break;
        case TypeTag::kAggregateExp: {
          const auto& aggregate = expression->AsAggregateExpression();
          if (aggregate.HasWhereFilter() || aggregate.NeedsGroupContext() ||
              aggregate.HasInnerGroupBy()) {
            return false;
          }
          const auto kind = aggregate.GetType();
          if (kind != AggregationType::kCount &&
              kind != AggregationType::kSum && kind != AggregationType::kMin &&
              kind != AggregationType::kMax && kind != AggregationType::kAvg) {
            return false;
          }
          break;
        }
        default:
          return false;
      }
      return std::ranges::all_of(
          ExpressionChildren(expression),
          [&](const Expression& child) { return self(self, child); });
    };
    if (!compiled->immutable_exact_read && has_aggregate &&
        !dml_fold_applied_ && !query.require_row_position_ &&
        compiled->parameters.empty() && compiled->parameter_slots.empty() &&
        !plan->ContainsParameterSlots() && select->Sources().size() == 1 &&
        plan->ScanSource() != nullptr &&
        plan->ScanSource()->GetSchema().Name() == select->Sources()[0].table &&
        !select->Sources()[0].query && !select->Sources()[0].unnest &&
        !select->Sources()[0].sample && !select->Sources()[0].join_condition &&
        !select->AsStruct() && !select->Distinct() &&
        !select->HasDistinctOn() && !select->WithTies() &&
        !select->ForUpdate() && select->WithQueries().empty() &&
        select->GroupBy().empty() && !select->Having() && !select->Qualify() &&
        !select->HasLimit() && select->Offset() == 0 &&
        query.order_expressions_.empty() &&
        (!query.where_ ||
         (query.where_->Type() == TypeTag::kConstantValue &&
          query.where_->AsConstantValue().GetValue().Truthy())) &&
        std::ranges::all_of(select->SelectList(),
                            [&](const NamedExpression& item) {
                              return scalar_read_expression(
                                  scalar_read_expression, item.expression);
                            })) {
      compiled->immutable_exact_read = true;
      StoreCompiledPlan(plan_cache_fingerprint_, compiled);
    }
    StoreThreadCompiledPlan(plan_cache_fingerprint_, std::move(compiled));
  }
  return WithPlanExplain(std::move(executor), std::move(plan));
}

std::optional<Executor> SqlEngine::ServeFromPlanCache(
    TransactionContext& ctx, const std::string& fingerprint,
    const std::vector<Value>& parameters, bool require_immutable_exact_read) {
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
  if (require_immutable_exact_read && !compiled->immutable_exact_read) {
    return std::nullopt;
  }
  if (compiled->retired.load(std::memory_order_relaxed)) {
    ForgetThreadCompiledPlan(fingerprint);
    return std::nullopt;
  }
  if (CompliancePrimaryKeyMode() &&
      (compiled->kind == CompiledPlan::Kind::kInsert ||
       compiled->kind == CompiledPlan::Kind::kUpdate)) {
    return std::nullopt;
  }
  // #1100: identity check against the per-database incarnation map — a
  // catalog GetTable per retained table per cache hit (LSM read + full
  // Table deserialization) used to undo the point of the plan cache.
  const auto same_incarnation = [&](const Table& expected) {
    return database_ != nullptr &&
           database_->IsCurrentIncarnation(expected.GetSchema().Name(),
                                           expected.IncarnationState());
  };
  bool valid = true;
  for (const auto& table : compiled->retained_tables) {
    if (table == nullptr || !same_incarnation(*table)) {
      valid = false;
      break;
    }
  }
  if (compiled->table != nullptr && !same_incarnation(*compiled->table)) {
    valid = false;
  }
  if (compiled->insert_shape != nullptr &&
      compiled->insert_shape->table != nullptr &&
      !same_incarnation(*compiled->insert_shape->table)) {
    valid = false;
  }
  if (!valid) {
    PlanCacheStats().epoch_invalidations.fetch_add(1,
                                                   std::memory_order_relaxed);
    ForgetThreadCompiledPlan(fingerprint);
    return std::nullopt;
  }
  RememberThreadCompiledPlan(fingerprint, compiled);
  if (!compiled->parameter_slots.empty()) {
    for (const Expression& expression : compiled->parameter_slots) {
      auto* slot = dynamic_cast<ParameterSlot*>(expression.get());
      if (slot == nullptr || slot->Index() >= parameters.size() ||
          slot->ExpectedType() != parameters[slot->Index()].type) {
        PlanCacheStats().parameter_mismatches.fetch_add(
            1, std::memory_order_relaxed);
        return std::nullopt;
      }
      slot->Bind(parameters[slot->Index()]);
    }
  } else if (compiled->kind != CompiledPlan::Kind::kInsert &&
             compiled->parameters != parameters) {
    // The shape could not keep its slots. Recompile and replace the entry;
    // do not retire the fingerprint after a few mismatches.
    PlanCacheStats().parameter_mismatches.fetch_add(1,
                                                    std::memory_order_relaxed);
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
        result_column_types_ = compiled->select_shape->column_types;
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        Executor timed = std::make_shared<TimedSelectReuse>(
            std::move(*served), fingerprint, compiled);
        return RetainCompiledPlan(std::move(timed), compiled);
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
        StatusOr<std::vector<ForeignKeyCheck>> fk_checks =
            ResolveForeignKeyChecks(ctx, *compiled->table);
        StatusOr<std::vector<ReferencedByCheck>> ref_checks =
            ResolveReferencedByChecks(database_, ctx, *compiled->table);
        if (!fk_checks.HasValue() || !ref_checks.HasValue()) {
          break;
        }
        Executor executor = std::make_shared<Update>(
            ctx.txn_, compiled->table.get(), compiled->plan->EmitExecutor(ctx),
            /*assert_rows_modified=*/-1, /*enforce_primary_key=*/false,
            fk_checks.MoveValue(), ref_checks.MoveValue());
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(executor), compiled);
      }
      case CompiledPlan::Kind::kDelete: {
        last_statement_type_ = StatementType::kDelete;
        last_dml_table_ = compiled->table->GetSchema().Name();
        StatusOr<std::vector<ReferencedByCheck>> ref_checks =
            ResolveReferencedByChecks(database_, ctx, *compiled->table);
        if (!ref_checks.HasValue()) {
          break;
        }
        Executor executor = std::make_shared<DeleteExecutor>(
            ctx.txn_, *compiled->table, compiled->plan->EmitExecutor(ctx),
            /*assert_rows_modified=*/-1, ref_checks.MoveValue());
        PlanCacheStats().replays.fetch_add(1, std::memory_order_relaxed);
        return RetainCompiledPlan(std::move(executor), compiled);
      }
    }
  } catch (const std::exception&) {
    // Any failure inside the cached-plan fast path (schema drift, dropped
    // table, ...) degrades to planning the statement from scratch.
    return std::nullopt;
  }
  return std::nullopt;
}

}  // namespace tinylamb
