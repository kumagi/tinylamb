/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/sql_engine.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "common/constants.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/delete.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/projection.hpp"
#include "executor/relational.hpp"
#include "executor/sort.hpp"
#include "executor/update.hpp"
#include "expression/named_expression.hpp"
#include "expression/expression.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/plan_cache.hpp"
#include "query/query_data.hpp"
#include "query/sql_template.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
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
    if (!prepared.HasValue()) { return prepared.GetStatus(); }
    return QueryResult(std::move(prepared.Value()), last_statement_type_,
                       result_column_names_);
  } catch (const std::exception& e) {
    last_error_ = e.what();
    return Status::kUnknown;
  }
}

uint64_t SqlEngine::ThreadExecutionCount() {
  return tls_sql_execution_count;
}

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

// INSERT accepts a single STRUCT (or NULL) expression for a multi-column
// table: struct fields expand positionally across the columns and a whole-row
// NULL becomes a fully-NULL row. Struct values are stored as JSON text, so
// expansion parses the top-level object members. Returns false when `single`
// is not an expandable shape.
bool ExpandStructInsertValue(const Value& single, size_t width,
                             std::vector<Value>* out) {
  if (width <= 1) { return false; }
  if (single.IsNull()) {
    *out = std::vector<Value>(width);
    return true;
  }
  const auto json_value_to_value = [](const std::string& token,
                                      Value* parsed) -> bool {
    const std::string trimmed = [&] {
      size_t b = token.find_first_not_of(" \t\r\n");
      if (b == std::string::npos) { return std::string(); }
      size_t e = token.find_last_not_of(" \t\r\n");
      return token.substr(b, e - b + 1);
    }();
    if (trimmed == "null") { *parsed = Value(); return true; }
    if (trimmed == "true") { *parsed = Value(int64_t{1}); return true; }
    if (trimmed == "false") { *parsed = Value(int64_t{0}); return true; }
    if (trimmed.size() >= 2 && trimmed.front() == '"' &&
        trimmed.back() == '"') {
      std::string unescaped;
      unescaped.reserve(trimmed.size());
      for (size_t i = 1; i + 1 < trimmed.size(); ++i) {
        if (trimmed[i] == '\\' && i + 2 < trimmed.size()) {
          ++i;
          switch (trimmed[i]) {
            case 'n': unescaped.push_back('\n'); break;
            case 't': unescaped.push_back('\t'); break;
            case 'r': unescaped.push_back('\r'); break;
            default: unescaped.push_back(trimmed[i]); break;
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
      } catch (const std::exception&) {
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
    if (!current.empty()) { parts.push_back(std::move(current)); }
    return parts;
  };

  if (single.type != ValueType::kVarChar) { return false; }
  const std::string text(single.value.varchar_value);
  if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
    return false;
  }
  const std::vector<std::string> members =
      split_top_level(text.substr(1, text.size() - 2));
  if (members.size() != width) { return false; }
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
        if (c == '\\') { ++i; }
        else if (c == quote) { in_str = false; }
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
    std::string value_text = colon == std::string::npos ? member
                                                        : member.substr(colon + 1);
    if (colon == std::string::npos) {
      // Anonymous positional shape: the member itself is the value.
      value_text = member;
    }
    Value parsed;
    if (!json_value_to_value(value_text, &parsed)) { return false; }
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
    if (input->size() < keyword.size()) { return false;
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
  if (!consume(&remainder, "EXPLAIN")) { return std::nullopt;
}
  const bool analyze = consume(&remainder, "ANALYZE");
  remainder = trim(remainder);
  if (remainder.empty()) { return ExplainRequest{.analyze=analyze, .query={}};
}
  return ExplainRequest{.analyze=analyze, .query=remainder};
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
    if (end == std::string_view::npos) { break;
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
  if (statement.Type() != StatementType::kSelect) { return false;
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
  if (cached == shard.cache.end()) { return nullptr;
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
    if (input->size() < keyword.size()) { return false;
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
  if (!consume(&remainder, "ANALYZE")) { return std::nullopt;
}
  remainder = trim(remainder);
  if (!remainder.empty() && remainder.back() == ';') {
    remainder.remove_suffix(1);
    remainder = trim(remainder);
  }
  std::ignore = consume(&remainder, "TABLE");
  remainder = trim(remainder);

  AnalyzeRequest request;
  if (remainder.empty()) { return request;
}

  while (!remainder.empty()) {
    remainder = trim(remainder);
    if (remainder.empty()) { break;
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
    if (remainder.empty()) { break;
}
    if (remainder.front() != ',') { return std::nullopt;
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
  if (shape.distinct) {
    executor = std::make_shared<DistinctExecutor>(std::move(executor));
  }
  if (!shape.order_expressions.empty() &&
      !compiled.plan->IsOrderedBy(shape.order_expressions,
                                  shape.order_ascending)) {
    std::vector<SortExecutor::Key> keys;
    keys.reserve(shape.sort_keys.size());
    for (const auto& key : shape.sort_keys) {
      keys.push_back({key.first, key.second});
    }
    executor = std::make_shared<SortExecutor>(std::move(executor),
                                              compiled.plan->GetSchema(),
                                              std::move(keys));
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
      evaluated.push_back(CloneWithPreparedValues(cell, values)
                              ->Evaluate(Row(), Schema()));
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
      const ValueType expected = shape.schema.GetColumn(i).Type();
      if (evaluated[i].IsNull() || evaluated[i].type == expected) {
        continue;
      }
      if (expected == ValueType::kDouble &&
          evaluated[i].type == ValueType::kInt64) {
        evaluated[i] =
            Value(static_cast<double>(evaluated[i].value.int_value));
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
                             std::shared_ptr<Table> table) {
  if (fingerprint.empty() || IsVolatileSpecializedPlan(fingerprint)) {
    return;
  }
  auto compiled = std::make_shared<CompiledPlan>();
  compiled->kind = kind;
  compiled->epoch = epoch;
  compiled->parameters = std::move(parameters);
  compiled->plan = std::move(plan);
  compiled->table = std::move(table);
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
      StatusOr<Executor> executed =
          ExecuteAnalyze(*database_, ctx, *analyze);
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
  // Phase 2-1: consult the compiled-plan cache before any parse/bind/plan
  // work. Only non-EXPLAIN, templatable statements participate.
  if (!explain && templated.templatable) {
    if (std::optional<Executor> served = ServeFromPlanCache(
            ctx, templated.fingerprint, templated.parameters)) {
      return std::move(*served);
    }
  }
  if (templated.templatable) {
    if (const std::shared_ptr<Statement> cached =
            FindTemplate(templated.fingerprint)) {
      try {
        statement = BindStatementLiterals(*cached, templated.parameters);
        cache_hit = true;
      } catch (const std::exception&) {
        // Literal shape drifted from the cached tree; parse the original SQL.
        statement = nullptr;
      }
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
      statement = GoogleSqlAstVisitor::Visit(*ast);
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
    const auto planning_start = std::chrono::steady_clock::now();
    StatusOr<Executor> prepared = PrepareStatement(ctx, std::move(statement));
    const auto planning_end = std::chrono::steady_clock::now();
    if (!prepared.HasValue()) { return prepared.GetStatus();
}

    uint64_t rows = 0;
    std::chrono::steady_clock::time_point execution_end = planning_end;
    if (explain->analyze) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) { ++rows;
}
      execution_end = std::chrono::steady_clock::now();
    }
    std::ostringstream output;
    prepared.Value()->Explain(output, 0);
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
    }
    result_column_names_ = {"QUERY PLAN"};
    return Executor(
        std::make_shared<ConstantExecutor>(ExplainRows(output.str())));
  }

  if (templated.templatable && !cache_hit && !IsExplicitZeroLimit(*statement)) {
    try {
      RememberTemplate(
          templated.fingerprint,
          BindStatementLiterals(*statement, templated.parameters));
    } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch) // Template caching is best-effort; any bind failure just means the statement is parsed verbatim next time.
    }
  }
  // Arm the compiled-plan fill sites inside PrepareStatement; the guard
  // disarms them on every exit (including EXPLAIN, which never sets one).
  set_plan_cache_candidate(templated.fingerprint, templated.parameters);
  PlanCacheCandidateGuard candidate_guard{this};
  return PrepareStatement(ctx, std::move(statement));
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
    const NestedDmlItem* item;
  };
  std::vector<ResolvedItem> resolved;
  resolved.reserve(update.NestedItems().size());
  for (const NestedDmlItem& item : update.NestedItems()) {
    const ColumnName name(item.target_path);
    if (!name.schema.empty() || item.target_path.find('.') != std::string::npos) {
      throw std::runtime_error(
          "nested DML on a non-array field is not supported: " +
          item.target_path);
    }
    const int offset = schema.Offset(name);
    if (offset < 0) {
      throw std::runtime_error("nested DML target not found: " +
                               item.target_path);
    }
    resolved.push_back(ResolvedItem{static_cast<size_t>(offset), name.name,
                                    &item});
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
      const Column element_column(
          ColumnName("", entry.element_name),
          cell.IsNull() ? ValueType::kNull
                        : cell.IsArray() ? ValueType::kInt64 : cell.type);
      Schema element_schema("", {element_column});
      relational_detail::Scope outer_scope{.row = &new_row,
                                           .schema = &schema,
                                           .outer = nullptr};
      auto eval_on_element =
          [&](const Expression& expr, const Value& element) -> Value {
        if (!expr) { return Value(true); }
        Row element_row({element});
        relational_detail::Scope element_scope{.row = &element_row,
                                               .schema = &element_schema,
                                               .outer = &outer_scope};
        return relational_detail::Evaluate(expr, element_scope, nullptr, ctx,
                                           relational_detail::CteMap{});
      };
      ArrayEditState& state = edit_state[entry.offset];
      auto load_working = [&]() {
        if (!state.loaded) {
          state.working = cell.ArrayElements();
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          state.loaded = true;
        }
      };
      switch (item.kind) {
        case NestedDmlItem::Kind::kDelete: {
          if (cell.IsNull()) {
            throw std::runtime_error(
                "Cannot execute a nested DELETE statement on a NULL array "
                "value");
          }
          if (!cell.IsArray()) {
            throw std::runtime_error("nested DELETE requires an ARRAY value "
                                     "in column " +
                                     item.target_path);
          }
          load_working();
          std::vector<Value> kept;
          kept.reserve(state.working.size());
          int64_t touched = 0;
          for (Value& element : state.working) {
            const Value matches = eval_on_element(item.predicate, element);
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
            throw std::runtime_error(message.str());
          }
          state.working = std::move(kept);
          state.update_baseline = state.working;
          state.update_touched.assign(state.working.size(), false);
          cell = Value::Array(state.working, cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kUpdate: {
          if (cell.IsNull()) {
            throw std::runtime_error(
                "Cannot execute a nested UPDATE statement on a NULL array "
                "value");
          }
          if (!cell.IsArray()) {
            throw std::runtime_error("nested UPDATE requires an ARRAY value "
                                     "in column " +
                                     item.target_path);
          }
          load_working();
          int64_t touched = 0;
          for (size_t i = 0; i < state.working.size(); ++i) {
            const Value matches =
                eval_on_element(item.predicate, state.update_baseline[i]);
            if (!matches.IsNull() && relational_detail::Truthy(matches)) {
              if (i < state.update_touched.size() &&
                  state.update_touched[i]) {
                throw std::runtime_error(
                    "Attempted to modify an array element with multiple "
                    "nested UPDATE statements");
              }
              ++touched;
              if (i < state.update_touched.size()) {
                state.update_touched[i] = true;
              }
              state.working[i] =
                  eval_on_element(item.set_value, state.update_baseline[i]);
            }
          }
          if (item.assert_rows_modified >= 0 &&
              touched != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << touched;
            throw std::runtime_error(message.str());
          }
          cell = Value::Array(state.working, cell.ArrayElementSqlType());
          break;
        }
        case NestedDmlItem::Kind::kInsert: {
          if (cell.IsNull()) {
            throw std::runtime_error(
                "Cannot execute a nested INSERT statement on a NULL array "
                "value");
          }
          if (!cell.IsArray()) {
            throw std::runtime_error("nested INSERT requires an ARRAY value "
                                     "in column " +
                                     item.target_path);
          }
          load_working();
          int64_t inserted = 0;
          if (item.insert_query != nullptr) {
            relational_detail::Relation produced =
                relational_detail::ExecuteQuery(ctx, *item.insert_query,
                                                nullptr, {});
            produced.ForEachRow([&](const Row& row) {
              if (!row.values_.empty()) {
                state.working.push_back(row.values_.front());
                ++inserted;
              }
            });
          } else {
            for (const auto& values : item.insert_values) {
              if (values.empty()) { continue;
}
              state.working.push_back(relational_detail::Evaluate(
                  values.front(), outer_scope, nullptr, ctx,
                  relational_detail::CteMap{}));
              ++inserted;
            }
          }
          if (item.assert_rows_modified >= 0 &&
              inserted != item.assert_rows_modified) {
            std::ostringstream message;
            message << "ASSERT_ROWS_MODIFIED expected "
                    << item.assert_rows_modified
                    << " array elements modified, but found " << inserted;
            throw std::runtime_error(message.str());
          }
          state.update_baseline = state.working;
          cell = Value::Array(state.working, cell.ArrayElementSqlType());
          break;
        }
      }
    }
    StatusOr<RowPosition> updated = table->Update(ctx.txn_, position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      throw std::runtime_error("update failed on table " +
                               std::string(schema.Name()));
    }
    ++modified_rows;
  }
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    throw std::runtime_error("ASSERT_ROWS_MODIFIED was specified with " +
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
      case '"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      default: escaped.push_back(c);
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
  if (b == std::string::npos) { return Value(); }
  size_t e = trimmed.find_last_not_of(" \t\r\n");
  trimmed = trimmed.substr(b, e - b + 1);
  if (trimmed.size() >= 4 && trimmed.front() == '"' &&
      trimmed.back() == '"') {
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
    if (current.size() < 2 || current.front() != '{' ||
        current.back() != '}') {
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
    if (decoded.IsNull()) { return std::nullopt; }
    current = decoded.type == ValueType::kVarChar
                  ? std::string(decoded.value.varchar_value)
                  : std::string("null");
  }
  return ordinals;
}

// Rewrites `json` by replacing the member addressed by `ordinals` (one index
// per path segment). Missing positions leave the document untouched.
std::optional<std::string> SetStructPathByOrdinals(
    const std::string& json, const std::vector<std::string>& segs,
    const std::vector<int>& ordinals, size_t depth, const Value& new_value) {
  if (json.size() < 2 || json.front() != '{' || json.back() != '}') {
    // Assigning through a NULL / non-object intermediate.
    throw std::runtime_error("Cannot set field of NULL STRUCT");
  }
  const auto members =
      SplitJsonObjectMembers(json.substr(1, json.size() - 2));
  const int index = ordinals[depth];
  if (index < 0 || static_cast<size_t>(index) >= members.size()) {
    return std::nullopt;
  }
  std::string rebuilt = "{";
  bool first = true;
  for (size_t i = 0; i < members.size(); ++i) {
    if (!first) { rebuilt += ","; }
    first = false;
    rebuilt += "\"";
    rebuilt += EscapeStructKey(members[i].first);
    rebuilt += "\":";
    if (static_cast<int>(i) == index) {
      if (depth + 1 == segs.size()) {
        rebuilt += EncodeStructMemberJson(new_value);
      } else {
        Value nested_value = DecodeStructMember(members[i].second);
        auto nested = SetStructPathByOrdinals(
            nested_value.IsNull()
                ? std::string("null")
                : nested_value.type == ValueType::kVarChar
                      ? std::string(nested_value.value.varchar_value)
                      : std::string("null"),
            segs, ordinals, depth + 1, new_value);
        rebuilt += nested.has_value() ? *nested : members[i].second;
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
        field_targets.push_back(FieldTarget{static_cast<size_t>(base),
                                            std::move(segments), &expression,
                                            {}});
        continue;
      }
    }
    int offset = -1;
    if (!target.name.empty()) { offset = column_index(target.name); }
    if (offset < 0) {
      throw std::runtime_error("UPDATE SET target not found: " +
                               target.ToString());
    }
    plain_targets.emplace_back(
        ColumnName(update.TableName(),
                   schema.GetColumn(static_cast<size_t>(offset)).Name().name),
        &expression);
  }

  QueryData query;
  query.from_ = {update.TableName()};
  // Dotted references inside the predicate resolve as STRUCT field access
  // against the column named by the first path component.
  Expression where_clause = update.WhereClause();
  if (where_clause) {
    std::function<Expression(const Expression&)> bind_struct_fields =
        [&](const Expression& expr) -> Expression {
      if (!expr) { return expr; }
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
                     schema.GetColumn(static_cast<size_t>(base))
                         .Name().name)),
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
      return changed ? WithExpressionChildren(expr, std::move(children))
                     : expr;
    };
    where_clause = bind_struct_fields(where_clause);
  }
  query.where_ = where_clause ? where_clause : ConstantValueExp(Value(true));
  query.select_.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    query.select_.emplace_back(schema.GetColumn(i).Name().name,
                               ColumnValueExp(schema.GetColumn(i).Name()));
  }
  query.require_row_position_ = true;
  RETURN_IF_FAIL(query.Rewrite(ctx));
  ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
  Executor source = plan->EmitExecutor(ctx);

  std::vector<std::pair<Row, RowPosition>> pending;
  Row row;
  RowPosition position;
  while (source->Next(&row, &position)) {
    pending.emplace_back(std::move(row), position);
    row = Row();
  }

  // Resolve each path segment's positional index from any matched row that
  // carries the segment names explicitly (anonymous tuples store fN keys).
  for (auto& target : field_targets) {
    target.ordinals.assign(target.segments.size(), -1);
    for (const auto& [candidate, candidate_position] : pending) {
      const Value& cell = candidate.values_[target.offset];
      if (cell.IsNull() || cell.type != ValueType::kVarChar) { continue; }
      auto found = FindStructPathOrdinals(
          std::string(cell.value.varchar_value), target.segments);
      if (found.has_value()) {
        target.ordinals = *found;
        break;
      }
    }
  }

  int64_t modified_rows = 0;
  for (auto& [new_row, row_position] : pending) {
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
          throw std::runtime_error("Cannot set field of NULL `" + proto_type +
                                   "`");
        }
        relational_detail::Scope row_scope{.row = &new_row,
                                           .schema = &schema,
                                           .outer = nullptr};
        const Value assigned =
            relational_detail::Evaluate(*target.value, row_scope, nullptr, ctx,
                                        relational_detail::CteMap{});
        ValidateEnumFieldValue(
            proto_type, target.segments.empty() ? std::string()
                                                : target.segments.back(),
            assigned);
        auto rewritten =
            ProtoTextSetField(std::string(payload), target.segments, assigned,
                              proto_type);
        if (rewritten.has_value()) {
          cell = Value(std::move(*rewritten));
        }
        continue;
      }
      if (cell.IsNull()) {
        throw std::runtime_error("Cannot set field of NULL STRUCT");
      }
      const std::string text(cell.value.varchar_value);
      relational_detail::Scope row_scope{.row = &new_row,
                                         .schema = &schema,
                                         .outer = nullptr};
      const Value assigned =
          relational_detail::Evaluate(*target.value, row_scope, nullptr, ctx,
                                      relational_detail::CteMap{});
      auto rewritten = SetStructPathByOrdinals(text, target.segments,
                                               target.ordinals, 0, assigned);
      if (rewritten.has_value()) {
        cell = Value(std::move(*rewritten));
      }
    }
    StatusOr<RowPosition> updated =
        table->Update(ctx.txn_, row_position, new_row);
    if (updated.GetStatus() != Status::kSuccess) {
      throw std::runtime_error("update failed on table " +
                               std::string(schema.Name()));
    }
    ++modified_rows;
  }
  if (update.HasAssert() && modified_rows != update.AssertRowsModified()) {
    throw std::runtime_error("ASSERT_ROWS_MODIFIED was specified with " +
                             std::to_string(update.AssertRowsModified()) +
                             " rows, but " + std::to_string(modified_rows) +
                             " rows were modified");
  }
  return Executor(std::make_shared<ConstantExecutor>(
      Row({Value("Update Rows"), Value(modified_rows)})));
}

}  // namespace

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
        relational_detail::Relation materialized =
            relational_detail::ExecuteQuery(ctx, *create.AsQuery(), nullptr, {});
        std::vector<Row> rows;
        materialized.ForEachRow([&rows](const Row& row) { rows.push_back(row); });
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
          columns.emplace_back(col_name, vtype);
        }
        ASSIGN_OR_RETURN(Table, table,
                         database_->CreateTable(
                             ctx, Schema(create.TableName(), columns)));
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
          ASSIGN_OR_RETURN(RowPosition, pos, table.Insert(ctx.txn_, row));
        }
        return Executor(std::make_shared<ConstantExecutor>(
            Row({Value("CREATE TABLE"), Value(static_cast<int64_t>(rows.size()))})));
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
        relational_detail::Relation materialized =
            relational_detail::ExecuteQuery(ctx, *insert.Query(), nullptr, {});
        materialized.ForEachRow([&rows](Row row) { rows.push_back(std::move(row)); });
      } else {
        if (insert.Values().empty()) {
          return Status::kUnknown;
        }
        rows.reserve(insert.Values().size());
        for (const auto& values : insert.Values()) {
          std::vector<Value> row;
          row.reserve(values.size());
          for (const auto& value : values) {
            row.push_back(value->Evaluate(Row(), Schema()));
          }
          rows.emplace_back(std::move(row));
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
          const int destination =
              table->GetSchema().Offset(ColumnName(column));
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
          if (ExpandStructInsertValue(row[0],
                                      table->GetSchema().ColumnCount(),
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
          const ValueType expected = table->GetSchema().GetColumn(i).Type();
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
          last_error_ = "INSERT type mismatch for column " +
                        table->GetSchema().GetColumn(i).Name().name;
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
        if (!optimized.HasValue()) { return optimized.GetStatus(); }
        return optimized.Value()->EmitExecutor(ctx);
      };
      // An explicit LIMIT 0 yields zero rows on every route (relational or
      // optimizer); neither LimitExecutor nor LimitedRows can express that
      // because they read limit==0 as "unbounded" (§6.3).
      if (select->HasLimit() && select->Limit() == 0) {
        return Executor(std::make_shared<ConstantExecutor>(
            std::vector<Row>{}));
      }
      const bool has_unnest = std::any_of(
          select->Sources().begin(), select->Sources().end(),
          [](const SelectSource& s) { return static_cast<bool>(s.unnest); });
      if (select->RequiresRelationalEvaluation() ||
          select->Sources().empty() || has_unnest) {
        return emit_relational();
      }

      // Phase 8 routing: queries whose FROM uses table aliases (including
      // self-joins of one physical table) go through the cost-based
      // optimizer, which renames scan schemas to the alias identity. Plain
      // unaliased joins keep the tuned relational path; outer joins and
      // FROM-subqueries never reach this point (the visitor marks them
      // complex above).
      // Phase 8 routing: the cost-based optimizer owns single-relation
      // queries (as before) plus multi-relation queries whose sources use
      // table aliases -- including self-joins of one physical table, which
      // the optimizer represents by renaming scan schemas to the alias
      // identity. Plain unaliased joins keep the tuned relational path, and
      // outer joins / FROM-subqueries never reach this point (the visitor
      // marks them complex above).
      const bool uses_aliases =
          std::any_of(select->Sources().begin(), select->Sources().end(),
                      [](const SelectSource& source) {
                        return !source.alias.empty() &&
                               source.alias != source.table;
                      });
      // Unqualified column references across several relations are rejected
      // by the relational resolver with a precise ambiguity diagnostic; keep
      // them there.
      bool has_unqualified_column = false;
      if (select->Sources().size() > 1) {
        // A bare `*` is an expansion directive, not a column reference.
        const auto touches_unqualified = [](const NamedExpression& item) {
          for (const ColumnName& column : item.expression->TouchedColumns()) {
            if (column.schema.empty() && column.name != "*") { return true;
}
          }
          return false;
        };
        has_unqualified_column =
            std::ranges::any_of(select->SelectList(), touches_unqualified) ||
            (select->WhereClause() &&
             std::ranges::any_of(select->WhereClause()->TouchedColumns(),
                                 [](const ColumnName& column) {
                                   return column.schema.empty() &&
                                          column.name != "*";
                                 }));
      }
      const bool multi_relation = select->Sources().size() > 1;
      if (multi_relation && (!uses_aliases || has_unqualified_column)) {
        return emit_relational();
      }
      QueryData query;
      Expression where = select->WhereClause();
      std::unordered_set<std::string> seen_relations;
      for (const SelectSource& source : select->Sources()) {
        if (!seen_relations.insert(source.alias).second) {
          last_error_ = "duplicate FROM relation; use distinct aliases";
          return Status::kUnknown;
        }
        query.from_.push_back(source.alias);
        if (!source.alias.empty() && source.alias != source.table) {
          query.aliases_.emplace(source.alias, source.table);
        }
        if (source.join_condition) {
          where = where ? BinaryExpressionExp(where, BinaryOperation::kAnd,
                                              source.join_condition)
                        : source.join_condition;
        }
      }
      query.where_ = where ? where : ConstantValueExp(Value(true));
      query.select_ = select->SelectList();
      // Expand "*" projections here so the visible output width is the
      // expanded width; the optimizer's internal expansion would otherwise
      // disagree with visible_columns and truncate the final projection.
      {
        std::vector<NamedExpression> expanded;
        bool has_star = false;
        for (const NamedExpression& item : query.select_) {
          if (item.expression->Type() == TypeTag::kColumnValue) {
            const ColumnName& column =
                item.expression->AsColumnValue().GetColumnName();
            if (column.name == "*") {
              has_star = true;
              for (const std::string& relation : query.from_) {
                const auto aliased = query.aliases_.find(relation);
                const std::string& physical =
                    aliased == query.aliases_.end() ? relation
                                                    : aliased->second;
                if (!column.schema.empty() && column.schema != relation &&
                    column.schema != physical) { continue;
}
                StatusOr<std::shared_ptr<Table>> found =
                    ctx.GetTable(physical);
                if (!found.HasValue()) { continue;
}
                for (size_t i = 0;
                     i < found.Value()->GetSchema().ColumnCount(); ++i) {
                  expanded.emplace_back(
                      ColumnName(relation,
                                 found.Value()->GetSchema()
                                     .GetColumn(i)
                                     .Name()
                                     .name));
                }
              }
              continue;
            }
          }
          expanded.push_back(item);
        }
        if (has_star && !expanded.empty()) { query.select_ = std::move(expanded);
}
      }
      const size_t visible_columns = query.select_.size();
      std::vector<Expression> sort_expressions;
      std::vector<bool> sort_ascending;
      sort_expressions.reserve(select->OrderBy().size());
      for (size_t i = 0; i < select->OrderBy().size(); ++i) {
        const auto& order = select->OrderBy()[i];
        query.order_expressions_.push_back(order.expression);
        query.order_ascending_.push_back(order.ascending);
        auto selected = std::ranges::find_if(
            query.select_, [&](const auto& item) {
              return item.expression->ToString() ==
                     order.expression->ToString();
            });
        if (selected != query.select_.end()) {
          // Sorting consumes the plan OUTPUT, whose columns are named by the
          // select list; normalize the key to that name so qualifiers
          // (table aliases) cannot break resolution after projection.
          if (!selected->name.empty()) {
            sort_expressions.push_back(ColumnValueExp(ColumnName(selected->name)));
          } else {
            sort_expressions.push_back(order.expression);
          }
        } else {
          const std::string hidden_name = "$order" + std::to_string(i);
          query.select_.emplace_back(hidden_name, order.expression);
          sort_expressions.push_back(ColumnValueExp(hidden_name));
        }
        sort_ascending.push_back(order.ascending);
      }
      // DISTINCT must see every row before truncation, so the optimizer is
      // not told about LIMIT here; Distinct -> Sort -> Limit stays in this
      // order above an untruncated plan (D6 soundness).
      query.limit_count_ = select->Distinct() ? 0 : select->Limit();
      query.limit_offset_ = select->Distinct() ? 0 : select->Offset();
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      Executor executor = plan->EmitExecutor(ctx);
      if (select->Distinct()) {
        executor = std::make_shared<DistinctExecutor>(std::move(executor));
      }
      if (!select->OrderBy().empty() &&
          !plan->IsOrderedBy(query.order_expressions_, query.order_ascending_)) {
        std::vector<SortExecutor::Key> keys;
        keys.reserve(select->OrderBy().size());
        for (size_t i = 0; i < select->OrderBy().size(); ++i) {
          keys.push_back({sort_expressions[i], sort_ascending[i]});
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
        shape->sort_keys.reserve(sort_expressions.size());
        for (size_t i = 0; i < sort_expressions.size(); ++i) {
          shape->sort_keys.emplace_back(sort_expressions[i],
                                        static_cast<bool>(sort_ascending[i]));
        }
        shape->distinct = select->Distinct();
        shape->has_limit = select->HasLimit();
        shape->limit = select->Limit();
        shape->offset = select->Offset();
        shape->visible_columns = visible_columns;
        shape->final_select_size = query.select_.size();
        auto compiled = std::make_shared<CompiledPlan>();
        compiled->kind = CompiledPlan::Kind::kSelect;
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
        return ExecuteNestedArrayUpdate(ctx, update, table.get());
      }
      const Schema& schema = table->GetSchema();
      {
        bool dotted_target = false;
        for (const auto& [target, expression] : update.SetClause()) {
          if (!target.schema.empty() && !target.name.empty() &&
              target.schema != update.TableName()) {
            dotted_target = true;
          }
        }
        if (dotted_target) {
          return ExecuteStructFieldUpdate(ctx, update, table.get());
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
        const ColumnName only_column(
            update.TableName(), schema.GetColumn(0).Name().name);
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
            if (!expr) { return expr; }
            if (expr->Type() == TypeTag::kColumnValue) {
              const ColumnName& name =
                  expr->AsColumnValue().GetColumnName();
              if (matches_alias(name)) {
                return ColumnValueExp(only_column);
              }
              if (!name.schema.empty() &&
                  IdentifierEquals(name.schema, value_table_alias)) {
                return ColumnValueExp(ColumnName(update.TableName(),
                                                 name.name));
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
          if (!expr) { return expr; }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if (!name.schema.empty() && name.schema != update.TableName()) {
              const int base_column = column_index(name.schema);
              if (base_column >= 0 && !name.name.empty() &&
                  name.name != "*") {
                return FunctionCallExp(
                    "__get_field_safe",
                    {ColumnValueExp(ColumnName(
                         update.TableName(),
                         schema.GetColumn(static_cast<size_t>(base_column))
                             .Name().name)),
                     ConstantValueExp(Value(std::string(name.name)))});
              }
            }
            if (single_column_table && name.schema.empty() &&
                !name.name.empty() && name.name != "*" &&
                !IdentifierEquals(name.name,
                                  schema.GetColumn(0).Name().name)) {
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
              (target.schema.empty() ||
               target.schema == update.TableName())) {
            expression = set_clause[j].second;
            applied[j] = true;
            break;
          }
        }
        output.emplace_back(column.Name().name, std::move(expression));
      }
      for (size_t j = 0; j < applied.size(); ++j) {
        if (applied[j]) { continue; }
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
        last_error_ =
            "UPDATE SET target not found: " +
            (set_clause[j].first.schema.empty()
                 ? set_clause[j].first.name
                 : set_clause[j].first.ToString());
        return Status::kNotExists;
      }
      if (!proto_sets.empty()) {
        // Wrap the column read with chained __proto_set calls so each field
        // assignment applies to the payload produced by the previous one.
        for (auto& entry : output) {
          if (!IdentifierEquals(entry.name,
                                schema.GetColumn(0).Name().name)) {
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
      query.where_ = where_clause ? where_clause
                                  : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      if (!update.HasAssert()) {
        RememberSpecializedPlan(
            plan_cache_fingerprint_, database_->SchemaEpoch(),
            plan_cache_parameters_, CompiledPlan::Kind::kUpdate, plan, table);
      }
      return Executor(std::make_shared<Update>(
          ctx.txn_, table.get(), plan->EmitExecutor(ctx),
          update.AssertRowsModified()));
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
          if (!expr) { return expr; }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if ((name.schema.empty() ||
                 IdentifierEquals(name.schema, delete_alias)) &&
                IdentifierEquals(name.name, delete_alias)) {
              return ColumnValueExp(only_column);
            }
            if (!name.schema.empty() &&
                IdentifierEquals(name.schema, delete_alias)) {
              return ColumnValueExp(
                  ColumnName(remove.TableName(), name.name));
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
          if (!expr) { return expr; }
          if (expr->Type() == TypeTag::kColumnValue) {
            const ColumnName& name = expr->AsColumnValue().GetColumnName();
            if (name.schema.empty() && !name.name.empty() &&
                name.name != "*" &&
                !IdentifierEquals(name.name,
                                  schema.GetColumn(0).Name().name)) {
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
      query.where_ = where_clause ? where_clause
                                  : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      query.wait_for_write_intent_ = false;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      if (!remove.HasAssert()) {
        RememberSpecializedPlan(
            plan_cache_fingerprint_, database_->SchemaEpoch(),
            plan_cache_parameters_, CompiledPlan::Kind::kDelete, plan, table);
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
  if (IsVolatileSpecializedPlan(fingerprint)) { return std::nullopt; }
  CompiledPlanPtr compiled =
      FindThreadCompiledPlan(fingerprint, database_->SchemaEpoch());
  if (!compiled) {
    compiled = PreparedPlanCache::Instance().Find(
        fingerprint, database_->SchemaEpoch());
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
            ctx.txn_, compiled->table.get(),
            compiled->plan->EmitExecutor(ctx));
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
  } catch (const std::exception&) {  // NOLINT(bugprone-empty-catch) // Any fast-path doubt falls back to the authoritative legacy compile.
  }
  return std::nullopt;
}

}  // namespace tinylamb
