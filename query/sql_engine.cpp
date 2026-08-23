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
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
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

TemplateShard& ShardFor(const std::string& fingerprint) {
  return template_shards[std::hash<std::string>{}(fingerprint) %
                         kTemplateCacheShards];
}

void RememberTemplate(const std::string& fingerprint,
                      std::unique_ptr<Statement> statement) {
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  if (shard.cache.size() >= kMaxCachedTemplatesPerShard) {
    shard.cache.erase(shard.cache.begin());
  }
  shard.cache.insert_or_assign(fingerprint,
                               std::shared_ptr<Statement>(std::move(statement)));
}

std::shared_ptr<Statement> FindTemplate(const std::string& fingerprint) {
  TemplateShard& shard = ShardFor(fingerprint);
  std::scoped_lock lock(shard.mutex);
  const auto cached = shard.cache.find(fingerprint);
  if (cached == shard.cache.end()) { return nullptr;
}
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

  // Parse (reusing the template cache when possible). This stage is pure:
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
  return PrepareStatement(ctx, std::move(statement));
}

StatusOr<Executor> SqlEngine::PrepareStatement(
    TransactionContext& ctx, std::unique_ptr<Statement> statement) {
  last_statement_type_ = statement->Type();
  switch (statement->Type()) {
    case StatementType::kCreateTable: {
      const auto& create =
          dynamic_cast<const CreateTableStatement&>(*statement);
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
      if (insert.Values().empty()) {
        return Status::kUnknown;
      }
      std::vector<Row> rows;
      rows.reserve(insert.Values().size());
      for (const auto& values : insert.Values()) {
        std::vector<Value> row;
        row.reserve(values.size());
        for (const auto& value : values) {
          row.push_back(value->Evaluate(Row(), Schema()));
        }
        if (!insert.Columns().empty()) {
          if (insert.Columns().size() != row.size()) {
            last_error_ = "INSERT column/value count mismatch";
            return Status::kUnknown;
          }
          std::unordered_set<std::string> seen_columns;
          std::vector<Value> reordered(table->GetSchema().ColumnCount());
          for (size_t i = 0; i < insert.Columns().size(); ++i) {
            if (!seen_columns.insert(insert.Columns()[i]).second) {
              // A duplicate target would silently overwrite the earlier
              // value; reject it instead.
              last_error_ = "duplicate INSERT column: " + insert.Columns()[i];
              return Status::kUnknown;
            }
            const int destination =
                table->GetSchema().Offset(ColumnName(insert.Columns()[i]));
            if (destination < 0) {
              last_error_ = "unknown INSERT column: " + insert.Columns()[i];
              return Status::kNotExists;
            }
            reordered[static_cast<size_t>(destination)] = row[i];
          }
          row = std::move(reordered);
        }
        if (row.size() != table->GetSchema().ColumnCount()) {
          last_error_ = "INSERT value count does not match table schema (got " +
                        std::to_string(row.size()) + ", expected " +
                        std::to_string(table->GetSchema().ColumnCount()) + ")";
          return Status::kUnknown;
        }
        for (size_t i = 0; i < row.size(); ++i) {
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
        rows.emplace_back(std::move(row));
      }
      return Executor(std::make_shared<Insert>(
          ctx.txn_, table.get(),
          std::make_shared<ConstantExecutor>(std::move(rows))));
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
      result_column_names_.reserve(select->SelectList().size());
      for (const NamedExpression& item : select->SelectList()) {
        result_column_names_.push_back(
            item.name.empty() ? item.expression->ToString() : item.name);
      }
      // An explicit LIMIT 0 yields zero rows on every route (relational or
      // optimizer); neither LimitExecutor nor LimitedRows can express that
      // because they read limit==0 as "unbounded" (§6.3).
      if (select->HasLimit() && select->Limit() == 0) {
        return Executor(std::make_shared<ConstantExecutor>(
            std::vector<Row>{}));
      }
      if (select->RequiresRelationalEvaluation()) {
        return Executor(std::make_shared<RelationalExecutor>(ctx, select));
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
        return Executor(std::make_shared<RelationalExecutor>(ctx, select));
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
      return executor;
    }
    case StatementType::kUpdate: {
      const auto& update = dynamic_cast<const UpdateStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(update.TableName()));
      std::vector<NamedExpression> output;
      const Schema& schema = table->GetSchema();
      output.reserve(schema.ColumnCount());
      // SET targets may be qualified ("UPDATE t SET t.a = ..."); match the
      // full ColumnName instead of the bare name so qualified assignments are
      // not silently ignored.
      std::vector<bool> applied(update.SetClause().size(), false);
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const Column& column = schema.GetColumn(i);
        Expression expression = ColumnValueExp(column.Name());
        for (size_t j = 0; j < update.SetClause().size(); ++j) {
          const ColumnName& target = update.SetClause()[j].first;
          if (target.name == column.Name().name &&
              (target.schema.empty() ||
               target.schema == update.TableName())) {
            expression = update.SetClause()[j].second;
            applied[j] = true;
            break;
          }
        }
        output.emplace_back(column.Name().name, std::move(expression));
      }
      for (size_t j = 0; j < applied.size(); ++j) {
        if (!applied[j]) {
          last_error_ =
              "UPDATE SET target not found: " +
              (update.SetClause()[j].first.schema.empty()
                   ? update.SetClause()[j].first.name
                   : update.SetClause()[j].first.ToString());
          return Status::kNotExists;
        }
      }
      QueryData query;
      query.from_ = {update.TableName()};
      query.where_ = update.WhereClause() ? update.WhereClause()
                                          : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      return Executor(std::make_shared<Update>(ctx.txn_, table.get(),
                                               plan->EmitExecutor(ctx)));
    }
    case StatementType::kDelete: {
      const auto& remove = dynamic_cast<const DeleteStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(remove.TableName()));
      std::vector<NamedExpression> output;
      const Schema& schema = table->GetSchema();
      output.reserve(schema.ColumnCount());
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        output.emplace_back(schema.GetColumn(i).Name());
      }
      QueryData query;
      query.from_ = {remove.TableName()};
      query.where_ = remove.WhereClause() ? remove.WhereClause()
                                          : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      return Executor(std::make_shared<DeleteExecutor>(
          ctx.txn_, *table, plan->EmitExecutor(ctx)));
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

}  // namespace tinylamb
