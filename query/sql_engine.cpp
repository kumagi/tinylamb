/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "query/sql_engine.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/delete.hpp"
#include "executor/distinct.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/projection.hpp"
#include "executor/query_scheduler.hpp"
#include "executor/relational.hpp"
#include "executor/sort.hpp"
#include "executor/update.hpp"
#include "expression/constant_value.hpp"
#include "parser/ast.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "query/googlesql_ast.hpp"
#include "query/googlesql_ast_visitor.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/query_data.hpp"
#include "table/table.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

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
    if (input->size() < keyword.size()) return false;
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
  if (!consume(&remainder, "EXPLAIN")) return std::nullopt;
  const bool analyze = consume(&remainder, "ANALYZE");
  remainder = trim(remainder);
  if (remainder.empty()) return ExplainRequest{analyze, {}};
  return ExplainRequest{analyze, remainder};
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
    if (end == std::string_view::npos) break;
    begin = end + 1;
  }
  return rows;
}

}  // namespace

StatusOr<Executor> SqlEngine::Prepare(TransactionContext& ctx,
                                      std::string_view sql) {
  last_error_.clear();
  last_statement_type_.reset();
  result_column_names_.clear();
  if (const std::optional<ExplainRequest> explain = ParseExplain(sql)) {
    if (explain->query.empty()) {
      last_error_ = "EXPLAIN requires a query";
      return Status::kUnknown;
    }
    const auto planning_start = std::chrono::steady_clock::now();
    StatusOr<Executor> prepared = Prepare(ctx, explain->query);
    const auto planning_end = std::chrono::steady_clock::now();
    if (!prepared.HasValue()) return prepared.GetStatus();
    if (!last_statement_type_ ||
        *last_statement_type_ != StatementType::kSelect) {
      last_error_ = "EXPLAIN currently supports SELECT and WITH queries";
      return Status::kNotImplemented;
    }

    uint64_t rows = 0;
    std::chrono::steady_clock::time_point execution_end = planning_end;
    if (explain->analyze) {
      Row row;
      while (prepared.Value()->Next(&row, nullptr)) ++rows;
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
    last_statement_type_ = StatementType::kSelect;
    result_column_names_ = {"QUERY PLAN"};
    return Executor(
        std::make_shared<ConstantExecutor>(ExplainRows(output.str())));
  }
  GoogleSqlParseResult parsed = GoogleSqlFrontend::Parse(sql);
  if (!parsed.ok) {
    last_error_ = std::move(parsed.error);
    return Status::kUnknown;
  }
  try {
    ASSIGN_OR_RETURN(std::unique_ptr<GoogleSqlAstNode>, ast,
                     GoogleSqlAstParser::Parse(parsed.ast));
    return PrepareStatement(ctx, GoogleSqlAstVisitor::Visit(*ast));
  } catch (const std::exception& error) {
    last_error_ = error.what();
    return Status::kUnknown;
  }
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
          std::vector<Value> reordered(table->GetSchema().ColumnCount());
          for (size_t i = 0; i < insert.Columns().size(); ++i) {
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
          last_error_ = "INSERT value count does not match table schema";
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
      auto select = std::shared_ptr<SelectStatement>(
          static_cast<SelectStatement*>(statement.release()));
      result_column_names_.reserve(select->SelectList().size());
      for (const NamedExpression& item : select->SelectList()) {
        result_column_names_.push_back(
            item.name.empty() ? item.expression->ToString() : item.name);
      }
      if (select->RequiresRelationalEvaluation()) {
        Executor executor = std::make_shared<RelationalExecutor>(ctx, select);
        QueryScheduler& scheduler = QueryScheduler::Global();
        return Executor(std::make_shared<ScheduledExecutor>(
            std::move(executor), scheduler,
            std::min<size_t>(4, scheduler.CpuCapacity()), size_t{64} << 20));
      }
      QueryData query;
      query.from_ = select->FromClause();
      query.aliases_ = select->Aliases();
      query.where_ = select->WhereClause() ? select->WhereClause()
                                           : ConstantValueExp(Value(true));
      query.select_ = select->SelectList();
      const size_t visible_columns = query.select_.size();
      std::vector<Expression> sort_expressions;
      sort_expressions.reserve(select->OrderBy().size());
      for (size_t i = 0; i < select->OrderBy().size(); ++i) {
        const auto& order = select->OrderBy()[i];
        auto selected = std::find_if(query.select_.begin(), query.select_.end(),
                                     [&](const auto& item) {
                                       return item.expression->ToString() ==
                                              order.expression->ToString();
                                     });
        if (selected != query.select_.end()) {
          sort_expressions.push_back(order.expression);
        } else {
          const std::string hidden_name = "$order" + std::to_string(i);
          query.select_.emplace_back(hidden_name, order.expression);
          sort_expressions.push_back(ColumnValueExp(hidden_name));
        }
      }
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      Executor executor = plan->EmitExecutor(ctx);
      if (select->Distinct()) {
        executor = std::make_shared<DistinctExecutor>(std::move(executor));
      }
      if (!select->OrderBy().empty()) {
        std::vector<SortExecutor::Key> keys;
        keys.reserve(select->OrderBy().size());
        for (size_t i = 0; i < select->OrderBy().size(); ++i) {
          keys.push_back({sort_expressions[i], select->OrderBy()[i].ascending});
        }
        executor = std::make_shared<SortExecutor>(
            std::move(executor), plan->GetSchema(), std::move(keys));
      }
      if (select->Limit() != 0 || select->Offset() != 0) {
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
      QueryScheduler& scheduler = QueryScheduler::Global();
      return Executor(std::make_shared<ScheduledExecutor>(
          std::move(executor), scheduler,
          std::min<size_t>(4, scheduler.CpuCapacity()), size_t{64} << 20));
    }
    case StatementType::kUpdate: {
      const auto& update = dynamic_cast<const UpdateStatement&>(*statement);
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, table,
                       ctx.GetTable(update.TableName()));
      std::vector<NamedExpression> output;
      const Schema& schema = table->GetSchema();
      output.reserve(schema.ColumnCount());
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const Column& column = schema.GetColumn(i);
        Expression expression = ColumnValueExp(column.Name());
        for (const auto& assignment : update.SetClause()) {
          if (assignment.first.name == column.Name().name) {
            expression = assignment.second;
            break;
          }
        }
        output.emplace_back(column.Name().name, std::move(expression));
      }
      QueryData query;
      query.from_ = {update.TableName()};
      query.where_ = update.WhereClause() ? update.WhereClause()
                                          : ConstantValueExp(Value(true));
      query.select_ = std::move(output);
      query.require_row_position_ = true;
      RETURN_IF_FAIL(query.Rewrite(ctx));
      ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(query, ctx));
      Executor stable_source = std::make_shared<SortExecutor>(
          plan->EmitExecutor(ctx), plan->GetSchema(),
          std::vector<SortExecutor::Key>{});
      return Executor(std::make_shared<Update>(ctx.txn_, table.get(),
                                               std::move(stable_source)));
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
      Executor stable_source = std::make_shared<SortExecutor>(
          plan->EmitExecutor(ctx), plan->GetSchema(),
          std::vector<SortExecutor::Key>{});
      return Executor(std::make_shared<DeleteExecutor>(
          ctx.txn_, *table, std::move(stable_source)));
    }
    case StatementType::kDropTable:
      return Status::kNotImplemented;
  }
  return Status::kNotImplemented;
}

}  // namespace tinylamb
