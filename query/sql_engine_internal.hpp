/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_QUERY_SQL_ENGINE_INTERNAL_HPP
#define TINYLAMB_QUERY_SQL_ENGINE_INTERNAL_HPP

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "executor/foreign_key.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "plan/plan.hpp"
#include "query/plan_cache.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Database;
class Table;
class TransactionContext;

// --- Foreign Key & Referential Integrity Resolution ---
StatusOr<std::vector<ForeignKeyCheck>> ResolveForeignKeyChecks(
    TransactionContext& ctx, const Table& target);

StatusOr<std::vector<ReferencedByCheck>> ResolveReferencedByChecks(
    Database* database, TransactionContext& ctx, const Table& target);

Status ValidateNarrowInteger(const Column& column, const Value& value);

// --- Compiled Plan Serving & Slots ---
std::optional<Executor> ServeCompiledInsert(TransactionContext& ctx,
                                            const CompiledPlan& compiled,
                                            std::vector<Value> parameters);

struct SqlLiteralSlots {
  const std::vector<Value>* parameters = nullptr;
  size_t cursor = 0;
  bool ok = true;

  Expression Apply(const Expression& expression) {
    if (!ok || parameters == nullptr || !expression) {
      return expression;
    }
    bool local_ok = true;
    Expression slotted =
        SlotizeLiterals(expression, &cursor, &local_ok, parameters, nullptr);
    if (!local_ok) {
      ok = false;
      return expression;
    }
    return slotted;
  }

  [[nodiscard]] bool Complete() const {
    return ok && parameters != nullptr && cursor == parameters->size();
  }
};

std::vector<Expression> InstallPlanSlots(const Plan& plan,
                                         const std::vector<Value>& parameters);

void RememberSpecializedPlan(const std::string& fingerprint, uint64_t epoch,
                             std::vector<Value> parameters,
                             CompiledPlan::Kind kind, Plan plan,
                             std::shared_ptr<Table> table,
                             const Database* database,
                             const TransactionContext& ctx,
                             std::vector<Expression> slots);

// --- EXPLAIN Support ---
// EXPLAIN has no StatementType: it is a Prepare-level string prefix recognised
// by ParseExplain for SELECT/WITH only.  Besides `EXPLAIN [ANALYZE]` the parser
// accepts `EXPLAIN [ANALYZE] PIPELINE` and the reversed `EXPLAIN PIPELINE
// ANALYZE`; `pipeline` records that extension.
struct ExplainRequest {
  bool analyze{false};
  bool pipeline{false};
  std::string_view query;
};

std::optional<ExplainRequest> ParseExplain(std::string_view sql);
std::vector<Row> ExplainRows(std::string_view plan);
std::vector<ValueType> SchemaColumnTypes(const Schema& schema);

Executor WithPlanExplain(Executor executor, Plan plan,
                         std::vector<ValueType>* output_types = nullptr);

// --- ANALYZE Support ---
struct AnalyzeRequest {
  std::vector<std::string> tables;
};

std::optional<AnalyzeRequest> ParseAnalyze(std::string_view sql);
StatusOr<Executor> ExecuteAnalyze(Database& database, TransactionContext& ctx,
                                  const AnalyzeRequest& request);

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_SQL_ENGINE_INTERNAL_HPP
