/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_SQL_ENGINE_HPP
#define TINYLAMB_SQL_ENGINE_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Database;
class Statement;
class SelectStatement;
class TransactionContext;
enum class StatementType : uint8_t;

struct SqlRuntimeStats {
  uint64_t prepare_ns{0};
  uint64_t collect_ns{0};
};

// Unified streaming result returned to CLI, pgwire and benchmarks. Consumers
// choose a sink, but preparation, row iteration and affected-row decoding no
// longer get reimplemented at every boundary.
class QueryResult {
 public:
  QueryResult(Executor executor, std::optional<StatementType> type,
              std::vector<std::string> column_names)
      : executor_(std::move(executor)),
        statement_type_(type),
        column_names_(std::move(column_names)) {}

  bool Next(Row* row);
  size_t ForEach(const std::function<void(const Row&)>& sink);
  size_t Drain();
  std::vector<Row> Collect();
  int64_t AffectedRows();
  void Dump(std::ostream& output, int indent = 0) const;
  [[nodiscard]] const std::optional<StatementType>& Statement() const {
    return statement_type_;
  }
  [[nodiscard]] const std::vector<std::string>& ColumnNames() const {
    return column_names_;
  }

 private:
  Executor executor_;
  std::optional<StatementType> statement_type_;
  std::vector<std::string> column_names_;
};

class SqlEngine {
 public:
  explicit SqlEngine(Database& database);

  StatusOr<QueryResult> Execute(TransactionContext& ctx, std::string_view sql);
  StatusOr<Executor> Prepare(TransactionContext& ctx, std::string_view sql);
  // Per-worker ingress count used by benchmark integrity gates.  It advances
  // before parsing/preparing, so exceptions cannot make an SQL invocation
  // disappear from the measurement.
  [[nodiscard]] static uint64_t ThreadExecutionCount();
  static void SetThreadRuntimeProfiling(bool enabled);
  [[nodiscard]] static SqlRuntimeStats ThreadRuntimeStats();
  [[nodiscard]] const std::string& LastError() const { return last_error_; }
  [[nodiscard]] const std::optional<StatementType>& LastStatementType() const {
    return last_statement_type_;
  }
  // Target table of the most recent DDL/DML statement (INSERT target, UPDATE
  // target, DELETE FROM target, or the created table); used by the compliance
  // driver to inspect post-statement table state.
  [[nodiscard]] const std::string& LastDmlTable() const {
    return last_dml_table_;
  }
  [[nodiscard]] const std::vector<std::string>& ResultColumnNames() const {
    return result_column_names_;
  }

  // Compliance-driver hook: when enabled, INSERT statements emulate
  // first_column_is_primary_key semantics (duplicate-key errors, and
  // IGNORE/UPDATE/REPLACE conflict handling). Off by default; plain heap
  // semantics otherwise.
  static void SetCompliancePrimaryKeyMode(bool enabled);
  [[nodiscard]] static bool CompliancePrimaryKeyMode();

 private:
  StatusOr<Executor> PrepareStatement(TransactionContext& ctx,
                                      std::unique_ptr<Statement> statement);

  // Cascades-route set operations: each operand runs through normal statement
  // routing (recursively) and the operands combine at the executor level with
  // INTERSECT binding tighter than UNION/EXCEPT.
  StatusOr<Executor> ExecuteSetOperation(const SelectStatement& select,
                                         TransactionContext& ctx);

  // GROUP BY / HAVING / aggregate routing: optimizes the FROM + WHERE core
  // through Cascades and wraps it in a GroupByPlan finish node.
  StatusOr<Executor> ExecuteGroupedSelect(const SelectStatement& select,
                                          TransactionContext& ctx);

  // Phase 2-1 compiled-plan cache. Returns a served Executor on a hit
  // (nullopt = miss or any doubt; callers fall back to the legacy path).
  std::optional<Executor> ServeFromPlanCache(
      TransactionContext& ctx, const std::string& fingerprint,
      const std::vector<Value>& parameters);

  // Fingerprint/parameters of the statement currently being prepared; fill
  // sites inside PrepareStatement consult these. An empty fingerprint
  // disables caching (EXPLAIN, non-templatable SQL).
  void set_plan_cache_candidate(std::string fingerprint,
                                std::vector<Value> parameters) {
    plan_cache_fingerprint_ = std::move(fingerprint);
    plan_cache_parameters_ = std::move(parameters);
  }
  void clear_plan_cache_candidate() {
    plan_cache_fingerprint_.clear();
    plan_cache_parameters_.clear();
  }
  // Disarms the fill sites no matter how Prepare() exits.
  class PlanCacheCandidateGuard {
   public:
    explicit PlanCacheCandidateGuard(SqlEngine* engine) : engine_(engine) {}
    ~PlanCacheCandidateGuard() { engine_->clear_plan_cache_candidate(); }

   private:
    SqlEngine* engine_;
  };

  Database* database_;
  std::string last_error_;
  std::optional<StatementType> last_statement_type_;
  std::string last_dml_table_;
  std::vector<std::string> result_column_names_;
  std::string plan_cache_fingerprint_;
  std::vector<Value> plan_cache_parameters_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_ENGINE_HPP
#include <functional>
