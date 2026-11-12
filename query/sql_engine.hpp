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
#include <unordered_map>
#include <vector>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "plan/plan.hpp"
#include "query/query_data.hpp"
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
  [[nodiscard]] Status GetStatus() const {
    return executor_ ? executor_->GetStatus() : Status::kSuccess;
  }
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
  // Conservative certification of a parsed, table-free literal arithmetic
  // SELECT. Consumers may reuse a successful complete result under exact SQL.
  // Functions, variables, subqueries and statement modifiers never qualify.
  [[nodiscard]] bool LastQueryIsLiteralConstant() const {
    return last_query_is_literal_constant_;
  }
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
  // Number of sibling derived-aggregate fusions performed while preparing
  // the most recent statement (0 otherwise; see FuseSiblingDerivedAggregates).
  [[nodiscard]] size_t LastFusedSiblingAggregates() const {
    return fused_sibling_aggregates_;
  }
  // Number of shared-CTE restriction pushdowns performed while preparing the
  // most recent statement (0 otherwise; see PushSharedCteRestrictions).
  [[nodiscard]] size_t LastCteRestrictionPushdowns() const {
    return cte_restriction_pushdowns_;
  }

  // Speculative eager CTE drains during the most recent SELECT preparation.
  [[nodiscard]] size_t LastCteMaterializationAttempts() const {
    return cte_materialization_attempts_;
  }

  // Compliance-driver hook: when enabled, INSERT statements emulate
  // first_column_is_primary_key semantics (duplicate-key errors, and
  // IGNORE/UPDATE/REPLACE conflict handling). Off by default; plain heap
  // semantics otherwise.
  static void SetCompliancePrimaryKeyMode(bool enabled);
  [[nodiscard]] static bool CompliancePrimaryKeyMode();

  void SetForceRelationalFallback(bool force) {
    force_relational_fallback_ = force;
  }
  // SELECT ... FOR UPDATE lock acquisition policy.  GoogleSQL has no
  // NOWAIT/SKIP LOCKED grammar (the frontend's parser stops at FOR UPDATE), so
  // the policy is an embedder knob rather than SQL syntax:
  //   kWait   - blocking acquisition (the default; the engine's 5s cap and
  //             deadlock-victim wounding apply).
  //   kNoWait - try-lock: a conflicting writer fails the statement at once with
  //             a kConflicts error, the same policy DELETE's source scan uses.
  // The mode is part of the compiled-plan cache key, so a cached plan can never
  // replay the other policy.
  enum class ForUpdateWaitMode { kWait, kNoWait };
  void SetForUpdateWaitMode(ForUpdateWaitMode mode) {
    for_update_wait_mode_ = mode;
  }
  [[nodiscard]] ForUpdateWaitMode ForUpdateWaitModeValue() const {
    return for_update_wait_mode_;
  }
  [[nodiscard]] bool ForceRelationalFallback() const {
    return force_relational_fallback_;
  }

 private:
  StatusOr<Executor> PrepareStatement(TransactionContext& ctx,
                                      std::unique_ptr<Statement> statement);

  // Folds non-correlated `x IN (SELECT ...)` (and its negation) inside a
  // DML WHERE into a constant IN list by running the subquery once now.
  // The positional DML plans cannot evaluate a raw query expression: the
  // scalar evaluator raises "requires relational evaluation" (masks as a
  // runtime error) or the decorated semi-join drops row positions.
  // Correlated or failing inner statements stay untouched (fallback
  // behaviour), never a wrong answer produced from this helper itself.
  StatusOr<Expression> DecorateDmlWhere(TransactionContext& ctx,
                                        const Expression& expressionssion);

  // `x op ANY/ALL (SELECT ...)` non-correlated fold; see the definition.
  StatusOr<Expression> FoldQuantifiedComparison(TransactionContext& ctx,
                                                const QueryExpression& query);

  // M4: lifts every remaining non-recursive CTE into a shared eager cell
  // (planned + executed once via a fresh engine, scanned from every
  // reference site through kValues memo leaves). Atomic: any un-liftable CTE
  // keeps the whole layer mapped for the existing materialized path (the map
  // below is never disturbed, so the abort path is identical to never having
  // tried). Records covered CTE names in `lifted`. Clears the plan cache
  // fingerprint when cells are built (embedded rows are data, never
  // cacheable).
  void MaterializeCtes(SelectStatement* outer, TransactionContext& ctx,
                       LiftedCtes* lifted);

  // M4: references singly-used recursive CTEs as opaque memo leaves (the
  // fixpoint still executes through the proven worktable driver). Records
  // coverage in `lifted` and seals `fully_covered` last: routing treats a
  // covered map as vestigial.
  void LiftRecursiveCtes(SelectStatement* outer, LiftedCtes* lifted);

  // Cascades-route set operations: each operand runs through normal statement
  // routing (recursively) and the operands combine at the executor level with
  // INTERSECT binding tighter than UNION/EXCEPT.
  StatusOr<Executor> ExecuteSetOperation(const SelectStatement& select,
                                         TransactionContext& ctx);

  // MERGE INTO: materializes the source, classifies target/source rows
  // against the WHEN clauses, and applies the pending DML in one transaction.
  StatusOr<Executor> ExecuteMerge(TransactionContext& ctx,
                                  const MergeStatement& merge);

  // GROUP BY / HAVING / aggregate routing: optimizes the FROM + WHERE core
  // through Cascades and wraps it in a GroupByPlan finish node.
  // `lifted` carries the lifted CTE layer (M4) into the core QueryData;
  // null selects the legacy behavior.
  StatusOr<Executor> ExecuteGroupedSelect(const SelectStatement& select,
                                          TransactionContext& ctx,
                                          const LiftedCtes* lifted = nullptr);

  // Cascades routing for UNNEST queries: lowers UNNEST and lateral sources
  // into LogicalOperator::kUnnest over DummyScan/input plans.
  StatusOr<Executor> ExecuteUnnestSelect(const SelectStatement& select,
                                         TransactionContext& ctx);

  // Phase 2-1 compiled-plan cache. Returns a served Executor on a hit
  // (nullopt = miss or any doubt; callers fall back to the legacy path).
  std::optional<Executor> ServeFromPlanCache(
      TransactionContext& ctx, const std::string& fingerprint,
      const std::vector<Value>& parameters,
      bool require_immutable_exact_read = false);

  // Fingerprint/parameters of the statement currently being prepared; fill
  // sites inside PrepareStatement consult these. An empty fingerprint
  // disables fills (for example EXPLAIN). Replay eligibility is checked
  // separately so unsupported template shapes cannot use the binder.
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
    // Non-copyable by design: instances are owned by smart pointers and
    // referenced by raw pointers throughout the executor/graph object web.
    PlanCacheCandidateGuard(const PlanCacheCandidateGuard&) = delete;
    PlanCacheCandidateGuard& operator=(const PlanCacheCandidateGuard&) = delete;
    PlanCacheCandidateGuard(PlanCacheCandidateGuard&&) = delete;
    PlanCacheCandidateGuard& operator=(PlanCacheCandidateGuard&&) = delete;
    explicit PlanCacheCandidateGuard(SqlEngine* engine) : engine_(engine) {}
    ~PlanCacheCandidateGuard() { engine_->clear_plan_cache_candidate(); }

   private:
    SqlEngine* engine_;
  };

  // One Cascades plan of a grouped FROM+WHERE core, reused by later UNION
  // ALL branches (GROUPING SETS / ROLLUP / CUBE) whose required columns are
  // already in that plan. Cleared at each public Prepare.
  struct SharedGroupingCore {
    std::string fingerprint;
    Plan plan;
  };

  // Re-registers persisted view definitions into the session ViewRegistry
  // when the catalog's schema epoch moved (CREATE/DROP VIEW bump it).  The
  // per-engine epoch cache keeps a quiet statement's cost at one compare.
  void EnsureViewsRegistered(TransactionContext& ctx);

  Database* database_;
  std::string last_error_;
  std::optional<StatementType> last_statement_type_;
  bool last_query_is_literal_constant_{false};
  std::string last_dml_table_;
  std::vector<std::string> result_column_names_;
  std::string plan_cache_fingerprint_;
  size_t fused_sibling_aggregates_{0};
  size_t cte_restriction_pushdowns_{0};
  size_t cte_materialization_attempts_{0};

  // True when this statement's WHERE/SET carried a subquery folded to
  // constants at prepare time (Bug 57): the folded values are valid only
  // for the data as of now, so the compiled plan must not be cached.
  bool dml_fold_applied_{false};
  std::vector<Value> plan_cache_parameters_;
  bool force_relational_fallback_{false};
  ForUpdateWaitMode for_update_wait_mode_{ForUpdateWaitMode::kWait};
  std::vector<SharedGroupingCore> shared_grouping_cores_;
  uint64_t views_registered_epoch_{~uint64_t{0}};
};

}  // namespace tinylamb

#endif  // TINYLAMB_SQL_ENGINE_HPP
