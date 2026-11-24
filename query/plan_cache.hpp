/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_PLAN_CACHE_HPP
#define TINYLAMB_PLAN_CACHE_HPP

// Phase 2-1: compiled-plan cache ("prepared plans"), the compiled-plan cache
// design notes.
//
// Key:    SQL fingerprint, database and schema/statistics epoch, optimizer
//         toggles, and memory settings used to select the physical plan.
// Value:  a CompiledPlan, one of
//           (a) kSelect/kUpdate/kDelete: a plan whose bindable literals are
//               ParameterSlots. Replay binds the incoming values into those
//               slots and rebuilds index seek keys from the predicate, so a
//               repeated shape hits regardless of its literals. A SELECT whose
//               later execution is both slower than 50ms and at least 8x its
//               first replay is dropped and planned again (parameter sniffing).
//               Shapes that cannot keep the slots fall back to an
//               identical-parameter replay and are never permanently retired.
//           (b) kInsert: a parametric artifact whose bindable literals were
//               replaced by ParameterSlot placeholders; runtime values are
//               injected per execution ("prepared" style), so every repeated
//               INSERT shape hits regardless of its literal values.
//
// Invalidation: Database::BumpSchemaEpoch() advances on CREATE TABLE / DROP
// TABLE / CREATE INDEX / statistics refresh (ANALYZE). Entries whose epoch no
// longer matches are dropped lazily on lookup and recompiled through the
// legacy path. Row-count drift between ANALYZEs only skews cost estimates,
// never results, so it does not need its own invalidation trigger -- with one
// exception: a *clustered index order* a full scan reports is a measured,
// revocable proof (plan/full_scan_plan.hpp). A statement planned while such a
// proof was live may have baked an order assumption into its tree (a folded
// LIMIT, an incremental sort over the clustered prefix), which a later
// execution of the cached plan would take at face value. SqlEngine therefore
// keeps such statements out of this cache: it samples
// AnyResolvedTableHasClusteredOrder() before optimizing and again before
// storing, and skips the fill if either sample was true.
//
// External retention policy matches the existing sql_template AST cache:
// entries are advisory process-global state; nothing outside the cache pins
// them, and eviction simply drops them.
//
// Safety valve: any shape the fast path cannot represent (DDL, EXPLAIN,
// non-deterministic functions like CURRENT_TIMESTAMP, subqueries in INSERT
// VALUES, relational-route SELECTs) is never stored; lookups miss and the
// legacy Prepare path runs verbatim.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/memory_budget.hpp"
#include "common/platform/process.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Table;
class TableStatistics;
class Database;

// ---------------------------------------------------------------------------
// Observability: process-global counters. Tests read deltas; benchmarks can
// log PlanCacheStats() snapshots to expose hit rates.
// ---------------------------------------------------------------------------
struct PlanCacheCounters {
  std::atomic<uint64_t> hits{0};
  // A lookup hit is not necessarily executable: specialized OLTP plans bake
  // constants into their index bounds.  Keep replay and literal-mismatch
  // counts separate so a busy fingerprint cannot masquerade as a useful
  // cache hit merely because its last artifact was found.
  std::atomic<uint64_t> replays{0};
  std::atomic<uint64_t> parameter_mismatches{0};
  std::atomic<uint64_t> misses{0};
  std::atomic<uint64_t> fills{0};
  std::atomic<uint64_t> evictions{0};
  std::atomic<uint64_t> epoch_invalidations{0};
  // The parsed-tree cache (`FindTemplate`/`RememberTemplate` in sql_engine) is
  // separate from compiled plans and is keyed by the literal-free fingerprint.
  // `template_fills` counts a stored tree; `template_hits` counts a lookup
  // whose literals rebound successfully (a failed rebind parses the SQL
  // verbatim, so it is neither a hit nor a fill).
  std::atomic<uint64_t> template_fills{0};
  std::atomic<uint64_t> template_hits{0};
  // Shard-capacity evictions from the parsed-tree cache (distinct from the
  // compiled-plan `evictions` above).
  std::atomic<uint64_t> template_evictions{0};
};

inline PlanCacheCounters& PlanCacheStats() {
  // Function-local statics are initialized thread-safely, and every field of
  // PlanCacheCounters is std::atomic, so concurrent callers need no extra lock.
  static PlanCacheCounters counters;
  return counters;
}

// Rebuilds `expression` so every ParameterSlot reads from `values`. Slot-free
// subtrees are shared under the compiled-plan cache's immutable-tree
// convention; only nodes on paths carrying slots are copied.
inline Expression CloneWithPreparedValues(  // NOLINT(misc-no-recursion)
    const Expression& expression,
    const std::shared_ptr<const PreparedValues>& values) {
  if (!expression || !ContainsParameterSlot(expression)) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      const auto* slot = dynamic_cast<const ParameterSlot*>(expression.get());
      return slot == nullptr ? expression : slot->Rebind(values);
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return BinaryExpressionExp(
          CloneWithPreparedValues(binary.Left(), values), binary.Op(),
          CloneWithPreparedValues(binary.Right(), values));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(CloneWithPreparedValues(unary.Child(), values),
                                unary.Op());
    }
    case TypeTag::kAggregateExp: {
      // P295: rebuild every attribute ContainsParameterSlot descends into
      // (WHERE filter, HAVING, inner ORDER BY / GROUP BY, secondary and
      // trailing args), not just (type, child, distinct).
      return RebuildAggregateAttributes(
          expression,
          [&](const Expression& inner) {
            return CloneWithPreparedValues(inner, values);
          },
          /*ok=*/nullptr);
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        clauses.emplace_back(CloneWithPreparedValues(clause.first, values),
                             CloneWithPreparedValues(clause.second, values));
      }
      return CaseExpressionExp(
          std::move(clauses),
          CloneWithPreparedValues(searched.else_clause_, values));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(CloneWithPreparedValues(item, values));
      }
      return InExpressionExp(CloneWithPreparedValues(in.child_, values),
                             std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (const Expression& arg : call.Args()) {
        args.push_back(CloneWithPreparedValues(arg, values));
      }
      // FunctionCallExpression is already fully desugared by the frontend:
      // named arguments (`name => value`) are resolved to positional order,
      // and `SAFE.<fn>` is represented by a nulliferror(<fn>) expression.
      // There is no separate SAFE/error-mode metadata to copy. If unrelated
      // metadata is added to this node later, preserve it in this rebuild (and
      // the parallel one in expression/parameter_slot.hpp) as well.
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(CloneWithPreparedValues(element, values));
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kCastExp: {
      const auto& cast = expression->AsCastExpression();
      return CastExpressionExp(CloneWithPreparedValues(cast.Child(), values),
                               cast.TargetTypeName(), cast.ReturnNullOnError());
    }
    default:
      return expression;
  }
}

// True when evaluating `expression` per row could yield different results
// between executions of the same cached artifact (e.g. CURRENT_TIMESTAMP).
inline bool ContainsNonDeterministicCall(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      if (GetFunctionVolatility(call.FuncName()) != Volatility::kImmutable) {
        return true;
      }
      return std::ranges::any_of(call.Args(), [](const Expression& arg) {
        return ContainsNonDeterministicCall(arg);
      });
    }
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return ContainsNonDeterministicCall(binary.Left()) ||
             ContainsNonDeterministicCall(binary.Right());
    }
    case TypeTag::kUnaryExp:
      return ContainsNonDeterministicCall(
          expression->AsUnaryExpression().Child());
    case TypeTag::kAggregateExp: {
      // A non-deterministic call can hide in any of the aggregate's modifiers,
      // not just its child (cf. the parallel deep scan in ContainsParameterSlot
      // in expression/parameter_slot.hpp). Missing one here would let a plan
      // filled with, say, ARRAY_AGG(x ORDER BY RAND()) or SUM(x WHERE f()),
      // be replayed with the fill-time value.
      const auto& aggregate = expression->AsAggregateExpression();
      if (ContainsNonDeterministicCall(aggregate.Child()) ||
          ContainsNonDeterministicCall(aggregate.SecondaryArg()) ||
          ContainsNonDeterministicCall(aggregate.WhereFilter()) ||
          ContainsNonDeterministicCall(aggregate.HavingCondition())) {
        return true;
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (ContainsNonDeterministicCall(extra)) {
          return true;
        }
      }
      return std::ranges::any_of(
                 aggregate.InnerOrderBy(),
                 [](const WindowOrderTerm& term) {
                   return ContainsNonDeterministicCall(term.expression);
                 }) ||
             std::ranges::any_of(aggregate.InnerGroupBy(),
                                 [](const Expression& key) {
                                   return ContainsNonDeterministicCall(key);
                                 });
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      for (const auto& clause : searched.when_clauses_) {
        if (ContainsNonDeterministicCall(clause.first) ||
            ContainsNonDeterministicCall(clause.second)) {
          return true;
        }
      }
      return ContainsNonDeterministicCall(searched.else_clause_);
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (ContainsNonDeterministicCall(in.child_)) {
        return true;
      }
      if (std::ranges::any_of(in.list_, [](const Expression& item) {
            return ContainsNonDeterministicCall(item);
          })) {
        return true;
      }
      return false;
    }
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        if (ContainsNonDeterministicCall(element)) {
          return true;
        }
      }
      return false;
    case TypeTag::kCastExp:
      // A nondeterministic call hidden under a cast must still disqualify
      // the artifact, or a replay would serve the fill-time value.
      return ContainsNonDeterministicCall(
          expression->AsCastExpression().Child());
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------
// Cached artifact.
// ---------------------------------------------------------------------------
struct PlanMemorySettings {
  size_t process_limit;
  size_t operator_limit;
  bool always_spill;

  static PlanMemorySettings Current() noexcept {
    const auto& budget = ProcessMemoryBudget::Global();
    return {.process_limit = budget.Limit(),
            .operator_limit = budget.OpMemoryOverrideBytes(),
            .always_spill = budget.AlwaysSpillEnabled()};
  }
  bool operator==(const PlanMemorySettings&) const = default;
};

struct CompiledPlan {
  enum class Kind : uint8_t { kSelect, kInsert, kUpdate, kDelete };

  // Defaulted so the implicit default constructor cannot leave it
  // uninitialized (every caller overwrites it immediately after).
  Kind kind{Kind::kSelect};
  // Plans retain Table/Index objects from the database that produced them.
  // Schema epochs are local to a Database, so an epoch alone cannot prevent a
  // plan compiled for another database instance from being replayed here.
  const Database* database{nullptr};
  uint64_t epoch{0};
  // Optimizer/bytecode toggle bitmask active at compile time
  // (PlanCacheToggleInvalidation.lean).
  uint64_t opt_bits{0};
  PlanMemorySettings memory_settings = PlanMemorySettings::Current();
  // Fill-time parameter values. When `parameter_slots` is non-empty the plan
  // is parametric and these values are only the sniffed bindings from the
  // fill. When it is empty, a non-INSERT hit still requires exact equality.
  std::vector<Value> parameters;
  // Optimizer-resident slots shared with the plan tree, in parameter order.
  // Empty when the shape could not be slotized; replay then requires equal
  // parameters and a miss recompiles without retiring the fingerprint.
  std::vector<Expression> parameter_slots;
  // First successful SELECT replay's elapsed time. Later replays that are
  // both above the floor and several times this baseline drop the entry.
  mutable std::atomic<uint64_t> reuse_baseline_ns{0};
  // A replay that ends in error is not a timing sample, but repeated errors
  // (spill/OOM under memory pressure) still signal a plan that must be
  // replanned instead of being replayed forever. A successful replay clears it.
  mutable std::atomic<uint32_t> reuse_error_streak{0};
  // An immutable SELECT artifact can be shared across workers. Retiring the
  // artifact itself also prevents another worker from reviving a slow plan.
  mutable std::atomic<bool> retired{false};
  // Audited slot-free bounded column read or scalar aggregate. Replay by exact
  // SQL even when there are no expression literals for the template binder.
  bool immutable_exact_read{false};

  // Specialized tiers: the compiled physical plan.
  std::shared_ptr<PlanBase> plan;
  // kUpdate/kDelete target table image captured at fill time; DDL is guarded
  // by the epoch check.
  std::shared_ptr<Table> table;

  // kSelect: every fill-time Table the compiled plan tree borrows. Scan and
  // join plans embed `const Table&` / `const Index&` captured while optimizing
  // (indexes live inside their Table), and each referenced Table is owned only
  // by the fill-time TransactionContext (Database::GetTable deserializes a
  // fresh instance per call; ctx.tables_ holds its sole shared_ptr). Without
  // this retention, replaying a hit after the fill transaction ends walks
  // freed memory -- e.g. IndexScanPlan::EmitExecutor copying the dead Schema.
  // Planning resolves every relation through TransactionContext::GetTable, so
  // the snapshot taken after Optimize is a superset of what the plan touches.
  std::vector<std::shared_ptr<Table>> retained_tables;
  // Same story for TableStatistics: scan plans embed `const TableStatistics&`
  // (e.g. FullScanPlan::stats_), whose shared_ptr lives only in the fill-time
  // ctx.stats_ cache. ANALYZE/DROP erase that cache, so a replay without
  // retention reads freed memory (ASan: heap-use-after-free in
  // TableStatistics::Rows).
  std::vector<std::shared_ptr<TableStatistics>> retained_stats;

  // kSelect: everything the executor-construction tail needs, captured from
  // the fill-time bound statement so hits replay it without re-binding.
  struct SelectShape {
    std::vector<std::string> column_names;
    std::vector<ValueType> column_types;
    std::vector<Expression> order_expressions;
    std::vector<bool> order_ascending;
    // Parallels order_expressions; absent entries mean the engine default.
    std::vector<std::optional<bool>> order_nulls_first;
    std::vector<std::pair<Expression, bool>> sort_keys;
    bool distinct{false};
    bool has_limit{false};
    bool with_ties{false};
    size_t limit{0};
    size_t offset{0};
    size_t visible_columns{0};
    size_t final_select_size{0};
  };
  std::shared_ptr<const SelectShape> select_shape;

  // kInsert: parametric artifact (prepared style). Cells carry ParameterSlot
  // placeholders in SQL-text order; `reorder` maps provided-cell index to
  // destination offset when the statement named explicit columns.
  struct InsertShape {
    std::shared_ptr<Table> table;
    Schema schema;
    std::vector<std::vector<Expression>> cells;
    std::vector<size_t> reorder;  // empty when columns were not named
    bool has_named_columns{false};
  };
  std::shared_ptr<const InsertShape> insert_shape;
};

using CompiledPlanPtr = std::shared_ptr<const CompiledPlan>;

// Bindable SELECT/UPDATE/DELETE plans are thread-local: Bind() mutates their
// slots. INSERT clones its slots per execution. A slot-free bounded ordered
// column read may also use the global cache; replay requires exact parameters.
inline thread_local std::unordered_map<std::string, CompiledPlanPtr>
    thread_compiled_plans;
inline thread_local std::unordered_set<std::string>
    thread_volatile_specialized_plans;
inline constexpr size_t kThreadCompiledPlanCapacity = 256;
inline constexpr size_t kVolatileFingerprintCapacity = 1024;
inline thread_local bool thread_volatile_fingerprints_saturated = false;

// A migrating SQL session exclusively owns its mutable parameter slots.
// The scheduler must allow at most one active scope per session; unrelated
// sessions never share these entries, even when their worker changes.
struct PreparedPlanSessionCache {
  std::unordered_map<std::string, CompiledPlanPtr> plans;
  std::unordered_set<std::string> volatile_fingerprints;
  bool volatile_fingerprints_saturated = false;
};
inline thread_local PreparedPlanSessionCache* active_plan_session = nullptr;
class ScopedPreparedPlanSession {
 public:
  explicit ScopedPreparedPlanSession(PreparedPlanSessionCache* session)
      : previous_(active_plan_session) {
    active_plan_session = session;
  }
  ~ScopedPreparedPlanSession() { active_plan_session = previous_; }
  ScopedPreparedPlanSession(const ScopedPreparedPlanSession&) = delete;
  ScopedPreparedPlanSession& operator=(const ScopedPreparedPlanSession&) =
      delete;
  ScopedPreparedPlanSession(ScopedPreparedPlanSession&&) = delete;
  ScopedPreparedPlanSession& operator=(ScopedPreparedPlanSession&&) = delete;

 private:
  PreparedPlanSessionCache* previous_;
};
inline auto& CurrentCompiledPlans() {
  return (active_plan_session != nullptr) ? active_plan_session->plans
                                          : thread_compiled_plans;
}
inline auto& CurrentVolatileFingerprints() {
  return (active_plan_session != nullptr)
             ? active_plan_session->volatile_fingerprints
             : thread_volatile_specialized_plans;
}

inline bool& CurrentVolatileFingerprintsSaturated() {
  return active_plan_session != nullptr
             ? active_plan_session->volatile_fingerprints_saturated
             : thread_volatile_fingerprints_saturated;
}

// A reused SELECT is planned again when a later execution is slower than
// 50ms and more than 8x the first replay. Point lookups stay cached. A plan
// that becomes pathologically slow for a new literal is dropped.
inline constexpr uint64_t kSelectReuseLatencyFloorNs = 50'000'000ULL;
inline constexpr uint64_t kSelectReuseLatencyFactor = 8;

inline bool SelectReuseExceededThreshold(uint64_t elapsed_ns,
                                         uint64_t baseline_ns) {
  if (baseline_ns == 0 || elapsed_ns <= kSelectReuseLatencyFloorNs) {
    return false;
  }
  // Guard the product against wraparound: a corrupted/overflowed baseline can
  // be larger than UINT64_MAX / factor, in which case no measurable elapsed_ns
  // could ever exceed the true threshold. Letting it wrap would make the
  // safety valve fire on a tiny bogus threshold instead.
  if (baseline_ns >
      std::numeric_limits<uint64_t>::max() / kSelectReuseLatencyFactor) {
    return false;
  }
  return elapsed_ns > baseline_ns * kSelectReuseLatencyFactor;
}

inline bool IsVolatileSpecializedPlan(const std::string& fingerprint) {
  return CurrentVolatileFingerprintsSaturated() ||
         CurrentVolatileFingerprints().contains(fingerprint);
}

// Prepends a fingerprint to the volatile set for the duration of one
// statement compile (used by the DML subquery fold path), returning true
// when the entry was freshly added so the caller can demote it later.
inline bool MarkFingerprintVolatile(const std::string& fingerprint) {
  if (CurrentVolatileFingerprintsSaturated()) {
    return false;
  }
  auto& fingerprints = CurrentVolatileFingerprints();
  if (fingerprints.contains(fingerprint)) {
    return false;
  }
  if (fingerprints.size() >= kVolatileFingerprintCapacity) {
    // Forgetting one mark could resurrect a stale globally cached DML fold.
    // Saturation therefore disables compiled-plan reuse for this session.
    // The flag costs constant space and survives every temporary demotion.
    CurrentVolatileFingerprintsSaturated() = true;
    fingerprints.clear();
    return false;
  }
  return fingerprints.insert(fingerprint).second;
}

inline void DemoteVolatileFingerprint(const std::string& fingerprint,
                                      bool was_fresh) {
  if (was_fresh) {
    CurrentVolatileFingerprints().erase(fingerprint);
  }
}

// A compile that folds session data permanently excludes this fingerprint
// from reuse, including when compilation exits by throwing.
class ScopedFoldFingerprint {  // NOLINT(cppcoreguidelines-special-member-functions):
                               // scope-owned cleanup.

 public:
  ScopedFoldFingerprint(const std::string& fingerprint, const bool& folded)
      : fingerprint_(fingerprint),
        folded_(folded),
        fresh_(!fingerprint.empty() && MarkFingerprintVolatile(fingerprint)) {}
  ~ScopedFoldFingerprint() {
    DemoteVolatileFingerprint(fingerprint_, fresh_ && !folded_);
  }
  ScopedFoldFingerprint(const ScopedFoldFingerprint&) = delete;
  ScopedFoldFingerprint& operator=(const ScopedFoldFingerprint&) = delete;

 private:
  const std::string& fingerprint_;
  const bool& folded_;
  const bool fresh_;
};

// Lean proof: lean/TinylambOpt/ResearchProofs/PlanCacheToggleInvalidation.lean
//   (sound)
[[nodiscard]] inline uint64_t CurrentPlanOptToggleBits() noexcept {
  auto bits = static_cast<uint64_t>(BytecodeOptToggleBits());
  if (!BytecodeEnabled()) {
    bits |= uint64_t{1} << 8U;
  }
  const auto env_on = [](const char* name) noexcept {
    const char* v = ::tinylamb::platform::GetEnv(name);
    return v != nullptr && v[0] != '\0';
  };
  if (env_on("TINYLAMB_DISABLE_QUALIFY_PUSHDOWN")) {
    bits |= uint64_t{1} << 9U;
  }
  if (env_on("TINYLAMB_DISABLE_DISTINCT_ON_TOP1")) {
    bits |= uint64_t{1} << 10U;
  }
  if (env_on("TINYLAMB_DISABLE_OUTER_OR_BRANCHES")) {
    bits |= uint64_t{1} << 11U;
  }
  if (env_on("TINYLAMB_DISABLE_OUTER_FETCH_TIES")) {
    bits |= uint64_t{1} << 12U;
  }
  if (env_on("TINYLAMB_DISABLE_OUTER_JOIN_COUNTS")) {
    bits |= uint64_t{1} << 13U;
  }
  if (env_on("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE")) {
    bits |= uint64_t{1} << 14U;
  }
  if (env_on("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME")) {
    bits |= uint64_t{1} << 15U;
  }
  if (env_on("TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX")) {
    bits |= uint64_t{1} << 16U;
  }
  if (env_on("TINYLAMB_DISABLE_QUANTIFIED_RANGE_SUMMARY")) {
    bits |= uint64_t{1} << 17U;
  }
  if (env_on("TINYLAMB_DISABLE_DEMAND_SCALAR")) {
    bits |= uint64_t{1} << 18U;
  }
  if (env_on("TINYLAMB_DISABLE_SKYLINE_ANTIJOIN")) {
    bits |= uint64_t{1} << 19U;
  }
  if (env_on("TINYLAMB_DISABLE_INTERVAL_COUNT_SWEEP")) {
    bits |= uint64_t{1} << 20U;
  }
  if (env_on("TINYLAMB_DISABLE_PACKED_ORDER_KEY")) {
    bits |= uint64_t{1} << 21U;
  }
  if (env_on("TINYLAMB_DISABLE_WINDOW_HALO")) {
    bits |= uint64_t{1} << 22U;
  }
  if (env_on("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION")) {
    bits |= uint64_t{1} << 23U;
  }
  if (env_on("TINYLAMB_DISABLE_BLOCK_KLEENE")) {
    bits |= uint64_t{1} << 24U;
  }
  return bits;
}

[[nodiscard]] inline std::string PlanCacheKeyWithOptBits(
    const std::string& fingerprint, uint64_t opt_bits) {
  if (opt_bits == 0) {
    return fingerprint;
  }
  return fingerprint + "\x1fopt:" + std::to_string(opt_bits);
}

inline CompiledPlanPtr FindThreadCompiledPlan(
    const std::string& fingerprint, const Database* database, uint64_t epoch,
    uint64_t opt_bits = CurrentPlanOptToggleBits()) {
  const std::string key = PlanCacheKeyWithOptBits(fingerprint, opt_bits);
  const auto found = CurrentCompiledPlans().find(key);
  if (found == CurrentCompiledPlans().end()) {
    return nullptr;
  }
  if (found->second->database != database || found->second->epoch != epoch ||
      found->second->opt_bits != opt_bits ||
      found->second->memory_settings != PlanMemorySettings::Current()) {
    CurrentCompiledPlans().erase(found);
    PlanCacheStats().epoch_invalidations.fetch_add(1,
                                                   std::memory_order_relaxed);
    return nullptr;
  }
  PlanCacheStats().hits.fetch_add(1, std::memory_order_relaxed);
  return found->second;
}

inline void RememberThreadCompiledPlan(const std::string& fingerprint,
                                       CompiledPlanPtr plan) {
  const uint64_t opt_bits = plan ? plan->opt_bits : CurrentPlanOptToggleBits();
  const std::string key = PlanCacheKeyWithOptBits(fingerprint, opt_bits);
  if (CurrentCompiledPlans().size() >= ((active_plan_session != nullptr)
                                            ? 64
                                            : kThreadCompiledPlanCapacity) &&
      !CurrentCompiledPlans().contains(key)) {
    CurrentCompiledPlans().clear();
  }
  CurrentCompiledPlans().insert_or_assign(key, std::move(plan));
}

inline void StoreThreadCompiledPlan(const std::string& fingerprint,
                                    CompiledPlanPtr plan) {
  RememberThreadCompiledPlan(fingerprint, std::move(plan));
  PlanCacheStats().fills.fetch_add(1, std::memory_order_relaxed);
}

inline void ForgetThreadCompiledPlan(
    const std::string& fingerprint,
    uint64_t opt_bits = CurrentPlanOptToggleBits()) {
  CurrentCompiledPlans().erase(PlanCacheKeyWithOptBits(fingerprint, opt_bits));
}

// ---------------------------------------------------------------------------
// Process-global sharded LRU cache (mirrors the sql_template shard layout).
// ---------------------------------------------------------------------------
class PreparedPlanCache {
 public:
  static constexpr size_t kShards = 16;
  static constexpr size_t kCapacity = 256;
  static constexpr size_t kCapacityPerShard = kCapacity / kShards;

  static PreparedPlanCache& Instance() {
    // The function-local static is initialized thread-safely and every access
    // to its contents goes through a per-shard mutex (Find/Store/Clear), so it
    // is safe to share across threads without thread_local or an outer lock.
    static PreparedPlanCache cache;
    return cache;
  }

  // Returns the entry for `fingerprint` when present AND stamped with
  // `epoch` and `opt_bits`; stale entries are dropped here.
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/PlanCacheToggleInvalidation.lean
  //   (sound)
  CompiledPlanPtr Find(const std::string& fingerprint, const Database* database,
                       uint64_t epoch,
                       uint64_t opt_bits = CurrentPlanOptToggleBits()) {
    const std::string key = PlanCacheKeyWithOptBits(fingerprint, opt_bits);
    Shard& shard = shard_for(key);
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.index.find(key);
    if (found == shard.index.end()) {
      PlanCacheStats().misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    if (found->second->plan->database != database ||
        found->second->plan->epoch != epoch ||
        found->second->plan->opt_bits != opt_bits ||
        found->second->plan->memory_settings != PlanMemorySettings::Current()) {
      shard.lru.erase(found->second);
      shard.index.erase(found);
      PlanCacheStats().epoch_invalidations.fetch_add(1,
                                                     std::memory_order_relaxed);
      PlanCacheStats().misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    shard.lru.splice(shard.lru.begin(), shard.lru, found->second);
    PlanCacheStats().hits.fetch_add(1, std::memory_order_relaxed);
    return found->second->plan;
  }

  void Store(const std::string& fingerprint, CompiledPlanPtr plan) {
    const uint64_t opt_bits =
        plan ? plan->opt_bits : CurrentPlanOptToggleBits();
    const std::string key = PlanCacheKeyWithOptBits(fingerprint, opt_bits);
    Shard& shard = shard_for(key);
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.index.find(key);
    if (found != shard.index.end()) {
      found->second->plan = std::move(plan);
      shard.lru.splice(shard.lru.begin(), shard.lru, found->second);
      PlanCacheStats().fills.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    shard.lru.push_front(
        CacheEntry{.fingerprint = key, .plan = std::move(plan)});
    shard.index.emplace(key, shard.lru.begin());
    if (shard.lru.size() > kCapacityPerShard) {
      shard.index.erase(shard.lru.back().fingerprint);
      shard.lru.pop_back();
      PlanCacheStats().evictions.fetch_add(1, std::memory_order_relaxed);
    }
    PlanCacheStats().fills.fetch_add(1, std::memory_order_relaxed);
  }

  void Clear() {
    for (Shard& shard : shards_) {
      std::scoped_lock lock(shard.mutex);
      shard.lru.clear();
      shard.index.clear();
    }
  }

 private:
  struct CacheEntry {
    std::string fingerprint;
    CompiledPlanPtr plan;
  };
  struct Shard {
    std::mutex mutex;
    std::list<CacheEntry> lru;  // front = most recently used
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> index;
  };

  Shard& shard_for(const std::string& fingerprint) {
    return shards_[std::hash<std::string>{}(fingerprint) % kShards];
  }

  std::array<Shard, kShards> shards_{};
};

inline void StoreCompiledPlan(const std::string& fingerprint,
                              CompiledPlanPtr plan) {
  PreparedPlanCache::Instance().Store(fingerprint, std::move(plan));
}

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_CACHE_HPP
