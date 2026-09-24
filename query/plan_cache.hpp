/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_PLAN_CACHE_HPP
#define TINYLAMB_PLAN_CACHE_HPP

// Phase 2-1: compiled-plan cache ("prepared plans"), docs/next-actions.md.
//
// Key:    SQL fingerprint + database schema/statistics epoch.
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
// never results, so it does not need its own invalidation trigger.
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
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/parameter_slot.hpp"
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
};

inline PlanCacheCounters& PlanCacheStats() {
  static PlanCacheCounters counters;
  return counters;
}

// Rebuilds `expression` so every ParameterSlot reads from `values`. Slot-free
// subtrees are shared (trees are immutable); only nodes on paths carrying
// slots are copied.
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
      const auto& aggregate = expression->AsAggregateExpression();
      return AggregateExpressionExp(
          aggregate.GetType(),
          CloneWithPreparedValues(aggregate.Child(), values),
          aggregate.Distinct());
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
    default:
      return expression;
  }
}

// True when evaluating `expression` per row could yield different results
// between executions of the same cached artifact (currently CURRENT_TIMESTAMP;
// function names arrive lowercased from the frontend).
inline bool ContainsNonDeterministicCall(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      if (call.FuncName() == "current_timestamp") {
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
    case TypeTag::kAggregateExp:
      return ContainsNonDeterministicCall(
          expression->AsAggregateExpression().Child());
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
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------
// Cached artifact.
// ---------------------------------------------------------------------------
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
    std::vector<Expression> order_expressions;
    std::vector<bool> order_ascending;
    // Parallels order_expressions; absent entries mean the engine default.
    std::vector<std::optional<bool>> order_nulls_first;
    std::vector<std::pair<Expression, bool>> sort_keys;
    bool distinct{false};
    bool has_limit{false};
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

// SELECT/UPDATE/DELETE plans are thread-local: Bind() mutates the slots in
// the cached tree, so two workers must not share one artifact. INSERT stays
// in the process-global cache because each execution clones its slots.
inline thread_local std::unordered_map<std::string, CompiledPlanPtr>
    thread_compiled_plans;
inline thread_local std::unordered_set<std::string>
    thread_volatile_specialized_plans;
inline constexpr size_t kThreadCompiledPlanCapacity = 256;

// A reused SELECT is planned again when a later execution is slower than
// 50ms and at least 8x the first replay. Point lookups stay cached. A plan
// that becomes pathologically slow for a new literal is dropped.
inline constexpr uint64_t kSelectReuseLatencyFloorNs = 50'000'000ULL;
inline constexpr uint64_t kSelectReuseLatencyFactor = 8;

inline bool SelectReuseExceededThreshold(uint64_t elapsed_ns,
                                         uint64_t baseline_ns) {
  if (baseline_ns == 0 || elapsed_ns <= kSelectReuseLatencyFloorNs) {
    return false;
  }
  return elapsed_ns > baseline_ns * kSelectReuseLatencyFactor;
}

inline bool IsVolatileSpecializedPlan(const std::string& fingerprint) {
  return thread_volatile_specialized_plans.contains(fingerprint);
}

// Prepends a fingerprint to the volatile set for the duration of one
// statement compile (used by the DML subquery fold path), returning true
// when the entry was freshly added so the caller can demote it later.
inline bool MarkFingerprintVolatile(const std::string& fingerprint) {
  return thread_volatile_specialized_plans.insert(fingerprint).second;
}

inline void DemoteVolatileFingerprint(const std::string& fingerprint,
                                      bool was_fresh) {
  if (was_fresh) {
    thread_volatile_specialized_plans.erase(fingerprint);
  }
}

inline CompiledPlanPtr FindThreadCompiledPlan(const std::string& fingerprint,
                                              const Database* database,
                                              uint64_t epoch) {
  const auto found = thread_compiled_plans.find(fingerprint);
  if (found == thread_compiled_plans.end()) {
    return nullptr;
  }
  if (found->second->database != database || found->second->epoch != epoch) {
    thread_compiled_plans.erase(found);
    PlanCacheStats().epoch_invalidations.fetch_add(1,
                                                   std::memory_order_relaxed);
    return nullptr;
  }
  PlanCacheStats().hits.fetch_add(1, std::memory_order_relaxed);
  return found->second;
}

inline void RememberThreadCompiledPlan(const std::string& fingerprint,
                                       CompiledPlanPtr plan) {
  if (thread_compiled_plans.size() >= kThreadCompiledPlanCapacity &&
      !thread_compiled_plans.contains(fingerprint)) {
    thread_compiled_plans.clear();
  }
  thread_compiled_plans.insert_or_assign(fingerprint, std::move(plan));
}

inline void StoreThreadCompiledPlan(const std::string& fingerprint,
                                    CompiledPlanPtr plan) {
  RememberThreadCompiledPlan(fingerprint, std::move(plan));
  PlanCacheStats().fills.fetch_add(1, std::memory_order_relaxed);
}

inline void ForgetThreadCompiledPlan(const std::string& fingerprint) {
  thread_compiled_plans.erase(fingerprint);
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
    static PreparedPlanCache cache;
    return cache;
  }

  // Returns the entry for `fingerprint` when present AND stamped with
  // `epoch`; stale entries are dropped here (lazy invalidation).
  CompiledPlanPtr Find(const std::string& fingerprint, const Database* database,
                       uint64_t epoch) {
    Shard& shard = shard_for(fingerprint);
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.index.find(fingerprint);
    if (found == shard.index.end()) {
      PlanCacheStats().misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    if (found->second->plan->database != database ||
        found->second->plan->epoch != epoch) {
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
    Shard& shard = shard_for(fingerprint);
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.index.find(fingerprint);
    if (found != shard.index.end()) {
      found->second->plan = std::move(plan);
      shard.lru.splice(shard.lru.begin(), shard.lru, found->second);
      return;
    }
    shard.lru.push_front(
        CacheEntry{.fingerprint = fingerprint, .plan = std::move(plan)});
    shard.index.emplace(fingerprint, shard.lru.begin());
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
