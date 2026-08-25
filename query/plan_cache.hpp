/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_PLAN_CACHE_HPP
#define TINYLAMB_PLAN_CACHE_HPP

// Phase 2-1: compiled-plan cache ("prepared plans"), docs/tpcc-improvements.md.
//
// Key:    SQL fingerprint + database schema/statistics epoch.
// Value:  a CompiledPlan, one of
//           (a) kSelect/kUpdate/kDelete: the specialized Plan compiled at fill
//               time, replayed only while incoming parameters equal the
//               fill-time values (identical parameters guarantee an identical
//               bound statement, so baked index ranges stay correct);
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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <mutex>
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
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

class Table;

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

// ---------------------------------------------------------------------------
// PreparedValues: one instance per execution, holding that execution's bound
// parameter values. ParameterSlot nodes read through a shared_ptr to it, so
// concurrent executions of one cached artifact never share mutable state.
// ---------------------------------------------------------------------------
class PreparedValues {
 public:
  explicit PreparedValues(std::vector<Value> values)
      : values_(std::move(values)) {}
  [[nodiscard]] const Value& at(size_t index) const { return values_[index]; }

 private:
  std::vector<Value> values_;
};

// A placeholder standing in for one bindable literal inside a cached INSERT
// artifact. Evaluate() returns this execution's value for the slot.
//
// Type() masquerades as kConstantValue so existing expression walkers treat
// the node as a leaf. INVARIANT: these nodes must remain confined to plan-cache
// artifacts; nothing may call AsConstantValue() on them (it would downcast a
// ParameterSlot to ConstantValue -- undefined behavior). The artifact is only
// ever consumed by Evaluate() in the insert fast path and by ToString/Dump.
class ParameterSlot final : public ExpressionBase {
 public:
  ParameterSlot(size_t slot_index, ValueType expected,
                std::shared_ptr<const PreparedValues> values)
      : slot_index_(slot_index),
        expected_(expected),
        values_(std::move(values)) {}

  [[nodiscard]] size_t Index() const { return slot_index_; }

  // Rebinds this slot (inside a cloned subtree) to another execution's values.
  [[nodiscard]] Expression Rebind(
      const std::shared_ptr<const PreparedValues>& values) const {
    return std::make_shared<ParameterSlot>(slot_index_, expected_, values);
  }

  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kConstantValue;
  }
  [[nodiscard]] Value Evaluate(const Row& /*row*/,
                               const Schema& /*schema*/) const override {
    return values_->at(slot_index_);
  }
  [[nodiscard]] Value Evaluate(const Row* /*left*/, const Schema& /*left_schema*/,
                               const Row* /*right*/,
                               const Schema& /*right_schema*/) const override {
    return values_->at(slot_index_);
  }
  [[nodiscard]] tinylamb::Type ResultType(const Schema&) const override {
    return TypeFor(expected_);
  }
  [[nodiscard]] tinylamb::Type ResultType(const Schema&,
                                          const Schema&) const override {
    return TypeFor(expected_);
  }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override {
    return {};
  }
  [[nodiscard]] std::string ToString() const override {
    return "<?" + std::to_string(slot_index_) + ">";
  }
  void Dump(std::ostream& o) const override { o << ToString(); }

 private:
  static tinylamb::Type TypeFor(ValueType value_type) {
    switch (value_type) {
      case ValueType::kInt64:
        return tinylamb::Type(TypeTag::kBigInt);
      case ValueType::kDouble:
        return tinylamb::Type(TypeTag::kDouble);
      case ValueType::kVarChar:
        return tinylamb::Type(TypeTag::kVarChar);
      case ValueType::kDate:
        return tinylamb::Type(TypeTag::kDate);
      case ValueType::kArray:
        return tinylamb::Type(TypeTag::kArray);
      default:
        return tinylamb::Type(TypeTag::kInvalid);
    }
  }

  size_t slot_index_;
  ValueType expected_;
  std::shared_ptr<const PreparedValues> values_;
};

// True when any bindable literal (the kinds ExtractSqlTemplate turns into
// parameters) sits under `expression`.
inline bool IsBindableLiteral(const Value& value) {
  return !value.IsNull() &&
         (value.type == ValueType::kInt64 || value.type == ValueType::kDouble ||
          value.type == ValueType::kVarChar || value.type == ValueType::kDate);
}

inline bool ContainsParameterSlot(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue:
      return dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr;
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return ContainsParameterSlot(binary.Left()) ||
             ContainsParameterSlot(binary.Right());
    }
    case TypeTag::kUnaryExp:
      return ContainsParameterSlot(expression->AsUnaryExpression().Child());
    case TypeTag::kAggregateExp:
      {
        const auto& aggregate = expression->AsAggregateExpression();
        if (ContainsParameterSlot(aggregate.Child()) ||
            ContainsParameterSlot(aggregate.HavingCondition()) ||
            ContainsParameterSlot(aggregate.WhereFilter()) ||
            ContainsParameterSlot(aggregate.SecondaryArg())) {
          return true;
        }
        for (const auto& term : aggregate.InnerOrderBy()) {
          if (ContainsParameterSlot(term.expression)) { return true; }
        }
        return false;
      }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      for (const auto& clause : searched.when_clauses_) {
        if (ContainsParameterSlot(clause.first) ||
            ContainsParameterSlot(clause.second)) {
          return true;
        }
      }
      return ContainsParameterSlot(searched.else_clause_);
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (ContainsParameterSlot(in.child_)) {
        return true;
      }
      for (const Expression& item : in.list_) {
        if (ContainsParameterSlot(item)) {
          return true;
        }
      }
      return false;
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& arg :
           expression->AsFunctionCallExpression().Args()) {
        if (ContainsParameterSlot(arg)) {
          return true;
        }
      }
      return false;
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        if (ContainsParameterSlot(element)) {
          return true;
        }
      }
      return false;
    default:
      return false;
  }
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
      return UnaryExpressionExp(
          CloneWithPreparedValues(unary.Child(), values), unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(),
          CloneWithPreparedValues(aggregate.Child(), values),
          aggregate.Distinct());
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        rebuilt->SetHaving(
            aggregate.Having(),
            CloneWithPreparedValues(aggregate.HavingCondition(), values));
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(
            CloneWithPreparedValues(aggregate.WhereFilter(), values));
      }
      if (!aggregate.InnerOrderBy().empty()) {
        std::vector<WindowOrderTerm> terms;
        terms.reserve(aggregate.InnerOrderBy().size());
        for (const auto& term : aggregate.InnerOrderBy()) {
          terms.push_back(WindowOrderTerm{
              CloneWithPreparedValues(term.expression, values),
              term.ascending, term.nulls_first});
        }
        rebuilt->SetInnerOrderBy(std::move(terms));
      }
      if (aggregate.InnerLimit().has_value()) {
        rebuilt->SetInnerLimit(aggregate.InnerLimit());
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(
            CloneWithPreparedValues(aggregate.SecondaryArg(), values));
      }
      return rebuilt;
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        clauses.emplace_back(CloneWithPreparedValues(clause.first, values),
                             CloneWithPreparedValues(clause.second, values));
      }
      return CaseExpressionExp(std::move(clauses),
                               CloneWithPreparedValues(searched.else_clause_,
                                                       values));
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
      return FunctionCallExp(call.FuncName(), std::move(args),
                             call.IsCanonicalIf());
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
      for (const Expression& arg : call.Args()) {
        if (ContainsNonDeterministicCall(arg)) {
          return true;
        }
      }
      return false;
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
      for (const Expression& item : in.list_) {
        if (ContainsNonDeterministicCall(item)) {
          return true;
        }
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

// Replaces bindable literals with ParameterSlot placeholders (fill time).
// `ok` is cleared on unsupported shapes (subqueries): callers then skip
// caching and the statement keeps using the legacy path forever.
inline Expression SlotizeLiterals(  // NOLINT(misc-no-recursion)
    const Expression& expression, size_t* slot_cursor, bool* ok) {
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      const Value current = expression->AsConstantValue().GetValue();
      if (!IsBindableLiteral(current)) {
        return expression;
      }
      return std::make_shared<ParameterSlot>(
          (*slot_cursor)++, current.type,
          std::make_shared<PreparedValues>(std::vector<Value>{}));
    }
    case TypeTag::kColumnValue:
      return expression;
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      return BinaryExpressionExp(
          SlotizeLiterals(binary.Left(), slot_cursor, ok), binary.Op(),
          SlotizeLiterals(binary.Right(), slot_cursor, ok));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(
          SlotizeLiterals(unary.Child(), slot_cursor, ok), unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      auto rebuilt = std::make_shared<AggregateExpression>(
          aggregate.GetType(),
          SlotizeLiterals(aggregate.Child(), slot_cursor, ok),
          aggregate.Distinct());
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        rebuilt->SetHaving(
            aggregate.Having(),
            SlotizeLiterals(aggregate.HavingCondition(), slot_cursor, ok));
      }
      if (aggregate.WhereFilter()) {
        rebuilt->SetWhereFilter(
            SlotizeLiterals(aggregate.WhereFilter(), slot_cursor, ok));
      }
      if (!aggregate.InnerOrderBy().empty()) {
        std::vector<WindowOrderTerm> terms;
        terms.reserve(aggregate.InnerOrderBy().size());
        for (const auto& term : aggregate.InnerOrderBy()) {
          terms.push_back(WindowOrderTerm{
              SlotizeLiterals(term.expression, slot_cursor, ok),
              term.ascending, term.nulls_first});
        }
        rebuilt->SetInnerOrderBy(std::move(terms));
      }
      if (aggregate.InnerLimit().has_value()) {
        rebuilt->SetInnerLimit(aggregate.InnerLimit());
      }
      if (aggregate.SecondaryArg()) {
        rebuilt->SetSecondaryArg(
            SlotizeLiterals(aggregate.SecondaryArg(), slot_cursor, ok));
      }
      return rebuilt;
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        clauses.emplace_back(SlotizeLiterals(clause.first, slot_cursor, ok),
                             SlotizeLiterals(clause.second, slot_cursor, ok));
        if (!*ok) {
          return expression;
        }
      }
      return CaseExpressionExp(
          std::move(clauses),
          SlotizeLiterals(searched.else_clause_, slot_cursor, ok));
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(SlotizeLiterals(item, slot_cursor, ok));
        if (!*ok) {
          return expression;
        }
      }
      return InExpressionExp(SlotizeLiterals(in.child_, slot_cursor, ok),
                             std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (const Expression& arg : call.Args()) {
        args.push_back(SlotizeLiterals(arg, slot_cursor, ok));
        if (!*ok) {
          return expression;
        }
      }
      return FunctionCallExp(call.FuncName(), std::move(args),
                             call.IsCanonicalIf());
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(SlotizeLiterals(element, slot_cursor, ok));
        if (!*ok) {
          return expression;
        }
      }
      return ArrayExpressionExp(std::move(elements), array.ElementSqlType());
    }
    case TypeTag::kIntervalExp: {
      const auto& interval = expression->AsIntervalExpression();
      return IntervalExpressionExp(interval.Amount(), interval.Unit());
    }
    default:
      *ok = false;  // e.g. kQueryExp: not expressible as a flat slot template.
      return expression;
  }
}

// ---------------------------------------------------------------------------
// Cached artifact.
// ---------------------------------------------------------------------------
struct CompiledPlan {
  enum class Kind : uint8_t { kSelect, kInsert, kUpdate, kDelete };

  Kind kind;
  uint64_t epoch{0};
  // Fill-time parameter values for the specialized tiers (kSelect/kUpdate/
  // kDelete). A hit requires exact equality: identical parameters produce an
  // identical bound statement, so the specialized plan's baked constants and
  // index ranges remain correct.
  std::vector<Value> parameters;

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

  // kSelect: everything the executor-construction tail needs, captured from
  // the fill-time bound statement so hits replay it without re-binding.
  struct SelectShape {
    std::vector<std::string> column_names;
    std::vector<Expression> order_expressions;
    std::vector<bool> order_ascending;
    std::vector<std::optional<bool>> order_nulls_first;
    struct SortKey {
      Expression expression;
      bool ascending{true};
      std::optional<bool> nulls_first;
    };
    std::vector<SortKey> sort_keys;
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

// Constant-specialized SELECT/UPDATE/DELETE plans bake their literal bounds
// into the plan and therefore almost never match another OLTP request. Keep
// those artifacts local to the worker that compiled them; after repeated
// literal churn, stop replacing the artifact for that fingerprint.
inline thread_local std::unordered_map<std::string, CompiledPlanPtr>
    thread_compiled_plans;
inline thread_local std::unordered_map<std::string, uint8_t>
    thread_specialized_mismatches;
inline thread_local std::unordered_set<std::string>
    thread_volatile_specialized_plans;
inline constexpr size_t kThreadCompiledPlanCapacity = 256;

inline bool IsVolatileSpecializedPlan(const std::string& fingerprint) {
  return thread_volatile_specialized_plans.contains(fingerprint);
}

inline void NoteSpecializedParameterMismatch(const std::string& fingerprint) {
  uint8_t& mismatches = thread_specialized_mismatches[fingerprint];
  if (++mismatches < 4) { return; }
  thread_volatile_specialized_plans.insert(fingerprint);
  thread_compiled_plans.erase(fingerprint);
}

inline CompiledPlanPtr FindThreadCompiledPlan(const std::string& fingerprint,
                                              uint64_t epoch) {
  const auto found = thread_compiled_plans.find(fingerprint);
  if (found == thread_compiled_plans.end()) { return nullptr;
}
  if (found->second->epoch != epoch) {
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
  CompiledPlanPtr Find(const std::string& fingerprint, uint64_t epoch) {
    Shard& shard = shard_for(fingerprint);
    std::scoped_lock lock(shard.mutex);
    const auto found = shard.index.find(fingerprint);
    if (found == shard.index.end()) {
      PlanCacheStats().misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    if (found->second->plan->epoch != epoch) {
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
    shard.lru.push_front(CacheEntry{fingerprint, std::move(plan)});
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

  Shard shards_[kShards];
};

inline void StoreCompiledPlan(const std::string& fingerprint,
                              CompiledPlanPtr plan) {
  PreparedPlanCache::Instance().Store(fingerprint, std::move(plan));
}

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_CACHE_HPP
