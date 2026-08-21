/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <deque>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "database/transaction_context.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "parser/ast.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/date.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"

namespace tinylamb {
namespace {

constexpr size_t kSpillPartitions = 32;

struct ExecutionRuntime;
thread_local ExecutionRuntime* active_runtime = nullptr;
void NoteRelationSpill();

struct Relation {
  Schema schema;
  std::vector<Row> rows;
  std::shared_ptr<SpillFile> spill;
  std::shared_ptr<SpillFile> spill_tail_;
  size_t charged_bytes_{0};
  size_t hash_joins{0};
  size_t hybrid_hash_joins{0};
  size_t in_memory_hash_joins{0};
  size_t nested_loop_joins{0};
  size_t join_comparisons{0};
  size_t peak_intermediate_rows{0};
  size_t spilled_rows_{0};

  Relation() = default;
  Relation(const Relation& other)
      : schema(other.schema),
        rows(other.rows),
        spill(other.spill),
        spill_tail_(other.spill_tail_),
        hash_joins(other.hash_joins),
        hybrid_hash_joins(other.hybrid_hash_joins),
        in_memory_hash_joins(other.in_memory_hash_joins),
        nested_loop_joins(other.nested_loop_joins),
        join_comparisons(other.join_comparisons),
        peak_intermediate_rows(other.peak_intermediate_rows),
        spilled_rows_(other.spilled_rows_) {
    for (const Row& row : rows) {
      charged_bytes_ += EstimateRowBytes(row);
    }
    if (charged_bytes_ != 0) {
      QueryMemoryBudget::Global().ReserveForced(charged_bytes_);
    }
  }
  Relation& operator=(const Relation& other) {
    if (this != &other) {
      ReleaseCharge();
      schema = other.schema;
      rows = other.rows;
      spill = other.spill;
      spill_tail_ = other.spill_tail_;
      hash_joins = other.hash_joins;
      hybrid_hash_joins = other.hybrid_hash_joins;
      in_memory_hash_joins = other.in_memory_hash_joins;
      nested_loop_joins = other.nested_loop_joins;
      join_comparisons = other.join_comparisons;
      peak_intermediate_rows = other.peak_intermediate_rows;
      spilled_rows_ = other.spilled_rows_;
      charged_bytes_ = 0;
      for (const Row& row : rows) {
        charged_bytes_ += EstimateRowBytes(row);
      }
      if (charged_bytes_ != 0) {
        QueryMemoryBudget::Global().ReserveForced(charged_bytes_);
      }
    }
    return *this;
  }
  Relation(Relation&& other) noexcept
      : schema(std::move(other.schema)),
        rows(std::move(other.rows)),
        spill(std::move(other.spill)),
        spill_tail_(std::move(other.spill_tail_)),
        charged_bytes_(other.charged_bytes_),
        hash_joins(other.hash_joins),
        hybrid_hash_joins(other.hybrid_hash_joins),
        in_memory_hash_joins(other.in_memory_hash_joins),
        nested_loop_joins(other.nested_loop_joins),
        join_comparisons(other.join_comparisons),
        peak_intermediate_rows(other.peak_intermediate_rows),
        spilled_rows_(other.spilled_rows_) {
    other.charged_bytes_ = 0;
  }
  Relation& operator=(Relation&& other) noexcept {
    if (this != &other) {
      ReleaseCharge();
      schema = std::move(other.schema);
      rows = std::move(other.rows);
      spill = std::move(other.spill);
      spill_tail_ = std::move(other.spill_tail_);
      charged_bytes_ = other.charged_bytes_;
      other.charged_bytes_ = 0;
      hash_joins = other.hash_joins;
      hybrid_hash_joins = other.hybrid_hash_joins;
      in_memory_hash_joins = other.in_memory_hash_joins;
      nested_loop_joins = other.nested_loop_joins;
      join_comparisons = other.join_comparisons;
      peak_intermediate_rows = other.peak_intermediate_rows;
      spilled_rows_ = other.spilled_rows_;
    }
    return *this;
  }
  ~Relation() { ReleaseCharge(); }

  void ReleaseCharge() {
    if (charged_bytes_ != 0) {
      QueryMemoryBudget::Global().Release(charged_bytes_);
      charged_bytes_ = 0;
    }
  }

  void EnsureSpill() {
    if (spill) {
      return;
    }
    NoteRelationSpill();
    spill = std::make_shared<SpillFile>();
    for (const Row& row : rows) {
      spill->Append(row);
    }
    spill->FinishWriting();
    rows.clear();
    rows.shrink_to_fit();
    ReleaseCharge();
    spill_tail_ = std::make_shared<SpillFile>();
  }

  void AddRow(Row row) {
    const size_t bytes = EstimateRowBytes(row);
    if (spill_tail_ || !QueryMemoryBudget::Global().CanReserve(bytes)) {
      if (!spill_tail_) {
        EnsureSpill();
      }
      spill_tail_->Append(row);
      peak_intermediate_rows =
          std::max(peak_intermediate_rows, rows.size() + spilled_rows_ + 1);
      ++spilled_rows_;
      return;
    }
    QueryMemoryBudget::Global().ReserveForced(bytes);
    charged_bytes_ += bytes;
    rows.push_back(std::move(row));
    peak_intermediate_rows = std::max(peak_intermediate_rows, rows.size());
  }

  void FinishSpill() {
    if (spill_tail_) {
      spill_tail_->FinishWriting();
    }
  }

  template <typename Fn>
  void ForEachRow(Fn&& fn) {
    for (const Row& row : rows) {
      fn(row);
    }
    if (spill) {
      spill->ForEachRow(fn);
    }
    if (spill_tail_) {
      spill_tail_->ForEachRow(fn);
    }
  }

  [[nodiscard]] bool HasSpill() const {
    return spill != nullptr || spill_tail_ != nullptr;
  }

  [[nodiscard]] size_t TotalRows() const {
    size_t total = rows.size();
    if (spill) total += spill->Count();
    if (spill_tail_) total += spill_tail_->Count();
    return total;
  }
};

struct Scope {
  const Row* row{nullptr};
  const Schema* schema{nullptr};
  const Scope* outer{nullptr};
};

using CteMap = std::unordered_map<std::string, Relation>;

struct CorrelatedIndex {
  Schema schema;
  std::unordered_multimap<std::string, Row> rows;
  std::vector<slot_t> local_columns;
  std::vector<ColumnName> outer_columns;
  std::vector<ColumnName> cache_outer_columns;
  std::unordered_map<std::string, Relation> cached_results;
  bool preaggregated{false};
};

struct ExecutionRuntime {
  std::unordered_map<std::string, Relation> base_relations;
  std::unordered_set<std::string> reusable_base_relations;
  // Shared column projection for tables referenced more than once so every
  // alias hits the same base_relations cache entry.
  std::unordered_map<std::string, std::vector<slot_t>> reusable_projections;
  // Integer IN-list filters pushed from selective join drivers; reused by
  // later scans of the same base table (including correlated subqueries).
  std::unordered_map<std::string, std::unordered_set<int64_t>> table_key_filters;
  std::unordered_map<std::string, slot_t> table_key_filter_columns;
  const SelectStatement* root_statement{nullptr};
  std::unordered_map<const SelectStatement*, std::unique_ptr<CorrelatedIndex>>
      correlated_indexes;
  std::unordered_set<const SelectStatement*> unindexable_queries;
  std::unordered_map<const SelectStatement*, Relation> uncorrelated_results;
  std::unordered_map<const SelectStatement*, std::unordered_set<Value>>
      uncorrelated_membership;
  std::unordered_set<const SelectStatement*> noncacheable_queries;
  size_t correlated_index_builds{0};
  size_t correlated_index_probes{0};
  size_t correlated_result_cache_hits{0};
  size_t uncorrelated_cache_hits{0};
  size_t uncorrelated_hash_builds{0};
  size_t uncorrelated_hash_probes{0};
  double scan_ms{0};
  double filter_ms{0};
  double join_ms{0};
  double project_ms{0};
  double sort_ms{0};
  size_t base_scan_cache_hits{0};
  size_t aggregate_input_rows{0};
  size_t aggregate_groups{0};
  size_t aggregate_updates{0};
  size_t scan_rows{0};
  size_t scan_output_rows{0};
  size_t scan_values_decoded{0};
  size_t scan_values_available{0};
  size_t relation_spills{0};
  size_t key_filter_scans{0};
  size_t key_filter_keys{0};
  size_t key_filter_rejected{0};
};

void NoteRelationSpill() {
  if (active_runtime) {
    ++active_runtime->relation_spills;
  }
}

double ElapsedMs(std::chrono::steady_clock::time_point begin) {
  return std::chrono::duration<double, std::milli>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

void CountStatementTables(const SelectStatement& statement,
                          std::unordered_map<std::string, size_t>* counts);

void CountExpressionTables(const Expression& expression,
                           std::unordered_map<std::string, size_t>* counts) {
  if (!expression) return;
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    CountStatementTables(*query.Query(), counts);
    CountExpressionTables(query.Test(), counts);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CountExpressionTables(child, counts);
  }
}

void CountStatementTables(const SelectStatement& statement,
                          std::unordered_map<std::string, size_t>* counts) {
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)name;
    CountStatementTables(*query, counts);
  }
  for (const SelectSource& source : statement.Sources()) {
    if (source.query) {
      CountStatementTables(*source.query, counts);
    } else if (!statement.WithQueries().contains(source.table)) {
      ++(*counts)[source.table];
    }
    CountExpressionTables(source.join_condition, counts);
  }
  CountExpressionTables(statement.WhereClause(), counts);
  for (const NamedExpression& item : statement.SelectList()) {
    CountExpressionTables(item.expression, counts);
  }
  for (const Expression& expression : statement.GroupBy()) {
    CountExpressionTables(expression, counts);
  }
  CountExpressionTables(statement.Having(), counts);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CountExpressionTables(term.expression, counts);
  }
}

Relation ExecuteQuery(TransactionContext& context,
                      const SelectStatement& statement, const Scope* outer,
                      const CteMap& inherited_ctes);
Relation FinishQuery(TransactionContext& context,
                     const SelectStatement& statement, Relation input,
                     const Scope* outer, const CteMap& ctes,
                     bool apply_where = true);
std::optional<Relation> ExecuteCorrelatedSingleSource(
    TransactionContext& context, const SelectStatement& statement,
    const Scope& outer, const CteMap& ctes);
const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes);
bool ExpressionUsesOnlyScopes(TransactionContext& context,
                              const Expression& expression,
                              const std::vector<Relation>& sources,
                              const CteMap& ctes);
bool StatementUsesOnlyScopes(TransactionContext& context,
                             const SelectStatement& statement,
                             const std::vector<Relation>& outer_sources,
                             const CteMap& ctes);

bool ContainsOnlyUncorrelatedQueries(TransactionContext& context,
                                     const Expression& expression,
                                     const CteMap& ctes) {
  if (!expression) return true;
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    return StatementUsesOnlyScopes(context, *query.Query(), {}, ctes) &&
           ContainsOnlyUncorrelatedQueries(context, query.Test(), ctes);
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [&](const Expression& child) {
        return ContainsOnlyUncorrelatedQueries(context, child, ctes);
      });
}

bool Truthy(const Value& value) {
  if (value.IsNull()) return false;
  if (value.type == ValueType::kInt64) return value.value.int_value != 0;
  if (value.type == ValueType::kDouble) return value.value.double_value != 0.0;
  return !value.value.varchar_value.empty();
}

double Number(const Value& value) {
  if (value.type == ValueType::kDouble) return value.value.double_value;
  if (value.type == ValueType::kInt64) {
    return static_cast<double>(value.value.int_value);
  }
  throw std::runtime_error("numeric value required");
}

int FindColumn(const Schema& schema, const ColumnName& name) {
  int match = -1;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    const bool exact = !name.schema.empty() &&
                       candidate.schema == name.schema &&
                       candidate.name == name.name;
    const bool unqualified = name.schema.empty() && candidate.name == name.name;
    if (exact || unqualified) {
      if (match >= 0 && unqualified) {
        throw std::runtime_error("ambiguous column " + name.name);
      }
      match = static_cast<int>(i);
    }
  }
  if (match < 0 && !name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      if (schema.GetColumn(i).Name().name == name.name) {
        if (match >= 0) return -1;
        match = static_cast<int>(i);
      }
    }
  }
  return match;
}

Value Lookup(const ColumnName& name, const Scope& scope) {
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (!current->row || !current->schema) continue;
    const int offset = FindColumn(*current->schema, name);
    if (offset >= 0) return (*current->row)[static_cast<size_t>(offset)];
  }
  throw std::runtime_error("column " + name.ToString() + " not found");
}

bool Like(std::string_view value, std::string_view pattern) {
  // Fast paths for the common TPC-H shapes: 'foo%', '%foo', '%foo%'.
  if (pattern == "%") return true;
  if (pattern.empty()) return value.empty();
  const bool leading = pattern.front() == '%';
  const bool trailing = pattern.back() == '%';
  if (leading || trailing) {
    std::string_view core = pattern;
    if (leading) core.remove_prefix(1);
    if (trailing && !core.empty()) core.remove_suffix(1);
    if (core.find('%') == std::string_view::npos &&
        core.find('_') == std::string_view::npos) {
      if (leading && trailing) {
        return value.find(core) != std::string_view::npos;
      }
      if (trailing) {
        return value.size() >= core.size() && value.substr(0, core.size()) == core;
      }
      if (leading) {
        return value.size() >= core.size() &&
               value.substr(value.size() - core.size()) == core;
      }
    }
  }

  size_t value_pos = 0;
  size_t pattern_pos = 0;
  size_t wildcard = std::string_view::npos;
  size_t retry = 0;
  while (value_pos < value.size()) {
    if (pattern_pos < pattern.size() &&
        (pattern[pattern_pos] == '_' ||
         pattern[pattern_pos] == value[value_pos])) {
      ++value_pos;
      ++pattern_pos;
    } else if (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
      wildcard = pattern_pos++;
      retry = value_pos;
    } else if (wildcard != std::string_view::npos) {
      pattern_pos = wildcard + 1;
      value_pos = ++retry;
    } else {
      return false;
    }
  }
  while (pattern_pos < pattern.size() && pattern[pattern_pos] == '%') {
    ++pattern_pos;
  }
  return pattern_pos == pattern.size();
}

Value Binary(BinaryOperation operation, const Value& left, const Value& right) {
  if (left.IsNull() || right.IsNull()) return Value();
  if (operation == BinaryOperation::kAnd) {
    return Value(Truthy(left) && Truthy(right));
  }
  if (operation == BinaryOperation::kOr) {
    return Value(Truthy(left) || Truthy(right));
  }
  if (operation == BinaryOperation::kXor) {
    return Value(Truthy(left) != Truthy(right));
  }
  if (operation == BinaryOperation::kLike ||
      operation == BinaryOperation::kNotLike) {
    if (left.type != ValueType::kVarChar || right.type != ValueType::kVarChar) {
      throw std::runtime_error("LIKE requires string operands");
    }
    const bool matched =
        Like(left.value.varchar_value, right.value.varchar_value);
    return Value(operation == BinaryOperation::kLike ? matched : !matched);
  }

  const bool numeric =
      (left.type == ValueType::kInt64 || left.type == ValueType::kDouble) &&
      (right.type == ValueType::kInt64 || right.type == ValueType::kDouble);
  if (numeric) {
    const bool integral =
        left.type == ValueType::kInt64 && right.type == ValueType::kInt64;
    const double lhs = Number(left);
    const double rhs = Number(right);
    switch (operation) {
      case BinaryOperation::kAdd:
        return integral ? Value(left.value.int_value + right.value.int_value)
                        : Value(lhs + rhs);
      case BinaryOperation::kSubtract:
        return integral ? Value(left.value.int_value - right.value.int_value)
                        : Value(lhs - rhs);
      case BinaryOperation::kMultiply:
        return integral ? Value(left.value.int_value * right.value.int_value)
                        : Value(lhs * rhs);
      case BinaryOperation::kDivide:
        return Value(lhs / rhs);
      case BinaryOperation::kModulo:
        return integral ? Value(left.value.int_value % right.value.int_value)
                        : Value(std::fmod(lhs, rhs));
      case BinaryOperation::kEquals:
        return Value(lhs == rhs);
      case BinaryOperation::kNotEquals:
        return Value(lhs != rhs);
      case BinaryOperation::kLessThan:
        return Value(lhs < rhs);
      case BinaryOperation::kLessThanEquals:
        return Value(lhs <= rhs);
      case BinaryOperation::kGreaterThan:
        return Value(lhs > rhs);
      case BinaryOperation::kGreaterThanEquals:
        return Value(lhs >= rhs);
      default:
        break;
    }
  }
  if (left.type != right.type) throw std::runtime_error("type mismatch");
  switch (operation) {
    case BinaryOperation::kEquals:
      return Value(left == right);
    case BinaryOperation::kNotEquals:
      return Value(left != right);
    case BinaryOperation::kLessThan:
      return Value(left < right);
    case BinaryOperation::kLessThanEquals:
      return Value(left <= right);
    case BinaryOperation::kGreaterThan:
      return Value(left > right);
    case BinaryOperation::kGreaterThanEquals:
      return Value(left >= right);
    case BinaryOperation::kAdd:
      if (left.type == ValueType::kVarChar) {
        return Value(std::string(left.value.varchar_value) +
                     std::string(right.value.varchar_value));
      }
      break;
    default:
      break;
  }
  throw std::runtime_error("unsupported binary operation");
}

bool ContainsAggregate(const Expression& expression) {
  if (!expression) return false;
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
      return true;
    case TypeTag::kBinaryExp:
      return ContainsAggregate(expression->AsBinaryExpression().Left()) ||
             ContainsAggregate(expression->AsBinaryExpression().Right());
    case TypeTag::kUnaryExp:
      return ContainsAggregate(expression->AsUnaryExpression().Child());
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (ContainsAggregate(condition) || ContainsAggregate(result))
          return true;
      }
      return ContainsAggregate(value.else_clause_);
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& argument :
           expression->AsFunctionCallExpression().Args()) {
        if (ContainsAggregate(argument)) return true;
      }
      return false;
    default:
      return false;
  }
}

using AggregateResultMap =
    std::unordered_map<const AggregateExpression*, Value>;

Value Evaluate(const Expression& expression, const Scope& scope,
               const AggregateResultMap* aggregates,
               TransactionContext& context,
               const CteMap& ctes);

Value Aggregate(const AggregateExpression& aggregate,
                const AggregateResultMap& aggregates) {
  const auto result = aggregates.find(&aggregate);
  if (result == aggregates.end()) {
    throw std::runtime_error("aggregate was not prepared");
  }
  return result->second;
}

void CollectAggregates(const Expression& expression,
                       std::vector<const AggregateExpression*>* aggregates,
                       std::unordered_set<const AggregateExpression*>* seen) {
  if (!expression) return;
  if (expression->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression* aggregate =
        &expression->AsAggregateExpression();
    if (seen->insert(aggregate).second) aggregates->push_back(aggregate);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectAggregates(child, aggregates, seen);
  }
}

struct AggregateAccumulator {
  explicit AggregateAccumulator(const AggregateExpression* aggregate)
      : expression(aggregate),
        distinct(aggregate->Distinct()
                     ? std::make_unique<std::unordered_set<Value>>()
                     : nullptr) {}

  void Add(const Value& value) {
    if (value.IsNull()) return;
    if (distinct) {
      if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
        if (!distinct_ints) {
          distinct_ints = std::make_unique<std::unordered_set<int64_t>>();
          distinct_ints->reserve(distinct->size() + 8);
          for (const Value& seen : *distinct) {
            if (seen.type == ValueType::kInt64 || seen.type == ValueType::kDate) {
              distinct_ints->insert(seen.value.int_value);
            }
          }
          distinct.reset();
        }
        if (!distinct_ints->insert(value.value.int_value).second) return;
      } else if (!distinct->insert(value).second) {
        return;
      }
    }
    switch (expression->GetType()) {
      case AggregationType::kCount:
        ++count;
        break;
      case AggregationType::kSum:
      case AggregationType::kAvg:
        total += Number(value);
        total_is_double =
            total_is_double || value.type == ValueType::kDouble;
        ++count;
        break;
      case AggregationType::kMin:
        if (extreme.IsNull() || value < extreme) extreme = value;
        break;
      case AggregationType::kMax:
        if (extreme.IsNull() || extreme < value) extreme = value;
        break;
    }
  }

  Value Finish() const {
    switch (expression->GetType()) {
      case AggregationType::kCount:
        return Value(count);
      case AggregationType::kAvg:
        return count == 0
                   ? Value()
                   : Value(total / static_cast<double>(count));
      case AggregationType::kSum:
        if (count == 0) return Value();
        return total_is_double ? Value(total)
                               : Value(static_cast<int64_t>(total));
      case AggregationType::kMin:
      case AggregationType::kMax:
        return extreme;
    }
    return Value();
  }

  const AggregateExpression* expression;
  int64_t count = 0;
  double total = 0.0;
  bool total_is_double = false;
  Value extreme;
  std::unique_ptr<std::unordered_set<Value>> distinct;
  std::unique_ptr<std::unordered_set<int64_t>> distinct_ints;
};

Value EvaluateFunction(const FunctionCallExpression& call, const Scope& scope,
                       const AggregateResultMap* aggregates,
                       TransactionContext& context, const CteMap& ctes) {
  const std::string& name = call.FuncName();
  if (name == "date_add" || name == "date_sub") {
    if (call.Args().size() != 2 ||
        call.Args()[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB arity");
    }
    const Value date =
        Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    const auto& interval = call.Args()[1]->AsIntervalExpression();
    const int64_t amount = name == "date_sub" ? -interval.Amount()
                                               : interval.Amount();
    const int64_t days = date.type == ValueType::kDate
                             ? date.DateDays()
                             : ParseDateDays(date.value.varchar_value);
    const int64_t result = AddDateIntervalDays(days, amount, interval.Unit());
    return date.type == ValueType::kDate
               ? Value::DateFromDays(result)
               : Value(FormatDateDays(result));
  }
  std::vector<Value> arguments;
  for (const Expression& argument : call.Args()) {
    arguments.push_back(
        Evaluate(argument, scope, aggregates, context, ctes));
  }
  if (name == "substr" || name == "substring") {
    if (arguments.size() < 2 || arguments[0].IsNull()) return Value();
    const std::string input(arguments[0].value.varchar_value);
    const int64_t start = arguments[1].value.int_value;
    const size_t begin = start <= 1 ? 0 : static_cast<size_t>(start - 1);
    const size_t length =
        arguments.size() >= 3
            ? static_cast<size_t>(arguments[2].value.int_value)
            : std::string::npos;
    return Value(input.substr(begin, length));
  }
  if (name.starts_with("extract_")) {
    if (arguments.size() != 1 || arguments[0].IsNull()) return Value();
    const std::string date = arguments[0].type == ValueType::kDate
                                 ? arguments[0].AsString()
                                 : std::string(arguments[0].value.varchar_value);
    if (name == "extract_year") return Value(std::stoll(date.substr(0, 4)));
    if (name == "extract_month") return Value(std::stoll(date.substr(5, 2)));
    if (name == "extract_day") return Value(std::stoll(date.substr(8, 2)));
  }
  if (name == "coalesce") {
    for (Value& value : arguments) {
      if (!value.IsNull()) return value;
    }
    return Value();
  }
  if (name == "concat") {
    std::string result;
    for (const Value& value : arguments) {
      if (!value.IsNull()) result += std::string(value.value.varchar_value);
    }
    return Value(std::move(result));
  }
  if (name == "current_timestamp") {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm);
    return Value(std::string(buffer));
  }
  throw std::runtime_error("unsupported function " + name);
}

Value Evaluate(const Expression& expression, const Scope& scope,
               const AggregateResultMap* aggregates,
               TransactionContext& context,
               const CteMap& ctes) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.name == "*") return Value(1);
      return Lookup(name, scope);
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue();
    case TypeTag::kBinaryExp: {
      const auto& value = expression->AsBinaryExpression();
      if (value.Op() == BinaryOperation::kAnd) {
        const Value left =
            Evaluate(value.Left(), scope, aggregates, context, ctes);
        if (!Truthy(left)) return Value(false);
      }
      if (value.Op() == BinaryOperation::kOr) {
        const Value left =
            Evaluate(value.Left(), scope, aggregates, context, ctes);
        if (Truthy(left)) return Value(true);
      }
      return Binary(value.Op(),
                    Evaluate(value.Left(), scope, aggregates, context, ctes),
                    Evaluate(value.Right(), scope, aggregates, context,
                             ctes));
    }
    case TypeTag::kUnaryExp: {
      const auto& value = expression->AsUnaryExpression();
      Value child =
          Evaluate(value.Child(), scope, aggregates, context, ctes);
      if (value.Op() == UnaryOperation::kIsNull) return Value(child.IsNull());
      if (value.Op() == UnaryOperation::kIsNotNull)
        return Value(!child.IsNull());
      if (value.Op() == UnaryOperation::kNot) return Value(!Truthy(child));
      if (child.type == ValueType::kDouble)
        return Value(-child.value.double_value);
      return Value(-child.value.int_value);
    }
    case TypeTag::kAggregateExp:
      if (!aggregates)
        throw std::runtime_error("aggregate outside grouping");
      return Aggregate(expression->AsAggregateExpression(), *aggregates);
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (Truthy(Evaluate(condition, scope, aggregates, context, ctes))) {
          return Evaluate(result, scope, aggregates, context, ctes);
        }
      }
      return value.else_clause_
                 ? Evaluate(value.else_clause_, scope, aggregates, context,
                            ctes)
                 : Value();
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      const Value test =
          Evaluate(value.child_, scope, aggregates, context, ctes);
      for (const Expression& item : value.list_) {
        if (Binary(BinaryOperation::kEquals, test,
                   Evaluate(item, scope, aggregates, context, ctes))
                .Truthy()) {
          return Value(true);
        }
      }
      return Value(false);
    }
    case TypeTag::kFunctionCallExp:
      return EvaluateFunction(expression->AsFunctionCallExpression(), scope,
                              aggregates, context, ctes);
    case TypeTag::kQueryExp: {
      const auto& value = expression->AsQueryExpression();
      std::optional<Relation> indexed =
          ExecuteCorrelatedSingleSource(context, *value.Query(), scope, ctes);
      std::optional<Relation> executed;
      const Relation* relation = indexed ? &*indexed : nullptr;
      bool uncorrelated = false;
      if (!relation) {
        relation = ExecuteCachedUncorrelated(context, *value.Query(), ctes);
        uncorrelated = relation != nullptr;
      }
      if (!relation) {
        executed = ExecuteQuery(context, *value.Query(), &scope, ctes);
        relation = &*executed;
      }
      if (value.Exists()) {
        const bool exists = !relation->rows.empty();
        return Value(value.Negated() ? !exists : exists);
      }
      if (value.Test()) {
        const Value test =
            Evaluate(value.Test(), scope, aggregates, context, ctes);
        bool found = false;
        if (uncorrelated && active_runtime) {
          auto [cached, inserted] =
              active_runtime->uncorrelated_membership.try_emplace(
                  value.Query().get());
          if (inserted) {
            cached->second.reserve(relation->rows.size());
            for (const Row& row : relation->rows) {
              if (!row.values_.empty() && !row[0].IsNull()) {
                cached->second.insert(row[0]);
              }
            }
            ++active_runtime->uncorrelated_hash_builds;
          }
          ++active_runtime->uncorrelated_hash_probes;
          found = !test.IsNull() && cached->second.contains(test);
        } else {
          for (const Row& row : relation->rows) {
            if (!row.values_.empty() &&
                Truthy(Binary(BinaryOperation::kEquals, test, row[0]))) {
              found = true;
              break;
            }
          }
        }
        return Value(value.Negated() ? !found : found);
      }
      if (relation->rows.empty() || relation->rows[0].values_.empty())
        return Value();
      return relation->rows[0][0];
    }
    case TypeTag::kIntervalExp:
      return expression->Evaluate(Row(), Schema());
    default:
      throw std::runtime_error("unsupported expression type");
  }
}

Schema QualifySchema(const Schema& schema, std::string_view qualifier) {
  std::vector<Column> columns;
  columns.reserve(schema.ColumnCount());
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& column = schema.GetColumn(i);
    columns.emplace_back(ColumnName(qualifier, column.Name().name),
                         column.Type());
  }
  return Schema("", std::move(columns));
}

void CollectStatementColumns(const SelectStatement& statement,
                             std::unordered_set<ColumnName>* columns);

void CollectExpressionColumns(const Expression& expression,
                              std::unordered_set<ColumnName>* columns) {
  if (!expression) return;
  std::unordered_set<ColumnName> touched = expression->TouchedColumns();
  columns->merge(touched);
  if (expression->Type() == TypeTag::kQueryExp) {
    CollectStatementColumns(*expression->AsQueryExpression().Query(), columns);
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectExpressionColumns(child, columns);
  }
}

void CollectStatementColumns(const SelectStatement& statement,
                             std::unordered_set<ColumnName>* columns) {
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectExpressionColumns(projection.expression, columns);
  }
  CollectExpressionColumns(statement.WhereClause(), columns);
  for (const Expression& key : statement.GroupBy()) {
    CollectExpressionColumns(key, columns);
  }
  CollectExpressionColumns(statement.Having(), columns);
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectExpressionColumns(term.expression, columns);
  }
  for (const SelectSource& source : statement.Sources()) {
    CollectExpressionColumns(source.join_condition, columns);
    if (source.query) CollectStatementColumns(*source.query, columns);
  }
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)name;
    CollectStatementColumns(*query, columns);
  }
}

std::vector<slot_t> RequiredColumns(const SelectStatement& statement,
                                    const Schema& schema,
                                    bool ignore_star = false) {
  const bool selects_star =
      !ignore_star &&
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return projection.expression->Type() ==
                               TypeTag::kColumnValue &&
                           projection.expression->AsColumnValue()
                                   .GetColumnName()
                                   .name == "*";
                  });
  std::unordered_set<ColumnName> referenced;
  CollectStatementColumns(statement, &referenced);
  std::vector<slot_t> result;
  result.reserve(schema.ColumnCount());
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    const bool needed =
        selects_star || std::ranges::any_of(referenced, [&](const auto& name) {
          if (name.name == "*") return false;
          return name.name == candidate.name &&
                 (name.schema.empty() || name.schema == candidate.schema);
        });
    if (needed) result.push_back(i);
  }
  return result;
}

void EnsureReusableProjections(TransactionContext& context,
                               ExecutionRuntime* runtime) {
  if (!runtime || !runtime->root_statement ||
      runtime->reusable_base_relations.empty() ||
      !runtime->reusable_projections.empty()) {
    return;
  }
  for (const std::string& table : runtime->reusable_base_relations) {
    StatusOr<std::shared_ptr<Table>> loaded = context.GetTable(table);
    if (!loaded.HasValue()) continue;
    runtime->reusable_projections[table] =
        RequiredColumns(*runtime->root_statement, loaded.Value()->GetSchema());
  }
}

const std::vector<slot_t>* ReusableProjection(std::string_view table) {
  if (!active_runtime) return nullptr;
  const auto found = active_runtime->reusable_projections.find(std::string(table));
  if (found == active_runtime->reusable_projections.end() ||
      found->second.empty()) {
    return nullptr;
  }
  return &found->second;
}

Schema ProjectSchema(const Schema& schema,
                     const std::vector<slot_t>& projection) {
  std::vector<Column> columns;
  columns.reserve(projection.size());
  for (slot_t offset : projection) {
    columns.push_back(schema.GetColumn(offset));
  }
  return Schema(schema.Name(), std::move(columns));
}

std::string BaseRelationCacheKey(
    std::string_view table, const std::vector<slot_t>* projection) {
  std::string key(table);
  if (!projection) return key;
  key.push_back('#');
  for (slot_t column : *projection) {
    key += std::to_string(column);
    key.push_back(',');
  }
  return key;
}

bool ReusesBaseRelation(const SelectSource& source) {
  return active_runtime &&
         active_runtime->reusable_base_relations.contains(source.table);
}

void FilterRelation(TransactionContext& context, Relation* relation,
                    const std::vector<Expression>& predicates,
                    const Scope* outer, const CteMap& ctes);
std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name);

// Fast path for conjuncts of the form `column <cmp> constant` (incl. DATE).
struct SimpleComparePredicate {
  slot_t column{0};
  BinaryOperation op{BinaryOperation::kEquals};
  Value constant;
  bool int_payload{false};
  int64_t int_constant{0};
  bool double_payload{false};
  double double_constant{0.0};
};

BinaryOperation FlipCompare(BinaryOperation operation) {
  switch (operation) {
    case BinaryOperation::kLessThan:
      return BinaryOperation::kGreaterThan;
    case BinaryOperation::kLessThanEquals:
      return BinaryOperation::kGreaterThanEquals;
    case BinaryOperation::kGreaterThan:
      return BinaryOperation::kLessThan;
    case BinaryOperation::kGreaterThanEquals:
      return BinaryOperation::kLessThanEquals;
    default:
      return operation;
  }
}

bool MatchSimpleCompare(const Row& row, const SimpleComparePredicate& pred) {
  const Value& value = row[pred.column];
  if (value.IsNull() || pred.constant.IsNull()) return false;

  if (pred.int_payload &&
      (value.type == ValueType::kInt64 || value.type == ValueType::kDate)) {
    const int64_t left = value.value.int_value;
    const int64_t right = pred.int_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }
  if (pred.double_payload && value.type == ValueType::kDouble) {
    const double left = value.value.double_value;
    const double right = pred.double_constant;
    switch (pred.op) {
      case BinaryOperation::kEquals:
        return left == right;
      case BinaryOperation::kNotEquals:
        return left != right;
      case BinaryOperation::kLessThan:
        return left < right;
      case BinaryOperation::kLessThanEquals:
        return left <= right;
      case BinaryOperation::kGreaterThan:
        return left > right;
      case BinaryOperation::kGreaterThanEquals:
        return left >= right;
      default:
        return false;
    }
  }

  const auto as_double = [](const Value& v) -> std::optional<double> {
    if (v.type == ValueType::kDouble) return v.value.double_value;
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return static_cast<double>(v.value.int_value);
    }
    return std::nullopt;
  };
  const auto as_int = [](const Value& v) -> std::optional<int64_t> {
    if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
      return v.value.int_value;
    }
    return std::nullopt;
  };

  switch (pred.op) {
    case BinaryOperation::kEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value == pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li == *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld == *rd;
      }
      return false;
    }
    case BinaryOperation::kNotEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value != pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li != *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld != *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value < pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li < *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld < *rd;
      }
      return false;
    }
    case BinaryOperation::kLessThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value <= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li <= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld <= *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThan: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value > pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li > *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld > *rd;
      }
      return false;
    }
    case BinaryOperation::kGreaterThanEquals: {
      if (value.type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar) {
        return value.value.varchar_value >= pred.constant.value.varchar_value;
      }
      if (const auto li = as_int(value), ri = as_int(pred.constant); li && ri) {
        return *li >= *ri;
      }
      if (const auto ld = as_double(value), rd = as_double(pred.constant);
          ld && rd) {
        return *ld >= *rd;
      }
      return false;
    }
    default:
      return false;
  }
}

std::optional<SimpleComparePredicate> TryCompileSimpleCompare(
    const Expression& predicate, const Schema& schema) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) return std::nullopt;
  Expression folded =
      ExpressionRewriter(ExpressionRuleSet::Default()).Rewrite(predicate);
  if (!folded || folded->Type() != TypeTag::kBinaryExp) return std::nullopt;
  const BinaryExpression& binary = folded->AsBinaryExpression();
  if (!IsComparison(binary.Op())) return std::nullopt;
  Expression column = binary.Left();
  Expression constant = binary.Right();
  BinaryOperation op = binary.Op();
  if (column->Type() == TypeTag::kConstantValue &&
      constant->Type() == TypeTag::kColumnValue) {
    std::swap(column, constant);
    op = FlipCompare(op);
  }
  if (column->Type() != TypeTag::kColumnValue ||
      constant->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const auto offset =
      LocalColumnOffset(schema, column->AsColumnValue().GetColumnName());
  if (!offset) return std::nullopt;
  SimpleComparePredicate compiled;
  compiled.column = static_cast<slot_t>(*offset);
  compiled.op = op;
  compiled.constant = constant->AsConstantValue().GetValue();
  if (compiled.constant.type == ValueType::kInt64 ||
      compiled.constant.type == ValueType::kDate) {
    compiled.int_payload = true;
    compiled.int_constant = compiled.constant.value.int_value;
  } else if (compiled.constant.type == ValueType::kDouble) {
    compiled.double_payload = true;
    compiled.double_constant = compiled.constant.value.double_value;
  }
  return compiled;
}

struct CompiledScanFilter {
  std::vector<SimpleComparePredicate> simple;
  std::vector<Expression> residual;
  bool all_simple{false};
};

CompiledScanFilter CompileScanFilter(const std::vector<Expression>& predicates,
                                     const Schema& schema) {
  CompiledScanFilter compiled;
  compiled.all_simple = true;
  for (const Expression& predicate : predicates) {
    if (auto simple = TryCompileSimpleCompare(predicate, schema)) {
      compiled.simple.push_back(std::move(*simple));
    } else {
      compiled.all_simple = false;
      compiled.residual.push_back(predicate);
    }
  }
  return compiled;
}

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes) {
  for (const SimpleComparePredicate& pred : filter.simple) {
    if (!MatchSimpleCompare(row, pred)) return false;
  }
  if (filter.residual.empty()) return true;
  Scope scope{&row, &schema, outer};
  for (const Expression& predicate : filter.residual) {
    if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
      return false;
    }
  }
  return true;
}

bool MatchScanFilter(const Row& row, const Schema& schema,
                     const CompiledScanFilter& filter, const Scope* outer,
                     TransactionContext& context, const CteMap& ctes);

// Parallel morsel scan that materializes matching rows directly (avoids the
// ParallelScan DataChunk round-trip that previously regressed TPC-H).
bool TryParallelTableScan(TransactionContext& context, Table& table,
                          const std::vector<slot_t>* projection,
                          const std::unordered_set<int64_t>* key_filter,
                          std::optional<slot_t> full_key_column,
                          bool filter_during_scan,
                          const CompiledScanFilter* scan_filter,
                          const Schema& result_schema, const Scope* outer,
                          const CteMap& ctes, Relation* result) {
  std::vector<Table::ScanMorsel> morsels =
      table.BuildScanMorsels(context.txn_, 8);
  const size_t workers = std::min(
      static_cast<size_t>(std::thread::hardware_concurrency()),
      std::max<size_t>(1, morsels.size()));
  if (workers <= 1 || morsels.size() < 8) return false;

  std::atomic<size_t> next_morsel{0};
  std::vector<std::vector<Row>> shards(workers);
  std::vector<size_t> shard_seen(workers, 0);
  std::vector<size_t> shard_out(workers, 0);
  std::mutex error_mu;
  std::exception_ptr error;
  std::optional<std::vector<slot_t>> proj_opt;
  if (projection) proj_opt = *projection;

  {
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
      threads.emplace_back([&, w] {
        try {
          auto& local = shards[w];
          local.reserve(1024);
          while (true) {
            const size_t mi = next_morsel.fetch_add(1);
            if (mi >= morsels.size()) break;
            Iterator iterator = table.BeginMorselScan(
                context.txn_, morsels[mi], proj_opt, key_filter,
                full_key_column);
            while (iterator.IsValid()) {
              ++shard_seen[w];
              bool matches = true;
              if (filter_during_scan && scan_filter) {
                matches = MatchScanFilter(*iterator, result_schema, *scan_filter,
                                          outer, context, ctes);
              }
              if (matches) {
                local.push_back(*iterator);
                ++shard_out[w];
              }
              ++iterator;
            }
          }
        } catch (...) {
          std::scoped_lock lock(error_mu);
          if (!error) error = std::current_exception();
        }
      });
    }
  }
  if (error) std::rethrow_exception(error);
  for (size_t w = 0; w < workers; ++w) {
    if (active_runtime) {
      active_runtime->scan_rows += shard_seen[w];
      active_runtime->scan_values_available +=
          shard_seen[w] * table.GetSchema().ColumnCount();
      active_runtime->scan_values_decoded +=
          shard_out[w] * result_schema.ColumnCount();
      active_runtime->scan_output_rows += shard_out[w];
    }
    for (Row& row : shards[w]) {
      result->AddRow(std::move(row));
    }
  }
  return true;
}

Relation LoadSource(TransactionContext& context, const SelectSource& source,
                    const Scope* outer, const CteMap& ctes,
                    const std::vector<slot_t>* projection = nullptr,
                    const std::vector<Expression>* scan_predicates = nullptr,
                    const std::unordered_set<int64_t>* int_key_filter = nullptr,
                    std::optional<slot_t> int_key_column = std::nullopt) {
  Relation result;
  if (source.query) {
    result = ExecuteQuery(context, *source.query, outer, ctes);
  } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
    result = cte->second;
  } else {
    const bool reusable =
        active_runtime &&
        active_runtime->reusable_base_relations.contains(source.table);
    const std::string cache_key =
        BaseRelationCacheKey(source.table, projection);
    const bool filter_during_scan =
        !reusable && scan_predicates && !scan_predicates->empty();
    const auto cached =
        reusable ? active_runtime->base_relations.find(cache_key)
                 : std::unordered_map<std::string, Relation>::iterator{};
    if (reusable && cached != active_runtime->base_relations.end()) {
      // Materialize only rows that survive this alias's local predicates so
      // we never deep-copy a multi-million-row cache and filter afterwards.
      Relation& cached_relation = cached->second;
      result.schema = cached_relation.schema;
      cached_relation.FinishSpill();
      if (!scan_predicates || scan_predicates->empty()) {
        cached_relation.ForEachRow([&](const Row& row) {
          if (int_key_filter && int_key_column) {
            const Value& key = row[*int_key_column];
            if (key.IsNull() || !int_key_filter->contains(key.value.int_value)) {
              return;
            }
          }
          result.AddRow(row);
        });
      } else {
        const auto filter_begin = std::chrono::steady_clock::now();
        const CompiledScanFilter scan_filter =
            CompileScanFilter(*scan_predicates, cached_relation.schema);
        cached_relation.ForEachRow([&](const Row& row) {
          if (int_key_filter && int_key_column) {
            const Value& key = row[*int_key_column];
            if (key.IsNull() ||
                !int_key_filter->contains(key.value.int_value)) {
              return;
            }
          }
          if (MatchScanFilter(row, cached_relation.schema, scan_filter, outer,
                              context, ctes)) {
            result.AddRow(row);
          }
        });
        active_runtime->filter_ms += ElapsedMs(filter_begin);
      }
      ++active_runtime->base_scan_cache_hits;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        throw std::runtime_error("table " + source.table + " not found");
      }
      const Schema& table_schema = table.Value()->GetSchema();
      result.schema = projection ? ProjectSchema(table_schema, *projection)
                                 : table_schema;
      CompiledScanFilter scan_filter;
      if (filter_during_scan) {
        scan_filter = CompileScanFilter(*scan_predicates, result.schema);
      }
      const auto scan_begin = std::chrono::steady_clock::now();
      const auto filter_begin = scan_begin;
      // Prefer skipping full-row decode when an integer key IN-list is active.
      std::optional<slot_t> full_key_column;
      if (int_key_filter && int_key_column && projection) {
        if (*int_key_column < projection->size()) {
          full_key_column = (*projection)[*int_key_column];
        }
      } else if (int_key_filter && int_key_column && !projection) {
        full_key_column = *int_key_column;
      }
      const bool parallel_ok = TryParallelTableScan(
          context, *table.Value(), projection, int_key_filter, full_key_column,
          filter_during_scan, filter_during_scan ? &scan_filter : nullptr,
          result.schema, outer, ctes, &result);
      if (!parallel_ok) {
        Iterator iterator =
            full_key_column
                ? (projection
                       ? table.Value()->BeginFullScan(context.txn_, *projection,
                                                      int_key_filter,
                                                      *full_key_column)
                       : table.Value()->BeginFullScan(context.txn_,
                                                      int_key_filter,
                                                      *full_key_column))
                : projection
                      ? table.Value()->BeginFullScan(context.txn_, *projection)
                      : table.Value()->BeginFullScan(context.txn_);
        while (iterator.IsValid()) {
          if (active_runtime) {
            ++active_runtime->scan_rows;
            active_runtime->scan_values_available += table_schema.ColumnCount();
            active_runtime->scan_values_decoded += result.schema.ColumnCount();
          }
          bool matches = true;
          if (!full_key_column && int_key_filter && int_key_column) {
            const Value& key = (*iterator)[*int_key_column];
            if (key.IsNull() ||
                !int_key_filter->contains(key.value.int_value)) {
              matches = false;
              if (active_runtime) ++active_runtime->key_filter_rejected;
            }
          }
          if (matches && filter_during_scan) {
            matches = MatchScanFilter(*iterator, result.schema, scan_filter,
                                      outer, context, ctes);
          }
          if (matches) {
            result.AddRow(*iterator);
            if (active_runtime) ++active_runtime->scan_output_rows;
          }
          ++iterator;
        }
      }
      if (active_runtime) {
        active_runtime->scan_ms += ElapsedMs(scan_begin);
        if (filter_during_scan) {
          active_runtime->filter_ms += ElapsedMs(filter_begin);
        }
        if (reusable && !int_key_filter) {
          active_runtime->base_relations.emplace(cache_key, result);
        }
      }
      if (reusable && scan_predicates && !scan_predicates->empty()) {
        FilterRelation(context, &result, *scan_predicates, outer, ctes);
      }
    }
  }
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (!qualifier.empty())
    result.schema = QualifySchema(result.schema, qualifier);
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  return result;
}

bool ContainsQuery(const Expression& expression) {
  if (!expression) return false;
  if (expression->Type() == TypeTag::kQueryExp) return true;
  for (const Expression& child : ExpressionChildren(expression)) {
    if (ContainsQuery(child)) return true;
  }
  return false;
}

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name) {
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (candidate.name != name.name) continue;
    if (!name.schema.empty() && candidate.schema != name.schema) continue;
    if (match) return std::nullopt;
    match = i;
  }
  return match;
}

struct PredicateInfo {
  Expression expression;
  std::unordered_set<size_t> sources;
  bool resolved{true};
  bool contains_query{false};
};

std::vector<PredicateInfo> AnalyzePredicates(
    const Expression& where, const std::vector<Relation>& relations) {
  std::vector<PredicateInfo> result;
  for (const Expression& expression : SplitConjuncts(where)) {
    PredicateInfo predicate{expression, {}, true, false};
    predicate.contains_query = ContainsQuery(expression);
    for (const ColumnName& column : expression->TouchedColumns()) {
      std::optional<size_t> owner;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!LocalColumnOffset(relations[i].schema, column)) continue;
        if (owner) {
          predicate.resolved = false;
          break;
        }
        owner = i;
      }
      if (!owner) predicate.resolved = false;
      if (owner) predicate.sources.insert(*owner);
    }
    result.push_back(std::move(predicate));
  }
  return result;
}

std::vector<Expression> SplitDisjuncts(const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kBinaryExp &&
      expression->AsBinaryExpression().Op() == BinaryOperation::kOr) {
    std::vector<Expression> result =
        SplitDisjuncts(expression->AsBinaryExpression().Left());
    std::vector<Expression> right =
        SplitDisjuncts(expression->AsBinaryExpression().Right());
    result.insert(result.end(), right.begin(), right.end());
    return result;
  }
  return expression ? std::vector<Expression>{expression}
                    : std::vector<Expression>{};
}

Expression CombineDisjuncts(const std::vector<Expression>& expressions) {
  if (expressions.empty()) return nullptr;
  Expression result = expressions.front();
  for (size_t i = 1; i < expressions.size(); ++i) {
    result = BinaryExpressionExp(result, BinaryOperation::kOr, expressions[i]);
  }
  return result;
}

Expression NecessaryLocalDisjunction(const Expression& expression,
                                     size_t source,
                                     const std::vector<Relation>& relations) {
  const std::vector<Expression> branches = SplitDisjuncts(expression);
  if (branches.size() < 2) return nullptr;
  std::vector<Expression> local_branches;
  local_branches.reserve(branches.size());
  for (const Expression& branch : branches) {
    std::vector<Expression> local;
    for (const Expression& conjunct : SplitConjuncts(branch)) {
      const std::vector<PredicateInfo> analyzed =
          AnalyzePredicates(conjunct, relations);
      if (analyzed.size() == 1 && analyzed[0].resolved &&
          !analyzed[0].contains_query && analyzed[0].sources.size() == 1 &&
          analyzed[0].sources.contains(source)) {
        local.push_back(conjunct);
      }
    }
    // A branch without a condition on this source can be true for every row;
    // in that case no source-local condition is implied by the disjunction.
    if (local.empty()) return nullptr;
    local_branches.push_back(CombineConjuncts(local));
  }
  return CombineDisjuncts(local_branches);
}

void FilterRelation(TransactionContext& context, Relation* relation,
                    const std::vector<Expression>& predicates,
                    const Scope* outer, const CteMap& ctes) {
  if (predicates.empty()) return;
  const auto filter_begin = std::chrono::steady_clock::now();
  const CompiledScanFilter scan_filter =
      CompileScanFilter(predicates, relation->schema);
  Relation filtered;
  filtered.schema = relation->schema;
  filtered.hash_joins = relation->hash_joins;
  filtered.hybrid_hash_joins = relation->hybrid_hash_joins;
  filtered.in_memory_hash_joins = relation->in_memory_hash_joins;
  filtered.nested_loop_joins = relation->nested_loop_joins;
  filtered.join_comparisons = relation->join_comparisons;
  relation->FinishSpill();
  relation->ForEachRow([&](const Row& row) {
    if (MatchScanFilter(row, relation->schema, scan_filter, outer, context,
                        ctes)) {
      filtered.AddRow(row);
    }
  });
  filtered.FinishSpill();
  *relation = std::move(filtered);
  if (active_runtime) active_runtime->filter_ms += ElapsedMs(filter_begin);
}

struct EqualityKey {
  size_t left;
  size_t right;
};

bool IsColumnEqualityPredicate(const Expression& predicate) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) return false;
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  return binary.Op() == BinaryOperation::kEquals &&
         binary.Left()->Type() == TypeTag::kColumnValue &&
         binary.Right()->Type() == TypeTag::kColumnValue;
}

bool MapsToEqualityKey(const Schema& left, const Schema& right,
                       const Expression& predicate) {
  if (!IsColumnEqualityPredicate(predicate)) return false;
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  const auto lhs_left = LocalColumnOffset(left, lhs);
  const auto lhs_right = LocalColumnOffset(right, lhs);
  const auto rhs_left = LocalColumnOffset(left, rhs);
  const auto rhs_right = LocalColumnOffset(right, rhs);
  return (lhs_left && rhs_right) || (rhs_left && lhs_right);
}

std::vector<Expression> ResidualJoinPredicates(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<Expression> residual;
  residual.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (MapsToEqualityKey(left, right, predicate)) continue;
    residual.push_back(predicate);
  }
  return residual;
}

std::vector<EqualityKey> EqualityKeys(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<EqualityKey> keys;
  for (const Expression& predicate : predicates) {
    if (!IsColumnEqualityPredicate(predicate)) continue;
    const BinaryExpression& binary = predicate->AsBinaryExpression();
    const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
    const auto lhs_left = LocalColumnOffset(left, lhs);
    const auto lhs_right = LocalColumnOffset(right, lhs);
    const auto rhs_left = LocalColumnOffset(left, rhs);
    const auto rhs_right = LocalColumnOffset(right, rhs);
    if (lhs_left && rhs_right) {
      keys.push_back({*lhs_left, *rhs_right});
    } else if (rhs_left && lhs_right) {
      keys.push_back({*rhs_left, *lhs_right});
    }
  }
  return keys;
}

bool HasNullKey(const Row& row, const std::vector<slot_t>& columns);

bool SingleIntegerJoinKey(const Schema& schema,
                          const std::vector<slot_t>& columns) {
  if (columns.size() != 1) return false;
  const ValueType type = schema.GetColumn(columns[0]).Type();
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

int64_t IntegerJoinKey(const Row& row, slot_t column) {
  return row[column].value.int_value;
}

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns) {
  return row.Extract(columns).EncodeMemcomparableFormat();
}

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates) {
  const std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  if (keys.empty()) {
    if (left.rows.empty() || right.rows.empty()) return 0;
    if (left.rows.size() >
        std::numeric_limits<size_t>::max() / right.rows.size()) {
      return std::numeric_limits<size_t>::max();
    }
    return left.rows.size() * right.rows.size();
  }
  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  for (const EqualityKey& key : keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  std::unordered_map<std::string, size_t> frequencies;
  frequencies.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    if (!HasNullKey(row, right_columns)) {
      ++frequencies[row.Extract(right_columns).EncodeMemcomparableFormat()];
    }
  }
  size_t estimate = 0;
  for (const Row& row : left.rows) {
    if (HasNullKey(row, left_columns)) continue;
    const auto found =
        frequencies.find(row.Extract(left_columns).EncodeMemcomparableFormat());
    if (found != frequencies.end()) estimate += found->second;
  }
  return estimate;
}

bool HasNullKey(const Row& row, const std::vector<slot_t>& columns) {
  return std::any_of(columns.begin(), columns.end(),
                     [&](slot_t column) { return row[column].IsNull(); });
}

size_t SpillPartitionOf(const std::string& key, size_t partitions) {
  return std::hash<std::string>{}(key) % partitions;
}

size_t SpillPartitionOf(int64_t key, size_t partitions) {
  return static_cast<size_t>(std::hash<int64_t>{}(key) % partitions);
}

// DeWitt-style Hybrid Hash Join: keep partition 0 resident; spill the rest.
Relation HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons) {
  size_t build_estimate = 0;
  if (right.HasSpill()) {
    build_estimate = right.TotalRows() * 128;
  } else {
    for (const Row& row : right.rows) {
      build_estimate += EstimateRowBytes(row) + 64;
    }
  }
  const size_t partitions = HybridPartitionCount(build_estimate);
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  const bool integer_key =
      SingleIntegerJoinKey(left.schema, left_columns) &&
      SingleIntegerJoinKey(right.schema, right_columns);
  const slot_t left_key_column =
      integer_key ? left_columns[0] : static_cast<slot_t>(0);
  const slot_t right_key_column =
      integer_key ? right_columns[0] : static_cast<slot_t>(0);

  std::vector<Row> resident_right;
  QueryMemoryCharge resident_charge;
  std::vector<SpillFile> left_parts(partitions);
  std::vector<SpillFile> right_parts(partitions);

  left.FinishSpill();
  right.FinishSpill();

  auto right_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, right_key_column),
                              partitions);
    }
    return SpillPartitionOf(EncodeJoinKey(row, right_columns), partitions);
  };
  auto left_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, left_key_column), partitions);
    }
    return SpillPartitionOf(EncodeJoinKey(row, left_columns), partitions);
  };

  right.ForEachRow([&](const Row& row) {
    if (HasNullKey(row, right_columns)) {
      return;
    }
    const size_t part = right_partition(row);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && budget.CanReserve(bytes)) {
      resident_charge.Add(bytes);
      resident_right.push_back(row);
    } else {
      right_parts[part].Append(row);
    }
  });
  right.rows.clear();
  right.rows.shrink_to_fit();
  right.ReleaseCharge();

  std::unordered_multimap<int64_t, const Row*> int_buckets;
  std::unordered_multimap<std::string, const Row*> str_buckets;
  if (integer_key) {
    int_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
    }
  } else {
    str_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      str_buckets.emplace(EncodeJoinKey(row, right_columns), &row);
    }
  }

  Relation result;
  result.schema = left.schema + right.schema;
  const size_t right_width = right.schema.ColumnCount();

  auto probe_resident = [&](const Row& left_row) {
    bool matched = false;
    if (integer_key) {
      const int64_t key = IntegerJoinKey(left_row, left_key_column);
      const auto [begin, end] = int_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = left_row + *iter->second;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    } else {
      const std::string key = EncodeJoinKey(left_row, left_columns);
      const auto [begin, end] = str_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = left_row + *iter->second;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    }
    if (!matched && left_join) {
      std::vector<Value> nulls(right_width);
      result.AddRow(left_row + Row(std::move(nulls)));
    }
  };

  left.ForEachRow([&](const Row& left_row) {
    if (HasNullKey(left_row, left_columns)) {
      if (left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(left_row + Row(std::move(nulls)));
      }
      return;
    }
    const size_t part = left_partition(left_row);
    if (part == 0 && right_parts[0].Empty()) {
      probe_resident(left_row);
    } else {
      left_parts[part].Append(left_row);
    }
  });
  left.rows.clear();
  left.rows.shrink_to_fit();
  left.ReleaseCharge();

  int_buckets.clear();
  str_buckets.clear();
  if (!right_parts[0].Empty()) {
    for (const Row& row : resident_right) {
      right_parts[0].Append(row);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t part = 0; part < partitions; ++part) {
    left_parts[part].FinishWriting();
    right_parts[part].FinishWriting();
  }

  for (size_t part = 0; part < partitions; ++part) {
    if (left_parts[part].Empty() && right_parts[part].Empty()) {
      continue;
    }
    std::vector<Row> right_rows = right_parts[part].ReadAllRows();
    QueryMemoryCharge part_charge;
    for (const Row& row : right_rows) {
      part_charge.Add(EstimateRowBytes(row));
    }
    std::unordered_multimap<int64_t, const Row*> part_int_buckets;
    std::unordered_multimap<std::string, const Row*> part_str_buckets;
    if (integer_key) {
      part_int_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
      }
    } else {
      part_str_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_str_buckets.emplace(EncodeJoinKey(row, right_columns), &row);
      }
    }
    left_parts[part].ForEachRow([&](const Row& left_row) {
      bool matched = false;
      if (integer_key) {
        const int64_t key = IntegerJoinKey(left_row, left_key_column);
        const auto [begin, end] = part_int_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = left_row + *iter->second;
          if (matches(combined)) {
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      } else {
        const std::string key = EncodeJoinKey(left_row, left_columns);
        const auto [begin, end] = part_str_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = left_row + *iter->second;
          if (matches(combined)) {
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched && left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(left_row + Row(std::move(nulls)));
      }
    });
  }
  result.FinishSpill();
  return result;
}

bool ShouldHybridJoin(const Relation& left, const Relation& right) {
  if (left.HasSpill() || right.HasSpill()) {
    return true;
  }
  // Rough hash-table estimate: keys + pointers for the build (right) side.
  size_t estimate = 0;
  for (const Row& row : right.rows) {
    estimate += EstimateRowBytes(row) + 64;
  }
  return PreferHybridHashJoin(estimate);
}

Relation Join(TransactionContext& context, Relation left, Relation right,
              const SelectSource& source, const Scope* outer,
              const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result;
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<Expression> predicates =
      SplitConjuncts(source.join_condition);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    if (residual.empty()) return true;
    Scope scope{&combined, &result.schema, outer};
    return std::all_of(
        residual.begin(), residual.end(), [&](const Expression& predicate) {
          return Truthy(Evaluate(predicate, scope, nullptr, context, ctes));
        });
  };
  auto emit_unmatched = [&](const Row& left_row) {
    if (source.join_type != JoinType::kLeft) return;
    std::vector<Value> nulls(right.schema.ColumnCount());
    result.AddRow(left_row + Row(std::move(nulls)));
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      bool matched = false;
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) {
          result.AddRow(std::move(combined));
          matched = true;
        }
      });
      if (!matched) emit_unmatched(left_row);
    });
  } else {
    ++result.hash_joins;
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      Relation joined =
          HybridHashJoin(std::move(left), std::move(right), left_columns,
                         right_columns, matches,
                         source.join_type == JoinType::kLeft,
                         &result.join_comparisons);
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (active_runtime) active_runtime->join_ms += ElapsedMs(join_begin);
      return joined;
    }
    ++result.in_memory_hash_joins;
    const bool integer_key =
        SingleIntegerJoinKey(left.schema, left_columns) &&
        SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      std::unordered_multimap<int64_t, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (!HasNullKey(row, right_columns)) {
          buckets.emplace(IntegerJoinKey(row, right_columns[0]), &row);
        }
      }
      for (const Row& left_row : left.rows) {
        bool matched = false;
        if (!HasNullKey(left_row, left_columns)) {
          const int64_t key = IntegerJoinKey(left_row, left_columns[0]);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = left_row + *iter->second;
            if (matches(combined)) {
              result.AddRow(std::move(combined));
              matched = true;
            }
          }
        }
        if (!matched) emit_unmatched(left_row);
      }
    } else {
      std::unordered_multimap<std::string, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (!HasNullKey(row, right_columns)) {
          buckets.emplace(EncodeJoinKey(row, right_columns), &row);
        }
      }
      for (const Row& left_row : left.rows) {
        bool matched = false;
        if (!HasNullKey(left_row, left_columns)) {
          const std::string key = EncodeJoinKey(left_row, left_columns);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = left_row + *iter->second;
            if (matches(combined)) {
              result.AddRow(std::move(combined));
              matched = true;
            }
          }
        }
        if (!matched) emit_unmatched(left_row);
      }
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (active_runtime) active_runtime->join_ms += ElapsedMs(join_begin);
  return result;
}

Relation InnerJoin(TransactionContext& context, Relation left, Relation right,
                   const std::vector<Expression>& predicates,
                   const Scope* outer, const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result;
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    if (residual.empty()) return true;
    Scope scope{&combined, &result.schema, outer};
    return std::all_of(
        residual.begin(), residual.end(), [&](const Expression& predicate) {
          return Truthy(Evaluate(predicate, scope, nullptr, context, ctes));
        });
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) result.AddRow(std::move(combined));
      });
    });
  } else {
    ++result.hash_joins;
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    left_columns.reserve(equality_keys.size());
    right_columns.reserve(equality_keys.size());
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      Relation joined = HybridHashJoin(
          std::move(left), std::move(right), left_columns, right_columns,
          matches, false, &result.join_comparisons);
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (active_runtime) active_runtime->join_ms += ElapsedMs(join_begin);
      return joined;
    }
    ++result.in_memory_hash_joins;
    const bool integer_key =
        SingleIntegerJoinKey(left.schema, left_columns) &&
        SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      std::unordered_multimap<int64_t, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (HasNullKey(row, right_columns)) continue;
        buckets.emplace(IntegerJoinKey(row, right_columns[0]), &row);
      }
      for (const Row& left_row : left.rows) {
        if (HasNullKey(left_row, left_columns)) continue;
        const int64_t key = IntegerJoinKey(left_row, left_columns[0]);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result.join_comparisons;
          Row combined = left_row + *iter->second;
          if (matches(combined)) result.AddRow(std::move(combined));
        }
      }
    } else {
      std::unordered_multimap<std::string, const Row*> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (HasNullKey(row, right_columns)) continue;
        buckets.emplace(EncodeJoinKey(row, right_columns), &row);
      }
      for (const Row& left_row : left.rows) {
        if (HasNullKey(left_row, left_columns)) continue;
        const std::string key = EncodeJoinKey(left_row, left_columns);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result.join_comparisons;
          Row combined = left_row + *iter->second;
          if (matches(combined)) result.AddRow(std::move(combined));
        }
      }
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (active_runtime) active_runtime->join_ms += ElapsedMs(join_begin);
  return result;
}

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset) {
  return std::all_of(values.begin(), values.end(),
                     [&](size_t value) { return superset.contains(value); });
}

Relation BuildInput(TransactionContext& context,
                    const SelectStatement& statement, const Scope* outer,
                    const CteMap& ctes, bool* where_fully_applied) {
  *where_fully_applied = false;
  if (statement.Sources().empty()) {
    Relation singleton;
    singleton.rows.emplace_back();
    singleton.peak_intermediate_rows = 1;
    return singleton;
  }

  std::vector<Relation> relations(statement.Sources().size());
  std::vector<bool> base_sources(statement.Sources().size(), false);
  std::vector<std::vector<slot_t>> projections(statement.Sources().size());
  for (size_t i = 0; i < statement.Sources().size(); ++i) {
    const SelectSource& source = statement.Sources()[i];
    base_sources[i] = !source.query && !ctes.contains(source.table);
    if (!base_sources[i]) {
      relations[i] = LoadSource(context, source, outer, ctes);
      continue;
    }
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      throw std::runtime_error("table " + source.table + " not found");
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    relations[i].schema = qualifier.empty()
                              ? table.Value()->GetSchema()
                              : QualifySchema(table.Value()->GetSchema(),
                                              qualifier);
    projections[i] = RequiredColumns(statement, relations[i].schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(source.table)) {
      projections[i] = *shared;
    }
  }

  // Multi-alias tables share one projected cache entry: take the union of
  // columns required by every reference so each alias can reuse the scan.
  {
    std::unordered_map<std::string, std::set<slot_t>> column_union;
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] || !ReusesBaseRelation(statement.Sources()[i])) {
        continue;
      }
      auto& columns = column_union[statement.Sources()[i].table];
      for (slot_t column : projections[i]) columns.insert(column);
    }
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] || !ReusesBaseRelation(statement.Sources()[i])) {
        continue;
      }
      const auto& columns = column_union[statement.Sources()[i].table];
      projections[i].assign(columns.begin(), columns.end());
    }
  }

  // LEFT JOIN predicates have null-extension semantics, so retain their
  // syntactic order. The optimizer below is valid for inner/cross joins.
  const bool has_left_join =
      std::any_of(statement.Sources().begin() + 1, statement.Sources().end(),
                  [](const SelectSource& source) {
                    return source.join_type == JoinType::kLeft;
                  });
  if (has_left_join) {
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        relations[i] = LoadSource(context, statement.Sources()[i], outer,
                                  ctes, &projections[i]);
      }
    }
    Relation result = std::move(relations.front());
    for (size_t i = 1; i < relations.size(); ++i) {
      result = Join(context, std::move(result), std::move(relations[i]),
                    statement.Sources()[i], outer, ctes);
    }
    return result;
  }

  std::vector<Expression> all_predicates =
      SplitConjuncts(statement.WhereClause());
  for (size_t i = 1; i < statement.Sources().size(); ++i) {
    const Expression& condition = statement.Sources()[i].join_condition;
    if (condition) all_predicates.push_back(condition);
  }
  std::vector<PredicateInfo> predicates = AnalyzePredicates(
      all_predicates.empty() ? Expression() : CombineConjuncts(all_predicates),
      relations);
  const std::vector<PredicateInfo> where_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  auto locally_evaluable = [&](const PredicateInfo& predicate) {
    if (!predicate.resolved || predicate.sources.size() != 1) return false;
    if (!predicate.contains_query) return true;
    return ContainsOnlyUncorrelatedQueries(context, predicate.expression, ctes);
  };
  *where_fully_applied =
      std::all_of(where_predicates.begin(), where_predicates.end(),
                  [&](const PredicateInfo& predicate) {
                    return locally_evaluable(predicate) ||
                           (predicate.resolved && !predicate.contains_query &&
                            predicate.sources.size() > 1);
                  });
  std::vector<std::vector<Expression>> local_predicates(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    std::vector<Expression>& local = local_predicates[i];
    for (const PredicateInfo& predicate : predicates) {
      if (predicate.sources.contains(i) && locally_evaluable(predicate)) {
        local.push_back(predicate.expression);
      } else if (predicate.resolved && !predicate.contains_query &&
                 predicate.sources.size() > 1) {
        Expression implied =
            NecessaryLocalDisjunction(predicate.expression, i, relations);
        if (implied) local.push_back(std::move(implied));
      }
    }
  }

  // Push uncorrelated `col IN (SELECT ...)` results as integer key filters
  // before scanning the owning base table (TPC-H Q18).
  if (active_runtime) {
    for (const Expression& expression :
         SplitConjuncts(statement.WhereClause())) {
      if (!expression || expression->Type() != TypeTag::kQueryExp) continue;
      const QueryExpression& query = expression->AsQueryExpression();
      if (query.Exists() || query.Negated() || !query.Test() ||
          query.Test()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const Relation* membership =
          ExecuteCachedUncorrelated(context, *query.Query(), ctes);
      if (!membership || membership->rows.empty()) continue;
      std::unordered_set<int64_t> keys;
      keys.reserve(membership->rows.size());
      bool all_int = true;
      auto consume_row = [&](const Row& row) {
        if (row.values_.empty() || row[0].IsNull()) return;
        if (row[0].type != ValueType::kInt64 &&
            row[0].type != ValueType::kDate) {
          all_int = false;
          return;
        }
        keys.insert(row[0].value.int_value);
      };
      if (membership->HasSpill()) {
        Relation copy = *membership;
        copy.FinishSpill();
        copy.ForEachRow(consume_row);
      } else {
        for (const Row& row : membership->rows) consume_row(row);
      }
      if (!all_int || keys.empty() || keys.size() >= 2'000'000) continue;
      const ColumnName& column =
          query.Test()->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!base_sources[i]) continue;
        const auto offset = LocalColumnOffset(relations[i].schema, column);
        if (!offset) continue;
        if (!SingleIntegerJoinKey(relations[i].schema,
                                  {static_cast<slot_t>(*offset)})) {
          continue;
        }
        const std::string& table = statement.Sources()[i].table;
        auto& stored = active_runtime->table_key_filters[table];
        if (stored.empty() || keys.size() < stored.size()) {
          stored = keys;
          active_runtime->table_key_filter_columns[table] =
              static_cast<slot_t>(*offset);
        }
      }
    }
  }

  // Load selective relations first, then push their integer join keys into
  // later scans (TPC-H Q9: filter lineitem by partkeys matching LIKE).
  std::vector<size_t> load_order;
  load_order.reserve(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) load_order.push_back(i);
  std::stable_sort(load_order.begin(), load_order.end(), [&](size_t a, size_t b) {
    const auto rank = [&](size_t i) {
      if (!local_predicates[i].empty()) return 0;
      if (active_runtime &&
          active_runtime->table_key_filters.contains(
              statement.Sources()[i].table)) {
        return 0;
      }
      return 1;
    };
    const int a_rank = rank(a);
    const int b_rank = rank(b);
    if (a_rank != b_rank) return a_rank < b_rank;
    return a < b;
  });
  std::vector<bool> loaded(relations.size(), false);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i]) loaded[i] = true;  // already materialized above
  }
  for (size_t idx : load_order) {
    if (!base_sources[idx]) {
      FilterRelation(context, &relations[idx], local_predicates[idx], outer,
                     ctes);
      loaded[idx] = true;
      continue;
    }
    std::unordered_set<int64_t> key_filter;
    std::optional<slot_t> key_column;  // offset in the projected scan row
    size_t best_driver_rows = std::numeric_limits<size_t>::max();
    bool best_driver_selective = false;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 ||
          !predicate.sources.contains(idx) ||
          !IsColumnEqualityPredicate(predicate.expression)) {
        continue;
      }
      size_t other = *predicate.sources.begin();
      if (other == idx) other = *std::next(predicate.sources.begin());
      if (!loaded[other]) continue;
      const bool other_selective =
          !local_predicates[other].empty() ||
          (active_runtime &&
           active_runtime->table_key_filters.contains(
               statement.Sources()[other].table));
      const size_t driver_rows = relations[other].TotalRows();
      // Prefer drivers that were themselves filtered (e.g. part LIKE), even
      // if an unfiltered neighbor has slightly fewer rows (supplier).
      if (key_column) {
        if (best_driver_selective && !other_selective) continue;
        if (best_driver_selective == other_selective &&
            driver_rows >= best_driver_rows) {
          continue;
        }
      }
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
      const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
      const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
      const auto other_right = LocalColumnOffset(relations[other].schema, rhs);
      std::optional<slot_t> idx_col;
      std::optional<slot_t> other_col;
      if (idx_left && other_right) {
        idx_col = static_cast<slot_t>(*idx_left);
        other_col = static_cast<slot_t>(*other_right);
      } else if (idx_right && other_left) {
        idx_col = static_cast<slot_t>(*idx_right);
        other_col = static_cast<slot_t>(*other_left);
      }
      if (!idx_col || !other_col) continue;
      if (!SingleIntegerJoinKey(relations[idx].schema, {*idx_col}) ||
          !SingleIntegerJoinKey(relations[other].schema, {*other_col})) {
        continue;
      }
      // Map full-schema column index into the projected scan layout.
      const auto& proj = projections[idx];
      const auto proj_it = std::find(proj.begin(), proj.end(), *idx_col);
      if (proj_it == proj.end()) continue;
      const slot_t projected_col =
          static_cast<slot_t>(std::distance(proj.begin(), proj_it));

      std::unordered_set<int64_t> candidate;
      relations[other].FinishSpill();
      relations[other].ForEachRow([&](const Row& row) {
        const Value& value = row[*other_col];
        if (!value.IsNull()) candidate.insert(value.value.int_value);
      });
      if (candidate.empty() || candidate.size() >= 2'000'000) continue;
      // Skip near-full key domains (e.g. all supplier keys vs lineitem).
      if (!other_selective && candidate.size() >= driver_rows &&
          driver_rows >= 1000) {
        continue;
      }
      key_filter = std::move(candidate);
      key_column = projected_col;
      best_driver_rows = driver_rows;
      best_driver_selective = other_selective;
    }
    const bool use_key_filter = key_column.has_value();
    if (use_key_filter && active_runtime) {
      ++active_runtime->key_filter_scans;
      active_runtime->key_filter_keys += key_filter.size();
      // Remember for correlated / later scans of this base table. Column is
      // stored as the full-schema slot so other projections can remap.
      const auto& proj = projections[idx];
      const slot_t full_slot = proj[*key_column];
      active_runtime->table_key_filters[statement.Sources()[idx].table] =
          key_filter;
      active_runtime->table_key_filter_columns[statement.Sources()[idx].table] =
          full_slot;
    }
    const std::unordered_set<int64_t>* filter_ptr =
        use_key_filter ? &key_filter : nullptr;
    std::optional<slot_t> filter_col = use_key_filter ? key_column : std::nullopt;
    if (!filter_ptr && active_runtime) {
      const auto stored = active_runtime->table_key_filters.find(
          statement.Sources()[idx].table);
      const auto stored_col = active_runtime->table_key_filter_columns.find(
          statement.Sources()[idx].table);
      if (stored != active_runtime->table_key_filters.end() &&
          stored_col != active_runtime->table_key_filter_columns.end()) {
        const auto& proj = projections[idx];
        const auto proj_it =
            std::find(proj.begin(), proj.end(), stored_col->second);
        if (proj_it != proj.end()) {
          filter_ptr = &stored->second;
          filter_col = static_cast<slot_t>(std::distance(proj.begin(), proj_it));
        }
      }
    }
    relations[idx] = LoadSource(context, statement.Sources()[idx], outer, ctes,
                                &projections[idx], &local_predicates[idx],
                                filter_ptr, filter_col);
    loaded[idx] = true;
  }

  size_t first = 0;
  for (size_t i = 1; i < relations.size(); ++i) {
    if (relations[i].rows.size() < relations[first].rows.size()) first = i;
  }
  Relation result = std::move(relations[first]);
  std::unordered_set<size_t> joined{first};
  std::unordered_set<size_t> remaining;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i != first) remaining.insert(i);
  }

  while (!remaining.empty()) {
    size_t next = *remaining.begin();
    size_t next_estimate = std::numeric_limits<size_t>::max();
    bool next_connected = false;
    for (size_t candidate : remaining) {
      std::unordered_set<size_t> after = joined;
      after.insert(candidate);
      std::vector<Expression> applicable;
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() < 2 ||
            !predicate.sources.contains(candidate) ||
            !IsSubset(predicate.sources, after)) {
          continue;
        }
        applicable.push_back(predicate.expression);
      }
      const size_t estimate =
          EstimateJoinRows(result, relations[candidate], applicable);
      const bool connected = !applicable.empty();
      const bool cheaper =
          estimate < next_estimate ||
          (estimate == next_estimate &&
           relations[candidate].rows.size() < relations[next].rows.size());
      if ((connected && !next_connected) ||
          (connected == next_connected && cheaper)) {
        next = candidate;
        next_estimate = estimate;
        next_connected = connected;
      }
    }

    std::unordered_set<size_t> after = joined;
    after.insert(next);
    std::vector<Expression> applicable;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() < 2 || !predicate.sources.contains(next) ||
          !IsSubset(predicate.sources, after)) {
        continue;
      }
      applicable.push_back(predicate.expression);
    }
    result = InnerJoin(context, std::move(result), std::move(relations[next]),
                       applicable, outer, ctes);
    joined.insert(next);
    remaining.erase(next);
  }
  return result;
}

ValueType ValueTypeOf(const Value& value) { return value.type; }

std::string ProjectionName(const NamedExpression& projection, size_t index) {
  if (!projection.name.empty()) return projection.name;
  if (projection.expression->Type() == TypeTag::kColumnValue) {
    return projection.expression->AsColumnValue().GetColumnName().name;
  }
  return "$expr" + std::to_string(index);
}

Relation Project(TransactionContext& context, const SelectStatement& statement,
                 Relation input, const Scope* outer, const CteMap& ctes) {
  const auto project_begin = std::chrono::steady_clock::now();
  const bool grouped =
      !statement.GroupBy().empty() ||
      std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                  [](const NamedExpression& projection) {
                    return ContainsAggregate(projection.expression);
                  }) ||
      ContainsAggregate(statement.Having());
  std::vector<const AggregateExpression*> aggregate_expressions;
  std::unordered_set<const AggregateExpression*> seen_aggregates;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectAggregates(projection.expression, &aggregate_expressions,
                      &seen_aggregates);
  }
  CollectAggregates(statement.Having(), &aggregate_expressions,
                    &seen_aggregates);

  struct GroupState {
    Row representative;
    size_t accumulator_offset{0};
  };
  std::deque<AggregateAccumulator> aggregate_states;
  auto make_group = [&]() {
    GroupState group;
    group.accumulator_offset = aggregate_states.size();
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      aggregate_states.emplace_back(aggregate);
    }
    return group;
  };

  std::vector<GroupState> groups;
  if (grouped) {
    input.FinishSpill();
    auto accumulate_row = [&](Row& row, std::unordered_map<Row, size_t>* offsets,
                              std::vector<GroupState>* local_groups,
                              std::deque<AggregateAccumulator>* local_states) {
      Scope scope{&row, &input.schema, outer};
      std::vector<Value> key_values;
      for (const Expression& key : statement.GroupBy()) {
        key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
      }
      Row key(std::move(key_values));
      auto [iter, inserted] = offsets->emplace(key, local_groups->size());
      if (inserted) {
        GroupState group;
        group.accumulator_offset = local_states->size();
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          local_states->emplace_back(aggregate);
        }
        local_groups->push_back(std::move(group));
      }
      GroupState& group = (*local_groups)[iter->second];
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        AggregateAccumulator& accumulator =
            (*local_states)[group.accumulator_offset + i];
        const AggregateExpression& aggregate = *accumulator.expression;
        const bool count_star =
            aggregate.GetType() == AggregationType::kCount &&
            aggregate.Child()->Type() == TypeTag::kColumnValue &&
            aggregate.Child()->AsColumnValue().GetColumnName().name == "*";
        accumulator.Add(
            count_star
                ? Value(1)
                : Evaluate(aggregate.Child(), scope, nullptr, context, ctes));
        if (active_runtime) ++active_runtime->aggregate_updates;
      }
      if (inserted) group.representative = std::move(row);
      if (active_runtime) ++active_runtime->aggregate_input_rows;
    };

    const bool partition_agg =
        !statement.GroupBy().empty() &&
        (input.HasSpill() ||
         !QueryMemoryBudget::Global().CanReserve(
             std::max<size_t>(1, input.TotalRows()) * 128));
    if (partition_agg) {
      std::vector<SpillFile> parts(kSpillPartitions);
      input.ForEachRow([&](const Row& row) {
        Scope scope{&row, &input.schema, outer};
        std::vector<Value> key_values;
        for (const Expression& key : statement.GroupBy()) {
          key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
        }
        Row key(std::move(key_values));
        parts[SpillPartitionOf(key.EncodeMemcomparableFormat(),
                               kSpillPartitions)]
            .Append(row);
      });
      for (SpillFile& part : parts) {
        part.FinishWriting();
      }
      input.rows.clear();
      input.rows.shrink_to_fit();
      input.ReleaseCharge();
      for (size_t part = 0; part < kSpillPartitions; ++part) {
        std::unordered_map<Row, size_t> offsets;
        std::vector<GroupState> local_groups;
        std::deque<AggregateAccumulator> local_states;
        parts[part].ForEachRow([&](const Row& row) {
          Row copy = row;
          accumulate_row(copy, &offsets, &local_groups, &local_states);
        });
        // Stash into the shared group vectors used by emit below.
        const size_t base = groups.size();
        for (GroupState& group : local_groups) {
          group.accumulator_offset += aggregate_states.size();
          groups.push_back(std::move(group));
        }
        for (AggregateAccumulator& state : local_states) {
          aggregate_states.push_back(std::move(state));
        }
        (void)base;
      }
    } else {
      std::unordered_map<Row, size_t> offsets;
      input.ForEachRow([&](const Row& row) {
        Row copy = row;
        accumulate_row(copy, &offsets, &groups, &aggregate_states);
      });
      if (input.TotalRows() == 0 && statement.GroupBy().empty()) {
        groups.push_back(make_group());
      }
    }
    if (active_runtime) active_runtime->aggregate_groups += groups.size();
  }

  Relation output;
  output.hash_joins = input.hash_joins;
  output.hybrid_hash_joins = input.hybrid_hash_joins;
  output.in_memory_hash_joins = input.in_memory_hash_joins;
  output.nested_loop_joins = input.nested_loop_joins;
  output.join_comparisons = input.join_comparisons;
  output.peak_intermediate_rows = input.peak_intermediate_rows;
  std::vector<Column> output_columns;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const NamedExpression& projection = statement.SelectList()[i];
    if (projection.expression->Type() == TypeTag::kColumnValue &&
        projection.expression->AsColumnValue().GetColumnName().name == "*") {
      for (size_t column = 0; column < input.schema.ColumnCount(); ++column) {
        output_columns.emplace_back(input.schema.GetColumn(column).Name().name,
                                    input.schema.GetColumn(column).Type());
      }
    } else {
      output_columns.emplace_back(ProjectionName(projection, i),
                                  ValueType::kNull);
    }
  }

  auto emit = [&](const Row& representative,
                  const AggregateResultMap* aggregates) {
    Scope scope{&representative, &input.schema, outer};
    if (statement.Having() &&
        !Truthy(Evaluate(statement.Having(), scope, aggregates, context,
                         ctes))) {
      return;
    }
    std::vector<Value> values;
    for (const NamedExpression& projection : statement.SelectList()) {
      if (projection.expression->Type() == TypeTag::kColumnValue &&
          projection.expression->AsColumnValue().GetColumnName().name == "*") {
        values.insert(values.end(), representative.values_.begin(),
                      representative.values_.end());
      } else {
        values.push_back(Evaluate(projection.expression, scope, aggregates,
                                  context, ctes));
      }
    }
    output.AddRow(Row(std::move(values)));
  };

  if (grouped) {
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        const AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        aggregate_results.emplace(accumulator.expression,
                                  accumulator.Finish());
      }
      emit(group.representative, &aggregate_results);
    }
  } else {
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) { emit(row, nullptr); });
  }
  output.FinishSpill();
  if (!output.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
    }
  }
  output.schema = Schema("", std::move(output_columns));
  if (active_runtime) active_runtime->project_ms += ElapsedMs(project_begin);
  return output;
}

Relation FinishQuery(TransactionContext& context,
                     const SelectStatement& statement, Relation input,
                     const Scope* outer, const CteMap& ctes,
                     bool apply_where) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    Relation filtered;
    filtered.schema = input.schema;
    filtered.hash_joins = input.hash_joins;
    filtered.hybrid_hash_joins = input.hybrid_hash_joins;
    filtered.in_memory_hash_joins = input.in_memory_hash_joins;
    filtered.nested_loop_joins = input.nested_loop_joins;
    filtered.join_comparisons = input.join_comparisons;
    input.FinishSpill();
    input.ForEachRow([&](const Row& row) {
      Scope scope{&row, &input.schema, outer};
      if (Truthy(Evaluate(statement.WhereClause(), scope, nullptr, context,
                          ctes))) {
        filtered.AddRow(row);
      }
    });
    filtered.FinishSpill();
    input = std::move(filtered);
    if (active_runtime) active_runtime->filter_ms += ElapsedMs(filter_begin);
  }

  Relation output = Project(context, statement, std::move(input), outer, ctes);
  if (statement.Distinct()) {
    std::unordered_set<Row> seen;
    Relation distinct;
    distinct.schema = output.schema;
    output.FinishSpill();
    output.ForEachRow([&](const Row& row) {
      if (seen.insert(row).second) distinct.AddRow(row);
    });
    distinct.FinishSpill();
    output = std::move(distinct);
  }
  if (!statement.OrderBy().empty()) {
    const auto sort_begin = std::chrono::steady_clock::now();
    output.FinishSpill();
    // External sort when the result does not fit the query memory budget.
    std::vector<Row> sortable;
    output.ForEachRow([&](const Row& row) { sortable.push_back(row); });
    output.rows.clear();
    output.ReleaseCharge();
    std::stable_sort(
        sortable.begin(), sortable.end(),
        [&](const Row& left, const Row& right) {
          Scope left_scope{&left, &output.schema, outer};
          Scope right_scope{&right, &output.schema, outer};
          for (const auto& key : statement.OrderBy()) {
            const Value a =
                Evaluate(key.expression, left_scope, nullptr, context, ctes);
            const Value b =
                Evaluate(key.expression, right_scope, nullptr, context, ctes);
            if (a == b) continue;
            if (a.IsNull()) return key.ascending;
            if (b.IsNull()) return !key.ascending;
            return key.ascending ? a < b : b < a;
          }
          return false;
        });
    for (Row& row : sortable) {
      output.AddRow(std::move(row));
    }
    output.FinishSpill();
    if (active_runtime) active_runtime->sort_ms += ElapsedMs(sort_begin);
  }
  output.FinishSpill();
  std::vector<Row> all_rows;
  output.ForEachRow([&](const Row& row) { all_rows.push_back(row); });
  const size_t begin = std::min(statement.Offset(), all_rows.size());
  const size_t available = all_rows.size() - begin;
  const size_t count = statement.Limit() == 0
                           ? available
                           : std::min(statement.Limit(), available);
  Relation limited;
  limited.schema = output.schema;
  limited.hash_joins = output.hash_joins;
  limited.hybrid_hash_joins = output.hybrid_hash_joins;
  limited.in_memory_hash_joins = output.in_memory_hash_joins;
  limited.nested_loop_joins = output.nested_loop_joins;
  limited.join_comparisons = output.join_comparisons;
  for (size_t i = 0; i < count; ++i) {
    limited.AddRow(std::move(all_rows[begin + i]));
  }
  limited.FinishSpill();
  return limited;
}

std::optional<Relation> ExecuteCorrelatedSingleSource(
    TransactionContext& context, const SelectStatement& statement,
    const Scope& outer, const CteMap& ctes) {
  if (!active_runtime || statement.Sources().empty() ||
      std::ranges::any_of(
          statement.Sources(),
          [](const SelectSource& source) { return source.query != nullptr; }) ||
      active_runtime->unindexable_queries.contains(&statement)) {
    return std::nullopt;
  }

  CorrelatedIndex* index = nullptr;
  const auto cached = active_runtime->correlated_indexes.find(&statement);
  if (cached != active_runtime->correlated_indexes.end()) {
    index = cached->second.get();
  } else {
    const SelectSource& from = statement.Sources()[0];
    Schema peek_schema;
    if (statement.Sources().size() == 1 && !from.query && !from.table.empty()) {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(from.table);
      if (table.HasValue()) peek_schema = table.Value()->GetSchema();
    }
    auto has_correlated_equality = [&](const Schema& schema) {
      if (schema.ColumnCount() == 0) return false;
      for (const Expression& predicate :
           SplitConjuncts(statement.WhereClause())) {
        if (predicate->Type() != TypeTag::kBinaryExp) continue;
        const BinaryExpression& binary = predicate->AsBinaryExpression();
        if (binary.Op() != BinaryOperation::kEquals ||
            binary.Left()->Type() != TypeTag::kColumnValue ||
            binary.Right()->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& left =
            binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& right =
            binary.Right()->AsColumnValue().GetColumnName();
        const auto left_local = LocalColumnOffset(schema, left);
        const auto right_local = LocalColumnOffset(schema, right);
        if ((left_local && !right_local) || (right_local && !left_local)) {
          return true;
        }
      }
      return false;
    };
    if (statement.Sources().size() == 1 && peek_schema.ColumnCount() > 0 &&
        !has_correlated_equality(peek_schema)) {
      active_runtime->unindexable_queries.insert(&statement);
      return std::nullopt;
    }
    Relation source;
    if (statement.Sources().size() == 1) {
      std::vector<slot_t> projection;
      if (peek_schema.ColumnCount() > 0) {
        if (const std::vector<slot_t>* shared =
                ReusableProjection(from.table)) {
          projection = *shared;
        } else {
          projection = RequiredColumns(statement, peek_schema, true);
        }
        if (projection.empty()) projection.push_back(0);
      }
      const std::unordered_set<int64_t>* filter_ptr = nullptr;
      std::optional<slot_t> filter_col;
      if (active_runtime) {
        const auto stored =
            active_runtime->table_key_filters.find(from.table);
        const auto stored_col =
            active_runtime->table_key_filter_columns.find(from.table);
        if (stored != active_runtime->table_key_filters.end() &&
            stored_col != active_runtime->table_key_filter_columns.end()) {
          const auto proj_it = std::find(projection.begin(), projection.end(),
                                         stored_col->second);
          if (proj_it != projection.end()) {
            filter_ptr = &stored->second;
            filter_col =
                static_cast<slot_t>(std::distance(projection.begin(), proj_it));
          }
        }
      }
      source = LoadSource(context, from, &outer, ctes,
                          projection.empty() ? nullptr : &projection, nullptr,
                          filter_ptr, filter_col);
    } else {
      bool predicates_applied = false;
      source =
          BuildInput(context, statement, &outer, ctes, &predicates_applied);
    }
    auto created = std::make_unique<CorrelatedIndex>();
    created->schema = source.schema;
    std::vector<Expression> indexed_equalities;
    for (const Expression& predicate :
         SplitConjuncts(statement.WhereClause())) {
      if (predicate->Type() != TypeTag::kBinaryExp) continue;
      const BinaryExpression& binary = predicate->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals ||
          binary.Left()->Type() != TypeTag::kColumnValue ||
          binary.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& left = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& right = binary.Right()->AsColumnValue().GetColumnName();
      const auto left_local = LocalColumnOffset(source.schema, left);
      const auto right_local = LocalColumnOffset(source.schema, right);
      if (left_local && !right_local) {
        created->local_columns.push_back(static_cast<slot_t>(*left_local));
        created->outer_columns.push_back(right);
        indexed_equalities.push_back(predicate);
      } else if (right_local && !left_local) {
        created->local_columns.push_back(static_cast<slot_t>(*right_local));
        created->outer_columns.push_back(left);
        indexed_equalities.push_back(predicate);
      }
    }
    if (created->local_columns.empty()) {
      active_runtime->unindexable_queries.insert(&statement);
      return std::nullopt;
    }
    std::vector<Expression> correlated_expressions =
        SplitConjuncts(statement.WhereClause());
    for (const NamedExpression& item : statement.SelectList()) {
      correlated_expressions.push_back(item.expression);
    }
    correlated_expressions.insert(correlated_expressions.end(),
                                  statement.GroupBy().begin(),
                                  statement.GroupBy().end());
    if (statement.Having()) {
      correlated_expressions.push_back(statement.Having());
    }
    for (const Expression& expression : correlated_expressions) {
      for (const ColumnName& column : expression->TouchedColumns()) {
        if (column.name == "*") continue;
        if (LocalColumnOffset(source.schema, column)) continue;
        if (std::ranges::find(created->cache_outer_columns, column) ==
            created->cache_outer_columns.end()) {
          created->cache_outer_columns.push_back(column);
        }
      }
    }

    const bool aggregate_only =
        statement.GroupBy().empty() && !statement.SelectList().empty() &&
        std::all_of(statement.SelectList().begin(), statement.SelectList().end(),
                    [](const NamedExpression& item) {
                      return ContainsAggregate(item.expression);
                    });
    std::vector<Expression> local_predicates;
    for (const Expression& predicate :
         SplitConjuncts(statement.WhereClause())) {
      const bool indexed =
          std::find(indexed_equalities.begin(), indexed_equalities.end(),
                    predicate) != indexed_equalities.end();
      if (indexed) continue;
      local_predicates.push_back(predicate);
    }

    source.FinishSpill();
    const bool integer_key =
        SingleIntegerJoinKey(source.schema, created->local_columns);
    if (aggregate_only) {
      // One-pass hash aggregate into finished scalar results. Storing only
      // aggregates (not every lineitem row) keeps Q17-style subqueries small.
      created->preaggregated = true;
      std::vector<const AggregateExpression*> aggregate_expressions;
      std::unordered_set<const AggregateExpression*> seen_aggregates;
      for (const NamedExpression& item : statement.SelectList()) {
        CollectAggregates(item.expression, &aggregate_expressions,
                          &seen_aggregates);
      }
      struct GroupAggs {
        std::vector<AggregateAccumulator> accumulators;
      };
      std::unordered_map<int64_t, GroupAggs> int_groups;
      std::unordered_map<std::string, GroupAggs> str_groups;
      const CompiledScanFilter local_filter =
          CompileScanFilter(local_predicates, source.schema);
      source.ForEachRow([&](const Row& row) {
        if (HasNullKey(row, created->local_columns)) return;
        if (!MatchScanFilter(row, source.schema, local_filter, nullptr, context,
                             ctes)) {
          return;
        }
        GroupAggs* group = nullptr;
        std::string str_key;
        int64_t int_key = 0;
        if (integer_key) {
          int_key = IntegerJoinKey(row, created->local_columns[0]);
          group = &int_groups[int_key];
        } else {
          str_key = EncodeJoinKey(row, created->local_columns);
          group = &str_groups[str_key];
        }
        if (group->accumulators.empty()) {
          group->accumulators.reserve(aggregate_expressions.size());
          for (const AggregateExpression* aggregate : aggregate_expressions) {
            group->accumulators.emplace_back(aggregate);
          }
        }
        Scope scope{&row, &source.schema, nullptr};
        for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
          const AggregateExpression& aggregate = *aggregate_expressions[i];
          const bool count_star =
              aggregate.GetType() == AggregationType::kCount &&
              aggregate.Child()->Type() == TypeTag::kColumnValue &&
              aggregate.Child()->AsColumnValue().GetColumnName().name == "*";
          group->accumulators[i].Add(
              count_star ? Value(1)
                         : Evaluate(aggregate.Child(), scope, nullptr, context,
                                    ctes));
        }
      });
      auto emit_group = [&](const std::string& key, GroupAggs& group) {
        AggregateResultMap aggregate_results;
        aggregate_results.reserve(group.accumulators.size());
        for (const AggregateAccumulator& accumulator : group.accumulators) {
          aggregate_results.emplace(accumulator.expression,
                                    accumulator.Finish());
        }
        Row representative;
        Scope scope{&representative, &source.schema, nullptr};
        std::vector<Value> values;
        values.reserve(statement.SelectList().size());
        for (const NamedExpression& item : statement.SelectList()) {
          values.push_back(Evaluate(item.expression, scope, &aggregate_results,
                                    context, ctes));
        }
        Relation finished;
        finished.AddRow(Row(std::move(values)));
        std::vector<Column> columns;
        columns.reserve(finished.rows[0].values_.size());
        for (size_t i = 0; i < finished.rows[0].values_.size(); ++i) {
          columns.emplace_back(ProjectionName(statement.SelectList()[i], i),
                               ValueTypeOf(finished.rows[0][i]));
        }
        finished.schema = Schema("", std::move(columns));
        created->cached_results.emplace(key, std::move(finished));
      };
      if (integer_key) {
        for (auto& [key, group] : int_groups) {
          emit_group(Row({Value(key)}).EncodeMemcomparableFormat(), group);
        }
      } else {
        for (auto& [key, group] : str_groups) {
          emit_group(key, group);
        }
      }
    } else {
      source.ForEachRow([&](const Row& row) {
        if (HasNullKey(row, created->local_columns)) return;
        if (!local_predicates.empty()) {
          Scope scope{&row, &source.schema, nullptr};
          for (const Expression& predicate : local_predicates) {
            if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
              return;
            }
          }
        }
        const std::string key = EncodeJoinKey(row, created->local_columns);
        created->rows.emplace(key, row);
      });
    }
    source.rows.clear();
    source.rows.shrink_to_fit();
    source.ReleaseCharge();
    source.spill.reset();
    source.spill_tail_.reset();
    index = created.get();
    active_runtime->correlated_indexes.emplace(&statement, std::move(created));
    ++active_runtime->correlated_index_builds;
  }

  ++active_runtime->correlated_index_probes;

  std::vector<Value> cache_values;
  cache_values.reserve(index->cache_outer_columns.size());
  for (const ColumnName& column : index->cache_outer_columns) {
    cache_values.push_back(Lookup(column, outer));
  }
  const std::string cache_key =
      Row(std::move(cache_values)).EncodeMemcomparableFormat();
  if (const auto cached_result = index->cached_results.find(cache_key);
      cached_result != index->cached_results.end()) {
    ++active_runtime->correlated_result_cache_hits;
    return cached_result->second;
  }

  if (index->preaggregated) {
    Relation empty;
    empty.schema = index->schema;
    return FinishQuery(context, statement, std::move(empty), &outer, ctes,
                       false);
  }

  std::vector<Value> outer_values;
  outer_values.reserve(index->outer_columns.size());
  for (const ColumnName& column : index->outer_columns) {
    Value value = Lookup(column, outer);
    if (value.IsNull()) {
      Relation empty;
      empty.schema = index->schema;
      return FinishQuery(context, statement, std::move(empty), &outer, ctes);
    }
    outer_values.push_back(std::move(value));
  }
  const std::string key =
      Row(std::move(outer_values)).EncodeMemcomparableFormat();
  Relation candidates;
  candidates.schema = index->schema;
  const auto [begin, end] = index->rows.equal_range(key);
  for (auto iter = begin; iter != end; ++iter) {
    candidates.rows.push_back(iter->second);
  }
  candidates.peak_intermediate_rows = candidates.rows.size();
  Relation result =
      FinishQuery(context, statement, std::move(candidates), &outer, ctes);
  index->cached_results.emplace(cache_key, result);
  return result;
}

bool ExpressionUsesOnlyScopes(TransactionContext& context,
                              const Expression& expression,
                              const std::vector<Relation>& sources,
                              const CteMap& ctes) {
  if (!expression) return true;
  if (expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& column = expression->AsColumnValue().GetColumnName();
    if (column.name == "*") return true;
    return std::ranges::any_of(sources, [&](const Relation& source) {
      return LocalColumnOffset(source.schema, column).has_value();
    });
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    return ExpressionUsesOnlyScopes(context, query.Test(), sources, ctes) &&
           StatementUsesOnlyScopes(context, *query.Query(), sources, ctes);
  }
  return std::ranges::all_of(
      ExpressionChildren(expression), [&](const Expression& child) {
        return ExpressionUsesOnlyScopes(context, child, sources, ctes);
      });
}

bool StatementUsesOnlyScopes(TransactionContext& context,
                             const SelectStatement& statement,
                             const std::vector<Relation>& outer_sources,
                             const CteMap& ctes) {
  std::vector<Relation> scopes = outer_sources;
  for (const SelectSource& source : statement.Sources()) {
    if (source.query) return false;
    Relation metadata;
    if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      metadata.schema = cte->second.schema;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) return false;
      metadata.schema = table.Value()->GetSchema();
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty()) {
      metadata.schema = QualifySchema(metadata.schema, qualifier);
    }
    scopes.push_back(std::move(metadata));
    if (!ExpressionUsesOnlyScopes(context, source.join_condition, scopes,
                                  ctes)) {
      return false;
    }
  }
  std::vector<Expression> expressions = SplitConjuncts(statement.WhereClause());
  for (const NamedExpression& item : statement.SelectList()) {
    expressions.push_back(item.expression);
  }
  expressions.insert(expressions.end(), statement.GroupBy().begin(),
                     statement.GroupBy().end());
  if (statement.Having()) expressions.push_back(statement.Having());
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    expressions.push_back(term.expression);
  }
  return std::ranges::all_of(expressions, [&](const Expression& expression) {
    return ExpressionUsesOnlyScopes(context, expression, scopes, ctes);
  });
}

bool ExpressionsAreLocal(TransactionContext& context,
                         const SelectStatement& statement,
                         const std::vector<Relation>& sources,
                         const CteMap& ctes) {
  std::vector<Expression> expressions = SplitConjuncts(statement.WhereClause());
  for (const NamedExpression& item : statement.SelectList()) {
    expressions.push_back(item.expression);
  }
  expressions.insert(expressions.end(), statement.GroupBy().begin(),
                     statement.GroupBy().end());
  if (statement.Having()) expressions.push_back(statement.Having());
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    expressions.push_back(term.expression);
  }
  for (const Expression& expression : expressions) {
    if (!ExpressionUsesOnlyScopes(context, expression, sources, ctes)) {
      return false;
    }
    for (const ColumnName& column : expression->TouchedColumns()) {
      bool found = false;
      for (const Relation& source : sources) {
        if (LocalColumnOffset(source.schema, column)) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
  }
  return true;
}

const Relation* ExecuteCachedUncorrelated(TransactionContext& context,
                                          const SelectStatement& statement,
                                          const CteMap& ctes) {
  if (!active_runtime ||
      active_runtime->noncacheable_queries.contains(&statement)) {
    return nullptr;
  }
  const auto cached = active_runtime->uncorrelated_results.find(&statement);
  if (cached != active_runtime->uncorrelated_results.end()) {
    ++active_runtime->uncorrelated_cache_hits;
    return &cached->second;
  }

  std::vector<Relation> schemas;
  schemas.reserve(statement.Sources().size());
  for (const SelectSource& source : statement.Sources()) {
    Relation metadata;
    if (source.query) {
      if (!StatementUsesOnlyScopes(context, *source.query, {}, ctes)) {
        active_runtime->noncacheable_queries.insert(&statement);
        return nullptr;
      }
      std::vector<Column> columns;
      columns.reserve(source.query->SelectList().size());
      for (size_t i = 0; i < source.query->SelectList().size(); ++i) {
        const NamedExpression& projection = source.query->SelectList()[i];
        if (projection.expression->Type() == TypeTag::kColumnValue &&
            projection.expression->AsColumnValue().GetColumnName().name ==
                "*") {
          active_runtime->noncacheable_queries.insert(&statement);
          return nullptr;
        }
        columns.emplace_back(ProjectionName(projection, i), ValueType::kNull);
      }
      metadata.schema = Schema("", std::move(columns));
    } else if (const auto cte = ctes.find(source.table); cte != ctes.end()) {
      metadata.schema = cte->second.schema;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        active_runtime->noncacheable_queries.insert(&statement);
        return nullptr;
      }
      metadata.schema = table.Value()->GetSchema();
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    if (!qualifier.empty()) {
      metadata.schema = QualifySchema(metadata.schema, qualifier);
    }
    schemas.push_back(std::move(metadata));
  }
  if (!ExpressionsAreLocal(context, statement, schemas, ctes)) {
    active_runtime->noncacheable_queries.insert(&statement);
    return nullptr;
  }
  Relation result = ExecuteQuery(context, statement, nullptr, ctes);
  auto [iter, inserted] = active_runtime->uncorrelated_results.emplace(
      &statement, std::move(result));
  return &iter->second;
}

Relation ExecuteQuery(TransactionContext& context,
                      const SelectStatement& statement, const Scope* outer,
                      const CteMap& inherited_ctes) {
  EnsureReusableProjections(context, active_runtime);
  CteMap ctes = inherited_ctes;
  for (const auto& [name, query] : statement.WithQueries()) {
    ctes[name] = ExecuteQuery(context, *query, outer, ctes);
  }

  // Single-table aggregation: filter and aggregate while scanning so we never
  // materialize millions of qualifying rows (TPC-H Q1/Q6/Q21-derived pattern).
  // When the table is reusable, populate the shared cache once then aggregate
  // from the cache without deep-copying into an intermediate relation.
  const bool stream_agg =
      outer == nullptr && statement.WithQueries().empty() &&
      statement.Sources().size() == 1 && !statement.Sources()[0].query &&
      !ctes.contains(statement.Sources()[0].table) &&
      (!statement.WhereClause() || !ContainsQuery(statement.WhereClause())) &&
      (!statement.GroupBy().empty() ||
       std::any_of(statement.SelectList().begin(), statement.SelectList().end(),
                   [](const NamedExpression& projection) {
                     return ContainsAggregate(projection.expression);
                   }) ||
       ContainsAggregate(statement.Having()));
  if (stream_agg) {
    const SelectSource& source = statement.Sources()[0];
    const bool reusable =
        active_runtime &&
        active_runtime->reusable_base_relations.contains(source.table);
    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      throw std::runtime_error("table " + source.table + " not found");
    }
    const Schema& table_schema = table.Value()->GetSchema();
    std::vector<slot_t> projection = RequiredColumns(statement, table_schema);
    if (const std::vector<slot_t>* shared = ReusableProjection(source.table)) {
      projection = *shared;
    }
    Schema scan_schema = projection.empty()
                             ? table_schema
                             : ProjectSchema(table_schema, projection);
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    Schema qualified_schema =
        qualifier.empty() ? scan_schema
                          : QualifySchema(scan_schema, qualifier);

    std::vector<Expression> scan_predicates =
        SplitConjuncts(statement.WhereClause());
    CompiledScanFilter scan_filter =
        CompileScanFilter(scan_predicates, scan_schema);

    Relation input;
    input.schema = qualified_schema;
    // Project aggregates against the qualified schema; feed rows one at a time
    // without retaining them in input.rows.
    std::vector<const AggregateExpression*> aggregate_expressions;
    std::unordered_set<const AggregateExpression*> seen_aggregates;
    for (const NamedExpression& projection_item : statement.SelectList()) {
      CollectAggregates(projection_item.expression, &aggregate_expressions,
                        &seen_aggregates);
    }
    CollectAggregates(statement.Having(), &aggregate_expressions,
                      &seen_aggregates);

    struct GroupState {
      Row representative;
      size_t accumulator_offset{0};
    };
    std::deque<AggregateAccumulator> aggregate_states;
    std::vector<GroupState> groups;
    std::unordered_map<Row, size_t> offsets;

    std::vector<std::optional<slot_t>> group_offsets;
    group_offsets.reserve(statement.GroupBy().size());
    bool group_keys_are_columns = true;
    for (const Expression& key : statement.GroupBy()) {
      if (key->Type() != TypeTag::kColumnValue) {
        group_keys_are_columns = false;
        group_offsets.push_back(std::nullopt);
        continue;
      }
      group_offsets.push_back(LocalColumnOffset(
          input.schema, key->AsColumnValue().GetColumnName()));
      if (!group_offsets.back()) group_keys_are_columns = false;
    }
    std::vector<std::optional<slot_t>> aggregate_child_offsets;
    aggregate_child_offsets.reserve(aggregate_expressions.size());
    for (const AggregateExpression* aggregate : aggregate_expressions) {
      const bool count_star =
          aggregate->GetType() == AggregationType::kCount &&
          aggregate->Child()->Type() == TypeTag::kColumnValue &&
          aggregate->Child()->AsColumnValue().GetColumnName().name == "*";
      if (count_star) {
        aggregate_child_offsets.push_back(std::nullopt);  // sentinel via count*
        continue;
      }
      if (aggregate->Child()->Type() == TypeTag::kColumnValue) {
        aggregate_child_offsets.push_back(LocalColumnOffset(
            input.schema, aggregate->Child()->AsColumnValue().GetColumnName()));
      } else {
        aggregate_child_offsets.push_back(std::nullopt);
      }
    }
    const bool count_star_flags_size = aggregate_expressions.size();
    std::vector<bool> is_count_star(count_star_flags_size, false);
    for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
      const AggregateExpression& aggregate = *aggregate_expressions[i];
      is_count_star[i] =
          aggregate.GetType() == AggregationType::kCount &&
          aggregate.Child()->Type() == TypeTag::kColumnValue &&
          aggregate.Child()->AsColumnValue().GetColumnName().name == "*";
    }

    const auto scan_begin = std::chrono::steady_clock::now();
    auto accumulate_row = [&](Row row) {
      if (active_runtime) ++active_runtime->scan_output_rows;
      Scope scope{&row, &input.schema, outer};
      std::vector<Value> key_values;
      key_values.reserve(statement.GroupBy().size());
      if (group_keys_are_columns) {
        for (const auto& offset : group_offsets) {
          key_values.push_back(row[*offset]);
        }
      } else {
        for (const Expression& key : statement.GroupBy()) {
          key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
        }
      }
      Row key(std::move(key_values));
      auto [iter, inserted] = offsets.emplace(key, groups.size());
      if (inserted) {
        GroupState group;
        group.accumulator_offset = aggregate_states.size();
        for (const AggregateExpression* aggregate : aggregate_expressions) {
          aggregate_states.emplace_back(aggregate);
        }
        group.representative = row;
        groups.push_back(std::move(group));
      }
      GroupState& group = groups[iter->second];
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
        if (is_count_star[i]) {
          accumulator.Add(Value(1));
        } else if (aggregate_child_offsets[i]) {
          accumulator.Add(row[*aggregate_child_offsets[i]]);
        } else {
          accumulator.Add(Evaluate(aggregate_expressions[i]->Child(), scope,
                                   nullptr, context, ctes));
        }
        if (active_runtime) ++active_runtime->aggregate_updates;
      }
      if (active_runtime) ++active_runtime->aggregate_input_rows;
    };

    if (reusable) {
      const std::string cache_key = BaseRelationCacheKey(
          source.table, projection.empty() ? nullptr : &projection);
      auto cached = active_runtime->base_relations.find(cache_key);
      if (cached == active_runtime->base_relations.end()) {
        Relation cache_rel;
        cache_rel.schema = scan_schema;
        // Apply any stashed integer key filter while filling the shared cache.
        const std::unordered_set<int64_t>* filter_ptr = nullptr;
        std::optional<slot_t> filter_col;
        const auto stored =
            active_runtime->table_key_filters.find(source.table);
        const auto stored_col =
            active_runtime->table_key_filter_columns.find(source.table);
        if (stored != active_runtime->table_key_filters.end() &&
            stored_col != active_runtime->table_key_filter_columns.end()) {
          if (projection.empty()) {
            filter_ptr = &stored->second;
            filter_col = stored_col->second;
          } else {
            const auto proj_it = std::find(projection.begin(), projection.end(),
                                           stored_col->second);
            if (proj_it != projection.end()) {
              filter_ptr = &stored->second;
              filter_col = static_cast<slot_t>(
                  std::distance(projection.begin(), proj_it));
            }
          }
        }
        std::optional<slot_t> full_key_column = filter_col;
        if (filter_ptr && filter_col && !projection.empty()) {
          full_key_column = projection[*filter_col];
        }
        Iterator iterator =
            full_key_column
                ? (projection.empty()
                       ? table.Value()->BeginFullScan(context.txn_, filter_ptr,
                                                      *full_key_column)
                       : table.Value()->BeginFullScan(context.txn_, projection,
                                                      filter_ptr,
                                                      *full_key_column))
                : (projection.empty()
                       ? table.Value()->BeginFullScan(context.txn_)
                       : table.Value()->BeginFullScan(context.txn_,
                                                      projection));
        while (iterator.IsValid()) {
          if (active_runtime) {
            ++active_runtime->scan_rows;
            active_runtime->scan_values_available += table_schema.ColumnCount();
            active_runtime->scan_values_decoded += scan_schema.ColumnCount();
          }
          cache_rel.AddRow(*iterator);
          ++iterator;
        }
        cache_rel.FinishSpill();
        cached =
            active_runtime->base_relations.emplace(cache_key, std::move(cache_rel))
                .first;
      } else {
        ++active_runtime->base_scan_cache_hits;
      }
      cached->second.FinishSpill();
      cached->second.ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, scan_schema, scan_filter, outer, context,
                             ctes)) {
          return;
        }
        accumulate_row(row);
      });
    } else {
      // Apply stashed key filters on the direct scan path too.
      const std::unordered_set<int64_t>* filter_ptr = nullptr;
      std::optional<slot_t> full_key_column;
      if (active_runtime) {
        const auto stored =
            active_runtime->table_key_filters.find(source.table);
        const auto stored_col =
            active_runtime->table_key_filter_columns.find(source.table);
        if (stored != active_runtime->table_key_filters.end() &&
            stored_col != active_runtime->table_key_filter_columns.end()) {
          filter_ptr = &stored->second;
          full_key_column = stored_col->second;
        }
      }
      Iterator iterator =
          full_key_column
              ? (projection.empty()
                     ? table.Value()->BeginFullScan(context.txn_, filter_ptr,
                                                    *full_key_column)
                     : table.Value()->BeginFullScan(context.txn_, projection,
                                                    filter_ptr,
                                                    *full_key_column))
              : (projection.empty()
                     ? table.Value()->BeginFullScan(context.txn_)
                     : table.Value()->BeginFullScan(context.txn_, projection));
      while (iterator.IsValid()) {
        if (active_runtime) {
          ++active_runtime->scan_rows;
          active_runtime->scan_values_available += table_schema.ColumnCount();
          active_runtime->scan_values_decoded += scan_schema.ColumnCount();
        }
        if (!MatchScanFilter(*iterator, scan_schema, scan_filter, outer, context,
                             ctes)) {
          ++iterator;
          continue;
        }
        accumulate_row(*iterator);
        ++iterator;
      }
    }
    if (active_runtime) {
      active_runtime->scan_ms += ElapsedMs(scan_begin);
      active_runtime->filter_ms += ElapsedMs(scan_begin);
      active_runtime->aggregate_groups += groups.size();
    }
    if (groups.empty() && statement.GroupBy().empty()) {
      GroupState group;
      group.accumulator_offset = aggregate_states.size();
      for (const AggregateExpression* aggregate : aggregate_expressions) {
        aggregate_states.emplace_back(aggregate);
      }
      groups.push_back(std::move(group));
    }

    Relation output;
    output.schema = input.schema;
    std::vector<Column> output_columns;
    for (size_t i = 0; i < statement.SelectList().size(); ++i) {
      const NamedExpression& projection_item = statement.SelectList()[i];
      output_columns.emplace_back(ProjectionName(projection_item, i),
                                  ValueType::kNull);
    }
    for (const GroupState& group : groups) {
      AggregateResultMap aggregate_results;
      aggregate_results.reserve(aggregate_expressions.size());
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        aggregate_results.emplace(
            aggregate_expressions[i],
            aggregate_states[group.accumulator_offset + i].Finish());
      }
      Scope scope{&group.representative, &input.schema, outer};
      if (statement.Having() &&
          !Truthy(Evaluate(statement.Having(), scope, &aggregate_results,
                           context, ctes))) {
        continue;
      }
      std::vector<Value> values;
      values.reserve(statement.SelectList().size());
      for (const NamedExpression& projection_item : statement.SelectList()) {
        values.push_back(Evaluate(projection_item.expression, scope,
                                  &aggregate_results, context, ctes));
      }
      output.AddRow(Row(std::move(values)));
    }
    for (size_t i = 0; i < output_columns.size() && !output.rows.empty(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
    }
    output.schema = Schema("", std::move(output_columns));
    output.FinishSpill();
    // DISTINCT / ORDER BY / LIMIT without running Project again.
    if (statement.Distinct()) {
      std::unordered_set<Row> seen;
      Relation distinct;
      distinct.schema = output.schema;
      output.ForEachRow([&](const Row& row) {
        if (seen.insert(row).second) distinct.AddRow(row);
      });
      distinct.FinishSpill();
      output = std::move(distinct);
    }
    if (!statement.OrderBy().empty()) {
      const auto sort_begin = std::chrono::steady_clock::now();
      output.FinishSpill();
      std::vector<Row> sortable;
      output.ForEachRow([&](const Row& row) { sortable.push_back(row); });
      output.rows.clear();
      output.ReleaseCharge();
      std::stable_sort(
          sortable.begin(), sortable.end(),
          [&](const Row& left, const Row& right) {
            Scope left_scope{&left, &output.schema, outer};
            Scope right_scope{&right, &output.schema, outer};
            for (const auto& key : statement.OrderBy()) {
              const Value a =
                  Evaluate(key.expression, left_scope, nullptr, context, ctes);
              const Value b =
                  Evaluate(key.expression, right_scope, nullptr, context, ctes);
              if (a == b) continue;
              if (a.IsNull()) return key.ascending;
              if (b.IsNull()) return !key.ascending;
              return key.ascending ? a < b : b < a;
            }
            return false;
          });
      for (Row& row : sortable) {
        output.AddRow(std::move(row));
      }
      output.FinishSpill();
      if (active_runtime) active_runtime->sort_ms += ElapsedMs(sort_begin);
    }
    output.FinishSpill();
    std::vector<Row> all_rows;
    output.ForEachRow([&](const Row& row) { all_rows.push_back(row); });
    const size_t begin = std::min(statement.Offset(), all_rows.size());
    const size_t available = all_rows.size() - begin;
    const size_t count = statement.Limit() == 0
                             ? available
                             : std::min(statement.Limit(), available);
    Relation limited;
    limited.schema = output.schema;
    limited.hash_joins = output.hash_joins;
    limited.hybrid_hash_joins = output.hybrid_hash_joins;
    limited.in_memory_hash_joins = output.in_memory_hash_joins;
    limited.nested_loop_joins = output.nested_loop_joins;
    limited.join_comparisons = output.join_comparisons;
    limited.peak_intermediate_rows = output.peak_intermediate_rows;
    for (size_t i = 0; i < count; ++i) {
      limited.AddRow(std::move(all_rows[begin + i]));
    }
    limited.FinishSpill();
    return limited;
  }

  bool where_fully_applied = false;
  Relation input =
      BuildInput(context, statement, outer, ctes, &where_fully_applied);

  return FinishQuery(context, statement, std::move(input), outer, ctes,
                     !where_fully_applied);
}

std::string IndentLines(std::string_view text, int spaces) {
  if (text.empty()) return {};
  const std::string pad(static_cast<size_t>(spaces), ' ');
  std::ostringstream out;
  size_t start = 0;
  while (start < text.size()) {
    const size_t end = text.find('\n', start);
    out << pad;
    if (end == std::string::npos) {
      out << text.substr(start);
      break;
    }
    out << text.substr(start, end - start) << '\n';
    start = end + 1;
  }
  return out.str();
}

std::string FormatBytes(size_t bytes) {
  std::ostringstream out;
  if (bytes >= (size_t{1} << 30)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 30)) << "GiB";
  } else if (bytes >= (size_t{1} << 20)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 20)) << "MiB";
  } else if (bytes >= (size_t{1} << 10)) {
    out << std::fixed << std::setprecision(1)
        << (static_cast<double>(bytes) / (size_t{1} << 10)) << "KiB";
  } else {
    out << bytes << "B";
  }
  return out.str();
}

size_t StatsRows(TransactionContext& context, const std::string& table,
                 bool* rows_known) {
  StatusOr<std::shared_ptr<TableStatistics>> stats = context.GetStats(table);
  if (!stats.HasValue()) {
    *rows_known = false;
    return 0;
  }
  const size_t rows = stats.Value()->Rows();
  // Fresh / unloaded stats are all zeros; treat as unknown for planning.
  *rows_known = rows > 0;
  return rows;
}

struct EstimatedPlanNode {
  Schema schema;
  size_t rows{0};
  bool rows_known{false};
  std::string text;
};

std::string FormatRows(const EstimatedPlanNode& node) {
  if (!node.rows_known) return "unknown";
  return std::to_string(node.rows);
}

bool PreferHybridForBuild(const EstimatedPlanNode& build) {
  if (!build.rows_known) {
    // Without cardinality stats, prefer Hybrid under a finite memory budget.
    return !QueryMemoryBudget::Global().Unlimited();
  }
  return PreferHybridHashJoin(build.rows * kHashJoinRowBytesEstimate);
}

EstimatedPlanNode MakeScanNode(TransactionContext& context,
                               const SelectSource& source,
                               const CteMap& /*ctes*/) {
  EstimatedPlanNode node;
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (source.query) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "SubqueryScan AS " + qualifier + " rows~unknown";
    std::vector<Column> columns;
    for (size_t i = 0; i < source.query->SelectList().size(); ++i) {
      columns.emplace_back(ProjectionName(source.query->SelectList()[i], i),
                           ValueType::kNull);
    }
    node.schema = Schema("", std::move(columns));
    if (!qualifier.empty()) {
      node.schema = QualifySchema(node.schema, qualifier);
    }
    return node;
  }
  StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
  if (!table.HasValue()) {
    node.rows = 0;
    node.rows_known = false;
    node.text = "CteOrMissingScan " + source.table + " rows~unknown";
    return node;
  }
  node.schema = qualifier.empty()
                    ? table.Value()->GetSchema()
                    : QualifySchema(table.Value()->GetSchema(), qualifier);
  node.rows = StatsRows(context, source.table, &node.rows_known);
  std::ostringstream line;
  line << "SeqScan " << source.table;
  if (!source.alias.empty() && source.alias != source.table) {
    line << " AS " << source.alias;
  }
  line << " rows~" << FormatRows(node);
  node.text = line.str();
  return node;
}

EstimatedPlanNode MakeJoinNode(EstimatedPlanNode left, EstimatedPlanNode right,
                               const std::vector<Expression>& predicates,
                               std::string_view join_kind) {
  const std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  EstimatedPlanNode out;
  out.schema = left.schema + right.schema;
  out.rows_known = left.rows_known && right.rows_known;
  std::ostringstream head;
  if (keys.empty()) {
    if (!out.rows_known) {
      out.rows = 0;
    } else if (left.rows == 0 || right.rows == 0) {
      out.rows = 0;
    } else if (left.rows > std::numeric_limits<size_t>::max() / right.rows) {
      out.rows = std::numeric_limits<size_t>::max();
    } else {
      out.rows = left.rows * right.rows;
    }
    head << "NestedLoopJoin";
  } else {
    out.rows = out.rows_known ? std::min(left.rows, right.rows) : 0;
    if (PreferHybridForBuild(right)) {
      head << "HybridHashJoin build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~"
             << FormatBytes(right.rows * kHashJoinRowBytesEstimate) << ")";
      }
    } else {
      head << "HashJoin build~" << FormatRows(right);
      if (right.rows_known) {
        head << " (~"
             << FormatBytes(right.rows * kHashJoinRowBytesEstimate) << ")";
      }
    }
  }
  if (!join_kind.empty()) {
    head << " type=" << join_kind;
  }
  head << " rows~" << FormatRows(out);
  out.text = head.str() + "\n" + IndentLines(left.text, 2) + "\n" +
             IndentLines(right.text, 2);
  return out;
}

void WriteEstimatedPhysicalPlan(TransactionContext& context,
                                const SelectStatement& statement,
                                std::ostream& output, int indent) {
  const std::string pad(static_cast<size_t>(indent), ' ');
  CteMap empty_ctes;
  if (statement.Sources().empty()) {
    output << pad << "Result rows~1\n";
  } else {
    std::vector<EstimatedPlanNode> nodes;
    nodes.reserve(statement.Sources().size());
    for (const SelectSource& source : statement.Sources()) {
      nodes.push_back(MakeScanNode(context, source, empty_ctes));
    }

    const bool has_left_join = std::any_of(
        statement.Sources().begin() + 1, statement.Sources().end(),
        [](const SelectSource& source) {
          return source.join_type == JoinType::kLeft;
        });

    EstimatedPlanNode plan;
    if (has_left_join) {
      output << pad << "JoinOrder=syntactic (left joins present)\n";
      plan = std::move(nodes.front());
      for (size_t i = 1; i < nodes.size(); ++i) {
        const SelectSource& source = statement.Sources()[i];
        std::vector<Expression> predicates =
            SplitConjuncts(source.join_condition);
        const char* kind = "cross";
        if (source.join_type == JoinType::kInner) kind = "inner";
        if (source.join_type == JoinType::kLeft) kind = "left";
        plan = MakeJoinNode(std::move(plan), std::move(nodes[i]), predicates,
                            kind);
      }
    } else {
      output << pad
             << "JoinOrder=greedy_filtered_cardinality "
                "(equality=hash|hybrid, fallback=nested_loop)\n";
      std::vector<Expression> all_predicates =
          SplitConjuncts(statement.WhereClause());
      for (size_t i = 1; i < statement.Sources().size(); ++i) {
        if (statement.Sources()[i].join_condition) {
          all_predicates.push_back(statement.Sources()[i].join_condition);
        }
      }
      std::vector<Relation> schema_only(nodes.size());
      for (size_t i = 0; i < nodes.size(); ++i) {
        schema_only[i].schema = nodes[i].schema;
      }
      const std::vector<PredicateInfo> predicates = AnalyzePredicates(
          all_predicates.empty() ? Expression()
                                 : CombineConjuncts(all_predicates),
          schema_only);

      size_t first = 0;
      for (size_t i = 1; i < nodes.size(); ++i) {
        if (nodes[i].rows < nodes[first].rows) first = i;
      }
      plan = std::move(nodes[first]);
      std::unordered_set<size_t> joined{first};
      std::unordered_set<size_t> remaining;
      for (size_t i = 0; i < nodes.size(); ++i) {
        if (i != first) remaining.insert(i);
      }
      while (!remaining.empty()) {
        size_t next = *remaining.begin();
        size_t next_estimate = std::numeric_limits<size_t>::max();
        bool next_connected = false;
        for (size_t candidate : remaining) {
          std::unordered_set<size_t> after = joined;
          after.insert(candidate);
          std::vector<Expression> applicable;
          for (const PredicateInfo& predicate : predicates) {
            if (!predicate.resolved || predicate.contains_query ||
                predicate.sources.size() < 2 ||
                !predicate.sources.contains(candidate) ||
                !IsSubset(predicate.sources, after)) {
              continue;
            }
            applicable.push_back(predicate.expression);
          }
          size_t estimate = 0;
          if (EqualityKeys(plan.schema, nodes[candidate].schema, applicable)
                  .empty()) {
            estimate =
                plan.rows == 0 || nodes[candidate].rows == 0
                    ? 0
                    : plan.rows > std::numeric_limits<size_t>::max() /
                                      std::max<size_t>(nodes[candidate].rows, 1)
                          ? std::numeric_limits<size_t>::max()
                          : plan.rows * nodes[candidate].rows;
          } else {
            estimate = std::min(plan.rows, nodes[candidate].rows);
          }
          const bool connected = !applicable.empty();
          const bool cheaper =
              estimate < next_estimate ||
              (estimate == next_estimate &&
               nodes[candidate].rows < nodes[next].rows);
          if ((connected && !next_connected) ||
              (connected == next_connected && cheaper)) {
            next = candidate;
            next_estimate = estimate;
            next_connected = connected;
          }
        }
        std::unordered_set<size_t> after = joined;
        after.insert(next);
        std::vector<Expression> applicable;
        for (const PredicateInfo& predicate : predicates) {
          if (!predicate.resolved || predicate.contains_query ||
              predicate.sources.size() < 2 ||
              !predicate.sources.contains(next) ||
              !IsSubset(predicate.sources, after)) {
            continue;
          }
          applicable.push_back(predicate.expression);
        }
        plan = MakeJoinNode(std::move(plan), std::move(nodes[next]), applicable,
                            "inner");
        joined.insert(next);
        remaining.erase(next);
      }
    }
    output << IndentLines(plan.text, indent) << '\n';
  }

  if (statement.WhereClause()) {
    output << pad << "Filter " << *statement.WhereClause() << '\n';
  }
  if (!statement.GroupBy().empty() || statement.Having()) {
    output << pad << "Aggregate group_keys=" << statement.GroupBy().size()
           << " having=" << (statement.Having() ? "true" : "false") << '\n';
  }
  output << pad << "Project columns=" << statement.SelectList().size()
         << " distinct=" << (statement.Distinct() ? "true" : "false") << '\n';
  if (!statement.OrderBy().empty()) {
    output << pad << "Sort keys=" << statement.OrderBy().size() << '\n';
  }
  if (statement.Limit() != 0 || statement.Offset() != 0) {
    output << pad << "Limit count=" << statement.Limit()
           << " offset=" << statement.Offset() << '\n';
  }
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  output << pad << "QueryMemory limit=";
  if (budget.Unlimited()) {
    output << "unlimited";
  } else {
    output << FormatBytes(budget.Limit()) << " soft="
           << FormatBytes(budget.Limit() / 5 * 4);
  }
  output << '\n';
}

}  // namespace

RelationalExecutor::RelationalExecutor(
    TransactionContext& context,
    std::shared_ptr<const SelectStatement> statement)
    : context_(&context), statement_(std::move(statement)), rows_() {}

void RelationalExecutor::Initialize() {
  if (initialized_) return;
  ExecutionRuntime runtime;
  runtime.root_statement = statement_.get();
  std::unordered_map<std::string, size_t> table_counts;
  CountStatementTables(*statement_, &table_counts);
  for (const auto& [table, count] : table_counts) {
    if (count > 1) runtime.reusable_base_relations.insert(table);
  }
  ExecutionRuntime* previous_runtime = active_runtime;
  active_runtime = &runtime;
  Relation result;
  try {
    result = ExecuteQuery(*context_, *statement_, nullptr, {});
  } catch (...) {
    active_runtime = previous_runtime;
    throw;
  }
  active_runtime = previous_runtime;
  result.FinishSpill();
  rows_.clear();
  result.ForEachRow([&](const Row& row) { rows_.push_back(row); });
  hash_joins_ = result.hash_joins;
  hybrid_hash_joins_ = result.hybrid_hash_joins;
  in_memory_hash_joins_ = result.in_memory_hash_joins;
  nested_loop_joins_ = result.nested_loop_joins;
  join_comparisons_ = result.join_comparisons;
  peak_intermediate_rows_ = result.peak_intermediate_rows;
  correlated_index_builds_ = runtime.correlated_index_builds;
  correlated_index_probes_ = runtime.correlated_index_probes;
  correlated_result_cache_hits_ = runtime.correlated_result_cache_hits;
  uncorrelated_cache_hits_ = runtime.uncorrelated_cache_hits;
  uncorrelated_hash_builds_ = runtime.uncorrelated_hash_builds;
  uncorrelated_hash_probes_ = runtime.uncorrelated_hash_probes;
  scan_ms_ = runtime.scan_ms;
  filter_ms_ = runtime.filter_ms;
  join_ms_ = runtime.join_ms;
  project_ms_ = runtime.project_ms;
  sort_ms_ = runtime.sort_ms;
  base_scan_cache_hits_ = runtime.base_scan_cache_hits;
  aggregate_input_rows_ = runtime.aggregate_input_rows;
  aggregate_groups_ = runtime.aggregate_groups;
  aggregate_updates_ = runtime.aggregate_updates;
  scan_rows_ = runtime.scan_rows;
  scan_output_rows_ = runtime.scan_output_rows;
  scan_values_decoded_ = runtime.scan_values_decoded;
  scan_values_available_ = runtime.scan_values_available;
  relation_spills_ = runtime.relation_spills;
  initialized_ = true;
}

bool RelationalExecutor::Next(Row* destination, RowPosition* position) {
  Initialize();
  if (offset_ >= rows_.size()) return false;
  *destination = rows_[offset_++];
  if (position) *position = RowPosition();
  return true;
}

void RelationalExecutor::Dump(std::ostream& output, int) const {
  const_cast<RelationalExecutor*>(this)->Initialize();
  output << "RelationalExecutor(materialized=" << rows_.size()
         << ", hash_joins=" << hash_joins_
         << ", hybrid_hash_joins=" << hybrid_hash_joins_
         << ", in_memory_hash_joins=" << in_memory_hash_joins_
         << ", nested_loop_joins=" << nested_loop_joins_
         << ", join_comparisons=" << join_comparisons_
         << ", peak_intermediate_rows=" << peak_intermediate_rows_
         << ", relation_spills=" << relation_spills_
         << ", correlated_index_builds=" << correlated_index_builds_
         << ", correlated_index_probes=" << correlated_index_probes_
         << ", correlated_result_cache_hits=" << correlated_result_cache_hits_
         << ", uncorrelated_cache_hits=" << uncorrelated_cache_hits_
         << ", uncorrelated_hash_builds=" << uncorrelated_hash_builds_
         << ", uncorrelated_hash_probes=" << uncorrelated_hash_probes_
         << ", scan_ms=" << scan_ms_ << ", filter_ms=" << filter_ms_
         << ", join_ms=" << join_ms_ << ", project_ms=" << project_ms_
         << ", sort_ms=" << sort_ms_
         << ", base_scan_cache_hits=" << base_scan_cache_hits_
         << ", aggregate_input_rows=" << aggregate_input_rows_
         << ", aggregate_groups=" << aggregate_groups_
         << ", aggregate_updates=" << aggregate_updates_
         << ", scan_rows=" << scan_rows_
         << ", scan_output_rows=" << scan_output_rows_
         << ", scan_values_decoded=" << scan_values_decoded_
         << ", scan_values_available=" << scan_values_available_ << ")";
}

void RelationalExecutor::Explain(std::ostream& output, int) const {
  output << "Relational Physical Plan (estimated)\n";
  WriteEstimatedPhysicalPlan(*context_, *statement_, output, 2);
  if (initialized_) {
    output << "Actual Joins: hybrid_hash_joins=" << hybrid_hash_joins_
           << " in_memory_hash_joins=" << in_memory_hash_joins_
           << " nested_loop_joins=" << nested_loop_joins_
           << " relation_spills=" << relation_spills_ << '\n';
  }
}

}  // namespace tinylamb
