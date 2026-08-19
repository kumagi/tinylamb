/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/relational.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
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
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/date.hpp"

namespace tinylamb {
namespace {

struct Relation {
  Schema schema;
  std::vector<Row> rows;
  size_t hash_joins{0};
  size_t nested_loop_joins{0};
  size_t join_comparisons{0};
  size_t peak_intermediate_rows{0};
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
};

struct ExecutionRuntime {
  std::unordered_map<std::string, Relation> base_relations;
  std::unordered_set<std::string> reusable_base_relations;
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
};

thread_local ExecutionRuntime* active_runtime = nullptr;

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
    if (distinct && !distinct->insert(value).second) return;
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
                                    const Schema& schema) {
  const bool selects_star =
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

Relation LoadSource(TransactionContext& context, const SelectSource& source,
                    const Scope* outer, const CteMap& ctes,
                    const std::vector<slot_t>* projection = nullptr,
                    const std::vector<Expression>* scan_predicates = nullptr) {
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
      result = cached->second;
      ++active_runtime->base_scan_cache_hits;
    } else {
      StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
      if (!table.HasValue()) {
        throw std::runtime_error("table " + source.table + " not found");
      }
      const Schema& table_schema = table.Value()->GetSchema();
      result.schema = projection ? ProjectSchema(table_schema, *projection)
                                 : table_schema;
      const auto scan_begin = std::chrono::steady_clock::now();
      const auto filter_begin = scan_begin;
      Iterator iterator =
          projection ? table.Value()->BeginFullScan(context.txn_, *projection)
                     : table.Value()->BeginFullScan(context.txn_);
      while (iterator.IsValid()) {
        if (active_runtime) {
          ++active_runtime->scan_rows;
          active_runtime->scan_values_available += table_schema.ColumnCount();
          active_runtime->scan_values_decoded += result.schema.ColumnCount();
        }
        bool matches = true;
        if (filter_during_scan) {
          Scope scope{&*iterator, &result.schema, outer};
          for (const Expression& predicate : *scan_predicates) {
            if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
              matches = false;
              break;
            }
          }
        }
        if (matches) {
          result.rows.push_back(*iterator);
          if (active_runtime) ++active_runtime->scan_output_rows;
        }
        ++iterator;
      }
      if (active_runtime) {
        active_runtime->scan_ms += ElapsedMs(scan_begin);
        if (filter_during_scan) {
          active_runtime->filter_ms += ElapsedMs(filter_begin);
        }
        if (reusable) {
          active_runtime->base_relations.emplace(cache_key, result);
        }
      }
    }
  }
  const std::string qualifier =
      source.alias.empty() ? source.table : source.alias;
  if (!qualifier.empty())
    result.schema = QualifySchema(result.schema, qualifier);
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.rows.size());
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
  std::vector<Row> filtered;
  filtered.reserve(relation->rows.size());
  for (Row& row : relation->rows) {
    Scope scope{&row, &relation->schema, outer};
    bool matches = true;
    for (const Expression& predicate : predicates) {
      if (!Truthy(Evaluate(predicate, scope, nullptr, context, ctes))) {
        matches = false;
        break;
      }
    }
    if (matches) filtered.push_back(std::move(row));
  }
  relation->rows = std::move(filtered);
  if (active_runtime) active_runtime->filter_ms += ElapsedMs(filter_begin);
}

struct EqualityKey {
  size_t left;
  size_t right;
};

std::vector<EqualityKey> EqualityKeys(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<EqualityKey> keys;
  for (const Expression& predicate : predicates) {
    if (predicate->Type() != TypeTag::kBinaryExp) continue;
    const BinaryExpression& binary = predicate->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() != TypeTag::kColumnValue ||
        binary.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
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

Relation Join(TransactionContext& context, Relation left, Relation right,
              const SelectSource& source, const Scope* outer,
              const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result;
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<Expression> predicates =
      SplitConjuncts(source.join_condition);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    if (!source.join_condition) return true;
    Scope scope{&combined, &result.schema, outer};
    return Truthy(
        Evaluate(source.join_condition, scope, nullptr, context, ctes));
  };
  auto emit_unmatched = [&](const Row& left_row) {
    if (source.join_type != JoinType::kLeft) return;
    std::vector<Value> nulls(right.schema.ColumnCount());
    result.rows.push_back(left_row + Row(std::move(nulls)));
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    for (const Row& left_row : left.rows) {
      bool matched = false;
      for (const Row& right_row : right.rows) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) {
          result.rows.push_back(std::move(combined));
          matched = true;
        }
      }
      if (!matched) emit_unmatched(left_row);
    }
  } else {
    ++result.hash_joins;
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
    }
    std::unordered_multimap<std::string, const Row*> buckets;
    buckets.reserve(right.rows.size());
    for (const Row& row : right.rows) {
      if (!HasNullKey(row, right_columns)) {
        buckets.emplace(row.Extract(right_columns).EncodeMemcomparableFormat(),
                        &row);
      }
    }
    for (const Row& left_row : left.rows) {
      bool matched = false;
      if (!HasNullKey(left_row, left_columns)) {
        const std::string key =
            left_row.Extract(left_columns).EncodeMemcomparableFormat();
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result.join_comparisons;
          Row combined = left_row + *iter->second;
          if (matches(combined)) {
            result.rows.push_back(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched) emit_unmatched(left_row);
    }
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.rows.size());
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
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);

  auto matches = [&](const Row& combined) {
    Scope scope{&combined, &result.schema, outer};
    return std::all_of(
        predicates.begin(), predicates.end(), [&](const Expression& predicate) {
          return Truthy(Evaluate(predicate, scope, nullptr, context, ctes));
        });
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    for (const Row& left_row : left.rows) {
      for (const Row& right_row : right.rows) {
        ++result.join_comparisons;
        Row combined = left_row + right_row;
        if (matches(combined)) result.rows.push_back(std::move(combined));
      }
    }
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, result.rows.size());
    if (active_runtime) active_runtime->join_ms += ElapsedMs(join_begin);
    return result;
  }

  ++result.hash_joins;

  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  left_columns.reserve(equality_keys.size());
  right_columns.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  std::unordered_multimap<std::string, const Row*> buckets;
  buckets.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    if (HasNullKey(row, right_columns)) continue;
    buckets.emplace(row.Extract(right_columns).EncodeMemcomparableFormat(),
                    &row);
  }
  for (const Row& left_row : left.rows) {
    if (HasNullKey(left_row, left_columns)) continue;
    const std::string key =
        left_row.Extract(left_columns).EncodeMemcomparableFormat();
    const auto [begin, end] = buckets.equal_range(key);
    for (auto iter = begin; iter != end; ++iter) {
      ++result.join_comparisons;
      Row combined = left_row + *iter->second;
      if (matches(combined)) result.rows.push_back(std::move(combined));
    }
  }
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.rows.size());
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
    // A table referenced more than once is cached once and shared by aliases
    // and nested queries. Use one full-width cache entry so differing local
    // projections cannot silently cause repeated physical scans.
    if (ReusesBaseRelation(source)) {
      projections[i].clear();
      projections[i].reserve(relations[i].schema.ColumnCount());
      for (slot_t column = 0; column < relations[i].schema.ColumnCount();
           ++column) {
        projections[i].push_back(column);
      }
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
        const std::vector<slot_t>* projection =
            ReusesBaseRelation(statement.Sources()[i]) ? nullptr
                                                       : &projections[i];
        relations[i] = LoadSource(context, statement.Sources()[i], outer,
                                  ctes, projection);
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
    if (base_sources[i]) {
      const bool reusable = ReusesBaseRelation(statement.Sources()[i]);
      const std::vector<slot_t>* projection =
          reusable ? nullptr : &projections[i];
      relations[i] = LoadSource(context, statement.Sources()[i], outer, ctes,
                                projection, &local);
      if (reusable) {
        FilterRelation(context, &relations[i], local, outer, ctes);
      }
    } else {
      FilterRelation(context, &relations[i], local, outer, ctes);
    }
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
    std::unordered_map<Row, size_t> offsets;
    for (Row& row : input.rows) {
      Scope scope{&row, &input.schema, outer};
      std::vector<Value> key_values;
      for (const Expression& key : statement.GroupBy()) {
        key_values.push_back(Evaluate(key, scope, nullptr, context, ctes));
      }
      Row key(std::move(key_values));
      auto [iter, inserted] = offsets.emplace(key, groups.size());
      if (inserted) groups.push_back(make_group());
      GroupState& group = groups[iter->second];
      for (size_t i = 0; i < aggregate_expressions.size(); ++i) {
        AggregateAccumulator& accumulator =
            aggregate_states[group.accumulator_offset + i];
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
    }
    if (input.rows.empty() && statement.GroupBy().empty()) {
      groups.push_back(make_group());
    }
    if (active_runtime) active_runtime->aggregate_groups += groups.size();
  }

  Relation output;
  output.hash_joins = input.hash_joins;
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
    output.rows.emplace_back(std::move(values));
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
    for (const Row& row : input.rows) emit(row, nullptr);
  }
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
                     bool apply_where = true) {
  if (apply_where && statement.WhereClause()) {
    const auto filter_begin = std::chrono::steady_clock::now();
    std::vector<Row> filtered;
    for (Row& row : input.rows) {
      Scope scope{&row, &input.schema, outer};
      if (Truthy(Evaluate(statement.WhereClause(), scope, nullptr, context,
                          ctes))) {
        filtered.push_back(std::move(row));
      }
    }
    input.rows = std::move(filtered);
    if (active_runtime) active_runtime->filter_ms += ElapsedMs(filter_begin);
  }

  Relation output = Project(context, statement, std::move(input), outer, ctes);
  if (statement.Distinct()) {
    std::unordered_set<Row> seen;
    std::vector<Row> distinct;
    for (Row& row : output.rows) {
      if (seen.insert(row).second) distinct.push_back(std::move(row));
    }
    output.rows = std::move(distinct);
  }
  if (!statement.OrderBy().empty()) {
    const auto sort_begin = std::chrono::steady_clock::now();
    std::stable_sort(
        output.rows.begin(), output.rows.end(),
        [&](const Row& left, const Row& right) {
          Scope left_scope{&left, &output.schema, outer};
          Scope right_scope{&right, &output.schema, outer};
          for (const auto& term : statement.OrderBy()) {
            const Value lhs =
                Evaluate(term.expression, left_scope, nullptr, context, ctes);
            const Value rhs =
                Evaluate(term.expression, right_scope, nullptr, context, ctes);
            if (lhs == rhs) continue;
            if (lhs.IsNull()) return term.ascending;
            if (rhs.IsNull()) return !term.ascending;
            return term.ascending ? lhs < rhs : rhs < lhs;
          }
          return false;
        });
    if (active_runtime) active_runtime->sort_ms += ElapsedMs(sort_begin);
  }
  const size_t begin = std::min(statement.Offset(), output.rows.size());
  const size_t available = output.rows.size() - begin;
  const size_t count = statement.Limit() == 0
                           ? available
                           : std::min(statement.Limit(), available);
  if (begin != 0 || count != output.rows.size()) {
    std::vector<Row> limited;
    limited.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      limited.push_back(std::move(output.rows[begin + i]));
    }
    output.rows = std::move(limited);
  }
  return output;
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
    Relation source;
    if (statement.Sources().size() == 1) {
      source = LoadSource(context, statement.Sources()[0], &outer, ctes);
    } else {
      bool predicates_applied = false;
      source =
          BuildInput(context, statement, &outer, ctes, &predicates_applied);
    }
    auto created = std::make_unique<CorrelatedIndex>();
    created->schema = source.schema;
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
      } else if (right_local && !left_local) {
        created->local_columns.push_back(static_cast<slot_t>(*right_local));
        created->outer_columns.push_back(left);
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
    created->rows.reserve(source.rows.size());
    for (Row& row : source.rows) {
      if (HasNullKey(row, created->local_columns)) continue;
      created->rows.emplace(
          row.Extract(created->local_columns).EncodeMemcomparableFormat(),
          std::move(row));
    }
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
  CteMap ctes = inherited_ctes;
  for (const auto& [name, query] : statement.WithQueries()) {
    ctes[name] = ExecuteQuery(context, *query, outer, ctes);
  }

  bool where_fully_applied = false;
  Relation input =
      BuildInput(context, statement, outer, ctes, &where_fully_applied);

  return FinishQuery(context, statement, std::move(input), outer, ctes,
                     !where_fully_applied);
}

}  // namespace

RelationalExecutor::RelationalExecutor(
    TransactionContext& context,
    std::shared_ptr<const SelectStatement> statement)
    : context_(&context), statement_(std::move(statement)), rows_() {}

void RelationalExecutor::Initialize() {
  if (initialized_) return;
  ExecutionRuntime runtime;
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
  rows_ = std::move(result.rows);
  hash_joins_ = result.hash_joins;
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
         << ", nested_loop_joins=" << nested_loop_joins_
         << ", join_comparisons=" << join_comparisons_
         << ", peak_intermediate_rows=" << peak_intermediate_rows_
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
  const std::string indent(2, ' ');
  output << "RelationalExecutor\n"
         << indent
         << "Join(strategy=greedy_filtered_cardinality, equality=hash, "
            "fallback=nested_loop)";
  for (size_t i = 0; i < statement_->Sources().size(); ++i) {
    const SelectSource& source = statement_->Sources()[i];
    output << "\n" << indent << "  Source[" << i << "]=";
    if (source.query) {
      output << "derived_query";
    } else {
      output << source.table;
    }
    if (!source.alias.empty() && source.alias != source.table) {
      output << " AS " << source.alias;
    }
    if (i != 0) {
      const char* join_type = "cross";
      if (source.join_type == JoinType::kInner) join_type = "inner";
      if (source.join_type == JoinType::kLeft) join_type = "left";
      output << " join=" << join_type;
      if (source.join_condition) {
        output << " on=" << *source.join_condition;
      }
    }
  }
  if (statement_->WhereClause()) {
    output << "\n" << indent << "Filter=" << *statement_->WhereClause();
  }
  if (!statement_->GroupBy().empty() || statement_->Having()) {
    output << "\n"
           << indent << "Aggregate(group_keys=" << statement_->GroupBy().size()
           << ", having=" << (statement_->Having() ? "true" : "false") << ")";
  }
  output << "\n"
         << indent << "Project(columns=" << statement_->SelectList().size()
         << ", distinct=" << (statement_->Distinct() ? "true" : "false") << ")";
  if (!statement_->OrderBy().empty()) {
    output << "\n"
           << indent << "Sort(keys=" << statement_->OrderBy().size() << ")";
  }
  if (statement_->Limit() != 0 || statement_->Offset() != 0) {
    output << "\n"
           << indent << "Limit(count=" << statement_->Limit()
           << ", offset=" << statement_->Offset() << ")";
  }
}

}  // namespace tinylamb
