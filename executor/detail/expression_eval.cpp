/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/expression_eval.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>
#include <unordered_set>
#include <string>
#include <utility>

#include "common/constants.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "executor/detail/relation.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "type/column_name.hpp"
#include "query/statement.hpp"
#include "type/date.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"

namespace tinylamb::relational_detail {

bool Truthy(const Value& value) {
  if (value.IsNull()) { return false;
}
  if (value.type == ValueType::kInt64 || value.type == ValueType::kDate) {
    return value.value.int_value != 0;
  }
  if (value.type == ValueType::kDouble) { return value.value.double_value != 0.0;
}
  return !value.value.varchar_value.empty();
}

namespace {

double Number(const Value& value) {
  if (value.type == ValueType::kDouble) { return value.value.double_value;
}
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
        if (match >= 0) { return -1;
}
        match = static_cast<int>(i);
      }
    }
  }
  return match;
}

}  // namespace

Value Lookup(const ColumnName& name, const Scope& scope) {
  for (const Scope* current = &scope; current != nullptr;
       current = current->outer) {
    if (current->row == nullptr || current->schema == nullptr) { continue;
}
    const int offset = FindColumn(*current->schema, name);
    if (offset >= 0) { return (*current->row)[static_cast<size_t>(offset)];
}
  }
  throw std::runtime_error("column " + name.ToString() + " not found");
}

bool Like(std::string_view value, std::string_view pattern) {
  // Fast paths for the common TPC-H shapes: 'foo%', '%foo', '%foo%'.
  if (pattern == "%") { return true;
}
  if (pattern.empty()) { return value.empty();
}
  const bool leading = pattern.front() == '%';
  const bool trailing = pattern.back() == '%';
  if (leading || trailing) {
    std::string_view core = pattern;
    if (leading) { core.remove_prefix(1);
}
    if (trailing && !core.empty()) { core.remove_suffix(1);
}
    if (core.find('%') == std::string_view::npos &&
        core.find('_') == std::string_view::npos) {
      if (leading && trailing) {
        return value.find(core) != std::string_view::npos;
      }
      if (trailing) {
        return value.starts_with(core);
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
  if (left.IsNull() || right.IsNull()) { return {};
}
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
    if (integral) {
      // Keep int64 arithmetic and comparisons exact; only overflow falls back
      // to double (signed overflow would be UB).
      int64_t wide = 0;
      switch (operation) {
        case BinaryOperation::kAdd:
          if (!__builtin_add_overflow(left.value.int_value,
                                      right.value.int_value, &wide)) {
            return Value(wide);
          }
          return Value(lhs + rhs);
        case BinaryOperation::kSubtract:
          if (!__builtin_sub_overflow(left.value.int_value,
                                      right.value.int_value, &wide)) {
            return Value(wide);
          }
          return Value(lhs - rhs);
        case BinaryOperation::kMultiply:
          if (!__builtin_mul_overflow(left.value.int_value,
                                      right.value.int_value, &wide)) {
            return Value(wide);
          }
          return Value(lhs * rhs);
        case BinaryOperation::kModulo:
          if (right.value.int_value == 0) {
            throw std::runtime_error("integer modulo by zero");
          }
          if (left.value.int_value != std::numeric_limits<int64_t>::min() ||
              right.value.int_value != -1) {
            return Value(left.value.int_value % right.value.int_value);
          }
          return Value(0);  // MIN % -1 overflows only at the extreme pair.
        case BinaryOperation::kEquals:
          return Value(left.value.int_value == right.value.int_value);
        case BinaryOperation::kNotEquals:
          return Value(left.value.int_value != right.value.int_value);
        case BinaryOperation::kLessThan:
          return Value(left.value.int_value < right.value.int_value);
        case BinaryOperation::kLessThanEquals:
          return Value(left.value.int_value <= right.value.int_value);
        case BinaryOperation::kGreaterThan:
          return Value(left.value.int_value > right.value.int_value);
        case BinaryOperation::kGreaterThanEquals:
          return Value(left.value.int_value >= right.value.int_value);
        default:
          break;
      }
    } else {
      switch (operation) {
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
    switch (operation) {
      case BinaryOperation::kAdd:
        return Value(lhs + rhs);
      case BinaryOperation::kSubtract:
        return Value(lhs - rhs);
      case BinaryOperation::kMultiply:
        return Value(lhs * rhs);
      case BinaryOperation::kDivide:
        return Value(lhs / rhs);
      case BinaryOperation::kModulo:
        return Value(std::fmod(lhs, rhs));
      default:
        break;
    }
  }
  if (left.type != right.type) { throw std::runtime_error("type mismatch");
}
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

bool ContainsAggregate(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression) { return false;
}
  switch (expression->Type()) {
    case TypeTag::kAggregateExp:
      return true;
    case TypeTag::kBinaryExp:
      return ContainsAggregate(expression->AsBinaryExpression().Left()) ||
             ContainsAggregate(expression->AsBinaryExpression().Right());
    case TypeTag::kUnaryExp:
      return ContainsAggregate(expression->AsUnaryExpression().Child());
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      if (ContainsAggregate(value.child_)) { return true;
}
      return std::ranges::any_of(value.list_, [](const Expression& item) {  // NOLINT(misc-no-recursion)
        return ContainsAggregate(item);
      });
    }
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        if (ContainsAggregate(condition) || ContainsAggregate(result)) {
          return true;
}
      }
      return ContainsAggregate(value.else_clause_);
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& argument :
           expression->AsFunctionCallExpression().Args()) {
        if (ContainsAggregate(argument)) { return true;
}
      }
      return false;
    default:
      return false;
  }
}

namespace {

Value Aggregate(const AggregateExpression& aggregate,
                const AggregateResultMap& aggregates) {
  const auto result = aggregates.find(&aggregate);
  if (result == aggregates.end()) {
    throw std::runtime_error("aggregate was not prepared");
  }
  return result->second;
}

}  // namespace

void CollectAggregates(  // NOLINT(misc-no-recursion)
    const Expression& expression,
    std::vector<const AggregateExpression*>* aggregates,
    std::unordered_set<const AggregateExpression*>* seen) {
  if (!expression) { return;
}
  if (expression->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression* aggregate =
        &expression->AsAggregateExpression();
    if (seen->insert(aggregate).second) { aggregates->push_back(aggregate);
}
    return;
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectAggregates(child, aggregates, seen);
  }
}

AggregateAccumulator::AggregateAccumulator(const AggregateExpression* aggregate)
      : expression(aggregate),
        distinct(aggregate->Distinct()
                     ? std::make_unique<std::unordered_set<Value>>()
                     : nullptr) {}

void AggregateAccumulator::Add(const Value& value) {
    if (value.IsNull()) { return;
}
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
          // Keep `distinct`: it still tracks non-integer values seen so far,
          // so mixed-type COUNT(DISTINCT ...) cannot double count.
        }
        if (!distinct_ints->insert(value.value.int_value).second) { return;
}
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
        if (value.type == ValueType::kDouble) {
          total += value.value.double_value;
          total_is_double = true;
        } else if (value.type == ValueType::kInt64 ||
                   value.type == ValueType::kDate) {
          int_total += static_cast<uint64_t>(value.value.int_value);
        } else {
          throw std::runtime_error("numeric value required");
        }
        ++count;
        break;
      case AggregationType::kMin:
        if (extreme.IsNull() || value < extreme) { extreme = value;
}
        break;
      case AggregationType::kMax:
        if (extreme.IsNull() || extreme < value) { extreme = value;
}
        break;
    }
  }

Value AggregateAccumulator::Finish() const {
    switch (expression->GetType()) {
      case AggregationType::kCount:
        return Value(count);
      case AggregationType::kAvg:
        return count == 0
                   ? Value()
                   : Value((total + static_cast<double>(int_total)) /
                           static_cast<double>(count));
      case AggregationType::kSum:
        if (count == 0) { return {};
}
        if (!total_is_double && total == 0.0) {
          return Value(static_cast<int64_t>(int_total));
        }
        return Value(total + static_cast<double>(int_total));
      case AggregationType::kMin:
      case AggregationType::kMax:
        return extreme;
    }
    return {};
  }


namespace {

// Mutual recursion with Evaluate above; expression trees are the intended
// shape here.
Value EvaluateFunction(  // NOLINT(misc-no-recursion)
    const FunctionCallExpression& call, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  const std::string& name = call.FuncName();
  if (name == "date_add" || name == "date_sub") {
    if (call.Args().size() != 2 ||
        call.Args()[1]->Type() != TypeTag::kIntervalExp) {
      throw std::runtime_error("DATE_ADD/DATE_SUB arity");
    }
    const Value date =
        Evaluate(call.Args()[0], scope, aggregates, context, ctes);
    if (date.IsNull()) { return {};
}
    const auto& interval = call.Args()[1]->AsIntervalExpression();
    const int64_t amount = name == "date_sub" ? -interval.Amount()
                                                : interval.Amount();
    int64_t days = 0;
    if (date.type == ValueType::kDate) {
      days = date.DateDays();
    } else if (date.type == ValueType::kVarChar) {
      days = ParseDateDays(date.value.varchar_value);
    } else {
      throw std::runtime_error("DATE_ADD/DATE_SUB requires a date");
    }
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
    if (arguments.size() < 2 || arguments[0].IsNull()) { return {};
}
    if (arguments[1].IsNull() ||
        (arguments.size() >= 3 && arguments[2].IsNull())) {
      return {};
    }
    if (arguments[0].type != ValueType::kVarChar ||
        arguments[1].type != ValueType::kInt64 ||
        (arguments.size() >= 3 && arguments[2].type != ValueType::kInt64)) {
      throw std::runtime_error("substr requires (string, int[, int])");
    }
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
    if (arguments.size() != 1 || arguments[0].IsNull()) { return {};
}
    if (arguments[0].type != ValueType::kDate &&
        arguments[0].type != ValueType::kVarChar) {
      throw std::runtime_error(name + " requires a date");
    }
    const std::string date = arguments[0].type == ValueType::kDate
                                 ? arguments[0].AsString()
                                 : std::string(arguments[0].value.varchar_value);
    if (name == "extract_year") { return Value(std::stoll(date.substr(0, 4)));
}
    if (name == "extract_month") { return Value(std::stoll(date.substr(5, 2)));
}
    if (name == "extract_day") { return Value(std::stoll(date.substr(8, 2)));
}
  }
  if (name == "coalesce") {
    for (Value& value : arguments) {
      if (!value.IsNull()) { return value;
}
    }
    return {};
  }
  if (name == "concat") {
    std::string result;
    for (const Value& value : arguments) {
      if (!value.IsNull()) {
        if (value.type != ValueType::kVarChar) {
          throw std::runtime_error("concat requires string arguments");
        }
        result += std::string(value.value.varchar_value);
      }
    }
    return Value(std::move(result));
  }
  if (name == "current_timestamp") {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    std::array<char, 32> buffer{};
    if (std::strftime(buffer.data(), buffer.size(), "%Y-%m-%d %H:%M:%S",
                      &tm) == 0) {
      throw std::runtime_error("timestamp formatting failed");
    }
    return Value(std::string(buffer.data()));
  }
  throw std::runtime_error("unsupported function " + name);
}

}  // namespace

// Recursive descent over the expression tree is the intended evaluation
// strategy.
Value Evaluate(  // NOLINT(misc-no-recursion)
    const Expression& expression, const Scope& scope,
    const AggregateResultMap* aggregates, TransactionContext& context,
    const CteMap& ctes) {
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (name.name == "*") { return Value(1);
}
      return Lookup(name, scope);
    }
    case TypeTag::kConstantValue:
      return expression->AsConstantValue().GetValue();
    case TypeTag::kBinaryExp: {
      const auto& value = expression->AsBinaryExpression();
      if (value.Op() == BinaryOperation::kAnd) {
        const Value left =
            Evaluate(value.Left(), scope, aggregates, context, ctes);
        if (!Truthy(left)) { return Value(false);
}
        return Binary(value.Op(), left,
                      Evaluate(value.Right(), scope, aggregates, context,
                               ctes));
      }
      if (value.Op() == BinaryOperation::kOr) {
        const Value left =
            Evaluate(value.Left(), scope, aggregates, context, ctes);
        if (Truthy(left)) { return Value(true);
}
        return Binary(value.Op(), left,
                      Evaluate(value.Right(), scope, aggregates, context,
                               ctes));
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
      if (value.Op() == UnaryOperation::kIsNull) { return Value(child.IsNull());
}
      if (value.Op() == UnaryOperation::kIsNotNull) {
        return Value(!child.IsNull());
}
      if (value.Op() == UnaryOperation::kNot) { return Value(!Truthy(child));
}
      // Unary minus: NULL propagates and only numeric values are negated.
      if (child.IsNull()) { return {};
}
      if (child.type == ValueType::kDouble) {
        return Value(-child.value.double_value);
      }
      if (child.type == ValueType::kInt64) {
        return Value(-child.value.int_value);
      }
      throw std::runtime_error("numeric value required");
    }
    case TypeTag::kAggregateExp:
      if (aggregates == nullptr) {
        throw std::runtime_error("aggregate outside grouping");
}
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
      if (relation == nullptr) {
        relation = ExecuteCachedUncorrelated(context, *value.Query(), ctes);
        uncorrelated = relation != nullptr;
      }
      if (relation == nullptr) {
        executed = ExecuteQuery(context, *value.Query(), &scope, ctes);
        relation = &*executed;
      }
      auto& row_source = const_cast<Relation&>(*relation);
      if (value.Exists()) {
        const bool exists = relation->TotalRows() > 0;
        return Value(value.Negated() ? !exists : exists);
      }
      if (value.Test()) {
        const Value test =
            Evaluate(value.Test(), scope, aggregates, context, ctes);
        bool found = false;
        if (uncorrelated && active_runtime != nullptr) {
          auto [cached, inserted] =
              active_runtime->uncorrelated_membership.try_emplace(
                  value.Query().get());
          if (inserted) {
            cached->second.reserve(relation->TotalRows());
            row_source.ForEachRow([&](const Row& row) {
              if (!row.values_.empty() && !row[0].IsNull()) {
                cached->second.insert(row[0]);
              }
            });
            ++active_runtime->uncorrelated_hash_builds;
          }
          ++active_runtime->uncorrelated_hash_probes;
          found = !test.IsNull() && cached->second.contains(test);
        } else {
          row_source.ForEachRow([&](const Row& row) {
            if (found || row.values_.empty()) { return;
}
            if (Truthy(Binary(BinaryOperation::kEquals, test, row[0]))) {
              found = true;
            }
          });
        }
        return Value(value.Negated() ? !found : found);
      }
      std::optional<Row> first;
      row_source.ForEachRow([&](const Row& row) {
        // Scalar subquery: only the first row matters.
        if (!first) { first = row;
}
      });
      if (!first || first->values_.empty()) { return {};
}
      return (*first)[0];
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
  return {"", std::move(columns)};
}

namespace {
// Mutual recursion with CollectStatementColumns below; the statement/expression
// nesting is bounded by query depth.
void CollectExpressionColumns(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::unordered_set<ColumnName>* columns) {
  if (!expression) { return;
}
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
}  // namespace

void CollectStatementColumns(  // NOLINT(misc-no-recursion)
    const SelectStatement& statement,
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
    if (source.query) { CollectStatementColumns(*source.query, columns);
}
  }
  for (const auto& [name, query] : statement.WithQueries()) {
    (void)name;
    CollectStatementColumns(*query, columns);
  }
}

std::vector<slot_t> RequiredColumns(const SelectStatement& statement,
                                    const Schema& schema,
                                    bool ignore_star) {
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
          if (name.name == "*") { return false;
}
          return name.name == candidate.name &&
                 (name.schema.empty() || name.schema == candidate.schema);
        });
    if (needed) { result.push_back(i);
}
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
  return {schema.Name(), std::move(columns)};
}

std::string BaseRelationCacheKey(
    std::string_view table, const std::vector<slot_t>* projection) {
  std::string key(table);
  if (projection == nullptr) { return key;
}
  key.push_back('#');
  for (slot_t column : *projection) {
    key += std::to_string(column);
    key.push_back(',');
  }
  return key;
}

bool ReusesBaseRelation(const SelectSource& source) {
  return active_runtime != nullptr &&
         active_runtime->reusable_base_relations.contains(source.table);
}

ValueType ValueTypeOf(const Value& value) { return value.type; }

std::string ProjectionName(const NamedExpression& projection, size_t index) {
  if (!projection.name.empty()) { return projection.name;
}
  if (projection.expression->Type() == TypeTag::kColumnValue) {
    return projection.expression->AsColumnValue().GetColumnName().name;
  }
  return "$expr" + std::to_string(index);
}

}  // namespace tinylamb::relational_detail
