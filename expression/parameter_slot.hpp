/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#ifndef TINYLAMB_PARAMETER_SLOT_HPP
#define TINYLAMB_PARAMETER_SLOT_HPP

// A bindable literal standing in for one SQL parameter. Type() is
// kConstantValue and the class derives from ConstantValue, so planner code
// that calls AsConstantValue().GetValue() sees the current binding. INSERT
// artifacts keep a per-execution PreparedValues pointer (immutable cached
// tree). SELECT/UPDATE/DELETE artifacts call Bind() so one thread-local plan
// serves every literal combination.

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/unary_expression.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class PreparedValues {
 public:
  explicit PreparedValues(std::vector<Value> values)
      : values_(std::move(values)) {}
  [[nodiscard]] const Value& at(size_t index) const { return values_[index]; }
  [[nodiscard]] size_t size() const { return values_.size(); }

 private:
  std::vector<Value> values_;
};

class ParameterSlot final : public ConstantValue {
 public:
  // INSERT fast path: Evaluate() reads this execution's PreparedValues.
  ParameterSlot(size_t slot_index, ValueType expected,
                std::shared_ptr<const PreparedValues> values)
      : ConstantValue(Value()),
        slot_index_(slot_index),
        expected_(expected),
        values_(std::move(values)),
        use_prepared_values_(true) {}

  // Optimizer-resident slot. GetValue()/Evaluate() return `initial` until
  // Bind() replaces it. The node stays shared with the plan tree.
  ParameterSlot(size_t slot_index, const Value& initial)
      : ConstantValue(initial),
        slot_index_(slot_index),
        expected_(initial.type),
        use_prepared_values_(false) {}

  [[nodiscard]] size_t Index() const { return slot_index_; }
  [[nodiscard]] ValueType ExpectedType() const { return expected_; }

  void Bind(const Value& value) { ResetValue(value); }

  [[nodiscard]] Expression Rebind(
      const std::shared_ptr<const PreparedValues>& values) const {
    return std::make_shared<ParameterSlot>(slot_index_, expected_, values);
  }

  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override {
    if (use_prepared_values_) {
      return values_->at(slot_index_);
    }
    return ConstantValue::Evaluate(row, schema);
  }
  [[nodiscard]] Value Evaluate(const Row* left, const Schema& left_schema,
                               const Row* right,
                               const Schema& right_schema) const override {
    if (use_prepared_values_) {
      return values_->at(slot_index_);
    }
    return ConstantValue::Evaluate(left, left_schema, right, right_schema);
  }
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& /*unused*/) const override {
    return TypeFor(expected_);
  }
  [[nodiscard]] tinylamb::Type ResultType(
      const Schema& /*unused*/, const Schema& /*unused*/) const override {
    return TypeFor(expected_);
  }
  [[nodiscard]] std::unordered_set<ColumnName> TouchedColumns() const override {
    return {};
  }
  [[nodiscard]] std::string ToString() const override {
    if (!use_prepared_values_) {
      return ConstantValue::ToString();
    }
    return "<?" + std::to_string(slot_index_) + ">";
  }
  void Dump(std::ostream& o) const override { o << ToString(); }

 private:
  static tinylamb::Type TypeFor(ValueType value_type) {
    switch (value_type) {
      case ValueType::kInt64:
        return {TypeTag::kBigInt};
      case ValueType::kDouble:
        return {TypeTag::kDouble};
      case ValueType::kVarChar:
        return {TypeTag::kVarChar};
      case ValueType::kDate:
        return {TypeTag::kDate};
      case ValueType::kArray:
        return {TypeTag::kArray};
      default:
        return {TypeTag::kInvalid};
    }
  }

  size_t slot_index_;
  ValueType expected_;
  std::shared_ptr<const PreparedValues> values_;
  bool use_prepared_values_;
};

// Pointer identity, or the same shape. Two ParameterSlots that currently hold
// the same value are still different parameters: `d_w_id = 1 AND d_id = 1`
// must not compare equal and let a rewrite move one slot onto the other column.
inline bool SamePrintedExpression(const Expression& left,
                                  const Expression& right) {
  if (left == right) {
    return true;
  }
  if (!left || !right || left->Type() != right->Type()) {
    return false;
  }
  if (left->Type() == TypeTag::kConstantValue) {
    const auto* left_slot = dynamic_cast<const ParameterSlot*>(left.get());
    const auto* right_slot = dynamic_cast<const ParameterSlot*>(right.get());
    if (left_slot != nullptr || right_slot != nullptr) {
      return left_slot != nullptr && right_slot != nullptr &&
             left_slot->Index() == right_slot->Index();
    }
  }
  return left->ToString() == right->ToString();
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
    case TypeTag::kAggregateExp: {
      // Same attribute set SlotizeLiterals rebuilds: a slot can live in the
      // WHERE filter, HAVING condition, secondary/trailing arguments, inner
      // ORDER BY or inner GROUP BY, not just the child.
      const auto& aggregate = expression->AsAggregateExpression();
      if (ContainsParameterSlot(aggregate.Child()) ||
          ContainsParameterSlot(aggregate.SecondaryArg()) ||
          ContainsParameterSlot(aggregate.WhereFilter()) ||
          ContainsParameterSlot(aggregate.HavingCondition())) {
        return true;
      }
      for (const Expression& extra : aggregate.TrailingArgs()) {
        if (ContainsParameterSlot(extra)) {
          return true;
        }
      }
      return std::ranges::any_of(
                 aggregate.InnerOrderBy(),
                 [](const WindowOrderTerm& term) {
                   return ContainsParameterSlot(term.expression);
                 }) ||
             std::ranges::any_of(aggregate.InnerGroupBy(),
                                 [](const Expression& key) {
                                   return ContainsParameterSlot(key);
                                 });
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      return std::ranges::any_of(searched.when_clauses_,
                                 [](const auto& clause) {
                                   return ContainsParameterSlot(clause.first) ||
                                          ContainsParameterSlot(clause.second);
                                 }) ||
             ContainsParameterSlot(searched.else_clause_);
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      if (ContainsParameterSlot(in.child_)) {
        return true;
      }
      return std::ranges::any_of(
          in.list_, [](const Expression& item) {
            return ContainsParameterSlot(item);
          });
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      return std::ranges::any_of(
          call.Args(), [](const Expression& arg) {
            return ContainsParameterSlot(arg);
          });
    }
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

inline bool IsBindableLiteral(const Value& value) {
  return !value.IsNull() &&
         (value.type == ValueType::kInt64 || value.type == ValueType::kDouble ||
          value.type == ValueType::kVarChar || value.type == ValueType::kDate);
}

// `expected` non-null: the constant must equal the next parameter or `ok` is
// cleared, and the slot is recorded. A null `expected` builds the INSERT form.
inline Expression SlotizeLiterals(  // NOLINT(misc-no-recursion)
    const Expression& expression, size_t* slot_cursor, bool* ok,
    const std::vector<Value>* expected = nullptr,
    std::vector<Expression>* slots = nullptr) {
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kConstantValue: {
      if (dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
        if (slots != nullptr) {
          slots->push_back(expression);
        }
        return expression;
      }
      const Value current = expression->AsConstantValue().GetValue();
      if (!IsBindableLiteral(current)) {
        return expression;
      }
      if (expected != nullptr) {
        if (*slot_cursor >= expected->size() ||
            (*expected)[*slot_cursor] != current) {
          *ok = false;
          return expression;
        }
        auto slot = std::make_shared<ParameterSlot>(*slot_cursor, current);
        if (slots != nullptr) {
          slots->push_back(slot);
        }
        ++(*slot_cursor);
        return slot;
      }
      return std::make_shared<ParameterSlot>(
          (*slot_cursor)++, current.type,
          std::make_shared<PreparedValues>(std::vector<Value>{}));
    }
    case TypeTag::kColumnValue:
      return expression;
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      // Slot numbering follows traversal order, so the two recursive calls
      // must be sequenced: passing them as function arguments leaves their
      // evaluation order unspecified (GCC numbers right-to-left, which
      // silently swaps `d_w_id = 1 AND d_id = 2` bindings on replay).
      Expression left =
          SlotizeLiterals(binary.Left(), slot_cursor, ok, expected, slots);
      if (!*ok) {
        return expression;
      }
      Expression right =
          SlotizeLiterals(binary.Right(), slot_cursor, ok, expected, slots);
      if (!*ok) {
        return expression;
      }
      return BinaryExpressionExp(std::move(left), binary.Op(),
                                 std::move(right));
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      return UnaryExpressionExp(
          SlotizeLiterals(unary.Child(), slot_cursor, ok, expected, slots),
          unary.Op());
    }
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      // Rebuild every expression-bearing attribute in the same SQL text order
      // BindExpression uses (child -> secondary -> trailing -> WHERE ->
      // HAVING -> inner ORDER BY -> inner GROUP BY), so slot numbering stays
      // identical. Rebuilding only type/child/distinct silently dropped
      // `AGG(arg WHERE cond)` filters, inner ordering, limits and extra
      // arguments on every parameterized plan.
      const auto slotize = [&](const Expression& inner) {
        return SlotizeLiterals(inner, slot_cursor, ok, expected, slots);
      };
      auto bound = std::make_shared<AggregateExpression>(
          aggregate.GetType(),
          SlotizeLiterals(aggregate.Child(), slot_cursor, ok, expected, slots),
          aggregate.Distinct());
      if (!*ok) {
        return expression;
      }
      if (aggregate.SecondaryArg()) {
        bound->SetSecondaryArg(slotize(aggregate.SecondaryArg()));
        if (!*ok) {
          return expression;
        }
      }
      if (!aggregate.TrailingArgs().empty()) {
        std::vector<Expression> trailing;
        trailing.reserve(aggregate.TrailingArgs().size());
        for (const Expression& extra : aggregate.TrailingArgs()) {
          if (!extra) {
            trailing.emplace_back();
            continue;
          }
          trailing.push_back(slotize(extra));
          if (!*ok) {
            return expression;
          }
        }
        bound->SetTrailingArgs(std::move(trailing));
      }
      if (aggregate.HasWhereFilter()) {
        bound->SetWhereFilter(slotize(aggregate.WhereFilter()));
        if (!*ok) {
          return expression;
        }
      }
      if (aggregate.Having() != AggregateHavingModifier::kNone) {
        bound->SetHaving(aggregate.Having(),
                         slotize(aggregate.HavingCondition()));
        if (!*ok) {
          return expression;
        }
      }
      if (!aggregate.InnerOrderBy().empty()) {
        std::vector<WindowOrderTerm> order;
        order.reserve(aggregate.InnerOrderBy().size());
        for (const WindowOrderTerm& term : aggregate.InnerOrderBy()) {
          WindowOrderTerm bound_term = term;
          bound_term.expression = slotize(term.expression);
          if (!*ok) {
            return expression;
          }
          order.push_back(std::move(bound_term));
        }
        bound->SetInnerOrderBy(std::move(order));
      }
      if (aggregate.InnerLimit().has_value()) {
        bound->SetInnerLimit(aggregate.InnerLimit());
      }
      if (aggregate.HasInnerGroupBy()) {
        std::vector<Expression> group_by;
        group_by.reserve(aggregate.InnerGroupBy().size());
        for (const Expression& key : aggregate.InnerGroupBy()) {
          group_by.push_back(slotize(key));
          if (!*ok) {
            return expression;
          }
        }
        bound->SetInnerGroupBy(std::move(group_by));
      }
      bound->SetArrayElementSqlType(aggregate.ArrayElementSqlType());
      return Expression{std::move(bound)};
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(searched.when_clauses_.size());
      for (const auto& clause : searched.when_clauses_) {
        // Sequenced: WHEN condition numbers before its result (pair members
        // passed as constructor arguments would evaluate in unspecified
        // order and scramble slot numbering).
        Expression condition =
            SlotizeLiterals(clause.first, slot_cursor, ok, expected, slots);
        if (!*ok) {
          return expression;
        }
        Expression result =
            SlotizeLiterals(clause.second, slot_cursor, ok, expected, slots);
        if (!*ok) {
          return expression;
        }
        clauses.emplace_back(std::move(condition), std::move(result));
      }
      Expression else_clause = SlotizeLiterals(
          searched.else_clause_, slot_cursor, ok, expected, slots);
      if (!*ok) {
        return expression;
      }
      return CaseExpressionExp(std::move(clauses), std::move(else_clause));
    }
    case TypeTag::kInExp: {
      // Child, then list: the same order ExtractSqlTemplate / BindExpression
      // consume, so `x IN (1, 2)` does not swap with a literal inside x.
      const auto& in = expression->AsInExpression();
      Expression child =
          SlotizeLiterals(in.child_, slot_cursor, ok, expected, slots);
      if (!*ok) {
        return expression;
      }
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(SlotizeLiterals(item, slot_cursor, ok, expected, slots));
        if (!*ok) {
          return expression;
        }
      }
      return InExpressionExp(std::move(child), std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& call = expression->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(call.Args().size());
      for (size_t i = 0; i < call.Args().size(); ++i) {
        // Structural arguments (operator names, field paths, interval units)
        // are not SQL literals. Numbering them would shift every later slot.
        const std::string& func = call.FuncName();
        const bool structural =
            (func == "__quantified__" && i >= 2) ||
            (func == "__struct_json__" && i % 3 != 1) ||
            ((func == "make_interval" || func == "get_field") && i == 1);
        if (structural) {
          args.push_back(call.Args()[i]);
          continue;
        }
        args.push_back(
            SlotizeLiterals(call.Args()[i], slot_cursor, ok, expected, slots));
        if (!*ok) {
          return expression;
        }
      }
      return FunctionCallExp(call.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& array = expression->AsArrayExpression();
      std::vector<Expression> elements;
      elements.reserve(array.Elements().size());
      for (const Expression& element : array.Elements()) {
        elements.push_back(
            SlotizeLiterals(element, slot_cursor, ok, expected, slots));
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
      *ok = false;
      return expression;
  }
}

inline void AppendParameterSlots(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::vector<Expression>* slots) {
  if (!expression || slots == nullptr) {
    return;
  }
  if (expression->Type() == TypeTag::kConstantValue) {
    if (dynamic_cast<const ParameterSlot*>(expression.get()) != nullptr) {
      slots->push_back(expression);
    }
    return;
  }
  switch (expression->Type()) {
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      AppendParameterSlots(binary.Left(), slots);
      AppendParameterSlots(binary.Right(), slots);
      return;
    }
    case TypeTag::kUnaryExp:
      AppendParameterSlots(expression->AsUnaryExpression().Child(), slots);
      return;
    case TypeTag::kAggregateExp: {
      const auto& aggregate = expression->AsAggregateExpression();
      AppendParameterSlots(aggregate.Child(), slots);
      AppendParameterSlots(aggregate.SecondaryArg(), slots);
      for (const Expression& extra : aggregate.TrailingArgs()) {
        AppendParameterSlots(extra, slots);
      }
      AppendParameterSlots(aggregate.WhereFilter(), slots);
      AppendParameterSlots(aggregate.HavingCondition(), slots);
      for (const WindowOrderTerm& term : aggregate.InnerOrderBy()) {
        AppendParameterSlots(term.expression, slots);
      }
      for (const Expression& key : aggregate.InnerGroupBy()) {
        AppendParameterSlots(key, slots);
      }
      return;
    }
    case TypeTag::kCaseExp: {
      const auto& searched = expression->AsCaseExpression();
      for (const auto& clause : searched.when_clauses_) {
        AppendParameterSlots(clause.first, slots);
        AppendParameterSlots(clause.second, slots);
      }
      AppendParameterSlots(searched.else_clause_, slots);
      return;
    }
    case TypeTag::kInExp: {
      const auto& in = expression->AsInExpression();
      AppendParameterSlots(in.child_, slots);
      for (const Expression& item : in.list_) {
        AppendParameterSlots(item, slots);
      }
      return;
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& arg :
           expression->AsFunctionCallExpression().Args()) {
        AppendParameterSlots(arg, slots);
      }
      return;
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        AppendParameterSlots(element, slots);
      }
      return;
    default:
      return;
  }
}

inline bool SlotizeInPlace(Expression* expression,
                           const std::vector<Value>& /*parameters*/,
                           size_t* /*cursor*/, std::vector<Expression>* slots) {
  if (expression == nullptr || !*expression) {
    return true;
  }
  AppendParameterSlots(*expression, slots);
  return true;
}

}  // namespace tinylamb

#endif  // TINYLAMB_PARAMETER_SLOT_HPP
